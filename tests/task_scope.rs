use std::{
    net::{SocketAddr, TcpListener, TcpStream},
    path::Path,
    process::{Child, Command, Output, Stdio},
    thread,
    time::{Duration, Instant},
};

#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;

use serde_json::Value;
use tempfile::tempdir;

const BINARY: &str = env!("CARGO_BIN_EXE_clarity");

#[cfg(unix)]
#[test]
fn amp_launcher_manages_presence_and_retains_completion_evidence() {
    let directory = tempdir().unwrap();
    git_init(directory.path());
    let tools = directory.path().join("tools");
    std::fs::create_dir(&tools).unwrap();
    let receipt = directory.path().join("live-session.json");
    let amp = tools.join("amp");
    std::fs::write(
        &amp,
        r#"#!/bin/sh
set -eu
work_id="$($CLARITY_BIN --endpoint "$CLARITY_ENDPOINT" claim \
  --summary "Exercise automatic harness coordination" \
  --resource tests/task_scope.rs --id-only)"
$CLARITY_BIN --endpoint "$CLARITY_ENDPOINT" observe > "$CLARITY_RECEIPT"
$CLARITY_BIN --endpoint "$CLARITY_ENDPOINT" signal done \
  --work "$work_id" \
  --summary "Automatic harness coordination completed" \
  --artifact test_receipt:amp-launcher-e2e
"#,
    )
    .unwrap();
    let mut permissions = std::fs::metadata(&amp).unwrap().permissions();
    permissions.set_mode(0o755);
    std::fs::set_permissions(&amp, permissions).unwrap();

    let http_address = available_address();
    let endpoint = format!("http://{http_address}");
    let up = Command::new(BINARY)
        .args(["--endpoint", &endpoint, "up"])
        .current_dir(directory.path())
        .env("RUST_LOG", "error")
        .output()
        .unwrap();
    assert_success(&up);
    let daemon_record: Value = serde_json::from_slice(
        &std::fs::read(directory.path().join(".clarity/daemon.json")).unwrap(),
    )
    .unwrap();
    let mut daemon = ProcessGuard(u32::try_from(daemon_record["pid"].as_u64().unwrap()).unwrap());
    create_workspace(directory.path(), &endpoint, "TG-182");

    let path = format!(
        "{}:{}",
        tools.display(),
        std::env::var("PATH").unwrap_or_default()
    );
    let launched = Command::new(BINARY)
        .args(["--endpoint", &endpoint, "run", "amp", "TG-182"])
        .current_dir(directory.path())
        .env("PATH", path)
        .env("CLARITY_BIN", BINARY)
        .env("CLARITY_RECEIPT", &receipt)
        .env_remove("CLARITY_PARTICIPANT_ID")
        .env_remove("CLARITY_SCOPE")
        .env_remove("LINEAR_API_KEY")
        .env("RUST_LOG", "error")
        .output()
        .unwrap();
    assert_success(&launched);

    let live: Value = serde_json::from_slice(&std::fs::read(&receipt).unwrap()).unwrap();
    assert_eq!(live["scope"], "linear:TG-182");
    assert_eq!(live["participants"].as_array().unwrap().len(), 1);
    assert_eq!(live["participants"][0]["manifest"]["harness"], "amp");
    assert!(live["participants"][0]["id"].is_string());
    assert_eq!(live["active_work"].as_array().unwrap().len(), 1);

    let after = observe(directory.path(), &endpoint, "linear:TG-182");
    assert!(after["participants"].as_array().unwrap().is_empty());
    assert!(after["active_work"].as_array().unwrap().is_empty());
    assert_eq!(after["signals"].as_array().unwrap().len(), 1);
    assert_eq!(
        after["signals"][0]["signal"]["summary"],
        "Automatic harness coordination completed"
    );
    assert_eq!(
        after["signals"][0]["signal"]["evidence"][0]["uri"],
        "amp-launcher-e2e"
    );
    daemon.stop();
}

#[test]
fn task_local_scope_wins_over_stale_checkout_selection() {
    let directory = tempdir().unwrap();
    git_init(directory.path());
    std::fs::create_dir(directory.path().join(".clarity")).unwrap();
    std::fs::write(
        directory.path().join(".clarity/active-workspace"),
        "linear:TG-187\n",
    )
    .unwrap();

    let http_address = available_address();
    let endpoint = format!("http://{http_address}");
    let daemon = Command::new(BINARY)
        .args([
            "serve",
            "--listen",
            &http_address.to_string(),
            "--database",
            ".clarity/task-scope.db",
            "--owner-token",
            "task-scope-test",
            "--repository",
            "local/task-scope-test",
        ])
        .current_dir(directory.path())
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let mut daemon = ChildGuard(daemon);
    wait_for_listener(http_address);

    create_workspace(directory.path(), &endpoint, "TG-187");
    create_workspace(directory.path(), &endpoint, "TG-182");

    let task_child = Command::new(BINARY)
        .args([
            "--endpoint",
            &endpoint,
            "session",
            "--scope",
            "linear:TG-182",
            "--name",
            "scope-test",
            "--harness",
            "test",
            "--",
            BINARY,
            "--endpoint",
            &endpoint,
            "observe",
        ])
        .current_dir(directory.path())
        .env_remove("CLARITY_SCOPE")
        .env("RUST_LOG", "error")
        .output()
        .unwrap();
    assert_success(&task_child);
    assert_eq!(output_scope(&task_child), "linear:TG-182");

    let unrelated = Command::new(BINARY)
        .args(["--endpoint", &endpoint, "observe"])
        .current_dir(directory.path())
        .env_remove("CLARITY_SCOPE")
        .env_remove("CLARITY_PARTICIPANT_ID")
        .env("RUST_LOG", "error")
        .output()
        .unwrap();
    assert_success(&unrelated);
    assert_eq!(output_scope(&unrelated), "linear:TG-187");

    let explicit = Command::new(BINARY)
        .args([
            "--endpoint",
            &endpoint,
            "observe",
            "--scope",
            "linear:TG-187",
        ])
        .current_dir(directory.path())
        .env("CLARITY_SCOPE", "linear:TG-182")
        .env("RUST_LOG", "error")
        .output()
        .unwrap();
    assert_success(&explicit);
    assert_eq!(output_scope(&explicit), "linear:TG-187");

    assert_eq!(
        std::fs::read_to_string(directory.path().join(".clarity/active-workspace")).unwrap(),
        "linear:TG-187\n"
    );
    daemon.stop();
}

fn create_workspace(directory: &Path, endpoint: &str, issue: &str) {
    let output = Command::new(BINARY)
        .args([
            "--endpoint",
            endpoint,
            "workspace",
            "create",
            "--issue",
            issue,
            "--title",
            issue,
            "--objective",
            issue,
        ])
        .current_dir(directory)
        .output()
        .unwrap();
    assert_success(&output);
}

fn output_scope(output: &Output) -> String {
    let value: Value = serde_json::from_slice(&output.stdout).unwrap();
    value["scope"].as_str().unwrap().to_owned()
}

fn observe(directory: &Path, endpoint: &str, scope: &str) -> Value {
    let output = Command::new(BINARY)
        .args(["--endpoint", endpoint, "observe", "--scope", scope])
        .current_dir(directory)
        .env("RUST_LOG", "error")
        .output()
        .unwrap();
    assert_success(&output);
    serde_json::from_slice(&output.stdout).unwrap()
}

fn assert_success(output: &Output) {
    assert!(
        output.status.success(),
        "command failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

fn git_init(directory: &Path) {
    let status = Command::new("git")
        .args(["init", "-q"])
        .current_dir(directory)
        .status()
        .unwrap();
    assert!(status.success());
}

fn available_address() -> SocketAddr {
    let http = TcpListener::bind("127.0.0.1:0").unwrap();
    http.local_addr().unwrap()
}

fn wait_for_listener(address: SocketAddr) {
    let deadline = Instant::now() + Duration::from_secs(5);
    while TcpStream::connect(address).is_err() {
        assert!(
            Instant::now() < deadline,
            "daemon did not start at {address}"
        );
        thread::sleep(Duration::from_millis(20));
    }
}

struct ChildGuard(Child);

impl ChildGuard {
    fn stop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

impl Drop for ChildGuard {
    fn drop(&mut self) {
        self.stop();
    }
}

struct ProcessGuard(u32);

impl ProcessGuard {
    fn stop(&mut self) {
        if self.0 == 0 {
            return;
        }
        let _ = Command::new("kill").arg(self.0.to_string()).status();
        self.0 = 0;
    }
}

impl Drop for ProcessGuard {
    fn drop(&mut self) {
        self.stop();
    }
}
