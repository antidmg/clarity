use std::{
    collections::hash_map::DefaultHasher,
    hash::{Hash, Hasher},
    path::{Path, PathBuf},
    process::Command,
};

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::protocol::{LinearIssue, LinearMetadata, RepositoryRef, Workspace};

pub const ACTIVE_WORKSPACE_FILE: &str = ".clarity/active-workspace";
pub const DAEMON_RECORD_FILE: &str = ".clarity/daemon.json";
pub const DAEMON_PROTOCOL_VERSION: u32 = 1;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RepositoryContext {
    pub root: PathBuf,
    pub identity: String,
    pub revision: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct DaemonRecord {
    pub endpoint: String,
    pub pid: u32,
    #[serde(default)]
    pub owner_token: Option<String>,
    #[serde(default)]
    pub repository: Option<String>,
    #[serde(default)]
    pub protocol_version: Option<u32>,
    #[serde(default)]
    pub build_id: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct DaemonIdentity {
    pub owner_token: String,
    pub repository: String,
    pub protocol_version: u32,
    pub build_id: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DaemonProbe {
    Absent,
    Identified(DaemonIdentity),
    ForeignResponse,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DaemonDisposition {
    OwnedCompatible,
    OwnedStale,
    Foreign,
    Absent,
}

#[derive(Debug, Error)]
pub enum BootstrapError {
    #[error("I/O failed: {0}")]
    Io(#[from] std::io::Error),
    #[error("Git command failed: {0}")]
    Git(String),
    #[error("repository origin is not a recognizable Git URL: {0}")]
    InvalidOrigin(String),
    #[error("no active Clarity workstream in this checkout; run `clarity up TG-*` first")]
    MissingSelection,
    #[error("invalid daemon record: {0}")]
    InvalidDaemonRecord(#[from] serde_json::Error),
}

/// Finds the enclosing Git root and reads its origin identity and current commit,
/// when the repository has one.
///
/// # Errors
///
/// Returns an error when `start` is not in a Git repository, Git cannot run,
/// or its origin cannot identify a repository. An unborn repository is valid
/// and has no revision.
pub fn discover_repository(start: &Path) -> Result<RepositoryContext, BootstrapError> {
    let root = git(start, &["rev-parse", "--show-toplevel"])?;
    let root = PathBuf::from(root);
    let revision = git_head(&root)?;
    let identity = match git_optional(&root, &["remote", "get-url", "origin"])? {
        Some(origin) => repository_identity(&origin)?,
        None => local_repository_identity(&root)?,
    };
    Ok(RepositoryContext {
        root,
        identity,
        revision,
    })
}

fn local_repository_identity(root: &Path) -> Result<String, BootstrapError> {
    root.file_name()
        .and_then(|name| name.to_str())
        .filter(|name| !name.is_empty())
        .map(|name| format!("local/{name}"))
        .ok_or_else(|| BootstrapError::Git("repository root has no usable directory name".into()))
}

fn git_head(directory: &Path) -> Result<Option<String>, BootstrapError> {
    let output = Command::new("git")
        .args(["rev-parse", "--verify", "--quiet", "HEAD"])
        .current_dir(directory)
        .output()?;
    if output.status.success() {
        return Ok(Some(
            String::from_utf8_lossy(&output.stdout).trim().to_owned(),
        ));
    }
    if output.status.code() == Some(1) {
        return Ok(None);
    }
    Err(BootstrapError::Git(
        String::from_utf8_lossy(&output.stderr).trim().to_owned(),
    ))
}

fn git(directory: &Path, arguments: &[&str]) -> Result<String, BootstrapError> {
    git_optional(directory, arguments)?.ok_or_else(|| {
        BootstrapError::Git(format!("git {} returned no output", arguments.join(" ")))
    })
}

fn git_optional(directory: &Path, arguments: &[&str]) -> Result<Option<String>, BootstrapError> {
    let output = Command::new("git")
        .args(arguments)
        .current_dir(directory)
        .output()?;
    if !output.status.success() {
        let detail = String::from_utf8_lossy(&output.stderr).trim().to_owned();
        return if arguments == ["remote", "get-url", "origin"] {
            Ok(None)
        } else {
            Err(BootstrapError::Git(detail))
        };
    }
    Ok(Some(
        String::from_utf8_lossy(&output.stdout).trim().to_owned(),
    ))
}

fn repository_identity(origin: &str) -> Result<String, BootstrapError> {
    let without_suffix = origin.trim().trim_end_matches('/').trim_end_matches(".git");
    let path = if let Some((_, path)) = without_suffix.split_once("://") {
        path.split_once('/').map(|(_, path)| path)
    } else if let Some((_, path)) = without_suffix.split_once(':') {
        Some(path)
    } else {
        Some(without_suffix)
    };
    let path = path
        .filter(|path| path.split('/').count() >= 2)
        .ok_or_else(|| BootstrapError::InvalidOrigin(origin.to_owned()))?;
    Ok(path.to_owned())
}

pub fn workspace_from_issue(
    identifier: String,
    title: String,
    url: String,
    description: Option<String>,
    metadata: Option<LinearMetadata>,
    repository: &RepositoryContext,
) -> Workspace {
    Workspace {
        scope: format!("linear:{identifier}"),
        objective: description
            .filter(|description| !description.trim().is_empty())
            .unwrap_or_else(|| title.clone()),
        title,
        linear_issue: LinearIssue {
            identifier,
            url: Some(url),
            metadata,
        },
        repository: Some(RepositoryRef {
            repository: repository.identity.clone(),
            revision: repository.revision.clone(),
        }),
    }
}

/// Atomically selects a canonical workspace scope for one checkout.
///
/// # Errors
///
/// Returns an error when the checkout-local state directory or pointer cannot
/// be written.
pub fn write_selection(root: &Path, scope: &str) -> Result<(), BootstrapError> {
    let path = root.join(ACTIVE_WORKSPACE_FILE);
    let parent = root.join(".clarity");
    std::fs::create_dir_all(parent)?;
    let temporary = root.join(".clarity/active-workspace.tmp");
    std::fs::write(&temporary, format!("{scope}\n"))?;
    std::fs::rename(temporary, path)?;
    Ok(())
}

/// Reads the active canonical workspace scope for one checkout.
///
/// # Errors
///
/// Returns [`BootstrapError::MissingSelection`] when no non-empty pointer
/// exists, or an I/O error when it cannot be read.
pub fn read_selection(root: &Path) -> Result<String, BootstrapError> {
    let scope = std::fs::read_to_string(root.join(ACTIVE_WORKSPACE_FILE)).map_err(|error| {
        match error.kind() {
            std::io::ErrorKind::NotFound => BootstrapError::MissingSelection,
            _ => BootstrapError::Io(error),
        }
    })?;
    let scope = scope.trim();
    if scope.is_empty() {
        return Err(BootstrapError::MissingSelection);
    }
    Ok(scope.to_owned())
}

pub fn daemon_disposition(
    endpoint: &str,
    repository: &str,
    build_id: &str,
    record: Option<&DaemonRecord>,
    probe: &DaemonProbe,
) -> DaemonDisposition {
    let legacy_owned = record.is_some_and(|record| {
        record.endpoint == endpoint
            && record.owner_token.is_none()
            && record.repository.is_none()
            && record.protocol_version.is_none()
            && record.build_id.is_none()
    });
    match probe {
        DaemonProbe::Absent if record.is_some() => DaemonDisposition::OwnedStale,
        DaemonProbe::Absent => DaemonDisposition::Absent,
        DaemonProbe::ForeignResponse if legacy_owned => DaemonDisposition::OwnedStale,
        DaemonProbe::ForeignResponse => DaemonDisposition::Foreign,
        DaemonProbe::Identified(identity) => {
            if legacy_owned {
                return DaemonDisposition::OwnedStale;
            }
            let owned = record.is_some_and(|record| {
                record.endpoint == endpoint
                    && record.owner_token.as_deref() == Some(&identity.owner_token)
                    && record.repository.as_deref() == Some(&identity.repository)
            });
            if !owned || identity.repository != repository {
                return DaemonDisposition::Foreign;
            }
            if identity.protocol_version == DAEMON_PROTOCOL_VERSION && identity.build_id == build_id
            {
                DaemonDisposition::OwnedCompatible
            } else {
                DaemonDisposition::OwnedStale
            }
        }
    }
}

/// Identifies the exact executable image used by this process. The daemon
/// captures this value at startup, so replacing an installed binary cannot
/// make an already-running daemon appear current.
///
/// # Errors
///
/// Returns an error when the current executable cannot be located or read.
pub fn current_build_id() -> Result<String, BootstrapError> {
    let executable = std::env::current_exe()?;
    let bytes = std::fs::read(executable)?;
    let mut hasher = DefaultHasher::new();
    bytes.hash(&mut hasher);
    Ok(format!("{:016x}", hasher.finish()))
}

/// Reads the checkout-local daemon process record when present.
///
/// # Errors
///
/// Returns an error when the record cannot be read or decoded.
pub fn read_daemon_record(root: &Path) -> Result<Option<DaemonRecord>, BootstrapError> {
    let path = root.join(DAEMON_RECORD_FILE);
    match std::fs::read(path) {
        Ok(contents) => Ok(Some(serde_json::from_slice(&contents)?)),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error.into()),
    }
}

/// Persists the process identity of a daemon confirmed healthy by the caller.
///
/// # Errors
///
/// Returns an error when the state directory cannot be created, the record
/// cannot be serialized, or the file cannot be written.
pub fn write_daemon_record(root: &Path, record: &DaemonRecord) -> Result<(), BootstrapError> {
    let path = root.join(DAEMON_RECORD_FILE);
    std::fs::create_dir_all(root.join(".clarity"))?;
    std::fs::write(path, serde_json::to_vec_pretty(record)?)?;
    Ok(())
}

/// Removes a checkout-local daemon record after its process is gone.
///
/// # Errors
///
/// Returns an error when an existing record cannot be removed.
pub fn remove_daemon_record(root: &Path) -> Result<(), BootstrapError> {
    match std::fs::remove_file(root.join(DAEMON_RECORD_FILE)) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error.into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn discovers_repository_from_nested_directory() {
        let directory = tempdir().unwrap();
        run_git(directory.path(), &["init", "-q"]);
        run_git(
            directory.path(),
            &["config", "user.email", "test@example.com"],
        );
        run_git(directory.path(), &["config", "user.name", "Test"]);
        run_git(
            directory.path(),
            &[
                "remote",
                "add",
                "origin",
                "git@github.com:antidmg/clarity.git",
            ],
        );
        std::fs::write(directory.path().join("README"), "test").unwrap();
        run_git(directory.path(), &["add", "README"]);
        run_git(directory.path(), &["commit", "-qm", "initial"]);
        let nested = directory.path().join("one/two");
        std::fs::create_dir_all(&nested).unwrap();

        let repository = discover_repository(&nested).unwrap();

        assert_eq!(repository.root, directory.path().canonicalize().unwrap());
        assert_eq!(repository.identity, "antidmg/clarity");
        assert_eq!(repository.revision.unwrap().len(), 40);
    }

    #[test]
    fn discovers_repository_before_its_first_commit() {
        let directory = tempdir().unwrap();
        run_git(directory.path(), &["init", "-q"]);
        run_git(
            directory.path(),
            &[
                "remote",
                "add",
                "origin",
                "git@github.com:antidmg/clarity.git",
            ],
        );

        let repository = discover_repository(directory.path()).unwrap();

        assert_eq!(repository.identity, "antidmg/clarity");
        assert_eq!(repository.revision, None);
    }

    #[test]
    fn discovers_local_repository_without_an_origin() {
        let directory = tempdir().unwrap();
        let root = directory.path().join("prototype");
        std::fs::create_dir(&root).unwrap();
        run_git(&root, &["init", "-q"]);

        let repository = discover_repository(&root).unwrap();

        assert_eq!(repository.identity, "local/prototype");
        assert_eq!(repository.revision, None);
    }

    #[test]
    fn selection_round_trips() {
        let directory = tempdir().unwrap();
        write_selection(directory.path(), "linear:TG-183").unwrap();
        assert_eq!(read_selection(directory.path()).unwrap(), "linear:TG-183");
    }

    #[test]
    fn maps_linear_issue_to_canonical_workspace() {
        let repository = RepositoryContext {
            root: PathBuf::from("/tmp/clarity"),
            identity: "antidmg/clarity".into(),
            revision: Some("abc123".into()),
        };
        let workspace = workspace_from_issue(
            "TG-183".into(),
            "One-command entry".into(),
            "https://linear.app/issue/TG-183".into(),
            Some("Create or reconnect the workspace".into()),
            None,
            &repository,
        );

        assert_eq!(workspace.scope, "linear:TG-183");
        assert_eq!(workspace.objective, "Create or reconnect the workspace");
        assert_eq!(workspace.repository.unwrap().repository, "antidmg/clarity");
    }

    #[test]
    fn daemon_state_distinguishes_ownership_and_compatibility() {
        let record = DaemonRecord {
            endpoint: "http://127.0.0.1:7331".into(),
            pid: 42,
            owner_token: Some("ours".into()),
            repository: Some("antidmg/clarity".into()),
            protocol_version: Some(DAEMON_PROTOCOL_VERSION),
            build_id: Some("current".into()),
        };
        let identity = DaemonIdentity {
            owner_token: "ours".into(),
            repository: "antidmg/clarity".into(),
            protocol_version: DAEMON_PROTOCOL_VERSION,
            build_id: "current".into(),
        };
        let compatible = DaemonProbe::Identified(identity.clone());
        let stale = DaemonProbe::Identified(DaemonIdentity {
            build_id: "old".into(),
            ..identity
        });
        let legacy_record = DaemonRecord {
            endpoint: record.endpoint.clone(),
            pid: 41,
            owner_token: None,
            repository: None,
            protocol_version: None,
            build_id: None,
        };

        assert_eq!(
            daemon_disposition(
                &record.endpoint,
                "antidmg/clarity",
                "current",
                Some(&record),
                &compatible,
            ),
            DaemonDisposition::OwnedCompatible
        );
        assert_eq!(
            daemon_disposition(
                &record.endpoint,
                "antidmg/clarity",
                "current",
                Some(&record),
                &stale,
            ),
            DaemonDisposition::OwnedStale
        );
        assert_eq!(
            daemon_disposition(
                &record.endpoint,
                "antidmg/clarity",
                "current",
                Some(&legacy_record),
                &DaemonProbe::ForeignResponse,
            ),
            DaemonDisposition::OwnedStale
        );
        assert_eq!(
            daemon_disposition(
                &record.endpoint,
                "antidmg/clarity",
                "current",
                Some(&legacy_record),
                &compatible,
            ),
            DaemonDisposition::OwnedStale
        );
        assert_eq!(
            daemon_disposition(
                &record.endpoint,
                "antidmg/clarity",
                "current",
                None,
                &compatible,
            ),
            DaemonDisposition::Foreign
        );
        assert_eq!(
            daemon_disposition(
                &record.endpoint,
                "antidmg/clarity",
                "current",
                None,
                &DaemonProbe::Absent,
            ),
            DaemonDisposition::Absent
        );
        assert_eq!(
            daemon_disposition(
                &record.endpoint,
                "antidmg/clarity",
                "current",
                Some(&record),
                &DaemonProbe::Absent,
            ),
            DaemonDisposition::OwnedStale
        );
    }

    fn run_git(directory: &Path, arguments: &[&str]) {
        assert!(
            Command::new("git")
                .args(arguments)
                .current_dir(directory)
                .status()
                .unwrap()
                .success()
        );
    }
}
