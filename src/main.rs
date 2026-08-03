use std::{
    net::SocketAddr,
    path::{Path, PathBuf},
    process::Stdio,
    str::FromStr,
    time::Duration,
};

use clap::{Parser, Subcommand, ValueEnum};
use clarity::{
    Runtime,
    bootstrap::{
        BootstrapError, DAEMON_PROTOCOL_VERSION, DAEMON_RECORD_FILE, DaemonDisposition,
        DaemonIdentity, DaemonProbe, DaemonRecord, RepositoryContext, current_build_id,
        discover_repository, read_daemon_record, read_selection, remove_daemon_record,
        workspace_from_issue, write_daemon_record, write_selection,
    },
    protocol::{
        ArtifactKind, ArtifactRef, Command, CommandResult, Direction, DirectionId, DirectionTarget,
        EnvironmentManifest, EventEnvelope, LinearIssue, LinearMetadata, ParticipantId,
        RepositoryRef, RequestId, RequestIdentity, ScopeSnapshot, Signal, WorkId, Workspace,
    },
};
use reqwest::Client;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use thiserror::Error;
use tokio::process::Command as ProcessCommand;
use tracing_subscriber::EnvFilter;
use uuid::Uuid;

const TASK_SCOPE_ENV: &str = "CLARITY_SCOPE";

#[derive(Parser)]
#[command(version, about)]
struct Cli {
    #[arg(
        long,
        env = "CLARITY_ENDPOINT",
        default_value = "http://127.0.0.1:7331"
    )]
    endpoint: String,
    #[command(subcommand)]
    command: CliCommand,
}

#[derive(Subcommand)]
enum CliCommand {
    /// Bring this repository's local coordination service online.
    Up {
        /// Deprecated compatibility alias for selecting a Linear workspace.
        issue: Option<String>,
    },
    /// Resolve a task workspace and run an agent in it.
    Run {
        #[arg(value_enum)]
        harness: AgentHarness,
        /// Linear issue identifier, for example TG-187.
        issue: String,
        /// Additional arguments passed to the harness.
        #[arg(last = true)]
        child_arguments: Vec<String>,
    },
    /// Run the single-writer coordination daemon.
    Serve {
        #[arg(long, default_value = "127.0.0.1:7331")]
        listen: String,
        #[arg(long, default_value = ".clarity/clarity.db")]
        database: PathBuf,
        #[arg(long, hide = true)]
        owner_token: Option<String>,
        #[arg(long, hide = true)]
        repository: Option<String>,
    },
    /// Create or verify the durable workspace for a Linear issue.
    Workspace {
        #[command(subcommand)]
        command: WorkspaceCommand,
    },
    /// Run a process as a leased Clarity participant.
    Session {
        #[arg(long)]
        scope: Option<String>,
        #[arg(long)]
        name: String,
        #[arg(long)]
        harness: String,
        #[arg(long)]
        repository: Option<String>,
        #[arg(long)]
        revision: Option<String>,
        #[arg(long)]
        worktree: Option<String>,
        #[arg(long = "capability")]
        capabilities: Vec<String>,
        #[arg(long, default_value_t = 15)]
        lease_seconds: u64,
        #[arg(last = true, required = true)]
        child: Vec<String>,
    },
    /// Atomically claim resources before starting work.
    Claim {
        #[arg(long, env = "CLARITY_PARTICIPANT_ID")]
        participant: ParticipantId,
        #[arg(long)]
        summary: String,
        #[arg(long = "resource", required = true)]
        resources: Vec<String>,
        #[arg(long)]
        id_only: bool,
    },
    /// Publish a typed coordination signal.
    Signal {
        #[arg(long, env = "CLARITY_PARTICIPANT_ID")]
        participant: ParticipantId,
        #[arg(long)]
        work: Option<WorkId>,
        #[arg(value_enum)]
        kind: SignalKind,
        #[arg(long)]
        summary: String,
        #[arg(long = "artifact")]
        artifacts: Vec<ArtifactArgument>,
        #[arg(long)]
        requested_action: Option<String>,
        #[arg(long = "choice")]
        choices: Vec<String>,
        #[arg(long)]
        recommendation: Option<String>,
        #[arg(long)]
        known_risk: Option<String>,
        #[arg(long)]
        request_id: Option<RequestId>,
        #[arg(long)]
        request_key: Option<String>,
        #[arg(long)]
        supersedes_request: Option<RequestId>,
    },
    /// Read the current projection for a scope.
    Observe {
        #[arg(long)]
        scope: Option<String>,
    },
    /// Replay canonical events for a scope.
    Events {
        #[arg(long)]
        scope: Option<String>,
        #[arg(long, default_value_t = 0)]
        after: u64,
    },
    /// Publish a human instruction into the coordination protocol.
    Intervene {
        #[arg(long)]
        scope: Option<String>,
        #[arg(long)]
        message: String,
        #[arg(long, default_value = "human")]
        author: String,
        #[arg(long, requires_all = ["participant", "request"])]
        source_event: Option<Uuid>,
        #[arg(long, requires = "source_event")]
        request: Option<RequestId>,
        #[arg(long, requires = "source_event")]
        participant: Option<ParticipantId>,
        #[arg(long, requires = "source_event")]
        work: Option<WorkId>,
    },
    /// Receive relevant targeted direction and workspace broadcasts.
    Directions {
        #[arg(long, env = "CLARITY_PARTICIPANT_ID")]
        participant: ParticipantId,
    },
    /// Durably record that this harness consumed a delivered direction.
    ConsumeDirection {
        #[arg(long, env = "CLARITY_PARTICIPANT_ID")]
        participant: ParticipantId,
        direction: DirectionId,
    },
    /// Explicitly resolve one attention request after handling it.
    ResolveRequest {
        #[arg(long, env = "CLARITY_PARTICIPANT_ID")]
        participant: ParticipantId,
        request: RequestId,
        #[arg(long)]
        summary: String,
    },
}

#[derive(Subcommand)]
enum WorkspaceCommand {
    Create {
        #[arg(long)]
        issue: String,
        #[arg(long)]
        title: String,
        #[arg(long)]
        objective: String,
        #[arg(long)]
        url: Option<String>,
        #[arg(long)]
        repository: Option<String>,
        #[arg(long)]
        revision: Option<String>,
    },
}

#[derive(Clone, Copy, ValueEnum)]
enum SignalKind {
    Working,
    Finding,
    Offering,
    HelpNeeded,
    DecisionNeeded,
    Blocked,
    Checkpoint,
    ReviewRequested,
    Done,
}

#[derive(Clone, Copy, ValueEnum)]
enum AgentHarness {
    Amp,
}

impl AgentHarness {
    fn executable(self) -> &'static str {
        match self {
            Self::Amp => "amp",
        }
    }
}

#[derive(Clone)]
struct ArtifactArgument(ArtifactRef);

struct SignalArguments {
    kind: SignalKind,
    summary: String,
    artifacts: Vec<ArtifactArgument>,
    requested_action: Option<String>,
    choices: Vec<String>,
    recommendation: Option<String>,
    known_risk: Option<String>,
    request_id: Option<RequestId>,
    request_key: Option<String>,
    supersedes_request: Option<RequestId>,
}

struct InterventionArguments {
    scope: Option<String>,
    message: String,
    author: String,
    source_event: Option<Uuid>,
    request_id: Option<RequestId>,
    participant: Option<ParticipantId>,
    work: Option<WorkId>,
}

impl FromStr for ArtifactArgument {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let (kind, uri) = value
            .split_once(':')
            .ok_or_else(|| "artifact must be KIND:URI".to_owned())?;
        if uri.trim().is_empty() {
            return Err("artifact URI must not be empty".into());
        }
        let kind = match kind {
            "file" => ArtifactKind::File,
            "patch" => ArtifactKind::Patch,
            "revision" => ArtifactKind::Revision,
            "test_receipt" => ArtifactKind::TestReceipt,
            "url" => ArtifactKind::Url,
            other => return Err(format!("unknown artifact kind: {other}")),
        };
        Ok(Self(ArtifactRef {
            kind,
            uri: uri.to_owned(),
            digest: None,
        }))
    }
}

#[derive(Debug, Error)]
enum CliError {
    #[error(transparent)]
    Bootstrap(#[from] BootstrapError),
    #[error("request failed: {0}")]
    Request(#[from] reqwest::Error),
    #[error("runtime failed: {0}")]
    Runtime(#[from] clarity::RuntimeError),
    #[error("I/O failed: {0}")]
    Io(#[from] std::io::Error),
    #[error("invalid listen address: {0}")]
    Address(#[from] std::net::AddrParseError),
    #[error("daemon rejected command: {0}")]
    Rejected(String),
    #[error("session join returned no participant identity")]
    MissingParticipant,
    #[error("child process exited unsuccessfully: {0}")]
    Child(std::process::ExitStatus),
    #[error("session heartbeat stopped unexpectedly")]
    HeartbeatStopped,
    #[error("session interrupted")]
    Interrupted,
    #[error("session task failed: {0}")]
    Task(#[from] tokio::task::JoinError),
    #[error(
        "LINEAR_API_KEY is not set; create a Linear personal API key and export LINEAR_API_KEY"
    )]
    MissingLinearApiKey,
    #[error("Linear API rejected the request: {0}")]
    Linear(String),
    #[error("invalid Linear issue identifier `{0}`; expected a value such as TG-183")]
    InvalidIssue(String),
    #[error(
        "daemon at {endpoint} is not owned by or compatible with this checkout; recover with exactly: `{recovery}`"
    )]
    ForeignDaemon { endpoint: String, recovery: String },
    #[error("configured endpoint must be a loopback HTTP address: {0}")]
    InvalidEndpoint(String),
    #[error("daemon failed to start at {0}; the HTTP port may already be unavailable")]
    DaemonStart(String),
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| "clarity=info".into()),
        )
        .init();
    let cli = Cli::parse();
    if let Err(error) = run(cli).await {
        eprintln!("error: {error}");
        std::process::exit(1);
    }
}

#[allow(clippy::too_many_lines)]
async fn run(cli: Cli) -> Result<(), CliError> {
    if let CliCommand::Serve {
        listen,
        database,
        owner_token,
        repository,
    } = cli.command
    {
        return serve(&listen, database, owner_token, repository).await;
    }
    let client = CoordinationClient::new(&cli.endpoint);
    match cli.command {
        CliCommand::Serve { .. } => unreachable!(),
        CliCommand::Up { issue } => run_up(&client, &cli.endpoint, issue.as_deref()).await,
        CliCommand::Run {
            harness,
            issue,
            child_arguments,
        } => run_agent(&client, &cli.endpoint, harness, &issue, child_arguments).await,
        CliCommand::Workspace { command } => run_workspace_command(&client, command).await,
        CliCommand::Session {
            scope,
            name,
            harness,
            repository,
            revision,
            worktree,
            capabilities,
            lease_seconds,
            child,
        } => {
            let scope = selected_scope(scope)?;
            let manifest = EnvironmentManifest {
                harness,
                repository,
                revision,
                worktree,
                capabilities,
            };
            run_session(&client, scope, name, manifest, lease_seconds, child).await
        }
        CliCommand::Claim {
            participant,
            summary,
            resources,
            id_only,
        } => run_claim(&client, participant, summary, resources, id_only).await,
        CliCommand::Signal {
            participant,
            work,
            kind,
            summary,
            artifacts,
            requested_action,
            choices,
            recommendation,
            known_risk,
            request_id,
            request_key,
            supersedes_request,
        } => {
            run_signal(
                &client,
                participant,
                work,
                SignalArguments {
                    kind,
                    summary,
                    artifacts,
                    requested_action,
                    choices,
                    recommendation,
                    known_risk,
                    request_id,
                    request_key,
                    supersedes_request,
                },
            )
            .await
        }
        CliCommand::Observe { scope } => {
            let scope = selected_scope(scope)?;
            print_json(&client.snapshot(&scope).await?);
            Ok(())
        }
        CliCommand::Events { scope, after } => {
            let scope = selected_scope(scope)?;
            print_json(&client.events(&scope, after).await?);
            Ok(())
        }
        CliCommand::Intervene {
            scope,
            message,
            author,
            source_event,
            request,
            participant,
            work,
        } => {
            run_intervene(
                &client,
                InterventionArguments {
                    scope,
                    message,
                    author,
                    source_event,
                    request_id: request,
                    participant,
                    work,
                },
            )
            .await
        }
        CliCommand::Directions { participant } => run_directions(&client, participant).await,
        CliCommand::ConsumeDirection {
            participant,
            direction,
        } => run_consume_direction(&client, participant, direction).await,
        CliCommand::ResolveRequest {
            participant,
            request,
            summary,
        } => run_resolve_request(&client, participant, request, summary).await,
    }
}

async fn run_intervene(
    client: &CoordinationClient,
    arguments: InterventionArguments,
) -> Result<(), CliError> {
    let InterventionArguments {
        scope,
        message,
        author,
        source_event,
        request_id,
        participant,
        work,
    } = arguments;
    let scope = selected_scope(scope)?;
    let target = source_event.map(|source_event_id| DirectionTarget {
        request_id,
        source_event_id,
        participant_id: participant.expect("clap requires participant with source event"),
        work_id: work,
    });
    let result = client
        .command(&Command::Intervene {
            scope,
            message,
            author,
            target,
        })
        .await?;
    print_json(&result);
    command_succeeded(result)
}

async fn run_directions(
    client: &CoordinationClient,
    participant: ParticipantId,
) -> Result<(), CliError> {
    command_succeeded(
        client
            .command(&Command::DeliverDirections {
                participant_id: participant,
            })
            .await?,
    )?;
    print_json(&client.directions(participant).await?);
    Ok(())
}

async fn run_consume_direction(
    client: &CoordinationClient,
    participant: ParticipantId,
    direction: DirectionId,
) -> Result<(), CliError> {
    let result = client
        .command(&Command::ConsumeDirection {
            participant_id: participant,
            direction_id: direction,
        })
        .await?;
    print_json(&result);
    command_succeeded(result)
}

async fn run_claim(
    client: &CoordinationClient,
    participant: ParticipantId,
    summary: String,
    resources: Vec<String>,
    id_only: bool,
) -> Result<(), CliError> {
    let result = client
        .command(&Command::ClaimWork {
            participant_id: participant,
            summary,
            resources,
        })
        .await?;
    if id_only
        && let CommandResult::Accepted {
            work_id: Some(work_id),
            ..
        } = result
    {
        println!("{work_id}");
        return Ok(());
    }
    print_json(&result);
    command_succeeded(result)
}

async fn run_signal(
    client: &CoordinationClient,
    participant: ParticipantId,
    work: Option<WorkId>,
    arguments: SignalArguments,
) -> Result<(), CliError> {
    let signal = build_signal(arguments);
    let result = client
        .command(&Command::PublishSignal {
            participant_id: participant,
            work_id: work,
            signal,
        })
        .await?;
    print_json(&result);
    command_succeeded(result)
}

async fn run_resolve_request(
    client: &CoordinationClient,
    participant_id: ParticipantId,
    request_id: RequestId,
    summary: String,
) -> Result<(), CliError> {
    let result = client
        .command(&Command::ResolveAttention {
            participant_id,
            request_id,
            summary,
        })
        .await?;
    print_json(&result);
    command_succeeded(result)
}

#[derive(Deserialize)]
struct LinearResponse {
    data: Option<LinearData>,
    #[serde(default)]
    errors: Vec<LinearGraphqlError>,
}

#[derive(Deserialize)]
struct LinearData {
    issue: Option<ResolvedLinearIssue>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ResolvedLinearIssue {
    identifier: String,
    title: String,
    url: String,
    description: Option<String>,
    state: ResolvedLinearState,
    updated_at: chrono::DateTime<chrono::Utc>,
}

#[derive(Deserialize)]
struct ResolvedLinearState {
    name: String,
}

#[derive(Deserialize)]
struct LinearGraphqlError {
    message: String,
}

async fn run_up(
    client: &CoordinationClient,
    endpoint: &str,
    compatibility_issue: Option<&str>,
) -> Result<(), CliError> {
    let current_directory = std::env::current_dir()?;
    let repository = discover_repository(&current_directory)?;
    let daemon = ensure_daemon(client, endpoint, &repository.root).await?;
    let selected_workspace = if let Some(issue) = compatibility_issue {
        let workspace = ensure_workspace(client, issue, &repository).await?;
        write_selection(&repository.root, &workspace.scope)?;
        Some(workspace)
    } else {
        None
    };

    let address = endpoint_address(endpoint)?;
    let daemon_label = match daemon {
        DaemonDisposition::OwnedCompatible => "reused",
        DaemonDisposition::OwnedStale | DaemonDisposition::Absent => "started",
        DaemonDisposition::Foreign => unreachable!(),
    };
    println!("\nCLARITY\n");
    println!("Repository  {}", repository.identity);
    println!("Daemon      {daemon_label} at {address}");
    if let Some(workspace) = selected_workspace {
        println!(
            "Workspace   {} · {} (compatibility selection)",
            workspace.linear_issue.identifier, workspace.title
        );
        println!("\n`clarity up TG-*` is deprecated; use `clarity run amp TG-*`.");
    }
    println!("\nRun an agent task:\n  clarity run amp TG-187");
    Ok(())
}

async fn run_agent(
    client: &CoordinationClient,
    endpoint: &str,
    harness: AgentHarness,
    issue: &str,
    child_arguments: Vec<String>,
) -> Result<(), CliError> {
    let repository = discover_repository(&std::env::current_dir()?)?;
    ensure_daemon(client, endpoint, &repository.root).await?;
    let workspace = ensure_workspace(client, issue, &repository).await?;
    let executable = harness.executable();
    let mut child = vec![executable.to_owned()];
    child.extend(child_arguments);
    let manifest = EnvironmentManifest {
        harness: executable.to_owned(),
        repository: Some(repository.identity),
        revision: repository.revision,
        worktree: Some(repository.root.display().to_string()),
        capabilities: Vec::new(),
    };
    run_session(
        client,
        workspace.scope,
        executable.to_owned(),
        manifest,
        15,
        child,
    )
    .await
}

async fn ensure_workspace(
    client: &CoordinationClient,
    issue: &str,
    repository: &RepositoryContext,
) -> Result<Workspace, CliError> {
    let issue = normalize_issue_identifier(issue)?;
    let scope = format!("linear:{issue}");
    let existing = client.snapshot(&scope).await?;
    let cached = existing.workspace;
    let linear_issue = match resolve_linear_issue(&issue).await {
        Ok(issue) => issue,
        Err(CliError::MissingLinearApiKey) if cached.is_some() => {
            return Ok(cached.expect("checked cached workspace"));
        }
        Err(error) => return Err(error),
    };
    let mut workspace = workspace_from_issue(
        linear_issue.identifier,
        linear_issue.title,
        linear_issue.url,
        linear_issue.description,
        Some(LinearMetadata {
            status: linear_issue.state.name,
            updated_at: linear_issue.updated_at,
        }),
        repository,
    );
    let command = if let Some(cached) = cached {
        workspace.repository = cached.repository;
        Command::RefreshWorkspaceMetadata {
            workspace: workspace.clone(),
        }
    } else {
        Command::DefineWorkspace {
            workspace: workspace.clone(),
        }
    };
    command_succeeded(client.command(&command).await?)?;
    Ok(workspace)
}

fn normalize_issue_identifier(issue: &str) -> Result<String, CliError> {
    let issue = issue.trim().to_ascii_uppercase();
    let Some((team, number)) = issue.split_once('-') else {
        return Err(CliError::InvalidIssue(issue));
    };
    if team.is_empty()
        || !team
            .chars()
            .all(|character| character.is_ascii_alphabetic())
        || number.is_empty()
        || !number.chars().all(|character| character.is_ascii_digit())
    {
        return Err(CliError::InvalidIssue(issue));
    }
    Ok(issue)
}

async fn resolve_linear_issue(identifier: &str) -> Result<ResolvedLinearIssue, CliError> {
    let api_key = linear_api_key(std::env::var("LINEAR_API_KEY").ok())?;
    let api_url = std::env::var("LINEAR_API_URL")
        .unwrap_or_else(|_| "https://api.linear.app/graphql".to_owned());
    let response: LinearResponse = Client::new()
        .post(api_url)
        .header(reqwest::header::AUTHORIZATION, api_key)
        .json(&serde_json::json!({
            "query": "query Issue($id: String!) { issue(id: $id) { identifier title url description updatedAt state { name } } }",
            "variables": { "id": identifier }
        }))
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    if let Some(issue) = response.data.and_then(|data| data.issue) {
        return Ok(issue);
    }
    let reason = response
        .errors
        .into_iter()
        .map(|error| error.message)
        .collect::<Vec<_>>()
        .join("; ");
    Err(CliError::Linear(if reason.is_empty() {
        format!("issue {identifier} was not found")
    } else {
        reason
    }))
}

fn linear_api_key(value: Option<String>) -> Result<String, CliError> {
    value
        .filter(|value| !value.trim().is_empty())
        .ok_or(CliError::MissingLinearApiKey)
}

async fn ensure_daemon(
    client: &CoordinationClient,
    endpoint: &str,
    root: &Path,
) -> Result<DaemonDisposition, CliError> {
    let repository = discover_repository(root)?;
    let build_id = current_build_id()?;
    let probe = client.probe().await;
    let record = read_daemon_record(root)?;
    let disposition = clarity::bootstrap::daemon_disposition(
        endpoint,
        &repository.identity,
        &build_id,
        record.as_ref(),
        &probe,
    );
    match disposition {
        DaemonDisposition::OwnedCompatible => Ok(disposition),
        DaemonDisposition::Foreign => Err(CliError::ForeignDaemon {
            endpoint: endpoint.to_owned(),
            recovery: daemon_recovery_command(endpoint, root, record.as_ref())?,
        }),
        DaemonDisposition::OwnedStale | DaemonDisposition::Absent => {
            if disposition == DaemonDisposition::OwnedStale
                && !matches!(probe, DaemonProbe::Absent)
                && let Some(record) = &record
            {
                if let DaemonProbe::Identified(identity) = &probe {
                    eprintln!(
                        "daemon compatibility mismatch (protocol {}, build {}); replacing owned daemon with protocol {}, build {}",
                        identity.protocol_version,
                        identity.build_id,
                        DAEMON_PROTOCOL_VERSION,
                        build_id
                    );
                } else {
                    eprintln!("upgrading checkout-owned legacy daemon");
                }
                stop_daemon(record.pid).await?;
                for _ in 0..40 {
                    if matches!(client.probe().await, DaemonProbe::Absent) {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
                if !matches!(client.probe().await, DaemonProbe::Absent) {
                    return Err(CliError::DaemonStart(endpoint.to_owned()));
                }
            }
            remove_daemon_record(root)?;
            let address = endpoint_address(endpoint)?;
            let owner_token = Uuid::now_v7().to_string();
            let executable = std::env::current_exe()?;
            let mut child = ProcessCommand::new(executable)
                .arg("serve")
                .arg("--listen")
                .arg(address.to_string())
                .arg("--owner-token")
                .arg(&owner_token)
                .arg("--repository")
                .arg(&repository.identity)
                .current_dir(root)
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .spawn()?;
            for _ in 0..40 {
                let started = client.probe().await;
                if let DaemonProbe::Identified(identity) = started
                    && identity.owner_token == owner_token
                    && identity.repository == repository.identity
                    && identity.protocol_version == DAEMON_PROTOCOL_VERSION
                    && identity.build_id == build_id
                {
                    write_daemon_record(
                        root,
                        &DaemonRecord {
                            endpoint: endpoint.to_owned(),
                            pid: child.id().unwrap_or_default(),
                            owner_token: Some(owner_token),
                            repository: Some(repository.identity),
                            protocol_version: Some(DAEMON_PROTOCOL_VERSION),
                            build_id: Some(build_id),
                        },
                    )?;
                    return Ok(disposition);
                }
                if child.try_wait()?.is_some() {
                    return Err(CliError::DaemonStart(endpoint.to_owned()));
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            Err(CliError::DaemonStart(endpoint.to_owned()))
        }
    }
}

async fn stop_daemon(pid: u32) -> Result<(), CliError> {
    let status = ProcessCommand::new("kill")
        .arg(pid.to_string())
        .status()
        .await?;
    if status.success() {
        Ok(())
    } else {
        Err(CliError::DaemonStart(format!("process {pid}")))
    }
}

fn daemon_recovery_command(
    endpoint: &str,
    root: &Path,
    record: Option<&DaemonRecord>,
) -> Result<String, CliError> {
    if let Some(record) = record {
        return Ok(format!(
            "kill {} && rm {}",
            record.pid,
            root.join(DAEMON_RECORD_FILE).display()
        ));
    }
    let port = endpoint_address(endpoint)?.port();
    Ok(format!("kill $(lsof -tiTCP:{port} -sTCP:LISTEN)"))
}

fn endpoint_address(endpoint: &str) -> Result<SocketAddr, CliError> {
    let address = endpoint
        .strip_prefix("http://")
        .ok_or_else(|| CliError::InvalidEndpoint(endpoint.to_owned()))?
        .parse::<SocketAddr>()
        .map_err(|_| CliError::InvalidEndpoint(endpoint.to_owned()))?;
    if !address.ip().is_loopback() {
        return Err(CliError::InvalidEndpoint(endpoint.to_owned()));
    }
    Ok(address)
}

fn selected_scope(explicit: Option<String>) -> Result<String, CliError> {
    if let Some(scope) = explicit {
        return Ok(scope);
    }
    if let Ok(scope) = std::env::var(TASK_SCOPE_ENV)
        && !scope.trim().is_empty()
    {
        return Ok(scope);
    }
    let repository = discover_repository(&std::env::current_dir()?)?;
    Ok(read_selection(&repository.root)?)
}

async fn run_workspace_command(
    client: &CoordinationClient,
    command: WorkspaceCommand,
) -> Result<(), CliError> {
    let WorkspaceCommand::Create {
        issue,
        title,
        objective,
        url,
        repository,
        revision,
    } = command;
    let scope = format!("linear:{issue}");
    let repository = repository.map(|repository| RepositoryRef {
        repository,
        revision,
    });
    let result = client
        .command(&Command::DefineWorkspace {
            workspace: Workspace {
                scope,
                title,
                objective,
                linear_issue: LinearIssue {
                    identifier: issue,
                    url,
                    metadata: None,
                },
                repository,
            },
        })
        .await?;
    print_json(&result);
    command_succeeded(result)
}

async fn serve(
    listen: &str,
    database: PathBuf,
    owner_token: Option<String>,
    repository: Option<String>,
) -> Result<(), CliError> {
    let repository = match repository {
        Some(repository) => repository,
        None => discover_repository(&std::env::current_dir()?)?.identity,
    };
    let identity = DaemonIdentity {
        owner_token: owner_token.unwrap_or_else(|| Uuid::now_v7().to_string()),
        repository,
        protocol_version: DAEMON_PROTOCOL_VERSION,
        build_id: current_build_id()?,
    };
    let runtime = Runtime::open(database)?;
    let listener = tokio::net::TcpListener::bind(listen.parse::<std::net::SocketAddr>()?).await?;
    tracing::info!(address = %listener.local_addr()?, "coordination daemon listening");
    axum::serve(listener, clarity::http::router(runtime, identity))
        .await
        .map_err(CliError::Io)
}

async fn run_session(
    client: &CoordinationClient,
    scope: String,
    name: String,
    manifest: EnvironmentManifest,
    lease_seconds: u64,
    child: Vec<String>,
) -> Result<(), CliError> {
    let joined = client
        .command(&Command::Join {
            scope: scope.clone(),
            name,
            manifest,
            lease_seconds,
        })
        .await?;
    let participant_id = match joined {
        CommandResult::Accepted {
            participant_id: Some(id),
            ..
        } => id,
        CommandResult::Rejected { reason } => return Err(CliError::Rejected(reason)),
        _ => return Err(CliError::MissingParticipant),
    };
    tracing::info!(%participant_id, "participant joined");

    let mut process = ProcessCommand::new(&child[0]);
    process
        .args(&child[1..])
        .env("CLARITY_PARTICIPANT_ID", participant_id.to_string())
        .env("CLARITY_ENDPOINT", &client.endpoint)
        .env(TASK_SCOPE_ENV, scope)
        .kill_on_drop(true);
    let mut child = match process.spawn() {
        Ok(child) => child,
        Err(error) => {
            let _ = client.command(&Command::Leave { participant_id }).await;
            return Err(error.into());
        }
    };
    let heartbeat_client = client.clone();
    let mut heartbeat = tokio::spawn(async move {
        let period = Duration::from_secs((lease_seconds / 3).max(1));
        let mut interval = tokio::time::interval(period);
        interval.tick().await;
        loop {
            interval.tick().await;
            match heartbeat_client
                .command(&Command::RenewPresence {
                    participant_id,
                    lease_seconds,
                })
                .await?
            {
                CommandResult::Accepted { .. } => {}
                CommandResult::Rejected { reason } => return Err(CliError::Rejected(reason)),
                CommandResult::Conflict { .. } => return Err(CliError::HeartbeatStopped),
            }
        }
    });
    let outcome = tokio::select! {
        status = child.wait() => SessionOutcome::Child(status?),
        result = &mut heartbeat => SessionOutcome::Heartbeat(result?),
        () = termination_signal() => SessionOutcome::Interrupted,
    };
    let status = match outcome {
        SessionOutcome::Child(status) => {
            heartbeat.abort();
            status
        }
        SessionOutcome::Heartbeat(result) => {
            let _ = child.kill().await;
            let _ = child.wait().await;
            let _ = client.command(&Command::Leave { participant_id }).await;
            return Err(result.err().unwrap_or(CliError::HeartbeatStopped));
        }
        SessionOutcome::Interrupted => {
            heartbeat.abort();
            let _ = child.kill().await;
            let _ = child.wait().await;
            let _ = client.command(&Command::Leave { participant_id }).await;
            return Err(CliError::Interrupted);
        }
    };
    let _ = client.command(&Command::Leave { participant_id }).await;
    if status.success() {
        Ok(())
    } else {
        Err(CliError::Child(status))
    }
}

enum SessionOutcome {
    Child(std::process::ExitStatus),
    Heartbeat(Result<(), CliError>),
    Interrupted,
}

#[cfg(unix)]
async fn termination_signal() {
    let Ok(mut terminate) =
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
    else {
        std::future::pending::<()>().await;
        return;
    };
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {}
        _ = terminate.recv() => {}
    }
}

#[cfg(not(unix))]
async fn termination_signal() {
    let _ = tokio::signal::ctrl_c().await;
}

fn build_signal(arguments: SignalArguments) -> Signal {
    let SignalArguments {
        kind,
        summary,
        artifacts,
        requested_action,
        choices,
        recommendation,
        known_risk,
        request_id,
        request_key,
        supersedes_request,
    } = arguments;
    let identity = request_key.map(|request_key| RequestIdentity {
        request_id: request_id.unwrap_or_else(|| RequestId(Uuid::now_v7())),
        request_key,
        supersedes_request_id: supersedes_request,
    });
    let artifacts = artifacts.into_iter().map(|argument| argument.0).collect();
    match kind {
        SignalKind::Working => Signal::Working { summary },
        SignalKind::Finding => Signal::Finding { summary, artifacts },
        SignalKind::Offering => Signal::Offering { summary, artifacts },
        SignalKind::HelpNeeded => Signal::HelpNeeded {
            identity,
            summary,
            requested_action: requested_action.unwrap_or_default(),
            evidence: artifacts,
        },
        SignalKind::DecisionNeeded => Signal::DecisionNeeded {
            identity,
            summary,
            choices,
            recommendation,
            evidence: artifacts,
        },
        SignalKind::Blocked => Signal::Blocked {
            identity,
            summary,
            requested_action: requested_action.unwrap_or_default(),
            evidence: artifacts,
        },
        SignalKind::Checkpoint => Signal::Checkpoint { summary, artifacts },
        SignalKind::ReviewRequested => Signal::ReviewRequested {
            identity,
            summary,
            requested_action: requested_action.unwrap_or_default(),
            known_risk: known_risk.unwrap_or_default(),
            evidence: artifacts,
        },
        SignalKind::Done => Signal::Done {
            summary,
            evidence: artifacts,
        },
    }
}

fn command_succeeded(result: CommandResult) -> Result<(), CliError> {
    match result {
        CommandResult::Accepted { .. } => Ok(()),
        CommandResult::Conflict { .. } => Err(CliError::Rejected(
            "overlapping resources are already claimed".into(),
        )),
        CommandResult::Rejected { reason } => Err(CliError::Rejected(reason)),
    }
}

fn print_json(value: &impl Serialize) {
    println!(
        "{}",
        serde_json::to_string_pretty(value).expect("protocol values serialize")
    );
}

#[derive(Clone)]
struct CoordinationClient {
    endpoint: String,
    http: Client,
}

#[derive(Deserialize)]
struct HealthResponse {
    daemon: DaemonIdentity,
}

impl CoordinationClient {
    fn new(endpoint: &str) -> Self {
        Self {
            endpoint: endpoint.trim_end_matches('/').to_owned(),
            http: Client::new(),
        }
    }

    async fn command(&self, command: &Command) -> Result<CommandResult, CliError> {
        self.request(
            self.http
                .post(format!("{}/commands", self.endpoint))
                .json(command),
        )
        .await
    }

    async fn probe(&self) -> DaemonProbe {
        let response = self
            .http
            .get(format!("{}/health", self.endpoint))
            .timeout(Duration::from_millis(250))
            .send()
            .await;
        let Ok(response) = response else {
            return DaemonProbe::Absent;
        };
        if !response.status().is_success() {
            return DaemonProbe::ForeignResponse;
        }
        match response.json::<HealthResponse>().await {
            Ok(response) => DaemonProbe::Identified(response.daemon),
            Err(_) => DaemonProbe::ForeignResponse,
        }
    }

    async fn snapshot(&self, scope: &str) -> Result<ScopeSnapshot, CliError> {
        self.request(
            self.http
                .get(format!("{}/scopes/{scope}/snapshot", self.endpoint)),
        )
        .await
    }

    async fn events(&self, scope: &str, after: u64) -> Result<Vec<EventEnvelope>, CliError> {
        self.request(
            self.http
                .get(format!("{}/scopes/{scope}/events", self.endpoint))
                .query(&[("after", after)]),
        )
        .await
    }

    async fn directions(&self, participant: ParticipantId) -> Result<Vec<Direction>, CliError> {
        self.request(self.http.get(format!(
            "{}/participants/{participant}/directions",
            self.endpoint
        )))
        .await
    }

    async fn request<T: DeserializeOwned>(
        &self,
        request: reqwest::RequestBuilder,
    ) -> Result<T, CliError> {
        Ok(request.send().await?.error_for_status()?.json().await?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn missing_linear_credentials_are_actionable() {
        assert!(matches!(
            linear_api_key(None),
            Err(CliError::MissingLinearApiKey)
        ));
        assert!(matches!(
            linear_api_key(Some(String::new())),
            Err(CliError::MissingLinearApiKey)
        ));
    }

    #[test]
    fn issue_identifiers_are_normalized_and_validated() {
        assert_eq!(normalize_issue_identifier("tg-183").unwrap(), "TG-183");
        assert!(normalize_issue_identifier("TG").is_err());
        assert!(normalize_issue_identifier("TG-next").is_err());
    }

    #[test]
    fn up_activates_the_repository_without_selecting_a_task() {
        let cli = Cli::try_parse_from(["clarity", "up"]).unwrap();
        assert!(matches!(cli.command, CliCommand::Up { issue: None }));
    }

    #[test]
    fn run_selects_each_task_for_only_its_agent_process() {
        let first = Cli::try_parse_from(["clarity", "run", "amp", "TG-187"]).unwrap();
        let second = Cli::try_parse_from(["clarity", "run", "amp", "TG-188"]).unwrap();

        assert!(matches!(
            first.command,
            CliCommand::Run {
                harness: AgentHarness::Amp,
                issue,
                ..
            } if issue == "TG-187"
        ));
        assert!(matches!(
            second.command,
            CliCommand::Run {
                harness: AgentHarness::Amp,
                issue,
                ..
            } if issue == "TG-188"
        ));
    }
}
