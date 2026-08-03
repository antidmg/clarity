use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

macro_rules! id_type {
    ($name:ident) => {
        #[derive(
            Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize,
        )]
        #[serde(transparent)]
        pub struct $name(pub Uuid);

        impl std::fmt::Display for $name {
            fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                self.0.fmt(formatter)
            }
        }

        impl std::str::FromStr for $name {
            type Err = uuid::Error;

            fn from_str(value: &str) -> Result<Self, Self::Err> {
                Uuid::parse_str(value).map(Self)
            }
        }
    };
}

id_type!(ParticipantId);
id_type!(WorkId);
id_type!(DirectionId);
id_type!(RequestId);

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct EnvironmentManifest {
    pub harness: String,
    pub repository: Option<String>,
    pub revision: Option<String>,
    pub worktree: Option<String>,
    #[serde(default)]
    pub capabilities: Vec<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ArtifactKind {
    File,
    Patch,
    Revision,
    TestReceipt,
    Url,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ArtifactRef {
    pub kind: ArtifactKind,
    pub uri: String,
    pub digest: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct LinearIssue {
    pub identifier: String,
    pub url: Option<String>,
    #[serde(default)]
    pub metadata: Option<LinearMetadata>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct LinearMetadata {
    pub status: String,
    pub updated_at: DateTime<Utc>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct RepositoryRef {
    pub repository: String,
    pub revision: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Workspace {
    pub scope: String,
    pub title: String,
    pub objective: String,
    pub linear_issue: LinearIssue,
    pub repository: Option<RepositoryRef>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Signal {
    Working {
        summary: String,
    },
    Finding {
        summary: String,
        #[serde(default)]
        artifacts: Vec<ArtifactRef>,
    },
    Offering {
        summary: String,
        #[serde(default)]
        artifacts: Vec<ArtifactRef>,
    },
    HelpNeeded {
        #[serde(default)]
        identity: Option<RequestIdentity>,
        summary: String,
        #[serde(default)]
        requested_action: String,
        #[serde(default)]
        evidence: Vec<ArtifactRef>,
    },
    DecisionNeeded {
        #[serde(default)]
        identity: Option<RequestIdentity>,
        summary: String,
        #[serde(default)]
        choices: Vec<String>,
        #[serde(default)]
        recommendation: Option<String>,
        #[serde(default)]
        evidence: Vec<ArtifactRef>,
    },
    Blocked {
        #[serde(default)]
        identity: Option<RequestIdentity>,
        summary: String,
        #[serde(default)]
        requested_action: String,
        #[serde(default)]
        evidence: Vec<ArtifactRef>,
    },
    Checkpoint {
        summary: String,
        #[serde(default)]
        artifacts: Vec<ArtifactRef>,
    },
    ReviewRequested {
        #[serde(default)]
        identity: Option<RequestIdentity>,
        summary: String,
        #[serde(default)]
        requested_action: String,
        #[serde(default)]
        known_risk: String,
        #[serde(default)]
        evidence: Vec<ArtifactRef>,
    },
    Done {
        summary: String,
        #[serde(default)]
        evidence: Vec<ArtifactRef>,
    },
}

impl Signal {
    pub fn request_identity(&self) -> Option<&RequestIdentity> {
        match self {
            Self::HelpNeeded { identity, .. }
            | Self::DecisionNeeded { identity, .. }
            | Self::Blocked { identity, .. }
            | Self::ReviewRequested { identity, .. } => identity.as_ref(),
            _ => None,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Command {
    DefineWorkspace {
        workspace: Workspace,
    },
    RefreshWorkspaceMetadata {
        workspace: Workspace,
    },
    Join {
        scope: String,
        name: String,
        manifest: EnvironmentManifest,
        lease_seconds: u64,
    },
    RenewPresence {
        participant_id: ParticipantId,
        lease_seconds: u64,
    },
    ClaimWork {
        participant_id: ParticipantId,
        summary: String,
        resources: Vec<String>,
    },
    PublishSignal {
        participant_id: ParticipantId,
        work_id: Option<WorkId>,
        signal: Signal,
    },
    ResolveAttention {
        participant_id: ParticipantId,
        request_id: RequestId,
        summary: String,
    },
    Leave {
        participant_id: ParticipantId,
    },
    Intervene {
        scope: String,
        message: String,
        author: String,
        target: Option<DirectionTarget>,
    },
    DeliverDirections {
        participant_id: ParticipantId,
    },
    ConsumeDirection {
        participant_id: ParticipantId,
        direction_id: DirectionId,
    },
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Conflict {
    pub work_id: WorkId,
    pub owner: ParticipantId,
    pub resources: Vec<String>,
    pub summary: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum CommandResult {
    Accepted {
        #[serde(skip_serializing_if = "Option::is_none")]
        participant_id: Option<ParticipantId>,
        #[serde(skip_serializing_if = "Option::is_none")]
        work_id: Option<WorkId>,
        #[serde(skip_serializing_if = "Option::is_none")]
        request_id: Option<RequestId>,
        event_sequences: Vec<u64>,
    },
    Conflict {
        conflicts: Vec<Conflict>,
        event_sequences: Vec<u64>,
    },
    Rejected {
        reason: String,
    },
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Participant {
    pub id: ParticipantId,
    pub scope: String,
    pub name: String,
    pub manifest: EnvironmentManifest,
    pub joined_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct WorkClaim {
    pub id: WorkId,
    pub scope: String,
    pub owner: ParticipantId,
    pub summary: String,
    pub resources: Vec<String>,
    pub claimed_at: DateTime<Utc>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct PublishedSignal {
    pub participant_id: ParticipantId,
    pub work_id: Option<WorkId>,
    pub signal: Signal,
    pub published_at: DateTime<Utc>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum AttentionCategory {
    Decision,
    Blocked,
    Help,
    ReadyForReview,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RequestLifecycle {
    Open,
    Resolved,
    Superseded,
    Expired,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct RequestIdentity {
    pub request_id: RequestId,
    pub request_key: String,
    #[serde(default)]
    pub supersedes_request_id: Option<RequestId>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum AttentionRequest {
    Decision {
        question: String,
        choices: Vec<String>,
        recommendation: Option<String>,
    },
    Intervention {
        blocker: String,
        requested_action: String,
    },
    Review {
        summary: String,
        requested_action: String,
        known_risk: String,
    },
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct AttentionTarget {
    pub scope: String,
    pub event_id: Uuid,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct AttentionItem {
    pub request_id: RequestId,
    pub request_key: String,
    pub source_scope: String,
    pub source_event_id: Uuid,
    pub work_id: Option<WorkId>,
    pub participant_id: ParticipantId,
    pub category: AttentionCategory,
    pub created_at: DateTime<Utc>,
    pub lifecycle: RequestLifecycle,
    pub request: AttentionRequest,
    #[serde(default)]
    pub evidence: Vec<ArtifactRef>,
    pub target: AttentionTarget,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct DirectionTarget {
    #[serde(default)]
    pub request_id: Option<RequestId>,
    pub source_event_id: Uuid,
    pub participant_id: ParticipantId,
    pub work_id: Option<WorkId>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Direction {
    pub id: DirectionId,
    pub scope: String,
    pub message: String,
    pub author: String,
    pub created_at: DateTime<Utc>,
    /// `None` is an explicit workspace broadcast.
    pub target: Option<DirectionTarget>,
    #[serde(default)]
    pub deliveries: Vec<DirectionDelivery>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct DirectionDelivery {
    pub participant_id: ParticipantId,
    pub delivered_at: DateTime<Utc>,
    pub consumed_at: Option<DateTime<Utc>>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum EventKind {
    WorkspaceDefined {
        workspace: Workspace,
    },
    WorkspaceMetadataUpdated {
        workspace: Workspace,
    },
    ParticipantJoined {
        participant: Participant,
    },
    PresenceRenewed {
        participant_id: ParticipantId,
        expires_at: DateTime<Utc>,
    },
    ParticipantLeft {
        participant_id: ParticipantId,
        reason: LeaveReason,
    },
    WorkClaimed {
        work: WorkClaim,
    },
    OverlapDetected {
        attempted_by: ParticipantId,
        attempted_resources: Vec<String>,
        conflicts: Vec<Conflict>,
    },
    WorkReleased {
        work_id: WorkId,
        previous_owner: ParticipantId,
        reason: ReleaseReason,
    },
    SignalPublished {
        published: PublishedSignal,
    },
    AttentionResolved {
        request_id: RequestId,
        participant_id: ParticipantId,
        summary: String,
        resolved_at: DateTime<Utc>,
    },
    HumanIntervened {
        message: String,
    },
    DirectionIssued {
        direction: Direction,
    },
    DirectionDelivered {
        direction_id: DirectionId,
        participant_id: ParticipantId,
        delivered_at: DateTime<Utc>,
    },
    DirectionConsumed {
        direction_id: DirectionId,
        participant_id: ParticipantId,
        consumed_at: DateTime<Utc>,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LeaveReason {
    Graceful,
    LeaseExpired,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReleaseReason {
    Completed,
    ParticipantLeft,
    LeaseExpired,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct EventEnvelope {
    pub sequence: u64,
    pub event_id: Uuid,
    pub scope: String,
    pub emitted_at: DateTime<Utc>,
    pub event: EventKind,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct CoordinationMetrics {
    pub prevented_overlaps: u64,
    pub human_interventions: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ScopeSnapshot {
    pub scope: String,
    #[serde(default)]
    pub workspace: Option<Workspace>,
    pub participants: Vec<Participant>,
    pub active_work: Vec<WorkClaim>,
    pub signals: Vec<PublishedSignal>,
    #[serde(default)]
    pub attention: Vec<AttentionItem>,
    pub metrics: CoordinationMetrics,
    pub last_sequence: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkspaceLifecycle {
    Active,
    Completed,
    Idle,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct WorkspaceSummary {
    pub scope: String,
    pub title: Option<String>,
    pub lifecycle: WorkspaceLifecycle,
    pub active_participants: usize,
    pub active_work: usize,
    pub last_sequence: u64,
}
