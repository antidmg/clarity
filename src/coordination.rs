use std::collections::{BTreeMap, BTreeSet};

use chrono::{DateTime, Duration, Utc};
use uuid::Uuid;

use crate::protocol::{
    ArtifactKind, ArtifactRef, AttentionCategory, AttentionItem, AttentionRequest, AttentionTarget,
    Command, Conflict, CoordinationMetrics, Direction, DirectionDelivery, DirectionId,
    DirectionTarget, EventEnvelope, EventKind, LeaveReason, Participant, ParticipantId,
    PublishedSignal, ReleaseReason, RequestId, RequestIdentity, RequestLifecycle, ScopeSnapshot,
    Signal, WorkClaim, WorkId, Workspace, WorkspaceLifecycle, WorkspaceSummary,
};

#[derive(Clone, Debug)]
struct StoredSignal {
    scope: String,
    event_id: Uuid,
    sequence: u64,
    published: PublishedSignal,
}

#[derive(Clone, Debug)]
pub struct DecisionContext {
    pub now: DateTime<Utc>,
    pub participant_id: ParticipantId,
    pub work_id: WorkId,
    pub direction_id: DirectionId,
}

impl DecisionContext {
    pub fn at(now: DateTime<Utc>) -> Self {
        Self {
            now,
            participant_id: ParticipantId(Uuid::now_v7()),
            work_id: WorkId(Uuid::now_v7()),
            direction_id: DirectionId(Uuid::now_v7()),
        }
    }
}

#[derive(Clone, Debug)]
pub struct PendingEvent {
    pub scope: String,
    pub event: EventKind,
}

#[derive(Clone, Debug)]
pub enum Decision {
    Accepted {
        events: Vec<PendingEvent>,
        participant_id: Option<ParticipantId>,
        work_id: Option<WorkId>,
    },
    Conflict {
        events: Vec<PendingEvent>,
        conflicts: Vec<Conflict>,
    },
    Rejected(String),
}

#[derive(Clone, Debug, Default)]
pub struct CoordinationState {
    scopes: BTreeSet<String>,
    scope_sequences: BTreeMap<String, u64>,
    scope_activity_sequences: BTreeMap<String, u64>,
    workspaces: BTreeMap<String, Workspace>,
    participants: BTreeMap<ParticipantId, Participant>,
    work: BTreeMap<WorkId, WorkClaim>,
    signals: Vec<StoredSignal>,
    resolved_requests: BTreeSet<RequestId>,
    directions: BTreeMap<DirectionId, Direction>,
    metrics: BTreeMap<String, CoordinationMetrics>,
    last_sequence: u64,
}

impl CoordinationState {
    pub fn decide(&self, command: Command, context: &DecisionContext) -> Decision {
        match command {
            Command::DefineWorkspace { workspace } => self.define_workspace(workspace),
            Command::RefreshWorkspaceMetadata { workspace } => {
                self.refresh_workspace_metadata(workspace)
            }
            Command::Join {
                scope,
                name,
                manifest,
                lease_seconds,
            } => {
                if let Err(reason) = validate_text("scope", &scope)
                    .and_then(|()| validate_text("name", &name))
                    .and_then(|()| validate_lease(lease_seconds))
                {
                    return Decision::Rejected(reason);
                }
                let participant = Participant {
                    id: context.participant_id,
                    scope: scope.clone(),
                    name,
                    manifest,
                    joined_at: context.now,
                    expires_at: lease_expiry(context.now, lease_seconds),
                };
                Decision::Accepted {
                    events: vec![PendingEvent {
                        scope,
                        event: EventKind::ParticipantJoined { participant },
                    }],
                    participant_id: Some(context.participant_id),
                    work_id: None,
                }
            }
            Command::RenewPresence {
                participant_id,
                lease_seconds,
            } => {
                let Some(participant) = self.live_participant(participant_id, context.now) else {
                    return Decision::Rejected("participant is absent or its lease expired".into());
                };
                if let Err(reason) = validate_lease(lease_seconds) {
                    return Decision::Rejected(reason);
                }
                Decision::Accepted {
                    events: vec![PendingEvent {
                        scope: participant.scope.clone(),
                        event: EventKind::PresenceRenewed {
                            participant_id,
                            expires_at: lease_expiry(context.now, lease_seconds),
                        },
                    }],
                    participant_id: Some(participant_id),
                    work_id: None,
                }
            }
            Command::ClaimWork {
                participant_id,
                summary,
                resources,
            } => self.claim(participant_id, summary, resources, context),
            Command::PublishSignal {
                participant_id,
                work_id,
                signal,
            } => self.publish(participant_id, work_id, signal, context),
            Command::ResolveAttention {
                participant_id,
                request_id,
                summary,
            } => self.resolve_attention(participant_id, request_id, summary, context),
            Command::Leave { participant_id } => {
                let Some(participant) = self.participants.get(&participant_id) else {
                    return Decision::Rejected("participant does not exist".into());
                };
                Decision::Accepted {
                    events: self.departure_events(participant, LeaveReason::Graceful),
                    participant_id: Some(participant_id),
                    work_id: None,
                }
            }
            Command::Intervene {
                scope,
                message,
                author,
                target,
            } => self.intervene(scope, message, author, target, context),
            Command::DeliverDirections { participant_id } => {
                self.deliver_directions(participant_id, context)
            }
            Command::ConsumeDirection {
                participant_id,
                direction_id,
            } => self.consume_direction(participant_id, direction_id, context),
        }
    }

    pub fn expire(&self, now: DateTime<Utc>) -> Vec<PendingEvent> {
        self.participants
            .values()
            .filter(|participant| participant.expires_at <= now)
            .flat_map(|participant| self.departure_events(participant, LeaveReason::LeaseExpired))
            .collect()
    }

    pub fn apply(&mut self, envelope: &EventEnvelope) {
        self.last_sequence = self.last_sequence.max(envelope.sequence);
        self.scopes.insert(envelope.scope.clone());
        self.scope_sequences
            .insert(envelope.scope.clone(), envelope.sequence);
        if !matches!(&envelope.event, EventKind::PresenceRenewed { .. }) {
            self.scope_activity_sequences
                .insert(envelope.scope.clone(), envelope.sequence);
        }
        match &envelope.event {
            EventKind::WorkspaceDefined { workspace }
            | EventKind::WorkspaceMetadataUpdated { workspace } => {
                self.workspaces
                    .insert(workspace.scope.clone(), workspace.clone());
            }
            EventKind::ParticipantJoined { participant } => {
                self.participants
                    .insert(participant.id, participant.clone());
            }
            EventKind::PresenceRenewed {
                participant_id,
                expires_at,
            } => {
                if let Some(participant) = self.participants.get_mut(participant_id) {
                    participant.expires_at = *expires_at;
                }
            }
            EventKind::ParticipantLeft { participant_id, .. } => {
                self.participants.remove(participant_id);
            }
            EventKind::WorkClaimed { work } => {
                self.work.insert(work.id, work.clone());
            }
            EventKind::OverlapDetected { .. } => {
                self.metrics
                    .entry(envelope.scope.clone())
                    .or_default()
                    .prevented_overlaps += 1;
            }
            EventKind::WorkReleased { work_id, .. } => {
                self.work.remove(work_id);
            }
            EventKind::SignalPublished { published } => {
                self.signals.push(StoredSignal {
                    scope: envelope.scope.clone(),
                    event_id: envelope.event_id,
                    sequence: envelope.sequence,
                    published: published.clone(),
                });
            }
            EventKind::AttentionResolved { request_id, .. } => {
                self.resolved_requests.insert(*request_id);
            }
            EventKind::HumanIntervened { .. } => {
                self.metrics
                    .entry(envelope.scope.clone())
                    .or_default()
                    .human_interventions += 1;
            }
            EventKind::DirectionIssued { direction } => {
                self.directions.insert(direction.id, direction.clone());
                self.metrics
                    .entry(envelope.scope.clone())
                    .or_default()
                    .human_interventions += 1;
            }
            EventKind::DirectionDelivered {
                direction_id,
                participant_id,
                delivered_at,
            } => {
                if let Some(direction) = self.directions.get_mut(direction_id) {
                    direction.deliveries.push(DirectionDelivery {
                        participant_id: *participant_id,
                        delivered_at: *delivered_at,
                        consumed_at: None,
                    });
                }
            }
            EventKind::DirectionConsumed {
                direction_id,
                participant_id,
                consumed_at,
            } => {
                if let Some(delivery) =
                    self.directions.get_mut(direction_id).and_then(|direction| {
                        direction
                            .deliveries
                            .iter_mut()
                            .find(|delivery| delivery.participant_id == *participant_id)
                    })
                {
                    delivery.consumed_at = Some(*consumed_at);
                }
            }
        }
    }

    pub fn snapshot(&self, scope: &str) -> ScopeSnapshot {
        ScopeSnapshot {
            scope: scope.to_owned(),
            workspace: self.workspaces.get(scope).cloned(),
            participants: self
                .participants
                .values()
                .filter(|participant| participant.scope == scope)
                .cloned()
                .collect(),
            active_work: self
                .work
                .values()
                .filter(|work| work.scope == scope)
                .cloned()
                .collect(),
            signals: self
                .signals
                .iter()
                .filter(|signal| signal.scope == scope)
                .map(|signal| signal.published.clone())
                .collect(),
            attention: self.attention_for_scope(scope),
            metrics: self.metrics.get(scope).cloned().unwrap_or_default(),
            last_sequence: self.scope_sequences.get(scope).copied().unwrap_or_default(),
        }
    }

    /// Returns unresolved human-attention items across every workspace.
    ///
    /// Ordering is deterministic: decision, blocked, help, then ready-for-review;
    /// within a category older items come first, followed by scope and event ID.
    pub fn attention(&self) -> Vec<AttentionItem> {
        let mut attention = self
            .scopes
            .iter()
            .flat_map(|scope| self.attention_for_scope(scope))
            .collect::<Vec<_>>();
        attention.sort_by(|left, right| {
            attention_rank(left.category)
                .cmp(&attention_rank(right.category))
                .then_with(|| left.created_at.cmp(&right.created_at))
                .then_with(|| left.source_scope.cmp(&right.source_scope))
                .then_with(|| left.source_event_id.cmp(&right.source_event_id))
        });
        attention
    }

    pub fn workspaces(&self) -> Vec<WorkspaceSummary> {
        let mut workspaces = self
            .scopes
            .iter()
            .map(|scope| {
                let snapshot = self.snapshot(scope);
                WorkspaceSummary {
                    scope: scope.clone(),
                    title: snapshot
                        .workspace
                        .as_ref()
                        .map(|workspace| workspace.title.clone()),
                    lifecycle: workspace_lifecycle(&snapshot),
                    active_participants: snapshot.participants.len(),
                    active_work: snapshot.active_work.len(),
                    last_sequence: snapshot.last_sequence,
                }
            })
            .collect::<Vec<_>>();
        workspaces.sort_by(|left, right| {
            self.scope_activity_sequences
                .get(&right.scope)
                .cmp(&self.scope_activity_sequences.get(&left.scope))
                .then_with(|| left.scope.cmp(&right.scope))
        });
        workspaces
    }

    fn intervene(
        &self,
        scope: String,
        message: String,
        author: String,
        target: Option<DirectionTarget>,
        context: &DecisionContext,
    ) -> Decision {
        if let Err(reason) = validate_text("scope", &scope)
            .and_then(|()| validate_text("message", &message))
            .and_then(|()| validate_text("author", &author))
        {
            return Decision::Rejected(reason);
        }
        if let Some(target) = &target
            && !self.signals.iter().any(|signal| {
                let identity = stored_request_identity(signal);
                signal.scope == scope
                    && signal.event_id == target.source_event_id
                    && signal.published.participant_id == target.participant_id
                    && signal.published.work_id == target.work_id
                    && identity
                        .as_ref()
                        .is_some_and(|identity| target.request_id == Some(identity.request_id))
                    && !attention_resolved(
                        signal,
                        &self.signals,
                        &self.directions,
                        &self.resolved_requests,
                    )
            })
        {
            return Decision::Rejected(
                "direction target does not identify a source request in this workspace".into(),
            );
        }
        Decision::Accepted {
            events: vec![PendingEvent {
                scope: scope.clone(),
                event: EventKind::DirectionIssued {
                    direction: Direction {
                        id: context.direction_id,
                        scope,
                        message,
                        author,
                        created_at: context.now,
                        target,
                        deliveries: Vec::new(),
                    },
                },
            }],
            participant_id: None,
            work_id: None,
        }
    }

    pub fn directions_for(&self, participant_id: ParticipantId) -> Vec<Direction> {
        if !self.participants.contains_key(&participant_id) {
            return Vec::new();
        }
        self.directions
            .values()
            .filter(|direction| {
                direction.deliveries.iter().any(|delivery| {
                    delivery.participant_id == participant_id && delivery.consumed_at.is_none()
                })
            })
            .cloned()
            .collect()
    }

    fn deliver_directions(
        &self,
        participant_id: ParticipantId,
        context: &DecisionContext,
    ) -> Decision {
        let Some(participant) = self.live_participant(participant_id, context.now) else {
            return Decision::Rejected("participant is absent or its lease expired".into());
        };
        let events = self
            .directions
            .values()
            .filter(|direction| direction.scope == participant.scope)
            .filter(|direction| {
                direction_is_relevant(direction, participant_id, &self.participants, &self.work)
            })
            .filter(|direction| {
                !direction
                    .deliveries
                    .iter()
                    .any(|delivery| delivery.participant_id == participant_id)
            })
            .map(|direction| PendingEvent {
                scope: participant.scope.clone(),
                event: EventKind::DirectionDelivered {
                    direction_id: direction.id,
                    participant_id,
                    delivered_at: context.now,
                },
            })
            .collect();
        Decision::Accepted {
            events,
            participant_id: Some(participant_id),
            work_id: None,
        }
    }

    fn consume_direction(
        &self,
        participant_id: ParticipantId,
        direction_id: DirectionId,
        context: &DecisionContext,
    ) -> Decision {
        if self.live_participant(participant_id, context.now).is_none() {
            return Decision::Rejected("participant is absent or its lease expired".into());
        }
        let Some(direction) = self.directions.get(&direction_id) else {
            return Decision::Rejected("direction does not exist".into());
        };
        let Some(delivery) = direction
            .deliveries
            .iter()
            .find(|delivery| delivery.participant_id == participant_id)
        else {
            return Decision::Rejected("direction was not delivered to this participant".into());
        };
        if delivery.consumed_at.is_some() {
            return Decision::Accepted {
                events: Vec::new(),
                participant_id: Some(participant_id),
                work_id: None,
            };
        }
        Decision::Accepted {
            events: vec![PendingEvent {
                scope: direction.scope.clone(),
                event: EventKind::DirectionConsumed {
                    direction_id,
                    participant_id,
                    consumed_at: context.now,
                },
            }],
            participant_id: Some(participant_id),
            work_id: None,
        }
    }

    fn attention_for_scope(&self, scope: &str) -> Vec<AttentionItem> {
        self.signals
            .iter()
            .filter(|stored| stored.scope == scope)
            .filter_map(|stored| {
                let (category, request, own_evidence) = attention_signal(&stored.published.signal)?;
                if stored.published.signal.request_identity().is_none()
                    || validate_signal(&stored.published.signal).is_err()
                {
                    return None;
                }
                let identity = stored_request_identity(stored)?;
                if attention_resolved(
                    stored,
                    &self.signals,
                    &self.directions,
                    &self.resolved_requests,
                ) {
                    return None;
                }
                let mut evidence = self
                    .signals
                    .iter()
                    .filter(|candidate| {
                        candidate.scope == scope
                            && candidate.sequence < stored.sequence
                            && candidate.published.work_id == stored.published.work_id
                            && attention_signal(&candidate.published.signal).is_none()
                    })
                    .flat_map(|candidate| signal_artifacts(&candidate.published.signal))
                    .cloned()
                    .collect::<Vec<_>>();
                evidence.extend(own_evidence.iter().cloned());
                evidence.sort_by(|left, right| {
                    artifact_rank(left)
                        .cmp(&artifact_rank(right))
                        .then_with(|| left.uri.cmp(&right.uri))
                });
                evidence.dedup();
                Some(AttentionItem {
                    request_id: identity.request_id,
                    request_key: identity.request_key,
                    source_scope: scope.to_owned(),
                    source_event_id: stored.event_id,
                    work_id: stored.published.work_id,
                    participant_id: stored.published.participant_id,
                    category,
                    created_at: stored.published.published_at,
                    lifecycle: RequestLifecycle::Open,
                    request,
                    evidence,
                    target: AttentionTarget {
                        scope: scope.to_owned(),
                        event_id: stored.event_id,
                    },
                })
            })
            .collect()
    }

    fn define_workspace(&self, workspace: Workspace) -> Decision {
        if let Err(reason) = validate_workspace(&workspace) {
            return Decision::Rejected(reason);
        }
        if let Some(existing) = self.workspaces.get(&workspace.scope) {
            return if existing == &workspace {
                Decision::Accepted {
                    events: vec![],
                    participant_id: None,
                    work_id: None,
                }
            } else {
                Decision::Rejected("workspace is already defined with different metadata".into())
            };
        }
        Decision::Accepted {
            events: vec![PendingEvent {
                scope: workspace.scope.clone(),
                event: EventKind::WorkspaceDefined { workspace },
            }],
            participant_id: None,
            work_id: None,
        }
    }

    fn refresh_workspace_metadata(&self, workspace: Workspace) -> Decision {
        if let Err(reason) = validate_workspace(&workspace) {
            return Decision::Rejected(reason);
        }
        let Some(existing) = self.workspaces.get(&workspace.scope) else {
            return Decision::Rejected("workspace must be defined before metadata refresh".into());
        };
        if existing.linear_issue.identifier != workspace.linear_issue.identifier
            || existing.repository != workspace.repository
        {
            return Decision::Rejected("workspace identity cannot change during refresh".into());
        }
        let Some(metadata) = &workspace.linear_issue.metadata else {
            return Decision::Rejected(
                "refreshed Linear metadata must include source state".into(),
            );
        };
        if let Err(reason) = validate_text("Linear status", &metadata.status) {
            return Decision::Rejected(reason);
        }
        if let Some(existing_metadata) = &existing.linear_issue.metadata {
            if metadata.updated_at < existing_metadata.updated_at {
                return Decision::Rejected(
                    "Linear metadata refresh is older than cached state".into(),
                );
            }
            if metadata.updated_at == existing_metadata.updated_at && existing != &workspace {
                return Decision::Rejected(
                    "Linear metadata changed without a newer source timestamp".into(),
                );
            }
        }
        if existing == &workspace {
            return Decision::Accepted {
                events: vec![],
                participant_id: None,
                work_id: None,
            };
        }
        Decision::Accepted {
            events: vec![PendingEvent {
                scope: workspace.scope.clone(),
                event: EventKind::WorkspaceMetadataUpdated { workspace },
            }],
            participant_id: None,
            work_id: None,
        }
    }

    fn claim(
        &self,
        participant_id: ParticipantId,
        summary: String,
        resources: Vec<String>,
        context: &DecisionContext,
    ) -> Decision {
        let Some(participant) = self.live_participant(participant_id, context.now) else {
            return Decision::Rejected("participant is absent or its lease expired".into());
        };
        if let Err(reason) = validate_text("summary", &summary) {
            return Decision::Rejected(reason);
        }
        let resources = match normalize_resources(resources) {
            Ok(resources) => resources,
            Err(reason) => return Decision::Rejected(reason),
        };
        if resources.is_empty() {
            return Decision::Rejected("a work claim needs at least one resource".into());
        }
        let conflicts: Vec<_> = self
            .work
            .values()
            .filter(|work| {
                work.scope == participant.scope && resources_overlap(&resources, &work.resources)
            })
            .map(|work| Conflict {
                work_id: work.id,
                owner: work.owner,
                resources: work.resources.clone(),
                summary: work.summary.clone(),
            })
            .collect();
        if !conflicts.is_empty() {
            return Decision::Conflict {
                events: vec![PendingEvent {
                    scope: participant.scope.clone(),
                    event: EventKind::OverlapDetected {
                        attempted_by: participant_id,
                        attempted_resources: resources,
                        conflicts: conflicts.clone(),
                    },
                }],
                conflicts,
            };
        }
        let work = WorkClaim {
            id: context.work_id,
            scope: participant.scope.clone(),
            owner: participant_id,
            summary,
            resources,
            claimed_at: context.now,
        };
        Decision::Accepted {
            events: vec![PendingEvent {
                scope: participant.scope.clone(),
                event: EventKind::WorkClaimed { work },
            }],
            participant_id: Some(participant_id),
            work_id: Some(context.work_id),
        }
    }

    fn publish(
        &self,
        participant_id: ParticipantId,
        work_id: Option<WorkId>,
        signal: Signal,
        context: &DecisionContext,
    ) -> Decision {
        let Some(participant) = self.live_participant(participant_id, context.now) else {
            return Decision::Rejected("participant is absent or its lease expired".into());
        };
        if let Err(reason) = validate_signal(&signal) {
            return Decision::Rejected(reason);
        }
        if let Some(identity) = signal.request_identity() {
            if self.signals.iter().any(|stored| {
                stored_request_identity(stored)
                    .is_some_and(|existing| existing.request_id == identity.request_id)
            }) {
                return Decision::Rejected("request ID already exists".into());
            }
            if identity.supersedes_request_id == Some(identity.request_id) {
                return Decision::Rejected("request cannot supersede itself".into());
            }
            if let Some(superseded) = identity.supersedes_request_id
                && !self.signals.iter().any(|stored| {
                    stored.scope == participant.scope
                        && stored.published.participant_id == participant_id
                        && stored.published.work_id == work_id
                        && stored_request_identity(stored)
                            .is_some_and(|existing| existing.request_id == superseded)
                })
            {
                return Decision::Rejected("superseded request does not exist".into());
            }
        }
        if let Some(id) = work_id {
            let Some(work) = self.work.get(&id) else {
                return Decision::Rejected("work claim is not active".into());
            };
            if work.scope != participant.scope {
                return Decision::Rejected("work claim belongs to a different workspace".into());
            }
            if signal_requires_ownership(&signal) && work.owner != participant_id {
                return Decision::Rejected("signal requires ownership of the work claim".into());
            }
        } else if signal_requires_work(&signal) {
            return Decision::Rejected("signal requires an active work claim".into());
        }
        let completes_work = matches!(signal, Signal::Done { .. });
        let mut events = vec![PendingEvent {
            scope: participant.scope.clone(),
            event: EventKind::SignalPublished {
                published: PublishedSignal {
                    participant_id,
                    work_id,
                    signal,
                    published_at: context.now,
                },
            },
        }];
        if completes_work {
            let id = work_id.expect("done signals require work");
            let work = &self.work[&id];
            events.push(PendingEvent {
                scope: participant.scope.clone(),
                event: EventKind::WorkReleased {
                    work_id: id,
                    previous_owner: work.owner,
                    reason: ReleaseReason::Completed,
                },
            });
        }
        Decision::Accepted {
            events,
            participant_id: Some(participant_id),
            work_id,
        }
    }

    fn resolve_attention(
        &self,
        participant_id: ParticipantId,
        request_id: RequestId,
        summary: String,
        context: &DecisionContext,
    ) -> Decision {
        let Some(participant) = self.live_participant(participant_id, context.now) else {
            return Decision::Rejected("participant is absent or its lease expired".into());
        };
        if let Err(reason) = validate_text("summary", &summary) {
            return Decision::Rejected(reason);
        }
        let Some(source) = self.signals.iter().find(|signal| {
            stored_request_identity(signal)
                .is_some_and(|identity| identity.request_id == request_id)
        }) else {
            return Decision::Rejected("request does not exist".into());
        };
        if self.resolved_requests.contains(&request_id) || request_superseded(source, &self.signals)
        {
            return Decision::Rejected("request is no longer open".into());
        }
        let owns_request = source.published.participant_id == participant_id
            || source.published.work_id.is_some_and(|work_id| {
                self.work
                    .get(&work_id)
                    .is_some_and(|work| work.owner == participant_id)
            });
        if !owns_request || source.scope != participant.scope {
            return Decision::Rejected("participant cannot resolve this request".into());
        }
        Decision::Accepted {
            events: vec![PendingEvent {
                scope: source.scope.clone(),
                event: EventKind::AttentionResolved {
                    request_id,
                    participant_id,
                    summary,
                    resolved_at: context.now,
                },
            }],
            participant_id: Some(participant_id),
            work_id: source.published.work_id,
        }
    }

    fn live_participant(&self, id: ParticipantId, now: DateTime<Utc>) -> Option<&Participant> {
        self.participants
            .get(&id)
            .filter(|participant| participant.expires_at > now)
    }

    fn departure_events(
        &self,
        participant: &Participant,
        reason: LeaveReason,
    ) -> Vec<PendingEvent> {
        let release_reason = match reason {
            LeaveReason::Graceful => ReleaseReason::ParticipantLeft,
            LeaveReason::LeaseExpired => ReleaseReason::LeaseExpired,
        };
        let mut events: Vec<_> = self
            .work
            .values()
            .filter(|work| work.owner == participant.id)
            .map(|work| PendingEvent {
                scope: participant.scope.clone(),
                event: EventKind::WorkReleased {
                    work_id: work.id,
                    previous_owner: participant.id,
                    reason: release_reason,
                },
            })
            .collect();
        events.push(PendingEvent {
            scope: participant.scope.clone(),
            event: EventKind::ParticipantLeft {
                participant_id: participant.id,
                reason,
            },
        });
        events
    }
}

fn attention_signal(
    signal: &Signal,
) -> Option<(AttentionCategory, AttentionRequest, &[ArtifactRef])> {
    match signal {
        Signal::DecisionNeeded {
            summary,
            choices,
            recommendation,
            evidence,
            ..
        } => Some((
            AttentionCategory::Decision,
            AttentionRequest::Decision {
                question: summary.clone(),
                choices: choices.clone(),
                recommendation: recommendation.clone(),
            },
            evidence,
        )),
        Signal::Blocked {
            summary,
            requested_action,
            evidence,
            ..
        } => Some((
            AttentionCategory::Blocked,
            AttentionRequest::Intervention {
                blocker: summary.clone(),
                requested_action: legacy_action(
                    requested_action,
                    "Provide direction to unblock this work.",
                ),
            },
            evidence,
        )),
        Signal::HelpNeeded {
            summary,
            requested_action,
            evidence,
            ..
        } => Some((
            AttentionCategory::Help,
            AttentionRequest::Intervention {
                blocker: summary.clone(),
                requested_action: legacy_action(
                    requested_action,
                    "Provide the requested help or direction.",
                ),
            },
            evidence,
        )),
        Signal::ReviewRequested {
            summary,
            requested_action,
            known_risk,
            evidence,
            ..
        } => Some((
            AttentionCategory::ReadyForReview,
            AttentionRequest::Review {
                summary: summary.clone(),
                requested_action: legacy_action(
                    requested_action,
                    "Inspect the referenced change and mark it reviewed or request changes.",
                ),
                known_risk: legacy_action(known_risk, "Not recorded by the legacy request."),
            },
            evidence,
        )),
        Signal::Working { .. }
        | Signal::Finding { .. }
        | Signal::Offering { .. }
        | Signal::Checkpoint { .. }
        | Signal::Done { .. } => None,
    }
}

fn legacy_action(value: &str, fallback: &str) -> String {
    if value.trim().is_empty() {
        fallback.into()
    } else {
        value.into()
    }
}

fn attention_resolved(
    source: &StoredSignal,
    signals: &[StoredSignal],
    directions: &BTreeMap<DirectionId, Direction>,
    resolved_requests: &BTreeSet<RequestId>,
) -> bool {
    let Some(source_identity) = stored_request_identity(source) else {
        return true;
    };
    if resolved_requests.contains(&source_identity.request_id) {
        return true;
    }
    if directions.values().any(|direction| {
        direction.target.as_ref().is_some_and(|target| {
            target.request_id.map_or_else(
                || {
                    target.source_event_id == source.event_id
                        && target.participant_id == source.published.participant_id
                        && target.work_id == source.published.work_id
                },
                |request_id| request_id == source_identity.request_id,
            )
        })
    }) {
        return true;
    }
    request_superseded(source, signals)
}

fn request_superseded(source: &StoredSignal, signals: &[StoredSignal]) -> bool {
    let Some(source_identity) = stored_request_identity(source) else {
        return false;
    };
    signals.iter().any(|candidate| {
        candidate.scope == source.scope
            && candidate.sequence > source.sequence
            && candidate.published.participant_id == source.published.participant_id
            && candidate.published.work_id == source.published.work_id
            && stored_request_identity(candidate).is_some_and(|candidate_identity| {
                candidate_identity.request_key == source_identity.request_key
                    || candidate_identity.supersedes_request_id == Some(source_identity.request_id)
            })
    })
}

fn stored_request_identity(stored: &StoredSignal) -> Option<RequestIdentity> {
    attention_signal(&stored.published.signal)?;
    stored.published.signal.request_identity().cloned()
}

fn direction_is_relevant(
    direction: &Direction,
    participant_id: ParticipantId,
    participants: &BTreeMap<ParticipantId, Participant>,
    active_work: &BTreeMap<WorkId, WorkClaim>,
) -> bool {
    direction.target.as_ref().is_none_or(|target| {
        if participants.contains_key(&target.participant_id) {
            target.participant_id == participant_id
        } else {
            target.work_id.is_some_and(|work_id| {
                active_work
                    .get(&work_id)
                    .is_some_and(|work| work.owner == participant_id)
            })
        }
    })
}

fn signal_artifacts(signal: &Signal) -> &[ArtifactRef] {
    match signal {
        Signal::Finding { artifacts, .. }
        | Signal::Offering { artifacts, .. }
        | Signal::Checkpoint { artifacts, .. } => artifacts,
        Signal::HelpNeeded { evidence, .. }
        | Signal::DecisionNeeded { evidence, .. }
        | Signal::Blocked { evidence, .. }
        | Signal::ReviewRequested { evidence, .. }
        | Signal::Done { evidence, .. } => evidence,
        Signal::Working { .. } => &[],
    }
}

const fn attention_rank(category: AttentionCategory) -> u8 {
    match category {
        AttentionCategory::Decision => 0,
        AttentionCategory::Blocked => 1,
        AttentionCategory::Help => 2,
        AttentionCategory::ReadyForReview => 3,
    }
}

const fn artifact_rank(artifact: &ArtifactRef) -> u8 {
    match artifact.kind {
        ArtifactKind::File => 0,
        ArtifactKind::Patch => 1,
        ArtifactKind::Revision => 2,
        ArtifactKind::TestReceipt => 3,
        ArtifactKind::Url => 4,
    }
}

fn workspace_lifecycle(snapshot: &ScopeSnapshot) -> WorkspaceLifecycle {
    if !snapshot.participants.is_empty() || !snapshot.active_work.is_empty() {
        WorkspaceLifecycle::Active
    } else if snapshot
        .signals
        .last()
        .is_some_and(|published| matches!(published.signal, Signal::Done { .. }))
    {
        WorkspaceLifecycle::Completed
    } else {
        WorkspaceLifecycle::Idle
    }
}

fn validate_workspace(workspace: &Workspace) -> Result<(), String> {
    validate_text("scope", &workspace.scope)?;
    validate_text("title", &workspace.title)?;
    validate_text("objective", &workspace.objective)?;
    validate_text(
        "Linear issue identifier",
        &workspace.linear_issue.identifier,
    )?;
    if workspace.scope != format!("linear:{}", workspace.linear_issue.identifier) {
        return Err("workspace scope must be linear:<issue identifier>".into());
    }
    if let Some(repository) = &workspace.repository {
        validate_text("repository", &repository.repository)?;
    }
    Ok(())
}

fn validate_signal(signal: &Signal) -> Result<(), String> {
    let summary = match signal {
        Signal::Working { summary }
        | Signal::Finding { summary, .. }
        | Signal::Offering { summary, .. }
        | Signal::HelpNeeded { summary, .. }
        | Signal::DecisionNeeded { summary, .. }
        | Signal::Blocked { summary, .. }
        | Signal::Checkpoint { summary, .. }
        | Signal::ReviewRequested { summary, .. }
        | Signal::Done { summary, .. } => summary,
    };
    validate_text("summary", summary)?;
    for artifact in signal_artifacts(signal) {
        validate_text("artifact URI", &artifact.uri)?;
    }
    if matches!(
        signal,
        Signal::HelpNeeded { .. }
            | Signal::DecisionNeeded { .. }
            | Signal::Blocked { .. }
            | Signal::ReviewRequested { .. }
    ) {
        let identity = signal
            .request_identity()
            .ok_or_else(|| "attention request needs request identity".to_owned())?;
        validate_text("request key", &identity.request_key)?;
    }
    match signal {
        Signal::HelpNeeded {
            requested_action, ..
        }
        | Signal::Blocked {
            requested_action, ..
        } => validate_text("requested action", requested_action),
        Signal::DecisionNeeded {
            choices,
            recommendation,
            evidence,
            ..
        } => {
            if choices.len() > 9 {
                return Err("a decision request supports at most 9 choices".into());
            }
            for choice in choices {
                validate_text("choice", choice)?;
            }
            if let Some(recommendation) = recommendation {
                validate_text("recommendation", recommendation)?;
            }
            if evidence.is_empty() {
                return Err("a decision request needs supporting evidence".into());
            }
            Ok(())
        }
        Signal::ReviewRequested {
            requested_action,
            known_risk,
            evidence,
            ..
        } => {
            validate_text("requested action", requested_action)?;
            validate_text("known risk", known_risk)?;
            if !evidence.iter().any(|artifact| {
                matches!(artifact.kind, ArtifactKind::Patch | ArtifactKind::Revision)
            }) {
                return Err("a review request needs a revision or diff artifact".into());
            }
            if !evidence
                .iter()
                .any(|artifact| artifact.kind == ArtifactKind::TestReceipt)
            {
                return Err("a review request needs a verification receipt".into());
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

fn validate_text(field: &str, value: &str) -> Result<(), String> {
    if value.trim().is_empty() {
        Err(format!("{field} must not be empty"))
    } else {
        Ok(())
    }
}

fn validate_lease(seconds: u64) -> Result<(), String> {
    if (5..=300).contains(&seconds) {
        Ok(())
    } else {
        Err("lease must be between 5 and 300 seconds".into())
    }
}

fn lease_expiry(now: DateTime<Utc>, seconds: u64) -> DateTime<Utc> {
    now + Duration::seconds(i64::try_from(seconds).unwrap_or(i64::MAX))
}

fn normalize_resources(resources: Vec<String>) -> Result<Vec<String>, String> {
    let mut normalized = Vec::with_capacity(resources.len());
    for resource in resources {
        let mut parts = Vec::new();
        for component in std::path::Path::new(resource.trim()).components() {
            match component {
                std::path::Component::Normal(part) => {
                    parts.push(part.to_string_lossy().into_owned());
                }
                std::path::Component::CurDir => {}
                std::path::Component::ParentDir => {
                    if parts.pop().is_none() {
                        return Err("resource must not escape the repository root".into());
                    }
                }
                std::path::Component::RootDir | std::path::Component::Prefix(_) => {
                    return Err("resource must be a repository-relative path".into());
                }
            }
        }
        if !parts.is_empty() {
            normalized.push(parts.join("/"));
        }
    }
    normalized.sort();
    normalized.dedup();
    Ok(normalized)
}

fn resources_overlap(left: &[String], right: &[String]) -> bool {
    left.iter()
        .any(|left| right.iter().any(|right| path_overlaps(left, right)))
}

fn path_overlaps(left: &str, right: &str) -> bool {
    left == right
        || left
            .strip_prefix(right)
            .is_some_and(|suffix| suffix.starts_with('/'))
        || right
            .strip_prefix(left)
            .is_some_and(|suffix| suffix.starts_with('/'))
}

fn signal_requires_work(signal: &Signal) -> bool {
    matches!(
        signal,
        Signal::Working { .. }
            | Signal::Checkpoint { .. }
            | Signal::ReviewRequested { .. }
            | Signal::Done { .. }
    )
}

fn signal_requires_ownership(signal: &Signal) -> bool {
    signal_requires_work(signal)
}

#[cfg(test)]
mod tests {
    use super::{
        ArtifactKind, ArtifactRef, AttentionRequest, ParticipantId, PublishedSignal, RequestId,
        RequestIdentity, Signal, StoredSignal, Utc, Uuid, attention_signal,
        stored_request_identity, validate_signal,
    };

    #[test]
    fn legacy_attention_events_replay_with_actionable_fallbacks() {
        let blocked: Signal =
            serde_json::from_str(r#"{"kind":"blocked","summary":"Credentials are unavailable"}"#)
                .unwrap();
        let review: Signal = serde_json::from_str(
            r#"{"kind":"review_requested","summary":"Legacy change is ready","evidence":[]}"#,
        )
        .unwrap();

        let (_, blocked_request, _) = attention_signal(&blocked).unwrap();
        assert_eq!(
            validate_signal(&blocked),
            Err("attention request needs request identity".into())
        );
        assert!(matches!(
            blocked_request,
            AttentionRequest::Intervention { requested_action, .. }
                if !requested_action.trim().is_empty()
        ));
        let (_, review_request, _) = attention_signal(&review).unwrap();
        assert!(matches!(
            review_request,
            AttentionRequest::Review { requested_action, known_risk, .. }
                if !requested_action.trim().is_empty() && !known_risk.trim().is_empty()
        ));
        let replayed = StoredSignal {
            sequence: 1,
            event_id: Uuid::now_v7(),
            scope: "linear:TG-192".into(),
            published: PublishedSignal {
                participant_id: ParticipantId(Uuid::now_v7()),
                work_id: None,
                signal: blocked,
                published_at: Utc::now(),
            },
        };
        assert_eq!(stored_request_identity(&replayed), None);
    }

    #[test]
    fn actionable_requests_reject_blank_artifact_uris_at_the_protocol_boundary() {
        let blank = ArtifactRef {
            kind: ArtifactKind::Revision,
            uri: String::new(),
            digest: None,
        };
        let decision = Signal::DecisionNeeded {
            identity: Some(RequestIdentity {
                request_id: RequestId(Uuid::now_v7()),
                request_key: "decision".into(),
                supersedes_request_id: None,
            }),
            summary: "Choose a path".into(),
            choices: vec![],
            recommendation: None,
            evidence: vec![blank.clone()],
        };
        let review = Signal::ReviewRequested {
            identity: Some(RequestIdentity {
                request_id: RequestId(Uuid::now_v7()),
                request_key: "review".into(),
                supersedes_request_id: None,
            }),
            summary: "Inspect the change".into(),
            requested_action: "Mark reviewed or request changes".into(),
            known_risk: "None identified".into(),
            evidence: vec![
                blank,
                ArtifactRef {
                    kind: ArtifactKind::TestReceipt,
                    uri: String::new(),
                    digest: None,
                },
            ],
        };

        assert_eq!(
            validate_signal(&decision),
            Err("artifact URI must not be empty".into())
        );
        assert_eq!(
            validate_signal(&review),
            Err("artifact URI must not be empty".into())
        );
    }
}
