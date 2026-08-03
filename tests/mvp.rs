use std::time::Duration;

use clarity::{
    Runtime,
    protocol::{
        ArtifactKind, ArtifactRef, AttentionCategory, AttentionRequest, Command, CommandResult,
        DirectionTarget, EnvironmentManifest, EventKind, LinearIssue, LinearMetadata,
        RepositoryRef, RequestId, RequestIdentity, RequestLifecycle, Signal, WorkId, Workspace,
        WorkspaceLifecycle,
    },
};
use tempfile::tempdir;

#[tokio::test]
async fn independent_participants_prevent_overlap_and_exchange_a_finding() {
    let directory = tempdir().unwrap();
    let database = directory.path().join("events.db");
    let runtime = Runtime::open(&database).unwrap();
    let scope = "linear:TG-181";

    let workspace = test_workspace(scope);
    define_twice(&runtime, &workspace).await;

    let agent_a = join(&runtime, scope, "agent-a").await;
    let agent_b = join(&runtime, scope, "agent-b").await;

    let first_claim = runtime
        .command(Command::ClaimWork {
            participant_id: agent_a,
            summary: "Inspect overlap semantics".into(),
            resources: vec!["src/coordination.rs".into()],
        })
        .await
        .unwrap();
    assert!(matches!(first_claim, CommandResult::Accepted { .. }));

    runtime
        .command(Command::PublishSignal {
            participant_id: agent_a,
            work_id: None,
            signal: Signal::Finding {
                summary: "Directory-prefix overlap is already implemented".into(),
                artifacts: vec![ArtifactRef {
                    kind: ArtifactKind::File,
                    uri: "src/coordination.rs".into(),
                    digest: Some("sha256:example".into()),
                }],
            },
        })
        .await
        .unwrap();

    let overlapping_claim = runtime
        .command(Command::ClaimWork {
            participant_id: agent_b,
            summary: "Rewrite the coordination domain".into(),
            resources: vec!["./src/".into()],
        })
        .await
        .unwrap();
    let CommandResult::Conflict { conflicts, .. } = overlapping_claim else {
        panic!("overlapping work should be prevented");
    };
    assert_eq!(conflicts.len(), 1);
    assert_eq!(conflicts[0].owner, agent_a);

    runtime
        .command(Command::Intervene {
            scope: scope.into(),
            message: "Agent B should consume Agent A's finding".into(),
            author: "test-human".into(),
            target: None,
        })
        .await
        .unwrap();

    let snapshot = runtime.snapshot(scope.into()).await.unwrap();
    assert_eq!(snapshot.active_work.len(), 1);
    assert_eq!(snapshot.signals.len(), 1);
    let Signal::Finding { summary, artifacts } = &snapshot.signals[0].signal else {
        panic!("the observed signal should remain a typed finding");
    };
    assert_eq!(snapshot.signals[0].participant_id, agent_a);
    assert_eq!(summary, "Directory-prefix overlap is already implemented");
    assert_eq!(artifacts[0].kind, ArtifactKind::File);
    assert_eq!(artifacts[0].uri, "src/coordination.rs");
    assert_eq!(snapshot.metrics.prevented_overlaps, 1);
    assert_eq!(snapshot.metrics.human_interventions, 1);
    assert_eq!(snapshot.workspace, Some(workspace.clone()));

    let workspaces = runtime.workspaces().await.unwrap();
    assert_eq!(workspaces.len(), 1);
    assert_eq!(workspaces[0].scope, scope);
    assert_eq!(
        workspaces[0].title.as_deref(),
        Some(workspace.title.as_str())
    );
    assert_eq!(workspaces[0].lifecycle, WorkspaceLifecycle::Active);
    assert_eq!(workspaces[0].active_participants, 2);

    let events = runtime.events(scope.into(), 0).await.unwrap();
    assert!(
        events
            .iter()
            .any(|event| matches!(event.event, EventKind::OverlapDetected { .. }))
    );
    assert!(
        events
            .iter()
            .any(|event| matches!(event.event, EventKind::SignalPublished { .. }))
    );

    drop(runtime);
    tokio::time::sleep(Duration::from_millis(20)).await;

    let replayed = Runtime::open(database).unwrap();
    let snapshot = replayed.snapshot(scope.into()).await.unwrap();
    assert_eq!(snapshot.active_work.len(), 1);
    assert_eq!(snapshot.signals.len(), 1);
    assert_eq!(snapshot.metrics.prevented_overlaps, 1);
    assert_eq!(snapshot.metrics.human_interventions, 1);
    assert_eq!(replayed.workspaces().await.unwrap()[0].scope, scope);
}

async fn define_twice(runtime: &Runtime, workspace: &Workspace) {
    runtime
        .command(Command::DefineWorkspace {
            workspace: workspace.clone(),
        })
        .await
        .unwrap();
    let reconnected = runtime
        .command(Command::DefineWorkspace {
            workspace: workspace.clone(),
        })
        .await
        .unwrap();
    assert!(matches!(
        reconnected,
        CommandResult::Accepted {
            event_sequences,
            ..
        } if event_sequences.is_empty()
    ));
}

fn test_workspace(scope: &str) -> Workspace {
    let identifier = scope.strip_prefix("linear:").unwrap_or(scope);
    Workspace {
        scope: scope.into(),
        title: "Prove independent-agent coordination".into(),
        objective: "Two agents coordinate without manual context routing".into(),
        linear_issue: LinearIssue {
            identifier: identifier.into(),
            url: Some(format!("https://linear.app/tinygarden/issue/{identifier}")),
            metadata: None,
        },
        repository: Some(RepositoryRef {
            repository: "antidmg/clarity".into(),
            revision: Some("main".into()),
        }),
    }
}

fn request_identity(key: &str) -> RequestIdentity {
    RequestIdentity {
        request_id: RequestId(uuid::Uuid::now_v7()),
        request_key: key.into(),
        supersedes_request_id: None,
    }
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn linear_metadata_refresh_is_idempotent_replayable_and_independent_of_work_history() {
    let directory = tempdir().unwrap();
    let database = directory.path().join("events.db");
    let runtime = Runtime::open(&database).unwrap();
    let scope = "linear:TG-REFRESH";
    let mut workspace = test_workspace(scope);
    workspace.linear_issue.metadata = Some(LinearMetadata {
        status: "Todo".into(),
        updated_at: chrono::Utc::now(),
    });
    define_twice(&runtime, &workspace).await;

    let mut refreshed = workspace.clone();
    refreshed.title = "Renamed in Linear".into();
    refreshed.objective = "Continue reopened work without deleting history".into();
    refreshed.linear_issue.url = Some("https://linear.app/issue/TG-REFRESH-renamed".into());
    refreshed.linear_issue.metadata = Some(LinearMetadata {
        status: "In Progress".into(),
        updated_at: chrono::Utc::now() + chrono::Duration::seconds(1),
    });
    let changed = runtime
        .command(Command::RefreshWorkspaceMetadata {
            workspace: refreshed.clone(),
        })
        .await
        .unwrap();
    assert!(matches!(
        changed,
        CommandResult::Accepted {
            ref event_sequences,
            ..
        } if event_sequences.len() == 1
    ));
    let unchanged = runtime
        .command(Command::RefreshWorkspaceMetadata {
            workspace: refreshed.clone(),
        })
        .await
        .unwrap();
    assert!(matches!(
        unchanged,
        CommandResult::Accepted {
            ref event_sequences,
            ..
        } if event_sequences.is_empty()
    ));
    assert_eq!(
        runtime
            .snapshot(scope.into())
            .await
            .unwrap()
            .workspace
            .as_ref(),
        Some(&refreshed)
    );
    assert!(
        runtime
            .events(scope.into(), 0)
            .await
            .unwrap()
            .iter()
            .any(|event| matches!(event.event, EventKind::WorkspaceMetadataUpdated { .. }))
    );

    let participant = join(&runtime, scope, "reopened-agent").await;
    let claim = runtime
        .command(Command::ClaimWork {
            participant_id: participant,
            summary: "Complete one unit of reopened work".into(),
            resources: vec!["src/reopened.rs".into()],
        })
        .await
        .unwrap();
    let CommandResult::Accepted {
        work_id: Some(work_id),
        ..
    } = claim
    else {
        panic!("claim should return work identity");
    };
    publish(
        &runtime,
        participant,
        Some(work_id),
        Signal::Done {
            summary: "One work claim completed".into(),
            evidence: vec![],
        },
    )
    .await;
    runtime
        .command(Command::Leave {
            participant_id: participant,
        })
        .await
        .unwrap();
    assert_eq!(
        runtime.workspaces().await.unwrap()[0].lifecycle,
        WorkspaceLifecycle::Completed
    );
    assert!(
        runtime
            .snapshot(scope.into())
            .await
            .unwrap()
            .signals
            .iter()
            .any(|published| matches!(published.signal, Signal::Done { .. }))
    );

    drop(runtime);
    let replayed = Runtime::open(database).unwrap();
    let replayed_snapshot = replayed.snapshot(scope.into()).await.unwrap();
    assert_eq!(replayed_snapshot.workspace.as_ref(), Some(&refreshed));
    assert_eq!(
        replayed.workspaces().await.unwrap()[0].lifecycle,
        WorkspaceLifecycle::Completed
    );
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn attention_is_typed_ordered_traceable_and_resolved_explicitly() {
    let directory = tempdir().unwrap();
    let runtime = Runtime::open(directory.path().join("events.db")).unwrap();
    let cases = [
        ("linear:TG-DECISION", "decision-agent"),
        ("linear:TG-BLOCKED", "blocked-agent"),
        ("linear:TG-HELP", "help-agent"),
        ("linear:TG-REVIEW", "review-agent"),
        ("linear:TG-QUIET", "quiet-agent"),
    ];
    let mut work = Vec::new();
    for (scope, name) in cases {
        define_twice(&runtime, &test_workspace(scope)).await;
        let participant = join(&runtime, scope, name).await;
        let result = runtime
            .command(Command::ClaimWork {
                participant_id: participant,
                summary: format!("Work in {scope}"),
                resources: vec![format!("{name}.rs")],
            })
            .await
            .unwrap();
        let CommandResult::Accepted {
            work_id: Some(work_id),
            ..
        } = result
        else {
            panic!("claim should return a work identity");
        };
        work.push((scope, participant, work_id));
    }

    let [
        (decision_scope, decision_agent, decision_work),
        (_, blocked_agent, blocked_work),
        (_, help_agent, help_work),
        (_, review_agent, review_work),
        (_, quiet_agent, quiet_work),
    ] = work.as_slice()
    else {
        unreachable!();
    };
    publish(
        &runtime,
        *decision_agent,
        Some(*decision_work),
        Signal::Finding {
            summary: "The API contract is the relevant finding".into(),
            artifacts: vec![ArtifactRef {
                kind: ArtifactKind::File,
                uri: "src/protocol.rs".into(),
                digest: Some("sha256:evidence".into()),
            }],
        },
    )
    .await;
    publish(
        &runtime,
        *review_agent,
        Some(*review_work),
        Signal::ReviewRequested {
            identity: Some(request_identity("projection-review")),
            summary: "Initial projection is ready for review".into(),
            requested_action: "Inspect the initial projection".into(),
            known_risk: "Projection ordering may change".into(),
            evidence: vec![
                ArtifactRef {
                    kind: ArtifactKind::Revision,
                    uri: "git:old-projection".into(),
                    digest: None,
                },
                ArtifactRef {
                    kind: ArtifactKind::TestReceipt,
                    uri: "cargo nextest run old-attention".into(),
                    digest: None,
                },
            ],
        },
    )
    .await;
    publish(
        &runtime,
        *review_agent,
        Some(*review_work),
        Signal::ReviewRequested {
            identity: Some(request_identity("projection-review")),
            summary: "Projection is ready for review".into(),
            requested_action: "Inspect the new projection".into(),
            known_risk: "None identified".into(),
            evidence: vec![
                ArtifactRef {
                    kind: ArtifactKind::Revision,
                    uri: "git:new-projection".into(),
                    digest: None,
                },
                ArtifactRef {
                    kind: ArtifactKind::TestReceipt,
                    uri: "cargo nextest run attention".into(),
                    digest: None,
                },
            ],
        },
    )
    .await;
    let first_help_identity = request_identity("help-contract-v1");
    let first_help_id = first_help_identity.request_id;
    let second_help_identity = RequestIdentity {
        request_id: RequestId(uuid::Uuid::now_v7()),
        request_key: "blocked-contract".into(),
        supersedes_request_id: Some(first_help_identity.request_id),
    };
    publish(
        &runtime,
        *help_agent,
        Some(*help_work),
        Signal::HelpNeeded {
            identity: Some(first_help_identity),
            summary: "First help request".into(),
            requested_action: "Provide the first answer".into(),
            evidence: vec![],
        },
    )
    .await;
    publish(
        &runtime,
        *help_agent,
        Some(*help_work),
        Signal::HelpNeeded {
            identity: Some(second_help_identity),
            summary: "Second help request".into(),
            requested_action: "Provide the second answer".into(),
            evidence: vec![],
        },
    )
    .await;
    publish(
        &runtime,
        *blocked_agent,
        Some(*blocked_work),
        Signal::Blocked {
            identity: Some(request_identity("blocked-contract")),
            summary: "Cannot continue without an intervention".into(),
            requested_action: "Provide an intervention".into(),
            evidence: vec![],
        },
    )
    .await;
    publish(
        &runtime,
        *decision_agent,
        Some(*decision_work),
        Signal::DecisionNeeded {
            identity: Some(request_identity("target-contract")),
            summary: "Choose the stable target contract".into(),
            choices: vec!["Per-recipient receipt".into(), "Shared receipt".into()],
            recommendation: Some("Per-recipient receipt".into()),
            evidence: vec![ArtifactRef {
                kind: ArtifactKind::File,
                uri: "src/protocol.rs".into(),
                digest: Some("sha256:evidence".into()),
            }],
        },
    )
    .await;
    publish(
        &runtime,
        *quiet_agent,
        Some(*quiet_work),
        Signal::Working {
            summary: "Normal work is progressing".into(),
        },
    )
    .await;
    publish(
        &runtime,
        *quiet_agent,
        Some(*quiet_work),
        Signal::Checkpoint {
            summary: "Routine checkpoint remains quiet".into(),
            artifacts: vec![],
        },
    )
    .await;

    let attention = runtime.attention().await.unwrap();
    assert_eq!(
        attention
            .iter()
            .map(|item| item.category)
            .collect::<Vec<_>>(),
        vec![
            AttentionCategory::Decision,
            AttentionCategory::Blocked,
            AttentionCategory::Help,
            AttentionCategory::ReadyForReview,
        ]
    );
    assert!(matches!(
        &attention[2].request,
        AttentionRequest::Intervention { blocker, .. } if blocker == "Second help request"
    ));
    assert!(
        !attention
            .iter()
            .any(|item| item.request_id == first_help_id)
    );
    assert!(matches!(
        &attention[3].request,
        AttentionRequest::Review { summary, .. } if summary == "Projection is ready for review"
    ));
    assert_eq!(attention[3].evidence.len(), 2);
    let review = attention[3].clone();
    let decision = &attention[0];
    assert_eq!(decision.source_scope, *decision_scope);
    assert_eq!(decision.work_id, Some(*decision_work));
    assert_eq!(decision.participant_id, *decision_agent);
    assert_eq!(decision.lifecycle, RequestLifecycle::Open);
    assert_eq!(decision.target.scope, decision.source_scope);
    assert_eq!(decision.target.event_id, decision.source_event_id);
    assert!(matches!(
        &decision.request,
        AttentionRequest::Decision { question, choices, .. }
            if question == "Choose the stable target contract" && choices.len() == 2
    ));
    assert_eq!(decision.evidence.len(), 1);
    assert_eq!(decision.evidence[0].uri, "src/protocol.rs");
    assert!(
        runtime
            .snapshot("linear:TG-QUIET".into())
            .await
            .unwrap()
            .attention
            .is_empty()
    );

    runtime
        .command(Command::Intervene {
            scope: review.source_scope,
            message: "Reviewed; the explicit projection is correct".into(),
            author: "reviewer@example.com".into(),
            target: Some(DirectionTarget {
                request_id: Some(review.request_id),
                source_event_id: review.source_event_id,
                participant_id: review.participant_id,
                work_id: review.work_id,
            }),
        })
        .await
        .unwrap();
    assert!(
        !runtime
            .attention()
            .await
            .unwrap()
            .iter()
            .any(|item| item.source_event_id == review.source_event_id)
    );

    publish(
        &runtime,
        *decision_agent,
        Some(*decision_work),
        Signal::Done {
            summary: "Decision applied and work completed".into(),
            evidence: vec![ArtifactRef {
                kind: ArtifactKind::Revision,
                uri: "git:abc123".into(),
                digest: None,
            }],
        },
    )
    .await;
    let resolved = runtime.attention().await.unwrap();
    assert!(resolved.iter().any(|item| {
        item.category == AttentionCategory::Decision && item.work_id == Some(*decision_work)
    }));
    runtime
        .command(Command::ResolveAttention {
            participant_id: *decision_agent,
            request_id: decision.request_id,
            summary: "Applied the selected target contract".into(),
        })
        .await
        .unwrap();
    assert!(
        !runtime
            .attention()
            .await
            .unwrap()
            .iter()
            .any(|item| item.request_id == decision.request_id)
    );
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn targeted_direction_is_delivered_consumed_and_resolved_by_follow_up() {
    let directory = tempdir().unwrap();
    let database = directory.path().join("events.db");
    let runtime = Runtime::open(&database).unwrap();
    let scope = "linear:TG-191";
    define_twice(&runtime, &test_workspace(scope)).await;
    let target = join(&runtime, scope, "target-agent").await;
    let unrelated = join(&runtime, scope, "unrelated-agent").await;
    let claim = runtime
        .command(Command::ClaimWork {
            participant_id: target,
            summary: "Implement exact direction routing".into(),
            resources: vec!["src/protocol.rs".into()],
        })
        .await
        .unwrap();
    let CommandResult::Accepted {
        work_id: Some(work_id),
        ..
    } = claim
    else {
        panic!("claim should return work identity");
    };
    let direction_identity = request_identity("direction-shape");
    let published_request = publish(
        &runtime,
        target,
        Some(work_id),
        Signal::DecisionNeeded {
            identity: Some(direction_identity.clone()),
            summary: "Choose the durable direction shape".into(),
            choices: vec!["Per-recipient receipt".into()],
            recommendation: None,
            evidence: vec![ArtifactRef {
                kind: ArtifactKind::File,
                uri: "src/protocol.rs".into(),
                digest: None,
            }],
        },
    )
    .await;
    assert!(matches!(
        published_request,
        CommandResult::Accepted {
            request_id: Some(request_id),
            ..
        } if request_id == direction_identity.request_id
    ));
    publish(
        &runtime,
        target,
        Some(work_id),
        Signal::HelpNeeded {
            identity: Some(request_identity("independent-help")),
            summary: "Confirm the rollout window".into(),
            requested_action: "Provide a rollout window".into(),
            evidence: vec![],
        },
    )
    .await;
    let requests = runtime.attention().await.unwrap();
    assert_eq!(requests.len(), 2);
    let request = requests
        .iter()
        .find(|item| item.category == AttentionCategory::Decision)
        .unwrap()
        .clone();
    let independent = requests
        .iter()
        .find(|item| item.category == AttentionCategory::Help)
        .unwrap()
        .clone();

    runtime
        .command(Command::Intervene {
            scope: scope.into(),
            message: "Use an explicit per-recipient delivery receipt".into(),
            author: "reviewer@example.com".into(),
            target: Some(DirectionTarget {
                request_id: Some(request.request_id),
                source_event_id: request.source_event_id,
                participant_id: target,
                work_id: Some(work_id),
            }),
        })
        .await
        .unwrap();
    for participant_id in [target, unrelated] {
        runtime
            .command(Command::DeliverDirections { participant_id })
            .await
            .unwrap();
    }
    let delivered = runtime.directions(target).await.unwrap();
    assert_eq!(delivered.len(), 1);
    assert_eq!(delivered[0].author, "reviewer@example.com");
    assert_eq!(
        delivered[0].target.as_ref().unwrap().source_event_id,
        request.source_event_id
    );
    assert!(runtime.directions(unrelated).await.unwrap().is_empty());

    let direction_id = delivered[0].id;
    runtime
        .command(Command::ConsumeDirection {
            participant_id: target,
            direction_id,
        })
        .await
        .unwrap();
    assert!(runtime.directions(target).await.unwrap().is_empty());

    runtime
        .command(Command::Intervene {
            scope: scope.into(),
            message: "Workspace-wide release window changed".into(),
            author: "release-manager@example.com".into(),
            target: None,
        })
        .await
        .unwrap();
    for participant_id in [target, unrelated] {
        runtime
            .command(Command::DeliverDirections { participant_id })
            .await
            .unwrap();
        let broadcasts = runtime.directions(participant_id).await.unwrap();
        assert_eq!(broadcasts.len(), 1);
        assert!(broadcasts[0].target.is_none());
        runtime
            .command(Command::ConsumeDirection {
                participant_id,
                direction_id: broadcasts[0].id,
            })
            .await
            .unwrap();
    }
    publish(
        &runtime,
        target,
        Some(work_id),
        Signal::Checkpoint {
            summary: "Direction changed the implementation to per-recipient receipts".into(),
            artifacts: vec![ArtifactRef {
                kind: ArtifactKind::TestReceipt,
                uri: "targeted-direction-integration".into(),
                digest: None,
            }],
        },
    )
    .await;
    assert!(
        !runtime
            .attention()
            .await
            .unwrap()
            .iter()
            .any(|item| item.source_event_id == request.source_event_id)
    );
    assert!(
        runtime
            .attention()
            .await
            .unwrap()
            .iter()
            .any(|item| item.request_id == independent.request_id)
    );
    runtime
        .command(Command::ResolveAttention {
            participant_id: target,
            request_id: request.request_id,
            summary: "Applied and verified the selected direction".into(),
        })
        .await
        .unwrap();
    let stale_response = runtime
        .command(Command::Intervene {
            scope: scope.into(),
            message: "This response arrived after resolution".into(),
            author: "reviewer@example.com".into(),
            target: Some(DirectionTarget {
                request_id: Some(request.request_id),
                source_event_id: request.source_event_id,
                participant_id: target,
                work_id: Some(work_id),
            }),
        })
        .await
        .unwrap();
    assert!(matches!(stale_response, CommandResult::Rejected { .. }));
    publish(
        &runtime,
        target,
        Some(work_id),
        Signal::Done {
            summary: "Targeted direction was applied".into(),
            evidence: vec![ArtifactRef {
                kind: ArtifactKind::TestReceipt,
                uri: "targeted-direction-integration".into(),
                digest: None,
            }],
        },
    )
    .await;
    assert!(
        runtime
            .attention()
            .await
            .unwrap()
            .iter()
            .any(|item| item.request_id == independent.request_id)
    );
    runtime
        .command(Command::ResolveAttention {
            participant_id: target,
            request_id: independent.request_id,
            summary: "Rollout window is no longer needed".into(),
        })
        .await
        .unwrap();
    assert!(
        !runtime
            .attention()
            .await
            .unwrap()
            .iter()
            .any(|item| item.request_id == independent.request_id)
    );

    let chain = runtime.events(scope.into(), 0).await.unwrap();
    let kinds = chain
        .iter()
        .filter_map(|event| match &event.event {
            EventKind::SignalPublished { published }
                if matches!(published.signal, Signal::DecisionNeeded { .. }) =>
            {
                Some("request")
            }
            EventKind::DirectionIssued { direction } if direction.id == direction_id => {
                Some("decision")
            }
            EventKind::DirectionDelivered {
                direction_id: id, ..
            } if *id == direction_id => Some("delivered"),
            EventKind::DirectionConsumed {
                direction_id: id, ..
            } if *id == direction_id => Some("consumed"),
            EventKind::SignalPublished { published }
                if matches!(published.signal, Signal::Checkpoint { .. }) =>
            {
                Some("checkpoint")
            }
            EventKind::SignalPublished { published }
                if matches!(published.signal, Signal::Done { .. }) =>
            {
                Some("done")
            }
            EventKind::AttentionResolved { request_id, .. }
                if *request_id == request.request_id =>
            {
                Some("request-resolved")
            }
            EventKind::AttentionResolved { request_id, .. }
                if *request_id == independent.request_id =>
            {
                Some("resolved")
            }
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(
        kinds,
        [
            "request",
            "decision",
            "delivered",
            "consumed",
            "checkpoint",
            "request-resolved",
            "done",
            "resolved"
        ]
    );

    drop(runtime);
    tokio::time::sleep(Duration::from_millis(20)).await;
    let replayed = Runtime::open(database).unwrap();
    let replayed_events = replayed.events(scope.into(), 0).await.unwrap();
    let direction = replayed_events
        .iter()
        .find_map(|event| match &event.event {
            EventKind::DirectionIssued { direction } if direction.id == direction_id => {
                Some(direction)
            }
            _ => None,
        })
        .unwrap();
    assert_eq!(direction.id, direction_id);
    assert_eq!(
        direction.target.as_ref().unwrap().request_id,
        Some(request.request_id)
    );
    assert!(replayed_events.iter().any(|event| matches!(
        event.event,
        EventKind::DirectionConsumed { direction_id: id, .. } if id == direction_id
    )));
    assert!(replayed_events.iter().any(|event| matches!(
        event.event,
        EventKind::AttentionResolved { request_id, .. }
            if request_id == independent.request_id
    )));
    assert!(replayed_events.iter().any(|event| matches!(
        event.event,
        EventKind::AttentionResolved { request_id, .. }
            if request_id == request.request_id
    )));
}

#[tokio::test]
async fn presence_renewals_do_not_reorder_workspaces() {
    let directory = tempdir().unwrap();
    let runtime = Runtime::open(directory.path().join("events.db")).unwrap();
    let older_scope = "linear:TG-188";
    let newer_scope = "linear:TG-191";
    define_twice(&runtime, &test_workspace(older_scope)).await;
    let older = join(&runtime, older_scope, "older-agent").await;
    define_twice(&runtime, &test_workspace(newer_scope)).await;
    let newer = join(&runtime, newer_scope, "newer-agent").await;
    assert_eq!(runtime.workspaces().await.unwrap()[0].scope, newer_scope);

    for participant_id in [older, newer, older] {
        runtime
            .command(Command::RenewPresence {
                participant_id,
                lease_seconds: 60,
            })
            .await
            .unwrap();
    }
    assert_eq!(runtime.workspaces().await.unwrap()[0].scope, newer_scope);

    publish(
        &runtime,
        older,
        None,
        Signal::Finding {
            summary: "A meaningful update may reorder the workspace".into(),
            artifacts: vec![],
        },
    )
    .await;
    assert_eq!(runtime.workspaces().await.unwrap()[0].scope, older_scope);
}

async fn publish(
    runtime: &Runtime,
    participant_id: clarity::protocol::ParticipantId,
    work_id: Option<WorkId>,
    signal: Signal,
) -> CommandResult {
    let result = runtime
        .command(Command::PublishSignal {
            participant_id,
            work_id,
            signal,
        })
        .await
        .unwrap();
    assert!(matches!(result, CommandResult::Accepted { .. }));
    result
}

#[tokio::test]
async fn concurrent_overlapping_claims_have_exactly_one_winner() {
    let directory = tempdir().unwrap();
    let runtime = Runtime::open(directory.path().join("events.db")).unwrap();
    let scope = "linear:TG-181:concurrency";
    let agent_a = join(&runtime, scope, "agent-a").await;
    let agent_b = join(&runtime, scope, "agent-b").await;

    let runtime_a = runtime.clone();
    let runtime_b = runtime.clone();
    let (left, right) = tokio::join!(
        runtime_a.command(Command::ClaimWork {
            participant_id: agent_a,
            summary: "Edit one file".into(),
            resources: vec!["src/../src/coordination.rs".into()],
        }),
        runtime_b.command(Command::ClaimWork {
            participant_id: agent_b,
            summary: "Edit the containing module".into(),
            resources: vec!["src".into()],
        })
    );

    let results = [left.unwrap(), right.unwrap()];
    assert_eq!(
        results
            .iter()
            .filter(|result| matches!(result, CommandResult::Accepted { .. }))
            .count(),
        1
    );
    assert_eq!(
        results
            .iter()
            .filter(|result| matches!(result, CommandResult::Conflict { .. }))
            .count(),
        1
    );
    let snapshot = runtime.snapshot(scope.into()).await.unwrap();
    assert_eq!(snapshot.active_work.len(), 1);
    assert_eq!(snapshot.metrics.prevented_overlaps, 1);
}

async fn join(runtime: &Runtime, scope: &str, name: &str) -> clarity::protocol::ParticipantId {
    let result = runtime
        .command(Command::Join {
            scope: scope.into(),
            name: name.into(),
            manifest: EnvironmentManifest {
                harness: format!("test-{name}"),
                repository: Some("clarity".into()),
                revision: Some("test".into()),
                worktree: None,
                capabilities: vec!["rust".into()],
            },
            lease_seconds: 30,
        })
        .await
        .unwrap();
    let CommandResult::Accepted {
        participant_id: Some(participant_id),
        ..
    } = result
    else {
        panic!("join should return a participant identity");
    };
    participant_id
}
