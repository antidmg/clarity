use std::path::Path;

use chrono::{DateTime, Utc};
use rusqlite::{Connection, params};
use thiserror::Error;
use tokio::sync::{mpsc, oneshot};
use uuid::Uuid;

use crate::{
    coordination::{CoordinationState, Decision, DecisionContext, PendingEvent},
    protocol::{
        AttentionItem, Command, CommandResult, Direction, EventEnvelope, EventKind, ParticipantId,
        ScopeSnapshot, WorkspaceSummary,
    },
};

#[derive(Debug, Error)]
pub enum RuntimeError {
    #[error("coordination runtime stopped")]
    Stopped,
    #[error("event store failed: {0}")]
    Store(#[from] rusqlite::Error),
    #[error("event serialization failed: {0}")]
    Serialization(#[from] serde_json::Error),
    #[error("stored event has a negative sequence: {0}")]
    InvalidSequence(i64),
}

#[derive(Clone)]
pub struct Runtime {
    sender: mpsc::Sender<Request>,
}

impl Runtime {
    /// Opens the durable event store and starts the single-writer actor.
    ///
    /// # Errors
    ///
    /// Returns an error when the database cannot be opened, initialized, or replayed.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, RuntimeError> {
        let store = EventStore::open(path.as_ref())?;
        let mut state = CoordinationState::default();
        for event in store.load_all()? {
            state.apply(&event);
        }
        let (sender, receiver) = mpsc::channel(128);
        tokio::spawn(run_actor(receiver, state, store));
        Ok(Self { sender })
    }

    /// Serializes a command through the authoritative writer.
    ///
    /// # Errors
    ///
    /// Returns an error when the runtime stops or persistence fails.
    pub async fn command(&self, command: Command) -> Result<CommandResult, RuntimeError> {
        self.request(|reply| Request::Command { command, reply })
            .await
    }

    /// Returns the current projection for one coordination scope.
    ///
    /// # Errors
    ///
    /// Returns an error when the runtime stops before replying.
    pub async fn snapshot(&self, scope: String) -> Result<ScopeSnapshot, RuntimeError> {
        self.request(|reply| Request::Snapshot { scope, reply })
            .await
    }

    /// Replays committed events in a scope after the given sequence.
    ///
    /// # Errors
    ///
    /// Returns an error when the runtime stops or the store cannot be read.
    pub async fn events(
        &self,
        scope: String,
        after: u64,
    ) -> Result<Vec<EventEnvelope>, RuntimeError> {
        self.request(|reply| Request::Events {
            scope,
            after,
            reply,
        })
        .await
    }

    /// Returns summaries for every durable coordination scope.
    ///
    /// # Errors
    ///
    /// Returns an error when the runtime stops before replying.
    pub async fn workspaces(&self) -> Result<Vec<WorkspaceSummary>, RuntimeError> {
        self.request(|reply| Request::Workspaces { reply }).await
    }

    /// Returns the ordered unresolved attention projection across all workspaces.
    ///
    /// # Errors
    ///
    /// Returns an error when the runtime stops before replying.
    pub async fn attention(&self) -> Result<Vec<AttentionItem>, RuntimeError> {
        self.request(|reply| Request::Attention { reply }).await
    }

    /// Returns delivered, unconsumed direction relevant to one participant.
    ///
    /// # Errors
    ///
    /// Returns an error when the runtime stops before replying.
    pub async fn directions(
        &self,
        participant_id: ParticipantId,
    ) -> Result<Vec<Direction>, RuntimeError> {
        self.request(|reply| Request::Directions {
            participant_id,
            reply,
        })
        .await
    }

    async fn request<T>(
        &self,
        build: impl FnOnce(oneshot::Sender<Result<T, RuntimeError>>) -> Request,
    ) -> Result<T, RuntimeError> {
        let (reply, response) = oneshot::channel();
        self.sender
            .send(build(reply))
            .await
            .map_err(|_| RuntimeError::Stopped)?;
        response.await.map_err(|_| RuntimeError::Stopped)?
    }
}

enum Request {
    Command {
        command: Command,
        reply: oneshot::Sender<Result<CommandResult, RuntimeError>>,
    },
    Snapshot {
        scope: String,
        reply: oneshot::Sender<Result<ScopeSnapshot, RuntimeError>>,
    },
    Events {
        scope: String,
        after: u64,
        reply: oneshot::Sender<Result<Vec<EventEnvelope>, RuntimeError>>,
    },
    Workspaces {
        reply: oneshot::Sender<Result<Vec<WorkspaceSummary>, RuntimeError>>,
    },
    Attention {
        reply: oneshot::Sender<Result<Vec<AttentionItem>, RuntimeError>>,
    },
    Directions {
        participant_id: ParticipantId,
        reply: oneshot::Sender<Result<Vec<Direction>, RuntimeError>>,
    },
}

async fn run_actor(
    mut receiver: mpsc::Receiver<Request>,
    mut state: CoordinationState,
    mut store: EventStore,
) {
    let mut lease_tick = tokio::time::interval(std::time::Duration::from_secs(1));
    loop {
        tokio::select! {
            _ = lease_tick.tick() => {
                if let Err(error) = expire_due(&mut store, &mut state, Utc::now()) {
                    tracing::error!(%error, "failed to persist lease expiry");
                }
            }
            request = receiver.recv() => {
                let Some(request) = request else { break };
                match request {
                    Request::Command { command, reply } => {
                        let now = Utc::now();
                        if let Err(error) = expire_due(&mut store, &mut state, now) {
                            let _ = reply.send(Err(error));
                            continue;
                        }
                        let decision = state.decide(command, &DecisionContext::at(now));
                        let result = commit_decision(&mut store, &mut state, decision, now);
                        let _ = reply.send(result);
                    }
                    Request::Snapshot { scope, reply } => {
                        let result = expire_due(&mut store, &mut state, Utc::now())
                            .map(|()| state.snapshot(&scope));
                        let _ = reply.send(result);
                    }
                    Request::Events { scope, after, reply } => {
                        let _ = reply.send(store.events(&scope, after));
                    }
                    Request::Workspaces { reply } => {
                        let result = expire_due(&mut store, &mut state, Utc::now())
                            .map(|()| state.workspaces());
                        let _ = reply.send(result);
                    }
                    Request::Attention { reply } => {
                        let result = expire_due(&mut store, &mut state, Utc::now())
                            .map(|()| state.attention());
                        let _ = reply.send(result);
                    }
                    Request::Directions { participant_id, reply } => {
                        let result = expire_due(&mut store, &mut state, Utc::now())
                            .map(|()| state.directions_for(participant_id));
                        let _ = reply.send(result);
                    }
                }
            }
        }
    }
}

fn expire_due(
    store: &mut EventStore,
    state: &mut CoordinationState,
    now: DateTime<Utc>,
) -> Result<(), RuntimeError> {
    let pending = state.expire(now);
    if pending.is_empty() {
        return Ok(());
    }
    for event in &store.append(pending, now)? {
        state.apply(event);
    }
    Ok(())
}

fn commit_decision(
    store: &mut EventStore,
    state: &mut CoordinationState,
    decision: Decision,
    now: DateTime<Utc>,
) -> Result<CommandResult, RuntimeError> {
    match decision {
        Decision::Accepted {
            events,
            participant_id,
            work_id,
        } => {
            let request_id = events.iter().find_map(|event| match &event.event {
                EventKind::SignalPublished { published } => published
                    .signal
                    .request_identity()
                    .map(|identity| identity.request_id),
                _ => None,
            });
            let committed = store.append(events, now)?;
            for event in &committed {
                state.apply(event);
            }
            Ok(CommandResult::Accepted {
                participant_id,
                work_id,
                request_id,
                event_sequences: committed.iter().map(|event| event.sequence).collect(),
            })
        }
        Decision::Conflict { events, conflicts } => {
            let committed = store.append(events, now)?;
            for event in &committed {
                state.apply(event);
            }
            Ok(CommandResult::Conflict {
                conflicts,
                event_sequences: committed.iter().map(|event| event.sequence).collect(),
            })
        }
        Decision::Rejected(reason) => Ok(CommandResult::Rejected { reason }),
    }
}

struct EventStore {
    connection: Connection,
}

impl EventStore {
    fn open(path: &Path) -> Result<Self, RuntimeError> {
        if let Some(parent) = path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
        {
            std::fs::create_dir_all(parent).map_err(|error| {
                RuntimeError::Store(rusqlite::Error::ToSqlConversionFailure(Box::new(error)))
            })?;
        }
        let connection = Connection::open(path)?;
        connection.execute_batch(
            "PRAGMA journal_mode = WAL;
             PRAGMA synchronous = NORMAL;
             CREATE TABLE IF NOT EXISTS events (
                 sequence INTEGER PRIMARY KEY AUTOINCREMENT,
                 event_id TEXT NOT NULL UNIQUE,
                 scope TEXT NOT NULL,
                 emitted_at TEXT NOT NULL,
                 event_json TEXT NOT NULL
             );
             CREATE INDEX IF NOT EXISTS events_scope_sequence
                 ON events(scope, sequence);",
        )?;
        Ok(Self { connection })
    }

    fn append(
        &mut self,
        pending: Vec<PendingEvent>,
        emitted_at: DateTime<Utc>,
    ) -> Result<Vec<EventEnvelope>, RuntimeError> {
        let transaction = self.connection.transaction()?;
        let mut committed = Vec::with_capacity(pending.len());
        for pending in pending {
            let event_id = Uuid::now_v7();
            let event_json = serde_json::to_string(&pending.event)?;
            transaction.execute(
                "INSERT INTO events(event_id, scope, emitted_at, event_json)
                 VALUES (?1, ?2, ?3, ?4)",
                params![
                    event_id.to_string(),
                    pending.scope,
                    emitted_at.to_rfc3339(),
                    event_json
                ],
            )?;
            let sequence = sequence(transaction.last_insert_rowid())?;
            committed.push(EventEnvelope {
                sequence,
                event_id,
                scope: pending.scope,
                emitted_at,
                event: pending.event,
            });
        }
        transaction.commit()?;
        Ok(committed)
    }

    fn load_all(&self) -> Result<Vec<EventEnvelope>, RuntimeError> {
        self.query_events(
            "SELECT sequence, event_id, scope, emitted_at, event_json
             FROM events ORDER BY sequence",
            params![],
        )
    }

    fn events(&self, scope: &str, after: u64) -> Result<Vec<EventEnvelope>, RuntimeError> {
        self.query_events(
            "SELECT sequence, event_id, scope, emitted_at, event_json
             FROM events WHERE scope = ?1 AND sequence > ?2
             ORDER BY sequence LIMIT 1000",
            params![scope, after],
        )
    }

    fn query_events<P: rusqlite::Params>(
        &self,
        sql: &str,
        parameters: P,
    ) -> Result<Vec<EventEnvelope>, RuntimeError> {
        let mut statement = self.connection.prepare(sql)?;
        let rows = statement.query_map(parameters, |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, String>(4)?,
            ))
        })?;
        rows.map(|row| {
            let (raw_sequence, event_id, scope, emitted_at, event_json) = row?;
            Ok(EventEnvelope {
                sequence: sequence(raw_sequence)?,
                event_id: Uuid::parse_str(&event_id).map_err(|error| {
                    RuntimeError::Serialization(serde_json::Error::io(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        error,
                    )))
                })?,
                scope,
                emitted_at: DateTime::parse_from_rfc3339(&emitted_at)
                    .map_err(|error| {
                        RuntimeError::Serialization(serde_json::Error::io(std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            error,
                        )))
                    })?
                    .with_timezone(&Utc),
                event: serde_json::from_str::<EventKind>(&event_json)?,
            })
        })
        .collect()
    }
}

fn sequence(value: i64) -> Result<u64, RuntimeError> {
    u64::try_from(value).map_err(|_| RuntimeError::InvalidSequence(value))
}
