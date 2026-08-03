use axum::{
    Extension, Json, Router,
    extract::{Path, Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post},
};
use serde::Deserialize;
use serde_json::{Value, json};
use tower_http::trace::TraceLayer;

use crate::{
    Runtime, RuntimeError,
    bootstrap::DaemonIdentity,
    protocol::{
        AttentionItem, Command, CommandResult, Direction, EventEnvelope, ParticipantId,
        ScopeSnapshot, WorkspaceSummary,
    },
};

pub fn router(runtime: Runtime, identity: DaemonIdentity) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/workspaces", get(workspaces))
        .route("/attention", get(attention))
        .route("/participants/{participant}/directions", get(directions))
        .route("/commands", post(command))
        .route("/scopes/{scope}/snapshot", get(snapshot))
        .route("/scopes/{scope}/events", get(events))
        .layer(TraceLayer::new_for_http())
        .layer(Extension(identity))
        .with_state(runtime)
}

async fn health(Extension(identity): Extension<DaemonIdentity>) -> Json<Value> {
    Json(json!({ "status": "ok", "daemon": identity }))
}

async fn workspaces(
    State(runtime): State<Runtime>,
) -> Result<Json<Vec<WorkspaceSummary>>, ApiError> {
    Ok(Json(runtime.workspaces().await?))
}

async fn attention(State(runtime): State<Runtime>) -> Result<Json<Vec<AttentionItem>>, ApiError> {
    Ok(Json(runtime.attention().await?))
}

async fn directions(
    State(runtime): State<Runtime>,
    Path(participant): Path<ParticipantId>,
) -> Result<Json<Vec<Direction>>, ApiError> {
    Ok(Json(runtime.directions(participant).await?))
}

async fn command(
    State(runtime): State<Runtime>,
    Json(command): Json<Command>,
) -> Result<Json<CommandResult>, ApiError> {
    Ok(Json(runtime.command(command).await?))
}

async fn snapshot(
    State(runtime): State<Runtime>,
    Path(scope): Path<String>,
) -> Result<Json<ScopeSnapshot>, ApiError> {
    Ok(Json(runtime.snapshot(scope).await?))
}

#[derive(Deserialize)]
struct EventsQuery {
    #[serde(default)]
    after: u64,
}

async fn events(
    State(runtime): State<Runtime>,
    Path(scope): Path<String>,
    Query(query): Query<EventsQuery>,
) -> Result<Json<Vec<EventEnvelope>>, ApiError> {
    Ok(Json(runtime.events(scope, query.after).await?))
}

struct ApiError(RuntimeError);

impl From<RuntimeError> for ApiError {
    fn from(error: RuntimeError) -> Self {
        Self(error)
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": self.0.to_string() })),
        )
            .into_response()
    }
}
