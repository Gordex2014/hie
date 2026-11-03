//! The HTTP API. Each feature registers its own routes (`sources`, `loads`,
//! `records`, `transforms`, `monitoring`, `alerts`, `bulk`); `router` puts them
//! together. Everything under `/api` needs a bearer token when auth is on;
//! `/health`, `/metrics` and `/bulk` stay open.

mod alerts;
mod bulk;
mod loads;
mod monitoring;
mod records;
mod sources;
mod transforms;

use std::sync::Arc;

use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::{Json, Router};
use serde_json::{Value, json};

use crate::db::{Db, Param};
use crate::manager::{ManagerError, SourceManager};

#[derive(Clone)]
pub struct AppState {
    pub db: Arc<Db>,
    pub manager: Arc<SourceManager>,
    pub auth: crate::auth::Auth,
    pub started: Started,
    /// Where the engine writes the NDJSON it serves for transformed submissions.
    pub bulk_dir: std::path::PathBuf,
}

/// When the process came up, for the uptime that `/health` reports.
#[derive(Clone, Copy)]
pub struct Started {
    at: chrono::DateTime<chrono::Utc>,
    instant: std::time::Instant,
}

impl Started {
    pub fn now() -> Self {
        Started {
            at: chrono::Utc::now(),
            instant: std::time::Instant::now(),
        }
    }
}

/// Everything under `/api` needs a bearer token when auth is on; `/health` and
/// `/metrics` stay open for probes and the metrics scraper.
pub fn router(state: AppState) -> Router {
    let api = Router::new()
        .merge(sources::router())
        .merge(loads::router())
        .merge(transforms::router())
        .merge(monitoring::router())
        .merge(records::router())
        .merge(alerts::router())
        .route_layer(axum::middleware::from_fn_with_state(
            state.auth.clone(),
            crate::auth::require_bearer,
        ));
    Router::new()
        .merge(monitoring::open_router())
        // What HFS fetches during a $bulk-submit: the manifest, and the objects of
        // sources that cannot sign URLs. The token in the path is unguessable.
        .merge(bulk::open_router())
        .merge(api)
        .with_state(state)
}

const DAY_MS: i64 = 86_400_000;

struct ApiError(StatusCode, String);

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (self.0, Json(json!({"error": self.1}))).into_response()
    }
}

impl From<anyhow::Error> for ApiError {
    fn from(e: anyhow::Error) -> Self {
        tracing::error!("API request failed: {e:#}");
        ApiError(StatusCode::INTERNAL_SERVER_ERROR, format!("{e:#}"))
    }
}

impl From<ManagerError> for ApiError {
    fn from(e: ManagerError) -> Self {
        match e {
            ManagerError::Invalid(m) => ApiError(StatusCode::BAD_REQUEST, m),
            ManagerError::NotFound(m) => ApiError(StatusCode::NOT_FOUND, m),
            ManagerError::Conflict(m) => ApiError(StatusCode::CONFLICT, m),
            ManagerError::Internal(e) => e.into(),
        }
    }
}

type ApiResult = Result<Json<Value>, ApiError>;

fn not_found(what: &str) -> ApiError {
    ApiError(StatusCode::NOT_FOUND, format!("{what} not found"))
}

fn iso_ms(ms: i64) -> Option<String> {
    chrono::DateTime::from_timestamp_millis(ms).map(|t| t.to_rfc3339())
}

fn count(db: &Db, sql: &str, p: impl Into<Vec<Param>>) -> anyhow::Result<i64> {
    Ok(db
        .query_json(sql, p)?
        .first()
        .and_then(|r| r.get("n"))
        .and_then(Value::as_i64)
        .unwrap_or(0))
}
