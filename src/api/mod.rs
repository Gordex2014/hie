//! The HTTP API. Each feature registers its own routes (`sources`, `loads`,
//! `records`, `transforms`, `monitoring`, `alerts`, `bulk`, `secrets`,
//! `admission`, `evidence`, `destination`, `pins`); `router` puts them
//! together. Everything under `/api` needs a bearer token when auth is on;
//! `/health`, `/metrics` and `/bulk` stay open.
//!
//! Shared by every route: `error` (the `{"error", "code"}` answer), `body`
//! (strict and legacy JSON bodies), `paging` (opaque cursors), and
//! [`crate::idempotency`] for the routes that create something.

mod admission;
mod alerts;
pub mod body;
mod bulk;
mod destination;
pub mod error;
mod evidence;
mod loads;
mod monitoring;
pub mod paging;
mod pins;
mod records;
mod secrets;
mod sources;
mod stats;
mod transforms;

use std::sync::Arc;

use axum::{Json, Router};
use serde_json::Value;

pub use error::ApiError;

use crate::config::Config;
use crate::db::{Db, Param};
use crate::manager::SourceManager;
use crate::versions::Versions;

#[derive(Clone)]
pub struct AppState {
    pub db: Arc<Db>,
    pub manager: Arc<SourceManager>,
    pub auth: crate::auth::Auth,
    pub started: Started,
    /// Where the engine writes the NDJSON it serves for transformed submissions.
    pub bulk_dir: std::path::PathBuf,
    /// `hie.yaml` as loaded, for the settings routes read (`admission`,
    /// `validation`, `destination`, `try`, key files).
    pub config: Arc<Config>,
    /// The transform versions loads and Try run.
    pub versions: Arc<Versions>,
    /// Database samples, kept in memory for a while (`smp_` ids).
    pub samples: Arc<crate::evidence::SampleRegistry>,
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
        .merge(secrets::router())
        .merge(admission::router())
        .merge(evidence::router())
        .merge(destination::router())
        .merge(pins::router())
        .merge(stats::router())
        .route_layer(axum::middleware::from_fn_with_state(
            state.auth.clone(),
            crate::auth::require_bearer,
        ));
    let app = Router::new()
        .merge(monitoring::open_router())
        // What HFS fetches during a $bulk-submit: the manifest, and the objects of
        // sources that cannot sign URLs. The token in the path is unguessable.
        .merge(bulk::open_router())
        .merge(api)
        .with_state(state);
    // Every answer, the framework's own included, is `{"error", "code"}`.
    error::conventions(app)
}

const DAY_MS: i64 = 86_400_000;

type ApiResult = Result<Json<Value>, ApiError>;

/// `404` with `code`, the sentence naming `what`.
fn not_found(code: &'static str, what: &str) -> ApiError {
    ApiError::not_found(code, format!("{what} not found"))
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
