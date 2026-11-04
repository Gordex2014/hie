//! Destination facts (`GET /api/destination`): the destination's `/metadata`,
//! read at most once a minute, with its display name and the validation mode.

use axum::extract::State;
use axum::routing::get;
use axum::{Json, Router};

use super::{ApiResult, AppState};

pub(super) fn router() -> Router<AppState> {
    Router::new().route("/api/destination", get(facts))
}

async fn facts(State(s): State<AppState>) -> ApiResult {
    Ok(Json(s.manager.destination().await))
}
