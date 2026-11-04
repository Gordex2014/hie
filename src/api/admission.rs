//! The admission policy as configured (`GET /api/admission`).

use axum::extract::State;
use axum::routing::get;
use axum::{Json, Router};

use super::{ApiResult, AppState};

pub(super) fn router() -> Router<AppState> {
    Router::new().route("/api/admission", get(policy))
}

async fn policy(State(s): State<AppState>) -> ApiResult {
    Ok(Json(s.manager.admission().view()))
}
