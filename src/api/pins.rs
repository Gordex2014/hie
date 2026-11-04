//! A source's transform pin (`PUT /api/sources/{id}/pin`): `{"version": n}`
//! or `{"follow": "current"}`; answers the source. Loads that start
//! afterwards run the new pin; a running one keeps its version.

use axum::extract::{Path, State};
use axum::routing::put;
use axum::{Json, Router};
use serde::Deserialize;

use super::body::StrictJson;
use super::sources::source_status;
use super::{ApiError, ApiResult, AppState};
use crate::versions::PinChange;

pub(super) fn router() -> Router<AppState> {
    Router::new().route("/api/sources/{id}/pin", put(set_pin))
}

/// Exactly one of the two members; optional members rather than an enum, so
/// the strict reader sees any other.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct PinBody {
    #[serde(default)]
    version: Option<i64>,
    #[serde(default)]
    follow: Option<String>,
}

async fn set_pin(
    State(s): State<AppState>,
    Path(id): Path<String>,
    StrictJson(body): StrictJson<PinBody>,
) -> ApiResult {
    let change = match (body.version, body.follow.as_deref()) {
        (Some(v), None) if v >= 1 => PinChange::Version(v),
        (None, Some("current")) => PinChange::FollowCurrent,
        _ => {
            return Err(ApiError::invalid_body(
                "a pin is {\"version\": n} or {\"follow\": \"current\"}",
            ));
        }
    };
    s.versions.ready().await;
    let feed = s.manager.set_pin(&id, change)?;
    s.versions.reclaim().await;
    Ok(Json(source_status(&s, &feed)?))
}
