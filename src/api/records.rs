//! Records: listed with their outcome, read one by one (`no-store`: they
//! carry source rows), their immutable history, and reprocessed (optionally
//! corrected).

use axum::extract::{Path, Query, State};
use axum::http::{HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::Deserialize;
use serde_json::{Value, json};

use super::body::LooseJson;
use super::{ApiError, ApiResult, AppState, not_found};

pub(super) fn router() -> Router<AppState> {
    Router::new()
        .route("/api/records", get(list_records))
        .route("/api/records/{id}", get(get_record))
        .route("/api/records/{id}/history", get(record_history))
        .route("/api/records/{id}/reprocess", post(reprocess))
}

#[derive(Deserialize)]
struct RecordQuery {
    source: Option<String>,
    status: Option<String>,
    file_id: Option<i64>,
    limit: Option<i64>,
}

async fn list_records(State(s): State<AppState>, Query(q): Query<RecordQuery>) -> ApiResult {
    let rows = s.db.record_list(
        q.source.as_deref(),
        q.status.as_deref(),
        q.file_id,
        q.limit.unwrap_or(100),
    )?;
    Ok(Json(json!({"records": rows})))
}

async fn get_record(State(s): State<AppState>, Path(id): Path<i64>) -> Result<Response, ApiError> {
    let mut row =
        s.db.record_view(id)?
            .ok_or_else(|| not_found("record_not_found", &format!("record {id}")))?;
    let state = row
        .as_object_mut()
        .and_then(|r| r.remove("content_state"))
        .and_then(|v| v.as_str().map(str::to_string));
    row["content"] = json!(state.as_deref().unwrap_or("kept"));
    Ok((
        [(header::CACHE_CONTROL, HeaderValue::from_static("no-store"))],
        Json(row),
    )
        .into_response())
}

/// A record's immutable attempts, oldest first.
async fn record_history(State(s): State<AppState>, Path(id): Path<i64>) -> ApiResult {
    s.db.record_history(id)?
        .map(Json)
        .ok_or_else(|| not_found("record_not_found", &format!("record {id}")))
}

#[derive(Deserialize, Default)]
struct ReprocessBody {
    raw: Option<Value>,
}

async fn reprocess(
    State(s): State<AppState>,
    Path(id): Path<i64>,
    body: Option<LooseJson<ReprocessBody>>,
) -> Result<(StatusCode, Json<Value>), ApiError> {
    let raw = body.and_then(|LooseJson(b)| b.raw);
    if !s.db.reprocess(id, raw.as_ref())? {
        return Err(not_found("record_not_found", &format!("record {id}")));
    }
    Ok((
        StatusCode::ACCEPTED,
        Json(json!({"id": id, "status": "received", "corrected": raw.is_some()})),
    ))
}
