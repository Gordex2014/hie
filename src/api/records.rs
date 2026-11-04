//! Records: listed, read one by one, and reprocessed (optionally corrected).

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::Deserialize;
use serde_json::{Value, json};

use super::body::LooseJson;
use super::{ApiError, ApiResult, AppState, not_found};
use crate::db::params;

pub(super) fn router() -> Router<AppState> {
    Router::new()
        .route("/api/records", get(list_records))
        .route("/api/records/{id}", get(get_record))
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
    let rows = s.db.query_json(
        &format!(
            "SELECT r.id, r.feed_id AS source_id, r.file_id, f.name AS file, r.line, r.status, r.error_kind, r.error,
                    r.attempts, {} AS updated_at
             FROM records r JOIN files f ON f.id = r.file_id
             WHERE (CAST($1 AS TEXT) IS NULL OR r.feed_id = CAST($1 AS TEXT)) AND (CAST($2 AS TEXT) IS NULL OR r.status = CAST($2 AS TEXT)) AND (CAST($3 AS BIGINT) IS NULL OR r.file_id = CAST($3 AS BIGINT))
             ORDER BY r.id DESC LIMIT $4",
            s.db.iso("r.updated_at")
        ),
        params![q.source, q.status, q.file_id, q.limit.unwrap_or(100)],
    )?;
    Ok(Json(json!({"records": rows})))
}

async fn get_record(State(s): State<AppState>, Path(id): Path<i64>) -> ApiResult {
    let row = s
        .db
        .query_json(
            &format!(
                "SELECT r.id, r.feed_id AS source_id, r.file_id, f.name AS file, f.sha256 AS file_sha256, r.line, r.status,
                        r.raw, r.original_raw, r.resources, r.response, r.error_kind, r.error, r.attempts,
                        {} AS received_at, {} AS updated_at
                 FROM records r JOIN files f ON f.id = r.file_id WHERE r.id = $1",
                s.db.iso("f.received_at"),
                s.db.iso("r.updated_at")
            ),
            params![id],
        )?
        .into_iter()
        .next()
        .ok_or_else(|| not_found("record_not_found", &format!("record {id}")))?;
    Ok(Json(row))
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
