//! Loads: one file (or object) and what became of its rows, its failures by
//! reason, and retrying them.

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
        .route("/api/sources/{id}/files", get(list_files))
        .route("/api/sources/{id}/loads", get(list_files))
        .route("/api/loads/{file_id}", get(get_load))
        .route("/api/loads/{file_id}/retry", post(retry_load))
}

#[derive(Deserialize)]
struct Limit {
    limit: Option<i64>,
}

const LOAD_COLUMNS: &str = "f.id, f.feed_id AS source_id, f.name, f.status, f.duplicate_of, f.size, f.error, f.source_key, f.archived,
                    COUNT(r.id) AS records,
                    CAST(COALESCE(SUM(CASE WHEN r.status = 'sent' THEN 1 ELSE 0 END), 0) AS BIGINT) AS sent,
                    CAST(COALESCE(SUM(CASE WHEN r.status = 'rejected' THEN 1 ELSE 0 END), 0) AS BIGINT) AS rejected,
                    CAST(COALESCE(SUM(CASE WHEN r.status IN ('received', 'pending', 'submitted') THEN 1 ELSE 0 END), 0) AS BIGINT) AS in_progress,
                    CASE WHEN f.finished_at IS NULL THEN NULL ELSE f.finished_at - f.received_at END AS took_ms";

/// A load is one file (or object) and what became of its rows. The same rows serve
/// `/files` and `/loads`; the UI calls them loads.
async fn list_files(
    State(s): State<AppState>,
    Path(id): Path<String>,
    Query(q): Query<Limit>,
) -> ApiResult {
    let method = s
        .manager
        .source(&id)
        .map(|f| serde_json::to_value(f.delivery.method).unwrap_or(Value::Null))
        .unwrap_or(Value::Null);
    let mut rows = s.db.query_json(
        &format!(
            "SELECT {LOAD_COLUMNS}, {} AS received_at, {} AS finished_at
             FROM files f LEFT JOIN records r ON r.file_id = f.id
             WHERE f.feed_id = $1 GROUP BY f.id, f.feed_id, f.name, f.status, f.duplicate_of, f.size, f.error, f.source_key, f.archived, f.finished_at, f.received_at ORDER BY f.id DESC LIMIT $2",
            s.db.iso("f.received_at"),
            s.db.iso("f.finished_at")
        ),
        params![id, q.limit.unwrap_or(50)],
    )?;
    for row in &mut rows {
        row["method"] = method.clone();
    }
    Ok(Json(json!({"files": rows, "loads": rows})))
}

/// One load with its failures grouped by reason, each with sample rows, so a
/// person can see what went wrong and retry the rows after a fix.
async fn get_load(State(s): State<AppState>, Path(file_id): Path<i64>) -> ApiResult {
    let mut load =
        s.db.query_json(
            &format!(
                "SELECT {LOAD_COLUMNS}, {} AS received_at, {} AS finished_at
                 FROM files f LEFT JOIN records r ON r.file_id = f.id
                 WHERE f.id = $1 GROUP BY f.id, f.feed_id, f.name, f.status, f.duplicate_of, f.size, f.error, f.source_key, f.archived, f.finished_at, f.received_at",
                s.db.iso("f.received_at"),
                s.db.iso("f.finished_at")
            ),
            params![file_id],
        )?
        .into_iter()
        .next()
        .ok_or_else(|| not_found("load_not_found", &format!("load {file_id}")))?;
    let source_id = load["source_id"].as_str().unwrap_or_default().to_string();
    load["method"] = s
        .manager
        .source(&source_id)
        .map(|f| serde_json::to_value(f.delivery.method).unwrap_or(Value::Null))
        .unwrap_or(Value::Null);
    load["failures"] = Value::Array(s.db.failure_groups(file_id)?);
    Ok(Json(load))
}

#[derive(Deserialize, Default)]
struct RetryBody {
    /// Retry only the rows rejected with exactly this reason.
    reason: Option<String>,
}

/// Puts a load's rejected rows back at the start: all of them, or those with one reason.
async fn retry_load(
    State(s): State<AppState>,
    Path(file_id): Path<i64>,
    body: Option<LooseJson<RetryBody>>,
) -> Result<(StatusCode, Json<Value>), ApiError> {
    let reason = body.and_then(|LooseJson(b)| b.reason);
    let source_id =
        s.db.query_json("SELECT feed_id FROM files WHERE id = $1", params![file_id])?
            .into_iter()
            .next()
            .and_then(|r| r["feed_id"].as_str().map(str::to_string))
            .ok_or_else(|| not_found("load_not_found", &format!("load {file_id}")))?;
    let retried = s.db.reprocess_rejected(file_id, reason.as_deref())?;
    if retried > 0 {
        let _ = s.manager.check_now(&source_id);
    }
    Ok((
        StatusCode::ACCEPTED,
        Json(json!({"load": file_id, "retried": retried, "reason": reason})),
    ))
}
