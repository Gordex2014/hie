//! Loads: one file (or object) and what became of its rows, paged; its
//! failure groups and their samples; durable retries of a group; the 0.1
//! retry; and purging expired failed content.

use axum::body::Bytes;
use axum::extract::{Path, Query, State};
use axum::http::{HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::Deserialize;
use serde_json::{Value, json};

use super::body::{LooseJson, StrictJson};
use super::paging::{self, Direction};
use super::{ApiError, ApiResult, AppState, not_found};
use crate::db::{Group, RetryRefusal};
use crate::idempotency::Idempotent;
use crate::versions::VersionRef;

pub(super) fn router() -> Router<AppState> {
    Router::new()
        .route("/api/sources/{id}/files", get(list_loads))
        .route("/api/sources/{id}/loads", get(list_loads))
        .route("/api/loads/{file_id}", get(get_load))
        .route("/api/loads/{file_id}/retry", post(legacy_retry))
        .route(
            "/api/loads/{file_id}/retries",
            post(create_retry).get(list_retries),
        )
        .route("/api/loads/{file_id}/retries/latest", get(latest_retry))
        .route("/api/loads/{file_id}/retries/{retry}", get(get_retry))
        .route("/api/loads/{file_id}/failures/{group}", get(get_group))
        .route(
            "/api/loads/{file_id}/failures/{group}/samples",
            get(group_samples),
        )
        .route("/api/failed-content/purge", post(purge_failed_content))
}

/// `Cache-Control: no-store`, for answers that carry source rows.
fn no_store(body: Value) -> Response {
    (
        [(header::CACHE_CONTROL, HeaderValue::from_static("no-store"))],
        Json(body),
    )
        .into_response()
}

/// The row count of the evidence a source was started with (`read.rows` of
/// its Test), the total of its first load's progress; `None` without one.
fn tested_total_rows(state: &AppState, source_id: &str) -> Option<i64> {
    let evidence = state.manager.evidence();
    let id = evidence.binding(source_id).ok()??.evidence?;
    evidence.get(&id).ok()?.body["read"]["rows"].as_i64()
}

/// The members of a load its view in the store cannot know: its delivery
/// method, whether it is its source's first, and the first load's total.
fn complete_load(state: &AppState, load: &mut Value, first: Option<i64>) {
    let source_id = load["source_id"].as_str().unwrap_or_default().to_string();
    let is_first = first == load["id"].as_i64();
    load["first"] = json!(is_first);
    let feed = state.manager.source(&source_id);
    load["method"] = feed
        .as_ref()
        .map(|f| {
            let method = match f.delivery.initial_method {
                Some(m) if is_first => m,
                _ => f.delivery.method,
            };
            serde_json::to_value(method).unwrap_or(Value::Null)
        })
        .unwrap_or(Value::Null);
    if is_first
        && let Some(total) = tested_total_rows(state, &source_id)
        && load["progress"]["rows_admitted"].as_i64().unwrap_or(0) <= total
    {
        load["progress"]["total_rows"] = json!(total);
    }
}

#[derive(Deserialize)]
struct ListQuery {
    limit: Option<i64>,
    cursor: Option<String>,
    around: Option<i64>,
}

/// A source's loads, newest first, paged by an opaque cursor; `around` answers
/// the page holding that load. `/files` is the 0.1 name of the same list.
async fn list_loads(
    State(s): State<AppState>,
    Path(id): Path<String>,
    Query(q): Query<ListQuery>,
) -> ApiResult {
    let list = format!("loads:source={id}");
    let request = paging::request::<i64>(&s.db, &list, q.limit, q.cursor.as_deref())?;
    let limit = request.limit;
    let views = |below: Option<i64>, above: Option<i64>, ascending: bool| {
        s.db.load_views(&id, below, above, ascending, limit + 1)
    };
    let (rows, more_older, more_newer) = match (q.around, request.position) {
        (Some(around), _) => {
            let older = views(Some(around.saturating_add(1)), None, false)?;
            let newer = views(None, Some(around), true)?;
            // Centre the load, filling from the other side at either end.
            let older_room = older.len().min(limit);
            let mut take_newer = newer.len().min((limit - 1) / 2);
            if older_room + take_newer < limit {
                take_newer = newer.len().min(limit - older_room);
            }
            let take_older = older.len().min(limit - take_newer);
            let mut rows: Vec<Value> = newer[..take_newer].iter().rev().cloned().collect();
            rows.extend(older[..take_older].iter().cloned());
            (rows, older.len() > take_older, newer.len() > take_newer)
        }
        (None, None) => {
            let mut rows = views(None, None, false)?;
            let more = rows.len() > limit;
            rows.truncate(limit);
            (rows, more, false)
        }
        (None, Some(p)) if p.dir == Direction::Next => {
            let mut rows = views(Some(p.after), None, false)?;
            let more = rows.len() > limit;
            rows.truncate(limit);
            (rows, more, true)
        }
        (None, Some(p)) => {
            let mut rows = views(None, Some(p.after), true)?;
            let more = rows.len() > limit;
            rows.truncate(limit);
            rows.reverse();
            (rows, true, more)
        }
    };
    let first = s.db.first_load_id(&id)?;
    let mut rows = rows;
    for row in &mut rows {
        complete_load(&s, row, first);
    }
    let edge = |i: Option<&Value>| i.and_then(|r| r["id"].as_i64());
    let next = match edge(rows.last()) {
        Some(last) if more_older => Some(paging::cursor(&s.db, &list, Direction::Next, &last)?),
        _ => None,
    };
    let previous = match edge(rows.first()) {
        Some(top) if more_newer => Some(paging::cursor(&s.db, &list, Direction::Previous, &top)?),
        _ => None,
    };
    Ok(Json(json!({
        "loads": rows,
        "files": rows,
        "page": paging::page(next, previous),
    })))
}

fn load_or_404(s: &AppState, load_id: i64) -> Result<Value, ApiError> {
    let mut load =
        s.db.load_view(load_id)?
            .ok_or_else(|| not_found("load_not_found", &format!("load {load_id}")))?;
    let source = load["source_id"].as_str().unwrap_or_default().to_string();
    let first = s.db.first_load_id(&source)?;
    complete_load(s, &mut load, first);
    Ok(load)
}

/// One load with its failure groups, each with up to three sample rows while
/// their content is kept.
async fn get_load(
    State(s): State<AppState>,
    Path(file_id): Path<i64>,
) -> Result<Response, ApiError> {
    let mut load = load_or_404(&s, file_id)?;
    load["failures"] = Value::Array(s.db.load_failures(file_id)?);
    Ok(no_store(load))
}

fn group_or_404(s: &AppState, load_id: i64, group: &str) -> Result<Group, ApiError> {
    if s.db.load_source(load_id)?.is_none() {
        return Err(not_found("load_not_found", &format!("load {load_id}")));
    }
    s.db.failure_group(load_id, group)?
        .ok_or_else(|| not_found("group_not_found", &format!("failure group {group}")))
}

/// One failure group with its failing records' ids, lines and attempts.
async fn get_group(
    State(s): State<AppState>,
    Path((file_id, group)): Path<(i64, String)>,
) -> Result<Response, ApiError> {
    let group = group_or_404(&s, file_id, &group)?;
    let mut body = group.to_json();
    body["records"] = Value::Array(s.db.group_records(&group, 1000)?);
    Ok(no_store(body))
}

#[derive(Deserialize)]
struct SamplesQuery {
    limit: Option<i64>,
}

/// A failure group's failed rows while their content is kept; `410` after.
async fn group_samples(
    State(s): State<AppState>,
    Path((file_id, group)): Path<(i64, String)>,
    Query(q): Query<SamplesQuery>,
) -> Result<Response, ApiError> {
    let limit = match q.limit {
        None => 3,
        Some(n) if (1..=50).contains(&n) => n as usize,
        Some(_) => {
            return Err(ApiError::invalid_body("limit must be 1 to 50").with("field", "limit"));
        }
    };
    let group = group_or_404(&s, file_id, &group)?;
    if group.content != "kept" {
        return Err(content_expired());
    }
    let samples: Vec<Value> =
        s.db.group_rows(&group, limit)?
            .into_iter()
            .map(|(record, line, raw)| json!({"record": record, "line": line, "raw": raw}))
            .collect();
    Ok(no_store(json!({
        "group": group.id,
        "samples": samples,
        "content": "kept",
    })))
}

fn content_expired() -> ApiError {
    ApiError::new(
        StatusCode::GONE,
        "content_expired",
        "the failed rows' content was purged or expired",
    )
}

// ---- retries ------------------------------------------------------------------

#[derive(Deserialize)]
struct RetryBody {
    group: String,
    #[serde(default)]
    version: Option<i64>,
}

/// The version a retry of `group` runs: the one named, or the latest saved
/// version of the group's transform now.
fn retry_version(
    s: &AppState,
    group: &Group,
    source_id: &str,
    version: Option<i64>,
) -> Result<VersionRef, ApiError> {
    let transform_id = match &group.transform {
        Some((id, _)) => id.clone(),
        None => s
            .manager
            .source(source_id)
            .map(|f| f.transform.id())
            .ok_or_else(|| {
                ApiError::conflict(
                    "transform_deleted",
                    "the load's transform is not known any more",
                )
            })?,
    };
    match version {
        Some(n) => s.versions.get(&transform_id, n)?.ok_or_else(|| {
            not_found(
                "version_not_found",
                &format!("version {n} of transform {transform_id}"),
            )
        }),
        None => s.versions.latest(&transform_id)?.ok_or_else(|| {
            ApiError::conflict(
                "transform_deleted",
                format!("transform {transform_id} was deleted"),
            )
        }),
    }
}

fn refusal(e: RetryRefusal) -> ApiError {
    match e {
        RetryRefusal::ContentExpired => content_expired(),
        RetryRefusal::InProgress(retry) => {
            ApiError::conflict("retry_in_progress", "an unfinished retry covers these rows")
                .with("retry", retry)
        }
        RetryRefusal::NothingToRetry => {
            ApiError::conflict("nothing_to_retry", "the group has no failing rows")
        }
        RetryRefusal::Store(e) => e.into(),
    }
}

/// `POST /api/loads/{id}/retries` (key required): a durable retry of one
/// failure group, running the version named or the latest saved one.
async fn create_retry(
    State(s): State<AppState>,
    Path(file_id): Path<i64>,
    request: Idempotent,
) -> Result<Response, ApiError> {
    let db = s.db.clone();
    request
        .run(&db, true, move |_attempt, body: Bytes| async move {
            let RetryBody { group, version } = super::body::strict(&body)?;
            let source_id =
                s.db.load_source(file_id)?
                    .ok_or_else(|| not_found("load_not_found", &format!("load {file_id}")))?;
            let group = group_or_404(&s, file_id, &group)?;
            let version = retry_version(&s, &group, &source_id, version)?;
            let retry = s.db.create_retry(&group, &version).map_err(refusal)?;
            let _ = s.manager.check_now(&source_id);
            Ok((StatusCode::ACCEPTED, json!({"retry": retry})))
        })
        .await
}

async fn list_retries(State(s): State<AppState>, Path(file_id): Path<i64>) -> ApiResult {
    if s.db.load_source(file_id)?.is_none() {
        return Err(not_found("load_not_found", &format!("load {file_id}")));
    }
    Ok(Json(json!({"retries": s.db.retries(file_id)?})))
}

async fn latest_retry(State(s): State<AppState>, Path(file_id): Path<i64>) -> ApiResult {
    if s.db.load_source(file_id)?.is_none() {
        return Err(not_found("load_not_found", &format!("load {file_id}")));
    }
    let retry =
        s.db.retries(file_id)?
            .into_iter()
            .next()
            .ok_or_else(|| not_found("retry_not_found", "a retry of this load"))?;
    Ok(Json(json!({"retry": retry})))
}

async fn get_retry(
    State(s): State<AppState>,
    Path((file_id, retry)): Path<(i64, String)>,
) -> ApiResult {
    if s.db.load_source(file_id)?.is_none() {
        return Err(not_found("load_not_found", &format!("load {file_id}")));
    }
    let retry =
        s.db.retry(file_id, &retry)?
            .ok_or_else(|| not_found("retry_not_found", &format!("retry {retry}")))?;
    Ok(Json(json!({"retry": retry})))
}

#[derive(Deserialize, Default)]
struct LegacyRetryBody {
    /// Retry only the groups whose rows failed with exactly this reason.
    reason: Option<String>,
}

/// The 0.1 retry: one retry with the latest saved version for each failure
/// group (with `reason`, when given), skipping groups an unfinished retry
/// covers. Answers the 0.1 body plus the retries' ids.
async fn legacy_retry(
    State(s): State<AppState>,
    Path(file_id): Path<i64>,
    body: Option<LooseJson<LegacyRetryBody>>,
) -> Result<(StatusCode, Json<Value>), ApiError> {
    let reason = body.and_then(|LooseJson(b)| b.reason);
    let source_id =
        s.db.load_source(file_id)?
            .ok_or_else(|| not_found("load_not_found", &format!("load {file_id}")))?;
    let mut retried = 0;
    let mut retries = Vec::new();
    for group in s.db.groups_to_retry(file_id, reason.as_deref())? {
        let Ok(version) = retry_version(&s, &group, &source_id, None) else {
            continue;
        };
        match s.db.create_retry(&group, &version) {
            Ok(retry) => {
                retried += retry["rows"].as_i64().unwrap_or(0);
                retries.push(retry["id"].clone());
            }
            Err(RetryRefusal::Store(e)) => return Err(e.into()),
            Err(_) => {}
        }
    }
    if retried > 0 {
        let _ = s.manager.check_now(&source_id);
    }
    Ok((
        StatusCode::ACCEPTED,
        Json(json!({"load": file_id, "retried": retried, "reason": reason, "retries": retries})),
    ))
}

// ---- failed content -------------------------------------------------------------

#[derive(Deserialize)]
struct PurgeBody {
    #[serde(default)]
    before: Option<String>,
}

/// Purges expired failed content now; `before` also expires the content of
/// rows that failed before that instant.
async fn purge_failed_content(
    State(s): State<AppState>,
    body: Option<StrictJson<PurgeBody>>,
) -> ApiResult {
    let before = match body.and_then(|StrictJson(b)| b.before) {
        None => None,
        Some(text) => Some(
            chrono::DateTime::parse_from_rfc3339(&text)
                .map_err(|_| {
                    ApiError::invalid_body("before must be an RFC 3339 time")
                        .with("field", "before")
                })?
                .timestamp_millis(),
        ),
    };
    let purged = crate::purge::expired(&s.db, s.config.retention.failed_content, before)?;
    Ok(Json(json!({"purged": purged})))
}
