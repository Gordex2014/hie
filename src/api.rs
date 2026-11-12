use std::sync::Arc;

use crate::db::{Param, params};
use axum::extract::{Path, Query, State};
use axum::http::{StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::Deserialize;
use serde_json::{Map, Value, json};

use crate::config::FeedConfig;
use crate::db::{Db, Rule, now_ms};
use crate::manager::{ManagerError, SourceManager};
use crate::pipeline::human;

#[derive(Clone)]
pub struct AppState {
    pub db: Arc<Db>,
    pub manager: Arc<SourceManager>,
    pub auth: crate::auth::Auth,
    pub started: Started,
    /// Where the engine writes the NDJSON it serves for transformed submissions.
    pub bulk_dir: std::path::PathBuf,
    /// Connector credentials, or why there are none.
    pub secrets: crate::secrets::Secrets,
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
        .route("/api/sources", get(list_sources).post(create_source))
        .route("/api/sources/test", post(test_source))
        .route(
            "/api/sources/{id}",
            get(get_source).put(replace_source).delete(delete_source),
        )
        .route("/api/sources/{id}/pause", post(pause_source))
        .route("/api/sources/{id}/resume", post(resume_source))
        .route("/api/sources/{id}/check-now", post(check_now))
        .route("/api/sources/{id}/files", get(list_files))
        .route("/api/sources/{id}/loads", get(list_files))
        .route("/api/loads/{file_id}", get(get_load))
        .route("/api/loads/{file_id}/retry", post(retry_load))
        .route("/api/sources/{id}/stats", get(source_stats))
        .route("/api/transforms", get(list_transforms))
        .route(
            "/api/transforms/{name}",
            get(get_transform).put(put_transform),
        )
        .route("/api/transforms/{name}/try", post(try_transform))
        .route("/api/monitoring/status", get(monitoring_status))
        .route("/api/records", get(list_records))
        .route("/api/records/{id}", get(get_record))
        .route("/api/records/{id}/reprocess", post(reprocess))
        .route("/api/alerts", get(list_alerts))
        .route("/api/alert-rules", get(list_rules).post(create_rule))
        .route(
            "/api/alert-rules/{id}",
            get(get_rule).put(replace_rule).delete(delete_rule),
        )
        .route_layer(axum::middleware::from_fn_with_state(
            state.auth.clone(),
            crate::auth::require_bearer,
        ));
    Router::new()
        .route("/health", get(health))
        .route("/metrics", get(metrics))
        // What HFS fetches during a $bulk-submit: the manifest, and the files the
        // engine holds itself (written, read from disk or unpacked by it). A store
        // object is never served here. The token in the path is unguessable.
        .route("/bulk/{token}/manifest.json", get(bulk_manifest))
        .route("/bulk/{token}/files/{file_id}", get(bulk_file))
        .merge(api)
        .with_state(state)
}

async fn health(State(s): State<AppState>) -> Json<Value> {
    Json(json!({
        "status": "ok",
        "service": "hie",
        "version": env!("CARGO_PKG_VERSION"),
        "uptime_seconds": s.started.instant.elapsed().as_secs_f64(),
        "started_at": s.started.at.to_rfc3339(),
        "timestamp": chrono::Utc::now().to_rfc3339(),
        "secrets": s.secrets.health(),
    }))
}

async fn bulk_manifest(State(s): State<AppState>, Path(token): Path<String>) -> ApiResult {
    let submission =
        s.db.submission(&token)?
            .ok_or_else(|| not_found("submission"))?;
    Ok(Json(submission.manifest))
}

async fn bulk_file(
    State(s): State<AppState>,
    Path((token, file_id)): Path<(String, i64)>,
) -> Result<Response, ApiError> {
    tracing::debug!(file_id, "bulk file request");
    let submission =
        s.db.submission(&token)?
            .ok_or_else(|| not_found("submission"))?;
    // A file the engine wrote for a transformed submission, or else a kept file
    // the engine itself holds: one its source reads from disk, or an entry it
    // unpacked. Where a file lives is what was recorded when it was taken, never
    // a guess from its key.
    let engine_file = s.bulk_dir.join(&token).join(format!("{file_id}.ndjson"));
    let path = if engine_file.is_file() {
        engine_file
    } else {
        let (key, origin) =
            s.db.submission_file(&token, file_id)?
                .ok_or_else(|| not_found("file"))?;
        let path = std::path::PathBuf::from(&key);
        let served = match origin.as_deref() {
            Some("local") => true,
            // Staged entries live in the engine's bulk directory, nowhere else.
            Some("staged") => crate::bulk::is_within(&path, &s.bulk_dir),
            // A store object, or a file of unknown origin: read from its store.
            _ => false,
        };
        if !served {
            tracing::warn!(feed = %submission.feed_id, file_id, "refused: a store object is fetched from its store, never from the engine");
            return Err(not_found("file"));
        }
        path
    };
    let bytes = tokio::fs::read(&path)
        .await
        .map_err(|_| not_found("file"))?;
    tracing::debug!(feed = %submission.feed_id, file_id, "bulk file served");
    Ok(([(header::CONTENT_TYPE, "application/fhir+ndjson")], bytes).into_response())
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
            ManagerError::Busy(m) => ApiError(StatusCode::SERVICE_UNAVAILABLE, m),
            ManagerError::InvalidDefinition(m) => ApiError(StatusCode::BAD_REQUEST, m),
            ManagerError::StoreBusy => ApiError(
                StatusCode::SERVICE_UNAVAILABLE,
                "the store is busy; try again shortly".into(),
            ),
            ManagerError::NotStarted(m) => ApiError(StatusCode::INTERNAL_SERVER_ERROR, m),
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

/// A source's definition and its state, as one object.
fn source_status(s: &AppState, feed: &FeedConfig) -> anyhow::Result<Value> {
    let db = &*s.db;
    let last = db.last_received_at(&feed.id)?;
    let paused = db.paused_state(&feed.id)?;
    let lease = db.lease(&feed.id)?;
    let started = s
        .manager
        .runner(&feed.id)
        .map(|r| r.started_at())
        .unwrap_or_else(now_ms);
    let freshness = match (feed.expect_every, last) {
        (None, _) => "not_monitored",
        (Some(w), _) if now_ms() - last.unwrap_or(started) > w.as_millis() as i64 => "silent",
        (Some(_), None) => "no_data_yet",
        _ => "ok",
    };
    let state = match (&paused, freshness) {
        (Some(_), _) => "paused",
        (None, "silent") => "late",
        (None, "no_data_yet") => "waiting",
        _ => "receiving",
    };
    let last_alert = db.query_json(
        &format!("SELECT kind, message, {} AS created_at FROM alerts WHERE feed_id = $1 ORDER BY id DESC LIMIT 1", db.iso("created_at")),
        params![feed.id],
    )?;
    let since = now_ms() - DAY_MS;
    let rejected_24h = count(
        db,
        "SELECT COUNT(*) AS n FROM records WHERE feed_id = $1 AND status = 'rejected' AND updated_at >= $2",
        params![feed.id, since],
    )?;
    Ok(json!({
        "id": feed.id,
        "name": feed.name,
        "source": feed.source.describe(),
        "format": feed.format,
        "transform": feed.transform.display().to_string(),
        "poll_seconds": feed.source.poll_interval().as_secs(),
        "expect_every": feed.expect_every.map(human),
        "state": if state != "paused" && lease.as_ref().is_some_and(|l| l.owner != crate::pipeline::instance_id()) { "standby" } else { state },
        "freshness": freshness,
        "paused": paused.map(|(at, by)| json!({"since": iso_ms(at), "by": by})),
        "lease": lease.map(|l| json!({"owner": l.owner, "until": iso_ms(l.until), "mine": l.owner == crate::pipeline::instance_id()})),
        "instance": crate::pipeline::instance_id(),
        "last_received_at": last.and_then(iso_ms),
        "records": db.feed_counts(&feed.id)?,
        "rejected_last_24h": rejected_24h,
        "last_alert": last_alert.into_iter().next(),
        "definition": s.manager.definition(&feed.id)?,
    }))
}

async fn list_sources(State(s): State<AppState>) -> ApiResult {
    let sources: anyhow::Result<Vec<Value>> = s
        .manager
        .sources()
        .iter()
        .map(|f| source_status(&s, f))
        .collect();
    Ok(Json(json!({"sources": sources?})))
}

async fn get_source(State(s): State<AppState>, Path(id): Path<String>) -> ApiResult {
    let feed = s
        .manager
        .source(&id)
        .ok_or_else(|| not_found(&format!("source {id}")))?;
    Ok(Json(source_status(&s, &feed)?))
}

async fn create_source(
    State(s): State<AppState>,
    principal: Option<axum::Extension<Arc<helios_auth::Principal>>>,
    Json(body): Json<Value>,
) -> Result<(StatusCode, Json<Value>), ApiError> {
    let by = principal.map(|axum::Extension(p)| p.subject().to_string());
    let feed = s.manager.create(&body, by.as_deref()).await?;
    Ok((StatusCode::CREATED, Json(source_status(&s, &feed)?)))
}

async fn replace_source(
    State(s): State<AppState>,
    Path(id): Path<String>,
    Json(body): Json<Value>,
) -> ApiResult {
    let feed = s.manager.replace(&id, &body).await?;
    Ok(Json(source_status(&s, &feed)?))
}

async fn delete_source(
    State(s): State<AppState>,
    Path(id): Path<String>,
) -> Result<StatusCode, ApiError> {
    s.manager.delete(&id).await?;
    Ok(StatusCode::NO_CONTENT)
}

#[derive(Deserialize, Default)]
struct PauseBody {
    by: Option<String>,
}

async fn pause_source(
    State(s): State<AppState>,
    Path(id): Path<String>,
    principal: Option<axum::Extension<Arc<helios_auth::Principal>>>,
    body: Option<Json<PauseBody>>,
) -> ApiResult {
    // Who paused it: the body's name, else the authenticated subject.
    let by = body
        .and_then(|Json(b)| b.by)
        .or_else(|| principal.map(|axum::Extension(p)| p.subject().to_string()));
    s.manager.pause(&id, by.as_deref())?;
    get_source(State(s), Path(id)).await
}

async fn resume_source(State(s): State<AppState>, Path(id): Path<String>) -> ApiResult {
    s.manager.resume(&id)?;
    get_source(State(s), Path(id)).await
}

async fn check_now(
    State(s): State<AppState>,
    Path(id): Path<String>,
) -> Result<(StatusCode, Json<Value>), ApiError> {
    s.manager.check_now(&id)?;
    Ok((
        StatusCode::ACCEPTED,
        Json(json!({"id": id, "checking": true})),
    ))
}

async fn test_source(State(s): State<AppState>, Json(body): Json<Value>) -> ApiResult {
    let probe = s.manager.test(&body).await?;
    Ok(Json(
        serde_json::to_value(probe).map_err(anyhow::Error::from)?,
    ))
}

#[derive(Deserialize)]
struct StatsQuery {
    days: Option<i64>,
}

/// What a source's page draws: today's numbers and one row per day for the last N days.
async fn source_stats(
    State(s): State<AppState>,
    Path(id): Path<String>,
    Query(q): Query<StatsQuery>,
) -> ApiResult {
    s.manager
        .source(&id)
        .ok_or_else(|| not_found(&format!("source {id}")))?;
    let days = q.days.unwrap_or(14).clamp(1, 366);
    let today = chrono::Utc::now().date_naive();
    let first = today - chrono::Days::new(days as u64 - 1);
    let since = first
        .and_hms_opt(0, 0, 0)
        .unwrap()
        .and_utc()
        .timestamp_millis();

    let mut by_day: Map<String, Value> = (0..days)
        .map(|i| {
            let day = (first + chrono::Days::new(i as u64)).to_string();
            (day, json!({"files": 0, "sent": 0, "rejected": 0}))
        })
        .collect();
    for row in s.db.query_json(
        &format!(
            "SELECT {} AS day, COUNT(*) AS n FROM files
             WHERE feed_id = $1 AND status <> 'duplicate' AND received_at >= $2 GROUP BY 1",
            s.db.day("received_at")
        ),
        params![id, since],
    )? {
        if let Some(entry) = row["day"].as_str().and_then(|d| by_day.get_mut(d)) {
            entry["files"] = row["n"].clone();
        }
    }
    for row in s.db.query_json(
        &format!(
            "SELECT {} AS day, status, COUNT(*) AS n FROM records
             WHERE feed_id = $1 AND status IN ('sent', 'rejected') AND updated_at >= $2 GROUP BY 1, status",
            s.db.day("updated_at")
        ),
        params![id, since],
    )? {
        if let (Some(entry), Some(status)) = (
            row["day"].as_str().and_then(|d| by_day.get_mut(d)),
            row["status"].as_str(),
        ) {
            entry[status] = row["n"].clone();
        }
    }
    let rows: Vec<Value> = by_day
        .into_iter()
        .map(|(day, v)| json!({"day": day, "files": v["files"], "sent": v["sent"], "rejected": v["rejected"]}))
        .collect();

    let today_since = today
        .and_hms_opt(0, 0, 0)
        .unwrap()
        .and_utc()
        .timestamp_millis();
    let today_json = json!({
        "files": count(&s.db, "SELECT COUNT(*) AS n FROM files WHERE feed_id = $1 AND status <> 'duplicate' AND received_at >= $2", params![id, today_since])?,
        "records": count(&s.db, "SELECT COUNT(r.id) AS n FROM records r JOIN files f ON f.id = r.file_id WHERE r.feed_id = $1 AND f.received_at >= $2", params![id, today_since])?,
        "sent": count(&s.db, "SELECT COUNT(*) AS n FROM records WHERE feed_id = $1 AND status = 'sent' AND updated_at >= $2", params![id, today_since])?,
        "rejected": count(&s.db, "SELECT COUNT(*) AS n FROM records WHERE feed_id = $1 AND status = 'rejected' AND updated_at >= $2", params![id, today_since])?,
        "pending": count(&s.db, "SELECT COUNT(*) AS n FROM records WHERE feed_id = $1 AND status IN ('received', 'pending')", params![id])?,
    });
    Ok(Json(
        json!({"source": id, "days": days, "today": today_json, "by_day": rows}),
    ))
}

async fn list_transforms(State(s): State<AppState>) -> ApiResult {
    Ok(Json(json!({"transforms": s.manager.transforms()?})))
}

async fn get_transform(State(s): State<AppState>, Path(name): Path<String>) -> ApiResult {
    let code = s
        .manager
        .transform_code(&name)?
        .ok_or_else(|| not_found(&format!("transform {name}")))?;
    Ok(Json(json!({"name": name, "code": code})))
}

#[derive(Deserialize)]
struct TransformBody {
    code: String,
}

async fn put_transform(
    State(s): State<AppState>,
    Path(name): Path<String>,
    Json(body): Json<TransformBody>,
) -> Result<(StatusCode, Json<Value>), ApiError> {
    let existed = s.manager.save_transform(&name, &body.code).await?;
    let status = if existed {
        StatusCode::OK
    } else {
        StatusCode::CREATED
    };
    Ok((status, Json(json!({"name": name, "saved": true}))))
}

#[derive(Deserialize)]
struct TryBody {
    records: Vec<Value>,
}

/// Runs a transform on the supplied records without loading anything.
async fn try_transform(
    State(s): State<AppState>,
    Path(name): Path<String>,
    Json(body): Json<TryBody>,
) -> ApiResult {
    if body.records.len() > 100 {
        return Err(ApiError(
            StatusCode::BAD_REQUEST,
            "try at most 100 records at a time".into(),
        ));
    }
    let results = s.manager.try_transform(&name, &body.records).await?;
    Ok(Json(json!({"name": name, "results": results})))
}

/// What the monitoring page opens on: what needs a person, the last 24 hours, every source.
async fn monitoring_status(State(s): State<AppState>) -> ApiResult {
    let mut sources = Vec::new();
    let mut needs_you = Vec::new();
    for feed in s.manager.sources() {
        let status = source_status(&s, &feed)?;
        let rejected = status["rejected_last_24h"].as_i64().unwrap_or(0);
        let reason = match status["state"].as_str() {
            Some("late") => Some(format!(
                "nothing received since {}, expected at least every {}",
                status["last_received_at"].as_str().unwrap_or("the start"),
                status["expect_every"].as_str().unwrap_or("?")
            )),
            Some("paused") => Some(format!(
                "paused{} since {}",
                status["paused"]["by"]
                    .as_str()
                    .map(|b| format!(" by {b}"))
                    .unwrap_or_default(),
                status["paused"]["since"].as_str().unwrap_or("?")
            )),
            _ if rejected > 0 => Some(format!("{rejected} records rejected in the last 24 hours")),
            _ => None,
        };
        if let Some(reason) = reason {
            needs_you.push(json!({"id": feed.id, "name": feed.name, "state": status["state"], "reason": reason}));
        }
        sources.push(status);
    }
    let since = now_ms() - DAY_MS;
    let last_24h = json!({
        "files": count(&s.db, "SELECT COUNT(*) AS n FROM files WHERE status <> 'duplicate' AND received_at >= $1", params![since])?,
        "sent": count(&s.db, "SELECT COUNT(*) AS n FROM records WHERE status = 'sent' AND updated_at >= $1", params![since])?,
        "rejected": count(&s.db, "SELECT COUNT(*) AS n FROM records WHERE status = 'rejected' AND updated_at >= $1", params![since])?,
        "alerts": count(&s.db, "SELECT COUNT(*) AS n FROM alerts WHERE created_at >= $1", params![since])?,
    });
    Ok(Json(json!({
        "state": if needs_you.is_empty() { "all_receiving" } else { "needs_you" },
        "needs_you": needs_you,
        "last_24h": last_24h,
        "sources": sources,
    })))
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
        .ok_or_else(|| not_found(&format!("load {file_id}")))?;
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
    body: Option<Json<RetryBody>>,
) -> Result<(StatusCode, Json<Value>), ApiError> {
    let reason = body.and_then(|Json(b)| b.reason);
    let source_id =
        s.db.query_json("SELECT feed_id FROM files WHERE id = $1", params![file_id])?
            .into_iter()
            .next()
            .and_then(|r| r["feed_id"].as_str().map(str::to_string))
            .ok_or_else(|| not_found(&format!("load {file_id}")))?;
    let retried = s.db.reprocess_rejected(file_id, reason.as_deref())?;
    if retried > 0 {
        let _ = s.manager.check_now(&source_id);
    }
    Ok((
        StatusCode::ACCEPTED,
        Json(json!({"load": file_id, "retried": retried, "reason": reason})),
    ))
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
        .ok_or_else(|| not_found(&format!("record {id}")))?;
    Ok(Json(row))
}

#[derive(Deserialize, Default)]
struct ReprocessBody {
    raw: Option<Value>,
}

async fn reprocess(
    State(s): State<AppState>,
    Path(id): Path<i64>,
    body: Option<Json<ReprocessBody>>,
) -> Result<(StatusCode, Json<Value>), ApiError> {
    let raw = body.and_then(|Json(b)| b.raw);
    if !s.db.reprocess(id, raw.as_ref())? {
        return Err(not_found(&format!("record {id}")));
    }
    Ok((
        StatusCode::ACCEPTED,
        Json(json!({"id": id, "status": "received", "corrected": raw.is_some()})),
    ))
}

#[derive(Deserialize)]
struct AlertQuery {
    source: Option<String>,
    limit: Option<i64>,
    /// `true` lists only alerts that have not cleared.
    open: Option<bool>,
}

async fn list_alerts(State(s): State<AppState>, Query(q): Query<AlertQuery>) -> ApiResult {
    let rows = s.db.query_json(
        &format!(
            "SELECT id, feed_id AS source_id, kind, message, rule_id, notify, channels,
                    {} AS created_at, {} AS cleared_at, cleared_reason
             FROM alerts
             WHERE (CAST($1 AS TEXT) IS NULL OR feed_id = CAST($1 AS TEXT)) AND (CAST($3 AS BIGINT) = 0 OR cleared_at IS NULL)
             ORDER BY id DESC LIMIT $2",
            s.db.iso("created_at"),
            s.db.iso("cleared_at")
        ),
        params![q.source, q.limit.unwrap_or(100), q.open.unwrap_or(false)],
    )?;
    let rows: Vec<Value> = rows
        .into_iter()
        .map(|mut row| {
            for key in ["notify", "channels"] {
                if let Some(Value::String(text)) = row.get(key) {
                    let parsed =
                        serde_json::from_str::<Value>(text).unwrap_or(Value::Array(vec![]));
                    row[key] = parsed;
                }
            }
            row
        })
        .collect();
    Ok(Json(json!({"alerts": rows})))
}

// ---- Alert rules --------------------------------------------------------------

async fn list_rules(State(s): State<AppState>, Query(q): Query<AlertQuery>) -> ApiResult {
    let rules: Vec<Rule> =
        s.db.rules()?
            .into_iter()
            .filter(|r| {
                q.source
                    .as_ref()
                    .is_none_or(|id| r.source_id.as_ref() == Some(id))
            })
            .collect();
    Ok(Json(json!({"rules": rules})))
}

async fn get_rule(State(s): State<AppState>, Path(id): Path<i64>) -> ApiResult {
    let rule =
        s.db.rule(id)?
            .ok_or_else(|| ApiError(StatusCode::NOT_FOUND, format!("rule {id} not found")))?;
    Ok(Json(json!(rule)))
}

/// Parses a rule body, checks its shape and that the source it names exists.
fn parse_rule(s: &AppState, body: Value) -> Result<Rule, ApiError> {
    let rule: Rule = serde_json::from_value(body)
        .map_err(|e| ApiError(StatusCode::BAD_REQUEST, e.to_string()))?;
    crate::alerts::validate_rule(&rule).map_err(|m| ApiError(StatusCode::BAD_REQUEST, m))?;
    if let Some(source) = &rule.source_id
        && s.manager.source(source).is_none()
    {
        return Err(ApiError(
            StatusCode::BAD_REQUEST,
            format!("source {source} not found"),
        ));
    }
    Ok(rule)
}

async fn create_rule(
    State(s): State<AppState>,
    principal: Option<axum::Extension<Arc<helios_auth::Principal>>>,
    Json(body): Json<Value>,
) -> Result<(StatusCode, Json<Value>), ApiError> {
    let mut rule = parse_rule(&s, body)?;
    if rule.created_by.is_none() {
        rule.created_by = principal.map(|axum::Extension(p)| p.subject().to_string());
    }
    rule.id = s.db.insert_rule(&rule)?;
    Ok((StatusCode::CREATED, Json(json!(rule))))
}

async fn replace_rule(
    State(s): State<AppState>,
    Path(id): Path<i64>,
    Json(body): Json<Value>,
) -> ApiResult {
    let mut rule = parse_rule(&s, body)?;
    if !s.db.update_rule(id, &rule)? {
        return Err(ApiError(
            StatusCode::NOT_FOUND,
            format!("rule {id} not found"),
        ));
    }
    rule.id = id;
    Ok(Json(json!(rule)))
}

async fn delete_rule(
    State(s): State<AppState>,
    Path(id): Path<i64>,
) -> Result<StatusCode, ApiError> {
    if !s.db.delete_rule(id)? {
        return Err(ApiError(
            StatusCode::NOT_FOUND,
            format!("rule {id} not found"),
        ));
    }
    Ok(StatusCode::NO_CONTENT)
}

/// Prometheus text format, so the numbers can live in the same monitoring stack as HFS.
async fn metrics(State(s): State<AppState>) -> Result<Response, ApiError> {
    let sources = s.manager.sources();
    let mut out = String::new();
    out.push_str("# TYPE hie_records gauge\n");
    for f in &sources {
        if let Value::Object(counts) = s.db.feed_counts(&f.id)? {
            for status in ["received", "pending", "submitted", "sent", "rejected"] {
                let n = counts.get(status).and_then(Value::as_i64).unwrap_or(0);
                out.push_str(&format!(
                    "hie_records{{source=\"{}\",status=\"{status}\"}} {n}\n",
                    f.id
                ));
            }
        }
    }
    out.push_str("# TYPE hie_source_last_received_timestamp_seconds gauge\n");
    for f in &sources {
        if let Some(t) = s.db.last_received_at(&f.id)? {
            out.push_str(&format!(
                "hie_source_last_received_timestamp_seconds{{source=\"{}\"}} {}\n",
                f.id,
                t / 1000
            ));
        }
    }
    out.push_str("# TYPE hie_source_paused gauge\n");
    for f in &sources {
        out.push_str(&format!(
            "hie_source_paused{{source=\"{}\"}} {}\n",
            f.id,
            u8::from(s.db.is_paused(&f.id)?)
        ));
    }
    out.push_str("# TYPE hie_alerts_total counter\n");
    for row in s.db.query_json("SELECT COALESCE(feed_id, '') AS source, kind, COUNT(*) AS n FROM alerts GROUP BY feed_id, kind", [])? {
        out.push_str(&format!(
            "hie_alerts_total{{source=\"{}\",kind=\"{}\"}} {}\n",
            row["source"].as_str().unwrap_or_default(),
            row["kind"].as_str().unwrap_or_default(),
            row["n"]
        ));
    }
    Ok(([(header::CONTENT_TYPE, "text/plain; version=0.0.4")], out).into_response())
}
