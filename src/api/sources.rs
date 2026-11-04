//! Sources: their definitions and state, created, replaced, paused, resumed,
//! checked now (and the passes that asks for) and tested. A source's view
//! adds what it is bound to (`credential`, `evidence`, `first_load`); its
//! `definition` holds secret references, never values. Failures of the
//! engine answer a fixed sentence (the log has the reason).

use std::sync::Arc;

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::Deserialize;
use serde_json::{Value, json};

use axum::response::Response;

use super::body::{LooseJson, loose};
use super::secrets::internal;
use super::{ApiError, ApiResult, AppState, DAY_MS, count, iso_ms, not_found};
use crate::config::FeedConfig;
use crate::db::{now_ms, params};
use crate::idempotency::Idempotent;
use crate::ids::rfc3339_ms_at;
use crate::manager::{ErrorStatus, ManagerError};
use crate::pipeline::human;

/// A manager refusal as the API answers it; an engine failure says nothing
/// of itself in the answer.
fn refused(e: ManagerError) -> ApiError {
    if e.status() == ErrorStatus::Internal {
        return internal(e.into_cause());
    }
    ApiError::from(e)
}

pub(super) fn router() -> Router<AppState> {
    Router::new()
        .route("/api/sources", get(list_sources).post(create_source))
        .route("/api/sources/test", post(test_source))
        .route(
            "/api/sources/{id}",
            get(get_source).put(replace_source).delete(delete_source),
        )
        .route("/api/sources/{id}/pause", post(pause_source))
        .route("/api/sources/{id}/resume", post(resume_source))
        .route("/api/sources/{id}/check-now", post(check_now))
        .route("/api/sources/{id}/passes/{pass}", get(get_pass))
}

/// A source's definition and its state, as one object.
pub(super) fn source_status(s: &AppState, feed: &FeedConfig) -> anyhow::Result<Value> {
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
    let binding = s.manager.evidence().binding(&feed.id)?;
    let credential = binding
        .as_ref()
        .and_then(|b| b.credential.clone())
        .or_else(|| match &feed.source {
            crate::config::SourceConfig::Database {
                connection: Some(c),
                ..
            } => c
                .password
                .as_ref()
                .and_then(|p| p.revision.map(|r| (p.secret.clone(), r))),
            _ => None,
        });
    let first_load = match binding.as_ref().and_then(|b| b.first_load) {
        Some(id) => Some(json!({"id": id, "status": db.load_status(id)?})),
        None => None,
    };
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
        "pin": s.versions.pin_view(feed).unwrap_or(Value::Null),
        "credential": credential.map(|(secret, revision)| json!({"secret": secret, "revision": revision})),
        "evidence": binding.as_ref().and_then(|b| b.evidence.clone()),
        "first_load": first_load,
    }))
}

async fn list_sources(State(s): State<AppState>) -> ApiResult {
    let sources: anyhow::Result<Vec<Value>> = s
        .manager
        .sources()
        .iter()
        .map(|f| source_status(&s, f))
        .collect();
    Ok(Json(json!({"sources": sources.map_err(internal)?})))
}

async fn get_source(State(s): State<AppState>, Path(id): Path<String>) -> ApiResult {
    let feed = s
        .manager
        .source(&id)
        .ok_or_else(|| not_found("source_not_found", &format!("source {id}")))?;
    Ok(Json(source_status(&s, &feed).map_err(internal)?))
}

/// A Start: the definition (with `evidence` for a database by parts);
/// `Idempotency-Key` honoured.
async fn create_source(
    State(s): State<AppState>,
    principal: Option<axum::Extension<Arc<helios_auth::Principal>>>,
    idem: Idempotent,
) -> Result<Response, ApiError> {
    let by = principal.map(|axum::Extension(p)| p.subject().to_string());
    let state = s.clone();
    idem.run(&s.db, false, move |attempt, body| async move {
        let body: Value = loose(&body)?;
        let feed = state
            .manager
            .create(&body, by.as_deref(), attempt.recovered)
            .map_err(refused)?;
        Ok((
            StatusCode::CREATED,
            source_status(&state, &feed).map_err(internal)?,
        ))
    })
    .await
}

async fn replace_source(
    State(s): State<AppState>,
    Path(id): Path<String>,
    LooseJson(body): LooseJson<Value>,
) -> ApiResult {
    let feed = s.manager.replace(&id, &body).map_err(refused)?;
    Ok(Json(source_status(&s, &feed).map_err(internal)?))
}

async fn delete_source(
    State(s): State<AppState>,
    Path(id): Path<String>,
) -> Result<StatusCode, ApiError> {
    s.manager.delete(&id).map_err(refused)?;
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
    body: Option<LooseJson<PauseBody>>,
) -> ApiResult {
    // Who paused it: the body's name, else the authenticated subject.
    let by = body
        .and_then(|LooseJson(b)| b.by)
        .or_else(|| principal.map(|axum::Extension(p)| p.subject().to_string()));
    s.manager.pause(&id, by.as_deref()).map_err(refused)?;
    get_source(State(s), Path(id)).await
}

async fn resume_source(State(s): State<AppState>, Path(id): Path<String>) -> ApiResult {
    s.manager.resume(&id).map_err(refused)?;
    get_source(State(s), Path(id)).await
}

async fn check_now(
    State(s): State<AppState>,
    Path(id): Path<String>,
) -> Result<(StatusCode, Json<Value>), ApiError> {
    let pass = s.manager.request_pass(&id).map_err(refused)?;
    Ok((
        StatusCode::ACCEPTED,
        Json(json!({"id": id, "checking": true, "pass": pass})),
    ))
}

async fn get_pass(
    State(s): State<AppState>,
    Path((id, pass)): Path<(String, String)>,
) -> ApiResult {
    if s.manager.source(&id).is_none() {
        return Err(not_found("source_not_found", &format!("source {id}")));
    }
    let pass =
        s.db.pass(&id, &pass)
            .map_err(internal)?
            .ok_or_else(|| not_found("pass_not_found", "pass"))?;
    Ok(Json(json!({
        "id": pass.id,
        "status": pass.status,
        "skipped_reason": pass.skipped_reason,
        "error": pass.error,
        "loads": pass.loads,
        "new_rows": pass.new_rows,
        "requested_at": rfc3339_ms_at(pass.requested_at),
        "finished_at": pass.finished_at.and_then(rfc3339_ms_at),
    })))
}

/// A Test: `{"source": {...}}` or a whole definition; `Idempotency-Key` honoured.
async fn test_source(State(s): State<AppState>, idem: Idempotent) -> Result<Response, ApiError> {
    let state = s.clone();
    idem.run(&s.db, false, move |_, body| async move {
        let body: Value = loose(&body)?;
        let answer = state.manager.test(&body).await.map_err(refused)?;
        Ok((StatusCode::OK, answer))
    })
    .await
}
