//! `POST /api/sources/{id}/manifests`: a FHIR Bulk Data manifest handed to the
//! FHIR server for a `manifest` source, by URL or uploaded. See `docs/api.md`
//! (Manifests) and `crate::manifest`.

use axum::body::Body;
use axum::extract::{Path, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use futures::StreamExt;
use serde_json::{Value, json};

use super::{ApiError, AppState, iso_ms, load_detail};
use crate::db::manifest::{Admission, DISPATCHED, NewLoad};
use crate::manager::ManagerError;
use crate::manifest::{BODY_MAX, BodyError, Given, Kickoff};

/// How often a write after the kick-off is tried before the answer is given
/// from what the request knows.
const WRITE_TRIES: u32 = 5;

/// The header a refusal of the FHIR server passed through carries.
const REFUSED_BY: &str = "x-hie-refused-by";

pub(super) async fn submit_manifest(
    State(s): State<AppState>,
    Path(id): Path<String>,
    headers: HeaderMap,
    body: Body,
) -> Result<Response, ApiError> {
    let json = headers
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.split(';').next())
        .is_some_and(|t| t.trim().eq_ignore_ascii_case("application/json"));
    if !json {
        return Err(ApiError::new(
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            "send the submission as application/json",
        )
        .code("unsupported_media_type"));
    }
    let bytes = read_body(body).await?;
    let request = crate::manifest::read_request(&bytes).map_err(|e| match e {
        BodyError::Invalid { field, error } => ApiError::new(StatusCode::BAD_REQUEST, error)
            .code("invalid_body")
            .with("field", field.map_or(Value::Null, Value::from)),
        BodyError::Manifest(error) => {
            ApiError::new(StatusCode::BAD_REQUEST, error).code("invalid_manifest")
        }
    })?;
    let client = s.manager.manifest_client();
    let token = crate::bulk::token();
    let (forwarded, served, name, fhir_base_url) = match &request.given {
        Given::Url(url) => (
            url.clone(),
            None,
            crate::manifest::stored_url(url),
            crate::manifest::origin(url),
        ),
        Given::Upload {
            manifest,
            file_name,
        } => {
            let first = manifest["output"][0]["url"].as_str().unwrap_or_default();
            (
                client.upload_url(&token),
                Some(serde_json::to_string(manifest).map_err(anyhow::Error::from)?),
                file_name.clone(),
                crate::manifest::origin(first),
            )
        }
    };
    let fhir_base_url = fhir_base_url.unwrap_or_default();
    let sha = crate::manifest::sha256_hex(served.as_deref().unwrap_or(&forwarded).as_bytes());
    let by = match request.given {
        Given::Url(_) => "url",
        Given::Upload { .. } => "upload",
    };

    // Admitted under the source's lock and, in the store, its admission
    // lock: a delete, a replace or a pause comes before or after, and nothing
    // is sent while the store could not record the load.
    let admitted = s
        .manager
        .with_source_locked(&id, |feed| {
            if !feed.is_manifest() {
                return Ok(None);
            }
            let oauth = match &feed.source {
                crate::config::SourceConfig::Manifest {
                    oauth_metadata_url, ..
                } => oauth_metadata_url.clone(),
                _ => None,
            };
            let load = NewLoad {
                feed: &feed.id,
                source_name: &feed.name,
                name: &name,
                sha: &sha,
                size: served.as_ref().map_or(0, String::len),
                by,
                request_id: request.request_id.as_deref(),
                request_hash: &request.hash,
                token: &token,
                served: served.as_deref(),
            };
            s.db.admit_manifest(&load)
                .map(|a| Some((a, oauth, feed.name.clone())))
        })
        .await
        .map_err(|e| match e {
            ManagerError::NotFound(_) => super::not_found(&format!("source {id}")),
            other => other.into(),
        })?;
    let (admission, oauth, source_name) = match admitted {
        Ok(Some(found)) => found,
        Ok(None) => {
            return Err(ApiError::new(
                StatusCode::BAD_REQUEST,
                format!("source {id} is not a manifest source"),
            )
            .code("not_a_manifest_source"));
        }
        Err(e) if crate::db::is_busy(&e) => return Err(ApiError::store_busy()),
        Err(e) => return Err(e.into()),
    };
    let admitted = match admission {
        Admission::New(admitted) => admitted,
        Admission::Existing(load) => {
            let detail = load_detail(&s, load)?.ok_or_else(|| super::not_found("load"))?;
            return Ok((StatusCode::OK, located(load), axum::Json(detail)).into_response());
        }
        Admission::InProgress => {
            return Err(ApiError::new(
                StatusCode::CONFLICT,
                "a submission with this request_id is still being sent; ask again shortly",
            )
            .code("request_in_progress"));
        }
        Admission::Conflict => {
            return Err(ApiError::new(
                StatusCode::CONFLICT,
                "a load of this source already has this request_id, for a different body",
            )
            .code("request_id_conflict"));
        }
        Admission::Paused => {
            return Err(ApiError::new(
                StatusCode::CONFLICT,
                format!("source {id} is paused; resume it to send manifests"),
            )
            .code("source_paused"));
        }
    };

    // The kick-off's deadline runs from when the load was recorded: the
    // token, the dispatch and the answer all end by then, and a load still
    // unlisted twice that long after it was recorded is its source's to
    // confirm.
    let deadline_ms = crate::manifest::kickoff_timeout().as_millis() as i64;
    let left = (admitted.received_at + deadline_ms - crate::db::now_ms()).max(0);
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_millis(left as u64);
    let load = admitted.load_id;
    // The load as this request knows it, should the store not answer.
    let known = json!({
        "id": load, "source_id": id, "source_name": source_name, "name": name, "status": "processing",
        "duplicate_of": null, "size": served.as_ref().map(String::len),
        "error": null, "source_key": null, "archived": null, "records": 0, "sent": 0, "rejected": 0,
        "failing": 0, "in_progress": 0, "took_ms": null, "received_at": iso_ms(admitted.received_at),
        "finished_at": null, "method": "bulk_submit", "failed": false, "failures": [],
        "manifest": {"by": by, "request_id": request.request_id, "submission_id": admitted.submission_id,
                     "kickoff": "unconfirmed", "sent_by_type": {}, "deleted": 0},
    });
    let bearer = match client.kickoff_token(deadline).await {
        Ok(bearer) => bearer,
        Err(nothing_sent) => return Ok(refused(&s, &id, &token, 0, nothing_sent, known).await),
    };
    // Claimed right before it is sent: a load its source already listed as
    // left behind, or past its deadline, is not sent.
    match s.db.manifest_dispatch(&token, deadline_ms) {
        Ok(true) => {}
        Ok(false) => {
            if let Some(detail) = load_detail(&s, load).ok().flatten() {
                return Ok(
                    (StatusCode::ACCEPTED, located(load), axum::Json(detail)).into_response()
                );
            }
            tracing::warn!(feed = %id, load, "the manifest was not sent: its deadline passed before it could be");
            return Ok(refused(&s, &id, &token, 0, Kickoff::NotSent, known).await);
        }
        Err(e) => {
            tracing::warn!(feed = %id, load, "the manifest was not sent: {e:#}");
            return Ok(refused(&s, &id, &token, 0, Kickoff::NotSent, known).await);
        }
    }
    let kickoff = crate::manifest::Client::kickoff_body(
        &id,
        &admitted.submission_id,
        &forwarded,
        &fhir_base_url,
        oauth.as_deref(),
    );
    match client.kickoff(&kickoff, bearer.as_deref(), deadline).await {
        Kickoff::Accepted => Ok(listed(&s, &token, load, true, known).await),
        Kickoff::Lost(reason) => {
            tracing::warn!(feed = %id, load, "the kick-off's answer was lost, the source will confirm it: {}", crate::bulk::scrub(&reason, &token));
            Ok(listed(&s, &token, load, false, known).await)
        }
        refusal => Ok(refused(&s, &id, &token, DISPATCHED, refusal, known).await),
    }
}

/// The body as received, refused once it passes `BODY_MAX`.
async fn read_body(body: Body) -> Result<Vec<u8>, ApiError> {
    let too_large = || {
        ApiError::new(StatusCode::PAYLOAD_TOO_LARGE, "the body is over 4 MiB")
            .code("body_too_large")
    };
    let mut stream = body.into_data_stream();
    let mut bytes = Vec::new();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|_| {
            ApiError::new(StatusCode::BAD_REQUEST, "the body could not be read")
                .code("invalid_body")
                .with("field", Value::Null)
        })?;
        if bytes.len() + chunk.len() > BODY_MAX {
            return Err(too_large());
        }
        bytes.extend_from_slice(&chunk);
    }
    Ok(bytes)
}

fn located(load: i64) -> [(header::HeaderName, String); 1] {
    [(header::LOCATION, format!("/api/loads/{load}"))]
}

/// Lists the load after its kick-off and answers with it: `201` when the
/// FHIR server took the manifest, `202` when its answer was lost. A write
/// that keeps failing answers `202` with the load as the request knows it;
/// the source lists and confirms it later.
async fn listed(s: &AppState, token: &str, load: i64, accepted: bool, known: Value) -> Response {
    let mut written = None;
    for attempt in 0..WRITE_TRIES {
        if attempt > 0 {
            tokio::time::sleep(std::time::Duration::from_millis(200 * u64::from(attempt))).await;
        }
        match s.db.manifest_kicked(token, accepted) {
            Ok(moved) => {
                written = Some(moved);
                break;
            }
            Err(e) => tracing::warn!(load, "listing a manifest load: {e:#}"),
        }
    }
    let answer = match written {
        Some(_) => load_detail(s, load).ok().flatten(),
        None => None,
    };
    match answer {
        Some(detail) => {
            let status = if detail["manifest"]["kickoff"] == "accepted" {
                StatusCode::CREATED
            } else {
                StatusCode::ACCEPTED
            };
            (status, located(load), axum::Json(detail)).into_response()
        }
        None => (StatusCode::ACCEPTED, located(load), axum::Json(known)).into_response(),
    }
}

/// A kick-off the FHIR server refused, or one never sent: its load is marked
/// refused first (so it is never served, listed or answered as a load, even
/// should its removal fail), then removed, then the refusal is answered. One
/// whose mark cannot be written is answered `202` with the load as the
/// request knows it: it follows the recovery of a lost answer. `epoch` is
/// the submission's as the request left it.
async fn refused(
    s: &AppState,
    id: &str,
    token: &str,
    epoch: i64,
    answer: Kickoff,
    known: Value,
) -> Response {
    let load = known["id"].as_i64().unwrap_or_default();
    let mut marked = None;
    for attempt in 0..WRITE_TRIES {
        if attempt > 0 {
            tokio::time::sleep(std::time::Duration::from_millis(200 * u64::from(attempt))).await;
        }
        match s.db.manifest_refuse(token, epoch) {
            Ok(done) => {
                marked = Some(done);
                break;
            }
            Err(e) => tracing::warn!(feed = %id, load, "marking a refused manifest load: {e:#}"),
        }
    }
    match marked {
        Some(true) => {}
        // Its source moved it meanwhile: answered as it is now.
        Some(false) => {
            let detail = load_detail(s, load).ok().flatten().unwrap_or(known);
            return (StatusCode::ACCEPTED, located(load), axum::Json(detail)).into_response();
        }
        None => return (StatusCode::ACCEPTED, located(load), axum::Json(known)).into_response(),
    }
    for attempt in 0..WRITE_TRIES {
        if attempt > 0 {
            tokio::time::sleep(std::time::Duration::from_millis(200 * u64::from(attempt))).await;
        }
        match s.db.manifest_remove_refused(token) {
            Ok(()) => break,
            Err(e) => {
                tracing::warn!(feed = %id, load, "removing a refused manifest load; its source removes it: {e:#}")
            }
        }
    }
    match answer {
        Kickoff::Refused {
            status,
            body,
            content_type,
            retry_after,
        } => {
            tracing::info!(feed = %id, status, "the FHIR server refused the manifest; its answer is passed through");
            let mut response = Response::new(Body::from(body));
            *response.status_mut() =
                StatusCode::from_u16(status).unwrap_or(StatusCode::BAD_GATEWAY);
            let h = response.headers_mut();
            if let Some(v) = content_type {
                h.insert(header::CONTENT_TYPE, v);
            }
            if let Some(v) = retry_after {
                h.insert(header::RETRY_AFTER, v);
            }
            h.insert(REFUSED_BY, HeaderValue::from_static("destination"));
            response
        }
        Kickoff::TooLarge { status } => ApiError::new(
            StatusCode::BAD_GATEWAY,
            format!(
                "the FHIR server refused the manifest with HTTP {status}, and its answer could not be passed on (over 64 KiB, or not read whole in time)"
            ),
        )
        .code("destination_answer_too_large")
        .with("destination_status", status)
        .into_response(),
        Kickoff::Auth(detail) => {
            tracing::warn!(feed = %id, "the FHIR server refused the engine's token on a manifest kick-off: {}", crate::bulk::scrub(&detail, token));
            ApiError::new(
                StatusCode::BAD_GATEWAY,
                "the FHIR server did not accept the engine's own credentials; the engine log says why",
            )
            .code("destination_auth")
            .into_response()
        }
        Kickoff::Unreachable(reason) => {
            tracing::warn!(feed = %id, "the manifest was not sent: {}", crate::bulk::scrub(&reason, token));
            ApiError::new(StatusCode::BAD_GATEWAY, "the FHIR server cannot be reached")
                .code("destination_unreachable")
                .into_response()
        }
        Kickoff::NotSent => ApiError::store_busy().into_response(),
        // Never handed here: they list the load.
        Kickoff::Accepted | Kickoff::Lost(_) => {
            (StatusCode::ACCEPTED, located(load), axum::Json(known)).into_response()
        }
    }
}
