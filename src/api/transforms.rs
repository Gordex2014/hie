//! Transforms and their versions (contract "Transforms and versions"), their
//! usage, and Try (contract "Try"). The 0.1 routes (`GET`/`PUT
//! /api/transforms/{name}`, `POST /api/transforms/{name}/try` with
//! `{"records"}`) keep working over the version store.

use std::time::Duration;

use axum::extract::{Path, Query, State};
use axum::http::{HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::Deserialize;
use serde_json::{Value, json};

use super::body::{LooseJson, StrictJson, strict};
use super::{ApiError, ApiResult, AppState, not_found};
use crate::db::FailedRowsError;
use crate::idempotency::Idempotent;
use crate::tryrun::{MAX_RECORDS, SavedVersion, TryInput};
use crate::versions::{Draft, USAGE_RUNS};

pub(super) fn router() -> Router<AppState> {
    Router::new()
        .route(
            "/api/transforms",
            get(list_transforms).post(create_transform),
        )
        .route("/api/transforms/try", post(try_inline))
        .route("/api/transforms/try-runs/{run}", get(get_try_run))
        .route(
            "/api/transforms/try-runs/{run}/cancel",
            post(cancel_try_run),
        )
        .route(
            "/api/transforms/{id}",
            get(get_transform)
                .put(put_transform)
                .patch(rename_transform)
                .delete(delete_transform),
        )
        .route(
            "/api/transforms/{id}/versions",
            get(list_versions).post(save_version),
        )
        .route("/api/transforms/{id}/usage", get(usage))
        .route("/api/transforms/{id}/try", post(try_saved))
}

fn transform_missing(id: &str) -> ApiError {
    not_found("transform_not_found", &format!("transform {id}"))
}

async fn list_transforms(State(s): State<AppState>) -> ApiResult {
    s.versions.ready().await;
    Ok(Json(json!({"transforms": s.manager.transforms()?})))
}

#[derive(Deserialize)]
struct VersionQuery {
    version: Option<String>,
}

/// One transform with its code and version: the current one, or `?version=`.
/// A 0.1 name reaches its transform too.
async fn get_transform(
    State(s): State<AppState>,
    Path(name): Path<String>,
    Query(q): Query<VersionQuery>,
) -> ApiResult {
    s.versions.ready().await;
    let id = s
        .versions
        .resolve_name(&name)?
        .ok_or_else(|| transform_missing(&name))?;
    let t = s
        .versions
        .transform(&id)?
        .ok_or_else(|| transform_missing(&id))?;
    let number = match q.version.as_deref() {
        None => t.current_version,
        Some(v) => v.parse::<i64>().map_err(|_| {
            ApiError::invalid_body("version is a version number").with("field", "version")
        })?,
    };
    let version_missing = || {
        not_found(
            "version_not_found",
            &format!("version {number} of transform {id}"),
        )
    };
    let v = s
        .versions
        .version(&id, number)?
        .ok_or_else(version_missing)?;
    let code = s.versions.code(&id, number)?.ok_or_else(version_missing)?;
    let mut body = s.versions.transform_json(&t);
    body["code"] = json!(code);
    body["version"] = v.to_json();
    Ok(Json(body))
}

/// The `environment` a save names, read before the body is checked (so the
/// runtime is asked once, before the transaction).
fn requested_environment(body: &[u8]) -> Option<String> {
    serde_json::from_slice::<Value>(body)
        .ok()?
        .get("environment")?
        .as_str()
        .map(str::to_string)
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CreateBody {
    display_name: String,
    code: String,
    #[serde(default)]
    note: Option<String>,
    #[serde(default)]
    author: Option<String>,
    #[serde(default)]
    environment: Option<String>,
}

/// `POST /api/transforms` (key required): the transform and its version 1.
async fn create_transform(
    State(s): State<AppState>,
    idem: Idempotent,
) -> Result<Response, ApiError> {
    s.versions.ready().await;
    let environment = s
        .versions
        .environment_for(requested_environment(idem.body()).as_deref())
        .await;
    let mut created: Option<String> = None;
    let response = idem
        .run_in_tx(&s.db, true, |tx, _, bytes| {
            let body: CreateBody = strict(bytes)?;
            let draft = Draft {
                code: body.code,
                note: body.note,
                author: body.author,
            };
            // Read before the transaction (`requested_environment`).
            let _ = body.environment;
            let environment = environment?;
            let answer = s
                .versions
                .create_in(tx, &body.display_name, &draft, &environment)?;
            created = answer["id"].as_str().map(str::to_string);
            Ok((StatusCode::CREATED, answer))
        })
        .await?;
    if let Some(id) = created {
        s.versions.saved(&id);
    }
    Ok(response)
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SaveBody {
    code: String,
    base_version: i64,
    #[serde(default)]
    note: Option<String>,
    #[serde(default)]
    author: Option<String>,
    #[serde(default)]
    environment: Option<String>,
}

/// `POST /api/transforms/{id}/versions` (key required): a new version, with
/// conflict detection on `base_version`.
async fn save_version(
    State(s): State<AppState>,
    Path(id): Path<String>,
    idem: Idempotent,
) -> Result<Response, ApiError> {
    s.versions.ready().await;
    let environment = s
        .versions
        .environment_for(requested_environment(idem.body()).as_deref())
        .await;
    let mut saved = false;
    let response = idem
        .run_in_tx(&s.db, true, |tx, _, bytes| {
            let body: SaveBody = strict(bytes)?;
            // Read before the transaction (`requested_environment`).
            let _ = body.environment;
            if tx.transform(&id)?.filter(|t| !t.deleted).is_none() {
                return Err(transform_missing(&id));
            }
            let environment = environment?;
            let draft = Draft {
                code: body.code,
                note: body.note,
                author: body.author,
            };
            let answer = s
                .versions
                .save_in(tx, &id, body.base_version, &draft, &environment)?;
            saved = true;
            Ok((StatusCode::CREATED, answer))
        })
        .await?;
    if saved {
        s.versions.saved(&id);
        s.versions.reclaim().await;
    }
    Ok(response)
}

async fn list_versions(State(s): State<AppState>, Path(id): Path<String>) -> ApiResult {
    s.versions
        .transform(&id)?
        .ok_or_else(|| transform_missing(&id))?;
    let versions: Vec<Value> = s
        .versions
        .versions(&id)?
        .iter()
        .map(|v| v.to_json())
        .collect();
    Ok(Json(json!({"versions": versions})))
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RenameBody {
    display_name: String,
}

async fn rename_transform(
    State(s): State<AppState>,
    Path(id): Path<String>,
    StrictJson(body): StrictJson<RenameBody>,
) -> ApiResult {
    let answer = s.db.transaction(|tx| -> Result<Value, ApiError> {
        Ok(s.versions.rename_in(tx, &id, &body.display_name)?)
    })?;
    Ok(Json(answer))
}

async fn delete_transform(
    State(s): State<AppState>,
    Path(id): Path<String>,
) -> Result<StatusCode, ApiError> {
    s.versions.ready().await;
    s.manager.delete_transform(&id)?;
    s.versions.reclaim().await;
    Ok(StatusCode::NO_CONTENT)
}

#[derive(Deserialize)]
struct LegacySave {
    code: String,
}

/// `PUT /api/transforms/{name}` (0.1): a new version when the code differs.
async fn put_transform(
    State(s): State<AppState>,
    Path(name): Path<String>,
    LooseJson(body): LooseJson<LegacySave>,
) -> Result<(StatusCode, Json<Value>), ApiError> {
    s.versions.ready().await;
    let (created, version) = s.versions.save_legacy(&name, &body.code).await?;
    s.versions.reclaim().await;
    let status = if created {
        StatusCode::CREATED
    } else {
        StatusCode::OK
    };
    Ok((
        status,
        Json(json!({"name": name, "saved": true, "version": version})),
    ))
}

#[derive(Deserialize)]
struct UsageQuery {
    limit: Option<String>,
}

async fn usage(
    State(s): State<AppState>,
    Path(id): Path<String>,
    Query(q): Query<UsageQuery>,
) -> ApiResult {
    let limit = match q.limit.as_deref() {
        None => USAGE_RUNS,
        Some(l) => l
            .parse::<usize>()
            .ok()
            .filter(|n| (1..=200).contains(n))
            .ok_or_else(|| ApiError::invalid_body("limit is 1 to 200").with("field", "limit"))?,
    };
    let usage = s
        .versions
        .usage(&id, limit)?
        .ok_or_else(|| transform_missing(&id))?;
    Ok(Json(usage))
}

// ---- Try ---------------------------------------------------------------------------

/// `input`: exactly one of `records`, `sample` (with an optional `limit`) or
/// `failures`. Kept as optional members rather than an enum, so every unknown
/// member is seen by the strict reader.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct TryInputBody {
    #[serde(default)]
    records: Option<Vec<Value>>,
    #[serde(default)]
    sample: Option<String>,
    #[serde(default)]
    limit: Option<usize>,
    #[serde(default)]
    failures: Option<FailuresBody>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct FailuresBody {
    load: i64,
    group: String,
    #[serde(default)]
    records: Option<Vec<i64>>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct InlineTryBody {
    run_id: String,
    code: String,
    input: TryInputBody,
    #[serde(default)]
    deadline_ms: Option<u64>,
    #[serde(default)]
    validate: Option<bool>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SavedTryBody {
    run_id: String,
    #[serde(default)]
    version: Option<i64>,
    input: TryInputBody,
    #[serde(default)]
    deadline_ms: Option<u64>,
    #[serde(default)]
    validate: Option<bool>,
}

fn check_run_id(run_id: &str) -> Result<(), ApiError> {
    let ok = (1..=64).contains(&run_id.len())
        && run_id
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'.' || b == b'_' || b == b'-');
    if ok {
        Ok(())
    } else {
        Err(ApiError::invalid_body("run_id is 1 to 64 of [A-Za-z0-9._-]").with("field", "run_id"))
    }
}

fn deadline(ms: Option<u64>) -> Result<Duration, ApiError> {
    let ms = ms.unwrap_or(30_000);
    if !(1000..=120_000).contains(&ms) {
        return Err(
            ApiError::invalid_body("deadline_ms is 1000 to 120000").with("field", "deadline_ms")
        );
    }
    Ok(Duration::from_millis(ms))
}

fn records_in_range(records: Vec<Value>) -> Result<Vec<Value>, ApiError> {
    if records.is_empty() || records.len() > MAX_RECORDS {
        return Err(
            ApiError::invalid_body(format!("a Try takes 1 to {MAX_RECORDS} records"))
                .with("field", "input.records"),
        );
    }
    Ok(records)
}

/// The records an `input` names.
fn input_records(s: &AppState, input: TryInputBody) -> Result<Vec<Value>, ApiError> {
    let given = [
        input.records.is_some(),
        input.sample.is_some(),
        input.failures.is_some(),
    ]
    .iter()
    .filter(|g| **g)
    .count();
    if given != 1 {
        return Err(ApiError::invalid_body(
            "input is one of {\"records\"}, {\"sample\"} or {\"failures\"}",
        )
        .with("field", "input"));
    }
    if input.limit.is_some() && input.sample.is_none() {
        return Err(ApiError::unknown_field("input.limit"));
    }
    if let Some(records) = input.records {
        return records_in_range(records);
    }
    if let Some(sample) = input.sample {
        let rows = s.samples.get(&sample).ok_or_else(|| {
            ApiError::not_found("not_found", format!("sample {sample} not found or expired"))
        })?;
        let limit = input.limit.unwrap_or(MAX_RECORDS);
        if !(1..=MAX_RECORDS).contains(&limit) {
            return Err(
                ApiError::invalid_body(format!("limit is 1 to {MAX_RECORDS}"))
                    .with("field", "input.limit"),
            );
        }
        return records_in_range(rows.into_iter().take(limit).collect());
    }
    let failures = input.failures.expect("one input");
    let rows =
        s.db.failed_rows(failures.load, &failures.group, failures.records.as_deref())
            .map_err(|e| match e {
                FailedRowsError::LoadNotFound => {
                    not_found("load_not_found", &format!("load {}", failures.load))
                }
                FailedRowsError::GroupNotFound => not_found(
                    "group_not_found",
                    &format!("failure group {}", failures.group),
                ),
                FailedRowsError::RecordNotInGroup(id) => not_found(
                    "record_not_found",
                    &format!("record {id} in failure group {}", failures.group),
                ),
                FailedRowsError::ContentExpired => ApiError::new(
                    StatusCode::GONE,
                    "content_expired",
                    "the failed rows' content is no longer kept",
                ),
                FailedRowsError::Store(e) => ApiError::from(e),
            })?;
    records_in_range(rows.into_iter().map(|r| r.raw).collect())
}

/// Runs a Try and answers it `no-store`; `429 try_busy` carries `Retry-After`.
async fn run_try(
    s: &AppState,
    input: TryInput,
    legacy_name: Option<&str>,
) -> Result<Response, ApiError> {
    let fhir = s.manager.services().fhir.clone();
    let max = s.config.try_runs.max_concurrent;
    let mut body = crate::tryrun::start(s.versions.clone(), fhir, max, input)
        .await
        .map_err(|e| {
            let busy = e.code() == "try_busy";
            let error = ApiError::from(e);
            if busy {
                error.header(header::RETRY_AFTER, HeaderValue::from_static("1"))
            } else {
                error
            }
        })?;
    if let Some(name) = legacy_name {
        body["name"] = json!(name);
    }
    let mut response = Json(body).into_response();
    response
        .headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    Ok(response)
}

/// `POST /api/transforms/try`: code sent inline.
async fn try_inline(
    State(s): State<AppState>,
    StrictJson(body): StrictJson<InlineTryBody>,
) -> Result<Response, ApiError> {
    check_run_id(&body.run_id)?;
    let deadline = deadline(body.deadline_ms)?;
    if body.code.len() > crate::versions::MAX_CODE_BYTES {
        return Err(ApiError::new(
            StatusCode::PAYLOAD_TOO_LARGE,
            "code_too_large",
            "code is at most 256 KiB",
        ));
    }
    let records = input_records(&s, body.input)?;
    let input = TryInput {
        run_id: body.run_id,
        code: body.code,
        saved: None,
        records,
        deadline,
        validate: body.validate.unwrap_or(true),
    };
    run_try(&s, input, None).await
}

/// `POST /api/transforms/{id}/try`: a saved version, the current one by
/// default; the 0.1 body `{"records": [...]}` gets a generated run id.
async fn try_saved(
    State(s): State<AppState>,
    Path(id): Path<String>,
    bytes: axum::body::Bytes,
) -> Result<Response, ApiError> {
    let parsed: Value = serde_json::from_slice(&bytes)
        .map_err(|_| ApiError::invalid_body("the body is not valid JSON"))?;
    let legacy = parsed.get("run_id").is_none() && parsed.get("records").is_some();
    let (run_id, version, given, deadline_ms, validate) = if legacy {
        #[derive(Deserialize)]
        struct LegacyTry {
            records: Vec<Value>,
        }
        let body: LegacyTry = super::body::loose(&bytes)?;
        if body.records.len() > MAX_RECORDS {
            return Err(ApiError::invalid_body("try at most 100 records at a time"));
        }
        let run_id = format!(
            "try-{:016x}{:08x}",
            rand::random::<u64>(),
            rand::random::<u32>()
        );
        (run_id, None, Err(body.records), None, None)
    } else {
        let body: SavedTryBody = strict(&bytes)?;
        check_run_id(&body.run_id)?;
        (
            body.run_id,
            body.version,
            Ok(body.input),
            body.deadline_ms,
            body.validate,
        )
    };
    let deadline = deadline(deadline_ms)?;
    s.versions.ready().await;
    let name = id;
    let id = s
        .versions
        .resolve_name(&name)?
        .ok_or_else(|| transform_missing(&name))?;
    let t = s
        .versions
        .transform(&id)?
        .ok_or_else(|| transform_missing(&id))?;
    let number = version.unwrap_or(t.current_version);
    let v = s.versions.version(&id, number)?.ok_or_else(|| {
        not_found(
            "version_not_found",
            &format!("version {number} of transform {id}"),
        )
    })?;
    let code = s
        .versions
        .code(&id, number)?
        .ok_or_else(|| transform_missing(&id))?;
    let records = match given {
        Ok(input) => input_records(&s, input)?,
        Err(records) => records,
    };
    let input = TryInput {
        run_id,
        code,
        saved: Some(SavedVersion {
            transform_id: id.clone(),
            version: number,
            environment: v.environment,
        }),
        records,
        deadline,
        validate: validate.unwrap_or(true),
    };
    run_try(&s, input, legacy.then_some(name.as_str())).await
}

async fn get_try_run(State(s): State<AppState>, Path(run): Path<String>) -> ApiResult {
    Ok(Json(crate::tryrun::get(&s.versions, &run)?))
}

async fn cancel_try_run(
    State(s): State<AppState>,
    Path(run): Path<String>,
) -> Result<(StatusCode, Json<Value>), ApiError> {
    Ok((
        StatusCode::ACCEPTED,
        Json(crate::tryrun::cancel(&s.versions, &run)?),
    ))
}
