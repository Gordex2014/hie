//! Connection evidence from a Test, and describe and sample through it
//! (`/api/evidence`). Both reconnect with the evidence's definition and
//! credential revision after checking admission again. Sample rows are held
//! in memory only (`SampleRegistry`) and answered `no-store`.

use axum::extract::{Path, State};
use axum::http::{HeaderValue, header};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::Deserialize;
use serde_json::json;

use super::body::StrictJson;
use super::secrets::internal;
use super::{ApiError, ApiResult, AppState};
use crate::config::{DatabaseRead, TableName};
use crate::ids::rfc3339_ms_at;
use crate::manager::{ErrorStatus, ManagerError};

pub(super) fn router() -> Router<AppState> {
    Router::new()
        .route("/api/evidence/{id}", get(read))
        .route("/api/evidence/{id}/describe", post(describe))
        .route("/api/evidence/{id}/sample", post(sample))
}

fn refused(e: ManagerError) -> ApiError {
    if e.status() == ErrorStatus::Internal {
        return internal(e.into_cause());
    }
    ApiError::from(e)
}

async fn read(State(s): State<AppState>, Path(id): Path<String>) -> ApiResult {
    Ok(Json(s.manager.evidence().get(&id).map_err(refused)?.body))
}

#[derive(Deserialize, Default)]
struct DescribeBody {
    #[serde(default)]
    table: Option<TableName>,
}

async fn describe(
    State(s): State<AppState>,
    Path(id): Path<String>,
    body: Option<StrictJson<DescribeBody>>,
) -> ApiResult {
    let body = body.map(|StrictJson(b)| b).unwrap_or_default();
    let answer = s
        .manager
        .evidence()
        .describe(&id, body.table.as_ref())
        .await
        .map_err(refused)?;
    Ok(Json(answer))
}

#[derive(Deserialize, Default)]
struct SampleBody {
    #[serde(default)]
    read: Option<DatabaseRead>,
    #[serde(default)]
    limit: Option<i64>,
}

async fn sample(
    State(s): State<AppState>,
    Path(id): Path<String>,
    body: Option<StrictJson<SampleBody>>,
) -> Result<Response, ApiError> {
    let body = body.map(|StrictJson(b)| b).unwrap_or_default();
    let limit = body.limit.unwrap_or(5);
    if !(1..=50).contains(&limit) {
        return Err(ApiError::invalid_body("limit must be 1 to 50").with("field", "limit"));
    }
    let rows = s
        .manager
        .evidence()
        .sample(&id, body.read.as_ref(), limit as usize)
        .await
        .map_err(refused)?;
    let count = rows.len();
    let (sample, expires_at) = s.samples.put(rows.clone());
    let mut response = Json(json!({"sample": {
        "id": sample,
        "rows": rows,
        "count": count,
        "expires_at": rfc3339_ms_at(expires_at),
    }}))
    .into_response();
    response
        .headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    Ok(response)
}
