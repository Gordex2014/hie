//! Transforms: listed, read, saved and tried on supplied records.

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::Deserialize;
use serde_json::{Value, json};

use super::body::LooseJson;
use super::{ApiError, ApiResult, AppState, not_found};

pub(super) fn router() -> Router<AppState> {
    Router::new()
        .route("/api/transforms", get(list_transforms))
        .route(
            "/api/transforms/{name}",
            get(get_transform).put(put_transform),
        )
        .route("/api/transforms/{name}/try", post(try_transform))
}

async fn list_transforms(State(s): State<AppState>) -> ApiResult {
    Ok(Json(json!({"transforms": s.manager.transforms()?})))
}

async fn get_transform(State(s): State<AppState>, Path(name): Path<String>) -> ApiResult {
    let code = s
        .manager
        .transform_code(&name)?
        .ok_or_else(|| not_found("transform_not_found", &format!("transform {name}")))?;
    Ok(Json(json!({"name": name, "code": code})))
}

#[derive(Deserialize)]
struct TransformBody {
    code: String,
}

async fn put_transform(
    State(s): State<AppState>,
    Path(name): Path<String>,
    LooseJson(body): LooseJson<TransformBody>,
) -> Result<(StatusCode, Json<Value>), ApiError> {
    let existed = s.manager.save_transform(&name, &body.code)?;
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
    LooseJson(body): LooseJson<TryBody>,
) -> ApiResult {
    if body.records.len() > 100 {
        return Err(ApiError::invalid_body("try at most 100 records at a time"));
    }
    let results = s.manager.try_transform(&name, &body.records).await?;
    Ok(Json(json!({"name": name, "results": results})))
}
