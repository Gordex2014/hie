//! Secrets: write-only values kept encrypted (`/api/secrets`). No answer
//! carries a value; a body that cannot be read is never echoed; the
//! idempotency hash of a request that carries a value is keyed (see
//! `Secrets::request_key`). Without custody every route is `503
//! secrets_unavailable`.

use std::sync::Arc;

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::Response;
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::Deserialize;
use serde_json::json;

use super::body::strict;
use super::{ApiError, ApiResult, AppState};
use crate::idempotency::Idempotent;
use crate::secrets::{SecretError, Secrets};

pub(super) fn router() -> Router<AppState> {
    Router::new()
        .route("/api/secrets", post(create).get(list))
        .route(
            "/api/secrets/{id}",
            get(read).put(revise).delete(delete_secret),
        )
}

/// A secret error as the API answers it.
pub(super) fn secret_error(e: SecretError) -> ApiError {
    match e {
        SecretError::Unavailable => ApiError::new(
            StatusCode::SERVICE_UNAVAILABLE,
            "secrets_unavailable",
            "secrets need custody configured: secrets.key_file in hie.yaml, or HIE_SECRETS_KEY",
        ),
        SecretError::NotFound | SecretError::RevisionNotFound => {
            ApiError::not_found("secret_not_found", "secret not found")
        }
        SecretError::NameInUse => {
            ApiError::conflict("name_in_use", "a secret with this name exists")
        }
        SecretError::InUse(sources) => {
            ApiError::conflict("secret_in_use", "sources refer to this secret")
                .with("sources", sources)
        }
        SecretError::Sealed => internal(anyhow::anyhow!("a kept secret does not open")),
        SecretError::Store(e) => internal(e),
    }
}

/// A `500` whose answer names nothing of the failure; the log has it.
pub(super) fn internal(e: anyhow::Error) -> ApiError {
    tracing::error!("API request failed: {e:#}");
    ApiError::new(
        StatusCode::INTERNAL_SERVER_ERROR,
        "internal_error",
        "the engine failed to do this; its log has the reason",
    )
}

fn secrets(s: &AppState) -> Result<Arc<Secrets>, ApiError> {
    let secrets = s.manager.secrets().clone();
    if !secrets.available() {
        return Err(secret_error(SecretError::Unavailable));
    }
    Ok(secrets)
}

#[derive(Deserialize)]
struct NewSecret {
    name: String,
    value: String,
}

#[derive(Deserialize)]
struct NewValue {
    value: String,
}

fn check_value(value: &str) -> Result<(), ApiError> {
    if value.is_empty() || value.len() > 4096 {
        return Err(
            ApiError::invalid_body("value must be 1 to 4096 bytes of UTF-8").with("field", "value"),
        );
    }
    Ok(())
}

async fn create(State(s): State<AppState>, idem: Idempotent) -> Result<Response, ApiError> {
    let secrets = secrets(&s)?;
    let idem = idem.keyed(&secrets.request_key().map_err(secret_error)?)?;
    idem.run(&s.db, true, move |_, body| async move {
        let NewSecret { name, value } = strict(&body)?;
        let chars = name.chars().count();
        if !(1..=64).contains(&chars) || name.trim().is_empty() {
            return Err(
                ApiError::invalid_body("name must be 1 to 64 characters").with("field", "name")
            );
        }
        check_value(&value)?;
        let view = secrets.create(&name, &value).map_err(secret_error)?;
        Ok((StatusCode::CREATED, view))
    })
    .await
}

async fn revise(
    State(s): State<AppState>,
    Path(id): Path<String>,
    idem: Idempotent,
) -> Result<Response, ApiError> {
    let secrets = secrets(&s)?;
    let idem = idem.keyed(&secrets.request_key().map_err(secret_error)?)?;
    idem.run(&s.db, true, move |_, body| async move {
        let NewValue { value } = strict(&body)?;
        check_value(&value)?;
        let view = secrets.revise(&id, &value).map_err(secret_error)?;
        Ok((StatusCode::OK, view))
    })
    .await
}

async fn list(State(s): State<AppState>) -> ApiResult {
    let all = secrets(&s)?.list().map_err(secret_error)?;
    Ok(Json(json!({"secrets": all})))
}

async fn read(State(s): State<AppState>, Path(id): Path<String>) -> ApiResult {
    Ok(Json(secrets(&s)?.get(&id).map_err(secret_error)?))
}

async fn delete_secret(
    State(s): State<AppState>,
    Path(id): Path<String>,
) -> Result<StatusCode, ApiError> {
    secrets(&s)?.delete(&id).map_err(secret_error)?;
    Ok(StatusCode::NO_CONTENT)
}
