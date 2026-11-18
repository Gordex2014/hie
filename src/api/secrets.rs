//! `/api/secrets`: connector credentials. Values go in, metadata comes out; no
//! answer ever carries a value.
//!
//! Bodies are read as bytes and parsed here, never by the `Json` extractor,
//! whose rejection would carry serde's message (and the input it quotes) into
//! the trace log. A body that does not parse gets one fixed sentence.

use std::sync::Arc;

use axum::Json;
use axum::body::Bytes;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use secrecy::SecretString;
use serde::Deserialize;
use serde_json::{Value, json};
use zeroize::Zeroizing;

use super::{ApiError, AppState};
use crate::secrets::{
    NewSecret, SecretError, SecretKind, SecretMeta, SecretStore, SecretValue, check_name, is_id,
};

const CREATE_SHAPE: &str = r#"the body must be a JSON object {"name", "kind": "aws", "value": {"access_key_id", "secret_access_key"}} and nothing else"#;
const REPLACE_SHAPE: &str = r#"the body must be a JSON object {"value": {"access_key_id", "secret_access_key"}} and nothing else"#;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CreateBody {
    name: Option<String>,
    kind: Option<String>,
    value: Option<ValueBody>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ReplaceBody {
    value: Option<ValueBody>,
}

/// An `aws` value as submitted; each field is wiped on drop.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ValueBody {
    access_key_id: Option<Zeroizing<String>>,
    secret_access_key: Option<Zeroizing<String>>,
}

impl ValueBody {
    fn into_value(self, kind: SecretKind) -> Result<SecretValue, SecretError> {
        let fields = [
            ("access_key_id", self.access_key_id),
            ("secret_access_key", self.secret_access_key),
        ]
        .into_iter()
        .filter_map(|(name, text)| {
            text.map(|text| (name.to_string(), SecretString::from(exact(&text))))
        });
        SecretValue::new(kind, fields)
    }
}

/// A copy exactly as long as `text`, so the secret string made from it keeps
/// its buffer (`into_boxed_str` does not reallocate) and no other copy is left.
fn exact(text: &str) -> String {
    let mut copy = String::with_capacity(text.len());
    copy.push_str(text);
    copy
}

fn invalid_body(sentence: &str) -> ApiError {
    ApiError::new(StatusCode::BAD_REQUEST, sentence).code("invalid_body")
}

/// The answer for a store error. Store and integrity errors are logged and
/// answered without their detail.
fn answer(e: SecretError) -> ApiError {
    match e {
        SecretError::Unavailable(reason) => unavailable(reason),
        SecretError::NotFound => {
            ApiError::new(StatusCode::NOT_FOUND, "credential not found").code("secret_not_found")
        }
        SecretError::NameInUse => ApiError::new(
            StatusCode::CONFLICT,
            "a credential with this name already exists",
        )
        .code("name_in_use"),
        SecretError::InUse(sources) => ApiError::new(
            StatusCode::CONFLICT,
            format!("sources use this credential: {}", sources.join(", ")),
        )
        .code("secret_in_use")
        .with("sources", sources),
        SecretError::Busy => ApiError::store_busy(),
        SecretError::Invalid { field, message } => {
            invalid_body(&format!("{field}: {message}")).with("field", field)
        }
        e @ (SecretError::Integrity(_) | SecretError::Store(_)) => {
            tracing::error!("credential request failed: {e}");
            ApiError::new(
                StatusCode::INTERNAL_SERVER_ERROR,
                "the engine failed to do this; its log has the reason",
            )
            .code("internal_error")
        }
    }
}

fn unavailable(reason: String) -> ApiError {
    ApiError::new(StatusCode::SERVICE_UNAVAILABLE, reason).code("secrets_unavailable")
}

/// The store, or the 503 that says why there is none (rechecked once per
/// process, so the reason is current).
async fn store(s: &AppState) -> Result<Arc<dyn SecretStore>, ApiError> {
    if let Ok(store) = s.secrets.store() {
        return Ok(store.clone());
    }
    s.secrets.recheck_once().await;
    s.secrets.store().cloned().map_err(answer)
}

fn view(meta: &SecretMeta) -> Result<Value, ApiError> {
    serde_json::to_value(meta).map_err(|e| ApiError::from(anyhow::Error::from(e)))
}

pub(super) async fn create_secret(
    State(s): State<AppState>,
    body: Bytes,
) -> Result<(StatusCode, Json<Value>), ApiError> {
    let store = store(&s).await?;
    let body: CreateBody = serde_json::from_slice(&body).map_err(|_| invalid_body(CREATE_SHAPE))?;
    let name = body.name.ok_or_else(|| answer(required("name")))?;
    check_name(&name).map_err(answer)?;
    let kind = match body.kind.as_deref() {
        None => return Err(answer(required("kind"))),
        Some(kind) => SecretKind::parse(kind).ok_or_else(|| {
            answer(SecretError::Invalid {
                field: "kind",
                message: "must be aws".into(),
            })
        })?,
    };
    let value = body
        .value
        .ok_or_else(|| answer(required("value")))?
        .into_value(kind)
        .map_err(answer)?;
    let meta = store
        .create(NewSecret { name, kind, value })
        .await
        .map_err(answer)?;
    tracing::info!(credential = %meta.name, id = %meta.id, "credential stored");
    Ok((StatusCode::CREATED, Json(view(&meta)?)))
}

pub(super) async fn replace_secret(
    State(s): State<AppState>,
    Path(id): Path<String>,
    body: Bytes,
) -> Result<Json<Value>, ApiError> {
    let store = store(&s).await?;
    let body: ReplaceBody =
        serde_json::from_slice(&body).map_err(|_| invalid_body(REPLACE_SHAPE))?;
    if !is_id(&id) {
        return Err(answer(SecretError::NotFound));
    }
    let kind = store
        .find(&id)
        .await
        .map_err(answer)?
        .ok_or_else(|| answer(SecretError::NotFound))?
        .kind;
    let value = body
        .value
        .ok_or_else(|| answer(required("value")))?
        .into_value(kind)
        .map_err(answer)?;
    let meta = store.replace(&id, value).await.map_err(answer)?;
    tracing::info!(credential = %meta.name, id = %meta.id, revision = meta.revision, "credential replaced");
    Ok(Json(view(&meta)?))
}

pub(super) async fn list_secrets(State(s): State<AppState>) -> Result<Json<Value>, ApiError> {
    let store = store(&s).await?;
    let secrets = store.list().await.map_err(answer)?;
    let secrets: Result<Vec<Value>, ApiError> = secrets.iter().map(view).collect();
    Ok(Json(json!({"secrets": secrets?})))
}

pub(super) async fn get_secret(
    State(s): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<Value>, ApiError> {
    let store = store(&s).await?;
    if !is_id(&id) {
        return Err(answer(SecretError::NotFound));
    }
    let meta = store
        .find(&id)
        .await
        .map_err(answer)?
        .ok_or_else(|| answer(SecretError::NotFound))?;
    Ok(Json(view(&meta)?))
}

pub(super) async fn delete_secret(
    State(s): State<AppState>,
    Path(id): Path<String>,
) -> Result<StatusCode, ApiError> {
    let store = store(&s).await?;
    store.delete(&id).await.map_err(answer)?;
    tracing::info!(id = %id, "credential deleted");
    Ok(StatusCode::NO_CONTENT)
}

fn required(field: &'static str) -> SecretError {
    SecretError::Invalid {
        field,
        message: "is required".into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_submitted_value_is_copied_once_at_its_exact_length() {
        let body: ValueBody = serde_json::from_str(
            r#"{"access_key_id": "AKIDEXAMPLE", "secret_access_key": "escaped\/value"}"#,
        )
        .unwrap();
        let text = body.secret_access_key.unwrap();
        let copy = exact(&text);
        assert_eq!(copy, "escaped/value");
        assert_eq!(copy.capacity(), copy.len());
    }

    #[test]
    fn unknown_members_are_refused_at_every_level() {
        assert!(serde_json::from_str::<CreateBody>(r#"{"name": "a", "extra": 1}"#).is_err());
        assert!(
            serde_json::from_str::<ReplaceBody>(
                r#"{"value": {"access_key_id": "a", "secret_access_key": "b", "session_token": "c"}}"#
            )
            .is_err()
        );
    }
}
