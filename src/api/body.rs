//! Request bodies. 0.2.0 routes take theirs strictly ([`StrictJson`], or
//! [`strict`] on bytes a route already holds): a member the route does not
//! define is `400 unknown_field` with `field`. Legacy routes take theirs
//! loosely ([`LooseJson`]) and ignore unknown members, as in 0.1. Either way a
//! body that cannot be read is `400 invalid_body` with a sentence that names
//! at most a member's path, never a value from the body: a body may hold a
//! secret.

use axum::body::Bytes;
use axum::extract::{FromRequest, OptionalFromRequest, Request};
use serde::de::DeserializeOwned;
use serde_json::Value;

use super::error::ApiError;

/// A body taken strictly: unknown members are refused.
pub struct StrictJson<T>(pub T);

/// A body taken as 0.1 routes take it: unknown members are ignored.
pub struct LooseJson<T>(pub T);

/// Parses a 0.2.0 route's body: `400 invalid_body` when it is not JSON or does
/// not have the route's shape, `400 unknown_field` (with `field`, the member's
/// path such as `input.limit`) when it holds a member the route does not define.
pub fn strict<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, ApiError> {
    let value = json_value(bytes)?;
    let mut unknown: Option<String> = None;
    let result = {
        let mut seen = |path: serde_ignored::Path| {
            if unknown.is_none() {
                unknown = Some(render(&path));
            }
        };
        let de = serde_ignored::Deserializer::new(&value, &mut seen);
        serde_path_to_error::deserialize::<_, T>(de)
    };
    if let Some(field) = unknown {
        return Err(ApiError::unknown_field(&field));
    }
    result.map_err(shape_error)
}

/// Parses a legacy route's body: unknown members are ignored.
pub fn loose<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, ApiError> {
    let value = json_value(bytes)?;
    serde_path_to_error::deserialize::<_, T>(&value).map_err(shape_error)
}

fn json_value(bytes: &[u8]) -> Result<Value, ApiError> {
    serde_json::from_slice(bytes).map_err(|_| ApiError::invalid_body("the body is not valid JSON"))
}

/// A sentence for a body of the wrong shape, without any value from it.
fn shape_error(e: serde_path_to_error::Error<serde_json::Error>) -> ApiError {
    let path = e.path().to_string();
    let inner = e.inner().to_string();
    // serde's own sentence for a missing member names only the member.
    if let Some(rest) = inner.strip_prefix("missing field ") {
        let field = rest.split_whitespace().next().unwrap_or(rest);
        let field = field.trim_matches('`');
        let at = if path == "." {
            String::new()
        } else {
            format!(" in `{path}`")
        };
        return ApiError::invalid_body(format!("member `{field}` is required{at}"))
            .with("field", field);
    }
    if path == "." {
        return ApiError::invalid_body("the body does not have the shape this route takes");
    }
    ApiError::invalid_body(format!("member `{path}` has the wrong type or value"))
        .with("field", path)
}

/// `a.b[0].c`, without serde_ignored's `?` for options and newtypes.
fn render(path: &serde_ignored::Path) -> String {
    use serde_ignored::Path;
    match path {
        Path::Root => String::new(),
        Path::Seq { parent, index } => format!("{}[{index}]", render(parent)),
        Path::Map { parent, key } => {
            let parent = render(parent);
            if parent.is_empty() {
                key.clone()
            } else {
                format!("{parent}.{key}")
            }
        }
        Path::Some { parent }
        | Path::NewtypeStruct { parent }
        | Path::NewtypeVariant { parent } => render(parent),
    }
}

async fn bytes<S: Send + Sync>(req: Request, state: &S) -> Result<Bytes, ApiError> {
    Bytes::from_request(req, state)
        .await
        .map_err(|_| ApiError::invalid_body("the body could not be read"))
}

fn is_empty(bytes: &[u8]) -> bool {
    bytes.iter().all(u8::is_ascii_whitespace)
}

impl<S: Send + Sync, T: DeserializeOwned> FromRequest<S> for StrictJson<T> {
    type Rejection = ApiError;

    async fn from_request(req: Request, state: &S) -> Result<Self, Self::Rejection> {
        let bytes = bytes(req, state).await?;
        strict(&bytes).map(StrictJson)
    }
}

/// `Option<StrictJson<T>>`: no body (or only whitespace) is `None`.
impl<S: Send + Sync, T: DeserializeOwned> OptionalFromRequest<S> for StrictJson<T> {
    type Rejection = ApiError;

    async fn from_request(req: Request, state: &S) -> Result<Option<Self>, Self::Rejection> {
        let bytes = bytes(req, state).await?;
        if is_empty(&bytes) {
            return Ok(None);
        }
        strict(&bytes).map(|t| Some(StrictJson(t)))
    }
}

impl<S: Send + Sync, T: DeserializeOwned> FromRequest<S> for LooseJson<T> {
    type Rejection = ApiError;

    async fn from_request(req: Request, state: &S) -> Result<Self, Self::Rejection> {
        let bytes = bytes(req, state).await?;
        loose(&bytes).map(LooseJson)
    }
}

/// `Option<LooseJson<T>>`: no body (or only whitespace) is `None`.
impl<S: Send + Sync, T: DeserializeOwned> OptionalFromRequest<S> for LooseJson<T> {
    type Rejection = ApiError;

    async fn from_request(req: Request, state: &S) -> Result<Option<Self>, Self::Rejection> {
        let bytes = bytes(req, state).await?;
        if is_empty(&bytes) {
            return Ok(None);
        }
        loose(&bytes).map(|t| Some(LooseJson(t)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(serde::Deserialize, Debug)]
    #[allow(dead_code)]
    struct Body {
        name: String,
        input: Option<Input>,
    }

    #[derive(serde::Deserialize, Debug)]
    #[allow(dead_code)]
    struct Input {
        records: Vec<Value>,
    }

    #[test]
    fn strict_bodies_refuse_unknown_members_by_path() {
        let e = strict::<Body>(br#"{"name": "x", "colour": "blue"}"#).unwrap_err();
        assert_eq!(
            (e.code(), e.body()["field"].clone()),
            ("unknown_field", "colour".into())
        );
        let e = strict::<Body>(br#"{"name": "x", "input": {"records": [{"a": 1}], "limit": 2}}"#)
            .unwrap_err();
        assert_eq!(e.body()["field"], "input.limit");
        // Members inside free-form values are not the route's to refuse.
        assert!(strict::<Body>(br#"{"name": "x", "input": {"records": [{"any": 1}]}}"#).is_ok());
        assert!(loose::<Body>(br#"{"name": "x", "colour": "blue"}"#).is_ok());
    }

    #[test]
    fn unreadable_bodies_are_never_echoed() {
        for body in [
            &br#"{"name": "Corpus-Secret-1!""#[..],
            br#"{"name": ["Corpus-Secret-1!"]}"#,
            br#"["Corpus-Secret-1!"]"#,
            br#"{"input": {"records": "Corpus-Secret-1!"}, "name": "x"}"#,
        ] {
            let e = strict::<Body>(body).unwrap_err();
            assert_eq!(e.code(), "invalid_body");
            assert!(
                !e.body().to_string().contains("Corpus-Secret"),
                "{}",
                e.body()
            );
        }
        let e = strict::<Body>(br#"{"input": null}"#).unwrap_err();
        assert_eq!(
            (e.code(), e.body()["field"].clone()),
            ("invalid_body", "name".into())
        );
    }
}
