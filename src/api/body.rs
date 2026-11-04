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
///
/// Members are found unknown by serde as it reads the body, and by any
/// `#[serde(deny_unknown_fields)]` type. What serde reads through a buffer
/// (internally tagged and untagged enums, `flatten`) it cannot report: a body
/// with such a shape names its members with [`strict_with`] and
/// [`only_members`] / [`only_tagged_members`].
pub fn strict<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, ApiError> {
    strict_with(bytes, |_| Ok(()))
}

/// [`strict`], with `check` run on the parsed body first: the place to refuse
/// the members of buffered shapes (tagged or untagged enums) that serde cannot
/// see, with [`only_members`] and [`only_tagged_members`].
pub fn strict_with<T: DeserializeOwned>(
    bytes: &[u8],
    check: impl FnOnce(&Value) -> Result<(), ApiError>,
) -> Result<T, ApiError> {
    let value = json_value(bytes)?;
    check(&value)?;
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

/// Refuses (`400 unknown_field`) any member of the object `value` other than
/// `allowed`; `at` is the object's path in the body (`""` for the body itself),
/// so `field` names the member as `at.member`. Anything but an object passes:
/// its shape is the deserializer's to judge.
pub fn only_members(value: &Value, at: &str, allowed: &[&str]) -> Result<(), ApiError> {
    let Some(members) = value.as_object() else {
        return Ok(());
    };
    match members
        .keys()
        .find(|name| !allowed.contains(&name.as_str()))
    {
        Some(name) => Err(ApiError::unknown_field(&join(at, name))),
        None => Ok(()),
    }
}

/// [`only_members`] for an internally tagged object: `variants` lists, per
/// value of the `tag` member, the other members that variant takes. An object
/// without the tag, or with a tag no variant has, passes: the deserializer
/// refuses it with its own reason.
pub fn only_tagged_members(
    value: &Value,
    at: &str,
    tag: &str,
    variants: &[(&str, &[&str])],
) -> Result<(), ApiError> {
    let Some(name) = value.get(tag).and_then(Value::as_str) else {
        return Ok(());
    };
    let Some((_, members)) = variants.iter().find(|(variant, _)| *variant == name) else {
        return Ok(());
    };
    let allowed: Vec<&str> = std::iter::once(tag)
        .chain(members.iter().copied())
        .collect();
    only_members(value, at, &allowed)
}

fn join(at: &str, name: &str) -> String {
    if at.is_empty() || at == "." {
        name.to_string()
    } else {
        format!("{at}.{name}")
    }
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
    // A `deny_unknown_fields` type refused a member: the path ends with it,
    // unless serde read it through a buffer, where the path stops short.
    if let Some(rest) = inner.strip_prefix("unknown field `") {
        let name = rest.split('`').next().unwrap_or_default();
        let field = if path == name || path.ends_with(&format!(".{name}")) {
            path
        } else {
            join(&path, name)
        };
        return ApiError::unknown_field(&field);
    }
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

    #[derive(serde::Deserialize, Debug)]
    #[allow(dead_code)]
    struct Connection {
        password: Option<crate::config::SecretRef>,
    }

    #[test]
    fn types_that_deny_unknown_members_answer_unknown_field() {
        let e =
            strict::<Connection>(br#"{"password": {"secret": "sec_1", "colour": 1}}"#).unwrap_err();
        assert_eq!(
            (e.code(), e.body()["field"].clone()),
            ("unknown_field", "password.colour".into())
        );
    }

    #[derive(serde::Deserialize, Debug)]
    #[allow(dead_code)]
    struct Definition {
        source: crate::config::SourceConfig,
    }

    #[test]
    fn buffered_shapes_name_their_members() {
        let body = br#"{"source": {"type": "folder", "path": "inbox", "colour": "blue"}}"#;
        // serde reads a tagged enum through a buffer and cannot see `colour`.
        assert!(strict::<Definition>(body).is_ok());
        let folder: &[&str] = &[
            "path",
            "pattern",
            "poll_seconds",
            "min_file_age_ms",
            "archive",
        ];
        let check =
            |v: &Value| only_tagged_members(&v["source"], "source", "type", &[("folder", folder)]);
        let e = strict_with::<Definition>(body, check).unwrap_err();
        assert_eq!(
            (e.code(), e.body()["field"].clone()),
            ("unknown_field", "source.colour".into())
        );
        let good = br#"{"source": {"type": "folder", "path": "inbox", "pattern": "*.csv"}}"#;
        assert!(strict_with::<Definition>(good, check).is_ok());
        // An unknown member of the body itself is still serde's to find.
        let extra = br#"{"source": {"type": "folder", "path": "inbox"}, "colour": 1}"#;
        assert_eq!(
            strict_with::<Definition>(extra, check).unwrap_err().body()["field"],
            "colour"
        );
        assert_eq!(
            only_members(&serde_json::json!({"a": 1, "b": 2}), "", &["a"])
                .unwrap_err()
                .body()["field"],
            "b"
        );
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
