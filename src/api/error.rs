//! The error convention every route answers with: `{"error": "<a sentence>",
//! "code": "<stable code>"}` plus the members the route names (`field`,
//! `current_version`, `sources`, ...). Clients branch on `code`; `error` is
//! for people. A route builds one with a constructor and adds members with
//! [`ApiError::with`]; a manager error carries its own code and converts.
//!
//! [`conventions`] holds the rest of the API to it: what the framework answers
//! by itself (a path or query value that does not parse, no such route, a
//! method the route does not take, a body too large) becomes the same shape,
//! with a fixed sentence that never repeats what the request sent.

use axum::extract::Request;
use axum::http::{HeaderName, HeaderValue, StatusCode, header};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use axum::{Json, Router};
use serde_json::{Map, Value};

use crate::manager::{ErrorStatus, ManagerError};

#[derive(Debug)]
pub struct ApiError {
    status: StatusCode,
    code: &'static str,
    error: String,
    members: Map<String, Value>,
    headers: Vec<(HeaderName, HeaderValue)>,
}

impl ApiError {
    pub fn new(status: StatusCode, code: &'static str, error: impl Into<String>) -> Self {
        Self {
            status,
            code,
            error: error.into(),
            members: Map::new(),
            headers: Vec::new(),
        }
    }

    /// `400`: the request is wrong.
    pub fn bad_request(code: &'static str, error: impl Into<String>) -> Self {
        Self::new(StatusCode::BAD_REQUEST, code, error)
    }

    /// `400 invalid_body`.
    pub fn invalid_body(error: impl Into<String>) -> Self {
        Self::bad_request("invalid_body", error)
    }

    /// `400 unknown_field` naming the member a strict route does not define.
    pub fn unknown_field(field: &str) -> Self {
        Self::bad_request("unknown_field", format!("unknown member `{field}`")).with("field", field)
    }

    /// `404` with the code naming what was not found (`load_not_found`, ...).
    pub fn not_found(code: &'static str, error: impl Into<String>) -> Self {
        Self::new(StatusCode::NOT_FOUND, code, error)
    }

    /// `409`.
    pub fn conflict(code: &'static str, error: impl Into<String>) -> Self {
        Self::new(StatusCode::CONFLICT, code, error)
    }

    /// Adds a member the route names beside `error` and `code`.
    pub fn with(mut self, name: &str, value: impl Into<Value>) -> Self {
        self.members.insert(name.to_string(), value.into());
        self
    }

    /// Adds a header to the answer (`Retry-After` on `429 try_busy`, ...).
    pub fn header(mut self, name: HeaderName, value: HeaderValue) -> Self {
        self.headers.push((name, value));
        self
    }

    pub fn status(&self) -> StatusCode {
        self.status
    }

    pub fn code(&self) -> &'static str {
        self.code
    }

    /// The sentence for people.
    pub fn message(&self) -> &str {
        &self.error
    }

    /// The answer's body.
    pub fn body(&self) -> Value {
        let mut body = Map::new();
        body.insert("error".into(), Value::String(self.error.clone()));
        body.insert("code".into(), Value::String(self.code.into()));
        for (name, value) in &self.members {
            body.insert(name.clone(), value.clone());
        }
        Value::Object(body)
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let mut response = (self.status, Json(self.body())).into_response();
        for (name, value) in self.headers {
            response.headers_mut().insert(name, value);
        }
        response
    }
}

impl From<anyhow::Error> for ApiError {
    fn from(e: anyhow::Error) -> Self {
        tracing::error!("API request failed: {e:#}");
        ApiError::new(
            StatusCode::INTERNAL_SERVER_ERROR,
            "internal_error",
            format!("{e:#}"),
        )
    }
}

impl From<ManagerError> for ApiError {
    fn from(e: ManagerError) -> Self {
        let status = match e.status() {
            ErrorStatus::Invalid => StatusCode::BAD_REQUEST,
            ErrorStatus::Forbidden => StatusCode::FORBIDDEN,
            ErrorStatus::NotFound => StatusCode::NOT_FOUND,
            ErrorStatus::Conflict => StatusCode::CONFLICT,
            ErrorStatus::Gone => StatusCode::GONE,
            ErrorStatus::TooLarge => StatusCode::PAYLOAD_TOO_LARGE,
            ErrorStatus::Busy => StatusCode::TOO_MANY_REQUESTS,
            ErrorStatus::Unavailable => StatusCode::SERVICE_UNAVAILABLE,
            ErrorStatus::Internal => return ApiError::from(e.into_cause()),
        };
        let mut error = ApiError::new(status, e.code(), e.message());
        for (name, value) in e.members() {
            error = error.with(name, value.clone());
        }
        error
    }
}

/// Applies the error convention to everything `router` answers: unknown paths
/// are `404 not_found`, and any error the framework answers in its own words
/// is replaced by `{"error", "code"}` (its status and headers, such as
/// `Allow`, are kept).
pub fn conventions<S: Clone + Send + Sync + 'static>(router: Router<S>) -> Router<S> {
    router
        .fallback(|| async { ApiError::not_found("not_found", "nothing is at this path") })
        .layer(axum::middleware::from_fn(normalize))
}

async fn normalize(request: Request, next: Next) -> Response {
    let response = next.run(request).await;
    let status = response.status();
    let is_json = response
        .headers()
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| v.starts_with("application/json"));
    if !(status.is_client_error() || status.is_server_error()) || is_json {
        return response;
    }
    let (code, sentence) = match status {
        StatusCode::BAD_REQUEST
        | StatusCode::UNPROCESSABLE_ENTITY
        | StatusCode::UNSUPPORTED_MEDIA_TYPE => (
            "invalid_body",
            "the request's path, query or body is not one this route takes",
        ),
        StatusCode::UNAUTHORIZED => ("unauthorized", "a bearer token is required"),
        StatusCode::FORBIDDEN => ("forbidden", "the token may not do this"),
        StatusCode::NOT_FOUND => ("not_found", "nothing is at this path"),
        StatusCode::METHOD_NOT_ALLOWED => {
            ("method_not_allowed", "this path does not take this method")
        }
        StatusCode::PAYLOAD_TOO_LARGE => ("body_too_large", "the body is too large"),
        s if s.is_client_error() => ("invalid_request", "the request is not one this route takes"),
        _ => ("internal_error", "the engine failed to answer"),
    };
    // The original answer keeps its status and headers (every value of a
    // repeated one); only the body and the headers describing it change.
    let (mut parts, _) = response.into_parts();
    let (normalized, body) = ApiError::new(status, code, sentence)
        .into_response()
        .into_parts();
    for name in [
        header::CONTENT_LENGTH,
        header::CONTENT_ENCODING,
        header::CONTENT_TYPE,
    ] {
        parts.headers.remove(&name);
    }
    if let Some(content_type) = normalized.headers.get(header::CONTENT_TYPE) {
        parts
            .headers
            .insert(header::CONTENT_TYPE, content_type.clone());
    }
    Response::from_parts(parts, body)
}

#[cfg(test)]
mod tests {
    use axum::extract::{Path, Query};
    use axum::routing::get;

    use super::*;

    #[derive(serde::Deserialize)]
    #[allow(dead_code)]
    struct Limit {
        limit: Option<i64>,
    }

    async fn serve(router: Router) -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
        format!("http://{addr}")
    }

    #[tokio::test]
    async fn the_frameworks_own_answers_follow_the_convention() {
        let app = conventions(
            Router::new()
                .route(
                    "/records/{id}",
                    get(|Path(id): Path<i64>| async move { id.to_string() }),
                )
                .route("/records", get(|Query(_): Query<Limit>| async { "ok" }))
                .route(
                    "/cached",
                    get(|| async {
                        let mut response = (StatusCode::BAD_REQUEST, "plain text").into_response();
                        let headers = response.headers_mut();
                        headers.append(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
                        headers.append(header::CACHE_CONTROL, HeaderValue::from_static("private"));
                        headers.append(header::SET_COOKIE, HeaderValue::from_static("a=1"));
                        headers.append(header::SET_COOKIE, HeaderValue::from_static("b=2"));
                        response
                    }),
                )
                .route(
                    "/refused",
                    get(|| async { ApiError::not_found("record_not_found", "record 1 not found") }),
                ),
        );
        let base = serve(app).await;
        let http = reqwest::Client::new();
        for (method, path, status, code) in [
            ("GET", "/records/Secret-Path-Value", 400, "invalid_body"),
            (
                "GET",
                "/records?limit=Secret-Query-Value",
                400,
                "invalid_body",
            ),
            ("GET", "/nowhere/Secret-Path-Value", 404, "not_found"),
            ("DELETE", "/records/1", 405, "method_not_allowed"),
            ("GET", "/refused", 404, "record_not_found"),
        ] {
            let res = http
                .request(method.parse().unwrap(), format!("{base}{path}"))
                .send()
                .await
                .unwrap();
            assert_eq!(res.status().as_u16(), status, "{method} {path}");
            if status == 405 {
                assert!(res.headers().contains_key(header::ALLOW));
            }
            let text = res.text().await.unwrap();
            assert!(!text.contains("Secret"), "{text}");
            let body: Value = serde_json::from_str(&text).unwrap();
            assert_eq!(body["code"], code, "{method} {path}: {body}");
            assert!(body["error"].is_string());
        }
        let res = http.get(format!("{base}/cached")).send().await.unwrap();
        let values = |name| -> Vec<String> {
            res.headers()
                .get_all(name)
                .iter()
                .map(|v| v.to_str().unwrap().to_string())
                .collect()
        };
        assert_eq!(values(header::CACHE_CONTROL), ["no-store", "private"]);
        assert_eq!(values(header::SET_COOKIE), ["a=1", "b=2"]);
        assert_eq!(values(header::CONTENT_TYPE), ["application/json"]);
        assert_eq!(res.json::<Value>().await.unwrap()["code"], "invalid_body");
        assert_eq!(
            http.get(format!("{base}/records/7"))
                .send()
                .await
                .unwrap()
                .text()
                .await
                .unwrap(),
            "7"
        );
    }
}
