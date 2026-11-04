//! The error convention every route answers with: `{"error": "<a sentence>",
//! "code": "<stable code>"}` plus the members the route names (`field`,
//! `current_version`, `sources`, ...). Clients branch on `code`; `error` is
//! for people. A route builds one with a constructor and adds members with
//! [`ApiError::with`]; a manager error carries its own code and converts.

use axum::Json;
use axum::http::{HeaderName, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
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
