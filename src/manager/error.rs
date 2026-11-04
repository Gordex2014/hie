//! What a manager operation answers when it does not succeed: a status class,
//! a stable code (`source_not_found`, `invalid_definition`, ...), a sentence,
//! and the members the route names beside them. The API turns it into its
//! `{"error", "code", ...}` answer; nothing here knows about HTTP.

use serde_json::{Map, Value};

/// The class of a refusal; the API maps each to its HTTP status.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorStatus {
    /// `400`.
    Invalid,
    /// `403`.
    Forbidden,
    /// `404`.
    NotFound,
    /// `409`.
    Conflict,
    /// `410`.
    Gone,
    /// `413`.
    TooLarge,
    /// `429`.
    Busy,
    /// `503`.
    Unavailable,
    /// `500`: the engine failed, not the request.
    Internal,
}

#[derive(Debug)]
pub struct ManagerError {
    status: ErrorStatus,
    code: &'static str,
    message: String,
    members: Map<String, Value>,
    cause: Option<anyhow::Error>,
}

impl ManagerError {
    pub fn new(status: ErrorStatus, code: &'static str, message: impl Into<String>) -> Self {
        Self {
            status,
            code,
            message: message.into(),
            members: Map::new(),
            cause: None,
        }
    }

    pub fn invalid(code: &'static str, message: impl Into<String>) -> Self {
        Self::new(ErrorStatus::Invalid, code, message)
    }

    pub fn not_found(code: &'static str, message: impl Into<String>) -> Self {
        Self::new(ErrorStatus::NotFound, code, message)
    }

    pub fn conflict(code: &'static str, message: impl Into<String>) -> Self {
        Self::new(ErrorStatus::Conflict, code, message)
    }

    /// Adds a member the route names beside `error` and `code` (`field`, `sources`, ...).
    pub fn with(mut self, name: &str, value: impl Into<Value>) -> Self {
        self.members.insert(name.to_string(), value.into());
        self
    }

    pub fn status(&self) -> ErrorStatus {
        self.status
    }

    pub fn code(&self) -> &'static str {
        self.code
    }

    pub fn message(&self) -> &str {
        &self.message
    }

    pub fn members(&self) -> &Map<String, Value> {
        &self.members
    }

    /// The engine failure behind an `Internal` error.
    pub fn into_cause(self) -> anyhow::Error {
        self.cause
            .unwrap_or_else(|| anyhow::anyhow!("{}", self.message))
    }
}

impl std::fmt::Display for ManagerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.cause {
            Some(cause) => write!(f, "{cause:#}"),
            None => write!(f, "{} ({})", self.message, self.code),
        }
    }
}

impl From<anyhow::Error> for ManagerError {
    fn from(e: anyhow::Error) -> Self {
        Self {
            status: ErrorStatus::Internal,
            code: "internal_error",
            message: "the engine failed to do this".into(),
            members: Map::new(),
            cause: Some(e),
        }
    }
}
