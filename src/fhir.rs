use std::time::Duration;

use serde_json::{Value, json};

use crate::credentials::Credentials;

/// Why a Bundle could not be delivered as a whole.
#[derive(Debug, Clone, PartialEq)]
pub enum SendError {
    /// Connection failure, timeout, 429 or 5xx: try again later.
    Transient(String),
    /// The server refused the Bundle itself (4xx).
    Refused(u16, Value),
}

/// The fate of one entry in a batch-response.
#[derive(Debug, Clone, PartialEq)]
pub enum EntryResult {
    Ok(Value),
    Rejected(u16, Value),
    Transient(u16, Value),
}

pub struct FhirClient {
    base: String,
    http: reqwest::Client,
    credentials: Credentials,
}

impl FhirClient {
    /// A client that calls HFS anonymously.
    pub fn new(base: &str) -> Self {
        Self::with_credentials(base, None)
    }

    pub fn with_credentials(base: &str, credentials: Credentials) -> Self {
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(120))
            .build()
            .expect("HTTP client");
        Self {
            base: base.trim_end_matches('/').to_string(),
            http,
            credentials,
        }
    }

    pub fn credentials(&self) -> &Credentials {
        &self.credentials
    }

    pub async fn post_bundle(&self, bundle: &Value) -> Result<Value, SendError> {
        let request = self
            .http
            .post(&self.base)
            .header("Content-Type", "application/fhir+json")
            .header("Accept", "application/fhir+json")
            .json(bundle);
        let resp = authorize(request, &self.credentials)
            .await?
            .send()
            .await
            .map_err(|e| SendError::Transient(format!("cannot reach {}: {e}", self.base)))?;
        let status = resp.status().as_u16();
        let body: Value = resp.json().await.unwrap_or(Value::Null);
        match status {
            200..=299 => Ok(body),
            429 | 500..=599 => Err(SendError::Transient(format!(
                "HTTP {status} from {}",
                self.base
            ))),
            401 | 403 if self.credentials.is_some() => {
                Err(refused_token(status, &self.credentials).await)
            }
            _ => Err(SendError::Refused(status, body)),
        }
    }
}

/// What the destination's `$validate` said of one resource.
#[derive(Debug, Clone, PartialEq)]
pub enum Validated {
    /// An `OperationOutcome`: its issues (none, or only warnings, when valid).
    Outcome { issues: Vec<Issue> },
    /// The destination could not validate: unreachable, a timeout, a `5xx`, or
    /// an answer that is not an `OperationOutcome`. Never a verdict on the resource.
    Unavailable,
}

impl Validated {
    /// Whether the resource passed: an outcome without `error` or `fatal` issues.
    pub fn is_valid(&self) -> bool {
        matches!(self, Validated::Outcome { issues } if !issues.iter().any(Issue::is_error))
    }
}

/// One issue of an `OperationOutcome`, as Try answers it.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Issue {
    /// `fatal`, `error`, `warning` or `information`.
    pub severity: String,
    pub code: String,
    #[serde(default)]
    pub diagnostics: Option<String>,
    #[serde(default)]
    pub expression: Vec<String>,
}

impl Issue {
    /// An `error` or `fatal` issue: the resource fails.
    pub fn is_error(&self) -> bool {
        matches!(self.severity.as_str(), "error" | "fatal")
    }
}

/// The destination's facts from its CapabilityStatement (`GET /metadata`).
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct Metadata {
    pub fhir_version: Option<String>,
    pub software: Option<Software>,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct Software {
    pub name: Option<String>,
    pub version: Option<String>,
}

/// How long a `$validate` may take before the destination counts as unable.
const VALIDATE_TIMEOUT: Duration = Duration::from_secs(30);
const METADATA_TIMEOUT: Duration = Duration::from_secs(10);

impl FhirClient {
    /// Checks one resource with the destination's `POST {base}/{type}/$validate`.
    pub async fn validate(&self, resource: &Value) -> Validated {
        let Some(resource_type) = resource
            .get("resourceType")
            .and_then(Value::as_str)
            .filter(|t| !t.is_empty() && t.bytes().all(|b| b.is_ascii_alphanumeric()))
        else {
            return Validated::Outcome {
                issues: vec![Issue {
                    severity: "error".into(),
                    code: "structure".into(),
                    diagnostics: Some("the resource has no resourceType".into()),
                    expression: vec![],
                }],
            };
        };
        let request = self
            .http
            .post(format!("{}/{resource_type}/$validate", self.base))
            .timeout(VALIDATE_TIMEOUT)
            .header("Content-Type", "application/fhir+json")
            .header("Accept", "application/fhir+json")
            .json(resource);
        let Ok(request) = authorize(request, &self.credentials).await else {
            return Validated::Unavailable;
        };
        let response = match request.send().await {
            Ok(r) => r,
            Err(e) => {
                tracing::warn!("$validate at {} unavailable: {e}", self.base);
                return Validated::Unavailable;
            }
        };
        let status = response.status().as_u16();
        if matches!(status, 401 | 403) && self.credentials.is_some() {
            refused_token(status, &self.credentials).await;
            return Validated::Unavailable;
        }
        if status >= 500 {
            tracing::warn!("$validate at {} answered HTTP {status}", self.base);
            return Validated::Unavailable;
        }
        // A 4xx with an OperationOutcome is a verdict (HFS answers 400 for an
        // invalid resource); anything that is not one is not.
        match response.json::<Value>().await {
            Ok(body)
                if body.get("resourceType").and_then(Value::as_str) == Some("OperationOutcome") =>
            {
                Validated::Outcome {
                    issues: issues(&body),
                }
            }
            _ => {
                tracing::warn!(
                    "$validate at {} did not answer an OperationOutcome (HTTP {status})",
                    self.base
                );
                Validated::Unavailable
            }
        }
    }

    /// The destination's facts from `GET {base}/metadata`.
    pub async fn metadata(&self) -> anyhow::Result<Metadata> {
        let request = self
            .http
            .get(format!("{}/metadata", self.base))
            .timeout(METADATA_TIMEOUT)
            .header("Accept", "application/fhir+json");
        let request = authorize(request, &self.credentials)
            .await
            .map_err(|e| anyhow::anyhow!("no token for {}: {e:?}", self.base))?;
        let response = request.send().await?.error_for_status()?;
        let body: Value = response.json().await?;
        if body.get("resourceType").and_then(Value::as_str) != Some("CapabilityStatement") {
            anyhow::bail!("{}/metadata is not a CapabilityStatement", self.base);
        }
        let text = |v: Option<&Value>| v.and_then(Value::as_str).map(str::to_string);
        Ok(Metadata {
            fhir_version: text(body.get("fhirVersion")),
            software: body.get("software").map(|s| Software {
                name: text(s.get("name")),
                version: text(s.get("version")),
            }),
        })
    }

    /// The base URL calls go to.
    pub fn base_url(&self) -> &str {
        &self.base
    }
}

/// The issues of an `OperationOutcome`.
fn issues(outcome: &Value) -> Vec<Issue> {
    outcome
        .get("issue")
        .and_then(Value::as_array)
        .map(|issues| {
            issues
                .iter()
                .map(|i| Issue {
                    severity: i
                        .get("severity")
                        .and_then(Value::as_str)
                        .unwrap_or("error")
                        .to_string(),
                    code: i
                        .get("code")
                        .and_then(Value::as_str)
                        .unwrap_or("unknown")
                        .to_string(),
                    diagnostics: i
                        .get("diagnostics")
                        .or_else(|| i.pointer("/details/text"))
                        .and_then(Value::as_str)
                        .map(str::to_string),
                    expression: i
                        .get("expression")
                        .or_else(|| i.get("location"))
                        .and_then(Value::as_array)
                        .map(|e| {
                            e.iter()
                                .filter_map(Value::as_str)
                                .map(str::to_string)
                                .collect()
                        })
                        .unwrap_or_default(),
                })
                .collect()
        })
        .unwrap_or_default()
}

/// Adds the engine's bearer token to a request towards HFS, when it has one.
pub async fn authorize(
    request: reqwest::RequestBuilder,
    credentials: &Credentials,
) -> Result<reqwest::RequestBuilder, SendError> {
    match credentials {
        Some(source) => Ok(request.bearer_auth(source.bearer().await?)),
        None => Ok(request),
    }
}

/// HFS did not accept the engine's token: forget it and try again later with a
/// fresh one. The data waits rather than being rejected.
pub async fn refused_token(status: u16, credentials: &Credentials) -> SendError {
    if let Some(source) = credentials {
        source.invalidate().await;
    }
    SendError::Transient(format!(
        "HTTP {status}: HFS did not accept the engine's token; a new one will be requested"
    ))
}

/// Encodes a search value for a conditional URL, keeping the `|` separator literal.
fn encode_component(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        if b.is_ascii_alphanumeric() || b"-._~:/".contains(&b) {
            out.push(b as char);
        } else {
            out.push_str(&format!("%{b:02X}"));
        }
    }
    out
}

pub fn conditional_url(resource_type: &str, system: &str, value: &str) -> String {
    format!(
        "{resource_type}?identifier={}|{}",
        encode_component(system),
        encode_component(value)
    )
}

/// What makes a resource's delivery idempotent: its own id, or a business identifier.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DeliveryKey {
    /// The resource carries an `id`: `PUT {type}/{id}`.
    Id { resource_type: String, id: String },
    /// `PUT {type}?identifier={system}|{value}`.
    Identifier {
        resource_type: String,
        system: String,
        value: String,
    },
}

pub fn request_url(key: &DeliveryKey) -> String {
    match key {
        DeliveryKey::Id { resource_type, id } => {
            format!("{resource_type}/{}", encode_component(id))
        }
        DeliveryKey::Identifier {
            resource_type,
            system,
            value,
        } => conditional_url(resource_type, system, value),
    }
}

/// Builds a `batch` Bundle of conditional updates: an entry fails alone, and sending
/// the same record twice updates the resource instead of creating a second one.
pub fn batch_bundle(entries: &[(String, Value)]) -> Value {
    let entry: Vec<Value> = entries
        .iter()
        .map(|(url, resource)| json!({"resource": resource, "request": {"method": "PUT", "url": url}}))
        .collect();
    json!({"resourceType": "Bundle", "type": "batch", "entry": entry})
}

pub fn entry_results(response: &Value, expected: usize) -> Vec<EntryResult> {
    let entries = response
        .get("entry")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    (0..expected)
        .map(|i| {
            let Some(entry) = entries.get(i) else {
                return EntryResult::Transient(
                    0,
                    json!({"error": "missing entry in batch-response"}),
                );
            };
            let resp = entry.get("response").cloned().unwrap_or(Value::Null);
            let status = resp
                .get("status")
                .and_then(Value::as_str)
                .and_then(|s| s.split_whitespace().next())
                .and_then(|s| s.parse::<u16>().ok())
                .unwrap_or(0);
            match status {
                200..=299 => EntryResult::Ok(resp),
                429 | 500..=599 | 0 => EntryResult::Transient(status, resp),
                _ => EntryResult::Rejected(status, resp),
            }
        })
        .collect()
}

/// A short, human-readable reason from an OperationOutcome (or anything else).
pub fn outcome_text(v: &Value) -> String {
    let outcome = v.get("outcome").unwrap_or(v);
    let texts: Vec<String> = outcome
        .get("issue")
        .and_then(Value::as_array)
        .map(|issues| {
            issues
                .iter()
                .filter_map(|i| {
                    i.pointer("/details/text")
                        .or_else(|| i.get("diagnostics"))
                        .and_then(Value::as_str)
                        .map(str::to_string)
                })
                .collect()
        })
        .unwrap_or_default();
    if texts.is_empty() {
        v.to_string()
    } else {
        texts.join("; ")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn conditional_urls_encode_values_but_not_the_separator() {
        assert_eq!(
            conditional_url(
                "Observation",
                "urn:helios:hie:lab-results",
                "Patient/1 2345-7&x"
            ),
            "Observation?identifier=urn:helios:hie:lab-results|Patient/1%202345-7%26x"
        );
    }

    #[test]
    fn batch_entries_are_classified_by_status() {
        let resp = json!({"entry": [
            {"response": {"status": "201 Created"}},
            {"response": {"status": "400 Bad Request", "outcome": {"issue": [{"details": {"text": "bad status"}}]}}},
            {"response": {"status": "503 Service Unavailable"}}
        ]});
        let r = entry_results(&resp, 4);
        assert!(matches!(r[0], EntryResult::Ok(_)));
        assert!(matches!(r[1], EntryResult::Rejected(400, _)));
        assert!(matches!(r[2], EntryResult::Transient(503, _)));
        assert!(matches!(r[3], EntryResult::Transient(0, _)));
        if let EntryResult::Rejected(_, v) = &r[1] {
            assert_eq!(outcome_text(v), "bad status");
        }
    }
}
