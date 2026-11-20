use std::sync::Arc;
use std::time::Duration;

use serde_json::{Value, json};

use crate::credentials::Credentials;
use crate::destination::Destination;

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
    destination: Arc<Destination>,
}

/// How long one HTTP call to FHIR may take, `$bulk-submit` calls included.
pub const HTTP_TIMEOUT: Duration = Duration::from_secs(120);

impl FhirClient {
    /// A client that calls HFS anonymously.
    pub fn new(base: &str) -> Self {
        Self::with_credentials(base, None)
    }

    pub fn with_credentials(base: &str, credentials: Credentials) -> Self {
        let http = reqwest::Client::builder()
            .timeout(HTTP_TIMEOUT)
            .build()
            .expect("HTTP client");
        Self {
            base: base.trim_end_matches('/').to_string(),
            http,
            credentials,
            destination: Arc::new(Destination::new(base)),
        }
    }

    pub fn credentials(&self) -> &Credentials {
        &self.credentials
    }

    /// Whether the server answers, as this client and the probe saw it.
    pub fn destination(&self) -> &Arc<Destination> {
        &self.destination
    }

    /// Posts a batch Bundle. A failure to reach the server is told with the
    /// destination's fixed sentence, never the URL or a library's error text:
    /// it reaches the record's error and the delivery alert. The log has the
    /// detail.
    pub async fn post_bundle(&self, bundle: &Value) -> Result<Value, SendError> {
        let request = self
            .http
            .post(&self.base)
            .header("Content-Type", "application/fhir+json")
            .header("Accept", "application/fhir+json")
            .json(bundle);
        // Built before it counts as a request to the server: one that cannot
        // be built never leaves the engine.
        let request = authorize(request, &self.credentials)
            .await?
            .build()
            .map_err(|e| {
                tracing::warn!(
                    "a request to the FHIR server could not be built: {}",
                    crate::destination::send_detail(&e)
                );
                SendError::Transient("the request to the FHIR server could not be built".into())
            })?;
        // Held until the answer has been read: the destination is not idle
        // while it downloads.
        let exchange = self.destination.start();
        let sent = self.http.execute(request).await;
        exchange.observe_send(&sent);
        let resp = sent.map_err(|e| {
            SendError::Transient(
                crate::destination::send_failure(&e)
                    .map_or("the request to the FHIR server failed", |f| f.sentence())
                    .into(),
            )
        })?;
        let status = resp.status().as_u16();
        let body: Value = resp.json().await.unwrap_or(Value::Null);
        drop(exchange);
        match status {
            200..=299 => Ok(body),
            429 | 500..=599 => Err(SendError::Transient(format!(
                "HTTP {status} from the FHIR server"
            ))),
            401 | 403 if self.credentials.is_some() => {
                Err(refused_token(status, &self.credentials).await)
            }
            _ => Err(SendError::Refused(status, body)),
        }
    }
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
