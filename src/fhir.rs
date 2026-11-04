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
        match self.send_bundle(bundle).await {
            BundleAnswer::Answered { status, body } => match status {
                200..=299 => Ok(body),
                _ => Err(SendError::Refused(status, body)),
            },
            BundleAnswer::Unsent(reason) | BundleAnswer::Lost(reason) => {
                Err(SendError::Transient(reason))
            }
        }
    }

    /// Sends a Bundle and says what is known of it: answered (2xx or a
    /// definitive 4xx), never sent (the connection or the token failed before
    /// the request left), or lost (a timeout, a broken answer, a `429` or a
    /// `5xx`: the destination may have applied it).
    pub async fn send_bundle(&self, bundle: &Value) -> BundleAnswer {
        let request = self
            .http
            .post(&self.base)
            .header("Content-Type", "application/fhir+json")
            .header("Accept", "application/fhir+json")
            .json(bundle);
        let request = match authorize(request, &self.credentials).await {
            Ok(r) => r,
            Err(SendError::Transient(reason)) => return BundleAnswer::Unsent(reason),
            Err(SendError::Refused(status, _)) => {
                return BundleAnswer::Unsent(format!("HTTP {status} getting a token"));
            }
        };
        let resp = match request.send().await {
            Ok(r) => r,
            Err(e) if e.is_connect() => {
                return BundleAnswer::Unsent(format!("cannot reach {}: {e}", self.base));
            }
            Err(e) => return BundleAnswer::Lost(format!("no answer from {}: {e}", self.base)),
        };
        let status = resp.status().as_u16();
        let body = match resp.bytes().await {
            Ok(bytes) => serde_json::from_slice(&bytes).unwrap_or(Value::Null),
            Err(e) if (200..300).contains(&status) => {
                return BundleAnswer::Lost(format!("the answer from {} broke off: {e}", self.base));
            }
            Err(_) => Value::Null,
        };
        match status {
            200..=299 if body.is_object() => BundleAnswer::Answered { status, body },
            200..=299 => {
                BundleAnswer::Lost(format!("HTTP {status} from {} without a Bundle", self.base))
            }
            429 | 500..=599 => BundleAnswer::Lost(format!("HTTP {status} from {}", self.base)),
            401 | 403 if self.credentials.is_some() => {
                BundleAnswer::Unsent(match refused_token(status, &self.credentials).await {
                    SendError::Transient(reason) => reason,
                    SendError::Refused(status, _) => format!("HTTP {status}"),
                })
            }
            _ => BundleAnswer::Answered { status, body },
        }
    }
}

/// What is known of a Bundle sent to the destination.
#[derive(Debug, Clone, PartialEq)]
pub enum BundleAnswer {
    /// An answer that settles it: `2xx` with the response Bundle, or a
    /// definitive refusal (`4xx`) with its body.
    Answered { status: u16, body: Value },
    /// It never left: nothing was applied.
    Unsent(String),
    /// Sent without a confirming answer: it may have been applied.
    Lost(String),
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
                match issues(&body) {
                    Some(issues) => Validated::Outcome { issues },
                    None => {
                        tracing::warn!(
                            "$validate at {} answered a malformed OperationOutcome (HTTP {status})",
                            self.base
                        );
                        Validated::Unavailable
                    }
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

/// The issues of an `OperationOutcome`, when it is one: a non-empty `issue`
/// array whose every issue has a `severity` of the FHIR vocabulary (`fatal`,
/// `error`, `warning`, `information`) and a `code`. Anything else is not a
/// verdict (`None`), so a malformed answer never passes a resource.
fn issues(outcome: &Value) -> Option<Vec<Issue>> {
    let issues = outcome.get("issue")?.as_array()?;
    if issues.is_empty() {
        return None;
    }
    issues
        .iter()
        .map(|i| {
            let severity = i.get("severity")?.as_str()?;
            if !matches!(severity, "fatal" | "error" | "warning" | "information") {
                return None;
            }
            let code = i.get("code")?.as_str()?;
            let diagnostics = match i.get("diagnostics").or_else(|| i.pointer("/details/text")) {
                None | Some(Value::Null) => None,
                Some(Value::String(text)) => Some(text.clone()),
                Some(_) => return None,
            };
            let expression = match i.get("expression").or_else(|| i.get("location")) {
                None | Some(Value::Null) => Vec::new(),
                Some(Value::Array(items)) => items
                    .iter()
                    .map(|e| e.as_str().map(str::to_string))
                    .collect::<Option<Vec<_>>>()?,
                Some(_) => return None,
            };
            Some(Issue {
                severity: severity.to_string(),
                code: code.to_string(),
                diagnostics,
                expression,
            })
        })
        .collect()
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
    bundle("batch", entries)
}

/// A `batch` or `transaction` Bundle of conditional updates.
pub fn bundle(kind: &str, entries: &[(String, Value)]) -> Value {
    let entry: Vec<Value> = entries
        .iter()
        .map(|(url, resource)| json!({"resource": resource, "request": {"method": "PUT", "url": url}}))
        .collect();
    json!({"resourceType": "Bundle", "type": kind, "entry": entry})
}

/// Every `reference` string in a resource.
fn references(value: &Value, out: &mut Vec<String>) {
    match value {
        Value::Object(map) => {
            for (k, v) in map {
                match (k.as_str(), v) {
                    ("reference", Value::String(r)) => out.push(r.clone()),
                    _ => references(v, out),
                }
            }
        }
        Value::Array(items) => items.iter().for_each(|v| references(v, out)),
        _ => {}
    }
}

/// Whether a reference is conditional (`Type?query`), resolved by the
/// destination inside a transaction.
fn is_conditional(reference: &str) -> bool {
    !reference.contains("://")
        && !reference.starts_with('#')
        && !reference.starts_with("urn:")
        && reference
            .split_once('?')
            .is_some_and(|(t, _)| !t.is_empty() && t.bytes().all(|b| b.is_ascii_alphanumeric()))
}

/// The conditional references (`Type?query`) a resource holds.
pub fn conditional_references(resource: &Value) -> Vec<String> {
    let mut all = Vec::new();
    references(resource, &mut all);
    all.retain(|r| is_conditional(r));
    all
}

/// The system of the tag a namespaced resource carries, with its source id.
pub const SOURCE_TAG_SYSTEM: &str = "urn:helios:hie:source";

/// The id a namespaced source writes a resource under: the first 48 hex
/// characters of SHA-256 over `{source}|{type}|{origin id}`.
pub fn namespaced_id(source: &str, resource_type: &str, id: &str) -> String {
    use sha2::{Digest, Sha256};
    let digest = Sha256::digest(format!("{source}|{resource_type}|{id}").as_bytes());
    hex::encode(digest)[..48].to_string()
}

/// Rewrites relative `Type/id` references to the source's derived ids.
fn rewrite_references(source: &str, value: &mut Value) {
    match value {
        Value::Object(map) => {
            for (k, v) in map.iter_mut() {
                if k == "reference"
                    && let Value::String(r) = v
                {
                    if let Some((t, id)) = r.split_once('/')
                        && !t.is_empty()
                        && t.bytes().all(|b| b.is_ascii_alphanumeric())
                        && t.as_bytes()[0].is_ascii_uppercase()
                        && !id.is_empty()
                        && !id.contains(['/', '?', '#'])
                    {
                        *r = format!("{t}/{}", namespaced_id(source, t, id));
                    }
                } else {
                    rewrite_references(source, v);
                }
            }
        }
        Value::Array(items) => items.iter_mut().for_each(|v| rewrite_references(source, v)),
        _ => {}
    }
}

/// A resource as a namespaced source delivers it: its `id` derived from the
/// source, its relative references to the same source's derived ids (absolute,
/// conditional, contained and `urn:` ones left as written), and the source's
/// tag in `meta.tag`.
pub fn namespace(source: &str, resource: &mut Value) {
    let Some(resource_type) = resource
        .get("resourceType")
        .and_then(Value::as_str)
        .map(str::to_string)
    else {
        return;
    };
    let origin = resource
        .get("id")
        .and_then(Value::as_str)
        .map(str::to_string);
    rewrite_references(source, resource);
    let Value::Object(map) = resource else { return };
    if let Some(origin) = origin {
        map.insert(
            "id".into(),
            Value::String(namespaced_id(source, &resource_type, &origin)),
        );
    }
    let meta = map.entry("meta").or_insert_with(|| json!({}));
    if !meta.is_object() {
        *meta = json!({});
    }
    let tags = meta
        .as_object_mut()
        .expect("meta is an object")
        .entry("tag")
        .or_insert_with(|| json!([]));
    if !tags.is_array() {
        *tags = json!([]);
    }
    let tag = json!({"system": SOURCE_TAG_SYSTEM, "code": source});
    let tags = tags.as_array_mut().expect("tag is an array");
    if !tags.contains(&tag) {
        tags.push(tag);
    }
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

/// A short, human-readable reason from an OperationOutcome: its issues'
/// texts. An answer that holds none is never echoed (it may carry rows).
pub fn outcome_text(v: &Value) -> String {
    let texts = issue_texts(v);
    if texts.is_empty() {
        "the answer holds no OperationOutcome text".to_string()
    } else {
        texts.join("; ")
    }
}

fn issue_texts(v: &Value) -> Vec<String> {
    let outcome = v.get("outcome").unwrap_or(v);
    outcome
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
        .unwrap_or_default()
}

/// The reason HIE keeps for an answer: `HTTP <status>` and the text of its
/// first issue; the rest of the answer stays only in the row's sealed
/// response.
pub fn refusal_reason(status: u16, body: &Value) -> String {
    match issue_texts(body).into_iter().next() {
        Some(text) => format!("HTTP {status}: {text}"),
        None => format!("HTTP {status}"),
    }
}

/// Whether an answer is a definitive refusal: `400`, `404`, `409`, `412` or
/// `422` with an `OperationOutcome`, so the destination committed nothing.
/// Any other answer that is not a success leaves the outcome unknown.
pub fn is_definitive_refusal(status: u16, body: &Value) -> bool {
    matches!(status, 400 | 404 | 409 | 412 | 422)
        && body.get("resourceType").and_then(Value::as_str) == Some("OperationOutcome")
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
    fn namespaced_ids_and_references_are_derived_from_the_source() {
        let mut patient = json!({"resourceType": "Patient", "id": "1"});
        namespace("alpha", &mut patient);
        assert_eq!(
            patient["id"],
            "dc5b31240df4416c42a9972426cc8ac89697b75b8851b742"
        );
        assert_eq!(
            patient["meta"]["tag"],
            json!([{"system": "urn:helios:hie:source", "code": "alpha"}])
        );
        let mut obs = json!({"resourceType": "Observation", "id": "o1",
            "subject": {"reference": "Patient/1"},
            "performer": [{"reference": "https://x.org/fhir/Practitioner/9"},
                          {"reference": "Practitioner?identifier=urn:x|9"},
                          {"reference": "#c1"}, {"reference": "urn:uuid:abc"}]});
        namespace("alpha", &mut obs);
        assert_eq!(
            obs["subject"]["reference"],
            "Patient/dc5b31240df4416c42a9972426cc8ac89697b75b8851b742"
        );
        assert_eq!(
            obs["performer"][0]["reference"],
            "https://x.org/fhir/Practitioner/9"
        );
        assert_eq!(
            obs["performer"][1]["reference"],
            "Practitioner?identifier=urn:x|9"
        );
        assert_eq!(obs["performer"][2]["reference"], "#c1");
        assert_eq!(obs["performer"][3]["reference"], "urn:uuid:abc");
        assert_eq!(
            conditional_references(&obs),
            vec!["Practitioner?identifier=urn:x|9".to_string()]
        );
    }

    #[test]
    fn a_malformed_outcome_is_not_a_verdict() {
        for body in [
            json!({"resourceType": "OperationOutcome", "issue": "malformed"}),
            json!({"resourceType": "OperationOutcome", "issue": []}),
            json!({"resourceType": "OperationOutcome", "issue": [{"severity": "bad", "code": "x"}]}),
            json!({"resourceType": "OperationOutcome", "issue": [{"code": "x"}]}),
            json!({"resourceType": "OperationOutcome", "issue": [{"severity": "error"}]}),
        ] {
            assert_eq!(issues(&body), None, "{body}");
        }
        let ok = issues(&json!({"resourceType": "OperationOutcome",
            "issue": [{"severity": "warning", "code": "x", "diagnostics": "w", "expression": ["A.b"]}]}))
        .unwrap();
        assert_eq!(ok[0].expression, vec!["A.b".to_string()]);
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
