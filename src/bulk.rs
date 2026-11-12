//! The `$bulk-submit` delivery path: HFS fetches the source objects itself from a
//! manifest the engine serves, and the engine reads HFS's receipts back.
//!
//! HFS publishes one `{"reference": "Type/id"}` line per resource it stored and one
//! `OperationOutcome` per line it could not, without the input line number. Receipts
//! are therefore mapped to records by `Type/id`, which every pass-through record
//! carries.

use std::collections::HashSet;
use std::path::Path;

use serde_json::{Value, json};

use crate::credentials::Credentials;
use crate::fhir::{self, SendError};

/// Whether `path` lies inside `dir`, as the filesystem resolves both (symbolic
/// links and `..` included). A path that cannot be resolved, because it is missing
/// or unreadable, is not inside. A recorded path is otherwise used exactly as
/// stored, so reading or removing it reaches the same file.
pub fn is_within(path: &Path, dir: &Path) -> bool {
    match (std::fs::canonicalize(path), std::fs::canonicalize(dir)) {
        (Ok(path), Ok(dir)) => path.starts_with(dir),
        _ => false,
    }
}

/// The origin of a kept file recorded before origins were stored, decided once
/// at start-up. A store object's name is its basename, so only an entry the
/// engine unpacked is named `zip!/entry`; with its file under the engine's bulk
/// directory, it is one the engine staged itself.
pub fn historical_origin(
    name: &str,
    key: &str,
    local_source: bool,
    bulk_dir: &Path,
) -> &'static str {
    if name.contains("!/") && is_within(Path::new(key), bulk_dir) {
        "staged"
    } else if local_source {
        "local"
    } else {
        "store"
    }
}

/// What a status poll found.
#[derive(Debug)]
pub enum Poll {
    InProgress,
    Done {
        /// `Type/id` of every resource HFS stored.
        references: HashSet<String>,
        /// What HFS said about the lines it could not store.
        errors: Vec<String>,
    },
}

pub struct BulkSubmitter {
    http: reqwest::Client,
    fhir_base: String,
    public_url: String,
    credentials: Credentials,
}

/// The identifier system the engine submits under; the value is the source id.
pub const SUBMITTER_SYSTEM: &str = "urn:helios:hie:source";

impl BulkSubmitter {
    pub fn new(fhir_base: &str, public_url: &str) -> Self {
        let http = reqwest::Client::builder()
            .timeout(crate::fhir::HTTP_TIMEOUT)
            .build()
            .expect("HTTP client");
        Self {
            http,
            fhir_base: fhir_base.trim_end_matches('/').to_string(),
            public_url: public_url.trim_end_matches('/').to_string(),
            credentials: None,
        }
    }

    /// The same submitter, calling HFS with the engine's token.
    pub fn with_credentials(mut self, credentials: Credentials) -> Self {
        self.credentials = credentials;
        self
    }

    pub fn manifest_url(&self, token: &str) -> String {
        format!("{}/bulk/{token}/manifest.json", self.public_url)
    }

    pub fn file_url(&self, token: &str, file_id: i64) -> String {
        format!("{}/bulk/{token}/files/{file_id}", self.public_url)
    }

    /// The manifest HFS reads: one output entry per object.
    pub fn manifest(&self, entries: &[(String, String, usize)]) -> Value {
        let output: Vec<Value> = entries
            .iter()
            .map(|(resource_type, url, count)| json!({"type": resource_type, "url": url, "count": count}))
            .collect();
        json!({
            "transactionTime": chrono::Utc::now().to_rfc3339(),
            "request": self.public_url,
            "requiresAccessToken": false,
            "outputFormat": "application/fhir+ndjson",
            "output": output,
            "deleted": [],
            "error": [],
        })
    }

    fn parameters(source_id: &str, submission_id: &str, more: Vec<Value>) -> Value {
        let mut parameter = vec![
            json!({"name": "submitter", "valueIdentifier": {"system": SUBMITTER_SYSTEM, "value": source_id}}),
            json!({"name": "submissionId", "valueString": submission_id}),
        ];
        parameter.extend(more);
        json!({"resourceType": "Parameters", "parameter": parameter})
    }

    async fn post(&self, path: &str, body: &Value) -> Result<reqwest::Response, SendError> {
        let request = self
            .http
            .post(format!("{}/{path}", self.fhir_base))
            .header("Content-Type", "application/fhir+json")
            .header("Accept", "application/fhir+json")
            .json(body);
        fhir::authorize(request, &self.credentials)
            .await?
            .send()
            .await
            .map_err(|e| SendError::Transient(format!("cannot reach {}: {e}", self.fhir_base)))
    }

    async fn get(&self, url: &str, accept: &str) -> Result<reqwest::Response, SendError> {
        let request = self.http.get(url).header("Accept", accept);
        fhir::authorize(request, &self.credentials)
            .await?
            .send()
            .await
            .map_err(|e| SendError::Transient(format!("fetching {url}: {e}")))
    }

    async fn classify(&self, response: reqwest::Response) -> Result<reqwest::Response, SendError> {
        let status = response.status().as_u16();
        match status {
            200..=299 => Ok(response),
            429 | 500..=599 => Err(SendError::Transient(format!("HTTP {status}"))),
            401 | 403 if self.credentials.is_some() => {
                Err(fhir::refused_token(status, &self.credentials).await)
            }
            _ => {
                let body: Value = response.json().await.unwrap_or(Value::Null);
                Err(SendError::Refused(status, body))
            }
        }
    }

    /// Hands the manifest to HFS and closes the submission: nothing more will be sent under it.
    pub async fn kickoff(
        &self,
        source_id: &str,
        submission_id: &str,
        manifest_url: &str,
    ) -> Result<(), SendError> {
        let body = Self::parameters(
            source_id,
            submission_id,
            vec![
                json!({"name": "manifestUrl", "valueUrl": manifest_url}),
                json!({"name": "fhirBaseUrl", "valueUrl": self.public_url}),
                json!({"name": "submissionStatus", "valueCoding": {"system": "http://hl7.org/fhir/event-status", "code": "completed"}}),
            ],
        );
        self.classify(self.post("$bulk-submit", &body).await?)
            .await?;
        Ok(())
    }

    /// Asks HFS where this submission's status can be polled.
    pub async fn status_url(
        &self,
        source_id: &str,
        submission_id: &str,
    ) -> Result<String, SendError> {
        let body = Self::parameters(source_id, submission_id, vec![]);
        let response = self
            .classify(self.post("$bulk-submit-status", &body).await?)
            .await?;
        response
            .headers()
            .get("content-location")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string())
            .ok_or_else(|| {
                SendError::Transient("no Content-Location on the status kick-off".into())
            })
    }

    /// One poll of the status URL. Done means every page of the status manifest and
    /// every receipt file were read.
    pub async fn poll(&self, status_url: &str) -> Result<Poll, SendError> {
        let mut references = HashSet::new();
        let mut errors = Vec::new();
        let mut next = Some(status_url.to_string());
        while let Some(url) = next.take() {
            let response = self.get(&url, "application/json").await?;
            match response.status().as_u16() {
                202 | 429 => return Ok(Poll::InProgress),
                200 => {}
                status @ (401 | 403) if self.credentials.is_some() => {
                    return Err(fhir::refused_token(status, &self.credentials).await);
                }
                status @ (500..=599) => {
                    return Err(SendError::Transient(format!("HTTP {status} polling {url}")));
                }
                status => {
                    let body: Value = response.json().await.unwrap_or(Value::Null);
                    return Err(SendError::Refused(status, body));
                }
            }
            let manifest: Value = response
                .json()
                .await
                .map_err(|e| SendError::Transient(format!("status manifest at {url}: {e}")))?;
            for entry in manifest["output"].as_array().into_iter().flatten() {
                if let Some(file_url) = entry["url"].as_str() {
                    for line in self.lines(file_url).await? {
                        if let Some(reference) = line.get("reference").and_then(Value::as_str) {
                            references.insert(reference.to_string());
                        }
                    }
                }
            }
            for entry in manifest["outcome"].as_array().into_iter().flatten() {
                if let Some(file_url) = entry["url"].as_str() {
                    for line in self.lines(file_url).await? {
                        errors.push(fhir::outcome_text(&line));
                    }
                }
            }
            next = manifest["link"]
                .as_array()
                .into_iter()
                .flatten()
                .find(|l| l["relation"] == "next")
                .and_then(|l| l["url"].as_str())
                .map(str::to_string);
        }
        Ok(Poll::Done { references, errors })
    }

    async fn lines(&self, url: &str) -> Result<Vec<Value>, SendError> {
        let text = self
            .get(url, "application/fhir+ndjson, application/x-ndjson, */*")
            .await?
            .error_for_status()
            .map_err(|e| SendError::Transient(format!("fetching {url}: {e}")))?
            .text()
            .await
            .map_err(|e| SendError::Transient(format!("reading {url}: {e}")))?;
        Ok(text
            .lines()
            .filter(|l| !l.trim().is_empty())
            .filter_map(|l| serde_json::from_str(l).ok())
            .collect())
    }
}

/// A stable id for a drop: the same files submitted again get the same id, so HFS
/// refuses the repeat instead of ingesting it twice.
pub fn submission_id(source_id: &str, file_hashes: &[&str]) -> String {
    use sha2::Digest;
    let mut hasher = sha2::Sha256::new();
    hasher.update(source_id.as_bytes());
    for h in file_hashes {
        hasher.update(b"\n");
        hasher.update(h.as_bytes());
    }
    format!("hie-{}", &hex::encode(hasher.finalize())[..16])
}

/// An unguessable token for the manifest and file URLs the engine serves.
pub fn token() -> String {
    hex::encode(rand::random::<[u8; 16]>())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn submission_ids_are_stable_for_the_same_drop() {
        assert_eq!(
            submission_id("lab", &["a", "b"]),
            submission_id("lab", &["a", "b"])
        );
        assert_ne!(
            submission_id("lab", &["a", "b"]),
            submission_id("lab", &["b"])
        );
        assert_ne!(submission_id("lab", &["a"]), submission_id("other", &["a"]));
        assert!(submission_id("lab", &[]).starts_with("hie-"));
    }

    #[test]
    fn a_manifest_lists_each_object_once() {
        let s = BulkSubmitter::new("http://hfs", "http://hie/");
        let m = s.manifest(&[("Patient".into(), "http://hie/bulk/t/files/1".into(), 3)]);
        assert_eq!(m["output"][0]["type"], "Patient");
        assert_eq!(m["output"][0]["count"], 3);
        assert_eq!(m["requiresAccessToken"], false);
        assert_eq!(s.manifest_url("t"), "http://hie/bulk/t/manifest.json");
    }
}
