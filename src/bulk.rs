//! The `$bulk-submit` delivery path: HFS fetches the source objects itself from a
//! manifest the engine serves, and the engine reads HFS's receipts back.
//!
//! HFS publishes one `{"reference": "Type/id"}` line per resource it stored and one
//! `OperationOutcome` per line it could not, without the input line number. Receipts
//! are therefore mapped to records by `Type/id`, which every pass-through record
//! carries.

use std::collections::HashSet;
use std::path::Path;
use std::time::Duration;

use anyhow::Context;

use serde_json::{Value, json};

use crate::config::{DeliveryIds, DeliveryMethod, FeedConfig, Validation};
use crate::credentials::Credentials;
use crate::db::Db;
use crate::fhir::{self, SendError};

/// What a status poll found.
#[derive(Debug)]
pub enum Poll {
    InProgress,
    Done {
        /// `Type/id` of every resource HFS stored.
        references: HashSet<String>,
        /// What HFS said about the lines it could not store: its
        /// `OperationOutcome`s, whole (each is kept sealed with the row it is
        /// about; only its first issue becomes the plaintext reason).
        errors: Vec<Value>,
        /// Receipt lines that could not be read: the receipts are incomplete.
        malformed: usize,
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
            .timeout(Duration::from_secs(120))
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
        let mut malformed = 0;
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
                    let (lines, bad) = self.lines(file_url).await?;
                    malformed += bad;
                    for line in lines {
                        match line.get("reference").and_then(Value::as_str) {
                            Some(reference) => {
                                references.insert(reference.to_string());
                            }
                            None => malformed += 1,
                        }
                    }
                }
            }
            for entry in manifest["outcome"].as_array().into_iter().flatten() {
                if let Some(file_url) = entry["url"].as_str() {
                    let (lines, bad) = self.lines(file_url).await?;
                    malformed += bad;
                    errors.extend(lines);
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
        Ok(Poll::Done {
            references,
            errors,
            malformed,
        })
    }

    /// The JSON lines of an NDJSON file, and how many lines were not JSON.
    async fn lines(&self, url: &str) -> Result<(Vec<Value>, usize), SendError> {
        let text = self
            .get(url, "application/fhir+ndjson, application/x-ndjson, */*")
            .await?
            .error_for_status()
            .map_err(|e| SendError::Transient(format!("fetching {url}: {e}")))?
            .text()
            .await
            .map_err(|e| SendError::Transient(format!("reading {url}: {e}")))?;
        let mut bad = 0;
        let lines = text
            .lines()
            .filter(|l| !l.trim().is_empty())
            .filter_map(|l| {
                let parsed = serde_json::from_str(l).ok();
                bad += usize::from(parsed.is_none());
                parsed
            })
            .collect();
        Ok((lines, bad))
    }
}

/// Writes a file the engine serves itself for the destination to fetch (the
/// transformed NDJSON, a staged zip entry), sealed for its path: no row
/// content is written to disk in plaintext.
pub fn write_served(db: &Db, path: &Path, bytes: &[u8]) -> anyhow::Result<()> {
    let sealed = db.seal_file(&path.to_string_lossy(), bytes);
    std::fs::write(path, sealed).with_context(|| format!("writing {}", path.display()))
}

/// A file as `/bulk` serves it: opened when the engine wrote it under
/// `bulk_dir`, as it is when it is the source's own object.
pub fn read_served(db: &Db, bulk_dir: &Path, path: &Path) -> anyhow::Result<Vec<u8>> {
    let bytes = std::fs::read(path).with_context(|| format!("reading {}", path.display()))?;
    if !path.starts_with(bulk_dir) {
        return Ok(bytes);
    }
    db.open_file(&path.to_string_lossy(), &String::from_utf8_lossy(&bytes))
}

/// Refuses a definition whose `$bulk-submit` reads the source objects
/// themselves (`bulk_from: source_objects`, or `auto` resolving to it) where
/// the engine must see each resource first: `validation: required` (nothing is
/// validated before HFS reads the objects) or `delivery.ids: namespaced` (the
/// objects' ids cannot be rewritten).
pub fn check_delivery(feed: &FeedConfig, validation: Validation) -> anyhow::Result<()> {
    let objects = feed.delivery.method == DeliveryMethod::BulkSubmit && !feed.bulk_transformed();
    if objects && validation == Validation::Required {
        anyhow::bail!(
            "source {}: $bulk-submit of the source objects cannot validate resources first; with validation: required use bulk_from: transformed or method: rest",
            feed.id
        );
    }
    if objects && feed.delivery.ids == DeliveryIds::Namespaced {
        anyhow::bail!(
            "source {}: $bulk-submit of the source objects cannot namespace ids; with ids: namespaced use bulk_from: transformed or method: rest",
            feed.id
        );
    }
    Ok(())
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
    fn the_source_objects_path_refuses_what_it_cannot_do() {
        let feed = |extra: &str| -> FeedConfig {
            serde_yaml::from_str(&format!(
                "id: x\nname: X\nsource:\n  type: folder\n  path: in\nformat: ndjson\ntransform: transforms/fhir_passthrough.py\ndelivery:\n  method: bulk_submit\n{extra}"
            ))
            .unwrap()
        };
        let objects = feed("");
        assert!(!objects.bulk_transformed());
        assert!(check_delivery(&objects, Validation::Off).is_ok());
        assert!(check_delivery(&objects, Validation::Required).is_err());
        assert!(check_delivery(&feed("  ids: namespaced\n"), Validation::Off).is_err());
        let transformed = feed("  bulk_from: transformed\n  ids: namespaced\n");
        assert!(check_delivery(&transformed, Validation::Required).is_ok());
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
