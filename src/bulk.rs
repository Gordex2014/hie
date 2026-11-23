//! The `$bulk-submit` delivery path: HFS fetches the source objects itself from a
//! manifest the engine serves, and the engine reads HFS's receipts back.
//!
//! HFS publishes one `{"reference": "Type/id"}` line per resource it stored and one
//! `OperationOutcome` per line it could not, without the input line number. Receipts
//! are therefore mapped to records by `Type/id`, which every pass-through record
//! carries.

use std::collections::HashSet;
use std::path::Path;
use std::sync::Arc;

use serde_json::{Value, json};

use crate::credentials::Credentials;
use crate::destination::{Destination, InFlight};
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

/// A bulk failure reason as it may be logged, alerted or stored. Every URL in it
/// (`http://` or `https://`, in any case, up to whitespace, a quote, `<` or `>`)
/// becomes its origin followed by `/…`: its path, query, fragment and user
/// information may carry signatures and tokens. Brackets and parentheses are
/// part of a URL, so a bracketed IPv6 host or a path with parentheses goes with
/// it. Any other word with a query, a `?` followed later in the word by `=`
/// (a relative target such as `/receipts/1?sig=…`, as a server may echo one),
/// loses what follows the `?`: `/receipts/1?…`.
/// The rest of the reason, the operation, the HTTP status and the error, is
/// kept.
pub fn safe_reason(reason: &str) -> String {
    let lower = reason.to_ascii_lowercase();
    let mut out = String::with_capacity(reason.len());
    let mut at = 0;
    while let Some(start) = ["http://", "https://"]
        .iter()
        .filter_map(|scheme| lower[at..].find(scheme).map(|n| at + n))
        .min()
    {
        out.push_str(&reason[at..start]);
        let end = reason[start..]
            .find(|c: char| c.is_whitespace() || matches!(c, '"' | '\'' | '`' | '<' | '>'))
            .map_or(reason.len(), |n| start + n);
        out.push_str(&origin_of(&reason[start..end]));
        at = end;
    }
    out.push_str(&reason[at..]);
    cut_relative_queries(&out)
}

/// Every word (up to whitespace, a quote, `<` or `>`) that has a `?` followed
/// later in the word by `=` loses what follows that `?`.
fn cut_relative_queries(text: &str) -> String {
    let stop = |c: char| c.is_whitespace() || matches!(c, '"' | '\'' | '`' | '<' | '>');
    let mut out = String::with_capacity(text.len());
    let mut word = String::new();
    let flush = |word: &mut String, out: &mut String| {
        match word.find('?') {
            Some(query) if word[query..].contains('=') => {
                out.push_str(&word[..query]);
                out.push_str("?…");
            }
            _ => out.push_str(word),
        }
        word.clear();
    };
    for c in text.chars() {
        if stop(c) {
            flush(&mut word, &mut out);
            out.push(c);
        } else {
            word.push(c);
        }
    }
    flush(&mut word, &mut out);
    out
}

/// `safe_reason`, with a submission's token replaced by `<token>` wherever it
/// still appears (a path echoed without its scheme, for instance): what the bulk
/// paths store and log about a submission.
pub fn scrub(reason: &str, token: &str) -> String {
    let safe = safe_reason(reason);
    if token.is_empty() {
        safe
    } else {
        safe.replace(token, "<token>")
    }
}

/// A server's answer about a submission, as it is stored: every string in it
/// passed through `scrub`, its structure kept.
pub fn scrub_value(value: &Value, token: &str) -> Value {
    match value {
        Value::String(s) => Value::String(scrub(s, token)),
        Value::Array(items) => Value::Array(items.iter().map(|v| scrub_value(v, token)).collect()),
        Value::Object(map) => Value::Object(
            map.iter()
                .map(|(k, v)| (scrub(k, token), scrub_value(v, token)))
                .collect(),
        ),
        other => other.clone(),
    }
}

/// A public key for one submission run: the first 16 hex characters of the
/// SHA-256 of its token. It tells runs apart in the event log and grants nothing.
pub fn run_key(token: &str) -> String {
    use sha2::Digest;
    hex::encode(sha2::Sha256::digest(token.as_bytes()))[..16].to_string()
}

/// A request target as a failure reason may name it: the origin of an absolute
/// URL followed by `/…`, or `<URL>` for anything else (a relative target, or text
/// that does not parse). Never the path, query or user information, which may
/// carry signatures and tokens.
pub fn shown(target: &str) -> String {
    let lower = target.trim_start().to_ascii_lowercase();
    if lower.starts_with("http://") || lower.starts_with("https://") {
        origin_of(target.trim())
    } else {
        "<URL>".into()
    }
}

/// `scheme://host[:port]/…` of a URL, or `<URL>` when it has no host.
fn origin_of(url: &str) -> String {
    match url::Url::parse(url) {
        Ok(parsed) => match (parsed.host_str(), parsed.port()) {
            (Some(host), Some(port)) => format!("{}://{host}:{port}/…", parsed.scheme()),
            (Some(host), None) => format!("{}://{host}/…", parsed.scheme()),
            (None, _) => "<URL>".into(),
        },
        Err(_) => "<URL>".into(),
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
    /// Told how each request under the base went; the status pages and receipt
    /// files, whose URLs HFS chooses, are not observed.
    destination: Option<Arc<Destination>>,
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
            destination: None,
        }
    }

    /// The same submitter, telling `destination` how its requests to HFS went.
    pub fn with_destination(mut self, destination: Arc<Destination>) -> Self {
        self.destination = Some(destination);
        self
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

    /// Posts to HFS under the base. The returned exchange is held until the
    /// answer has been read.
    async fn post(
        &self,
        path: &str,
        body: &Value,
    ) -> Result<(reqwest::Response, Option<InFlight>), SendError> {
        let request = self
            .http
            .post(format!("{}/{path}", self.fhir_base))
            .header("Content-Type", "application/fhir+json")
            .header("Accept", "application/fhir+json")
            .json(body);
        // Built before it counts as a request to the server.
        let request = fhir::authorize(request, &self.credentials)
            .await?
            .build()
            .map_err(|e| {
                SendError::Transient(format!(
                    "the request to {} could not be built: {}",
                    shown(&self.fhir_base),
                    crate::destination::send_detail(&e)
                ))
            })?;
        let exchange = self.destination.as_ref().map(|d| d.start());
        let sent = self.http.execute(request).await;
        if let Some(exchange) = &exchange {
            exchange.observe_send(&sent);
        }
        let response = sent.map_err(|e| {
            SendError::Transient(format!(
                "cannot reach {}: {}",
                shown(&self.fhir_base),
                e.without_url()
            ))
        })?;
        Ok((response, exchange))
    }

    async fn get(&self, url: &str, accept: &str) -> Result<reqwest::Response, SendError> {
        let request = self.http.get(url).header("Accept", accept);
        fhir::authorize(request, &self.credentials)
            .await?
            .send()
            .await
            .map_err(|e| {
                SendError::Transient(format!("fetching {}: {}", shown(url), e.without_url()))
            })
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
        let (response, _exchange) = self.post("$bulk-submit", &body).await?;
        self.classify(response).await?;
        Ok(())
    }

    /// Asks HFS where this submission's status can be polled.
    pub async fn status_url(
        &self,
        source_id: &str,
        submission_id: &str,
    ) -> Result<String, SendError> {
        let body = Self::parameters(source_id, submission_id, vec![]);
        let (response, _exchange) = self.post("$bulk-submit-status", &body).await?;
        let response = self.classify(response).await?;
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
                    return Err(SendError::Transient(format!(
                        "HTTP {status} polling {}",
                        shown(&url)
                    )));
                }
                status => {
                    let body: Value = response.json().await.unwrap_or(Value::Null);
                    return Err(SendError::Refused(status, body));
                }
            }
            let manifest: Value = response.json().await.map_err(|e| {
                SendError::Transient(format!(
                    "status manifest at {}: {}",
                    shown(&url),
                    e.without_url()
                ))
            })?;
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
            .map_err(|e| {
                SendError::Transient(format!("fetching {}: {}", shown(url), e.without_url()))
            })?
            .text()
            .await
            .map_err(|e| {
                SendError::Transient(format!("reading {}: {}", shown(url), e.without_url()))
            })?;
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
    fn failure_reasons_keep_only_the_origin_of_each_url() {
        assert_eq!(
            safe_reason(
                "HTTP 500 polling https://user:pw@b.s3.amazonaws.com/k/a.ndjson?X-Amz-Signature=s#f"
            ),
            "HTTP 500 polling https://b.s3.amazonaws.com/…"
        );
        // Any case; a comma inside the query does not end the URL; a path that
        // carries a credential (padding, an encoded slash) goes with the path.
        assert_eq!(
            safe_reason("failed HTTPS://h/status?sig=CANARY-UPPERCASE"),
            "failed https://h/…"
        );
        assert_eq!(
            safe_reason("fetching https://h/status?sig=abc,CANARY-COMMA-TAIL: 503"),
            "fetching https://h/… 503"
        );
        assert_eq!(
            safe_reason("reading http://h:8080/t/CANARY-PATH==/x%2FCANARY-ENC/r (timed out)"),
            "reading http://h:8080/… (timed out)"
        );
        // Quotes end a URL; the rest of the reason stays. A bracket does not:
        // it may be part of the host or the path.
        assert_eq!(
            safe_reason(r#"HTTP 400: could not fetch "https://h/o?t=x" [see https://h2/d?k=y]"#),
            r#"HTTP 400: could not fetch "https://h/…" [see https://h2/…"#
        );
        assert_eq!(
            safe_reason("polling https://[::1]:8080/s?X-Amz-Signature=CANARY-V6 failed"),
            "polling https://[::1]:8080/… failed"
        );
        assert_eq!(
            safe_reason("fetching <https://h/a(CANARY-PAREN)/b?sig=x>: 503"),
            "fetching <https://h/…>: 503"
        );
        assert_eq!(
            safe_reason("no URL here, HTTP 503"),
            "no URL here, HTTP 503"
        );
        // A relative target keeps its path, not its query.
        assert_eq!(
            safe_reason(r#"cannot read "/receipts/1?X-Amz-Signature=CANARY" (or ./r?sig=x): 404"#),
            r#"cannot read "/receipts/1?…" (or ./r?… 404"#
        );
        assert_eq!(
            safe_reason("receipts.ndjson?X-Amz-Signature=CANARY and manifest.json?sig=CANARY"),
            "receipts.ndjson?… and manifest.json?…"
        );
        assert_eq!(safe_reason("token ?sig=CANARY"), "token ?…");
        // A question is not a query.
        for text in ["Was it A/B?", "and/or?", "mg/dL?", "is it done? yes"] {
            assert_eq!(safe_reason(text), text);
        }
    }

    #[test]
    fn a_request_target_is_shown_by_its_origin_or_not_at_all() {
        assert_eq!(
            shown("https://h:8443/s?X-Amz-Signature=CANARY"),
            "https://h:8443/…"
        );
        assert_eq!(shown("/status/1?X-Amz-Signature=CANARY"), "<URL>");
        assert_eq!(shown("status/1?sig=CANARY"), "<URL>");
        assert_eq!(shown("https://[::1]/a(b)?sig=CANARY"), "https://[::1]/…");
    }

    #[test]
    fn a_token_is_scrubbed_by_value_and_a_run_key_does_not_reveal_it() {
        let token = "0123456789abcdef0123456789abcdef";
        assert_eq!(
            scrub(&format!("cannot read /bulk/{token}/files/2: 404"), token),
            "cannot read /bulk/<token>/files/2: 404"
        );
        let answer = json!({"issue": [{"diagnostics": format!("GET /bulk/{token}/manifest.json at https://h/x?sig=s"), "n": 1}]});
        assert_eq!(
            scrub_value(&answer, token),
            json!({"issue": [{"diagnostics": "GET /bulk/<token>/manifest.json at https://h/…", "n": 1}]})
        );
        let run = run_key(token);
        assert_eq!(run.len(), 16);
        assert!(!token.contains(&run));
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
