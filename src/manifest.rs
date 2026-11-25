//! The `manifest` source: FHIR Bulk Data manifests a user sends, by URL or
//! uploaded, handed to the FHIR server with `$bulk-submit` and recorded as one
//! load each from the server's receipts. The engine never downloads the
//! provider's NDJSON: the FHIR server is the Data Consumer.
//!
//! What lives here: the rules of a submission's body, the request to the FHIR
//! server and how its answer is read, and the tally of a submission's
//! receipts. The store's side is `db::manifest`, the route `api::manifests`,
//! and the runner's polling `pipeline::manifest`. See `docs/sources.md`
//! (Manifest).

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use tokio::time::Instant;

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::bulk::{SUBMITTER_SYSTEM, safe_reason, shown};
use crate::credentials::Credentials;
use crate::destination::Destination;

/// The largest body `POST /api/sources/{id}/manifests` takes, counted as received.
pub const BODY_MAX: usize = 4 << 20;

/// The largest refusal body passed through to the client as the FHIR server sent it.
pub const REFUSAL_MAX: usize = 64 << 10;

/// The longest line of an outcome file the engine reads.
pub const LINE_MAX: usize = 1 << 20;

/// Failure groups a load keeps, the largest first.
pub const GROUPS_KEPT: usize = 100;

/// Distinct failure groups counted while receipts are read; outcomes beyond
/// them count in `other`.
pub const GROUPS_TRACKED: usize = 1000;

/// Failed reads of one receipt file before the load is given up as unreadable.
pub const RECEIPT_ATTEMPTS: u32 = 5;

/// Redirects followed when a status page or a receipt file is read.
const REDIRECTS: usize = 5;

/// The longest file name an uploaded manifest's load is shown with, in characters.
pub const FILE_NAME_MAX: usize = 255;

/// The error of a load whose manifest the FHIR server never took.
pub const NOT_RECEIVED: &str = "the FHIR server did not receive this manifest; send it again";

static KICKOFF_TIMEOUT_MS: AtomicU64 = AtomicU64::new(30_000);

/// How long the engine waits for the FHIR server's answer to a kick-off,
/// getting its own token included: 30 seconds.
pub fn kickoff_timeout() -> Duration {
    Duration::from_millis(KICKOFF_TIMEOUT_MS.load(Ordering::Relaxed))
}

/// Changes `kickoff_timeout` for the whole process. For tests, which cannot
/// wait 30 seconds for an answer that never comes.
#[doc(hidden)]
pub fn set_kickoff_timeout(timeout: Duration) {
    KICKOFF_TIMEOUT_MS.store(timeout.as_millis() as u64, Ordering::Relaxed);
}

/// From the moment a load is recorded, how long its kick-off may still be in
/// the hands of the request that sends it: twice the kick-off timeout (the
/// send, then the writes that list the load). A load still unlisted after
/// that is listed as unconfirmed by its source.
pub fn sending_until_ms() -> i64 {
    2 * kickoff_timeout().as_millis() as i64
}

/// From the moment a load is recorded, how long the FHIR server may still be
/// handling a kick-off the engine stopped waiting for: the engine's wait, and
/// as long again. Before that, a status request the server answers `404` is
/// asked again.
pub fn unknown_after_ms() -> i64 {
    2 * kickoff_timeout().as_millis() as i64
}

/// The `submissionId` of a load: fixed when the load is recorded, before
/// anything is sent, and never the same for two loads.
pub fn submission_id(source_id: &str, load_id: i64, token: &str) -> String {
    use sha2::Digest;
    let mut hasher = sha2::Sha256::new();
    hasher.update(source_id.as_bytes());
    hasher.update(b"\n");
    hasher.update(load_id.to_string().as_bytes());
    hasher.update(b"\n");
    hasher.update(token.as_bytes());
    format!("hie-manifest-{}", &hex::encode(hasher.finalize())[..16])
}

/// SHA-256 of `bytes`, as hex.
pub fn sha256_hex(bytes: &[u8]) -> String {
    use sha2::Digest;
    hex::encode(sha2::Sha256::digest(bytes))
}

/// A URL as the engine stores and shows it: without its user information,
/// query and fragment, followed by `?…` when it had a query. What it
/// forwards is the URL as given.
pub fn stored_url(text: &str) -> String {
    let Ok(mut url) = url::Url::parse(text) else {
        return shown(text);
    };
    let had_query = url.query().is_some();
    url.set_query(None);
    url.set_fragment(None);
    let _ = url.set_username("");
    let _ = url.set_password(None);
    let mut out = url.to_string();
    if had_query {
        out.push_str("?…");
    }
    out
}

/// `scheme://host[:port]` of an absolute URL.
pub fn origin(text: &str) -> Option<String> {
    let url = url::Url::parse(text).ok()?;
    let host = url.host_str()?;
    Some(match url.port() {
        Some(port) => format!("{}://{host}:{port}", url.scheme()),
        None => format!("{}://{host}", url.scheme()),
    })
}

/// What a manifest submission hands over.
#[derive(Debug, Clone, PartialEq)]
pub enum Given {
    /// A manifest the provider serves, forwarded unchanged.
    Url(String),
    /// A manifest uploaded, served by the engine; shown as `file_name`.
    Upload { manifest: Value, file_name: String },
}

/// A submission's body, read and checked.
#[derive(Debug, Clone)]
pub struct Request {
    pub given: Given,
    pub request_id: Option<String>,
    /// What the body asks for, apart from its `request_id`: two bodies with
    /// one key are the same request only when this is the same.
    pub hash: String,
}

/// Why a submission's body was refused.
#[derive(Debug, Clone, PartialEq)]
pub enum BodyError {
    /// `invalid_body`, naming the member when there is one.
    Invalid {
        field: Option<&'static str>,
        error: String,
    },
    /// `invalid_manifest`.
    Manifest(String),
}

fn invalid(field: Option<&'static str>, error: impl Into<String>) -> BodyError {
    BodyError::Invalid {
        field,
        error: error.into(),
    }
}

/// Reads a submission's body: exactly one of `manifest_url` and `manifest`,
/// with `file_name` only beside `manifest`, and an optional `request_id`.
pub fn read_request(body: &[u8]) -> Result<Request, BodyError> {
    let value: Value =
        serde_json::from_slice(body).map_err(|_| invalid(None, "the body is not JSON"))?;
    let Some(object) = value.as_object() else {
        return Err(invalid(None, "the body must be a JSON object"));
    };
    for key in object.keys() {
        if !matches!(
            key.as_str(),
            "manifest_url" | "manifest" | "file_name" | "request_id"
        ) {
            let shown: String = key.chars().take(64).collect();
            return Err(BodyError::Invalid {
                field: None,
                error: format!(
                    "unknown member {shown:?} (use manifest_url or manifest, file_name, request_id)"
                ),
            });
        }
    }
    let request_id = match object.get("request_id") {
        None => None,
        Some(Value::String(id))
            if (1..=128).contains(&id.len())
                && id.bytes().all(|b| {
                    b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b':' | b'-')
                }) =>
        {
            Some(id.clone())
        }
        Some(_) => {
            return Err(invalid(
                Some("request_id"),
                "request_id must be 1 to 128 characters of A-Z a-z 0-9 . _ : -",
            ));
        }
    };
    let given = match (object.get("manifest_url"), object.get("manifest")) {
        (Some(_), Some(_)) => {
            return Err(invalid(
                None,
                "send exactly one of manifest_url and manifest, not both",
            ));
        }
        (None, None) => {
            return Err(invalid(
                None,
                "send exactly one of manifest_url and manifest",
            ));
        }
        (Some(url), None) => {
            if object.contains_key("file_name") {
                return Err(invalid(
                    Some("file_name"),
                    "file_name goes with an uploaded manifest only",
                ));
            }
            match url.as_str() {
                Some(url) if crate::config::absolute_http_url(url) => Given::Url(url.to_string()),
                _ => {
                    return Err(invalid(
                        Some("manifest_url"),
                        format!(
                            "manifest_url must be an absolute http or https URL of at most {} characters",
                            crate::config::MANIFEST_URL_MAX_CHARS
                        ),
                    ));
                }
            }
        }
        (None, Some(manifest)) => {
            check_manifest(manifest)?;
            let file_name = match object.get("file_name") {
                None => "manifest.json".to_string(),
                Some(Value::String(name))
                    if (1..=FILE_NAME_MAX).contains(&name.chars().count())
                        && !name.chars().any(char::is_control) =>
                {
                    name.clone()
                }
                Some(_) => {
                    return Err(invalid(
                        Some("file_name"),
                        "file_name must be 1 to 255 characters, without control characters",
                    ));
                }
            };
            Given::Upload {
                manifest: manifest.clone(),
                file_name,
            }
        }
    };
    let asked = match &given {
        Given::Url(url) => json!({"manifest_url": url}),
        Given::Upload {
            manifest,
            file_name,
        } => json!({"manifest": manifest, "file_name": file_name}),
    };
    Ok(Request {
        given,
        request_id,
        hash: sha256_hex(canonical(&asked).as_bytes()),
    })
}

/// An uploaded manifest's shape: an object whose `output` is a non-empty
/// array, each entry with a string `type` and an absolute `http`/`https` `url`.
fn check_manifest(manifest: &Value) -> Result<(), BodyError> {
    let Some(output) = manifest.get("output").and_then(Value::as_array) else {
        return Err(BodyError::Manifest(
            "manifest must be an object with an output array".into(),
        ));
    };
    if output.is_empty() {
        return Err(BodyError::Manifest(
            "manifest.output must name at least one file".into(),
        ));
    }
    for (i, entry) in output.iter().enumerate() {
        if !entry.get("type").is_some_and(Value::is_string) {
            return Err(BodyError::Manifest(format!(
                "manifest.output[{i}] has no type"
            )));
        }
        if !entry
            .get("url")
            .and_then(Value::as_str)
            .is_some_and(crate::config::absolute_http_url_any_length)
        {
            return Err(BodyError::Manifest(format!(
                "manifest.output[{i}].url must be an absolute http or https URL"
            )));
        }
    }
    Ok(())
}

/// JSON text with every object's keys sorted, whatever order they came in.
fn canonical(value: &Value) -> String {
    match value {
        Value::Object(map) => {
            let mut keys: Vec<&String> = map.keys().collect();
            keys.sort();
            let members: Vec<String> = keys
                .into_iter()
                .map(|k| format!("{}:{}", Value::String(k.clone()), canonical(&map[k])))
                .collect();
            format!("{{{}}}", members.join(","))
        }
        Value::Array(items) => format!(
            "[{}]",
            items.iter().map(canonical).collect::<Vec<_>>().join(",")
        ),
        other => other.to_string(),
    }
}

/// How the FHIR server answered a kick-off.
#[derive(Debug)]
pub enum Kickoff {
    /// A `2xx`: it took the manifest.
    Accepted,
    /// A definite refusal, passed through to the client as it came.
    Refused {
        status: u16,
        body: Vec<u8>,
        content_type: Option<reqwest::header::HeaderValue>,
        retry_after: Option<reqwest::header::HeaderValue>,
    },
    /// A refusal whose body is over `REFUSAL_MAX`.
    TooLarge { status: u16 },
    /// The server refused the engine's own token, or the engine could not
    /// get one: why, made safe to log (no URL beyond its origin, no token).
    Auth(String),
    /// No connection could be made: nothing reached the server.
    Unreachable(String),
    /// Nothing was sent: the store could not claim the kick-off before its
    /// deadline. Answered `503 store_busy`.
    NotSent,
    /// The request went out and its answer did not come back as one the
    /// engine can rely on (a timeout, a broken connection, a `5xx` but `501`,
    /// a `408`, a redirect, any status not listed): the server may have
    /// taken it.
    Lost(String),
}

/// What a status-only kick-off found out about a submission the server knows.
#[derive(Debug, PartialEq, Eq)]
pub enum Probe {
    /// `409`: the submission was completed; its kick-off finished.
    Completed,
    /// `2xx`: the submission is still open.
    Open,
}

/// What a status request found.
#[derive(Debug)]
pub enum Status {
    /// Where the submission's status is polled.
    Found(String),
    /// The server does not know the submission (`404`).
    Unknown,
    /// The server refused the request for good.
    Refused(String),
}

/// Why a step of a submission did not go through.
#[derive(Debug)]
pub enum StepError {
    /// May pass: tried again later, counted as an attempt.
    Transient(String),
    /// Will not pass: the load is unreadable, for this reason.
    Unreadable(String),
}

/// One status page, read.
#[derive(Debug)]
pub enum Page {
    /// Still running: polled again after this long at least (`Retry-After`).
    InProgress(Option<Duration>),
    /// The page's manifest.
    Done(Value),
    /// The server no longer knows the submission.
    Unknown,
}

/// The FHIR server's side of manifest submissions: the kick-off, the status
/// request, the status pages and the receipt files. The engine's token goes
/// only to the FHIR server's origin and to the origin of the status URL that
/// server gave; any other host (a pre-signed receipt URL) is read without it.
pub struct Client {
    /// Kick-offs, status requests and status pages: each whole within
    /// `fhir::HTTP_TIMEOUT`.
    http: reqwest::Client,
    /// Receipt files, which may be large: no bound on the whole, only on
    /// each wait for more of it.
    receipts: reqwest::Client,
    fhir_base: String,
    public_url: String,
    credentials: Credentials,
    /// Told how each request under `fhir_base` went, as the outbound path's.
    destination: Option<Arc<Destination>>,
}

/// What is logged and stored when the engine cannot get its own token: a
/// fixed sentence, since what the identity provider answered may echo the
/// engine's secret.
pub const NO_TOKEN: &str = "the engine could not get its own token from its identity provider";

/// The most of a status page the engine reads.
pub const STATUS_MAX: usize = 4 << 20;

/// How long a receipt file may send nothing before its read is given up.
const RECEIPT_IDLE: Duration = Duration::from_secs(60);

impl Client {
    pub fn new(fhir_base: &str, public_url: &str, credentials: Credentials) -> Self {
        // A connection that cannot be made is known well before the
        // kick-off's deadline: nothing was sent.
        let connect = Duration::from_secs(10).min(kickoff_timeout() / 3);
        // Redirects are never followed by the client: a kick-off's `3xx` is an
        // answer, and a status page's is followed by hand, hop by hop, so
        // that each hop decides on its own whether it gets the token.
        let http = reqwest::Client::builder()
            .timeout(crate::fhir::HTTP_TIMEOUT)
            .connect_timeout(connect)
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .expect("HTTP client");
        let receipts = reqwest::Client::builder()
            .connect_timeout(connect)
            .read_timeout(RECEIPT_IDLE)
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .expect("HTTP client");
        Self {
            http,
            receipts,
            fhir_base: fhir_base.trim_end_matches('/').to_string(),
            public_url: public_url.trim_end_matches('/').to_string(),
            credentials,
            destination: None,
        }
    }

    /// The same client, telling `destination` how its requests to the FHIR
    /// server went.
    pub fn with_destination(mut self, destination: Arc<Destination>) -> Self {
        self.destination = Some(destination);
        self
    }

    /// The URL the engine serves an uploaded manifest at.
    pub fn upload_url(&self, token: &str) -> String {
        format!("{}/bulk/{token}/manifest.json", self.public_url)
    }

    fn parameters(source_id: &str, submission_id: &str, more: Vec<Value>) -> Value {
        let mut parameter = vec![
            json!({"name": "submitter", "valueIdentifier": {"system": SUBMITTER_SYSTEM, "value": source_id}}),
            json!({"name": "submissionId", "valueString": submission_id}),
        ];
        parameter.extend(more);
        json!({"resourceType": "Parameters", "parameter": parameter})
    }

    /// The kick-off's `Parameters`: the manifest URL as forwarded, the origin
    /// the server requires beside it, one manifest per submission, and the
    /// source's `oauthMetadataUrl` when it has one.
    pub fn kickoff_body(
        source_id: &str,
        submission_id: &str,
        manifest_url: &str,
        fhir_base_url: &str,
        oauth_metadata_url: Option<&str>,
    ) -> Value {
        let mut more = vec![
            json!({"name": "manifestUrl", "valueUrl": manifest_url}),
            json!({"name": "fhirBaseUrl", "valueUrl": fhir_base_url}),
            json!({"name": "submissionStatus", "valueCoding": {"system": "http://hl7.org/fhir/event-status", "code": "completed"}}),
        ];
        if let Some(url) = oauth_metadata_url {
            more.push(json!({"name": "oauthMetadataUrl", "valueUrl": url}));
        }
        Self::parameters(source_id, submission_id, more)
    }

    /// The bearer token, when the engine has credentials; `NO_TOKEN` when it
    /// cannot get one.
    async fn bearer(&self) -> Result<Option<String>, String> {
        match &self.credentials {
            None => Ok(None),
            Some(source) => source.bearer().await.map(Some).map_err(|_| NO_TOKEN.into()),
        }
    }

    async fn forget_token(&self) {
        if let Some(source) = &self.credentials {
            source.invalidate().await;
        }
    }

    /// The values a server's text must never carry into a log: the engine's
    /// client secret and the bearer it sent.
    fn secrets(&self, bearer: Option<&str>) -> Vec<String> {
        let mut secrets: Vec<String> = bearer.map(str::to_string).into_iter().collect();
        if let Some(source) = &self.credentials {
            secrets.push(source.settings().client_secret.clone());
        }
        secrets.retain(|s| !s.is_empty());
        secrets
    }

    /// The first phase of a kick-off: the engine's token, within its share
    /// of the kick-off's `deadline` (a third of the kick-off timeout at
    /// most). Nothing is sent yet; a token the engine cannot get is a
    /// `Kickoff::Auth`, with `NO_TOKEN`.
    pub async fn kickoff_token(&self, deadline: Instant) -> Result<Option<String>, Kickoff> {
        let Some(source) = &self.credentials else {
            return Ok(None);
        };
        let share = deadline.min(Instant::now() + kickoff_timeout() / 3);
        match tokio::time::timeout_at(share, source.bearer()).await {
            Ok(Ok(token)) => Ok(Some(token)),
            Ok(Err(_)) => Err(Kickoff::Auth(NO_TOKEN.into())),
            Err(_) => Err(Kickoff::Auth(format!("{NO_TOKEN} in time"))),
        }
    }

    /// The second phase: hands one manifest to the FHIR server and reads its
    /// answer, all before `deadline`. Only a request that went out and got
    /// no status line in time, or a status that says nothing for sure, is
    /// `Lost`.
    pub async fn kickoff(&self, body: &Value, bearer: Option<&str>, deadline: Instant) -> Kickoff {
        let mut request = self
            .http
            .post(format!("{}/$bulk-submit", self.fhir_base))
            .header("Content-Type", "application/fhir+json")
            .header("Accept", "application/fhir+json")
            .json(body);
        if let Some(bearer) = bearer {
            request = request.bearer_auth(bearer);
        }
        let exchange = self.destination.as_ref().map(|d| d.start());
        let Ok(sent) = tokio::time::timeout_at(deadline, request.send()).await else {
            return Kickoff::Lost(format!(
                "no answer from {} within {}",
                shown(&self.fhir_base),
                crate::pipeline::human(kickoff_timeout())
            ));
        };
        if let Some(exchange) = &exchange {
            exchange.observe_send(&sent);
        }
        let response = match sent {
            Ok(response) => response,
            Err(e) if e.is_connect() => {
                return Kickoff::Unreachable(format!(
                    "cannot reach {}: {}",
                    shown(&self.fhir_base),
                    e.without_url()
                ));
            }
            Err(e) => {
                return Kickoff::Lost(format!(
                    "the answer of {} was lost: {}",
                    shown(&self.fhir_base),
                    e.without_url()
                ));
            }
        };
        let status = response.status().as_u16();
        match status {
            200..=299 => Kickoff::Accepted,
            401 | 403 => {
                self.forget_token().await;
                let secrets = self.secrets(bearer);
                let detail =
                    match tokio::time::timeout_at(deadline, read_capped(response, REFUSAL_MAX))
                        .await
                    {
                        Ok(Ok(Some(body))) => refusal_issues(status, &body, &secrets),
                        Ok(Ok(None)) => format!("HTTP {status}, a body over {REFUSAL_MAX} bytes"),
                        _ => format!("HTTP {status}, a body that could not be read in time"),
                    };
                Kickoff::Auth(redact(&detail, &secrets))
            }
            408 => Kickoff::Lost(format!("HTTP 408 from {}", shown(&self.fhir_base))),
            400..=499 | 501 => {
                let content_type = response
                    .headers()
                    .get(reqwest::header::CONTENT_TYPE)
                    .cloned();
                let retry_after = response
                    .headers()
                    .get(reqwest::header::RETRY_AFTER)
                    .cloned();
                // The status says the server refused it; a body that does not
                // arrive whole, in time and small enough cannot be passed on.
                match tokio::time::timeout_at(deadline, read_capped(response, REFUSAL_MAX)).await {
                    Ok(Ok(Some(body))) => Kickoff::Refused {
                        status,
                        body,
                        content_type,
                        retry_after,
                    },
                    _ => Kickoff::TooLarge { status },
                }
            }
            _ => Kickoff::Lost(format!("HTTP {status} from {}", shown(&self.fhir_base))),
        }
    }

    /// A POST under the FHIR server's base, with the engine's token, told to
    /// the destination observer.
    async fn post_fhir(&self, path: &str, body: &Value) -> Result<reqwest::Response, StepError> {
        let bearer = self.bearer().await.map_err(StepError::Transient)?;
        let mut request = self
            .http
            .post(format!("{}/{path}", self.fhir_base))
            .header("Content-Type", "application/fhir+json")
            .header("Accept", "application/fhir+json")
            .json(body);
        if let Some(bearer) = &bearer {
            request = request.bearer_auth(bearer);
        }
        let exchange = self.destination.as_ref().map(|d| d.start());
        let sent = request.send().await;
        if let Some(exchange) = &exchange {
            exchange.observe_send(&sent);
        }
        sent.map_err(|e| {
            StepError::Transient(format!(
                "cannot reach {}: {}",
                shown(&self.fhir_base),
                e.without_url()
            ))
        })
    }

    /// A status-only kick-off (`submissionStatus: in-progress`, no manifest),
    /// which changes nothing at the server: it tells a completed submission
    /// (`409`) from one still open (`2xx`). Sent only for a submission the
    /// server already said it knows; for an unknown one it would make one.
    pub async fn probe(&self, source_id: &str, submission_id: &str) -> Result<Probe, StepError> {
        let body = Self::parameters(
            source_id,
            submission_id,
            vec![
                json!({"name": "submissionStatus", "valueCoding": {"system": "http://hl7.org/fhir/event-status", "code": "in-progress"}}),
            ],
        );
        let response = self.post_fhir("$bulk-submit", &body).await?;
        match response.status().as_u16() {
            409 => Ok(Probe::Completed),
            200..=299 => Ok(Probe::Open),
            501 => Err(StepError::Unreadable(BULK_OFF.into())),
            status => {
                if matches!(status, 401 | 403) {
                    self.forget_token().await;
                }
                Err(StepError::Transient(format!(
                    "HTTP {status} asking {} whether the submission was completed",
                    shown(&self.fhir_base)
                )))
            }
        }
    }

    /// Asks the FHIR server where the submission's status is polled.
    pub async fn status_url(
        &self,
        source_id: &str,
        submission_id: &str,
    ) -> Result<Status, StepError> {
        let body = Self::parameters(source_id, submission_id, vec![]);
        let response = self.post_fhir("$bulk-submit-status", &body).await?;
        let status = response.status().as_u16();
        match status {
            200..=299 => response
                .headers()
                .get("content-location")
                .and_then(|v| v.to_str().ok())
                .and_then(|location| {
                    url::Url::parse(&format!("{}/", self.fhir_base))
                        .ok()?
                        .join(location)
                        .ok()
                })
                .map(|u| Status::Found(u.to_string()))
                .ok_or_else(|| {
                    StepError::Transient("no Content-Location on the status request".into())
                }),
            404 => Ok(Status::Unknown),
            501 => Ok(Status::Refused(BULK_OFF.into())),
            401 | 403 if self.credentials.is_some() => {
                self.forget_token().await;
                Err(StepError::Transient(format!(
                    "HTTP {status}: the FHIR server did not accept the engine's token; a new one will be requested"
                )))
            }
            408 | 429 | 500..=599 => Err(StepError::Transient(format!(
                "HTTP {status} asking {} for the submission's status",
                shown(&self.fhir_base)
            ))),
            _ => {
                let text = refusal_text(response).await;
                Ok(Status::Refused(format!(
                    "the FHIR server refused the status request: HTTP {status}{text}"
                )))
            }
        }
    }

    /// Whether `url` is on an origin the engine sends its token to.
    fn trusted(&self, url: &str, status_url: &str) -> bool {
        let Some(target) = origin(url) else {
            return false;
        };
        origin(&self.fhir_base).as_deref() == Some(target.as_str())
            || origin(status_url).as_deref() == Some(target.as_str())
    }

    /// GET `url` with `http`, following redirects by hand: each hop gets the
    /// engine's token only when it is on a trusted origin (`trusted`).
    async fn get(
        &self,
        http: &reqwest::Client,
        url: &str,
        status_url: &str,
        accept: &str,
    ) -> Result<reqwest::Response, StepError> {
        let mut target = url.to_string();
        for _ in 0..=REDIRECTS {
            let mut request = http.get(&target).header("Accept", accept);
            if self.trusted(&target, status_url)
                && let Some(bearer) = self.bearer().await.map_err(StepError::Transient)?
            {
                request = request.bearer_auth(bearer);
            }
            let response = request.send().await.map_err(|e| {
                StepError::Transient(format!("fetching {}: {}", shown(&target), e.without_url()))
            })?;
            if response.status().is_redirection() {
                let next = response
                    .headers()
                    .get(reqwest::header::LOCATION)
                    .and_then(|v| v.to_str().ok())
                    .and_then(|location| url::Url::parse(&target).ok()?.join(location).ok());
                match next {
                    Some(next) => {
                        target = next.to_string();
                        continue;
                    }
                    None => {
                        return Err(StepError::Transient(format!(
                            "a redirect without a Location from {}",
                            shown(&target)
                        )));
                    }
                }
            }
            return Ok(response);
        }
        Err(StepError::Transient(format!(
            "more than {REDIRECTS} redirects from {}",
            shown(url)
        )))
    }

    /// One page of the status manifest of `submission_id`, checked: a JSON
    /// object of at most `STATUS_MAX` bytes, its `output`, `outcome` and
    /// `deleted` arrays of entries with a `url`, for this submission.
    pub async fn page(
        &self,
        url: &str,
        status_url: &str,
        submission_id: &str,
    ) -> Result<Page, StepError> {
        let response = self
            .get(&self.http, url, status_url, "application/json")
            .await?;
        let status = response.status().as_u16();
        let retry_after = response
            .headers()
            .get(reqwest::header::RETRY_AFTER)
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.trim().parse::<u64>().ok())
            .map(Duration::from_secs);
        match status {
            200 => {}
            202 | 429 => return Ok(Page::InProgress(retry_after)),
            404 => return Ok(Page::Unknown),
            501 => return Err(StepError::Unreadable(BULK_OFF.into())),
            401 | 403 if self.credentials.is_some() => {
                self.forget_token().await;
                return Err(StepError::Transient(format!(
                    "HTTP {status}: the FHIR server did not accept the engine's token; a new one will be requested"
                )));
            }
            408 | 500..=599 => {
                return Err(StepError::Transient(format!(
                    "HTTP {status} polling {}",
                    shown(url)
                )));
            }
            _ => {
                let text = refusal_text(response).await;
                return Err(StepError::Unreadable(format!(
                    "the FHIR server refused the status poll: HTTP {status}{text}"
                )));
            }
        }
        let body = match read_capped(response, STATUS_MAX).await {
            Ok(Some(body)) => body,
            Ok(None) => {
                return Err(StepError::Unreadable(format!(
                    "the FHIR server's status manifest at {} is over {STATUS_MAX} bytes",
                    shown(url)
                )));
            }
            Err(e) => {
                return Err(StepError::Transient(format!(
                    "reading {}: {}",
                    shown(url),
                    e.without_url()
                )));
            }
        };
        let manifest = serde_json::from_slice::<Value>(&body).unwrap_or(Value::Null);
        check_status_manifest(&manifest, submission_id).map_err(|why| {
            StepError::Unreadable(format!(
                "the FHIR server's status manifest at {} {why}",
                shown(url)
            ))
        })?;
        Ok(Page::Done(manifest))
    }

    /// Reads a receipt file line by line, giving each non-blank line to
    /// `each`. A line over `LINE_MAX` makes the load unreadable.
    pub async fn lines(
        &self,
        url: &str,
        status_url: &str,
        mut each: impl FnMut(&[u8]) -> Result<(), StepError>,
    ) -> Result<(), StepError> {
        let mut response = self
            .get(
                &self.receipts,
                url,
                status_url,
                "application/fhir+ndjson, application/x-ndjson, */*",
            )
            .await?;
        let status = response.status().as_u16();
        if !response.status().is_success() {
            if matches!(status, 401 | 403) && self.credentials.is_some() {
                self.forget_token().await;
            }
            return Err(StepError::Transient(format!(
                "HTTP {status} fetching the receipts at {}",
                shown(url)
            )));
        }
        let mut line: Vec<u8> = Vec::new();
        let too_long = || {
            StepError::Unreadable(format!(
                "a receipt line at {} is longer than {} bytes",
                shown(url),
                LINE_MAX
            ))
        };
        loop {
            let chunk = response.chunk().await.map_err(|e| {
                StepError::Transient(format!("reading {}: {}", shown(url), e.without_url()))
            })?;
            let Some(chunk) = chunk else {
                break;
            };
            let mut rest: &[u8] = &chunk;
            while let Some(end) = rest.iter().position(|b| *b == b'\n') {
                if line.len() + end > LINE_MAX {
                    return Err(too_long());
                }
                line.extend_from_slice(&rest[..end]);
                if !line.iter().all(u8::is_ascii_whitespace) {
                    each(&line)?;
                }
                line.clear();
                rest = &rest[end + 1..];
            }
            if line.len() + rest.len() > LINE_MAX {
                return Err(too_long());
            }
            line.extend_from_slice(rest);
        }
        if !line.iter().all(u8::is_ascii_whitespace) {
            each(&line)?;
        }
        Ok(())
    }
}

/// Why a load is unreadable when the FHIR server's `$bulk-submit` is off.
pub const BULK_OFF: &str = "the FHIR server's $bulk-submit is off (HTTP 501)";

/// A status page's shape: a JSON object; `output`, `outcome` and `deleted`,
/// when present, arrays of objects each with a string `url`; `submissionId`,
/// when present, the one asked about. Why not, otherwise.
fn check_status_manifest(manifest: &Value, submission_id: &str) -> Result<(), String> {
    let Some(object) = manifest.as_object() else {
        return Err("is not a JSON object".into());
    };
    for member in ["output", "outcome", "deleted"] {
        let Some(entries) = object.get(member) else {
            continue;
        };
        let Some(entries) = entries.as_array() else {
            return Err(format!("has a {member} that is not an array"));
        };
        if entries
            .iter()
            .any(|e| !e.get("url").is_some_and(Value::is_string))
        {
            return Err(format!("has an {member} entry without a url"));
        }
    }
    if let Some(id) = object.get("submissionId")
        && id.as_str() != Some(submission_id)
    {
        return Err("is for another submission".into());
    }
    Ok(())
}

/// The issues logged when the FHIR server refuses the engine's token, never
/// the body itself: the status and, for an `OperationOutcome`, each issue's
/// severity, code and text (`diagnostics`, or `details.text` without one),
/// with `secrets` taken out and every URL cut to its origin, then each text
/// cut to 500 characters, at most 10 issues; for any other body, its length
/// only.
fn refusal_issues(status: u16, body: &[u8], secrets: &[String]) -> String {
    let outcome = serde_json::from_slice::<Value>(body)
        .ok()
        .filter(|v| v["resourceType"] == "OperationOutcome");
    let Some(outcome) = outcome else {
        return format!("HTTP {status}, a body of {} bytes", body.len());
    };
    let issues: Vec<String> = outcome["issue"]
        .as_array()
        .into_iter()
        .flatten()
        .take(10)
        .map(|issue| {
            let word = |key: &str| -> String {
                issue[key]
                    .as_str()
                    .map(|v| safe_reason(&redact(v, secrets)).chars().take(40).collect())
                    .unwrap_or_else(|| "-".into())
            };
            let text = issue["diagnostics"]
                .as_str()
                .or_else(|| issue.pointer("/details/text").and_then(Value::as_str))
                .unwrap_or("");
            let text: String = safe_reason(&redact(text, secrets))
                .chars()
                .take(500)
                .collect();
            format!("{} {}: {text}", word("severity"), word("code"))
        })
        .collect();
    if issues.is_empty() {
        format!("HTTP {status}")
    } else {
        format!("HTTP {status}: {}", issues.join("; "))
    }
}

/// `text` with every one of `secrets` replaced, should a server echo one.
fn redact(text: &str, secrets: &[String]) -> String {
    secrets
        .iter()
        .filter(|s| !s.is_empty())
        .fold(text.to_string(), |text, secret| {
            text.replace(secret.as_str(), "<redacted>")
        })
}

/// Reads a body up to `max` bytes: `None` when it is longer.
async fn read_capped(
    mut response: reqwest::Response,
    max: usize,
) -> reqwest::Result<Option<Vec<u8>>> {
    let mut body = Vec::new();
    while let Some(chunk) = response.chunk().await? {
        if body.len() + chunk.len() > max {
            return Ok(None);
        }
        body.extend_from_slice(&chunk);
    }
    Ok(Some(body))
}

/// `: <what the server said>`, made safe to store, or nothing.
async fn refusal_text(response: reqwest::Response) -> String {
    match read_capped(response, REFUSAL_MAX).await {
        Ok(Some(body)) if !body.is_empty() => {
            let text = serde_json::from_slice::<Value>(&body)
                .map(|v| crate::fhir::outcome_text(&v))
                .unwrap_or_else(|_| String::from_utf8_lossy(&body).into_owned());
            format!(": {}", safe_reason(text.trim()))
        }
        _ => String::new(),
    }
}

/// A group of failing outcomes: those whose first `error` or `fatal` issue
/// has this severity, code and text.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Group {
    pub severity: String,
    pub code: String,
    pub reason: String,
    pub count: i64,
}

/// What a submission's receipts said so far: kept with the submission after
/// every status page, so a poll that stops halfway resumes where it was.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct Tally {
    /// The status page to read next; `None` before the first.
    pub next: Option<String>,
    pub sent: i64,
    pub sent_by_type: BTreeMap<String, i64>,
    pub deleted: i64,
    /// Failing outcomes: those with an `error` or `fatal` issue.
    pub rejected: i64,
    /// In the order first seen.
    pub groups: Vec<Group>,
    /// Failing outcomes beyond `GROUPS_TRACKED` groups.
    pub other: i64,
    /// Whether a page listed any file (`output`, `outcome` or `deleted`).
    pub files: bool,
    /// Failed reads of a receipt file of the page `next` names, since that
    /// page was last read whole.
    pub receipt_failures: u32,
}

impl Tally {
    /// Counts one outcome line: a failing one adds to `rejected` and to its
    /// group. A line that is not an `OperationOutcome` makes the load
    /// unreadable.
    pub fn outcome(&mut self, line: &[u8], url: &str) -> Result<(), StepError> {
        let malformed = || {
            StepError::Unreadable(format!(
                "a receipt at {} is not an OperationOutcome",
                shown(url)
            ))
        };
        let value: Value = serde_json::from_slice(line).map_err(|_| malformed())?;
        if value["resourceType"] != "OperationOutcome" {
            return Err(malformed());
        }
        let Some(issue) = value["issue"].as_array().and_then(|issues| {
            issues
                .iter()
                .find(|i| matches!(i["severity"].as_str(), Some("error" | "fatal")))
        }) else {
            return Ok(());
        };
        self.rejected += 1;
        let severity = issue["severity"].as_str().unwrap_or("error").to_string();
        let code = issue["code"].as_str().unwrap_or("").to_string();
        let text = issue
            .pointer("/details/text")
            .or_else(|| issue.get("diagnostics"))
            .and_then(Value::as_str)
            .unwrap_or("");
        let reason = crate::events::reason_text(&safe_reason(text));
        if let Some(group) = self
            .groups
            .iter_mut()
            .find(|g| g.severity == severity && g.code == code && g.reason == reason)
        {
            group.count += 1;
        } else if self.groups.len() < GROUPS_TRACKED {
            self.groups.push(Group {
                severity,
                code,
                reason,
                count: 1,
            });
        } else {
            self.other += 1;
        }
        Ok(())
    }

    /// The groups a settled load keeps: the largest `GROUPS_KEPT`, ties in
    /// the order first seen.
    pub fn kept_groups(&self) -> Vec<Group> {
        let mut groups = self.groups.clone();
        groups.sort_by_key(|g| std::cmp::Reverse(g.count));
        groups.truncate(GROUPS_KEPT);
        groups
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_stored_url_loses_its_query_fragment_and_user() {
        assert_eq!(
            stored_url("https://u:p@provider.example/exports/m.json?sig=CANARY#x"),
            "https://provider.example/exports/m.json?…"
        );
        assert_eq!(
            stored_url("https://provider.example/m.json"),
            "https://provider.example/m.json"
        );
        assert_eq!(
            origin("https://provider.example:8443/a/b?c=d").as_deref(),
            Some("https://provider.example:8443")
        );
    }

    #[test]
    fn a_request_hash_does_not_depend_on_member_order() {
        let a = read_request(br#"{"manifest": {"output": [{"type": "Patient", "url": "https://p.example/a"}], "error": []}, "request_id": "k"}"#).unwrap();
        let b = read_request(br#"{"request_id": "other", "manifest": {"error": [], "output": [{"url": "https://p.example/a", "type": "Patient"}]}}"#).unwrap();
        assert_eq!(a.hash, b.hash);
        let c = read_request(br#"{"manifest_url": "https://p.example/m.json"}"#).unwrap();
        assert_ne!(a.hash, c.hash);
    }

    #[test]
    fn outcomes_count_and_group_by_their_first_failing_issue() {
        let mut tally = Tally::default();
        let line = |severity: &str, code: &str, text: &str| {
            json!({"resourceType": "OperationOutcome", "issue": [
                {"severity": "information", "code": "informational", "diagnostics": "noise"},
                {"severity": severity, "code": code, "diagnostics": text}]})
            .to_string()
        };
        for l in [
            line(
                "error",
                "not-found",
                "fetching https://p.example/f?sig=X failed",
            ),
            line(
                "error",
                "not-found",
                "fetching https://p.example/f?sig=Y failed",
            ),
            line("fatal", "exception", "boom"),
            line("warning", "processing", "only a warning"),
        ] {
            tally.outcome(l.as_bytes(), "https://hfs/x").unwrap();
        }
        assert_eq!(tally.rejected, 3);
        let groups = tally.kept_groups();
        assert_eq!(groups[0].count, 2);
        assert_eq!(groups[0].reason, "fetching https://p.example/… failed");
        assert_eq!(
            (groups[1].severity.as_str(), groups[1].code.as_str()),
            ("fatal", "exception")
        );
        assert!(
            tally
                .outcome(b"{\"resourceType\":\"Patient\"}", "https://hfs/x")
                .is_err()
        );
    }
}
