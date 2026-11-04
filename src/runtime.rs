//! python-runtime's HIE facade as a transform executor (`runtime:` in
//! `hie.yaml`; decisions 34 and 39): the consumer's capabilities (its default
//! environment), a version's code stored under a name (`PUT
//! /transforms/{name}`), batches run against it (`POST
//! /transforms/{name}/run`), inline code tried once (`POST /transforms/try`),
//! and names listed and deleted (`GET /transforms`, `DELETE
//! /transforms/{name}`). Every body that runs or stores code names the
//! version's environment; every answer after a partition started names the
//! environment that ran it in `Python-Runtime-Environment-*` headers.
//!
//! This is only the HTTP client: what an answer means for a load is
//! `versions::execute`'s, for a Try `tryrun`'s.

use std::collections::BTreeMap;
use std::time::Duration;

use anyhow::{Context, Result, anyhow};
use reqwest::StatusCode;
use reqwest::header::HeaderMap;
use serde_json::{Value, json};

use crate::versions::Environment;

/// The facade's own ceiling on requests per call.
pub const MAX_REQUESTS_PER_CALL: usize = 1000;
/// Longer than the facade's run budget (120 s by default), so a slow batch is
/// answered rather than cut on this side.
const CALL_TIMEOUT: Duration = Duration::from_secs(180);
/// Capabilities, listing and deleting answer at once or not at all.
const SHORT_TIMEOUT: Duration = Duration::from_secs(10);

/// The executor name versions saved for the runtime carry.
pub const EXECUTOR: &str = "python-runtime";

#[derive(Debug, Clone)]
pub struct RuntimeSettings {
    pub url: String,
    pub token: Option<String>,
    /// Requests per run call; the runtime partitions them further itself.
    pub batch_size: usize,
}

/// One answer of the facade: its status, body, the environment its headers
/// name (when a partition started) and its `Python-Runtime-Error` header (the
/// mark that the facade itself answered).
#[derive(Debug, Clone)]
pub struct Answer {
    pub status: StatusCode,
    pub body: Value,
    pub environment: Option<Environment>,
    pub facade_error: Option<String>,
}

impl Answer {
    /// The `error` code of an error body.
    pub fn error_code(&self) -> &str {
        self.body
            .get("error")
            .and_then(Value::as_str)
            .unwrap_or_default()
    }
}

/// What the consumer may run in: the default environment and those offered.
#[derive(Debug, Clone, Default)]
pub struct Capabilities {
    pub default_environment: Option<String>,
    /// Content id to descriptor digest.
    pub environments: BTreeMap<String, String>,
    pub aliases: BTreeMap<String, String>,
}

impl Capabilities {
    fn read(body: &Value) -> Self {
        let environments = body
            .get("environments")
            .and_then(Value::as_array)
            .map(|list| {
                list.iter()
                    .filter_map(|e| {
                        Some((
                            e.get("id")?.as_str()?.to_string(),
                            e.get("digest")?.as_str()?.to_string(),
                        ))
                    })
                    .collect()
            })
            .unwrap_or_default();
        let aliases = body
            .get("aliases")
            .and_then(Value::as_object)
            .map(|m| {
                m.iter()
                    .filter_map(|(k, v)| Some((k.clone(), v.as_str()?.to_string())))
                    .collect()
            })
            .unwrap_or_default();
        Self {
            default_environment: body
                .get("default_environment")
                .and_then(Value::as_str)
                .map(str::to_string),
            environments,
            aliases,
        }
    }

    /// The environment `id` (a content id or an alias) names, when offered.
    pub fn environment(&self, id: &str) -> Option<Environment> {
        let id = self.aliases.get(id).map(String::as_str).unwrap_or(id);
        self.environments.get(id).map(|digest| Environment {
            executor: EXECUTOR.into(),
            id: id.to_string(),
            digest: Some(digest.clone()),
        })
    }

    /// The default environment, when the runtime names one it offers.
    pub fn default(&self) -> Option<Environment> {
        self.environment(self.default_environment.as_deref()?)
    }
}

/// Why a Try's call has no answer.
#[derive(Debug)]
pub enum TryCallError {
    /// The request never reached the runtime: nothing ran.
    NotSent(anyhow::Error),
    /// It was sent and no answer came: anything may have run.
    Lost(anyhow::Error),
}

/// The facade client.
#[derive(Clone)]
pub struct Facade {
    settings: RuntimeSettings,
    client: reqwest::Client,
}

impl Facade {
    pub fn new(settings: RuntimeSettings) -> Self {
        // A redirect is not the facade's answer, and nothing is sent twice:
        // no redirects followed, no automatic retries (not even of a stream
        // the server refused), so a Try is dispatched at most once.
        let client = reqwest::Client::builder()
            .user_agent(concat!("hie/", env!("CARGO_PKG_VERSION")))
            .redirect(reqwest::redirect::Policy::none())
            .retry(reqwest::retry::never())
            .build()
            .expect("reqwest client");
        Self { settings, client }
    }

    pub fn settings(&self) -> &RuntimeSettings {
        &self.settings
    }

    fn base(&self) -> &str {
        self.settings.url.trim_end_matches('/')
    }

    fn authorized(&self, request: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        match &self.settings.token {
            Some(token) => request.bearer_auth(token),
            None => request,
        }
    }

    /// The consumer's `GET /v1/capabilities`.
    pub async fn capabilities(&self) -> Result<Capabilities> {
        let response = self
            .authorized(self.client.get(format!("{}/v1/capabilities", self.base())))
            .timeout(SHORT_TIMEOUT)
            .send()
            .await
            .map_err(|e| {
                anyhow!(
                    "the runtime at {} cannot be reached: {e}",
                    self.settings.url
                )
            })?;
        let status = response.status();
        if !status.is_success() {
            anyhow::bail!("the runtime's capabilities answered HTTP {status}");
        }
        let body: Value = response
            .json()
            .await
            .context("reading the runtime's capabilities")?;
        Ok(Capabilities::read(&body))
    }

    /// Stores `code` under `name`, for `environment`.
    pub async fn put(&self, name: &str, code: &str, environment: Option<&str>) -> Result<Answer> {
        let mut body = json!({"code": code});
        if let Some(env) = environment {
            body["environment"] = json!(env);
        }
        let request = self
            .client
            .put(format!("{}/transforms/{name}", self.base()))
            .timeout(CALL_TIMEOUT);
        self.send(request, &body).await
    }

    /// Runs the code stored under `name` over `requests`, in `environment`.
    pub async fn run(
        &self,
        name: &str,
        environment: Option<&str>,
        requests: &[(Value, Value)],
    ) -> Result<Answer> {
        let request = self
            .client
            .post(format!("{}/transforms/{name}/run", self.base()))
            .timeout(CALL_TIMEOUT);
        self.send(request, &envelope(requests, None, environment))
            .await
    }

    /// Runs `code` once over `requests`; nothing is stored, nothing retried.
    /// No timeout here: the caller stops it at its deadline by dropping it,
    /// which cancels the running partition.
    pub async fn try_code(
        &self,
        code: &str,
        environment: Option<&str>,
        requests: &[(Value, Value)],
    ) -> std::result::Result<Answer, TryCallError> {
        let request = self
            .authorized(self.client.post(format!("{}/transforms/try", self.base())))
            .json(&envelope(requests, Some(code), environment));
        let response = request.send().await.map_err(|e| {
            if e.is_connect() {
                TryCallError::NotSent(anyhow!("the runtime cannot be reached: {e}"))
            } else {
                TryCallError::Lost(anyhow!("the runtime's answer was lost: {e}"))
            }
        })?;
        read(response).await.map_err(TryCallError::Lost)
    }

    /// The names the consumer holds, with the digest of their code.
    pub async fn list(&self) -> Result<Vec<(String, String)>> {
        let answer = self
            .send_empty(
                self.client
                    .get(format!("{}/transforms", self.base()))
                    .timeout(SHORT_TIMEOUT),
            )
            .await?;
        if answer.status != StatusCode::OK {
            anyhow::bail!(
                "listing the runtime's transforms answered HTTP {}",
                answer.status
            );
        }
        Ok(answer
            .body
            .get("transforms")
            .and_then(Value::as_array)
            .map(|list| {
                list.iter()
                    .filter_map(|t| {
                        Some((
                            t.get("name")?.as_str()?.to_string(),
                            t.get("sha256")?.as_str().unwrap_or_default().to_string(),
                        ))
                    })
                    .collect()
            })
            .unwrap_or_default())
    }

    /// Frees a name: `true` once the runtime no longer holds it (deleted now,
    /// or `404 transform_unknown`).
    pub async fn delete(&self, name: &str) -> Result<bool> {
        let answer = self
            .send_empty(
                self.client
                    .delete(format!("{}/transforms/{name}", self.base()))
                    .timeout(SHORT_TIMEOUT),
            )
            .await?;
        Ok(answer.status == StatusCode::OK
            || (answer.status == StatusCode::NOT_FOUND
                && answer.error_code() == "transform_unknown"))
    }

    async fn send(&self, request: reqwest::RequestBuilder, body: &Value) -> Result<Answer> {
        let response = self
            .authorized(request.json(body))
            .send()
            .await
            .map_err(|e| {
                anyhow!(
                    "the runtime at {} cannot be reached: {e}",
                    self.settings.url
                )
            })?;
        read(response).await
    }

    async fn send_empty(&self, request: reqwest::RequestBuilder) -> Result<Answer> {
        let response = self.authorized(request).send().await.map_err(|e| {
            anyhow!(
                "the runtime at {} cannot be reached: {e}",
                self.settings.url
            )
        })?;
        read(response).await
    }
}

async fn read(response: reqwest::Response) -> Result<Answer> {
    let status = response.status();
    let headers = response.headers().clone();
    let text = response
        .text()
        .await
        .map_err(|e| anyhow!("reading the runtime's answer: {e}"))?;
    Ok(Answer {
        status,
        body: serde_json::from_str(&text).unwrap_or(Value::Null),
        environment: environment_of(&headers),
        facade_error: header(&headers, "python-runtime-error"),
    })
}

fn header(headers: &HeaderMap, name: &str) -> Option<String> {
    headers
        .get(name)
        .and_then(|v| v.to_str().ok())
        .map(str::to_string)
}

/// The environment an answer's headers name: only complete evidence, an id
/// (`[a-z0-9][a-z0-9.@-]`, at most 64 bytes) and a 64-hex descriptor digest;
/// anything less names none.
fn environment_of(headers: &HeaderMap) -> Option<Environment> {
    let id = header(headers, "python-runtime-environment-id")?;
    let digest = header(headers, "python-runtime-environment-digest")?;
    let id_ok = (1..=64).contains(&id.len())
        && id.starts_with(|c: char| c.is_ascii_lowercase() || c.is_ascii_digit())
        && id
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b"-.@".contains(&b));
    let digest_ok = digest.len() == 64
        && digest
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b));
    (id_ok && digest_ok).then(|| Environment {
        executor: EXECUTOR.into(),
        id,
        digest: Some(digest),
    })
}

fn envelope(requests: &[(Value, Value)], code: Option<&str>, environment: Option<&str>) -> Value {
    let requests: Vec<Value> = requests
        .iter()
        .enumerate()
        .map(|(i, (record, context))| json!({"id": i, "record": record, "context": context}))
        .collect();
    let mut body = json!({"requests": requests});
    if let Some(code) = code {
        body["code"] = json!(code);
    }
    if let Some(env) = environment {
        body["environment"] = json!(env);
    }
    body
}

/// What a refusal means for the operator, by the facade's status mapping.
pub fn describe(answer: &Answer) -> String {
    let code = answer.error_code();
    let status = answer.status;
    match status.as_u16() {
        422 => {
            let detail = answer
                .body
                .get("load_error")
                .and_then(|e| e.get("message"))
                .and_then(Value::as_str)
                .unwrap_or("no detail");
            format!("the transform failed to load ({detail})")
        }
        503 => {
            format!("the runtime is busy or not ready ({code}); the records wait for the next pass")
        }
        408 | 500 | 502 => format!("HTTP {status} {code}; the records wait for the next pass"),
        401 | 403 => format!("HTTP {status} {code}; check the runtime token and consumer profile"),
        400 | 405 | 409 | 413 | 415 => {
            format!("HTTP {status} {code}; check the runtime configuration")
        }
        _ => format!("HTTP {status} {code}"),
    }
}

/// The name one version's code is stored under at the runtime:
/// `{transform}-v{version}-{12 hex}`, the hex a SHA-256 of the whole identity
/// (transform id, version, code SHA-256), so a shortened transform id never
/// makes two versions share a name; within the facade's `[A-Za-z0-9_-]{1,64}`
/// and never `try`.
pub fn program_name(transform_id: &str, version: i64, code_sha256: &str) -> String {
    let identity = format!("{transform_id}\n{version}\n{code_sha256}");
    let hash = crate::formats::sha256_hex(identity.as_bytes());
    let suffix = format!("-v{version}-{}", &hash[..12]);
    let room = 64usize.saturating_sub(suffix.len());
    let stem: String = transform_id
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '_' || c == '-' {
                c
            } else {
                '_'
            }
        })
        .take(room)
        .collect();
    let stem = if stem.is_empty() {
        "t".to_string()
    } else {
        stem
    };
    format!("{stem}{suffix}")
}

/// Whether a name at the runtime has the shape [`program_name`] gives.
pub fn is_program_name(name: &str) -> bool {
    let Some((head, sha)) = name.rsplit_once('-') else {
        return false;
    };
    let Some((_, version)) = head.rsplit_once("-v") else {
        return false;
    };
    sha.len() == 12
        && sha
            .bytes()
            .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
        && !version.is_empty()
        && version.bytes().all(|b| b.is_ascii_digit())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn names_follow_the_facade_rules_and_differ_per_version_and_content() {
        let sha = "0123456789abcdef".repeat(4);
        assert!(program_name("lab_results", 3, &sha).starts_with("lab_results-v3-"));
        // Two ids that only differ past the shortening keep two names.
        let a = format!("{}a", "x".repeat(70));
        let b = format!("{}b", "x".repeat(70));
        assert_ne!(program_name(&a, 1, &sha), program_name(&b, 1, &sha));
        assert!(is_program_name(&program_name("lab_results", 3, &sha)));
        let long = program_name(&"x".repeat(80), 12, &sha);
        assert_eq!(long.len(), 64);
        assert!(
            long.bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
        );
        assert_ne!(program_name("t", 1, &sha), program_name("t", 2, &sha));
        assert_ne!(
            program_name("t", 1, &sha),
            program_name("t", 1, &"f".repeat(64))
        );
        assert_ne!(program_name("try", 1, &sha), "try");
        assert!(!is_program_name("orders"));
    }
}
