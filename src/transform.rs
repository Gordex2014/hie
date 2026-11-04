//! What a transform made of a record ([`Outcome`]), the engine's own Python
//! worker (one process per transform script, one JSON line per record), and
//! the checks HIE itself makes on the resources a transform returns.

use std::path::{Path, PathBuf};
use std::process::Stdio;

use anyhow::{Result, anyhow, bail};
use serde_json::{Value, json};

use crate::fhir::DeliveryKey;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, ChildStdin, ChildStdout, Command};
use tokio::sync::Mutex;

/// What a transform made of one record.
#[derive(Debug, Clone, PartialEq)]
pub enum Outcome {
    Resources(Vec<Value>),
    Rejected {
        kind: String,
        message: String,
    },
    /// Something the transform depends on (the terminology server) was not
    /// there: the record is kept and tried again later.
    Transient(String),
}

/// How to start the engine's own Python worker over one script.
#[derive(Debug, Clone)]
pub struct WorkerCommand {
    pub python: String,
    pub worker_script: PathBuf,
    pub transform_script: PathBuf,
    /// The HTS the worker may ask, handed over as `HIE_TERMINOLOGY_SERVER`.
    pub terminology: Option<String>,
    /// Whether the transform's own output (its `print`s, which the worker sends
    /// to stderr) reaches the engine's stderr; Try discards it.
    pub inherit_stderr: bool,
}

/// Why a worker did not start: the script did not load (with the worker's
/// `{"message", "trace"}`), or the process itself failed.
#[derive(Debug)]
pub enum SpawnError {
    LoadFailed(Value),
    Process(anyhow::Error),
}

impl std::fmt::Display for SpawnError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SpawnError::LoadFailed(e) => write!(
                f,
                "the transform failed to load: {}",
                e.get("message")
                    .and_then(Value::as_str)
                    .unwrap_or("no detail")
            ),
            SpawnError::Process(e) => write!(f, "{e:#}"),
        }
    }
}

/// One running worker process: requests in on stdin, replies out on stdout,
/// in order. Killed when dropped.
pub struct Worker {
    child: Child,
    stdin: ChildStdin,
    stdout: BufReader<ChildStdout>,
    next_id: u64,
}

impl Worker {
    /// Starts the worker and waits for its first line.
    pub async fn spawn(command: &WorkerCommand) -> std::result::Result<Self, SpawnError> {
        let mut cmd = Command::new(&command.python);
        cmd.arg("-u")
            .arg(&command.worker_script)
            .arg(&command.transform_script)
            // The protocol is UTF-8 on every platform, whatever the locale.
            .env("PYTHONIOENCODING", "utf-8")
            .env("PYTHONUTF8", "1");
        if let Some(url) = &command.terminology {
            cmd.env("HIE_TERMINOLOGY_SERVER", url);
        }
        let mut child = cmd
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(if command.inherit_stderr {
                Stdio::inherit()
            } else {
                Stdio::null()
            })
            .kill_on_drop(true)
            .spawn()
            .map_err(|e| {
                SpawnError::Process(anyhow!(
                    "starting {} {}: {e}",
                    command.python,
                    command.worker_script.display()
                ))
            })?;
        let stdin = child
            .stdin
            .take()
            .ok_or_else(|| SpawnError::Process(anyhow!("no stdin")))?;
        let mut stdout = BufReader::new(
            child
                .stdout
                .take()
                .ok_or_else(|| SpawnError::Process(anyhow!("no stdout")))?,
        );
        let mut line = String::new();
        stdout
            .read_line(&mut line)
            .await
            .map_err(|e| SpawnError::Process(e.into()))?;
        let first: Value = serde_json::from_str(&line).unwrap_or(Value::Null);
        if first.get("ready") != Some(&json!(true)) {
            let _ = child.kill().await;
            return Err(match first.get("load_error") {
                Some(e) => SpawnError::LoadFailed(e.clone()),
                None => SpawnError::Process(anyhow!(
                    "transform {} failed to load",
                    command.transform_script.display()
                )),
            });
        }
        Ok(Worker {
            child,
            stdin,
            stdout,
            next_id: 1,
        })
    }

    /// One record through the worker: its reply line.
    pub async fn call(&mut self, record: &Value, context: &Value) -> Result<Value> {
        let id = self.next_id;
        self.next_id += 1;
        let mut request = json!({"id": id, "record": record, "context": context}).to_string();
        request.push('\n');
        self.stdin.write_all(request.as_bytes()).await?;
        self.stdin.flush().await?;
        let mut line = String::new();
        if self.stdout.read_line(&mut line).await? == 0 {
            bail!("transform process exited");
        }
        // A reply that cannot be read is the transform's fault for this record,
        // not the process's: the worker answered in order and stays; the record
        // is rejected with the reason rather than tried again forever.
        let reply: Value = match serde_json::from_str(&line) {
            Ok(v) => v,
            Err(e) => json!({"id": id, "error": {"kind": "transform",
                "message": format!("the transform's reply could not be read as JSON ({e}); the resources it returned are not valid JSON")}}),
        };
        if reply.get("id") != Some(&json!(id)) {
            bail!("transform reply out of order");
        }
        Ok(reply)
    }

    /// Stops the process and waits until it is gone.
    pub async fn kill(mut self) -> Result<()> {
        self.child.kill().await?;
        Ok(())
    }
}

/// One transform script run by a long-lived worker of this engine, restarted
/// once when it fails mid-record (the load path; Try never retries).
pub struct Transformer {
    command: WorkerCommand,
    worker: Mutex<Option<Worker>>,
}

impl Transformer {
    pub fn new(python: &str, runtime_dir: &Path, transform_script: &Path) -> Self {
        Self {
            command: WorkerCommand {
                python: python.to_string(),
                worker_script: runtime_dir.join("worker.py"),
                transform_script: transform_script.to_path_buf(),
                terminology: None,
                inherit_stderr: true,
            },
            worker: Mutex::new(None),
        }
    }

    pub fn set_terminology(&mut self, url: Option<&str>) {
        self.command.terminology = url.map(str::to_string);
    }

    /// Transforms a batch of `(record, context)` pairs, one worker round trip
    /// per record. One outcome per pair, in order. An error means the process
    /// failed, not the data: nothing in the batch is decided.
    pub async fn transform_many(&self, items: &[(Value, Value)]) -> Result<Vec<Outcome>> {
        let mut outcomes = Vec::with_capacity(items.len());
        for (record, context) in items {
            outcomes.push(self.transform(record, context).await?);
        }
        Ok(outcomes)
    }

    /// Transforms one record. An error here means the transform process itself failed,
    /// not the data: the record stays queued and is tried again.
    pub async fn transform(&self, record: &Value, context: &Value) -> Result<Outcome> {
        let mut guard = self.worker.lock().await;
        for attempt in 0..2 {
            if guard.is_none() {
                *guard = Some(
                    Worker::spawn(&self.command)
                        .await
                        .map_err(|e| anyhow!("{e}"))?,
                );
            }
            let worker = guard.as_mut().expect("worker present");
            match worker.call(record, context).await {
                Ok(reply) => return Ok(parse_reply(&reply)),
                Err(e) => {
                    if let Some(w) = guard.take() {
                        let _ = w.kill().await;
                    }
                    if attempt == 1 {
                        return Err(e);
                    }
                }
            }
        }
        unreachable!()
    }
}

/// A worker's (or the runtime's) reply line as an outcome; a trace is kept
/// after the message.
pub fn parse_reply(reply: &Value) -> Outcome {
    if let Some(err) = reply.get("error") {
        let kind = err
            .get("kind")
            .and_then(Value::as_str)
            .unwrap_or("transform")
            .to_string();
        let mut message = err
            .get("message")
            .and_then(Value::as_str)
            .unwrap_or("transform failed")
            .to_string();
        if let Some(trace) = err.get("trace").and_then(Value::as_str) {
            message = format!("{message}\n{trace}");
        }
        if kind == "transient" {
            return Outcome::Transient(message);
        }
        return Outcome::Rejected { kind, message };
    }
    let resources = reply
        .get("resources")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    Outcome::Resources(resources)
}

/// Gives a resource that has only an identifier an `id` derived from it, so it
/// can go where only ids are accepted (`$bulk-submit`) and still be the same
/// resource every time it is sent: 48 hex characters of the SHA-256 of
/// `system|value`, which fits FHIR's id rules.
pub fn derive_id(resource: &mut Value) -> std::result::Result<DeliveryKey, String> {
    match check_resource(resource)? {
        key @ DeliveryKey::Id { .. } => Ok(key),
        DeliveryKey::Identifier {
            resource_type,
            system,
            value,
        } => {
            use sha2::Digest;
            let digest = sha2::Sha256::digest(format!("{system}|{value}").as_bytes());
            let id = hex::encode(digest)[..48].to_string();
            resource["id"] = Value::String(id.clone());
            Ok(DeliveryKey::Id { resource_type, id })
        }
    }
}

/// Checks what HIE itself relies on: a resource type, and a key that makes delivery
/// idempotent. A resource with an `id` is written to `{type}/{id}`; one without needs
/// a business identifier and is written as a conditional update on it.
pub fn check_resource(resource: &Value) -> std::result::Result<DeliveryKey, String> {
    let rt = resource
        .get("resourceType")
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .ok_or("resource has no resourceType")?;
    if let Some(id) = resource
        .get("id")
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
    {
        return Ok(DeliveryKey::Id {
            resource_type: rt.to_string(),
            id: id.to_string(),
        });
    }
    let ident = resource
        .get("identifier")
        .and_then(Value::as_array)
        .and_then(|ids| ids.first())
        .ok_or_else(|| {
            format!(
                "{rt} has neither id nor identifier; HIE needs one to avoid duplicates on re-send"
            )
        })?;
    let system = ident
        .get("system")
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty());
    let value = ident
        .get("value")
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty());
    match (system, value) {
        (Some(s), Some(v)) => Ok(DeliveryKey::Identifier {
            resource_type: rt.to_string(),
            system: s.to_string(),
            value: v.to_string(),
        }),
        _ => Err(format!("{rt}.identifier[0] needs both system and value")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn replies_map_to_outcomes() {
        assert_eq!(
            parse_reply(&json!({"id": 1, "resources": [{"resourceType": "Observation"}]})),
            Outcome::Resources(vec![json!({"resourceType": "Observation"})])
        );
        assert_eq!(
            parse_reply(&json!({"id": 1, "error": {"kind": "data", "message": "bad value"}})),
            Outcome::Rejected {
                kind: "data".into(),
                message: "bad value".into()
            }
        );
    }

    #[test]
    fn resources_need_an_id_or_an_identifier() {
        let by_identifier = json!({"resourceType": "Observation", "identifier": [{"system": "urn:x", "value": "1"}]});
        assert_eq!(
            check_resource(&by_identifier).unwrap(),
            DeliveryKey::Identifier {
                resource_type: "Observation".into(),
                system: "urn:x".into(),
                value: "1".into()
            }
        );
        let by_id =
            json!({"resourceType": "Patient", "id": "p1", "identifier": [{"value": "mrn"}]});
        assert_eq!(
            check_resource(&by_id).unwrap(),
            DeliveryKey::Id {
                resource_type: "Patient".into(),
                id: "p1".into()
            }
        );
        assert!(
            check_resource(&json!({"resourceType": "Observation"}))
                .unwrap_err()
                .contains("neither id nor identifier")
        );
        assert!(check_resource(&json!({"identifier": []})).is_err());
    }
}
