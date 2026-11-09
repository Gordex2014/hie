use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Result, anyhow};
use serde_json::Value;
use tokio::sync::Mutex;

use crate::fhir::DeliveryKey;
use crate::worker::{
    Exchange, Generation, GenerationReport, Launch, MAX_LINE, Verdict, WorkerLimits,
    resolve_interpreter,
};

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

/// One feed's transform script, run by a long-lived Python worker under the
/// `limited-process@1` contract (see `crate::worker`).
pub struct Transformer {
    /// The interpreter as configured; resolved again for every new worker, so
    /// an interpreter installed or a `PATH` fixed later is picked up.
    python: String,
    launcher: PathBuf,
    worker_script: PathBuf,
    transform_script: PathBuf,
    /// The HTS the worker may ask, handed over as `HIE_TERMINOLOGY_SERVER`.
    terminology: Option<String>,
    limits: WorkerLimits,
    run_root: Option<PathBuf>,
    /// The current generation; taken out for the length of a call, so a call
    /// that is cancelled takes its half-used process with it.
    worker: Arc<Mutex<Option<Generation>>>,
}

fn absolute(path: &Path) -> PathBuf {
    std::path::absolute(path).unwrap_or_else(|_| path.to_path_buf())
}

impl Transformer {
    pub fn new(python: &str, runtime_dir: &Path, transform_script: &Path) -> Self {
        Self {
            python: python.to_string(),
            launcher: absolute(&runtime_dir.join("limits.py")),
            worker_script: absolute(&runtime_dir.join("worker.py")),
            transform_script: absolute(transform_script),
            terminology: None,
            limits: WorkerLimits::default(),
            run_root: None,
            worker: Arc::new(Mutex::new(None)),
        }
    }

    pub fn set_terminology(&mut self, url: Option<&str>) {
        self.terminology = url.map(str::to_string);
    }

    /// The limits and deadlines each worker process runs under.
    pub fn set_limits(&mut self, limits: WorkerLimits) {
        self.limits = limits;
    }

    /// Where the workers' run directories are made; the system temporary
    /// directory by default.
    pub fn set_run_root(&mut self, dir: &Path) {
        self.run_root = Some(absolute(dir));
    }

    /// Transforms a batch of `(record, context)` pairs, one worker round trip
    /// per record. One outcome per pair, in order. An error means the worker
    /// could not run, not the data; the outcomes decided before it are not
    /// returned.
    pub async fn transform_many(&self, items: &[(Value, Value)]) -> Result<Vec<Outcome>> {
        let mut outcomes = Vec::with_capacity(items.len());
        for (record, context) in items {
            outcomes.push(self.transform(record, context).await?);
        }
        Ok(outcomes)
    }

    fn launch(&self) -> Launch {
        let mut extra_env = Vec::new();
        if let Some(url) = &self.terminology {
            extra_env.push(("HIE_TERMINOLOGY_SERVER".to_string(), url.clone()));
        }
        // Passed on purpose: the transform's `lookup` and `translate` send it.
        if let Ok(token) = std::env::var("HIE_TERMINOLOGY_TOKEN") {
            extra_env.push(("HIE_TERMINOLOGY_TOKEN".to_string(), token));
        }
        Launch {
            python: resolve_interpreter(&self.python, std::env::var_os("PATH").as_deref()),
            launcher: self.launcher.clone(),
            program: self.worker_script.clone(),
            args: vec![self.transform_script.clone()],
            extra_env,
            limits: self.limits.clone(),
            run_root: self.run_root.clone(),
            label: self.transform_script.display().to_string(),
        }
    }

    /// Transforms one record. An error means the worker could not be started
    /// (or the call was cancelled): the record stays queued and is tried again.
    /// A worker that times out, dies or breaks the protocol on this record is
    /// the record's outcome instead: it is rejected, never sent again by the
    /// engine, and the next record gets a new worker.
    pub async fn transform(&self, record: &Value, context: &Value) -> Result<Outcome> {
        let mut slot = self.worker.lock().await;
        let mut generation = match slot.take() {
            Some(generation) if !generation.has_exited() => generation,
            exited => {
                if let Some(generation) = exited {
                    generation.close().await;
                }
                let generation = Generation::start(&self.launch()).await.map_err(|reason| {
                    anyhow!(
                        "transform {} could not start: {reason}",
                        self.transform_script.display()
                    )
                })?;
                self.watch_idle(&generation);
                generation
            }
        };
        let request = self.limits.request;
        let rejected = |headline: String, report: GenerationReport| {
            Ok(Outcome::Rejected {
                kind: "transform".into(),
                message: rejection(&headline, &report),
            })
        };
        match generation.exchange(record, context, request).await {
            Exchange::Reply(reply) => {
                *slot = Some(generation);
                Ok(parse_reply(&reply))
            }
            // worker.py keeps file descriptor 1 off the protocol, so a line
            // that is not JSON came from something else: its reply may still
            // be in the pipe, and the next record needs a new worker.
            Exchange::NotJson(stray) => {
                let report = generation.retire(Verdict::UserError, Some("unknown")).await;
                rejected(
                    format!("something wrote to the worker's stdout: {stray:?}"),
                    report,
                )
            }
            Exchange::TimedOut => {
                let report = generation.retire(Verdict::TimedOut, None).await;
                rejected(
                    format!(
                        "the transform did not answer within {}",
                        crate::worker::seconds(request)
                    ),
                    report,
                )
            }
            Exchange::OutOfOrder => {
                let report = generation.retire(Verdict::UserError, Some("unknown")).await;
                rejected("the transform's reply was out of order".into(), report)
            }
            Exchange::Overlong => {
                let report = generation.retire(Verdict::UserError, Some("unknown")).await;
                rejected(
                    format!(
                        "the transform's reply exceeded {} MiB",
                        MAX_LINE / (1024 * 1024)
                    ),
                    report,
                )
            }
            Exchange::Ended => {
                let report = generation.ended().await;
                let headline = match (report.verdict, report.cause) {
                    (Verdict::LimitExceeded, Some("memory")) => format!(
                        "the transform exceeded its memory limit ({} MiB)",
                        self.limits.memory_mib
                    ),
                    (Verdict::LimitExceeded, Some("file")) => format!(
                        "the transform wrote a file past its size limit ({} MiB)",
                        self.limits.file_mib
                    ),
                    _ => match (report.exit_code, report.signal) {
                        (Some(code), _) => {
                            format!("the transform's process exited with code {code}")
                        }
                        (_, Some(signal)) => {
                            format!("the transform's process was killed by signal {signal}")
                        }
                        _ => "the transform's process stopped answering".into(),
                    },
                };
                rejected(headline, report)
            }
        }
    }

    /// Closes the generation as soon as its process exits while it waits for
    /// the next record, so nothing it started runs on and its directory goes.
    /// A call in progress notices the exit itself.
    fn watch_idle(&self, generation: &Generation) {
        let slot = Arc::downgrade(&self.worker);
        let serial = generation.serial();
        let mut exited = generation.exit_watch();
        tokio::spawn(async move {
            let _ = exited.wait_for(Option::is_some).await;
            let Some(slot) = slot.upgrade() else {
                return;
            };
            let mut slot = slot.lock().await;
            if slot.as_ref().is_some_and(|g| g.serial() == serial) {
                let generation = slot.take().expect("checked above");
                drop(slot);
                generation.close().await;
            }
        });
    }

    /// Ends the worker normally, if one is running: its stdin is closed and it
    /// exits. The report says how it went.
    pub async fn close(&self) -> Option<GenerationReport> {
        let generation = self.worker.lock().await.take()?;
        Some(generation.close().await)
    }
}

/// A rejected record's message: what happened, the contract's verdict and the
/// end of the worker's stderr.
fn rejection(headline: &str, report: &GenerationReport) -> String {
    let cause = report
        .cause
        .map(|c| format!(", cause {c}"))
        .unwrap_or_default();
    let mut message = format!(
        "{headline}; its worker was restarted (verdict {}{cause})",
        report.verdict
    );
    let tail = report.short_tail();
    if !tail.is_empty() {
        message.push('\n');
        message.push_str(tail);
    }
    message
}

fn parse_reply(reply: &Value) -> Outcome {
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
    use serde_json::json;

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
