//! What a dispatched run proved, item by item, its validation, and the
//! contract's answer built from it.

use serde_json::{Value, json};
use tokio::sync::watch;
use tokio::time::Instant;

use super::dispatch::stopped;

use super::TryInput;
use crate::fhir::{FhirClient, Validated};
use crate::ids::now_rfc3339_ms;
use crate::transform::check_resource;
use crate::versions::Environment;

/// One requested item, as the dispatch left it.
#[derive(Debug, Clone)]
pub enum Item {
    /// The executor answered for it: `{"resources"}` or `{"error"}`, and its
    /// diagnostics exactly as returned.
    Reported {
        result: Value,
        diagnostics: Option<Value>,
        validation: Option<Value>,
    },
    /// Dispatched, without an answer.
    NotReported,
    NotStarted,
}

impl Item {
    /// The item of an executor's reply line.
    pub fn of_reply(reply: &Value) -> Item {
        let result = match reply.get("error") {
            Some(error) => {
                let mut e = serde_json::Map::new();
                for key in ["kind", "message", "trace"] {
                    if let Some(v) = error.get(key) {
                        e.insert(key.into(), v.clone());
                    }
                }
                json!({"error": e})
            }
            None => {
                json!({"resources": reply.get("resources").cloned().unwrap_or_else(|| json!([]))})
            }
        };
        Item::Reported {
            result,
            diagnostics: reply.get("diagnostics").cloned(),
            validation: None,
        }
    }
}

/// A run once its dispatch ended.
#[derive(Debug, Clone)]
pub struct Dispatched {
    /// `completed`, `stopped`, `indeterminate` or `not_started`.
    pub outcome: &'static str,
    pub reason: Option<String>,
    /// `confirmed` or `unconfirmed`.
    pub termination: &'static str,
    pub cleanup: &'static str,
    pub environment: Option<Environment>,
    pub load_error: Option<Value>,
    pub items: Vec<Item>,
}

impl Dispatched {
    /// Every item the same way, nothing confirmed unless said.
    pub fn all(n: usize, outcome: &'static str, reason: &str, confirmed: bool, item: Item) -> Self {
        let c = if confirmed {
            "confirmed"
        } else {
            "unconfirmed"
        };
        Self {
            outcome,
            reason: Some(reason.to_string()),
            termination: c,
            cleanup: c,
            environment: None,
            load_error: None,
            items: vec![item; n],
        }
    }
}

/// Validates every resource of every reported item with the destination's
/// `$validate`: `completed`, or `unavailable` when the destination could not
/// validate (then no item carries validation); `skipped` when nothing was
/// reported. Validation is part of the run: a cancel or the run's deadline
/// stops it at once (no further request), no item keeps validation, it reads
/// `unavailable` and the run `stopped`.
pub async fn validate(
    fhir: &FhirClient,
    d: &mut Dispatched,
    cancelled: &mut watch::Receiver<bool>,
    deadline: Instant,
) -> &'static str {
    let mut any = false;
    let mut verdicts: Vec<Option<Value>> = vec![None; d.items.len()];
    for (index, item) in d.items.iter().enumerate() {
        let Item::Reported { result, .. } = item else {
            continue;
        };
        any = true;
        let Some(resources) = result.get("resources").and_then(Value::as_array) else {
            continue;
        };
        let mut checked = Vec::with_capacity(resources.len());
        for (i, resource) in resources.iter().enumerate() {
            let verdict = tokio::select! {
                v = fhir.validate(resource) => v,
                stop = stopped(cancelled, deadline) => {
                    clear(d);
                    d.outcome = "stopped";
                    d.reason = Some(stop.reason().into());
                    return "unavailable";
                }
            };
            match verdict {
                Validated::Outcome { issues } => {
                    let valid = !issues.iter().any(|i| i.is_error());
                    checked.push(json!({"resource": i, "valid": valid, "issues": issues}));
                }
                Validated::Unavailable => {
                    clear(d);
                    return "unavailable";
                }
            }
        }
        verdicts[index] = Some(Value::Array(checked));
    }
    for (item, verdict) in d.items.iter_mut().zip(verdicts) {
        if let Item::Reported { validation, .. } = item {
            *validation = verdict;
        }
    }
    if any { "completed" } else { "skipped" }
}

fn clear(d: &mut Dispatched) {
    for item in &mut d.items {
        if let Item::Reported { validation, .. } = item {
            *validation = None;
        }
    }
}

/// The run's metadata (what the registry keeps), its coverage, and the whole
/// `200` body.
pub struct Built {
    pub run: Value,
    pub coverage: Value,
    pub body: Value,
}

pub fn build(input: &TryInput, d: &Dispatched, validation: &str, started_at: &str) -> Built {
    let reported = d
        .items
        .iter()
        .filter(|i| matches!(i, Item::Reported { .. }))
        .count();
    let requested = input.records.len();
    let run = json!({
        "id": input.run_id,
        "status": "finished",
        "outcome": d.outcome,
        "reason": d.reason,
        "termination": d.termination,
        "cleanup": d.cleanup,
        "transform": input.saved.as_ref().map(|s| json!({"id": s.transform_id, "version": s.version})),
        "code_sha256": crate::formats::sha256_hex(input.code.as_bytes()),
        "environment": d.environment,
        "validation": validation,
        "load_error": d.load_error,
        "started_at": started_at,
        "finished_at": now_rfc3339_ms(),
    });
    let coverage = json!({"requested": requested, "reported": reported,
        "complete": reported == requested});
    let mut items = Vec::with_capacity(requested);
    let mut results = Vec::with_capacity(requested);
    for (index, (item, record)) in d.items.iter().zip(&input.records).enumerate() {
        match item {
            Item::Reported {
                result,
                diagnostics,
                validation,
            } => {
                let mut entry = json!({"index": index, "disposition": "reported",
                    "result": result, "validation": validation});
                if let Some(diagnostics) = diagnostics {
                    entry["diagnostics"] = diagnostics.clone();
                }
                items.push(entry);
                results.push(legacy_result(record, result));
            }
            Item::NotReported | Item::NotStarted => {
                let disposition = if matches!(item, Item::NotStarted) {
                    "not_started"
                } else {
                    "not_reported"
                };
                items.push(json!({"index": index, "disposition": disposition, "validation": null}));
                results.push(json!({"record": record, "error": {"kind": "transform",
                    "message": format!("the record was not run to the end ({})", d.reason.as_deref().unwrap_or(d.outcome))}}));
            }
        }
    }
    let body = json!({"run": run, "items": items, "coverage": coverage, "results": results});
    Built {
        run,
        coverage,
        body,
    }
}

/// The 0.1 answer for one record: its resources, or why it was rejected (a
/// resource HIE could not deliver is a `validation` error, as before).
fn legacy_result(record: &Value, result: &Value) -> Value {
    if let Some(error) = result.get("error") {
        let mut error = error.clone();
        if let (Some(trace), Some(message)) = (
            error
                .get("trace")
                .and_then(Value::as_str)
                .map(str::to_string),
            error
                .get("message")
                .and_then(Value::as_str)
                .map(str::to_string),
        ) {
            error["message"] = json!(format!("{message}\n{trace}"));
            if let Some(e) = error.as_object_mut() {
                e.remove("trace");
            }
        }
        return json!({"record": record, "error": error});
    }
    let resources = result
        .get("resources")
        .cloned()
        .unwrap_or_else(|| json!([]));
    let problem = resources
        .as_array()
        .and_then(|list| list.iter().find_map(|r| check_resource(r).err()));
    match problem {
        Some(message) => json!({"record": record, "resources": resources,
            "error": {"kind": "validation", "message": message}}),
        None => json!({"record": record, "resources": resources}),
    }
}
