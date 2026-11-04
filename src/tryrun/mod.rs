//! Try (decision 39): code inline or a saved version run once on a few
//! records, storing nothing and writing nothing to the destination.
//!
//! - A run id is reserved for 24 hours before anything runs (`409
//!   run_id_in_use`, nothing run); a client that lost its answer reads
//!   [`get`] instead of sending the run again.
//! - At most `try.max_concurrent` runs execute at once per engine (`429
//!   try_busy`).
//! - The run executes on a task of its own, so it finishes, and its outcome is
//!   recorded, even if the client goes away; the registry keeps only the run's
//!   metadata and coverage, never inputs, results or diagnostics.
//! - It is dispatched at most once (`dispatch`): never through the retrying
//!   load path, never sent again after an uncertain answer. Cancel and the
//!   deadline stop it; what the executor confirms about termination and
//!   cleanup is reported, nothing more.
//! - Resources produced are checked with the destination's `$validate` when
//!   asked (`validate`).

mod answer;
mod dispatch;

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde_json::{Value, json};

use crate::fhir::FhirClient;
use crate::ids::now_rfc3339_ms;
use crate::manager::{ErrorStatus, ManagerError};
use crate::versions::{Environment, Versions};

pub use answer::Dispatched;

/// The most records one run takes.
pub const MAX_RECORDS: usize = 100;
/// How long after its deadline a run nobody finished is read as lost (the
/// engine that ran it stopped).
const STALE_AFTER_DEADLINE: Duration = Duration::from_secs(120);

/// A saved version a run executes.
#[derive(Debug, Clone)]
pub struct SavedVersion {
    pub transform_id: String,
    pub version: i64,
    pub environment: Environment,
}

/// One run as asked for, its inputs resolved to records.
#[derive(Debug, Clone)]
pub struct TryInput {
    pub run_id: String,
    pub code: String,
    /// `None` for inline code.
    pub saved: Option<SavedVersion>,
    pub records: Vec<Value>,
    pub deadline: Duration,
    pub validate: bool,
}

/// How a running run is stopped, and whether it has its answer.
#[derive(Debug)]
struct Control {
    cancel: tokio::sync::watch::Sender<bool>,
    state: Mutex<State>,
}

#[derive(Debug, Default)]
struct State {
    cancelled: bool,
    finished: bool,
}

/// The engine's Try runs: those executing here, and how many.
#[derive(Debug, Default)]
pub struct TryRuns {
    active: Mutex<HashMap<String, Arc<Control>>>,
    running: Arc<AtomicUsize>,
}

/// A slot of `try.max_concurrent`, given back when dropped.
struct Permit(Arc<AtomicUsize>);

impl Drop for Permit {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::AcqRel);
    }
}

impl TryRuns {
    fn permit(&self, max: usize) -> Option<Permit> {
        let mut n = self.running.load(Ordering::Acquire);
        loop {
            if n >= max {
                return None;
            }
            match self
                .running
                .compare_exchange(n, n + 1, Ordering::AcqRel, Ordering::Acquire)
            {
                Ok(_) => return Some(Permit(self.running.clone())),
                Err(now) => n = now,
            }
        }
    }
}

/// Runs a Try and answers its `200` body. Refusals (nothing run): `429
/// try_busy`, `409 run_id_in_use`, `409 environment_unavailable` (the saved
/// version's environment is not the executor's), `503 executor_unavailable`
/// (the runtime could not be reached).
pub async fn start(
    versions: Arc<Versions>,
    fhir: Arc<FhirClient>,
    max_concurrent: usize,
    input: TryInput,
) -> Result<Value, ManagerError> {
    let runs = versions.tries();
    let Some(permit) = runs.permit(max_concurrent.max(1)) else {
        return Err(ManagerError::new(
            ErrorStatus::Busy,
            "try_busy",
            format!("at most {max_concurrent} Try runs at once"),
        ));
    };
    let requested = input.records.len();
    let started_at = now_rfc3339_ms();
    let running = json!({"id": input.run_id, "status": "running", "outcome": null,
        "termination": null, "cleanup": null, "started_at": started_at});
    let coverage = json!({"requested": requested, "reported": 0, "complete": false});
    let stale_at = crate::db::now_ms() + (input.deadline + STALE_AFTER_DEADLINE).as_millis() as i64;
    if !versions
        .db()
        .try_reserve(&input.run_id, &running, &coverage, stale_at)?
    {
        return Err(ManagerError::conflict(
            "run_id_in_use",
            format!("run id {} was used in the last 24 hours", input.run_id),
        ));
    }
    let (cancel, cancelled) = tokio::sync::watch::channel(false);
    let control = Arc::new(Control {
        cancel,
        state: Mutex::new(State::default()),
    });
    runs.active
        .lock()
        .expect("try runs")
        .insert(input.run_id.clone(), control.clone());
    let (answered, answer) = tokio::sync::oneshot::channel();
    let task_versions = versions.clone();
    tokio::spawn(async move {
        let versions = task_versions;
        // The slot travels with the work, which outlives the request.
        let _permit = permit;
        let result = execute(&versions, &fhir, &input, &started_at, cancelled, &control).await;
        versions
            .tries()
            .active
            .lock()
            .expect("try runs")
            .remove(&input.run_id);
        let _ = answered.send(result);
    });
    answer.await.map_err(|_| {
        ManagerError::from(anyhow::anyhow!(
            "the Try run's task ended without an answer"
        ))
    })?
}

/// Dispatches, validates, records the outcome and builds the answer.
async fn execute(
    versions: &Versions,
    fhir: &FhirClient,
    input: &TryInput,
    started_at: &str,
    cancelled: tokio::sync::watch::Receiver<bool>,
    control: &Control,
) -> Result<Value, ManagerError> {
    let db = versions.db();
    let dispatched = match dispatch::dispatch(versions, input, cancelled).await {
        Ok(d) => d,
        Err(refusal) => {
            // Nothing was kept: the run id is given back.
            control.state.lock().expect("try state").finished = true;
            if let Err(e) = db.try_release(&input.run_id) {
                tracing::error!(target: "hie::tryrun", "releasing a Try run id: {e:#}");
            }
            return Err(refusal);
        }
    };
    let mut dispatched = dispatched;
    let validation = if input.validate {
        answer::validate(fhir, &mut dispatched).await
    } else {
        "skipped"
    };
    {
        let mut state = control.state.lock().expect("try state");
        state.finished = true;
        if state.cancelled && dispatched.outcome == "completed" {
            // A cancel was accepted before the answer arrived: the run reads stopped.
            dispatched.outcome = "stopped";
            dispatched.reason = Some("cancelled".into());
        }
    }
    let built = answer::build(input, &dispatched, validation, started_at);
    if let Err(e) = db.try_finish(&input.run_id, &built.run, &built.coverage) {
        tracing::error!(target: "hie::tryrun", "recording a Try run's outcome: {e:#}");
    }
    Ok(built.body)
}

/// `GET /api/transforms/try-runs/{run}`: the run and its coverage.
pub fn get(versions: &Versions, run_id: &str) -> Result<Value, ManagerError> {
    let (run, coverage) = read(versions, run_id)?.ok_or_else(|| not_found(run_id))?;
    Ok(json!({"run": run, "coverage": coverage}))
}

/// `POST /api/transforms/try-runs/{run}/cancel`.
pub fn cancel(versions: &Versions, run_id: &str) -> Result<Value, ManagerError> {
    let (run, _) = read(versions, run_id)?.ok_or_else(|| not_found(run_id))?;
    let finished =
        || ManagerError::conflict("run_finished", format!("run {run_id} has its answer"));
    if run["status"] == "finished" {
        return Err(finished());
    }
    let control = versions
        .tries()
        .active
        .lock()
        .expect("try runs")
        .get(run_id)
        .cloned();
    let Some(control) = control else {
        return Err(ManagerError::new(
            ErrorStatus::Unavailable,
            "executor_unavailable",
            format!("run {run_id} executes on another engine instance"),
        ));
    };
    let mut state = control.state.lock().expect("try state");
    if state.finished {
        return Err(finished());
    }
    state.cancelled = true;
    let _ = control.cancel.send(true);
    Ok(json!({"run": run_id, "cancelling": true}))
}

fn not_found(run_id: &str) -> ManagerError {
    ManagerError::not_found("run_not_found", format!("Try run {run_id} not found"))
}

/// A kept run; one still running past its deadline whose engine stopped is
/// read (and recorded) as lost.
fn read(versions: &Versions, run_id: &str) -> Result<Option<(Value, Value)>, ManagerError> {
    let db = versions.db();
    let Some((finished, mut run, mut coverage, stale_at)) = db.try_run(run_id)? else {
        return Ok(None);
    };
    let here = versions
        .tries()
        .active
        .lock()
        .expect("try runs")
        .contains_key(run_id);
    if !finished && !here && stale_at < crate::db::now_ms() {
        let obj = run.as_object_mut().expect("a run object");
        obj.insert("status".into(), json!("finished"));
        obj.insert("outcome".into(), json!("indeterminate"));
        obj.insert("reason".into(), json!("response_lost"));
        obj.insert("termination".into(), json!("unconfirmed"));
        obj.insert("cleanup".into(), json!("unconfirmed"));
        coverage["complete"] = json!(false);
        db.try_finish(run_id, &run, &coverage)?;
    }
    Ok(Some((run, coverage)))
}
