//! One dispatch of a Try run, never repeated: to python-runtime's `POST
//! /transforms/try` with the code inline (no redirect followed, no automatic
//! retry), or to a fresh worker of the engine's own over the code written to a
//! scratch file. Cancel and the run's deadline stop it: at the runtime by
//! dropping the request (the facade then cancels the partition), locally by
//! killing the worker and reaping it.
//!
//! How a facade answer is read (orchestrator decision a, contract "Try"):
//! when it carries python-runtime 0.1.3's `execution` evidence and the
//! `Python-Runtime-Error` header (or is a `200`), termination and cleanup are
//! taken from it (`stopped_confirmed`/`cleaned` are `confirmed`, anything else
//! `unconfirmed`) and dispositions from its items (`unknown` is
//! `not_reported`; `reported` without a reply, as a failed try returns none, is
//! `not_reported` too). Without it, only a `200` with every reply and a `422
//! transform_load_failed` carrying the header confirm both; any other answer
//! (a redirect included) is `indeterminate`, both `unconfirmed`. A lost answer
//! is the same, reason `response_lost`.
//!
//! Environments: a saved version's results are kept only with complete
//! evidence (the environment id and digest headers) that it ran in its own
//! environment; otherwise the POST answers `409 environment_unavailable` and
//! the run keeps its id, recorded without results. Inline code records what the
//! headers say; a `200` without them is `indeterminate`.

use reqwest::StatusCode;
use serde_json::{Value, json};
use tokio::sync::watch;
use tokio::time::Instant;

use super::TryInput;
use super::answer::{Dispatched, Item};
use crate::manager::{ErrorStatus, ManagerError};
use crate::runtime::{Answer, Facade, TryCallError};
use crate::transform::{SpawnError, Worker, WorkerCommand};
use crate::versions::{Environment, Versions};

/// Why a run stopped before its answer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Stop {
    Cancelled,
    Deadline,
}

impl Stop {
    pub fn reason(self) -> &'static str {
        match self {
            Stop::Cancelled => "cancelled",
            Stop::Deadline => "deadline",
        }
    }
}

/// Resolves when the run is cancelled or its deadline passes.
pub async fn stopped(cancelled: &mut watch::Receiver<bool>, deadline: Instant) -> Stop {
    let cancel = async {
        let asked = cancelled.wait_for(|c| *c).await.is_ok();
        if !asked {
            // The registry went away: only the deadline is left.
            std::future::pending::<()>().await;
        }
    };
    tokio::select! {
        _ = cancel => Stop::Cancelled,
        _ = tokio::time::sleep_until(deadline) => Stop::Deadline,
    }
}

/// A run the route answers with an error. `ran: None` means nothing was
/// dispatched (the run id is given back); otherwise the run was dispatched,
/// keeps its id for 24 hours, and is recorded as `ran` says (no results).
#[derive(Debug)]
pub struct Refusal {
    pub error: ManagerError,
    pub ran: Option<Box<Dispatched>>,
}

impl From<ManagerError> for Refusal {
    fn from(error: ManagerError) -> Self {
        Self { error, ran: None }
    }
}

fn environment_unavailable(id: &str) -> ManagerError {
    ManagerError::conflict(
        "environment_unavailable",
        format!("the executor does not run environment {id} now"),
    )
}

fn executor_unavailable(e: impl std::fmt::Display) -> ManagerError {
    ManagerError::new(
        ErrorStatus::Unavailable,
        "executor_unavailable",
        format!("the executor cannot be reached: {e}"),
    )
}

fn requests(records: &[Value]) -> Vec<(Value, Value)> {
    records
        .iter()
        .enumerate()
        .map(|(i, r)| {
            (
                r.clone(),
                json!({"feed": "try", "file": "try", "line": i + 1}),
            )
        })
        .collect()
}

/// Dispatches the run once.
pub async fn dispatch(
    versions: &Versions,
    input: &TryInput,
    cancelled: &mut watch::Receiver<bool>,
    deadline: Instant,
) -> Result<Dispatched, Refusal> {
    let wanted = input.saved.as_ref().map(|s| &s.environment);
    match versions.facade() {
        Some(facade) => {
            if let Some(env) = wanted
                && env.executor != crate::runtime::EXECUTOR
            {
                return Err(environment_unavailable(&env.id).into());
            }
            remote(facade, input, wanted, cancelled, deadline).await
        }
        None => {
            if let Some(env) = wanted
                && env.executor != "local"
            {
                return Err(environment_unavailable(&env.id).into());
            }
            local(versions, input, cancelled, deadline).await
        }
    }
}

// ---- python-runtime ---------------------------------------------------------------

async fn remote(
    facade: &Facade,
    input: &TryInput,
    wanted: Option<&Environment>,
    cancelled: &mut watch::Receiver<bool>,
    deadline: Instant,
) -> Result<Dispatched, Refusal> {
    let n = input.records.len();
    let requests = requests(&input.records);
    let call = facade.try_code(&input.code, wanted.map(|e| e.id.as_str()), &requests);
    let answer = tokio::select! {
        answer = call => answer,
        stop = stopped(cancelled, deadline) => {
            return Ok(Dispatched::all(n, "stopped", stop.reason(), false, Item::NotReported));
        }
    };
    let answer = match answer {
        Ok(a) => a,
        Err(TryCallError::NotSent(e)) => return Err(executor_unavailable(e).into()),
        Err(TryCallError::Lost(_)) => {
            return Ok(Dispatched::all(
                n,
                "indeterminate",
                "response_lost",
                false,
                Item::NotReported,
            ));
        }
    };
    let wanted_id = wanted.map_or("(default)", |e| e.id.as_str());
    if answer.status == StatusCode::FORBIDDEN
        && answer.facade_error.is_some()
        && answer.error_code() == "environment_unavailable"
    {
        // The facade refused before running anything, but it was sent: the
        // run keeps its id.
        return Err(Refusal {
            error: environment_unavailable(wanted_id),
            ran: Some(Box::new(Dispatched::all(
                n,
                "not_started",
                "environment_unavailable",
                false,
                Item::NotStarted,
            ))),
        });
    }
    let mut d = read_answer(&answer, n);
    match (wanted, &answer.environment) {
        // A saved version's results need evidence that it ran where it was saved.
        (Some(wanted), ran)
            if ran.as_ref() != Some(wanted)
                && (ran.is_some() || answer.status == StatusCode::OK) =>
        {
            let reason = if ran.is_some() {
                "environment_mismatch"
            } else {
                "environment_unreported"
            };
            let mut kept = Dispatched::all(n, "indeterminate", reason, false, Item::NotReported);
            kept.termination = d.termination;
            kept.cleanup = d.cleanup;
            kept.environment = answer.environment.clone();
            return Err(Refusal {
                error: environment_unavailable(&wanted.id),
                ran: Some(Box::new(kept)),
            });
        }
        (None, None) if answer.status == StatusCode::OK => {
            // Inline code whose answer does not say where it ran.
            d.outcome = "indeterminate";
            d.reason = Some("environment_unreported".into());
        }
        _ => {}
    }
    d.environment = answer.environment.clone();
    Ok(d)
}

fn axis(value: Option<&str>, confirmed: &str) -> &'static str {
    if value == Some(confirmed) {
        "confirmed"
    } else {
        "unconfirmed"
    }
}

/// The facade's own dispositions, when its evidence lists one per item.
fn dispositions(execution: &Value, n: usize) -> Option<Vec<&str>> {
    let items = execution.get("items")?.as_array()?;
    (items.len() == n).then(|| {
        items
            .iter()
            .map(|i| {
                i.get("disposition")
                    .and_then(Value::as_str)
                    .unwrap_or("unknown")
            })
            .collect()
    })
}

fn read_answer(answer: &Answer, n: usize) -> Dispatched {
    let execution = answer.body.get("execution").filter(|e| e.is_object());
    let attributable = answer.status == StatusCode::OK || answer.facade_error.is_some();
    let evidence = execution.filter(|_| attributable);
    let (termination, cleanup) = match evidence {
        Some(e) => (
            axis(
                e.get("termination").and_then(Value::as_str),
                "stopped_confirmed",
            ),
            axis(e.get("cleanup").and_then(Value::as_str), "cleaned"),
        ),
        None => ("confirmed", "confirmed"),
    };
    let label = evidence
        .and_then(|e| e.get("outcome"))
        .and_then(Value::as_str)
        .map(str::to_string);

    if answer.status == StatusCode::OK {
        let replies = answer.body.get("replies").and_then(Value::as_array);
        if let Some(replies) = replies.filter(|r| r.len() == n) {
            let all_reported = evidence
                .and_then(|e| dispositions(e, n))
                .is_none_or(|d| d.iter().all(|d| *d == "reported"));
            return Dispatched {
                outcome: if all_reported {
                    "completed"
                } else {
                    "indeterminate"
                },
                reason: None,
                termination,
                cleanup,
                environment: None,
                load_error: None,
                items: replies.iter().map(Item::of_reply).collect(),
            };
        }
        return Dispatched::all(
            n,
            "indeterminate",
            "response_lost",
            false,
            Item::NotReported,
        );
    }
    if answer.status == StatusCode::UNPROCESSABLE_ENTITY
        && answer.facade_error.as_deref() == Some("transform_load_failed")
    {
        let mut d = Dispatched::all(n, "not_started", "load_failed", true, Item::NotStarted);
        d.termination = termination;
        d.cleanup = cleanup;
        d.load_error = answer.body.get("load_error").cloned();
        return d;
    }
    match evidence {
        Some(e) => {
            let disp = dispositions(e, n).unwrap_or_else(|| vec!["unknown"; n]);
            let items: Vec<Item> = disp
                .iter()
                .map(|d| match *d {
                    "not_started" => Item::NotStarted,
                    _ => Item::NotReported,
                })
                .collect();
            let none_started = disp.iter().all(|d| *d == "not_started")
                && e.get("termination").and_then(Value::as_str) == Some("not_started");
            let reason = label.map(|l| {
                if l.starts_with("rejected:busy") {
                    "busy".to_string()
                } else {
                    l
                }
            });
            Dispatched {
                outcome: if none_started {
                    "not_started"
                } else {
                    "indeterminate"
                },
                reason: reason.or_else(|| Some(answer.error_code().to_string())),
                termination,
                cleanup,
                environment: None,
                load_error: None,
                items,
            }
        }
        // Not provably the facade's, or without evidence: it may have run.
        None => {
            let reason = if answer.facade_error.is_some() && !answer.error_code().is_empty() {
                answer.error_code().to_string()
            } else {
                "response_lost".to_string()
            };
            Dispatched::all(n, "indeterminate", &reason, false, Item::NotReported)
        }
    }
}

// ---- the engine's own worker ----------------------------------------------------------

async fn local(
    versions: &Versions,
    input: &TryInput,
    cancelled: &mut watch::Receiver<bool>,
    deadline: Instant,
) -> Result<Dispatched, Refusal> {
    let n = input.records.len();
    let scratch = versions.scratch_file("try").map_err(ManagerError::from)?;
    std::fs::write(&scratch, &input.code)
        .map_err(|e| ManagerError::from(anyhow::Error::from(e)))?;
    let settings = versions.settings();
    let command = WorkerCommand {
        python: settings.python.clone(),
        worker_script: settings.runtime_dir.join("worker.py"),
        transform_script: scratch.clone(),
        terminology: settings.terminology.clone(),
        // Each record's own output comes back in its diagnostics, bounded.
        capture_streams: true,
    };
    let finish = |mut d: Dispatched, reaped: bool| {
        if !reaped {
            d.termination = "unconfirmed";
        }
        if std::fs::remove_file(&scratch).is_err() && scratch.exists() || !reaped {
            d.cleanup = "unconfirmed";
        }
        d.environment = Some(Environment::local());
        d
    };
    let mut worker = match Worker::launch(&command) {
        Ok(w) => w,
        Err(e) => {
            let _ = std::fs::remove_file(&scratch);
            return Err(executor_unavailable(e).into());
        }
    };
    let ready = tokio::select! {
        r = worker.ready(&command) => Ok(r),
        stop = stopped(cancelled, deadline) => Err(stop),
    };
    match ready {
        Ok(Ok(())) => {}
        Ok(Err(SpawnError::LoadFailed(load_error))) => {
            let reaped = worker.kill().await.is_ok();
            let mut d = Dispatched::all(n, "not_started", "load_failed", true, Item::NotStarted);
            d.load_error = Some(load_error);
            return Ok(finish(d, reaped));
        }
        Ok(Err(SpawnError::Process(_))) => {
            let reaped = worker.kill().await.is_ok();
            let d = Dispatched::all(n, "indeterminate", "worker_exited", true, Item::NotStarted);
            return Ok(finish(d, reaped));
        }
        Err(stop) => {
            let reaped = worker.kill().await.is_ok();
            let d = Dispatched::all(n, "stopped", stop.reason(), true, Item::NotStarted);
            return Ok(finish(d, reaped));
        }
    }
    let mut items = Vec::with_capacity(n);
    let mut ended: Option<&'static str> = None;
    for (record, context) in requests(&input.records) {
        let step = tokio::select! {
            reply = worker.call(&record, &context) => Ok(reply),
            stop = stopped(cancelled, deadline) => Err(stop),
        };
        match step {
            Ok(Ok(reply)) => items.push(Item::of_reply(&reply)),
            Ok(Err(_)) => {
                items.push(Item::NotReported);
                ended = Some("worker_exited");
                break;
            }
            Err(stop) => {
                items.push(Item::NotReported);
                ended = Some(stop.reason());
                break;
            }
        }
    }
    let reaped = worker.kill().await.is_ok();
    items.resize(n, Item::NotStarted);
    let (outcome, reason) = match ended {
        None => ("completed", None),
        Some(r @ ("cancelled" | "deadline")) => ("stopped", Some(r.to_string())),
        Some(r) => ("indeterminate", Some(r.to_string())),
    };
    let d = Dispatched {
        outcome,
        reason,
        termination: "confirmed",
        cleanup: "confirmed",
        environment: None,
        load_error: None,
        items,
    };
    Ok(finish(d, reaped))
}
