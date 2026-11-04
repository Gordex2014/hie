//! One dispatch of a Try run, never repeated: to python-runtime's `POST
//! /transforms/try` with the code inline, or to a fresh worker of the engine's
//! own over the code written to a scratch file. Cancel and the deadline stop
//! it: at the runtime by dropping the request (the facade then cancels the
//! partition), locally by killing the worker.
//!
//! How a facade answer is read (orchestrator decision a, contract "Try"):
//! when it carries python-runtime 0.1.3's `execution` evidence and the
//! `Python-Runtime-Error` header (or is a `200`), termination and cleanup are
//! taken from it (`stopped_confirmed`/`cleaned` are `confirmed`, anything else
//! `unconfirmed`) and dispositions from its items (`unknown` is
//! `not_reported`; `reported` without a reply, as a failed try returns none, is
//! `not_reported` too). Without it, only a `200` with every reply and a `422
//! transform_load_failed` carrying the header confirm both; any other answer
//! is `indeterminate`, both `unconfirmed`. A lost answer is the same, reason
//! `response_lost`.

use std::time::Duration;

use reqwest::StatusCode;
use serde_json::{Value, json};
use tokio::sync::watch;

use super::TryInput;
use super::answer::{Dispatched, Item};
use crate::manager::{ErrorStatus, ManagerError};
use crate::runtime::{Answer, Facade, TryCallError};
use crate::transform::{SpawnError, Worker, WorkerCommand};
use crate::versions::{Environment, Versions};

/// Why a run stopped before its answer.
#[derive(Debug, Clone, Copy)]
enum Stop {
    Cancelled,
    Deadline,
}

impl Stop {
    fn reason(self) -> &'static str {
        match self {
            Stop::Cancelled => "cancelled",
            Stop::Deadline => "deadline",
        }
    }
}

async fn stopped(cancelled: &mut watch::Receiver<bool>, deadline: Duration) -> Stop {
    let cancel = async {
        let asked = cancelled.wait_for(|c| *c).await.is_ok();
        if !asked {
            // The registry went away: only the deadline is left.
            std::future::pending::<()>().await;
        }
    };
    tokio::select! {
        _ = cancel => Stop::Cancelled,
        _ = tokio::time::sleep(deadline) => Stop::Deadline,
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

/// Dispatches the run once. A refusal (nothing ran, or what ran in another
/// environment is not kept) is an error; everything else is a [`Dispatched`].
pub async fn dispatch(
    versions: &Versions,
    input: &TryInput,
    mut cancelled: watch::Receiver<bool>,
) -> Result<Dispatched, ManagerError> {
    let wanted = input.saved.as_ref().map(|s| &s.environment);
    match versions.facade() {
        Some(facade) => {
            if let Some(env) = wanted
                && env.executor != crate::runtime::EXECUTOR
            {
                return Err(environment_unavailable(&env.id));
            }
            remote(facade, input, wanted, &mut cancelled).await
        }
        None => {
            if let Some(env) = wanted
                && env.executor != "local"
            {
                return Err(environment_unavailable(&env.id));
            }
            local(versions, input, &mut cancelled).await
        }
    }
}

// ---- python-runtime ---------------------------------------------------------------

async fn remote(
    facade: &Facade,
    input: &TryInput,
    wanted: Option<&Environment>,
    cancelled: &mut watch::Receiver<bool>,
) -> Result<Dispatched, ManagerError> {
    let n = input.records.len();
    let requests = requests(&input.records);
    let call = facade.try_code(&input.code, wanted.map(|e| e.id.as_str()), &requests);
    let answer = tokio::select! {
        answer = call => answer,
        stop = stopped(cancelled, input.deadline) => {
            return Ok(Dispatched::all(n, "stopped", stop.reason(), false, Item::NotReported));
        }
    };
    let answer = match answer {
        Ok(a) => a,
        Err(TryCallError::NotSent(e)) => return Err(executor_unavailable(e)),
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
    if answer.status == StatusCode::FORBIDDEN
        && answer.facade_error.is_some()
        && answer.error_code() == "environment_unavailable"
    {
        return Err(environment_unavailable(
            wanted.map_or("(default)", |e| e.id.as_str()),
        ));
    }
    if let (Some(wanted), Some(ran)) = (wanted, &answer.environment)
        && ran != wanted
    {
        // A version only runs where it was saved: nothing of this run is kept.
        return Err(environment_unavailable(&wanted.id));
    }
    let mut d = read_answer(&answer, n);
    d.environment = answer.environment.clone().or_else(|| wanted.cloned());
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
) -> Result<Dispatched, ManagerError> {
    let n = input.records.len();
    let scratch = versions.scratch_file("try")?;
    std::fs::write(&scratch, &input.code).map_err(anyhow::Error::from)?;
    let settings = versions.settings();
    let command = WorkerCommand {
        python: settings.python.clone(),
        worker_script: settings.runtime_dir.join("worker.py"),
        transform_script: scratch.clone(),
        terminology: settings.terminology.clone(),
        // A Try's output is never logged.
        inherit_stderr: false,
    };
    let remove = |d: &mut Dispatched| {
        if std::fs::remove_file(&scratch).is_err() && scratch.exists() {
            d.cleanup = "unconfirmed";
        }
    };
    let deadline = tokio::time::Instant::now() + input.deadline;
    let spawned = tokio::select! {
        w = Worker::spawn(&command) => w,
        stop = stopped(cancelled, input.deadline) => {
            let mut d = Dispatched::all(n, "stopped", stop.reason(), true, Item::NotStarted);
            d.environment = Some(Environment::local());
            remove(&mut d);
            return Ok(d);
        }
    };
    let mut worker = match spawned {
        Ok(w) => w,
        Err(SpawnError::LoadFailed(load_error)) => {
            let mut d = Dispatched::all(n, "not_started", "load_failed", true, Item::NotStarted);
            d.environment = Some(Environment::local());
            d.load_error = Some(load_error);
            remove(&mut d);
            return Ok(d);
        }
        Err(SpawnError::Process(e)) => {
            let _ = std::fs::remove_file(&scratch);
            return Err(executor_unavailable(format!("{e:#}")));
        }
    };
    let mut items = Vec::with_capacity(n);
    let mut ended: Option<&'static str> = None;
    for (record, context) in requests(&input.records) {
        let left = deadline.saturating_duration_since(tokio::time::Instant::now());
        let step = tokio::select! {
            reply = worker.call(&record, &context) => Ok(reply),
            stop = stopped(cancelled, left) => Err(stop),
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
    let killed = worker.kill().await.is_ok();
    items.resize(n, Item::NotStarted);
    let (outcome, reason) = match ended {
        None => ("completed", None),
        Some(r @ ("cancelled" | "deadline")) => ("stopped", Some(r.to_string())),
        Some(r) => ("indeterminate", Some(r.to_string())),
    };
    let mut d = Dispatched {
        outcome,
        reason,
        termination: if killed { "confirmed" } else { "unconfirmed" },
        cleanup: "confirmed",
        environment: Some(Environment::local()),
        load_error: None,
        items,
    };
    remove(&mut d);
    Ok(d)
}
