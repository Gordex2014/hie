//! REST delivery: Bundles of conditional updates, with retries and backoff,
//! while the transform works on the next chunk.
//!
//! - **Validation.** With `validation: required` every resource goes through
//!   the destination's `$validate` first. Errors fail the row (`validation`);
//!   a destination that cannot validate holds the whole batch
//!   (`validation_unavailable`), retried with the delivery backoff.
//! - **Bundle type.** `delivery.bundle`: `batch`, `transaction`, or `auto`
//!   (a transaction when a resource holds a conditional reference).
//! - **Answers.** A definitive refusal of a transaction splits it into one
//!   transaction per row (`reference` when the row's conditional reference
//!   matched nothing or several, else `server`). A `429`, a `5xx`, a timeout or
//!   a lost answer leaves the rows `outcome_unknown`: a batch's rows are sent
//!   again, a transaction is kept as it was sent (`dispatches`) and the same
//!   one is sent again until an answer settles it, across restarts.
//! - **Schedule.** Deferred rows and open submissions are retried when due,
//!   independently of the source's poll interval ([`FeedRunner::next_delivery_in`]).

use std::collections::BTreeSet;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use anyhow::Result;
use futures::StreamExt;
use serde_json::{Value, json};

use crate::config::{BundleType, Validation};
use crate::db::{Deferral, Waiting, WorkItem, now_ms};
use crate::fhir::{self, BundleAnswer, EntryResult, Validated};
use crate::transform;
use tokio::sync::Notify;

use super::FeedRunner;

/// `$validate` calls in flight at once.
const VALIDATE_CONCURRENCY: usize = 8;

/// The shortest wait before a due delivery round, so a row due now cannot spin.
const MIN_WAIT: Duration = Duration::from_millis(50);

/// What a batch's validation decided.
pub(super) enum Checked {
    /// The rows that may be delivered.
    Deliver(Vec<WorkItem>),
    /// The destination could not validate: nothing of the batch goes.
    Held,
}

impl FeedRunner {
    /// Transforms and delivers at the same time: HFS takes each transformed
    /// batch while Python works on the next, instead of after the whole file.
    pub(super) async fn transform_and_deliver(&self) -> Result<()> {
        let progress = Notify::new();
        let done = AtomicBool::new(false);
        let transform = async {
            let result = self.transform_received(Some(&progress)).await;
            done.store(true, Ordering::SeqCst);
            progress.notify_one();
            result
        };
        let deliver = async {
            loop {
                self.deliver_due().await?;
                if done.load(Ordering::SeqCst) {
                    return self.deliver_due().await;
                }
                progress.notified().await;
            }
        };
        let (transformed, delivered) = tokio::join!(transform, deliver);
        transformed?;
        delivered
    }

    /// How long until the feed's next delivery attempt is due: a deferred row
    /// or an open submission; `None` when nothing waits.
    pub(super) fn next_delivery_in(&self) -> Option<Duration> {
        match self.db.next_attempt_at(&self.feed.id) {
            Ok(at) => {
                at.map(|at| Duration::from_millis((at - now_ms()).max(0) as u64).max(MIN_WAIT))
            }
            Err(e) => {
                tracing::error!(target: "hie::pipeline", feed = %self.feed.id, "{e:#}");
                None
            }
        }
    }

    /// A delivery round without a pass over the source: what is due is sent
    /// (or its submissions polled), then loads and retries that settled end.
    pub(super) async fn delivery_round(&self) -> Result<()> {
        if !self
            .db
            .acquire_lease(&self.feed.id, &self.instance, self.lease_ttl_ms())?
            || self.db.is_paused(&self.feed.id)?
        {
            return Ok(());
        }
        if self.bulk.is_none() || self.initial_bulk_only() {
            self.deliver_due().await?;
        }
        if self.bulk.is_some() {
            self.submit_pending().await?;
            self.poll_submissions().await?;
        }
        self.finish()
    }

    pub(super) async fn deliver_due(&self) -> Result<()> {
        loop {
            // The first load of an `initial_method: bulk_submit` source goes by `$bulk-submit`.
            let bulk_load = self.initial_bulk_load()?;
            let items =
                self.db
                    .due_records(&self.feed.id, self.feed.delivery.batch_size, bulk_load)?;
            if items.is_empty() {
                return Ok(());
            }
            // A transaction sent without a confirming answer goes again as it was.
            let stored: BTreeSet<String> =
                items.iter().filter_map(|i| i.dispatch.clone()).collect();
            for dispatch in stored {
                if !self.resend(&dispatch).await? {
                    return Ok(());
                }
            }
            let fresh: Vec<WorkItem> = items.into_iter().filter(|i| i.dispatch.is_none()).collect();
            if !fresh.is_empty() && !self.deliver(&fresh).await? {
                return Ok(());
            }
        }
    }

    /// Validates (when required) and sends `items` in one Bundle. Returns
    /// false when the destination is unavailable, so the caller stops until
    /// the next round instead of hammering it.
    pub(super) async fn deliver(&self, items: &[WorkItem]) -> Result<bool> {
        let items = match self.validate(items).await? {
            Checked::Held => return Ok(false),
            Checked::Deliver(items) if items.is_empty() => return Ok(true),
            Checked::Deliver(items) => items,
        };
        let transaction = match self.feed.delivery.bundle {
            BundleType::Batch => false,
            BundleType::Transaction => true,
            BundleType::Auto => items.iter().any(|i| {
                resources(i)
                    .iter()
                    .any(|r| !fhir::conditional_references(r).is_empty())
            }),
        };
        if transaction {
            self.send_transaction(&items, None).await
        } else {
            self.send_batch(&items).await
        }
    }

    /// Checks every resource not yet validated with the destination's
    /// `$validate`. Invalid rows fail and are not delivered; when the
    /// destination cannot validate, the whole batch waits.
    pub(super) async fn validate(&self, items: &[WorkItem]) -> Result<Checked> {
        if self.validation != Validation::Required {
            return Ok(Checked::Deliver(items.to_vec()));
        }
        let pending: Vec<(usize, Value)> = items
            .iter()
            .enumerate()
            .filter(|(_, i)| !i.validated)
            .flat_map(|(n, i)| resources(i).into_iter().map(move |r| (n, r)))
            .collect();
        let verdicts: Vec<(usize, Validated)> = futures::stream::iter(pending)
            .map(|(n, resource)| async move { (n, self.fhir.validate(&resource).await) })
            .buffered(VALIDATE_CONCURRENCY)
            .collect()
            .await;
        if verdicts
            .iter()
            .any(|(_, v)| matches!(v, Validated::Unavailable))
        {
            tracing::warn!(target: "hie::pipeline", feed = %self.feed.id, rows = items.len(), "the destination cannot validate; the batch waits");
            self.hold(
                items,
                &Deferral {
                    waiting: Waiting::Validation,
                    reason: "the destination cannot validate now",
                    sent: false,
                    dispatch: None,
                },
            )?;
            return Ok(Checked::Held);
        }
        let mut invalid: Vec<Option<String>> = vec![None; items.len()];
        for (n, verdict) in &verdicts {
            if let Validated::Outcome { issues } = verdict
                && invalid[*n].is_none()
                && let Some(issue) = issues.iter().find(|i| i.is_error())
            {
                let text = issue
                    .diagnostics
                    .clone()
                    .unwrap_or_else(|| issue.code.clone());
                let reason = match issue.expression.first() {
                    Some(path) if !text.contains(path.as_str()) => format!("{text} ({path})"),
                    _ => text,
                };
                invalid[*n] = Some(reason);
            }
        }
        let mut deliver = Vec::new();
        let mut valid = Vec::new();
        for (item, reason) in items.iter().zip(invalid) {
            match reason {
                Some(reason) => self
                    .db
                    .mark_failed(item.id, "validation", &reason, None, None)?,
                None => {
                    if !item.validated {
                        valid.push(item.id);
                    }
                    deliver.push(item.clone());
                }
            }
        }
        self.db.mark_validated(&valid)?;
        Ok(Checked::Deliver(deliver))
    }

    /// Sends a `batch`: each entry settles its own row.
    async fn send_batch(&self, items: &[WorkItem]) -> Result<bool> {
        let (entries, owner) = entries(items)?;
        match self
            .fhir
            .send_bundle(&fhir::bundle("batch", &entries))
            .await
        {
            BundleAnswer::Unsent(reason) => self.unconfirmed(items, &reason, false, None),
            BundleAnswer::Lost(reason) => self.unconfirmed(items, &reason, true, None),
            BundleAnswer::Answered { status, .. } if status >= 300 && items.len() > 1 => {
                tracing::warn!(target: "hie::pipeline", feed = %self.feed.id, status, "batch refused as a whole; sending records one by one");
                for item in items {
                    if !Box::pin(self.send_batch(std::slice::from_ref(item))).await? {
                        return Ok(false);
                    }
                }
                Ok(true)
            }
            BundleAnswer::Answered { status, body } if status >= 300 => {
                self.recovered()?;
                let reason = format!("HTTP {status}: {}", fhir::outcome_text(&body));
                self.db
                    .mark_rejected(items[0].id, "server", &reason, Some(&body))?;
                Ok(true)
            }
            BundleAnswer::Answered { body, .. } => {
                self.recovered()?;
                let results = fhir::entry_results(&body, entries.len());
                let mut sent = Vec::new();
                let mut unknown = Vec::new();
                for (i, item) in items.iter().enumerate() {
                    let mine: Vec<&EntryResult> = results
                        .iter()
                        .zip(&owner)
                        .filter(|(_, o)| **o == i)
                        .map(|(r, _)| r)
                        .collect();
                    let responses: Vec<Value> = mine
                        .iter()
                        .map(|r| match r {
                            EntryResult::Ok(v)
                            | EntryResult::Rejected(_, v)
                            | EntryResult::Transient(_, v) => v.clone(),
                        })
                        .collect();
                    if let Some(EntryResult::Rejected(status, v)) =
                        mine.iter().find(|r| matches!(r, EntryResult::Rejected(..)))
                    {
                        let reason = format!("HTTP {status}: {}", fhir::outcome_text(v));
                        self.db.mark_rejected(
                            item.id,
                            "server",
                            &reason,
                            Some(&Value::Array(responses)),
                        )?;
                    } else if let Some(EntryResult::Transient(status, _)) = mine
                        .iter()
                        .find(|r| matches!(r, EntryResult::Transient(..)))
                    {
                        unknown.push((item.clone(), format!("HTTP {status} for this entry")));
                    } else {
                        sent.push((item.id, Value::Array(responses)));
                    }
                }
                self.db.mark_sent_many(&sent)?;
                for (item, reason) in unknown {
                    self.hold(
                        std::slice::from_ref(&item),
                        &Deferral {
                            waiting: Waiting::Destination,
                            reason: &reason,
                            sent: true,
                            dispatch: None,
                        },
                    )?;
                }
                Ok(true)
            }
        }
    }

    /// Sends `items` as one `transaction` (the stored one `dispatch` names,
    /// when it is a re-send). All of it commits or none does.
    async fn send_transaction(&self, items: &[WorkItem], dispatch: Option<&str>) -> Result<bool> {
        let (bundle, owner) = match dispatch.map(|d| self.db.dispatch_payload(d)).transpose()? {
            Some(Some(stored)) => (
                stored["bundle"].clone(),
                stored["owners"]
                    .as_array()
                    .into_iter()
                    .flatten()
                    .filter_map(Value::as_i64)
                    .collect::<Vec<i64>>(),
            ),
            _ => {
                let (entries, owner) = entries(items)?;
                let ids = owner.iter().map(|i| items[*i].id).collect();
                (fhir::bundle("transaction", &entries), ids)
            }
        };
        match self.fhir.send_bundle(&bundle).await {
            BundleAnswer::Unsent(reason) => self.unconfirmed(items, &reason, false, None),
            BundleAnswer::Lost(reason) => {
                // It may have committed: keep it exactly as sent, to send again.
                let dispatch = match dispatch {
                    Some(d) => d.to_string(),
                    None => self.db.create_dispatch(
                        &self.feed.id,
                        &json!({"bundle": bundle, "owners": owner}),
                    )?,
                };
                self.unconfirmed(items, &reason, true, Some(&dispatch))
            }
            BundleAnswer::Answered { status, body } if status >= 300 => {
                // A definitive refusal: nothing was committed.
                self.recovered()?;
                if items.len() > 1 {
                    tracing::warn!(target: "hie::pipeline", feed = %self.feed.id, status, "transaction refused; sending each row in a transaction of its own");
                    let alone = self.release(items)?;
                    for item in &alone {
                        if !Box::pin(self.send_transaction(std::slice::from_ref(item), None))
                            .await?
                        {
                            break;
                        }
                    }
                } else {
                    let item = &items[0];
                    let reason = format!("HTTP {status}: {}", fhir::outcome_text(&body));
                    let kind = if refuses_reference(item, &body) {
                        "reference"
                    } else {
                        "server"
                    };
                    self.db
                        .mark_failed(item.id, kind, &reason, Some(&body), None)?;
                }
                if let Some(d) = dispatch {
                    self.db.drop_dispatch_if_settled(d)?;
                }
                Ok(true)
            }
            BundleAnswer::Answered { body, .. } => {
                self.recovered()?;
                let answers: Vec<Value> = body["entry"]
                    .as_array()
                    .into_iter()
                    .flatten()
                    .map(|e| e.get("response").cloned().unwrap_or(Value::Null))
                    .collect();
                let sent: Vec<(i64, Value)> = items
                    .iter()
                    .map(|item| {
                        let mine: Vec<Value> = owner
                            .iter()
                            .zip(answers.iter().chain(std::iter::repeat(&Value::Null)))
                            .filter(|(o, _)| **o == item.id)
                            .map(|(_, a)| a.clone())
                            .collect();
                        (item.id, Value::Array(mine))
                    })
                    .collect();
                self.db.mark_sent_many(&sent)?;
                if let Some(d) = dispatch {
                    self.db.drop_dispatch_if_settled(d)?;
                }
                Ok(true)
            }
        }
    }

    /// Sends a stored transaction again, with every row it holds. `false`
    /// when the destination is still unavailable.
    async fn resend(&self, dispatch: &str) -> Result<bool> {
        let items = self.db.dispatch_records(dispatch)?;
        if items.is_empty() {
            self.db.drop_dispatch_if_settled(dispatch)?;
            return Ok(true);
        }
        if self.db.dispatch_payload(dispatch)?.is_none() {
            // Nothing kept to send again: the rows go as new ones.
            let items = self.release(&items)?;
            return self.deliver(&items).await;
        }
        self.send_transaction(&items, Some(dispatch)).await
    }

    /// Takes rows out of their stored transaction (it was refused, or is
    /// gone), so each can go on its own.
    fn release(&self, items: &[WorkItem]) -> Result<Vec<WorkItem>> {
        let ids: Vec<i64> = items.iter().map(|i| i.id).collect();
        self.db.leave_dispatch(&ids)?;
        Ok(items
            .iter()
            .cloned()
            .map(|mut i| {
                i.dispatch = None;
                i
            })
            .collect())
    }

    /// The destination did not confirm `items`: they wait with the delivery
    /// backoff, outcome unknown when the write was sent. Answers `false`.
    fn unconfirmed(
        &self,
        items: &[WorkItem],
        reason: &str,
        sent: bool,
        dispatch: Option<&str>,
    ) -> Result<bool> {
        self.hold(
            items,
            &Deferral {
                waiting: Waiting::Destination,
                reason,
                sent,
                dispatch,
            },
        )?;
        let worst = items.iter().map(|i| i.attempts + 1).max().unwrap_or(0);
        self.outage(worst, reason)?;
        Ok(false)
    }

    /// Defers rows with the delivery backoff.
    fn hold(&self, items: &[WorkItem], deferral: &Deferral) -> Result<()> {
        let now = now_ms();
        let plan: Vec<(i64, u32, i64)> = items
            .iter()
            .map(|item| {
                let attempts = item.attempts + 1;
                (item.id, attempts, now + self.backoff_ms(attempts))
            })
            .collect();
        self.db.defer_many(&plan, deferral)
    }

    fn backoff_ms(&self, attempts: u32) -> i64 {
        let d = &self.feed.delivery;
        d.initial_backoff_ms
            .saturating_mul(1u64 << (attempts.max(1) - 1).min(20))
            .min(d.max_backoff_ms) as i64
    }
}

/// A Bundle entry: its request url and its resource.
type Entry = (String, Value);

/// A record's resources.
fn resources(item: &WorkItem) -> Vec<Value> {
    item.resources
        .as_ref()
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default()
}

/// The Bundle entries of `items` (request url, resource), and which item
/// (by position) each entry belongs to.
fn entries(items: &[WorkItem]) -> Result<(Vec<Entry>, Vec<usize>)> {
    let mut entries = Vec::new();
    let mut owner = Vec::new();
    for (i, item) in items.iter().enumerate() {
        for resource in resources(item) {
            let key = transform::check_resource(&resource).map_err(anyhow::Error::msg)?;
            entries.push((fhir::request_url(&key), resource));
            owner.push(i);
        }
    }
    Ok((entries, owner))
}

/// Whether a row's own transaction was refused over a conditional reference
/// of it that matched no resource, or several.
fn refuses_reference(item: &WorkItem, body: &Value) -> bool {
    let references: Vec<String> = resources(item)
        .iter()
        .flat_map(fhir::conditional_references)
        .collect();
    if references.is_empty() {
        return false;
    }
    let outcome = body.get("outcome").unwrap_or(body);
    outcome["issue"]
        .as_array()
        .into_iter()
        .flatten()
        .any(|issue| {
            let code = issue["code"].as_str().unwrap_or_default();
            let text = issue["diagnostics"]
                .as_str()
                .or_else(|| issue.pointer("/details/text").and_then(Value::as_str))
                .unwrap_or_default();
            matches!(code, "not-found" | "multiple-matches" | "duplicate")
                || references.iter().any(|r| text.contains(r.as_str()))
        })
}
