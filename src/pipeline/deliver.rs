//! REST delivery: batch Bundles of conditional updates, with retries and
//! backoff, while the transform works on the next chunk.

use std::sync::atomic::{AtomicBool, Ordering};

use anyhow::Result;
use serde_json::Value;

use crate::db::{WorkItem, now_ms};
use crate::fhir::{self, EntryResult, SendError};
use crate::transform;
use tokio::sync::Notify;

use super::FeedRunner;

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

    pub(super) async fn deliver_due(&self) -> Result<()> {
        loop {
            let items = self
                .db
                .due_records(&self.feed.id, self.feed.delivery.batch_size)?;
            if items.is_empty() {
                return Ok(());
            }
            if !self.deliver(&items).await? {
                return Ok(());
            }
        }
    }

    /// Sends `items` in one batch Bundle. Returns false when HFS is unavailable,
    /// so the caller stops until the next pass instead of hammering it.
    pub(super) async fn deliver(&self, items: &[WorkItem]) -> Result<bool> {
        let mut entries = Vec::new();
        let mut owner = Vec::new();
        for (i, item) in items.iter().enumerate() {
            for resource in item
                .resources
                .as_ref()
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
            {
                let key = transform::check_resource(resource).map_err(anyhow::Error::msg)?;
                entries.push((fhir::request_url(&key), resource.clone()));
                owner.push(i);
            }
        }
        match self.fhir.post_bundle(&fhir::batch_bundle(&entries)).await {
            Err(SendError::Transient(reason)) => {
                for item in items {
                    self.defer(item, &reason)?;
                }
                let worst = items.iter().map(|i| i.attempts + 1).max().unwrap_or(0);
                self.outage(worst, &reason)?;
                Ok(false)
            }
            Err(SendError::Refused(status, _)) if items.len() > 1 => {
                tracing::warn!(target: "hie::pipeline", feed = %self.feed.id, status, "batch refused as a whole; sending records one by one");
                for item in items {
                    if !Box::pin(self.deliver(std::slice::from_ref(item))).await? {
                        return Ok(false);
                    }
                }
                Ok(true)
            }
            Err(SendError::Refused(status, body)) => {
                let reason = format!("HTTP {status}: {}", fhir::outcome_text(&body));
                self.db
                    .mark_rejected(items[0].id, "server", &reason, Some(&body))?;
                Ok(true)
            }
            Ok(response) => {
                self.recovered()?;
                let results = fhir::entry_results(&response, entries.len());
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
                        self.defer(item, &format!("HTTP {status} for this entry"))?;
                    } else {
                        self.db.mark_sent(item.id, &Value::Array(responses))?;
                    }
                }
                Ok(true)
            }
        }
    }

    pub(super) fn defer(&self, item: &WorkItem, reason: &str) -> Result<()> {
        let attempts = item.attempts + 1;
        let d = &self.feed.delivery;
        let backoff = d
            .initial_backoff_ms
            .saturating_mul(1u64 << (attempts - 1).min(20))
            .min(d.max_backoff_ms);
        self.db
            .defer(item.id, attempts, now_ms() + backoff as i64, reason)
    }
}
