//! What becomes of a record once checked: rejected (by the transform, filtered
//! out, or failing validation), transformed, passed through, or left for the
//! next pass when the transform cannot finish now.

use anyhow::Result;
use serde_json::Value;

use crate::config::DeliveryIds;
use crate::db::WorkItem;
use crate::fhir::{self, DeliveryKey};
use crate::transform::{self, Outcome};
use crate::versions::{Exec, ItemOutcome};

use super::{FeedRunner, TRANSFORM_CHUNK};

impl FeedRunner {
    /// Records what the transform made of one record, run as `exec` says.
    /// `false` when it could not finish now: the record and the rest of its
    /// group wait for the next pass.
    pub(super) fn record_outcome(
        &self,
        item: &WorkItem,
        exec: &Exec,
        outcome: ItemOutcome,
    ) -> Result<bool> {
        // Who ran it (the record's retry and version) is read from the record by
        // the history; of the diagnostics only the location is kept.
        let _ = exec;
        let location = outcome
            .diagnostics
            .as_ref()
            .map(crate::diagnostics::for_storage)
            .and_then(|d| d.get("location").cloned())
            .filter(|l| !l.is_null());
        if !matches!(outcome.outcome, Outcome::Transient(_))
            && let Some(retry) = &item.retry
        {
            self.db.retry_started(retry)?;
        }
        match outcome.outcome {
            Outcome::Transient(message) => {
                // Not the row's fault: leave it and the rest for the next pass.
                tracing::warn!(target: "hie::pipeline", feed = %self.feed.id, line = item.line, "transform cannot finish now, will retry: {message}");
                return Ok(false);
            }
            Outcome::Rejected { kind, message } => {
                self.db
                    .mark_failed(item.id, item.run, &kind, &message, None, location.as_ref())?
            }
            Outcome::Resources(resources) if resources.is_empty() => self.db.mark_failed(
                item.id,
                item.run,
                "filtered",
                "transform returned no resources",
                None,
                None,
            )?,
            Outcome::Resources(mut resources) => {
                // On the transformed bulk path a resource must carry an id:
                // one is derived from the identifier when there is none.
                let by_bulk = self.bulk_transformed
                    && (!self.initial_bulk_only()
                        || self.initial_bulk_load()? == Some(item.file_id));
                let checked = if by_bulk {
                    resources
                        .iter_mut()
                        .map(transform::derive_id)
                        .collect::<Result<Vec<_>, _>>()
                } else {
                    resources
                        .iter()
                        .map(transform::check_resource)
                        .collect::<Result<Vec<_>, _>>()
                };
                match checked {
                    Ok(_) => {
                        if self.feed.delivery.ids == DeliveryIds::Namespaced {
                            for resource in &mut resources {
                                fhir::namespace(&self.feed.id, resource);
                            }
                        }
                        self.db
                            .mark_transformed(item.id, item.run, &Value::Array(resources))?
                    }
                    Err(msg) => {
                        self.db
                            .mark_failed(item.id, item.run, "validation", &msg, None, None)?
                    }
                }
            }
        }
        Ok(true)
    }

    /// The source-objects bulk path has no transform: a record is the resource
    /// HFS will read. The engine only predicts what HFS will refuse (no
    /// resourceType, no id); the receipt is what decides. A row a retry runs
    /// is never preflighted: it runs its retry's version through the
    /// transform, and goes by REST.
    pub(super) async fn preflight_received(&self) -> Result<()> {
        let mut after = 0;
        let mut retried = false;
        loop {
            let items = self
                .db
                .received_records_after(&self.feed.id, after, TRANSFORM_CHUNK)?;
            let Some(last) = items.last() else { break };
            after = last.id;
            let mut good = Vec::with_capacity(items.len());
            for item in &items {
                if item.retry.is_some() {
                    retried = true;
                    continue;
                }
                let refused = match transform::check_resource(&item.raw) {
                    Ok(DeliveryKey::Id { .. }) => {
                        good.push(item);
                        None
                    }
                    Ok(DeliveryKey::Identifier { resource_type, .. }) => Some(format!(
                        "{resource_type} has no id; $bulk-submit stores resources by id"
                    )),
                    Err(msg) => Some(msg),
                };
                if let Some(reason) = refused {
                    self.db
                        .mark_failed(item.id, item.run, "validation", &reason, None, None)?;
                }
            }
            self.db.mark_passthrough(&good)?;
        }
        if retried {
            // Only retried rows are still received now.
            self.transform_received(None).await?;
        }
        Ok(())
    }
}
