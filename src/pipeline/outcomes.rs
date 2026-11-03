//! What becomes of a record once checked: rejected (by the transform, filtered
//! out, or failing validation), transformed, passed through, or left for the
//! next pass when the transform cannot finish now.

use anyhow::Result;
use serde_json::Value;

use crate::db::WorkItem;
use crate::fhir::DeliveryKey;
use crate::transform::{self, Outcome};

use super::{FeedRunner, TRANSFORM_CHUNK};

impl FeedRunner {
    /// Records what the transform made of one record. `false` when it could not
    /// finish now: the record and the rest of its chunk wait for the next pass.
    pub(super) fn record_outcome(&self, item: &WorkItem, outcome: Outcome) -> Result<bool> {
        match outcome {
            Outcome::Transient(message) => {
                // Not the row's fault: leave it and the rest for the next pass.
                tracing::warn!(feed = %self.feed.id, line = item.line, "transform cannot finish now, will retry: {message}");
                return Ok(false);
            }
            Outcome::Rejected { kind, message } => {
                self.db.mark_rejected(item.id, &kind, &message, None)?
            }
            Outcome::Resources(resources) if resources.is_empty() => self.db.mark_rejected(
                item.id,
                "filtered",
                "transform returned no resources",
                None,
            )?,
            Outcome::Resources(mut resources) => {
                // On the transformed bulk path a resource must carry an id:
                // one is derived from the identifier when there is none.
                let checked = if self.bulk_transformed {
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
                    Ok(_) => self
                        .db
                        .mark_transformed(item.id, &Value::Array(resources))?,
                    Err(msg) => self.db.mark_rejected(item.id, "validation", &msg, None)?,
                }
            }
        }
        Ok(true)
    }

    /// The bulk path has no transform: a record is the resource HFS will read. The
    /// engine only predicts what HFS will refuse (no resourceType, no id); the receipt
    /// is what decides.
    pub(super) fn preflight_received(&self) -> Result<()> {
        loop {
            let items = self.db.received_records(&self.feed.id, TRANSFORM_CHUNK)?;
            if items.is_empty() {
                return Ok(());
            }
            let mut good = Vec::with_capacity(items.len());
            for item in &items {
                match transform::check_resource(&item.raw) {
                    Ok(DeliveryKey::Id { .. }) => good.push(item.id),
                    Ok(DeliveryKey::Identifier { resource_type, .. }) => self.db.mark_rejected(
                        item.id,
                        "validation",
                        &format!("{resource_type} has no id; $bulk-submit stores resources by id"),
                        None,
                    )?,
                    Err(msg) => self.db.mark_rejected(item.id, "validation", &msg, None)?,
                }
            }
            self.db.mark_passthrough(&good)?;
        }
    }
}
