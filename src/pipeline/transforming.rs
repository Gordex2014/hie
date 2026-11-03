//! Running the transform over received records, a chunk at a time; what each
//! record became is recorded by `outcomes`.

use anyhow::Result;
use serde_json::{Value, json};
use tokio::sync::Notify;

use super::{FeedRunner, TRANSFORM_CHUNK};

impl FeedRunner {
    /// Transforms what was received, a chunk at a time; `progress` is told after
    /// each chunk so delivery can start on it.
    pub(super) async fn transform_received(&self, progress: Option<&Notify>) -> Result<()> {
        loop {
            let items = self.db.received_records(&self.feed.id, TRANSFORM_CHUNK)?;
            if items.is_empty() {
                return Ok(());
            }
            let requests: Vec<(Value, Value)> = items
                .iter()
                .map(|item| {
                    (
                        item.raw.clone(),
                        json!({"feed": self.feed.id, "file": item.file_name, "line": item.line}),
                    )
                })
                .collect();
            let outcomes = match self.transformer.transform_many(&requests).await {
                Ok(o) => o,
                Err(e) => {
                    tracing::error!(feed = %self.feed.id, "transform unavailable, will retry: {e:#}");
                    return Ok(());
                }
            };
            for (item, outcome) in items.iter().zip(outcomes) {
                if !self.record_outcome(item, outcome)? {
                    return Ok(());
                }
            }
            if let Some(progress) = progress {
                progress.notify_one();
            }
        }
    }
}
