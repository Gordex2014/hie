//! Running the transform over received records, a chunk at a time; what each
//! record became is recorded by `outcomes`.
//!
//! Each record runs the version its attempt names (its retry's, else its
//! load's snapshot; a load admitted before loads recorded one runs what the
//! source resolves to now). A chunk is split by version and each group runs
//! through that version's executor. A group that cannot run this round (its
//! version is unknown, its executor failed, it ran in another environment, or
//! something it depends on is down) is left where it is and the round goes on
//! with the other groups. A group held because the executor runs another
//! environment than its version's marks its loads and retries waiting
//! (`environment_unavailable`, read through `Db::version_waiting`) until a
//! later round runs them.

use std::collections::{BTreeMap, HashSet};

use anyhow::Result;
use serde_json::{Value, json};
use tokio::sync::Notify;

use super::{FeedRunner, TRANSFORM_CHUNK};
use crate::db::WorkItem;
use crate::versions::{EnvironmentUnavailable, Exec, VersionKey, VersionRef};

const ENVIRONMENT_UNAVAILABLE: &str = "environment_unavailable";

impl FeedRunner {
    /// Transforms what was received, a chunk at a time; `progress` is told after
    /// each chunk so delivery can start on it.
    pub(super) async fn transform_received(&self, progress: Option<&Notify>) -> Result<()> {
        self.versions.ready().await;
        // What the source runs now, for records whose load recorded no version.
        let current = match self.versions.resolve_for_source(&self.feed) {
            Ok(v) => Some(v),
            Err(e) => {
                tracing::error!(target: "hie::pipeline", feed = %self.feed.id, "transform unavailable, will retry: {e:#}");
                None
            }
        };
        // Versions that could not run this round: their records wait.
        let mut held: HashSet<Option<VersionKey>> = HashSet::new();
        let mut after = 0;
        loop {
            let items = self
                .db
                .received_records_after(&self.feed.id, after, TRANSFORM_CHUNK)?;
            let Some(last) = items.last() else {
                self.versions.reclaim().await;
                return Ok(());
            };
            after = last.id;
            let mut groups: BTreeMap<Option<(String, i64)>, Vec<&WorkItem>> = BTreeMap::new();
            for item in &items {
                if !held.contains(&item.version) {
                    let key = item
                        .version
                        .as_ref()
                        .map(|k| (k.transform_id.clone(), k.version));
                    groups.entry(key).or_default().push(item);
                }
            }
            for (key, group) in groups {
                let key = key.map(|(transform_id, version)| VersionKey {
                    transform_id,
                    version,
                });
                if !self
                    .transform_group(key.as_ref(), current.as_ref(), &group)
                    .await?
                {
                    held.insert(key);
                }
            }
            if let Some(progress) = progress {
                progress.notify_one();
            }
        }
    }

    /// Runs one version over its records and records each outcome. `false`
    /// when the group could not run or finish now; its records wait.
    async fn transform_group(
        &self,
        key: Option<&VersionKey>,
        current: Option<&VersionRef>,
        items: &[&WorkItem],
    ) -> Result<bool> {
        let version = match key {
            None => current.cloned(),
            Some(key) => match self.versions.get(&key.transform_id, key.version) {
                Ok(v) => v,
                Err(e) => {
                    tracing::error!(target: "hie::pipeline", feed = %self.feed.id, "transform {}@{} unavailable, will retry: {e:#}", key.transform_id, key.version);
                    None
                }
            },
        };
        let Some(version) = version else {
            if let Some(key) = key {
                tracing::warn!(target: "hie::pipeline", feed = %self.feed.id, "transform {}@{} not found; its records wait", key.transform_id, key.version);
            }
            return Ok(false);
        };
        let executor = match self.versions.executor(&self.feed.id, &version) {
            Ok(e) => e,
            Err(e) => {
                if e.downcast_ref::<EnvironmentUnavailable>().is_some() {
                    self.hold_for_environment(items, true)?;
                }
                tracing::error!(target: "hie::pipeline", feed = %self.feed.id, "transform unavailable, will retry: {e:#}");
                return Ok(false);
            }
        };
        let requests: Vec<(Value, Value)> = items
            .iter()
            .map(|item| {
                (
                    item.raw.clone(),
                    json!({"feed": self.feed.id, "file": item.file_name, "line": item.line}),
                )
            })
            .collect();
        let execution = match executor.run(&requests).await {
            Ok(execution) => execution,
            Err(e) => {
                if e.downcast_ref::<EnvironmentUnavailable>().is_some() {
                    self.hold_for_environment(items, true)?;
                }
                tracing::error!(target: "hie::pipeline", feed = %self.feed.id, "transform unavailable, will retry: {e:#}");
                return Ok(false);
            }
        };
        if execution.environment != version.environment {
            // A version only runs where it was saved: nothing of this run is kept.
            tracing::warn!(target: "hie::pipeline", feed = %self.feed.id,
                "transform {}@{} ran in environment {} instead of {}; its records wait",
                version.transform_id, version.version, execution.environment.id, version.environment.id);
            self.hold_for_environment(items, true)?;
            return Ok(false);
        }
        if execution.items.len() != items.len() {
            anyhow::bail!(
                "the executor answered {} outcomes for {} records",
                execution.items.len(),
                items.len()
            );
        }
        self.hold_for_environment(items, false)?;
        for (item, outcome) in items.iter().zip(execution.items) {
            let exec = Exec {
                load_id: item.file_id,
                retry: item.retry.clone(),
                version: version.clone(),
            };
            if !self.record_outcome(item, &exec, outcome)? {
                return Ok(false);
            }
        }
        Ok(true)
    }

    /// Marks (or clears) the loads and retries of these records as waiting
    /// for their version's environment.
    fn hold_for_environment(&self, items: &[&WorkItem], held: bool) -> Result<()> {
        let mut done: HashSet<(i64, Option<&str>)> = HashSet::new();
        for item in items {
            let key = (item.file_id, item.retry.as_deref());
            if done.insert(key) {
                self.db.set_version_waiting(
                    key.0,
                    key.1,
                    held.then_some(ENVIRONMENT_UNAVAILABLE),
                )?;
            }
        }
        Ok(())
    }
}
