//! The handle the pipeline and the manager hold: one connector, bound to the
//! store its cursors live in.

use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use serde_json::Value;

use super::object::ObjectSource;
use super::{Capability, Connector, Cursors, Incoming, Listed, Probe};
use crate::config::FeedConfig;
use crate::db::Db;

/// A feed's source: its connector and its cursors. Optional abilities the
/// connector lacks answer as a source without them always has: nothing to
/// start, nothing to settle, no URL to share, nowhere to stage.
pub struct Source {
    connector: Box<dyn Connector>,
    cursors: Cursors,
}

impl Source {
    /// The connector the registry builds for a resolved definition. Cursors are
    /// not kept until `with_db`.
    pub fn from_config(feed: &FeedConfig) -> Result<Self> {
        Ok(Self {
            connector: super::build::connector(feed)?,
            cursors: Cursors::new(&feed.id, None),
        })
    }

    /// A source over a connector built by hand, for tests and sources that are
    /// not configured from a file.
    pub fn new(connector: impl Connector + 'static) -> Self {
        Self {
            connector: Box::new(connector),
            cursors: Cursors::default(),
        }
    }

    /// An object store source built by hand.
    #[allow(non_snake_case)]
    pub fn Object(source: ObjectSource) -> Self {
        Self::new(source)
    }

    /// Keeps the source's cursors in the engine's store. Sources that take
    /// files have none.
    pub fn with_db(mut self, db: Arc<Db>) -> Self {
        self.cursors.db = Some(db);
        self
    }

    pub fn connector(&self) -> &dyn Connector {
        self.connector.as_ref()
    }

    pub fn cursors(&self) -> &Cursors {
        &self.cursors
    }

    /// Whether objects stay where they are once taken (someone else's bucket).
    pub fn is_read_only(&self) -> bool {
        self.connector.read_only()
    }

    /// Starts whatever listens: the MLLP port. Safe to call again; a port that
    /// cannot be bound is reported and tried again on the next call.
    pub async fn start(&self) -> Result<()> {
        self.connector.start().await
    }

    /// What a pass would take, with each object's name and size, so a source
    /// that is never moved (read-only) can be skipped without downloading what
    /// was already taken. Sources without sizes answer 0.
    pub async fn list_meta(&self) -> Result<Vec<Listed>> {
        self.connector.list(&self.cursors).await
    }

    /// The keys a pass would take.
    pub async fn list(&self) -> Result<Vec<String>> {
        Ok(self
            .list_meta()
            .await?
            .into_iter()
            .map(|listed| listed.key)
            .collect())
    }

    pub async fn fetch(&self, key: &str) -> Result<Incoming> {
        self.connector.fetch(key).await
    }

    /// Moves the object out of the way and returns where it is now.
    pub async fn acknowledge(&self, key: &str, archived_name: &str) -> Result<String> {
        self.connector
            .acknowledge(key, archived_name, &self.cursors)
            .await
    }

    /// Acknowledges the notifications behind the objects a pass took; nothing
    /// to do for sources without a queue.
    pub async fn settle_notifications(&self) -> Result<()> {
        self.connector.settle().await
    }

    /// Lists the source once and reports what is there, without taking anything.
    pub async fn probe(&self) -> Result<Probe> {
        self.connector.probe(&self.cursors).await
    }

    pub async fn describe(&self) -> Capability<Value> {
        self.connector.describe(&self.cursors).await
    }

    pub async fn sample(&self, limit: usize) -> Capability<Vec<Value>> {
        self.connector.sample(&self.cursors, limit).await
    }

    /// A URL a third party (HFS) can read the object from for `ttl`: a pre-signed
    /// GET on S3. `None` when the source cannot sign, and the engine serves the
    /// object itself.
    pub async fn share(&self, key: &str, ttl: Duration) -> Result<Option<String>> {
        match self.connector.staging() {
            Some(staging) => staging.share(key, ttl).await,
            None => Ok(None),
        }
    }

    /// Writes a file the engine produced for a submission next to the archive, so
    /// HFS can fetch it from the store by a pre-signed URL. `None` when the
    /// source has no store (a folder): the engine then serves the file itself.
    pub async fn put_submission_file(
        &self,
        token: &str,
        name: &str,
        bytes: Vec<u8>,
    ) -> Result<Option<String>> {
        match self.connector.staging() {
            Some(staging) => staging.put_submission_file(token, name, bytes).await,
            None => Ok(None),
        }
    }

    /// Writes an entry unpacked from a zip next to the archive, so HFS can fetch
    /// it by a pre-signed URL on the `source_objects` bulk path. `None` when the
    /// store cannot be written or cannot sign: the engine then keeps the entry
    /// itself and serves it.
    pub async fn put_unpacked(
        &self,
        container: i64,
        name: &str,
        bytes: &[u8],
    ) -> Result<Option<String>> {
        match self.connector.staging() {
            Some(staging) => staging.put_unpacked(container, name, bytes).await,
            None => Ok(None),
        }
    }

    /// Removes what `put_submission_file` wrote for a submission, once HFS has
    /// read it and answered.
    pub async fn remove_submission_files(&self, token: &str) -> Result<()> {
        match self.connector.staging() {
            Some(staging) => staging.remove_submission_files(token).await,
            None => Ok(()),
        }
    }
}
