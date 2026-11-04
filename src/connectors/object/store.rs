//! An object store, polled by listing a prefix: S3 in production (any
//! `object_store` in tests), optionally told of new objects by an SQS queue.
//! Acknowledged objects are renamed under the archive prefix.

use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use async_trait::async_trait;
use chrono::{DateTime, TimeDelta, Utc};
use futures::StreamExt;
use object_store::ObjectStore;
use object_store::path::Path as ObjectPath;
use object_store::signer::Signer;

use super::sqs::SqsQueue;
use crate::config::glob_match;
use crate::connectors::{Connector, Cursors, Incoming, Listed, Probe, Staging};

/// Notices taken from a queue in one pass.
const NOTICES_PER_PASS: usize = 1000;

/// An object store (S3 in production, in-memory in tests), polled by listing a prefix.
/// Acknowledged objects are renamed under the archive prefix.
pub struct ObjectSource {
    store: Arc<dyn ObjectStore>,
    /// Mints pre-signed URLs; set for S3, absent for stores that cannot sign.
    signer: Option<Arc<dyn Signer>>,
    prefix: Option<ObjectPath>,
    archive: ObjectPath,
    pattern: String,
    min_age: TimeDelta,
    /// Nothing is moved or written in the bucket.
    read_only: bool,
    /// Where the bucket announces new objects; without one every pass lists.
    queue: Option<SqsQueue>,
    /// How often the bucket is listed anyway when a queue announces objects.
    relist: Duration,
    last_listed: std::sync::Mutex<Option<Instant>>,
    /// Messages whose objects this pass took, deleted once the pass is through.
    receipts: std::sync::Mutex<Vec<String>>,
}

impl ObjectSource {
    pub fn new(
        store: Arc<dyn ObjectStore>,
        prefix: &str,
        archive_prefix: &str,
        pattern: &str,
        min_age: Duration,
    ) -> Self {
        let prefix = prefix.trim_matches('/');
        Self {
            store,
            signer: None,
            prefix: (!prefix.is_empty()).then(|| ObjectPath::from(prefix)),
            archive: ObjectPath::from(archive_prefix.trim_matches('/')),
            pattern: pattern.to_string(),
            min_age: TimeDelta::from_std(min_age).unwrap_or_default(),
            read_only: false,
            queue: None,
            relist: Duration::from_secs(3600),
            last_listed: std::sync::Mutex::new(None),
            receipts: std::sync::Mutex::new(Vec::new()),
        }
    }

    pub fn read_only(mut self, read_only: bool) -> Self {
        self.read_only = read_only;
        self
    }

    /// Reads new objects from the bucket's notification queue instead of
    /// listing on every pass.
    pub fn with_queue(mut self, queue: SqsQueue) -> Self {
        self.queue = Some(queue);
        self
    }

    /// With a queue, how often the bucket is still listed in full.
    pub fn relist_every(mut self, every: Duration) -> Self {
        self.relist = every;
        self
    }

    /// Whether a key is one this source takes: under its prefix, not archived,
    /// named by its pattern.
    fn wants(&self, key: &str) -> bool {
        if let Some(prefix) = &self.prefix
            && !key.starts_with(&format!("{prefix}/"))
        {
            return false;
        }
        if key.starts_with(&format!("{}/", self.archive)) {
            return false;
        }
        key.rsplit('/')
            .next()
            .is_some_and(|name| glob_match(&self.pattern, name))
    }

    /// What the queue announced since the last pass, as `list_meta` shapes it.
    async fn announced(&self, queue: &SqsQueue) -> Result<Vec<(String, String, u64)>> {
        let notices = queue.receive(NOTICES_PER_PASS).await?;
        let mut keys = Vec::new();
        let mut receipts = Vec::with_capacity(notices.len());
        for notice in notices {
            receipts.push(notice.receipt);
            if !self.wants(&notice.key) {
                continue;
            }
            let name = notice
                .key
                .rsplit('/')
                .next()
                .unwrap_or("object")
                .to_string();
            keys.push((notice.key, name, notice.size.unwrap_or(0)));
        }
        self.receipts.lock().expect("receipts").extend(receipts);
        keys.sort();
        keys.dedup_by(|a, b| a.0 == b.0);
        Ok(keys)
    }

    /// Deletes the notifications of the objects a finished pass took.
    async fn settle_notifications(&self) -> Result<()> {
        let Some(queue) = &self.queue else {
            return Ok(());
        };
        let receipts = std::mem::take(&mut *self.receipts.lock().expect("receipts"));
        if receipts.is_empty() {
            return Ok(());
        }
        queue
            .delete(&receipts)
            .await
            .context("acknowledging notifications")
    }

    /// The keys a pass would take.
    pub async fn list(&self) -> Result<Vec<String>> {
        Ok(self
            .list_meta()
            .await?
            .into_iter()
            .map(|(k, _, _)| k)
            .collect())
    }

    /// Every object the source would take: key, file name and size. With a
    /// queue, what it announced, except at start and every `relist` when the
    /// bucket is listed in full so nothing announced badly is missed for good.
    async fn list_meta(&self) -> Result<Vec<(String, String, u64)>> {
        if let Some(queue) = &self.queue {
            let listing_due = self
                .last_listed
                .lock()
                .expect("last_listed")
                .is_none_or(|at| at.elapsed() >= self.relist);
            if !listing_due {
                return self.announced(queue).await;
            }
        }
        let listed = self.list_all().await?;
        *self.last_listed.lock().expect("last_listed") = Some(Instant::now());
        Ok(listed)
    }

    async fn list_all(&self) -> Result<Vec<(String, String, u64)>> {
        let archive_prefix = format!("{}/", self.archive);
        let mut stream = self.store.list(self.prefix.as_ref());
        let mut keys = Vec::new();
        while let Some(meta) = stream.next().await {
            let meta = meta.context("listing objects")?;
            let key = meta.location.to_string();
            if key.starts_with(&archive_prefix) {
                continue;
            }
            let Some(name) = meta.location.filename() else {
                continue;
            };
            if !glob_match(&self.pattern, name) {
                continue;
            }
            if Utc::now().signed_duration_since(meta.last_modified) < self.min_age {
                continue;
            }
            keys.push((key, name.to_string(), meta.size as u64));
        }
        keys.sort();
        Ok(keys)
    }

    async fn fetch(&self, key: &str) -> Result<Incoming> {
        let path = ObjectPath::from(key);
        let bytes = self
            .store
            .get(&path)
            .await
            .with_context(|| format!("fetching {key}"))?
            .bytes()
            .await?
            .to_vec();
        Ok(Incoming {
            key: key.to_string(),
            name: path.filename().unwrap_or("object").to_string(),
            bytes,
        })
    }

    async fn acknowledge(&self, key: &str, archived_name: &str) -> Result<String> {
        if self.read_only {
            return Ok(key.to_string());
        }
        let to = ObjectPath::from(format!("{}/{archived_name}", self.archive));
        self.store
            .rename(&ObjectPath::from(key), &to)
            .await
            .with_context(|| format!("archiving {key}"))?;
        Ok(to.to_string())
    }
}

impl ObjectSource {
    pub fn with_signer(mut self, signer: Arc<dyn Signer>) -> Self {
        self.signer = Some(signer);
        self
    }

    fn submission_prefix(&self, token: &str) -> String {
        format!("{}/submissions/{token}", self.archive)
    }
}

impl ObjectSource {
    /// Every object matching the pattern, whatever its age, with when it changed.
    async fn found(&self) -> Result<Vec<(String, DateTime<Utc>)>> {
        let archive_prefix = format!("{}/", self.archive);
        let mut stream = self.store.list(self.prefix.as_ref());
        let mut found = Vec::new();
        while let Some(meta) = stream.next().await {
            let meta = meta.context("listing objects")?;
            let key = meta.location.to_string();
            if key.starts_with(&archive_prefix) {
                continue;
            }
            let Some(name) = meta.location.filename() else {
                continue;
            };
            if !glob_match(&self.pattern, name) {
                continue;
            }
            found.push((key, meta.last_modified));
        }
        Ok(found)
    }
}

#[async_trait]
impl Connector for ObjectSource {
    async fn list(&self, _cursors: &Cursors) -> Result<Vec<Listed>> {
        Ok(self
            .list_meta()
            .await?
            .into_iter()
            .map(|(key, name, size)| Listed { key, name, size })
            .collect())
    }

    async fn fetch(&self, key: &str) -> Result<Incoming> {
        ObjectSource::fetch(self, key).await
    }

    async fn acknowledge(
        &self,
        key: &str,
        archived_name: &str,
        _cursors: &Cursors,
    ) -> Result<String> {
        ObjectSource::acknowledge(self, key, archived_name).await
    }

    async fn settle(&self) -> Result<()> {
        self.settle_notifications().await
    }

    async fn probe(&self, _cursors: &Cursors) -> Result<Probe> {
        let started = Instant::now();
        Ok(Probe::found(started, &self.found().await?))
    }

    fn read_only(&self) -> bool {
        self.read_only
    }

    /// Every object store stages; each operation keeps its own conditions.
    fn staging(&self) -> Option<&dyn Staging> {
        Some(self)
    }
}

/// Sharing needs a signer (and works on a read-only bucket); writing needs a
/// signer and a writable bucket; cleaning up needs a writable bucket.
#[async_trait]
impl Staging for ObjectSource {
    async fn share(&self, key: &str, ttl: Duration) -> Result<Option<String>> {
        match &self.signer {
            None => Ok(None),
            Some(signer) => {
                let url = signer
                    .signed_url(http::Method::GET, &ObjectPath::from(key), ttl)
                    .await
                    .with_context(|| format!("signing a URL for {key}"))?;
                Ok(Some(url.to_string()))
            }
        }
    }

    async fn put_submission_file(
        &self,
        token: &str,
        name: &str,
        bytes: Vec<u8>,
    ) -> Result<Option<String>> {
        if self.signer.is_none() || self.read_only {
            return Ok(None);
        }
        let key = format!("{}/{name}", self.submission_prefix(token));
        self.store
            .put(&ObjectPath::from(key.as_str()), bytes.into())
            .await
            .with_context(|| format!("writing {key}"))?;
        Ok(Some(key))
    }

    async fn put_unpacked(
        &self,
        container: i64,
        name: &str,
        bytes: &[u8],
    ) -> Result<Option<String>> {
        if self.signer.is_none() || self.read_only {
            return Ok(None);
        }
        let key = format!("{}/unpacked/{container}/{name}", self.archive);
        self.store
            .put(&ObjectPath::from(key.as_str()), bytes.to_vec().into())
            .await
            .with_context(|| format!("writing {key}"))?;
        Ok(Some(key))
    }

    async fn remove_submission_files(&self, token: &str) -> Result<()> {
        if self.read_only {
            return Ok(());
        }
        let prefix = ObjectPath::from(self.submission_prefix(token));
        let mut stream = self.store.list(Some(&prefix));
        let mut keys = Vec::new();
        while let Some(meta) = stream.next().await {
            keys.push(meta.context("listing submission files")?.location);
        }
        for key in keys {
            self.store
                .delete(&key)
                .await
                .with_context(|| format!("removing {key}"))?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::PutPayload;
    use object_store::memory::InMemory;

    #[tokio::test]
    async fn objects_are_listed_fetched_and_archived() {
        let store = Arc::new(InMemory::new());
        for key in [
            "in/b.csv",
            "in/a.csv",
            "in/notes.txt",
            "in/processed/old.csv",
            "elsewhere/c.csv",
        ] {
            store
                .put(&ObjectPath::from(key), PutPayload::from_static(b"x"))
                .await
                .unwrap();
        }
        let source =
            ObjectSource::new(store.clone(), "in", "in/processed", "*.csv", Duration::ZERO);
        assert_eq!(source.list().await.unwrap(), ["in/a.csv", "in/b.csv"]);

        let got = source.fetch("in/a.csv").await.unwrap();
        assert_eq!(
            (got.name.as_str(), got.bytes.as_slice()),
            ("a.csv", &b"x"[..])
        );

        source.acknowledge("in/a.csv", "7-a.csv").await.unwrap();
        assert_eq!(source.list().await.unwrap(), ["in/b.csv"]);
        assert!(
            store
                .head(&ObjectPath::from("in/processed/7-a.csv"))
                .await
                .is_ok()
        );
    }

    #[tokio::test]
    async fn a_fresh_object_waits_for_the_age_guard() {
        let store = Arc::new(InMemory::new());
        store
            .put(&ObjectPath::from("x.csv"), PutPayload::from_static(b"x"))
            .await
            .unwrap();
        let waits = ObjectSource::new(store.clone(), "", "processed", "*", Duration::from_secs(60));
        assert!(waits.list().await.unwrap().is_empty());
        let takes = ObjectSource::new(store, "", "processed", "*", Duration::ZERO);
        assert_eq!(takes.list().await.unwrap(), ["x.csv"]);
    }
}
