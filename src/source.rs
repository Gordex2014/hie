use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

use anyhow::{Context, Result, bail};
use chrono::{DateTime, TimeDelta, Utc};
use futures::StreamExt;
use object_store::ObjectStore;
use object_store::aws::{AmazonS3Builder, AmazonS3ConfigKey};
use object_store::path::Path as ObjectPath;
use object_store::signer::Signer;
use percent_encoding::{AsciiSet, NON_ALPHANUMERIC, utf8_percent_encode};
use serde::Serialize;

use crate::config::{FeedConfig, RemoteAuth, SourceConfig, glob_match};
use crate::credentials::{CredentialSettings, Credentials, TokenSource};
use crate::database::DatabaseSource;
use crate::db::Db;
use crate::sqs::SqsQueue;

/// Notices taken from a queue in one pass.
const NOTICES_PER_PASS: usize = 1000;

/// One object fetched from a source: a file, an S3 object.
pub struct Incoming {
    /// The source's own handle: a file path, an object key.
    pub key: String,
    /// The human-readable name kept with every record.
    pub name: String,
    pub bytes: Vec<u8>,
}

/// Where a feed's objects come from. `list` names what is waiting, `fetch` reads
/// one object, and `acknowledge` moves it out of the way once its records are stored.
pub enum Source {
    Folder(FolderSource),
    Object(ObjectSource),
    FhirServer(FhirServerSource),
    Database(DatabaseSource),
    Mllp(MllpSource),
}

impl Source {
    pub fn from_config(feed: &FeedConfig) -> Result<Self> {
        Ok(match &feed.source {
            SourceConfig::Folder {
                path,
                pattern,
                min_file_age_ms,
                archive,
                ..
            } => Source::Folder(FolderSource {
                dir: path.clone(),
                pattern: pattern.clone(),
                min_age: Duration::from_millis(*min_file_age_ms),
                archive: archive.clone().unwrap_or_else(|| path.join("processed")),
            }),
            SourceConfig::S3 { .. } => {
                Source::Object(object_source_from(feed, AmazonS3Builder::from_env())?)
            }
            SourceConfig::Mllp { listen, spool, .. } => Source::Mllp(MllpSource {
                feed_id: feed.id.clone(),
                listen: listen.clone(),
                folder: FolderSource {
                    dir: spool.clone(),
                    pattern: "*.hl7".into(),
                    min_age: Duration::ZERO,
                    archive: spool.join("processed"),
                },
                started: std::sync::Mutex::new(false),
            }),
            SourceConfig::Database {
                url_env,
                query,
                cursor_column,
                cursor_start,
                max_runs,
                ..
            } => Source::Database(DatabaseSource::new(
                &feed.id,
                url_env,
                query,
                cursor_column,
                cursor_start.clone(),
                *max_runs,
            )?),
            SourceConfig::FhirServer {
                url,
                types,
                page_size,
                max_pages,
                since,
                auth,
                ..
            } => Source::FhirServer(FhirServerSource::new(
                &feed.id,
                url,
                types,
                *page_size,
                *max_pages,
                since.clone(),
                auth.as_ref(),
            )?),
        })
    }

    /// Whether objects stay where they are once taken (someone else's bucket).
    pub fn is_read_only(&self) -> bool {
        matches!(self, Source::Object(o) if o.read_only)
    }

    /// Acknowledges the notifications behind the objects a pass took; nothing
    /// to do for sources without a queue.
    pub async fn settle_notifications(&self) -> Result<()> {
        match self {
            Source::Object(o) => o.settle_notifications().await,
            _ => Ok(()),
        }
    }

    /// Starts whatever listens: the MLLP port. Other sources have nothing to
    /// start. Safe to call again; a port that cannot be bound is reported and
    /// tried again on the next call.
    pub async fn start(&self) -> Result<()> {
        let Source::Mllp(m) = self else {
            return Ok(());
        };
        if *m.started.lock().expect("mllp mutex") {
            return Ok(());
        }
        let listener = tokio::net::TcpListener::bind(&m.listen)
            .await
            .with_context(|| format!("cannot listen for MLLP on {}", m.listen))?;
        tracing::info!(feed = %m.feed_id, listen = %m.listen, spool = %m.folder.dir.display(), "listening for MLLP");
        tokio::spawn(crate::hl7::serve(
            m.feed_id.clone(),
            listener,
            m.folder.dir.clone(),
        ));
        *m.started.lock().expect("mllp mutex") = true;
        Ok(())
    }

    /// Gives a pulling source the store its cursors live in. Sources that take
    /// files need none.
    pub fn with_db(self, db: Arc<Db>) -> Self {
        match self {
            Source::FhirServer(mut f) => {
                f.db = Some(db);
                Source::FhirServer(f)
            }
            Source::Database(mut d) => {
                d.db = Some(db);
                Source::Database(d)
            }
            other => other,
        }
    }

    /// What a pass would take, with each object's name and size, so a source
    /// that is never moved (read-only) can be skipped without downloading what
    /// was already taken. Sources without sizes answer 0.
    pub async fn list_meta(&self) -> Result<Vec<(String, String, u64)>> {
        match self {
            Source::Object(o) => o.list_meta().await,
            other => Ok(other
                .list()
                .await?
                .into_iter()
                .map(|k| {
                    let name = k.rsplit(['/', '\\']).next().unwrap_or(&k).to_string();
                    (k, name, 0)
                })
                .collect()),
        }
    }

    pub async fn list(&self) -> Result<Vec<String>> {
        match self {
            Source::Folder(f) => f.list(),
            Source::Object(o) => o.list().await,
            Source::FhirServer(r) => r.list().await,
            Source::Database(d) => d.list().await,
            Source::Mllp(m) => m.folder.list(),
        }
    }

    pub async fn fetch(&self, key: &str) -> Result<Incoming> {
        match self {
            Source::Folder(f) => f.fetch(key),
            Source::Object(o) => o.fetch(key).await,
            Source::FhirServer(r) => r.fetch(key),
            Source::Database(d) => d.fetch(key),
            Source::Mllp(m) => m.folder.fetch(key),
        }
    }

    /// Moves the object out of the way and returns where it is now.
    pub async fn acknowledge(&self, key: &str, archived_name: &str) -> Result<String> {
        match self {
            Source::Folder(f) => f.acknowledge(key, archived_name),
            Source::Object(o) => o.acknowledge(key, archived_name).await,
            Source::FhirServer(r) => r.acknowledge(key),
            Source::Database(d) => d.acknowledge(key),
            Source::Mllp(m) => m.folder.acknowledge(key, archived_name),
        }
    }
}

/// Builds an S3 source on `seed` (the environment in production:
/// `AmazonS3Builder::from_env()`), so tests can seed inherited values without
/// touching the process environment.
pub(crate) fn object_source_from(feed: &FeedConfig, seed: AmazonS3Builder) -> Result<ObjectSource> {
    let SourceConfig::S3 {
        bucket,
        prefix,
        region,
        endpoint,
        pattern,
        min_file_age_ms,
        archive_prefix,
        anonymous,
        read_only,
        queue,
        relist_seconds,
        ..
    } = &feed.source
    else {
        bail!("feed {}: not an S3 source", feed.id);
    };
    // Objects of hundreds of megabytes take minutes to fetch.
    let mut builder = seed.with_bucket_name(bucket).with_client_options(
        object_store::ClientOptions::new().with_timeout(Duration::from_secs(1800)),
    );
    if *anonymous {
        builder = builder.with_skip_signature(true);
    }
    if let Some(region) = region.as_deref().filter(|r| !r.is_empty()) {
        builder = builder.with_region(region);
    }
    if let Some(endpoint) = endpoint.as_deref().filter(|e| !e.is_empty()) {
        builder = builder
            .with_endpoint(endpoint)
            .with_allow_http(true)
            .with_virtual_hosted_style_request(false);
    }
    // What is in effect once the environment is taken into account. The builder
    // cannot unset a value, and with an empty one the store and the public URL
    // would address different hosts: refuse it.
    let effective_region = builder.get_config_value(&AmazonS3ConfigKey::Region);
    if effective_region.as_deref() == Some("") {
        bail!(
            "feed {}: s3: the region in effect is empty (an empty AWS_REGION or AWS_DEFAULT_REGION?); set it in the source or unset the variable",
            feed.id
        );
    }
    let effective_endpoint = builder.get_config_value(&AmazonS3ConfigKey::Endpoint);
    if effective_endpoint.as_deref() == Some("") {
        bail!(
            "feed {}: s3: the endpoint in effect is empty (an empty AWS_ENDPOINT?); set it in the source or unset the variable",
            feed.id
        );
    }
    let virtual_hosted = builder
        .get_config_value(&AmazonS3ConfigKey::VirtualHostedStyleRequest)
        .is_some_and(|v| {
            matches!(
                v.to_ascii_lowercase().as_str(),
                "1" | "true" | "on" | "yes" | "y"
            )
        });
    let public = anonymous.then(|| {
        PublicBucket::new(
            bucket,
            effective_region.as_deref(),
            effective_endpoint.as_deref(),
            virtual_hosted,
        )
    });
    let store = Arc::new(
        builder
            .build()
            .with_context(|| format!("feed {}: S3 source", feed.id))?,
    );
    let mut source = ObjectSource::new(
        store.clone(),
        prefix,
        archive_prefix,
        pattern,
        Duration::from_millis(*min_file_age_ms),
    )
    .read_only(*read_only);
    if let Some(queue) = queue {
        let region = SqsQueue::region_of(queue)
            .or_else(|| region.clone())
            .or_else(|| std::env::var("AWS_DEFAULT_REGION").ok())
            .or_else(|| std::env::var("AWS_REGION").ok())
            .unwrap_or_else(|| "us-east-1".into());
        source = source
            .with_queue(SqsQueue::new(queue, &region, store.credentials().clone()))
            .relist_every(Duration::from_secs(*relist_seconds));
    }
    Ok(match public {
        Some(public) => source.with_public_urls(public),
        None => source.with_signer(store),
    })
}

/// A key as the store lists it (raw UTF-8), or one the engine composed, as a
/// store path, unescaped.
fn object_path(key: &str) -> Result<ObjectPath> {
    ObjectPath::parse(key).map_err(|e| {
        anyhow::Error::new(Unshareable(format!(
            "{key:?} is not a valid object key: {e}"
        )))
    })
}

/// A store object that can never be handed over as configured. Terminal:
/// retrying cannot help.
#[derive(Debug)]
pub struct Unshareable(pub String);

impl std::fmt::Display for Unshareable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for Unshareable {}

/// What a key segment keeps unescaped in a URL: RFC 3986's unreserved characters.
const KEY_SEGMENT: &AsciiSet = &NON_ALPHANUMERIC
    .remove(b'-')
    .remove(b'.')
    .remove(b'_')
    .remove(b'~');

/// A bucket anyone may read: where a third party fetches its objects without
/// credentials.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublicBucket {
    base: String,
}

impl PublicBucket {
    /// `region` and `endpoint` are the values in effect (`None` = unset).
    ///
    /// Without an endpoint, AWS virtual-hosted style,
    /// `https://{bucket}.s3.{region}.amazonaws.com`, or path style,
    /// `https://s3.{region}.amazonaws.com/{bucket}`, for a bucket name with a dot,
    /// which AWS's wildcard certificate does not cover. The region defaults to
    /// `us-east-1`.
    ///
    /// With an endpoint, `{endpoint}/{bucket}`; or, when `virtual_hosted`, the
    /// endpoint unchanged (it already names the bucket), as the store client does.
    pub fn new(
        bucket: &str,
        region: Option<&str>,
        endpoint: Option<&str>,
        virtual_hosted: bool,
    ) -> Self {
        let base = match endpoint {
            Some(endpoint) if virtual_hosted => endpoint.to_string(),
            Some(endpoint) => format!("{}/{bucket}", endpoint.trim_end_matches('/')),
            None => {
                let region = region.unwrap_or("us-east-1");
                if bucket.contains('.') {
                    format!("https://s3.{region}.amazonaws.com/{bucket}")
                } else {
                    format!("https://{bucket}.s3.{region}.amazonaws.com")
                }
            }
        };
        Self { base }
    }

    /// The object's URL: the key encoded once, segment by segment.
    pub fn url(&self, key: &ObjectPath) -> String {
        let key: Vec<String> = key
            .as_ref()
            .split('/')
            .map(|segment| utf8_percent_encode(segment, KEY_SEGMENT).to_string())
            .collect();
        format!("{}/{}", self.base, key.join("/"))
    }
}

/// An MLLP listener whose messages wait in a spool folder: the listener writes
/// them, the folder source takes them.
pub struct MllpSource {
    feed_id: String,
    listen: String,
    folder: FolderSource,
    started: std::sync::Mutex<bool>,
}

/// A local folder, polled. A file is taken once it has not changed for `min_age`,
/// so a file still being written is left alone.
pub struct FolderSource {
    dir: PathBuf,
    pattern: String,
    min_age: Duration,
    archive: PathBuf,
}

impl FolderSource {
    fn list(&self) -> Result<Vec<String>> {
        std::fs::create_dir_all(&self.dir)
            .with_context(|| format!("creating {}", self.dir.display()))?;
        let mut keys = Vec::new();
        for entry in std::fs::read_dir(&self.dir)? {
            let path = entry?.path();
            if !path.is_file() {
                continue;
            }
            let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
                continue;
            };
            if !glob_match(&self.pattern, name) {
                continue;
            }
            let age = std::fs::metadata(&path)?
                .modified()
                .ok()
                .and_then(|m| SystemTime::now().duration_since(m).ok())
                .unwrap_or_default();
            if age < self.min_age {
                continue;
            }
            keys.push(path.to_string_lossy().into_owned());
        }
        keys.sort();
        Ok(keys)
    }

    fn fetch(&self, key: &str) -> Result<Incoming> {
        let path = Path::new(key);
        let name = path
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("file")
            .to_string();
        let bytes = std::fs::read(path).with_context(|| format!("reading {key}"))?;
        Ok(Incoming {
            key: key.to_string(),
            name,
            bytes,
        })
    }

    fn acknowledge(&self, key: &str, archived_name: &str) -> Result<String> {
        std::fs::create_dir_all(&self.archive)?;
        let target = self.archive.join(archived_name);
        if std::fs::rename(key, &target).is_err() {
            std::fs::copy(key, &target)?;
            std::fs::remove_file(key)?;
        }
        Ok(target.to_string_lossy().into_owned())
    }
}

/// An object store (S3 in production, in-memory in tests), polled by listing a prefix.
/// Acknowledged objects are renamed under the archive prefix.
pub struct ObjectSource {
    store: Arc<dyn ObjectStore>,
    /// Mints pre-signed URLs; set for S3, absent for stores that cannot sign.
    signer: Option<Arc<dyn Signer>>,
    /// Where anyone reads the objects of a public bucket, which is never signed.
    public: Option<PublicBucket>,
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
            public: None,
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
            if let Err(e) = object_path(&notice.key) {
                tracing::warn!("announced object skipped: {e:#}");
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

    async fn list(&self) -> Result<Vec<String>> {
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
        let path = object_path(key)?;
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
        let to = object_path(&format!("{}/{archived_name}", self.archive))?;
        self.store
            .rename(&object_path(key)?, &to)
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

    /// Hands objects over by their public URL: a bucket anyone may read.
    pub fn with_public_urls(mut self, public: PublicBucket) -> Self {
        self.public = Some(public);
        self
    }

    fn submission_prefix(&self, token: &str) -> String {
        format!("{}/submissions/{token}", self.archive)
    }
}

impl Source {
    /// Writes a file the engine produced for a submission next to the archive, so
    /// HFS can fetch it from the store by a pre-signed URL. `None` when the
    /// source has no store (a folder): the engine then serves the file itself.
    pub async fn put_submission_file(
        &self,
        token: &str,
        name: &str,
        bytes: Vec<u8>,
    ) -> Result<Option<String>> {
        let Source::Object(o) = self else {
            return Ok(None);
        };
        if o.signer.is_none() || o.read_only {
            return Ok(None);
        }
        let key = format!("{}/{name}", o.submission_prefix(token));
        o.store
            .put(&object_path(&key)?, bytes.into())
            .await
            .with_context(|| format!("writing {key}"))?;
        Ok(Some(key))
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
        let Source::Object(o) = self else {
            return Ok(None);
        };
        if o.signer.is_none() || o.read_only {
            return Ok(None);
        }
        let key = format!("{}/unpacked/{container}/{name}", o.archive);
        o.store
            .put(&object_path(&key)?, bytes.to_vec().into())
            .await
            .with_context(|| format!("writing {key}"))?;
        Ok(Some(key))
    }

    /// Removes what `put_submission_file` wrote for a submission, once HFS has
    /// read it and answered.
    pub async fn remove_submission_files(&self, token: &str) -> Result<()> {
        let Source::Object(o) = self else {
            return Ok(());
        };
        if o.read_only {
            return Ok(());
        }
        let prefix = object_path(&o.submission_prefix(token))?;
        let mut stream = o.store.list(Some(&prefix));
        let mut keys = Vec::new();
        while let Some(meta) = stream.next().await {
            keys.push(meta.context("listing submission files")?.location);
        }
        for key in keys {
            o.store
                .delete(&key)
                .await
                .with_context(|| format!("removing {key}"))?;
        }
        Ok(())
    }
}

impl Source {
    /// A URL a third party (HFS) can read the object from for `ttl`: a pre-signed
    /// GET, or the object's public URL in a public bucket.
    ///
    /// `None` only for a source with no store; the engine serves its files itself.
    /// A store object answers a URL in its store, or an error: `Unshareable` when
    /// no configuration can ever hand it over, any other error when a retry may
    /// succeed (signing).
    pub async fn share(&self, key: &str, ttl: Duration) -> Result<Option<String>> {
        match self {
            Source::Folder(_) | Source::FhirServer(_) | Source::Database(_) | Source::Mllp(_) => {
                Ok(None)
            }
            Source::Object(o) => {
                let path = object_path(key)?;
                match (&o.signer, &o.public) {
                    (Some(signer), _) => {
                        let url = signer
                            .signed_url(http::Method::GET, &path, ttl)
                            .await
                            .with_context(|| format!("signing a URL for {key}"))?;
                        Ok(Some(url.to_string()))
                    }
                    (None, Some(public)) => Ok(Some(public.url(&path))),
                    (None, None) => Err(anyhow::Error::new(Unshareable(format!(
                        "cannot hand {key} over: this store neither signs URLs nor is a public bucket"
                    )))),
                }
            }
        }
    }
}

/// What a connection test found: the evidence the UI keeps on the page.
#[derive(Debug, Clone, Serialize)]
pub struct Probe {
    pub reachable: bool,
    pub elapsed_ms: u64,
    /// Objects matching the pattern, whatever their age.
    pub objects: usize,
    pub newest: Option<String>,
    pub newest_at: Option<String>,
}

impl Source {
    /// Lists the source once and reports what is there, without taking anything.
    pub async fn probe(&self) -> Result<Probe> {
        let started = Instant::now();
        let found = match self {
            Source::Folder(f) => f.probe()?,
            Source::Object(o) => o.probe().await?,
            Source::FhirServer(r) => return r.probe(started).await,
            Source::Database(d) => return d.probe(started).await,
            Source::Mllp(m) => {
                // Can the port be taken? Then what waits in the spool.
                if !*m.started.lock().expect("mllp mutex") {
                    drop(
                        tokio::net::TcpListener::bind(&m.listen)
                            .await
                            .with_context(|| format!("cannot listen on {}", m.listen))?,
                    );
                }
                m.folder.probe()?
            }
        };
        let newest = found.iter().max_by_key(|(_, at)| *at);
        Ok(Probe {
            reachable: true,
            elapsed_ms: started.elapsed().as_millis() as u64,
            objects: found.len(),
            newest: newest.map(|(k, _)| k.clone()),
            newest_at: newest.map(|(_, at)| at.to_rfc3339()),
        })
    }
}

impl FolderSource {
    fn probe(&self) -> Result<Vec<(String, DateTime<Utc>)>> {
        let mut found = Vec::new();
        for entry in std::fs::read_dir(&self.dir)
            .with_context(|| format!("reading {}", self.dir.display()))?
        {
            let path = entry?.path();
            let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
                continue;
            };
            if !path.is_file() || !glob_match(&self.pattern, name) {
                continue;
            }
            let modified = std::fs::metadata(&path)?
                .modified()
                .map(DateTime::<Utc>::from)
                .unwrap_or_default();
            found.push((name.to_string(), modified));
        }
        Ok(found)
    }
}

impl ObjectSource {
    async fn probe(&self) -> Result<Vec<(String, DateTime<Utc>)>> {
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

/// Another FHIR server, pulled by `_lastUpdated`. Each resource type is a key: a
/// pass searches it for what changed after the type's cursor, in pages sorted by
/// `_lastUpdated`, and hands the resources on as one NDJSON "file". The cursor
/// moves to the newest `meta.lastUpdated` seen once the engine has stored the
/// records, so a crash in between pulls the same resources again (idempotent
/// `PUT`s) rather than skipping any.
pub struct FhirServerSource {
    feed_id: String,
    base: String,
    types: Vec<String>,
    page_size: usize,
    max_pages: usize,
    since: Option<String>,
    http: reqwest::Client,
    credentials: Credentials,
    bearer: Option<String>,
    pub(crate) db: Option<Arc<Db>>,
    /// What `list` pulled, waiting for `fetch` and `acknowledge`.
    pulled: std::sync::Mutex<HashMap<String, Pulled>>,
}

struct Pulled {
    bytes: Vec<u8>,
    count: usize,
    newest: String,
}

impl FhirServerSource {
    pub fn new(
        feed_id: &str,
        url: &str,
        types: &[String],
        page_size: usize,
        max_pages: usize,
        since: Option<String>,
        auth: Option<&RemoteAuth>,
    ) -> Result<Self> {
        let env = |name: &str| {
            std::env::var(name)
                .ok()
                .filter(|v| !v.trim().is_empty())
                .with_context(|| {
                    format!("environment variable {name} (named in the source's auth) is not set")
                })
        };
        let (credentials, bearer) = match auth {
            None => (None, None),
            Some(RemoteAuth::Bearer { token_env }) => (None, Some(env(token_env)?)),
            Some(RemoteAuth::ClientCredentials {
                token_url,
                client_id,
                client_secret_env,
                scope,
            }) => (
                Some(Arc::new(TokenSource::new(CredentialSettings {
                    token_url: token_url.clone(),
                    client_id: client_id.clone(),
                    client_secret: env(client_secret_env)?,
                    scope: scope.clone(),
                }))),
                None,
            ),
        };
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(120))
            .build()
            .expect("HTTP client");
        Ok(Self {
            feed_id: feed_id.to_string(),
            base: url.trim_end_matches('/').to_string(),
            types: types.to_vec(),
            page_size,
            max_pages,
            since,
            http,
            credentials,
            bearer,
            db: None,
            pulled: std::sync::Mutex::new(HashMap::new()),
        })
    }

    fn cursor(&self, resource_type: &str) -> Result<Option<String>> {
        match &self.db {
            Some(db) => Ok(db
                .cursor(&self.feed_id, resource_type)?
                .or_else(|| self.since.clone())),
            None => Ok(self.since.clone()),
        }
    }

    async fn get(&self, url: &str) -> Result<serde_json::Value> {
        let mut request = self.http.get(url).header("Accept", "application/fhir+json");
        if let Some(token) = &self.bearer {
            request = request.bearer_auth(token);
        }
        if let Some(source) = &self.credentials {
            let token = source
                .bearer()
                .await
                .map_err(|e| anyhow::anyhow!("{e:?}"))?;
            request = request.bearer_auth(token);
        }
        let response = request
            .send()
            .await
            .with_context(|| format!("cannot reach {url}"))?;
        let status = response.status();
        if !status.is_success() {
            let body = response.text().await.unwrap_or_default();
            bail!(
                "{url} answered {status}: {}",
                body.chars().take(300).collect::<String>()
            );
        }
        response
            .json()
            .await
            .with_context(|| format!("{url} did not answer with JSON"))
    }

    /// Searches one type for what changed after its cursor, page by page.
    async fn pull(&self, resource_type: &str) -> Result<Pulled> {
        let cursor = self.cursor(resource_type)?;
        let mut url = format!(
            "{}/{resource_type}?_sort=_lastUpdated&_count={}",
            self.base, self.page_size
        );
        if let Some(c) = &cursor {
            url.push_str(&format!("&_lastUpdated=gt{c}"));
        }
        let mut bytes = Vec::new();
        let mut count = 0;
        let mut newest = cursor.clone().unwrap_or_default();
        let mut next = Some(url);
        let mut pages = 0;
        while let Some(url) = next.take() {
            if pages >= self.max_pages {
                break;
            }
            pages += 1;
            let bundle = self.get(&url).await?;
            for entry in bundle["entry"].as_array().into_iter().flatten() {
                let resource = &entry["resource"];
                if resource.get("resourceType").and_then(|t| t.as_str()) != Some(resource_type) {
                    continue;
                }
                if let Some(at) = resource["meta"]["lastUpdated"].as_str()
                    && later(at, &newest)
                {
                    newest = at.to_string();
                }
                bytes.extend_from_slice(resource.to_string().as_bytes());
                bytes.push(b'\n');
                count += 1;
            }
            next = bundle["link"]
                .as_array()
                .into_iter()
                .flatten()
                .find(|l| l["relation"] == "next")
                .and_then(|l| l["url"].as_str())
                .map(str::to_string);
        }
        Ok(Pulled {
            bytes,
            count,
            newest,
        })
    }

    async fn list(&self) -> Result<Vec<String>> {
        let mut keys = Vec::new();
        for resource_type in &self.types {
            let pulled = self.pull(resource_type).await?;
            if pulled.count == 0 {
                continue;
            }
            tracing::debug!(feed = %self.feed_id, resource_type, resources = pulled.count, newest = %pulled.newest, "pulled");
            self.pulled
                .lock()
                .expect("pulled mutex")
                .insert(resource_type.clone(), pulled);
            keys.push(resource_type.clone());
        }
        Ok(keys)
    }

    fn fetch(&self, key: &str) -> Result<Incoming> {
        let pulled = self.pulled.lock().expect("pulled mutex");
        let p = pulled
            .get(key)
            .with_context(|| format!("{key} was not pulled in this pass"))?;
        Ok(Incoming {
            key: key.to_string(),
            name: format!("{key}-{}.ndjson", Utc::now().format("%Y%m%dT%H%M%SZ")),
            bytes: p.bytes.clone(),
        })
    }

    /// The records are stored: the cursor may move past them.
    fn acknowledge(&self, key: &str) -> Result<String> {
        let newest = self
            .pulled
            .lock()
            .expect("pulled mutex")
            .remove(key)
            .map(|p| p.newest)
            .with_context(|| format!("{key} was not pulled in this pass"))?;
        if let Some(db) = &self.db
            && !newest.is_empty()
        {
            db.set_cursor(&self.feed_id, key, &newest)?;
        }
        Ok(format!("cursor {newest}"))
    }

    /// The server's capability statement, then what each type holds past its cursor.
    async fn probe(&self, started: Instant) -> Result<Probe> {
        self.get(&format!("{}/metadata", self.base)).await?;
        let mut objects = 0;
        let mut newest: Option<(String, String)> = None;
        for resource_type in &self.types {
            let mut url = format!("{}/{resource_type}?_sort=-_lastUpdated&_count=1", self.base);
            if let Some(c) = self.cursor(resource_type)? {
                url.push_str(&format!("&_lastUpdated=gt{c}"));
            }
            let bundle = self.get(&url).await?;
            // Not every server puts a total on a page (HFS does not); ask for it.
            let total = match bundle["total"].as_u64() {
                Some(t) => t,
                None => self
                    .get(&url.replace("&_count=1", "&_summary=count"))
                    .await?["total"]
                    .as_u64()
                    .unwrap_or(bundle["entry"].as_array().map_or(0, Vec::len) as u64),
            };
            objects += total as usize;
            if let Some(at) = bundle["entry"][0]["resource"]["meta"]["lastUpdated"].as_str()
                && newest.as_ref().is_none_or(|(_, n)| later(at, n))
            {
                newest = Some((resource_type.clone(), at.to_string()));
            }
        }
        Ok(Probe {
            reachable: true,
            elapsed_ms: started.elapsed().as_millis() as u64,
            objects,
            newest: newest.as_ref().map(|(t, _)| t.clone()),
            newest_at: newest.map(|(_, at)| at),
        })
    }
}

/// Whether instant `a` is after `b`, comparing as instants when both parse and as
/// text otherwise (an empty `b` is before everything).
fn later(a: &str, b: &str) -> bool {
    if b.is_empty() {
        return true;
    }
    match (
        DateTime::parse_from_rfc3339(a),
        DateTime::parse_from_rfc3339(b),
    ) {
        (Ok(a), Ok(b)) => a > b,
        _ => a > b,
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
    fn key(raw: &str) -> ObjectPath {
        ObjectPath::parse(raw).unwrap()
    }

    #[test]
    fn public_urls_on_aws() {
        let at = |bucket, region, vh| {
            PublicBucket::new(bucket, region, None, vh).url(&key("in/a.ndjson"))
        };
        assert_eq!(
            at("hfs-test", Some("eu-west-1"), false),
            "https://hfs-test.s3.eu-west-1.amazonaws.com/in/a.ndjson"
        );
        assert_eq!(
            at("hfs-test", None, false),
            "https://hfs-test.s3.us-east-1.amazonaws.com/in/a.ndjson"
        );
        assert_eq!(
            at("hfs-test", None, true),
            "https://hfs-test.s3.us-east-1.amazonaws.com/in/a.ndjson"
        );
        // A dotted bucket name is not covered by AWS's wildcard certificate.
        assert_eq!(
            at("logs.example", Some("us-east-1"), false),
            "https://s3.us-east-1.amazonaws.com/logs.example/in/a.ndjson"
        );
    }

    #[test]
    fn public_urls_follow_the_endpoint_style() {
        let at = |endpoint, vh| {
            PublicBucket::new("files", Some("us-east-1"), Some(endpoint), vh).url(&key("a.ndjson"))
        };
        assert_eq!(
            at("https://store:9000/", false),
            "https://store:9000/files/a.ndjson"
        );
        assert_eq!(
            at("https://host/base", false),
            "https://host/base/files/a.ndjson"
        );
        // Virtual-hosted under an endpoint: the endpoint already names the bucket,
        // and is kept exactly as the store client sends it.
        assert_eq!(
            at("https://files.store:9000", true),
            "https://files.store:9000/a.ndjson"
        );
        assert_eq!(
            at("https://files.store:9000/", true),
            "https://files.store:9000//a.ndjson"
        );
    }

    #[test]
    fn public_url_keys_are_encoded_once() {
        let bucket = PublicBucket::new("b", None, Some("https://store"), false);
        assert_eq!(
            bucket.url(&key("a b/Obs+1 é 50%~x.ndjson")),
            "https://store/b/a%20b/Obs%2B1%20%C3%A9%2050%25~x.ndjson"
        );
    }

    fn s3_feed(extra: &str) -> FeedConfig {
        serde_yaml::from_str(&format!(
            "id: x\nname: X\nsource:\n  type: s3\n  bucket: b\n  anonymous: true\n{extra}format: ndjson\ntransform: t.py\n"
        ))
        .unwrap()
    }

    async fn shared(feed: &FeedConfig, seed: AmazonS3Builder) -> Result<String> {
        let source = Source::Object(object_source_from(feed, seed)?);
        Ok(source
            .share("a.ndjson", Duration::from_secs(60))
            .await?
            .expect("a store object has a URL"))
    }

    #[tokio::test]
    async fn effective_settings_come_from_the_seed_and_the_source_wins() {
        use AmazonS3ConfigKey::*;
        // The source's endpoint wins over an inherited one, and forces path style.
        let seed = AmazonS3Builder::new()
            .with_config(Endpoint, "https://inherited:9000")
            .with_config(VirtualHostedStyleRequest, "true");
        assert_eq!(
            shared(&s3_feed("  endpoint: https://store:9000\n"), seed)
                .await
                .unwrap(),
            "https://store:9000/b/a.ndjson"
        );
        // An inherited default region is the region in effect.
        let seed = AmazonS3Builder::new().with_config(DefaultRegion, "eu-west-2");
        assert_eq!(
            shared(&s3_feed(""), seed).await.unwrap(),
            "https://b.s3.eu-west-2.amazonaws.com/a.ndjson"
        );
        // An empty inherited region or endpoint cannot be unset: refused.
        let seed = AmazonS3Builder::new().with_config(Region, "");
        let err = shared(&s3_feed(""), seed).await.unwrap_err().to_string();
        assert!(err.contains("the region in effect is empty"), "{err}");
        let seed = AmazonS3Builder::new().with_config(Endpoint, "");
        let err = shared(&s3_feed(""), seed).await.unwrap_err().to_string();
        assert!(err.contains("the endpoint in effect is empty"), "{err}");
        // The source's own region replaces an empty inherited one.
        let seed = AmazonS3Builder::new().with_config(Region, "");
        assert_eq!(
            shared(&s3_feed("  region: us-east-1\n"), seed)
                .await
                .unwrap(),
            "https://b.s3.us-east-1.amazonaws.com/a.ndjson"
        );
    }
}
