use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use anyhow::{Context, Result};
use serde_json::{Value, json};

use chrono::Timelike;

use crate::alerts::{Alerter, parse_clock};
use crate::bulk::{self, BulkSubmitter, Poll};
use crate::config::{DeliveryMethod, FeedConfig, Format, parse_duration};
use crate::db::{Db, Rule, WorkItem, now_ms};
use crate::fhir::{self, DeliveryKey, EntryResult, FhirClient, SendError};
use crate::reader;
use crate::secrets::Secrets;
use crate::source::{Incoming, Source, Unshareable};
use crate::transform::{self, Outcome, Transformer};
use crate::worker::WorkerLimits;
use tokio::sync::Notify;

const TRANSFORM_CHUNK: usize = 200;
/// Records settled per page when a submission's receipts come back.
const SETTLE_PAGE: usize = 5_000;

/// How the manager drives a running feed: `poke` makes it pass now, `stopped` ends it.
#[derive(Default)]
pub struct RunnerControl {
    pub poke: tokio::sync::Notify,
    pub stopped: AtomicBool,
}

/// Moves one feed's data through: source -> stored rows -> transform -> HFS.
/// Every row is in the database before its object leaves the source, so a crash or an
/// HFS outage never loses data, and every row keeps its object name and line number.
pub struct FeedRunner {
    pub feed: FeedConfig,
    /// The source as built from the definition, rebuilt when the current
    /// revision of its credential changes.
    source: std::sync::RwLock<Arc<Source>>,
    /// Where the source's credential comes from; `None` for runners over a
    /// given source, which never rebuild it.
    secrets: Option<Secrets>,
    /// The credential (id, revision) the source signs with, once loaded.
    loaded: std::sync::Mutex<Option<(String, u32)>>,
    /// Whether this runner raised a `credential` alert that is still open.
    credential_alert: AtomicBool,
    /// What a test runs at this runner's pause points.
    #[cfg(all(test, feature = "secrets-native"))]
    pause: Option<crate::AsyncPause>,
    db: Arc<Db>,
    fhir: Arc<FhirClient>,
    bulk: Option<BulkSubmitter>,
    transformer: Transformer,
    alerts: Arc<Alerter>,
    started_at: i64,
    outage_alerted: AtomicBool,
    /// Which engine instance this runner acts as, for the source's lease.
    instance: String,
    /// Where transformed submissions are written when the engine serves them.
    bulk_dir: PathBuf,
    /// `$bulk-submit` over the engine's transformed NDJSON, not the source objects.
    bulk_transformed: bool,
    /// How the manager drives this runner, once it runs: a halt is seen between records.
    control: std::sync::OnceLock<Arc<RunnerControl>>,
}

/// This process's name among the instances sharing a store: `HIE_INSTANCE_ID`,
/// or the host, the process id and a random tail.
pub fn instance_id() -> &'static str {
    static ID: std::sync::OnceLock<String> = std::sync::OnceLock::new();
    ID.get_or_init(|| {
        std::env::var("HIE_INSTANCE_ID")
            .ok()
            .filter(|v| !v.trim().is_empty())
            .unwrap_or_else(|| {
                use rand::Rng;
                let host = std::env::var("COMPUTERNAME")
                    .or_else(|_| std::env::var("HOSTNAME"))
                    .unwrap_or_else(|_| "hie".into());
                format!(
                    "{host}-{}-{:04x}",
                    std::process::id(),
                    rand::thread_rng().r#gen::<u16>()
                )
            })
    })
}

impl FeedRunner {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        feed: FeedConfig,
        db: Arc<Db>,
        fhir: Arc<FhirClient>,
        alerts: Arc<Alerter>,
        python: &str,
        runtime_dir: &Path,
        fhir_base_url: &str,
        public_url: &str,
    ) -> Result<Self> {
        let source = Source::from_config(&feed)?.with_db(db.clone());
        Ok(Self::with_source(
            feed,
            source,
            db,
            fhir,
            alerts,
            python,
            runtime_dir,
            fhir_base_url,
            public_url,
        ))
    }

    /// Builds a runner over a given source, for tests and for sources that are
    /// not configured from a file.
    #[allow(clippy::too_many_arguments)]
    pub fn with_source(
        feed: FeedConfig,
        source: Source,
        db: Arc<Db>,
        fhir: Arc<FhirClient>,
        alerts: Arc<Alerter>,
        python: &str,
        runtime_dir: &Path,
        fhir_base_url: &str,
        public_url: &str,
    ) -> Self {
        let transformer = Transformer::new(python, runtime_dir, &feed.transform);
        let feed_bulk_transformed = feed.bulk_transformed();
        let bulk = (feed.delivery.method == DeliveryMethod::BulkSubmit).then(|| {
            BulkSubmitter::new(fhir_base_url, public_url)
                .with_credentials(fhir.credentials().clone())
        });
        Self {
            feed,
            source: std::sync::RwLock::new(Arc::new(source)),
            secrets: None,
            loaded: std::sync::Mutex::new(None),
            credential_alert: AtomicBool::new(false),
            #[cfg(all(test, feature = "secrets-native"))]
            pause: None,
            db,
            fhir,
            bulk,
            transformer,
            alerts,
            started_at: now_ms(),
            instance: instance_id().to_string(),
            bulk_dir: std::env::temp_dir().join("hie-bulk"),
            bulk_transformed: feed_bulk_transformed,
            outage_alerted: AtomicBool::new(false),
            control: std::sync::OnceLock::new(),
        }
    }

    /// The source this pass reads: the one built from the definition, or the
    /// rebuild for the credential's current revision.
    pub fn source(&self) -> Arc<Source> {
        self.source.read().expect("source lock").clone()
    }

    /// Reads the credential the definition names from `secrets`, rebuilding the
    /// source whenever its current revision changes.
    pub fn with_secrets(mut self, secrets: Secrets) -> Self {
        self.secrets = Some(secrets);
        self
    }

    #[cfg(all(test, feature = "secrets-native"))]
    pub(crate) fn with_pause(mut self, pause: Option<crate::AsyncPause>) -> Self {
        self.pause = pause;
        self
    }

    #[cfg(all(test, feature = "secrets-native"))]
    async fn pause_at(&self, name: &'static str) {
        if let Some(pause) = &self.pause {
            pause(name).await;
        }
    }

    /// When this runner started: the reference for silence until data arrives.
    pub fn started_at(&self) -> i64 {
        self.started_at
    }

    /// Runs the passes as this instance instead of the process-wide one; for
    /// tests that put two instances on one store.
    pub fn as_instance(mut self, id: &str) -> Self {
        self.instance = id.to_string();
        self
    }

    pub fn instance(&self) -> &str {
        &self.instance
    }

    /// Where this runner writes the NDJSON of transformed submissions.
    pub fn with_bulk_dir(mut self, dir: &Path) -> Self {
        self.bulk_dir = dir.to_path_buf();
        self
    }

    /// The terminology server the transform may ask.
    pub fn with_terminology(mut self, url: Option<&str>) -> Self {
        self.transformer.set_terminology(url);
        self
    }

    /// The limits the transform's worker runs under.
    pub fn with_worker_limits(mut self, limits: WorkerLimits) -> Self {
        self.transformer.set_limits(limits);
        self
    }

    fn halted(&self) -> bool {
        self.control
            .get()
            .is_some_and(|c| c.stopped.load(Ordering::Acquire))
    }

    /// How long a pass may hold the source before another instance may take it:
    /// a few polls, and never less than half a minute.
    fn lease_ttl_ms(&self) -> i64 {
        (self.feed.source.poll_interval().as_millis() as i64 * 3).max(30_000)
    }

    /// One pass as this instance: takes or renews the source's lease, then runs
    /// the pipeline unless the source is paused. `false` when another instance
    /// holds the source, in which case nothing ran.
    pub async fn pass(&self) -> Result<bool> {
        if !self
            .db
            .acquire_lease(&self.feed.id, &self.instance, self.lease_ttl_ms())?
        {
            tracing::debug!(feed = %self.feed.id, "held by another instance; standing by");
            return Ok(false);
        }
        if let Err(e) = self.source().start().await {
            tracing::error!(feed = %self.feed.id, "{e:#}");
            self.alerts.raise(
                Some(&self.feed.id),
                "listener",
                &format!("{}: {e:#}", self.feed.name),
            )?;
        }
        if !self.db.is_paused(&self.feed.id)? {
            self.ensure_credential().await;
            self.tick().await?;
        }
        Ok(true)
    }

    /// Whether the source can read its store: it names no credential, or it is
    /// a given source, or the credential's current revision is loaded.
    fn credential_ready(&self) -> bool {
        self.secrets.is_none()
            || self.feed.source.credential().is_none()
            || self.loaded.lock().expect("loaded lock").is_some()
    }

    /// Loads the current revision of the credential the definition names when it
    /// is not the one in use: registers the source for it first (so it cannot be
    /// deleted while used), then rebuilds the source with it. On any problem the
    /// source keeps what it had, a `credential` alert is open, and the next pass
    /// tries again; once loaded, the alert is cleared.
    pub(crate) async fn ensure_credential(&self) {
        let Some(secrets) = &self.secrets else {
            return;
        };
        let Some(reference) = self.feed.source.credential() else {
            return;
        };
        let store = match secrets.store() {
            Ok(store) => store,
            Err(e) => {
                secrets.recheck_once().await;
                let reason = secrets
                    .store()
                    .err()
                    .map_or_else(|| e.to_string(), |e| e.to_string());
                self.credential_problem(reference, &reason);
                return;
            }
        };
        let meta = match store.find(reference).await {
            Ok(Some(meta)) => meta,
            Ok(None) => {
                // Gone: nothing may sign with it any more.
                if self.loaded.lock().expect("loaded lock").take().is_some()
                    && let Ok(source) = Source::from_config(&self.feed)
                {
                    *self.source.write().expect("source lock") =
                        Arc::new(source.with_db(self.db.clone()));
                }
                self.credential_problem(
                    reference,
                    "does not exist; store it with POST /api/secrets",
                );
                return;
            }
            Err(e) => {
                self.credential_problem(reference, &e.to_string());
                return;
            }
        };
        let current = (meta.id.clone(), meta.revision);
        if self.loaded.lock().expect("loaded lock").as_ref() == Some(&current) {
            if self.credential_alert.swap(false, Ordering::AcqRel)
                && let Err(e) =
                    self.alerts
                        .clear_open(&self.feed.id, "credential", None, "credential loaded")
            {
                tracing::error!(feed = %self.feed.id, "{e:#}");
            }
            return;
        }
        pause_point!(await self, "runner:credential:before-hold");
        #[cfg(feature = "secrets-native")]
        if let Err(e) = self
            .db
            .secret_hold(&meta.id, &self.feed.id, secrets.owner())
        {
            // The source keeps what it had; nothing is released.
            self.credential_problem(reference, &e.to_string());
            return;
        }
        let revealed = match store.reveal(&meta.id).await {
            Ok(revealed) => revealed,
            Err(e) => {
                self.credential_problem(reference, &e.to_string());
                return;
            }
        };
        let source = match Source::from_config_with(&self.feed, Some(&revealed)) {
            Ok(source) => source.with_db(self.db.clone()),
            Err(e) => {
                self.credential_problem(reference, &format!("{e:#}"));
                return;
            }
        };
        *self.source.write().expect("source lock") = Arc::new(source);
        *self.loaded.lock().expect("loaded lock") =
            Some((revealed.meta.id.clone(), revealed.meta.revision));
        tracing::info!(
            feed = %self.feed.id,
            credential = %revealed.meta.name,
            revision = revealed.meta.revision,
            "using the credential's current revision"
        );
        self.credential_alert.store(false, Ordering::Release);
        if let Err(e) =
            self.alerts
                .clear_open(&self.feed.id, "credential", None, "credential loaded")
        {
            tracing::error!(feed = %self.feed.id, "{e:#}");
        }
    }

    /// Logs why the credential cannot be used, and opens a `credential` alert
    /// unless one is open for the source.
    fn credential_problem(&self, reference: &str, message: &str) {
        let message = format!("{}: credential {reference}: {message}", self.feed.name);
        tracing::error!(feed = %self.feed.id, "{message}");
        let open = match self.db.open_alert(&self.feed.id, "credential", None) {
            Ok(open) => open.is_some(),
            Err(e) => {
                tracing::error!(feed = %self.feed.id, "{e:#}");
                return;
            }
        };
        if !open
            && let Err(e) = self
                .alerts
                .raise(Some(&self.feed.id), "credential", &message)
        {
            tracing::error!(feed = %self.feed.id, "{e:#}");
        }
        self.credential_alert.store(true, Ordering::Release);
    }

    /// The feed's loop: a pass every poll interval, or sooner when poked; nothing
    /// while the source is paused or held by another instance; until stopped.
    pub async fn run(self: Arc<Self>, control: Arc<RunnerControl>) {
        tracing::info!(feed = %self.feed.id, source = %self.feed.source.describe(), instance = %self.instance, "watching");
        let _ = self.control.set(control.clone());
        loop {
            if control.stopped.load(Ordering::Acquire) {
                self.transformer.close().await;
                if let Err(e) = self.db.release_lease(&self.feed.id, &self.instance) {
                    tracing::error!(feed = %self.feed.id, "{e:#}");
                }
                tracing::info!(feed = %self.feed.id, "stopped");
                return;
            }
            if let Err(e) = self.pass().await {
                tracing::error!(feed = %self.feed.id, "{e:#}");
            }
            tokio::select! {
                _ = tokio::time::sleep(self.feed.source.poll_interval()) => {}
                _ = control.poke.notified() => {}
            }
        }
    }

    /// One pass of the whole pipeline. Each step picks up whatever the previous
    /// passes left, so a step that fails is simply retried on the next pass.
    pub async fn tick(&self) -> Result<()> {
        // Without its credential loaded, a source cannot read its store: nothing
        // is listed and nothing is handed over this pass, but what is open is
        // still polled and settled.
        let ready = self.credential_ready();
        // A source that cannot be read this pass must not hold back what is
        // already in the store: the rest of the pass runs, the scan is retried next.
        if ready && let Err(e) = self.scan().await {
            tracing::error!(feed = %self.feed.id, "scan failed, will retry next pass: {e:#}");
        }
        if self.bulk.is_some() {
            if self.bulk_transformed {
                self.transform_received(None).await?;
            } else {
                self.preflight_received()?;
            }
            // A submission that cannot be opened this pass must not hold back the
            // ones already open: they are polled anyway.
            if ready && let Err(e) = self.submit_pending().await {
                let reason = bulk::safe_reason(&format!("{e:#}"));
                tracing::error!(feed = %self.feed.id, "opening submissions failed, will retry next pass: {reason}");
            }
            self.poll_submissions().await?;
        } else {
            self.transform_and_deliver().await?;
        }
        self.finish()?;
        self.check_silence()?;
        Ok(())
    }

    async fn scan(&self) -> Result<()> {
        let source = self.source();
        let read_only = source.is_read_only();
        for (key, name, size) in source.list_meta().await? {
            // Someone else's bucket keeps what we took: known by name and size,
            // it is not downloaded again just to recognise its hash.
            if read_only && size > 0 && self.db.has_file(&self.feed.id, &name, size)? {
                continue;
            }
            let incoming = match source.fetch(&key).await {
                Ok(incoming) => incoming,
                // Announced, then gone before this pass reached it: nothing to take.
                Err(e) if is_not_found(&e) => {
                    tracing::warn!(feed = %self.feed.id, key, "object no longer there, skipped");
                    continue;
                }
                Err(e) => return Err(e),
            };
            self.ingest(incoming).await?;
        }
        // Notifications are acknowledged only once what they announced is in the
        // store: a pass that failed before this leaves them to be delivered again.
        source.settle_notifications().await?;
        Ok(())
    }

    /// Transforms and delivers at the same time: HFS takes each transformed
    /// batch while Python works on the next, instead of after the whole file.
    async fn transform_and_deliver(&self) -> Result<()> {
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

    async fn ingest(&self, incoming: Incoming) -> Result<()> {
        let feed = &self.feed.id;
        let Incoming { key, name, bytes } = incoming;
        let sha = reader::sha256_hex(&bytes);

        if let Some(original) = self.db.find_file_by_hash(feed, &sha)? {
            if self.source().is_read_only() {
                // Someone else's bucket: the object stays, and was already taken.
                return Ok(());
            }
            if self.bulk.is_some() && self.db.file_source(original)?.is_some() {
                // The same object, still waiting for HFS to fetch it: leave it alone.
                return Ok(());
            }
            let id = self.db.insert_file(
                feed,
                &name,
                &sha,
                bytes.len() as u64,
                "duplicate",
                Some(original),
                None,
                None,
            )?;
            tracing::info!(feed, file = %name, duplicate_of = original, "same content already received; not reprocessed");
            let archived = self
                .source()
                .acknowledge(&key, &format!("{id}-{name}"))
                .await?;
            return self.db.mark_file_archived(id, &archived);
        }

        if name.to_ascii_lowercase().ends_with(".zip") {
            return self.unpack(&key, &name, &sha, bytes).await;
        }

        // On the bulk path the object stays at its source until HFS has read it.
        let keep = self.bulk.is_some() && !self.bulk_transformed;
        let origin = if matches!(*self.source(), Source::Object(_)) {
            "store"
        } else {
            "local"
        };
        let file_id = self.db.insert_file(
            feed,
            &name,
            &sha,
            bytes.len() as u64,
            "processing",
            None,
            keep.then_some(key.as_str()),
            keep.then_some(origin),
        )?;
        let readable = self.admit(file_id, &name, &bytes)?;
        if keep && readable {
            return Ok(());
        }
        let archived = self
            .source()
            .acknowledge(&key, &format!("{file_id}-{name}"))
            .await?;
        self.db.mark_file_archived(file_id, &archived)
    }

    /// Reads a file's records into the store; `false` when the file could not be read.
    fn admit(&self, file_id: i64, name: &str, bytes: &[u8]) -> Result<bool> {
        let feed = &self.feed.id;
        match reader::read(self.feed.format, bytes) {
            Ok(records) => {
                let rows: Vec<_> = records
                    .into_iter()
                    .map(|r| (r.line, r.raw, r.parse_error))
                    .collect();
                self.db.insert_records(file_id, feed, &rows)?;
                tracing::info!(feed, file = %name, records = rows.len(), "received");
                Ok(true)
            }
            Err(e) => {
                self.db.fail_file(file_id, &format!("{e:#}"))?;
                self.alerts.raise(
                    Some(feed),
                    "unreadable_file",
                    &format!("{}: cannot read {name}: {e:#}", self.feed.name),
                )?;
                Ok(false)
            }
        }
    }

    /// A zip is a drop of several files: every entry whose extension fits the
    /// source's format becomes a load of its own, named `drop.zip!/entry`; the
    /// zip is recorded as `unpacked` (so it is not read twice) and archived.
    async fn unpack(&self, key: &str, name: &str, sha: &str, bytes: Vec<u8>) -> Result<()> {
        let feed = &self.feed.id;
        let container = self.db.insert_file(
            feed,
            name,
            sha,
            bytes.len() as u64,
            "unpacked",
            None,
            None,
            None,
        )?;
        let wanted = match self.feed.format {
            Format::Csv => &["csv"][..],
            Format::Ndjson => &["ndjson", "jsonl"][..],
            Format::FhirBundle => &["json"][..],
            Format::Hl7v2 => &["hl7", "txt"][..],
        };
        let entries =
            match tokio::task::spawn_blocking(move || -> Result<Vec<(String, Vec<u8>)>> {
                let mut archive = zip::ZipArchive::new(std::io::Cursor::new(bytes))?;
                let mut out = Vec::new();
                for i in 0..archive.len() {
                    let mut entry = archive.by_index(i)?;
                    if entry.is_dir() {
                        continue;
                    }
                    let path = entry.name().to_string();
                    let base = path.rsplit('/').next().unwrap_or(&path).to_string();
                    if base.starts_with('.') || path.starts_with("__MACOSX") {
                        continue;
                    }
                    let mut data = Vec::with_capacity(entry.size() as usize);
                    std::io::Read::read_to_end(&mut entry, &mut data)?;
                    out.push((path, data));
                }
                Ok(out)
            })
            .await?
            {
                Ok(entries) => entries,
                Err(e) => {
                    self.db.fail_file(container, &format!("{e:#}"))?;
                    self.alerts.raise(
                        Some(feed),
                        "unreadable_file",
                        &format!("{}: cannot open {name}: {e:#}", self.feed.name),
                    )?;
                    let archived = self
                        .source()
                        .acknowledge(key, &format!("{container}-{name}"))
                        .await?;
                    return self.db.mark_file_archived(container, &archived);
                }
            };
        let mut taken = 0;
        for (path, data) in entries {
            let extension = path.rsplit('.').next().unwrap_or("").to_ascii_lowercase();
            if !wanted.contains(&extension.as_str()) {
                tracing::debug!(feed, file = %name, entry = %path, "entry skipped: not a {:?} file", self.feed.format);
                continue;
            }
            let entry_name = format!("{name}!/{path}");
            let entry_sha = reader::sha256_hex(&data);
            if let Some(original) = self.db.find_file_by_hash(feed, &entry_sha)? {
                self.db.insert_file(
                    feed,
                    &entry_name,
                    &entry_sha,
                    data.len() as u64,
                    "duplicate",
                    Some(original),
                    None,
                    None,
                )?;
                continue;
            }
            // On the source_objects bulk path HFS reads the entry, not the zip: the
            // entry is staged where HFS can fetch it and kept until the receipts.
            let staged = if self.bulk.is_some() && !self.bulk_transformed {
                let base = format!("{}.ndjson", taken + 1);
                match self.source().put_unpacked(container, &base, &data).await? {
                    Some(key) => Some((key, "store")),
                    None => {
                        let dir = self.bulk_dir.join("unpacked").join(container.to_string());
                        std::fs::create_dir_all(&dir)
                            .with_context(|| format!("creating {}", dir.display()))?;
                        let local = dir.join(&base);
                        std::fs::write(&local, &data)
                            .with_context(|| format!("writing {}", local.display()))?;
                        Some((local.to_string_lossy().into_owned(), "staged"))
                    }
                }
            } else {
                None
            };
            let file_id = self.db.insert_file(
                feed,
                &entry_name,
                &entry_sha,
                data.len() as u64,
                "processing",
                None,
                staged.as_ref().map(|(key, _)| key.as_str()),
                staged.as_ref().map(|(_, origin)| *origin),
            )?;
            self.admit(file_id, &entry_name, &data)?;
            taken += 1;
        }
        tracing::info!(feed, file = %name, entries = taken, "unpacked");
        let archived = self
            .source()
            .acknowledge(key, &format!("{container}-{name}"))
            .await?;
        self.db.mark_file_archived(container, &archived)
    }

    /// Transforms what was received, one record at a time, each outcome stored
    /// before the next record is sent, so a decided record is never sent again;
    /// `progress` is told after each chunk so delivery can start on it.
    async fn transform_received(&self, progress: Option<&Notify>) -> Result<()> {
        loop {
            let items = self.db.received_records(&self.feed.id, TRANSFORM_CHUNK)?;
            if items.is_empty() {
                return Ok(());
            }
            for item in &items {
                // A halted or replaced source stops after its current record.
                if self.halted() {
                    return Ok(());
                }
                let context =
                    json!({"feed": self.feed.id, "file": item.file_name, "line": item.line});
                let outcome = match self.transformer.transform(&item.raw, &context).await {
                    Ok(outcome) => outcome,
                    Err(e) => {
                        tracing::error!(feed = %self.feed.id, "transform unavailable, will retry: {e:#}");
                        return Ok(());
                    }
                };
                match outcome {
                    Outcome::Transient(message) => {
                        // Not the row's fault: leave it and the rest for the next pass.
                        tracing::warn!(feed = %self.feed.id, line = item.line, "transform cannot finish now, will retry: {message}");
                        return Ok(());
                    }
                    Outcome::Rejected { kind, message } => {
                        self.db.mark_rejected(item.id, &kind, &message, None)?
                    }
                    Outcome::Resources(resources) if resources.is_empty() => {
                        self.db.mark_rejected(
                            item.id,
                            "filtered",
                            "transform returned no resources",
                            None,
                        )?
                    }
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
            }
            if let Some(progress) = progress {
                progress.notify_one();
            }
        }
    }

    /// The bulk path has no transform: a record is the resource HFS will read. The
    /// engine only predicts what HFS will refuse (no resourceType, no id); the receipt
    /// is what decides.
    fn preflight_received(&self) -> Result<()> {
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

    /// Opens one submission per drop: the files with pending records, up to the
    /// manifest cap, handed to HFS through a manifest the engine serves.
    async fn submit_pending(&self) -> Result<()> {
        let Some(bulk) = &self.bulk else {
            return Ok(());
        };
        if self.bulk_transformed {
            return self.submit_transformed(bulk).await;
        }
        loop {
            // A halted or replaced source opens no more submissions.
            if self.halted() {
                return Ok(());
            }
            let files = self
                .db
                .submittable_files(&self.feed.id, self.feed.delivery.manifest_objects_max)?;
            if files.is_empty() {
                return Ok(());
            }
            let token = bulk::token();
            let ttl = Duration::from_secs(self.feed.delivery.url_ttl_seconds);
            let mut entries = Vec::new();
            let mut file_ids = Vec::new();
            let mut records = 0usize;
            let mut hashes = Vec::new();
            for file in &files {
                let url = match (file.origin.as_deref(), &file.source_key) {
                    // A file the engine keeps or reads from disk is served by it.
                    (Some("staged" | "local"), _) | (_, None) => {
                        bulk.file_url(&token, file.file_id)
                    }
                    // A store object is always read from its store, never served by
                    // the engine, even when the source no longer reads from one.
                    (_, Some(key)) => match self.source().share(key, ttl).await.and_then(|url| {
                        url.ok_or_else(|| {
                            anyhow::Error::new(Unshareable(format!(
                                "cannot hand {key} over: the source no longer reads from a store"
                            )))
                        })
                    }) {
                        Ok(url) => url,
                        Err(e) if e.downcast_ref::<Unshareable>().is_some() => {
                            // No retry can help: the file's records are settled.
                            let reason = bulk::safe_reason(&format!(
                                "cannot be handed over to the FHIR server: {e:#}"
                            ));
                            tracing::error!(feed = %self.feed.id, file = %file.name, "{reason}");
                            if self
                                .db
                                .reject_unsubmitted(file.file_id, "server", &reason)?
                                == 0
                            {
                                return Err(e);
                            }
                            continue;
                        }
                        // Signing may succeed later: the records stay pending, and
                        // nothing of this submission was written yet.
                        Err(e) => return Err(e),
                    },
                };
                entries.push((file.resource_type.clone(), url, file.count));
                file_ids.push(file.file_id);
                records += file.count;
                hashes.push(file.name.clone());
            }
            if entries.is_empty() {
                continue;
            }
            let hash_refs: Vec<&str> = hashes.iter().map(String::as_str).collect();
            let submission_id = bulk::submission_id(&self.feed.id, &hash_refs);
            let manifest = bulk.manifest(&entries);
            self.db.create_submission_for_files(
                &token,
                &self.feed.id,
                &submission_id,
                &manifest,
                &file_ids,
            )?;
            tracing::info!(feed = %self.feed.id, submission = %submission_id, files = files.len(), records, "submission opened");
        }
    }

    /// Moves every open submission one step: kick-off, then status polls until the
    /// receipts arrive, then the records' dispositions and the objects' archive.
    async fn poll_submissions(&self) -> Result<()> {
        let Some(bulk) = &self.bulk else {
            return Ok(());
        };
        for sub in self.db.submissions_due(&self.feed.id)? {
            let step = match sub.status.as_str() {
                "kickoff" => {
                    match bulk
                        .kickoff(
                            &self.feed.id,
                            &sub.submission_id,
                            &bulk.manifest_url(&sub.token),
                        )
                        .await
                    {
                        Ok(()) => match bulk.status_url(&self.feed.id, &sub.submission_id).await {
                            Ok(url) => Ok(Some(("running", Some(url)))),
                            Err(e) => Err(e),
                        },
                        // HFS already holds this drop: a repeat of the same files.
                        Err(SendError::Refused(409, _)) => {
                            match bulk.status_url(&self.feed.id, &sub.submission_id).await {
                                Ok(url) => Ok(Some(("running", Some(url)))),
                                Err(e) => Err(e),
                            }
                        }
                        Err(e) => Err(e),
                    }
                }
                _ => match sub.status_url.as_deref() {
                    None => Ok(Some(("kickoff", None))),
                    Some(url) => match bulk.poll(url).await {
                        Ok(Poll::InProgress) => Ok(None),
                        Ok(Poll::Done { references, errors }) => {
                            self.settle(&sub.token, &references, &errors).await?;
                            Ok(Some(("done", None)))
                        }
                        Err(e) => Err(e),
                    },
                },
            };
            match step {
                Ok(None) => {
                    self.db.update_submission(
                        &sub.token,
                        &sub.status,
                        None,
                        sub.attempts,
                        now_ms() + 2000,
                        None,
                    )?;
                }
                Ok(Some((status, url))) => {
                    self.db
                        .update_submission(&sub.token, status, url.as_deref(), 0, 0, None)?;
                    if status == "done" {
                        self.recovered()?;
                    }
                }
                Err(SendError::Refused(status, body)) => {
                    let reason =
                        bulk::safe_reason(&format!("HTTP {status}: {}", fhir::outcome_text(&body)));
                    tracing::warn!(feed = %self.feed.id, submission = %sub.submission_id, "refused: {reason}");
                    for (record_id, _, _) in self.db.submitted_records(&sub.token)? {
                        self.db
                            .mark_rejected(record_id, "server", &reason, Some(&body))?;
                    }
                    self.db.update_submission(
                        &sub.token,
                        "failed",
                        None,
                        sub.attempts,
                        0,
                        Some(&reason),
                    )?;
                    self.archive_submission(&sub.token).await?;
                }
                Err(SendError::Transient(reason)) => {
                    let reason = bulk::safe_reason(&reason);
                    let attempts = sub.attempts + 1;
                    let d = &self.feed.delivery;
                    let backoff = d
                        .initial_backoff_ms
                        .saturating_mul(1u64 << (attempts - 1).min(20))
                        .min(d.max_backoff_ms);
                    self.db.update_submission(
                        &sub.token,
                        &sub.status,
                        None,
                        attempts,
                        now_ms() + backoff as i64,
                        Some(&reason),
                    )?;
                    self.outage(attempts, &reason)?;
                }
            }
        }
        Ok(())
    }

    /// Gives every submitted record its disposition from the receipts, then archives
    /// the objects HFS has read.
    async fn settle(
        &self,
        token: &str,
        references: &std::collections::HashSet<String>,
        errors: &[String],
    ) -> Result<()> {
        let hint = match errors.len() {
            0 => String::new(),
            n => format!(" HFS reported {n} error(s); the first: {}", errors[0]),
        };
        // Page through the submission: a large one holds a million records, and
        // each page's receipts are applied in sets.
        let mut after_id = 0;
        loop {
            let page = self
                .db
                .submitted_records_after(token, after_id, SETTLE_PAGE)?;
            if page.is_empty() {
                break;
            }
            let mut sent = Vec::with_capacity(page.len());
            for (record_id, _, resources) in &page {
                let reference = resources
                    .get(0)
                    .and_then(|r| transform::check_resource(r).ok())
                    .map(|key| fhir::request_url(&key));
                match reference {
                    Some(r) if references.contains(&r) => {
                        sent.push((*record_id, json!([{"reference": r, "via": "$bulk-submit"}])))
                    }
                    _ => self.db.mark_rejected(
                        *record_id,
                        "server",
                        &format!("no receipt from HFS for this resource.{hint}"),
                        None,
                    )?,
                }
                after_id = *record_id;
            }
            self.db.mark_sent_many(&sent)?;
        }
        self.archive_submission(token).await?;
        self.remove_submission_files(token).await;
        Ok(())
    }

    /// Drops what the engine wrote for a transformed submission, now that HFS has
    /// read it and answered.
    async fn remove_submission_files(&self, token: &str) {
        let dir = self.bulk_dir.join(token);
        if dir.is_dir()
            && let Err(e) = std::fs::remove_dir_all(&dir)
        {
            tracing::warn!(feed = %self.feed.id, "cannot remove {}: {e}", dir.display());
        }
        if let Err(e) = self.source().remove_submission_files(token).await {
            tracing::warn!(feed = %self.feed.id, "cannot remove submission files at the source: {e:#}");
        }
    }

    /// Writes the pending records' resources as NDJSON per resource type, in the
    /// source's store when it can sign URLs (HFS fetches from there) or under
    /// `bulk_dir` served by the engine, and opens one submission over them.
    async fn submit_transformed(&self, bulk: &BulkSubmitter) -> Result<()> {
        loop {
            if self.halted() {
                return Ok(());
            }
            let records = self
                .db
                .submittable_records(&self.feed.id, self.feed.delivery.bulk_records_max)?;
            if records.is_empty() {
                return Ok(());
            }
            let token = bulk::token();
            let ttl = Duration::from_secs(self.feed.delivery.url_ttl_seconds);
            let mut by_type: std::collections::BTreeMap<String, Vec<u8>> = Default::default();
            let mut counts: std::collections::BTreeMap<String, usize> = Default::default();
            for (_, resources) in &records {
                for resource in resources.as_array().into_iter().flatten() {
                    let kind = resource["resourceType"]
                        .as_str()
                        .unwrap_or("Resource")
                        .to_string();
                    let lines = by_type.entry(kind.clone()).or_default();
                    lines.extend_from_slice(resource.to_string().as_bytes());
                    lines.push(b'\n');
                    *counts.entry(kind).or_default() += 1;
                }
            }
            let mut entries = Vec::new();
            for (n, (kind, bytes)) in by_type.into_iter().enumerate() {
                let n = n as i64 + 1;
                let name = format!("{n}.ndjson");
                let shared = match self
                    .source()
                    .put_submission_file(&token, &name, bytes.clone())
                    .await?
                {
                    Some(key) => self.source().share(&key, ttl).await?,
                    None => None,
                };
                let url = match shared {
                    Some(url) => url,
                    None => {
                        let dir = self.bulk_dir.join(&token);
                        std::fs::create_dir_all(&dir)
                            .with_context(|| format!("creating {}", dir.display()))?;
                        std::fs::write(dir.join(&name), &bytes)
                            .with_context(|| format!("writing {}", dir.join(&name).display()))?;
                        bulk.file_url(&token, n)
                    }
                };
                entries.push((kind.clone(), url, counts[&kind]));
            }
            let record_ids: Vec<i64> = records.iter().map(|(id, _)| *id).collect();
            let span = format!(
                "records {}-{}",
                record_ids[0],
                record_ids[record_ids.len() - 1]
            );
            let submission_id = bulk::submission_id(&self.feed.id, &[span.as_str()]);
            let manifest = bulk.manifest(&entries);
            self.db.create_submission(
                &token,
                &self.feed.id,
                &submission_id,
                &manifest,
                &record_ids,
            )?;
            tracing::info!(feed = %self.feed.id, submission = %submission_id, files = entries.len(), records = record_ids.len(), "submission opened (transformed)");
        }
    }

    async fn archive_submission(&self, token: &str) -> Result<()> {
        let file_ids: Vec<i64> = {
            let rows = self.db.query_json(
                "SELECT DISTINCT file_id FROM records WHERE submission = $1",
                crate::db::params![token],
            )?;
            rows.iter().filter_map(|r| r["file_id"].as_i64()).collect()
        };
        for file_id in file_ids {
            if let Some((key, name, origin)) = self.db.file_source(file_id)? {
                let archived = if origin.as_deref() == Some("staged") {
                    // An unpacked entry the engine served: nothing to move, it goes,
                    // but only from inside the bulk directory.
                    if !bulk::is_within(Path::new(&key), &self.bulk_dir) {
                        tracing::warn!(feed = %self.feed.id, "{key} is not inside the bulk directory; left in place");
                    } else if let Err(e) = std::fs::remove_file(&key) {
                        tracing::warn!(feed = %self.feed.id, "cannot remove {key}: {e}");
                    } else if let Some(dir) = Path::new(&key).parent() {
                        // The container's directory goes with its last entry.
                        let _ = std::fs::remove_dir(dir);
                    }
                    format!("removed {key}")
                } else {
                    self.source()
                        .acknowledge(&key, &format!("{file_id}-{name}"))
                        .await?
                };
                self.db.mark_file_archived(file_id, &archived)?;
            }
        }
        Ok(())
    }

    async fn deliver_due(&self) -> Result<()> {
        loop {
            // A halted or replaced source stops between batches: what is due stays
            // for the runner that replaces it.
            if self.halted() {
                return Ok(());
            }
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
    async fn deliver(&self, items: &[WorkItem]) -> Result<bool> {
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
                tracing::warn!(feed = %self.feed.id, status, "batch refused as a whole; sending records one by one");
                for item in items {
                    // Halted: the rest stays due, for the runner that replaces this one.
                    if self.halted() {
                        break;
                    }
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

    fn defer(&self, item: &WorkItem, reason: &str) -> Result<()> {
        let attempts = item.attempts + 1;
        let d = &self.feed.delivery;
        let backoff = d
            .initial_backoff_ms
            .saturating_mul(1u64 << (attempts - 1).min(20))
            .min(d.max_backoff_ms);
        self.db
            .defer(item.id, attempts, now_ms() + backoff as i64, reason)
    }

    fn finish(&self) -> Result<()> {
        for f in self.db.finish_files(&self.feed.id)? {
            tracing::info!(feed = %self.feed.id, file = %f.name, total = f.total, sent = f.sent, rejected = f.rejected, "file done");
            // The rules say whether a few rejected rows are worth an alert; without
            // one, every rejection is.
            let share_rules = self.rules("rejected_share")?;
            if f.newly_rejected > 0 && share_rules.is_empty() {
                let first = f
                    .first_rejection
                    .as_ref()
                    .map(|(line, err)| {
                        format!(
                            " First: line {line}: {}",
                            err.lines().next().unwrap_or_default()
                        )
                    })
                    .unwrap_or_default();
                self.alerts.raise(
                    Some(&self.feed.id),
                    "rejected_records",
                    &format!(
                        "{}: {} of {} records rejected in {}.{first}",
                        self.feed.name, f.rejected, f.total, f.name
                    ),
                )?;
            }
            // "More than N% of the rows in a load fail": fires on this load, clears on
            // the next load within the limit.
            let share = if f.total > 0 {
                f.rejected as f64 * 100.0 / f.total as f64
            } else {
                0.0
            };
            for rule in share_rules {
                let limit: f64 = rule.threshold.trim_end_matches('%').parse().unwrap_or(1.0);
                if share > limit {
                    self.alerts.fire_once(
                        &self.feed.id,
                        "rejected_share",
                        &rule,
                        &format!(
                            "{}: {share:.1}% of the rows in {} failed, more than the {limit}% allowed ({} of {})",
                            self.feed.name, f.name, f.rejected, f.total
                        ),
                    )?;
                } else {
                    self.alerts.clear_open(
                        &self.feed.id,
                        "rejected_share",
                        Some(rule.id),
                        &format!(
                            "{}: {} loaded with {share:.1}% failed, within the limit",
                            self.feed.name, f.name
                        ),
                    )?;
                }
            }
        }
        Ok(())
    }

    fn rules(&self, condition: &str) -> Result<Vec<Rule>> {
        Ok(self
            .db
            .rules_for(&self.feed.id)?
            .into_iter()
            .filter(|r| r.condition == condition)
            .collect())
    }

    /// Delivery failed `attempts` times in a row. The unreachable rules say when to
    /// tell someone; without one, the source's `alert_after_attempts` does.
    fn outage(&self, attempts: u32, reason: &str) -> Result<()> {
        let message = format!(
            "{}: cannot deliver to HFS after {attempts} attempts ({reason}); data is kept and retried",
            self.feed.name
        );
        let rules = self.rules("unreachable")?;
        if rules.is_empty() {
            if attempts >= self.feed.delivery.alert_after_attempts
                && !self.outage_alerted.swap(true, Ordering::SeqCst)
            {
                self.alerts
                    .raise(Some(&self.feed.id), "delivery", &message)?;
            }
            return Ok(());
        }
        for rule in &rules {
            if attempts >= rule.threshold.parse().unwrap_or(5) {
                self.alerts
                    .fire_once(&self.feed.id, "delivery", rule, &message)?;
            }
        }
        Ok(())
    }

    fn recovered(&self) -> Result<()> {
        let message = format!("{}: delivery to HFS recovered", self.feed.name);
        if self.outage_alerted.swap(false, Ordering::SeqCst) {
            self.alerts
                .raise(Some(&self.feed.id), "delivery_recovered", &message)?;
        }
        for rule in self.rules("unreachable")? {
            self.alerts
                .clear_open(&self.feed.id, "delivery", Some(rule.id), &message)?;
        }
        Ok(())
    }

    /// The rules that watch time rather than loads: silence, and a file expected by
    /// a time of day. Each fires once and clears itself when data arrives.
    fn check_silence(&self) -> Result<()> {
        let last = self.db.last_received_at(&self.feed.id)?;
        let since = last.unwrap_or(self.started_at);
        let now = now_ms();
        for rule in self.db.rules_for(&self.feed.id)? {
            match rule.condition.as_str() {
                "silence" => {
                    let Ok(window) = parse_duration(&rule.threshold) else {
                        continue;
                    };
                    let silent_for = now - since;
                    if silent_for > window.as_millis() as i64 {
                        self.alerts.fire_once(
                            &self.feed.id,
                            "silence",
                            &rule,
                            &format!(
                                "{}: nothing received for {} (expected at least every {})",
                                self.feed.name,
                                human(Duration::from_millis(silent_for as u64)),
                                human(window)
                            ),
                        )?;
                    } else {
                        self.alerts.clear_open(
                            &self.feed.id,
                            "silence",
                            Some(rule.id),
                            &format!("{}: data arrived", self.feed.name),
                        )?;
                    }
                }
                "missing_by" => {
                    let Some(due_minute) = parse_clock(&rule.threshold) else {
                        continue;
                    };
                    let now_utc = chrono::Utc::now();
                    let today = now_utc
                        .date_naive()
                        .and_hms_opt(0, 0, 0)
                        .expect("midnight")
                        .and_utc()
                        .timestamp_millis();
                    let received_today = last.is_some_and(|t| t >= today);
                    let due = now_utc.hour() * 60 + now_utc.minute() >= due_minute;
                    if received_today {
                        self.alerts.clear_open(
                            &self.feed.id,
                            "missing",
                            Some(rule.id),
                            &format!("{}: the file arrived", self.feed.name),
                        )?;
                    } else if due {
                        self.alerts.fire_once(
                            &self.feed.id,
                            "missing",
                            &rule,
                            &format!(
                                "{}: nothing has arrived today and the file was expected by {} UTC",
                                self.feed.name, rule.threshold
                            ),
                        )?;
                    }
                }
                _ => {}
            }
        }
        Ok(())
    }
}

pub fn human(d: Duration) -> String {
    let s = d.as_secs();
    match s {
        0..=119 => format!("{s}s"),
        120..=7199 => format!("{}m", s / 60),
        7200..=172_799 => format!("{}h", s / 3600),
        _ => format!("{}d", s / 86_400),
    }
}

/// Whether an error from a source says the object is not there any more.
fn is_not_found(e: &anyhow::Error) -> bool {
    e.chain().any(|cause| {
        matches!(
            cause.downcast_ref::<object_store::Error>(),
            Some(object_store::Error::NotFound { .. })
        ) || cause
            .downcast_ref::<std::io::Error>()
            .is_some_and(|io| io.kind() == std::io::ErrorKind::NotFound)
    })
}

#[cfg(all(test, feature = "secrets-native"))]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::secrets::{self, KeySettings, NewSecret, SecretKind, SecretValue};

    fn value(key_id: &str) -> SecretValue {
        SecretValue::new(
            SecretKind::Aws,
            [
                ("access_key_id".to_string(), key_id.to_string().into()),
                (
                    "secret_access_key".to_string(),
                    "generated-secret".to_string().into(),
                ),
            ],
        )
        .unwrap()
    }

    /// Holds a task at `name` until the test lets it go.
    fn pause_at(name: &'static str) -> (crate::AsyncPause, Arc<Notify>, Arc<Notify>) {
        let reached = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let once = Arc::new(AtomicBool::new(false));
        let (r, l) = (reached.clone(), release.clone());
        let hook: crate::AsyncPause = Arc::new(move |at| {
            let (r, l, once) = (r.clone(), l.clone(), once.clone());
            Box::pin(async move {
                if at == name && !once.swap(true, Ordering::SeqCst) {
                    r.notify_one();
                    l.notified().await;
                }
            })
        });
        (hook, reached, release)
    }

    async fn signed_with(runner: &FeedRunner) -> String {
        let url = runner
            .source()
            .share("a.ndjson", Duration::from_secs(60))
            .await
            .unwrap()
            .unwrap();
        let at = url.find("X-Amz-Credential=").unwrap() + "X-Amz-Credential=".len();
        url[at..].split("%2F").next().unwrap().to_string()
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_credential_is_registered_before_use_and_a_failed_registration_keeps_the_old_one() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(
            dir.path().join("hie.yaml"),
            "fhir_base_url: http://127.0.0.1:9\ndatabase: data/hie.db\n",
        )
        .unwrap();
        let (config, _) = crate::config::load(&dir.path().join("hie.yaml")).unwrap();
        let db = Arc::new(Db::open(&config.database).unwrap());
        let key = {
            let k = crate::crypto::MasterKey::generate().unwrap();
            String::from_utf8(k.to_line()[..44].to_vec()).unwrap()
        };
        let secrets = secrets::open(&config, &db, KeySettings::inline(&key)).unwrap();
        let store = secrets.store().unwrap().clone();
        let id = store
            .create(NewSecret {
                name: "lab".into(),
                kind: SecretKind::Aws,
                value: value("AKIDFIRST"),
            })
            .await
            .unwrap()
            .id;
        let feed: FeedConfig = serde_yaml::from_str(
            "id: s\nname: S\nsource:\n  type: s3\n  bucket: b\n  region: us-east-1\n  endpoint: http://127.0.0.1:9\n  credential: lab\nformat: ndjson\ntransform: t.py\n",
        )
        .unwrap();
        let alerts = Arc::new(Alerter::new(db.clone(), None));
        let (hook, reached, release) = pause_at("runner:credential:before-hold");
        let runner = Arc::new(
            FeedRunner::new(
                feed,
                db.clone(),
                Arc::new(FhirClient::with_credentials("http://127.0.0.1:9", None)),
                alerts,
                "python3",
                dir.path(),
                "http://127.0.0.1:9",
                "http://127.0.0.1:9",
            )
            .unwrap()
            .with_secrets(secrets.clone())
            .with_pause(Some(hook)),
        );
        let owner = secrets.owner().to_string();

        // The registration comes before the credential is used.
        assert!(!runner.credential_ready());
        let loading = tokio::spawn({
            let runner = runner.clone();
            async move { runner.ensure_credential().await }
        });
        reached.notified().await;
        assert!(db.secret_holds_of(&owner).unwrap().is_empty());
        assert!(!runner.credential_ready());
        release.notify_one();
        loading.await.unwrap();
        assert_eq!(
            db.secret_holds_of(&owner).unwrap(),
            [("s".to_string(), id.clone())]
        );
        assert!(runner.credential_ready());
        assert_eq!(signed_with(&runner).await, "AKIDFIRST");

        // A new revision whose registration cannot be taken (the lock wait runs
        // out): the old client stays, no row is removed, a credential alert opens.
        store.replace(&id, value("AKIDSECOND")).await.unwrap();
        let other = Arc::new(Db::open(&config.database).unwrap());
        let held = Arc::new(std::sync::Barrier::new(2));
        let go = Arc::new(std::sync::Barrier::new(2));
        {
            let (held, go) = (held.clone(), go.clone());
            other.on_pause(Arc::new(move |at| {
                if at == "ref:hold:after-lock" {
                    held.wait();
                    go.wait();
                }
            }));
        }
        let blocker = {
            let (other, id) = (other.clone(), id.clone());
            std::thread::spawn(move || other.secret_hold(&id, "t", "another-instance"))
        };
        held.wait();
        if db.is_postgres() {
            db.query_json("SET lock_timeout = '50ms'", Vec::new())
                .unwrap();
        } else {
            db.set_busy_timeout(Duration::from_millis(50));
        }
        runner.ensure_credential().await;
        assert_eq!(signed_with(&runner).await, "AKIDFIRST");
        assert_eq!(
            db.secret_holds_of(&owner).unwrap(),
            [("s".to_string(), id.clone())]
        );
        // On SQLite the same write lock keeps the alert from being stored; on
        // PostgreSQL only the secret's row is locked.
        if db.is_postgres() {
            assert!(db.open_alert("s", "credential", None).unwrap().is_some());
        }
        assert!(runner.credential_alert.load(Ordering::Acquire));
        go.wait();
        blocker.join().unwrap().unwrap();
        if db.is_postgres() {
            db.query_json("SET lock_timeout = 0", Vec::new()).unwrap();
        } else {
            db.set_busy_timeout(Duration::from_secs(5));
        }

        // Next pass: the new revision is used, and the alert is cleared.
        runner.ensure_credential().await;
        assert_eq!(signed_with(&runner).await, "AKIDSECOND");
        assert!(db.open_alert("s", "credential", None).unwrap().is_none());
        drop(other);
    }
}
