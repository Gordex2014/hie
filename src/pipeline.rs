use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use anyhow::{Context, Result};
use serde_json::{Value, json};

use crate::alerts::Alerter;
use crate::bulk::{self, BulkSubmitter, Poll};
use crate::config::{DeliveryMethod, FeedConfig, Format, parse_duration};
use crate::db::{Admission, CheckFailure, Db, Delivery, NewFile, Rule, WorkItem, now_ms};
use crate::fhir::{self, DeliveryKey, EntryResult, FhirClient, SendError};
use crate::reader;
use crate::secrets::Secrets;
use crate::source::{Incoming, Source, Unshareable};
use crate::transform::{self, Outcome, Transformer};
use crate::worker::WorkerLimits;
use tokio::sync::Notify;

mod manifest;

const TRANSFORM_CHUNK: usize = 200;
/// Records settled per page when a submission's receipts come back.
const SETTLE_PAGE: usize = 5_000;

/// A scan that failed: at which step, for the check it answers, and why, for the log.
struct ScanError(CheckFailure, anyhow::Error);

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
    /// The FHIR server's side of a `manifest` source's loads.
    manifest: Option<crate::manifest::Client>,
    transformer: Transformer,
    alerts: Arc<Alerter>,
    started_at: i64,
    /// Which engine instance this runner acts as, for the source's lease.
    instance: String,
    /// Where transformed submissions are written when the engine serves them.
    bulk_dir: PathBuf,
    /// `$bulk-submit` over the engine's transformed NDJSON, not the source objects.
    bulk_transformed: bool,
    /// Tag what this runner delivers with its source (`delivery.stamp_source`).
    stamp_source: bool,
    /// Whether this runner runs its passes under the source's lease
    /// (`pass`): its submission claims then need that lease still held.
    leased: std::sync::atomic::AtomicBool,
    /// How the manager drives this runner, once it runs: a halt is seen between records.
    control: std::sync::OnceLock<Arc<RunnerControl>>,
    /// The last record the transform could not finish now, as (id, revision,
    /// reason): its `record.attempt_failed` is written once, not on every pass.
    last_transient: std::sync::Mutex<Option<(i64, i64, String)>>,
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
        alerts.know_source(&feed.id, &feed.name);
        let feed_bulk_transformed = feed.bulk_transformed();
        let bulk = (feed.delivery.method == DeliveryMethod::BulkSubmit && !feed.is_manifest())
            .then(|| {
                BulkSubmitter::new(fhir_base_url, public_url)
                    .with_credentials(fhir.credentials().clone())
                    .with_destination(fhir.destination().clone())
            });
        let manifest = feed.is_manifest().then(|| {
            crate::manifest::Client::new(fhir_base_url, public_url, fhir.credentials().clone())
                .with_destination(fhir.destination().clone())
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
            manifest,
            transformer,
            alerts,
            started_at: now_ms(),
            instance: instance_id().to_string(),
            bulk_dir: std::env::temp_dir().join("hie-bulk"),
            bulk_transformed: feed_bulk_transformed,
            stamp_source: false,
            leased: std::sync::atomic::AtomicBool::new(false),
            control: std::sync::OnceLock::new(),
            last_transient: std::sync::Mutex::new(None),
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

    /// Tags every resource this runner delivers by REST or in a transformed
    /// submission with its source (`delivery.stamp_source` in `hie.yaml`).
    pub fn with_stamp_source(mut self, on: bool) -> Self {
        self.stamp_source = on;
        self
    }

    /// The resources of one record as they are sent: tagged copies when
    /// stamping is on, the stored ones, borrowed, otherwise.
    fn outgoing<'a>(
        &self,
        resources: Option<&'a Value>,
    ) -> Result<Vec<std::borrow::Cow<'a, Value>>, String> {
        use std::borrow::Cow;
        resources
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .map(|r| {
                if self.stamp_source {
                    fhir::stamp_source(r, &self.feed.id).map(Cow::Owned)
                } else {
                    Ok(Cow::Borrowed(r))
                }
            })
            .collect()
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
        self.leased
            .store(true, std::sync::atomic::Ordering::Release);
        if let Err(e) = self.source().start().await {
            tracing::error!(feed = %self.feed.id, "{e:#}");
            self.alerts.raise(
                Some(&self.feed.id),
                "listener",
                &format!("{}: {e:#}", self.feed.name),
            )?;
        }
        if self.db.is_paused(&self.feed.id)? {
            // A pause still in place once the waiting checks are read fails
            // them. Resumed meanwhile, they are left to the pass the resume
            // starts, which scans.
            let snapshot = self.pending_checks();
            if !snapshot.is_empty() && self.db.is_paused(&self.feed.id)? {
                self.answer_checks(&snapshot, Some(CheckFailure::Paused));
            }
        } else {
            self.ensure_credential().await;
            // Read here, after the credential, so the snapshot is taken just
            // before the scan; `tick_with` directly keeps this future shallow.
            let snapshot = self.pending_checks();
            // Paused while the credential was loaded: the checks read after
            // the pause fail, and the pass stops here.
            if !snapshot.is_empty() && self.db.is_paused(&self.feed.id)? {
                self.answer_checks(&snapshot, Some(CheckFailure::Paused));
                return Ok(true);
            }
            self.tick_with(&snapshot).await?;
        }
        Ok(true)
    }

    /// The source's checks waiting now, which the scan about to start answers;
    /// none for a runner that is being replaced or stopped, so its successor
    /// answers them. A store error is logged: the scan runs anyway. Public for
    /// tests that drive a scan by hand.
    pub fn pending_checks(&self) -> Vec<i64> {
        if self.halted() {
            return Vec::new();
        }
        let snapshot = self.db.pending_checks(&self.feed.id).unwrap_or_else(|e| {
            tracing::error!(feed = %self.feed.id, "reading the pending checks: {e:#}");
            Vec::new()
        });
        // Stopped while they were read: a check asked for after the replace
        // may be among them, and is the successor's to answer.
        if self.halted() {
            return Vec::new();
        }
        snapshot
    }

    /// Answers the checks of `snapshot`. A store error is logged: they stay
    /// pending, and the next scan answers them with every load since each was
    /// asked for, so nothing taken meanwhile is left out.
    fn answer_checks(&self, snapshot: &[i64], failure: Option<CheckFailure>) {
        if let Err(e) = self.db.answer_checks(&self.feed.id, snapshot, failure) {
            tracing::error!(feed = %self.feed.id, "answering checks {snapshot:?}: {e:#}");
        }
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
            if self.credential_alert.swap(false, Ordering::AcqRel) {
                self.credential_loaded();
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
        self.credential_loaded();
    }

    /// Clears every open `credential` alert of the source, an earlier
    /// definition's included: the credential works.
    fn credential_loaded(&self) {
        let cleared = self
            .db
            .open_alert_ids(&self.feed.id, "credential")
            .and_then(|open| {
                open.into_iter().try_for_each(|id| {
                    self.alerts
                        .clear(id, &self.feed.id, "credential", "credential loaded")
                        .map(drop)
                })
            });
        if let Err(e) = cleared {
            tracing::error!(feed = %self.feed.id, "{e:#}");
        }
    }

    /// Logs why the credential cannot be used, and opens a `credential` alert
    /// unless one is open for the source.
    fn credential_problem(&self, reference: &str, message: &str) {
        let message = format!("{}: credential {reference}: {message}", self.feed.name);
        tracing::error!(feed = %self.feed.id, "{message}");
        // A runner stopped (its source deleted or replaced) raises nothing
        // more: its successor, if any, finds the same problem.
        if self.halted() {
            return;
        }
        // One an earlier definition of the id left open does not stand for
        // this one's: the delete that ended it clears it.
        let open = match self.db.open_alert_since_birth(&self.feed.id, "credential") {
            Ok(open) => open,
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
        let snapshot = self.pending_checks();
        self.tick_with(&snapshot).await
    }

    /// A pass that answers the checks of `snapshot`, taken before it started.
    /// Public for tests that drive a scan by hand.
    pub async fn tick_with(&self, snapshot: &[i64]) -> Result<()> {
        if self.feed.is_manifest() {
            return self.manifest_pass(snapshot).await;
        }
        // Without its credential loaded, a source cannot read its store: nothing
        // is listed and nothing is handed over this pass, but what is open is
        // still polled and settled.
        let ready = self.credential_ready();
        // A source that cannot be read this pass must not hold back what is
        // already in the store: the rest of the pass runs, the scan is retried next.
        // The checks are answered by the scan, before anything is delivered.
        if ready {
            match self.scan().await {
                Ok(()) => self.answer_checks(snapshot, None),
                Err(ScanError(failure, e)) => {
                    tracing::error!(feed = %self.feed.id, "scan failed, will retry next pass: {e:#}");
                    self.answer_checks(snapshot, Some(failure));
                }
            }
        } else {
            self.answer_checks(snapshot, Some(CheckFailure::CredentialNotLoaded));
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

    async fn scan(&self) -> Result<(), ScanError> {
        use CheckFailure::*;
        let source = self.source();
        let read_only = source.is_read_only();
        let listed = source
            .list_meta()
            .await
            .map_err(|e| ScanError(Listing, e))?;
        for (key, name, size) in listed {
            // Someone else's bucket keeps what we took: known by name and size,
            // it is not downloaded again just to recognise its hash.
            if read_only
                && size > 0
                && self
                    .db
                    .has_file(&self.feed.id, &name, size)
                    .map_err(|e| ScanError(Taking, e))?
            {
                continue;
            }
            let incoming = match source.fetch(&key).await {
                Ok(incoming) => incoming,
                // Announced, then gone before this pass reached it: nothing to take.
                Err(e) if is_not_found(&e) => {
                    tracing::warn!(feed = %self.feed.id, key, "object no longer there, skipped");
                    continue;
                }
                Err(e) => return Err(ScanError(Fetching, e)),
            };
            self.ingest(incoming)
                .await
                .map_err(|e| ScanError(Taking, e))?;
        }
        // Notifications are acknowledged only once what they announced is in the
        // store: a pass that failed before this leaves them to be delivered again.
        source
            .settle_notifications()
            .await
            .map_err(|e| ScanError(Notifications, e))?;
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
            return self
                .duplicate(&key, &name, &sha, bytes.len() as u64, original)
                .await;
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
        let (admission, readable) = self.admit(
            &NewFile {
                feed,
                source_name: &self.feed.name,
                name: &name,
                sha: &sha,
                size: bytes.len() as u64,
                source_key: keep.then_some(key.as_str()),
                origin: keep.then_some(origin),
            },
            &bytes,
        )?;
        let file_id = match admission {
            Admission::Admitted(id) => id,
            // Taken meanwhile, by an instance that held the source before.
            Admission::AlreadyTaken(original) => {
                return self
                    .duplicate(&key, &name, &sha, bytes.len() as u64, original)
                    .await;
            }
        };
        if keep && readable {
            return Ok(());
        }
        let archived = self
            .source()
            .acknowledge(&key, &format!("{file_id}-{name}"))
            .await?;
        self.db.mark_file_archived(file_id, &archived)
    }

    /// A file whose content was already taken, as `original`.
    async fn duplicate(
        &self,
        key: &str,
        name: &str,
        sha: &str,
        size: u64,
        original: i64,
    ) -> Result<()> {
        let feed = &self.feed.id;
        if self.source().is_read_only() {
            // Someone else's bucket: the object stays, and was already taken.
            return Ok(());
        }
        if self.bulk.is_some() && self.db.file_source(original)?.is_some() {
            // The same object, still waiting for HFS to fetch it: leave it alone.
            return Ok(());
        }
        let id = self
            .db
            .insert_duplicate(feed, &self.feed.name, name, sha, size, original)?;
        tracing::info!(feed, file = %name, duplicate_of = original, "same content already received; not reprocessed");
        let archived = self
            .source()
            .acknowledge(key, &format!("{id}-{name}"))
            .await?;
        self.db.mark_file_archived(id, &archived)
    }

    /// Records a file and reads its records into the store, in one transaction
    /// (`Db::admit_file`). Returns what was done with it, and `false` when it
    /// could not be read.
    fn admit(&self, file: &NewFile, bytes: &[u8]) -> Result<(Admission, bool)> {
        let feed = &self.feed.id;
        let name = file.name;
        match reader::read(self.feed.format, bytes) {
            Ok(records) => {
                let rows: Vec<_> = records
                    .into_iter()
                    .map(|r| (r.line, r.raw, r.parse_error))
                    .collect();
                let admission = self.db.admit_file(file, Ok(&rows))?;
                if let Admission::Admitted(_) = admission {
                    tracing::info!(feed, file = %name, records = rows.len(), "received");
                }
                Ok((admission, true))
            }
            Err(e) => {
                let admission = self.db.admit_file(file, Err(&format!("{e:#}")))?;
                if let Admission::Admitted(_) = admission {
                    self.alerts.raise(
                        Some(feed),
                        "unreadable_file",
                        &format!("{}: cannot read {name}: {e:#}", self.feed.name),
                    )?;
                }
                Ok((admission, false))
            }
        }
    }

    /// A zip is a drop of several files: every entry whose extension fits the
    /// source's format becomes a load of its own, named `drop.zip!/entry`; the
    /// zip is recorded as `unpacked` (so it is not read twice) and archived.
    async fn unpack(&self, key: &str, name: &str, sha: &str, bytes: Vec<u8>) -> Result<()> {
        let feed = &self.feed.id;
        let container =
            self.db
                .insert_container(feed, &self.feed.name, name, sha, bytes.len() as u64)?;
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
        let mut stage_count = 0usize;
        for (path, data) in entries {
            let extension = path.rsplit('.').next().unwrap_or("").to_ascii_lowercase();
            if !wanted.contains(&extension.as_str()) {
                tracing::debug!(feed, file = %name, entry = %path, "entry skipped: not a {:?} file", self.feed.format);
                continue;
            }
            let entry_name = format!("{name}!/{path}");
            let entry_sha = reader::sha256_hex(&data);
            if let Some(original) = self.db.find_file_by_hash(feed, &entry_sha)? {
                self.db.insert_duplicate(
                    feed,
                    &self.feed.name,
                    &entry_name,
                    &entry_sha,
                    data.len() as u64,
                    original,
                )?;
                continue;
            }
            // On the source_objects bulk path HFS reads the entry, not the zip: the
            // entry is staged where HFS can fetch it and kept until the receipts.
            let staged = if self.bulk.is_some() && !self.bulk_transformed {
                // Each entry staged gets a name of its own, also after one that
                // turned out to be taken already.
                stage_count += 1;
                let base = format!("{stage_count}.ndjson");
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
            // Each entry is admitted in a transaction of its own; one that fails
            // part way leaves the container recognised by its hash next time.
            let (admission, _) = self.admit(
                &NewFile {
                    feed,
                    source_name: &self.feed.name,
                    name: &entry_name,
                    sha: &entry_sha,
                    size: data.len() as u64,
                    source_key: staged.as_ref().map(|(key, _)| key.as_str()),
                    origin: staged.as_ref().map(|(_, origin)| *origin),
                },
                &data,
            )?;
            if let Admission::AlreadyTaken(original) = admission {
                // Taken meanwhile: a duplicate. A copy staged on disk goes; one
                // staged in the store stays where it was written (the source has
                // no way to remove a single object), under a name not used again.
                if let Some((path, "staged")) = &staged {
                    let _ = std::fs::remove_file(path);
                }
                self.db.insert_duplicate(
                    feed,
                    &self.feed.name,
                    &entry_name,
                    &entry_sha,
                    data.len() as u64,
                    original,
                )?;
                continue;
            }
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
                // Each outcome changes the record only while it is still at the
                // revision it was selected at; a person may have put it back since.
                match outcome {
                    Outcome::Transient(message) => {
                        // Not the row's fault: leave it and the rest for the next pass.
                        tracing::warn!(feed = %self.feed.id, line = item.line, "transform cannot finish now, will retry: {message}");
                        // Logged once for the record and reason while this runner
                        // runs; again when the reason changes. Remembered only once
                        // stored, so an event that could not be is tried again.
                        let seen = (item.id, item.revision, message);
                        let repeat = self.last_transient.lock().expect("transient lock").as_ref()
                            == Some(&seen);
                        if !repeat && self.db.transform_deferred(item, &seen.2)? {
                            *self.last_transient.lock().expect("transient lock") = Some(seen);
                        }
                        return Ok(());
                    }
                    Outcome::Rejected { kind, message } => {
                        self.db.reject_item(item, "received", &kind, &message)?;
                    }
                    Outcome::Resources(resources) if resources.is_empty() => {
                        self.db.reject_item(
                            item,
                            "received",
                            "filtered",
                            "transform returned no resources",
                        )?;
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
                            Ok(_) => {
                                self.db.mark_transformed(item, &Value::Array(resources))?;
                            }
                            Err(msg) => {
                                self.db.reject_item(item, "received", "validation", &msg)?;
                            }
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
                    Ok(DeliveryKey::Id { .. }) => good.push((item.id, item.revision)),
                    Ok(DeliveryKey::Identifier { resource_type, .. }) => {
                        self.db.reject_item(
                            item,
                            "received",
                            "validation",
                            &format!(
                                "{resource_type} has no id; $bulk-submit stores resources by id"
                            ),
                        )?;
                    }
                    Err(msg) => {
                        self.db.reject_item(item, "received", "validation", &msg)?;
                    }
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
            let mut claims = Vec::new();
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
                claims.push((file.file_id, file.count));
                records += file.count;
                hashes.push(file.name.clone());
            }
            if entries.is_empty() {
                continue;
            }
            let hash_refs: Vec<&str> = hashes.iter().map(String::as_str).collect();
            let submission_id = bulk::submission_id(&self.feed.id, &hash_refs);
            let manifest = bulk.manifest(&entries);
            if !self.db.create_submission_for_files(
                &token,
                &self.feed.id,
                &submission_id,
                &manifest,
                &claims,
            )? {
                // A record changed after the manifest was built for it: nothing
                // was opened, and it is built again from what is pending now.
                tracing::info!(feed = %self.feed.id, "a record changed while its submission was built; building it again");
                continue;
            }
            tracing::info!(feed = %self.feed.id, submission = %submission_id, files = files.len(), records, "submission opened");
        }
    }

    /// Moves every open submission one step: kick-off, then status polls until the
    /// receipts arrive, then the records' dispositions and the objects' archive.
    async fn poll_submissions(&self) -> Result<()> {
        let Some(bulk) = &self.bulk else {
            return Ok(());
        };
        let leased = self.leased.load(std::sync::atomic::Ordering::Acquire);
        for sub in self.db.submissions_due(&self.feed.id)? {
            // A runner stopped meanwhile (its source deleted or replaced) sends
            // nothing more, and takes no lease again: its successor, if any,
            // moves the submissions.
            if self.halted() {
                tracing::info!(feed = %self.feed.id, "stopped during the pass; its submissions are left to its successor");
                return Ok(());
            }
            // Under the lease, it is renewed right before each claim: a pass
            // that ran past it (a large file transformed, a slow answer for
            // an earlier submission) would otherwise claim nothing more. One
            // another instance took meanwhile stops here.
            if leased
                && !self
                    .db
                    .acquire_lease(&self.feed.id, &self.instance, self.lease_ttl_ms())?
            {
                tracing::info!(feed = %self.feed.id, "another instance took the source during the pass; its submissions are left to it");
                return Ok(());
            }
            // Claimed before its request: only while it is still as read here,
            // so a request is never built from a row another runner has moved
            // on, and the answer to any earlier claim changes nothing.
            let Some(claim) = self.db.claim_submission(&sub, &self.instance, leased)? else {
                match self.db.lease(&self.feed.id)? {
                    Some(l) if l.owner != self.instance => {
                        tracing::info!(feed = %self.feed.id, submission = %sub.submission_id, "not claimed: another instance holds the source");
                    }
                    // Renewed just before, yet run out by the claim (a long
                    // wait for the store).
                    None if leased => {
                        tracing::info!(feed = %self.feed.id, submission = %sub.submission_id, "not claimed: this instance's lease on the source ran out");
                    }
                    _ => {
                        tracing::debug!(feed = %self.feed.id, submission = %sub.submission_id, "not claimed: another runner moved it");
                    }
                }
                continue;
            };
            // Stopped while it was claimed: nothing is sent. The claim only
            // moved the epoch; the successor claims it again.
            if self.halted() {
                tracing::info!(feed = %self.feed.id, "stopped during the pass; its submissions are left to its successor");
                return Ok(());
            }
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
                            if self.settle(&sub.token, claim, &references, &errors).await? {
                                Ok(Some(("done", None)))
                            } else {
                                Ok(Some(("stale", None)))
                            }
                        }
                        Err(e) => Err(e),
                    },
                },
            };
            // Each write below holds only while the submission is still under
            // this request's claim: an older runner's answer (a progress, a
            // failure, a refusal or the receipts) changes nothing.
            match step {
                // Answered for an older attempt: the current runner decides.
                Ok(Some(("stale", _))) => {}
                Ok(None) => {
                    self.db.progress_submission(
                        &sub.token,
                        Some(claim),
                        &sub.status,
                        None,
                        sub.attempts,
                        now_ms() + 2000,
                        None,
                    )?;
                }
                Ok(Some(("done", _))) => {
                    if self.db.settle_submission(&sub.token, claim)? {
                        self.recovered()?;
                    }
                }
                Ok(Some((status, url))) => {
                    self.db.progress_submission(
                        &sub.token,
                        Some(claim),
                        status,
                        url.as_deref(),
                        0,
                        0,
                        None,
                    )?;
                }
                Err(SendError::Refused(status, body)) => {
                    // What the server said may echo signed URLs or the engine's
                    // own paths: scrubbed before it is logged or stored.
                    let reason = bulk::scrub(
                        &format!("HTTP {status}: {}", fhir::outcome_text(&body)),
                        &sub.token,
                    );
                    if self
                        .db
                        .reject_submission(
                            &sub.token,
                            claim,
                            "server",
                            &reason,
                            &bulk::scrub_value(&body, &sub.token),
                        )?
                        .is_some()
                    {
                        // Logged only when it took: an older request's refusal
                        // changed nothing.
                        tracing::warn!(feed = %self.feed.id, submission = %sub.submission_id, "refused: {reason}");
                        self.archive_submission(&sub.token).await?;
                    }
                }
                Err(SendError::Transient(reason)) => {
                    let reason = bulk::scrub(&reason, &sub.token);
                    let attempts = claim.attempts + 1;
                    let d = &self.feed.delivery;
                    let backoff = d
                        .initial_backoff_ms
                        .saturating_mul(1u64 << (attempts - 1).min(20))
                        .min(d.max_backoff_ms);
                    if self.db.submission_attempt_failed(
                        &sub.token,
                        claim,
                        now_ms() + backoff as i64,
                        &reason,
                    )? {
                        self.outage(attempts, &reason)?;
                    }
                }
            }
        }
        Ok(())
    }

    /// Gives every submitted record its disposition from the receipts, then archives
    /// the objects HFS has read. Only while the submission is still under the
    /// poll's `claim`: `false` when it moved, with the pages already applied
    /// kept and the rest, and the archive, left to the runner that moved it.
    async fn settle(
        &self,
        token: &str,
        claim: crate::db::Claim,
        references: &std::collections::HashSet<String>,
        errors: &[String],
    ) -> Result<bool> {
        let hint = match errors.len() {
            0 => String::new(),
            n => format!(" HFS reported {n} error(s); the first: {}", errors[0]),
        };
        let missing_reason = bulk::scrub(
            &format!("no receipt from HFS for this resource.{hint}"),
            token,
        );
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
            let mut missing = Vec::new();
            for (record_id, _, resources) in &page {
                let reference = resources
                    .get(0)
                    .and_then(|r| transform::check_resource(r).ok())
                    .map(|key| fhir::request_url(&key));
                match reference {
                    Some(r) if references.contains(&r) => {
                        sent.push((*record_id, json!([{"reference": r, "via": "$bulk-submit"}])))
                    }
                    _ => missing.push(*record_id),
                }
                after_id = *record_id;
            }
            // The page's outcomes, its tallies and its events in one transaction.
            if !self
                .db
                .settle_page(token, claim, &sent, &missing, &missing_reason)?
            {
                return Ok(false);
            }
        }
        // Nothing is archived for an answer to an older poll.
        if !self.db.submission_open_at(token, claim)? {
            return Ok(false);
        }
        self.archive_submission(token).await?;
        self.remove_submission_files(token).await;
        Ok(true)
    }

    /// Drops what the engine wrote for a transformed submission, now that HFS has
    /// read it and answered.
    async fn remove_submission_files(&self, token: &str) {
        // The token names the directory and the objects: never logged.
        let dir = self.bulk_dir.join(token);
        if dir.is_dir()
            && let Err(e) = std::fs::remove_dir_all(&dir)
        {
            let reason = bulk::scrub(&format!("cannot remove {}: {e}", dir.display()), token);
            tracing::warn!(feed = %self.feed.id, "{reason}");
        }
        if let Err(e) = self.source().remove_submission_files(token).await {
            let reason = bulk::scrub(
                &format!("cannot remove submission files at the source: {e:#}"),
                token,
            );
            tracing::warn!(feed = %self.feed.id, "{reason}");
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
            // Tagged here, just before they are written: a record whose
            // resources cannot carry the tag is rejected and left out.
            let mut records_out = Vec::with_capacity(records.len());
            for (id, revision, resources) in &records {
                match self.outgoing(Some(resources)) {
                    Ok(resources) => records_out.push((*id, *revision, resources)),
                    Err(msg) => {
                        self.db
                            .reject_before_submission(*id, *revision, "validation", &msg)?;
                    }
                }
            }
            if records_out.is_empty() {
                continue;
            }
            let token = bulk::token();
            let ttl = Duration::from_secs(self.feed.delivery.url_ttl_seconds);
            let mut by_type: std::collections::BTreeMap<String, Vec<u8>> = Default::default();
            let mut counts: std::collections::BTreeMap<String, usize> = Default::default();
            for (_, _, resources) in &records_out {
                for resource in resources.iter().map(|r| &**r) {
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
            let written = async {
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
                            std::fs::write(dir.join(&name), &bytes).with_context(|| {
                                format!("writing {}", dir.join(&name).display())
                            })?;
                            bulk.file_url(&token, n)
                        }
                    };
                    entries.push((kind.clone(), url, counts[&kind]));
                }
                Ok::<_, anyhow::Error>(entries)
            }
            .await;
            // The paths and keys written are named after the token: an error
            // about them never carries it.
            let entries =
                written.map_err(|e| anyhow::anyhow!(bulk::scrub(&format!("{e:#}"), &token)))?;
            let claims: Vec<(i64, i64)> =
                records_out.iter().map(|(id, rev, _)| (*id, *rev)).collect();
            let submission_id = bulk::transformed_submission_id(
                &self.feed.id,
                claims[0].0,
                claims[claims.len() - 1].0,
                self.stamp_source.then_some(token.as_str()),
            );
            let manifest = bulk.manifest(&entries);
            if !self.db.create_submission(
                &token,
                &self.feed.id,
                &submission_id,
                &manifest,
                &claims,
                self.stamp_source,
            )? {
                // A record changed while its file was written (a person put it
                // back), or another runner took one: nothing was opened; the
                // files go, and the submission is built again from what is
                // pending now.
                tracing::info!(feed = %self.feed.id, "a record changed while its submission was written; building it again");
                self.remove_submission_files(&token).await;
                continue;
            }
            tracing::info!(feed = %self.feed.id, submission = %submission_id, files = entries.len(), records = claims.len(), "submission opened (transformed)");
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
        // A record whose resources cannot carry the source tag is rejected as a
        // whole and goes no further: not in the Bundle, not in what is settled.
        let mut kept: Vec<&WorkItem> = Vec::with_capacity(items.len());
        let mut untaggable = Vec::new();
        let mut entries = Vec::new();
        let mut owner = Vec::new();
        for item in items {
            match self.outgoing(item.resources.as_ref()) {
                Ok(resources) => {
                    for resource in resources {
                        let key =
                            transform::check_resource(&resource).map_err(anyhow::Error::msg)?;
                        entries.push((fhir::request_url(&key), resource.into_owned()));
                        owner.push(kept.len());
                    }
                    kept.push(item);
                }
                Err(msg) => untaggable.push((item, msg)),
            }
        }
        if !untaggable.is_empty() {
            let rejected: Vec<(&WorkItem, Delivery)> = untaggable
                .iter()
                .map(|(item, msg)| {
                    (
                        *item,
                        Delivery::Rejected {
                            kind: "validation",
                            error: msg,
                            response: None,
                        },
                    )
                })
                .collect();
            self.db.delivered(&rejected)?;
        }
        if kept.is_empty() {
            return Ok(true);
        }
        let items = &kept[..];
        // Untagged, HFS may hold what this request writes until well after it
        // was sent: the bound is stored before it leaves, once the token is in
        // hand, and never cleared, so neither a crash nor another instance's
        // request can make it earlier. A store that cannot take it in time
        // fails the pass, as any store failure does: nothing is sent.
        let untagged = !self.stamp_source;
        let sent = self
            .fhir
            .post_bundle_with(&fhir::batch_bundle(&entries), || {
                if untagged {
                    // A write slower than its slack (the store was held, by a
                    // large file's rows, say) is taken again from a fresh instant:
                    // the bound only grows, and the second write rarely waits.
                    // The slack runs from when the store's connection is held,
                    // not from when it was asked for, and ends here, right
                    // before the request leaves.
                    let slack =
                        std::time::Duration::from_millis(crate::db::UNTAGGED_WRITE_SLACK_MS as u64);
                    for _ in 0..2 {
                        let started = self
                            .db
                            .mark_untagged_from_now(&self.feed.id, crate::db::UNTAGGED_MARGIN_MS)
                            .context("recording an untagged delivery before sending it")?;
                        if started.elapsed() <= slack {
                            return Ok(());
                        }
                    }
                    anyhow::bail!(
                        "recording an untagged delivery took longer than its slack twice; not sent"
                    );
                }
                Ok(())
            })
            .await?;
        match sent {
            Err(SendError::Transient(reason)) => {
                let deferred: Vec<_> = items
                    .iter()
                    .map(|item| (*item, self.defer(item, &reason)))
                    .collect();
                self.db.delivered(&deferred)?;
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
                self.db.delivered(&[(
                    items[0],
                    Delivery::Rejected {
                        kind: "server",
                        error: &reason,
                        response: Some(&body),
                    },
                )])?;
                Ok(true)
            }
            Ok(response) => {
                let results = fhir::entry_results(&response, entries.len());
                // HFS answered, but delivery recovered only if it decided at
                // least one entry: a batch whose every entry is to be retried
                // is still an outage for this source.
                if results
                    .iter()
                    .any(|r| matches!(r, EntryResult::Ok(_) | EntryResult::Rejected(..)))
                {
                    self.recovered()?;
                }
                // Each record's outcome, worked out first, then stored together
                // with their events before the next batch is sent.
                let mut decided: Vec<(&WorkItem, Decided)> = Vec::with_capacity(items.len());
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
                    let responses = Value::Array(responses);
                    if let Some(EntryResult::Rejected(status, v)) =
                        mine.iter().find(|r| matches!(r, EntryResult::Rejected(..)))
                    {
                        let reason = format!("HTTP {status}: {}", fhir::outcome_text(v));
                        decided.push((item, Decided::Rejected(reason, responses)));
                    } else if let Some(EntryResult::Transient(status, _)) = mine
                        .iter()
                        .find(|r| matches!(r, EntryResult::Transient(..)))
                    {
                        decided.push((
                            item,
                            Decided::Deferred(format!("HTTP {status} for this entry")),
                        ));
                    } else {
                        decided.push((item, Decided::Sent(responses)));
                    }
                }
                let outcomes: Vec<(&WorkItem, Delivery)> = decided
                    .iter()
                    .map(|(item, d)| {
                        let outcome = match d {
                            Decided::Sent(responses) => Delivery::Sent(responses),
                            Decided::Rejected(reason, responses) => Delivery::Rejected {
                                kind: "server",
                                error: reason,
                                response: Some(responses),
                            },
                            Decided::Deferred(reason) => self.defer(item, reason),
                        };
                        (*item, outcome)
                    })
                    .collect();
                self.db.delivered(&outcomes)?;
                Ok(true)
            }
        }
    }

    /// The next attempt of a record whose delivery failed for a reason that may pass.
    fn defer<'a>(&self, item: &WorkItem, reason: &'a str) -> Delivery<'a> {
        let attempts = item.attempts + 1;
        let d = &self.feed.delivery;
        let backoff = d
            .initial_backoff_ms
            .saturating_mul(1u64 << (attempts - 1).min(20))
            .min(d.max_backoff_ms);
        Delivery::Deferred {
            attempts,
            next_attempt_at: now_ms() + backoff as i64,
            error: reason,
        }
    }

    /// Finishes each file that looks finished, one at a time, and weighs its
    /// alerts right after it is stored: a later file that fails to finish never
    /// leaves an earlier one without them.
    fn finish(&self) -> Result<()> {
        for candidate in self.db.finish_candidates(&self.feed.id)? {
            let Some(f) = self.db.finish_file(&self.feed.id, candidate)? else {
                continue;
            };
            tracing::info!(feed = %self.feed.id, file = %f.name, total = f.total, sent = f.sent, rejected = f.rejected, failing = f.failing, "file done");
            // Rows a transform filtered out on purpose are not failures: they
            // neither raise these alerts nor count in a share.
            let filtered = f.rejected - f.failing;
            // The filtered rows, named apart from the failing ones.
            let others = |before: &str, after: &str| match filtered {
                0 => String::new(),
                1 => format!("{before}1 other was filtered out by its transform{after}"),
                n => format!("{before}{n} others were filtered out by its transform{after}"),
            };
            // The rules say whether a few failing rows are worth an alert; without
            // one, every failing row is.
            let share_rules = self.rules("rejected_share")?;
            if f.newly_failing > 0 && share_rules.is_empty() {
                let first = f
                    .first_failure
                    .as_ref()
                    .map(|(line, err)| {
                        format!(
                            " First: line {line}: {}",
                            err.lines().next().unwrap_or_default()
                        )
                    })
                    .unwrap_or_default();
                let note = others(" ", ".");
                self.alerts.raise(
                    Some(&self.feed.id),
                    "rejected_records",
                    &format!(
                        "{}: {} of {} records failed in {}.{note}{first}",
                        self.feed.name, f.failing, f.total, f.name
                    ),
                )?;
            }
            // "More than N% of the rows a load kept fail": fires on this load,
            // clears on the next load within the limit. A load that kept no row
            // (all filtered out, or none at all, as a zip of entries) says nothing
            // either way.
            let kept = f.total - filtered;
            if kept <= 0 {
                continue;
            }
            let share = f.failing as f64 * 100.0 / kept as f64;
            for rule in share_rules {
                let limit: f64 = rule.threshold.trim_end_matches('%').parse().unwrap_or(1.0);
                if share > limit {
                    self.alerts.fire_once(
                        &self.feed.id,
                        "rejected_share",
                        &rule,
                        &format!(
                            "{}: {}% of the rows kept from {} failed, more than the {limit}% allowed ({} of {}{})",
                            self.feed.name,
                            above(share, limit),
                            f.name,
                            f.failing,
                            kept,
                            others(", ", "")
                        ),
                    )?;
                } else {
                    self.alerts.clear_open(
                        &self.feed.id,
                        "rejected_share",
                        Some(rule.id),
                        &format!(
                            "{}: {} loaded with {}% failed, within the limit",
                            self.feed.name,
                            f.name,
                            within(share, limit)
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
    /// tell someone; without one, the source's `alert_after_attempts` does, once
    /// per outage: no second alert while one is open, whichever process raised it.
    fn outage(&self, attempts: u32, reason: &str) -> Result<()> {
        // A runner stopped while its request was in flight (its source
        // deleted or replaced) raises nothing: a deleted source's alert would
        // outlive it, and a successor sees the same outage at its next attempt.
        if self.halted() {
            return Ok(());
        }
        let message = format!(
            "{}: cannot deliver to HFS after {attempts} attempts ({reason}); data is kept and retried",
            self.feed.name
        );
        let rules = self.rules("unreachable")?;
        if rules.is_empty() {
            // An open one left by an earlier definition of this id (deleted
            // and created again) does not stand for this outage; this
            // definition's next recovery clears both.
            if attempts >= self.feed.delivery.alert_after_attempts
                && !self.db.open_alert_since_birth(&self.feed.id, "delivery")?
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

    /// Delivery works again: every open `delivery` alert of the source is
    /// cleared, with a rule or without, and whether or not its rule still
    /// exists. The cleared alert is the recovery notice.
    fn recovered(&self) -> Result<()> {
        let open = self.db.open_alert_ids(&self.feed.id, "delivery")?;
        if open.is_empty() {
            return Ok(());
        }
        let message = format!("{}: delivery to HFS recovered", self.feed.name);
        for id in open {
            self.alerts.clear(id, &self.feed.id, "delivery", &message)?;
        }
        Ok(())
    }

    /// The rules that watch time rather than loads: silence, and a file expected by
    /// a time of day (in the rule's zone). Each fires once and clears itself when
    /// data arrives.
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
                    self.alerts.check_missing_by(
                        &self.feed.id,
                        &self.feed.name,
                        &rule,
                        chrono::Utc::now(),
                    )?;
                }
                _ => {}
            }
        }
        Ok(())
    }
}

/// A record's outcome from a batch answer, before it is stored.
enum Decided {
    Sent(Value),
    Rejected(String, Value),
    Deferred(String),
}

/// A share at or below `limit`, with one decimal or as many more as it takes to
/// read within it: below the limit unless it is the limit, and above 0 unless
/// nothing failed. 9.96% against 10% is 9.96, not 10.0.
fn within(share: f64, limit: f64) -> String {
    (1..=6)
        .map(|decimals| format!("{share:.decimals$}"))
        .find(|shown| {
            shown.parse::<f64>().is_ok_and(|v| {
                (v < limit || (v == limit && share == limit)) && (v > 0.0 || share == 0.0)
            })
        })
        .unwrap_or_else(|| share.to_string())
}

/// A share above `limit`, with one decimal or as many more as it takes to
/// read above it, and below 100 unless every row failed: 1 in 81 against
/// 1.23% is 1.235, not 1.2.
fn above(share: f64, limit: f64) -> String {
    (1..=6)
        .map(|decimals| format!("{share:.decimals$}"))
        .find(|shown| {
            shown
                .parse::<f64>()
                .is_ok_and(|v| v > limit && (v < 100.0 || share >= 100.0))
        })
        .unwrap_or_else(|| share.to_string())
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

    /// A source created again whose credential is missing raises its own
    /// `credential` alert, though one an earlier definition of its id left is
    /// still open; once the credential loads, both are cleared.
    #[tokio::test]
    async fn a_source_created_again_raises_its_own_credential_alert() {
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
        let feed: FeedConfig = serde_yaml::from_str(
            "id: s\nname: S\nsource:\n  type: s3\n  bucket: b\n  region: us-east-1\n  endpoint: http://127.0.0.1:9\n  credential: lab\nformat: ndjson\ntransform: t.py\n",
        )
        .unwrap();
        // The earlier definition's alert, open; then the id is born again.
        let born = now_ms();
        let earlier = db
            .query_json(
                "INSERT INTO alerts (feed_id, kind, message, created_at) VALUES ('s', 'credential', 'old', $1) RETURNING id",
                vec![crate::db::Param::Int(born - 1000)],
            )
            .unwrap()[0]["id"]
            .as_i64()
            .unwrap();
        db.query_json(
            "INSERT INTO source_births (feed_id, born_at) VALUES ('s', $1) RETURNING feed_id",
            vec![crate::db::Param::Int(born)],
        )
        .unwrap();
        let runner = FeedRunner::new(
            feed,
            db.clone(),
            Arc::new(FhirClient::with_credentials("http://127.0.0.1:9", None)),
            Arc::new(Alerter::new(db.clone(), None)),
            "python3",
            dir.path(),
            "http://127.0.0.1:9",
            "http://127.0.0.1:9",
        )
        .unwrap()
        .with_secrets(secrets.clone());

        runner.ensure_credential().await;
        let open = db.open_alert_ids("s", "credential").unwrap();
        assert_eq!(open.len(), 2, "its own beside the earlier one: {open:?}");
        assert_eq!(open[0], earlier);
        // A second pass raises no more.
        runner.ensure_credential().await;
        assert_eq!(db.open_alert_ids("s", "credential").unwrap().len(), 2);

        secrets
            .store()
            .unwrap()
            .create(NewSecret {
                name: "lab".into(),
                kind: SecretKind::Aws,
                value: value("AKIDFIRST"),
            })
            .await
            .unwrap();
        runner.ensure_credential().await;
        assert!(db.open_alert_ids("s", "credential").unwrap().is_empty());
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

#[cfg(test)]
mod share_tests {
    use super::{above, within};

    #[test]
    fn a_share_reads_within_the_limit_it_keeps() {
        assert_eq!(within(9.96, 10.0), "9.96");
        assert_eq!(within(10.0, 10.0), "10.0");
        assert_eq!(within(0.0, 10.0), "0.0");
        assert_eq!(within(0.04, 10.0), "0.04");
        assert_eq!(within(500.0 / 6.0, 90.0), "83.3");
        // Closer than six decimals: shown in full, still not above.
        let shown = within(9.999_999_9, 10.0);
        assert!(shown.parse::<f64>().unwrap() < 10.0, "{shown}");
    }

    #[test]
    fn a_share_reads_above_the_limit_it_exceeds() {
        assert_eq!(above(500.0 / 6.0, 10.0), "83.3");
        assert_eq!(above(100.0 / 81.0, 1.23), "1.235");
        assert_eq!(above(100.0, 99.99), "100.0");
        assert_eq!(above(9996.0 / 100.0, 99.95), "99.96");
        // Closer than six decimals: shown in full, still above.
        let shown = above(100.0 / 3.0, 33.333_333_3);
        assert!(shown.parse::<f64>().unwrap() > 33.333_333_3, "{shown}");
    }
}
