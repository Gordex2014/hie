//! Sources and transforms as resources: created, replaced, paused and tested
//! through the API, persisted as the files `config::load` reads, and run without
//! a restart.

use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use serde_json::{Value, json};

use crate::alerts::Alerter;
use crate::config::{Config, FeedConfig, SourceConfig};
use crate::db::{Db, Rule};
use crate::fhir::FhirClient;
use crate::pipeline::{FeedRunner, RunnerControl};
use crate::secrets::{SecretError, SecretKind, Secrets};
use crate::source::{Probe, Source};
use crate::transform::{Outcome, Transformer, check_resource};
use crate::worker::WorkerLimits;

/// Transform tries that may run at once, each with its own worker process.
const TRY_CONCURRENCY: usize = 4;
/// How long a try waits for its turn before it is turned away.
const TRY_WAIT: Duration = Duration::from_secs(10);
/// Beyond the longest single step (one record, or one HTTP call to FHIR), how
/// long a replaced runner has to store its outcome and stop before it is aborted.
const HANDOVER_SLACK: Duration = Duration::from_secs(30);

#[derive(Debug)]
pub enum ManagerError {
    Invalid(String),
    /// A source definition that cannot run as submitted: its shape, its paths,
    /// or the credential it names.
    InvalidDefinition(String),
    NotFound(String),
    Conflict(String),
    /// Too much running already; the same request may succeed later.
    Busy(String),
    /// The store's write lock was not free; the same request may succeed later.
    StoreBusy,
    /// The definition was saved, but its source did not start.
    NotStarted(String),
    Internal(anyhow::Error),
}

impl ManagerError {
    /// A secret store error met while checking or registering a credential.
    fn from_secret(source: &str, reference: &str, e: SecretError) -> Self {
        match e {
            SecretError::Unavailable(reason) => ManagerError::InvalidDefinition(format!(
                "source {source}: credential {reference}: {reason}"
            )),
            SecretError::NotFound => ManagerError::InvalidDefinition(format!(
                "source {source}: no credential named {reference}; store it first with POST /api/secrets"
            )),
            SecretError::Busy => ManagerError::StoreBusy,
            other => ManagerError::Internal(anyhow::Error::new(other)),
        }
    }
}

impl From<anyhow::Error> for ManagerError {
    fn from(e: anyhow::Error) -> Self {
        ManagerError::Internal(e)
    }
}

struct Managed {
    feed: FeedConfig,
    runner: Arc<FeedRunner>,
    control: Arc<RunnerControl>,
}

/// A runner task of a source that may not have finished yet.
#[derive(Clone)]
struct RunnerTask {
    /// Closed when the task ends, aborted or not: its sender lives in the task.
    done: tokio::sync::watch::Receiver<()>,
    abort: tokio::task::AbortHandle,
}

impl RunnerTask {
    fn finished(&self) -> bool {
        self.done.has_changed().is_err()
    }
}

/// Waits until every runner in `predecessors` has ended. After each `grace`, the
/// ones still running are aborted and waited for again: an aborted task ends at
/// its next await, so only one blocked inside a synchronous call stays, and the
/// warning repeats while it does.
async fn hand_over(id: &str, mut predecessors: Vec<RunnerTask>, grace: Duration) {
    predecessors.retain(|p| !p.finished());
    if predecessors.is_empty() {
        return;
    }
    async fn all_ended(tasks: &mut [RunnerTask]) {
        for task in tasks {
            // Nothing is ever sent: `changed` returns only once the task has ended.
            while task.done.changed().await.is_ok() {}
        }
    }
    tracing::info!(feed = %id, "waiting for the previous runner to stop");
    while tokio::time::timeout(grace, all_ended(&mut predecessors))
        .await
        .is_err()
    {
        tracing::warn!(feed = %id, "the previous runner did not stop within {}; aborting it", crate::pipeline::human(grace));
        for p in predecessors.iter().filter(|p| !p.finished()) {
            p.abort.abort();
        }
    }
}

pub struct SourceManager {
    python: String,
    runtime_dir: PathBuf,
    fhir_base_url: String,
    public_url: String,
    sources_dir: PathBuf,
    transforms_dir: PathBuf,
    base_dir: PathBuf,
    bulk_dir: PathBuf,
    terminology_url: Option<String>,
    worker_limits: WorkerLimits,
    tries: tokio::sync::Semaphore,
    db: Arc<Db>,
    fhir: Arc<FhirClient>,
    alerts: Arc<Alerter>,
    /// Connector credentials, or why there are none.
    secrets: Secrets,
    sources: Mutex<BTreeMap<String, Managed>>,
    /// Per source id, in the order they were started, the runner tasks that may
    /// still run, deleted sources included. Locked only inside `sources`.
    runners: Mutex<HashMap<String, Vec<RunnerTask>>>,
    handover_grace_ms: AtomicU64,
    /// Per source id, held across a change of the source: checking its
    /// credential, registering it, publishing the definition, starting it, and
    /// cleaning up the registrations it no longer needs.
    locks: Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>,
    /// What a test runs at the manager's and its runners' pause points.
    #[cfg(all(test, feature = "secrets-native"))]
    pause: Mutex<Option<crate::AsyncPause>>,
    /// Makes the next start after a definition is published fail, for tests.
    #[cfg(all(test, feature = "secrets-native"))]
    fail_next_start: std::sync::atomic::AtomicBool,
    /// Makes the next definition write fail, for tests.
    #[cfg(all(test, feature = "secrets-native"))]
    fail_next_write: std::sync::atomic::AtomicBool,
}

impl SourceManager {
    pub fn new(
        config: &Config,
        runtime_dir: &Path,
        db: Arc<Db>,
        fhir: Arc<FhirClient>,
        alerts: Arc<Alerter>,
        secrets: Secrets,
    ) -> Arc<Self> {
        Arc::new(Self {
            python: config.python.clone(),
            runtime_dir: runtime_dir.to_path_buf(),
            fhir_base_url: config.fhir_base_url.clone(),
            public_url: config.public_url.clone(),
            sources_dir: config.sources_dir.clone(),
            transforms_dir: config.transforms_dir.clone(),
            base_dir: config.base_dir.clone(),
            bulk_dir: config.bulk_dir(),
            terminology_url: config.terminology_url.clone(),
            worker_limits: config.worker.limits(),
            tries: tokio::sync::Semaphore::new(TRY_CONCURRENCY),
            db,
            fhir,
            alerts,
            secrets,
            sources: Mutex::new(BTreeMap::new()),
            runners: Mutex::new(HashMap::new()),
            locks: Mutex::new(HashMap::new()),
            #[cfg(all(test, feature = "secrets-native"))]
            pause: Mutex::new(None),
            #[cfg(all(test, feature = "secrets-native"))]
            fail_next_start: std::sync::atomic::AtomicBool::new(false),
            #[cfg(all(test, feature = "secrets-native"))]
            fail_next_write: std::sync::atomic::AtomicBool::new(false),
            handover_grace_ms: AtomicU64::new(
                (config
                    .worker
                    .limits()
                    .request
                    .max(crate::fhir::HTTP_TIMEOUT)
                    + HANDOVER_SLACK)
                    .as_millis() as u64,
            ),
        })
    }

    /// How long a replacement waits for the runner it replaces before aborting it.
    pub fn set_handover_grace(&self, grace: Duration) {
        self.handover_grace_ms
            .store(grace.as_millis() as u64, Ordering::Relaxed);
    }

    /// Source ids that still have runners: the live sources, and deleted ones
    /// whose runner has not stopped yet. Also forgets the runners that have
    /// finished.
    pub fn runner_slots(&self) -> usize {
        let sources = self.sources.lock().expect("manager mutex");
        let mut runners = self.runners.lock().expect("manager mutex");
        Self::reap(&mut runners, &sources);
        runners.len()
    }

    /// The runners of a source that have not finished: more than one while a
    /// replacement waits for the runner it replaces. Also forgets the runners
    /// that have finished.
    pub fn unfinished_runners(&self, id: &str) -> usize {
        let sources = self.sources.lock().expect("manager mutex");
        let mut runners = self.runners.lock().expect("manager mutex");
        Self::reap(&mut runners, &sources);
        runners.get(id).map_or(0, Vec::len)
    }

    /// Forgets the runners that have finished, and the ids of deleted sources
    /// that have none left.
    fn reap(runners: &mut HashMap<String, Vec<RunnerTask>>, sources: &BTreeMap<String, Managed>) {
        runners.retain(|id, tasks| {
            tasks.retain(|t| !t.finished());
            !tasks.is_empty() || sources.contains_key(id)
        });
    }

    /// The lock a change of source `id` holds.
    fn source_lock(&self, id: &str) -> Arc<tokio::sync::Mutex<()>> {
        self.locks
            .lock()
            .expect("manager mutex")
            .entry(id.to_string())
            .or_default()
            .clone()
    }

    #[cfg(all(test, feature = "secrets-native"))]
    pub(crate) fn on_pause(&self, hook: crate::AsyncPause) {
        *self.pause.lock().expect("pause mutex") = Some(hook);
    }

    #[cfg(all(test, feature = "secrets-native"))]
    async fn pause_at(&self, name: &'static str) {
        let hook = self.pause.lock().expect("pause mutex").clone();
        if let Some(hook) = hook {
            hook(name).await;
        }
    }

    /// Builds and spawns the runner for a resolved definition, replacing a running one.
    pub fn start(self: &Arc<Self>, feed: FeedConfig) -> Result<()> {
        // Files recorded before origins were: decided once, here, before
        // the runner can hand any of them over. A stored origin never follows
        // later edits of the definition.
        self.db.backfill_file_origins(
            &feed.id,
            matches!(
                feed.source,
                SourceConfig::Folder { .. } | SourceConfig::Mllp { .. }
            ),
            &self.bulk_dir,
        )?;
        let runner = FeedRunner::new(
            feed.clone(),
            self.db.clone(),
            self.fhir.clone(),
            self.alerts.clone(),
            &self.python,
            &self.runtime_dir,
            &self.fhir_base_url,
            &self.public_url,
        )?
        .with_bulk_dir(&self.bulk_dir)
        .with_terminology(self.terminology_url.as_deref())
        .with_worker_limits(self.worker_limits.clone())
        .with_secrets(self.secrets.clone());
        #[cfg(all(test, feature = "secrets-native"))]
        let runner = runner.with_pause(self.pause.lock().expect("pause mutex").clone());
        let runner = Arc::new(runner);
        let control = Arc::new(RunnerControl::default());
        let id = feed.id.clone();
        let grace = Duration::from_millis(self.handover_grace_ms.load(Ordering::Relaxed));
        let mut sources = self.sources.lock().expect("manager mutex");
        let previous = sources.insert(
            id.clone(),
            Managed {
                feed,
                runner: runner.clone(),
                control: control.clone(),
            },
        );
        if let Some(p) = previous {
            Self::halt(&p);
        }
        let mut runners = self.runners.lock().expect("manager mutex");
        Self::reap(&mut runners, &sources);
        let tasks = runners.entry(id.clone()).or_default();
        // The call returns at once. The runner starts when every runner of the
        // source started before it has ended: after its current record, or
        // aborted after `grace`. Spawned here, so the order is the order of `start`.
        let predecessors = tasks.clone();
        let (done, ended) = tokio::sync::watch::channel(());
        let manager = Arc::downgrade(self);
        let task = tokio::spawn(async move {
            let _done = done;
            hand_over(&id, predecessors, grace).await;
            // Every earlier runner has ended: the registrations the current
            // definition does not need can go.
            if let Some(manager) = manager.upgrade() {
                manager.cleanup_refs(&id).await;
            }
            runner.run(control).await;
        });
        tasks.push(RunnerTask {
            done: ended,
            abort: task.abort_handle(),
        });
        Ok(())
    }

    fn halt(managed: &Managed) {
        managed.control.stopped.store(true, Ordering::Release);
        managed.control.poke.notify_one();
    }

    pub fn start_all(self: &Arc<Self>, feeds: Vec<FeedConfig>) -> Result<()> {
        // Before any runner: what an earlier run of this instance left behind.
        self.reconcile_refs(&feeds);
        for feed in feeds {
            self.ensure_default_rule(&feed, None)?;
            self.start(feed)?;
        }
        Ok(())
    }

    pub fn sources(&self) -> Vec<FeedConfig> {
        self.sources
            .lock()
            .expect("manager mutex")
            .values()
            .map(|m| m.feed.clone())
            .collect()
    }

    pub fn source(&self, id: &str) -> Option<FeedConfig> {
        self.sources
            .lock()
            .expect("manager mutex")
            .get(id)
            .map(|m| m.feed.clone())
    }

    pub fn runner(&self, id: &str) -> Option<Arc<FeedRunner>> {
        self.sources
            .lock()
            .expect("manager mutex")
            .get(id)
            .map(|m| m.runner.clone())
    }

    fn file_for(&self, id: &str) -> PathBuf {
        self.sources_dir.join(format!("{id}.yaml"))
    }

    /// The source's definition as its file holds it: paths as written, nothing resolved.
    pub fn definition(&self, id: &str) -> Result<Option<Value>> {
        let path = self.file_for(id);
        if !path.exists() {
            return Ok(None);
        }
        let text = std::fs::read_to_string(&path)
            .with_context(|| format!("reading {}", path.display()))?;
        let yaml: serde_yaml::Value = serde_yaml::from_str(&text)?;
        Ok(Some(serde_json::to_value(yaml)?))
    }

    /// Parses a submitted definition, validates it against the base directory and
    /// returns both the form to write (as submitted) and the resolved form to run.
    fn parse(&self, body: &Value) -> Result<(FeedConfig, FeedConfig), ManagerError> {
        let submitted: FeedConfig = serde_json::from_value(body.clone())
            .map_err(|e| ManagerError::InvalidDefinition(e.to_string()))?;
        let mut resolved = submitted.clone();
        resolved
            .resolve(&self.base_dir)
            .map_err(|e| ManagerError::InvalidDefinition(format!("{e:#}")))?;
        Source::from_config(&resolved)
            .map_err(|e| ManagerError::InvalidDefinition(format!("{e:#}")))?;
        Ok((submitted, resolved))
    }

    /// The id of the stored credential a definition names, after checking it
    /// exists and fits an S3 source; `None` when it names none.
    async fn credential_of(&self, feed: &FeedConfig) -> Result<Option<String>, ManagerError> {
        let Some(reference) = feed.source.credential() else {
            return Ok(None);
        };
        let store = match self.secrets.store() {
            Ok(store) => store,
            Err(_) => {
                self.secrets.recheck_once().await;
                let e = self.secrets.store().err().unwrap_or(SecretError::NotFound);
                return Err(ManagerError::from_secret(&feed.id, reference, e));
            }
        };
        let meta = store
            .find(reference)
            .await
            .map_err(|e| ManagerError::from_secret(&feed.id, reference, e))?
            .ok_or_else(|| ManagerError::from_secret(&feed.id, reference, SecretError::NotFound))?;
        match meta.kind {
            SecretKind::Aws => Ok(Some(meta.id)),
        }
    }

    /// Registers source `id`, as this instance, for the credential `secret_id`:
    /// `true` when this call made the registration, `false` when it existed.
    fn hold(&self, feed: &FeedConfig, secret_id: &str) -> Result<bool, ManagerError> {
        #[cfg(feature = "secrets-native")]
        {
            self.db
                .secret_hold(secret_id, &feed.id, self.secrets.owner())
                .map_err(|e| {
                    ManagerError::from_secret(
                        &feed.id,
                        feed.source.credential().unwrap_or(secret_id),
                        e,
                    )
                })
        }
        #[cfg(not(feature = "secrets-native"))]
        {
            let _ = (feed, secret_id);
            Ok(false)
        }
    }

    /// Takes back the registration a change made, when the change then failed
    /// before publishing its definition. Any registration that existed before
    /// the change stays: an earlier runner may still be using it, and only the
    /// cleanup after its handover, or the next start, may release it.
    fn take_back(&self, id: &str, secret: Option<&str>, made: bool) {
        #[cfg(feature = "secrets-native")]
        if let (Some(secret), true) = (secret, made)
            && let Err(e) = self.db.secret_unhold(secret, id, self.secrets.owner())
        {
            tracing::warn!(feed = %id, "a registration stays until the next start: {e}");
        }
        #[cfg(not(feature = "secrets-native"))]
        let _ = (id, secret, made);
    }

    /// Removes this instance's registrations of source `id`, except `keep`.
    #[cfg(feature = "secrets-native")]
    fn release(&self, id: &str, keep: Option<&str>) -> Result<(), SecretError> {
        self.db.secret_release(id, self.secrets.owner(), keep)
    }

    /// The default rule and the runner of a definition that was just published:
    /// a failure here leaves the definition saved and the registrations kept.
    fn start_published(
        self: &Arc<Self>,
        feed: &FeedConfig,
        by: Option<&str>,
    ) -> Result<(), ManagerError> {
        #[cfg(all(test, feature = "secrets-native"))]
        if self
            .fail_next_start
            .swap(false, std::sync::atomic::Ordering::SeqCst)
        {
            return Err(ManagerError::NotStarted(
                "the definition was saved, but the source could not be started: a test made the start fail; it starts with this definition at the next start, or on replace".into(),
            ));
        }
        self.ensure_default_rule(feed, by)
            .and_then(|()| self.start(feed.clone()))
            .map_err(|e| {
                tracing::error!(feed = %feed.id, "the definition was saved, but the source did not start: {e:#}");
                ManagerError::NotStarted(format!(
                    "the definition was saved, but the source could not be started: {e:#}; it starts with this definition at the next start, or on replace"
                ))
            })
    }

    /// Publishes the definition all at once: written to a temporary file of its
    /// own, then renamed over the source's file, so a failed write leaves the
    /// previous definition in place. `config::load` reads only `*.yaml` and
    /// `*.yml`, never the temporary file.
    fn write(&self, submitted: &FeedConfig) -> Result<()> {
        use std::io::Write;
        #[cfg(all(test, feature = "secrets-native"))]
        if self
            .fail_next_write
            .swap(false, std::sync::atomic::Ordering::SeqCst)
        {
            anyhow::bail!("writing {}: a test made the write fail", submitted.id);
        }
        std::fs::create_dir_all(&self.sources_dir)?;
        let path = self.file_for(&submitted.id);
        let yaml = submitted.to_yaml()?;
        let temp = self.sources_dir.join(format!(
            ".{}.yaml.{:08x}.tmp",
            submitted.id,
            rand::random::<u32>()
        ));
        let written = std::fs::File::create_new(&temp).and_then(|mut file| {
            file.write_all(yaml.as_bytes())?;
            file.sync_all()?;
            std::fs::rename(&temp, &path)
        });
        if written.is_err() {
            let _ = std::fs::remove_file(&temp);
        }
        written.with_context(|| format!("writing {}", path.display()))
    }

    pub async fn create(
        self: &Arc<Self>,
        body: &Value,
        by: Option<&str>,
    ) -> Result<FeedConfig, ManagerError> {
        let (submitted, resolved) = self.parse(body)?;
        let lock = self.source_lock(&submitted.id);
        let _changing = lock.lock().await;
        if self.source(&submitted.id).is_some() || self.file_for(&submitted.id).exists() {
            return Err(ManagerError::Conflict(format!(
                "source {} already exists",
                submitted.id
            )));
        }
        let secret = self.credential_of(&resolved).await?;
        pause_point!(await self, "manager:after-find");
        // Nothing is published and nothing released when this fails.
        let made = match &secret {
            Some(secret) => self.hold(&resolved, secret)?,
            None => false,
        };
        pause_point!(await self, "manager:after-hold");
        if let Err(e) = self.write(&submitted) {
            self.take_back(&submitted.id, secret.as_deref(), made);
            return Err(e.into());
        }
        pause_point!(await self, "manager:after-publish");
        self.start_published(&resolved, by)?;
        Ok(resolved)
    }

    /// Every source is born with one silence rule, so nobody has to remember to
    /// watch it: `expect_every` when the definition says so, two hours otherwise.
    fn ensure_default_rule(&self, feed: &FeedConfig, by: Option<&str>) -> Result<()> {
        if self.db.has_rule(&feed.id, "silence")? {
            return Ok(());
        }
        let rule = Rule {
            id: 0,
            source_id: Some(feed.id.clone()),
            condition: "silence".into(),
            threshold: feed
                .expect_every_text
                .clone()
                .unwrap_or_else(|| "2h".into()),
            notify: vec![],
            channels: vec!["app".into(), "webhook".into()],
            enabled: true,
            created_by: by.map(str::to_string),
        };
        self.db.insert_rule(&rule).map(drop)
    }

    pub async fn replace(
        self: &Arc<Self>,
        id: &str,
        body: &Value,
    ) -> Result<FeedConfig, ManagerError> {
        if self.source(id).is_none() {
            return Err(ManagerError::NotFound(format!("source {id} not found")));
        }
        let (submitted, resolved) = self.parse(body)?;
        if submitted.id != id {
            return Err(ManagerError::Invalid(format!(
                "the body's id {:?} does not match {id:?}",
                submitted.id
            )));
        }
        let lock = self.source_lock(id);
        let _changing = lock.lock().await;
        let secret = self.credential_of(&resolved).await?;
        pause_point!(await self, "manager:after-find");
        // The old registrations and definition stay untouched when this fails.
        let made = match &secret {
            Some(secret) => self.hold(&resolved, secret)?,
            None => false,
        };
        pause_point!(await self, "manager:after-hold");
        if let Err(e) = self.write(&submitted) {
            self.take_back(id, secret.as_deref(), made);
            return Err(e.into());
        }
        pause_point!(await self, "manager:after-publish");
        // The old credential's registration goes once the new runner has taken
        // over (`cleanup_refs`).
        self.start_published(&resolved, None)?;
        Ok(resolved)
    }

    /// The definition of `id` as its file holds it, resolved, found as `load`
    /// finds it (any `*.yaml` or `*.yml` whose body has that id): what the source
    /// runs once a start succeeds, which may differ from the running one after
    /// a start that failed. `None` once the file is gone.
    fn published(&self, id: &str) -> Result<Option<FeedConfig>> {
        crate::config::find_source(&self.sources_dir, &self.base_dir, id)
            .with_context(|| format!("reading the definition of {id}"))
    }

    /// Stops the source and removes its definition. Its records and alerts stay.
    /// Its registrations go once its runner has ended.
    pub async fn delete(self: &Arc<Self>, id: &str) -> Result<(), ManagerError> {
        let lock = self.source_lock(id);
        let _changing = lock.lock().await;
        let (removed, halted) = {
            let mut sources = self.sources.lock().expect("manager mutex");
            let removed = sources.remove(id);
            let mut runners = self.runners.lock().expect("manager mutex");
            Self::reap(&mut runners, &sources);
            let halted = runners.get(id).cloned().unwrap_or_default();
            (removed, halted)
        };
        let Some(managed) = removed else {
            return Err(ManagerError::NotFound(format!("source {id} not found")));
        };
        Self::halt(&managed);
        let path = self.file_for(id);
        if path.exists() {
            std::fs::remove_file(&path).with_context(|| format!("removing {}", path.display()))?;
        }
        let manager = Arc::downgrade(self);
        let grace = Duration::from_millis(self.handover_grace_ms.load(Ordering::Relaxed));
        let id = id.to_string();
        tokio::spawn(async move {
            hand_over(&id, halted, grace).await;
            if let Some(manager) = manager.upgrade() {
                manager.cleanup_refs(&id).await;
            }
        });
        Ok(())
    }

    /// Removes this instance's registrations of source `id` that its current
    /// definition does not need: all of them once it is deleted. Runs only when
    /// no earlier runner of the source is left, so nothing still signs with a
    /// credential it releases. While the published definition and the running
    /// one name different credentials (a start that failed after publishing),
    /// nothing is released: both stay until a start of the published one
    /// succeeds. Retried a few times; what is left is cleaned at the next start.
    async fn cleanup_refs(&self, id: &str) {
        #[cfg(feature = "secrets-native")]
        for attempt in 0..3u32 {
            if attempt > 0 {
                tokio::time::sleep(Duration::from_secs(2u64.pow(attempt))).await;
            }
            let lock = self.source_lock(id);
            let _changing = lock.lock().await;
            pause_point!(await self, "manager:cleanup:before");
            let running = self.source(id);
            let published = match self.published(id) {
                Ok(published) => published,
                Err(e) => {
                    tracing::warn!(feed = %id, "credential registrations kept: {e:#}");
                    return;
                }
            };
            if running.as_ref().map(|f| f.source.credential())
                != published.as_ref().map(|f| f.source.credential())
            {
                tracing::info!(feed = %id, "credential registrations kept until the saved definition starts");
                return;
            }
            let keep = match running {
                None => None,
                Some(feed) => match feed.source.credential() {
                    None => None,
                    Some(reference) => match self.db.secret_id_for(reference) {
                        Ok(keep) => keep,
                        Err(e) => {
                            tracing::warn!(feed = %id, "cleaning up credential registrations: {e}");
                            continue;
                        }
                    },
                },
            };
            match self.release(id, keep.as_deref()) {
                Ok(()) => return,
                Err(e) => {
                    tracing::warn!(feed = %id, "cleaning up credential registrations: {e}")
                }
            }
        }
        #[cfg(not(feature = "secrets-native"))]
        let _ = id;
    }

    /// At start, before any runner: makes this instance's registrations match the
    /// definitions it loaded, as a crash may have left them (a registration
    /// without a published file, a file removed without its cleanup, a replace
    /// whose old registration was not released yet). Other instances' rows are
    /// never touched; a failure leaves a row, which only keeps a secret.
    fn reconcile_refs(&self, feeds: &[FeedConfig]) {
        #[cfg(feature = "secrets-native")]
        {
            let owner = self.secrets.owner();
            if owner.is_empty() {
                return;
            }
            let loaded: std::collections::HashSet<&str> =
                feeds.iter().map(|f| f.id.as_str()).collect();
            for feed in feeds {
                let keep = match feed.source.credential() {
                    None => None,
                    Some(reference) => match self.db.secret_id_for(reference) {
                        Ok(keep) => keep,
                        Err(e) => {
                            tracing::warn!(feed = %feed.id, "reconciling credential registrations: {e}");
                            continue;
                        }
                    },
                };
                if let Some(secret) = &keep
                    && let Err(e) = self.db.secret_hold(secret, &feed.id, owner)
                {
                    tracing::warn!(feed = %feed.id, "reconciling credential registrations: {e}");
                    continue;
                }
                if let Err(e) = self.db.secret_release(&feed.id, owner, keep.as_deref()) {
                    tracing::warn!(feed = %feed.id, "reconciling credential registrations: {e}");
                }
            }
            match self.db.secret_holds_of(owner) {
                Ok(holds) => {
                    for (source, _) in holds {
                        if !loaded.contains(source.as_str())
                            && let Err(e) = self.db.secret_release(&source, owner, None)
                        {
                            tracing::warn!(feed = %source, "reconciling credential registrations: {e}");
                        }
                    }
                }
                Err(e) => tracing::warn!("reconciling credential registrations: {e}"),
            }
        }
        #[cfg(not(feature = "secrets-native"))]
        let _ = feeds;
    }

    pub fn pause(&self, id: &str, by: Option<&str>) -> Result<(), ManagerError> {
        self.require(id)?;
        self.db.set_paused(id, true, by)?;
        Ok(())
    }

    pub fn resume(&self, id: &str) -> Result<(), ManagerError> {
        self.require(id)?;
        self.db.set_paused(id, false, None)?;
        self.check_now(id)
    }

    /// Makes the source pass now instead of at its next poll.
    pub fn check_now(&self, id: &str) -> Result<(), ManagerError> {
        let sources = self.sources.lock().expect("manager mutex");
        let managed = sources
            .get(id)
            .ok_or_else(|| ManagerError::NotFound(format!("source {id} not found")))?;
        managed.control.poke.notify_one();
        Ok(())
    }

    fn require(&self, id: &str) -> Result<(), ManagerError> {
        if self.source(id).is_none() {
            return Err(ManagerError::NotFound(format!("source {id} not found")));
        }
        Ok(())
    }

    /// Tests a connection before a source exists: the body is a full definition or
    /// just `{"source": {...}}`. Nothing is taken from the source.
    pub async fn test(&self, body: &Value) -> Result<Probe, ManagerError> {
        let source_value = body.get("source").unwrap_or(body);
        let mut source: SourceConfig = serde_json::from_value(source_value.clone())
            .map_err(|e| ManagerError::InvalidDefinition(e.to_string()))?;
        source.resolve(&self.base_dir);
        source
            .check()
            .map_err(|e| ManagerError::InvalidDefinition(format!("source test: {e:#}")))?;
        let feed = FeedConfig {
            id: "test".into(),
            name: "test".into(),
            source,
            format: Default::default(),
            transform: PathBuf::new(),
            delivery: Default::default(),
            expect_every_text: None,
            expect_every: None,
        };
        // The credential's current revision, revealed for this probe only: a
        // test registers nothing.
        let revealed = match feed.source.credential() {
            None => None,
            Some(reference) => {
                let store = match self.secrets.store() {
                    Ok(store) => store,
                    Err(_) => {
                        self.secrets.recheck_once().await;
                        let e = self.secrets.store().err().unwrap_or(SecretError::NotFound);
                        return Err(ManagerError::from_secret("test", reference, e));
                    }
                };
                Some(
                    store
                        .reveal(reference)
                        .await
                        .map_err(|e| ManagerError::from_secret("test", reference, e))?,
                )
            }
        };
        let source = Source::from_config_with(&feed, revealed.as_ref())
            .map_err(|e| ManagerError::InvalidDefinition(format!("{e:#}")))?;
        match source.probe().await {
            Ok(probe) => Ok(probe),
            Err(e) => Ok(Probe {
                reachable: false,
                elapsed_ms: 0,
                objects: 0,
                newest: None,
                newest_at: Some(format!("{e:#}")),
            }),
        }
    }

    // ---- transforms --------------------------------------------------------

    fn transform_path(&self, name: &str) -> Result<PathBuf, ManagerError> {
        if name.is_empty()
            || !name
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
        {
            return Err(ManagerError::Invalid(format!(
                "transform name {name:?} must be letters, digits, underscores and dashes"
            )));
        }
        Ok(self.transforms_dir.join(format!("{name}.py")))
    }

    fn transform_name(path: &Path) -> Option<String> {
        (path.extension().and_then(|e| e.to_str()) == Some("py"))
            .then(|| path.file_stem()?.to_str().map(str::to_string))
            .flatten()
    }

    /// Every transform file, with the sources that use it.
    pub fn transforms(&self) -> Result<Vec<Value>> {
        std::fs::create_dir_all(&self.transforms_dir)?;
        let sources = self.sources();
        let mut entries: Vec<PathBuf> = std::fs::read_dir(&self.transforms_dir)?
            .filter_map(|e| e.ok().map(|e| e.path()))
            .filter(|p| p.is_file() && Self::transform_name(p).is_some())
            .collect();
        entries.sort();
        let mut out = Vec::new();
        for path in entries {
            let name = Self::transform_name(&path).unwrap_or_default();
            let meta = std::fs::metadata(&path)?;
            let modified = meta
                .modified()
                .ok()
                .map(|m| DateTime::<Utc>::from(m).to_rfc3339());
            let canonical = path.canonicalize().unwrap_or(path.clone());
            let used_by: Vec<&str> = sources
                .iter()
                .filter(|f| f.transform.canonicalize().ok().as_ref() == Some(&canonical))
                .map(|f| f.id.as_str())
                .collect();
            let relative = path
                .strip_prefix(&self.base_dir)
                .unwrap_or(&path)
                .to_string_lossy()
                .replace('\\', "/");
            out.push(json!({
                "name": name,
                "path": relative,
                "size": meta.len(),
                "modified_at": modified,
                "used_by": used_by,
            }));
        }
        Ok(out)
    }

    pub fn transform_code(&self, name: &str) -> Result<Option<String>, ManagerError> {
        let path = self.transform_path(name)?;
        if !path.is_file() {
            return Ok(None);
        }
        Ok(Some(
            std::fs::read_to_string(&path)
                .with_context(|| format!("reading {}", path.display()))?,
        ))
    }

    /// Writes the transform's code and restarts the sources that use it, so their
    /// Python workers load the new version.
    ///
    /// Each source restarts under its own lock with the definition its file
    /// holds, so a definition saved by a change whose start failed is the one
    /// that runs, not the older one still running.
    pub async fn save_transform(
        self: &Arc<Self>,
        name: &str,
        code: &str,
    ) -> Result<bool, ManagerError> {
        let path = self.transform_path(name)?;
        std::fs::create_dir_all(&self.transforms_dir).map_err(anyhow::Error::from)?;
        let existed = path.exists();
        std::fs::write(&path, code).with_context(|| format!("writing {}", path.display()))?;
        let canonical = path.canonicalize().unwrap_or(path);
        for running in self.sources() {
            if running.transform.canonicalize().ok().as_ref() != Some(&canonical) {
                continue;
            }
            let lock = self.source_lock(&running.id);
            let _changing = lock.lock().await;
            if self.source(&running.id).is_none() {
                continue; // deleted meanwhile
            }
            let feed = match self.published(&running.id)? {
                Some(feed) => feed,
                None => continue,
            };
            self.start(feed)?;
        }
        Ok(existed)
    }

    /// Runs a transform on supplied records without loading anything: what each
    /// record became, or why it was rejected.
    pub async fn try_transform(
        &self,
        name: &str,
        records: &[Value],
    ) -> Result<Vec<Value>, ManagerError> {
        let path = self.transform_path(name)?;
        if !path.is_file() {
            return Err(ManagerError::NotFound(format!(
                "transform {name} not found"
            )));
        }
        let _permit = tokio::time::timeout(TRY_WAIT, self.tries.acquire())
            .await
            .map_err(|_| ManagerError::Busy("too many transform tries at once, try again".into()))?
            .map_err(|e| ManagerError::Internal(e.into()))?;
        let mut transformer = Transformer::new(&self.python, &self.runtime_dir, &path);
        transformer.set_terminology(self.terminology_url.as_deref());
        transformer.set_limits(self.worker_limits.clone());
        let items: Vec<(Value, Value)> = records
            .iter()
            .enumerate()
            .map(|(i, record)| {
                (
                    record.clone(),
                    json!({"feed": "try", "file": "try", "line": i + 1}),
                )
            })
            .collect();
        let outcomes = transformer.transform_many(&items).await;
        transformer.close().await;
        let outcomes = outcomes
            .map_err(|e| ManagerError::Invalid(format!("transform failed to run: {e:#}")))?;
        let mut out = Vec::with_capacity(records.len());
        for (record, outcome) in records.iter().zip(outcomes) {
            out.push(match outcome {
                Outcome::Rejected { kind, message } => {
                    json!({"record": record, "error": {"kind": kind, "message": message}})
                }
                Outcome::Transient(message) => {
                    json!({"record": record, "error": {"kind": "transient", "message": message}})
                }
                Outcome::Resources(resources) => {
                    let problem = resources.iter().find_map(|r| check_resource(r).err());
                    match problem {
                        Some(message) => json!({"record": record, "resources": resources,
                            "error": {"kind": "validation", "message": message}}),
                        None => json!({"record": record, "resources": resources}),
                    }
                }
            });
        }
        Ok(out)
    }
}

#[cfg(all(test, feature = "secrets-native"))]
mod reference_tests;
