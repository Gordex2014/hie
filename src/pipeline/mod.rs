//! The pipeline: one runner per feed moves its data through, source -> stored
//! rows -> transform -> HFS. `mod.rs` holds the runner and its pass; each stage
//! is a file of its own: `ingest`, `transforming`, `outcomes`, `deliver` (REST), `submit`
//! (`$bulk-submit`) and `alerting`.

mod alerting;
mod deliver;
mod ingest;
mod outcomes;
mod submit;
mod transforming;

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::Result;

use crate::alerts::Alerter;
use crate::bulk::BulkSubmitter;
use crate::config::{DeliveryMethod, FeedConfig, Validation};
use crate::connectors::Source;
use crate::crypto::Keyring;
use crate::db::{Db, PassStatus, now_ms};
use crate::fhir::FhirClient;
use crate::versions::{ExecutorSettings, Versions};

/// Records transformed or checked per chunk.
const TRANSFORM_CHUNK: usize = 200;

/// How the manager drives a running feed: `poke` makes it pass now, `stopped` ends it.
#[derive(Default)]
pub struct RunnerControl {
    pub poke: tokio::sync::Notify,
    pub stopped: AtomicBool,
}

/// What every runner of the engine shares, handed to [`FeedRunner::new`]. A
/// new engine-wide dependency of the pipeline is a member here.
#[derive(Clone)]
pub struct Services {
    pub db: Arc<Db>,
    pub fhir: Arc<FhirClient>,
    pub alerts: Arc<Alerter>,
    /// Which version a load runs, and its executor.
    pub versions: Arc<Versions>,
    pub fhir_base_url: String,
    /// The URL HFS reaches this engine at, for `$bulk-submit` manifests.
    pub public_url: String,
    /// Where transformed submissions are written when the engine serves them.
    pub bulk_dir: PathBuf,
    /// Whether resources go through the destination's `$validate` first.
    pub validation: Validation,
    /// The keys failed rows' content is sealed with; `None` in tests that
    /// build a runner without an engine.
    pub content_keys: Option<Arc<Keyring>>,
}

/// What the pass being run took: the loads it opened and the rows they hold.
#[derive(Default)]
struct PassTally {
    loads: Vec<i64>,
    new_rows: i64,
}

/// Moves one feed's data through: source -> stored rows -> transform -> HFS.
/// Every row is in the database before its object leaves the source, so a crash or an
/// HFS outage never loses data, and every row keeps its object name and line number.
pub struct FeedRunner {
    pub feed: FeedConfig,
    source: Source,
    db: Arc<Db>,
    fhir: Arc<FhirClient>,
    bulk: Option<BulkSubmitter>,
    versions: Arc<Versions>,
    validation: Validation,
    #[allow(dead_code)]
    content_keys: Option<Arc<Keyring>>,
    alerts: Arc<Alerter>,
    started_at: i64,
    outage_alerted: AtomicBool,
    /// Which engine instance this runner acts as, for the source's lease.
    instance: String,
    /// Where transformed submissions are written when the engine serves them.
    bulk_dir: PathBuf,
    /// `$bulk-submit` over the engine's transformed NDJSON, not the source objects.
    bulk_transformed: bool,
    tally: Mutex<PassTally>,
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
    /// The runner of a configured source, over the engine's shared services.
    pub fn new(feed: FeedConfig, services: &Services) -> Result<Self> {
        let source = Source::from_config(&feed)?.with_db(services.db.clone());
        Ok(Self::from_services(feed, source, services))
    }

    /// A runner over a given source and the engine's shared services.
    pub fn from_services(feed: FeedConfig, source: Source, services: &Services) -> Self {
        let feed_bulk_transformed = feed.bulk_transformed();
        let bulk = (feed.delivery.method == DeliveryMethod::BulkSubmit).then(|| {
            BulkSubmitter::new(&services.fhir_base_url, &services.public_url)
                .with_credentials(services.fhir.credentials().clone())
        });
        Self {
            feed,
            source,
            db: services.db.clone(),
            fhir: services.fhir.clone(),
            bulk,
            versions: services.versions.clone(),
            validation: services.validation,
            content_keys: services.content_keys.clone(),
            alerts: services.alerts.clone(),
            started_at: now_ms(),
            instance: instance_id().to_string(),
            bulk_dir: services.bulk_dir.clone(),
            bulk_transformed: feed_bulk_transformed,
            outage_alerted: AtomicBool::new(false),
            tally: Mutex::new(PassTally::default()),
        }
    }

    /// Builds a runner over a given source without an engine around it, for
    /// tests: the transform runs on the local worker, validation is off.
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
        let transforms_dir = feed
            .transform
            .file()
            .and_then(Path::parent)
            .map(Path::to_path_buf)
            .unwrap_or_default();
        let versions = Arc::new(Versions::new(
            db.clone(),
            ExecutorSettings {
                python: python.to_string(),
                runtime_dir: runtime_dir.to_path_buf(),
                terminology: None,
                runtime: None,
            },
            &transforms_dir,
        ));
        let services = Services {
            db,
            fhir,
            alerts,
            versions,
            fhir_base_url: fhir_base_url.to_string(),
            public_url: public_url.to_string(),
            bulk_dir: std::env::temp_dir().join("hie-bulk"),
            validation: Validation::Off,
            content_keys: None,
        };
        Self::from_services(feed, source, &services)
    }

    pub fn source(&self) -> &Source {
        &self.source
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
        if let Err(e) = self.source.start().await {
            tracing::error!(feed = %self.feed.id, "{e:#}");
            self.alerts.raise(
                Some(&self.feed.id),
                "listener",
                &format!("{}: {e:#}", self.feed.name),
            )?;
        }
        // Every pass asked for until now is served by this one.
        let passes = self.db.start_passes(&self.feed.id)?;
        if self.db.is_paused(&self.feed.id)? {
            self.db
                .finish_passes(&passes, PassStatus::Skipped, Some("paused"), None, &[], 0)?;
            return Ok(true);
        }
        *self.tally.lock().expect("pass tally") = PassTally::default();
        let result = self.run_pass().await;
        let tally = std::mem::take(&mut *self.tally.lock().expect("pass tally"));
        let (status, error) = match &result {
            Ok(None) => (PassStatus::Done, None),
            // The reason stays in the log: a source's error may carry what
            // its connection used.
            Ok(Some(_)) | Err(_) => (
                PassStatus::Failed,
                Some("the source could not be read; the engine's log has the reason"),
            ),
        };
        self.db
            .finish_passes(&passes, status, None, error, &tally.loads, tally.new_rows)?;
        result.map(|_| true)
    }

    /// Counts a load the pass opened and the rows stored for it.
    pub(crate) fn tally_load(&self, load_id: i64, rows: usize) {
        let mut tally = self.tally.lock().expect("pass tally");
        if !tally.loads.contains(&load_id) {
            tally.loads.push(load_id);
        }
        tally.new_rows += rows as i64;
    }

    /// The feed's loop: a pass every poll interval, or sooner when poked; nothing
    /// while the source is paused or held by another instance; until stopped.
    pub async fn run(self: Arc<Self>, control: Arc<RunnerControl>) {
        tracing::info!(feed = %self.feed.id, source = %self.feed.source.describe(), instance = %self.instance, "watching");
        // A delivery attempt that came due runs without a pass over the source.
        let mut delivery_due = false;
        loop {
            if control.stopped.load(Ordering::Acquire) {
                if let Err(e) = self.db.release_lease(&self.feed.id, &self.instance) {
                    tracing::error!(feed = %self.feed.id, "{e:#}");
                }
                tracing::info!(feed = %self.feed.id, "stopped");
                return;
            }
            let result = if delivery_due {
                self.delivery_round().await
            } else {
                self.pass().await.map(drop)
            };
            if let Err(e) = result {
                tracing::error!(feed = %self.feed.id, "{e:#}");
            }
            let poll = self.feed.source.poll_interval();
            let due = self.next_delivery_in().filter(|d| *d < poll);
            delivery_due = false;
            tokio::select! {
                _ = tokio::time::sleep(poll) => {}
                _ = control.poke.notified() => {}
                _ = tokio::time::sleep(due.unwrap_or(poll)), if due.is_some() => {
                    delivery_due = true;
                }
            }
        }
    }

    /// One pass of the whole pipeline. Each step picks up whatever the previous
    /// passes left, so a step that fails is simply retried on the next pass.
    pub async fn tick(&self) -> Result<()> {
        self.run_pass().await.map(drop)
    }

    /// [`FeedRunner::tick`], answering the error that kept the source from
    /// being read, if any (the rest of the pass still ran).
    async fn run_pass(&self) -> Result<Option<anyhow::Error>> {
        // A source that cannot be read this pass must not hold back what is
        // already in the store: the rest of the pass runs, the scan is retried next.
        let unread = match self.scan().await {
            Ok(()) => None,
            Err(e) => {
                tracing::error!(feed = %self.feed.id, "scan failed, will retry next pass: {e:#}");
                Some(e)
            }
        };
        if self.bulk.is_some() {
            if self.bulk_transformed {
                self.transform_received(None).await?;
            } else {
                self.preflight_received()?;
            }
            self.submit_pending().await?;
            self.poll_submissions().await?;
        } else {
            self.transform_and_deliver().await?;
        }
        self.finish()?;
        self.check_silence()?;
        Ok(unread)
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
