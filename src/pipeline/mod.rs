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
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use anyhow::Result;

use crate::alerts::Alerter;
use crate::bulk::BulkSubmitter;
use crate::config::{DeliveryMethod, FeedConfig};
use crate::connectors::Source;
use crate::db::{Db, now_ms};
use crate::fhir::FhirClient;
use crate::runtime::RuntimeSettings;
use crate::transform::Transformer;

/// Records transformed or checked per chunk.
const TRANSFORM_CHUNK: usize = 200;

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
    source: Source,
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
            source,
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
        }
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

    /// The terminology server the transform may ask.
    pub fn with_terminology(mut self, url: Option<&str>) -> Self {
        self.transformer.set_terminology(url);
        self
    }

    /// The shared runtime the transform runs at, instead of a local process.
    pub fn with_runtime(mut self, settings: Option<&RuntimeSettings>) -> Self {
        self.transformer.set_runtime(settings);
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
        if !self.db.is_paused(&self.feed.id)? {
            self.tick().await?;
        }
        Ok(true)
    }

    /// The feed's loop: a pass every poll interval, or sooner when poked; nothing
    /// while the source is paused or held by another instance; until stopped.
    pub async fn run(self: Arc<Self>, control: Arc<RunnerControl>) {
        tracing::info!(feed = %self.feed.id, source = %self.feed.source.describe(), instance = %self.instance, "watching");
        loop {
            if control.stopped.load(Ordering::Acquire) {
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
        // A source that cannot be read this pass must not hold back what is
        // already in the store: the rest of the pass runs, the scan is retried next.
        if let Err(e) = self.scan().await {
            tracing::error!(feed = %self.feed.id, "scan failed, will retry next pass: {e:#}");
        }
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
