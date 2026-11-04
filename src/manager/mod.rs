//! Sources and transforms as resources: created, replaced, paused and tested
//! through the API, persisted as the files `config::load` reads, and run without
//! a restart. `transforms` holds the transform files' side: listing, saving
//! (and restarting the sources that use one) and trying one on records;
//! `probe` the connection test of a definition that is not a source yet.

mod error;
mod probe;
mod transforms;

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};

use anyhow::{Context, Result};
use serde_json::Value;

use crate::alerts::Alerter;
use crate::config::{Config, FeedConfig, RuntimeConfig};
use crate::connectors::Source;
use crate::db::{Db, Rule};
use crate::fhir::FhirClient;
use crate::pipeline::{FeedRunner, RunnerControl, Services};
use crate::runtime::RuntimeSettings;
use crate::versions::{ExecutorSettings, Versions};

pub use error::{ErrorStatus, ManagerError};

struct Managed {
    feed: FeedConfig,
    runner: Arc<FeedRunner>,
    control: Arc<RunnerControl>,
}

pub struct SourceManager {
    python: String,
    runtime_dir: PathBuf,
    sources_dir: PathBuf,
    transforms_dir: PathBuf,
    base_dir: PathBuf,
    terminology_url: Option<String>,
    /// The shared runtime transforms run at, when configured.
    runtime: Option<RuntimeSettings>,
    db: Arc<Db>,
    /// `hie.yaml` as loaded.
    config: Arc<Config>,
    /// What every runner shares (the store, HFS, versions, ...).
    services: Services,
    sources: Mutex<BTreeMap<String, Managed>>,
}

impl SourceManager {
    pub fn new(
        config: &Config,
        runtime_dir: &Path,
        db: Arc<Db>,
        fhir: Arc<FhirClient>,
        alerts: Arc<Alerter>,
    ) -> Result<Arc<Self>> {
        let runtime = config
            .runtime
            .as_ref()
            .filter(|r| !r.url.trim().is_empty())
            .map(RuntimeConfig::settings);
        let versions = Arc::new(Versions::new(
            db.clone(),
            ExecutorSettings {
                python: config.python.clone(),
                runtime_dir: runtime_dir.to_path_buf(),
                terminology: config.terminology_url.clone(),
                runtime: runtime.clone(),
            },
            &config.transforms_dir,
        ));
        let services = Services {
            db: db.clone(),
            fhir,
            alerts,
            versions,
            fhir_base_url: config.fhir_base_url.clone(),
            public_url: config.public_url.clone(),
            bulk_dir: config.bulk_dir(),
            validation: config.validation,
            content_keys: Some(Arc::new(config.content_keys()?)),
        };
        Ok(Arc::new(Self {
            python: config.python.clone(),
            runtime_dir: runtime_dir.to_path_buf(),
            sources_dir: config.sources_dir.clone(),
            transforms_dir: config.transforms_dir.clone(),
            base_dir: config.base_dir.clone(),
            terminology_url: config.terminology_url.clone(),
            runtime,
            db,
            config: Arc::new(config.clone()),
            services,
            sources: Mutex::new(BTreeMap::new()),
        }))
    }

    /// `hie.yaml` as loaded.
    pub fn config(&self) -> &Arc<Config> {
        &self.config
    }

    /// The transform versions loads and Try run.
    pub fn versions(&self) -> &Arc<Versions> {
        &self.services.versions
    }

    /// What every runner shares.
    pub fn services(&self) -> &Services {
        &self.services
    }

    /// Builds and spawns the runner for a resolved definition, replacing a running one.
    pub fn start(&self, feed: FeedConfig) -> Result<()> {
        let runner = Arc::new(FeedRunner::new(feed.clone(), &self.services)?);
        let control = Arc::new(RunnerControl::default());
        tokio::spawn(runner.clone().run(control.clone()));
        let previous = self.sources.lock().expect("manager mutex").insert(
            feed.id.clone(),
            Managed {
                feed,
                runner,
                control,
            },
        );
        if let Some(p) = previous {
            Self::halt(&p);
        }
        Ok(())
    }

    fn halt(managed: &Managed) {
        managed.control.stopped.store(true, Ordering::Release);
        managed.control.poke.notify_one();
    }

    pub fn start_all(&self, feeds: Vec<FeedConfig>) -> Result<()> {
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
            .map_err(|e| ManagerError::invalid("invalid_definition", e.to_string()))?;
        let mut resolved = submitted.clone();
        resolved
            .resolve(&self.base_dir)
            .map_err(|e| ManagerError::invalid("invalid_definition", format!("{e:#}")))?;
        Source::from_config(&resolved)
            .map_err(|e| ManagerError::invalid("invalid_definition", format!("{e:#}")))?;
        Ok((submitted, resolved))
    }

    fn write(&self, submitted: &FeedConfig) -> Result<()> {
        std::fs::create_dir_all(&self.sources_dir)?;
        let path = self.file_for(&submitted.id);
        std::fs::write(&path, submitted.to_yaml()?)
            .with_context(|| format!("writing {}", path.display()))
    }

    pub fn create(&self, body: &Value, by: Option<&str>) -> Result<FeedConfig, ManagerError> {
        let (submitted, resolved) = self.parse(body)?;
        if self.source(&submitted.id).is_some() || self.file_for(&submitted.id).exists() {
            return Err(ManagerError::conflict(
                "source_exists",
                format!("source {} already exists", submitted.id),
            ));
        }
        self.write(&submitted)?;
        self.ensure_default_rule(&submitted, by)?;
        self.start(resolved.clone())?;
        self.on_source_created(&resolved)?;
        Ok(resolved)
    }

    // ---- lifecycle hooks: what other areas keep about a source -------------

    /// A source was created: its pin period starts (versions).
    fn on_source_created(&self, feed: &FeedConfig) -> Result<()> {
        self.versions().source_created(feed)
    }

    /// A source's definition was replaced: its pin period moves (versions); the
    /// evidence and secret revisions the old definition held are released (S).
    fn on_source_replaced(&self, before: &FeedConfig, after: &FeedConfig) -> Result<()> {
        self.versions().source_replaced(before, after)?;
        crate::evidence::source_replaced(&self.db, before, after)
    }

    /// A source was deleted: its pin period ends (versions), its failed
    /// content is purged (H), its evidence and secret revisions released (S).
    fn on_source_deleted(&self, feed: &FeedConfig) -> Result<()> {
        self.versions().source_deleted(&feed.id)?;
        crate::purge::source_deleted(&self.db, &feed.id)?;
        crate::evidence::source_deleted(&self.db, &feed.id)
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

    pub fn replace(&self, id: &str, body: &Value) -> Result<FeedConfig, ManagerError> {
        if self.source(id).is_none() {
            return Err(ManagerError::not_found(
                "source_not_found",
                format!("source {id} not found"),
            ));
        }
        let (submitted, resolved) = self.parse(body)?;
        if submitted.id != id {
            return Err(ManagerError::invalid(
                "invalid_definition",
                format!("the body's id {:?} does not match {id:?}", submitted.id),
            ));
        }
        let before = self.source(id);
        self.write(&submitted)?;
        self.start(resolved.clone())?;
        if let Some(before) = before {
            self.on_source_replaced(&before, &resolved)?;
        }
        Ok(resolved)
    }

    /// Stops the source and removes its definition. Its records and alerts stay.
    pub fn delete(&self, id: &str) -> Result<(), ManagerError> {
        let removed = self.sources.lock().expect("manager mutex").remove(id);
        let Some(managed) = removed else {
            return Err(ManagerError::not_found(
                "source_not_found",
                format!("source {id} not found"),
            ));
        };
        Self::halt(&managed);
        let path = self.file_for(id);
        if path.exists() {
            std::fs::remove_file(&path).with_context(|| format!("removing {}", path.display()))?;
        }
        self.on_source_deleted(&managed.feed)?;
        Ok(())
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
        self.request_pass(id).map(drop)
    }

    /// Asks for a pass of the source now; the pass's `ps_` id, which the
    /// runner marks running, then done, skipped or failed.
    pub fn request_pass(&self, id: &str) -> Result<String, ManagerError> {
        self.require(id)?;
        let pass = self.db.request_pass(id)?;
        if let Some(managed) = self.sources.lock().expect("manager mutex").get(id) {
            managed.control.poke.notify_one();
        }
        Ok(pass)
    }

    fn require(&self, id: &str) -> Result<(), ManagerError> {
        if self.source(id).is_none() {
            return Err(ManagerError::not_found(
                "source_not_found",
                format!("source {id} not found"),
            ));
        }
        Ok(())
    }
}
