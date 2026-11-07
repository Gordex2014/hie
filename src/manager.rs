//! Sources and transforms as resources: created, replaced, paused and tested
//! through the API, persisted as the files `config::load` reads, and run without
//! a restart.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use serde_json::{Value, json};

use crate::alerts::Alerter;
use crate::config::{Config, FeedConfig, SourceConfig};
use crate::db::{Db, Rule};
use crate::fhir::FhirClient;
use crate::pipeline::{FeedRunner, RunnerControl};
use crate::source::{Probe, Source};
use crate::transform::{Outcome, Transformer, check_resource};
use crate::worker::WorkerLimits;

/// Transform tries that may run at once, each with its own worker process.
const TRY_CONCURRENCY: usize = 4;
/// How long a try waits for its turn before it is turned away.
const TRY_WAIT: std::time::Duration = std::time::Duration::from_secs(10);

#[derive(Debug)]
pub enum ManagerError {
    Invalid(String),
    NotFound(String),
    Conflict(String),
    /// Too much running already; the same request may succeed later.
    Busy(String),
    Internal(anyhow::Error),
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
    sources: Mutex<BTreeMap<String, Managed>>,
}

impl SourceManager {
    pub fn new(
        config: &Config,
        runtime_dir: &Path,
        db: Arc<Db>,
        fhir: Arc<FhirClient>,
        alerts: Arc<Alerter>,
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
            sources: Mutex::new(BTreeMap::new()),
        })
    }

    /// Builds and spawns the runner for a resolved definition, replacing a running one.
    pub fn start(&self, feed: FeedConfig) -> Result<()> {
        let runner = Arc::new(
            FeedRunner::new(
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
            .with_worker_limits(self.worker_limits.clone()),
        );
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
            .map_err(|e| ManagerError::Invalid(e.to_string()))?;
        let mut resolved = submitted.clone();
        resolved
            .resolve(&self.base_dir)
            .map_err(|e| ManagerError::Invalid(format!("{e:#}")))?;
        Source::from_config(&resolved).map_err(|e| ManagerError::Invalid(format!("{e:#}")))?;
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
            return Err(ManagerError::Conflict(format!(
                "source {} already exists",
                submitted.id
            )));
        }
        self.write(&submitted)?;
        self.ensure_default_rule(&submitted, by)?;
        self.start(resolved.clone())?;
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

    pub fn replace(&self, id: &str, body: &Value) -> Result<FeedConfig, ManagerError> {
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
        self.write(&submitted)?;
        self.start(resolved.clone())?;
        Ok(resolved)
    }

    /// Stops the source and removes its definition. Its records and alerts stay.
    pub fn delete(&self, id: &str) -> Result<(), ManagerError> {
        let removed = self.sources.lock().expect("manager mutex").remove(id);
        let Some(managed) = removed else {
            return Err(ManagerError::NotFound(format!("source {id} not found")));
        };
        Self::halt(&managed);
        let path = self.file_for(id);
        if path.exists() {
            std::fs::remove_file(&path).with_context(|| format!("removing {}", path.display()))?;
        }
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
            .map_err(|e| ManagerError::Invalid(e.to_string()))?;
        source.resolve(&self.base_dir);
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
        let source =
            Source::from_config(&feed).map_err(|e| ManagerError::Invalid(format!("{e:#}")))?;
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
    pub fn save_transform(&self, name: &str, code: &str) -> Result<bool, ManagerError> {
        let path = self.transform_path(name)?;
        std::fs::create_dir_all(&self.transforms_dir).map_err(anyhow::Error::from)?;
        let existed = path.exists();
        std::fs::write(&path, code).with_context(|| format!("writing {}", path.display()))?;
        let canonical = path.canonicalize().unwrap_or(path);
        for feed in self.sources() {
            if feed.transform.canonicalize().ok().as_ref() == Some(&canonical) {
                self.start(feed)?;
            }
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
