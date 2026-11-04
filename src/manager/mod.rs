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

use crate::admission::Admission;
use crate::alerts::Alerter;
use crate::config::{Config, DatabaseRead, FeedConfig, RuntimeConfig, SourceConfig};
use crate::connectors::{Context as ConnectContext, Source};
use crate::db::{Db, Rule};
use crate::evidence::{Binding, Evidence, StartBinding};
use crate::fhir::FhirClient;
use crate::pipeline::{FeedRunner, RunnerControl, Services};
use crate::runtime::RuntimeSettings;
use crate::secrets::Secrets;
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
    /// The admission policy and the kept secrets connectors open connections with.
    connect: ConnectContext,
    evidence: Arc<Evidence>,
    /// The destination's facts, read at most once a minute.
    destination: tokio::sync::Mutex<Option<(std::time::Instant, serde_json::Value)>>,
    sources: Mutex<BTreeMap<String, Managed>>,
}

/// What a definition of a source names that is not part of [`FeedConfig`]:
/// the evidence a Start binds to.
fn evidence_member(body: &Value) -> Result<Option<String>, ManagerError> {
    match body.get("evidence") {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(id)) => Ok(Some(id.clone())),
        Some(_) => Err(ManagerError::invalid(
            "invalid_definition",
            "member `evidence` must be the id of a Test's evidence",
        )
        .with("field", "evidence")),
    }
}

/// Reads a definition (or a part of one) without echoing any value of it:
/// a failure names the member and what was expected, never what was sent.
/// Unknown members are ignored, as 0.1 definitions always were.
pub(crate) fn read_definition<T: serde::de::DeserializeOwned>(
    value: &Value,
) -> Result<T, ManagerError> {
    serde_path_to_error::deserialize::<_, T>(value).map_err(|e| {
        let path = e.path().to_string();
        let inner = e.inner().to_string();
        let at = if path == "." {
            String::new()
        } else {
            format!("member `{path}` ")
        };
        let what = if let Some(rest) = inner.strip_prefix("missing field ") {
            let field = rest
                .split_whitespace()
                .next()
                .unwrap_or(rest)
                .trim_matches('`');
            format!("member `{field}` is required")
        } else if let Some((_, expected)) = inner.split_once(", expected ") {
            let expected = expected.split(" at line ").next().unwrap_or(expected);
            format!("{at}has the wrong type or value: expected {expected}")
        } else if inner.contains("did not match any variant") {
            format!("{at}has none of the shapes it may take")
        } else {
            format!("{at}has the wrong type or value")
        };
        let mut error = ManagerError::invalid("invalid_definition", what.trim().to_string());
        if path != "." {
            error = error.with("field", path);
        }
        error
    })
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
        let connect = ConnectContext {
            admission: Arc::new(Admission::new(config.admission.clone())),
            secrets: Some(Arc::new(Secrets::new(db.clone(), config.secrets_keys()?))),
        };
        let evidence = Arc::new(Evidence::new(db.clone(), connect.clone()));
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
            connect,
            evidence,
            destination: tokio::sync::Mutex::new(None),
            sources: Mutex::new(BTreeMap::new()),
        }))
    }

    /// The admission policy in force.
    pub fn admission(&self) -> &Arc<Admission> {
        &self.connect.admission
    }

    /// The kept secrets.
    pub fn secrets(&self) -> &Arc<Secrets> {
        self.connect
            .secrets
            .as_ref()
            .expect("the manager keeps secrets")
    }

    /// Connection evidence, describe and sample.
    pub fn evidence(&self) -> &Arc<Evidence> {
        &self.evidence
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
    pub fn start(&self, mut feed: FeedConfig) -> Result<()> {
        // A definition written by hand may leave its password's revision
        // out: it runs with the current one, fixed now, never a later one.
        if let SourceConfig::Database {
            connection: Some(connection),
            ..
        } = &mut feed.source
            && connection
                .password
                .as_ref()
                .is_some_and(|p| p.revision.is_none())
            && let Ok(Some((_, revision))) = self.evidence.resolve_credential(connection)
            && let Some(password) = &mut connection.password
        {
            password.revision = Some(revision);
        }
        self.admit_running(&feed)?;
        let source = Source::from_config_in(&feed, &self.connect)?.with_db(self.db.clone());
        let runner = Arc::new(FeedRunner::from_services(
            feed.clone(),
            source,
            &self.services,
        ));
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

    /// A source whose connection the policy no longer admits is not passed:
    /// it reads `paused` by `admission` until the policy admits it again.
    fn admit_running(&self, feed: &FeedConfig) -> Result<()> {
        let paused_by_admission = self
            .db
            .paused_state(&feed.id)?
            .is_some_and(|(_, by)| by.as_deref() == Some("admission"));
        match self.connect.admission.check_source(&feed.source) {
            Err(refused) => {
                if !paused_by_admission {
                    tracing::warn!(feed = %feed.id, "{refused}; the source is paused");
                }
                self.db.set_paused(&feed.id, true, Some("admission"))
            }
            Ok(()) if paused_by_admission => self.db.set_paused(&feed.id, false, None),
            Ok(()) => Ok(()),
        }
    }

    fn halt(managed: &Managed) {
        managed.control.stopped.store(true, Ordering::Release);
        managed.control.poke.notify_one();
    }

    pub fn start_all(&self, feeds: Vec<FeedConfig>) -> Result<()> {
        for feed in feeds {
            // No runner of this process runs yet: a pass it left running died with it.
            self.db
                .recover_passes(&feed.id, crate::pipeline::instance_id())?;
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
        let submitted: FeedConfig = read_definition(body)?;
        let mut resolved = submitted.clone();
        resolved
            .resolve(&self.base_dir)
            .map_err(|e| ManagerError::invalid("invalid_definition", format!("{e:#}")))?;
        // Built once to check it, never run. A password reference without a
        // revision is checked with the current one; a Start replaces it with
        // its evidence's.
        let mut checked = resolved.clone();
        if let SourceConfig::Database {
            connection: Some(connection),
            ..
        } = &mut checked.source
        {
            // A reference to a secret that does not exist, or without custody.
            let credential = self.evidence.resolve_credential(connection)?;
            if let (Some(password), Some((_, revision))) = (&mut connection.password, credential) {
                password.revision = Some(revision);
            }
        }
        Source::from_config_in(&checked, &self.connect)
            .map_err(|e| ManagerError::invalid("invalid_definition", format!("{e:#}")))?;
        Ok((submitted, resolved))
    }

    fn write(&self, submitted: &FeedConfig) -> Result<()> {
        std::fs::create_dir_all(&self.sources_dir)?;
        let path = self.file_for(&submitted.id);
        std::fs::write(&path, submitted.to_yaml()?)
            .with_context(|| format!("writing {}", path.display()))
    }

    /// Writes a new source's file, only if none exists: of two attempts at
    /// one Start (a stale idempotency owner and its successor, or two
    /// clients), exactly one creates it. `false` when it exists.
    fn write_new(&self, submitted: &FeedConfig) -> Result<bool> {
        use std::io::Write;
        std::fs::create_dir_all(&self.sources_dir)?;
        let path = self.file_for(&submitted.id);
        let yaml = submitted.to_yaml()?;
        let mut file = match std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)
        {
            Ok(file) => file,
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => return Ok(false),
            Err(e) => return Err(e).with_context(|| format!("creating {}", path.display())),
        };
        file.write_all(yaml.as_bytes())
            .and_then(|()| file.sync_all())
            .with_context(|| format!("writing {}", path.display()))?;
        Ok(true)
    }

    /// Checks a definition as a Start (or replace) does, after it is valid:
    /// admission, then its evidence. The binding it takes, if any.
    fn admit_and_bind(
        &self,
        body: &Value,
        resolved: &FeedConfig,
    ) -> Result<Option<StartBinding>, ManagerError> {
        self.evidence.admit(&resolved.source)?;
        let evidence = evidence_member(body)?;
        self.evidence.bind(&resolved.source, evidence.as_deref())
    }

    /// Fills in what the binding fixes: the password's revision and the
    /// table read's key, so the file and the runner use exactly what was tested.
    fn fill(feed: &mut FeedConfig, binding: &StartBinding) {
        if let SourceConfig::Database {
            connection: Some(connection),
            read,
            ..
        } = &mut feed.source
        {
            if let (Some(password), Some((_, revision))) =
                (&mut connection.password, &binding.credential)
            {
                password.revision = Some(*revision);
            }
            if let (Some(DatabaseRead::Table { key, .. }), Some(filled)) = (read, &binding.key) {
                *key = filled.clone();
            }
        }
    }

    /// Creates a source (a Start). `recovered`: an earlier attempt with the
    /// same idempotency key may have created it; then it is answered as is.
    pub fn create(
        &self,
        body: &Value,
        by: Option<&str>,
        recovered: bool,
    ) -> Result<FeedConfig, ManagerError> {
        let (mut submitted, mut resolved) = self.parse(body)?;
        // V: the transform pin's check goes here (a transform or version that
        // does not exist is `400 invalid_definition`).
        if recovered && let Some(existing) = self.source(&submitted.id) {
            return Ok(existing);
        }
        let binding = self.admit_and_bind(body, &resolved)?;
        if self.source(&submitted.id).is_some() || self.file_for(&submitted.id).exists() {
            return Err(ManagerError::conflict(
                "source_exists",
                format!("source {} already exists", submitted.id),
            ));
        }
        if let Some(binding) = &binding {
            Self::fill(&mut submitted, binding);
            Self::fill(&mut resolved, binding);
        }
        if !self.write_new(&submitted)? {
            if recovered && let Some(existing) = self.source(&submitted.id) {
                return Ok(existing);
            }
            return Err(ManagerError::conflict(
                "source_exists",
                format!("source {} already exists", submitted.id),
            ));
        }
        self.ensure_default_rule(&submitted, by)?;
        if let Some(binding) = &binding {
            // The first load exists from the Start on, queued, so a client
            // can follow it at once; the first pass adopts it.
            let first_load = self.db.create_queued_load(&submitted.id)?;
            self.evidence.source_started(&Binding {
                source: submitted.id.clone(),
                evidence: Some(binding.evidence.clone()),
                credential: binding.credential.clone(),
                first_load: Some(first_load),
            })?;
        }
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
        self.versions().source_replaced(before, after)
    }

    /// A source was deleted: its pin period ends (versions), its failed
    /// content is purged (H), its evidence and secret revisions released (S).
    fn on_source_deleted(&self, feed: &FeedConfig) -> Result<()> {
        self.versions().source_deleted(&feed.id)?;
        crate::purge::source_deleted(&self.db, &feed.id)?;
        self.evidence.source_deleted(&feed.id)
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
        let (mut submitted, mut resolved) = self.parse(body)?;
        if submitted.id != id {
            return Err(ManagerError::invalid(
                "invalid_definition",
                format!("the body's id {:?} does not match {id:?}", submitted.id),
            ));
        }
        let binding = self.admit_and_bind(body, &resolved)?;
        if let Some(binding) = &binding {
            Self::fill(&mut submitted, binding);
            Self::fill(&mut resolved, binding);
        }
        let before = self.source(id);
        self.write(&submitted)?;
        // The new definition holds its evidence and revision; what the old
        // one held is released.
        self.evidence.source_started(&Binding {
            source: id.to_string(),
            evidence: binding.as_ref().map(|b| b.evidence.clone()),
            credential: binding.as_ref().and_then(|b| b.credential.clone()),
            first_load: None,
        })?;
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
        if let Some(feed) = self.source(id) {
            // A source the policy refuses stays paused by admission.
            self.evidence.admit(&feed.source)?;
        }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_definition_that_does_not_parse_names_no_value() {
        let body = serde_json::json!({
            "id": "labs", "name": "Labs", "transform": "t.py",
            "source": {"type": "database", "connection": {"engine": "sqlite", "database": "x.db",
                       "password": "Corpus-Secret-1!"}},
        });
        let e = read_definition::<FeedConfig>(&body).unwrap_err();
        assert_eq!(e.code(), "invalid_definition");
        assert!(!e.message().contains("Corpus-Secret-1!"), "{}", e.message());
        assert!(
            !e.members()
                .values()
                .any(|v| v.to_string().contains("Corpus-Secret-1!"))
        );
    }
}
