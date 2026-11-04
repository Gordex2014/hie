//! Transform versions: which version a load or retry runs, and an executor
//! that runs exactly that version and says where it ran (decision 35).
//!
//! The interface the pipeline, the manager and the API use:
//!
//! - [`Versions::resolve_for_source`]: the version a load of this source runs
//!   if it starts now (its pin, or the current version it follows), read
//!   afresh at every load start; the load keeps it until it finishes.
//! - [`Versions::latest`] and [`Versions::get`]: a transform's current version,
//!   and one version by number.
//! - [`Versions::executor`]: something that runs records for that exact
//!   version ([`Execute`]) and reports the [`Environment`] it ran in, so a run
//!   in another environment is not applied.
//! - Lifecycle hooks the manager calls: [`Versions::source_created`],
//!   [`Versions::source_replaced`], [`Versions::source_deleted`] (pin periods).
//!
//! **Phase 0** (what is here now; track V replaces the inside): each transform
//! is one file and has one version, 1, its current content; a pin may only name
//! version 1. The executor is the engine's [`Transformer`]: the local Python
//! worker, or python-runtime when `runtime` is configured. Locally the
//! environment is `{"executor": "local", "id": "local", "digest": null}`; at the
//! runtime it is a fixed `{"executor": "python-runtime", "id": "default"}` until
//! V reads the runtime's capabilities. A file changed while a load runs is
//! picked up as before 0.2.0.

mod execute;

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};

use crate::config::{FeedConfig, PinMode, TransformRef};
use crate::db::Db;
use crate::runtime::RuntimeSettings;

pub use execute::{Execute, Execution, ItemOutcome};

/// The identity of the place a version runs, as the contract's `environment`
/// object: `{"executor", "id", "digest"}`.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Environment {
    /// `local` or `python-runtime`.
    pub executor: String,
    pub id: String,
    pub digest: Option<String>,
}

impl Environment {
    /// The engine's own Python worker.
    pub fn local() -> Self {
        Self {
            executor: "local".into(),
            id: "local".into(),
            digest: None,
        }
    }
}

/// Names one version: what a load or retry records.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct VersionKey {
    pub transform_id: String,
    pub version: i64,
}

/// One immutable version, as a load or retry runs it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VersionRef {
    pub transform_id: String,
    pub version: i64,
    pub code_sha256: String,
    pub environment: Environment,
}

impl VersionRef {
    pub fn key(&self) -> VersionKey {
        VersionKey {
            transform_id: self.transform_id.clone(),
            version: self.version,
        }
    }
}

/// Who runs one record attempt: its load, the retry it belongs to (`None` for
/// the load's own attempt) and the version, resolved.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Exec {
    pub load_id: i64,
    pub retry: Option<String>,
    pub version: VersionRef,
}

/// How executors are made: the local worker's Python and runtime directory,
/// the terminology server, and the shared runtime when configured.
#[derive(Debug, Clone)]
pub struct ExecutorSettings {
    pub python: String,
    pub runtime_dir: PathBuf,
    pub terminology: Option<String>,
    pub runtime: Option<RuntimeSettings>,
}

type ExecutorKey = (String, i64, String);

pub struct Versions {
    #[allow(dead_code)]
    db: Arc<Db>,
    settings: ExecutorSettings,
    transforms_dir: PathBuf,
    /// Phase 0: the file each transform id was last resolved to.
    files: Mutex<HashMap<String, PathBuf>>,
    executors: Mutex<HashMap<ExecutorKey, Arc<dyn Execute>>>,
}

impl Versions {
    pub fn new(db: Arc<Db>, settings: ExecutorSettings, transforms_dir: &Path) -> Self {
        Self {
            db,
            settings,
            transforms_dir: transforms_dir.to_path_buf(),
            files: Mutex::new(HashMap::new()),
            executors: Mutex::new(HashMap::new()),
        }
    }

    /// The version a load of `feed` runs if it starts now: the version its
    /// pin names, or the current one it follows. Read afresh each time.
    pub fn resolve_for_source(&self, feed: &FeedConfig) -> Result<VersionRef> {
        let id = feed.transform.id();
        let path = match &feed.transform {
            TransformRef::File(path) => path.clone(),
            TransformRef::Pin(_) => self.transforms_dir.join(format!("{id}.py")),
        };
        self.files
            .lock()
            .expect("versions files")
            .insert(id.clone(), path.clone());
        let current = self.version_of(&id, &path)?;
        match feed.transform.mode() {
            PinMode::FollowCurrent => Ok(current),
            PinMode::Version(v) if v == current.version => Ok(current),
            PinMode::Version(v) => bail!("transform {id} has no version {v}"),
        }
    }

    /// The transform's current version; `None` when there is no such transform.
    pub fn latest(&self, transform_id: &str) -> Result<Option<VersionRef>> {
        let path = self.file_of(transform_id);
        if !path.is_file() {
            return Ok(None);
        }
        self.version_of(transform_id, &path).map(Some)
    }

    /// One version of a transform; `None` when either does not exist.
    pub fn get(&self, transform_id: &str, version: i64) -> Result<Option<VersionRef>> {
        Ok(self.latest(transform_id)?.filter(|v| v.version == version))
    }

    /// Runs records for exactly `version` and reports the environment it ran in.
    pub fn executor(&self, version: &VersionRef) -> Result<Arc<dyn Execute>> {
        let key = (
            version.transform_id.clone(),
            version.version,
            version.code_sha256.clone(),
        );
        let mut executors = self.executors.lock().expect("versions executors");
        if let Some(executor) = executors.get(&key) {
            return Ok(executor.clone());
        }
        let path = self.file_of(&version.transform_id);
        let executor: Arc<dyn Execute> = Arc::new(execute::TransformerExecutor::new(
            &self.settings,
            &path,
            version.environment.clone(),
        ));
        // Phase 0: a transform's earlier content is gone; so is its worker.
        executors.retain(|(id, _, _), _| id != &version.transform_id);
        executors.insert(key, executor.clone());
        Ok(executor)
    }

    /// A source was created: its pin period starts. Phase 0: nothing to keep.
    pub fn source_created(&self, feed: &FeedConfig) -> Result<()> {
        let _ = feed;
        Ok(())
    }

    /// A source's definition was replaced: one pin period ends, another starts.
    /// Phase 0: nothing to keep.
    pub fn source_replaced(&self, before: &FeedConfig, after: &FeedConfig) -> Result<()> {
        let _ = (before, after);
        Ok(())
    }

    /// A source was deleted: its pin period ends. Phase 0: nothing to keep.
    pub fn source_deleted(&self, source_id: &str) -> Result<()> {
        let _ = source_id;
        Ok(())
    }

    fn file_of(&self, transform_id: &str) -> PathBuf {
        self.files
            .lock()
            .expect("versions files")
            .get(transform_id)
            .cloned()
            .unwrap_or_else(|| self.transforms_dir.join(format!("{transform_id}.py")))
    }

    fn version_of(&self, transform_id: &str, path: &Path) -> Result<VersionRef> {
        let code =
            std::fs::read(path).with_context(|| format!("reading transform {}", path.display()))?;
        Ok(VersionRef {
            transform_id: transform_id.to_string(),
            version: 1,
            code_sha256: crate::formats::sha256_hex(&code),
            environment: self.environment(),
        })
    }

    fn environment(&self) -> Environment {
        match &self.settings.runtime {
            Some(_) => Environment {
                executor: "python-runtime".into(),
                id: "default".into(),
                digest: None,
            },
            None => Environment::local(),
        }
    }
}
