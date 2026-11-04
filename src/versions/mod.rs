//! Transform versions (decision 35): transforms with a stable id and immutable
//! numbered versions, each saved in one environment; which version a load or
//! retry runs; executors that run exactly that version; pins and their
//! periods; Try runs (`crate::tryrun`) are registered here too.
//!
//! The interface the pipeline, the manager and the API use:
//!
//! - Reading: [`Versions::transform`], [`Versions::transforms`],
//!   [`Versions::version`], [`Versions::versions`], [`Versions::code`],
//!   [`Versions::latest`], [`Versions::get`].
//! - Saving (`catalog`): [`Versions::environment_for`] (async: the executor's
//!   default environment, or a named one), then inside the caller's
//!   transaction [`Versions::create_in`] / [`Versions::save_in`] /
//!   [`Versions::rename_in`], then [`Versions::saved`] (the transform's file
//!   follows its current version). [`Versions::save_legacy`] for `PUT
//!   /api/transforms/{name}`, [`Versions::delete`].
//! - Pins (`pins`): [`Versions::resolve_for_source`] (read afresh at every
//!   load start), [`Versions::pin_view`], [`Versions::set_pin`],
//!   [`Versions::check_reference`], [`Versions::usage`], and the lifecycle
//!   hooks [`Versions::source_created`], [`Versions::source_replaced`],
//!   [`Versions::source_deleted`] (pin periods, local workers).
//! - Running (`execute`, `programs`): [`Versions::executor`] runs records for
//!   exactly one version (its code from the store, checked against its
//!   `code_sha256`) and reports the environment it ran in;
//!   [`Versions::reclaim`] frees what is no longer needed.
//!
//! Files under `transforms/` are imported when the store opens and whenever
//! a 0.1 reference's file changes (`files`); a file is identified by its
//! canonical path, so two files with one stem never share a transform.

mod catalog;
mod environment;
mod execute;
mod files;
mod pins;
mod programs;
#[cfg(test)]
mod tests;

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use anyhow::Result;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::db::Db;
use crate::runtime::{Facade, RuntimeSettings};

pub use catalog::{Draft, MAX_CODE_BYTES};
pub use execute::{EnvironmentUnavailable, Execute, Execution, ItemOutcome};
pub use pins::{PinChange, USAGE_RUNS};

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

/// A transform as the store holds it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransformInfo {
    pub id: String,
    pub display_name: String,
    pub current_version: i64,
    pub created_at: i64,
    pub updated_at: i64,
    pub deleted: bool,
    /// The canonical path of its file, when it has one.
    pub file_path: Option<String>,
    /// The SHA-256 of what HIE last wrote to or read from that file.
    pub file_sha256: Option<String>,
    /// Its 0.1 name, as given (`PUT /api/transforms/{name}`, a file's stem
    /// under `transforms/`), when it has one; it names its file there.
    pub legacy_name: Option<String>,
}

impl TransformInfo {
    /// The name 0.1 clients see: its 0.1 name, else its id.
    pub fn name(&self) -> &str {
        self.legacy_name.as_deref().unwrap_or(&self.id)
    }
}

/// What a delete found.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Deleted {
    Done,
    NotFound,
    /// Refused: the sources that refer to it (none when only unfinished work does).
    InUse(Vec<String>),
}

/// One version's metadata (its code is read apart, [`Versions::code`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VersionInfo {
    pub transform_id: String,
    pub version: i64,
    pub code_sha256: String,
    pub note: Option<String>,
    pub author: Option<String>,
    pub display_name: String,
    pub base_version: Option<i64>,
    pub created_at: i64,
    pub environment: Environment,
}

impl VersionInfo {
    /// The contract's version object, without code.
    pub fn to_json(&self) -> Value {
        json!({
            "version": self.version,
            "code_sha256": self.code_sha256,
            "note": self.note,
            "author": self.author,
            "display_name": self.display_name,
            "base_version": self.base_version,
            "created_at": crate::ids::rfc3339_ms_at(self.created_at),
            "environment": self.environment,
        })
    }

    pub fn reference(&self) -> VersionRef {
        VersionRef {
            transform_id: self.transform_id.clone(),
            version: self.version,
            code_sha256: self.code_sha256.clone(),
            environment: self.environment.clone(),
        }
    }
}

/// A source's pin as the store holds it: a version, or (`None`) the current
/// one; `declared` is the definition's pin it was last set from.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PinRow {
    pub transform_id: String,
    pub version: Option<i64>,
    pub declared: String,
}

/// One period a source pinned (`version`) or followed (`None`) a transform.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PinPeriod {
    pub source: String,
    pub version: Option<i64>,
    pub since: i64,
    pub until: Option<i64>,
}

/// A new version's content.
pub struct NewVersion<'a> {
    pub code: &'a str,
    pub code_sha256: &'a str,
    pub note: Option<&'a str>,
    pub author: Option<&'a str>,
    pub environment: &'a Environment,
}

/// What a save found.
pub enum Saved {
    Version(Box<VersionInfo>),
    /// `base_version` was not the current version.
    Conflict {
        current_version: i64,
    },
    NotFound,
}

/// Which executor a cached executor is: local workers are kept per source and
/// version (module state never crosses sources); the runtime's per version.
type ExecutorKey = (Option<String>, String, i64, String);

pub struct Versions {
    db: Arc<Db>,
    settings: ExecutorSettings,
    transforms_dir: PathBuf,
    facade: Option<Facade>,
    /// The runtime's default environment as last read (`None` until read).
    default_environment: Arc<Mutex<Option<Environment>>>,
    executors: Mutex<HashMap<ExecutorKey, Arc<dyn Execute>>>,
    /// Where local workers' code is written from the store, one file per
    /// version; removed with this value.
    materialized: PathBuf,
    /// Sources whose pin was reconciled with their definition in this process.
    seen: Mutex<HashSet<String>>,
    /// One lock per source, held while its pin is first reconciled.
    reconciling: Mutex<HashMap<String, Arc<Mutex<()>>>>,
    tries: crate::tryrun::TryRuns,
    reclaimed: Mutex<Option<std::time::Instant>>,
    /// Whether the transform files found at start were imported.
    imported: std::sync::atomic::AtomicBool,
}

impl Versions {
    pub fn new(db: Arc<Db>, settings: ExecutorSettings, transforms_dir: &Path) -> Self {
        let facade = settings.runtime.clone().map(Facade::new);
        let materialized = std::env::temp_dir().join(format!(
            "hie-versions-{}-{:016x}",
            std::process::id(),
            rand::random::<u64>()
        ));
        let versions = Self {
            db,
            settings,
            transforms_dir: transforms_dir.to_path_buf(),
            facade,
            default_environment: Arc::new(Mutex::new(None)),
            executors: Mutex::new(HashMap::new()),
            materialized,
            seen: Mutex::new(HashSet::new()),
            reconciling: Mutex::new(HashMap::new()),
            tries: crate::tryrun::TryRuns::default(),
            reclaimed: Mutex::new(None),
            imported: std::sync::atomic::AtomicBool::new(false),
        };
        if versions.facade.is_none() {
            tracing::warn!(target: "hie::versions",
                "no runtime configured: transforms run on the engine's own Python worker, with the engine's own rights (development and tests; configure `runtime:` to run them sandboxed at python-runtime)");
        }
        // Locally at once; at the runtime once its environment is known (read
        // in the background from now, [`Versions::ready`]).
        versions.import_dir();
        versions
    }

    pub(crate) fn db(&self) -> &Arc<Db> {
        &self.db
    }

    /// The shared runtime, when transforms run there.
    pub fn facade(&self) -> Option<&Facade> {
        self.facade.as_ref()
    }

    /// How local workers are started.
    pub fn settings(&self) -> &ExecutorSettings {
        &self.settings
    }

    /// A new file in the engine's scratch directory (code a worker loads).
    pub fn scratch_file(&self, prefix: &str) -> Result<PathBuf> {
        std::fs::create_dir_all(&self.materialized)?;
        Ok(self
            .materialized
            .join(format!("{prefix}-{:016x}.py", rand::random::<u64>())))
    }

    /// The Try runs of this engine.
    pub fn tries(&self) -> &crate::tryrun::TryRuns {
        &self.tries
    }

    // ---- reading ------------------------------------------------------------------

    /// A live transform.
    pub fn transform(&self, id: &str) -> Result<Option<TransformInfo>> {
        Ok(self.db.transform_info(id)?.filter(|t| !t.deleted))
    }

    /// Every live transform, by id.
    pub fn transforms(&self) -> Result<Vec<TransformInfo>> {
        self.db.transform_infos()
    }

    /// One version of a live transform.
    pub fn version(&self, id: &str, version: i64) -> Result<Option<VersionInfo>> {
        if self.transform(id)?.is_none() {
            return Ok(None);
        }
        self.db.version_info(id, version)
    }

    /// Every version of a live transform, newest first.
    pub fn versions(&self, id: &str) -> Result<Vec<VersionInfo>> {
        self.db.version_infos(id)
    }

    /// A version's code.
    pub fn code(&self, id: &str, version: i64) -> Result<Option<String>> {
        self.db.version_code(id, version)
    }

    /// The transform's current version; `None` when there is no such transform.
    pub fn latest(&self, transform_id: &str) -> Result<Option<VersionRef>> {
        let Some(t) = self.transform(transform_id)? else {
            return Ok(None);
        };
        Ok(self
            .db
            .version_info(transform_id, t.current_version)?
            .map(|v| v.reference()))
    }

    /// One version of a transform, deleted transforms included (a load that
    /// started on it still names it); `None` when it does not exist.
    pub fn get(&self, transform_id: &str, version: i64) -> Result<Option<VersionRef>> {
        Ok(self
            .db
            .version_info(transform_id, version)?
            .map(|v| v.reference()))
    }

    /// The contract's transform object (no code, no version).
    pub fn transform_json(&self, t: &TransformInfo) -> Value {
        json!({
            "id": t.id,
            "name": t.name(),
            "display_name": t.display_name,
            "current_version": t.current_version,
            "created_at": crate::ids::rfc3339_ms_at(t.created_at),
            "updated_at": crate::ids::rfc3339_ms_at(t.updated_at),
        })
    }
}

impl Drop for Versions {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.materialized);
    }
}

/// SHA-256 of text, as 64 hex digits.
fn sha256_hex(text: &str) -> String {
    crate::formats::sha256_hex(text.as_bytes())
}
