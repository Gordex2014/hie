//! Connectors: where a source's data comes from. Every kind of source is a
//! module of its own behind one interface, [`Connector`]; the pipeline, the
//! manager and the API only see that interface, the [`Source`] handle that
//! carries it, and the registry ([`kind`]) that knows which module serves
//! which `type:` of `SourceConfig`.
//!
//! - `object`: a folder, an S3 bucket (any `object_store`), optionally told of
//!   new objects by an SQS queue.
//! - `fhir_server`: another FHIR server, pulled by `_lastUpdated`.
//! - `database`: a SQL query that follows a cursor column (PostgreSQL, MySQL,
//!   SQLite, SQL Server).
//! - `mllp`: an MLLP listener for HL7 v2 that spools what it receives.
//!
//! How a fetched object is cut into records is not a connector's business; see
//! `crate::formats`.

mod build;
pub mod database;
pub mod fhir_server;
pub mod mllp;
pub mod object;
mod source;

use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Result;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::Serialize;
use serde_json::Value;

use crate::config::{FeedConfig, Format, SourceConfig};
use crate::db::Db;

pub use source::Source;

/// One object fetched from a source: a file, an S3 object, a pull.
pub struct Incoming {
    /// The source's own handle: a file path, an object key.
    pub key: String,
    /// The human-readable name kept with every record.
    pub name: String,
    pub bytes: Vec<u8>,
}

/// One object a pass would take, before it is fetched.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Listed {
    pub key: String,
    pub name: String,
    /// In bytes; 0 when the source does not say.
    pub size: u64,
}

impl Listed {
    /// A key with no size, named after its last path segment.
    pub fn from_key(key: impl Into<String>) -> Self {
        let key = key.into();
        let name = key.rsplit(['/', '\\']).next().unwrap_or(&key).to_string();
        Self { key, name, size: 0 }
    }
}

/// What a connection test found: the evidence the UI keeps on the page.
#[derive(Debug, Clone, Serialize)]
pub struct Probe {
    pub reachable: bool,
    pub elapsed_ms: u64,
    /// Objects matching the pattern, whatever their age.
    pub objects: usize,
    pub newest: Option<String>,
    pub newest_at: Option<String>,
}

impl Probe {
    /// A reachable source and what it holds, each with when it changed.
    pub fn found(started: Instant, found: &[(String, DateTime<Utc>)]) -> Self {
        let newest = found.iter().max_by_key(|(_, at)| *at);
        Probe {
            reachable: true,
            elapsed_ms: started.elapsed().as_millis() as u64,
            objects: found.len(),
            newest: newest.map(|(k, _)| k.clone()),
            newest_at: newest.map(|(_, at)| at.to_rfc3339()),
        }
    }
}

/// Where a pulling source keeps how far it has read, per key, in the engine's
/// store. Without a store nothing is kept: every pass starts from the source's
/// configured start, as a connection test does.
#[derive(Clone, Default)]
pub struct Cursors {
    feed_id: String,
    db: Option<Arc<Db>>,
}

impl Cursors {
    pub fn new(feed_id: &str, db: Option<Arc<Db>>) -> Self {
        Self {
            feed_id: feed_id.to_string(),
            db,
        }
    }

    /// Whether cursors are kept at all.
    pub fn kept(&self) -> bool {
        self.db.is_some()
    }

    pub fn get(&self, key: &str) -> Result<Option<String>> {
        match &self.db {
            Some(db) => db.cursor(&self.feed_id, key),
            None => Ok(None),
        }
    }

    /// Moves the cursor; nothing happens without a store.
    pub fn set(&self, key: &str, cursor: &str) -> Result<()> {
        match &self.db {
            Some(db) => db.set_cursor(&self.feed_id, key, cursor),
            None => Ok(()),
        }
    }
}

/// An optional capability a connector does not have.
#[derive(Debug)]
pub enum CapabilityError {
    /// The connector does not implement it (yet).
    Unsupported(&'static str),
    /// It ran and failed.
    Failed(anyhow::Error),
}

impl std::fmt::Display for CapabilityError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CapabilityError::Unsupported(what) => write!(f, "this source cannot {what}"),
            CapabilityError::Failed(e) => write!(f, "{e:#}"),
        }
    }
}

impl std::error::Error for CapabilityError {}

impl From<anyhow::Error> for CapabilityError {
    fn from(e: anyhow::Error) -> Self {
        CapabilityError::Failed(e)
    }
}

pub type Capability<T> = std::result::Result<T, CapabilityError>;

/// One kind of source. A pass calls `start`, then `list`, then `fetch` and,
/// once an object's records are in the store, `acknowledge` for each object it
/// takes, then `settle`. `probe` is the connection test; it takes nothing.
#[async_trait]
pub trait Connector: Send + Sync {
    /// Starts whatever listens (the MLLP port). Called before every pass, so it
    /// must be a no-op once started; a failure is reported and retried.
    async fn start(&self) -> Result<()> {
        Ok(())
    }

    /// What a pass would take, oldest first where the source orders.
    async fn list(&self, cursors: &Cursors) -> Result<Vec<Listed>>;

    /// Reads one listed object.
    async fn fetch(&self, key: &str) -> Result<Incoming>;

    /// The object's records are stored: move it out of the way (archived as
    /// `archived_name`) or move the cursor past it. Returns where it is now.
    async fn acknowledge(
        &self,
        key: &str,
        archived_name: &str,
        cursors: &Cursors,
    ) -> Result<String>;

    /// The pass is through: acknowledge what announced the objects it took
    /// (the SQS notifications).
    async fn settle(&self) -> Result<()> {
        Ok(())
    }

    /// Reaches the source once and reports what is there, without taking anything.
    async fn probe(&self, cursors: &Cursors) -> Result<Probe>;

    /// Whether objects stay where they are once taken (someone else's bucket).
    fn read_only(&self) -> bool {
        false
    }

    /// Where `$bulk-submit` can stage files for HFS to fetch from the source's
    /// own store; `None` when the engine serves them itself.
    fn staging(&self) -> Option<&dyn Staging> {
        None
    }

    /// What the source's data looks like (its columns, its resource types).
    async fn describe(&self, _cursors: &Cursors) -> Capability<Value> {
        Err(CapabilityError::Unsupported("describe its data"))
    }

    /// A few records as the next pass would read them, without taking them.
    async fn sample(&self, _cursors: &Cursors, _limit: usize) -> Capability<Vec<Value>> {
        Err(CapabilityError::Unsupported("sample its data"))
    }
}

/// A store the bulk path can hand HFS objects from by pre-signed URL. Each
/// operation has its own conditions (see `object::ObjectSource`): sharing needs
/// a signer, writing a signer and a writable store, cleaning up only a writable
/// store; an operation that does not apply answers `None` or does nothing.
#[async_trait]
pub trait Staging: Send + Sync {
    /// A URL a third party (HFS) can read the object from for `ttl`.
    async fn share(&self, key: &str, ttl: Duration) -> Result<Option<String>>;

    /// Writes a file the engine produced for a submission; its key.
    async fn put_submission_file(
        &self,
        token: &str,
        name: &str,
        bytes: Vec<u8>,
    ) -> Result<Option<String>>;

    /// Writes an entry unpacked from a zip; its key.
    async fn put_unpacked(
        &self,
        container: i64,
        name: &str,
        bytes: &[u8],
    ) -> Result<Option<String>>;

    /// Removes what `put_submission_file` wrote for a submission.
    async fn remove_submission_files(&self, token: &str) -> Result<()>;
}

/// A kind of source in the registry: what `SourceConfig` variant it serves and
/// what it does before a connector exists.
pub struct Kind {
    /// The `type:` in the source file.
    pub name: &'static str,
    /// The record format whatever the file says, for sources that hand over
    /// their own shape (a server, a query, an MLLP sender); `None` when the
    /// file's `format` applies.
    pub format: Option<Format>,
    /// What the type cannot check by itself.
    pub check: fn(&SourceConfig) -> Result<()>,
    /// Makes paths absolute against the directory holding `hie.yaml`.
    pub resolve: fn(&mut SourceConfig, &Path),
    /// Where the source reads from, for logs and the API.
    pub describe: fn(&SourceConfig) -> String,
    /// The connector for a resolved definition. Checks only what building needs
    /// (environment variables, URLs); does not connect.
    pub build: fn(&FeedConfig) -> Result<Box<dyn Connector>>,
}

/// The registry: which module serves a source definition.
pub fn kind(source: &SourceConfig) -> &'static Kind {
    match source {
        SourceConfig::Folder { .. } => &object::FOLDER,
        SourceConfig::S3 { .. } => &object::S3,
        SourceConfig::FhirServer { .. } => &fhir_server::KIND,
        SourceConfig::Database { .. } => &database::KIND,
        SourceConfig::Mllp { .. } => &mllp::KIND,
    }
}

/// Whether an error from a source says the object is not there any more.
pub fn is_gone(e: &anyhow::Error) -> bool {
    e.chain().any(|cause| {
        matches!(
            cause.downcast_ref::<object_store::Error>(),
            Some(object_store::Error::NotFound { .. })
        ) || cause
            .downcast_ref::<std::io::Error>()
            .is_some_and(|io| io.kind() == std::io::ErrorKind::NotFound)
    })
}
