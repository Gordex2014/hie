//! Connection evidence: what a Test of a database source established (its
//! checks, the credential revision it used, the rows it counted), describe and
//! sample, and the binding of a Start to its evidence (decisions 36 to 38).
//!
//! Track S fills it. The signatures below are fixed; other areas call them.

use std::collections::HashMap;
use std::sync::Mutex;

use anyhow::Result;
use serde_json::Value;

use crate::config::FeedConfig;
use crate::db::Db;

/// Samples taken from a source (`smp_` ids), kept in memory for a while so
/// Try can run on them (`input: {"sample": ...}`); never stored.
#[derive(Default)]
pub struct SampleRegistry {
    samples: Mutex<HashMap<String, Vec<Value>>>,
}

impl SampleRegistry {
    /// The rows of a sample, while it is kept.
    ///
    /// Stub: nothing is kept until track S takes samples.
    pub fn get(&self, sample_id: &str) -> Option<Vec<Value>> {
        self.samples
            .lock()
            .expect("sample registry")
            .get(sample_id)
            .cloned()
    }
}

/// A source's definition was replaced: the evidence and secret revisions the
/// old definition held are released. Stub until track S keeps evidence.
pub fn source_replaced(db: &Db, before: &FeedConfig, after: &FeedConfig) -> Result<()> {
    let _ = (db, before, after);
    Ok(())
}

/// A source was deleted: the evidence and secret revisions it held are
/// released. Stub until track S keeps evidence.
pub fn source_deleted(db: &Db, source_id: &str) -> Result<()> {
    let _ = (db, source_id);
    Ok(())
}
