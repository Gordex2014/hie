//! The immutable history: every attempt on a row as an event, retries as
//! records of their own, and what the usage and period counts are built from
//! (decision 42).
//!
//! Track H fills it. Its DDL and added columns are already registered in
//! `db/mod.rs` (`TABLES`, `ADDED_COLUMNS`), so adding a table or a column here
//! needs no change elsewhere.

use anyhow::Result;
use serde::Serialize;

use super::Db;

/// The DDL of this area's tables (`CREATE TABLE IF NOT EXISTS ...`).
pub(super) const TABLES: &str = "";

/// Columns added to this area's tables after a store was first created.
pub(super) const ADDED_COLUMNS: &[(&str, &str, &str)] = &[];

/// One load or retry that ran a transform version, as `GET
/// /api/transforms/{id}/usage` lists it under `runs`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct VersionRun {
    /// `load` or `retry`.
    pub kind: String,
    /// The load's id as text, or the retry's `rt_` id.
    pub id: String,
    pub source: String,
    pub load: i64,
    pub rows: i64,
    pub written: i64,
    pub failed: i64,
    /// `running`, `written`, `failed`, `partial` or `outcome_unknown`.
    pub outcome: String,
    /// RFC 3339 with milliseconds (`crate::ids::rfc3339_ms`).
    pub started_at: String,
    pub finished_at: Option<String>,
}

impl Db {
    /// Every load and retry that ran `version` of `transform_id`, newest first,
    /// at most `limit`; built from the immutable history, so it outlives the
    /// source and the transform.
    ///
    /// Stub: answers nothing until track H records the history (read by track
    /// V's usage route).
    pub fn version_runs(
        &self,
        transform_id: &str,
        version: i64,
        limit: usize,
    ) -> Result<Vec<VersionRun>> {
        let _ = (transform_id, version, limit);
        Ok(Vec::new())
    }
}
