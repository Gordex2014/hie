//! Failed content's custody over time: content is purged when its retention
//! (`retention.failed_content`) runs out, when its row is written by a retry
//! or reprocess, and when its source is deleted; counts, groups and events stay
//! (decision 36). Hourly, and on `POST /api/failed-content/purge`.
//!
//! Track H fills it.

use anyhow::Result;

use crate::db::Db;

/// A source was deleted: its failed content goes. Stub until track H keeps
/// content.
pub fn source_deleted(db: &Db, source_id: &str) -> Result<()> {
    let _ = (db, source_id);
    Ok(())
}
