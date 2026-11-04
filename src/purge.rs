//! Failed content's custody over time: content is purged when its retention
//! (`retention.failed_content`) runs out, when its row is written by a retry
//! or reprocess (`Db::mark_sent_many`), and when its source is deleted;
//! counts, groups and events stay (decision 36). Hourly, and on `POST
//! /api/failed-content/purge`.

use std::sync::{Arc, Weak};
use std::time::Duration;

use anyhow::Result;

use crate::config::Config;
use crate::db::{Db, now_ms};

const HOURLY: Duration = Duration::from_secs(3600);

/// Expires the content of rows that failed longer ago than `retention`, or
/// before `before_ms` when given; answers how many rows lost their content.
pub fn expired(db: &Db, retention: Duration, before_ms: Option<i64>) -> Result<u64> {
    let by_retention = now_ms() - retention.as_millis().min(i64::MAX as u128) as i64;
    let before = before_ms.map_or(by_retention, |b| b.max(by_retention));
    let n = db.expire_failed_content(before)?;
    if n > 0 {
        tracing::info!(target: "hie::purge", rows = n, "failed content expired");
    }
    Ok(n)
}

/// A source was deleted: its failed content goes.
pub fn source_deleted(db: &Db, source_id: &str) -> Result<()> {
    let n = db.purge_source_content(source_id)?;
    if n > 0 {
        tracing::info!(target: "hie::purge", source = source_id, rows = n, "failed content purged with its source");
    }
    Ok(())
}

/// Puts the engine's content custody in place on its store: content is sealed
/// with the configured content keys, and expired failed content is purged
/// every hour for as long as the store is open.
pub fn install(db: &Arc<Db>, config: &Config) -> Result<()> {
    db.use_content_keys(Arc::new(config.content_keys()?));
    let retention = config.retention.failed_content;
    if let Ok(handle) = tokio::runtime::Handle::try_current() {
        let store: Weak<Db> = Arc::downgrade(db);
        handle.spawn(async move {
            loop {
                let Some(db) = store.upgrade() else { return };
                if let Err(e) = expired(&db, retention, None) {
                    tracing::error!(target: "hie::purge", "purging failed content: {e:#}");
                }
                drop(db);
                tokio::time::sleep(HOURLY).await;
            }
        });
    }
    Ok(())
}
