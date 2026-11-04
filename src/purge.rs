//! Failed content's custody over time: content is purged when its retention
//! (`retention.failed_content`) runs out, when its row is written by a retry
//! or reprocess (`Db::mark_sent_many`), and when its source is deleted;
//! counts, groups and events stay (decision 36). Hourly, and on `POST
//! /api/failed-content/purge`.
//!
//! The files the engine writes for the destination to fetch (`$bulk-submit`
//! NDJSON and staged zip entries under `bulk_dir`, sealed) go once their
//! submission settles or is refused (`pipeline::submit`), when their source is
//! deleted, and when they outlive the retention.

use std::path::Path;
use std::sync::{Arc, Weak};
use std::time::{Duration, SystemTime};

use anyhow::Result;

use crate::config::Config;
use crate::db::{Db, now_ms};

const HOURLY: Duration = Duration::from_secs(3600);

/// Expires the content of rows that failed longer ago than `retention`, or
/// before `before_ms` when given; answers how many rows lost their content.
/// Served files older than the retention go too.
pub fn expired(db: &Db, retention: Duration, before_ms: Option<i64>) -> Result<u64> {
    let by_retention = now_ms() - retention.as_millis().min(i64::MAX as u128) as i64;
    let before = before_ms.map_or(by_retention, |b| b.max(by_retention));
    let n = db.expire_failed_content(before)?;
    if n > 0 {
        tracing::info!(target: "hie::purge", rows = n, "failed content expired");
    }
    if let Err(e) = expire_served_files(db, retention) {
        tracing::warn!(target: "hie::purge", "removing old served files: {e:#}");
    }
    Ok(n)
}

/// A source was deleted: its failed content goes, and the files the engine
/// was serving for its submissions and staged zip entries.
pub fn source_deleted(db: &Db, source_id: &str) -> Result<()> {
    let n = db.purge_source_content(source_id)?;
    if n > 0 {
        tracing::info!(target: "hie::purge", source = source_id, rows = n, "failed content purged with its source");
    }
    if let Some(dir) = db.served_dir() {
        for (token, _, _) in db.submission_tokens(Some(source_id))? {
            remove_dir(&dir.join(&token));
        }
        for load in db.source_load_ids(source_id)? {
            remove_dir(&dir.join("unpacked").join(load.to_string()));
        }
    }
    Ok(())
}

/// Files under `bulk_dir` that no open submission uses and that are older
/// than the retention.
fn expire_served_files(db: &Db, retention: Duration) -> Result<()> {
    let Some(dir) = db.served_dir() else {
        return Ok(());
    };
    let open: Vec<String> = db
        .submission_tokens(None)?
        .into_iter()
        .filter(|(_, status, _)| matches!(status.as_str(), "kickoff" | "running"))
        .map(|(token, _, _)| token)
        .collect();
    let old = |path: &Path| {
        path.metadata()
            .and_then(|m| m.modified())
            .ok()
            .and_then(|t| SystemTime::now().duration_since(t).ok())
            .is_some_and(|age| age > retention)
    };
    let Ok(entries) = std::fs::read_dir(&dir) else {
        return Ok(());
    };
    for entry in entries.flatten() {
        let path = entry.path();
        let name = entry.file_name().to_string_lossy().into_owned();
        if name == "unpacked" {
            for staged in std::fs::read_dir(&path).into_iter().flatten().flatten() {
                if old(&staged.path()) {
                    remove_dir(&staged.path());
                }
            }
        } else if path.is_dir() && !open.contains(&name) && old(&path) {
            remove_dir(&path);
        }
    }
    Ok(())
}

fn remove_dir(path: &Path) {
    if path.is_dir()
        && let Err(e) = std::fs::remove_dir_all(path)
    {
        tracing::warn!(target: "hie::purge", "cannot remove {}: {e}", path.display());
    }
}

/// Puts the engine's content custody in place on its store: content is sealed
/// with the configured content keys; rows from before custody are adopted
/// (grouped, sealed) in the background; expired failed content is purged
/// every hour for as long as the store is open.
pub fn install(db: &Arc<Db>, config: &Config) -> Result<()> {
    db.use_content_keys(Arc::new(config.content_keys()?));
    db.use_served_dir(&config.bulk_dir());
    let retention = config.retention.failed_content;
    if let Ok(handle) = tokio::runtime::Handle::try_current() {
        let store: Weak<Db> = Arc::downgrade(db);
        handle.spawn(async move {
            if let Some(db) = store.upgrade() {
                match tokio::task::spawn_blocking(move || db.adopt_legacy_rows()).await {
                    Ok(Ok((grouped, sealed))) if grouped + sealed > 0 => {
                        tracing::info!(target: "hie::purge", grouped, sealed, "rows from before custody adopted");
                    }
                    Ok(Ok(_)) => {}
                    Ok(Err(e)) => {
                        tracing::error!(target: "hie::purge", "adopting rows from before custody: {e:#}")
                    }
                    Err(e) => tracing::error!(target: "hie::purge", "adopting rows: {e}"),
                }
            }
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
