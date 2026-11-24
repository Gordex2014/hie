//! How long the engine keeps its history: one setting, `retention.history`, for
//! every kind of history it purges. Records, loads and submissions are not
//! history and are never purged here.
//!
//! Each kind of history has its own `Db::purge_*` and its own field in
//! `Purged`: events, alerts (their delivery state goes with them), and the
//! checks `check-now` records.

use std::sync::Weak;
use std::time::Duration;

use anyhow::Result;

use crate::config::RetentionConfig;
use crate::db::{Db, now_ms};

/// Rows removed per transaction.
pub const CHUNK: usize = 5_000;

/// How often the background purge runs, after the one at start.
pub const EVERY: Duration = Duration::from_secs(3600);

/// The retention a running engine applies.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Retention {
    history: Duration,
}

impl Retention {
    pub fn from_config(config: &RetentionConfig) -> Self {
        Self {
            history: config.history(),
        }
    }

    /// How long events, alerts and checks are kept.
    pub fn history(&self) -> Duration {
        self.history
    }

    /// History older than this instant, in epoch milliseconds, is purged.
    pub fn cutoff(&self, now_ms: i64) -> i64 {
        now_ms.saturating_sub(self.history.as_millis().min(i64::MAX as u128) as i64)
    }
}

/// What one purge removed, per kind of history.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Purged {
    pub events: usize,
    pub alerts: usize,
    pub checks: usize,
}

/// Removes the history older than `cutoff_ms`, in chunks of `CHUNK` rows, one
/// transaction each. Only what a statement removed is counted, so two purges
/// running at once never count a row twice.
pub fn purge(db: &Db, cutoff_ms: i64) -> Result<Purged> {
    Ok(Purged {
        events: in_chunks(|| db.purge_events(cutoff_ms, CHUNK))?,
        alerts: in_chunks(|| db.purge_alerts(cutoff_ms, CHUNK))?,
        checks: in_chunks(|| db.purge_checks(cutoff_ms, CHUNK))?,
    })
}

fn in_chunks(mut chunk: impl FnMut() -> Result<usize>) -> Result<usize> {
    let mut total = 0;
    loop {
        let n = chunk()?;
        total += n;
        if n < CHUNK {
            return Ok(total);
        }
    }
}

/// Purges at start and then every `EVERY`, on a blocking thread, until the
/// store is dropped. A purge that fails is logged and tried again next time.
pub fn spawn(db: Weak<Db>, retention: Retention) {
    tokio::spawn(async move {
        loop {
            let Some(store) = db.upgrade() else {
                return;
            };
            let cutoff = retention.cutoff(now_ms());
            match tokio::task::spawn_blocking(move || purge(&store, cutoff)).await {
                Ok(Ok(purged)) if purged != Purged::default() => {
                    tracing::info!(
                        events = purged.events,
                        alerts = purged.alerts,
                        checks = purged.checks,
                        "history older than the retention purged"
                    );
                }
                Ok(Ok(_)) => {}
                Ok(Err(e)) => tracing::error!("purging history failed, will retry: {e:#}"),
                Err(e) => tracing::error!("purging history failed, will retry: {e}"),
            }
            tokio::time::sleep(EVERY).await;
        }
    });
}
