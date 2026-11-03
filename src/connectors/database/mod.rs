//! A SQL database as a source: a query that follows a cursor column. Each pass
//! binds the cursor where the query says `{cursor}`, takes the rows past it, and
//! hands them on as one NDJSON load; the cursor becomes the largest value of the
//! cursor column once the records are stored.
//!
//! `connection` resolves the URL `url_env` names, `driver` holds the connection
//! and runs the query per engine (PostgreSQL, MySQL, SQLite, SQL Server), `rows`
//! decodes their rows, `cursor` is the value followed.

mod connection;
mod cursor;
mod driver;
mod rows;

use std::sync::Mutex;
use std::time::Instant;

use anyhow::{Context, Result, bail};
use async_trait::async_trait;
use chrono::Utc;
use serde_json::Value;

use super::{Connector, Cursors, Incoming, Kind, Listed, Probe};
use crate::config::{FeedConfig, Format, SourceConfig};
use cursor::{Cursor, instant};
use driver::Driver;

/// The database kind in the registry: its records are NDJSON rows whatever the
/// source file says.
pub(super) const KIND: Kind = Kind {
    name: "database",
    format: Some(Format::Ndjson),
    check,
    resolve: |_, _| {},
    describe,
    build,
};

/// The one key a database source has: its query. The cursor is stored under the
/// cursor column's name instead, so changing the column starts over from
/// `cursor_start` rather than binding a value of the wrong kind.
pub const KEY: &str = "query";

pub struct DatabaseSource {
    feed_id: String,
    query: String,
    cursor_column: String,
    cursor_start: Option<String>,
    max_runs: usize,
    driver: Driver,
    pulled: Mutex<Option<Pulled>>,
}

struct Pulled {
    bytes: Vec<u8>,
    count: usize,
    newest: Cursor,
}

/// What the type itself cannot check: a URL variable, a query with `{cursor}`,
/// a column name, at least one run.
fn check(source: &SourceConfig) -> Result<()> {
    let SourceConfig::Database {
        url_env,
        query,
        cursor_column,
        max_runs,
        ..
    } = source
    else {
        return Ok(());
    };
    if url_env.trim().is_empty() {
        bail!("database: url_env names the environment variable holding the connection URL");
    }
    if !query.contains("{cursor}") {
        bail!(
            "database: the query must use {{cursor}} where the cursor value goes, e.g. WHERE updated_at > {{cursor}}"
        );
    }
    if cursor_column.trim().is_empty()
        || !cursor_column
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_')
    {
        bail!("database: cursor_column must be a column name");
    }
    if *max_runs == 0 {
        bail!("database: max_runs must be at least 1");
    }
    Ok(())
}

fn describe(source: &SourceConfig) -> String {
    match source {
        SourceConfig::Database {
            url_env,
            cursor_column,
            ..
        } => format!("database ${url_env} by {cursor_column}"),
        _ => String::new(),
    }
}

fn build(feed: &FeedConfig) -> Result<Box<dyn Connector>> {
    let SourceConfig::Database {
        url_env,
        query,
        cursor_column,
        cursor_start,
        max_runs,
        ..
    } = &feed.source
    else {
        bail!("source {} is not a database", feed.id);
    };
    Ok(Box::new(DatabaseSource::new(
        &feed.id,
        url_env,
        query,
        cursor_column,
        cursor_start.clone(),
        *max_runs,
    )?))
}

impl DatabaseSource {
    pub fn new(
        feed_id: &str,
        url_env: &str,
        query: &str,
        cursor_column: &str,
        cursor_start: Option<String>,
        max_runs: usize,
    ) -> Result<Self> {
        let url = connection::url(url_env)?;
        Ok(Self {
            feed_id: feed_id.to_string(),
            query: query.to_string(),
            cursor_column: cursor_column.to_string(),
            cursor_start,
            max_runs,
            driver: Driver::new(url),
            pulled: Mutex::new(None),
        })
    }

    /// Where the query starts: the stored cursor, else `cursor_start`.
    fn cursor(&self, cursors: &Cursors) -> Result<Option<Cursor>> {
        if let Some(stored) = cursors.get(&self.cursor_column)? {
            return Ok(Some(
                serde_json::from_str(&stored).context("stored cursor")?,
            ));
        }
        Ok(self.cursor_start.as_deref().map(Cursor::parse))
    }

    /// Runs the query past the cursor, moving an in-memory cursor between runs,
    /// until a run brings nothing new or `max_runs` is reached.
    async fn pull(&self, cursors: &Cursors) -> Result<Pulled> {
        let mut cursor = self.cursor(cursors)?;
        let mut bytes = Vec::new();
        let mut count = 0;
        for _ in 0..self.max_runs {
            let rows = self.driver.run(&self.query, cursor.as_ref()).await?;
            if rows.is_empty() {
                break;
            }
            let mut newest = cursor.clone();
            for row in &rows {
                let Some(value) = row.get(&self.cursor_column) else {
                    bail!(
                        "the query does not return the cursor column {:?} (columns: {})",
                        self.cursor_column,
                        row.keys().cloned().collect::<Vec<_>>().join(", ")
                    );
                };
                if let Some(c) = Cursor::from_value(value)
                    && newest.as_ref().is_none_or(|n| c.later_than(n))
                {
                    newest = Some(c);
                }
                bytes.extend_from_slice(Value::Object(row.clone()).to_string().as_bytes());
                bytes.push(b'\n');
                count += 1;
            }
            if newest == cursor {
                // Every row sits at the cursor: the column is not moving, so
                // another run would bring the same rows again.
                break;
            }
            if rows.len() >= 2
                && rows.iter().all(|r| {
                    r.get(&self.cursor_column)
                        .and_then(Cursor::from_value)
                        .as_ref()
                        == newest.as_ref()
                })
            {
                // A run whose rows all share one cursor value may have been cut by
                // LIMIT in the middle of that value; the rest would never be read.
                tracing::warn!(
                    target: "hie::database",
                    feed = %self.feed_id,
                    cursor = %newest.as_ref().map(Cursor::text).unwrap_or_default(),
                    "every row of a run shares the cursor value; rows beyond LIMIT with that value are skipped. Use a unique cursor column (an id) or a finer timestamp"
                );
            }
            cursor = newest;
        }
        Ok(Pulled {
            bytes,
            count,
            newest: cursor.unwrap_or(Cursor::Int(i64::MIN)),
        })
    }
}

#[async_trait]
impl Connector for DatabaseSource {
    async fn list(&self, cursors: &Cursors) -> Result<Vec<Listed>> {
        let pulled = self.pull(cursors).await?;
        if pulled.count == 0 {
            return Ok(vec![]);
        }
        tracing::debug!(target: "hie::database", feed = %self.feed_id, rows = pulled.count, newest = %pulled.newest.text(), "pulled");
        *self.pulled.lock().expect("pulled mutex") = Some(pulled);
        Ok(vec![Listed::from_key(KEY)])
    }

    async fn fetch(&self, key: &str) -> Result<Incoming> {
        let pulled = self.pulled.lock().expect("pulled mutex");
        let p = pulled
            .as_ref()
            .filter(|_| key == KEY)
            .with_context(|| format!("{key} was not pulled in this pass"))?;
        Ok(Incoming {
            key: KEY.to_string(),
            name: format!(
                "{}-{}.ndjson",
                self.feed_id,
                Utc::now().format("%Y%m%dT%H%M%SZ")
            ),
            bytes: p.bytes.clone(),
        })
    }

    /// The records are stored: the cursor may move past them.
    async fn acknowledge(
        &self,
        key: &str,
        _archived_name: &str,
        cursors: &Cursors,
    ) -> Result<String> {
        let newest = self
            .pulled
            .lock()
            .expect("pulled mutex")
            .take()
            .filter(|_| key == KEY)
            .map(|p| p.newest)
            .with_context(|| format!("{key} was not pulled in this pass"))?;
        if cursors.kept() {
            cursors.set(&self.cursor_column, &serde_json::to_string(&newest)?)?;
        }
        Ok(format!("cursor {}", newest.text()))
    }

    /// Connects and runs the query once past the cursor: what waits, and the
    /// newest cursor value among it.
    async fn probe(&self, cursors: &Cursors) -> Result<Probe> {
        let started = Instant::now();
        let cursor = self.cursor(cursors)?;
        let rows = self.driver.run(&self.query, cursor.as_ref()).await?;
        let newest = rows
            .iter()
            .filter_map(|r| r.get(&self.cursor_column))
            .filter_map(Cursor::from_value)
            .fold(None::<Cursor>, |best, c| match best {
                Some(b) if !c.later_than(&b) => Some(b),
                _ => Some(c),
            });
        Ok(Probe {
            reachable: true,
            elapsed_ms: started.elapsed().as_millis() as u64,
            objects: rows.len(),
            newest: newest
                .as_ref()
                .map(|c| format!("{} = {}", self.cursor_column, c.text())),
            newest_at: newest.and_then(|c| match c {
                Cursor::Timestamp(t) => instant(&t).map(|i| i.to_rfc3339()),
                Cursor::Date(d) => Some(d),
                _ => None,
            }),
        })
    }
}
