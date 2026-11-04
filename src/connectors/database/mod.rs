//! A SQL database as a source: a query that follows a cursor column. Each pass
//! binds the cursor where the query says `{cursor}`, takes the rows past it, and
//! hands them on as one NDJSON load; the cursor becomes the largest value of the
//! cursor column once the records are stored.
//!
//! `connection` resolves the URL `url_env` names, `driver` holds the connection
//! and runs the query per engine (PostgreSQL, MySQL, SQLite, SQL Server), `rows`
//! decodes their rows, `cursor` is the value followed.
//!
//! The 0.2.0 shape (`connection` by parts, `read` a table or a query) is
//! `parts`: an `endpoint` that opens a session after admission and with the
//! password's revision, `table` for what a table is and how it is read by
//! keyset. Its Test, describe and sample are what `crate::evidence` builds on.

mod connection;
mod cursor;
mod driver;
mod endpoint;
mod parts;
mod rows;
mod table;

pub use driver::url_parts;
pub use endpoint::{DbError, Endpoint, Engine, Stage};
pub use parts::{Check, Described, TestReport};
pub use parts::{
    describe as describe_connection, sample as sample_connection, test as test_connection,
};
pub use table::{ColumnInfo, TableInfo, qualified};

use std::sync::Mutex;
use std::time::Instant;

use anyhow::{Context, Result, bail};
use async_trait::async_trait;
use chrono::Utc;
use serde_json::Value;

use super::{Connector, Context as ConnectContext, Cursors, Incoming, Kind, Listed, Probe};
use crate::config::{DatabaseRead, FeedConfig, Format, SourceConfig};
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
    /// The policy every connection is checked against, before it is opened.
    admission: std::sync::Arc<crate::admission::Admission>,
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
        connection,
        read,
        query,
        cursor_column,
        max_runs,
        ..
    } = source
    else {
        return Ok(());
    };
    if *max_runs == 0 {
        bail!("database: max_runs must be at least 1");
    }
    if connection.is_some() || read.is_some() {
        // The 0.2.0 shape: a connection by its parts and what to read.
        if url_env.is_some() || query.is_some() || cursor_column.is_some() {
            bail!("database: use either connection and read, or url_env, query and cursor_column");
        }
        let Some(connection) = connection else {
            bail!("database: a read needs a connection");
        };
        Endpoint::new(connection, Some(0)).map_err(|e| anyhow::anyhow!("database: {e}"))?;
        if Engine::parse(&connection.engine) == Some(Engine::Sqlite) {
            if connection
                .database
                .as_deref()
                .is_none_or(|d| d.trim().is_empty())
            {
                bail!("database: a sqlite connection names its database file");
            }
        } else if connection
            .host
            .as_deref()
            .is_none_or(|h| h.trim().is_empty())
        {
            bail!("database: connection.host is required");
        }
        match read {
            Some(DatabaseRead::Table {
                table, new_rows_by, ..
            }) => {
                if table.name.trim().is_empty() || new_rows_by.trim().is_empty() {
                    bail!("database: a table read names its table and new_rows_by");
                }
            }
            Some(DatabaseRead::Query {
                query,
                cursor_column,
                ..
            }) => {
                if !query.contains("{cursor}") {
                    bail!("database: the query must use {{cursor}} where the cursor value goes");
                }
                if cursor_column.trim().is_empty() {
                    bail!("database: cursor_column must be a column name");
                }
            }
            None => {}
        }
        return Ok(());
    }
    let (url_env, query, cursor_column) = (
        url_env.as_deref().unwrap_or_default(),
        query.as_deref().unwrap_or_default(),
        cursor_column.as_deref().unwrap_or_default(),
    );
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
    Ok(())
}

fn describe(source: &SourceConfig) -> String {
    match source {
        SourceConfig::Database {
            url_env: Some(url_env),
            cursor_column: Some(cursor_column),
            ..
        } => format!("database ${url_env} by {cursor_column}"),
        SourceConfig::Database {
            connection: Some(c),
            ..
        } => {
            let parts = c.parts();
            match (parts.host, parts.port) {
                (Some(host), Some(port)) => format!(
                    "{} {host}:{port}/{}",
                    parts.engine,
                    parts.database.unwrap_or_default()
                ),
                _ => format!("{} {}", parts.engine, parts.database.unwrap_or_default()),
            }
        }
        _ => String::new(),
    }
}

fn build(feed: &FeedConfig, ctx: &ConnectContext) -> Result<Box<dyn Connector>> {
    let SourceConfig::Database {
        url_env,
        connection,
        read,
        query,
        cursor_column,
        cursor_start,
        max_runs,
        ..
    } = &feed.source
    else {
        bail!("source {} is not a database", feed.id);
    };
    if let Some(connection) = connection {
        let Some(read) = read else {
            bail!("source {}: a database source needs a read", feed.id);
        };
        if connection.password.is_some() && ctx.secrets.as_ref().is_none_or(|s| !s.available()) {
            bail!(
                "source {}: its password is a kept secret, and secrets need custody configured (secrets.key_file or HIE_SECRETS_KEY)",
                feed.id
            );
        }
        let endpoint = Endpoint::new(connection, None)
            .map_err(|e| anyhow::anyhow!("source {}: {e}", feed.id))?;
        return Ok(Box::new(parts::PartsSource::new(
            &feed.id,
            endpoint,
            read.clone(),
            *max_runs,
            ctx.clone(),
        )));
    }
    let (Some(url_env), Some(query), Some(cursor_column)) = (url_env, query, cursor_column) else {
        bail!(
            "source {}: a database source needs url_env, query and cursor_column, or connection and read",
            feed.id
        );
    };
    Ok(Box::new(
        DatabaseSource::new(
            &feed.id,
            url_env,
            query,
            cursor_column,
            cursor_start.clone(),
            *max_runs,
        )?
        .admitted_by(ctx.admission.clone()),
    ))
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
            driver: Driver::new(&url)?,
            admission: Default::default(),
            pulled: Mutex::new(None),
        })
    }

    /// Checks every connection against `admission` before it is opened.
    pub fn admitted_by(mut self, admission: std::sync::Arc<crate::admission::Admission>) -> Self {
        self.admission = admission;
        self
    }

    /// The URL's parts must be admitted before the driver connects.
    fn admit(&self) -> Result<()> {
        if !self.admission.enforced() {
            return Ok(());
        }
        self.admission
            .check(&self.driver.target().parts())
            .map_err(anyhow::Error::from)
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
        self.admit()?;
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
        self.admit()?;
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
