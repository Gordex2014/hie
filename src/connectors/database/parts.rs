//! A database source named by its parts (0.2.0): its connection test, describe
//! and sample, and the connector a running source reads through.
//!
//! A table read (`read.mode: table`) follows a keyset on (`new_rows_by`,
//! `key`...), kept as the order columns' values of the last row stored; a
//! query read (`read.mode: query`) is the 0.1 query with its typed cursor.
//! Every operation opens its own session ([`Endpoint::open`], which checks
//! admission first) and closes it when done.

use std::sync::Mutex;
use std::time::Instant;

use anyhow::{Context as _, Result, anyhow, bail};
use async_trait::async_trait;
use chrono::Utc;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use super::cursor::{Cursor, DECODING, instant};
use super::endpoint::{Bind, DbError, Endpoint, Engine, Session, Stage};
use super::table::{self, Plan, TableInfo};
use crate::config::{DatabaseRead, TableName};
use crate::connectors::{Connector, Context, Cursors, Incoming, Listed, Probe};

/// Rows per run of a table read.
const PAGE: usize = 1000;
/// The one object a pass of a database source takes.
const KEY: &str = "rows";
/// Where a table read keeps its position.
const KEYSET: &str = "keyset";

/// The outcome of one check of a Test.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Check {
    Passed,
    /// With a secret-safe sentence.
    Failed(String),
    /// An earlier check failed.
    NotVerified,
}

/// What a Test of a connection found.
#[derive(Debug, Clone)]
pub struct TestReport {
    pub connect: Check,
    pub authenticate: Check,
    /// `None` when the definition has no read.
    pub read: Option<Check>,
    pub tables: Vec<TableName>,
    /// The read's row count, when it could be counted.
    pub rows: Option<i64>,
    /// A table read's table's primary key, as found.
    pub primary_key: Option<Vec<String>>,
    pub elapsed_ms: u64,
}

impl TestReport {
    pub fn passed(&self) -> bool {
        self.connect == Check::Passed
            && self.authenticate == Check::Passed
            && self.read.as_ref().is_none_or(|r| *r == Check::Passed)
    }
}

/// Checks a text before a driver's message is kept in it.
fn cleaner(ctx: &Context) -> impl Fn(&str) -> bool + Sync + '_ {
    move |text: &str| ctx.secrets.as_ref().is_none_or(|s| s.is_clean(text))
}

/// Connects, signs in and runs the read once; nothing is taken.
pub async fn test(endpoint: &Endpoint, read: Option<&DatabaseRead>, ctx: &Context) -> TestReport {
    let started = Instant::now();
    let mut report = TestReport {
        connect: Check::NotVerified,
        authenticate: Check::NotVerified,
        read: read.map(|_| Check::NotVerified),
        tables: vec![],
        rows: None,
        primary_key: None,
        elapsed_ms: 0,
    };
    let mut session = match endpoint.open(ctx).await {
        Ok(s) => s,
        Err(e) => {
            match e.stage {
                Stage::Connect => report.connect = Check::Failed(e.message),
                _ => {
                    report.connect = Check::Passed;
                    report.authenticate = Check::Failed(e.message);
                }
            }
            report.elapsed_ms = started.elapsed().as_millis() as u64;
            return report;
        }
    };
    report.connect = Check::Passed;
    report.authenticate = Check::Passed;
    let clean = cleaner(ctx);
    match table::tables(&mut session, &clean).await {
        Ok(tables) => report.tables = tables,
        Err(e) => {
            report.read = Some(Check::Failed(e.message));
            session.close().await;
            report.elapsed_ms = started.elapsed().as_millis() as u64;
            return report;
        }
    }
    if let Some(read) = read {
        report.read = Some(
            match check_read(&mut session, read, &clean, &mut report).await {
                Ok(()) => Check::Passed,
                Err(e) => Check::Failed(e.message),
            },
        );
    }
    session.close().await;
    report.elapsed_ms = started.elapsed().as_millis() as u64;
    report
}

async fn check_read(
    session: &mut Session,
    read: &DatabaseRead,
    clean: &(dyn Fn(&str) -> bool + Sync),
    report: &mut TestReport,
) -> Result<(), DbError> {
    match read {
        DatabaseRead::Table {
            table: name,
            key,
            new_rows_by,
        } => {
            let info = described(session, name, clean).await?;
            report.primary_key = Some(info.primary_key.clone());
            let key = if key.is_empty() {
                &info.primary_key
            } else {
                key
            };
            let Some(column) = info.column(new_rows_by) else {
                return Err(DbError::new(
                    Stage::Read,
                    format!("the table has no column {new_rows_by:?}"),
                ));
            };
            if let Some(reason) = table::cannot_order(column) {
                return Err(DbError::new(Stage::Read, reason));
            }
            if !key.is_empty() {
                let plan =
                    Plan::new(&info, key, new_rows_by).map_err(|m| DbError::new(Stage::Read, m))?;
                let (sql, binds) = plan.sql(None, 1);
                session.fetch(&sql, &binds, clean).await?;
            }
            report.rows = Some(table::row_count(session, name, clean).await?);
            Ok(())
        }
        DatabaseRead::Query {
            query,
            cursor_start,
            ..
        } => run_query(
            session,
            query,
            cursor_start.as_deref().map(Cursor::parse).as_ref(),
            clean,
        )
        .await
        .map(drop),
    }
}

async fn described(
    session: &mut Session,
    name: &TableName,
    clean: &(dyn Fn(&str) -> bool + Sync),
) -> Result<TableInfo, DbError> {
    table::table_info(session, name, clean)
        .await?
        .ok_or_else(|| {
            let name = table::qualified(session.engine(), name);
            DbError::new(
                Stage::Read,
                format!(
                    "the table {}.{} does not exist",
                    name.schema.unwrap_or_default(),
                    name.name
                ),
            )
        })
}

/// What describe found.
pub enum Described {
    Tables(Vec<TableName>),
    /// One table with its row count; `None` when it does not exist.
    Table(Option<(TableInfo, i64)>),
}

/// The tables, or one table's columns, key and row count.
pub async fn describe(
    endpoint: &Endpoint,
    table: Option<&TableName>,
    ctx: &Context,
) -> Result<Described, DbError> {
    let mut session = endpoint.open(ctx).await?;
    let clean = cleaner(ctx);
    let result = async {
        match table {
            None => Ok(Described::Tables(
                table::tables(&mut session, &clean).await?,
            )),
            Some(name) => match table::table_info(&mut session, name, &clean).await? {
                None => Ok(Described::Table(None)),
                Some(info) => {
                    let rows = table::row_count(&mut session, name, &clean).await?;
                    Ok(Described::Table(Some((info, rows))))
                }
            },
        }
    }
    .await;
    session.close().await;
    result
}

/// The first `limit` rows of a read, in its order.
pub async fn sample(
    endpoint: &Endpoint,
    read: &DatabaseRead,
    limit: usize,
    ctx: &Context,
) -> Result<Vec<Value>, DbError> {
    let mut session = endpoint.open(ctx).await?;
    let clean = cleaner(ctx);
    let result = async {
        let rows = match read {
            DatabaseRead::Table {
                table: name,
                key,
                new_rows_by,
            } => {
                let info = described(&mut session, name, &clean).await?;
                let key = if key.is_empty() {
                    info.primary_key.clone()
                } else {
                    key.clone()
                };
                let plan = Plan::new(&info, &key, new_rows_by)
                    .map_err(|m| DbError::new(Stage::Read, m))?;
                let (sql, binds) = plan.sql(None, limit);
                session
                    .fetch(&sql, &binds, &clean)
                    .await?
                    .into_iter()
                    .map(|row| plan.split(row).0)
                    .collect()
            }
            DatabaseRead::Query {
                query,
                cursor_start,
                ..
            } => {
                let mut rows = run_query(
                    &mut session,
                    query,
                    cursor_start.as_deref().map(Cursor::parse).as_ref(),
                    &clean,
                )
                .await?;
                rows.truncate(limit);
                rows
            }
        };
        Ok(rows.into_iter().map(Value::Object).collect())
    }
    .await;
    session.close().await;
    result
}

/// One run of a 0.1-style query past `cursor` (the lowest integer when none).
async fn run_query(
    session: &mut Session,
    query: &str,
    cursor: Option<&Cursor>,
    clean: &(dyn Fn(&str) -> bool + Sync),
) -> Result<Vec<Map<String, Value>>, DbError> {
    let engine = session.engine();
    let sql = query.replace("{cursor}", &engine.placeholder(1));
    let cursor = cursor.cloned().unwrap_or(Cursor::Int(i64::MIN));
    let bind = match (&cursor, engine) {
        (Cursor::Int(i), _) => Bind::Int(*i),
        (Cursor::Float(f), _) => Bind::Float(*f),
        (Cursor::Timestamp(t), Engine::Postgres) => instant(t)
            .map(|at| Bind::TimestampTz(at.fixed_offset()))
            .unwrap_or_else(|| Bind::Text(t.clone())),
        (Cursor::Timestamp(t), Engine::SqlServer) => instant(t)
            .map(|at| Bind::Timestamp(at.naive_utc()))
            .unwrap_or_else(|| Bind::Text(t.clone())),
        (Cursor::Date(d), Engine::Postgres | Engine::SqlServer) => {
            chrono::NaiveDate::parse_from_str(d, "%Y-%m-%d")
                .map(Bind::Date)
                .unwrap_or_else(|_| Bind::Text(d.clone()))
        }
        (Cursor::Timestamp(t) | Cursor::Date(t) | Cursor::Text(t), _) => Bind::Text(t.clone()),
    };
    session.fetch(&sql, &[bind], clean).await
}

/// Where a table read left off: the order columns and their values in the
/// last row stored. Another set of columns starts over.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct Position {
    columns: Vec<String>,
    after: Vec<Value>,
    /// How `after` was decoded ([`DECODING`]); 0 when written before it.
    #[serde(default)]
    version: u8,
}

/// A running source over a connection by parts.
pub struct PartsSource {
    feed_id: String,
    endpoint: Endpoint,
    read: DatabaseRead,
    max_runs: usize,
    ctx: Context,
    pulled: Mutex<Option<Pulled>>,
}

struct Pulled {
    bytes: Vec<u8>,
    count: usize,
    /// The cursor key and value to store once the rows are stored.
    cursor: (String, String),
}

impl PartsSource {
    pub fn new(
        feed_id: &str,
        endpoint: Endpoint,
        read: DatabaseRead,
        max_runs: usize,
        ctx: Context,
    ) -> Self {
        Self {
            feed_id: feed_id.to_string(),
            endpoint,
            read,
            max_runs,
            ctx,
            pulled: Mutex::new(None),
        }
    }

    async fn pull(&self, session: &mut Session, cursors: &Cursors) -> Result<Option<Pulled>> {
        let clean = cleaner(&self.ctx);
        let mut bytes = Vec::new();
        let mut count = 0;
        let mut push = |row: &Map<String, Value>| {
            bytes.extend_from_slice(Value::Object(row.clone()).to_string().as_bytes());
            bytes.push(b'\n');
            count += 1;
        };
        let cursor = match &self.read {
            DatabaseRead::Table {
                table: name,
                key,
                new_rows_by,
            } => {
                let info = described(session, name, &clean).await?;
                let key = if key.is_empty() {
                    info.primary_key.clone()
                } else {
                    key.clone()
                };
                let plan = Plan::new(&info, &key, new_rows_by).map_err(|m| anyhow!(m))?;
                let stored = cursors
                    .get(KEYSET)?
                    .and_then(|stored| serde_json::from_str::<Position>(&stored).ok())
                    .filter(|p| p.columns == plan.columns);
                let mut after = match stored {
                    // Written with the old datetimeoffset decoding: rows may
                    // have been skipped anywhere before it.
                    Some(p) if p.version < DECODING && plan.orders_by_datetimeoffset() => {
                        tracing::warn!(
                            target: "hie::database",
                            feed = %self.feed_id,
                            "the stored position was read with the old datetimeoffset decoding; reading the table again from the start"
                        );
                        None
                    }
                    Some(p) => Some(p.after),
                    None => None,
                };
                for _ in 0..self.max_runs {
                    let (sql, binds) = plan.sql(after.as_deref(), PAGE);
                    let rows = session.fetch(&sql, &binds, &clean).await?;
                    let read = rows.len();
                    for row in rows {
                        // The position keeps each order column's exact value.
                        let (row, position) = plan.split(row);
                        push(&row);
                        after = Some(position);
                    }
                    if read < PAGE {
                        break;
                    }
                }
                let position = Position {
                    columns: plan.columns.clone(),
                    after: after.unwrap_or_default(),
                    version: DECODING,
                };
                (KEYSET.to_string(), serde_json::to_string(&position)?)
            }
            DatabaseRead::Query {
                query,
                cursor_column,
                cursor_start,
            } => {
                let stored = match cursors.get(cursor_column)? {
                    Some(stored) => {
                        let sql_server = session.engine() == Engine::SqlServer;
                        let cursor =
                            Cursor::from_stored(&stored, sql_server).context("stored cursor")?;
                        if cursor.is_none() {
                            tracing::warn!(
                                target: "hie::database",
                                feed = %self.feed_id,
                                "the stored cursor was read with the old datetimeoffset decoding; reading again from cursor_start"
                            );
                        }
                        cursor
                    }
                    None => None,
                };
                let mut cursor = stored.or_else(|| cursor_start.as_deref().map(Cursor::parse));
                for _ in 0..self.max_runs {
                    let rows = run_query(session, query, cursor.as_ref(), &clean).await?;
                    if rows.is_empty() {
                        break;
                    }
                    let mut newest = cursor.clone();
                    for row in &rows {
                        let Some(value) = row.get(cursor_column) else {
                            bail!("the query does not return the cursor column {cursor_column:?}");
                        };
                        if let Some(c) = Cursor::from_value(value)
                            && newest.as_ref().is_none_or(|n| c.later_than(n))
                        {
                            newest = Some(c);
                        }
                        push(row);
                    }
                    if newest == cursor {
                        break;
                    }
                    cursor = newest;
                }
                (
                    cursor_column.clone(),
                    cursor.unwrap_or(Cursor::Int(i64::MIN)).stored()?,
                )
            }
        };
        if count == 0 {
            return Ok(None);
        }
        Ok(Some(Pulled {
            bytes,
            count,
            cursor,
        }))
    }
}

#[async_trait]
impl Connector for PartsSource {
    async fn list(&self, cursors: &Cursors) -> Result<Vec<Listed>> {
        let mut session = self.endpoint.open(&self.ctx).await?;
        let pulled = self.pull(&mut session, cursors).await;
        session.close().await;
        let Some(pulled) = pulled? else {
            return Ok(vec![]);
        };
        tracing::debug!(target: "hie::database", feed = %self.feed_id, rows = pulled.count, "pulled");
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

    /// The rows are stored: the position moves past them.
    async fn acknowledge(&self, key: &str, _archived: &str, cursors: &Cursors) -> Result<String> {
        let (name, value) = self
            .pulled
            .lock()
            .expect("pulled mutex")
            .take()
            .filter(|_| key == KEY)
            .map(|p| p.cursor)
            .with_context(|| format!("{key} was not pulled in this pass"))?;
        if cursors.kept() {
            cursors.set(&name, &value)?;
        }
        Ok(format!("{name} {value}"))
    }

    /// Connects and counts what the read holds, without taking anything.
    async fn probe(&self, _cursors: &Cursors) -> Result<Probe> {
        let started = Instant::now();
        let report = test(&self.endpoint, Some(&self.read), &self.ctx).await;
        for check in [&report.connect, &report.authenticate]
            .into_iter()
            .chain(report.read.as_ref())
        {
            if let Check::Failed(message) = check {
                bail!("{message}");
            }
        }
        Ok(Probe {
            reachable: true,
            elapsed_ms: started.elapsed().as_millis() as u64,
            objects: report.rows.unwrap_or(0).max(0) as usize,
            newest: None,
            newest_at: None,
        })
    }
}
