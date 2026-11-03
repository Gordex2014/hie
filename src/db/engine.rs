//! The two engines behind the store: SQLite through rusqlite and PostgreSQL
//! through the synchronous client on a thread of its own; bound values and rows.

use anyhow::{Context, Result};
use postgres::types::{IsNull, ToSql, Type};
use rusqlite::types::ValueRef;
use serde_json::{Map, Value, json};

use super::redact;

// ---- parameters and rows ---------------------------------------------------

/// A bound value. Booleans travel as integers so both engines take them in an
/// integer column.
#[derive(Debug, Clone)]
pub enum Param {
    Null,
    Int(i64),
    Float(f64),
    Text(String),
}

#[allow(clippy::wrong_self_convention)]
pub trait IntoParam {
    fn to_param(&self) -> Param;
}

impl IntoParam for i64 {
    fn to_param(&self) -> Param {
        Param::Int(*self)
    }
}
impl IntoParam for i32 {
    fn to_param(&self) -> Param {
        Param::Int(*self as i64)
    }
}
impl IntoParam for u32 {
    fn to_param(&self) -> Param {
        Param::Int(*self as i64)
    }
}
impl IntoParam for u64 {
    fn to_param(&self) -> Param {
        Param::Int(*self as i64)
    }
}
impl IntoParam for usize {
    fn to_param(&self) -> Param {
        Param::Int(*self as i64)
    }
}
impl IntoParam for f64 {
    fn to_param(&self) -> Param {
        Param::Float(*self)
    }
}
impl IntoParam for bool {
    fn to_param(&self) -> Param {
        Param::Int(*self as i64)
    }
}
impl IntoParam for str {
    fn to_param(&self) -> Param {
        Param::Text(self.to_string())
    }
}
impl IntoParam for String {
    fn to_param(&self) -> Param {
        Param::Text(self.clone())
    }
}
impl<T: IntoParam + ?Sized> IntoParam for &T {
    fn to_param(&self) -> Param {
        (**self).to_param()
    }
}
impl<T: IntoParam> IntoParam for Option<T> {
    fn to_param(&self) -> Param {
        match self {
            Some(v) => v.to_param(),
            None => Param::Null,
        }
    }
}
impl IntoParam for Param {
    fn to_param(&self) -> Param {
        self.clone()
    }
}

/// A NULL that fits any column type, for PostgreSQL.
#[derive(Debug)]
struct AnyNull;

impl ToSql for AnyNull {
    fn to_sql(
        &self,
        _: &Type,
        _: &mut postgres::types::private::BytesMut,
    ) -> std::result::Result<IsNull, Box<dyn std::error::Error + Sync + Send>> {
        Ok(IsNull::Yes)
    }
    fn accepts(_: &Type) -> bool {
        true
    }
    postgres::types::to_sql_checked!();
}

impl Param {
    fn pg(&self) -> Box<dyn ToSql + Sync + Send> {
        match self {
            Param::Null => Box::new(AnyNull),
            Param::Int(i) => Box::new(*i),
            Param::Float(f) => Box::new(*f),
            Param::Text(t) => Box::new(t.clone()),
        }
    }
}

impl rusqlite::ToSql for Param {
    fn to_sql(&self) -> rusqlite::Result<rusqlite::types::ToSqlOutput<'_>> {
        use rusqlite::types::{ToSqlOutput, Value as V};
        Ok(match self {
            Param::Null => ToSqlOutput::Owned(V::Null),
            Param::Int(i) => ToSqlOutput::Owned(V::Integer(*i)),
            Param::Float(f) => ToSqlOutput::Owned(V::Real(*f)),
            Param::Text(t) => ToSqlOutput::Borrowed(ValueRef::Text(t.as_bytes())),
        })
    }
}

/// One result row: the column names and the values, as JSON.
pub struct Row {
    names: Vec<String>,
    values: Vec<Value>,
}

impl Row {
    pub(super) fn get(&self, i: usize) -> &Value {
        self.values.get(i).unwrap_or(&Value::Null)
    }
    pub(super) fn i64(&self, i: usize) -> i64 {
        self.get(i).as_i64().unwrap_or(0)
    }
    pub(super) fn opt_i64(&self, i: usize) -> Option<i64> {
        self.get(i).as_i64()
    }
    pub(super) fn string(&self, i: usize) -> String {
        match self.get(i) {
            Value::String(s) => s.clone(),
            Value::Null => String::new(),
            other => other.to_string(),
        }
    }
    pub(super) fn opt_string(&self, i: usize) -> Option<String> {
        match self.get(i) {
            Value::Null => None,
            Value::String(s) => Some(s.clone()),
            other => Some(other.to_string()),
        }
    }
    pub(super) fn json(&self, i: usize) -> Option<Value> {
        self.opt_string(i)
            .and_then(|s| serde_json::from_str(&s).ok())
    }
    pub(super) fn object(&self) -> Value {
        let mut obj = Map::new();
        for (name, value) in self.names.iter().zip(&self.values) {
            let v = match value {
                Value::String(s)
                    if matches!(
                        name.as_str(),
                        "raw" | "original_raw" | "resources" | "response"
                    ) =>
                {
                    serde_json::from_str(s).unwrap_or(Value::String(s.clone()))
                }
                other => other.clone(),
            };
            obj.insert(name.clone(), v);
        }
        Value::Object(obj)
    }
}

// ---- the two engines --------------------------------------------------------

pub(super) enum Conn {
    Sqlite(rusqlite::Connection),
    Postgres(PgHandle),
}

/// What the PostgreSQL thread is asked to do.
enum PgJob {
    Execute(String, Vec<Param>),
    Query(String, Vec<Param>),
    Batch(String),
}

enum PgReply {
    Count(u64),
    Rows(Vec<Row>),
    Done,
}

/// The PostgreSQL client lives on a thread of its own: the synchronous client
/// drives its own runtime, which cannot run on a tokio worker thread, and the
/// store is called from async tasks. Calls block the caller for the round trip,
/// as a SQLite call does.
pub(super) struct PgHandle {
    jobs: std::sync::mpsc::Sender<(PgJob, std::sync::mpsc::Sender<Result<PgReply>>)>,
}

impl PgHandle {
    pub(super) fn connect(url: &str) -> Result<Self> {
        let (jobs, inbox) =
            std::sync::mpsc::channel::<(PgJob, std::sync::mpsc::Sender<Result<PgReply>>)>();
        let (ready_tx, ready_rx) = std::sync::mpsc::channel::<Result<()>>();
        let url = url.to_string();
        std::thread::Builder::new()
            .name("hie-postgres".into())
            .spawn(move || {
                let mut client = match postgres::Client::connect(&url, postgres::NoTls) {
                    Ok(c) => {
                        let _ = ready_tx.send(Ok(()));
                        c
                    }
                    Err(e) => {
                        let _ = ready_tx.send(Err(anyhow::Error::new(e)
                            .context(format!("connecting to PostgreSQL at {}", redact(&url)))));
                        return;
                    }
                };
                while let Ok((job, reply)) = inbox.recv() {
                    let result = match job {
                        PgJob::Execute(sql, params) => {
                            pg_execute(&mut client, &sql, &params).map(PgReply::Count)
                        }
                        PgJob::Query(sql, params) => {
                            pg_query(&mut client, &sql, &params).map(PgReply::Rows)
                        }
                        PgJob::Batch(sql) => client
                            .batch_execute(&sql)
                            .map(|_| PgReply::Done)
                            .map_err(anyhow::Error::new),
                    };
                    let _ = reply.send(result);
                }
            })
            .context("starting the PostgreSQL thread")?;
        ready_rx
            .recv()
            .context("the PostgreSQL thread went away before connecting")??;
        Ok(Self { jobs })
    }

    fn call(&self, job: PgJob) -> Result<PgReply> {
        let (tx, rx) = std::sync::mpsc::channel();
        self.jobs
            .send((job, tx))
            .map_err(|_| anyhow::anyhow!("the PostgreSQL connection thread has stopped"))?;
        rx.recv()
            .map_err(|_| anyhow::anyhow!("the PostgreSQL connection thread has stopped"))?
    }

    pub(super) fn execute(&self, sql: &str, params: &[Param]) -> Result<u64> {
        match self.call(PgJob::Execute(sql.to_string(), params.to_vec()))? {
            PgReply::Count(n) => Ok(n),
            _ => Ok(0),
        }
    }

    pub(super) fn query(&self, sql: &str, params: &[Param]) -> Result<Vec<Row>> {
        match self.call(PgJob::Query(sql.to_string(), params.to_vec()))? {
            PgReply::Rows(rows) => Ok(rows),
            _ => Ok(vec![]),
        }
    }

    pub(super) fn batch(&self, sql: &str) -> Result<()> {
        self.call(PgJob::Batch(sql.to_string())).map(drop)
    }
}

fn pg_execute(client: &mut postgres::Client, sql: &str, params: &[Param]) -> Result<u64> {
    let boxed: Vec<Box<dyn ToSql + Sync + Send>> = params.iter().map(Param::pg).collect();
    let refs: Vec<&(dyn ToSql + Sync)> = boxed
        .iter()
        .map(|b| b.as_ref() as &(dyn ToSql + Sync))
        .collect();
    Ok(client.execute(sql, refs.as_slice())?)
}

fn pg_query(client: &mut postgres::Client, sql: &str, params: &[Param]) -> Result<Vec<Row>> {
    let boxed: Vec<Box<dyn ToSql + Sync + Send>> = params.iter().map(Param::pg).collect();
    let refs: Vec<&(dyn ToSql + Sync)> = boxed
        .iter()
        .map(|b| b.as_ref() as &(dyn ToSql + Sync))
        .collect();
    let rows = client.query(sql, refs.as_slice())?;
    Ok(rows
        .iter()
        .map(|r| {
            let names: Vec<String> = r.columns().iter().map(|c| c.name().to_string()).collect();
            let values = r
                .columns()
                .iter()
                .enumerate()
                .map(|(i, col)| pg_value(r, i, col.type_()))
                .collect();
            Row { names, values }
        })
        .collect())
}

impl Conn {
    pub(super) fn execute(&mut self, sql: &str, params: &[Param]) -> Result<u64> {
        match self {
            Conn::Sqlite(c) => {
                let sql = sql.replace('$', "?");
                let refs: Vec<&dyn rusqlite::ToSql> =
                    params.iter().map(|p| p as &dyn rusqlite::ToSql).collect();
                Ok(c.execute(&sql, refs.as_slice())? as u64)
            }
            Conn::Postgres(h) => h.execute(sql, params),
        }
    }

    pub(super) fn query(&mut self, sql: &str, params: &[Param]) -> Result<Vec<Row>> {
        match self {
            Conn::Sqlite(c) => {
                let sql = sql.replace('$', "?");
                let mut stmt = c.prepare(&sql)?;
                let names: Vec<String> =
                    stmt.column_names().iter().map(|s| s.to_string()).collect();
                let refs: Vec<&dyn rusqlite::ToSql> =
                    params.iter().map(|p| p as &dyn rusqlite::ToSql).collect();
                let rows = stmt.query_map(refs.as_slice(), |r| {
                    let mut values = Vec::with_capacity(names.len());
                    for i in 0..names.len() {
                        values.push(match r.get_ref(i)? {
                            ValueRef::Null => Value::Null,
                            ValueRef::Integer(n) => json!(n),
                            ValueRef::Real(f) => json!(f),
                            ValueRef::Text(t) => {
                                Value::String(String::from_utf8_lossy(t).into_owned())
                            }
                            ValueRef::Blob(_) => Value::Null,
                        });
                    }
                    Ok(Row {
                        names: names.clone(),
                        values,
                    })
                })?;
                Ok(rows.collect::<rusqlite::Result<_>>()?)
            }
            Conn::Postgres(h) => h.query(sql, params),
        }
    }

    pub(super) fn batch(&mut self, sql: &str) -> Result<()> {
        match self {
            Conn::Sqlite(c) => Ok(c.execute_batch(sql)?),
            Conn::Postgres(h) => h.batch(sql),
        }
    }
}

fn pg_value(row: &postgres::Row, i: usize, kind: &Type) -> Value {
    match *kind {
        Type::INT8 => row
            .try_get::<_, Option<i64>>(i)
            .ok()
            .flatten()
            .map_or(Value::Null, Value::from),
        Type::INT4 => row
            .try_get::<_, Option<i32>>(i)
            .ok()
            .flatten()
            .map_or(Value::Null, |v| Value::from(v as i64)),
        Type::INT2 => row
            .try_get::<_, Option<i16>>(i)
            .ok()
            .flatten()
            .map_or(Value::Null, |v| Value::from(v as i64)),
        Type::FLOAT8 => row
            .try_get::<_, Option<f64>>(i)
            .ok()
            .flatten()
            .map_or(Value::Null, Value::from),
        Type::FLOAT4 => row
            .try_get::<_, Option<f32>>(i)
            .ok()
            .flatten()
            .map_or(Value::Null, |v| Value::from(v as f64)),
        Type::BOOL => row
            .try_get::<_, Option<bool>>(i)
            .ok()
            .flatten()
            .map_or(Value::Null, Value::Bool),
        Type::NUMERIC => row
            .try_get::<_, Option<rust_decimal::Decimal>>(i)
            .ok()
            .flatten()
            .map_or(Value::Null, |d| {
                serde_json::from_str(&d.to_string()).unwrap_or(Value::String(d.to_string()))
            }),
        _ => row
            .try_get::<_, Option<String>>(i)
            .ok()
            .flatten()
            .map_or(Value::Null, Value::String),
    }
}
