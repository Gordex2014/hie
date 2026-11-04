//! A table as a session sees it (its tables, columns, primary key and row
//! count, per engine), and a table read by keyset: rows ordered by
//! (`new_rows_by`, `key`...), each run taking the rows strictly past the last
//! one read on that whole tuple, so rows that share a `new_rows_by` value are
//! never skipped at a run's boundary.

use chrono::{DateTime, NaiveDate, NaiveDateTime, Utc};
use serde_json::{Map, Value};

use super::endpoint::{Bind, DbError, Engine, Session};
use crate::config::TableName;

/// What a column's type is, for binding a value of it and for choosing what
/// may say which rows are new.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Class {
    Int,
    Float,
    Decimal,
    Timestamp,
    TimestampTz,
    Date,
    Time,
    Text,
}

impl Class {
    /// The class of an engine's type name.
    pub fn of(engine: Engine, type_name: &str) -> Self {
        let t = type_name.to_ascii_lowercase();
        if engine == Engine::Sqlite {
            // SQLite's affinity rules, by the declared type.
            return if t.contains("int") {
                Class::Int
            } else if t.contains("char") || t.contains("clob") || t.contains("text") {
                Class::Text
            } else if t.contains("real") || t.contains("floa") || t.contains("doub") {
                Class::Float
            } else if t.contains("dec") || t.contains("num") {
                Class::Decimal
            } else if t.contains("datetime") || t.contains("timestamp") {
                Class::Timestamp
            } else if t == "date" {
                Class::Date
            } else if t.contains("time") {
                Class::Time
            } else {
                Class::Text
            };
        }
        if t == "timestamp" && engine == Engine::SqlServer {
            // SQL Server's `timestamp` is a row version, not a time.
            return Class::Text;
        }
        if t.contains("with time zone") || t == "timestamptz" || t == "datetimeoffset" {
            Class::TimestampTz
        } else if t.contains("timestamp") || t.contains("datetime") {
            Class::Timestamp
        } else if t == "date" {
            Class::Date
        } else if t.starts_with("time") {
            Class::Time
        } else if t.contains("int") || t.contains("serial") {
            Class::Int
        } else if t.contains("decimal") || t.contains("numeric") || t.contains("money") {
            Class::Decimal
        } else if t.contains("real") || t.contains("float") || t.contains("double") {
            Class::Float
        } else {
            Class::Text
        }
    }

    /// Integer, decimal, date or time types: what may say which rows are new.
    pub fn orders_new_rows(self) -> bool {
        !matches!(self, Class::Text)
    }

    /// A value read from a column of this class, to bind back.
    pub fn bind(self, engine: Engine, value: &Value) -> Bind {
        if engine == Engine::Sqlite {
            return by_json(value);
        }
        match (self, value) {
            (_, Value::Null) => Bind::Null,
            (Class::Int, Value::Number(n)) if n.is_i64() => Bind::Int(n.as_i64().unwrap_or(0)),
            (Class::Decimal, Value::Number(n)) => Bind::Decimal(n.to_string()),
            (Class::Decimal, Value::String(s)) => Bind::Decimal(s.clone()),
            (Class::Timestamp, Value::String(s)) => naive(s)
                .map(Bind::Timestamp)
                .unwrap_or_else(|| Bind::Text(s.clone())),
            (Class::TimestampTz, Value::String(s)) => DateTime::parse_from_rfc3339(s)
                .map(|t| Bind::TimestampTz(t.with_timezone(&Utc)))
                .unwrap_or_else(|_| Bind::Text(s.clone())),
            (Class::Date, Value::String(s)) => NaiveDate::parse_from_str(s, "%Y-%m-%d")
                .map(Bind::Date)
                .unwrap_or_else(|_| Bind::Text(s.clone())),
            (_, other) => by_json(other),
        }
    }
}

fn by_json(value: &Value) -> Bind {
    match value {
        Value::Null => Bind::Null,
        Value::Bool(b) => Bind::Int(*b as i64),
        Value::Number(n) => n
            .as_i64()
            .map(Bind::Int)
            .unwrap_or_else(|| Bind::Float(n.as_f64().unwrap_or(0.0))),
        Value::String(s) => Bind::Text(s.clone()),
        other => Bind::Text(other.to_string()),
    }
}

fn naive(text: &str) -> Option<NaiveDateTime> {
    ["%Y-%m-%dT%H:%M:%S%.f", "%Y-%m-%d %H:%M:%S%.f"]
        .iter()
        .find_map(|f| NaiveDateTime::parse_from_str(text, f).ok())
}

/// One column of a table.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnInfo {
    pub name: String,
    /// The engine's own type name.
    pub type_name: String,
    pub nullable: bool,
    pub class: Class,
}

/// A table's columns and primary key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableInfo {
    pub table: TableName,
    pub columns: Vec<ColumnInfo>,
    pub primary_key: Vec<String>,
}

impl TableInfo {
    pub fn column(&self, name: &str) -> Option<&ColumnInfo> {
        self.columns.iter().find(|c| c.name == name)
    }

    /// The columns that may say which rows are new: of an integer, decimal,
    /// date or time type, and not nullable.
    pub fn new_rows_candidates(&self) -> Vec<String> {
        self.columns
            .iter()
            .filter(|c| !c.nullable && c.class.orders_new_rows())
            .map(|c| c.name.clone())
            .collect()
    }
}

/// A table name with its schema filled in.
pub fn qualified(engine: Engine, table: &TableName) -> TableName {
    TableName {
        schema: Some(
            table
                .schema
                .clone()
                .unwrap_or_else(|| engine.default_schema().to_string()),
        ),
        name: table.name.clone(),
    }
}

fn text(row: &Map<String, Value>, name: &str) -> String {
    match row.get(name) {
        Some(Value::String(s)) => s.clone(),
        Some(Value::Null) | None => String::new(),
        Some(other) => other.to_string(),
    }
}

/// Every table (and view) the session can read.
pub async fn tables(
    session: &mut Session,
    clean: &(dyn Fn(&str) -> bool + Sync),
) -> Result<Vec<TableName>, DbError> {
    let sql = match session.engine() {
        Engine::Sqlite => {
            "SELECT 'main' AS s, name AS n FROM sqlite_master WHERE type IN ('table', 'view') AND name NOT LIKE 'sqlite_%' ORDER BY name"
        }
        Engine::Postgres => {
            "SELECT table_schema::text AS s, table_name::text AS n FROM information_schema.tables WHERE table_schema NOT IN ('pg_catalog', 'information_schema') ORDER BY 1, 2"
        }
        Engine::SqlServer => {
            "SELECT TABLE_SCHEMA AS s, TABLE_NAME AS n FROM INFORMATION_SCHEMA.TABLES ORDER BY 1, 2"
        }
    };
    Ok(session
        .fetch(sql, &[], clean)
        .await?
        .iter()
        .map(|r| TableName {
            schema: Some(text(r, "s")),
            name: text(r, "n"),
        })
        .collect())
}

/// A table's columns and primary key; `None` when it does not exist.
pub async fn table_info(
    session: &mut Session,
    table: &TableName,
    clean: &(dyn Fn(&str) -> bool + Sync),
) -> Result<Option<TableInfo>, DbError> {
    let engine = session.engine();
    let table = qualified(engine, table);
    let schema = table.schema.clone().unwrap_or_default();
    let (columns, key): (Vec<ColumnInfo>, Vec<String>) = match engine {
        Engine::Sqlite => {
            if schema != "main" {
                return Ok(None);
            }
            let rows = session
                .fetch(
                    "SELECT name AS n, type AS t, \"notnull\" AS nn, pk AS pk FROM pragma_table_info(?1) ORDER BY cid",
                    &[Bind::Text(table.name.clone())],
                    clean,
                )
                .await?;
            let mut key: Vec<(i64, String)> = rows
                .iter()
                .filter_map(|r| {
                    let pk = r.get("pk").and_then(Value::as_i64).unwrap_or(0);
                    (pk > 0).then(|| (pk, text(r, "n")))
                })
                .collect();
            key.sort();
            let columns = rows
                .iter()
                .map(|r| {
                    let type_name = text(r, "t");
                    let class = Class::of(engine, &type_name);
                    let notnull = r.get("nn").and_then(Value::as_i64).unwrap_or(0) == 1;
                    let pk = r.get("pk").and_then(Value::as_i64).unwrap_or(0);
                    // An INTEGER PRIMARY KEY is the rowid: never null.
                    let rowid =
                        pk > 0 && key.len() == 1 && type_name.eq_ignore_ascii_case("integer");
                    ColumnInfo {
                        name: text(r, "n"),
                        type_name,
                        nullable: !(notnull || rowid),
                        class,
                    }
                })
                .collect();
            (columns, key.into_iter().map(|(_, n)| n).collect())
        }
        Engine::Postgres | Engine::SqlServer => {
            let (columns_sql, key_sql) = if engine == Engine::Postgres {
                (
                    "SELECT column_name::text AS n, data_type::text AS t, is_nullable::text AS nl FROM information_schema.columns WHERE table_schema = $1 AND table_name = $2 ORDER BY ordinal_position",
                    "SELECT kcu.column_name::text AS n FROM information_schema.table_constraints tc JOIN information_schema.key_column_usage kcu ON tc.constraint_name = kcu.constraint_name AND tc.table_schema = kcu.table_schema AND tc.table_name = kcu.table_name WHERE tc.constraint_type = 'PRIMARY KEY' AND tc.table_schema = $1 AND tc.table_name = $2 ORDER BY kcu.ordinal_position",
                )
            } else {
                (
                    "SELECT COLUMN_NAME AS n, DATA_TYPE AS t, IS_NULLABLE AS nl FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = @P1 AND TABLE_NAME = @P2 ORDER BY ORDINAL_POSITION",
                    "SELECT kcu.COLUMN_NAME AS n FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu ON tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME AND tc.TABLE_SCHEMA = kcu.TABLE_SCHEMA AND tc.TABLE_NAME = kcu.TABLE_NAME WHERE tc.CONSTRAINT_TYPE = 'PRIMARY KEY' AND tc.TABLE_SCHEMA = @P1 AND tc.TABLE_NAME = @P2 ORDER BY kcu.ORDINAL_POSITION",
                )
            };
            let binds = [Bind::Text(schema.clone()), Bind::Text(table.name.clone())];
            let columns = session
                .fetch(columns_sql, &binds, clean)
                .await?
                .iter()
                .map(|r| {
                    let type_name = text(r, "t");
                    ColumnInfo {
                        name: text(r, "n"),
                        class: Class::of(engine, &type_name),
                        type_name,
                        nullable: text(r, "nl").eq_ignore_ascii_case("yes"),
                    }
                })
                .collect();
            let key = session
                .fetch(key_sql, &binds, clean)
                .await?
                .iter()
                .map(|r| text(r, "n"))
                .collect();
            (columns, key)
        }
    };
    if columns.is_empty() {
        return Ok(None);
    }
    Ok(Some(TableInfo {
        table,
        columns,
        primary_key: key,
    }))
}

fn from(engine: Engine, table: &TableName) -> String {
    let table = qualified(engine, table);
    format!(
        "{}.{}",
        engine.quote(table.schema.as_deref().unwrap_or_default()),
        engine.quote(&table.name)
    )
}

/// How many rows a table holds now.
pub async fn row_count(
    session: &mut Session,
    table: &TableName,
    clean: &(dyn Fn(&str) -> bool + Sync),
) -> Result<i64, DbError> {
    let engine = session.engine();
    let count = if engine == Engine::SqlServer {
        "COUNT_BIG(*)"
    } else {
        "COUNT(*)"
    };
    let rows = session
        .fetch(
            &format!("SELECT {count} AS n FROM {}", from(engine, table)),
            &[],
            clean,
        )
        .await?;
    Ok(rows
        .first()
        .and_then(|r| r.get("n"))
        .and_then(Value::as_i64)
        .unwrap_or(0))
}

/// A table read by keyset: the order columns (`new_rows_by`, then the key's
/// other columns) and their classes.
#[derive(Debug, Clone, PartialEq)]
pub struct Plan {
    pub table: TableName,
    pub columns: Vec<String>,
    classes: Vec<Class>,
}

impl Plan {
    /// The plan of a table read on a described table. Every named column must
    /// exist; `key` must not be empty.
    pub fn new(info: &TableInfo, key: &[String], new_rows_by: &str) -> Result<Self, String> {
        if key.is_empty() {
            return Err("the table has no primary key; name the read's key".into());
        }
        let mut columns = vec![new_rows_by.to_string()];
        for k in key {
            if !columns.contains(k) {
                columns.push(k.clone());
            }
        }
        let classes = columns
            .iter()
            .map(|c| {
                info.column(c)
                    .map(|c| c.class)
                    .ok_or_else(|| format!("the table has no column {c:?}"))
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self {
            table: info.table.clone(),
            columns,
            classes,
        })
    }

    /// The statement reading up to `limit` rows past `after` (the order
    /// columns' values of the last row read), and what it binds.
    pub fn sql(
        &self,
        engine: Engine,
        after: Option<&[Value]>,
        limit: usize,
    ) -> (String, Vec<Bind>) {
        let quoted: Vec<String> = self.columns.iter().map(|c| engine.quote(c)).collect();
        let mut binds = Vec::new();
        let mut filter = String::new();
        if let Some(after) = after.filter(|a| a.len() == self.columns.len()) {
            // (c1 > v1) OR (c1 = v1 AND c2 > v2) OR ...
            let mut alternatives = Vec::new();
            for i in 0..self.columns.len() {
                let mut terms = Vec::new();
                for j in 0..=i {
                    binds.push(self.classes[j].bind(engine, &after[j]));
                    let op = if j == i { ">" } else { "=" };
                    terms.push(format!(
                        "{} {op} {}",
                        quoted[j],
                        engine.placeholder(binds.len())
                    ));
                }
                alternatives.push(format!("({})", terms.join(" AND ")));
            }
            filter = format!(" WHERE {}", alternatives.join(" OR "));
        }
        let order = quoted.join(", ");
        let table = from(engine, &self.table);
        let sql = match engine {
            Engine::SqlServer => {
                format!("SELECT TOP ({limit}) * FROM {table}{filter} ORDER BY {order}")
            }
            _ => format!("SELECT * FROM {table}{filter} ORDER BY {order} LIMIT {limit}"),
        };
        (sql, binds)
    }

    /// The order columns' values of a row read.
    pub fn position(&self, row: &Map<String, Value>) -> Vec<Value> {
        self.columns
            .iter()
            .map(|c| row.get(c).cloned().unwrap_or(Value::Null))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn info() -> TableInfo {
        let column = |name: &str, t: &str| ColumnInfo {
            name: name.into(),
            type_name: t.into(),
            nullable: false,
            class: Class::of(Engine::SqlServer, t),
        };
        TableInfo {
            table: TableName {
                schema: None,
                name: "lab_results".into(),
            },
            columns: vec![column("id", "int"), column("updated_at", "datetime2")],
            primary_key: vec!["id".into()],
        }
    }

    #[test]
    fn rows_sharing_new_rows_by_are_read_past_on_the_whole_tuple() {
        let plan = Plan::new(&info(), &["id".into()], "updated_at").unwrap();
        assert_eq!(plan.columns, ["updated_at", "id"]);
        let (sql, binds) = plan.sql(Engine::SqlServer, None, 10);
        assert_eq!(
            sql,
            "SELECT TOP (10) * FROM [dbo].[lab_results] ORDER BY [updated_at], [id]"
        );
        assert!(binds.is_empty());
        let after = [
            serde_json::json!("2026-11-03T10:00:00"),
            serde_json::json!(7),
        ];
        let (sql, binds) = plan.sql(Engine::SqlServer, Some(&after), 10);
        assert_eq!(
            sql,
            "SELECT TOP (10) * FROM [dbo].[lab_results] WHERE ([updated_at] > @P1) OR ([updated_at] = @P2 AND [id] > @P3) ORDER BY [updated_at], [id]"
        );
        assert_eq!(binds.len(), 3);
        assert!(matches!(binds[0], Bind::Timestamp(_)));
        assert_eq!(binds[2], Bind::Int(7));
        assert!(Plan::new(&info(), &[], "id").is_err());
        assert!(Plan::new(&info(), &["nope".into()], "id").is_err());
    }
}
