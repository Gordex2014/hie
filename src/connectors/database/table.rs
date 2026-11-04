//! A table as a session sees it (its tables, columns, primary key and row
//! count, per engine), and a table read by keyset: rows ordered by
//! (`new_rows_by`, `key`...), each run taking the rows strictly past the last
//! one read on that whole tuple, so rows that share a `new_rows_by` value are
//! never skipped at a run's boundary.

use chrono::{DateTime, NaiveDate, NaiveDateTime};
use serde_json::{Map, Value};

use super::endpoint::{Bind, DbError, Engine, Session};
use crate::config::TableName;

/// What a column's type is, for binding a value of it back in its own type,
/// and for choosing what may order a read.
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
    Uuid,
    Bool,
    /// A type a keyset cannot compare or bind (binary, JSON, arrays, ...).
    Unsupported,
}

impl Class {
    /// The class of an engine's type name.
    pub fn of(engine: Engine, type_name: &str) -> Self {
        let t = type_name.trim().to_ascii_lowercase();
        match engine {
            // SQLite's affinity rules, by the declared type; values bind by
            // their own type, so only blobs cannot order.
            Engine::Sqlite => {
                if t.contains("int") {
                    Class::Int
                } else if t.contains("char") || t.contains("clob") || t.contains("text") {
                    Class::Text
                } else if t.contains("blob") || t.is_empty() {
                    Class::Unsupported
                } else if t.contains("real") || t.contains("floa") || t.contains("doub") {
                    Class::Float
                } else if t.contains("datetime") || t.contains("timestamp") {
                    Class::Timestamp
                } else if t == "date" {
                    Class::Date
                } else if t.contains("time") {
                    Class::Time
                } else {
                    Class::Decimal
                }
            }
            Engine::Postgres => match t.as_str() {
                "smallint" | "integer" | "bigint" => Class::Int,
                "numeric" | "decimal" => Class::Decimal,
                "real" | "double precision" => Class::Float,
                "timestamp without time zone" | "timestamp" => Class::Timestamp,
                "timestamp with time zone" | "timestamptz" => Class::TimestampTz,
                "date" => Class::Date,
                "time without time zone" | "time" => Class::Time,
                "uuid" => Class::Uuid,
                "boolean" => Class::Bool,
                "character varying" | "character" | "text" | "name" | "varchar" | "char"
                | "bpchar" => Class::Text,
                _ => Class::Unsupported,
            },
            Engine::SqlServer => match t.as_str() {
                "tinyint" | "smallint" | "int" | "bigint" => Class::Int,
                "decimal" | "numeric" | "money" | "smallmoney" => Class::Decimal,
                "real" | "float" => Class::Float,
                "datetime" | "datetime2" | "smalldatetime" => Class::Timestamp,
                "datetimeoffset" => Class::TimestampTz,
                "date" => Class::Date,
                "time" => Class::Time,
                "uniqueidentifier" => Class::Uuid,
                "bit" => Class::Bool,
                "char" | "varchar" | "nchar" | "nvarchar" => Class::Text,
                // `timestamp` is a row version; `text`, `ntext` cannot be compared.
                _ => Class::Unsupported,
            },
        }
    }

    /// Integer, decimal, date or time types: what may say which rows are new.
    pub fn orders_new_rows(self) -> bool {
        matches!(
            self,
            Class::Int
                | Class::Float
                | Class::Decimal
                | Class::Timestamp
                | Class::TimestampTz
                | Class::Date
                | Class::Time
        )
    }

    /// A value read from a column of this class, to bind back in the
    /// column's own type.
    pub fn bind(self, engine: Engine, value: &Value) -> Bind {
        if engine == Engine::Sqlite {
            return by_json(value);
        }
        match (self, value) {
            (_, Value::Null) => Bind::Null,
            (Class::Int, Value::Number(n)) if n.is_i64() => Bind::Int(n.as_i64().unwrap_or(0)),
            (Class::Decimal, Value::Number(n)) => Bind::Decimal(n.to_string()),
            (Class::Decimal, Value::String(s)) => Bind::Decimal(s.clone()),
            (Class::Bool, Value::Bool(b)) => Bind::Bool(*b),
            (Class::Uuid, Value::String(s)) => Bind::Uuid(s.clone()),
            (Class::Time, Value::String(s)) => chrono::NaiveTime::parse_from_str(s, "%H:%M:%S%.f")
                .map(Bind::Time)
                .unwrap_or_else(|_| Bind::Text(s.clone())),
            (Class::Timestamp, Value::String(s)) => naive(s)
                .map(Bind::Timestamp)
                .unwrap_or_else(|| Bind::Text(s.clone())),
            (Class::TimestampTz, Value::String(s)) => DateTime::parse_from_rfc3339(s)
                .map(Bind::TimestampTz)
                .unwrap_or_else(|_| Bind::Text(s.clone())),
            (Class::Date, Value::String(s)) => NaiveDate::parse_from_str(s, "%Y-%m-%d")
                .map(Bind::Date)
                .unwrap_or_else(|_| Bind::Text(s.clone())),
            (_, other) => by_json(other),
        }
    }
}

/// A SQL Server `datetime` keeps 1/300 s and a `smalldatetime` whole
/// minutes; a date-time binds as a `datetime2(7)`, which a `datetime` value
/// does not convert to exactly (2/300 s reads as .006666666 and converts as
/// .0066667), so the position is cast back to the column's own type, where
/// it is the value it was read from.
fn time_cast(engine: Engine, column: &ColumnInfo) -> Option<String> {
    let t = column.type_name.trim().to_ascii_lowercase();
    (engine == Engine::SqlServer && matches!(t.as_str(), "datetime" | "smalldatetime"))
        .then(|| format!("CAST({{}} AS {})", t.to_ascii_uppercase()))
}

fn by_json(value: &Value) -> Bind {
    match value {
        Value::Null => Bind::Null,
        Value::Bool(b) => Bind::Bool(*b),
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

/// Why a column cannot order a table read, if it cannot.
pub fn cannot_order(column: &ColumnInfo) -> Option<String> {
    if column.nullable {
        return Some(format!(
            "column {:?} may be null; a table read orders by columns that are never null",
            column.name
        ));
    }
    if column.class == Class::Unsupported {
        return Some(format!(
            "column {:?} is of type {}, which cannot order a table read",
            column.name, column.type_name
        ));
    }
    None
}

/// One column of a table.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnInfo {
    pub name: String,
    /// The engine's own type name.
    pub type_name: String,
    pub nullable: bool,
    pub class: Class,
    /// A decimal column's precision and scale, as the engine reports them.
    pub numeric: Option<(i64, i64)>,
}

/// A table's columns and primary key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableInfo {
    /// The engine the table lives in.
    pub engine: Engine,
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
                        numeric: None,
                    }
                })
                .collect();
            (columns, key.into_iter().map(|(_, n)| n).collect())
        }
        Engine::Postgres | Engine::SqlServer => {
            let (columns_sql, key_sql) = if engine == Engine::Postgres {
                (
                    "SELECT column_name::text AS n, data_type::text AS t, is_nullable::text AS nl, numeric_precision::int AS p, numeric_scale::int AS sc FROM information_schema.columns WHERE table_schema = $1 AND table_name = $2 ORDER BY ordinal_position",
                    "SELECT kcu.column_name::text AS n FROM information_schema.table_constraints tc JOIN information_schema.key_column_usage kcu ON tc.constraint_name = kcu.constraint_name AND tc.table_schema = kcu.table_schema AND tc.table_name = kcu.table_name WHERE tc.constraint_type = 'PRIMARY KEY' AND tc.table_schema = $1 AND tc.table_name = $2 ORDER BY kcu.ordinal_position",
                )
            } else {
                (
                    "SELECT COLUMN_NAME AS n, DATA_TYPE AS t, IS_NULLABLE AS nl, CAST(NUMERIC_PRECISION AS INT) AS p, NUMERIC_SCALE AS sc FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = @P1 AND TABLE_NAME = @P2 ORDER BY ORDINAL_POSITION",
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
                        numeric: r
                            .get("p")
                            .and_then(Value::as_i64)
                            .zip(r.get("sc").and_then(Value::as_i64)),
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
        engine,
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
/// other columns), how each is bound, and how its exact value is read.
#[derive(Debug, Clone, PartialEq)]
pub struct Plan {
    pub table: TableName,
    pub columns: Vec<String>,
    engine: Engine,
    classes: Vec<Class>,
    /// Per order column: the expression reading its exact text beside the
    /// row, and the alias it is read under (never a column of the table).
    exact: Vec<Option<(String, String)>>,
    /// Per order column: the placeholder's cast, so a bound position is
    /// compared exactly in the column's own type (`{}` is the placeholder).
    casts: Vec<Option<String>>,
}

impl Plan {
    /// The plan of a table read on a described table. Every named column must
    /// exist, never be null and be of a type that orders and binds exactly;
    /// `key` must not be empty.
    pub fn new(info: &TableInfo, key: &[String], new_rows_by: &str) -> Result<Self, String> {
        if key.is_empty() {
            return Err("the table has no primary key; name the read's key".into());
        }
        let engine = info.engine;
        let mut columns = vec![new_rows_by.to_string()];
        for k in key {
            if !columns.contains(k) {
                columns.push(k.clone());
            }
        }
        let mut classes = Vec::new();
        let mut exact = Vec::new();
        let mut casts = Vec::new();
        let mut aliases: Vec<String> = Vec::new();
        for (i, c) in columns.iter().enumerate() {
            let column = info
                .column(c)
                .ok_or_else(|| format!("the table has no column {c:?}"))?;
            if let Some(reason) = cannot_order(column) {
                return Err(reason);
            }
            classes.push(column.class);
            // Decimals are read beside the row as exact text and bound back
            // as text cast to the column's type: never through a float or a
            // 28-digit decimal. SQLite stores them as floats already.
            if column.class != Class::Decimal || engine == Engine::Sqlite {
                exact.push(None);
                casts.push(time_cast(engine, column));
                continue;
            }
            let mut alias = format!("hie_position_{i}");
            while info.column(&alias).is_some() || aliases.contains(&alias) {
                alias.push('_');
            }
            aliases.push(alias.clone());
            let quoted = engine.quote(c);
            let (read, cast) = match engine {
                Engine::Postgres => (format!("{quoted}::text"), "{}::numeric".to_string()),
                _ => {
                    let Some((precision, scale)) = column.numeric else {
                        return Err(format!(
                            "column {c:?} reports no precision, so it cannot order a table read exactly"
                        ));
                    };
                    // Style 2 keeps money's four decimals; decimal and
                    // numeric convert with their whole scale.
                    let style = if matches!(
                        column.type_name.to_ascii_lowercase().as_str(),
                        "money" | "smallmoney"
                    ) {
                        ", 2"
                    } else {
                        ""
                    };
                    (
                        format!("CONVERT(varchar(64), {quoted}{style})"),
                        format!("CAST({{}} AS DECIMAL({precision}, {scale}))"),
                    )
                }
            };
            exact.push(Some((read, alias)));
            casts.push(Some(cast));
        }
        Ok(Self {
            table: info.table.clone(),
            columns,
            engine,
            classes,
            exact,
            casts,
        })
    }

    /// Whether a SQL Server `datetimeoffset` column orders the read.
    pub fn orders_by_datetimeoffset(&self) -> bool {
        self.engine == Engine::SqlServer && self.classes.contains(&Class::TimestampTz)
    }

    /// The statement reading up to `limit` rows past `after` (the order
    /// columns' values of the last row read), and what it binds.
    pub fn sql(&self, after: Option<&[Value]>, limit: usize) -> (String, Vec<Bind>) {
        let engine = self.engine;
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
                    let placeholder = engine.placeholder(binds.len());
                    let placeholder = match &self.casts[j] {
                        Some(cast) => cast.replace("{}", &placeholder),
                        None => placeholder,
                    };
                    let op = if j == i { ">" } else { "=" };
                    terms.push(format!("{} {op} {placeholder}", quoted[j]));
                }
                alternatives.push(format!("({})", terms.join(" AND ")));
            }
            filter = format!(" WHERE {}", alternatives.join(" OR "));
        }
        let mut select = "*".to_string();
        for (read, alias) in self.exact.iter().flatten() {
            select.push_str(&format!(", {read} AS {}", engine.quote(alias)));
        }
        let order = quoted.join(", ");
        let table = from(engine, &self.table);
        let sql = match engine {
            Engine::SqlServer => {
                format!("SELECT TOP ({limit}) {select} FROM {table}{filter} ORDER BY {order}")
            }
            _ => format!("SELECT {select} FROM {table}{filter} ORDER BY {order} LIMIT {limit}"),
        };
        (sql, binds)
    }

    /// A row as read: the row itself (without the exact texts read beside
    /// it) and the order columns' exact values.
    pub fn split(&self, mut row: Map<String, Value>) -> (Map<String, Value>, Vec<Value>) {
        let mut position = Vec::with_capacity(self.columns.len());
        for (i, column) in self.columns.iter().enumerate() {
            let exact = self.exact[i]
                .as_ref()
                .and_then(|(_, alias)| row.remove(alias));
            position.push(exact.unwrap_or_else(|| row.get(column).cloned().unwrap_or(Value::Null)));
        }
        (row, position)
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
            numeric: None,
        };
        TableInfo {
            engine: Engine::SqlServer,
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
        let (sql, binds) = plan.sql(None, 10);
        assert_eq!(
            sql,
            "SELECT TOP (10) * FROM [dbo].[lab_results] ORDER BY [updated_at], [id]"
        );
        assert!(binds.is_empty());
        let after = [
            serde_json::json!("2026-11-03T10:00:00"),
            serde_json::json!(7),
        ];
        let (sql, binds) = plan.sql(Some(&after), 10);
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

    fn table(engine: Engine, columns: &[(&str, &str, bool)]) -> TableInfo {
        TableInfo {
            engine,
            table: TableName {
                schema: None,
                name: "t".into(),
            },
            columns: columns
                .iter()
                .map(|(name, t, nullable)| ColumnInfo {
                    name: (*name).into(),
                    type_name: (*t).into(),
                    nullable: *nullable,
                    class: Class::of(engine, t),
                    numeric: (Class::of(engine, t) == Class::Decimal).then_some((38, 30)),
                })
                .collect(),
            primary_key: vec![],
        }
    }

    #[test]
    fn nullable_or_unordered_columns_cannot_order_a_read() {
        let info = table(
            Engine::Postgres,
            &[
                ("id", "integer", false),
                ("seen", "timestamp without time zone", true),
                ("blob", "bytea", false),
            ],
        );
        let nullable = Plan::new(&info, &["id".into()], "seen").unwrap_err();
        assert!(nullable.contains("may be null"), "{nullable}");
        let binary = Plan::new(&info, &["blob".into()], "id").unwrap_err();
        assert!(binary.contains("cannot order"), "{binary}");
        assert!(Plan::new(&info, &["id".into()], "id").is_ok());
        let candidates = table(
            Engine::Postgres,
            &[
                ("id", "integer", false),
                ("flag", "boolean", false),
                ("u", "uuid", false),
            ],
        )
        .new_rows_candidates();
        assert_eq!(candidates, ["id"]);
    }

    #[test]
    fn decimal_positions_stay_exact_from_the_read_to_the_bind() {
        let info = table(
            Engine::Postgres,
            &[("amount", "numeric", false), ("id", "integer", false)],
        );
        let plan = Plan::new(&info, &["id".into()], "amount").unwrap();
        let (sql, _) = plan.sql(None, 5);
        assert_eq!(
            sql,
            "SELECT *, \"amount\"::text AS \"hie_position_0\" FROM \"public\".\"t\" ORDER BY \"amount\", \"id\" LIMIT 5"
        );
        // The decoded row has lost digits; the exact text read beside it has
        // not, whatever its length (30 fractional digits here).
        let exact = "1234567.123456789012345678901234567890";
        let mut row = serde_json::Map::new();
        row.insert("amount".into(), serde_json::json!(1234567.123456789));
        row.insert("id".into(), serde_json::json!(3));
        row.insert("hie_position_0".into(), serde_json::json!(exact));
        let (row, position) = plan.split(row);
        assert!(!row.contains_key("hie_position_0"));
        assert_eq!(position, [serde_json::json!(exact), serde_json::json!(3)]);
        // Persisted as JSON, bound back as that exact text, cast in SQL.
        let stored: Vec<serde_json::Value> =
            serde_json::from_str(&serde_json::to_string(&position).unwrap()).unwrap();
        let (sql, binds) = plan.sql(Some(&stored), 5);
        assert_eq!(binds[0], Bind::Decimal(exact.into()));
        assert!(
            sql.contains(
                "WHERE (\"amount\" > $1::numeric) OR (\"amount\" = $2::numeric AND \"id\" > $3)"
            ),
            "{sql}"
        );
    }

    #[test]
    fn sql_server_decimals_and_money_convert_and_bind_losslessly() {
        let mut info = table(
            Engine::SqlServer,
            &[
                ("amount", "decimal", false),
                ("price", "money", false),
                ("id", "int", false),
            ],
        );
        info.columns[1].numeric = Some((19, 4));
        let plan = Plan::new(&info, &["price".into(), "id".into()], "amount").unwrap();
        let after = [
            serde_json::json!("1.5"),
            serde_json::json!("2.0001"),
            serde_json::json!(1),
        ];
        let (sql, binds) = plan.sql(Some(&after), 5);
        assert!(
            sql.contains("CONVERT(varchar(64), [amount]) AS [hie_position_0]"),
            "{sql}"
        );
        // Style 2 keeps money's four decimals.
        assert!(
            sql.contains("CONVERT(varchar(64), [price], 2) AS [hie_position_1]"),
            "{sql}"
        );
        assert!(
            sql.contains("[amount] > CAST(@P1 AS DECIMAL(38, 30))"),
            "{sql}"
        );
        assert!(
            sql.contains("[price] > CAST(@P3 AS DECIMAL(19, 4))"),
            "{sql}"
        );
        assert_eq!(binds[2], Bind::Decimal("2.0001".into()));
        // Without a precision there is no exact bind: refused.
        info.columns[0].numeric = None;
        assert!(Plan::new(&info, &["id".into()], "amount").is_err());
    }

    #[test]
    fn an_exact_value_is_never_read_under_a_column_of_the_table() {
        let info = table(
            Engine::Postgres,
            &[
                ("amount", "numeric", false),
                ("hie_position_0", "text", false),
                ("id", "integer", false),
            ],
        );
        let plan = Plan::new(&info, &["id".into()], "amount").unwrap();
        let (sql, _) = plan.sql(None, 5);
        assert!(
            sql.contains("\"amount\"::text AS \"hie_position_0_\""),
            "{sql}"
        );
        let mut row = serde_json::Map::new();
        row.insert("amount".into(), serde_json::json!(1.5));
        row.insert(
            "hie_position_0".into(),
            serde_json::json!("the source's own value"),
        );
        row.insert("id".into(), serde_json::json!(1));
        row.insert("hie_position_0_".into(), serde_json::json!("1.50"));
        let (row, position) = plan.split(row);
        assert_eq!(row["hie_position_0"], "the source's own value");
        assert!(!row.contains_key("hie_position_0_"));
        assert_eq!(position[0], "1.50");
    }

    #[test]
    fn sql_server_times_bind_back_in_their_own_type() {
        let info = table(
            Engine::SqlServer,
            &[
                ("at", "datetimeoffset", false),
                ("old", "datetime", false),
                ("small", "smalldatetime", false),
                ("id", "int", false),
            ],
        );
        let plan = Plan::new(&info, &["old".into(), "small".into(), "id".into()], "at").unwrap();
        assert!(plan.orders_by_datetimeoffset());
        let after = [
            serde_json::json!("2026-10-01T00:20:00.1234567-04:00"),
            serde_json::json!("2026-10-01T00:20:00.006666666"),
            serde_json::json!("2026-10-01T00:20:00"),
            serde_json::json!(1),
        ];
        let (sql, binds) = plan.sql(Some(&after), 5);
        // The instant with its own offset: a `datetimeoffset`, not a UTC
        // `datetime2`.
        let Bind::TimestampTz(at) = &binds[0] else {
            panic!("{binds:?}")
        };
        assert_eq!(at.to_rfc3339(), "2026-10-01T00:20:00.123456700-04:00");
        assert!(sql.contains("[old] > CAST(@P3 AS DATETIME)"), "{sql}");
        assert!(
            sql.contains("[small] > CAST(@P6 AS SMALLDATETIME)"),
            "{sql}"
        );
        assert!(sql.contains("[at] > @P1"), "{sql}");
        let pg = table(
            Engine::Postgres,
            &[
                ("at", "timestamp with time zone", false),
                ("id", "integer", false),
            ],
        );
        let plan = Plan::new(&pg, &["id".into()], "at").unwrap();
        assert!(!plan.orders_by_datetimeoffset());
        let (sql, binds) = plan.sql(
            Some(&[
                serde_json::json!("2026-10-01T04:20:00+00:00"),
                serde_json::json!(1),
            ]),
            5,
        );
        assert!(sql.contains("(\"at\" > $1)"), "{sql}");
        assert!(matches!(binds[0], Bind::TimestampTz(_)));
    }

    #[test]
    fn keys_bind_in_their_own_type() {
        let info = table(
            Engine::Postgres,
            &[
                ("u", "uuid", false),
                ("flag", "boolean", false),
                ("at", "time without time zone", false),
            ],
        );
        let plan = Plan::new(&info, &["u".into(), "flag".into()], "at").unwrap();
        let after = [
            serde_json::json!("10:30:00.5"),
            serde_json::json!("6f9619ff-8b86-d011-b42d-00c04fc964ff"),
            serde_json::json!(true),
        ];
        let (_, binds) = plan.sql(Some(&after), 5);
        assert!(matches!(binds[0], Bind::Time(_)));
        assert_eq!(
            binds[2],
            Bind::Uuid("6f9619ff-8b86-d011-b42d-00c04fc964ff".into())
        );
        assert_eq!(binds[5], Bind::Bool(true));
    }
}
