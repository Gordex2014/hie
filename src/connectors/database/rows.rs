//! A row of each driver as a JSON object keyed by column name, with each
//! column's type decoded to the JSON value a transform expects.

use chrono::{DateTime, FixedOffset, NaiveDate, NaiveDateTime, Utc};
use serde_json::{Map, Value};
use sqlx::mysql::MySqlRow;
use sqlx::postgres::PgRow;
use sqlx::sqlite::SqliteRow;
use sqlx::{Column, Row, TypeInfo};

/// `try_get` of an optional value, as JSON through `f`.
macro_rules! get {
    ($row:expr, $i:expr, $t:ty, $f:expr) => {
        $row.try_get::<Option<$t>, _>($i)
            .ok()
            .map(|v| v.map_or(Value::Null, $f))
    };
}

pub(super) fn pg_row(row: &PgRow) -> Map<String, Value> {
    let mut out = Map::new();
    for (i, column) in row.columns().iter().enumerate() {
        let name = column.type_info().name();
        let value = match name {
            "BOOL" => get!(row, i, bool, Value::Bool),
            "INT2" => get!(row, i, i16, |v| Value::from(v as i64)),
            "INT4" => get!(row, i, i32, |v| Value::from(v as i64)),
            "INT8" => get!(row, i, i64, Value::from),
            "FLOAT4" => get!(row, i, f32, |v| Value::from(v as f64)),
            "FLOAT8" => get!(row, i, f64, Value::from),
            "NUMERIC" => get!(row, i, sqlx::types::Decimal, |d| number(&d.to_string())),
            "UUID" => get!(row, i, sqlx::types::Uuid, |u| Value::String(u.to_string())),
            "DATE" => get!(row, i, NaiveDate, |d| Value::String(d.to_string())),
            "TIME" => get!(row, i, chrono::NaiveTime, |t| Value::String(t.to_string())),
            "TIMESTAMP" => get!(row, i, NaiveDateTime, |t| Value::String(
                t.format("%Y-%m-%dT%H:%M:%S%.f").to_string()
            )),
            "TIMESTAMPTZ" => get!(row, i, DateTime<Utc>, |t| Value::String(t.to_rfc3339())),
            "JSON" | "JSONB" => get!(row, i, Value, |v| v),
            "BYTEA" => get!(row, i, Vec<u8>, |b| Value::String(hex::encode(b))),
            _ => get!(row, i, String, Value::String),
        };
        out.insert(
            column.name().to_string(),
            value.unwrap_or_else(|| {
                tracing::warn!(
                    target: "hie::database",
                    column = column.name(),
                    kind = name,
                    "column type not decoded; null"
                );
                Value::Null
            }),
        );
    }
    out
}

pub(super) fn mysql_row(row: &MySqlRow) -> Map<String, Value> {
    let mut out = Map::new();
    for (i, column) in row.columns().iter().enumerate() {
        let name = column.type_info().name();
        let value = match name {
            "BOOLEAN" => get!(row, i, bool, Value::Bool),
            "TINYINT" | "SMALLINT" | "MEDIUMINT" | "INT" | "BIGINT" => {
                get!(row, i, i64, Value::from)
            }
            "TINYINT UNSIGNED" | "SMALLINT UNSIGNED" | "MEDIUMINT UNSIGNED" | "INT UNSIGNED"
            | "BIGINT UNSIGNED" => get!(row, i, u64, Value::from),
            "FLOAT" | "DOUBLE" => get!(row, i, f64, Value::from),
            "DECIMAL" => get!(row, i, sqlx::types::Decimal, |d| number(&d.to_string())),
            "DATE" => get!(row, i, NaiveDate, |d| Value::String(d.to_string())),
            "TIME" => get!(row, i, chrono::NaiveTime, |t| Value::String(t.to_string())),
            "DATETIME" => get!(row, i, NaiveDateTime, |t| Value::String(
                t.format("%Y-%m-%dT%H:%M:%S%.f").to_string()
            )),
            "TIMESTAMP" => get!(row, i, DateTime<Utc>, |t| Value::String(t.to_rfc3339())),
            "JSON" => get!(row, i, Value, |v| v),
            "BLOB" | "TINYBLOB" | "MEDIUMBLOB" | "LONGBLOB" | "BINARY" | "VARBINARY" => {
                get!(row, i, Vec<u8>, |b| Value::String(hex::encode(b)))
            }
            _ => get!(row, i, String, Value::String),
        };
        out.insert(
            column.name().to_string(),
            value.unwrap_or_else(|| {
                tracing::warn!(
                    target: "hie::database",
                    column = column.name(),
                    kind = name,
                    "column type not decoded; null"
                );
                Value::Null
            }),
        );
    }
    out
}

pub(super) fn mssql_row(row: &tiberius::Row) -> Map<String, Value> {
    use tiberius::ColumnType as T;
    let mut out = Map::new();
    for (i, column) in row.columns().iter().enumerate() {
        let kind = column.column_type();
        let value: Option<Value> = match kind {
            T::Null => Some(Value::Null),
            T::Bit | T::Bitn => row
                .try_get::<bool, _>(i)
                .ok()
                .map(|v| v.map_or(Value::Null, Value::Bool)),
            T::Int1 => row
                .try_get::<u8, _>(i)
                .ok()
                .map(|v| v.map_or(Value::Null, |n| Value::from(n as i64))),
            T::Int2 => row
                .try_get::<i16, _>(i)
                .ok()
                .map(|v| v.map_or(Value::Null, |n| Value::from(n as i64))),
            T::Int4 => row
                .try_get::<i32, _>(i)
                .ok()
                .map(|v| v.map_or(Value::Null, |n| Value::from(n as i64))),
            T::Int8 => row
                .try_get::<i64, _>(i)
                .ok()
                .map(|v| v.map_or(Value::Null, Value::from)),
            T::Intn => row
                .try_get::<i64, _>(i)
                .ok()
                .map(|v| v.map_or(Value::Null, Value::from))
                .or_else(|| {
                    row.try_get::<i32, _>(i)
                        .ok()
                        .map(|v| v.map_or(Value::Null, |n| Value::from(n as i64)))
                })
                .or_else(|| {
                    row.try_get::<i16, _>(i)
                        .ok()
                        .map(|v| v.map_or(Value::Null, |n| Value::from(n as i64)))
                })
                .or_else(|| {
                    row.try_get::<u8, _>(i)
                        .ok()
                        .map(|v| v.map_or(Value::Null, |n| Value::from(n as i64)))
                }),
            T::Float4 => row
                .try_get::<f32, _>(i)
                .ok()
                .map(|v| v.map_or(Value::Null, |n| Value::from(n as f64))),
            T::Float8 => row
                .try_get::<f64, _>(i)
                .ok()
                .map(|v| v.map_or(Value::Null, Value::from)),
            T::Floatn => row
                .try_get::<f64, _>(i)
                .ok()
                .map(|v| v.map_or(Value::Null, Value::from))
                .or_else(|| {
                    row.try_get::<f32, _>(i)
                        .ok()
                        .map(|v| v.map_or(Value::Null, |n| Value::from(n as f64)))
                }),
            // `Numeric` holds any scale up to 38 (a 28-digit decimal cannot,
            // and panics past it); money arrives as a float.
            T::Decimaln | T::Numericn | T::Money | T::Money4 => row
                .try_get::<tiberius::numeric::Numeric, _>(i)
                .ok()
                .map(|v| v.map_or(Value::Null, |n| number(&numeric_text(n.value(), n.scale()))))
                .or_else(|| {
                    row.try_get::<f64, _>(i)
                        .ok()
                        .map(|v| v.map_or(Value::Null, Value::from))
                }),
            T::Guid => row
                .try_get::<tiberius::Uuid, _>(i)
                .ok()
                .map(|v| v.map_or(Value::Null, |u| Value::String(u.to_string()))),
            T::Daten => row
                .try_get::<NaiveDate, _>(i)
                .ok()
                .map(|v| v.map_or(Value::Null, |d| Value::String(d.to_string()))),
            T::Timen => row
                .try_get::<chrono::NaiveTime, _>(i)
                .ok()
                .map(|v| v.map_or(Value::Null, |t| Value::String(t.to_string()))),
            T::Datetime | T::Datetime4 | T::Datetimen | T::Datetime2 => {
                row.try_get::<NaiveDateTime, _>(i).ok().map(|v| {
                    v.map_or(Value::Null, |t| {
                        Value::String(t.format("%Y-%m-%dT%H:%M:%S%.f").to_string())
                    })
                })
            }
            // The instant with the value's own offset. TDS sends the UTC
            // date-time and the offset; tiberius's `DateTime<Utc>` subtracts
            // the offset once more, so a value is read |offset| off.
            T::DatetimeOffsetn => row
                .try_get::<DateTime<FixedOffset>, _>(i)
                .ok()
                .map(|v| v.map_or(Value::Null, |t| Value::String(t.to_rfc3339()))),
            T::BigVarBin | T::BigBinary | T::Image => row
                .try_get::<&[u8], _>(i)
                .ok()
                .map(|v| v.map_or(Value::Null, |b| Value::String(hex::encode(b)))),
            _ => row
                .try_get::<&str, _>(i)
                .ok()
                .map(|v| v.map_or(Value::Null, |s| Value::String(s.to_string()))),
        };
        out.insert(
            column.name().to_string(),
            value.unwrap_or_else(|| {
                tracing::warn!(target: "hie::database", column = column.name(), kind = ?kind, "column type not decoded; null");
                Value::Null
            }),
        );
    }
    out
}

pub(super) fn sqlite_row(row: &SqliteRow) -> Map<String, Value> {
    let mut out = Map::new();
    for (i, column) in row.columns().iter().enumerate() {
        // SQLite types are per value, not per column: try the kinds in turn.
        let value = get!(row, i, i64, Value::from)
            .filter(|v| !v.is_null())
            .or_else(|| get!(row, i, f64, Value::from).filter(|v| !v.is_null()))
            .or_else(|| get!(row, i, String, Value::String).filter(|v| !v.is_null()))
            .or_else(|| get!(row, i, Vec<u8>, |b| Value::String(hex::encode(b))))
            .unwrap_or(Value::Null);
        out.insert(column.name().to_string(), value);
    }
    out
}

/// A fixed-point value (`value` × 10^-`scale`) as decimal text, exactly.
fn numeric_text(value: i128, scale: u8) -> String {
    let digits = value.unsigned_abs().to_string();
    let scale = scale as usize;
    let sign = if value < 0 { "-" } else { "" };
    if scale == 0 {
        return format!("{sign}{digits}");
    }
    let digits = format!("{digits:0>width$}", width = scale + 1);
    let (int, frac) = digits.split_at(digits.len() - scale);
    format!("{sign}{int}.{frac}")
}

/// A decimal as a JSON number when it fits one, as text otherwise.
fn number(text: &str) -> Value {
    serde_json::from_str::<Value>(text)
        .ok()
        .filter(Value::is_number)
        .unwrap_or_else(|| Value::String(text.to_string()))
}

#[cfg(test)]
mod tests {
    use super::numeric_text;

    #[test]
    fn fixed_point_values_are_written_exactly() {
        assert_eq!(
            numeric_text(1_000_000_000_000_000_000_000_000_000_001, 30),
            "1.000000000000000000000000000001"
        );
        assert_eq!(numeric_text(-12345, 4), "-1.2345");
        assert_eq!(numeric_text(5, 3), "0.005");
        assert_eq!(numeric_text(42, 0), "42");
    }
}
