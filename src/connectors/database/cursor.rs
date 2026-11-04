//! The cursor a database source follows: a value of the cursor column, with the
//! kind it binds as.

use chrono::{DateTime, NaiveDate, NaiveDateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// A cursor value with the kind it binds as, kept as JSON in the store.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
pub enum Cursor {
    Int(i64),
    Float(f64),
    /// A date-time as the driver's decode wrote it (RFC 3339 for Postgres and
    /// MySQL; whatever the column holds for SQLite).
    Timestamp(String),
    Date(String),
    Text(String),
}

impl Cursor {
    /// What a person typed as `cursor_start`: a number, an instant, a date, or text.
    pub(super) fn parse(text: &str) -> Self {
        let t = text.trim();
        if let Ok(i) = t.parse::<i64>() {
            return Cursor::Int(i);
        }
        if let Ok(f) = t.parse::<f64>() {
            return Cursor::Float(f);
        }
        if DateTime::parse_from_rfc3339(t).is_ok() || naive(t).is_some() {
            return Cursor::Timestamp(t.to_string());
        }
        if NaiveDate::parse_from_str(t, "%Y-%m-%d").is_ok() {
            return Cursor::Date(t.to_string());
        }
        Cursor::Text(t.to_string())
    }

    /// The cursor a row's cursor column gives.
    pub(super) fn from_value(v: &Value) -> Option<Self> {
        match v {
            Value::Number(n) => n
                .as_i64()
                .map(Cursor::Int)
                .or_else(|| n.as_f64().map(Cursor::Float)),
            Value::String(s) => Some(Cursor::parse(s)),
            Value::Bool(b) => Some(Cursor::Int(*b as i64)),
            _ => None,
        }
    }

    pub(super) fn later_than(&self, other: &Cursor) -> bool {
        match (self, other) {
            (Cursor::Int(a), Cursor::Int(b)) => a > b,
            (Cursor::Float(a), Cursor::Float(b)) => a > b,
            (Cursor::Int(a), Cursor::Float(b)) => (*a as f64) > *b,
            (Cursor::Float(a), Cursor::Int(b)) => *a > (*b as f64),
            (Cursor::Timestamp(a), Cursor::Timestamp(b)) => match (instant(a), instant(b)) {
                (Some(a), Some(b)) => a > b,
                _ => a > b,
            },
            (a, b) => a.text() > b.text(),
        }
    }

    pub(super) fn text(&self) -> String {
        match self {
            Cursor::Int(i) => i.to_string(),
            Cursor::Float(f) => f.to_string(),
            Cursor::Timestamp(s) | Cursor::Date(s) | Cursor::Text(s) => s.clone(),
        }
    }
}

fn naive(t: &str) -> Option<NaiveDateTime> {
    ["%Y-%m-%dT%H:%M:%S%.f", "%Y-%m-%d %H:%M:%S%.f"]
        .iter()
        .find_map(|f| NaiveDateTime::parse_from_str(t, f).ok())
}

pub(super) fn instant(t: &str) -> Option<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(t)
        .map(|d| d.with_timezone(&Utc))
        .ok()
        .or_else(|| naive(t).map(|n| n.and_utc()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cursor_starts_are_typed() {
        assert_eq!(Cursor::parse("42"), Cursor::Int(42));
        assert_eq!(Cursor::parse("4.5"), Cursor::Float(4.5));
        assert_eq!(
            Cursor::parse("2026-10-01T10:00:00Z"),
            Cursor::Timestamp("2026-10-01T10:00:00Z".into())
        );
        assert_eq!(
            Cursor::parse("2026-10-01 10:00:00"),
            Cursor::Timestamp("2026-10-01 10:00:00".into())
        );
        assert_eq!(
            Cursor::parse("2026-10-01"),
            Cursor::Date("2026-10-01".into())
        );
        assert_eq!(Cursor::parse("abc"), Cursor::Text("abc".into()));
    }

    #[test]
    fn cursors_order_within_their_kind() {
        assert!(Cursor::Int(3).later_than(&Cursor::Int(2)));
        assert!(!Cursor::Int(2).later_than(&Cursor::Int(2)));
        assert!(
            Cursor::Timestamp("2026-10-01T10:00:00+02:00".into())
                .later_than(&Cursor::Timestamp("2026-10-01T07:00:00Z".into()))
        );
        assert!(Cursor::Text("b".into()).later_than(&Cursor::Text("a".into())));
    }
}
