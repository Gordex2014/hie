//! Reading a list by period and in pages: `since` as an RFC 3339 instant, and an
//! opaque keyset cursor. A malformed query answers `400 invalid_query` with a
//! fixed sentence that never repeats what was sent.

use axum::extract::rejection::QueryRejection;
use axum::http::StatusCode;

use super::ApiError;

fn invalid_query(sentence: &str) -> ApiError {
    ApiError::new(StatusCode::BAD_REQUEST, sentence).code("invalid_query")
}

/// A query string the extractor could not read (`limit=abc`, `open=yes`).
pub(super) fn rejected(_: QueryRejection) -> ApiError {
    invalid_query("the query string could not be read; check each parameter's type")
}

/// `since` as epoch milliseconds: the first stored millisecond at or after the
/// instant, so a finer instant rounds up. Empty means absent.
pub(super) fn since_ms(since: Option<&str>) -> Result<Option<i64>, ApiError> {
    let Some(text) = since.filter(|t| !t.is_empty()) else {
        return Ok(None);
    };
    let bad = || {
        invalid_query(
            "since must be an RFC 3339 instant, such as 2026-11-13T00:00:00Z (send + as %2B)",
        )
        .with("field", "since")
    };
    // The parser drops fraction digits past the ninth, which would round the
    // wrong way; and a leap second has no millisecond of its own in the store.
    let digits = text.split_once('.').map_or(0, |(_, fraction)| {
        fraction.bytes().take_while(u8::is_ascii_digit).count()
    });
    if digits > 9 {
        return Err(bad());
    }
    let at = chrono::DateTime::parse_from_rfc3339(text).map_err(|_| bad())?;
    let nanos = at.timestamp_subsec_nanos();
    if nanos >= 1_000_000_000 {
        return Err(bad());
    }
    let ms = at.timestamp_millis();
    Ok(Some(if nanos % 1_000_000 == 0 { ms } else { ms + 1 }))
}

/// The id a `cursor` stands for: the last row of the previous page. Empty means
/// the first page.
pub(super) fn cursor_id(cursor: Option<&str>) -> Result<Option<i64>, ApiError> {
    let Some(text) = cursor.filter(|t| !t.is_empty()) else {
        return Ok(None);
    };
    text.bytes()
        .all(|b| b.is_ascii_digit())
        .then(|| text.parse::<i64>().ok())
        .flatten()
        .filter(|id| *id > 0)
        .map(Some)
        .ok_or_else(|| {
            invalid_query("cursor must be a next_cursor this list returned").with("field", "cursor")
        })
}

/// The cursor that continues after the row with this id.
pub(super) fn next_cursor(id: i64) -> String {
    id.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn since_is_an_instant_rounded_up_to_the_millisecond() {
        let ms = |t: &str| since_ms(Some(t)).ok().flatten();
        assert_eq!(ms("1970-01-01T00:00:00.123Z"), Some(123));
        assert_eq!(ms("1970-01-01T00:00:00.1231Z"), Some(124));
        assert_eq!(ms("1970-01-01T00:00:00.123000001Z"), Some(124));
        assert_eq!(ms("1970-01-01T02:00:00.123+02:00"), Some(123));
        assert_eq!(ms("1969-12-31T23:59:59.9995Z"), Some(0));
        assert_eq!(since_ms(Some("")).ok().flatten(), None);
        assert_eq!(since_ms(None).ok().flatten(), None);
        for bad in [
            "yesterday",
            "2026-11-13",
            "1970-01-01T00:00:00.1230000001Z",
            "2016-12-31T23:59:60Z",
        ] {
            assert!(since_ms(Some(bad)).is_err(), "{bad}");
        }
    }

    #[test]
    fn a_cursor_is_a_positive_decimal_id() {
        assert_eq!(cursor_id(Some("42")).ok().flatten(), Some(42));
        assert_eq!(cursor_id(Some(&next_cursor(7))).ok().flatten(), Some(7));
        assert_eq!(cursor_id(Some("")).ok().flatten(), None);
        for bad in ["abc", "-1", "0", "+5", " 5", "99999999999999999999"] {
            assert!(cursor_id(Some(bad)).is_err(), "{bad}");
        }
    }
}
