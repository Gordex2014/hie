//! Reading a list in pages, with an opaque keyset cursor. A malformed query
//! answers `400 invalid_query` with a fixed sentence that never repeats what
//! was sent.

use axum::extract::rejection::QueryRejection;
use axum::http::StatusCode;

use super::ApiError;

pub(super) fn invalid_query(sentence: &str) -> ApiError {
    ApiError::new(StatusCode::BAD_REQUEST, sentence).code("invalid_query")
}

/// A query string the extractor could not read (`limit=abc`, `open=yes`).
pub(super) fn rejected(_: QueryRejection) -> ApiError {
    invalid_query("the query string could not be read; check each parameter's type")
}

/// What a cursor starts with: its form's version, so that a later form can be
/// told apart, and a bare id is not taken for one.
const CURSOR_MARK: &str = "a1.";

/// The id a `cursor` stands for: the last row of the previous page. Empty means
/// the first page.
pub(super) fn cursor_id(cursor: Option<&str>) -> Result<Option<i64>, ApiError> {
    let Some(text) = cursor.filter(|t| !t.is_empty()) else {
        return Ok(None);
    };
    text.strip_prefix(CURSOR_MARK)
        .filter(|id| {
            !id.starts_with('0') && !id.is_empty() && id.bytes().all(|b| b.is_ascii_digit())
        })
        .and_then(|id| id.parse::<i64>().ok())
        .filter(|id| *id > 0)
        .map(Some)
        .ok_or_else(|| {
            invalid_query("cursor must be a next_cursor this list returned").with("field", "cursor")
        })
}

/// The cursor that continues after the row with this id.
pub(super) fn next_cursor(id: i64) -> String {
    format!("{CURSOR_MARK}{id}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_cursor_is_a_marked_positive_id() {
        assert_eq!(cursor_id(Some("a1.42")).ok().flatten(), Some(42));
        assert_eq!(cursor_id(Some(&next_cursor(7))).ok().flatten(), Some(7));
        assert_eq!(cursor_id(Some("")).ok().flatten(), None);
        for bad in [
            "42",
            "a1.",
            "a2.42",
            "abc",
            "a1.-1",
            "a1.0",
            "a1.07",
            "a1.+5",
            "a1. 5",
            "a1.99999999999999999999",
        ] {
            assert!(cursor_id(Some(bad)).is_err(), "{bad}");
        }
    }
}
