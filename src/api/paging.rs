//! Paged lists: `?limit=&cursor=` in, `"page": {"next", "previous"}` out.
//!
//! A cursor is opaque and URL-safe (`[A-Za-z0-9_-]`). It carries a direction
//! and the sort key of the row it continues after (any serializable tuple, such
//! as `(received_at, id)`), expires 24 hours after it was issued, and is
//! authenticated with HMAC-SHA256 under an engine key kept in the store
//! (`Db::engine_key`), over the list it belongs to: a route names its list
//! with every filter that shapes it (`loads:source=labs`), so a cursor from
//! another list, an expired one or an altered one is `400 invalid_cursor`.
//!
//! The route does the traversal (keyset on its sort key, with a stable tie
//! breaker); this module only issues and reads cursors and the limit.

use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use hmac::{Hmac, Mac};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::Sha256;

use super::error::ApiError;
use crate::db::Db;

/// How long a cursor is valid.
pub const VALID_SECONDS: i64 = 86_400;
pub const DEFAULT_LIMIT: usize = 50;
pub const MAX_LIMIT: usize = 200;

const KEY_NAME: &str = "cursors";
const MAC_LEN: usize = 16;

/// Which way a cursor continues.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Direction {
    /// The rows after `after`, in the list's order.
    #[serde(rename = "n")]
    Next,
    /// The rows before `after`.
    #[serde(rename = "p")]
    Previous,
}

/// Where a cursor points: a direction and the sort key it continues after.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Position<K> {
    pub dir: Direction,
    pub after: K,
}

/// A page asked for: how many rows, and from where (`None`: the first page).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PageRequest<K> {
    pub limit: usize,
    pub position: Option<Position<K>>,
}

#[derive(Serialize, Deserialize)]
struct Payload<K> {
    d: Direction,
    a: K,
    e: i64,
}

/// `limit` checked: 1 to 200, 50 when absent; else `400 invalid_body` with
/// `field: "limit"`.
pub fn limit(limit: Option<i64>) -> Result<usize, ApiError> {
    match limit {
        None => Ok(DEFAULT_LIMIT),
        Some(n) if (1..=MAX_LIMIT as i64).contains(&n) => Ok(n as usize),
        Some(_) => Err(
            ApiError::invalid_body(format!("limit must be 1 to {MAX_LIMIT}"))
                .with("field", "limit"),
        ),
    }
}

/// Reads `limit` and `cursor` for the list named `list`.
pub fn request<K: DeserializeOwned>(
    db: &Db,
    list: &str,
    limit_param: Option<i64>,
    cursor: Option<&str>,
) -> Result<PageRequest<K>, ApiError> {
    Ok(PageRequest {
        limit: limit(limit_param)?,
        position: cursor
            .filter(|c| !c.is_empty())
            .map(|c| read(db, list, c))
            .transpose()?,
    })
}

/// A cursor for the list named `list`, continuing `dir` after `after`.
pub fn cursor<K: Serialize>(
    db: &Db,
    list: &str,
    dir: Direction,
    after: &K,
) -> anyhow::Result<String> {
    let payload = serde_json::to_vec(&Payload {
        d: dir,
        a: after,
        e: chrono::Utc::now().timestamp() + VALID_SECONDS,
    })?;
    let mut bytes = payload.clone();
    bytes.extend_from_slice(&mac(&db.engine_key(KEY_NAME)?, list, &payload));
    Ok(URL_SAFE_NO_PAD.encode(bytes))
}

/// The `page` member of a paged answer.
pub fn page(next: Option<String>, previous: Option<String>) -> Value {
    json!({"next": next, "previous": previous})
}

fn read<K: DeserializeOwned>(db: &Db, list: &str, cursor: &str) -> Result<Position<K>, ApiError> {
    let invalid = || {
        ApiError::bad_request(
            "invalid_cursor",
            "the cursor is not one this list gave, or it expired",
        )
    };
    let bytes = URL_SAFE_NO_PAD.decode(cursor).map_err(|_| invalid())?;
    if bytes.len() <= MAC_LEN {
        return Err(invalid());
    }
    let (payload, tag) = bytes.split_at(bytes.len() - MAC_LEN);
    let key = db.engine_key(KEY_NAME)?;
    let mut check = Hmac::<Sha256>::new_from_slice(&key).expect("any key length");
    check.update(&framed(list, payload));
    check.verify_truncated_left(tag).map_err(|_| invalid())?;
    let payload: Payload<K> = serde_json::from_slice(payload).map_err(|_| invalid())?;
    if payload.e < chrono::Utc::now().timestamp() {
        return Err(invalid());
    }
    Ok(Position {
        dir: payload.d,
        after: payload.a,
    })
}

fn mac(key: &[u8], list: &str, payload: &[u8]) -> [u8; MAC_LEN] {
    let mut mac = Hmac::<Sha256>::new_from_slice(key).expect("any key length");
    mac.update(&framed(list, payload));
    let full = mac.finalize().into_bytes();
    let mut out = [0u8; MAC_LEN];
    out.copy_from_slice(&full[..MAC_LEN]);
    out
}

fn framed(list: &str, payload: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(list.len() + payload.len() + 16);
    out.extend_from_slice(b"hie cursor\0");
    out.extend_from_slice(list.as_bytes());
    out.push(0);
    out.extend_from_slice(payload);
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cursors_round_trip_on_their_own_list_only() {
        let db = Db::in_memory().unwrap();
        let c = cursor(
            &db,
            "loads:source=lab",
            Direction::Next,
            &(1_700_000_000_000i64, 41i64),
        )
        .unwrap();
        assert!(
            c.bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_'),
            "{c}"
        );
        let page: PageRequest<(i64, i64)> =
            request(&db, "loads:source=lab", Some(2), Some(&c)).unwrap();
        assert_eq!(
            page,
            PageRequest {
                limit: 2,
                position: Some(Position {
                    dir: Direction::Next,
                    after: (1_700_000_000_000, 41)
                })
            }
        );
        let other = request::<(i64, i64)>(&db, "loads:source=other", None, Some(&c)).unwrap_err();
        assert_eq!(other.code(), "invalid_cursor");
        for bad in ["not-a-cursor", "", "AAAA"] {
            if !bad.is_empty() {
                assert_eq!(
                    request::<(i64, i64)>(&db, "loads:source=lab", None, Some(bad))
                        .unwrap_err()
                        .code(),
                    "invalid_cursor"
                );
            }
        }
        // One changed character anywhere is refused.
        let mut tampered: Vec<char> = c.chars().collect();
        tampered[3] = if tampered[3] == 'A' { 'B' } else { 'A' };
        let tampered: String = tampered.into_iter().collect();
        assert_eq!(
            request::<(i64, i64)>(&db, "loads:source=lab", None, Some(&tampered))
                .unwrap_err()
                .code(),
            "invalid_cursor"
        );
    }

    #[test]
    fn limits_are_one_to_two_hundred() {
        assert_eq!(limit(None).unwrap(), 50);
        assert_eq!(limit(Some(200)).unwrap(), 200);
        assert_eq!(limit(Some(0)).unwrap_err().code(), "invalid_body");
        assert!(limit(Some(201)).is_err());
    }

    #[test]
    fn an_expired_cursor_is_refused() {
        let db = Db::in_memory().unwrap();
        let payload = serde_json::to_vec(&Payload {
            d: Direction::Next,
            a: 1i64,
            e: 0,
        })
        .unwrap();
        let mut bytes = payload.clone();
        bytes.extend_from_slice(&mac(&db.engine_key(KEY_NAME).unwrap(), "l", &payload));
        let expired = URL_SAFE_NO_PAD.encode(bytes);
        assert_eq!(
            request::<i64>(&db, "l", None, Some(&expired))
                .unwrap_err()
                .code(),
            "invalid_cursor"
        );
    }
}
