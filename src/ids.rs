//! Ids and times for what 0.2.0 creates and answers.
//!
//! **Ids.** Everything HIE creates gets an opaque id: a prefix naming what it
//! is (`sec_`, `ev_`, `smp_`, `rt_`, `ps_`, `g_`) and 128 random bits from the
//! operating system, as 26 lower-case base32 characters, so an id cannot be
//! guessed from another. Clients treat them as opaque; HIE only checks the
//! prefix ([`is`]).
//!
//! **Times.** 0.2.0 members are RFC 3339 in UTC with milliseconds and `Z`
//! (`2026-11-03T10:15:00.000Z`), from [`rfc3339_ms`]. 0.1 members keep the
//! format they always had.

use chrono::{DateTime, SecondsFormat, Utc};
use rand::RngCore;

/// What an id names.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    Secret,
    Evidence,
    Sample,
    Retry,
    Pass,
    /// A failure group of a load.
    Group,
}

impl Kind {
    pub fn prefix(self) -> &'static str {
        match self {
            Kind::Secret => "sec_",
            Kind::Evidence => "ev_",
            Kind::Sample => "smp_",
            Kind::Retry => "rt_",
            Kind::Pass => "ps_",
            Kind::Group => "g_",
        }
    }
}

/// A new, unguessable id of this kind.
pub fn new(kind: Kind) -> String {
    let mut bytes = [0u8; 16];
    rand::rngs::OsRng.fill_bytes(&mut bytes);
    format!("{}{}", kind.prefix(), base32(&bytes))
}

/// Whether `id` has the shape of an id of this kind (prefix and characters);
/// not whether it exists.
pub fn is(kind: Kind, id: &str) -> bool {
    id.strip_prefix(kind.prefix()).is_some_and(|rest| {
        !rest.is_empty()
            && rest.len() <= 64
            && rest
                .bytes()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit())
    })
}

/// RFC 4648 base32, lower case, without padding.
fn base32(bytes: &[u8]) -> String {
    const ALPHABET: &[u8; 32] = b"abcdefghijklmnopqrstuvwxyz234567";
    let mut out = String::with_capacity(bytes.len() * 8 / 5 + 1);
    let (mut buffer, mut bits) = (0u32, 0u32);
    for &b in bytes {
        buffer = (buffer << 8) | u32::from(b);
        bits += 8;
        while bits >= 5 {
            bits -= 5;
            out.push(ALPHABET[((buffer >> bits) & 31) as usize] as char);
        }
    }
    if bits > 0 {
        out.push(ALPHABET[((buffer << (5 - bits)) & 31) as usize] as char);
    }
    out
}

/// A 0.2.0 time: RFC 3339, UTC, milliseconds, `Z`.
pub fn rfc3339_ms(t: DateTime<Utc>) -> String {
    t.to_rfc3339_opts(SecondsFormat::Millis, true)
}

/// [`rfc3339_ms`] of an epoch-milliseconds instant, as the store keeps them.
pub fn rfc3339_ms_at(epoch_ms: i64) -> Option<String> {
    DateTime::from_timestamp_millis(epoch_ms).map(rfc3339_ms)
}

/// The time now, as [`rfc3339_ms`] writes it.
pub fn now_rfc3339_ms() -> String {
    rfc3339_ms(Utc::now())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ids_are_prefixed_and_distinct() {
        let a = new(Kind::Secret);
        let b = new(Kind::Secret);
        assert!(a.starts_with("sec_") && a.len() == 4 + 26, "{a}");
        assert_ne!(a, b);
        assert!(is(Kind::Secret, &a));
        assert!(!is(Kind::Retry, &a));
        assert!(is(Kind::Group, &new(Kind::Group)));
        assert!(!is(Kind::Retry, "rt_"));
    }

    #[test]
    fn times_have_milliseconds_and_z() {
        assert_eq!(rfc3339_ms_at(0).unwrap(), "1970-01-01T00:00:00.000Z");
        assert_eq!(
            rfc3339_ms_at(1_793_787_300_123).unwrap(),
            "2026-11-04T10:15:00.123Z"
        );
    }
}
