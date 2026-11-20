//! The event log: what happened to each record, load and submission, appended
//! in the same transaction as the change it reports. Nothing updates an event;
//! only the retention purge removes them. See `docs/api.md` (Events).

use serde_json::Value;

/// Every kind of event, in the families `GET /api/events` filters by.
pub const KINDS: &[&str] = &[
    "record.delivered",
    "record.recovered",
    "record.attempt_failed",
    "record.rejected",
    "record.retried",
    "load.received",
    "load.duplicate",
    "load.unreadable",
    "load.finished",
    "load.retried",
    "submission.opened",
    "submission.attempt_failed",
    "submission.refused",
    "submission.settled",
];

/// The families a `kind` filter may name instead of an exact kind.
pub const FAMILIES: &[&str] = &["record", "load", "submission"];

/// The longest `reason` an event keeps, in characters.
pub const REASON_MAX: usize = 500;

/// One event, as it is written.
#[derive(Debug, Clone, Default)]
pub struct Event {
    pub kind: &'static str,
    pub source_id: Option<String>,
    pub load_id: Option<i64>,
    pub record_id: Option<i64>,
    pub attempt: Option<i64>,
    pub error_kind: Option<String>,
    pub reason: Option<String>,
    /// A JSON object, or nothing.
    pub detail: Option<Value>,
}

impl Event {
    pub fn new(kind: &'static str, source_id: &str) -> Self {
        Self {
            kind,
            source_id: Some(source_id.to_string()),
            ..Self::default()
        }
    }

    pub fn load(mut self, id: i64) -> Self {
        self.load_id = Some(id);
        self
    }

    pub fn record(mut self, id: i64) -> Self {
        self.record_id = Some(id);
        self
    }

    pub fn attempt(mut self, n: i64) -> Self {
        self.attempt = Some(n);
        self
    }

    pub fn error_kind(mut self, kind: &str) -> Self {
        self.error_kind = Some(kind.to_string());
        self
    }

    /// The reason, made safe to show: see `reason_text`.
    pub fn reason(mut self, text: &str) -> Self {
        self.reason = Some(reason_text(text));
        self
    }

    pub fn detail(mut self, detail: Value) -> Self {
        self.detail = Some(detail);
        self
    }
}

/// A reason as an event keeps it: every URL cut to its origin
/// (`bulk::safe_reason`), then at most `REASON_MAX` characters.
pub fn reason_text(text: &str) -> String {
    let safe = crate::bulk::safe_reason(text);
    match safe.char_indices().nth(REASON_MAX) {
        Some((end, _)) => safe[..end].to_string(),
        None => safe,
    }
}

/// Whether `name` is a kind or a family a filter may name.
pub fn is_known(name: &str) -> bool {
    KINDS.contains(&name) || FAMILIES.contains(&name)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reasons_are_cut_to_their_origin_and_length() {
        assert_eq!(
            reason_text("fetching https://h/a?sig=CANARY failed"),
            "fetching https://h/… failed"
        );
        let long = "é".repeat(REASON_MAX + 10);
        assert_eq!(reason_text(&long).chars().count(), REASON_MAX);
    }

    #[test]
    fn every_kind_belongs_to_a_family() {
        for kind in KINDS {
            let family = kind.split('.').next().unwrap();
            assert!(FAMILIES.contains(&family), "{kind}");
        }
        assert!(is_known("record") && is_known("load.finished"));
        assert!(!is_known("records") && !is_known("load.archived"));
    }
}
