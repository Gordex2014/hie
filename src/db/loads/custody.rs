//! Custody of record content: sealed with the engine's content key
//! (AES-256-GCM, `crate::crypto::Keyring`) before it is first written, each
//! ciphertext bound by its associated data to where it belongs, so it cannot
//! be moved to another row, column, stored transaction or file and still open.
//!
//! - A record's column is stored as `c1.<record id>.<sealed>`, its associated
//!   data `hie:records:<record id>:<column>`. The id in clear lets a generic
//!   read (one that did not select the record's id) open it; every read of
//!   HIE's own that knows the row checks that the ids agree.
//! - A stored transaction (`dispatches`) is bound to its id; a file the engine
//!   writes for the destination to fetch, to its path.
//! - A store written before custody holds plaintext rows: they stay readable,
//!   and [`Db::adopt_legacy_rows`] seals them in place and gives their failed
//!   rows failure groups and failure times.

use std::sync::{Arc, OnceLock, RwLock};

use anyhow::{Context, Result};
use serde_json::Value;

use super::super::history::{Failure, group_for};
use super::super::{Db, now_ms, params};
use super::bounded;
use crate::crypto::{Key, Keyring, OpenError};

/// A record column that holds content.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::db) enum Column {
    Raw,
    OriginalRaw,
    Resources,
    Response,
}

impl Column {
    pub(in crate::db) const ALL: [Column; 4] = [
        Column::Raw,
        Column::OriginalRaw,
        Column::Resources,
        Column::Response,
    ];

    pub(in crate::db) fn name(self) -> &'static str {
        match self {
            Column::Raw => "raw",
            Column::OriginalRaw => "original_raw",
            Column::Resources => "resources",
            Column::Response => "response",
        }
    }
}

/// The keys the store seals content with: the engine's once
/// [`Db::use_content_keys`] gives them, and until then a key of this process,
/// for stores opened without an engine (tests, tools).
#[derive(Default)]
pub(in crate::db) struct ContentKeys {
    engine: RwLock<Option<Arc<Keyring>>>,
    /// Where the engine writes the files it serves the destination.
    served_dir: RwLock<Option<std::path::PathBuf>>,
}

fn process_keys() -> &'static Keyring {
    static KEYS: OnceLock<Keyring> = OnceLock::new();
    KEYS.get_or_init(|| Keyring::new(vec![Key::generate()]).expect("one key"))
}

impl ContentKeys {
    fn seal(&self, plaintext: &[u8], ad: &[u8]) -> String {
        match &*self.engine.read().expect("content keys") {
            Some(keys) => keys.seal(plaintext, ad),
            None => process_keys().seal(plaintext, ad),
        }
    }

    fn open(&self, sealed: &str, ad: &[u8]) -> Result<Vec<u8>, OpenError> {
        if let Some(keys) = &*self.engine.read().expect("content keys") {
            match keys.open(sealed, ad) {
                Err(OpenError::UnknownKey(_)) => {}
                other => return other,
            }
        }
        process_keys().open(sealed, ad)
    }
}

const RECORD_PREFIX: &str = "c1.";

fn record_ad(id: i64, column: Column) -> Vec<u8> {
    format!("hie:records:{id}:{}", column.name()).into_bytes()
}

/// Whether a stored record value is sealed; plaintext content is JSON, which
/// never starts so.
pub(in crate::db) fn is_sealed(text: &str) -> bool {
    text.starts_with(RECORD_PREFIX)
}

impl Db {
    /// Seals content with these keys from now on (the engine's content keys);
    /// content sealed before stays readable.
    pub fn use_content_keys(&self, keys: Arc<Keyring>) {
        *self.content.engine.write().expect("content keys") = Some(keys);
    }

    /// Where the engine writes the files it serves the destination (its
    /// `bulk_dir`), so their custody can follow the content's.
    pub fn use_served_dir(&self, dir: &std::path::Path) {
        *self.content.served_dir.write().expect("served dir") = Some(dir.to_path_buf());
    }

    /// The directory [`Db::use_served_dir`] named.
    pub fn served_dir(&self) -> Option<std::path::PathBuf> {
        self.content.served_dir.read().expect("served dir").clone()
    }

    /// One record column's content as it is stored.
    pub(in crate::db) fn seal_record(&self, id: i64, column: Column, value: &Value) -> String {
        format!(
            "{RECORD_PREFIX}{id}.{}",
            self.content
                .seal(value.to_string().as_bytes(), &record_ad(id, column))
        )
    }

    /// A stored record column opened: sealed (and, when `id` is given, sealed
    /// for that record) or, from before custody, plain JSON; `None` for null
    /// or what cannot be opened.
    pub(in crate::db) fn open_record(
        &self,
        id: Option<i64>,
        column: Column,
        stored: &Value,
    ) -> Option<Value> {
        let text = stored.as_str()?;
        let Some(rest) = text.strip_prefix(RECORD_PREFIX) else {
            return serde_json::from_str(text).ok();
        };
        let (sealed_for, sealed) = rest.split_once('.')?;
        let sealed_for: i64 = sealed_for.parse().ok()?;
        if id.is_some_and(|id| id != sealed_for) {
            tracing::error!(target: "hie::store", record = ?id, "record content sealed for another record; not opened");
            return None;
        }
        match self.content.open(sealed, &record_ad(sealed_for, column)) {
            Ok(bytes) => serde_json::from_slice(&bytes).ok(),
            Err(e) => {
                tracing::error!(target: "hie::store", record = sealed_for, column = column.name(), "record content cannot be opened: {e}");
                None
            }
        }
    }

    /// A stored transaction's payload as it is stored, bound to its id.
    pub(in crate::db) fn seal_dispatch(&self, id: &str, payload: &Value) -> String {
        self.content.seal(
            payload.to_string().as_bytes(),
            format!("hie:dispatches:{id}").as_bytes(),
        )
    }

    pub(in crate::db) fn open_dispatch(&self, id: &str, stored: &str) -> Option<Value> {
        match self
            .content
            .open(stored, format!("hie:dispatches:{id}").as_bytes())
        {
            Ok(bytes) => serde_json::from_slice(&bytes).ok(),
            Err(e) => {
                tracing::error!(target: "hie::store", dispatch = id, "stored transaction cannot be opened: {e}");
                None
            }
        }
    }

    /// A file's content as the engine writes it to disk (a `$bulk-submit`
    /// NDJSON it serves itself): sealed, bound to `name` (its path).
    pub fn seal_file(&self, name: &str, bytes: &[u8]) -> String {
        self.content
            .seal(bytes, format!("hie:file:{name}").as_bytes())
    }

    /// The plaintext of a file [`Db::seal_file`] wrote under the same name.
    pub fn open_file(&self, name: &str, stored: &str) -> Result<Vec<u8>> {
        self.content
            .open(stored.trim(), format!("hie:file:{name}").as_bytes())
            .map_err(|e| anyhow::anyhow!("{name} cannot be opened: {e}"))
    }

    /// A row read by [`Db::query_json`] with its sealed record columns opened
    /// (checked against the row's `id` when it selected one).
    pub(in crate::db) fn reveal(&self, mut row: Value) -> Value {
        let id = row.get("id").and_then(Value::as_i64);
        if let Value::Object(members) = &mut row {
            for column in Column::ALL {
                if let Some(value) = members.get_mut(column.name())
                    && value.as_str().is_some_and(is_sealed)
                {
                    *value = self.open_record(id, column, value).unwrap_or(Value::Null);
                }
            }
        }
        row
    }

    /// Brings rows written before custody under it: failed rows without a
    /// failure group get one (by kind, reason and their load's version) and a
    /// failure time; plaintext content is sealed in place. Idempotent; answers
    /// how many rows were grouped and how many sealed.
    pub fn adopt_legacy_rows(&self) -> Result<(usize, usize)> {
        const PAGE: i64 = 500;
        let mut grouped = 0;
        loop {
            let rows = self.query(
                "SELECT r.id, r.file_id, r.error_kind, r.error, r.updated_at, f.transform_id, f.transform_version
                 FROM records r JOIN files f ON f.id = r.file_id
                 WHERE r.status = 'rejected' AND r.group_id IS NULL
                   AND COALESCE(r.error_kind, '') <> 'filtered'
                 ORDER BY r.id LIMIT $1",
                &params![PAGE],
            )?;
            if rows.is_empty() {
                break;
            }
            self.atomically(|c| {
                for r in &rows {
                    let kind = r.opt_string(2).unwrap_or_else(|| "unknown".into());
                    let reason = bounded(&r.string(3));
                    let failure = Failure {
                        kind: &kind,
                        reason: &reason,
                        location: None,
                    };
                    let transform = r.opt_string(5).zip(r.opt_i64(6));
                    let group = group_for(
                        c,
                        r.i64(1),
                        &failure,
                        transform.as_ref().map(|(t, v)| (t.as_str(), *v)),
                    )?;
                    c.execute(
                        "UPDATE records SET group_id = $2, error = $3, failed_at = COALESCE(failed_at, $4)
                         WHERE id = $1 AND group_id IS NULL",
                        &params![r.i64(0), group, reason, r.opt_i64(4).unwrap_or_else(now_ms)],
                    )?;
                }
                Ok(())
            })?;
            grouped += rows.len();
        }
        let mut sealed = 0;
        let mut after = 0;
        loop {
            let rows = self.query(
                "SELECT id, raw, original_raw, resources, response FROM records
                 WHERE id > $1 AND ((raw NOT LIKE 'c1.%' AND raw <> 'null')
                    OR (original_raw IS NOT NULL AND original_raw NOT LIKE 'c1.%')
                    OR (resources IS NOT NULL AND resources NOT LIKE 'c1.%')
                    OR (response IS NOT NULL AND response NOT LIKE 'c1.%'))
                 ORDER BY id LIMIT $2",
                &params![after, PAGE],
            )?;
            let Some(last) = rows.last() else { break };
            after = last.i64(0);
            self.atomically(|c| {
                for r in &rows {
                    let id = r.i64(0);
                    for (i, column) in Column::ALL.into_iter().enumerate() {
                        let Some(old) = r.opt_string(i + 1) else {
                            continue;
                        };
                        if is_sealed(&old) || (column == Column::Raw && old == "null") {
                            continue;
                        }
                        let value =
                            serde_json::from_str(&old).unwrap_or(Value::String(old.clone()));
                        // Only if it is still that plaintext: a write since
                        // sealed it already.
                        c.execute(
                            &format!(
                                "UPDATE records SET {name} = $2 WHERE id = $1 AND {name} = $3",
                                name = column.name()
                            ),
                            &params![id, self.seal_record(id, column, &value), old],
                        )
                        .with_context(|| format!("sealing record {id}"))?;
                    }
                }
                Ok(())
            })?;
            sealed += rows.len();
        }
        Ok((grouped, sealed))
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn content_opens_only_where_it_was_sealed() {
        let db = Db::in_memory().unwrap();
        let sealed = Value::String(db.seal_record(7, Column::Raw, &json!({"v": 1})));
        assert_eq!(
            db.open_record(Some(7), Column::Raw, &sealed),
            Some(json!({"v": 1}))
        );
        assert_eq!(
            db.open_record(None, Column::Raw, &sealed),
            Some(json!({"v": 1}))
        );
        // Another record, or another column of the same one: not opened.
        assert_eq!(db.open_record(Some(8), Column::Raw, &sealed), None);
        assert_eq!(db.open_record(Some(7), Column::Resources, &sealed), None);
        // The id in clear rewritten to another record's: the binding holds.
        let moved = Value::String(sealed.as_str().unwrap().replacen("c1.7.", "c1.8.", 1));
        assert_eq!(db.open_record(None, Column::Raw, &moved), None);
        // A stored transaction and a file are bound to their id and name.
        let tx = db.seal_dispatch("tx_a", &json!({"bundle": 1}));
        assert_eq!(db.open_dispatch("tx_a", &tx), Some(json!({"bundle": 1})));
        assert_eq!(db.open_dispatch("tx_b", &tx), None);
        let file = db.seal_file("/b/t/1.ndjson", b"{}\n");
        assert_eq!(db.open_file("/b/t/1.ndjson", &file).unwrap(), b"{}\n");
        assert!(db.open_file("/b/t/2.ndjson", &file).is_err());
    }

    /// A store from before custody: its failed rows get groups and failure
    /// times, its plaintext is sealed in place, and both stay readable.
    #[test]
    fn legacy_rows_are_grouped_and_sealed_in_place() {
        let db = Db::in_memory().unwrap();
        let load = db
            .insert_file("src", "a.csv", "sha", 3, "done", None, None)
            .unwrap();
        for (line, status, kind, error) in [
            (1, "rejected", Some("data"), Some("bad value")),
            (2, "rejected", Some("data"), Some("bad value")),
            (3, "rejected", Some("filtered"), Some("no resources")),
            (4, "sent", None, None),
        ] {
            db.execute(
                "INSERT INTO records (file_id, feed_id, line, raw, status, error_kind, error, resources, response, updated_at)
                 VALUES ($1, 'src', $2, $3, $4, $5, $6, $7, $8, 1000)",
                &params![
                    load,
                    line,
                    json!({"patient": "zq-legacy"}).to_string(),
                    status,
                    kind,
                    error,
                    (status == "sent").then(|| json!([{"resourceType": "Patient"}]).to_string()),
                    (status == "sent").then(|| json!([{"status": "201"}]).to_string())
                ],
            )
            .unwrap();
        }
        assert_eq!(db.adopt_legacy_rows().unwrap(), (2, 4));
        assert_eq!(db.adopt_legacy_rows().unwrap(), (0, 0), "idempotent");
        let failures = db.load_failures(load).unwrap();
        assert_eq!(failures.len(), 1);
        assert_eq!(
            (
                failures[0]["count"].as_i64(),
                failures[0]["content"].as_str()
            ),
            (Some(2), Some("kept"))
        );
        let stored = db
            .query(
                "SELECT raw, resources, response, failed_at FROM records ORDER BY id",
                &[],
            )
            .unwrap();
        for r in &stored {
            assert!(r.string(0).starts_with("c1."), "{}", r.string(0));
        }
        assert!(stored[3].string(1).starts_with("c1.") && stored[3].string(2).starts_with("c1."));
        assert_eq!(stored[0].opt_i64(3), Some(1000));
        assert_eq!(
            stored[2].opt_i64(3),
            None,
            "a filtered row is not failed content"
        );
        let rows = db
            .query_json("SELECT id, raw FROM records ORDER BY id", [])
            .unwrap();
        assert_eq!(rows[0]["raw"]["patient"], "zq-legacy");
        // Old rows now expire with their retention like any other.
        assert_eq!(db.expire_failed_content(2000).unwrap(), 2);
    }
}
