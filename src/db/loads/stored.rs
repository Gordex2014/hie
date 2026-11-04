//! Rows of one load as they were stored, by reference: Try's `stored` input
//! (contract "Try", "Stored rows as input"). Records named, failed or
//! written, in the order named, then up to `passing` written rows of the same
//! load not named, in line order.
//!
//! Everything is read on one snapshot of the store (a read transaction,
//! `REPEATABLE READ` on PostgreSQL), so a row a retry, reprocess or purge
//! changes meanwhile is either refused (its attempt or content as checked) or
//! given as it was: the attempt checked and the content opened are the same
//! row's. Content is opened only in memory, for the caller; nothing here is
//! written or logged.

use std::collections::{BTreeMap, BTreeSet};

use anyhow::Result;
use serde_json::{Value, json};

use super::super::engine::{Conn, Row};
use super::super::history::OUTCOME_SQL;
use super::super::{Db, Param, params};
use super::{Column, marks};

/// A record a `stored` input names: its id, and the attempt the caller saw.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StoredRef {
    pub record: i64,
    pub attempt: Option<i64>,
}

/// One stored row with its content, and what it last became.
#[derive(Debug, Clone, PartialEq)]
pub struct StoredRow {
    pub record_id: i64,
    pub load_id: i64,
    pub line: i64,
    /// As on records: `written`, `failed`, `filtered`, `outcome_unknown` or
    /// `in_progress`.
    pub outcome: String,
    /// The record's latest attempt, as its history numbers them.
    pub attempt: i64,
    /// The version that ran that attempt; `None` when the load recorded none.
    pub transform: Option<(String, i64)>,
    pub raw: Value,
}

impl StoredRow {
    /// The reference a Try item carries as `input`.
    pub fn reference(&self) -> Value {
        json!({
            "record": self.record_id,
            "load": self.load_id,
            "line": self.line,
            "outcome": self.outcome,
            "attempt": self.attempt,
            "transform": self.transform.as_ref().map(|(id, v)| json!({"id": id, "version": v})),
        })
    }
}

/// Why [`Db::stored_rows`] gives no rows, in the order the contract checks.
#[derive(Debug)]
pub enum StoredRowsError {
    LoadNotFound,
    /// The first record named that does not exist.
    RecordNotFound(i64),
    /// The first record named that belongs to another load.
    RecordNotInLoad {
        record: i64,
        load: i64,
    },
    /// The first record named with a newer attempt than the one given.
    AttemptChanged {
        record: i64,
        attempt: i64,
    },
    /// Every named row whose content is no longer kept, with why: `expired`,
    /// `purged` or `none`.
    ContentExpired(Vec<(i64, &'static str)>),
    /// No row is left to try.
    NothingToTry,
    Store(anyhow::Error),
}

impl From<anyhow::Error> for StoredRowsError {
    fn from(e: anyhow::Error) -> Self {
        StoredRowsError::Store(e)
    }
}

/// What every stored row is read with: the record, its load, its outcome, its
/// latest attempt and the version that ran it.
const STORED_COLUMNS: &str = "r.id, r.file_id, r.line, r.run,
    COALESCE(r.exec_transform_id, f.transform_id),
    CASE WHEN r.exec_transform_id IS NULL THEN f.transform_version ELSE r.exec_version END";

/// The row's reference without its content (`STORED_COLUMNS`, then the
/// outcome at `outcome`).
fn row_of(r: &Row, outcome: usize) -> StoredRow {
    let transform = match (r.opt_string(4), r.opt_i64(5)) {
        (Some(id), Some(version)) if !id.is_empty() => Some((id, version)),
        _ => None,
    };
    StoredRow {
        record_id: r.i64(0),
        load_id: r.i64(1),
        line: r.i64(2),
        attempt: r.i64(3),
        transform,
        outcome: r.string(outcome),
        raw: Value::Null,
    }
}

impl Db {
    /// The rows of load `load_id` a `stored` input names: `named` in their
    /// order, then up to `passing` written rows of the load not named, in
    /// line order, passing over those whose content is gone. The caller has
    /// checked the counts (1 to 100 in all) and that no record is named twice.
    pub fn stored_rows(
        &self,
        load_id: i64,
        named: &[StoredRef],
        passing: usize,
    ) -> std::result::Result<Vec<StoredRow>, StoredRowsError> {
        self.stored_rows_with(load_id, named, passing, || {})
    }

    /// [`Db::stored_rows`], calling `between` after the named rows' attempts
    /// and content states are checked and before their content is read (a
    /// test writes there, as a concurrent retry or purge would).
    fn stored_rows_with(
        &self,
        load_id: i64,
        named: &[StoredRef],
        passing: usize,
        between: impl FnOnce(),
    ) -> std::result::Result<Vec<StoredRow>, StoredRowsError> {
        self.snapshot(|c| {
            if c.query("SELECT 1 FROM files WHERE id = $1", &params![load_id])?
                .is_empty()
            {
                return Err(StoredRowsError::LoadNotFound);
            }
            let mut rows = named_rows(c, load_id, named)?;
            between();
            if !rows.is_empty() {
                let ids: Vec<Param> = rows.iter().map(|r| Param::Int(r.record_id)).collect();
                let stored: BTreeMap<i64, Value> = c
                    .query(
                        &format!(
                            "SELECT id, raw FROM records WHERE id IN ({})",
                            marks(1, ids.len())
                        ),
                        &ids,
                    )?
                    .iter()
                    .map(|r| (r.i64(0), r.get(1).clone()))
                    .collect();
                let mut gone = Vec::new();
                for row in &mut rows {
                    match stored
                        .get(&row.record_id)
                        .and_then(|raw| self.open_record(Some(row.record_id), Column::Raw, raw))
                        .filter(|raw| !raw.is_null())
                    {
                        Some(raw) => row.raw = raw,
                        None => gone.push((row.record_id, "none")),
                    }
                }
                if !gone.is_empty() {
                    return Err(StoredRowsError::ContentExpired(gone));
                }
            }
            let taken: BTreeSet<i64> = rows.iter().map(|r| r.record_id).collect();
            rows.extend(self.passing_rows(c, load_id, &taken, passing)?);
            if rows.is_empty() {
                return Err(StoredRowsError::NothingToTry);
            }
            Ok(rows)
        })
    }

    /// Up to `wanted` written rows of the load, not in `taken`, in line
    /// order, whose content opens.
    fn passing_rows(
        &self,
        c: &mut Conn,
        load_id: i64,
        taken: &BTreeSet<i64>,
        wanted: usize,
    ) -> Result<Vec<StoredRow>> {
        let mut out = Vec::new();
        let mut after = (i64::MIN, i64::MIN);
        // A page holds what is wanted plus every row that may be passed over
        // for being named, so one page is enough unless content fails to open.
        let page = (wanted + taken.len()) as i64;
        while out.len() < wanted {
            let rows = c.query(
                &format!(
                    "SELECT {STORED_COLUMNS}, {OUTCOME_SQL}, r.raw FROM records r JOIN files f ON f.id = r.file_id
                     WHERE r.file_id = $1 AND r.status = 'sent' AND r.content IS NULL AND r.raw <> 'null'
                       AND (r.line > $2 OR (r.line = $2 AND r.id > $3))
                     ORDER BY r.line, r.id LIMIT $4"
                ),
                &params![load_id, after.0, after.1, page],
            )?;
            for r in &rows {
                let mut row = row_of(r, 6);
                after = (row.line, row.record_id);
                if out.len() == wanted || taken.contains(&row.record_id) {
                    continue;
                }
                if let Some(raw) = self
                    .open_record(Some(row.record_id), Column::Raw, r.get(7))
                    .filter(|raw| !raw.is_null())
                {
                    row.raw = raw;
                    out.push(row);
                }
            }
            if (rows.len() as i64) < page {
                break;
            }
        }
        Ok(out)
    }

    /// Runs `f` on one read snapshot of the store, holding its connection:
    /// SQLite's transaction takes its snapshot at its first read, PostgreSQL's
    /// `REPEATABLE READ` at its first statement.
    fn snapshot<T>(
        &self,
        f: impl FnOnce(&mut Conn) -> std::result::Result<T, StoredRowsError>,
    ) -> std::result::Result<T, StoredRowsError> {
        let mut conn = self.conn.lock().expect("database mutex poisoned");
        let begin = match &*conn {
            Conn::Sqlite(_) => "BEGIN",
            Conn::Postgres(_) => "BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY",
        };
        conn.batch(begin)?;
        match f(&mut conn) {
            Ok(value) => {
                conn.batch("COMMIT")?;
                Ok(value)
            }
            Err(e) => {
                let _ = conn.batch("ROLLBACK");
                Err(e)
            }
        }
    }
}

/// The named rows without their content, refused in the contract's order: a
/// record that does not exist, then one of another load, then one whose
/// attempt moved on, then those whose content is gone.
fn named_rows(
    c: &mut Conn,
    load_id: i64,
    named: &[StoredRef],
) -> std::result::Result<Vec<StoredRow>, StoredRowsError> {
    if named.is_empty() {
        return Ok(Vec::new());
    }
    let ids: Vec<Param> = named.iter().map(|n| Param::Int(n.record)).collect();
    let found: BTreeMap<i64, (StoredRow, Option<String>)> = c
        .query(
            &format!(
                "SELECT {STORED_COLUMNS}, {OUTCOME_SQL},
                        CASE WHEN r.content IS NOT NULL THEN r.content WHEN r.raw = 'null' THEN 'none' END
                 FROM records r JOIN files f ON f.id = r.file_id WHERE r.id IN ({})",
                marks(1, ids.len())
            ),
            &ids,
        )?
        .iter()
        .map(|r| (r.i64(0), (row_of(r, 6), r.opt_string(7))))
        .collect();
    if let Some(missing) = named.iter().find(|n| !found.contains_key(&n.record)) {
        return Err(StoredRowsError::RecordNotFound(missing.record));
    }
    let row = |n: &StoredRef| &found[&n.record].0;
    if let Some(n) = named.iter().find(|n| row(n).load_id != load_id) {
        return Err(StoredRowsError::RecordNotInLoad {
            record: n.record,
            load: row(n).load_id,
        });
    }
    if let Some(n) = named
        .iter()
        .find(|n| n.attempt.is_some_and(|a| a != row(n).attempt))
    {
        return Err(StoredRowsError::AttemptChanged {
            record: n.record,
            attempt: row(n).attempt,
        });
    }
    let gone: Vec<(i64, &'static str)> = named
        .iter()
        .filter_map(|n| {
            let state = found[&n.record].1.as_deref()?;
            Some((
                n.record,
                match state {
                    "expired" => "expired",
                    "purged" => "purged",
                    _ => "none",
                },
            ))
        })
        .collect();
    if !gone.is_empty() {
        return Err(StoredRowsError::ContentExpired(gone));
    }
    Ok(named.iter().map(|n| found[&n.record].0.clone()).collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn version() -> crate::versions::VersionRef {
        crate::versions::VersionRef {
            transform_id: "lab".into(),
            version: 2,
            code_sha256: "a".repeat(64),
            environment: crate::versions::Environment::local(),
        }
    }

    /// A load of rows `{"v": line}`, lines 1 to 6: lines 1, 3 and 5 failed,
    /// the others written. Answers the load and its record ids by line.
    fn mixed_load(db: &Db, feed: &str) -> (i64, Vec<i64>) {
        let load = db
            .open_load(feed, "a.csv", "sha", 3, None, Some(&version()))
            .unwrap();
        let rows: Vec<(i64, Value, Option<String>)> =
            (1..=6).map(|l| (l, json!({"v": l}), None)).collect();
        db.insert_records(load, feed, &rows).unwrap();
        let mut ids = Vec::new();
        for item in db.received_records(feed, 100).unwrap() {
            ids.push(item.id);
            if item.line % 2 == 1 {
                db.mark_failed(item.id, item.run, "data", "bad", None, None)
                    .unwrap();
            } else {
                db.mark_transformed(item.id, item.run, &json!([{"resourceType": "Patient"}]))
                    .unwrap();
                db.mark_sent_many(&[(item.id, item.run, json!([]))])
                    .unwrap();
            }
        }
        (load, ids)
    }

    fn named(record: i64, attempt: Option<i64>) -> StoredRef {
        StoredRef { record, attempt }
    }

    /// Named rows in their order, then passing ones in line order, with
    /// their references; on PostgreSQL too (`HIE_TEST_DATABASE_URL`).
    #[test]
    fn named_rows_then_passing_rows_with_their_references() {
        let db = Db::in_memory().unwrap();
        let (load, ids) = mixed_load(&db, "src");
        let rows = db
            .stored_rows(load, &[named(ids[4], None), named(ids[1], Some(1))], 2)
            .unwrap();
        let seen: Vec<(i64, &str, Value)> = rows
            .iter()
            .map(|r| (r.line, r.outcome.as_str(), r.raw.clone()))
            .collect();
        assert_eq!(
            seen,
            [
                (5, "failed", json!({"v": 5})),
                (2, "written", json!({"v": 2})),
                (4, "written", json!({"v": 4})),
                (6, "written", json!({"v": 6})),
            ]
        );
        assert_eq!(
            rows[0].reference(),
            json!({"record": ids[4], "load": load, "line": 5, "outcome": "failed", "attempt": 1,
                   "transform": {"id": "lab", "version": 2}})
        );
        // Passing only; more asked than there are.
        let rows = db.stored_rows(load, &[], 9).unwrap();
        assert_eq!(rows.iter().map(|r| r.line).collect::<Vec<_>>(), [2, 4, 6]);
    }

    #[test]
    fn refusals_come_in_the_contracts_order() {
        let db = Db::in_memory().unwrap();
        let (load, ids) = mixed_load(&db, "src");
        let (other, others) = mixed_load(&db, "other");
        assert!(matches!(
            db.stored_rows(999_999, &[named(999_998, None)], 0),
            Err(StoredRowsError::LoadNotFound)
        ));
        assert!(matches!(
            db.stored_rows(load, &[named(others[0], Some(9)), named(999_998, None)], 0),
            Err(StoredRowsError::RecordNotFound(999_998))
        ));
        assert!(matches!(
            db.stored_rows(load, &[named(ids[0], Some(9)), named(others[0], None)], 0),
            Err(StoredRowsError::RecordNotInLoad { record, load: l }) if record == others[0] && l == other
        ));
        assert!(matches!(
            db.stored_rows(load, &[named(ids[0], Some(9))], 0),
            Err(StoredRowsError::AttemptChanged { record, attempt: 1 }) if record == ids[0]
        ));
        db.expire_failed_content(i64::MAX).unwrap();
        match db.stored_rows(load, &[named(ids[1], None), named(ids[0], Some(1))], 1) {
            Err(StoredRowsError::ContentExpired(gone)) => assert_eq!(gone, [(ids[0], "expired")]),
            other => panic!("{other:?}"),
        }
        // A load whose rows all failed has no passing row.
        let only_failed = db.open_load("src", "b.csv", "sha2", 3, None, None).unwrap();
        db.insert_records(only_failed, "src", &[(1, json!({"v": 1}), None)])
            .unwrap();
        assert!(matches!(
            db.stored_rows(only_failed, &[], 3),
            Err(StoredRowsError::NothingToTry)
        ));
    }

    /// A passing row whose content is gone is passed over for the next one.
    #[test]
    fn passing_rows_without_content_are_passed_over() {
        let db = Db::in_memory().unwrap();
        let (load, ids) = mixed_load(&db, "src");
        db.execute(
            "UPDATE records SET raw = 'null', content = 'purged' WHERE id = $1",
            &params![ids[1]],
        )
        .unwrap();
        db.execute(
            "UPDATE records SET raw = $2 WHERE id = $1",
            &params![ids[3], "c1.1.not-sealed-for-it"],
        )
        .unwrap();
        let rows = db.stored_rows(load, &[], 2).unwrap();
        assert_eq!(rows.iter().map(|r| r.line).collect::<Vec<_>>(), [6]);
        match db.stored_rows(load, &[named(ids[3], None)], 0) {
            Err(StoredRowsError::ContentExpired(gone)) => assert_eq!(gone, [(ids[3], "none")]),
            other => panic!("{other:?}"),
        }
    }

    /// A purge or a reprocess committed by another connection (another
    /// engine on the store) after the attempts were checked and before the
    /// content is read changes nothing in what is given: the rows as they
    /// were checked. Afterwards the same request is refused. On PostgreSQL
    /// with `HIE_TEST_DATABASE_URL`, both handles share a schema.
    #[test]
    fn rows_changed_while_they_are_read_are_given_as_they_were_checked() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("hie.db");
        let a = Db::open(&path).unwrap();
        let b = Db::open(&path).unwrap();
        let (load, ids) = mixed_load(&a, "src");
        let ask = [named(ids[0], Some(1)), named(ids[2], Some(1))];

        let rows = a
            .stored_rows_with(load, &ask, 1, || {
                assert_eq!(b.expire_failed_content(i64::MAX).unwrap(), 3);
            })
            .unwrap();
        assert_eq!(
            rows.iter().map(|r| r.raw.clone()).collect::<Vec<_>>(),
            [json!({"v": 1}), json!({"v": 3}), json!({"v": 2})]
        );
        assert!(matches!(
            a.stored_rows(load, &ask, 1),
            Err(StoredRowsError::ContentExpired(gone)) if gone.len() == 2
        ));

        // A reprocess with a correction: the row is given as attempt 1 was.
        let ask = [named(ids[3], Some(1))];
        let rows = a
            .stored_rows_with(load, &ask, 0, || {
                assert_eq!(
                    b.reprocess(ids[3], Some(&json!({"v": "corrected"})))
                        .unwrap(),
                    super::super::Reprocess::Done
                );
            })
            .unwrap();
        assert_eq!(
            (rows[0].attempt, rows[0].outcome.as_str(), &rows[0].raw),
            (1, "written", &json!({"v": 4}))
        );
        assert!(matches!(
            a.stored_rows(load, &ask, 0),
            Err(StoredRowsError::AttemptChanged { attempt: 2, .. })
        ));
        let rows = a.stored_rows(load, &[named(ids[3], Some(2))], 0).unwrap();
        assert_eq!(
            (rows[0].outcome.as_str(), &rows[0].raw),
            ("in_progress", &json!({"v": "corrected"}))
        );
    }
}
