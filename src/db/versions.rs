//! The store's transforms, their immutable versions and environments, source
//! pins and their periods, loads and retries held for another environment,
//! Try runs (metadata only) and the program names stored at the runtime.
//!
//! Reads are methods of [`Db`]; writes that must be atomic with something else
//! (an idempotent answer, a conflict check) are methods of [`Tx`].

use anyhow::{Context, Result};
use serde_json::Value;

use super::engine::Row;
use super::{Db, Param, Tx, now_ms, params};
use crate::versions::{NewVersion, PinPeriod, PinRow, Saved, TransformInfo, VersionInfo};

/// The DDL of this area's tables (`CREATE TABLE IF NOT EXISTS ...`).
pub(super) const TABLES: &str = "CREATE TABLE IF NOT EXISTS transforms (
  id TEXT PRIMARY KEY,
  display_name TEXT NOT NULL,
  display_key TEXT,
  current_version INTEGER NOT NULL,
  created_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL,
  deleted_at INTEGER,
  file_path TEXT,
  file_sha256 TEXT
);
CREATE UNIQUE INDEX IF NOT EXISTS transforms_display_key ON transforms(display_key);
CREATE TABLE IF NOT EXISTS transform_versions (
  transform_id TEXT NOT NULL,
  version INTEGER NOT NULL,
  code TEXT,
  code_sha256 TEXT NOT NULL,
  note TEXT,
  author TEXT,
  display_name TEXT NOT NULL,
  base_version INTEGER,
  created_at INTEGER NOT NULL,
  environment TEXT NOT NULL,
  PRIMARY KEY (transform_id, version)
);
CREATE TABLE IF NOT EXISTS source_pins (
  source_id TEXT PRIMARY KEY,
  transform_id TEXT NOT NULL,
  version INTEGER,
  declared TEXT NOT NULL,
  updated_at INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS pin_periods (
  id INTEGER PRIMARY KEY,
  source_id TEXT NOT NULL,
  transform_id TEXT NOT NULL,
  version INTEGER,
  since INTEGER NOT NULL,
  until INTEGER
);
CREATE INDEX IF NOT EXISTS pin_periods_transform ON pin_periods(transform_id);
CREATE INDEX IF NOT EXISTS pin_periods_source ON pin_periods(source_id, until);
CREATE TABLE IF NOT EXISTS version_waits (
  load_id INTEGER NOT NULL,
  retry TEXT NOT NULL,
  reason TEXT NOT NULL,
  since INTEGER NOT NULL,
  PRIMARY KEY (load_id, retry)
);
CREATE TABLE IF NOT EXISTS try_runs (
  id TEXT PRIMARY KEY,
  status TEXT NOT NULL,
  run TEXT NOT NULL,
  coverage TEXT NOT NULL,
  created_at INTEGER NOT NULL,
  stale_at INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS runtime_programs (
  name TEXT PRIMARY KEY,
  transform_id TEXT NOT NULL,
  version INTEGER NOT NULL,
  stored_at INTEGER NOT NULL
);
";

/// Columns added to this area's tables after a store was first created.
pub(super) const ADDED_COLUMNS: &[(&str, &str, &str)] = &[];

/// How long a Try run id stays reserved, and its outcome readable.
const TRY_KEPT_MS: i64 = 24 * 3_600_000;

/// What runs SQL: the store, or a transaction of it.
trait Sql {
    fn run(&mut self, sql: &str, p: &[Param]) -> Result<u64>;
    fn rows(&mut self, sql: &str, p: &[Param]) -> Result<Vec<Row>>;
    fn row(&mut self, sql: &str, p: &[Param]) -> Result<Option<Row>> {
        Ok(self.rows(sql, p)?.into_iter().next())
    }
}

impl Sql for &Db {
    fn run(&mut self, sql: &str, p: &[Param]) -> Result<u64> {
        self.execute(sql, p)
    }
    fn rows(&mut self, sql: &str, p: &[Param]) -> Result<Vec<Row>> {
        self.query(sql, p)
    }
}

impl Sql for Tx<'_> {
    fn run(&mut self, sql: &str, p: &[Param]) -> Result<u64> {
        self.conn.execute(sql, p)
    }
    fn rows(&mut self, sql: &str, p: &[Param]) -> Result<Vec<Row>> {
        self.conn.query(sql, p)
    }
}

const TRANSFORM_COLUMNS: &str =
    "id, display_name, current_version, created_at, updated_at, deleted_at, file_path, file_sha256";

fn transform_of(r: &Row) -> TransformInfo {
    TransformInfo {
        id: r.string(0),
        display_name: r.string(1),
        current_version: r.i64(2),
        created_at: r.i64(3),
        updated_at: r.i64(4),
        deleted: r.opt_i64(5).is_some(),
        file_path: r.opt_string(6),
        file_sha256: r.opt_string(7),
    }
}

const VERSION_COLUMNS: &str = "transform_id, version, code_sha256, note, author, display_name, base_version, created_at, environment";

fn version_of(r: &Row) -> Result<VersionInfo> {
    Ok(VersionInfo {
        transform_id: r.string(0),
        version: r.i64(1),
        code_sha256: r.string(2),
        note: r.opt_string(3),
        author: r.opt_string(4),
        display_name: r.string(5),
        base_version: r.opt_i64(6),
        created_at: r.i64(7),
        environment: serde_json::from_str(&r.string(8)).context("a version's environment")?,
    })
}

fn transform(sql: &mut impl Sql, id: &str) -> Result<Option<TransformInfo>> {
    Ok(sql
        .row(
            &format!("SELECT {TRANSFORM_COLUMNS} FROM transforms WHERE id = $1"),
            &params![id],
        )?
        .as_ref()
        .map(transform_of))
}

fn version(sql: &mut impl Sql, id: &str, version: i64) -> Result<Option<VersionInfo>> {
    sql.row(
        &format!("SELECT {VERSION_COLUMNS} FROM transform_versions WHERE transform_id = $1 AND version = $2"),
        &params![id, version],
    )?
    .as_ref()
    .map(version_of)
    .transpose()
}

impl Tx<'_> {
    /// One transform, deleted ones included.
    pub fn transform(&mut self, id: &str) -> Result<Option<TransformInfo>> {
        transform(self, id)
    }

    /// Whether a live transform other than `except` holds this display name,
    /// compared without case.
    pub fn display_name_taken(&mut self, key: &str, except: Option<&str>) -> Result<bool> {
        Ok(self
            .row(
                "SELECT id FROM transforms WHERE display_key = $1 AND id <> COALESCE(CAST($2 AS TEXT), '')",
                &params![key, except],
            )?
            .is_some())
    }

    /// Creates a transform with its version 1. `false` when the id or the
    /// display name was taken meanwhile (nothing written).
    pub fn insert_transform(
        &mut self,
        id: &str,
        display_name: &str,
        display_key: &str,
        first: &NewVersion<'_>,
    ) -> Result<Option<(TransformInfo, VersionInfo)>> {
        let now = now_ms();
        let inserted = self.run(
            "INSERT INTO transforms (id, display_name, display_key, current_version, created_at, updated_at)
             VALUES ($1, $2, $3, 1, $4, $4) ON CONFLICT DO NOTHING",
            &params![id, display_name, display_key, now],
        )?;
        if inserted == 0 {
            return Ok(None);
        }
        let v = self.insert_version(id, 1, None, display_name, first, now)?;
        let t = transform(self, id)?.context("the transform just created")?;
        Ok(Some((t, v)))
    }

    /// Brings a deleted transform back with a new version (a file found again).
    pub fn revive_transform(
        &mut self,
        id: &str,
        display_name: &str,
        display_key: &str,
        content: &NewVersion<'_>,
    ) -> Result<Option<VersionInfo>> {
        let Some(t) = transform(self, id)? else {
            return Ok(None);
        };
        let now = now_ms();
        let next = t.current_version + 1;
        self.run(
            "UPDATE transforms SET deleted_at = NULL, display_name = $2, display_key = $3,
                    current_version = $4, updated_at = $5 WHERE id = $1",
            &params![id, display_name, display_key, next, now],
        )?;
        self.insert_version(id, next, None, display_name, content, now)
            .map(Some)
    }

    fn insert_version(
        &mut self,
        id: &str,
        number: i64,
        base: Option<i64>,
        display_name: &str,
        v: &NewVersion<'_>,
        now: i64,
    ) -> Result<VersionInfo> {
        self.run(
            "INSERT INTO transform_versions (transform_id, version, code, code_sha256, note, author, display_name, base_version, created_at, environment)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)",
            &params![
                id,
                number,
                v.code,
                v.code_sha256,
                v.note,
                v.author,
                display_name,
                base,
                now,
                serde_json::to_string(v.environment)?
            ],
        )?;
        version(self, id, number)?.context("the version just saved")
    }

    /// Saves a new version on `base` (the current version when `None`): it is
    /// numbered `base + 1` only if `base` is still the current version. The
    /// check and the move are one conditional update, the transaction's first
    /// statement when `base` is given, so of two saves on one base exactly one
    /// wins, on either engine and across processes.
    pub fn save_version(
        &mut self,
        id: &str,
        base: Option<i64>,
        v: &NewVersion<'_>,
    ) -> Result<Saved> {
        let base = match base {
            Some(base) => base,
            None => match transform(self, id)?.filter(|t| !t.deleted) {
                Some(t) => t.current_version,
                None => return Ok(Saved::NotFound),
            },
        };
        let now = now_ms();
        let moved = self.run(
            "UPDATE transforms SET current_version = $2, updated_at = $3
             WHERE id = $1 AND current_version = $4 AND deleted_at IS NULL",
            &params![id, base + 1, now, base],
        )?;
        let t = transform(self, id)?.filter(|t| !t.deleted);
        let Some(t) = t else {
            return Ok(Saved::NotFound);
        };
        if moved == 0 {
            return Ok(Saved::Conflict {
                current_version: t.current_version,
            });
        }
        self.insert_version(id, base + 1, Some(base), &t.display_name, v, now)
            .map(|v| Saved::Version(Box::new(v)))
    }

    /// Renames a live transform; `false` when the name is taken meanwhile.
    pub fn rename_transform(&mut self, id: &str, display_name: &str, key: &str) -> Result<bool> {
        if self.display_name_taken(key, Some(id))? {
            return Ok(false);
        }
        self.run(
            "UPDATE transforms SET display_name = $2, display_key = $3, updated_at = $4 WHERE id = $1",
            &params![id, display_name, key, now_ms()],
        )?;
        Ok(true)
    }

    /// Deletes a transform: its id stays taken and its versions' metadata stays
    /// for the usage history; its code goes.
    pub fn delete_transform(&mut self, id: &str) -> Result<()> {
        let now = now_ms();
        self.run(
            "UPDATE transforms SET deleted_at = $2, display_key = NULL, updated_at = $2, file_sha256 = NULL WHERE id = $1",
            &params![id, now],
        )?;
        self.run(
            "UPDATE transform_versions SET code = NULL WHERE transform_id = $1",
            &params![id],
        )?;
        Ok(())
    }
}

impl Db {
    /// One transform, deleted ones included.
    pub fn transform_info(&self, id: &str) -> Result<Option<TransformInfo>> {
        transform(&mut &*self, id)
    }

    /// Every live transform, by id.
    pub fn transform_infos(&self) -> Result<Vec<TransformInfo>> {
        Ok(self
            .query(
                &format!("SELECT {TRANSFORM_COLUMNS} FROM transforms WHERE deleted_at IS NULL ORDER BY id"),
                &[],
            )?
            .iter()
            .map(transform_of)
            .collect())
    }

    /// One version's metadata.
    pub fn version_info(&self, id: &str, number: i64) -> Result<Option<VersionInfo>> {
        version(&mut &*self, id, number)
    }

    /// Every version of a transform, newest first.
    pub fn version_infos(&self, id: &str) -> Result<Vec<VersionInfo>> {
        self.query(
            &format!("SELECT {VERSION_COLUMNS} FROM transform_versions WHERE transform_id = $1 ORDER BY version DESC"),
            &params![id],
        )?
        .iter()
        .map(version_of)
        .collect()
    }

    /// A version's code; `None` once its transform is deleted.
    pub fn version_code(&self, id: &str, number: i64) -> Result<Option<String>> {
        Ok(self
            .query_one(
                "SELECT code FROM transform_versions WHERE transform_id = $1 AND version = $2",
                &params![id, number],
            )?
            .and_then(|r| r.opt_string(0)))
    }

    /// Records a transform's file (its canonical path) and the SHA-256 of
    /// what HIE last wrote to it or read from it.
    pub fn set_transform_file(&self, id: &str, path: &str, sha: &str) -> Result<()> {
        self.execute(
            "UPDATE transforms SET file_path = $2, file_sha256 = $3 WHERE id = $1",
            &params![id, path, sha],
        )
        .map(drop)
    }

    /// The transform a file (its canonical path) is, deleted ones included.
    pub fn transform_by_file(&self, path: &str) -> Result<Option<TransformInfo>> {
        Ok(self
            .query_one(
                &format!("SELECT {TRANSFORM_COLUMNS} FROM transforms WHERE file_path = $1 ORDER BY id LIMIT 1"),
                &params![path],
            )?
            .as_ref()
            .map(transform_of))
    }

    /// Whether an unfinished load or retry runs one of the transform's
    /// versions: rows of it waiting for the transform, or a load still being
    /// read with it.
    pub fn transform_running(&self, id: &str) -> Result<bool> {
        Ok(self
            .query_one(
                "SELECT 1 FROM records r JOIN files f ON f.id = r.file_id
                 WHERE r.status IN ('received', 'pending', 'submitted')
                   AND COALESCE(r.exec_transform_id, f.transform_id) = $1
                 UNION ALL
                 SELECT 1 FROM files WHERE transform_id = $1 AND status IN ('processing', 'unpacked')
                 LIMIT 1",
                &params![id],
            )?
            .is_some())
    }

    /// The versions rows still waiting for their transform run: what the
    /// runtime must keep holding.
    pub fn versions_waiting_to_run(&self) -> Result<Vec<(String, i64)>> {
        Ok(self
            .query(
                "SELECT DISTINCT COALESCE(r.exec_transform_id, f.transform_id),
                        CASE WHEN r.exec_transform_id IS NULL THEN f.transform_version ELSE r.exec_version END
                 FROM records r JOIN files f ON f.id = r.file_id
                 WHERE r.status = 'received'",
                &[],
            )?
            .iter()
            .filter_map(|r| Some((r.opt_string(0)?, r.opt_i64(1)?)))
            .collect())
    }

    // ---- pins -------------------------------------------------------------------

    /// A source's pin as the store holds it.
    pub fn pin_row(&self, source: &str) -> Result<Option<PinRow>> {
        Ok(self
            .query_one(
                "SELECT transform_id, version, declared FROM source_pins WHERE source_id = $1",
                &params![source],
            )?
            .map(|r| PinRow {
                transform_id: r.string(0),
                version: r.opt_i64(1),
                declared: r.string(2),
            }))
    }

    /// Sets a source's pin. When what it runs changes, the open pin period
    /// ends and another starts, in the same transaction.
    pub fn set_pin_row(&self, source: &str, pin: &PinRow) -> Result<()> {
        self.transaction(|tx| {
            let now = now_ms();
            let before = tx.row(
                "SELECT transform_id, version FROM source_pins WHERE source_id = $1",
                &params![source],
            )?;
            let same = before
                .as_ref()
                .is_some_and(|r| r.string(0) == pin.transform_id && r.opt_i64(1) == pin.version);
            tx.run(
                "INSERT INTO source_pins (source_id, transform_id, version, declared, updated_at)
                 VALUES ($1, $2, $3, $4, $5)
                 ON CONFLICT (source_id) DO UPDATE SET transform_id = excluded.transform_id,
                   version = excluded.version, declared = excluded.declared, updated_at = excluded.updated_at",
                &params![source, pin.transform_id, pin.version, pin.declared, now],
            )?;
            if !same {
                tx.run(
                    "UPDATE pin_periods SET until = $2 WHERE source_id = $1 AND until IS NULL",
                    &params![source, now],
                )?;
                tx.run(
                    "INSERT INTO pin_periods (source_id, transform_id, version, since) VALUES ($1, $2, $3, $4)",
                    &params![source, pin.transform_id, pin.version, now],
                )?;
            }
            Ok(())
        })
    }

    /// A source is gone: its pin period ends and its pin is forgotten.
    pub fn remove_pin_row(&self, source: &str) -> Result<()> {
        self.transaction(|tx| {
            tx.run(
                "UPDATE pin_periods SET until = $2 WHERE source_id = $1 AND until IS NULL",
                &params![source, now_ms()],
            )?;
            tx.run(
                "DELETE FROM source_pins WHERE source_id = $1",
                &params![source],
            )
            .map(drop)
        })
    }

    /// Every source whose pin names the transform.
    pub fn pinned_sources(&self, transform_id: &str) -> Result<Vec<String>> {
        Ok(self
            .query(
                "SELECT source_id FROM source_pins WHERE transform_id = $1 ORDER BY source_id",
                &params![transform_id],
            )?
            .iter()
            .map(|r| r.string(0))
            .collect())
    }

    /// Every pin: what the runtime must keep holding.
    pub fn pin_rows(&self) -> Result<Vec<PinRow>> {
        Ok(self
            .query(
                "SELECT transform_id, version, declared FROM source_pins",
                &[],
            )?
            .iter()
            .map(|r| PinRow {
                transform_id: r.string(0),
                version: r.opt_i64(1),
                declared: r.string(2),
            })
            .collect())
    }

    /// Every pin period that named the transform, oldest first.
    pub fn pin_periods(&self, transform_id: &str) -> Result<Vec<PinPeriod>> {
        Ok(self
            .query(
                "SELECT source_id, version, since, until FROM pin_periods WHERE transform_id = $1 ORDER BY since, id",
                &params![transform_id],
            )?
            .iter()
            .map(|r| PinPeriod {
                source: r.string(0),
                version: r.opt_i64(1),
                since: r.i64(2),
                until: r.opt_i64(3),
            })
            .collect())
    }

    // ---- loads and retries held for their environment ---------------------------

    /// Why a load (`retry: None`) or a retry of it is held without failing by
    /// the version store: `environment_unavailable` while the executor runs
    /// another environment than its version's; `None` otherwise.
    pub fn version_waiting(&self, load_id: i64, retry: Option<&str>) -> Result<Option<String>> {
        Ok(self
            .query_one(
                "SELECT reason FROM version_waits WHERE load_id = $1 AND retry = $2",
                &params![load_id, retry.unwrap_or("")],
            )?
            .map(|r| r.string(0)))
    }

    /// Marks a load or retry held, or (`reason: None`) no longer held.
    pub fn set_version_waiting(
        &self,
        load_id: i64,
        retry: Option<&str>,
        reason: Option<&str>,
    ) -> Result<()> {
        match reason {
            Some(reason) => self.execute(
                "INSERT INTO version_waits (load_id, retry, reason, since) VALUES ($1, $2, $3, $4)
                 ON CONFLICT (load_id, retry) DO UPDATE SET reason = excluded.reason",
                &params![load_id, retry.unwrap_or(""), reason, now_ms()],
            ),
            None => self.execute(
                "DELETE FROM version_waits WHERE load_id = $1 AND retry = $2",
                &params![load_id, retry.unwrap_or("")],
            ),
        }
        .map(drop)
    }

    // ---- Try runs ------------------------------------------------------------------

    /// Reserves a Try run id for 24 hours; `false` when it is already reserved.
    pub fn try_reserve(
        &self,
        id: &str,
        run: &Value,
        coverage: &Value,
        stale_at: i64,
    ) -> Result<bool> {
        let now = now_ms();
        self.execute(
            "DELETE FROM try_runs WHERE created_at < $1",
            &params![now - TRY_KEPT_MS],
        )?;
        Ok(self.execute(
            "INSERT INTO try_runs (id, status, run, coverage, created_at, stale_at)
             VALUES ($1, 'running', $2, $3, $4, $5) ON CONFLICT DO NOTHING",
            &params![id, run.to_string(), coverage.to_string(), now, stale_at],
        )? == 1)
    }

    /// Gives a reserved run id back (nothing was run).
    pub fn try_release(&self, id: &str) -> Result<()> {
        self.execute("DELETE FROM try_runs WHERE id = $1", &params![id])
            .map(drop)
    }

    /// Stores a finished run's metadata (never its inputs, results or diagnostics).
    pub fn try_finish(&self, id: &str, run: &Value, coverage: &Value) -> Result<()> {
        self.execute(
            "UPDATE try_runs SET status = 'finished', run = $2, coverage = $3 WHERE id = $1",
            &params![id, run.to_string(), coverage.to_string()],
        )
        .map(drop)
    }

    /// A run kept in the last 24 hours: `(finished, run, coverage, stale_at)`.
    pub fn try_run(&self, id: &str) -> Result<Option<(bool, Value, Value, i64)>> {
        let Some(r) = self.query_one(
            "SELECT status, run, coverage, stale_at, created_at FROM try_runs WHERE id = $1",
            &params![id],
        )?
        else {
            return Ok(None);
        };
        if r.i64(4) < now_ms() - TRY_KEPT_MS {
            return Ok(None);
        }
        Ok(Some((
            r.string(0) == "finished",
            serde_json::from_str(&r.string(1))?,
            serde_json::from_str(&r.string(2))?,
            r.i64(3),
        )))
    }

    // ---- program names at the runtime ---------------------------------------------

    /// A version's code was stored at the runtime under `name`.
    pub fn program_stored(&self, name: &str, transform_id: &str, version: i64) -> Result<()> {
        self.execute(
            "INSERT INTO runtime_programs (name, transform_id, version, stored_at) VALUES ($1, $2, $3, $4)
             ON CONFLICT (name) DO UPDATE SET stored_at = excluded.stored_at",
            &params![name, transform_id, version, now_ms()],
        )
        .map(drop)
    }

    /// Every name stored at the runtime, with its version.
    pub fn programs(&self) -> Result<Vec<(String, String, i64)>> {
        Ok(self
            .query(
                "SELECT name, transform_id, version FROM runtime_programs",
                &[],
            )?
            .iter()
            .map(|r| (r.string(0), r.string(1), r.i64(2)))
            .collect())
    }

    /// A name was removed from the runtime.
    pub fn program_forgotten(&self, name: &str) -> Result<()> {
        self.execute(
            "DELETE FROM runtime_programs WHERE name = $1",
            &params![name],
        )
        .map(drop)
    }
}
