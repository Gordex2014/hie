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
use crate::versions::{Deleted, NewVersion, PinPeriod, PinRow, Saved, TransformInfo, VersionInfo};

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
  file_sha256 TEXT,
  legacy_name TEXT
);
CREATE UNIQUE INDEX IF NOT EXISTS transforms_display_key ON transforms(display_key);
CREATE UNIQUE INDEX IF NOT EXISTS transforms_file_path ON transforms(file_path);
CREATE UNIQUE INDEX IF NOT EXISTS transforms_legacy_name ON transforms(legacy_name);
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

const TRANSFORM_COLUMNS: &str = "id, display_name, current_version, created_at, updated_at, deleted_at, file_path, file_sha256, legacy_name";

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
        legacy_name: r.opt_string(8),
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

fn transform_where(sql: &mut impl Sql, column: &str, value: &str) -> Result<Option<TransformInfo>> {
    Ok(sql
        .row(
            &format!("SELECT {TRANSFORM_COLUMNS} FROM transforms WHERE {column} = $1"),
            &params![value],
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

    /// The transform a file (its canonical path) is, deleted ones included.
    pub fn transform_by_file(&mut self, path: &str) -> Result<Option<TransformInfo>> {
        transform_where(self, "file_path", path)
    }

    /// The transform a 0.1 name (`PUT /api/transforms/{name}`, a file's stem
    /// under `transforms/`) is, deleted ones included.
    pub fn transform_by_legacy_name(&mut self, name: &str) -> Result<Option<TransformInfo>> {
        transform_where(self, "legacy_name", name)
    }

    /// Binds a transform to its file (and the SHA-256 HIE last read or wrote
    /// there); `false` when another transform owns that file.
    pub fn bind_transform_file(&mut self, id: &str, path: &str, sha: &str) -> Result<bool> {
        Ok(self.run(
            "UPDATE transforms SET file_path = $2, file_sha256 = $3
             WHERE id = $1 AND NOT EXISTS (SELECT 1 FROM transforms o WHERE o.file_path = $2 AND o.id <> $1)",
            &params![id, path, sha],
        )? == 1)
    }

    /// The SHA-256 of one version's code.
    pub fn version_sha(&mut self, id: &str, number: i64) -> Result<Option<String>> {
        Ok(version(self, id, number)?.map(|v| v.code_sha256))
    }

    /// Makes the transaction a writer before it reads: on SQLite a
    /// transaction that read first cannot write once another connection has
    /// written (it fails instead of waiting); one that writes first waits its
    /// turn. Harmless on PostgreSQL.
    pub fn begin_write(&mut self) -> Result<()> {
        self.run(
            "UPDATE transforms SET updated_at = updated_at WHERE 1 = 0",
            &[],
        )
        .map(drop)
    }

    /// Locks a live transform's row for the rest of the transaction; `false`
    /// when it is deleted or never existed. Deleting a transform takes the same
    /// lock, so whatever starts to refer to a transform (a pin, a source, a
    /// retry) and holds this lock cannot interleave with its deletion.
    pub fn lock_live_transform(&mut self, id: &str) -> Result<bool> {
        Ok(self.run(
            "UPDATE transforms SET updated_at = updated_at WHERE id = $1 AND deleted_at IS NULL",
            &params![id],
        )? == 1)
    }

    /// Creates a transform with its version 1, bound to its file and 0.1 name
    /// when it has them. `None` when the id, the display name, the file or
    /// the name was taken meanwhile (nothing written).
    pub fn insert_transform(
        &mut self,
        id: &str,
        display_name: &str,
        display_key: &str,
        first: &NewVersion<'_>,
        file: Option<(&str, &str)>,
        legacy_name: Option<&str>,
    ) -> Result<Option<(TransformInfo, VersionInfo)>> {
        let now = now_ms();
        let (path, sha) = file.map_or((None, None), |(p, s)| (Some(p), Some(s)));
        let inserted = self.run(
            "INSERT INTO transforms (id, display_name, display_key, current_version, created_at, updated_at, file_path, file_sha256, legacy_name)
             VALUES ($1, $2, $3, 1, $4, $4, $5, $6, $7) ON CONFLICT DO NOTHING",
            &params![id, display_name, display_key, now, path, sha, legacy_name],
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

    /// Deletes a transform unless something refers to it, in one
    /// transaction under its row lock ([`Tx::lock_live_transform`]): a pin
    /// in the store, one of `sources` (the definitions the manager holds), or
    /// an unfinished load or retry running one of its versions. Its id stays
    /// taken and its versions stay (a load that snapshotted one just before
    /// still runs it), for the usage history.
    pub fn delete_transform_unused(&mut self, id: &str, sources: &[String]) -> Result<Deleted> {
        if !self.lock_live_transform(id)? {
            return Ok(Deleted::NotFound);
        }
        let mut users: Vec<String> = self
            .rows(
                "SELECT source_id FROM source_pins WHERE transform_id = $1 ORDER BY source_id",
                &params![id],
            )?
            .iter()
            .map(|r| r.string(0))
            .collect();
        for source in sources {
            if !users.contains(source) {
                users.push(source.clone());
            }
        }
        users.sort();
        if !users.is_empty() || running(self, id)? {
            return Ok(Deleted::InUse(users));
        }
        let now = now_ms();
        self.run(
            "UPDATE transforms SET deleted_at = $2, display_key = NULL, updated_at = $2, file_sha256 = NULL WHERE id = $1",
            &params![id, now],
        )?;
        Ok(Deleted::Done)
    }
}

/// Whether an unfinished load or retry runs one of the transform's versions:
/// rows of it not settled yet, or a load still being read with it.
fn running(sql: &mut impl Sql, id: &str) -> Result<bool> {
    Ok(sql
        .row(
            "SELECT 1 FROM records r JOIN files f ON f.id = r.file_id
             WHERE r.status IN ('received', 'pending', 'submitted')
               AND COALESCE(r.exec_transform_id, f.transform_id) = $1
             UNION ALL
             SELECT 1 FROM files WHERE transform_id = $1 AND status IN ('queued', 'processing', 'unpacked')
             LIMIT 1",
            &params![id],
        )?
        .is_some())
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
    /// what HIE last wrote to it or read from it; `false` when another
    /// transform owns that file.
    pub fn set_transform_file(&self, id: &str, path: &str, sha: &str) -> Result<bool> {
        Ok(self.execute(
            "UPDATE transforms SET file_path = $2, file_sha256 = $3
             WHERE id = $1 AND NOT EXISTS (SELECT 1 FROM transforms o WHERE o.file_path = $2 AND o.id <> $1)",
            &params![id, path, sha],
        )? == 1)
    }

    /// Reserves a file for a transform before HIE writes it: its path is
    /// bound, and the SHA-256 recorded is what is there now (`on_disk`) unless
    /// the path was already the transform's; `false` when another transform
    /// owns that file.
    pub fn reserve_transform_file(
        &self,
        id: &str,
        path: &str,
        on_disk: Option<&str>,
    ) -> Result<bool> {
        Ok(self.execute(
            "UPDATE transforms
             SET file_sha256 = CASE WHEN file_path = $2 THEN file_sha256 ELSE $3 END, file_path = $2
             WHERE id = $1 AND NOT EXISTS (SELECT 1 FROM transforms o WHERE o.file_path = $2 AND o.id <> $1)",
            &params![id, path, on_disk],
        )? == 1)
    }

    /// The transform a file is, deleted ones included.
    pub fn transform_by_file(&self, path: &str) -> Result<Option<TransformInfo>> {
        transform_where(&mut &*self, "file_path", path)
    }

    /// The transform a 0.1 name is, deleted ones included.
    pub fn transform_by_legacy_name(&self, name: &str) -> Result<Option<TransformInfo>> {
        transform_where(&mut &*self, "legacy_name", name)
    }

    /// The versions an unfinished load or retry may still run: rows waiting
    /// for their transform, and loads not finished (also those with no rows
    /// yet). What the runtime must keep holding.
    pub fn versions_waiting_to_run(&self) -> Result<Vec<(String, i64)>> {
        Ok(self
            .query(
                "SELECT DISTINCT COALESCE(r.exec_transform_id, f.transform_id),
                        CASE WHEN r.exec_transform_id IS NULL THEN f.transform_version ELSE r.exec_version END
                 FROM records r JOIN files f ON f.id = r.file_id
                 WHERE r.status = 'received'
                 UNION
                 SELECT transform_id, transform_version FROM files
                 WHERE status IN ('queued', 'processing', 'unpacked') AND transform_id IS NOT NULL",
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

    /// Sets a source's pin, under its transform's row lock: `false` (nothing
    /// written) when the transform is deleted. When what it runs changes, the
    /// open pin period ends and another starts, in the same transaction.
    pub fn set_pin_row(&self, source: &str, pin: &PinRow) -> Result<bool> {
        self.transaction(|tx| {
            if !tx.lock_live_transform(&pin.transform_id)? {
                return Ok(false);
            }
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
            Ok(true)
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
