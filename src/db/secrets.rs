//! The native secret store's tables, and the registrations of sources for
//! secrets that keep a used secret from being deleted.
//!
//! `secret_keys` holds the store's key check, `secrets` one row per secret with
//! its current revision sealed, and `secret_refs` which source, published by
//! which engine instance, uses which secret. Only sealed values reach SQL.

use anyhow::{Context, Result};

use super::{Conn, Db, Param, is_busy, params};
use crate::secrets::SecretError;

const SECRET_SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS secret_keys (
  slot TEXT PRIMARY KEY,
  store_id TEXT NOT NULL,
  key_id TEXT NOT NULL,
  algorithm TEXT NOT NULL,
  check_value TEXT NOT NULL,
  created_at INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS secrets (
  id TEXT PRIMARY KEY,
  name TEXT NOT NULL UNIQUE,
  kind TEXT NOT NULL,
  hint TEXT,
  revision INTEGER NOT NULL,
  algorithm TEXT NOT NULL,
  key_id TEXT NOT NULL,
  sealed TEXT NOT NULL,
  created_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS secret_refs (
  secret_id TEXT NOT NULL,
  source_id TEXT NOT NULL,
  instance_id TEXT NOT NULL,
  created_at INTEGER NOT NULL,
  PRIMARY KEY (secret_id, source_id, instance_id)
);
CREATE INDEX IF NOT EXISTS secret_refs_owner ON secret_refs(instance_id, source_id);
";

/// The store's key check: which key sealed the store, and proof of it.
pub struct CheckRow {
    pub store_id: String,
    pub key_id: String,
    pub algorithm: String,
    pub check_value: String,
}

/// A secret's row. `sealed` is read only when the value is wanted.
pub struct SecretRow {
    pub id: String,
    pub name: String,
    pub kind: String,
    pub hint: Option<String>,
    pub revision: u32,
    pub algorithm: String,
    pub key_id: String,
    pub sealed: Option<String>,
    pub created_at: i64,
    pub updated_at: i64,
}

const SECRET_COLUMNS: &str =
    "id, name, kind, hint, revision, algorithm, key_id, created_at, updated_at";

impl SecretRow {
    fn from_row(r: &super::Row, sealed: bool) -> Self {
        Self {
            id: r.string(0),
            name: r.string(1),
            kind: r.string(2),
            hint: r.opt_string(3),
            revision: u32::try_from(r.i64(4)).unwrap_or(0),
            algorithm: r.string(5),
            key_id: r.string(6),
            created_at: r.i64(7),
            updated_at: r.i64(8),
            sealed: sealed.then(|| r.string(9)),
        }
    }
}

/// Runs `f` once more when the store was busy, and answers `Busy` when it is
/// busy again.
pub(crate) fn retry_busy<T>(
    mut f: impl FnMut() -> Result<T, SecretError>,
) -> Result<T, SecretError> {
    match f() {
        Err(SecretError::Store(e)) if is_busy(&e) => match f() {
            Err(SecretError::Store(e)) if is_busy(&e) => Err(SecretError::Busy),
            other => other,
        },
        other => other,
    }
}

impl Db {
    /// Creates the secret tables if they are missing; tried twice, as two
    /// instances starting together on PostgreSQL may race to create them.
    pub fn secret_tables(&self) -> Result<()> {
        let ddl = if self.postgres {
            SECRET_SCHEMA.replace("INTEGER", "BIGINT")
        } else {
            SECRET_SCHEMA.to_string()
        };
        self.with(|c| c.batch(&ddl))
            .or_else(|_| self.with(|c| c.batch(&ddl)))
            .context("creating the secret tables")
    }

    pub fn secret_check_row(&self) -> Result<Option<CheckRow>> {
        Ok(self
            .query_one(
                "SELECT store_id, key_id, algorithm, check_value FROM secret_keys WHERE slot = 'current'",
                &[],
            )?
            .map(|r| CheckRow {
                store_id: r.string(0),
                key_id: r.string(1),
                algorithm: r.string(2),
                check_value: r.string(3),
            }))
    }

    /// Writes the key check unless one exists: the first instance wins.
    pub fn secret_insert_check(&self, row: &CheckRow) -> Result<()> {
        self.execute(
            "INSERT INTO secret_keys (slot, store_id, key_id, algorithm, check_value, created_at) \
             VALUES ('current', $1, $2, $3, $4, $5) ON CONFLICT (slot) DO NOTHING",
            &params![
                row.store_id,
                row.key_id,
                row.algorithm,
                row.check_value,
                super::now_ms()
            ],
        )
        .map(drop)
    }

    pub fn secret_count(&self) -> Result<i64> {
        Ok(self
            .query_one("SELECT COUNT(*) FROM secrets", &[])?
            .map_or(0, |r| r.i64(0)))
    }

    /// Inserts a secret unless its id or name is taken: the rows inserted.
    pub fn secret_insert(&self, row: &SecretRow) -> Result<u64> {
        self.execute(
            "INSERT INTO secrets (id, name, kind, hint, revision, algorithm, key_id, sealed, created_at, updated_at) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10) ON CONFLICT DO NOTHING",
            &params![
                row.id,
                row.name,
                row.kind,
                row.hint,
                row.revision,
                row.algorithm,
                row.key_id,
                row.sealed.as_deref().unwrap_or_default(),
                row.created_at,
                row.updated_at
            ],
        )
    }

    pub fn secret_name_taken(&self, name: &str) -> Result<bool> {
        Ok(self
            .query_one("SELECT 1 FROM secrets WHERE name = $1", &params![name])?
            .is_some())
    }

    /// A secret by id (`sec_…`) or name, with its sealed value when `sealed`.
    pub fn secret_row(&self, reference: &str, sealed: bool) -> Result<Option<SecretRow>> {
        let column = if crate::secrets::is_id(reference) {
            "id"
        } else {
            "name"
        };
        let extra = if sealed { ", sealed" } else { "" };
        Ok(self
            .query_one(
                &format!("SELECT {SECRET_COLUMNS}{extra} FROM secrets WHERE {column} = $1"),
                &params![reference],
            )?
            .map(|r| SecretRow::from_row(&r, sealed)))
    }

    /// Every secret, without values, by name.
    pub fn secret_rows(&self) -> Result<Vec<SecretRow>> {
        let mut rows: Vec<SecretRow> = self
            .query(&format!("SELECT {SECRET_COLUMNS} FROM secrets"), &[])?
            .iter()
            .map(|r| SecretRow::from_row(r, false))
            .collect();
        rows.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(rows)
    }

    /// Replaces the sealed value if the revision is still `previous`: the rows
    /// changed (0 when another replace came first).
    pub fn secret_update(&self, row: &SecretRow, previous: u32) -> Result<u64> {
        self.execute(
            "UPDATE secrets SET revision = $1, sealed = $2, algorithm = $3, key_id = $4, hint = $5, updated_at = $6 \
             WHERE id = $7 AND revision = $8",
            &params![
                row.revision,
                row.sealed.as_deref().unwrap_or_default(),
                row.algorithm,
                row.key_id,
                row.hint,
                row.updated_at,
                row.id,
                previous
            ],
        )
    }

    /// The sources any instance has registered for a secret, deduplicated.
    pub fn secret_used_by(&self, secret_id: &str) -> Result<Vec<String>> {
        let rows = self.query(
            "SELECT DISTINCT source_id FROM secret_refs WHERE secret_id = $1 ORDER BY source_id",
            &params![secret_id],
        )?;
        Ok(rows.iter().map(|r| r.string(0)).collect())
    }

    /// Registers `source_id`, published by `owner`, for the secret: after
    /// locking the secret's row, so a concurrent delete either sees the
    /// registration or has removed the secret (`NotFound`). `true` when the
    /// registration is new, `false` when it was already there.
    pub fn secret_hold(
        &self,
        secret_id: &str,
        source_id: &str,
        owner: &str,
    ) -> Result<bool, SecretError> {
        retry_busy(|| {
            self.secret_tx(|c| {
                // The first statement takes the lock: nothing is read before it.
                if lock_secret(c, secret_id)? == 0 {
                    return Err(SecretError::NotFound);
                }
                pause_point!(self, "ref:hold:after-lock");
                let inserted = c
                    .execute(
                        "INSERT INTO secret_refs (secret_id, source_id, instance_id, created_at) \
                         VALUES ($1, $2, $3, $4) ON CONFLICT DO NOTHING",
                        &params![secret_id, source_id, owner, super::now_ms()],
                    )
                    .map_err(SecretError::Store)?;
                Ok(inserted > 0)
            })
        })
    }

    /// Removes `owner`'s registrations of `source_id`, except the one for `keep`.
    /// Never touches another owner's rows.
    pub fn secret_release(
        &self,
        source_id: &str,
        owner: &str,
        keep: Option<&str>,
    ) -> Result<(), SecretError> {
        retry_busy(|| {
            match keep {
                None => self.execute(
                    "DELETE FROM secret_refs WHERE source_id = $1 AND instance_id = $2",
                    &params![source_id, owner],
                ),
                Some(keep) => self.execute(
                    "DELETE FROM secret_refs WHERE source_id = $1 AND instance_id = $2 AND secret_id <> $3",
                    &params![source_id, owner, keep],
                ),
            }
            .map(drop)
            .map_err(SecretError::Store)
        })
    }

    /// Removes exactly one registration: what an operation that registered it
    /// and then failed before publishing takes back.
    pub fn secret_unhold(
        &self,
        secret_id: &str,
        source_id: &str,
        owner: &str,
    ) -> Result<(), SecretError> {
        retry_busy(|| {
            self.execute(
                "DELETE FROM secret_refs WHERE secret_id = $1 AND source_id = $2 AND instance_id = $3",
                &params![secret_id, source_id, owner],
            )
            .map(drop)
            .map_err(SecretError::Store)
        })
    }

    /// `owner`'s registrations, as (source id, secret id).
    pub fn secret_holds_of(&self, owner: &str) -> Result<Vec<(String, String)>, SecretError> {
        let rows = self
            .query(
                "SELECT source_id, secret_id FROM secret_refs WHERE instance_id = $1 ORDER BY source_id, secret_id",
                &params![owner],
            )
            .map_err(SecretError::Store)?;
        Ok(rows.iter().map(|r| (r.string(0), r.string(1))).collect())
    }

    /// The id a `credential:` value resolves to, without decrypting anything.
    pub fn secret_id_for(&self, reference: &str) -> Result<Option<String>, SecretError> {
        Ok(self
            .secret_row(reference, false)
            .map_err(SecretError::Store)?
            .map(|r| r.id))
    }

    /// Deletes the secret unless a source is registered for it, in one
    /// transaction that holds the secret's row lock.
    pub fn secret_delete_unless_held(&self, id: &str) -> Result<(), SecretError> {
        retry_busy(|| {
            self.secret_tx(|c| {
                if lock_secret(c, id)? == 0 {
                    return Err(SecretError::NotFound);
                }
                pause_point!(self, "ref:delete:after-lock");
                let users: Vec<String> = c
                    .query(
                        "SELECT DISTINCT source_id FROM secret_refs WHERE secret_id = $1 ORDER BY source_id",
                        &params![id],
                    )
                    .map_err(SecretError::Store)?
                    .iter()
                    .map(|r| r.string(0))
                    .collect();
                if !users.is_empty() {
                    return Err(SecretError::InUse(users));
                }
                c.execute("DELETE FROM secrets WHERE id = $1", &params![id])
                    .map_err(SecretError::Store)?;
                Ok(())
            })
        })
    }

    /// `f` inside BEGIN … COMMIT on this handle's connection; rolled back on any
    /// error.
    fn secret_tx<T>(
        &self,
        f: impl FnOnce(&mut Conn) -> Result<T, SecretError>,
    ) -> Result<T, SecretError> {
        let mut conn = self.conn.lock().expect("database mutex poisoned");
        let c = &mut *conn;
        c.batch("BEGIN").map_err(SecretError::Store)?;
        match f(c) {
            Ok(v) => match c.batch("COMMIT") {
                Ok(()) => Ok(v),
                Err(e) => {
                    let _ = c.batch("ROLLBACK");
                    Err(SecretError::Store(e))
                }
            },
            Err(e) => {
                let _ = c.batch("ROLLBACK");
                Err(e)
            }
        }
    }
}

/// Takes the secret's row lock (PostgreSQL) or the write lock (SQLite): the
/// rows matched, 0 when the secret is gone.
fn lock_secret(c: &mut Conn, id: &str) -> Result<u64, SecretError> {
    c.execute(
        "UPDATE secrets SET revision = revision WHERE id = $1",
        &[Param::Text(id.to_string())],
    )
    .map_err(SecretError::Store)
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Barrier};

    use super::*;

    /// Two handles on one store: two SQLite connections to one file, or one
    /// PostgreSQL schema under `HIE_TEST_DATABASE_URL` (named after the path).
    fn two_handles(dir: &std::path::Path) -> (Arc<Db>, Arc<Db>) {
        let path = dir.join("hie.db");
        let a = Arc::new(Db::open(&path).unwrap());
        a.secret_tables().unwrap();
        let b = Arc::new(Db::open(&path).unwrap());
        b.secret_tables().unwrap();
        (a, b)
    }

    fn seed(db: &Db, id: &str) {
        let now = super::super::now_ms();
        let inserted = db
            .secret_insert(&SecretRow {
                id: id.into(),
                name: format!("name-{id}"),
                kind: "aws".into(),
                hint: None,
                revision: 1,
                algorithm: "xchacha20poly1305".into(),
                key_id: "k".into(),
                sealed: Some("sealed".into()),
                created_at: now,
                updated_at: now,
            })
            .unwrap();
        assert_eq!(inserted, 1);
    }

    /// Holds the thread that reaches `name` until the test lets it go.
    fn pause_at(db: &Db, name: &'static str) -> (Arc<Barrier>, Arc<Barrier>) {
        let reached = Arc::new(Barrier::new(2));
        let release = Arc::new(Barrier::new(2));
        let (r, l) = (reached.clone(), release.clone());
        let once = std::sync::atomic::AtomicBool::new(false);
        db.on_pause(Arc::new(move |at| {
            if at == name && !once.swap(true, std::sync::atomic::Ordering::SeqCst) {
                r.wait();
                l.wait();
            }
        }));
        (reached, release)
    }

    #[test]
    fn a_hold_that_locks_first_makes_a_concurrent_delete_see_it() {
        let dir = tempfile::tempdir().unwrap();
        let (a, b) = two_handles(dir.path());
        seed(&a, "sec_a");
        let (reached, release) = pause_at(&a, "ref:hold:after-lock");
        let holder = {
            let a = a.clone();
            std::thread::spawn(move || a.secret_hold("sec_a", "S", "owner-1"))
        };
        reached.wait();
        eprintln!("order: hold holds the lock; delete starts");
        let deleter = {
            let b = b.clone();
            std::thread::spawn(move || b.secret_delete_unless_held("sec_a"))
        };
        // The delete blocks on the lock; give it time to get there.
        std::thread::sleep(std::time::Duration::from_millis(200));
        release.wait();
        holder.join().unwrap().unwrap();
        eprintln!("order: hold committed");
        match deleter.join().unwrap() {
            Err(SecretError::InUse(sources)) => assert_eq!(sources, ["S"]),
            other => panic!("expected InUse, got {other:?}"),
        }
        eprintln!("order: delete answered InUse");
    }

    #[test]
    fn a_delete_that_locks_first_makes_a_concurrent_hold_find_nothing() {
        let dir = tempfile::tempdir().unwrap();
        let (a, b) = two_handles(dir.path());
        seed(&a, "sec_b");
        let (reached, release) = pause_at(&b, "ref:delete:after-lock");
        let deleter = {
            let b = b.clone();
            std::thread::spawn(move || b.secret_delete_unless_held("sec_b"))
        };
        reached.wait();
        eprintln!("order: delete holds the lock; hold starts");
        let holder = {
            let a = a.clone();
            std::thread::spawn(move || a.secret_hold("sec_b", "S", "owner-1"))
        };
        std::thread::sleep(std::time::Duration::from_millis(200));
        release.wait();
        deleter.join().unwrap().unwrap();
        eprintln!("order: delete committed");
        match holder.join().unwrap() {
            Err(SecretError::NotFound) => {}
            other => panic!("expected NotFound, got {other:?}"),
        }
        eprintln!("order: hold answered NotFound");
        assert!(a.secret_holds_of("owner-1").unwrap().is_empty());
    }

    #[test]
    fn a_lock_wait_that_runs_out_twice_is_busy_not_a_store_error() {
        let dir = tempfile::tempdir().unwrap();
        let (a, b) = two_handles(dir.path());
        seed(&a, "sec_c");
        if a.is_postgres() {
            // PostgreSQL waits on the row lock; a lock timeout ends the wait with
            // 55P03, which counts as busy.
            a.query_json("SET lock_timeout = '50ms'", Vec::new())
                .unwrap();
            let (reached, release) = pause_at(&b, "ref:hold:after-lock");
            let holder = {
                let b = b.clone();
                std::thread::spawn(move || b.secret_hold("sec_c", "T", "owner-2"))
            };
            reached.wait();
            match a.secret_delete_unless_held("sec_c") {
                Err(SecretError::Busy) => {}
                other => panic!("expected Busy, got {other:?}"),
            }
            release.wait();
            holder.join().unwrap().unwrap();
            return;
        }
        a.set_busy_timeout(std::time::Duration::from_millis(50));
        let blocker = rusqlite::Connection::open(dir.path().join("hie.db")).unwrap();
        blocker.execute_batch("BEGIN IMMEDIATE").unwrap();
        match a.secret_hold("sec_c", "S", "owner-1") {
            Err(SecretError::Busy) => {}
            other => panic!("expected Busy, got {other:?}"),
        }
        match a.secret_delete_unless_held("sec_c") {
            Err(SecretError::Busy) => {}
            other => panic!("expected Busy, got {other:?}"),
        }
        blocker.execute_batch("ROLLBACK").unwrap();
        a.secret_hold("sec_c", "S", "owner-1").unwrap();
    }

    #[test]
    fn release_touches_only_its_owner_and_keeps_what_it_is_told_to() {
        let dir = tempfile::tempdir().unwrap();
        let (a, _b) = two_handles(dir.path());
        seed(&a, "sec_x");
        seed(&a, "sec_y");
        a.secret_hold("sec_x", "S", "one").unwrap();
        a.secret_hold("sec_y", "S", "one").unwrap();
        a.secret_hold("sec_x", "S", "two").unwrap();
        assert!(
            !a.secret_hold("sec_x", "S", "one").unwrap(),
            "again: no second row"
        );
        a.secret_hold("sec_y", "T", "one").unwrap();
        a.secret_unhold("sec_y", "T", "one").unwrap();
        a.secret_release("S", "one", Some("sec_y")).unwrap();
        assert_eq!(
            a.secret_holds_of("one").unwrap(),
            [("S".to_string(), "sec_y".to_string())]
        );
        assert_eq!(a.secret_used_by("sec_x").unwrap(), ["S"]);
        a.secret_release("S", "one", None).unwrap();
        assert!(a.secret_holds_of("one").unwrap().is_empty());
        assert_eq!(a.secret_holds_of("two").unwrap().len(), 1);
        assert!(matches!(
            a.secret_hold("sec_none", "S", "one"),
            Err(SecretError::NotFound)
        ));
    }
}
