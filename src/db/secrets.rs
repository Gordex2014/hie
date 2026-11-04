//! The store's secrets and their revisions (sealed values), and what refers to
//! them. Only ciphertext is kept (`crate::crypto`); this file never sees a
//! value. A revision that is not the current one is destroyed (its row
//! deleted) as soon as nothing refers to it.
//!
//! Every change to a secret's revisions or holds first locks the secret's row
//! (an update of it), so a rotation, a deletion and a new hold on one secret
//! never interleave; a hold names an existing revision (checked in the same
//! transaction, and by a foreign key), so it can never dangle. The `_in`
//! variants run inside a caller's transaction (an idempotent route's).

use anyhow::Result;

use super::{Db, Param, Tx, now_ms, params};
use crate::secrets::{DeleteSecret, Reserve, RevisionRow, SecretRow, SecretUse};
use serde_json::Value;

/// The DDL of this area's tables (`CREATE TABLE IF NOT EXISTS ...`).
pub(super) const TABLES: &str = "CREATE TABLE IF NOT EXISTS secrets (
  id TEXT PRIMARY KEY,
  name TEXT NOT NULL UNIQUE,
  current_revision INTEGER NOT NULL,
  created_at INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS secret_revisions (
  secret_id TEXT NOT NULL,
  revision INTEGER NOT NULL,
  sealed TEXT NOT NULL,
  created_at INTEGER NOT NULL,
  PRIMARY KEY (secret_id, revision)
);
CREATE TABLE IF NOT EXISTS secret_uses (
  secret_id TEXT NOT NULL,
  revision INTEGER NOT NULL,
  kind TEXT NOT NULL,
  holder TEXT NOT NULL,
  PRIMARY KEY (secret_id, revision, kind, holder),
  FOREIGN KEY (secret_id, revision) REFERENCES secret_revisions (secret_id, revision)
);
CREATE TABLE IF NOT EXISTS secret_custody (
  name TEXT PRIMARY KEY,
  sealed TEXT NOT NULL,
  created_at INTEGER NOT NULL
);
";

/// Columns added to this area's tables after a store was first created.
pub(super) const ADDED_COLUMNS: &[(&str, &str, &str)] = &[];

/// Locks a secret's row for the rest of the transaction; its current revision.
fn lock(tx: &mut Tx<'_>, id: &str) -> Result<Option<u32>> {
    tx.execute(
        "UPDATE secrets SET current_revision = current_revision WHERE id = $1",
        &params![id],
    )?;
    Ok(tx
        .query_json(
            "SELECT current_revision FROM secrets WHERE id = $1",
            &params![id],
        )?
        .first()
        .and_then(|r| r["current_revision"].as_i64())
        .map(|n| n as u32))
}

fn text(row: &Value, name: &str) -> String {
    row[name].as_str().unwrap_or_default().to_string()
}

impl Db {
    /// Creates a secret with its first revision, sealed by `seal(id, 1)`, in a
    /// transaction. `false` when the name is taken.
    pub fn insert_secret_in(
        tx: &mut Tx<'_>,
        id: &str,
        name: &str,
        seal: impl FnOnce(&str, u32) -> String,
    ) -> Result<bool> {
        let taken = !tx
            .query_json("SELECT id FROM secrets WHERE name = $1", &params![name])?
            .is_empty();
        if taken {
            return Ok(false);
        }
        let now = now_ms();
        tx.execute(
            "INSERT INTO secrets (id, name, current_revision, created_at) VALUES ($1, $2, 1, $3)",
            &params![id, name, now],
        )?;
        tx.execute(
            "INSERT INTO secret_revisions (secret_id, revision, sealed, created_at) VALUES ($1, 1, $2, $3)",
            &params![id, seal(id, 1), now],
        )?;
        Ok(true)
    }

    /// Adds a revision sealed by `seal(id, revision)` and makes it current, in a
    /// transaction; earlier revisions nothing refers to are destroyed. `None`
    /// when the secret does not exist.
    pub fn add_secret_revision_in(
        tx: &mut Tx<'_>,
        id: &str,
        seal: impl FnOnce(&str, u32) -> String,
    ) -> Result<Option<u32>> {
        let Some(current) = lock(tx, id)? else {
            return Ok(None);
        };
        let next = current + 1;
        tx.execute(
            "INSERT INTO secret_revisions (secret_id, revision, sealed, created_at) VALUES ($1, $2, $3, $4)",
            &params![id, next, seal(id, next), now_ms()],
        )?;
        tx.execute(
            "UPDATE secrets SET current_revision = $2 WHERE id = $1",
            &params![id, next],
        )?;
        destroy_unused(tx, Some(id))?;
        Ok(Some(next))
    }

    /// One secret, or every secret (by name), without values, in a transaction.
    pub fn secret_rows_in(tx: &mut Tx<'_>, id: Option<&str>) -> Result<Vec<SecretRow>> {
        let secrets = match id {
            Some(id) => tx.query_json(
                "SELECT id, name, current_revision, created_at FROM secrets WHERE id = $1",
                &params![id],
            )?,
            None => tx.query_json(
                "SELECT id, name, current_revision, created_at FROM secrets ORDER BY name",
                &[],
            )?,
        };
        let mut out = Vec::new();
        for s in secrets {
            let id = text(&s, "id");
            let uses = tx.query_json(
                "SELECT revision, kind, holder FROM secret_uses WHERE secret_id = $1 ORDER BY holder",
                &params![id],
            )?;
            let revisions = tx
                .query_json(
                    "SELECT revision, created_at FROM secret_revisions WHERE secret_id = $1 ORDER BY revision",
                    &params![id],
                )?
                .iter()
                .map(|r| {
                    let revision = r["revision"].as_i64().unwrap_or(0);
                    let holders = |kind: &str| {
                        uses.iter()
                            .filter(|u| u["revision"].as_i64() == Some(revision) && u["kind"] == kind)
                            .map(|u| text(u, "holder"))
                            .collect::<Vec<_>>()
                    };
                    RevisionRow {
                        revision: revision as u32,
                        created_at: r["created_at"].as_i64().unwrap_or(0),
                        evidence: holders("evidence"),
                        sources: holders("source"),
                    }
                })
                .collect();
            out.push(SecretRow {
                id,
                name: text(&s, "name"),
                current_revision: s["current_revision"].as_i64().unwrap_or(0) as u32,
                created_at: s["created_at"].as_i64().unwrap_or(0),
                revisions,
            });
        }
        Ok(out)
    }

    /// One secret, or every secret (by name), without values.
    pub fn secret_rows(&self, id: Option<&str>) -> Result<Vec<SecretRow>> {
        self.transaction(|tx| Self::secret_rows_in(tx, id))
    }

    /// A revision's sealed value, while it exists.
    pub fn secret_sealed(&self, id: &str, revision: u32) -> Result<Option<String>> {
        Ok(self
            .query_one(
                "SELECT sealed FROM secret_revisions WHERE secret_id = $1 AND revision = $2",
                &params![id, revision],
            )?
            .map(|r| r.string(0)))
    }

    /// Every sealed value kept, with its secret and revision.
    pub fn secrets_sealed(&self) -> Result<Vec<(String, u32, String)>> {
        Ok(self
            .query(
                "SELECT secret_id, revision, sealed FROM secret_revisions",
                &[],
            )?
            .iter()
            .map(|r| (r.string(0), r.i64(1) as u32, r.string(2)))
            .collect())
    }

    /// Deletes a secret and every revision, unless a source refers to it.
    pub fn delete_secret(&self, id: &str) -> Result<DeleteSecret> {
        self.transaction(|tx| {
            if lock(tx, id)?.is_none() {
                return Ok(DeleteSecret::NotFound);
            }
            let sources: Vec<String> = tx
                .query_json(
                    "SELECT DISTINCT holder FROM secret_uses WHERE secret_id = $1 AND kind = 'source' ORDER BY holder",
                    &params![id],
                )?
                .iter()
                .map(|r| text(r, "holder"))
                .collect();
            if !sources.is_empty() {
                return Ok(DeleteSecret::InUse(sources));
            }
            for sql in [
                "DELETE FROM secret_uses WHERE secret_id = $1",
                "DELETE FROM secret_revisions WHERE secret_id = $1",
                "DELETE FROM secrets WHERE id = $1",
            ] {
                tx.execute(sql, &params![id])?;
            }
            Ok(DeleteSecret::Deleted)
        })
    }

    /// Resolves a revision (the one named, else the current one) and records
    /// that `holder` refers to it, atomically: the revision cannot be
    /// destroyed between the two, and a hold is never recorded for one that
    /// does not exist.
    pub fn reserve_secret(
        &self,
        id: &str,
        revision: Option<u32>,
        kind: SecretUse,
        holder: &str,
    ) -> Result<Reserve> {
        self.transaction(|tx| {
            let Some(current) = lock(tx, id)? else {
                return Ok(Reserve::NotFound);
            };
            let revision = revision.unwrap_or(current);
            let exists = !tx
                .query_json(
                    "SELECT 1 AS x FROM secret_revisions WHERE secret_id = $1 AND revision = $2",
                    &params![id, revision],
                )?
                .is_empty();
            if !exists {
                return Ok(Reserve::RevisionNotFound);
            }
            tx.execute(
                "INSERT INTO secret_uses (secret_id, revision, kind, holder) VALUES ($1, $2, $3, $4)
                 ON CONFLICT (secret_id, revision, kind, holder) DO NOTHING",
                &params![id, revision, kind.as_str(), holder],
            )?;
            Ok(Reserve::Held(revision))
        })
    }

    /// `holder` refers to no revision any more but `keep`; revisions left
    /// without a reference, other than a secret's current one, are destroyed.
    pub fn release_secrets(
        &self,
        kind: SecretUse,
        holder: &str,
        keep: Option<(&str, u32)>,
    ) -> Result<()> {
        let (keep_secret, keep_revision) = keep.unwrap_or(("", 0));
        self.transaction(|tx| {
            let secrets: Vec<String> = tx
                .query_json(
                    "SELECT DISTINCT secret_id FROM secret_uses WHERE kind = $1 AND holder = $2 ORDER BY secret_id",
                    &params![kind.as_str(), holder],
                )?
                .iter()
                .map(|r| text(r, "secret_id"))
                .collect();
            for secret in &secrets {
                lock(tx, secret)?;
            }
            tx.execute(
                "DELETE FROM secret_uses WHERE kind = $1 AND holder = $2 AND NOT (secret_id = $3 AND revision = $4)",
                &params![kind.as_str(), holder, keep_secret, keep_revision],
            )?;
            for secret in &secrets {
                destroy_unused(tx, Some(secret))?;
            }
            Ok(())
        })
    }

    /// A value this engine keeps for itself, sealed with the secrets key
    /// (`seal` is called only when it does not exist yet); the sealed form.
    pub fn secret_custody(&self, name: &str, seal: impl FnOnce() -> String) -> Result<String> {
        if let Some(row) = self.query_one(
            "SELECT sealed FROM secret_custody WHERE name = $1",
            &params![name],
        )? {
            return Ok(row.string(0));
        }
        self.execute(
            "INSERT INTO secret_custody (name, sealed, created_at) VALUES ($1, $2, $3) ON CONFLICT (name) DO NOTHING",
            &[
                Param::Text(name.into()),
                Param::Text(seal()),
                Param::Int(now_ms()),
            ],
        )?;
        Ok(self
            .query_one(
                "SELECT sealed FROM secret_custody WHERE name = $1",
                &params![name],
            )?
            .map(|r| r.string(0))
            .unwrap_or_default())
    }
}

/// Destroys the revisions of `secret` (whose row the caller has locked) that
/// are not its current one and that nothing refers to.
fn destroy_unused(tx: &mut Tx<'_>, secret: Option<&str>) -> Result<()> {
    tx.execute(
        "DELETE FROM secret_revisions
         WHERE secret_id = $1
           AND NOT EXISTS (SELECT 1 FROM secrets s WHERE s.id = secret_revisions.secret_id AND s.current_revision = secret_revisions.revision)
           AND NOT EXISTS (SELECT 1 FROM secret_uses u WHERE u.secret_id = secret_revisions.secret_id AND u.revision = secret_revisions.revision)",
        &params![secret.unwrap_or_default()],
    )
    .map(drop)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn seal(_: &str, revision: u32) -> String {
        format!("sealed-{revision}")
    }

    #[test]
    fn a_reserved_revision_survives_rotation_and_a_hold_never_dangles() {
        let db = Db::in_memory().unwrap();
        db.transaction(|tx| Db::insert_secret_in(tx, "sec_a", "a", seal))
            .unwrap();
        // A Test resolves the current revision and holds it in one step.
        let held = db
            .reserve_secret("sec_a", None, SecretUse::Evidence, "ev_1")
            .unwrap();
        assert_eq!(held, Reserve::Held(1));
        // Rotated while the Test connects: revision 1 stays.
        db.transaction(|tx| Db::add_secret_revision_in(tx, "sec_a", seal))
            .unwrap();
        assert!(db.secret_sealed("sec_a", 1).unwrap().is_some());
        // Released: destroyed, and no hold can name it again.
        db.release_secrets(SecretUse::Evidence, "ev_1", None)
            .unwrap();
        assert!(db.secret_sealed("sec_a", 1).unwrap().is_none());
        assert_eq!(
            db.reserve_secret("sec_a", Some(1), SecretUse::Source, "labs")
                .unwrap(),
            Reserve::RevisionNotFound
        );
        assert_eq!(
            db.reserve_secret("sec_nope", None, SecretUse::Source, "labs")
                .unwrap(),
            Reserve::NotFound
        );
        let rows = db.secret_rows(Some("sec_a")).unwrap();
        assert!(rows[0].revisions.iter().all(|r| r.sources.is_empty()));
        // The store itself refuses a hold on a revision that does not exist.
        assert!(
            db.execute(
                "INSERT INTO secret_uses (secret_id, revision, kind, holder) VALUES ('sec_a', 9, 'source', 'x')",
                &[],
            )
            .is_err()
        );
    }

    #[test]
    fn a_failed_transaction_keeps_no_revision() {
        let db = Db::in_memory().unwrap();
        db.transaction(|tx| Db::insert_secret_in(tx, "sec_a", "a", seal))
            .unwrap();
        // The revision and an answer that fails to commit go together.
        let failed: Result<()> = db.transaction(|tx| {
            Db::add_secret_revision_in(tx, "sec_a", seal)?;
            anyhow::bail!("the answer could not be stored")
        });
        assert!(failed.is_err());
        assert_eq!(
            db.secret_rows(Some("sec_a")).unwrap()[0].current_revision,
            1
        );
    }
}
