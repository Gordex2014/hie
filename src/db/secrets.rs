//! The store's secrets and their revisions (sealed values), and what refers to
//! them. Only ciphertext is kept (`crate::crypto`); this file never sees a
//! value. A revision that is not the current one is destroyed (its row
//! deleted) as soon as nothing refers to it.

use anyhow::Result;

use super::{Db, Param, Tx, now_ms, params};
use crate::secrets::{DeleteSecret, RevisionRow, SecretRow, SecretUse};

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
  PRIMARY KEY (secret_id, revision, kind, holder)
);
CREATE TABLE IF NOT EXISTS secret_custody (
  name TEXT PRIMARY KEY,
  sealed TEXT NOT NULL,
  created_at INTEGER NOT NULL
);
";

/// Columns added to this area's tables after a store was first created.
pub(super) const ADDED_COLUMNS: &[(&str, &str, &str)] = &[];

impl Db {
    /// Creates a secret with its first revision, sealed by `seal(id, 1)`.
    /// `false` when the name is taken.
    pub fn insert_secret(
        &self,
        id: &str,
        name: &str,
        seal: impl FnOnce(&str, u32) -> String,
    ) -> Result<bool> {
        self.transaction(|tx| {
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
        })
    }

    /// Adds a revision sealed by `seal(id, revision)` and makes it current;
    /// earlier revisions nothing refers to are destroyed. `None` when the
    /// secret does not exist.
    pub fn add_secret_revision(
        &self,
        id: &str,
        seal: impl FnOnce(&str, u32) -> String,
    ) -> Result<Option<u32>> {
        self.transaction(|tx| {
            let rows = tx.query_json(
                "SELECT current_revision FROM secrets WHERE id = $1",
                &params![id],
            )?;
            let Some(current) = rows
                .first()
                .and_then(|r| r["current_revision"].as_i64())
            else {
                return Ok(None);
            };
            let next = current as u32 + 1;
            tx.execute(
                "INSERT INTO secret_revisions (secret_id, revision, sealed, created_at) VALUES ($1, $2, $3, $4)",
                &params![id, next, seal(id, next), now_ms()],
            )?;
            tx.execute(
                "UPDATE secrets SET current_revision = $2 WHERE id = $1",
                &params![id, next],
            )?;
            destroy_unused(tx)?;
            Ok(Some(next))
        })
    }

    /// One secret, or every secret (by name), without values.
    pub fn secret_rows(&self, id: Option<&str>) -> Result<Vec<SecretRow>> {
        let secrets = match id {
            Some(id) => self.query(
                "SELECT id, name, current_revision, created_at FROM secrets WHERE id = $1",
                &params![id],
            )?,
            None => self.query(
                "SELECT id, name, current_revision, created_at FROM secrets ORDER BY name",
                &[],
            )?,
        };
        let mut out = Vec::new();
        for s in secrets {
            let id = s.string(0);
            let uses = self.query(
                "SELECT revision, kind, holder FROM secret_uses WHERE secret_id = $1 ORDER BY holder",
                &params![id],
            )?;
            let revisions = self
                .query(
                    "SELECT revision, created_at FROM secret_revisions WHERE secret_id = $1 ORDER BY revision",
                    &params![id],
                )?
                .iter()
                .map(|r| {
                    let revision = r.i64(0);
                    let holders = |kind: &str| {
                        uses.iter()
                            .filter(|u| u.i64(0) == revision && u.string(1) == kind)
                            .map(|u| u.string(2))
                            .collect::<Vec<_>>()
                    };
                    RevisionRow {
                        revision: revision as u32,
                        created_at: r.i64(1),
                        evidence: holders("evidence"),
                        sources: holders("source"),
                    }
                })
                .collect();
            out.push(SecretRow {
                id,
                name: s.string(1),
                current_revision: s.i64(2) as u32,
                created_at: s.i64(3),
                revisions,
            });
        }
        Ok(out)
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
            if tx
                .query_json("SELECT id FROM secrets WHERE id = $1", &params![id])?
                .is_empty()
            {
                return Ok(DeleteSecret::NotFound);
            }
            let sources: Vec<String> = tx
                .query_json(
                    "SELECT DISTINCT holder FROM secret_uses WHERE secret_id = $1 AND kind = 'source' ORDER BY holder",
                    &params![id],
                )?
                .iter()
                .filter_map(|r| r["holder"].as_str().map(str::to_string))
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

    /// Records that `holder` refers to a revision.
    pub fn use_secret(&self, id: &str, revision: u32, kind: SecretUse, holder: &str) -> Result<()> {
        self.execute(
            "INSERT INTO secret_uses (secret_id, revision, kind, holder) VALUES ($1, $2, $3, $4)
             ON CONFLICT (secret_id, revision, kind, holder) DO NOTHING",
            &params![id, revision, kind.as_str(), holder],
        )
        .map(drop)
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
            tx.execute(
                "DELETE FROM secret_uses WHERE kind = $1 AND holder = $2 AND NOT (secret_id = $3 AND revision = $4)",
                &params![kind.as_str(), holder, keep_secret, keep_revision],
            )?;
            destroy_unused(tx)
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

/// Destroys every revision that is not its secret's current one and that
/// nothing refers to.
fn destroy_unused(tx: &mut Tx<'_>) -> Result<()> {
    tx.execute(
        "DELETE FROM secret_revisions
         WHERE NOT EXISTS (SELECT 1 FROM secrets s WHERE s.id = secret_revisions.secret_id AND s.current_revision = secret_revisions.revision)
           AND NOT EXISTS (SELECT 1 FROM secret_uses u WHERE u.secret_id = secret_revisions.secret_id AND u.revision = secret_revisions.revision)",
        &[],
    )
    .map(drop)
}
