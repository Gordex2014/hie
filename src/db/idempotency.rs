//! `Idempotency-Key`s: who claimed a key, for which request (its SHA-256, never
//! its body), and the answer once there is one. Kept 48 hours, so at least the
//! 24 the contract promises, and across restarts. See `crate::idempotency`.

use anyhow::Result;
use serde_json::Value;

use super::{Db, Tx, now_ms, params};

pub(super) const IDEMPOTENCY: &str = "CREATE TABLE IF NOT EXISTS idempotency_keys (
  idem_key TEXT PRIMARY KEY,
  request_sha256 TEXT NOT NULL,
  state TEXT NOT NULL,
  claim TEXT NOT NULL,
  status INTEGER,
  body TEXT,
  created_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idempotency_keys_created ON idempotency_keys(created_at);
";

/// How long a key is kept.
const KEEP_MS: i64 = 48 * 3_600_000;

/// What claiming a key found.
#[derive(Debug, Clone, PartialEq)]
pub enum Claim {
    /// The caller holds the key now and does the work; `token` completes or
    /// releases it. `recovered` when the key was taken over from a claim that
    /// never finished (the engine stopped in the middle of the work).
    Claimed { token: String, recovered: bool },
    /// Answered before.
    Completed {
        request_sha256: String,
        status: u16,
        body: Option<Value>,
    },
    /// Someone else holds it and has not answered yet.
    InProgress { request_sha256: String },
}

impl Db {
    /// Claims `key` for a request with this hash. A claim whose owner has not
    /// renewed it ([`Db::idempotency_renew`]) for `stale_after_ms` is taken over
    /// (same request only), with a new owner token. Whether it lapsed is
    /// decided by the takeover statement itself, so an owner that renews
    /// between the read and the takeover keeps its claim.
    pub fn idempotency_claim(
        &self,
        key: &str,
        request_sha256: &str,
        stale_after_ms: i64,
    ) -> Result<Claim> {
        self.execute(
            "DELETE FROM idempotency_keys WHERE created_at < $1",
            &params![now_ms() - KEEP_MS],
        )?;
        let token = {
            use rand::RngCore;
            let mut bytes = [0u8; 16];
            rand::rngs::OsRng.fill_bytes(&mut bytes);
            hex::encode(bytes)
        };
        // Each round reads the key afresh: another request may have inserted,
        // completed, renewed or pruned it in between.
        for _ in 0..5 {
            let now = now_ms();
            let inserted = self.execute(
                "INSERT INTO idempotency_keys (idem_key, request_sha256, state, claim, created_at, updated_at)
                 VALUES ($1, $2, 'in_progress', $3, $4, $4) ON CONFLICT (idem_key) DO NOTHING",
                &params![key, request_sha256, token, now],
            )?;
            if inserted == 1 {
                return Ok(Claim::Claimed {
                    token,
                    recovered: false,
                });
            }
            let Some(row) = self.query_one(
                "SELECT request_sha256, state, status, body, updated_at FROM idempotency_keys WHERE idem_key = $1",
                &params![key],
            )?
            else {
                continue;
            };
            let theirs = row.string(0);
            if row.string(1) == "completed" {
                return Ok(Claim::Completed {
                    request_sha256: theirs,
                    status: row.i64(2) as u16,
                    body: row.json(3),
                });
            }
            let cutoff = now - stale_after_ms;
            if theirs != request_sha256 || row.i64(4) >= cutoff {
                return Ok(Claim::InProgress {
                    request_sha256: theirs,
                });
            }
            if self.take_over(key, request_sha256, cutoff, &token)? {
                return Ok(Claim::Claimed {
                    token,
                    recovered: true,
                });
            }
        }
        // Still changing under us after several reads: someone is at work on it.
        Ok(Claim::InProgress {
            request_sha256: request_sha256.to_string(),
        })
    }

    /// Gives an `in_progress` claim of this request to `token`, only if it was
    /// last renewed before `cutoff`, in one statement.
    fn take_over(&self, key: &str, request_sha256: &str, cutoff: i64, token: &str) -> Result<bool> {
        Ok(self.execute(
            "UPDATE idempotency_keys SET claim = $4, updated_at = $5
             WHERE idem_key = $1 AND state = 'in_progress' AND request_sha256 = $2 AND updated_at < $3",
            &params![key, request_sha256, cutoff, token, now_ms()],
        )? == 1)
    }

    /// Renews the claim `token` owns, so it is not taken over while its work
    /// runs; `false` when the token no longer owns it.
    pub fn idempotency_renew(&self, key: &str, token: &str) -> Result<bool> {
        Ok(self.execute(RENEW, &params![key, token, now_ms()])? == 1)
    }

    /// [`Db::idempotency_renew`] inside a transaction: also holds the claim's
    /// row until the transaction ends.
    pub fn idempotency_renew_in(tx: &mut Tx<'_>, key: &str, token: &str) -> Result<bool> {
        Ok(tx.execute(RENEW, &params![key, token, now_ms()])? == 1)
    }

    /// Stores the answer of the work done under a claim; `false` (nothing
    /// stored) when `token` no longer owns it.
    pub fn idempotency_complete(
        &self,
        key: &str,
        token: &str,
        status: u16,
        body: Option<&Value>,
    ) -> Result<bool> {
        Ok(self.execute(COMPLETE, &complete_params(key, token, status, body))? == 1)
    }

    /// [`Db::idempotency_complete`] inside a transaction, so the work and its
    /// stored answer commit together.
    pub fn idempotency_complete_in(
        tx: &mut Tx<'_>,
        key: &str,
        token: &str,
        status: u16,
        body: Option<&Value>,
    ) -> Result<bool> {
        Ok(tx.execute(COMPLETE, &complete_params(key, token, status, body))? == 1)
    }

    /// Gives a claim up without an answer: the work failed, the key may be used again.
    pub fn idempotency_release(&self, key: &str, token: &str) -> Result<()> {
        self.execute(
            "DELETE FROM idempotency_keys WHERE idem_key = $1 AND claim = $2 AND state = 'in_progress'",
            &params![key, token],
        )
        .map(drop)
    }
}

const RENEW: &str = "UPDATE idempotency_keys SET updated_at = $3
     WHERE idem_key = $1 AND claim = $2 AND state = 'in_progress'";

const COMPLETE: &str =
    "UPDATE idempotency_keys SET state = 'completed', status = $3, body = $4, updated_at = $5
     WHERE idem_key = $1 AND claim = $2 AND state = 'in_progress'";

fn complete_params(key: &str, token: &str, status: u16, body: Option<&Value>) -> Vec<super::Param> {
    params![
        key,
        token,
        u32::from(status),
        body.map(Value::to_string),
        now_ms()
    ]
    .to_vec()
}

#[cfg(test)]
mod tests {
    use super::*;

    const LAPSE_MS: i64 = 60_000;

    fn lapse(db: &Db, key: &str) {
        db.execute(
            "UPDATE idempotency_keys SET updated_at = updated_at - 120000 WHERE idem_key = $1",
            &params![key],
        )
        .unwrap();
    }

    #[test]
    fn an_owner_that_renews_after_the_read_keeps_its_claim() {
        let db = Db::in_memory().unwrap();
        let Claim::Claimed { token: owner, .. } = db.idempotency_claim("k", "h", LAPSE_MS).unwrap()
        else {
            panic!("first claim");
        };
        lapse(&db, "k");
        // A second request reads the claim as lapsed ...
        let row = db
            .query_one(
                "SELECT updated_at FROM idempotency_keys WHERE idem_key = 'k'",
                &[],
            )
            .unwrap()
            .unwrap();
        let cutoff = now_ms() - LAPSE_MS;
        assert!(row.i64(0) < cutoff);
        // ... the owner renews before the takeover runs ...
        assert!(db.idempotency_renew("k", &owner).unwrap());
        // ... and the takeover finds nothing to take.
        assert!(!db.take_over("k", "h", cutoff, "intruder").unwrap());
        assert!(
            db.idempotency_renew("k", &owner).unwrap(),
            "still the owner's"
        );
        assert_eq!(
            db.idempotency_claim("k", "h", LAPSE_MS).unwrap(),
            Claim::InProgress {
                request_sha256: "h".into()
            }
        );
        // Once it really lapsed, the same request takes it over.
        lapse(&db, "k");
        assert!(matches!(
            db.idempotency_claim("k", "h", LAPSE_MS).unwrap(),
            Claim::Claimed {
                recovered: true,
                ..
            }
        ));
        assert!(!db.idempotency_renew("k", &owner).unwrap());
    }
}
