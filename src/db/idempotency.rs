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
    /// Claims `key` for a request with this hash. A claim older than
    /// `stale_after_ms` that never finished is taken over (same request only).
    pub fn idempotency_claim(
        &self,
        key: &str,
        request_sha256: &str,
        stale_after_ms: i64,
    ) -> Result<Claim> {
        let now = now_ms();
        self.execute(
            "DELETE FROM idempotency_keys WHERE created_at < $1",
            &params![now - KEEP_MS],
        )?;
        let token = {
            use rand::RngCore;
            let mut bytes = [0u8; 16];
            rand::rngs::OsRng.fill_bytes(&mut bytes);
            hex::encode(bytes)
        };
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
            "SELECT request_sha256, state, claim, status, body, updated_at FROM idempotency_keys WHERE idem_key = $1",
            &params![key],
        )?
        else {
            // Pruned between the two statements: claim it again.
            return self.idempotency_claim(key, request_sha256, stale_after_ms);
        };
        let theirs = row.string(0);
        if row.string(1) == "completed" {
            return Ok(Claim::Completed {
                request_sha256: theirs,
                status: row.i64(3) as u16,
                body: row.json(4),
            });
        }
        if theirs == request_sha256 && row.i64(5) < now - stale_after_ms {
            let taken = self.execute(
                "UPDATE idempotency_keys SET claim = $3, updated_at = $4
                 WHERE idem_key = $1 AND claim = $2 AND state = 'in_progress'",
                &params![key, row.string(2), token, now],
            )?;
            if taken == 1 {
                return Ok(Claim::Claimed {
                    token,
                    recovered: true,
                });
            }
        }
        Ok(Claim::InProgress {
            request_sha256: theirs,
        })
    }

    /// Stores the answer of the work done under a claim.
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
