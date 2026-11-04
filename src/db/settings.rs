//! Values the engine keeps for itself in the store, shared by every instance
//! on it: today the keys that make cursors tamper-evident.

use anyhow::{Context, Result};
use rand::RngCore;

use super::{Db, now_ms, params};

pub(super) const SETTINGS: &str = "CREATE TABLE IF NOT EXISTS engine_keys (
  name TEXT PRIMARY KEY,
  value TEXT NOT NULL,
  created_at INTEGER NOT NULL
);
";

impl Db {
    /// The engine's 256-bit key named `name`, created at random the first time
    /// any instance asks. Kept in the store so every instance on it agrees.
    pub fn engine_key(&self, name: &str) -> Result<[u8; 32]> {
        let mut fresh = [0u8; 32];
        rand::rngs::OsRng.fill_bytes(&mut fresh);
        self.execute(
            "INSERT INTO engine_keys (name, value, created_at) VALUES ($1, $2, $3) ON CONFLICT (name) DO NOTHING",
            &params![name, hex::encode(fresh), now_ms()],
        )?;
        let row = self
            .query_one(
                "SELECT value FROM engine_keys WHERE name = $1",
                &params![name],
            )?
            .context("the engine key was not stored")?;
        let bytes = hex::decode(row.string(0)).context("the engine key is not hex")?;
        bytes
            .try_into()
            .map_err(|_| anyhow::anyhow!("the engine key {name} is not 32 bytes"))
    }
}
