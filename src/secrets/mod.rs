//! Secrets a source definition names.
//!
//! - **Kept secrets** ([`Secrets`]): write-only values posted through
//!   `/api/secrets`, sealed with `crate::crypto` under `Config::secrets_keys`
//!   (AES-256-GCM, each ciphertext tagged with its key id, so a key rotates by
//!   putting a new one first and keeping the old ones). A new value is a new
//!   revision; a revision other than the current one is destroyed once no
//!   evidence or source refers to it. Nothing reads a value back: it is opened
//!   only to open a connection ([`Secrets::reveal`]) and held in a
//!   [`SecretValue`] that is wiped when dropped. Without a key every operation
//!   is [`SecretError::Unavailable`].
//! - **Named secrets** ([`named`]): 0.1 definitions name an environment
//!   variable of the engine's process instead.
//!
//! [`Secrets::is_clean`] tells whether a text (a driver's error message) holds
//! any value HIE keeps, so it may be shown.

use std::sync::{Arc, OnceLock};

use anyhow::{Context, Result};
use hmac::{Hmac, Mac};
use serde_json::{Value, json};
use sha2::Sha256;

use crate::crypto::Keyring;
use crate::db::Db;
use crate::ids::rfc3339_ms_at;

/// The value of the secret named `name`, if it is set.
pub fn named(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|v| !v.trim().is_empty())
}

/// What holds a revision: a Test's evidence, or a running source.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SecretUse {
    Evidence,
    Source,
}

impl SecretUse {
    pub fn as_str(self) -> &'static str {
        match self {
            SecretUse::Evidence => "evidence",
            SecretUse::Source => "source",
        }
    }
}

/// A secret as the store holds it, without any value.
#[derive(Debug, Clone, PartialEq)]
pub struct SecretRow {
    pub id: String,
    pub name: String,
    pub current_revision: u32,
    pub created_at: i64,
    pub revisions: Vec<RevisionRow>,
}

/// One revision that still exists, and what refers to it.
#[derive(Debug, Clone, PartialEq)]
pub struct RevisionRow {
    pub revision: u32,
    pub created_at: i64,
    pub evidence: Vec<String>,
    pub sources: Vec<String>,
}

/// What deleting a secret found.
#[derive(Debug, Clone, PartialEq)]
pub enum DeleteSecret {
    Deleted,
    NotFound,
    /// Sources refer to it.
    InUse(Vec<String>),
}

/// Why a secret operation did not happen.
#[derive(Debug)]
pub enum SecretError {
    /// No secrets key is configured.
    Unavailable,
    NotFound,
    /// The secret exists, the revision does not (any more).
    RevisionNotFound,
    NameInUse,
    /// Sources refer to it.
    InUse(Vec<String>),
    /// A stored value did not open (a key removed, a store tampered with).
    Sealed,
    Store(anyhow::Error),
}

impl From<anyhow::Error> for SecretError {
    fn from(e: anyhow::Error) -> Self {
        SecretError::Store(e)
    }
}

/// A secret's value, opened for a connection. Wiped when dropped; `Debug`
/// never shows it.
pub struct SecretValue(String);

impl SecretValue {
    pub fn expose(&self) -> &str {
        &self.0
    }
}

impl Drop for SecretValue {
    fn drop(&mut self) {
        // SAFETY: zeroes are valid UTF-8; the string is dropped right after.
        for b in unsafe { self.0.as_bytes_mut() } {
            unsafe { std::ptr::write_volatile(b, 0) };
        }
    }
}

impl std::fmt::Debug for SecretValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("SecretValue(..)")
    }
}

/// The kept secrets of an engine.
pub struct Secrets {
    db: Arc<Db>,
    keys: Option<Arc<Keyring>>,
    request_key: OnceLock<[u8; 32]>,
}

/// What a sealed revision is bound to, so it cannot be moved to another row.
fn associated_data(id: &str, revision: u32) -> Vec<u8> {
    format!("hie secret {id} {revision}").into_bytes()
}

impl Secrets {
    pub fn new(db: Arc<Db>, keys: Option<Keyring>) -> Self {
        Self {
            db,
            keys: keys.map(Arc::new),
            request_key: OnceLock::new(),
        }
    }

    /// Whether custody is configured.
    pub fn available(&self) -> bool {
        self.keys.is_some()
    }

    fn keys(&self) -> Result<&Keyring, SecretError> {
        self.keys.as_deref().ok_or(SecretError::Unavailable)
    }

    /// Keeps a new secret; its view.
    pub fn create(&self, name: &str, value: &str) -> Result<Value, SecretError> {
        let keys = self.keys()?;
        let id = crate::ids::new(crate::ids::Kind::Secret);
        let created = self.db.insert_secret(&id, name, |id, revision| {
            keys.seal(value.as_bytes(), &associated_data(id, revision))
        })?;
        if !created {
            return Err(SecretError::NameInUse);
        }
        self.get(&id)
    }

    /// A new revision of a secret; its view.
    pub fn revise(&self, id: &str, value: &str) -> Result<Value, SecretError> {
        let keys = self.keys()?;
        self.db
            .add_secret_revision(id, |id, revision| {
                keys.seal(value.as_bytes(), &associated_data(id, revision))
            })?
            .ok_or(SecretError::NotFound)?;
        self.get(id)
    }

    /// One secret's view.
    pub fn get(&self, id: &str) -> Result<Value, SecretError> {
        self.keys()?;
        let row = self
            .db
            .secret_rows(Some(id))?
            .into_iter()
            .next()
            .ok_or(SecretError::NotFound)?;
        Ok(view(&row))
    }

    /// Every secret's view, by name.
    pub fn list(&self) -> Result<Vec<Value>, SecretError> {
        self.keys()?;
        Ok(self.db.secret_rows(None)?.iter().map(view).collect())
    }

    /// Deletes a secret with every revision, unless a source refers to it.
    pub fn delete(&self, id: &str) -> Result<(), SecretError> {
        self.keys()?;
        match self.db.delete_secret(id)? {
            DeleteSecret::Deleted => Ok(()),
            DeleteSecret::NotFound => Err(SecretError::NotFound),
            DeleteSecret::InUse(sources) => Err(SecretError::InUse(sources)),
        }
    }

    /// The revision a reference without one takes now.
    pub fn current_revision(&self, id: &str) -> Result<u32, SecretError> {
        self.keys()?;
        self.db
            .secret_rows(Some(id))?
            .first()
            .map(|r| r.current_revision)
            .ok_or(SecretError::NotFound)
    }

    /// Whether a revision still exists.
    pub fn has_revision(&self, id: &str, revision: u32) -> Result<(), SecretError> {
        self.keys()?;
        let row = self
            .db
            .secret_rows(Some(id))?
            .into_iter()
            .next()
            .ok_or(SecretError::NotFound)?;
        if row.revisions.iter().any(|r| r.revision == revision) {
            Ok(())
        } else {
            Err(SecretError::RevisionNotFound)
        }
    }

    /// Opens a revision's value to open a connection with. Drop it as soon
    /// as the connection is open.
    pub fn reveal(&self, id: &str, revision: u32) -> Result<SecretValue, SecretError> {
        let keys = self.keys()?;
        let sealed = self.db.secret_sealed(id, revision)?.ok_or_else(|| {
            match self.db.secret_rows(Some(id)) {
                Ok(rows) if rows.is_empty() => SecretError::NotFound,
                _ => SecretError::RevisionNotFound,
            }
        })?;
        let plain = keys
            .open(&sealed, &associated_data(id, revision))
            .map_err(|_| SecretError::Sealed)?;
        String::from_utf8(plain)
            .map(SecretValue)
            .map_err(|_| SecretError::Sealed)
    }

    /// Whether `text` holds none of the values HIE keeps (every revision of
    /// every secret). Anything that cannot be checked counts as not clean.
    pub fn is_clean(&self, text: &str) -> bool {
        let Some(keys) = self.keys.as_deref() else {
            return true;
        };
        let Ok(sealed) = self.db.secrets_sealed() else {
            return false;
        };
        sealed.iter().all(|(id, revision, sealed)| {
            match keys.open(sealed, &associated_data(id, *revision)) {
                Ok(plain) => {
                    let value = SecretValue(String::from_utf8_lossy(&plain).into_owned());
                    value.0.is_empty() || !text.contains(value.expose())
                }
                Err(_) => false,
            }
        })
    }

    /// Records that `holder` refers to a revision.
    pub fn hold(&self, id: &str, revision: u32, kind: SecretUse, holder: &str) -> Result<()> {
        self.db.use_secret(id, revision, kind, holder)
    }

    /// `holder` refers to no revision any more but `keep`; what nothing
    /// refers to is destroyed.
    pub fn release(&self, kind: SecretUse, holder: &str, keep: Option<(&str, u32)>) -> Result<()> {
        self.db.release_secrets(kind, holder, keep)
    }

    /// The key idempotency hashes of requests that carry a value are keyed
    /// with: random, kept sealed under the secrets key, so no stored hash can
    /// be brute-forced back to a value without that key.
    pub fn request_key(&self) -> Result<[u8; 32], SecretError> {
        let keys = self.keys()?;
        if let Some(key) = self.request_key.get() {
            return Ok(*key);
        }
        const AD: &[u8] = b"hie idempotency request key";
        let sealed = self.db.secret_custody("idempotency", || {
            use rand::RngCore;
            let mut fresh = [0u8; 32];
            rand::rngs::OsRng.fill_bytes(&mut fresh);
            keys.seal(&fresh, AD)
        })?;
        let plain = keys.open(&sealed, AD).map_err(|_| SecretError::Sealed)?;
        let key: [u8; 32] = plain
            .try_into()
            .map_err(|_| SecretError::Store(anyhow::anyhow!("the request key is not 32 bytes")))?;
        Ok(*self.request_key.get_or_init(|| key))
    }
}

/// HMAC-SHA256 of `data` under `key`, hex.
pub fn keyed_hash(key: &[u8], data: &[u8]) -> Result<String> {
    let mut mac = Hmac::<Sha256>::new_from_slice(key).context("an HMAC key")?;
    mac.update(data);
    Ok(hex::encode(mac.finalize().into_bytes()))
}

fn view(row: &SecretRow) -> Value {
    json!({
        "id": row.id,
        "name": row.name,
        "revision": row.current_revision,
        "created_at": rfc3339_ms_at(row.created_at),
        "revisions": row.revisions.iter().map(|r| json!({
            "revision": r.revision,
            "created_at": rfc3339_ms_at(r.created_at),
            "used_by": {"evidence": r.evidence, "sources": r.sources},
        })).collect::<Vec<_>>(),
    })
}
