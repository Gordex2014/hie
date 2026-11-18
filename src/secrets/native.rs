//! The native backend: each secret's current revision sealed in the engine's own
//! database (SQLite or PostgreSQL), under a key derived from this instance's
//! master key.

use std::sync::Arc;

use anyhow::anyhow;
use secrecy::{ExposeSecret, SecretString};

use super::{
    NewSecret, Revealed, SecretError, SecretKind, SecretMeta, SecretStore, SecretValue, check_name,
};
use crate::crypto::{self, ALGORITHM, Sealer};
use crate::db::secrets::{SecretRow, retry_busy};
use crate::db::{Db, now_ms};

const VALUE_DOMAIN: &[u8] = b"hie-secret-value/v1";
/// Attempts at a replace that keeps losing to concurrent replaces.
const REPLACE_ATTEMPTS: usize = 5;

pub struct NativeStore {
    db: Arc<Db>,
    sealer: Sealer,
    /// The store's id, from its key check: part of every sealed value's binding.
    store_id: String,
}

impl NativeStore {
    pub(crate) fn new(db: Arc<Db>, sealer: Sealer, store_id: String) -> Self {
        Self {
            db,
            sealer,
            store_id,
        }
    }

    /// Seals `value` as revision `revision` of the secret `id` named `name`.
    fn seal(
        &self,
        id: &str,
        name: &str,
        kind: SecretKind,
        revision: u32,
        value: &SecretValue,
    ) -> Result<String, SecretError> {
        let count = (value.fields().len() as u64).to_be_bytes();
        let mut parts: Vec<&[u8]> = vec![VALUE_DOMAIN, &count];
        for (field, text) in value.fields() {
            parts.push(field.as_bytes());
            parts.push(text.expose_secret().as_bytes());
        }
        let plaintext = crypto::sealed_parts(&parts);
        let aad = crypto::secret_aad(
            &self.store_id,
            id,
            name,
            kind.as_str(),
            revision,
            ALGORITHM,
            self.sealer.key_id(),
        );
        self.sealer
            .seal(&plaintext, &aad)
            .map_err(SecretError::Store)
    }

    fn row(
        &self,
        id: &str,
        name: &str,
        kind: SecretKind,
        revision: u32,
        value: &SecretValue,
        created_at: i64,
    ) -> Result<SecretRow, SecretError> {
        Ok(SecretRow {
            id: id.to_string(),
            name: name.to_string(),
            kind: kind.as_str().to_string(),
            hint: kind.hint(value),
            revision,
            algorithm: ALGORITHM.to_string(),
            key_id: self.sealer.key_id().to_string(),
            sealed: Some(self.seal(id, name, kind, revision, value)?),
            created_at,
            updated_at: now_ms(),
        })
    }

    fn meta(&self, row: SecretRow) -> Result<SecretMeta, SecretError> {
        let kind = SecretKind::parse(&row.kind).ok_or_else(|| {
            SecretError::Integrity(format!(
                "credential {} revision {} has an unknown kind",
                row.id, row.revision
            ))
        })?;
        let used_by = self
            .db
            .secret_used_by(&row.id)
            .map_err(SecretError::Store)?;
        Ok(SecretMeta {
            id: row.id,
            name: row.name,
            kind,
            hint: row.hint,
            revision: row.revision,
            created_at: row.created_at,
            updated_at: row.updated_at,
            used_by,
        })
    }

    /// Opens and decodes a row's sealed value: exactly the kind's fields, in
    /// order, as UTF-8.
    fn open(&self, row: &SecretRow, kind: SecretKind) -> Result<SecretValue, SecretError> {
        let broken = |why: &str| {
            SecretError::Integrity(format!(
                "credential {} revision {} {why}",
                row.id, row.revision
            ))
        };
        if row.algorithm != ALGORITHM || row.key_id != self.sealer.key_id() {
            return Err(broken(
                "was sealed with another algorithm or key than this instance's",
            ));
        }
        let aad = crypto::secret_aad(
            &self.store_id,
            &row.id,
            &row.name,
            &row.kind,
            row.revision,
            &row.algorithm,
            &row.key_id,
        );
        let plaintext = self
            .sealer
            .open(row.sealed.as_deref().unwrap_or_default(), &aad)
            .map_err(|_| broken("does not open: it was altered or moved"))?;
        let parts = crypto::split_parts(&plaintext).ok_or_else(|| broken("does not decode"))?;
        let fields = kind.fields();
        if parts.len() != 2 + 2 * fields.len()
            || parts[0] != VALUE_DOMAIN
            || parts[1] != (fields.len() as u64).to_be_bytes()
        {
            return Err(broken("does not decode"));
        }
        let mut out = Vec::with_capacity(fields.len());
        for (i, field) in fields.iter().enumerate() {
            if parts[2 + 2 * i] != field.as_bytes() {
                return Err(broken("does not decode"));
            }
            let text =
                std::str::from_utf8(parts[3 + 2 * i]).map_err(|_| broken("does not decode"))?;
            // Exactly as long as the value, so the boxed string never reallocates.
            let mut copy = String::with_capacity(text.len());
            copy.push_str(text);
            out.push((*field, SecretString::from(copy)));
        }
        Ok(SecretValue::from_ordered(out))
    }

    fn new_id() -> Result<String, SecretError> {
        let mut bytes = [0u8; 8];
        getrandom::fill(&mut bytes)
            .map_err(|e| SecretError::Store(anyhow!("no random bytes for an id: {e}")))?;
        Ok(format!("sec_{}", hex::encode(bytes)))
    }
}

#[async_trait::async_trait]
impl SecretStore for NativeStore {
    fn backend(&self) -> &'static str {
        "native"
    }

    async fn create(&self, new: NewSecret) -> Result<SecretMeta, SecretError> {
        check_name(&new.name)?;
        new.kind.check(&new.value)?;
        let created_at = now_ms();
        // A second id, sealed again with a fresh nonce, if the first collides.
        for _ in 0..2 {
            let row = self.row(
                &Self::new_id()?,
                &new.name,
                new.kind,
                1,
                &new.value,
                created_at,
            )?;
            let inserted = retry_busy(|| self.db.secret_insert(&row).map_err(SecretError::Store))?;
            if inserted == 1 {
                return self.meta(row);
            }
            if self
                .db
                .secret_name_taken(&new.name)
                .map_err(SecretError::Store)?
            {
                return Err(SecretError::NameInUse);
            }
        }
        Err(SecretError::Store(anyhow!(
            "two generated credential ids were taken"
        )))
    }

    async fn replace(&self, id: &str, value: SecretValue) -> Result<SecretMeta, SecretError> {
        for _ in 0..REPLACE_ATTEMPTS {
            let current = self
                .db
                .secret_row(id, false)
                .map_err(SecretError::Store)?
                .filter(|r| r.id == id)
                .ok_or(SecretError::NotFound)?;
            let kind = SecretKind::parse(&current.kind).ok_or_else(|| {
                SecretError::Integrity(format!("credential {id} has an unknown kind"))
            })?;
            kind.check(&value)?;
            let row = self.row(
                id,
                &current.name,
                kind,
                current.revision + 1,
                &value,
                current.created_at,
            )?;
            let changed = retry_busy(|| {
                self.db
                    .secret_update(&row, current.revision)
                    .map_err(SecretError::Store)
            })?;
            if changed == 1 {
                return self.meta(row);
            }
        }
        Err(SecretError::Store(anyhow!(
            "credential {id} kept changing while it was replaced"
        )))
    }

    async fn list(&self) -> Result<Vec<SecretMeta>, SecretError> {
        self.db
            .secret_rows()
            .map_err(SecretError::Store)?
            .into_iter()
            .map(|row| self.meta(row))
            .collect()
    }

    async fn find(&self, reference: &str) -> Result<Option<SecretMeta>, SecretError> {
        self.db
            .secret_row(reference, false)
            .map_err(SecretError::Store)?
            .map(|row| self.meta(row))
            .transpose()
    }

    async fn reveal(&self, reference: &str) -> Result<Revealed, SecretError> {
        let row = self
            .db
            .secret_row(reference, true)
            .map_err(SecretError::Store)?
            .ok_or(SecretError::NotFound)?;
        let kind = SecretKind::parse(&row.kind).ok_or_else(|| {
            SecretError::Integrity(format!(
                "credential {} revision {} has an unknown kind",
                row.id, row.revision
            ))
        })?;
        let value = self.open(&row, kind)?;
        Ok(Revealed {
            meta: self.meta(row)?,
            value,
        })
    }

    async fn delete(&self, id: &str) -> Result<(), SecretError> {
        if !super::is_id(id) {
            return Err(SecretError::NotFound);
        }
        self.db.secret_delete_unless_held(id)
    }
}
