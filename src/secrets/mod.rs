//! Connector credentials: values a source needs to reach a bucket, kept by the
//! engine and never returned by it.
//!
//! A secret has a name, a kind, and a current revision; a source names it with
//! `credential:` and always uses the current revision. The store behind the
//! `SecretStore` trait is the native one (`secrets-native`): values sealed in the
//! engine's own database under a key of this instance. Without a backend the
//! engine starts, and every use of a credential is refused with the reason.

#[cfg(feature = "secrets-native")]
pub mod key;
pub mod kind;
#[cfg(feature = "secrets-native")]
pub mod native;

use std::ffi::OsString;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use secrecy::SecretString;
use zeroize::Zeroizing;

use crate::config::Config;
use crate::db::Db;

/// What a secret holds. Only `aws` exists: an access key id and its secret key.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SecretKind {
    Aws,
}

impl SecretKind {
    pub fn as_str(self) -> &'static str {
        match self {
            SecretKind::Aws => "aws",
        }
    }

    pub fn parse(text: &str) -> Option<Self> {
        match text {
            "aws" => Some(SecretKind::Aws),
            _ => None,
        }
    }
}

/// Everything about a secret except its value: safe to log, serialize and answer.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct SecretMeta {
    /// `sec_` and 16 lowercase hex characters.
    pub id: String,
    pub name: String,
    pub kind: SecretKind,
    /// The last characters of the access key id, to tell credentials apart.
    pub hint: Option<String>,
    /// 1 when created, one more on every replace.
    pub revision: u32,
    #[serde(serialize_with = "iso_ms")]
    pub created_at: i64,
    /// When the current revision was written.
    #[serde(serialize_with = "iso_ms")]
    pub updated_at: i64,
    /// The sources registered for this secret, by any instance on the store.
    pub used_by: Vec<String>,
}

fn iso_ms<S: serde::Serializer>(ms: &i64, s: S) -> Result<S::Ok, S::Error> {
    let at = chrono::DateTime::from_timestamp_millis(*ms).unwrap_or_default();
    s.serialize_str(&at.to_rfc3339_opts(chrono::SecondsFormat::Millis, true))
}

/// A secret's value: its kind's fields, each wiped on drop. It has no `Debug`,
/// `Clone`, `Display` or `Serialize`, so it cannot be printed or answered.
pub struct SecretValue {
    fields: Vec<(&'static str, SecretString)>,
}

impl SecretValue {
    /// The fields of a value of `kind`, checked against the kind's rules. A
    /// refusal names the field, never what was in it.
    pub fn new(
        kind: SecretKind,
        fields: impl IntoIterator<Item = (String, SecretString)>,
    ) -> Result<Self, SecretError> {
        let mut out: Vec<(&'static str, SecretString)> = Vec::new();
        for (name, value) in fields {
            let Some(known) = kind.fields().iter().find(|f| **f == name) else {
                return Err(SecretError::Invalid {
                    field: "value",
                    message: format!("a value of kind {} has no such field", kind.as_str()),
                });
            };
            if out.iter().any(|(f, _)| f == known) {
                return Err(SecretError::Invalid {
                    field: known,
                    message: "given twice".into(),
                });
            }
            out.push((known, value));
        }
        // In the kind's order, which is also the sealed order.
        out.sort_by_key(|(f, _)| kind.fields().iter().position(|k| k == f));
        let value = Self { fields: out };
        kind.check(&value)?;
        Ok(value)
    }

    #[cfg(feature = "secrets-native")]
    pub(crate) fn from_ordered(fields: Vec<(&'static str, SecretString)>) -> Self {
        Self { fields }
    }

    pub fn field(&self, name: &str) -> Option<&SecretString> {
        self.fields.iter().find(|(f, _)| *f == name).map(|(_, v)| v)
    }

    pub(crate) fn fields(&self) -> &[(&'static str, SecretString)] {
        &self.fields
    }
}

/// A secret's metadata and its current value. Internal only: never answered.
pub struct Revealed {
    pub meta: SecretMeta,
    pub value: SecretValue,
}

/// What `create` stores.
pub struct NewSecret {
    pub name: String,
    pub kind: SecretKind,
    pub value: SecretValue,
}

#[derive(Debug)]
pub enum SecretError {
    /// No backend, or no usable key for a store that is not sealed yet.
    Unavailable(String),
    NotFound,
    NameInUse,
    /// The sources registered for the secret, deduplicated and sorted.
    InUse(Vec<String>),
    /// The store's write lock was not free, twice; the same call may work later.
    Busy,
    /// A request that breaks a rule; never holds what was submitted.
    Invalid {
        field: &'static str,
        message: String,
    },
    /// A sealed value that does not open or does not decode: names the secret
    /// and revision only.
    Integrity(String),
    Store(anyhow::Error),
}

impl std::fmt::Display for SecretError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SecretError::Unavailable(reason) => f.write_str(reason),
            SecretError::NotFound => f.write_str("no such credential"),
            SecretError::NameInUse => f.write_str("a credential with this name already exists"),
            SecretError::InUse(sources) => {
                write!(f, "sources use this credential: {}", sources.join(", "))
            }
            SecretError::Busy => f.write_str("the store is busy; try again shortly"),
            SecretError::Invalid { field, message } => write!(f, "{field}: {message}"),
            SecretError::Integrity(message) => f.write_str(message),
            SecretError::Store(e) => write!(f, "{e:#}"),
        }
    }
}

impl std::error::Error for SecretError {}

#[async_trait::async_trait]
pub trait SecretStore: Send + Sync {
    /// `native`.
    fn backend(&self) -> &'static str;
    async fn create(&self, new: NewSecret) -> Result<SecretMeta, SecretError>;
    /// A new revision with `value`; the name and kind stay.
    async fn replace(&self, id: &str, value: SecretValue) -> Result<SecretMeta, SecretError>;
    /// Every secret, by name.
    async fn list(&self) -> Result<Vec<SecretMeta>, SecretError>;
    /// By id (`sec_…`) or name, without decrypting anything.
    async fn find(&self, reference: &str) -> Result<Option<SecretMeta>, SecretError>;
    /// The current revision's value. Internal only.
    async fn reveal(&self, reference: &str) -> Result<Revealed, SecretError>;
    /// Deletes unless any instance has a source registered for it:
    /// `NotFound`, `InUse(sources)` or `Busy` otherwise.
    async fn delete(&self, id: &str) -> Result<(), SecretError>;
}

/// The rules for a secret's name: 1 to 64 characters, no surrounding
/// whitespace, no control characters, and not `sec_…`, which is how ids look.
pub fn check_name(name: &str) -> Result<(), SecretError> {
    let invalid = |message: &str| SecretError::Invalid {
        field: "name",
        message: message.into(),
    };
    let count = name.chars().count();
    if count == 0 || name.trim().is_empty() {
        return Err(invalid("a name is required"));
    }
    if count > 64 {
        return Err(invalid("a name has at most 64 characters"));
    }
    if name.trim() != name {
        return Err(invalid("a name has no whitespace around it"));
    }
    if name.chars().any(char::is_control) {
        return Err(invalid("a name has no control characters"));
    }
    if name.starts_with("sec_") {
        return Err(invalid(
            "a name cannot start with sec_, which ids start with",
        ));
    }
    Ok(())
}

/// Whether a `credential:` value names a secret by id rather than by name.
pub fn is_id(reference: &str) -> bool {
    reference.starts_with("sec_")
}

/// The secret store as the engine holds it: available, or not with the reason.
#[derive(Clone)]
pub struct Secrets {
    state: Arc<State>,
    owner: Arc<str>,
}

enum State {
    #[cfg_attr(not(feature = "secrets-native"), allow(dead_code))]
    Available {
        store: Arc<dyn SecretStore>,
        key_id: String,
    },
    Unavailable {
        reason: Mutex<Arc<str>>,
        /// What the one-shot recheck needs; `None` without a backend.
        #[cfg(feature = "secrets-native")]
        recheck: Option<Mutex<Option<key::Recheck>>>,
    },
}

impl Secrets {
    /// No store, for `reason`.
    pub fn unavailable(reason: impl Into<Arc<str>>) -> Self {
        Self {
            state: Arc::new(State::Unavailable {
                reason: Mutex::new(reason.into()),
                #[cfg(feature = "secrets-native")]
                recheck: None,
            }),
            owner: Arc::from(""),
        }
    }

    #[cfg(feature = "secrets-native")]
    pub(crate) fn available(store: Arc<dyn SecretStore>, key_id: String, owner: String) -> Self {
        Self {
            state: Arc::new(State::Available { store, key_id }),
            owner: owner.into(),
        }
    }

    #[cfg(feature = "secrets-native")]
    pub(crate) fn unavailable_with_recheck(
        reason: String,
        recheck: key::Recheck,
        owner: String,
    ) -> Self {
        Self {
            state: Arc::new(State::Unavailable {
                reason: Mutex::new(reason.into()),
                recheck: Some(Mutex::new(Some(recheck))),
            }),
            owner: owner.into(),
        }
    }

    /// The store, or why there is none.
    pub fn store(&self) -> Result<&Arc<dyn SecretStore>, SecretError> {
        match &*self.state {
            State::Available { store, .. } => Ok(store),
            State::Unavailable { reason, .. } => Err(SecretError::Unavailable(
                reason.lock().expect("secrets mutex").to_string(),
            )),
        }
    }

    /// Who this instance's source registrations belong to.
    pub fn owner(&self) -> &str {
        &self.owner
    }

    /// Without a store, re-reads the key inputs and the store's key check once
    /// per process, so the reason given is current: a key file mounted after
    /// start, or a store sealed by another instance since. It never makes the
    /// store available; that takes a restart.
    pub async fn recheck_once(&self) {
        #[cfg(feature = "secrets-native")]
        if let State::Unavailable {
            reason,
            recheck: Some(recheck),
        } = &*self.state
        {
            let mut pending = recheck.lock().expect("secrets mutex");
            if let Some(recheck) = pending.take() {
                let refreshed = recheck.run();
                tracing::warn!("secret store still unavailable: {refreshed}");
                *reason.lock().expect("secrets mutex") = refreshed.into();
            }
        }
    }

    /// The `/health` member: the backend, whether it is available, and the
    /// one-way key id of this instance's key when it is.
    pub fn health(&self) -> serde_json::Value {
        let backend = if cfg!(feature = "secrets-native") {
            "native"
        } else {
            "none"
        };
        match &*self.state {
            State::Available { store, key_id } => serde_json::json!({
                "backend": store.backend(),
                "available": true,
                "key_id": key_id,
            }),
            State::Unavailable { .. } => serde_json::json!({
                "backend": backend,
                "available": false,
                "key_id": null,
            }),
        }
    }
}

/// The key inputs of the process, read once: `HIE_SECRET_KEY` and
/// `HIE_SECRET_KEY_FILE`. An empty value counts as unset.
pub struct KeySettings {
    pub(crate) key: EnvKey,
    pub(crate) key_file: Option<PathBuf>,
}

/// `HIE_SECRET_KEY` as given.
pub(crate) enum EnvKey {
    Unset,
    #[cfg_attr(not(feature = "secrets-native"), allow(dead_code))]
    Text(Zeroizing<String>),
    /// Set, but not UTF-8: malformed.
    NotUtf8,
}

impl KeySettings {
    /// From the process environment.
    pub fn load() -> Self {
        Self::from_vars(
            std::env::var_os("HIE_SECRET_KEY"),
            std::env::var_os("HIE_SECRET_KEY_FILE"),
        )
    }

    /// From the values the two variables would have.
    pub fn from_vars(key: Option<OsString>, key_file: Option<OsString>) -> Self {
        let key = match key {
            None => EnvKey::Unset,
            Some(raw) => match raw.into_string() {
                Ok(text) => {
                    let text = Zeroizing::new(text);
                    if text.trim().is_empty() {
                        EnvKey::Unset
                    } else {
                        EnvKey::Text(text)
                    }
                }
                Err(raw) => {
                    drop(Zeroizing::new(raw.into_encoded_bytes()));
                    EnvKey::NotUtf8
                }
            },
        };
        let key_file = key_file
            .filter(|v| !v.to_string_lossy().trim().is_empty())
            .map(PathBuf::from);
        Self { key, key_file }
    }

    /// A key given inline, as `HIE_SECRET_KEY` would.
    pub fn inline(base64: &str) -> Self {
        Self::from_vars(Some(base64.into()), None)
    }

    /// A key file, as `HIE_SECRET_KEY_FILE` would name it.
    pub fn file(path: impl Into<PathBuf>) -> Self {
        Self {
            key: EnvKey::Unset,
            key_file: Some(path.into()),
        }
    }

    /// No key input at all.
    pub fn none() -> Self {
        Self {
            key: EnvKey::Unset,
            key_file: None,
        }
    }

    /// The variables that are set, by name.
    pub(crate) fn names_set(&self) -> Vec<&'static str> {
        let mut names = Vec::new();
        if !matches!(self.key, EnvKey::Unset) {
            names.push("HIE_SECRET_KEY");
        }
        if self.key_file.is_some() {
            names.push("HIE_SECRET_KEY_FILE");
        }
        names
    }
}

/// Opens the secret store of the engine's database, deciding from the key
/// inputs and the store's key check whether credentials can be used. An error
/// is a refusal to start: a sealed store without its key, or an ambiguous or
/// unsafe key setting.
pub fn open(config: &Config, db: &Arc<Db>, keys: KeySettings) -> anyhow::Result<Secrets> {
    #[cfg(feature = "secrets-native")]
    {
        key::open(config, db, keys)
    }
    #[cfg(not(feature = "secrets-native"))]
    {
        let _ = db;
        let mut names = keys.names_set();
        if config.secrets.key_file.is_some() {
            names.push("secrets.key_file");
        }
        if !names.is_empty() {
            tracing::warn!(
                "{} set but ignored: this build has no secret backend",
                names.join(", ")
            );
        }
        tracing::warn!("secret store unavailable: {NO_BACKEND}");
        Ok(Secrets::unavailable(NO_BACKEND))
    }
}

/// Why a build without a backend has no store.
pub const NO_BACKEND: &str =
    "this build has no secret backend (built without the secrets-native feature)";

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn name_rules() {
        for good in [
            "lab-bucket",
            "a",
            &"x".repeat(64),
            "Lab bucket (EU)",
            "sec-1",
        ] {
            assert!(check_name(good).is_ok(), "{good:?}");
        }
        for bad in [
            "",
            "  ",
            &"x".repeat(65),
            " lab",
            "lab\n",
            "a\u{7}b",
            "sec_x",
        ] {
            assert!(check_name(bad).is_err(), "{bad:?}");
        }
    }

    #[test]
    fn empty_key_variables_count_as_unset() {
        let keys = KeySettings::from_vars(Some(" ".into()), Some("".into()));
        assert!(keys.names_set().is_empty());
        let keys = KeySettings::from_vars(Some("k".into()), Some("/k".into()));
        assert_eq!(keys.names_set(), ["HIE_SECRET_KEY", "HIE_SECRET_KEY_FILE"]);
    }

    #[test]
    fn the_metadata_is_answered_with_iso_instants() {
        let meta = SecretMeta {
            id: "sec_0123456789abcdef".into(),
            name: "lab".into(),
            kind: SecretKind::Aws,
            hint: Some("MPLE".into()),
            revision: 1,
            created_at: 1_794_477_600_000,
            updated_at: 1_794_477_600_000,
            used_by: vec![],
        };
        let json = serde_json::to_value(&meta).unwrap();
        assert_eq!(json["created_at"], "2026-11-12T10:00:00.000Z");
        assert_eq!(json["kind"], "aws");
        assert!(json.get("value").is_none());
    }
}
