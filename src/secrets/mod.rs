//! The secrets a source definition names: a database URL with its password, a
//! remote server's client secret or bearer token. A definition never holds the
//! secret itself, only its name; today the name is an environment variable of
//! the engine's process, and an empty value counts as unset.
//!
//! Track S adds write-only secrets kept by HIE (`POST /api/secrets`), sealed
//! with `crate::crypto` under `Config::secrets_keys`.

/// The value of the secret named `name`, if it is set.
pub fn named(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|v| !v.trim().is_empty())
}
