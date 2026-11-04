//! Authenticated encryption for what HIE keeps on behalf of others: secret
//! values (`secrets.key_file` / `HIE_SECRETS_KEY`) and failed rows' content
//! (`content.key_file` / `HIE_CONTENT_KEY`, or a random key file in the
//! store's directory). Decision 36.
//!
//! AES-256-GCM with a random 96-bit nonce per message and caller-chosen
//! associated data (bind a ciphertext to what it belongs to, such as a secret
//! id and revision, so it cannot be moved to another row). Every ciphertext
//! names the key that sealed it (`v1.<key id>.<base64url>`), so a key is
//! rotated by putting the new key first and keeping the old ones after it: a
//! [`Keyring`] seals with its first key and opens with whichever key a
//! ciphertext names.
//!
//! A key file or variable holds one key per line (or per comma), each 32 bytes
//! in base64; the first is the current one. Keys never appear in `Debug`
//! output, errors or logs; a key's id is a truncated hash of it.

use std::io::Write;
use std::path::Path;

use aes_gcm::aead::{Aead, KeyInit, Payload};
use aes_gcm::{Aes256Gcm, Nonce};
use anyhow::{Context, Result, bail};
use base64::Engine as _;
use base64::engine::general_purpose::{STANDARD, URL_SAFE_NO_PAD};
use rand::RngCore;
use sha2::{Digest, Sha256};

const VERSION: &str = "v1";
const NONCE_LEN: usize = 12;

/// One 256-bit key.
pub struct Key {
    bytes: [u8; 32],
    id: String,
}

impl Key {
    fn from_bytes(bytes: [u8; 32]) -> Self {
        let mut hash = Sha256::new();
        hash.update(b"hie key id\0");
        hash.update(bytes);
        let id = hex::encode(&hash.finalize()[..4]);
        Key { bytes, id }
    }

    /// A new random key.
    pub fn generate() -> Self {
        let mut bytes = [0u8; 32];
        rand::rngs::OsRng.fill_bytes(&mut bytes);
        Self::from_bytes(bytes)
    }

    /// A key written as base64 (standard or URL-safe, padding optional).
    pub fn from_base64(text: &str) -> Result<Self> {
        let text = text.trim();
        let decoded = STANDARD
            .decode(text)
            .or_else(|_| URL_SAFE_NO_PAD.decode(text.trim_end_matches('=')))
            .map_err(|_| anyhow::anyhow!("a key must be 32 bytes written in base64"))?;
        let bytes: [u8; 32] = decoded
            .try_into()
            .map_err(|_| anyhow::anyhow!("a key must be 32 bytes written in base64"))?;
        Ok(Self::from_bytes(bytes))
    }

    /// The key's id: 8 hex characters of a hash of it, safe to show.
    pub fn id(&self) -> &str {
        &self.id
    }

    fn cipher(&self) -> Aes256Gcm {
        Aes256Gcm::new_from_slice(&self.bytes).expect("a 32-byte key")
    }
}

impl Drop for Key {
    fn drop(&mut self) {
        for b in self.bytes.iter_mut() {
            // SAFETY: `b` is a valid, aligned reference into our own array.
            unsafe { std::ptr::write_volatile(b, 0) };
        }
    }
}

impl std::fmt::Debug for Key {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Key({})", self.id)
    }
}

/// The current key and the earlier ones kept for opening what they sealed.
#[derive(Debug)]
pub struct Keyring {
    keys: Vec<Key>,
}

/// Why a ciphertext did not open. Says nothing about the plaintext.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OpenError {
    /// Not something [`Keyring::seal`] wrote.
    Malformed,
    /// Sealed with a key this keyring does not hold (its id).
    UnknownKey(String),
    /// The ciphertext or its associated data was changed, or the key is wrong.
    Tampered,
}

impl std::fmt::Display for OpenError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            OpenError::Malformed => write!(f, "not a sealed value"),
            OpenError::UnknownKey(id) => write!(f, "sealed with key {id}, which is not configured"),
            OpenError::Tampered => write!(f, "the sealed value does not authenticate"),
        }
    }
}

impl std::error::Error for OpenError {}

impl Keyring {
    /// A keyring of these keys, the first being the one that seals.
    pub fn new(keys: Vec<Key>) -> Result<Self> {
        if keys.is_empty() {
            bail!("a keyring needs at least one key");
        }
        Ok(Self { keys })
    }

    /// Keys written one per line or comma, each base64, the current one first.
    pub fn parse(text: &str) -> Result<Self> {
        let keys = text
            .split(['\n', ','])
            .map(str::trim)
            .filter(|l| !l.is_empty() && !l.starts_with('#'))
            .map(Key::from_base64)
            .collect::<Result<Vec<_>>>()?;
        Self::new(keys)
    }

    /// The keys in a file (see [`Keyring::parse`]).
    pub fn from_file(path: &Path) -> Result<Self> {
        let text = std::fs::read_to_string(path)
            .with_context(|| format!("reading the key file {}", path.display()))?;
        Self::parse(&text).with_context(|| format!("the key file {}", path.display()))
    }

    /// The keys an environment variable holds; `None` when it is unset or empty.
    pub fn from_env(var: &str) -> Result<Option<Self>> {
        match std::env::var(var) {
            Ok(text) if !text.trim().is_empty() => Self::parse(&text)
                .with_context(|| format!("the key in {var}"))
                .map(Some),
            _ => Ok(None),
        }
    }

    /// The keys in `path`, or a new random key written there first (readable
    /// by this user only) when the file does not exist yet.
    pub fn load_or_create(path: &Path) -> Result<Self> {
        if !path.exists() {
            if let Some(dir) = path.parent() {
                std::fs::create_dir_all(dir)
                    .with_context(|| format!("creating {}", dir.display()))?;
            }
            let key = Key::generate();
            let mut options = std::fs::OpenOptions::new();
            options.write(true).create_new(true);
            #[cfg(unix)]
            {
                use std::os::unix::fs::OpenOptionsExt;
                options.mode(0o600);
            }
            match options.open(path) {
                Ok(mut file) => {
                    file.write_all(format!("{}\n", STANDARD.encode(key.bytes)).as_bytes())
                        .and_then(|_| file.sync_all())
                        .with_context(|| format!("writing the key file {}", path.display()))?;
                    return Self::new(vec![key]);
                }
                // Another process created it first: read theirs.
                Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {}
                Err(e) => {
                    return Err(e)
                        .with_context(|| format!("creating the key file {}", path.display()));
                }
            }
        }
        Self::from_file(path)
    }

    /// The id of the key that seals.
    pub fn current_id(&self) -> &str {
        self.keys[0].id()
    }

    /// Encrypts and authenticates `plaintext` together with `associated_data`
    /// (which is not stored; [`Keyring::open`] needs the same). Text, so it
    /// fits a TEXT column on both stores.
    pub fn seal(&self, plaintext: &[u8], associated_data: &[u8]) -> String {
        let key = &self.keys[0];
        let mut nonce = [0u8; NONCE_LEN];
        rand::rngs::OsRng.fill_bytes(&mut nonce);
        let ciphertext = key
            .cipher()
            .encrypt(
                Nonce::from_slice(&nonce),
                Payload {
                    msg: plaintext,
                    aad: &aad(key.id(), associated_data),
                },
            )
            .expect("AES-GCM encryption does not fail for in-memory data");
        let mut body = Vec::with_capacity(NONCE_LEN + ciphertext.len());
        body.extend_from_slice(&nonce);
        body.extend_from_slice(&ciphertext);
        format!("{VERSION}.{}.{}", key.id(), URL_SAFE_NO_PAD.encode(body))
    }

    /// The plaintext of what [`Keyring::seal`] wrote with the same associated data.
    pub fn open(&self, sealed: &str, associated_data: &[u8]) -> Result<Vec<u8>, OpenError> {
        let mut parts = sealed.splitn(3, '.');
        let (Some(VERSION), Some(id), Some(body)) = (parts.next(), parts.next(), parts.next())
        else {
            return Err(OpenError::Malformed);
        };
        let body = URL_SAFE_NO_PAD
            .decode(body)
            .map_err(|_| OpenError::Malformed)?;
        if body.len() < NONCE_LEN + 16 {
            return Err(OpenError::Malformed);
        }
        let key = self
            .keys
            .iter()
            .find(|k| k.id() == id)
            .ok_or_else(|| OpenError::UnknownKey(id.to_string()))?;
        let (nonce, ciphertext) = body.split_at(NONCE_LEN);
        key.cipher()
            .decrypt(
                Nonce::from_slice(nonce),
                Payload {
                    msg: ciphertext,
                    aad: &aad(id, associated_data),
                },
            )
            .map_err(|_| OpenError::Tampered)
    }
}

/// The key id is authenticated too, so a ciphertext cannot be relabelled.
fn aad(key_id: &str, associated_data: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(VERSION.len() + key_id.len() + associated_data.len() + 2);
    out.extend_from_slice(VERSION.as_bytes());
    out.push(0);
    out.extend_from_slice(key_id.as_bytes());
    out.push(0);
    out.extend_from_slice(associated_data);
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    const KEY: &str = "Y29uZm9ybWFuY2Utc2VjcmV0cy1rZXktMzItYnl0ZXM=";

    #[test]
    fn sealed_values_open_only_with_their_key_and_data() {
        let ring = Keyring::parse(KEY).unwrap();
        let sealed = ring.seal(b"Corpus-Secret-1!", b"sec_1:1");
        assert!(!sealed.contains("Corpus"));
        assert_ne!(
            sealed,
            ring.seal(b"Corpus-Secret-1!", b"sec_1:1"),
            "fresh nonce"
        );
        assert_eq!(ring.open(&sealed, b"sec_1:1").unwrap(), b"Corpus-Secret-1!");
        assert_eq!(ring.open(&sealed, b"sec_1:2"), Err(OpenError::Tampered));

        // One flipped character anywhere in the body fails to authenticate.
        let mut chars: Vec<char> = sealed.chars().collect();
        let i = chars.len() - 5;
        chars[i] = if chars[i] == 'A' { 'B' } else { 'A' };
        let tampered: String = chars.into_iter().collect();
        assert_eq!(ring.open(&tampered, b"sec_1:1"), Err(OpenError::Tampered));

        let other = Keyring::new(vec![Key::generate()]).unwrap();
        assert!(matches!(
            other.open(&sealed, b"sec_1:1"),
            Err(OpenError::UnknownKey(_))
        ));
        assert_eq!(ring.open("plain text", b""), Err(OpenError::Malformed));
    }

    #[test]
    fn a_rotated_keyring_opens_what_the_old_key_sealed() {
        let old = Keyring::parse(KEY).unwrap();
        let sealed = old.seal(b"row", b"");
        let new = Key::generate();
        let rotated = Keyring::new(vec![new, Key::from_base64(KEY).unwrap()]).unwrap();
        assert_eq!(rotated.open(&sealed, b"").unwrap(), b"row");
        assert_ne!(rotated.current_id(), old.current_id());
        assert!(rotated.seal(b"row", b"").contains(rotated.current_id()));
    }

    #[test]
    fn keys_are_checked_and_never_printed() {
        assert!(Key::from_base64("c2hvcnQ=").is_err());
        let ring = Keyring::parse(KEY).unwrap();
        let printed = format!("{ring:?}");
        assert!(!printed.contains(KEY) && printed.contains(ring.current_id()));
    }

    #[test]
    fn a_missing_key_file_is_created_private_and_reused() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("data").join("content.key");
        let first = Keyring::load_or_create(&path).unwrap();
        let again = Keyring::load_or_create(&path).unwrap();
        assert_eq!(first.current_id(), again.current_id());
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(&path).unwrap().permissions().mode();
            assert_eq!(mode & 0o777, 0o600);
        }
    }
}
