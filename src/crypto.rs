//! Sealing connector credentials: XChaCha20-Poly1305 under a key derived once,
//! at start, from the store's master key with HKDF-SHA256.
//!
//! Every sealed value is bound to what it belongs to (the store, the secret's id,
//! name, kind and revision, the algorithm and the key id) through its associated
//! data, so a value moved to another row, name, kind or revision does not open.
//! Each part of the associated data and of the sealed plaintext is written as a
//! 4-byte big-endian length followed by the part; a number is first written as
//! 8 big-endian bytes.

use anyhow::{Result, anyhow};
use base64::Engine;
use base64::engine::general_purpose::{STANDARD, STANDARD_NO_PAD, URL_SAFE_NO_PAD};
use chacha20poly1305::aead::{Aead, Payload};
use chacha20poly1305::{Key, KeyInit, XChaCha20Poly1305, XNonce};
use hkdf::Hkdf;
use sha2::Sha256;
use zeroize::{Zeroize, Zeroizing};

/// The only algorithm this version seals and opens with.
pub const ALGORITHM: &str = "xchacha20poly1305";
/// What the key check seals: opening it proves the key is the store's.
pub const CHECK_PLAINTEXT: &[u8] = b"hie secret key check v1";
const ENCRYPTION_INFO: &[u8] = b"hie/secrets/v1/encryption";
const KEY_ID_INFO: &[u8] = b"hie/secrets/v1/key-id";
const NONCE_LEN: usize = 24;
const TAG_LEN: usize = 16;

/// The store's master key: 32 random bytes, wiped on drop.
pub struct MasterKey(Zeroizing<[u8; 32]>);

/// A key text that is not 32 bytes in base64. It never carries the text.
#[derive(Debug, PartialEq, Eq)]
pub struct BadKey;

impl MasterKey {
    /// 32 bytes in base64, standard or URL-safe alphabet, padding optional (43 or
    /// 44 characters), surrounding whitespace ignored.
    pub fn from_base64(text: &str) -> Result<Self, BadKey> {
        let text = text.trim();
        let unpadded = match text.len() {
            43 => text,
            44 => text.strip_suffix('=').ok_or(BadKey)?,
            _ => return Err(BadKey),
        };
        // Room for the decoder's estimate (33 bytes for 43 characters).
        let mut buf = Zeroizing::new([0u8; 36]);
        let n = STANDARD_NO_PAD
            .decode_slice(unpadded, &mut buf[..])
            .or_else(|_| URL_SAFE_NO_PAD.decode_slice(unpadded, &mut buf[..]))
            .map_err(|_| BadKey)?;
        if n != 32 {
            return Err(BadKey);
        }
        let mut key = Zeroizing::new([0u8; 32]);
        key.copy_from_slice(&buf[..32]);
        Ok(Self(key))
    }

    /// A new key from the operating system's random source.
    pub fn generate() -> Result<Self> {
        let mut key = Zeroizing::new([0u8; 32]);
        getrandom::fill(&mut key[..]).map_err(|e| anyhow!("no random bytes for a key: {e}"))?;
        Ok(Self(key))
    }

    /// The key as a key file holds it: standard base64 and a newline.
    pub fn to_line(&self) -> Zeroizing<[u8; 45]> {
        let mut line = Zeroizing::new([0u8; 45]);
        let n = STANDARD
            .encode_slice(&self.0[..], &mut line[..44])
            .expect("32 bytes are 44 base64 characters");
        debug_assert_eq!(n, 44);
        line[44] = b'\n';
        line
    }

    /// Derives the encryption key and the key id. Runs once, at start.
    ///
    /// The pseudorandom key and the derived key are wiped. The HMAC key-setup
    /// pad blocks and the expansion temporaries inside `hmac` and `hkdf` are not:
    /// they exist once, while this runs.
    pub fn sealer(&self) -> Sealer {
        let (mut prk, hk) = Hkdf::<Sha256>::extract(None, &self.0[..]);
        prk.as_mut_slice().zeroize();
        let mut encryption = Zeroizing::new([0u8; 32]);
        hk.expand(ENCRYPTION_INFO, &mut encryption[..])
            .expect("32 bytes is a valid HKDF length");
        let mut id = [0u8; 8];
        hk.expand(KEY_ID_INFO, &mut id)
            .expect("8 bytes is a valid HKDF length");
        drop(hk);
        let key: &Key = (&encryption[..])
            .try_into()
            .expect("the derived key is 32 bytes");
        Sealer {
            cipher: XChaCha20Poly1305::new(key),
            key_id: hex::encode(id),
        }
    }
}

/// Seals and opens with the derived key, which the cipher wipes on drop.
pub struct Sealer {
    cipher: XChaCha20Poly1305,
    key_id: String,
}

/// Why a sealed value did not open.
#[derive(Debug, PartialEq, Eq)]
pub enum OpenError {
    /// Not base64url, or too short to hold a nonce and a tag.
    Malformed,
    /// The tag does not verify: another key, other associated data, or altered.
    Unauthentic,
}

impl Sealer {
    /// One-way: 16 hex characters derived from the master key.
    pub fn key_id(&self) -> &str {
        &self.key_id
    }

    /// `plaintext` sealed under a fresh random nonce, as
    /// base64url(nonce ‖ ciphertext ‖ tag) without padding. The only way to seal
    /// outside tests: the nonce is never chosen by the caller.
    pub fn seal(&self, plaintext: &[u8], aad: &[u8]) -> Result<String> {
        let mut nonce = [0u8; NONCE_LEN];
        getrandom::fill(&mut nonce).map_err(|e| anyhow!("no random bytes for a nonce: {e}"))?;
        Ok(self.encrypt(nonce, plaintext, aad))
    }

    /// `seal` with a given nonce: only for the known-answer tests.
    #[cfg(test)]
    fn seal_with_nonce(&self, nonce: &[u8; NONCE_LEN], plaintext: &[u8], aad: &[u8]) -> String {
        self.encrypt(*nonce, plaintext, aad)
    }

    /// Takes the nonce by value: callers outside tests hand over one they just
    /// drew and cannot reuse it.
    fn encrypt(&self, nonce: [u8; NONCE_LEN], plaintext: &[u8], aad: &[u8]) -> String {
        let sealed = self
            .cipher
            .encrypt(
                &XNonce::from(nonce),
                Payload {
                    msg: plaintext,
                    aad,
                },
            )
            .expect("a credential is far below the cipher's length limit");
        let mut out = Vec::with_capacity(NONCE_LEN + sealed.len());
        out.extend_from_slice(&nonce);
        out.extend_from_slice(&sealed);
        URL_SAFE_NO_PAD.encode(out)
    }

    pub fn open(&self, sealed: &str, aad: &[u8]) -> Result<Zeroizing<Vec<u8>>, OpenError> {
        let raw = URL_SAFE_NO_PAD
            .decode(sealed)
            .map_err(|_| OpenError::Malformed)?;
        if raw.len() < NONCE_LEN + TAG_LEN {
            return Err(OpenError::Malformed);
        }
        let (nonce, body) = raw.split_at(NONCE_LEN);
        let nonce: [u8; NONCE_LEN] = nonce.try_into().expect("split at the nonce length");
        self.cipher
            .decrypt(&XNonce::from(nonce), Payload { msg: body, aad })
            .map(Zeroizing::new)
            .map_err(|_| OpenError::Unauthentic)
    }
}

/// The length-prefixed concatenation of `parts`.
pub fn aad(parts: &[&[u8]]) -> Vec<u8> {
    let mut out = Vec::with_capacity(parts_len(parts));
    write_parts(&mut out, parts);
    out
}

/// The same encoding into a buffer that is wiped on drop, sized up front so it
/// never reallocates (and leaves no unwiped copy behind).
pub fn sealed_parts(parts: &[&[u8]]) -> Zeroizing<Vec<u8>> {
    let mut out = Zeroizing::new(Vec::with_capacity(parts_len(parts)));
    write_parts(&mut out, parts);
    out
}

/// Splits a length-prefixed encoding back into its parts; `None` unless the
/// whole input is consumed exactly.
pub fn split_parts(mut bytes: &[u8]) -> Option<Vec<&[u8]>> {
    let mut parts = Vec::new();
    while !bytes.is_empty() {
        let (len, rest) = bytes.split_at_checked(4)?;
        let len = u32::from_be_bytes(len.try_into().ok()?) as usize;
        let (part, rest) = rest.split_at_checked(len)?;
        parts.push(part);
        bytes = rest;
    }
    Some(parts)
}

fn parts_len(parts: &[&[u8]]) -> usize {
    parts.iter().map(|p| 4 + p.len()).sum()
}

fn write_parts(out: &mut Vec<u8>, parts: &[&[u8]]) {
    for part in parts {
        let len = u32::try_from(part.len()).expect("a part is shorter than 4 GiB");
        out.extend_from_slice(&len.to_be_bytes());
        out.extend_from_slice(part);
    }
}

/// The associated data of a secret's sealed value.
pub fn secret_aad(
    store_id: &str,
    id: &str,
    name: &str,
    kind: &str,
    revision: u32,
    algorithm: &str,
    key_id: &str,
) -> Vec<u8> {
    aad(&[
        b"hie-secret/v1",
        store_id.as_bytes(),
        id.as_bytes(),
        name.as_bytes(),
        kind.as_bytes(),
        &u64::from(revision).to_be_bytes(),
        algorithm.as_bytes(),
        key_id.as_bytes(),
    ])
}

/// The associated data of the store's key check: the only builder, used both
/// when the check is written and when it is verified.
pub fn check_aad(store_id: &str, algorithm: &str, key_id: &str) -> Vec<u8> {
    aad(&[
        b"hie-secret-key-check/v1",
        store_id.as_bytes(),
        algorithm.as_bytes(),
        key_id.as_bytes(),
    ])
}

#[cfg(test)]
mod tests {
    use super::*;

    const MASTER: &str = "AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8=";

    fn sealer() -> Sealer {
        MasterKey::from_base64(MASTER).unwrap().sealer()
    }

    fn sample_aad() -> Vec<u8> {
        secret_aad("store", "sec_1", "lab", "aws", 1, ALGORITHM, "k")
    }

    #[test]
    fn a_sealed_value_opens_and_does_not_contain_its_plaintext() {
        let s = sealer();
        let plaintext = b"CANARY-0f3a7a0e-6d3c-4c1e-9d55-4f7c1d2b9a10";
        let sealed = s.seal(plaintext, &sample_aad()).unwrap();
        assert!(!sealed.contains("CANARY"));
        let raw = URL_SAFE_NO_PAD.decode(&sealed).unwrap();
        assert!(!raw.windows(plaintext.len()).any(|w| w == plaintext));
        assert_eq!(&s.open(&sealed, &sample_aad()).unwrap()[..], plaintext);
    }

    #[test]
    fn any_change_to_the_associated_data_or_the_sealed_bytes_fails() {
        let s = sealer();
        let sealed = s.seal(b"value", &sample_aad()).unwrap();
        let changed = [
            secret_aad("other", "sec_1", "lab", "aws", 1, ALGORITHM, "k"),
            secret_aad("store", "sec_2", "lab", "aws", 1, ALGORITHM, "k"),
            secret_aad("store", "sec_1", "lab2", "aws", 1, ALGORITHM, "k"),
            secret_aad("store", "sec_1", "lab", "gcp", 1, ALGORITHM, "k"),
            secret_aad("store", "sec_1", "lab", "aws", 2, ALGORITHM, "k"),
            secret_aad("store", "sec_1", "lab", "aws", 1, "aes256gcm", "k"),
            secret_aad("store", "sec_1", "lab", "aws", 1, ALGORITHM, "j"),
        ];
        for aad in changed {
            assert_eq!(s.open(&sealed, &aad), Err(OpenError::Unauthentic));
        }
        let raw = URL_SAFE_NO_PAD.decode(&sealed).unwrap();
        // A byte of the nonce, of the ciphertext, and of the tag.
        for at in [0, NONCE_LEN + 1, raw.len() - 1] {
            let mut flipped = raw.clone();
            flipped[at] ^= 1;
            assert_eq!(
                s.open(&URL_SAFE_NO_PAD.encode(&flipped), &sample_aad()),
                Err(OpenError::Unauthentic)
            );
        }
        assert_eq!(
            s.open("not base64!", &sample_aad()),
            Err(OpenError::Malformed)
        );
        assert_eq!(s.open("AAAA", &sample_aad()), Err(OpenError::Malformed));
    }

    #[test]
    fn parts_that_shift_between_fields_give_different_associated_data() {
        assert_ne!(aad(&[b"ab", b"c"]), aad(&[b"a", b"bc"]));
        assert_eq!(
            aad(&[b"x", &3u64.to_be_bytes()]),
            [
                &[0, 0, 0, 1, b'x'][..],
                &[0, 0, 0, 8, 0, 0, 0, 0, 0, 0, 0, 3]
            ]
            .concat()
        );
    }

    #[test]
    fn every_seal_uses_a_fresh_nonce() {
        let s = sealer();
        assert_ne!(
            s.seal(b"same", &sample_aad()).unwrap(),
            s.seal(b"same", &sample_aad()).unwrap()
        );
        let mut nonces = std::collections::HashSet::new();
        for _ in 0..10_000 {
            let sealed = s.seal(b"", b"").unwrap();
            let raw = URL_SAFE_NO_PAD.decode(sealed).unwrap();
            assert!(nonces.insert(raw[..NONCE_LEN].to_vec()));
        }
    }

    /// Expected values computed outside this code base: HKDF with Python's
    /// `hmac`/`hashlib`, and the seal with libsodium's
    /// `crypto_aead_xchacha20poly1305_ietf_encrypt`.
    #[test]
    fn the_sealed_format_matches_an_independent_implementation() {
        let s = sealer();
        assert_eq!(s.key_id(), "d024517e26f979f8");
        let nonce: [u8; 24] = std::array::from_fn(|i| 0x40 + i as u8);
        let plaintext = sealed_parts(&[
            b"hie-secret-value/v1",
            &2u64.to_be_bytes(),
            b"access_key_id",
            b"AKIDKATEXAMPLE",
            b"secret_access_key",
            b"kat-not-a-real-secret",
        ]);
        assert_eq!(
            hex::encode(&plaintext[..]),
            "000000136869652d7365637265742d76616c75652f76310000000800000000000000020000000d6163636573735f6b65795f69640000000e414b49444b41544558414d504c45000000117365637265745f6163636573735f6b6579000000156b61742d6e6f742d612d7265616c2d736563726574"
        );
        let aad = secret_aad(
            "00000000-0000-4000-8000-000000000000",
            "sec_0123456789abcdef",
            "lab-bucket",
            "aws",
            3,
            ALGORITHM,
            s.key_id(),
        );
        let sealed = s.seal_with_nonce(&nonce, &plaintext, &aad);
        assert_eq!(
            sealed,
            "QEFCQ0RFRkdISUpLTE1OT1BRUlNUVVZXfH8h4q84uacgBKLNvVcuONc8QBZVZbONMpgPokXrIoW8a68n5AbddlK7xGFK5PT-PW5J8Yfl2gk0-ZWOGcFKqr_1CrPBYhECTM9ABEh6nEtRaunGJGyENYTng8oCVltwavdvD6DtbQn-WRj-GaP-peg7-fj41Ifb8OmWySwu77VMfq9M"
        );
        assert_eq!(&s.open(&sealed, &aad).unwrap()[..], &plaintext[..]);
    }

    /// RFC 5869 test case 3 (no salt, no info) through the call shape `sealer`
    /// uses: `extract(None, ..)` then `expand`.
    #[test]
    fn hkdf_without_salt_matches_rfc_5869() {
        let (prk, hk) = Hkdf::<Sha256>::extract(None, &[0x0b; 22]);
        assert_eq!(
            hex::encode(prk.as_slice()),
            "19ef24a32c717b167f33a91d6f648bdf96596776afdb6377ac434c1c293ccb04"
        );
        let mut okm = [0u8; 42];
        hk.expand(b"", &mut okm).unwrap();
        assert_eq!(
            hex::encode(okm),
            "8da4e775a563c18f715f802a063c5a31b8a11f5c5ee1879ec3454e5f3c738d2d9d201395faa4b61a96c8"
        );
    }

    #[test]
    fn key_text_rules() {
        let standard = MasterKey::from_base64(MASTER).unwrap();
        assert_eq!(&standard.to_line()[..44], MASTER.as_bytes());
        assert_eq!(standard.to_line()[44], b'\n');
        let id = standard.sealer().key_id().to_string();
        // Without padding, URL-safe, and with whitespace around: the same key.
        let key = MasterKey::generate().unwrap();
        let line = String::from_utf8(key.to_line()[..44].to_vec()).unwrap();
        let url_safe = line.replace('+', "-").replace('/', "_");
        for text in [
            MASTER.trim_end_matches('='),
            &format!("  {MASTER}\n"),
            &format!("\t{}\r\n", MASTER.trim_end_matches('=')),
        ] {
            assert_eq!(MasterKey::from_base64(text).unwrap().sealer().key_id(), id);
        }
        assert_eq!(
            MasterKey::from_base64(&url_safe).unwrap().sealer().key_id(),
            key.sealer().key_id()
        );
        // Refused: wrong lengths, other characters, two paddings, a short key.
        for bad in [
            "",
            "AAEC",
            &MASTER[..42],
            &format!("{MASTER}A"),
            "AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh*=",
            "AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHg==",
            "AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwd Hh8=",
        ] {
            assert!(MasterKey::from_base64(bad).is_err(), "{bad:?}");
        }
    }

    #[test]
    fn split_parts_consumes_exactly() {
        let encoded = aad(&[b"a", b"", b"bcd"]);
        assert_eq!(
            split_parts(&encoded).unwrap(),
            vec![&b"a"[..], &b""[..], &b"bcd"[..]]
        );
        assert!(split_parts(&encoded[..encoded.len() - 1]).is_none());
        assert!(split_parts(&[0, 0, 0]).is_none());
    }
}
