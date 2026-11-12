//! The native secret store through its public calls: persistence, the key
//! check at start, concurrent first starts, and sealed values that were moved
//! or altered in the database.
#![cfg(feature = "secrets-native")]

use std::path::Path;
use std::sync::Arc;

use hie::config::Config;
use hie::db::Db;
use hie::secrets::{self, KeySettings, NewSecret, SecretError, SecretKind, SecretValue, Secrets};
use secrecy::ExposeSecret;

fn key_text() -> String {
    use base64::Engine;
    let mut bytes = [0u8; 32];
    getrandom::fill(&mut bytes).unwrap();
    base64::engine::general_purpose::STANDARD.encode(bytes)
}

fn config(dir: &Path) -> Config {
    std::fs::write(
        dir.join("hie.yaml"),
        "fhir_base_url: http://127.0.0.1:9\ndatabase: data/hie.db\n",
    )
    .unwrap();
    hie::config::load(&dir.join("hie.yaml")).unwrap().0
}

fn db(c: &Config) -> Arc<Db> {
    Arc::new(Db::connect(&c.database.to_string_lossy()).unwrap())
}

fn value(id: &str, secret: &str) -> SecretValue {
    SecretValue::new(
        SecretKind::Aws,
        [
            ("access_key_id".to_string(), id.to_string().into()),
            ("secret_access_key".to_string(), secret.to_string().into()),
        ],
    )
    .unwrap()
}

async fn create(s: &Secrets, name: &str, id: &str, secret: &str) -> String {
    s.store()
        .unwrap()
        .create(NewSecret {
            name: name.into(),
            kind: SecretKind::Aws,
            value: value(id, secret),
        })
        .await
        .unwrap()
        .id
}

fn refusal(r: anyhow::Result<Secrets>) -> String {
    match r {
        Ok(_) => panic!("expected a refusal"),
        Err(e) => format!("{e:#}"),
    }
}

fn store_key(message: &str) -> &str {
    let at = message.find("(store key ").expect("suffix") + "(store key ".len();
    &message[at..at + 16]
}

#[tokio::test]
async fn credentials_survive_a_restart_with_the_same_key() {
    let dir = tempfile::tempdir().unwrap();
    let c = config(dir.path());
    let first = db(&c);
    let key = key_text();
    let s = secrets::open(&c, &first, KeySettings::inline(&key)).unwrap();
    let secret = format!("CANARY-{}", uuid());
    let id = create(&s, "lab", "AKIDEXAMPLE", &secret).await;
    let meta = s
        .store()
        .unwrap()
        .replace(&id, value("AKIDOTHER", &secret))
        .await
        .unwrap();
    assert_eq!((meta.revision, meta.hint.as_deref()), (2, Some("THER")));

    // Another handle on the same store, as after a restart.
    let again = secrets::open(&c, &db(&c), KeySettings::inline(&key)).unwrap();
    let revealed = again.store().unwrap().reveal("lab").await.unwrap();
    assert_eq!(revealed.meta.id, id);
    assert_eq!(revealed.meta.revision, 2);
    let field = |f: &str| revealed.value.field(f).unwrap().expose_secret().to_string();
    assert_eq!(field("access_key_id"), "AKIDOTHER");
    assert_eq!(field("secret_access_key"), secret);
    drop(first);
}

#[tokio::test]
async fn a_start_without_the_store_key_is_refused_with_both_ids() {
    let dir = tempfile::tempdir().unwrap();
    let c = config(dir.path());
    let keep = db(&c);
    // Sealed by the automatic key file.
    let s = secrets::open(&c, &keep, KeySettings::none()).unwrap();
    let store = s.health()["key_id"].as_str().unwrap().to_string();
    create(&s, "lab", "AKIDEXAMPLE", "generated").await;
    let auto = c.database.parent().unwrap().join("hie-secrets.key");
    assert!(auto.exists());

    // Another key: refused, naming both.
    let m = refusal(secrets::open(&c, &db(&c), KeySettings::inline(&key_text())));
    assert!(m.contains("sealed with another key"), "{m}");
    assert_eq!(store_key(&m), store);

    // The key file lost: refused, and no new key is made.
    std::fs::rename(&auto, dir.path().join("saved.key")).unwrap();
    let m = refusal(secrets::open(&c, &db(&c), KeySettings::none()));
    assert!(m.contains("this instance's key none"), "{m}");
    assert!(!auto.exists(), "a sealed store never gets a new key");

    // A malformed key, and a key file that is not there.
    let m = refusal(secrets::open(&c, &db(&c), KeySettings::inline("short")));
    assert!(
        m.contains("this instance's key malformed") && m.contains(&store),
        "{m}"
    );
    let m = refusal(secrets::open(
        &c,
        &db(&c),
        KeySettings::file(dir.path().join("nowhere.key")),
    ));
    assert!(m.contains("this instance's key unavailable"), "{m}");

    // Ambiguous: a key and a key file.
    let m = refusal(secrets::open(
        &c,
        &db(&c),
        KeySettings::from_vars(
            Some(key_text().into()),
            Some(dir.path().join("saved.key").into()),
        ),
    ));
    assert!(m.contains("this instance's key ambiguous"), "{m}");

    // The key file restored: it works again.
    std::fs::rename(dir.path().join("saved.key"), &auto).unwrap();
    assert!(secrets::open(&c, &db(&c), KeySettings::none()).is_ok());
}

#[tokio::test]
async fn an_unsealed_store_with_a_malformed_key_starts_without_credentials() {
    let dir = tempfile::tempdir().unwrap();
    let c = config(dir.path());
    let s = secrets::open(&c, &db(&c), KeySettings::inline("not-a-key")).unwrap();
    assert_eq!(s.health()["available"], false);
    assert_eq!(s.health()["backend"], "native");
    let Err(SecretError::Unavailable(reason)) = s.store() else {
        panic!("expected no store");
    };
    assert!(reason.contains("HIE_SECRET_KEY is not a key"), "{reason}");
}

#[tokio::test]
async fn concurrent_first_starts_with_one_key_seal_the_store_once() {
    let dir = tempfile::tempdir().unwrap();
    let c = config(dir.path());
    let key = key_text();
    let keep = db(&c);
    let handles: Vec<Secrets> = (0..8)
        .map(|_| {
            let (c, key) = (c.clone(), key.clone());
            std::thread::spawn(move || secrets::open(&c, &db(&c), KeySettings::inline(&key)))
        })
        .collect::<Vec<_>>()
        .into_iter()
        .map(|t| t.join().unwrap().unwrap())
        .collect();
    // A value sealed through one handle opens through every other: they all
    // bind it to the same store id.
    let id = create(&handles[0], "lab", "AKIDEXAMPLE", "generated").await;
    for h in &handles {
        assert_eq!(h.store().unwrap().reveal(&id).await.unwrap().meta.id, id);
    }
    // A second key cannot take over the store.
    let m = refusal(secrets::open(&c, &keep, KeySettings::inline(&key_text())));
    assert!(m.contains("sealed with another key"), "{m}");
}

#[tokio::test]
async fn a_sealed_value_moved_or_altered_in_the_database_does_not_open() {
    let dir = tempfile::tempdir().unwrap();
    let c = config(dir.path());
    let d = db(&c);
    let s = secrets::open(&c, &d, KeySettings::inline(&key_text())).unwrap();
    let a = create(&s, "alpha", "AKIDAAAA", "generated-a").await;
    let b = create(&s, "beta", "AKIDBBBB", "generated-b").await;
    let store = s.store().unwrap().clone();
    let integrity = |r: Result<hie::secrets::Revealed, SecretError>| match r {
        Err(SecretError::Integrity(m)) => {
            assert!(!m.contains("generated"), "{m}");
            m
        }
        Err(e) => panic!("expected an integrity error, got {e:?}"),
        Ok(_) => panic!("expected an integrity error, it opened"),
    };
    let exec = |sql: &str| {
        d.query_json(sql, Vec::new()).unwrap();
    };

    // Swap the sealed values of the two rows.
    exec(&format!(
        "UPDATE secrets SET sealed = (SELECT s.sealed FROM secrets s WHERE s.id = '{b}') WHERE id = '{a}'"
    ));
    integrity(store.reveal(&a).await);
    assert!(store.reveal(&b).await.is_ok());

    // Swap the names.
    let dir2 = tempfile::tempdir().unwrap();
    let c2 = config(dir2.path());
    let d2 = db(&c2);
    let s2 = secrets::open(&c2, &d2, KeySettings::inline(&key_text())).unwrap();
    let a2 = create(&s2, "alpha", "AKIDAAAA", "generated-a").await;
    let b2 = create(&s2, "beta", "AKIDBBBB", "generated-b").await;
    let store2 = s2.store().unwrap().clone();
    let exec2 = |sql: &str| {
        d2.query_json(sql, Vec::new()).unwrap();
    };
    exec2(&format!(
        "UPDATE secrets SET name = 'tmp' WHERE id = '{a2}'"
    ));
    exec2(&format!(
        "UPDATE secrets SET name = 'alpha' WHERE id = '{b2}'"
    ));
    exec2(&format!(
        "UPDATE secrets SET name = 'beta' WHERE id = '{a2}'"
    ));
    integrity(store2.reveal(&a2).await);
    integrity(store2.reveal(&b2).await);

    // A revision bumped, a key id or an algorithm edited.
    exec(&format!(
        "UPDATE secrets SET revision = revision + 1 WHERE id = '{b}'"
    ));
    integrity(store.reveal(&b).await);
    exec(&format!(
        "UPDATE secrets SET revision = revision - 1, key_id = '0000000000000000' WHERE id = '{b}'"
    ));
    integrity(store.reveal(&b).await);
    let k = s.health()["key_id"].as_str().unwrap().to_string();
    exec(&format!(
        "UPDATE secrets SET key_id = '{k}', algorithm = 'aes256gcm' WHERE id = '{b}'"
    ));
    integrity(store.reveal(&b).await);
    exec(&format!(
        "UPDATE secrets SET algorithm = 'xchacha20poly1305' WHERE id = '{b}'"
    ));
    assert!(store.reveal(&b).await.is_ok(), "restored, it opens again");

    // The key check removed while credentials exist: refused at start.
    exec("DELETE FROM secret_keys");
    let m = refusal(secrets::open(&c, &d, KeySettings::inline(&key_text())));
    assert!(m.contains("holds 2 credentials but no key check"), "{m}");
}

fn uuid() -> String {
    let mut b = [0u8; 16];
    getrandom::fill(&mut b).unwrap();
    hex::encode(b)
}
