use std::sync::{Arc, Barrier};

use super::*;
use crate::secrets::{NewSecret, SecretKind, SecretValue};

fn key_text() -> String {
    let key = MasterKey::generate().unwrap();
    String::from_utf8(key.to_line()[..44].to_vec()).unwrap()
}

fn key_id_of(text: &str) -> String {
    MasterKey::from_base64(text)
        .unwrap()
        .sealer()
        .key_id()
        .to_string()
}

/// A configuration in `dir` with a SQLite file store, plus `extra` YAML.
fn config(dir: &Path, extra: &str) -> Config {
    std::fs::write(
        dir.join("hie.yaml"),
        format!("fhir_base_url: http://127.0.0.1:9\ndatabase: data/hie.db\n{extra}"),
    )
    .unwrap();
    crate::config::load(&dir.join("hie.yaml")).unwrap().0
}

fn db_for(config: &Config) -> Arc<Db> {
    Arc::new(Db::connect(&config.database.to_string_lossy()).unwrap())
}

/// An in-memory store and a configuration that says so.
fn memory(dir: &Path) -> (Config, Arc<Db>) {
    let mut c = config(dir, "");
    c.database = PathBuf::from(":memory:");
    (c, Arc::new(Db::in_memory().unwrap()))
}

fn auto_path(c: &Config) -> PathBuf {
    c.database.parent().unwrap().join(AUTO_KEY_FILE)
}

/// The refusal's two markers, after checking the suffix has the stable shape.
fn markers(message: &str) -> (String, String) {
    let at = message
        .rfind("(store key ")
        .unwrap_or_else(|| panic!("no key suffix: {message}"));
    let tail = &message[at..];
    assert!(tail.ends_with(')'), "{message}");
    let inner = &tail["(store key ".len()..tail.len() - 1];
    let (store, instance) = inner
        .split_once(", this instance's key ")
        .unwrap_or_else(|| panic!("bad suffix: {message}"));
    let hex16 = |s: &str| {
        s.len() == 16
            && s.chars()
                .all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase())
    };
    assert!(hex16(store) || store == "none", "{message}");
    assert!(
        hex16(instance)
            || ["none", "malformed", "unavailable", "ambiguous", "unsafe"].contains(&instance),
        "{message}"
    );
    (store.to_string(), instance.to_string())
}

fn refusal(result: Result<Secrets>) -> String {
    match result {
        Ok(_) => panic!("expected a refusal"),
        Err(e) => format!("{e:#}"),
    }
}

fn reason(secrets: &Secrets) -> String {
    match secrets.store() {
        Ok(_) => panic!("expected no store"),
        Err(e) => e.to_string(),
    }
}

/// Seals the store with `key` and returns its key id.
fn seal(c: &Config, db: &Arc<Db>, key: &str) -> String {
    let s = open(c, db, KeySettings::inline(key)).unwrap();
    assert!(s.store().is_ok());
    s.health()["key_id"].as_str().unwrap().to_string()
}

#[cfg(unix)]
fn chmod(path: &Path, mode: u32) {
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode)).unwrap();
}

#[cfg(unix)]
fn mkfifo(path: &Path) {
    use std::os::unix::ffi::OsStrExt;
    let c = std::ffi::CString::new(path.as_os_str().as_bytes()).unwrap();
    // SAFETY: a valid NUL-terminated path.
    assert_eq!(unsafe { libc::mkfifo(c.as_ptr(), 0o600) }, 0);
}

#[test]
fn an_unsealed_store_seals_with_a_usable_key_or_starts_without_one() {
    // A key in the environment seals the store.
    let dir = tempfile::tempdir().unwrap();
    let c = config(dir.path(), "");
    let k = key_text();
    assert_eq!(seal(&c, &db_for(&c), &k), key_id_of(&k));
    assert!(!auto_path(&c).exists(), "no key file when a key is given");

    // None given, a SQLite file: the key file is made, owner-only, and used.
    let dir = tempfile::tempdir().unwrap();
    let c = config(dir.path(), "");
    let s = open(&c, &db_for(&c), KeySettings::none()).unwrap();
    assert!(s.store().is_ok());
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mode = std::fs::metadata(auto_path(&c))
            .unwrap()
            .permissions()
            .mode();
        assert_eq!(mode & 0o777, 0o600);
    }

    // In memory, nothing given: starts without a store.
    let dir = tempfile::tempdir().unwrap();
    let (c, db) = memory(dir.path());
    let s = open(&c, &db, KeySettings::none()).unwrap();
    assert!(reason(&s).contains("an in-memory store has no place for a key file"));
    assert_eq!(s.health()["available"], false);

    // A malformed key, or a key file that cannot be read: starts without a
    // store, with the reason.
    let dir = tempfile::tempdir().unwrap();
    let c = config(dir.path(), "");
    let s = open(&c, &db_for(&c), KeySettings::inline("not-a-key")).unwrap();
    let r = reason(&s);
    assert!(
        r.contains("HIE_SECRET_KEY is not a key") && !r.contains("sealed"),
        "{r}"
    );
    assert_eq!(markers(&r), ("none".into(), "malformed".into()));
    let s = open(
        &c,
        &db_for(&c),
        KeySettings::file(dir.path().join("missing.key")),
    )
    .unwrap();
    let r = reason(&s);
    assert!(r.contains("cannot be read"), "{r}");
    assert_eq!(markers(&r).1, "unavailable");
    let s = open(
        &c,
        &db_for(&c),
        KeySettings::from_vars(Some(std::ffi::OsString::from("")), None),
    )
    .unwrap();
    assert!(s.store().is_ok(), "an empty variable counts as unset");
}

#[test]
fn a_sealed_store_needs_its_key() {
    let dir = tempfile::tempdir().unwrap();
    let c = config(dir.path(), "");
    let db = db_for(&c);
    let a = key_text();
    let store = seal(&c, &db, &a);

    // The same key: available, the same key id.
    let s = open(&c, &db, KeySettings::inline(&format!(" {a}\n"))).unwrap();
    assert_eq!(s.health()["key_id"], store.as_str());

    // Another key: E8 with both ids.
    let b = key_text();
    let m = refusal(open(&c, &db, KeySettings::inline(&b)));
    assert!(m.contains("sealed with another key"), "{m}");
    assert_eq!(markers(&m), (store.clone(), key_id_of(&b)));

    // No key at all, with an auto path: E6, and no key file is made.
    let m = refusal(open(&c, &db, KeySettings::none()));
    assert!(
        m.contains("is missing") && m.contains("never creates a new key"),
        "{m}"
    );
    assert_eq!(markers(&m), (store.clone(), "none".into()));
    assert!(!auto_path(&c).exists());

    // Malformed in the environment (E2) or in a file (E3).
    let m = refusal(open(&c, &db, KeySettings::inline("AAAA")));
    assert!(m.contains("sealed, but HIE_SECRET_KEY is not a key"), "{m}");
    assert_eq!(markers(&m), (store.clone(), "malformed".into()));
    let bad = dir.path().join("bad.key");
    std::fs::write(&bad, "not a key\n").unwrap();
    let m = refusal(open(&c, &db, KeySettings::file(&bad)));
    assert!(m.contains("does not hold a key"), "{m}");
    assert_eq!(markers(&m).1, "malformed");

    // A key file that cannot be read: E4.
    let m = refusal(open(
        &c,
        &db,
        KeySettings::file(dir.path().join("gone.key")),
    ));
    assert!(m.contains("cannot be read"), "{m}");
    assert_eq!(markers(&m), (store.clone(), "unavailable".into()));

    // The key in a file works as well as in the environment.
    let good = dir.path().join("good.key");
    std::fs::write(&good, format!("{a}\n")).unwrap();
    assert!(open(&c, &db, KeySettings::file(&good)).is_ok());
}

#[test]
fn a_sealed_in_memory_store_without_a_key_names_the_variables() {
    let dir = tempfile::tempdir().unwrap();
    let (c, db) = memory(dir.path());
    let store = seal(&c, &db, &key_text());
    let m = refusal(open(&c, &db, KeySettings::none()));
    assert!(
        m.contains("set HIE_SECRET_KEY, HIE_SECRET_KEY_FILE or secrets.key_file"),
        "{m}"
    );
    assert_eq!(markers(&m), (store, "none".into()));
}

#[test]
fn a_key_set_in_more_than_one_place_is_refused_sealed_or_not() {
    let dir = tempfile::tempdir().unwrap();
    let file = dir.path().join("k.key");
    let k = key_text();
    std::fs::write(&file, format!("{k}\n")).unwrap();
    let c = config(dir.path(), "secrets:\n  key_file: k.key\n");
    let db = db_for(&c);
    // The environment's key and a key file.
    let both = || KeySettings::from_vars(Some(k.clone().into()), Some(file.clone().into()));
    let m = refusal(open(&c, &db, both()));
    assert!(
        m.contains("more than one place") && m.contains("hie.yaml"),
        "{m}"
    );
    assert_eq!(markers(&m), ("none".into(), "ambiguous".into()));
    // The same file named twice is one place.
    let store = open(&c, &db, KeySettings::file(&file)).unwrap();
    let store = store.health()["key_id"].as_str().unwrap().to_string();
    // Two different files: refused, also once sealed.
    let other = dir.path().join("other.key");
    std::fs::write(&other, format!("{k}\n")).unwrap();
    let m = refusal(open(&c, &db, KeySettings::file(&other)));
    assert_eq!(markers(&m), (store.clone(), "ambiguous".into()));
    let m = refusal(open(&c, &db, both()));
    assert_eq!(markers(&m), (store, "ambiguous".into()));
}

#[cfg(unix)]
#[test]
fn an_unsafe_automatic_key_file_is_refused_sealed_or_not() {
    // Readable by others.
    let dir = tempfile::tempdir().unwrap();
    let c = config(dir.path(), "");
    let db = db_for(&c);
    std::fs::create_dir_all(auto_path(&c).parent().unwrap()).unwrap();
    std::fs::write(auto_path(&c), format!("{}\n", key_text())).unwrap();
    chmod(&auto_path(&c), 0o644);
    let m = refusal(open(&c, &db, KeySettings::none()));
    assert!(m.contains("other users can read it (mode 644)"), "{m}");
    assert_eq!(markers(&m), ("none".into(), "unsafe".into()));
    // Once sealed, the same refusal names the store's key.
    let store = seal(&c, &db, &key_text());
    let m = refusal(open(&c, &db, KeySettings::none()));
    assert_eq!(markers(&m), (store, "unsafe".into()));

    // A symbolic link, and a FIFO (which must not block the start).
    for make in ["link", "fifo"] {
        let dir = tempfile::tempdir().unwrap();
        let c = config(dir.path(), "");
        std::fs::create_dir_all(auto_path(&c).parent().unwrap()).unwrap();
        if make == "link" {
            let target = dir.path().join("real.key");
            std::fs::write(&target, format!("{}\n", key_text())).unwrap();
            chmod(&target, 0o600);
            std::os::unix::fs::symlink(&target, auto_path(&c)).unwrap();
        } else {
            mkfifo(&auto_path(&c));
        }
        let m = refusal(open(&c, &db_for(&c), KeySettings::none()));
        let why = if make == "link" {
            "it is a symbolic link"
        } else {
            "it is not a regular file"
        };
        assert!(m.contains(why), "{m}");
        assert_eq!(markers(&m).1, "unsafe");
    }
}

#[cfg(unix)]
#[test]
fn key_file_reads_are_bounded_and_explicit_files_may_be_links() {
    let dir = tempfile::tempdir().unwrap();
    let auto = dir.path().join(AUTO_KEY_FILE);
    std::fs::write(&auto, "A".repeat(200)).unwrap();
    chmod(&auto, 0o600);
    assert!(matches!(read_auto(&auto), Input::Malformed(_)));
    // An existing key file is used as it is, never overwritten.
    let k = key_text();
    std::fs::write(&auto, format!("{k}\n")).unwrap();
    let Input::Usable(found, _) = read_auto(&auto) else {
        panic!("not usable");
    };
    assert_eq!(found.sealer().key_id(), key_id_of(&k));
    let (made, _) = create_auto_key(&auto, || Ok(())).unwrap();
    assert_eq!(made.sealer().key_id(), key_id_of(&k));
    assert_eq!(std::fs::read_to_string(&auto).unwrap(), format!("{k}\n"));

    // An explicit file: a link to a 0644 file is fine; a FIFO is unreadable.
    let target = dir.path().join("mounted.key");
    std::fs::write(&target, format!("{k}\n")).unwrap();
    chmod(&target, 0o644);
    let link = dir.path().join("link.key");
    std::os::unix::fs::symlink(&target, &link).unwrap();
    assert!(matches!(read_explicit(&link), Input::Usable(..)));
    let fifo = dir.path().join("fifo.key");
    mkfifo(&fifo);
    let Input::Unreadable(_, why) = read_explicit(&fifo) else {
        panic!("a FIFO is not a key file");
    };
    assert_eq!(why, "not a regular file");
    assert!(matches!(read_explicit(dir.path()), Input::Unreadable(..)));
}

/// Whatever is in `dir` besides the key file: a temporary key file left over.
fn leftovers(dir: &Path) -> Vec<String> {
    std::fs::read_dir(dir)
        .unwrap()
        .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
        .filter(|n| n != AUTO_KEY_FILE)
        .collect()
}

#[test]
fn a_creator_removes_its_own_temporary_file_on_every_path() {
    // It loses the link to a key file already there.
    let dir = tempfile::tempdir().unwrap();
    let auto = dir.path().join(AUTO_KEY_FILE);
    let (first, made) = create_auto_key(&auto, || Ok(())).unwrap();
    let (second, taken) = create_auto_key(&auto, || Ok(())).unwrap();
    assert!(made && !taken, "only the first made the file");
    assert_eq!(first.sealer().key_id(), second.sealer().key_id());
    assert!(
        leftovers(dir.path()).is_empty(),
        "{:?}",
        leftovers(dir.path())
    );

    // It fails after its temporary file was written.
    let dir = tempfile::tempdir().unwrap();
    let auto = dir.path().join(AUTO_KEY_FILE);
    assert!(create_auto_key(&auto, || Err(std::io::Error::other("disk full"))).is_err());
    assert!(!auto.exists());
    assert!(
        leftovers(dir.path()).is_empty(),
        "{:?}",
        leftovers(dir.path())
    );
}

#[test]
fn a_creator_paused_before_its_link_takes_the_winners_key() {
    let dir = tempfile::tempdir().unwrap();
    let auto = dir.path().join(AUTO_KEY_FILE);
    let reached = Arc::new(Barrier::new(2));
    let release = Arc::new(Barrier::new(2));
    let paused = {
        let (auto, reached, release) = (auto.clone(), reached.clone(), release.clone());
        std::thread::spawn(move || {
            create_auto_key(&auto, || {
                reached.wait();
                release.wait();
                Ok(())
            })
            .map(|(k, _)| k.sealer().key_id().to_string())
        })
    };
    reached.wait();
    // Another instance starts meanwhile and completes.
    let (winner, _) = create_auto_key(&auto, || Ok(())).unwrap();
    assert_eq!(
        leftovers(dir.path()).len(),
        1,
        "the paused creator's temp stays"
    );
    release.wait();
    let late = paused.join().unwrap().unwrap();
    assert_eq!(late, winner.sealer().key_id());
    assert!(
        leftovers(dir.path()).is_empty(),
        "{:?}",
        leftovers(dir.path())
    );
}

#[test]
fn concurrent_creators_agree_on_one_key() {
    let dir = tempfile::tempdir().unwrap();
    let auto = dir.path().join(AUTO_KEY_FILE);
    let start = Arc::new(Barrier::new(8));
    let ids: Vec<String> = (0..8)
        .map(|_| {
            let (auto, start) = (auto.clone(), start.clone());
            std::thread::spawn(move || {
                start.wait();
                create_auto_key(&auto, || Ok(()))
                    .unwrap()
                    .0
                    .sealer()
                    .key_id()
                    .to_string()
            })
        })
        .collect::<Vec<_>>()
        .into_iter()
        .map(|t| t.join().unwrap())
        .collect();
    assert!(ids.iter().all(|id| *id == ids[0]), "{ids:?}");
    assert!(
        leftovers(dir.path()).is_empty(),
        "{:?}",
        leftovers(dir.path())
    );
}

#[tokio::test]
async fn the_recheck_refreshes_the_reason_once_and_never_opens_the_store() {
    // A malformed key at start; another instance then seals the store.
    let dir = tempfile::tempdir().unwrap();
    let c = config(dir.path(), "");
    let s = open(&c, &db_for(&c), KeySettings::inline("AAAA")).unwrap();
    let store = seal(&c, &db_for(&c), &key_text());
    s.recheck_once().await;
    let r = reason(&s);
    assert!(r.contains("sealed, but HIE_SECRET_KEY is not a key"), "{r}");
    assert_eq!(markers(&r), (store, "malformed".into()));

    // A key file that appears after start: the reason says so; still no store.
    let dir = tempfile::tempdir().unwrap();
    let (c, db) = memory(dir.path());
    let file = dir.path().join("late.key");
    let s = open(&c, &db, KeySettings::file(&file)).unwrap();
    assert!(reason(&s).contains("cannot be read"));
    std::fs::write(&file, format!("{}\n", key_text())).unwrap();
    s.recheck_once().await;
    let r = reason(&s);
    assert!(r.contains("became usable after this engine started"), "{r}");
    assert_eq!(s.health()["available"], false);
    // Once per process: a later change is not looked at.
    std::fs::remove_file(&file).unwrap();
    s.recheck_once().await;
    assert_eq!(reason(&s), r);
}

#[test]
fn an_instance_that_read_no_key_check_verifies_one_sealed_meanwhile() {
    let dir = tempfile::tempdir().unwrap();
    let c = config(dir.path(), "");
    let k = key_text();
    let first = db_for(&c);
    let second = db_for(&c);
    second.secret_tables().unwrap();
    // The first instance stops right after its first read of the key check.
    let reached = Arc::new(Barrier::new(2));
    let release = Arc::new(Barrier::new(2));
    {
        let (reached, release) = (reached.clone(), release.clone());
        let once = std::sync::atomic::AtomicBool::new(false);
        first.on_pause(Arc::new(move |at| {
            if at == "open:after-check-read"
                && !once.swap(true, std::sync::atomic::Ordering::SeqCst)
            {
                reached.wait();
                release.wait();
            }
        }));
    }
    let opening = {
        let (c, first, k) = (c.clone(), first.clone(), k.clone());
        std::thread::spawn(move || {
            open(&c, &first, KeySettings::inline(&k))
                .map(|s| s.health()["key_id"].as_str().unwrap().to_string())
        })
    };
    reached.wait();
    // Meanwhile another instance seals the store and stores a credential.
    let other = open(&c, &second, KeySettings::inline(&k)).unwrap();
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(
        other.store().unwrap().create(NewSecret {
            name: "lab".into(),
            kind: SecretKind::Aws,
            value: SecretValue::new(
                SecretKind::Aws,
                [
                    (
                        "access_key_id".to_string(),
                        "AKIDEXAMPLE".to_string().into(),
                    ),
                    (
                        "secret_access_key".to_string(),
                        "generated".to_string().into(),
                    ),
                ],
            )
            .unwrap(),
        }),
    )
    .unwrap();
    release.wait();
    assert_eq!(opening.join().unwrap().unwrap(), key_id_of(&k));

    // A real orphan: credentials without any key check.
    first
        .query_json("DELETE FROM secret_keys", Vec::new())
        .unwrap();
    let m = refusal(open(&c, &first, KeySettings::inline(&k)));
    assert!(m.contains("holds 1 credentials but no key check"), "{m}");
    assert_eq!(markers(&m), ("none".into(), key_id_of(&k)));
}

#[test]
fn the_instance_id_is_kept_in_the_sources_directory() {
    let dir = tempfile::tempdir().unwrap();
    assert_eq!(owner_id(Some(" box-1 ".into()), dir.path()), "box-1");
    let made = owner_id(None, dir.path());
    assert_eq!(made.len(), 16);
    assert_eq!(owner_id(Some("".into()), dir.path()), made);
    // Two at once agree.
    let other = tempfile::tempdir().unwrap();
    let ids: Vec<String> = (0..4)
        .map(|_| {
            let d = other.path().to_path_buf();
            std::thread::spawn(move || owner_id(None, &d))
        })
        .collect::<Vec<_>>()
        .into_iter()
        .map(|t| t.join().unwrap())
        .collect();
    assert!(ids.iter().all(|id| *id == ids[0]));
    let names: Vec<String> = std::fs::read_dir(other.path())
        .unwrap()
        .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
        .collect();
    assert_eq!(names, [INSTANCE_ID_FILE]);
}

#[test]
fn a_key_file_made_while_another_key_sealed_the_store_is_named_and_kept() {
    let dir = tempfile::tempdir().unwrap();
    let c = config(dir.path(), "");
    let first = db_for(&c);
    let second = db_for(&c);
    second.secret_tables().unwrap();
    // This start reads no key check, then stops.
    let reached = Arc::new(Barrier::new(2));
    let release = Arc::new(Barrier::new(2));
    {
        let (reached, release) = (reached.clone(), release.clone());
        let once = std::sync::atomic::AtomicBool::new(false);
        first.on_pause(Arc::new(move |at| {
            if at == "open:after-check-read"
                && !once.swap(true, std::sync::atomic::Ordering::SeqCst)
            {
                reached.wait();
                release.wait();
            }
        }));
    }
    let opening = {
        let (c, first) = (c.clone(), first.clone());
        std::thread::spawn(move || open(&c, &first, KeySettings::none()).map(drop))
    };
    reached.wait();
    // Meanwhile another instance seals the store with a key of its own.
    let other = key_text();
    let store = seal(&c, &second, &other);
    release.wait();
    let m = refusal(
        opening
            .join()
            .unwrap()
            .map(|()| Secrets::unavailable("unused")),
    );
    assert!(m.contains("sealed with another key"), "{m}");
    let path = auto_path(&c).display().to_string();
    assert!(m.contains(&format!("This start created {path}")), "{m}");
    let (store_marker, instance) = markers(&m);
    assert_eq!(store_marker, store);
    assert_ne!(instance, store);
    // The engine never removes a key file: it stays for the operator.
    assert!(auto_path(&c).exists());
    assert!(
        leftovers(auto_path(&c).parent().unwrap())
            .iter()
            .all(|n| !n.ends_with(".tmp"))
    );
    // A start that finds that file, which it did not make, gets the plain
    // refusal, and the file stays.
    let m = refusal(open(&c, &first, KeySettings::none()));
    assert!(
        m.contains("sealed with another key") && !m.contains("This start created"),
        "{m}"
    );
    assert!(auto_path(&c).exists());
}
