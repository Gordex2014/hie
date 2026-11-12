//! This instance's key and the store's key check: whether the store can be
//! used, must be refused, or starts without credentials.
//!
//! The key comes from exactly one of `HIE_SECRET_KEY`, `HIE_SECRET_KEY_FILE` or
//! `secrets.key_file`, or, for a SQLite file store with none of them, from
//! `hie-secrets.key` next to the database, created the first time. The first
//! instance to seal the store writes a key check; every later start proves its
//! key against it. A sealed store without its key refuses to start, and the
//! engine never makes a new key for one.

use std::io::Read;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, Result, anyhow, bail};
use zeroize::Zeroizing;

use super::native::NativeStore;
use super::{EnvKey, KeySettings, Secrets};
use crate::config::Config;
use crate::crypto::{self, ALGORITHM, CHECK_PLAINTEXT, MasterKey, Sealer};
use crate::db::Db;
use crate::db::secrets::CheckRow;

/// The key file made next to a SQLite database when no key is given.
pub const AUTO_KEY_FILE: &str = "hie-secrets.key";
/// Where an instance keeps its id among the instances sharing a store, when
/// `HIE_INSTANCE_ID` is not set.
pub const INSTANCE_ID_FILE: &str = ".hie-instance-id";
/// More than a key line ever holds: anything longer is not a key.
const MAX_KEY_FILE: usize = 128;

/// Where this instance's key comes from.
#[derive(Clone)]
enum Origin {
    Env,
    /// `HIE_SECRET_KEY_FILE` or `secrets.key_file`.
    File(PathBuf),
    /// `hie-secrets.key` next to the database.
    Auto(PathBuf),
}

impl Origin {
    fn describe(&self) -> String {
        match self {
            Origin::Env => "HIE_SECRET_KEY".into(),
            Origin::File(p) | Origin::Auto(p) => format!("the key file {}", p.display()),
        }
    }
}

/// What the key inputs gave.
enum Input {
    Usable(MasterKey, Origin),
    Malformed(Origin),
    Unreadable(Origin, String),
    /// The automatic key file exists but must not be used.
    Unsafe(PathBuf, String),
    /// No key; `auto` is where one may be created, if anywhere.
    Absent {
        auto: Option<PathBuf>,
    },
}

impl Input {
    /// How the refusal suffix names this instance's key.
    fn marker(&self, sealer: Option<&Sealer>) -> String {
        match self {
            Input::Usable(..) => sealer.map_or("none".into(), |s| s.key_id().to_string()),
            Input::Malformed(_) => "malformed".into(),
            Input::Unreadable(..) => "unavailable".into(),
            Input::Unsafe(..) => "unsafe".into(),
            Input::Absent { .. } => "none".into(),
        }
    }
}

/// The key inputs, kept for the one-shot recheck.
struct Inputs {
    key: EnvKey,
    explicit: Option<PathBuf>,
    auto: Option<PathBuf>,
    /// What the store is, for the reason given without a key.
    store: StoreKind,
}

#[derive(Clone, Copy)]
enum StoreKind {
    Postgres,
    /// SQLite in memory, or given as a `file:` URI.
    NoDirectory,
    SqliteFile,
}

impl Inputs {
    fn classify(&self) -> Input {
        match &self.key {
            EnvKey::Text(text) => match MasterKey::from_base64(text) {
                Ok(k) => Input::Usable(k, Origin::Env),
                Err(_) => Input::Malformed(Origin::Env),
            },
            EnvKey::NotUtf8 => Input::Malformed(Origin::Env),
            EnvKey::Unset => match (&self.explicit, &self.auto) {
                (Some(path), _) => read_explicit(path),
                (None, Some(auto)) => read_auto(auto),
                (None, None) => Input::Absent { auto: None },
            },
        }
    }

    /// Why there is no store, when no key is given and none can be made.
    fn no_key_reason(&self) -> &'static str {
        match self.store {
            StoreKind::Postgres => {
                "a PostgreSQL store needs HIE_SECRET_KEY, HIE_SECRET_KEY_FILE or secrets.key_file before its first credential"
            }
            StoreKind::NoDirectory | StoreKind::SqliteFile => {
                "an in-memory store has no place for a key file; set HIE_SECRET_KEY"
            }
        }
    }
}

/// The refusal suffix: both key ids, or what stands for them.
fn ids(store: &str, instance: &str) -> String {
    format!("(store key {store}, this instance's key {instance})")
}

fn store_marker(row: Option<&CheckRow>) -> String {
    row.map_or("none".into(), |r| r.key_id.clone())
}

/// The sentence for a key that is malformed or cannot be read; `sealed`
/// decides whether it is a refusal (E2–E4) or the reason without a store.
fn unusable_text(input: &Input, sealed: bool, store: &str) -> String {
    let but = if sealed {
        "the store is sealed, but "
    } else {
        ""
    };
    let instance = input.marker(None);
    let suffix = ids(store, &instance);
    match input {
        Input::Malformed(Origin::Env) => format!(
            "secret store: {but}HIE_SECRET_KEY is not a key: it must be 32 random bytes in base64 (43 or 44 characters, surrounding whitespace ignored), for example from openssl rand -base64 32 {suffix}"
        ),
        Input::Malformed(Origin::File(p) | Origin::Auto(p)) => format!(
            "secret store: {but}the key file {} does not hold a key: it must hold 32 random bytes in base64 (43 or 44 characters, surrounding whitespace ignored) and nothing else {suffix}",
            p.display()
        ),
        Input::Unreadable(origin, why) => format!(
            "secret store: {but}{} cannot be read ({why}) {suffix}",
            origin.describe()
        ),
        _ => unreachable!("only for malformed or unreadable keys"),
    }
}

fn unsafe_text(path: &Path, why: &str, store: &str) -> String {
    format!(
        "secret store: {} is not safe to use as the key: {why} {}",
        path.display(),
        ids(store, "unsafe")
    )
}

/// The refusal of a sealed store for an input that is not a usable key.
fn sealed_refusal(input: &Input, store: &str) -> String {
    match input {
        Input::Malformed(_) | Input::Unreadable(..) => unusable_text(input, true, store),
        Input::Unsafe(path, why) => unsafe_text(path, why, store),
        Input::Absent { auto: Some(path) } => format!(
            "secret store: the store is sealed and this instance has no key: {} is missing and none of HIE_SECRET_KEY, HIE_SECRET_KEY_FILE and secrets.key_file is set. Restore the key file; the engine never creates a new key for a sealed store {}",
            path.display(),
            ids(store, "none")
        ),
        Input::Absent { auto: None } => format!(
            "secret store: the store is sealed and this instance has no key: set HIE_SECRET_KEY, HIE_SECRET_KEY_FILE or secrets.key_file to the store's key {}",
            ids(store, "none")
        ),
        Input::Usable(..) => unreachable!("a usable key is verified"),
    }
}

/// Opens the store's secrets: the state machine of the key inputs and the key
/// check. An error refuses the start.
pub(crate) fn open(config: &Config, db: &Arc<Db>, keys: KeySettings) -> Result<Secrets> {
    let owner = owner_id(std::env::var("HIE_INSTANCE_ID").ok(), &config.sources_dir);
    let spec = config.database.to_string_lossy().into_owned();
    db.secret_tables()?;
    let row = db.secret_check_row()?;
    pause_point!(db, "open:after-check-read");

    // Ambiguous key settings are refused whether or not the store is sealed.
    let config_file = config.secrets.key_file.as_ref().map(|p| absolute(p));
    let env_file = keys.key_file.as_ref().map(|p| absolute(p));
    let mut places = keys.names_set();
    if config_file.is_some() {
        places.push("secrets.key_file");
    }
    let ambiguous = (places.contains(&"HIE_SECRET_KEY") && places.len() > 1)
        || matches!((&env_file, &config_file), (Some(a), Some(b)) if a != b);
    if ambiguous {
        bail!(
            "secret store: the key is set in more than one place ({}): set HIE_SECRET_KEY alone, or one key file (HIE_SECRET_KEY_FILE, or secrets.key_file in {}; both may name the same file) {}",
            places.join(", "),
            config.path.display(),
            ids(&store_marker(row.as_ref()), "ambiguous")
        );
    }

    let store = if spec.starts_with("postgres://") || spec.starts_with("postgresql://") {
        StoreKind::Postgres
    } else if spec == ":memory:" || spec.starts_with("file:") || spec.is_empty() {
        StoreKind::NoDirectory
    } else {
        StoreKind::SqliteFile
    };
    let auto = matches!(store, StoreKind::SqliteFile).then(|| {
        Path::new(&spec)
            .parent()
            .map_or_else(|| PathBuf::from(AUTO_KEY_FILE), |d| d.join(AUTO_KEY_FILE))
    });
    let inputs = Inputs {
        key: keys.key,
        explicit: env_file.or(config_file),
        auto,
        store,
    };
    let input = inputs.classify();
    if let Input::Unsafe(path, why) = &input {
        bail!(unsafe_text(path, why, &store_marker(row.as_ref())));
    }

    let row = match row {
        Some(row) => Some(row),
        None => {
            // Another instance may have sealed the store and stored credentials
            // since the first read: a secret exists only after its key check.
            let n = db.secret_count()?;
            if n > 0 {
                match db.secret_check_row()? {
                    Some(row) => Some(row),
                    None => {
                        let sealer = match &input {
                            Input::Usable(k, _) => Some(k.sealer()),
                            _ => None,
                        };
                        bail!(
                            "secret store: the store holds {n} credentials but no key check; refusing to seal them again under a new key {}",
                            ids("none", &input.marker(sealer.as_ref()))
                        );
                    }
                }
            } else {
                None
            }
        }
    };

    if let Some(row) = row {
        return match input {
            Input::Usable(k, origin) => verify(db, &row, k.sealer(), &origin, false, owner),
            other => Err(anyhow!(sealed_refusal(&other, &row.key_id))),
        };
    }

    // Not sealed yet.
    match input {
        Input::Usable(k, origin) => init(db, &k, &origin, false, owner),
        Input::Absent { auto: Some(path) } => {
            let (k, created) = create_auto_key(&path, || Ok(()))?;
            if created {
                tracing::warn!(
                    "created the secret key {}: it unlocks every stored credential and sits on the same volume as the database, so a copy of that volume holds both; keep a copy elsewhere, or set HIE_SECRET_KEY or HIE_SECRET_KEY_FILE to keep the key off this volume",
                    path.display()
                );
            }
            let opened = init(db, &k, &Origin::Auto(path.clone()), created, owner);
            // Another instance sealed the store with another key first: the
            // key file this start made unlocks nothing. The engine never
            // removes a key file; the refusal names it for the operator.
            if opened.is_err()
                && created
                && let Some(row) = db
                    .secret_check_row()
                    .ok()
                    .flatten()
                    .filter(|row| row.key_id != k.sealer().key_id())
            {
                bail!(
                    "secret store: the store is sealed with another key: start this instance with the store's key; the engine never replaces the key of a sealed store. This start created {} with a new key just before another instance sealed the store with its own: that file unlocks nothing; remove it, then start with the store's key {}",
                    path.display(),
                    ids(&row.key_id, k.sealer().key_id())
                );
            }
            opened
        }
        Input::Absent { auto: None } => {
            let reason = inputs.no_key_reason().to_string();
            tracing::warn!(
                "secret store unavailable: {reason}; /api/secrets answers 503 and sources cannot use credentials"
            );
            Ok(Secrets::unavailable_with_recheck(
                reason,
                Recheck {
                    db: db.clone(),
                    inputs,
                },
                owner,
            ))
        }
        unusable @ (Input::Malformed(_) | Input::Unreadable(..)) => {
            let reason = unusable_text(&unusable, false, "none");
            tracing::error!(
                "secret store unavailable: {reason}; /api/secrets answers 503 and sources cannot use credentials"
            );
            Ok(Secrets::unavailable_with_recheck(
                reason,
                Recheck {
                    db: db.clone(),
                    inputs,
                },
                owner,
            ))
        }
        Input::Unsafe(..) => unreachable!("refused above"),
    }
}

/// Seals the store with `key` unless another instance sealed it first, then
/// verifies against whichever key check won.
fn init(
    db: &Arc<Db>,
    key: &MasterKey,
    origin: &Origin,
    created: bool,
    owner: String,
) -> Result<Secrets> {
    let sealer = key.sealer();
    let candidate = new_store_id()?;
    let check_value = sealer.seal(
        CHECK_PLAINTEXT,
        &crypto::check_aad(&candidate, ALGORITHM, sealer.key_id()),
    )?;
    db.secret_insert_check(&CheckRow {
        store_id: candidate,
        key_id: sealer.key_id().to_string(),
        algorithm: ALGORITHM.to_string(),
        check_value,
    })?;
    let row = db
        .secret_check_row()?
        .context("secret store: the key check is gone right after it was written")?;
    verify(db, &row, sealer, origin, created, owner)
}

fn verify(
    db: &Arc<Db>,
    row: &CheckRow,
    sealer: Sealer,
    origin: &Origin,
    created: bool,
    owner: String,
) -> Result<Secrets> {
    let suffix = ids(&row.key_id, sealer.key_id());
    if row.algorithm != ALGORITHM {
        bail!(
            "secret store: the key check uses {}, which this version does not know {suffix}",
            row.algorithm
        );
    }
    if row.key_id != sealer.key_id() {
        bail!(
            "secret store: the store is sealed with another key: start this instance with the store's key; the engine never replaces the key of a sealed store {suffix}"
        );
    }
    let opened = sealer.open(
        &row.check_value,
        &crypto::check_aad(&row.store_id, &row.algorithm, &row.key_id),
    );
    if !opened.is_ok_and(|p| p[..] == *CHECK_PLAINTEXT) {
        bail!(
            "secret store: the key check does not open with this instance's key although the ids match: the store or the key has been altered {suffix}"
        );
    }
    if let (Origin::Auto(path), false) = (origin, created) {
        tracing::warn!(
            "the secret key {} sits on the same volume as the database: a copy of that volume holds both",
            path.display()
        );
    }
    tracing::info!(
        "secret store: native, key {}, store {}",
        sealer.key_id(),
        row.store_id
    );
    let key_id = sealer.key_id().to_string();
    let store = NativeStore::new(db.clone(), sealer, row.store_id.clone());
    Ok(Secrets::available(Arc::new(store), key_id, owner))
}

/// A random UUIDv4, as text.
fn new_store_id() -> Result<String> {
    let mut b = [0u8; 16];
    getrandom::fill(&mut b).map_err(|e| anyhow!("no random bytes for a store id: {e}"))?;
    b[6] = (b[6] & 0x0f) | 0x40;
    b[8] = (b[8] & 0x3f) | 0x80;
    let h = hex::encode(b);
    Ok(format!(
        "{}-{}-{}-{}-{}",
        &h[0..8],
        &h[8..12],
        &h[12..16],
        &h[16..20],
        &h[20..32]
    ))
}

fn absolute(path: &Path) -> PathBuf {
    std::path::absolute(path).unwrap_or_else(|_| path.to_path_buf())
}

/// What a store without a usable key re-reads once, so the reason it gives is
/// current. It never seals the store and never makes it available.
pub struct Recheck {
    db: Arc<Db>,
    inputs: Inputs,
}

impl Recheck {
    /// The reason now.
    pub(crate) fn run(self) -> String {
        let row = match self.db.secret_check_row() {
            Ok(row) => row,
            Err(e) => return format!("secret store: the key check cannot be read: {e:#}"),
        };
        let input = self.inputs.classify();
        match (row, input) {
            (Some(row), Input::Usable(k, _)) => format!(
                "secret store: the store was sealed after this engine started; restart it with the store's key to use credentials {}",
                ids(&row.key_id, k.sealer().key_id())
            ),
            (Some(row), other) => sealed_refusal(&other, &row.key_id),
            (None, Input::Usable(_, origin)) => format!(
                "secret store: {} became usable after this engine started; restart it to use credentials",
                origin.describe()
            ),
            (None, Input::Unsafe(path, why)) => unsafe_text(&path, &why, "none"),
            (None, Input::Absent { .. }) => self.inputs.no_key_reason().to_string(),
            (None, unusable) => unusable_text(&unusable, false, "none"),
        }
    }
}

// ---- key files ------------------------------------------------------------

/// Reads at most `MAX_KEY_FILE` bytes into a buffer that is wiped on drop:
/// the buffer and the length read, or `None` when the file holds more.
type KeyBuf = Zeroizing<[u8; MAX_KEY_FILE + 1]>;

fn read_bounded(file: &mut std::fs::File) -> std::io::Result<Option<(KeyBuf, usize)>> {
    let mut buf = Zeroizing::new([0u8; MAX_KEY_FILE + 1]);
    let mut n = 0;
    loop {
        match file.read(&mut buf[n..]) {
            Ok(0) => break,
            Ok(read) => {
                n += read;
                if n > MAX_KEY_FILE {
                    return Ok(None);
                }
            }
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => {}
            Err(e) => return Err(e),
        }
    }
    Ok(Some((buf, n)))
}

fn parse_key(buf: &[u8], origin: Origin) -> Input {
    match std::str::from_utf8(buf)
        .ok()
        .and_then(|text| MasterKey::from_base64(text).ok())
    {
        Some(k) => Input::Usable(k, origin),
        None => Input::Malformed(origin),
    }
}

fn io_reason(e: &std::io::Error) -> String {
    e.kind().to_string()
}

/// An explicit key file: symbolic links followed and any owner or mode taken
/// (secret mounts are usually root-owned links), but only a regular file, read
/// within bounds, so a FIFO or a directory never blocks the start.
fn read_explicit(path: &Path) -> Input {
    let origin = Origin::File(path.to_path_buf());
    let mut options = std::fs::OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_CLOEXEC | libc::O_NONBLOCK);
    }
    let mut file = match options.open(path) {
        Ok(f) => f,
        Err(e) => return Input::Unreadable(origin, io_reason(&e)),
    };
    match file.metadata() {
        Ok(m) if m.is_file() => {}
        Ok(_) => return Input::Unreadable(origin, "not a regular file".into()),
        Err(e) => return Input::Unreadable(origin, io_reason(&e)),
    }
    match read_bounded(&mut file) {
        Ok(Some((buf, n))) => parse_key(&buf[..n], origin),
        Ok(None) => Input::Malformed(origin),
        Err(e) => Input::Unreadable(origin, io_reason(&e)),
    }
}

/// The automatic key file: never through a symbolic link, only a regular file
/// owned by this user and closed to everyone else. All checks are on the one
/// descriptor that is read.
fn read_auto(path: &Path) -> Input {
    let origin = Origin::Auto(path.to_path_buf());
    let unsafe_because = |why: String| Input::Unsafe(path.to_path_buf(), why);
    #[cfg(unix)]
    let mut file = {
        use std::os::unix::fs::OpenOptionsExt;
        match std::fs::OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC | libc::O_NONBLOCK)
            .open(path)
        {
            Ok(f) => f,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                return Input::Absent {
                    auto: Some(path.to_path_buf()),
                };
            }
            Err(e) if e.raw_os_error() == Some(libc::ELOOP) => {
                return unsafe_because("it is a symbolic link".into());
            }
            Err(e) => return Input::Unreadable(origin, io_reason(&e)),
        }
    };
    #[cfg(not(unix))]
    let mut file = {
        match std::fs::symlink_metadata(path) {
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                return Input::Absent {
                    auto: Some(path.to_path_buf()),
                };
            }
            Err(e) => return Input::Unreadable(origin, io_reason(&e)),
            Ok(m) if m.file_type().is_symlink() => {
                return unsafe_because("it is a symbolic link".into());
            }
            Ok(_) => {}
        }
        match std::fs::File::open(path) {
            Ok(f) => f,
            Err(e) => return Input::Unreadable(origin, io_reason(&e)),
        }
    };
    let meta = match file.metadata() {
        Ok(m) => m,
        Err(e) => return Input::Unreadable(origin, io_reason(&e)),
    };
    if !meta.is_file() {
        return unsafe_because("it is not a regular file".into());
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        // SAFETY: geteuid has no preconditions and cannot fail.
        let me = unsafe { libc::geteuid() };
        if meta.uid() != me {
            return unsafe_because(format!("it belongs to another user (uid {})", meta.uid()));
        }
        let mode = meta.mode() & 0o777;
        if mode & 0o077 != 0 {
            return unsafe_because(format!(
                "other users can read it (mode {mode:o}); restrict it with chmod 600"
            ));
        }
    }
    match read_bounded(&mut file) {
        Ok(Some((buf, n))) => parse_key(&buf[..n], origin),
        Ok(None) => Input::Malformed(origin),
        Err(e) => Input::Unreadable(origin, io_reason(&e)),
    }
}

/// Removes the file it names when dropped: a creator's own temporary file.
struct OwnTemp(PathBuf);

impl Drop for OwnTemp {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}

/// Creates the automatic key file: a new key written to a temporary file of
/// this process, owner-only, then linked into place without replacing one that
/// exists; the file in place is the key, whoever made it. The temporary file is
/// removed on every path but a crash. `before_link` runs just before the link.
/// Answers the key in place, and whether this call put it there.
fn create_auto_key(
    path: &Path,
    before_link: impl FnOnce() -> std::io::Result<()>,
) -> Result<(MasterKey, bool)> {
    let dir = path.parent().unwrap_or(Path::new("."));
    std::fs::create_dir_all(dir).with_context(|| format!("creating {}", dir.display()))?;
    let mut tail = [0u8; 8];
    getrandom::fill(&mut tail).map_err(|e| anyhow!("no random bytes for a file name: {e}"))?;
    let temp = dir.join(format!(".{AUTO_KEY_FILE}.{}.tmp", hex::encode(tail)));
    let key = MasterKey::generate()?;
    let created;
    {
        let mut options = std::fs::OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options
                .mode(0o600)
                .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC);
        }
        let mut file = options
            .open(&temp)
            .with_context(|| format!("creating {}", temp.display()))?;
        let _own = OwnTemp(temp.clone());
        use std::io::Write;
        file.write_all(&key.to_line()[..])
            .and_then(|()| file.sync_all())
            .with_context(|| format!("writing {}", temp.display()))?;
        before_link().with_context(|| format!("writing {}", temp.display()))?;
        created = match std::fs::hard_link(&temp, path) {
            Ok(()) => true,
            // Another instance made it first: its file is the key.
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => false,
            Err(e) => return Err(e).with_context(|| format!("creating {}", path.display())),
        };
    }
    sync_dir(dir);
    match read_auto(path) {
        Input::Usable(k, _) => Ok((k, created)),
        Input::Unsafe(path, why) => bail!(unsafe_text(&path, &why, "none")),
        other @ (Input::Malformed(_) | Input::Unreadable(..)) => {
            bail!(unusable_text(&other, false, "none"))
        }
        Input::Absent { .. } => bail!(
            "secret store: {} is gone right after it was created",
            path.display()
        ),
    }
}

fn sync_dir(dir: &Path) {
    #[cfg(unix)]
    if let Ok(d) = std::fs::File::open(dir) {
        let _ = d.sync_all();
    }
    #[cfg(not(unix))]
    let _ = dir;
}

/// Who this instance's registrations of sources for secrets belong to:
/// `HIE_INSTANCE_ID`, else the id kept in the sources directory, made once.
/// Unlike the per-process lease name, it stays the same across restarts, so
/// a restart finds and cleans what an earlier run of this instance left.
pub(crate) fn owner_id(env: Option<String>, sources_dir: &Path) -> String {
    if let Some(id) = env.map(|v| v.trim().to_string()).filter(|v| !v.is_empty()) {
        return id;
    }
    match stable_id(sources_dir) {
        Ok(id) => id,
        Err(e) => {
            tracing::warn!(
                "secret references: set HIE_INSTANCE_ID, or make {} writable; without a stable id, references left by an earlier run are not cleaned ({e:#})",
                sources_dir.display()
            );
            crate::pipeline::instance_id().to_string()
        }
    }
}

fn stable_id(dir: &Path) -> Result<String> {
    let path = dir.join(INSTANCE_ID_FILE);
    let read = |path: &Path| -> Result<Option<String>> {
        match std::fs::read_to_string(path) {
            Ok(text) => {
                let id = text.trim().to_string();
                if id.is_empty() || id.len() > 64 || id.chars().any(char::is_whitespace) {
                    bail!("{} does not hold an instance id", path.display());
                }
                Ok(Some(id))
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e).with_context(|| format!("reading {}", path.display())),
        }
    };
    if let Some(id) = read(&path)? {
        return Ok(id);
    }
    std::fs::create_dir_all(dir).with_context(|| format!("creating {}", dir.display()))?;
    let mut bytes = [0u8; 8];
    getrandom::fill(&mut bytes).map_err(|e| anyhow!("no random bytes for an id: {e}"))?;
    let id = hex::encode(bytes);
    let temp = dir.join(format!("{INSTANCE_ID_FILE}.{id}.tmp"));
    {
        let _own = OwnTemp(temp.clone());
        std::fs::write(&temp, format!("{id}\n"))
            .with_context(|| format!("writing {}", temp.display()))?;
        match std::fs::hard_link(&temp, &path) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {}
            Err(e) => return Err(e).with_context(|| format!("creating {}", path.display())),
        }
    }
    read(&path)?.with_context(|| format!("{} is gone right after it was made", path.display()))
}

#[cfg(test)]
mod tests;
