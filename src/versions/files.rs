//! Transform files. A file under `transforms/` found at start, or a file a
//! 0.1 reference (`transform: transforms/x.py`) names, is a transform. A file
//! is identified by its canonical path, which one transform owns (a unique
//! column): its id is its stem lower-cased, or, when that is another
//! transform's, the stem and 8 hex of its path; the id is chosen and the path
//! bound in the transaction that creates the transform, so two files never
//! share one. Under `transforms/` the stem is also its 0.1 name, as written.
//! When a file's content changes from what HIE last wrote or read there, it is
//! saved as a new version with `author: "file"`. A transform's file under
//! `transforms/` (`{0.1 name or id}.py`) follows its current version; a file
//! elsewhere is only read.

use std::path::{Path, PathBuf};

use anyhow::{Context, Result, anyhow};

use super::catalog::{allocate_id, free_display_name, is_legacy_name};
use super::{NewVersion, Saved, TransformInfo, Versions, sha256_hex};
use crate::db::Tx;

const FILE_AUTHOR: &str = "file";
/// How often an import that lost a race to another one is tried again.
const ATTEMPTS: usize = 5;

impl Versions {
    /// Imports every transform file under `transforms/`, once the
    /// environment they are saved in is known.
    pub(super) fn import_dir(&self) {
        if self.known_default_environment().is_none() {
            return;
        }
        self.imported
            .store(true, std::sync::atomic::Ordering::Release);
        let Ok(entries) = std::fs::read_dir(&self.transforms_dir) else {
            return;
        };
        let mut paths: Vec<PathBuf> = entries
            .filter_map(|e| e.ok().map(|e| e.path()))
            .filter(|p| p.is_file() && p.extension().and_then(|e| e.to_str()) == Some("py"))
            .collect();
        paths.sort();
        for path in paths {
            if let Err(e) = self.import_file(&path) {
                tracing::warn!(target: "hie::versions", file = %path.display(), "transform file not imported: {e:#}");
            }
        }
    }

    /// The transform a file is, saved first when the file is new to HIE or
    /// changed since HIE last wrote or read it. Its id.
    pub(super) fn import_file(&self, path: &Path) -> Result<String> {
        let canonical = path
            .canonicalize()
            .with_context(|| format!("transform {} not found", path.display()))?;
        let key = canonical.to_string_lossy().to_string();
        let code = std::fs::read_to_string(&canonical)
            .with_context(|| format!("reading {}", canonical.display()))?;
        let sha = sha256_hex(&code);
        if let Some(t) = self.db.transform_by_file(&key)?
            && t.file_sha256.as_deref() == Some(sha.as_str())
        {
            return Ok(t.id);
        }
        let environment = self
            .known_default_environment()
            .ok_or_else(|| anyhow!("the runtime's default environment is not known yet"))?;
        let stem = canonical
            .file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or("transform")
            .to_string();
        let in_dir = self
            .transforms_dir
            .canonicalize()
            .is_ok_and(|dir| canonical.parent() == Some(dir.as_path()));
        let legacy = (in_dir && is_legacy_name(&stem)).then_some(stem.as_str());
        let content = NewVersion {
            code: &code,
            code_sha256: &sha,
            note: None,
            author: Some(FILE_AUTHOR),
            environment: &environment,
        };
        for _ in 0..ATTEMPTS {
            let imported = self
                .db
                .transaction(|tx| import_in(tx, &key, &sha, &stem, legacy, &content))?;
            if let Some(id) = imported {
                return Ok(id);
            }
        }
        anyhow::bail!("transform file {key} kept losing to another import")
    }

    /// Where a transform's own file under `transforms/` is: `{0.1 name or id}.py`.
    fn own_file(&self, t: &TransformInfo) -> Result<PathBuf> {
        std::fs::create_dir_all(&self.transforms_dir)?;
        Ok(self
            .transforms_dir
            .canonicalize()?
            .join(format!("{}.py", t.name())))
    }

    /// Writes the transform's current version to its own file under
    /// `transforms/` (a transform whose file is elsewhere keeps that file).
    /// Never through a link: a destination that is a symlink, or anything
    /// but a regular file, is left alone. The path is reserved for the
    /// transform in the store before anything is written (refused when
    /// another transform owns it), and the code is published by writing a
    /// new file in the same directory and renaming it over the destination,
    /// which replaces a directory entry and never follows one.
    pub(super) fn write_file(&self, id: &str) -> Result<()> {
        let Some(t) = self.transform(id)? else {
            return Ok(());
        };
        let own = self.own_file(&t)?;
        let own_key = own.to_string_lossy().to_string();
        if t.file_path.as_deref().is_some_and(|p| p != own_key) {
            return Ok(());
        }
        let on_disk = match std::fs::symlink_metadata(&own) {
            Ok(meta) if !meta.file_type().is_file() => {
                tracing::warn!(target: "hie::versions", transform = id, file = %own.display(),
                    "the transform's file is a symlink or not a regular file; not written");
                return Ok(());
            }
            Ok(_) => Some(crate::formats::sha256_hex(&std::fs::read(&own)?)),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => None,
            Err(e) => return Err(e.into()),
        };
        let code = self
            .db
            .version_code(id, t.current_version)?
            .context("the current version's code")?;
        let sha = sha256_hex(&code);
        // Reserved first, keeping the SHA-256 of what is there now: a write
        // that fails leaves the file as the store knows it.
        if !self
            .db
            .reserve_transform_file(id, &own_key, on_disk.as_deref())?
        {
            tracing::warn!(target: "hie::versions", transform = id, file = %own.display(),
                "the transform's file belongs to another transform; not written");
            return Ok(());
        }
        if on_disk.as_deref() != Some(sha.as_str()) {
            publish(&own, &code)?;
        }
        self.db.set_transform_file(id, &own_key, &sha)?;
        Ok(())
    }

    /// A deleted transform's own file under `transforms/` goes with it, so it
    /// is not found again at the next start.
    pub(super) fn remove_file(&self, t: &TransformInfo) {
        let Ok(own) = self.own_file(t) else {
            return;
        };
        if t.file_path.as_deref() == Some(own.to_string_lossy().as_ref())
            && let Err(e) = std::fs::remove_file(&own)
            && e.kind() != std::io::ErrorKind::NotFound
        {
            tracing::warn!(target: "hie::versions", file = %own.display(), "removing a deleted transform's file: {e}");
        }
    }
}

/// Writes `code` to a new file beside `dest` (created exclusively, so never
/// through an existing entry) and renames it over `dest`.
fn publish(dest: &Path, code: &str) -> Result<()> {
    use std::io::Write;
    let name = dest
        .file_name()
        .and_then(|n| n.to_str())
        .context("a transform file name")?;
    let temp = dest.with_file_name(format!(".{name}.{:016x}.tmp", rand::random::<u64>()));
    let written = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&temp)
        .and_then(|mut f| f.write_all(code.as_bytes()).and_then(|()| f.sync_all()))
        .and_then(|()| std::fs::rename(&temp, dest));
    if let Err(e) = written {
        let _ = std::fs::remove_file(&temp);
        return Err(anyhow::Error::from(e).context(format!("writing {}", dest.display())));
    }
    Ok(())
}

/// One import attempt in one transaction: the file's transform found (by its
/// path, or by its 0.1 name when a 0.1 save made it without a file) and
/// brought up to date, or created with a fresh id bound to the path. `None`
/// when a concurrent import won the id, the path or the name: try again.
fn import_in(
    tx: &mut Tx<'_>,
    key: &str,
    sha: &str,
    stem: &str,
    legacy: Option<&str>,
    content: &NewVersion<'_>,
) -> Result<Option<String>> {
    tx.begin_write()?;
    let mut found = tx.transform_by_file(key)?;
    if found.is_none()
        && let Some(name) = legacy
        && let Some(t) = tx.transform_by_legacy_name(name)?
        && t.file_path.is_none()
    {
        if !tx.bind_transform_file(&t.id, key, "")? {
            return Ok(None);
        }
        found = Some(t);
    }
    let Some(t) = found else {
        let id = allocate_id(tx, stem, key)?;
        let (display, display_key) = free_display_name(tx, stem)?;
        let name = match legacy {
            Some(name) if tx.transform_by_legacy_name(name)?.is_none() => Some(name),
            _ => None,
        };
        return Ok(tx
            .insert_transform(&id, &display, &display_key, content, Some((key, sha)), name)?
            .map(|(t, _)| t.id));
    };
    if t.deleted {
        let (display, display_key) = free_display_name(tx, t.name())?;
        tx.revive_transform(&t.id, &display, &display_key, content)?;
    } else if tx.version_sha(&t.id, t.current_version)?.as_deref() != Some(sha) {
        match tx.save_version(&t.id, None, content)? {
            Saved::Version(v) => tracing::info!(target: "hie::versions",
                transform = %t.id, version = v.version, "transform file changed: saved as a new version"),
            Saved::Conflict { .. } | Saved::NotFound => return Ok(None),
        }
    }
    if !tx.bind_transform_file(&t.id, key, sha)? {
        return Ok(None);
    }
    Ok(Some(t.id))
}
