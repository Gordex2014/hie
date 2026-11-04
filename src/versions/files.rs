//! Transform files. A file under `transforms/` found at start, or a file a
//! 0.1 reference (`transform: transforms/x.py`) names, is a transform: its id
//! is its stem, unless another file already holds that id, when it gets
//! `{stem}-{8 hex of its path}` (a file is identified by its canonical path,
//! so two files never share a transform). When a file's content changes from
//! what HIE last wrote or read there, it is saved as a new version with
//! `author: "file"`. A transform's file under `transforms/` follows its
//! current version (it is what 0.1 clients and definitions read); a file
//! elsewhere is only read.

use std::path::{Path, PathBuf};

use anyhow::{Context, Result, anyhow};

use super::catalog::{free_display_name, id_free};
use super::{NewVersion, Saved, TransformInfo, Versions, sha256_hex};
use crate::config::is_transform_id;

const FILE_AUTHOR: &str = "file";

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
        let existing = self.db.transform_by_file(&key)?;
        if let Some(t) = &existing
            && t.file_sha256.as_deref() == Some(sha.as_str())
        {
            return Ok(t.id.clone());
        }
        let environment = self
            .known_default_environment()
            .ok_or_else(|| anyhow!("the runtime's default environment is not known yet"))?;
        let id = match existing {
            Some(t) => t.id,
            None => self.file_id(&canonical)?,
        };
        let content = NewVersion {
            code: &code,
            code_sha256: &sha,
            note: None,
            author: Some(FILE_AUTHOR),
            environment: &environment,
        };
        self.db.transaction(|tx| -> Result<()> {
            match tx.transform(&id)? {
                None => {
                    let (display, display_key) = free_display_name(tx, &id)?;
                    tx.insert_transform(&id, &display, &display_key, &content)?
                        .ok_or_else(|| anyhow!("transform {id} was created meanwhile"))?;
                }
                Some(t) if t.deleted => {
                    let (display, display_key) = free_display_name(tx, &id)?;
                    tx.revive_transform(&id, &display, &display_key, &content)?;
                }
                Some(t) => {
                    let current = tx_current_sha(tx, &id, t.current_version)?;
                    if current.as_deref() != Some(sha.as_str()) {
                        match tx.save_version(&id, None, &content)? {
                            Saved::Version(v) => tracing::info!(target: "hie::versions",
                                transform = %id, version = v.version, "transform file changed: saved as a new version"),
                            Saved::Conflict { .. } | Saved::NotFound => {
                                anyhow::bail!("transform {id} changed meanwhile")
                            }
                        }
                    }
                }
            }
            Ok(())
        })?;
        self.db.set_transform_file(&id, &key, &sha)?;
        Ok(id)
    }

    /// The id a file new to HIE gets: its stem, or the stem and 8 hex of its
    /// path when the stem is another transform's.
    fn file_id(&self, canonical: &Path) -> Result<String> {
        let stem: String = canonical
            .file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or("transform")
            .chars()
            .map(|c| {
                let c = c.to_ascii_lowercase();
                if c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-' || c == '_' {
                    c
                } else {
                    '_'
                }
            })
            .collect();
        let stem = if is_transform_id(&stem) {
            stem
        } else {
            format!("t{}", stem.chars().take(63).collect::<String>())
        };
        let free = self.db.transaction(|tx| id_free(tx, &stem))?;
        if free {
            return Ok(stem);
        }
        let path_sha = sha256_hex(&canonical.to_string_lossy());
        let id = format!(
            "{}-{}",
            stem.chars().take(55).collect::<String>(),
            &path_sha[..8]
        );
        tracing::warn!(target: "hie::versions", file = %canonical.display(),
            "transform id {stem} belongs to another file; this file is transform {id}");
        Ok(id)
    }

    /// Where a transform's file under `transforms/` is.
    fn own_file(&self, id: &str) -> Result<PathBuf> {
        std::fs::create_dir_all(&self.transforms_dir)?;
        Ok(self.transforms_dir.canonicalize()?.join(format!("{id}.py")))
    }

    /// Writes the transform's current version to its file under
    /// `transforms/` (a transform whose file is elsewhere keeps that file).
    pub(super) fn write_file(&self, id: &str) -> Result<()> {
        let Some(t) = self.transform(id)? else {
            return Ok(());
        };
        let own = self.own_file(id)?;
        let own_key = own.to_string_lossy().to_string();
        if t.file_path.as_deref().is_some_and(|p| p != own_key) {
            return Ok(());
        }
        let code = self
            .db
            .version_code(id, t.current_version)?
            .context("the current version's code")?;
        let sha = sha256_hex(&code);
        let same = std::fs::read_to_string(&own).is_ok_and(|on_disk| on_disk == code);
        if !same {
            std::fs::write(&own, &code).with_context(|| format!("writing {}", own.display()))?;
        }
        self.db.set_transform_file(id, &own_key, &sha)
    }

    /// A deleted transform's file under `transforms/` goes with it, so it is
    /// not found again at the next start.
    pub(super) fn remove_file(&self, t: &TransformInfo) {
        let Ok(own) = self.own_file(&t.id) else {
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

fn tx_current_sha(tx: &mut crate::db::Tx<'_>, id: &str, version: i64) -> Result<Option<String>> {
    Ok(tx
        .query_json(
            "SELECT code_sha256 FROM transform_versions WHERE transform_id = $1 AND version = $2",
            &crate::db::params![id, version],
        )?
        .first()
        .and_then(|r| r["code_sha256"].as_str().map(str::to_string)))
}
