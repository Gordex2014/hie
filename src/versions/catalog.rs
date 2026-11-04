//! Creating, saving, renaming and deleting transforms. The writes run inside
//! the caller's transaction (so an idempotent answer commits with them); the
//! environment is determined before, since it may need the runtime.

use serde_json::{Value, json};

use super::{Deleted, Environment, NewVersion, Saved, Versions, sha256_hex};
use crate::config::is_transform_id;
use crate::db::Tx;
use crate::manager::{ErrorStatus, ManagerError};

/// The most code a version holds.
pub const MAX_CODE_BYTES: usize = 256 * 1024;
const MAX_NOTE_CHARS: usize = 1000;
const MAX_AUTHOR_CHARS: usize = 200;
const MAX_NAME_CHARS: usize = 200;
/// Ids the routes under `/api/transforms` keep for themselves.
const RESERVED: &[&str] = &["try", "try-runs"];

/// What a save brings: the code, and the optional note and author.
#[derive(Debug, Clone, Default)]
pub struct Draft {
    pub code: String,
    pub note: Option<String>,
    pub author: Option<String>,
}

impl Draft {
    fn check(&self) -> Result<(), ManagerError> {
        if self.code.len() > MAX_CODE_BYTES {
            return Err(ManagerError::new(
                ErrorStatus::TooLarge,
                "code_too_large",
                format!("code is at most {MAX_CODE_BYTES} bytes"),
            ));
        }
        if self
            .note
            .as_ref()
            .is_some_and(|n| n.chars().count() > MAX_NOTE_CHARS)
        {
            return Err(invalid(
                "note",
                format!("note is at most {MAX_NOTE_CHARS} characters"),
            ));
        }
        if self
            .author
            .as_ref()
            .is_some_and(|a| a.chars().count() > MAX_AUTHOR_CHARS)
        {
            return Err(invalid(
                "author",
                format!("author is at most {MAX_AUTHOR_CHARS} characters"),
            ));
        }
        Ok(())
    }

    fn version<'a>(&'a self, sha: &'a str, environment: &'a Environment) -> NewVersion<'a> {
        NewVersion {
            code: &self.code,
            code_sha256: sha,
            note: self.note.as_deref(),
            author: self.author.as_deref(),
            environment,
        }
    }
}

fn invalid(field: &str, message: String) -> ManagerError {
    ManagerError::invalid("invalid_body", message).with("field", field)
}

fn not_found(id: &str) -> ManagerError {
    ManagerError::not_found("transform_not_found", format!("transform {id} not found"))
}

fn name_in_use(name: &str) -> ManagerError {
    ManagerError::conflict(
        "name_in_use",
        format!("another transform is named {name:?}"),
    )
}

/// A display name as kept, and its key (compared without case).
fn display_name(name: &str) -> Result<(String, String), ManagerError> {
    let name = name.trim();
    if name.is_empty() || name.chars().count() > MAX_NAME_CHARS {
        return Err(invalid(
            "display_name",
            format!("display_name is 1 to {MAX_NAME_CHARS} characters"),
        ));
    }
    Ok((name.to_string(), name.to_lowercase()))
}

/// An id from a display name: lower-case letters and digits, other runs as
/// one dash, within the id rules.
fn slug(name: &str) -> String {
    let mut out = String::new();
    for c in name.chars().flat_map(char::to_lowercase) {
        if c.is_ascii_lowercase() || c.is_ascii_digit() {
            out.push(c);
        } else if !out.ends_with('-') && !out.is_empty() {
            out.push('-');
        }
    }
    let mut out: String = out.trim_end_matches('-').chars().take(56).collect();
    while out.ends_with('-') {
        out.pop();
    }
    if out.is_empty() {
        out = "transform".into();
    }
    out
}

/// Whether `id` may name a new transform (deleted ones keep theirs).
pub(super) fn id_free(tx: &mut Tx<'_>, id: &str) -> anyhow::Result<bool> {
    Ok(is_transform_id(id) && !RESERVED.contains(&id) && tx.transform(id)?.is_none())
}

/// Whether `name` is a 0.1 transform name: `[A-Za-z0-9_-]{1,64}`, not a
/// reserved one.
pub fn is_legacy_name(name: &str) -> bool {
    (1..=64).contains(&name.len())
        && name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
        && !RESERVED.contains(&name)
}

/// The id a name HIE did not choose (a 0.1 name, a file's stem) starts from:
/// lower-cased, other characters as `_`, within the id rules.
pub(super) fn id_base(name: &str) -> String {
    let mut out: String = name
        .chars()
        .map(|c| {
            let c = c.to_ascii_lowercase();
            if c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-' || c == '_' {
                c
            } else {
                '_'
            }
        })
        .take(64)
        .collect();
    if !out.starts_with(|c: char| c.is_ascii_lowercase() || c.is_ascii_digit()) {
        out = format!("t{}", out.chars().take(63).collect::<String>());
    }
    out
}

/// A free id for a name HIE did not choose (a 0.1 name, a file's stem),
/// owned by `owner` (that name, or the file's path): the name itself when it
/// is a valid id and free; otherwise the name lower-cased ([`id_base`]) with 8
/// hex of `owner` (then a counter). A name that is not an id itself (upper
/// case, other characters) never gets the bare lower-cased id, which another
/// 0.1 name could be. Inside the transaction that inserts it; an insert that
/// loses a race is retried by the caller.
pub(super) fn allocate_id(tx: &mut Tx<'_>, name: &str, owner: &str) -> anyhow::Result<String> {
    if is_transform_id(name) && id_free(tx, name)? {
        return Ok(name.to_string());
    }
    let base = id_base(name);
    let stem: String = base.chars().take(54).collect();
    let hash = &sha256_hex(owner)[..8];
    for n in 1..1000 {
        let id = if n == 1 {
            format!("{stem}-{hash}")
        } else {
            format!("{stem}-{hash}-{n}")
        };
        if id_free(tx, &id)? {
            return Ok(id);
        }
    }
    anyhow::bail!("no free id for {base:?}")
}

/// A display name for a transform HIE names itself (a file, a 0.1 save):
/// `base`, or `base (2)`, ... when that is taken.
pub(super) fn free_display_name(tx: &mut Tx<'_>, base: &str) -> anyhow::Result<(String, String)> {
    let mut n = 1;
    loop {
        let name = if n == 1 {
            base.to_string()
        } else {
            format!("{base} ({n})")
        };
        let key = name.to_lowercase();
        if !tx.display_name_taken(&key, None)? {
            return Ok((name, key));
        }
        n += 1;
    }
}

impl Versions {
    /// `POST /api/transforms`: the transform and its version 1, together or
    /// not at all. Answers the `201` body.
    pub fn create_in(
        &self,
        tx: &mut Tx<'_>,
        name: &str,
        draft: &Draft,
        environment: &Environment,
    ) -> Result<Value, ManagerError> {
        draft.check()?;
        let (name, key) = display_name(name)?;
        if tx.display_name_taken(&key, None)? {
            return Err(name_in_use(&name));
        }
        let sha = sha256_hex(&draft.code);
        let base = slug(&name);
        for n in 1..1000 {
            let id = if n == 1 {
                base.clone()
            } else {
                format!("{base}-{n}")
            };
            if !id_free(tx, &id)? {
                continue;
            }
            match tx.insert_transform(
                &id,
                &name,
                &key,
                &draft.version(&sha, environment),
                None,
                None,
            )? {
                Some((t, v)) => {
                    let mut body = self.transform_json(&t);
                    body["version"] = v.to_json();
                    return Ok(body);
                }
                None if tx.display_name_taken(&key, None)? => return Err(name_in_use(&name)),
                None => continue,
            }
        }
        Err(anyhow::anyhow!("no free id for transform {name:?}").into())
    }

    /// `POST /api/transforms/{id}/versions`: a new version on `base_version`,
    /// `409 version_conflict` (with `current_version`) when another save got
    /// there first. Answers the version object.
    pub fn save_in(
        &self,
        tx: &mut Tx<'_>,
        id: &str,
        base_version: i64,
        draft: &Draft,
        environment: &Environment,
    ) -> Result<Value, ManagerError> {
        draft.check()?;
        let sha = sha256_hex(&draft.code);
        match tx.save_version(id, Some(base_version), &draft.version(&sha, environment))? {
            Saved::Version(v) => Ok(v.to_json()),
            Saved::Conflict { current_version } => Err(ManagerError::conflict(
                "version_conflict",
                format!("version {base_version} is not the current version of {id}"),
            )
            .with("current_version", current_version)),
            Saved::NotFound => Err(not_found(id)),
        }
    }

    /// `PATCH /api/transforms/{id}`: a new display name; the id and the
    /// versions stay. Answers the transform.
    pub fn rename_in(&self, tx: &mut Tx<'_>, id: &str, name: &str) -> Result<Value, ManagerError> {
        let (name, key) = display_name(name)?;
        if tx.transform(id)?.filter(|t| !t.deleted).is_none() {
            return Err(not_found(id));
        }
        if !tx.rename_transform(id, &name, &key)? {
            return Err(name_in_use(&name));
        }
        let t = tx.transform(id)?.ok_or_else(|| not_found(id))?;
        Ok(self.transform_json(&t))
    }

    /// `DELETE /api/transforms/{id}`: refused while a source pins or follows
    /// it (a pin in the store, or one of `sources`, the definitions the
    /// manager holds) or a load or retry running one of its versions is
    /// unfinished; checked and done in one transaction under the transform's
    /// row lock, which whatever starts to refer to it also takes. Its
    /// versions stay for the usage history; its file goes.
    pub fn delete(&self, id: &str, sources: &[String]) -> Result<(), ManagerError> {
        let t = self.transform(id)?.ok_or_else(|| not_found(id))?;
        match self
            .db
            .transaction(|tx| tx.delete_transform_unused(id, sources))?
        {
            Deleted::Done => {
                self.remove_file(&t);
                Ok(())
            }
            Deleted::NotFound => Err(not_found(id)),
            Deleted::InUse(users) => Err(ManagerError::conflict(
                "transform_in_use",
                format!("transform {id} is in use"),
            )
            .with("sources", json!(users))),
        }
    }

    /// The live transform a 0.1 name is: the transform of that id, else the
    /// one that has it as its 0.1 name.
    pub fn resolve_name(&self, name: &str) -> anyhow::Result<Option<String>> {
        if let Some(t) = self.transform(name)? {
            return Ok(Some(t.id));
        }
        Ok(self
            .db
            .transform_by_legacy_name(name)?
            .filter(|t| !t.deleted)
            .map(|t| t.id))
    }

    /// `PUT /api/transforms/{name}` (0.1): a new version on the current one
    /// when the code differs, nothing when it is the same, the transform and
    /// its version 1 when the name is new. A 0.1 name is
    /// `[A-Za-z0-9_-]{1,64}`; a new one gets a lower-case id of its own
    /// (collision-safe, [`allocate_id`]) and keeps the name as given for 0.1
    /// answers and its file. `(created, version)`.
    pub async fn save_legacy(&self, name: &str, code: &str) -> Result<(bool, i64), ManagerError> {
        if !is_legacy_name(name) {
            return Err(ManagerError::invalid(
                "invalid_body",
                format!(
                    "transform name {name:?} must be 1 to 64 letters, digits, underscores and dashes"
                ),
            ));
        }
        let draft = Draft {
            code: code.to_string(),
            ..Draft::default()
        };
        draft.check()?;
        let sha = sha256_hex(code);
        if let Some(id) = self.resolve_name(name)?
            && let Some(t) = self.transform(&id)?
            && self
                .db
                .version_info(&id, t.current_version)?
                .is_some_and(|v| v.code_sha256 == sha)
        {
            return Ok((false, t.current_version));
        }
        let environment = self.environment_for(None).await?;
        let content = draft.version(&sha, &environment);
        for _ in 0..5 {
            let saved =
                self.db
                    .transaction(|tx| -> Result<Option<(String, bool, i64)>, ManagerError> {
                        tx.begin_write()?;
                        let existing = match tx.transform(name)?.filter(|t| !t.deleted) {
                            Some(t) => Some(t),
                            None => tx.transform_by_legacy_name(name)?,
                        };
                        match existing {
                            None => {
                                let id = allocate_id(tx, name, name)?;
                                let (display, key) = free_display_name(tx, name)?;
                                Ok(tx
                                    .insert_transform(
                                        &id,
                                        &display,
                                        &key,
                                        &content,
                                        None,
                                        Some(name),
                                    )?
                                    .map(|(_, v)| (id, true, v.version)))
                            }
                            Some(t) if t.deleted => {
                                let (display, key) = free_display_name(tx, name)?;
                                let v = tx
                                    .revive_transform(&t.id, &display, &key, &content)?
                                    .ok_or_else(|| not_found(name))?;
                                Ok(Some((t.id, false, v.version)))
                            }
                            Some(t) => match tx.save_version(&t.id, None, &content)? {
                                Saved::Version(v) => Ok(Some((t.id, false, v.version))),
                                Saved::Conflict { current_version } => Err(ManagerError::conflict(
                                    "version_conflict",
                                    format!("transform {name} was saved meanwhile"),
                                )
                                .with("current_version", current_version)),
                                Saved::NotFound => Err(not_found(name)),
                            },
                        }
                    })?;
            if let Some((id, created, version)) = saved {
                self.saved(&id);
                return Ok((created, version));
            }
        }
        Err(ManagerError::conflict(
            "name_in_use",
            format!("transform {name} was created meanwhile"),
        ))
    }

    /// A save committed: the transform's file follows its current version.
    pub fn saved(&self, id: &str) {
        if let Err(e) = self.write_file(id) {
            tracing::warn!(target: "hie::versions", transform = id, "writing the transform's file: {e:#}");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::slug;

    #[test]
    fn ids_come_from_display_names() {
        assert_eq!(slug("Lab results"), "lab-results");
        assert_eq!(slug("  Glucose / HbA1c  "), "glucose-hba1c");
        assert_eq!(slug("ÁÉ"), "transform");
        assert!(slug(&"x".repeat(100)).len() <= 56);
    }

    #[test]
    fn names_hie_did_not_choose_become_ids() {
        assert_eq!(super::id_base("Lab_Results"), "lab_results");
        assert_eq!(super::id_base("a.b c"), "a_b_c");
        assert_eq!(super::id_base("_x"), "t_x");
        assert!(super::is_legacy_name("Lab-1"));
        assert!(!super::is_legacy_name("try"));
        assert!(!super::is_legacy_name("bad name"));
    }
}
