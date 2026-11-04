//! Taking objects from the source: listing, fetching, recognising duplicates,
//! unpacking zips, and storing every record before the object is acknowledged.

use anyhow::{Context, Result};

use crate::connectors::{self, Incoming, Listed};
use crate::formats;
use crate::versions::VersionRef;

use super::FeedRunner;

impl FeedRunner {
    pub(super) async fn scan(&self) -> Result<()> {
        let read_only = self.source.is_read_only();
        for Listed { key, name, size } in self.source.list_meta().await? {
            // Someone else's bucket keeps what we took: known by name and size,
            // it is not downloaded again just to recognise its hash.
            if read_only && size > 0 && self.db.has_file(&self.feed.id, &name, size)? {
                continue;
            }
            let incoming = match self.source.fetch(&key).await {
                Ok(incoming) => incoming,
                // Announced, then gone before this pass reached it: nothing to take.
                Err(e) if connectors::is_gone(&e) => {
                    tracing::warn!(target: "hie::pipeline", feed = %self.feed.id, key, "object no longer there, skipped");
                    continue;
                }
                Err(e) => return Err(e),
            };
            self.ingest(incoming).await?;
        }
        // Notifications are acknowledged only once what they announced is in the
        // store: a pass that failed before this leaves them to be delivered again.
        self.source.settle_notifications().await?;
        Ok(())
    }

    pub(super) async fn ingest(&self, incoming: Incoming) -> Result<()> {
        let feed = &self.feed.id;
        let Incoming { key, name, bytes } = incoming;
        // A load that runs a transform is admitted only with its version fixed:
        // when none can be fixed now, nothing is written and the object stays
        // at its source for the next pass. The one snapshot serves every load
        // the object makes (each entry of a zip).
        let version = if self.bulk.is_some() && !self.bulk_transformed {
            None
        } else {
            Some(self.versions.resolve_for_source(&self.feed).map_err(|e| {
                e.context(format!(
                    "{name}: no transform version can be fixed for a load yet"
                ))
            })?)
        };
        let sha = formats::sha256_hex(&bytes);

        if let Some(original) = self.db.find_file_by_hash(feed, &sha)? {
            if self.source.is_read_only() {
                // Someone else's bucket: the object stays, and was already taken.
                return Ok(());
            }
            if self.bulk.is_some() && self.db.file_source(original)?.is_some() {
                // The same object, still waiting for HFS to fetch it: leave it alone.
                return Ok(());
            }
            let id = self.db.insert_file(
                feed,
                &name,
                &sha,
                bytes.len() as u64,
                "duplicate",
                Some(original),
                None,
            )?;
            tracing::info!(target: "hie::pipeline", feed, file = %name, duplicate_of = original, "same content already received; not reprocessed");
            let archived = self
                .source
                .acknowledge(&key, &format!("{id}-{name}"))
                .await?;
            return self.db.mark_file_archived(id, &archived);
        }

        if formats::zip::is_container(&name) {
            return self
                .unpack(&key, &name, &sha, bytes, version.as_ref())
                .await;
        }

        // On the bulk path the object stays at its source until HFS has read it.
        let keep = self.bulk.is_some() && !self.bulk_transformed;
        let file_id = self.open_load(
            &name,
            &sha,
            bytes.len() as u64,
            keep.then_some(key.as_str()),
            version.as_ref(),
        )?;
        let readable = self.admit(file_id, &name, &bytes)?;
        if keep && readable {
            return Ok(());
        }
        let archived = self
            .source
            .acknowledge(&key, &format!("{file_id}-{name}"))
            .await?;
        self.db.mark_file_archived(file_id, &archived)
    }

    /// Starts the load of an object this pass took (adopting a `queued` one),
    /// with the transform version it runs from now on (`None` on the
    /// `source_objects` bulk path, which runs no transform).
    fn open_load(
        &self,
        name: &str,
        sha: &str,
        size: u64,
        source_key: Option<&str>,
        version: Option<&VersionRef>,
    ) -> Result<i64> {
        self.db
            .open_load(&self.feed.id, name, sha, size, source_key, version)
    }

    /// Reads a file's records into the store; `false` when the file could not be read.
    pub(super) fn admit(&self, file_id: i64, name: &str, bytes: &[u8]) -> Result<bool> {
        let feed = &self.feed.id;
        match formats::read(self.feed.format, bytes) {
            Ok(records) => {
                let rows: Vec<_> = records
                    .into_iter()
                    .map(|r| (r.line, r.raw, r.parse_error))
                    .collect();
                self.db.insert_records(file_id, feed, &rows)?;
                self.tally_load(file_id, rows.len());
                tracing::info!(target: "hie::pipeline", feed, file = %name, records = rows.len(), "received");
                Ok(true)
            }
            Err(e) => {
                self.db.fail_file(file_id, &format!("{e:#}"))?;
                self.alerts.raise(
                    Some(feed),
                    "unreadable_file",
                    &format!("{}: cannot read {name}: {e:#}", self.feed.name),
                )?;
                Ok(false)
            }
        }
    }

    /// A zip is a drop of several files: every entry whose extension fits the
    /// source's format becomes a load of its own, named `drop.zip!/entry`, all
    /// with the one version snapshot taken before anything was written. The
    /// zip is `unpacking` until every entry is admitted, then `unpacked` (so
    /// it is not read twice) and archived; a pass that stopped half-way leaves
    /// it `unpacking`, and the next one resumes it, skipping the entries
    /// already admitted.
    pub(super) async fn unpack(
        &self,
        key: &str,
        name: &str,
        sha: &str,
        bytes: Vec<u8>,
        version: Option<&VersionRef>,
    ) -> Result<()> {
        let feed = &self.feed.id;
        let container = match self.db.unfinished_container(feed, name, sha)? {
            Some(container) => container,
            None => {
                self.db
                    .insert_file(feed, name, sha, bytes.len() as u64, "unpacking", None, None)?
            }
        };
        let entries =
            match tokio::task::spawn_blocking(move || -> Result<Vec<(String, Vec<u8>)>> {
                formats::zip::entries(bytes)
            })
            .await?
            {
                Ok(entries) => entries,
                Err(e) => {
                    self.db.fail_file(container, &format!("{e:#}"))?;
                    self.alerts.raise(
                        Some(feed),
                        "unreadable_file",
                        &format!("{}: cannot open {name}: {e:#}", self.feed.name),
                    )?;
                    let archived = self
                        .source
                        .acknowledge(key, &format!("{container}-{name}"))
                        .await?;
                    return self.db.mark_file_archived(container, &archived);
                }
            };
        let mut taken = 0;
        for (n, (path, data)) in entries.into_iter().enumerate() {
            if !formats::zip::takes(self.feed.format, &path) {
                tracing::debug!(target: "hie::pipeline", feed, file = %name, entry = %path, "entry skipped: not a {:?} file", self.feed.format);
                continue;
            }
            let entry_name = format!("{name}!/{path}");
            let entry_sha = formats::sha256_hex(&data);
            // Admitted by the pass that stopped half-way: done already.
            let resumed = match self
                .db
                .entry_load(container, feed, &entry_name, &entry_sha)?
            {
                Some((_, true)) => continue,
                Some((load, false)) => Some(load),
                None => None,
            };
            if resumed.is_none()
                && let Some(original) = self.db.find_file_by_hash(feed, &entry_sha)?
            {
                self.db.insert_file(
                    feed,
                    &entry_name,
                    &entry_sha,
                    data.len() as u64,
                    "duplicate",
                    Some(original),
                    None,
                )?;
                continue;
            }
            // On the source_objects bulk path HFS reads the entry, not the zip: the
            // entry is staged where HFS can fetch it and kept until the receipts.
            let staged = if self.bulk.is_some() && !self.bulk_transformed {
                let base = format!("{}.ndjson", n + 1);
                match self.source.put_unpacked(container, &base, &data).await? {
                    Some(key) => Some(key),
                    None => {
                        let dir = self.bulk_dir.join("unpacked").join(container.to_string());
                        std::fs::create_dir_all(&dir)
                            .with_context(|| format!("creating {}", dir.display()))?;
                        let local = dir.join(&base);
                        crate::bulk::write_served(&self.db, &local, &data)?;
                        Some(local.to_string_lossy().into_owned())
                    }
                }
            } else {
                None
            };
            let file_id = match resumed {
                Some(load) => load,
                None => self.open_load(
                    &entry_name,
                    &entry_sha,
                    data.len() as u64,
                    staged.as_deref(),
                    version,
                )?,
            };
            self.admit(file_id, &entry_name, &data)?;
            taken += 1;
        }
        self.db.finish_container(container)?;
        tracing::info!(target: "hie::pipeline", feed, file = %name, entries = taken, "unpacked");
        let archived = self
            .source
            .acknowledge(key, &format!("{container}-{name}"))
            .await?;
        self.db.mark_file_archived(container, &archived)
    }
}
