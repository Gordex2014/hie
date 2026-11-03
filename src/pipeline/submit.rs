//! `$bulk-submit` delivery: submissions opened over the source objects or the
//! engine's transformed NDJSON, polled, settled from receipts, and archived.

use std::path::Path;
use std::time::Duration;

use anyhow::{Context, Result};
use serde_json::json;

use crate::bulk::{self, BulkSubmitter, Poll};
use crate::db::now_ms;
use crate::fhir::{self, SendError};
use crate::transform;

use super::FeedRunner;

/// Records settled per page when a submission's receipts come back.
const SETTLE_PAGE: usize = 5_000;

impl FeedRunner {
    /// Opens one submission per drop: the files with pending records, up to the
    /// manifest cap, handed to HFS through a manifest the engine serves.
    pub(super) async fn submit_pending(&self) -> Result<()> {
        let Some(bulk) = &self.bulk else {
            return Ok(());
        };
        if self.bulk_transformed {
            return self.submit_transformed(bulk).await;
        }
        loop {
            let files = self
                .db
                .submittable_files(&self.feed.id, self.feed.delivery.manifest_objects_max)?;
            if files.is_empty() {
                return Ok(());
            }
            let token = bulk::token();
            let ttl = Duration::from_secs(self.feed.delivery.url_ttl_seconds);
            let mut entries = Vec::new();
            let mut file_ids = Vec::new();
            let mut records = 0usize;
            let mut hashes = Vec::new();
            for file in &files {
                let url = match &file.source_key {
                    // An entry the engine keeps itself is served, never signed.
                    Some(key) if !self.is_staged_locally(key) => {
                        match self.source.share(key, ttl).await? {
                            Some(signed) => signed,
                            None => bulk.file_url(&token, file.file_id),
                        }
                    }
                    _ => bulk.file_url(&token, file.file_id),
                };
                entries.push((file.resource_type.clone(), url, file.count));
                file_ids.push(file.file_id);
                records += file.count;
                hashes.push(file.name.clone());
            }
            let hash_refs: Vec<&str> = hashes.iter().map(String::as_str).collect();
            let submission_id = bulk::submission_id(&self.feed.id, &hash_refs);
            let manifest = bulk.manifest(&entries);
            self.db.create_submission_for_files(
                &token,
                &self.feed.id,
                &submission_id,
                &manifest,
                &file_ids,
            )?;
            tracing::info!(target: "hie::pipeline", feed = %self.feed.id, submission = %submission_id, files = files.len(), records, "submission opened");
        }
    }

    /// Moves every open submission one step: kick-off, then status polls until the
    /// receipts arrive, then the records' dispositions and the objects' archive.
    pub(super) async fn poll_submissions(&self) -> Result<()> {
        let Some(bulk) = &self.bulk else {
            return Ok(());
        };
        for sub in self.db.submissions_due(&self.feed.id)? {
            let step = match sub.status.as_str() {
                "kickoff" => {
                    match bulk
                        .kickoff(
                            &self.feed.id,
                            &sub.submission_id,
                            &bulk.manifest_url(&sub.token),
                        )
                        .await
                    {
                        Ok(()) => match bulk.status_url(&self.feed.id, &sub.submission_id).await {
                            Ok(url) => Ok(Some(("running", Some(url)))),
                            Err(e) => Err(e),
                        },
                        // HFS already holds this drop: a repeat of the same files.
                        Err(SendError::Refused(409, _)) => {
                            match bulk.status_url(&self.feed.id, &sub.submission_id).await {
                                Ok(url) => Ok(Some(("running", Some(url)))),
                                Err(e) => Err(e),
                            }
                        }
                        Err(e) => Err(e),
                    }
                }
                _ => match sub.status_url.as_deref() {
                    None => Ok(Some(("kickoff", None))),
                    Some(url) => match bulk.poll(url).await {
                        Ok(Poll::InProgress) => Ok(None),
                        Ok(Poll::Done { references, errors }) => {
                            self.settle(&sub.token, &references, &errors).await?;
                            Ok(Some(("done", None)))
                        }
                        Err(e) => Err(e),
                    },
                },
            };
            match step {
                Ok(None) => {
                    self.db.update_submission(
                        &sub.token,
                        &sub.status,
                        None,
                        sub.attempts,
                        now_ms() + 2000,
                        None,
                    )?;
                }
                Ok(Some((status, url))) => {
                    self.db
                        .update_submission(&sub.token, status, url.as_deref(), 0, 0, None)?;
                    if status == "done" {
                        self.recovered()?;
                    }
                }
                Err(SendError::Refused(status, body)) => {
                    let reason = format!("HTTP {status}: {}", fhir::outcome_text(&body));
                    tracing::warn!(target: "hie::pipeline", feed = %self.feed.id, submission = %sub.submission_id, "refused: {reason}");
                    for (record_id, _, _) in self.db.submitted_records(&sub.token)? {
                        self.db
                            .mark_rejected(record_id, "server", &reason, Some(&body))?;
                    }
                    self.db.update_submission(
                        &sub.token,
                        "failed",
                        None,
                        sub.attempts,
                        0,
                        Some(&reason),
                    )?;
                    self.archive_submission(&sub.token).await?;
                }
                Err(SendError::Transient(reason)) => {
                    let attempts = sub.attempts + 1;
                    let d = &self.feed.delivery;
                    let backoff = d
                        .initial_backoff_ms
                        .saturating_mul(1u64 << (attempts - 1).min(20))
                        .min(d.max_backoff_ms);
                    self.db.update_submission(
                        &sub.token,
                        &sub.status,
                        None,
                        attempts,
                        now_ms() + backoff as i64,
                        Some(&reason),
                    )?;
                    self.outage(attempts, &reason)?;
                }
            }
        }
        Ok(())
    }

    /// Gives every submitted record its disposition from the receipts, then archives
    /// the objects HFS has read.
    pub(super) async fn settle(
        &self,
        token: &str,
        references: &std::collections::HashSet<String>,
        errors: &[String],
    ) -> Result<()> {
        let hint = match errors.len() {
            0 => String::new(),
            n => format!(" HFS reported {n} error(s); the first: {}", errors[0]),
        };
        // Page through the submission: a large one holds a million records, and
        // each page's receipts are applied in sets.
        let mut after_id = 0;
        loop {
            let page = self
                .db
                .submitted_records_after(token, after_id, SETTLE_PAGE)?;
            if page.is_empty() {
                break;
            }
            let mut sent = Vec::with_capacity(page.len());
            for (record_id, _, resources) in &page {
                let reference = resources
                    .get(0)
                    .and_then(|r| transform::check_resource(r).ok())
                    .map(|key| fhir::request_url(&key));
                match reference {
                    Some(r) if references.contains(&r) => {
                        sent.push((*record_id, json!([{"reference": r, "via": "$bulk-submit"}])))
                    }
                    _ => self.db.mark_rejected(
                        *record_id,
                        "server",
                        &format!("no receipt from HFS for this resource.{hint}"),
                        None,
                    )?,
                }
                after_id = *record_id;
            }
            self.db.mark_sent_many(&sent)?;
        }
        self.archive_submission(token).await?;
        self.remove_submission_files(token).await;
        Ok(())
    }

    /// Drops what the engine wrote for a transformed submission, now that HFS has
    /// read it and answered.
    pub(super) async fn remove_submission_files(&self, token: &str) {
        let dir = self.bulk_dir.join(token);
        if dir.is_dir()
            && let Err(e) = std::fs::remove_dir_all(&dir)
        {
            tracing::warn!(target: "hie::pipeline", feed = %self.feed.id, "cannot remove {}: {e}", dir.display());
        }
        if let Err(e) = self.source.remove_submission_files(token).await {
            tracing::warn!(target: "hie::pipeline", feed = %self.feed.id, "cannot remove submission files at the source: {e:#}");
        }
    }

    /// Writes the pending records' resources as NDJSON per resource type, in the
    /// source's store when it can sign URLs (HFS fetches from there) or under
    /// `bulk_dir` served by the engine, and opens one submission over them.
    pub(super) async fn submit_transformed(&self, bulk: &BulkSubmitter) -> Result<()> {
        loop {
            let records = self
                .db
                .submittable_records(&self.feed.id, self.feed.delivery.bulk_records_max)?;
            if records.is_empty() {
                return Ok(());
            }
            let token = bulk::token();
            let ttl = Duration::from_secs(self.feed.delivery.url_ttl_seconds);
            let mut by_type: std::collections::BTreeMap<String, Vec<u8>> = Default::default();
            let mut counts: std::collections::BTreeMap<String, usize> = Default::default();
            for (_, resources) in &records {
                for resource in resources.as_array().into_iter().flatten() {
                    let kind = resource["resourceType"]
                        .as_str()
                        .unwrap_or("Resource")
                        .to_string();
                    let lines = by_type.entry(kind.clone()).or_default();
                    lines.extend_from_slice(resource.to_string().as_bytes());
                    lines.push(b'\n');
                    *counts.entry(kind).or_default() += 1;
                }
            }
            let mut entries = Vec::new();
            for (n, (kind, bytes)) in by_type.into_iter().enumerate() {
                let n = n as i64 + 1;
                let name = format!("{n}.ndjson");
                let shared = match self
                    .source
                    .put_submission_file(&token, &name, bytes.clone())
                    .await?
                {
                    Some(key) => self.source.share(&key, ttl).await?,
                    None => None,
                };
                let url = match shared {
                    Some(url) => url,
                    None => {
                        let dir = self.bulk_dir.join(&token);
                        std::fs::create_dir_all(&dir)
                            .with_context(|| format!("creating {}", dir.display()))?;
                        std::fs::write(dir.join(&name), &bytes)
                            .with_context(|| format!("writing {}", dir.join(&name).display()))?;
                        bulk.file_url(&token, n)
                    }
                };
                entries.push((kind.clone(), url, counts[&kind]));
            }
            let record_ids: Vec<i64> = records.iter().map(|(id, _)| *id).collect();
            let span = format!(
                "records {}-{}",
                record_ids[0],
                record_ids[record_ids.len() - 1]
            );
            let submission_id = bulk::submission_id(&self.feed.id, &[span.as_str()]);
            let manifest = bulk.manifest(&entries);
            self.db.create_submission(
                &token,
                &self.feed.id,
                &submission_id,
                &manifest,
                &record_ids,
            )?;
            tracing::info!(target: "hie::pipeline", feed = %self.feed.id, submission = %submission_id, files = entries.len(), records = record_ids.len(), "submission opened (transformed)");
        }
    }

    pub(super) async fn archive_submission(&self, token: &str) -> Result<()> {
        let file_ids: Vec<i64> = {
            let rows = self.db.query_json(
                "SELECT DISTINCT file_id FROM records WHERE submission = $1",
                crate::db::params![token],
            )?;
            rows.iter().filter_map(|r| r["file_id"].as_i64()).collect()
        };
        for file_id in file_ids {
            if let Some((key, name)) = self.db.file_source(file_id)? {
                let archived = if self.is_staged_locally(&key) {
                    // An unpacked entry the engine served: nothing to move, it goes.
                    if let Err(e) = std::fs::remove_file(&key) {
                        tracing::warn!(target: "hie::pipeline", feed = %self.feed.id, "cannot remove {key}: {e}");
                    } else if let Some(dir) = Path::new(&key).parent() {
                        // The container's directory goes with its last entry.
                        let _ = std::fs::remove_dir(dir);
                    }
                    format!("removed {key}")
                } else {
                    self.source
                        .acknowledge(&key, &format!("{file_id}-{name}"))
                        .await?
                };
                self.db.mark_file_archived(file_id, &archived)?;
            }
        }
        Ok(())
    }

    /// Whether a kept file lives in the engine's own `bulk_dir` rather than at the source.
    pub(super) fn is_staged_locally(&self, key: &str) -> bool {
        let prefix = self.bulk_dir.to_string_lossy();
        key.starts_with(prefix.as_ref())
    }
}
