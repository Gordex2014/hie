//! The store's side of a `manifest` source's loads.
//!
//! A load is recorded, unlisted, before its kick-off is sent: a `files` row
//! with `listed = 0` and its `submissions` row (`load_id`) in `m_sending`. The
//! request that sends it then lists it (`m_status`), or removes both when the
//! FHIR server refused it. From `m_status` the source's runner asks where the
//! status is polled (`m_polling`), polls it and settles the load from the
//! receipts (`m_done`), or gives it up as unreadable (`m_failed`). A row still
//! `m_sending` past `manifest::sending_until_ms` was left by a request that
//! stopped: the runner lists it as unconfirmed.
//!
//! These statuses are the manifest path's own: the outbound `$bulk-submit`
//! path (`kickoff`, `running`, `done`, `failed`) never reads them, and the
//! manifest path never reads its.
//!
//! The load's `manifest` column holds what the API shows (`by`,
//! `submission_id`, `kickoff`, `sent_by_type`, `deleted`) and what only the
//! engine reads (`request_hash`, `answered`).

use anyhow::{Context, Result};
use serde_json::{Value, json};

use super::{Claim, Db, Tx, iso_ms, now_ms, params, storable_name};
use crate::events::Event;
use crate::manifest::Tally;

/// On PostgreSQL, the source's manifest admission lock, held until the
/// transaction ends: an admission and a pause of one source take turns. On
/// SQLite the write lock already orders them.
pub(super) fn admission_lock(tx: &mut Tx, feed: &str) -> Result<()> {
    if tx.postgres {
        tx.execute(
            "SELECT pg_advisory_xact_lock(hashtext('hie.manifest'), hashtext(current_schema() || ':' || $1))",
            &params![feed],
        )?;
    }
    Ok(())
}

/// A manifest load as it is recorded before its kick-off.
#[derive(Debug, Clone, Copy)]
pub struct NewLoad<'a> {
    pub feed: &'a str,
    pub source_name: &'a str,
    /// What the load is shown as: the URL as stored, or the file name.
    pub name: &'a str,
    /// SHA-256 of the forwarded URL, or of the served manifest.
    pub sha: &'a str,
    /// The served manifest's length; 0 for a URL.
    pub size: usize,
    /// `url` or `upload`.
    pub by: &'static str,
    pub request_id: Option<&'a str>,
    pub request_hash: &'a str,
    /// The token the submission is served under.
    pub token: &'a str,
    /// The uploaded manifest, as served; `None` for a URL.
    pub served: Option<&'a str>,
}

/// A load recorded for its kick-off.
#[derive(Debug, Clone)]
pub struct Admitted {
    pub load_id: i64,
    pub submission_id: String,
    pub received_at: i64,
}

/// What admitting a manifest did.
#[derive(Debug, Clone)]
pub enum Admission {
    /// Recorded, unlisted, for its kick-off.
    New(Admitted),
    /// A load of the source already has this `request_id` for the same body.
    Existing(i64),
    /// The submission with this `request_id` is still being sent.
    InProgress,
    /// A load of the source has this `request_id` for another body.
    Conflict,
    /// The source is paused: nothing was recorded.
    Paused,
}

/// A manifest submission due for a step of its runner.
#[derive(Debug, Clone)]
pub struct Due {
    pub token: String,
    pub feed_id: String,
    pub submission_id: String,
    pub status: String,
    pub status_url: Option<String>,
    pub attempts: u32,
    pub epoch: i64,
    pub load_id: i64,
    /// When the load was recorded.
    pub created_at: i64,
    pub progress: Tally,
    /// Whether the kick-off was answered `2xx`.
    pub answered: bool,
}

/// The live statuses of a manifest submission.
const OPEN: &str = "('m_sending', 'm_status', 'm_polling')";

impl Db {
    /// Records a manifest load, unlisted, and its submission, in one
    /// transaction under the source's admission lock: a `request_id` the
    /// source already has is answered from that load first (a replay sends
    /// nothing, paused or not), then a paused source refuses, then the load
    /// is recorded with its `submissionId`.
    pub fn admit_manifest(&self, load: &NewLoad) -> Result<Admission> {
        self.transaction(|tx| {
            admission_lock(tx, load.feed)?;
            let now = now_ms();
            if let Some(request_id) = load.request_id
                && let Some(found) = tx.query_one(
                    "SELECT f.id, f.listed, f.manifest, s.token, s.status, s.created_at
                     FROM files f LEFT JOIN submissions s ON s.load_id = f.id
                     WHERE f.feed_id = $1 AND f.request_id = $2",
                    &params![load.feed, request_id],
                )?
            {
                let meta = found.json(2).unwrap_or(Value::Null);
                if meta["request_hash"].as_str() != Some(load.request_hash) {
                    tx.rollback = true;
                    return Ok(Admission::Conflict);
                }
                let id = found.i64(0);
                if found.i64(1) == 1 {
                    return Ok(Admission::Existing(id));
                }
                let stale = found.opt_i64(5).unwrap_or(0) + crate::manifest::sending_until_ms();
                if found.string(4) == "m_sending" && stale > now {
                    tx.rollback = true;
                    return Ok(Admission::InProgress);
                }
                // Left by a request that stopped while it sent: listed as
                // unconfirmed, for its source to confirm.
                if let Some(token) = found.opt_string(3) {
                    list(tx, &token, None, false)?;
                }
                return Ok(Admission::Existing(id));
            }
            if tx
                .query_one(
                    "SELECT 1 FROM source_state WHERE feed_id = $1 AND paused = 1",
                    &params![load.feed],
                )?
                .is_some()
            {
                tx.rollback = true;
                return Ok(Admission::Paused);
            }
            let id = tx
                .query_one(
                    "INSERT INTO files (feed_id, name, sha256, size, status, received_at, source_name, listed, request_id, manifest)
                     VALUES ($1, $2, $3, $4, 'processing', $5, $6, 0, $7, '{}') RETURNING id",
                    &params![
                        load.feed,
                        load.name,
                        load.sha,
                        load.size,
                        now,
                        storable_name(load.source_name),
                        load.request_id
                    ],
                )?
                .map(|r| r.i64(0))
                .context("INSERT returned no id")?;
            let submission_id = crate::manifest::submission_id(load.feed, id, load.token);
            let meta = json!({
                "by": load.by,
                "submission_id": submission_id,
                "kickoff": "sending",
                "sent_by_type": {},
                "deleted": 0,
                "request_hash": load.request_hash,
                "answered": false,
            });
            tx.execute(
                "UPDATE files SET manifest = $2 WHERE id = $1",
                &params![id, meta.to_string()],
            )?;
            tx.execute(
                "INSERT INTO submissions (token, feed_id, submission_id, status, manifest, created_at, updated_at, load_id)
                 VALUES ($1, $2, $3, 'm_sending', $4, $5, $5, $6)",
                &params![
                    load.token,
                    load.feed,
                    submission_id,
                    load.served.unwrap_or("null"),
                    now,
                    id
                ],
            )?;
            Ok(Admission::New(Admitted {
                load_id: id,
                submission_id,
                received_at: now,
            }))
        })
    }

    /// The request that sent a kick-off lists its load: accepted (`2xx`) or
    /// unconfirmed (its answer was lost). Only while the submission is still
    /// as the request left it (`m_sending`, epoch 0): `false` when its source
    /// moved it meanwhile.
    pub fn manifest_kicked(&self, token: &str, accepted: bool) -> Result<bool> {
        self.transaction(|tx| list(tx, token, Some(0), accepted))
    }

    /// The FHIR server refused the kick-off: the load and its submission go,
    /// as though never recorded. Only while the request still holds them
    /// (`m_sending`, epoch 0).
    pub fn manifest_refused(&self, token: &str) -> Result<bool> {
        self.transaction(|tx| {
            let Some(r) = tx.query_one(
                "DELETE FROM submissions WHERE token = $1 AND status = 'm_sending' AND epoch = 0 RETURNING load_id",
                &params![token],
            )?
            else {
                return Ok(false);
            };
            tx.execute(
                "DELETE FROM files WHERE id = $1 AND listed = 0",
                &params![r.i64(0)],
            )?;
            Ok(true)
        })
    }

    /// The manifest an engine URL serves: `None` when `token` is no manifest
    /// submission, `Some(None)` when it is one with nothing to serve (a URL,
    /// or a load whose receipts are in).
    pub fn manifest_upload(&self, token: &str) -> Result<Option<Option<String>>> {
        Ok(self
            .query_one(
                "SELECT status, manifest FROM submissions WHERE token = $1 AND load_id IS NOT NULL",
                &params![token],
            )?
            .map(|r| {
                let open = matches!(r.string(0).as_str(), "m_sending" | "m_status" | "m_polling");
                r.opt_string(1).filter(|m| open && m != "null")
            }))
    }

    /// The source's manifest submissions due now: open and due, and those
    /// still `m_sending` past `manifest::sending_until_ms`.
    pub fn manifest_due(&self, feed: &str) -> Result<Vec<Due>> {
        let now = now_ms();
        Ok(self
            .query(
                "SELECT s.token, s.feed_id, s.submission_id, s.status, s.status_url, s.attempts, s.epoch,
                        s.load_id, s.created_at, s.progress, f.manifest
                 FROM submissions s JOIN files f ON f.id = s.load_id
                 WHERE s.feed_id = $1 AND s.load_id IS NOT NULL AND s.next_attempt_at <= $2
                   AND (s.status IN ('m_status', 'm_polling') OR (s.status = 'm_sending' AND s.created_at <= $3))
                 ORDER BY s.created_at",
                &params![feed, now, now - crate::manifest::sending_until_ms()],
            )?
            .iter()
            .map(|r| {
                let meta = r.json(10).unwrap_or(Value::Null);
                Due {
                    token: r.string(0),
                    feed_id: r.string(1),
                    submission_id: r.string(2),
                    status: r.string(3),
                    status_url: r.opt_string(4),
                    attempts: r.i64(5) as u32,
                    epoch: r.i64(6),
                    load_id: r.i64(7),
                    created_at: r.i64(8),
                    progress: r
                        .json(9)
                        .and_then(|v| serde_json::from_value(v).ok())
                        .unwrap_or_default(),
                    answered: meta["answered"].as_bool().unwrap_or(false),
                }
            })
            .collect())
    }

    /// Claims a due manifest submission for one step, as `claim_submission`
    /// claims an outbound one: only while it is as `read` it, due, and the
    /// source's lease allows. `None`: another runner moved it or holds the
    /// source.
    pub fn claim_manifest(&self, read: &Due, owner: &str, leased: bool) -> Result<Option<Claim>> {
        let lease = if leased {
            "EXISTS (SELECT 1 FROM source_leases l
                     WHERE l.feed_id = submissions.feed_id AND l.owner = $6 AND l.until >= $4)"
        } else {
            "NOT EXISTS (SELECT 1 FROM source_leases l
                         WHERE l.feed_id = submissions.feed_id AND l.owner <> $6 AND l.until >= $4)"
        };
        let sql = format!(
            "UPDATE submissions SET epoch = epoch + 1
             WHERE token = $1 AND status IN {OPEN} AND status = $5
               AND epoch = $2 AND attempts = $3 AND next_attempt_at <= $4
               AND {lease}
             RETURNING epoch, attempts"
        );
        Ok(self
            .with(|c| {
                let now = now_ms();
                Ok(c.query(
                    &sql,
                    &params![
                        read.token,
                        read.epoch,
                        read.attempts,
                        now,
                        read.status,
                        owner
                    ],
                )?
                .into_iter()
                .next())
            })?
            .map(|r| Claim {
                epoch: r.i64(0),
                attempts: r.i64(1) as u32,
            }))
    }

    /// A load left `m_sending` by a request that stopped: listed as
    /// unconfirmed, under the runner's claim.
    pub fn manifest_list_stale(&self, token: &str, claim: Claim) -> Result<bool> {
        self.transaction(|tx| list(tx, token, Some(claim.epoch), false))
    }

    /// The FHIR server knows the submission and says where its status is
    /// polled. A load whose kick-off was unconfirmed is accepted now, with
    /// `submission.opened`.
    pub fn manifest_status_found(
        &self,
        token: &str,
        claim: Claim,
        status_url: &str,
    ) -> Result<bool> {
        self.transaction(|tx| {
            let Some(r) = tx.query_one(
                "UPDATE submissions SET status = 'm_polling', status_url = $2, attempts = 0, next_attempt_at = 0,
                        error = NULL, updated_at = $3, epoch = epoch + 1
                 WHERE token = $1 AND status = 'm_status' AND epoch = $4 AND attempts = $5
                 RETURNING feed_id, submission_id, load_id",
                &params![token, status_url, now_ms(), claim.epoch, claim.attempts],
            )?
            else {
                return Ok(false);
            };
            let (feed, submission_id, load_id) = (r.string(0), r.string(1), r.i64(2));
            let meta = load_meta(tx, load_id)?;
            if meta["kickoff"] != "accepted" {
                let mut meta = meta;
                meta["kickoff"] = json!("accepted");
                tx.execute(
                    "UPDATE files SET manifest = $2 WHERE id = $1",
                    &params![load_id, meta.to_string()],
                )?;
                tx.insert_events(&[opened(&feed, token, &submission_id, load_id)])?;
            }
            Ok(true)
        })
    }

    /// Nothing to do yet: the next step is due at `next_attempt_at`.
    pub fn manifest_later(&self, token: &str, claim: Claim, next_attempt_at: i64) -> Result<bool> {
        Ok(self.execute(
            &format!(
                "UPDATE submissions SET next_attempt_at = $2, updated_at = $3, epoch = epoch + 1
                 WHERE token = $1 AND status IN {OPEN} AND epoch = $4 AND attempts = $5"
            ),
            &params![
                token,
                next_attempt_at,
                now_ms(),
                claim.epoch,
                claim.attempts
            ],
        )? > 0)
    }

    /// A step failed for a reason that may pass: the next attempt is due at
    /// `next_attempt_at`, with `submission.attempt_failed`. `reason` is
    /// already safe to store.
    pub fn manifest_attempt_failed(
        &self,
        token: &str,
        claim: Claim,
        next_attempt_at: i64,
        reason: &str,
    ) -> Result<bool> {
        let attempts = claim.attempts + 1;
        self.transaction(|tx| {
            let Some(r) = tx.query_one(
                &format!(
                    "UPDATE submissions SET attempts = $2, next_attempt_at = $3, error = $4, failures = failures + 1,
                            updated_at = $5, epoch = epoch + 1
                     WHERE token = $1 AND status IN {OPEN} AND epoch = $6 AND attempts = $7
                     RETURNING feed_id, submission_id, load_id"
                ),
                &params![token, attempts, next_attempt_at, reason, now_ms(), claim.epoch, claim.attempts],
            )?
            else {
                return Ok(false);
            };
            tx.insert_events(&[Event::new("submission.attempt_failed", &r.string(0))
                .load(r.i64(2))
                .attempt(attempts as i64)
                .reason(reason)
                .detail(json!({"submission": r.string(1), "run": crate::bulk::run_key(token),
                               "next_attempt_at": iso_ms(next_attempt_at)}))])?;
            Ok(true)
        })
    }

    /// Keeps what the receipts said so far, after a status page: a poll that
    /// stops resumes from here.
    pub fn manifest_checkpoint(&self, token: &str, claim: Claim, tally: &Tally) -> Result<bool> {
        Ok(self.execute(
            "UPDATE submissions SET progress = $2, updated_at = $3
             WHERE token = $1 AND status = 'm_polling' AND epoch = $4 AND attempts = $5",
            &params![
                token,
                serde_json::to_string(tally)?,
                now_ms(),
                claim.epoch,
                claim.attempts
            ],
        )? > 0)
    }

    /// Settles a load from its receipts, in one transaction: the load is
    /// `done` with its counts and failure groups, the stored manifest goes,
    /// with `submission.settled` and `load.finished`.
    pub fn manifest_settle(&self, token: &str, claim: Claim, tally: &Tally) -> Result<bool> {
        self.transaction(|tx| {
            let now = now_ms();
            let Some(r) = tx.query_one(
                "UPDATE submissions SET status = 'm_done', manifest = 'null', progress = NULL, attempts = 0,
                        next_attempt_at = 0, error = NULL, sent = $2, rejected = $3, updated_at = $4, epoch = epoch + 1
                 WHERE token = $1 AND status = 'm_polling' AND epoch = $5 AND attempts = $6
                 RETURNING feed_id, submission_id, load_id, failures",
                &params![token, tally.sent, tally.rejected, now, claim.epoch, claim.attempts],
            )?
            else {
                return Ok(false);
            };
            let (feed, submission_id, load_id) = (r.string(0), r.string(1), r.i64(2));
            let mut meta = load_meta(tx, load_id)?;
            meta["sent_by_type"] = json!(tally.sent_by_type);
            meta["deleted"] = json!(tally.deleted);
            tx.execute(
                "UPDATE files SET status = 'done', finished_at = $2, listed = 1, receipts_sent = $3,
                        receipts_rejected = $4, receipt_failures = $5, manifest = $6, error = NULL
                 WHERE id = $1",
                &params![
                    load_id,
                    now,
                    tally.sent,
                    tally.rejected,
                    serde_json::to_string(&tally.kept_groups())?,
                    meta.to_string()
                ],
            )?;
            tx.insert_events(&[
                Event::new("submission.settled", &feed).load(load_id).detail(json!({
                    "submission": submission_id,
                    "run": crate::bulk::run_key(token),
                    "sent": tally.sent,
                    "rejected": tally.rejected,
                    "failures": r.i64(3),
                })),
                Event::new("load.finished", &feed).load(load_id).detail(json!({
                    "records": tally.sent + tally.rejected,
                    "sent": tally.sent,
                    "rejected": tally.rejected,
                    "failing": tally.rejected,
                })),
            ])?;
            Ok(true)
        })
    }

    /// Gives a load up as unreadable, with `error` (already safe to store)
    /// and `load.unreadable`: the stored manifest goes. Under the runner's
    /// claim.
    pub fn manifest_unreadable(&self, token: &str, claim: Claim, error: &str) -> Result<bool> {
        self.transaction(|tx| {
            let now = now_ms();
            let Some(r) = tx.query_one(
                &format!(
                    "UPDATE submissions SET status = 'm_failed', manifest = 'null', progress = NULL,
                            next_attempt_at = 0, error = $2, updated_at = $3, epoch = epoch + 1
                     WHERE token = $1 AND status IN {OPEN} AND epoch = $4 AND attempts = $5
                     RETURNING feed_id, load_id"
                ),
                &params![token, error, now, claim.epoch, claim.attempts],
            )?
            else {
                return Ok(false);
            };
            let (feed, load_id) = (r.string(0), r.i64(1));
            let Some(f) = tx.query_one(
                "UPDATE files SET status = 'unreadable', error = $2, finished_at = $3, listed = 1
                 WHERE id = $1 RETURNING name, size, manifest",
                &params![load_id, error, now],
            )?
            else {
                return Ok(true);
            };
            let size = shown_size(&f.json(2).unwrap_or(Value::Null), f.i64(1));
            tx.insert_events(&[Event::new("load.unreadable", &feed)
                .load(load_id)
                .reason(error)
                .detail(json!({"name": f.string(0), "size": size}))])?;
            Ok(true)
        })
    }
}

/// A load's `manifest` metadata, as stored.
fn load_meta(tx: &mut Tx, load_id: i64) -> Result<Value> {
    Ok(tx
        .query_one(
            "SELECT manifest FROM files WHERE id = $1",
            &params![load_id],
        )?
        .and_then(|r| r.json(0))
        .unwrap_or_else(|| json!({})))
}

/// A load's size as shown: the served manifest's, or none for a URL.
pub(crate) fn shown_size(meta: &Value, size: i64) -> Value {
    if meta["by"] == "upload" {
        json!(size)
    } else {
        Value::Null
    }
}

/// `submission.opened` of a manifest load.
fn opened(feed: &str, token: &str, submission_id: &str, load_id: i64) -> Event {
    Event::new("submission.opened", feed)
        .load(load_id)
        .detail(json!({"submission": submission_id, "run": crate::bulk::run_key(token)}))
}

/// Lists a load whose kick-off was sent: its submission moves from
/// `m_sending` to `m_status` (only at `epoch` when given), the load is listed
/// as accepted or unconfirmed, with `load.received`, and `submission.opened`
/// when accepted. `false` when the submission had moved.
fn list(tx: &mut Tx, token: &str, epoch: Option<i64>, accepted: bool) -> Result<bool> {
    let Some(r) = tx.query_one(
        "UPDATE submissions SET status = 'm_status', attempts = 0, next_attempt_at = 0, updated_at = $2, epoch = epoch + 1
         WHERE token = $1 AND status = 'm_sending' AND (CAST($3 AS BIGINT) IS NULL OR epoch = CAST($3 AS BIGINT))
         RETURNING feed_id, submission_id, load_id",
        &params![token, now_ms(), epoch],
    )?
    else {
        return Ok(false);
    };
    let (feed, submission_id, load_id) = (r.string(0), r.string(1), r.i64(2));
    let Some(f) = tx.query_one(
        "SELECT name, size, manifest FROM files WHERE id = $1",
        &params![load_id],
    )?
    else {
        return Ok(true);
    };
    let mut meta = f.json(2).unwrap_or_else(|| json!({}));
    meta["kickoff"] = json!(if accepted { "accepted" } else { "unconfirmed" });
    meta["answered"] = json!(accepted);
    tx.execute(
        "UPDATE files SET listed = 1, manifest = $2 WHERE id = $1",
        &params![load_id, meta.to_string()],
    )?;
    let mut events = vec![
        Event::new("load.received", &feed)
            .load(load_id)
            .detail(json!({"name": f.string(0), "size": shown_size(&meta, f.i64(1))})),
    ];
    if accepted {
        events.push(opened(&feed, token, &submission_id, load_id));
    }
    tx.insert_events(&events)?;
    Ok(true)
}

impl Db {
    /// A manifest load's failure groups as `GET /api/loads/{id}` shows them;
    /// `None` for any other load.
    pub fn manifest_failures(&self, file_id: i64) -> Result<Option<Vec<Value>>> {
        let Some(r) = self.query_one(
            "SELECT manifest, receipt_failures FROM files WHERE id = $1 AND manifest IS NOT NULL",
            &params![file_id],
        )?
        else {
            return Ok(None);
        };
        let groups: Vec<crate::manifest::Group> = r
            .json(1)
            .and_then(|v| serde_json::from_value(v).ok())
            .unwrap_or_default();
        Ok(Some(
            groups
                .into_iter()
                .map(|g| {
                    json!({"kind": "server", "severity": g.severity, "code": g.code, "reason": g.reason,
                           "count": g.count, "first_line": null, "samples": []})
                })
                .collect(),
        ))
    }

    /// The receipts of the manifest loads that finished at or after `since`
    /// (and before `until`), of one source or of all: (sent, rejected).
    pub fn receipts_between(
        &self,
        feed: Option<&str>,
        since: i64,
        until: i64,
    ) -> Result<(i64, i64)> {
        let r = self
            .query_one(
                "SELECT CAST(COALESCE(SUM(receipts_sent), 0) AS BIGINT), CAST(COALESCE(SUM(receipts_rejected), 0) AS BIGINT)
                 FROM files WHERE manifest IS NOT NULL AND listed = 1 AND finished_at >= $1 AND finished_at < $2
                   AND (CAST($3 AS TEXT) IS NULL OR feed_id = CAST($3 AS TEXT))",
                &params![since, until, feed],
            )?
            .context("the receipts read returned no row")?;
        Ok((r.i64(0), r.i64(1)))
    }
}
