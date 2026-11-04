//! `Idempotency-Key` on the routes that create something (contract,
//! Conventions). A route takes an [`Idempotent`] instead of its body and hands
//! its work to [`Idempotent::run`] (work with effects outside the store, run
//! to completion even if the client goes away) or [`Idempotent::run_in_tx`]
//! (work done only in the store: the work and its stored answer commit in one
//! transaction).
//!
//! - A key is 1 to 128 printable ASCII characters (`400
//!   idempotency_key_invalid` otherwise). Routes that require one answer `400
//!   idempotency_key_required` without it.
//! - The request is identified by the SHA-256 of its method, route, target
//!   (path and query) and canonical JSON body; only that hash is stored, never
//!   the body. Keys live in the store 48 hours and survive restarts.
//! - The first request with a key claims it (`in_progress`) and does the
//!   work; its answer, when it succeeds, is stored (`completed`). The same key
//!   with the same request then replays that answer without doing the work
//!   again: `201` replays as `200`, any other status as itself (`202` stays
//!   `202`). This is checked before anything the route itself checks.
//! - The same key with another request is `409 idempotency_conflict`.
//! - A second request with the same key while the first is still working waits
//!   for its answer (up to 30 seconds, then `409 idempotency_conflict`): the
//!   work is done once.
//! - Work that fails (any error answer) gives the key up, so it can be sent again.
//! - A claim is owned by a token. While its work runs, the owner renews the
//!   claim every 10 seconds; only a claim whose renewal lapsed for a minute
//!   (its engine stopped or hung) is taken over, by the same request, which
//!   runs the work with [`Attempt::recovered`] set: the route then does the
//!   work only if what it creates does not exist yet (and otherwise answers
//!   what exists). Completing, renewing and giving up check the token, so an
//!   owner whose claim was taken over can no longer store an answer; in
//!   [`Idempotent::run_in_tx`] that rolls its work back.

use std::future::Future;
use std::time::{Duration, Instant};

use axum::Json;
use axum::body::Bytes;
use axum::extract::{FromRequest, MatchedPath, Request};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use serde_json::Value;
use sha2::{Digest, Sha256};

use crate::api::ApiError;
use crate::db::{Claim, Db, Tx};

const HEADER: &str = "idempotency-key";
/// How often a running claim is renewed.
#[cfg(not(test))]
const HEARTBEAT: Duration = Duration::from_secs(10);
#[cfg(test)]
const HEARTBEAT: Duration = Duration::from_millis(50);
/// A claim not renewed for this long belongs to an engine that stopped.
const LAPSED_MS: i64 = 60_000;
/// How long a request waits for another one with the same key to answer.
const WAIT: Duration = Duration::from_secs(30);

/// A request to a route that honours `Idempotency-Key`: the key, the request's
/// hash, and its body (which the route parses itself, inside its work).
pub struct Idempotent {
    key: Option<String>,
    request_sha256: String,
    body: Bytes,
}

/// How the work is being done.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Attempt {
    /// The key was taken over from a claim that never finished: the earlier
    /// attempt may have done some of the work. Do it only if its result does
    /// not exist yet.
    pub recovered: bool,
}

/// What the work answers: a status and a JSON body.
pub type Answer = (StatusCode, Value);

enum Start {
    Claimed {
        key: String,
        token: String,
        attempt: Attempt,
    },
    Unkeyed,
    Replay(Response),
}

impl Idempotent {
    /// The key, when the request has one.
    pub fn key(&self) -> Option<&str> {
        self.key.as_deref()
    }

    /// The request body as sent.
    pub fn body(&self) -> &Bytes {
        &self.body
    }

    /// Keys the request's hash with `key` (HMAC-SHA256 over the SHA-256), for
    /// routes whose body carries a secret value: what is stored then cannot
    /// be brute-forced back to the value without the key.
    pub fn keyed(mut self, key: &[u8]) -> Result<Self, ApiError> {
        self.request_sha256 = crate::secrets::keyed_hash(key, self.request_sha256.as_bytes())?;
        Ok(self)
    }

    /// Does `work` once per key. The work runs on a task of its own, so it
    /// finishes and its answer is stored even if the client goes away.
    pub async fn run<F, Fut>(
        self,
        db: &std::sync::Arc<Db>,
        required: bool,
        work: F,
    ) -> Result<Response, ApiError>
    where
        F: FnOnce(Attempt, Bytes) -> Fut + Send + 'static,
        Fut: Future<Output = Result<Answer, ApiError>> + Send + 'static,
    {
        let (key, token, attempt) = match self.start(db, required).await? {
            Start::Replay(response) => return Ok(response),
            Start::Unkeyed => return work(Attempt::default(), self.body).await.map(respond),
            Start::Claimed {
                key,
                token,
                attempt,
            } => (key, token, attempt),
        };
        let db = db.clone();
        let body = self.body;
        let task = tokio::spawn(async move {
            let work = work(attempt, body);
            tokio::pin!(work);
            let mut heartbeat = tokio::time::interval(HEARTBEAT);
            heartbeat.tick().await;
            let result = loop {
                tokio::select! {
                    result = &mut work => break result,
                    _ = heartbeat.tick() => match db.idempotency_renew(&key, &token) {
                        Ok(true) => {}
                        Ok(false) => tracing::error!("the claim on an idempotency key was taken over while its work ran"),
                        Err(e) => tracing::error!("renewing an idempotency key: {e:#}"),
                    },
                }
            };
            settle(&db, &key, &token, &result);
            result
        });
        match task.await {
            Ok(result) => result.map(respond),
            Err(e) => Err(anyhow::anyhow!("the request's work failed: {e}").into()),
        }
    }

    /// Does `work`, which only changes the store, once per key: the work and
    /// the stored answer commit together, or neither does.
    pub async fn run_in_tx<F>(self, db: &Db, required: bool, work: F) -> Result<Response, ApiError>
    where
        F: FnOnce(&mut Tx<'_>, Attempt, &[u8]) -> Result<Answer, ApiError>,
    {
        let (key, token, attempt) = match self.start(db, required).await? {
            Start::Replay(response) => return Ok(response),
            Start::Unkeyed => {
                return db
                    .transaction(|tx| work(tx, Attempt::default(), &self.body))
                    .map(respond);
            }
            Start::Claimed {
                key,
                token,
                attempt,
            } => (key, token, attempt),
        };
        // No await from here on: nothing can stop this half-way but the process.
        let result = db.transaction(|tx| {
            // Still ours, and held for the transaction (the row is written).
            if !Db::idempotency_renew_in(tx, &key, &token)? {
                return Err(taken_over());
            }
            let (status, value) = work(tx, attempt, &self.body)?;
            if status.is_success()
                && !Db::idempotency_complete_in(tx, &key, &token, status.as_u16(), Some(&value))?
            {
                return Err(taken_over());
            }
            Ok((status, value))
        });
        if !matches!(&result, Ok((status, _)) if status.is_success())
            && let Err(e) = db.idempotency_release(&key, &token)
        {
            tracing::error!("releasing idempotency key: {e:#}");
        }
        result.map(respond)
    }

    async fn start(&self, db: &Db, required: bool) -> Result<Start, ApiError> {
        let Some(key) = self.key.clone() else {
            if required {
                return Err(ApiError::bad_request(
                    "idempotency_key_required",
                    "this route needs an Idempotency-Key header",
                ));
            }
            return Ok(Start::Unkeyed);
        };
        let until = Instant::now() + WAIT;
        loop {
            match db.idempotency_claim(&key, &self.request_sha256, LAPSED_MS)? {
                Claim::Claimed { token, recovered } => {
                    return Ok(Start::Claimed {
                        key,
                        token,
                        attempt: Attempt { recovered },
                    });
                }
                Claim::Completed {
                    request_sha256,
                    status,
                    body,
                } => {
                    self.same_request(&request_sha256)?;
                    let status = match StatusCode::from_u16(status) {
                        Ok(StatusCode::CREATED) => StatusCode::OK,
                        Ok(s) => s,
                        Err(_) => StatusCode::OK,
                    };
                    return Ok(Start::Replay(match body {
                        Some(body) => (status, Json(body)).into_response(),
                        None => status.into_response(),
                    }));
                }
                Claim::InProgress { request_sha256 } => {
                    self.same_request(&request_sha256)?;
                    if Instant::now() >= until {
                        return Err(ApiError::conflict(
                            "idempotency_conflict",
                            "a request with this Idempotency-Key is still being answered",
                        ));
                    }
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
            }
        }
    }

    fn same_request(&self, theirs: &str) -> Result<(), ApiError> {
        if theirs == self.request_sha256 {
            Ok(())
        } else {
            Err(ApiError::conflict(
                "idempotency_conflict",
                "this Idempotency-Key was used with another request",
            ))
        }
    }
}

/// The claim this request held was taken over: its work does not count.
fn taken_over() -> ApiError {
    ApiError::conflict(
        "idempotency_conflict",
        "another request took this Idempotency-Key over; nothing was done",
    )
}

fn respond((status, body): Answer) -> Response {
    (status, Json(body)).into_response()
}

/// Stores a successful answer; gives the key up otherwise.
fn settle(db: &Db, key: &str, token: &str, result: &Result<Answer, ApiError>) {
    let stored = match result {
        Ok((status, body)) if status.is_success() => db
            .idempotency_complete(key, token, status.as_u16(), Some(body))
            .map(|stored| {
                if !stored {
                    tracing::error!(
                        "the claim on an idempotency key was taken over; this answer is not stored"
                    );
                }
            }),
        _ => db.idempotency_release(key, token),
    };
    if let Err(e) = stored {
        tracing::error!("recording the idempotency key's answer: {e:#}");
    }
}

impl<S: Send + Sync> FromRequest<S> for Idempotent {
    type Rejection = ApiError;

    async fn from_request(req: Request, state: &S) -> Result<Self, Self::Rejection> {
        let key = match req.headers().get(HEADER) {
            None => None,
            Some(value) => Some(
                value
                    .to_str()
                    .ok()
                    .filter(|k| valid_key(k))
                    .ok_or_else(|| {
                        ApiError::bad_request(
                            "idempotency_key_invalid",
                            "Idempotency-Key must be 1 to 128 printable ASCII characters",
                        )
                    })?
                    .to_string(),
            ),
        };
        let method = req.method().as_str().to_string();
        let target = req
            .uri()
            .path_and_query()
            .map(|p| p.as_str().to_string())
            .unwrap_or_else(|| req.uri().path().to_string());
        let route = req
            .extensions()
            .get::<MatchedPath>()
            .map(|m| m.as_str().to_string())
            .unwrap_or_else(|| req.uri().path().to_string());
        let body = Bytes::from_request(req, state)
            .await
            .map_err(|_| ApiError::invalid_body("the body could not be read"))?;
        Ok(Self {
            request_sha256: request_sha256(&method, &route, &target, &body),
            key,
            body,
        })
    }
}

fn valid_key(key: &str) -> bool {
    (1..=128).contains(&key.len()) && key.bytes().all(|b| (0x20..=0x7e).contains(&b))
}

/// SHA-256 over method, route, target and the canonical JSON of the body (its
/// bytes when it is not JSON), each ended by a newline.
pub fn request_sha256(method: &str, route: &str, target: &str, body: &[u8]) -> String {
    let mut hash = Sha256::new();
    for part in [method, route, target] {
        hash.update(part.as_bytes());
        hash.update(b"\n");
    }
    match serde_json::from_slice::<Value>(body) {
        Ok(value) => {
            let mut canonical = String::new();
            write_canonical(&value, &mut canonical);
            hash.update(b"json\n");
            hash.update(canonical.as_bytes());
        }
        Err(_) => {
            hash.update(b"bytes\n");
            hash.update(body);
        }
    }
    hex::encode(hash.finalize())
}

/// JSON with object members sorted by name and no whitespace.
fn write_canonical(value: &Value, out: &mut String) {
    match value {
        Value::Object(members) => {
            let mut names: Vec<&String> = members.keys().collect();
            names.sort();
            out.push('{');
            for (i, name) in names.into_iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                out.push_str(&Value::String(name.clone()).to_string());
                out.push(':');
                write_canonical(&members[name], out);
            }
            out.push('}');
        }
        Value::Array(items) => {
            out.push('[');
            for (i, item) in items.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                write_canonical(item, out);
            }
            out.push(']');
        }
        leaf => out.push_str(&leaf.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use axum::Router;
    use axum::routing::post;
    use serde_json::json;

    use super::*;
    use crate::params;

    #[test]
    fn the_hash_ignores_member_order_and_whitespace_only() {
        let a = request_sha256(
            "POST",
            "/api/x",
            "/api/x",
            br#"{"a": 1, "b": [1, {"d": 2, "c": 3}]}"#,
        );
        let b = request_sha256(
            "POST",
            "/api/x",
            "/api/x",
            br#"{"b":[1,{"c":3,"d":2}],"a":1}"#,
        );
        assert_eq!(a, b);
        assert_ne!(
            a,
            request_sha256(
                "POST",
                "/api/x",
                "/api/x",
                br#"{"a": 2, "b": [1, {"d": 2, "c": 3}]}"#
            )
        );
        assert_ne!(
            a,
            request_sha256(
                "POST",
                "/api/x",
                "/api/x?y=1",
                br#"{"a": 1, "b": [1, {"d": 2, "c": 3}]}"#
            )
        );
        assert!(
            valid_key("c1") && !valid_key("") && !valid_key(&"k".repeat(129)) && !valid_key("é")
        );
    }

    #[derive(serde::Deserialize)]
    struct Thing {
        name: String,
        #[serde(default)]
        fail: bool,
    }

    /// A route that counts its work and creates (201), with an optional delay.
    fn app(db: Arc<Db>, done: Arc<AtomicUsize>, in_tx: bool) -> Router {
        Router::new().route(
            "/things/{id}",
            post(move |idem: Idempotent| {
                let (db, done) = (db.clone(), done.clone());
                async move {
                    if in_tx {
                        let done = done.clone();
                        return idem
                            .run_in_tx(&db, true, move |tx, _, body| {
                                let body: Thing = crate::api::body::strict(body)?;
                                tx.execute(
                                    "INSERT INTO engine_keys (name, value, created_at) VALUES ($1, 'x', 0)",
                                    &params![body.name],
                                )?;
                                if body.fail {
                                    return Err(ApiError::conflict("nope", "refused after the write"));
                                }
                                done.fetch_add(1, Ordering::SeqCst);
                                Ok((StatusCode::CREATED, json!({"made": body.name})))
                            })
                            .await;
                    }
                    idem.run(&db, true, move |_, body| async move {
                        tokio::time::sleep(Duration::from_millis(200)).await;
                        let n = done.fetch_add(1, Ordering::SeqCst) + 1;
                        let body: Value = serde_json::from_slice(&body).unwrap_or(Value::Null);
                        Ok((StatusCode::CREATED, json!({"n": n, "body": body})))
                    })
                    .await
                }
            }),
        )
    }

    async fn serve(app: Router) -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        format!("http://{addr}/things/1")
    }

    async fn send(url: &str, key: Option<&str>, body: &str) -> (u16, Value) {
        let mut req = reqwest::Client::new().post(url).body(body.to_string());
        if let Some(key) = key {
            req = req.header(HEADER, key);
        }
        let res = req.send().await.unwrap();
        let status = res.status().as_u16();
        (status, res.json().await.unwrap_or(Value::Null))
    }

    #[tokio::test]
    async fn a_key_replays_its_first_answer_and_refuses_another_request() {
        let db = Arc::new(Db::in_memory().unwrap());
        let done = Arc::new(AtomicUsize::new(0));
        let app = serve(app(db, done.clone(), false)).await;
        assert_eq!(
            send(&app, None, "{}").await.1["code"],
            "idempotency_key_required"
        );
        let first = send(&app, Some("k1"), r#"{"a": 1, "b": 2}"#).await;
        assert_eq!(first.0, 201);
        let replay = send(&app, Some("k1"), r#"{"b": 2, "a": 1}"#).await;
        assert_eq!((replay.0, &replay.1), (200, &first.1));
        let other = send(&app, Some("k1"), r#"{"a": 2}"#).await;
        assert_eq!(
            (other.0, other.1["code"].clone()),
            (409, json!("idempotency_conflict"))
        );
        assert_eq!(done.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn concurrent_requests_with_one_key_do_the_work_once() {
        let db = Arc::new(Db::in_memory().unwrap());
        let done = Arc::new(AtomicUsize::new(0));
        let app = serve(app(db, done.clone(), false)).await;
        let (a, b) = tokio::join!(send(&app, Some("k"), "{}"), send(&app, Some("k"), "{}"));
        assert_eq!(done.load(Ordering::SeqCst), 1);
        assert_eq!(a.1, b.1);
        let mut statuses = [a.0, b.0];
        statuses.sort();
        assert_eq!(statuses, [200, 201]);
    }

    #[tokio::test]
    async fn store_only_work_commits_with_its_answer_or_not_at_all() {
        let db = Arc::new(Db::in_memory().unwrap());
        let done = Arc::new(AtomicUsize::new(0));
        let app = serve(app(db.clone(), done.clone(), true)).await;
        let refused = send(&app, Some("t1"), r#"{"name": "a", "fail": true}"#).await;
        assert_eq!(refused.0, 409);
        let rows = db
            .query_json("SELECT name FROM engine_keys WHERE name = 'a'", [])
            .unwrap();
        assert!(rows.is_empty(), "rolled back");
        // The failed attempt gave the key up: the same key works again.
        let made = send(&app, Some("t1"), r#"{"name": "a"}"#).await;
        assert_eq!(made.0, 201);
        let again = send(&app, Some("t1"), r#"{"name": "a"}"#).await;
        assert_eq!((again.0, &again.1), (200, &made.1));
        assert_eq!(done.load(Ordering::SeqCst), 1);
        let unknown = send(&app, Some("t2"), r#"{"name": "b", "colour": 1}"#).await;
        assert_eq!(unknown.1["code"], "unknown_field");
    }

    /// Makes a claim look as if its owner stopped renewing it two minutes ago.
    fn lapse(db: &Db, key: &str) {
        db.transaction(|tx| {
            tx.execute(
                "UPDATE idempotency_keys SET updated_at = updated_at - 120000 WHERE idem_key = $1",
                &params![key],
            )
        })
        .unwrap();
    }

    #[test]
    fn only_a_lapsed_claim_is_taken_over_and_the_old_owner_is_fenced() {
        let db = Db::in_memory().unwrap();
        let Claim::Claimed { token: first, .. } =
            db.idempotency_claim("k", "h1", LAPSED_MS).unwrap()
        else {
            panic!("first claim");
        };
        let in_progress = Claim::InProgress {
            request_sha256: "h1".into(),
        };
        assert_eq!(
            db.idempotency_claim("k", "h1", LAPSED_MS).unwrap(),
            in_progress
        );
        // Renewed after a lapse: still the owner's.
        lapse(&db, "k");
        assert!(db.idempotency_renew("k", &first).unwrap());
        assert_eq!(
            db.idempotency_claim("k", "h1", LAPSED_MS).unwrap(),
            in_progress
        );
        // Lapsed: another request may not take it, the same request may.
        lapse(&db, "k");
        assert!(matches!(
            db.idempotency_claim("k", "h2", LAPSED_MS).unwrap(),
            Claim::InProgress { .. }
        ));
        let Claim::Claimed {
            token: second,
            recovered: true,
        } = db.idempotency_claim("k", "h1", LAPSED_MS).unwrap()
        else {
            panic!("taken over");
        };
        // The first owner can no longer renew, answer or give it up.
        assert!(!db.idempotency_renew("k", &first).unwrap());
        assert!(
            !db.idempotency_complete("k", &first, 201, Some(&json!({"id": 0})))
                .unwrap()
        );
        db.idempotency_release("k", &first).unwrap();
        assert!(
            db.idempotency_complete("k", &second, 201, Some(&json!({"id": 1})))
                .unwrap()
        );
        assert!(matches!(
            db.idempotency_claim("k", "h1", LAPSED_MS).unwrap(),
            Claim::Completed { status: 201, .. }
        ));
    }

    #[tokio::test]
    async fn a_claim_is_renewed_while_its_work_runs() {
        let db = Arc::new(Db::in_memory().unwrap());
        let (finish, finished) = tokio::sync::oneshot::channel::<()>();
        let idem = Idempotent {
            key: Some("k".into()),
            request_sha256: "h".into(),
            body: Bytes::new(),
        };
        let running = tokio::spawn({
            let db = db.clone();
            async move {
                idem.run(&db, true, move |_, _| async move {
                    let _ = finished.await;
                    Ok((StatusCode::CREATED, json!({"made": true})))
                })
                .await
                .map(|r| r.status())
            }
        });
        tokio::time::sleep(Duration::from_millis(20)).await;
        lapse(&db, "k");
        tokio::time::sleep(HEARTBEAT * 3).await;
        assert_eq!(
            db.idempotency_claim("k", "h", LAPSED_MS).unwrap(),
            Claim::InProgress {
                request_sha256: "h".into()
            },
            "renewed, so not taken over"
        );
        finish.send(()).unwrap();
        assert_eq!(running.await.unwrap().unwrap(), StatusCode::CREATED);
    }

    #[tokio::test]
    async fn store_only_work_whose_claim_was_taken_over_is_rolled_back() {
        let db = Db::in_memory().unwrap();
        let idem = Idempotent {
            key: Some("k".into()),
            request_sha256: "h".into(),
            body: Bytes::new(),
        };
        let answer = idem
            .run_in_tx(&db, true, |tx, _, _| {
                tx.execute(
                    "INSERT INTO engine_keys (name, value, created_at) VALUES ('made', 'x', 0)",
                    &[],
                )?;
                // Another request takes the claim over before this one answers.
                tx.execute("UPDATE idempotency_keys SET claim = 'someone-else'", &[])?;
                Ok((StatusCode::CREATED, json!({})))
            })
            .await;
        assert_eq!(answer.unwrap_err().code(), "idempotency_conflict");
        assert!(
            db.query_json("SELECT 1 FROM engine_keys WHERE name = 'made'", [])
                .unwrap()
                .is_empty(),
            "rolled back with its answer"
        );
    }
}
