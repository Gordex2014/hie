//! `GET /api/loads` over a real listener: the loads of every source, newest
//! first, filtered, a page at a time while new loads arrive. The loads reach
//! their states through the store calls the pipeline itself makes; the last
//! test runs a real source, deletes it and creates its id again.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use hie::db::{Db, Delivery, WorkItem};
use serde_json::{Value, json};

struct Engine {
    api: String,
    http: reqwest::Client,
    db: Arc<Db>,
    _dir: tempfile::TempDir,
}

async fn serve(router: axum::Router) -> String {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("hie=warn")
        .with_test_writer()
        .try_init();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    format!("http://{addr}")
}

/// An engine with no source running: what the tests list is what they store.
async fn engine() -> Engine {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(
        dir.path().join("hie.yaml"),
        "fhir_base_url: http://127.0.0.1:9\ndatabase: data/hie.db\n",
    )
    .unwrap();
    let (config, feeds) = hie::config::load(&dir.path().join("hie.yaml")).unwrap();
    let (_manager, state) =
        hie::build(&config, feeds, &repo().join("hie_runtime"), None, None).unwrap();
    let db = state.db.clone();
    let api = serve(hie::api::router(state)).await;
    Engine {
        api,
        http: reqwest::Client::new(),
        db,
        _dir: dir,
    }
}

impl Engine {
    async fn get(&self, path: &str) -> (u16, Value) {
        let r = self
            .http
            .get(format!("{}{path}", self.api))
            .send()
            .await
            .unwrap();
        (r.status().as_u16(), r.json().await.unwrap())
    }

    async fn page(&self, query: &str) -> Value {
        let (status, body) = self.get(&format!("/api/loads?{query}")).await;
        assert_eq!(status, 200, "{query}: {body}");
        body
    }

    /// Every id a walk returns, following `next_cursor` from the first page.
    async fn walk(&self, query: &str) -> Vec<i64> {
        let mut ids = Vec::new();
        let mut cursor = String::new();
        loop {
            let page = self.page(&format!("{query}&cursor={cursor}")).await;
            ids.extend(ids_of(&page));
            match page["next_cursor"].as_str() {
                Some(next) => cursor = next.to_string(),
                None => return ids,
            }
        }
    }

    /// A load of `source` being processed, with `rows` records; the lines in
    /// `bad` did not parse, so they are rejected as they are stored.
    fn load(&self, source: &str, name: &str, rows: i64, bad: &[i64]) -> i64 {
        let id = self
            .db
            .insert_file(
                source,
                &format!("{source} feed"),
                name,
                &format!("sha-{source}-{name}"),
                10,
                "processing",
                None,
                None,
                None,
            )
            .unwrap();
        let rows: Vec<(i64, Value, Option<String>)> = (1..=rows)
            .map(|line| {
                let error = bad
                    .contains(&line)
                    .then(|| format!("line {line} is not CSV"));
                (line, json!({"line": line}), error)
            })
            .collect();
        if !rows.is_empty() {
            self.db.insert_records(id, source, &rows).unwrap();
        }
        id
    }

    /// The id of the record at `line` of load `file`.
    fn record(&self, file: i64, line: i64) -> i64 {
        self.db
            .query_json(
                "SELECT id FROM records WHERE file_id = $1 AND line = $2",
                hie::db::params![file, line],
            )
            .unwrap()[0]["id"]
            .as_i64()
            .unwrap()
    }

    /// The record at `line` of load `file` as the pipeline selects it, among
    /// those of its source that are `received` or due for delivery.
    fn item(&self, file: i64, line: i64, received: bool) -> WorkItem {
        let id = self.record(file, line);
        let source = self
            .db
            .query_json(
                "SELECT feed_id FROM records WHERE id = $1",
                hie::db::params![id],
            )
            .unwrap()[0]["feed_id"]
            .as_str()
            .unwrap()
            .to_string();
        let items = if received {
            self.db.received_records(&source, 10_000)
        } else {
            self.db.due_records(&source, 10_000)
        };
        items.unwrap().into_iter().find(|i| i.id == id).unwrap()
    }

    /// Transformed, through the same guarded write the pipeline makes.
    fn transform(&self, file: i64, line: i64) {
        let item = self.item(file, line, true);
        assert!(self.db.mark_transformed(&item, &json!([])).unwrap());
    }

    fn send(&self, file: i64, line: i64) {
        self.transform(file, line);
        let item = self.item(file, line, false);
        self.db
            .delivered(&[(&item, Delivery::Sent(&json!({"status": "201"})))])
            .unwrap();
    }

    fn reject(&self, file: i64, line: i64, reason: &str) {
        let id = self.record(file, line);
        self.db.mark_rejected(id, "data", reason, None).unwrap();
    }

    /// Waiting for another attempt after a delivery that may yet pass.
    fn defer(&self, file: i64, line: i64) {
        self.transform(file, line);
        let item = self.item(file, line, false);
        self.db
            .delivered(&[(
                &item,
                Delivery::Deferred {
                    attempts: 1,
                    next_attempt_at: i64::MAX,
                    error: "503 from the server",
                },
            )])
            .unwrap();
    }

    fn duplicate(&self, source: &str, name: &str, of: i64) -> i64 {
        self.db
            .insert_file(
                source,
                &format!("{source} feed"),
                name,
                "sha-dup",
                10,
                "duplicate",
                Some(of),
                None,
                None,
            )
            .unwrap()
    }
}

fn repo() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

fn ids_of(page: &Value) -> Vec<i64> {
    page["loads"]
        .as_array()
        .unwrap()
        .iter()
        .map(|l| l["id"].as_i64().unwrap())
        .collect()
}

#[tokio::test]
async fn loads_of_every_source_come_newest_first_with_the_per_source_fields() {
    let e = engine().await;
    let a1 = e.load("alpha", "a1.csv", 3, &[2]);
    let b1 = e.load("beta", "b1.csv", 2, &[]);
    let a2 = e.load("alpha", "a2.csv", 1, &[]);
    e.send(a1, 1);
    e.send(b1, 1);
    e.send(b1, 2);
    e.db.finish_files("beta").unwrap();

    let page = e.page("").await;
    assert_eq!(ids_of(&page), [a2, b1, a1]);
    assert_eq!(page["next_cursor"], Value::Null);

    // Each load has exactly the fields its source's list gives it, and the one-load
    // answer the same with its failures.
    for (source, ids) in [("alpha", [a2, a1].as_slice()), ("beta", &[b1])] {
        let (status, own) = e.get(&format!("/api/sources/{source}/loads")).await;
        assert_eq!(status, 200);
        assert_eq!(own["files"], own["loads"], "the legacy envelope stays");
        assert_eq!(ids_of(&own), ids);
        for load in own["loads"].as_array().unwrap() {
            let across = page["loads"]
                .as_array()
                .unwrap()
                .iter()
                .find(|l| l["id"] == load["id"])
                .unwrap();
            assert_eq!(across, load);
            let (_, mut one) = e.get(&format!("/api/loads/{}", load["id"])).await;
            assert!(one["failures"].is_array());
            one.as_object_mut().unwrap().remove("failures");
            assert_eq!(&one, load);
        }
    }
    let a1 = page["loads"]
        .as_array()
        .unwrap()
        .iter()
        .find(|l| l["id"] == a1)
        .unwrap();
    assert_eq!(a1["source_id"], "alpha");
    assert_eq!(a1["source_name"], "alpha feed");
    assert_eq!(
        (
            &a1["records"],
            &a1["sent"],
            &a1["rejected"],
            &a1["in_progress"]
        ),
        (&json!(3), &json!(1), &json!(1), &json!(1))
    );
    assert_eq!(a1["status"], "processing");
    assert_eq!(a1["failed"], true);
    assert_eq!(
        a1["method"],
        Value::Null,
        "no source is registered under alpha"
    );

    // The per-source list keeps its own limit: no cursor, no bound.
    let (_, one) = e.get("/api/sources/alpha/loads?limit=1").await;
    assert_eq!(ids_of(&one), [a2]);
    assert!(one.get("next_cursor").is_none());
}

#[tokio::test]
async fn loads_are_filtered_by_source_status_and_failing_now() {
    let e = engine().await;
    // Done, every row sent.
    let clean = e.load("alpha", "clean.csv", 2, &[]);
    e.send(clean, 1);
    e.send(clean, 2);
    // Done with a rejected row.
    let rejected = e.load("alpha", "rejected.csv", 2, &[]);
    e.send(rejected, 1);
    e.reject(rejected, 2, "value 'high' is not a number");
    e.db.finish_files("alpha").unwrap();
    // Still in progress, with a row that did not parse.
    let parsing = e.load("alpha", "parsing.csv", 3, &[1]);
    // In progress, a row waiting for another delivery attempt: not failing.
    let waiting = e.load("beta", "waiting.csv", 1, &[]);
    e.defer(waiting, 1);
    // A file that could not be read.
    let unreadable = e.load("beta", "unreadable.csv", 0, &[]);
    e.db.fail_file(unreadable, "not UTF-8").unwrap();
    // The same content again: no rows, and not failing for its original's rows.
    let duplicate = e.duplicate("alpha", "rejected-again.csv", rejected);
    // A bulk load whose rows were refused before submission.
    let refused = e.load("beta", "refused.ndjson", 1, &[]);
    e.transform(refused, 1);
    e.db.reject_unsubmitted(refused, "server", "the server refused the submission")
        .unwrap();
    // A zip: recorded as unpacked, done once a pass finishes it.
    let zip =
        e.db.insert_file(
            "beta",
            "beta feed",
            "drop.zip",
            "sha-zip",
            10,
            "unpacked",
            None,
            None,
            None,
        )
        .unwrap();

    for limit in [1, 2, 50] {
        assert_eq!(
            e.walk(&format!("status=failed&limit={limit}")).await,
            [refused, unreadable, parsing, rejected],
            "limit {limit}"
        );
    }
    assert_eq!(
        e.walk("status=failed&source=alpha").await,
        [parsing, rejected]
    );
    assert_eq!(
        e.walk("status=failed&source=beta").await,
        [refused, unreadable]
    );
    assert_eq!(e.walk("status=unreadable").await, [unreadable]);
    assert_eq!(e.walk("status=duplicate").await, [duplicate]);
    assert_eq!(e.walk("status=unpacked").await, [zip]);
    assert_eq!(e.walk("status=done").await, [rejected, clean]);
    assert_eq!(
        e.walk("status=processing&source=beta").await,
        [refused, waiting]
    );
    assert_eq!(
        e.walk("source=alpha").await,
        [duplicate, parsing, rejected, clean]
    );
    assert_eq!(e.walk("source=nobody").await, Vec::<i64>::new());
    e.db.finish_files("beta").unwrap();
    assert_eq!(e.walk("status=unpacked").await, Vec::<i64>::new());
    assert_eq!(e.walk("status=done&source=beta").await, [zip, refused]);

    let failed = |page: &Value, id: i64| {
        page["loads"]
            .as_array()
            .unwrap()
            .iter()
            .find(|l| l["id"] == id)
            .unwrap()["failed"]
            .clone()
    };
    let all = e.page("limit=200").await;
    for (id, expected) in [
        (clean, false),
        (rejected, true),
        (parsing, true),
        (waiting, false),
        (unreadable, true),
        (duplicate, false),
        (refused, true),
        (zip, false),
    ] {
        assert_eq!(failed(&all, id), expected, "load {id}");
    }

    // Retrying every rejected row takes a load out until a row fails again.
    assert_eq!(e.db.reprocess_rejected(parsing, None).unwrap(), 1);
    assert_eq!(e.walk("status=failed&source=alpha").await, [rejected]);
    // A Retry of one reason leaves the load failing while another reason remains.
    e.reject(rejected, 1, "a second reason");
    assert_eq!(
        e.db.reprocess_rejected(rejected, Some("a second reason"))
            .unwrap(),
        1
    );
    assert_eq!(e.walk("status=failed&source=alpha").await, [rejected]);
    // Reprocessing its last rejected row takes it out too.
    assert!(e.db.reprocess(e.record(rejected, 2), None).unwrap());
    assert_eq!(
        e.walk("status=failed&source=alpha").await,
        Vec::<i64>::new()
    );
    assert_eq!(
        e.walk("status=processing&source=alpha").await,
        [parsing, rejected]
    );
}

/// A row its transform filtered out on purpose is rejected, but it is not a failure.
#[tokio::test]
async fn rows_filtered_out_on_purpose_do_not_fail_a_load() {
    let e = engine().await;
    let filter = |file: i64, line: i64| {
        let id = e.record(file, line);
        e.db.mark_rejected(id, "filtered", "transform returned no resources", None)
            .unwrap();
    };
    // Done: one row sent, one filtered out.
    let kept = e.load("zeta", "kept.csv", 2, &[]);
    e.send(kept, 1);
    filter(kept, 2);
    // Done: one row filtered out, one rejected for its data.
    let mixed = e.load("zeta", "mixed.csv", 2, &[]);
    filter(mixed, 1);
    e.reject(mixed, 2, "value 'high' is not a number");
    e.db.finish_files("zeta").unwrap();
    // In progress, only filtered out so far.
    let early = e.load("zeta", "early.csv", 2, &[]);
    filter(early, 1);

    for limit in [1, 50] {
        assert_eq!(
            e.walk(&format!("status=failed&source=zeta&limit={limit}"))
                .await,
            [mixed],
            "limit {limit}"
        );
    }
    let page = e.page("source=zeta").await;
    let load = |id: i64| {
        page["loads"]
            .as_array()
            .unwrap()
            .iter()
            .find(|l| l["id"] == id)
            .unwrap()
            .clone()
    };
    // `rejected` still counts every rejected row; `failing` and `failed` leave the
    // filtered ones out, in every load answer.
    for (id, rejected, failing, failed) in [
        (early, 1, 0, false),
        (mixed, 2, 1, true),
        (kept, 1, 0, false),
    ] {
        let l = load(id);
        assert_eq!(
            (&l["rejected"], &l["failing"], &l["failed"]),
            (&json!(rejected), &json!(failing), &json!(failed)),
            "load {id}"
        );
        let (_, one) = e.get(&format!("/api/loads/{id}")).await;
        assert_eq!(
            (&one["failing"], &one["failed"]),
            (&json!(failing), &json!(failed))
        );
    }
    let (_, own) = e.get("/api/sources/zeta/loads").await;
    assert_eq!(own["loads"], page["loads"]);
    // Retrying the data rejection leaves only a filtered row: no longer failing.
    assert_eq!(
        e.db.reprocess_rejected(mixed, Some("value 'high' is not a number"))
            .unwrap(),
        1
    );
    assert_eq!(e.walk("status=failed&source=zeta").await, Vec::<i64>::new());
    let after = e.page("source=zeta").await;
    let mixed = after["loads"]
        .as_array()
        .unwrap()
        .iter()
        .find(|l| l["id"] == mixed)
        .unwrap();
    assert_eq!(
        (&mixed["rejected"], &mixed["failing"]),
        (&json!(1), &json!(0))
    );
}

#[tokio::test]
async fn a_walk_holds_while_loads_arrive_and_change() {
    let e = engine().await;
    let ids: Vec<i64> = (1..=5)
        .map(|n| e.load("alpha", &format!("{n}.csv"), 0, &[]))
        .collect();
    let newest_first: Vec<i64> = ids.iter().rev().copied().collect();

    // Pages of two: disjoint, in order, the last one exactly full.
    let first = e.page("limit=2").await;
    assert_eq!(ids_of(&first), newest_first[..2]);
    let cursor = first["next_cursor"].as_str().unwrap().to_string();
    assert!(cursor.starts_with("a1."));

    // Loads recorded meanwhile have higher ids: they stay outside this walk.
    let late = e.load("alpha", "late.csv", 0, &[]);
    e.load("beta", "late.csv", 0, &[]);
    let second = e.page(&format!("limit=2&cursor={cursor}")).await;
    assert_eq!(ids_of(&second), newest_first[2..4]);
    let third = e
        .page(&format!(
            "limit=2&cursor={}",
            second["next_cursor"].as_str().unwrap()
        ))
        .await;
    assert_eq!(ids_of(&third), newest_first[4..]);
    assert_eq!(third["next_cursor"], Value::Null);
    // A new walk finds them; a page exactly full at the end says there is no more.
    let mut all = newest_first.clone();
    all.insert(0, late);
    assert_eq!(e.walk("limit=3&source=alpha").await, all);
    let full = e.page("limit=6&source=alpha").await;
    assert_eq!(ids_of(&full), all);
    assert_eq!(full["next_cursor"], Value::Null);
    let short = e.page("limit=5&source=alpha").await;
    assert!(short["next_cursor"].is_string());

    // With `failed`: a load that starts failing above the cursor is not in this
    // walk, one that stops failing below it is skipped, and when the rest leave
    // the filter the next page is empty and ends the walk.
    let f: Vec<i64> = (1..=4)
        .map(|n| e.load("gamma", &format!("{n}.csv"), 1, &[1]))
        .collect();
    let first = e.page("status=failed&source=gamma&limit=2").await;
    assert_eq!(ids_of(&first), [f[3], f[2]]);
    let above = e.load("gamma", "above.csv", 1, &[1]);
    e.db.reprocess_rejected(f[1], None).unwrap();
    let cursor = first["next_cursor"].as_str().unwrap();
    let second = e
        .page(&format!(
            "status=failed&source=gamma&limit=2&cursor={cursor}"
        ))
        .await;
    assert_eq!(ids_of(&second), [f[0]]);
    assert_eq!(second["next_cursor"], Value::Null);
    assert!(!ids_of(&second).contains(&above));

    let first = e.page("status=failed&source=gamma&limit=1").await;
    assert_eq!(ids_of(&first), [above]);
    for id in [f[3], f[2], f[0]] {
        e.db.reprocess_rejected(id, None).unwrap();
    }
    let cursor = first["next_cursor"].as_str().unwrap();
    let empty = e
        .page(&format!(
            "status=failed&source=gamma&limit=1&cursor={cursor}"
        ))
        .await;
    assert_eq!(ids_of(&empty), Vec::<i64>::new());
    assert_eq!(empty["next_cursor"], Value::Null);

    // A status walk with a cursor.
    assert_eq!(
        e.walk("status=processing&source=gamma&limit=2").await,
        [above, f[3], f[2], f[1], f[0]]
    );

    // Unreadable loads alone page as rejected ones do.
    let u: Vec<i64> = (1..=3)
        .map(|n| {
            let id = e.load("delta", &format!("{n}.csv"), 0, &[]);
            e.db.fail_file(id, "not UTF-8").unwrap();
            id
        })
        .collect();
    let first = e.page("status=failed&source=delta&limit=2").await;
    assert_eq!(ids_of(&first), [u[2], u[1]]);
    assert!(first["next_cursor"].is_string());
    assert_eq!(
        e.walk("status=failed&source=delta&limit=2").await,
        [u[2], u[1], u[0]]
    );

    // Mixed, a load with many rejected rows among them, one load a page.
    let many = e.load("eps", "many.csv", 4, &[1, 2, 3, 4]);
    let unreadable = e.load("eps", "unreadable.csv", 0, &[]);
    e.db.fail_file(unreadable, "not UTF-8").unwrap();
    let clean = e.load("eps", "clean.csv", 1, &[]);
    let two = e.load("eps", "two.csv", 3, &[1, 3]);
    e.send(clean, 1);
    assert_eq!(
        e.walk("status=failed&source=eps&limit=1").await,
        [two, unreadable, many]
    );
    assert_eq!(
        e.walk("status=failed&limit=1").await[..3],
        [two, unreadable, many]
    );
}

#[tokio::test]
async fn limits_and_malformed_queries() {
    let e = engine().await;
    for n in 0..201 {
        e.load("alpha", &format!("{n}.csv"), 0, &[]);
    }
    let page = e.page("").await;
    assert_eq!(ids_of(&page).len(), 50, "50 by default");
    assert!(page["next_cursor"].is_string());
    let page = e.page("limit=200").await;
    assert_eq!(ids_of(&page).len(), 200);
    let rest = e
        .page(&format!(
            "limit=200&cursor={}",
            page["next_cursor"].as_str().unwrap()
        ))
        .await;
    assert_eq!(ids_of(&rest).len(), 1);
    assert_eq!(rest["next_cursor"], Value::Null);
    let one = e.page("limit=1").await;
    assert_eq!(ids_of(&one).len(), 1);
    assert_eq!(e.walk("limit=200").await.len(), 201);

    for (query, field) in [
        ("limit=0", Some("limit")),
        ("limit=201", Some("limit")),
        ("limit=-1", Some("limit")),
        ("status=rejected", Some("status")),
        ("status=FAILED", Some("status")),
        ("cursor=42", Some("cursor")),
        ("cursor=a1.0", Some("cursor")),
        ("cursor=a1.07", Some("cursor")),
        ("cursor=l1.5", Some("cursor")),
        ("limit=abc", None),
        ("limit=1&limit=2", None),
        ("source=lab%00", Some("source")),
    ] {
        let (status, body) = e.get(&format!("/api/loads?{query}")).await;
        assert_eq!(status, 400, "{query}");
        assert_eq!(body["code"], "invalid_query", "{query}");
        assert_eq!(body["field"].as_str(), field, "{query}");
        let sentence = body["error"].as_str().unwrap();
        let value = query.split('=').nth(1).unwrap();
        assert!(
            !sentence.contains(&format!(" {value}")),
            "{query} echoed: {sentence}"
        );
    }
}

/// A source name PostgreSQL cannot hold does not stop its loads: it is not kept.
#[tokio::test]
async fn a_name_that_cannot_be_stored_is_not_kept() {
    let e = engine().await;
    let id =
        e.db.insert_file(
            "alpha",
            "lab\0feed",
            "x.csv",
            "sha-x",
            1,
            "processing",
            None,
            None,
            None,
        )
        .unwrap();
    let page = e.page("source=alpha").await;
    assert_eq!(ids_of(&page), [id]);
    assert_eq!(page["loads"][0]["source_name"], Value::Null);
}

/// A store made by an earlier version: its loads have no source name.
#[tokio::test]
async fn loads_recorded_before_the_name_was_kept_have_none() {
    if std::env::var("HIE_TEST_DATABASE_URL").is_ok_and(|v| !v.is_empty()) {
        return; // The earlier schema is written here for SQLite.
    }
    let dir = tempfile::tempdir().unwrap();
    std::fs::create_dir_all(dir.path().join("data")).unwrap();
    let old = rusqlite::Connection::open(dir.path().join("data/hie.db")).unwrap();
    old.execute_batch(
        "CREATE TABLE files (
           id INTEGER PRIMARY KEY, feed_id TEXT NOT NULL, name TEXT NOT NULL, sha256 TEXT NOT NULL,
           size INTEGER NOT NULL, status TEXT NOT NULL, duplicate_of INTEGER, received_at INTEGER NOT NULL,
           finished_at INTEGER, error TEXT, rejected_reported INTEGER NOT NULL DEFAULT 0,
           source_key TEXT, archived INTEGER NOT NULL DEFAULT 0, origin TEXT);
         INSERT INTO files (feed_id, name, sha256, size, status, received_at, finished_at)
         VALUES ('alpha', 'old.csv', 'sha', 1, 'done', 1000, 2000);",
    )
    .unwrap();
    drop(old);
    std::fs::write(
        dir.path().join("hie.yaml"),
        "fhir_base_url: http://127.0.0.1:9\ndatabase: data/hie.db\n",
    )
    .unwrap();
    let (config, feeds) = hie::config::load(&dir.path().join("hie.yaml")).unwrap();
    let (_manager, state) =
        hie::build(&config, feeds, &repo().join("hie_runtime"), None, None).unwrap();
    let api = serve(hie::api::router(state)).await;
    let page: Value = reqwest::get(format!("{api}/api/loads"))
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(page["loads"].as_array().unwrap().len(), 1);
    let load = &page["loads"][0];
    assert_eq!(
        (&load["source_id"], &load["source_name"]),
        (&json!("alpha"), &Value::Null)
    );
    assert_eq!(
        (&load["status"], &load["failed"]),
        (&json!("done"), &json!(false))
    );
    let failed: Value = reqwest::get(format!("{api}/api/loads?status=failed"))
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(failed["loads"], json!([]));
}

fn python() -> Option<String> {
    let candidates = match std::env::var("HIE_TEST_PYTHON") {
        Ok(p) => vec![p],
        Err(_) => vec!["py".into(), "python3".into(), "python".into()],
    };
    candidates.into_iter().find(|p| {
        std::process::Command::new(p)
            .arg("-c")
            .arg("import sys; sys.exit(0 if sys.version_info >= (3, 9) else 1)")
            .output()
            .is_ok_and(|o| o.status.success())
    })
}

async fn batch(axum::Json(bundle): axum::Json<Value>) -> axum::Json<Value> {
    let entries: Vec<Value> = bundle["entry"]
        .as_array()
        .unwrap()
        .iter()
        .map(|_| json!({"response": {"status": "201 Created"}}))
        .collect();
    axum::Json(json!({"resourceType": "Bundle", "type": "batch-response", "entry": entries}))
}

/// A deleted source's loads stay listed under its id and the name it had; a new
/// source under the same id lists them with its own.
#[tokio::test]
async fn a_deleted_source_keeps_its_loads_and_a_reused_id_shares_them() {
    let Some(python) = python() else {
        eprintln!("skipping: no Python 3.9+ found (set HIE_TEST_PYTHON)");
        return;
    };
    let fhir = serve(axum::Router::new().route("/", axum::routing::post(batch))).await;
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    for d in ["transforms", "sources", "inbox"] {
        std::fs::create_dir_all(root.join(d)).unwrap();
    }
    std::fs::copy(
        repo().join("transforms/lab_results.py"),
        root.join("transforms/lab_results.py"),
    )
    .unwrap();
    std::fs::write(
        root.join("hie.yaml"),
        format!("fhir_base_url: {fhir}\ndatabase: data/hie.db\npython: {python}\n"),
    )
    .unwrap();
    let (config, feeds) = hie::config::load(&root.join("hie.yaml")).unwrap();
    let (_manager, state) =
        hie::build(&config, feeds, &repo().join("hie_runtime"), None, None).unwrap();
    let api = serve(hie::api::router(state)).await;
    let http = reqwest::Client::new();
    let definition = |name: &str| {
        json!({
            "id": "labs", "name": name,
            "source": {"type": "folder", "path": "inbox", "pattern": "*.csv", "poll_seconds": 60, "min_file_age_ms": 0},
            "transform": "transforms/lab_results.py"
        })
    };
    let loads = |query: &'static str| {
        let (http, api) = (http.clone(), api.clone());
        async move {
            http.get(format!("{api}/api/loads?{query}"))
                .send()
                .await
                .unwrap()
                .json::<Value>()
                .await
                .unwrap()["loads"]
                .as_array()
                .unwrap()
                .clone()
        }
    };
    // Passes now, and waits until `count` loads of the id are done.
    let take = |count: usize| {
        let (http, api) = (http.clone(), api.clone());
        async move {
            http.post(format!("{api}/api/sources/labs/check-now"))
                .send()
                .await
                .unwrap();
            let started = Instant::now();
            loop {
                let page: Value = http
                    .get(format!("{api}/api/loads?source=labs&status=done"))
                    .send()
                    .await
                    .unwrap()
                    .json()
                    .await
                    .unwrap();
                if page["loads"].as_array().unwrap().len() == count {
                    return;
                }
                assert!(
                    started.elapsed() < Duration::from_secs(30),
                    "timed out waiting for {count} loads"
                );
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
    };

    let created = http
        .post(format!("{api}/api/sources"))
        .json(&definition("Lab results"))
        .send()
        .await
        .unwrap();
    assert_eq!(created.status(), 201);
    // Written under another name and renamed, so a pass never reads half a file.
    std::fs::copy(
        repo().join("examples/data/labs.csv"),
        root.join("inbox/first.tmp"),
    )
    .unwrap();
    std::fs::rename(root.join("inbox/first.tmp"), root.join("inbox/first.csv")).unwrap();
    take(1).await;
    let deleted = http
        .delete(format!("{api}/api/sources/labs"))
        .send()
        .await
        .unwrap();
    assert!(deleted.status().is_success());

    let after_delete = loads("source=labs").await;
    assert_eq!(after_delete.len(), 1);
    assert_eq!(after_delete[0]["name"], "first.csv");
    assert_eq!(after_delete[0]["source_name"], "Lab results");
    assert_eq!(after_delete[0]["method"], Value::Null);

    // The same id again, under another name: it shares the earlier history.
    let created = http
        .post(format!("{api}/api/sources"))
        .json(&definition("Lab results v2"))
        .send()
        .await
        .unwrap();
    assert_eq!(created.status(), 201);
    // Other content, so it is not a duplicate of the first.
    let mut second = std::fs::read_to_string(repo().join("examples/data/labs.csv")).unwrap();
    second.push('\n');
    std::fs::write(root.join("inbox/second.tmp"), second).unwrap();
    std::fs::rename(root.join("inbox/second.tmp"), root.join("inbox/second.csv")).unwrap();
    take(2).await;
    let both = loads("source=labs").await;
    let names: Vec<(&str, &str)> = both
        .iter()
        .map(|l| {
            (
                l["name"].as_str().unwrap(),
                l["source_name"].as_str().unwrap(),
            )
        })
        .collect();
    assert_eq!(
        names,
        [
            ("second.csv", "Lab results v2"),
            ("first.csv", "Lab results")
        ]
    );
    assert_eq!(loads("").await.len(), 2);
}
