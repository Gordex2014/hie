//! What `$bulk-submit` delivery leaves in the logs, the alerts and the stored
//! submissions, at every level the production filter lets through: the operation,
//! the status and the error, never the signatures and tokens a URL carries. A test
//! binary of its own, so its log subscriber is the process's.

use std::path::PathBuf;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use axum::Router;
use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Json, Response};
use axum::routing::{get, post};
use serde_json::json;
use tracing_subscriber::layer::{Layer, SubscriberExt};
use tracing_subscriber::util::SubscriberInitExt;

/// `HIE_LOG` for these tests: everything at TRACE, and the statement parameters
/// asked for by name, which the production filter refuses anyway.
const HIE_LOG: &str = "trace,tokio_postgres::query=trace";

/// Everything the process logs.
#[derive(Clone, Default)]
struct Captured(Arc<Mutex<Vec<u8>>>);

impl std::io::Write for Captured {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(buf);
        Ok(buf.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// The process's log, through the production filter, installed once.
fn logs() -> &'static Captured {
    static LOGS: OnceLock<Captured> = OnceLock::new();
    LOGS.get_or_init(|| {
        let captured = Captured::default();
        let writer = captured.clone();
        tracing_subscriber::registry()
            .with(
                tracing_subscriber::fmt::layer()
                    .with_ansi(false)
                    .with_writer(move || writer.clone())
                    .with_filter(hie::log_filter(Some(HIE_LOG))),
            )
            .init();
        captured
    })
}

fn logged() -> String {
    String::from_utf8_lossy(&logs().0.lock().unwrap()).into_owned()
}

async fn bind() -> (tokio::net::TcpListener, String) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    (listener, url)
}

fn repo() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

/// A FHIR server's status endpoints: `poll` in progress, `done` with receipts
/// that cannot be fetched, `refused` with a diagnostic naming a signed URL, and
/// anything else failing.
async fn status(State(base): State<String>, Path(which): Path<String>) -> Response {
    match which.as_str() {
        "poll" => StatusCode::ACCEPTED.into_response(),
        "done" => Json(json!({"output": [{"type": "Patient",
            "url": format!("{base}/receipts/1?X-Amz-Signature=CANARY-RECEIPT-SIGNATURE&X-Amz-Security-Token=CANARY-RECEIPT-TOKEN"),
            "count": 1}], "outcome": []}))
        .into_response(),
        "refused" => (
            StatusCode::BAD_REQUEST,
            Json(json!({"resourceType": "OperationOutcome", "issue": [{"severity": "error", "code": "processing",
                "diagnostics": "could not fetch https://b.s3.amazonaws.com/a.ndjson?X-Amz-Signature=CANARY-REFUSED-SIGNATURE&X-Amz-Security-Token=CANARY-REFUSED-TOKEN"}]})),
        )
            .into_response(),
        _ => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    }
}

/// The status kick-off answers with a status URL that carries credentials.
async fn status_kickoff(State(base): State<String>) -> Response {
    let mut headers = HeaderMap::new();
    headers.insert(
        "content-location",
        format!("{base}/status/poll?X-Amz-Signature=CANARY-RUNTIME-SIGNATURE&X-Amz-Security-Token=CANARY-RUNTIME-TOKEN")
            .parse()
            .unwrap(),
    );
    (StatusCode::ACCEPTED, headers).into_response()
}

/// The stand-in FHIR server, at the returned URL.
async fn fhir_server() -> String {
    let (listener, base) = bind().await;
    let app = Router::new()
        .route(
            "/$bulk-submit",
            post(|| async { Json(json!({"resourceType": "OperationOutcome", "issue": []})) }),
        )
        .route("/$bulk-submit-status", post(status_kickoff))
        .route("/status/{which}", get(status))
        .route(
            "/receipts/{n}",
            get(|| async { StatusCode::INTERNAL_SERVER_ERROR }),
        )
        .with_state(base.clone());
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    base
}

fn runner(
    db: &Arc<hie::db::Db>,
    fhir: hie::fhir::FhirClient,
    base: &str,
) -> hie::pipeline::FeedRunner {
    let mut feed: hie::config::FeedConfig = serde_yaml::from_str(
        "id: drops\nname: Drops\nsource:\n  type: s3\n  bucket: unused\nformat: ndjson\ntransform: transforms/fhir_passthrough.py\n\
         delivery:\n  method: bulk_submit\n  bulk_from: source_objects\n  initial_backoff_ms: 10\n",
    )
    .unwrap();
    feed.delivery.alert_after_attempts = 1;
    let store = Arc::new(object_store::memory::InMemory::new());
    hie::pipeline::FeedRunner::with_source(
        feed,
        hie::source::Source::Object(hie::source::ObjectSource::new(
            store,
            "",
            "processed",
            "*.ndjson",
            Duration::ZERO,
        )),
        db.clone(),
        Arc::new(fhir),
        Arc::new(hie::alerts::Alerter::new(db.clone(), None)),
        "python3",
        &repo().join("hie_runtime"),
        base,
        "http://engine.test",
    )
}

/// A submission of one record, waiting for its kick-off or, with a status URL,
/// already running.
fn submission(db: &hie::db::Db, which: &str, status: Option<&str>) {
    let file = db
        .insert_file(
            "drops",
            &format!("{which}.ndjson"),
            which,
            1,
            "processing",
            None,
            None,
            None,
        )
        .unwrap();
    db.insert_records(
        file,
        "drops",
        &[(1, json!({"resourceType": "Patient", "id": which}), None)],
    )
    .unwrap();
    let record = db
        .query_json(
            "SELECT id FROM records WHERE file_id = $1",
            hie::db::params![file],
        )
        .unwrap()[0]["id"]
        .as_i64()
        .unwrap();
    db.mark_passthrough(&[record]).unwrap();
    let token = format!("hie-canary-{which}");
    db.create_submission_for_files(&token, "drops", &token, &json!({"output": []}), &[file])
        .unwrap();
    if let Some(status) = status {
        db.update_submission(&token, "running", Some(status), 0, 0, None)
            .unwrap();
    }
}

/// The stored reasons: the submissions' errors, the records' errors, the alerts.
fn stored(db: &hie::db::Db) -> (String, String, String) {
    let column = |sql: &str, name: &str| {
        db.query_json(sql, [])
            .unwrap()
            .iter()
            .map(|r| r[name].to_string())
            .collect::<Vec<_>>()
            .join("\n")
    };
    (
        column("SELECT error FROM submissions", "error"),
        column("SELECT error FROM records", "error"),
        column("SELECT message FROM alerts", "message"),
    )
}

/// A failing status poll, and receipts that cannot be fetched.
#[tokio::test]
async fn a_failing_poll_never_puts_credentials_in_logs_or_alerts() {
    logs();
    let base = fhir_server().await;
    let db = Arc::new(hie::db::Db::in_memory().unwrap());
    let runner = runner(&db, hie::fhir::FhirClient::new(&base), &base);
    for which in ["failing", "done"] {
        let status = format!(
            "{base}/status/{which}?X-Amz-Signature=CANARY-STATUS-SIGNATURE&X-Amz-Security-Token=CANARY-STATUS-TOKEN"
        );
        submission(&db, which, Some(&status));
    }
    runner.tick().await.unwrap();

    let logs = logged();
    let (submissions, _, alerts) = stored(&db);
    assert!(logs.contains("HTTP 500 polling"), "{logs}");
    // Both failures are stored: the failed poll and the receipts not fetched.
    assert!(
        !alerts.is_empty()
            && submissions.contains("HTTP 500 polling")
            && submissions.contains("fetching"),
        "{submissions}\n{alerts}"
    );
    for text in [&logs, &submissions, &alerts] {
        assert!(!text.contains("CANARY-STATUS"), "{text}");
        assert!(!text.contains("CANARY-RECEIPT"), "{text}");
    }
}

/// A refusal whose diagnostic names the signed URL the server could not fetch.
#[tokio::test]
async fn a_refusal_that_echoes_a_signed_url_keeps_it_out_of_logs_and_records() {
    logs();
    let base = fhir_server().await;
    let db = Arc::new(hie::db::Db::in_memory().unwrap());
    let runner = runner(&db, hie::fhir::FhirClient::new(&base), &base);
    submission(&db, "refused", Some(&format!("{base}/status/refused")));
    runner.tick().await.unwrap();

    let logs = logged();
    let (submissions, records, alerts) = stored(&db);
    assert!(logs.contains("refused: HTTP 400"), "{logs}");
    assert!(
        submissions.contains("HTTP 400") && submissions.contains("https://b.s3.amazonaws.com/…"),
        "{submissions}"
    );
    assert!(records.contains("HTTP 400"), "{records}");
    for text in [&logs, &submissions, &records, &alerts] {
        assert!(!text.contains("CANARY-REFUSED"), "{text}");
    }
}

/// A token endpoint, named with a credential in its URL, that cannot be reached
/// when a submission is kicked off.
#[tokio::test]
async fn a_token_endpoint_failure_keeps_its_url_out_of_logs_and_alerts() {
    logs();
    let base = fhir_server().await;
    let closed = {
        let (listener, url) = bind().await;
        drop(listener);
        url
    };
    let tokens = hie::credentials::TokenSource::new(hie::credentials::CredentialSettings {
        token_url: format!("{closed}/token?X-Amz-Security-Token=CANARY-AUTH-TOKEN"),
        client_id: "hie".into(),
        client_secret: "secret".into(),
        scope: None,
    });
    let db = Arc::new(hie::db::Db::in_memory().unwrap());
    let runner = runner(
        &db,
        hie::fhir::FhirClient::with_credentials(&base, Some(Arc::new(tokens))),
        &base,
    );
    submission(&db, "auth", None);
    runner.tick().await.unwrap();

    let logs = logged();
    let (submissions, _, alerts) = stored(&db);
    let origin = closed.clone() + "/…";
    assert!(
        submissions.contains("token from") && submissions.contains(&origin),
        "{submissions}"
    );
    assert!(alerts.contains(&origin), "{alerts}");
    for text in [&logs, &submissions, &alerts] {
        assert!(!text.contains("CANARY-AUTH"), "{text}");
    }
}

/// A delivery that works: the status URL the server hands back, credentials and
/// all, is stored, and no event at any level shows it, statement parameters of
/// the store included (on PostgreSQL, with `HIE_TEST_DATABASE_URL`).
#[tokio::test]
async fn a_status_url_stored_during_delivery_is_never_logged() {
    logs();
    let base = fhir_server().await;
    let db = Arc::new(hie::db::Db::in_memory().unwrap());
    let runner = runner(&db, hie::fhir::FhirClient::new(&base), &base);
    submission(&db, "runtime", None);
    runner.tick().await.unwrap();

    let kept = db
        .query_json("SELECT status, status_url FROM submissions", [])
        .unwrap();
    assert_eq!(kept[0]["status"], "running", "{kept:?}");
    assert!(
        kept[0]["status_url"]
            .as_str()
            .unwrap()
            .contains("CANARY-RUNTIME"),
        "{kept:?}"
    );
    let logs = logged();
    assert!(logs.contains(" TRACE "), "the filter lets TRACE through");
    assert!(!logs.contains("CANARY-RUNTIME"), "{logs}");
}
