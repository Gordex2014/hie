//! The engine calling HFS as itself: a token from a stand-in identity provider,
//! cached, refreshed when it expires, and dropped when HFS stops accepting it.

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::routing::post;
use axum::{Form, Json, Router};
use hie::credentials::{CredentialSettings, TokenSource};
use hie::pipeline::FeedRunner;
use serde_json::{Value, json};

#[derive(Default)]
struct World {
    /// Every token the provider issued, in order.
    issued: Vec<String>,
    /// What the form of the last token request carried.
    last_form: Vec<(String, String)>,
    /// Lifetime the provider announces for the next token.
    expires_in: u64,
    /// Tokens HFS still accepts.
    accepted: Vec<String>,
    /// Bundles HFS stored, with the token each came with.
    stored: Vec<String>,
}

type Shared = Arc<Mutex<World>>;

async fn token(State(w): State<Shared>, Form(form): Form<Vec<(String, String)>>) -> Json<Value> {
    let mut w = w.lock().unwrap();
    let t = format!("t{}", w.issued.len() + 1);
    w.issued.push(t.clone());
    w.accepted.push(t.clone());
    w.last_form = form;
    Json(json!({"access_token": t, "token_type": "Bearer", "expires_in": w.expires_in}))
}

async fn batch(
    State(w): State<Shared>,
    headers: HeaderMap,
    Json(bundle): Json<Value>,
) -> (StatusCode, Json<Value>) {
    let mut w = w.lock().unwrap();
    let bearer = headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .map(str::to_string);
    let Some(t) = bearer.filter(|t| w.accepted.contains(t)) else {
        return (
            StatusCode::UNAUTHORIZED,
            Json(json!({"resourceType": "OperationOutcome"})),
        );
    };
    w.stored.push(t);
    let entries: Vec<Value> = bundle["entry"]
        .as_array()
        .unwrap()
        .iter()
        .map(|_| json!({"response": {"status": "201 Created"}}))
        .collect();
    (
        StatusCode::OK,
        Json(json!({"resourceType": "Bundle", "type": "batch-response", "entry": entries})),
    )
}

async fn serve(router: Router) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    format!("http://{addr}")
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

fn repo() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

fn drop_file(inbox: &Path, name: &str, content: &str) {
    std::fs::create_dir_all(inbox).unwrap();
    std::fs::write(inbox.join(name), content).unwrap();
}

fn count(state: &hie::api::AppState, status: &str) -> i64 {
    state.db.feed_counts("lab-results").unwrap()[status]
        .as_i64()
        .unwrap_or(0)
}

/// Ends the backoff of the source's deferred rows, as the time running out would.
fn make_pending_due(database: &Path, expected: usize) {
    let conn = rusqlite::Connection::open(database).unwrap();
    conn.busy_timeout(std::time::Duration::from_secs(5))
        .unwrap();
    let n = conn
        .execute(
            "UPDATE records SET next_attempt_at = 0 WHERE feed_id = 'lab-results' AND status = 'pending'",
            [],
        )
        .unwrap();
    assert_eq!(n, expected);
}

#[tokio::test]
async fn the_engine_fetches_caches_refreshes_and_drops_its_token() {
    let Some(python) = python() else {
        eprintln!("skipping: no Python 3.9+ found (set HIE_TEST_PYTHON)");
        return;
    };
    let world: Shared = Arc::new(Mutex::new(World {
        expires_in: 3600,
        ..World::default()
    }));
    let idp = serve(
        Router::new()
            .route("/token", post(token))
            .with_state(world.clone()),
    )
    .await;
    let hfs = serve(
        Router::new()
            .route("/", post(batch))
            .with_state(world.clone()),
    )
    .await;

    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    std::fs::create_dir_all(root.join("sources")).unwrap();
    std::fs::create_dir_all(root.join("transforms")).unwrap();
    std::fs::copy(
        repo().join("transforms/lab_results.py"),
        root.join("transforms/lab_results.py"),
    )
    .unwrap();
    std::fs::write(
        root.join("hie.yaml"),
        format!("fhir_base_url: {hfs}\ndatabase: data/hie.db\npython: {python}\n"),
    )
    .unwrap();
    std::fs::write(
        root.join("sources/lab-results.yaml"),
        "id: lab-results\nname: Lab results\nsource:\n  type: folder\n  path: inbox\n  pattern: \"*.csv\"\n  min_file_age_ms: 0\n\
         transform: transforms/lab_results.py\ndelivery:\n  initial_backoff_ms: 3600000\n  max_backoff_ms: 3600000\n  alert_after_attempts: 2\n",
    )
    .unwrap();
    let source = Arc::new(TokenSource::new(CredentialSettings {
        token_url: format!("{idp}/token"),
        client_id: "hie".into(),
        client_secret: "s3cret".into(),
        scope: Some("system/*.cruds".into()),
    }));
    source.check().await.unwrap();
    {
        let w = world.lock().unwrap();
        assert_eq!(w.issued, ["t1"]);
        let form: Vec<(&str, &str)> = w
            .last_form
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect();
        assert_eq!(
            form,
            [
                ("grant_type", "client_credentials"),
                ("client_id", "hie"),
                ("client_secret", "s3cret"),
                ("scope", "system/*.cruds"),
            ]
        );
    }

    let (config, feeds) = hie::config::load(&root.join("hie.yaml")).unwrap();
    let (manager, state) = hie::build(
        &config,
        feeds,
        &repo().join("hie_runtime"),
        None,
        Some(source.clone()),
    )
    .unwrap();
    let runner: Arc<FeedRunner> = manager.runner("lab-results").unwrap();
    manager.pause("lab-results", None).unwrap();
    let inbox = root.join("inbox");
    let labs = std::fs::read_to_string(repo().join("examples/data/labs.csv")).unwrap();

    // First load: the check's token is still good, so no second request to the provider.
    drop_file(&inbox, "labs.csv", &labs);
    runner.tick().await.unwrap();
    assert_eq!(count(&state, "sent"), 3);
    {
        let w = world.lock().unwrap();
        assert_eq!(w.issued, ["t1"]);
        assert_eq!(w.stored, ["t1"]);
    }

    // HFS stops accepting the token (revoked, rotated): the rows wait, the engine
    // forgets the token and the next pass goes out with a fresh one.
    world.lock().unwrap().accepted.clear();
    drop_file(&inbox, "labs2.csv", &labs.replace("Patient/", "Patient/2"));
    runner.tick().await.unwrap();
    assert_eq!(
        count(&state, "pending"),
        3,
        "{:?}",
        state.db.feed_counts("lab-results")
    );
    assert_eq!(count(&state, "rejected"), 0);
    let pending = state
        .db
        .query_json("SELECT error FROM records WHERE status = 'pending'", [])
        .unwrap();
    assert!(
        pending[0]["error"]
            .as_str()
            .unwrap()
            .contains("did not accept the engine's token"),
        "{pending:?}"
    );
    // The backoff is an hour, so the refused rows cannot come due inside the
    // tick that deferred them (its final drain would send them with a fresh
    // token); they are made due here instead.
    make_pending_due(&root.join("data/hie.db"), 3);
    runner.tick().await.unwrap();
    assert_eq!(count(&state, "sent"), 6);
    {
        let w = world.lock().unwrap();
        assert_eq!(w.issued, ["t1", "t2"]);
        assert_eq!(w.stored, ["t1", "t2"]);
    }

    // A token announced as expiring at once is not reused.
    world.lock().unwrap().expires_in = 1;
    source.invalidate().await;
    assert_eq!(source.bearer().await.unwrap(), "t3");
    assert_eq!(source.bearer().await.unwrap(), "t4");
    world.lock().unwrap().expires_in = 3600;
    assert_eq!(source.bearer().await.unwrap(), "t5");
    assert_eq!(source.bearer().await.unwrap(), "t5");
}

#[tokio::test]
async fn an_unreachable_or_refusing_provider_is_a_transient_failure() {
    let world: Shared = Arc::default();
    let idp = serve(
        Router::new()
            .route(
                "/token",
                post(|| async {
                    (
                        StatusCode::UNAUTHORIZED,
                        Json(json!({"error": "invalid_client"})),
                    )
                }),
            )
            .with_state(world),
    )
    .await;
    let refusing = TokenSource::new(CredentialSettings {
        token_url: format!("{idp}/token"),
        client_id: "hie".into(),
        client_secret: "wrong".into(),
        scope: None,
    });
    let err = refusing.check().await.unwrap_err();
    assert!(format!("{err:#}").contains("401"), "{err:#}");
    match refusing.bearer().await {
        Err(hie::fhir::SendError::Transient(m)) => assert!(m.contains("invalid_client"), "{m}"),
        other => panic!("expected a transient failure, got {other:?}"),
    }

    let unreachable = TokenSource::new(CredentialSettings {
        token_url: "http://127.0.0.1:9/token".into(),
        client_id: "hie".into(),
        client_secret: "x".into(),
        scope: None,
    });
    match unreachable.bearer().await {
        Err(hie::fhir::SendError::Transient(m)) => assert!(m.contains("cannot reach"), "{m}"),
        other => panic!("expected a transient failure, got {other:?}"),
    }
}

#[test]
fn incomplete_settings_are_refused_and_absent_ones_mean_anonymous() {
    // Environment variables are process-wide; this test owns the HIE_FHIR_* ones.
    for v in [
        "HIE_FHIR_TOKEN_URL",
        "HIE_FHIR_CLIENT_ID",
        "HIE_FHIR_CLIENT_SECRET",
        "HIE_FHIR_SCOPE",
    ] {
        unsafe { std::env::remove_var(v) };
    }
    assert!(CredentialSettings::from_env().unwrap().is_none());
    unsafe { std::env::set_var("HIE_FHIR_TOKEN_URL", "http://idp/token") };
    let err = CredentialSettings::from_env().unwrap_err().to_string();
    assert!(
        err.contains("HIE_FHIR_CLIENT_ID, HIE_FHIR_CLIENT_SECRET"),
        "{err}"
    );
    unsafe {
        std::env::set_var("HIE_FHIR_CLIENT_ID", "hie");
        std::env::set_var("HIE_FHIR_CLIENT_SECRET", "s");
    }
    let s = CredentialSettings::from_env().unwrap().unwrap();
    assert_eq!((s.client_id.as_str(), s.scope), ("hie", None));
    for v in [
        "HIE_FHIR_TOKEN_URL",
        "HIE_FHIR_CLIENT_ID",
        "HIE_FHIR_CLIENT_SECRET",
    ] {
        unsafe { std::env::remove_var(v) };
    }
}
