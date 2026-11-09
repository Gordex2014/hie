//! HL7 v2 in: a file of messages through a folder source, and messages over
//! MLLP, each spooled to disk before its ACK and then delivered as a Patient.

use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use axum::extract::State;
use axum::routing::post;
use axum::{Json, Router};
use hie::alerts::Alerter;
use hie::db::Db;
use hie::fhir::FhirClient;
use hie::hl7;
use hie::pipeline::FeedRunner;
use hie::source::Source;
use serde_json::{Value, json};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const A01: &str = "MSH|^~\\&|LAB|NORTH|HIE|HFS|20261021120000||ADT^A01|MSG0001|P|2.5.1\rEVN|A01|20261021120000\rPID|1||12345^^^NORTH^MR||Doe^Jane^M||19800101|F\r";
const A08: &str = "MSH|^~\\&|LAB|NORTH|HIE|HFS|20261021120100||ADT^A08|MSG0002|P|2.5.1\rEVN|A08|20261021120100\rPID|1||67890^^^NORTH^MR||Roe^Richard||19751231|M\r";
const ORU: &str = "MSH|^~\\&|LAB|NORTH|HIE|HFS|20261021120200||ORU^R01|MSG0003|P|2.5.1\rPID|1||12345^^^NORTH^MR\rOBX|1|NM|2345-7^Glucose^LN||99|mg/dL\r";

type Urls = Arc<Mutex<Vec<(String, Value)>>>;

async fn batch(State(urls): State<Urls>, Json(bundle): Json<Value>) -> Json<Value> {
    let mut entries = Vec::new();
    for e in bundle["entry"].as_array().unwrap() {
        urls.lock().unwrap().push((
            e["request"]["url"].as_str().unwrap().to_string(),
            e["resource"].clone(),
        ));
        entries.push(json!({"response": {"status": "201 Created"}}));
    }
    Json(json!({"resourceType": "Bundle", "type": "batch-response", "entry": entries}))
}

async fn serve(router: Router) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    format!("http://{addr}")
}

fn python() -> Option<&'static str> {
    ["py", "python3", "python"].into_iter().find(|p| {
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

fn runner(
    feed: hie::config::FeedConfig,
    source: Source,
    db: Arc<Db>,
    hfs: &str,
    python: &str,
) -> FeedRunner {
    FeedRunner::with_source(
        feed,
        source,
        db.clone(),
        Arc::new(FhirClient::new(hfs)),
        Arc::new(Alerter::new(db, None)),
        python,
        &repo().join("hie_runtime"),
        hfs,
        "http://127.0.0.1:0",
    )
}

#[tokio::test]
async fn a_file_of_messages_becomes_patients() {
    let Some(python) = python() else {
        eprintln!("skipping: no Python 3.9+ found");
        return;
    };
    let urls: Urls = Arc::default();
    let hfs = serve(
        Router::new()
            .route("/", post(batch))
            .with_state(urls.clone()),
    )
    .await;
    let dir = tempfile::tempdir().unwrap();
    std::fs::create_dir_all(dir.path().join("inbox")).unwrap();
    // Three messages, one of them not an ADT, separated the way a file from a
    // Windows editor would be.
    std::fs::write(
        dir.path().join("inbox/adt.hl7"),
        format!(
            "{}\r\n\r\n{}\r\n{}",
            A01.replace('\r', "\r\n"),
            ORU.replace('\r', "\r\n"),
            A08.replace('\r', "\r\n")
        ),
    )
    .unwrap();
    let mut feed: hie::config::FeedConfig = serde_yaml::from_str(
        "id: adt\nname: ADT\nsource:\n  type: folder\n  path: inbox\n  pattern: \"*.hl7\"\n  min_file_age_ms: 0\nformat: hl7v2\ntransform: transforms/hl7_adt_patient.py\n",
    )
    .unwrap();
    feed.resolve(&repo()).unwrap();
    if let hie::config::SourceConfig::Folder { path, .. } = &mut feed.source {
        *path = dir.path().join("inbox");
    }
    let db = Arc::new(Db::in_memory().unwrap());
    let runner = runner(
        feed.clone(),
        Source::from_config(&feed).unwrap(),
        db.clone(),
        &hfs,
        python,
    );
    runner.tick().await.unwrap();

    let counts = db.feed_counts("adt").unwrap();
    assert_eq!(
        (counts["sent"].as_i64(), counts["rejected"].as_i64()),
        (Some(2), Some(1)),
        "{counts}"
    );
    let sent = urls.lock().unwrap();
    assert_eq!(sent[0].0, "Patient?identifier=urn:helios:hie:mrn|12345");
    assert_eq!(sent[0].1["name"][0]["family"], "Doe");
    assert_eq!(sent[0].1["name"][0]["given"][0], "Jane");
    assert_eq!(sent[0].1["birthDate"], "1980-01-01");
    assert_eq!(sent[0].1["gender"], "female");
    assert_eq!(sent[1].1["name"][0]["family"], "Roe");
    let rejected = db
        .query_json(
            "SELECT line, error, raw FROM records WHERE status = 'rejected'",
            [],
        )
        .unwrap();
    assert_eq!(rejected[0]["line"], 6);
    assert!(
        rejected[0]["error"]
            .as_str()
            .unwrap()
            .contains("not an ADT message: ORU^R01"),
        "{}",
        rejected[0]
    );
    assert_eq!(rejected[0]["raw"]["control_id"], "MSG0003");
    assert_eq!(rejected[0]["raw"]["segments"][2]["fields"][4], "99");
}

async fn send(stream: &mut tokio::net::TcpStream, message: &str) -> String {
    stream.write_all(&hl7::frame(message)).await.unwrap();
    let mut buf = Vec::new();
    let mut chunk = [0u8; 4096];
    loop {
        let n = stream.read(&mut chunk).await.unwrap();
        assert!(n > 0, "connection closed before an ACK");
        buf.extend_from_slice(&chunk[..n]);
        if buf.ends_with(&hl7::END) {
            break;
        }
    }
    assert_eq!(buf[0], hl7::START);
    String::from_utf8(buf[1..buf.len() - 2].to_vec()).unwrap()
}

#[tokio::test]
async fn messages_over_mllp_are_spooled_acknowledged_and_delivered() {
    let Some(python) = python() else {
        eprintln!("skipping: no Python 3.9+ found");
        return;
    };
    let urls: Urls = Arc::default();
    let hfs = serve(
        Router::new()
            .route("/", post(batch))
            .with_state(urls.clone()),
    )
    .await;
    let port = {
        let probe = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        probe.local_addr().unwrap().port()
    };
    let dir = tempfile::tempdir().unwrap();
    let mut feed: hie::config::FeedConfig = serde_yaml::from_str(&format!(
        "id: adt-mllp\nname: ADT over MLLP\nsource:\n  type: mllp\n  listen: 127.0.0.1:{port}\n  spool: spool\ntransform: transforms/hl7_adt_patient.py\n"
    ))
    .unwrap();
    feed.resolve(&repo()).unwrap();
    assert_eq!(feed.format, hie::config::Format::Hl7v2);
    if let hie::config::SourceConfig::Mllp { spool, .. } = &mut feed.source {
        *spool = dir.path().join("spool");
    }
    let db = Arc::new(Db::in_memory().unwrap());
    let runner = runner(
        feed.clone(),
        Source::from_config(&feed).unwrap(),
        db.clone(),
        &hfs,
        python,
    );

    // Nothing listens until the source starts.
    assert!(
        tokio::net::TcpStream::connect(("127.0.0.1", port))
            .await
            .is_err()
    );
    runner.source().start().await.unwrap();
    runner.source().start().await.unwrap(); // again is fine

    let mut sender = tokio::net::TcpStream::connect(("127.0.0.1", port))
        .await
        .unwrap();
    let ack = send(&mut sender, A01).await;
    assert!(ack.contains("|ACK^A01^ACK|"), "{ack}");
    assert!(ack.ends_with("MSA|AA|MSG0001|\r"), "{ack}");
    // Acknowledged means on disk.
    let spooled: Vec<_> = std::fs::read_dir(dir.path().join("spool"))
        .unwrap()
        .map(|e| e.unwrap().file_name().into_string().unwrap())
        .collect();
    assert_eq!(spooled.len(), 1);
    assert!(spooled[0].ends_with("-MSG0001.hl7"), "{spooled:?}");

    // Two in one write, then junk, on the same connection.
    let mut both = hl7::frame(A08);
    both.extend(hl7::frame(ORU));
    sender.write_all(&both).await.unwrap();
    let mut acks = String::new();
    while acks.matches("MSA|").count() < 2 {
        let mut chunk = [0u8; 4096];
        let n = sender.read(&mut chunk).await.unwrap();
        acks.push_str(&String::from_utf8_lossy(&chunk[..n]));
    }
    assert!(
        acks.contains("MSA|AA|MSG0002|") && acks.contains("MSA|AA|MSG0003|"),
        "{acks}"
    );
    let nack = send(&mut sender, "hello, not HL7").await;
    assert!(nack.contains("MSA|AR||"), "{nack}");
    assert!(nack.contains("MSH segment"), "{nack}");

    // A pass takes the spool: two Patients, the ORU rejected by the transform.
    runner.tick().await.unwrap();
    let counts = db.feed_counts("adt-mllp").unwrap();
    assert_eq!(
        (
            counts["files"].as_i64(),
            counts["sent"].as_i64(),
            counts["rejected"].as_i64()
        ),
        (Some(3), Some(2), Some(1)),
        "{counts}"
    );
    assert_eq!(urls.lock().unwrap()[1].1["name"][0]["family"], "Roe");
    assert!(
        dir.path()
            .join("spool/processed")
            .read_dir()
            .unwrap()
            .count()
            == 3
    );

    // The same message again is a duplicate, not a second Patient update.
    let ack = send(&mut sender, A01).await;
    assert!(ack.ends_with("MSA|AA|MSG0001|\r"));
    runner.tick().await.unwrap();
    let counts = db.feed_counts("adt-mllp").unwrap();
    assert_eq!(
        (counts["duplicate_files"].as_i64(), counts["sent"].as_i64()),
        (Some(1), Some(2))
    );

    let probe = runner.source().probe().await.unwrap();
    assert!(probe.reachable);
    assert_eq!(probe.objects, 0);
}

#[test]
fn an_mllp_source_is_checked_on_resolve() {
    let mut feed: hie::config::FeedConfig = serde_yaml::from_str(
        "id: x\nname: X\nsource:\n  type: mllp\n  listen: \"2575\"\ntransform: transforms/hl7_adt_patient.py\n",
    )
    .unwrap();
    let err = format!("{:#}", feed.resolve(&repo()).unwrap_err());
    assert!(err.contains("0.0.0.0:2575"), "{err}");
    let mut feed: hie::config::FeedConfig = serde_yaml::from_str(
        "id: x\nname: X\nsource:\n  type: mllp\n  listen: 0.0.0.0:2575\ntransform: transforms/hl7_adt_patient.py\ndelivery:\n  method: bulk_submit\n  bulk_from: source_objects\n",
    )
    .unwrap();
    let err = format!("{:#}", feed.resolve(&repo()).unwrap_err());
    assert!(
        err.contains("mllp source has no file for HFS to fetch"),
        "{err}"
    );
    let mut feed: hie::config::FeedConfig = serde_yaml::from_str(
        "id: x\nname: X\nsource:\n  type: mllp\n  listen: 0.0.0.0:2575\ntransform: transforms/hl7_adt_patient.py\n",
    )
    .unwrap();
    feed.resolve(&repo()).unwrap();
    assert_eq!(
        feed.source.describe(),
        format!(
            "mllp://0.0.0.0:2575 (spool {})",
            repo().join("spool").display()
        )
    );
}
