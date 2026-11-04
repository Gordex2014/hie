//! The database source on SQL Server, when one is at hand: set
//! `HIE_TEST_MSSQL_URL` (e.g. `sqlserver://sa:pass@localhost:14333/lab?trust_cert=true`)
//! to a server holding the `lab_results` table of `sources/lab-database.yaml.example`
//! with at least three rows; the test is skipped otherwise.

use std::path::PathBuf;

use hie::source::Source;
use serde_json::Value;

const QUERY: &str = "SELECT TOP 2 id, patient_mrn AS patient, loinc AS code, display, value, unit, taken_at, updated_at, ok, note FROM lab_results WHERE id > {cursor} ORDER BY id";

#[tokio::test]
async fn sql_server_rows_are_read_past_a_cursor() {
    let Ok(url) = std::env::var("HIE_TEST_MSSQL_URL") else {
        eprintln!("skipping: HIE_TEST_MSSQL_URL is not set");
        return;
    };
    unsafe { std::env::set_var("HIE_TEST_MSSQL_URL_COPY", &url) };
    let repo = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let mut feed: hie::config::FeedConfig = serde_yaml::from_str(&format!(
        "id: mssql\nname: SQL Server\nsource:\n  type: database\n  url_env: HIE_TEST_MSSQL_URL_COPY\n  query: \"{QUERY}\"\n  cursor_column: id\n  cursor_start: \"0\"\n  max_runs: 1\ntransform: transforms/sql_lab_results.py\n"
    ))
    .unwrap();
    feed.resolve(&repo).unwrap();

    // One run of TOP 2 past id 0: the first two rows, typed.
    let source = Source::from_config(&feed).unwrap();
    assert_eq!(source.list().await.unwrap(), ["query"]);
    let incoming = source.fetch("query").await.unwrap();
    let rows: Vec<Value> = String::from_utf8(incoming.bytes)
        .unwrap()
        .lines()
        .map(|l| serde_json::from_str(l).unwrap())
        .collect();
    assert_eq!(rows.len(), 2, "{rows:?}");
    let first = &rows[0];
    assert!(first["id"].is_i64(), "{first}");
    assert!(first["value"].is_number(), "{first}");
    assert!(
        first["taken_at"].as_str().unwrap().starts_with("20"),
        "{first}"
    );
    assert!(
        first["updated_at"].as_str().unwrap().contains('+')
            || first["updated_at"].as_str().unwrap().ends_with('Z'),
        "{first}"
    );
    assert_eq!(first["ok"], true);
    assert!(
        first["note"].is_null() || first["note"].is_string(),
        "{first}"
    );
    assert_eq!(
        source.acknowledge("query", "").await.unwrap(),
        format!("cursor {}", rows[1]["id"])
    );

    // The probe past the start sees the rest.
    let probe = source.probe().await.unwrap();
    assert!(probe.reachable);
    assert!(probe.objects >= 1, "{probe:?}");

    // A wrong password is refused at connection time, in the server's words.
    unsafe {
        std::env::set_var(
            "HIE_TEST_MSSQL_URL_COPY",
            url.replacen("://", "://sa:wrong@", 1),
        )
    };
    let source = Source::from_config(&feed).unwrap();
    let err = source.list().await.unwrap_err();
    assert!(format!("{err:#}").contains("SQL Server"), "{err:#}");
}

/// URL percent-decoding, without form rules (a `+` stays a `+`).
fn percent(text: &str) -> String {
    let bytes = text.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%'
            && i + 2 < bytes.len()
            && let Ok(b) = u8::from_str_radix(&text[i + 1..i + 3], 16)
        {
            out.push(b);
            i += 3;
        } else {
            out.push(bytes[i]);
            i += 1;
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// On SQL Server: a table read by parts ordered by a `money` column (four
/// decimals) and a `decimal(38, 30)` key, whose values are shared across a
/// run's boundary, reads every row once and then only the new ones: each
/// position is read and bound back without losing a digit.
#[tokio::test]
async fn sql_server_money_and_wide_decimals_order_a_table_read_exactly() {
    use std::sync::Arc;
    use tokio_util::compat::TokioAsyncWriteCompatExt;
    let Ok(url) = std::env::var("HIE_TEST_MSSQL_URL") else {
        eprintln!("skipping: HIE_TEST_MSSQL_URL is not set");
        return;
    };
    let parsed = url::Url::parse(&url).unwrap();
    let (host, port) = (
        parsed.host_str().unwrap().to_string(),
        parsed.port().unwrap_or(1433),
    );
    let (user, password) = (
        percent(parsed.username()),
        percent(parsed.password().unwrap()),
    );
    let database = parsed.path().trim_start_matches('/').to_string();
    let mut config = tiberius::Config::new();
    config.host(&host);
    config.port(port);
    config.database(&database);
    config.authentication(tiberius::AuthMethod::sql_server(&user, &password));
    config.trust_cert();
    let tcp = tokio::net::TcpStream::connect(config.get_addr())
        .await
        .unwrap();
    let mut client = tiberius::Client::connect(config, tcp.compat_write())
        .await
        .unwrap();
    let table = format!("keyset_{}", std::process::id());
    client
        .execute(
            format!(
                "CREATE TABLE dbo.{table} (id INT NOT NULL PRIMARY KEY, price MONEY NOT NULL, amount DECIMAL(38, 30) NOT NULL);
                 INSERT INTO dbo.{table} (id, price, amount)
                 SELECT TOP (1200) ROW_NUMBER() OVER (ORDER BY (SELECT NULL)), 1.2345, 1.000000000000000000000000000001
                 FROM sys.all_objects a CROSS JOIN sys.all_objects b"
            ),
            &[],
        )
        .await
        .unwrap();

    let db = Arc::new(hie::db::Db::in_memory().unwrap());
    let keys = hie::crypto::Keyring::parse("Y29uZm9ybWFuY2Utc2VjcmV0cy1rZXktMzItYnl0ZXM=").unwrap();
    let secrets = Arc::new(hie::secrets::Secrets::new(db.clone(), Some(keys)));
    let secret = db
        .transaction(|tx| {
            secrets
                .create_in(tx, "mssql", &password)
                .map_err(|e| anyhow::anyhow!("{e:?}"))
        })
        .unwrap()["id"]
        .as_str()
        .unwrap()
        .to_string();
    let repo = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let mut feed: hie::config::FeedConfig = serde_json::from_value(serde_json::json!({
        "id": "mskeys", "name": "Keys",
        "source": {"type": "database",
                   "connection": {"engine": "sqlserver", "host": host, "port": port, "database": database,
                                  "user": user, "password": {"secret": secret, "revision": 1},
                                  "encrypt": false, "trust_cert": true},
                   "read": {"mode": "table", "table": {"schema": "dbo", "name": table},
                            "key": ["amount", "id"], "new_rows_by": "price"}},
        "transform": "transforms/sql_lab_results.py",
    }))
    .unwrap();
    feed.resolve(&repo).unwrap();
    let ctx = hie::connectors::Context {
        admission: Default::default(),
        secrets: Some(secrets),
    };
    let source = Source::from_config_in(&feed, &ctx)
        .unwrap()
        .with_db(db.clone());
    let ids = |bytes: Vec<u8>| -> Vec<i64> {
        String::from_utf8(bytes)
            .unwrap()
            .lines()
            .map(|l| {
                serde_json::from_str::<Value>(l).unwrap()["id"]
                    .as_i64()
                    .unwrap()
            })
            .collect()
    };
    let key = source.list().await.unwrap().remove(0);
    let first = ids(source.fetch(&key).await.unwrap().bytes);
    source.acknowledge(&key, "").await.unwrap();
    let mut unique = first.clone();
    unique.sort();
    unique.dedup();
    assert_eq!((first.len(), unique.len()), (1200, 1200));
    assert!(source.list().await.unwrap().is_empty());

    client
        .execute(
            format!(
                "INSERT INTO dbo.{table} (id, price, amount) VALUES
                 (1201, 1.2345, 1.000000000000000000000000000002), (1202, 1.2346, 0)"
            ),
            &[],
        )
        .await
        .unwrap();
    let key = source.list().await.unwrap().remove(0);
    let later = ids(source.fetch(&key).await.unwrap().bytes);
    source.acknowledge(&key, "").await.unwrap();
    client
        .execute(format!("DROP TABLE dbo.{table}"), &[])
        .await
        .unwrap();
    assert_eq!(later, [1201, 1202]);
}
