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

/// A SQL Server named by `HIE_TEST_MSSQL_URL`, connected directly, with the
/// parts a connection by parts names.
struct Server {
    client: tiberius::Client<tokio_util::compat::Compat<tokio::net::TcpStream>>,
    host: String,
    port: u16,
    user: String,
    password: String,
    database: String,
}

async fn server() -> Option<Server> {
    use tokio_util::compat::TokioAsyncWriteCompatExt;
    let Ok(url) = std::env::var("HIE_TEST_MSSQL_URL") else {
        eprintln!("skipping: HIE_TEST_MSSQL_URL is not set");
        return None;
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
    let client = tiberius::Client::connect(config, tcp.compat_write())
        .await
        .unwrap();
    Some(Server {
        client,
        host,
        port,
        user,
        password,
        database,
    })
}

/// A table read by parts on `table` of `database` on `server`, keeping its
/// position in `db`.
fn table_source(
    server: &Server,
    db: &std::sync::Arc<hie::db::Db>,
    id: &str,
    (database, table): (&str, &str),
    key: &[&str],
    new_rows_by: &str,
    max_runs: usize,
) -> Source {
    use std::sync::Arc;
    let keys = hie::crypto::Keyring::parse("Y29uZm9ybWFuY2Utc2VjcmV0cy1rZXktMzItYnl0ZXM=").unwrap();
    let secrets = Arc::new(hie::secrets::Secrets::new(db.clone(), Some(keys)));
    let secret = db
        .transaction(|tx| {
            secrets
                .create_in(tx, &format!("mssql-{id}"), &server.password)
                .map_err(|e| anyhow::anyhow!("{e:?}"))
        })
        .unwrap()["id"]
        .as_str()
        .unwrap()
        .to_string();
    let repo = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let mut feed: hie::config::FeedConfig = serde_json::from_value(serde_json::json!({
        "id": id, "name": id,
        "source": {"type": "database",
                   "connection": {"engine": "sqlserver", "host": server.host, "port": server.port,
                                  "database": database, "user": server.user,
                                  "password": {"secret": secret, "revision": 1},
                                  "encrypt": false, "trust_cert": true},
                   "read": {"mode": "table", "table": {"schema": "dbo", "name": table},
                            "key": key, "new_rows_by": new_rows_by},
                   "max_runs": max_runs},
        "transform": "transforms/sql_lab_results.py",
    }))
    .unwrap();
    feed.resolve(&repo).unwrap();
    let ctx = hie::connectors::Context {
        admission: Default::default(),
        secrets: Some(secrets),
    };
    Source::from_config_in(&feed, &ctx)
        .unwrap()
        .with_db(db.clone())
}

/// One pass: the rows it delivered (none when nothing is new), acknowledged.
async fn pass(source: &Source) -> Vec<Value> {
    let Some(key) = source.list().await.unwrap().into_iter().next() else {
        return vec![];
    };
    let bytes = source.fetch(&key).await.unwrap().bytes;
    source.acknowledge(&key, "").await.unwrap();
    String::from_utf8(bytes)
        .unwrap()
        .lines()
        .map(|l| serde_json::from_str(l).unwrap())
        .collect()
}

/// How a time column of one SQL Server type is filled and what each row's
/// value must read as.
struct TimeCase {
    name: &'static str,
    sql_type: &'static str,
    /// The SQL value of row `id` (`{local}` is its local date-time).
    value: &'static str,
    /// The offset `value` carries, for a `datetimeoffset`.
    offset: Option<&'static str>,
    /// The fraction of a second each value carries, in nanoseconds.
    fraction_ns: i64,
}

const TIME_CASES: &[TimeCase] = &[
    // West of UTC: the case data was lost on (each page read 4 h late).
    TimeCase {
        name: "dto_west",
        sql_type: "DATETIMEOFFSET(0)",
        value: "TODATETIMEOFFSET({local}, '-04:00')",
        offset: Some("-04:00"),
        fraction_ns: 0,
    },
    // East of UTC, at full precision: each page was read early instead.
    TimeCase {
        name: "dto_east",
        sql_type: "DATETIMEOFFSET(7)",
        value: "TODATETIMEOFFSET({local}, '+05:30')",
        offset: Some("+05:30"),
        fraction_ns: 123_456_700,
    },
    // No offset at all.
    TimeCase {
        name: "dt2",
        sql_type: "DATETIME2(7)",
        value: "{local}",
        offset: None,
        fraction_ns: 123_456_700,
    },
    // datetime keeps 1/300 s: 7 ms is stored as 2/300 s.
    TimeCase {
        name: "dt",
        sql_type: "DATETIME",
        value: "CAST({local} AS DATETIME)",
        offset: None,
        fraction_ns: 6_666_666,
    },
];

/// On SQL Server: a table read keyed on a time column, of each time type
/// (`datetimeoffset` west and east of UTC, `datetime2`, `datetime`), with
/// three rows per value so values are shared across every page boundary,
/// reads every row exactly once over several pages and passes, delivers each
/// value as the instant it is (with its own offset), and records the last
/// row's exact value as its position; rows added later, at the last value
/// and past it, are read next.
#[tokio::test]
async fn sql_server_time_positions_read_every_row_exactly_once() {
    use chrono::{DateTime, Duration, FixedOffset, NaiveDateTime};
    use std::sync::Arc;
    let Some(mut server) = server().await else {
        return;
    };
    const ROWS: i64 = 2500;
    let base = NaiveDateTime::parse_from_str("2026-10-01T00:00:00", "%Y-%m-%dT%H:%M:%S").unwrap();
    let local = |id: i64, case: &TimeCase| {
        base + Duration::minutes(id / 3) + Duration::nanoseconds(case.fraction_ns)
    };
    let mut failures = Vec::new();
    for case in TIME_CASES {
        let table = format!("times_{}_{}", case.name, std::process::id());
        let fraction = match case.name {
            "dt" => "DATEADD(millisecond, 7, CAST(DATEADD(minute, n / 3, '2026-10-01T00:00:00') AS DATETIME))".to_string(),
            _ => format!(
                "DATEADD(nanosecond, {}, DATEADD(minute, n / 3, CAST('2026-10-01T00:00:00' AS DATETIME2(7))))",
                case.fraction_ns
            ),
        };
        let value = |local: &str| case.value.replace("{local}", local);
        server
            .client
            .execute(
                format!(
                    "CREATE TABLE dbo.{table} (id INT NOT NULL PRIMARY KEY, at {} NOT NULL);
                     WITH ns AS (SELECT TOP ({ROWS}) CAST(ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS INT) AS n
                                 FROM sys.all_objects a CROSS JOIN sys.all_objects b)
                     INSERT INTO dbo.{table} (id, at) SELECT n, {} FROM ns",
                    case.sql_type,
                    value(&fraction)
                ),
                &[],
            )
            .await
            .unwrap();

        let db = Arc::new(hie::db::Db::in_memory().unwrap());
        let feed = format!("times-{}", case.name.replace('_', "-"));
        let source = table_source(
            &server,
            &db,
            &feed,
            (&server.database, &table),
            &["id"],
            "at",
            2,
        );
        let expect = |row: &Value| -> Result<i64, String> {
            let id = row["id"].as_i64().unwrap();
            let text = row["at"].as_str().unwrap_or_default();
            let want = local(id, case);
            let exact = match case.offset {
                Some(offset) => {
                    let tz: FixedOffset = offset.parse().unwrap();
                    let want = want.and_local_timezone(tz).unwrap();
                    DateTime::parse_from_rfc3339(text)
                        .is_ok_and(|got| got == want && got.offset() == want.offset())
                }
                None => NaiveDateTime::parse_from_str(text, "%Y-%m-%dT%H:%M:%S%.f")
                    .is_ok_and(|got| got == want),
            };
            if exact {
                Ok(id)
            } else {
                Err(format!(
                    "row {id} read as {text:?}, expected {want} {:?}",
                    case.offset
                ))
            }
        };
        // Pass 1 takes two pages (2000 rows), pass 2 the last 500 from the
        // stored position, pass 3 nothing.
        let mut ids = Vec::new();
        let mut wrong = Vec::new();
        let mut passes = Vec::new();
        for _ in 0..3 {
            let rows = pass(&source).await;
            passes.push(rows.len());
            for row in &rows {
                match expect(row) {
                    Ok(id) => ids.push(id),
                    Err(e) => wrong.push(e),
                }
            }
        }
        let read = ids.len();
        ids.sort();
        ids.dedup();
        let position: Value =
            serde_json::from_str(&db.cursor(&feed, "keyset").unwrap().unwrap_or_default())
                .unwrap_or(Value::Null);
        let last = serde_json::json!({"id": ROWS, "at": position["after"][0]});

        server
            .client
            .execute(
                format!(
                    "INSERT INTO dbo.{table} (id, at) SELECT {}, at FROM dbo.{table} WHERE id = {ROWS};
                     INSERT INTO dbo.{table} (id, at) SELECT {}, DATEADD(minute, 1, at) FROM dbo.{table} WHERE id = {ROWS}",
                    ROWS + 1,
                    ROWS + 2,
                ),
                &[],
            )
            .await
            .unwrap();
        let later: Vec<i64> = pass(&source)
            .await
            .iter()
            .map(|r| r["id"].as_i64().unwrap())
            .collect();
        server
            .client
            .execute(format!("DROP TABLE dbo.{table}"), &[])
            .await
            .unwrap();

        let all = (1..=ROWS).collect::<Vec<_>>();
        if read != ROWS as usize || ids != all {
            failures.push(format!(
                "{}: {read} rows delivered, {} distinct of {ROWS}; per pass {passes:?}",
                case.name,
                ids.len()
            ));
        }
        if !wrong.is_empty() {
            failures.push(format!(
                "{}: {} values not exact, e.g. {}",
                case.name,
                wrong.len(),
                wrong[0]
            ));
        }
        if expect(&last).is_err() || position["after"][1] != ROWS {
            failures.push(format!("{}: recorded position {position}", case.name));
        }
        if later != [ROWS + 1, ROWS + 2] {
            failures.push(format!("{}: rows added later read as {later:?}", case.name));
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

/// On SQL Server: a table read's position or a query's cursor written before
/// the values carried their decoding (`version`) is not trusted when a
/// `datetimeoffset` orders the read: the old decoding read each value
/// |offset| off, so rows may have been skipped anywhere before it, and the
/// read starts over (the table from its start, the query from
/// `cursor_start`). A position on a `datetime2` is kept as it is.
#[tokio::test]
async fn sql_server_positions_from_the_old_datetimeoffset_decoding_read_again() {
    use std::sync::Arc;
    let Some(mut server) = server().await else {
        return;
    };
    let pid = std::process::id();
    let (dto, dt2, dt) = (
        format!("legacy_dto_{pid}"),
        format!("legacy_dt2_{pid}"),
        format!("legacy_dt_{pid}"),
    );
    server
        .client
        .execute(
            format!(
                "CREATE TABLE dbo.{dto} (id INT NOT NULL PRIMARY KEY, at DATETIMEOFFSET(0) NOT NULL);
                 CREATE TABLE dbo.{dt2} (id INT NOT NULL PRIMARY KEY, at DATETIME2(0) NOT NULL);
                 WITH ns AS (SELECT TOP (30) CAST(ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS INT) AS n FROM sys.all_objects)
                 INSERT INTO dbo.{dto} (id, at) SELECT n, TODATETIMEOFFSET(DATEADD(minute, n, CAST('2026-10-01T00:00:00' AS DATETIME2(0))), '-04:00') FROM ns;
                 INSERT INTO dbo.{dt2} (id, at) SELECT id, CAST(SWITCHOFFSET(at, '+00:00') AS DATETIME2(0)) FROM dbo.{dto};
                 CREATE TABLE dbo.{dt} (id INT NOT NULL PRIMARY KEY, at DATETIME NOT NULL);
                 INSERT INTO dbo.{dt} (id, at) SELECT id, at FROM dbo.{dt2}"
            ),
            &[],
        )
        .await
        .unwrap();
    let db = Arc::new(hie::db::Db::in_memory().unwrap());
    let ids =
        |rows: Vec<Value>| -> Vec<i64> { rows.iter().map(|r| r["id"].as_i64().unwrap()).collect() };

    // Row 20 is 00:20-04:00; the old decoding recorded it 4 h late, in UTC.
    let old = r#"{"columns":["at","id"],"after":["2026-10-01T08:20:00+00:00",20]}"#;
    db.set_cursor("legacy-dto", "keyset", old).unwrap();
    let source = table_source(
        &server,
        &db,
        "legacy-dto",
        (&server.database, &dto),
        &["id"],
        "at",
        1,
    );
    let again = ids(pass(&source).await);
    let position: Value =
        serde_json::from_str(&db.cursor("legacy-dto", "keyset").unwrap().unwrap()).unwrap();
    let after_again = pass(&source).await.len();

    // A datetime2 never went through it: its position stands.
    let old = r#"{"columns":["at","id"],"after":["2026-10-01T04:20:00",20]}"#;
    db.set_cursor("legacy-dt2", "keyset", old).unwrap();
    let source = table_source(
        &server,
        &db,
        "legacy-dt2",
        (&server.database, &dt2),
        &["id"],
        "at",
        1,
    );
    let kept = ids(pass(&source).await);

    // A datetime's old position was bound back inexactly: read again too.
    let old = r#"{"columns":["at","id"],"after":["2026-10-01T04:20:00",20]}"#;
    db.set_cursor("legacy-dt", "keyset", old).unwrap();
    let source = table_source(
        &server,
        &db,
        "legacy-dt",
        (&server.database, &dt),
        &["id"],
        "at",
        1,
    );
    let datetime_again = ids(pass(&source).await);

    // The 0.1 query: its cursor starts over from cursor_start.
    let url = std::env::var("HIE_TEST_MSSQL_URL").unwrap();
    unsafe { std::env::set_var("HIE_TEST_MSSQL_URL_LEGACY", &url) };
    let repo = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let mut feed: hie::config::FeedConfig = serde_json::from_value(serde_json::json!({
        "id": "legacy-query", "name": "legacy-query",
        "source": {"type": "database", "url_env": "HIE_TEST_MSSQL_URL_LEGACY",
                   "query": format!("SELECT TOP 100 id, at FROM dbo.{dto} WHERE at > {{cursor}} ORDER BY at"),
                   "cursor_column": "at", "cursor_start": "2026-09-30T00:00:00Z", "max_runs": 1},
        "transform": "transforms/sql_lab_results.py",
    }))
    .unwrap();
    feed.resolve(&repo).unwrap();
    db.set_cursor(
        "legacy-query",
        "at",
        r#"{"kind":"timestamp","value":"2026-10-01T08:20:00+00:00"}"#,
    )
    .unwrap();
    let source = Source::from_config(&feed).unwrap().with_db(db.clone());
    let query_again = ids(pass(&source).await);
    let cursor = db.cursor("legacy-query", "at").unwrap().unwrap();

    server
        .client
        .execute(
            format!("DROP TABLE dbo.{dto}; DROP TABLE dbo.{dt2}; DROP TABLE dbo.{dt}"),
            &[],
        )
        .await
        .unwrap();
    assert_eq!(again, (1..=30).collect::<Vec<_>>());
    assert_eq!(
        position,
        serde_json::json!({"columns": ["at", "id"], "after": ["2026-10-01T00:30:00-04:00", 30], "version": 2})
    );
    assert_eq!(after_again, 0);
    assert_eq!(kept, (21..=30).collect::<Vec<_>>());
    assert_eq!(datetime_again, (1..=30).collect::<Vec<_>>());
    assert_eq!(query_again, (1..=30).collect::<Vec<_>>());
    assert_eq!(
        cursor,
        r#"{"kind":"timestamp","value":"2026-10-01T00:30:00-04:00","version":2}"#
    );
}

/// On SQL Server, in a database at compatibility level 120: a `datetime`
/// value of one tick (.003) does not equal itself bound as the `datetime2`
/// it decodes to (.0033333), so a position bound that way skipped the rows
/// that share it; cast back to `datetime`, 1,500 rows sharing one such value
/// across a page boundary, and the rows after it, are each read once.
#[tokio::test]
async fn sql_server_datetime_positions_are_exact_at_compatibility_level_120() {
    use std::sync::Arc;
    let Some(mut server) = server().await else {
        return;
    };
    let pid = std::process::id();
    let database = format!("hie_compat120_{pid}");
    server
        .client
        .execute(
            format!(
                "CREATE DATABASE [{database}];
                 ALTER DATABASE [{database}] SET COMPATIBILITY_LEVEL = 120;"
            ),
            &[],
        )
        .await
        .unwrap();
    server
        .client
        .execute(
            format!(
                "CREATE TABLE [{database}].dbo.ticks (id INT NOT NULL PRIMARY KEY, at DATETIME NOT NULL);
                 WITH ns AS (SELECT TOP (2000) CAST(ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS INT) AS n
                             FROM sys.all_objects a CROSS JOIN sys.all_objects b)
                 INSERT INTO [{database}].dbo.ticks (id, at)
                 SELECT n, CASE WHEN n <= 1500 THEN CAST('2026-10-01T00:00:00.003' AS DATETIME)
                                ELSE DATEADD(minute, n, CAST('2026-10-01T00:00:00.003' AS DATETIME)) END FROM ns"
            ),
            &[],
        )
        .await
        .unwrap();
    // The cause, as the server sees it: the decoded value bound as a
    // datetime2 does not match the rows it was read from.
    let decoded = chrono::NaiveDateTime::parse_from_str(
        "2026-10-01T00:00:00.003333333",
        "%Y-%m-%dT%H:%M:%S%.f",
    )
    .unwrap();
    let matching = server
        .client
        .query(
            format!("SELECT COUNT_BIG(*) FROM [{database}].dbo.ticks WHERE at = @P1"),
            &[&decoded],
        )
        .await
        .unwrap()
        .into_row()
        .await
        .unwrap()
        .unwrap()
        .get::<i64, _>(0);

    let db = Arc::new(hie::db::Db::in_memory().unwrap());
    let source = table_source(
        &server,
        &db,
        "compat120",
        (&database, "ticks"),
        &["id"],
        "at",
        1,
    );
    let mut ids = Vec::new();
    for _ in 0..4 {
        ids.extend(
            pass(&source)
                .await
                .iter()
                .map(|r| r["id"].as_i64().unwrap()),
        );
    }
    server
        .client
        .execute(
            format!(
                "ALTER DATABASE [{database}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE [{database}]"
            ),
            &[],
        )
        .await
        .unwrap();
    assert_eq!(matching, Some(0));
    let read = ids.len();
    ids.sort();
    ids.dedup();
    assert_eq!((read, ids), (2000, (1..=2000).collect::<Vec<_>>()));
}
