//! The drivers a database source reads through: sqlx for PostgreSQL, MySQL and
//! SQLite, tiberius for SQL Server. One connection pool per source, opened on
//! first use; the query runs with the cursor bound, never spliced.

use std::time::Duration;

use anyhow::{Context, Result, bail};
use chrono::NaiveDate;
use serde_json::{Map, Value};
use sqlx::mysql::{MySqlPool, MySqlPoolOptions};
use sqlx::postgres::{PgPool, PgPoolOptions};
use sqlx::sqlite::{SqlitePool, SqlitePoolOptions};

use super::cursor::{Cursor, instant};
use super::rows::{mssql_row, mysql_row, pg_row, sqlite_row};

/// An open connection pool, per engine.
enum Pool {
    Postgres(PgPool),
    MySql(MySqlPool),
    Sqlite(SqlitePool),
    /// SQL Server opens a connection per run; tiberius has no pool.
    MsSql(Box<tiberius::Config>),
}

/// `sqlserver://user:password@host:1433/database?encrypt=on&trust_cert=true`
/// (`mssql://` too) as a tiberius configuration. `encrypt` is `on` by default;
/// `trust_cert=true` accepts a self-signed certificate, as a lab server has.
pub(super) fn mssql_config(url: &str) -> Result<tiberius::Config> {
    let parsed = url::Url::parse(url).context("parsing the SQL Server URL")?;
    let mut config = tiberius::Config::new();
    config.host(
        parsed
            .host_str()
            .context("the SQL Server URL has no host")?,
    );
    config.port(parsed.port().unwrap_or(1433));
    let database = parsed.path().trim_start_matches('/');
    if !database.is_empty() {
        config.database(database);
    }
    let user = parsed.username();
    if user.is_empty() {
        bail!("the SQL Server URL needs user:password (Windows authentication is not supported)");
    }
    let password = parsed.password().unwrap_or_default();
    config.authentication(tiberius::AuthMethod::sql_server(
        percent_decode(user),
        percent_decode(password),
    ));
    let mut encrypt = true;
    for (key, value) in parsed.query_pairs() {
        match key.as_ref() {
            "encrypt" => encrypt = !matches!(value.as_ref(), "off" | "false" | "no" | "0"),
            "trust_cert" | "trustServerCertificate" => {
                if matches!(value.as_ref(), "true" | "yes" | "1") {
                    config.trust_cert();
                }
            }
            "application" => config.application_name(value.as_ref()),
            other => bail!(
                "unknown SQL Server URL option {other:?} (use encrypt, trust_cert, application)"
            ),
        }
    }
    config.encryption(if encrypt {
        tiberius::EncryptionLevel::Required
    } else {
        tiberius::EncryptionLevel::NotSupported
    });
    Ok(config)
}

fn percent_decode(text: &str) -> String {
    url::form_urlencoded::parse(text.as_bytes())
        .map(|(k, v)| format!("{k}{v}"))
        .collect::<Vec<_>>()
        .join("")
}

async fn mssql_connect(
    config: &tiberius::Config,
) -> Result<tiberius::Client<tokio_util::compat::Compat<tokio::net::TcpStream>>> {
    use tokio_util::compat::TokioAsyncWriteCompatExt;
    let tcp = tokio::net::TcpStream::connect(config.get_addr())
        .await
        .with_context(|| format!("cannot reach SQL Server at {}", config.get_addr()))?;
    tcp.set_nodelay(true)?;
    tiberius::Client::connect(config.clone(), tcp.compat_write())
        .await
        .context("connecting to SQL Server")
}

/// The connection to the source's database, opened on first use.
pub(super) struct Driver {
    url: String,
    pool: tokio::sync::OnceCell<Pool>,
}

impl Driver {
    pub(super) fn new(url: String) -> Self {
        Self {
            url,
            pool: tokio::sync::OnceCell::new(),
        }
    }

    /// The URL connected to.
    pub(super) fn url(&self) -> &str {
        &self.url
    }

    async fn pool(&self) -> Result<&Pool> {
        self.pool
            .get_or_try_init(|| async {
                let timeout = Duration::from_secs(30);
                let pool =
                    if self.url.starts_with("sqlserver://") || self.url.starts_with("mssql://") {
                        let config = mssql_config(&self.url)?;
                        // Connect once now, so a wrong host or password shows at start.
                        mssql_connect(&config).await?;
                        Pool::MsSql(Box::new(config))
                    } else if self.url.starts_with("mysql://") {
                        Pool::MySql(
                            MySqlPoolOptions::new()
                                .max_connections(2)
                                .acquire_timeout(timeout)
                                .connect(&self.url)
                                .await?,
                        )
                    } else if self.url.starts_with("sqlite:") {
                        Pool::Sqlite(
                            SqlitePoolOptions::new()
                                .max_connections(1)
                                .acquire_timeout(timeout)
                                .connect(&self.url)
                                .await?,
                        )
                    } else {
                        Pool::Postgres(
                            PgPoolOptions::new()
                                .max_connections(2)
                                .acquire_timeout(timeout)
                                .connect(&self.url)
                                .await?,
                        )
                    };
                Ok::<_, anyhow::Error>(pool)
            })
            .await
            .context("connecting to the database")
    }

    /// The query with `{cursor}` replaced by the driver's placeholder, and how many
    /// times the value must be bound.
    fn sql(query: &str, pool: &Pool) -> (String, usize) {
        let occurrences = query.matches("{cursor}").count();
        match pool {
            Pool::Postgres(_) => (query.replace("{cursor}", "$1"), 1),
            Pool::Sqlite(_) => (query.replace("{cursor}", "?1"), 1),
            Pool::MySql(_) => (query.replace("{cursor}", "?"), occurrences),
            Pool::MsSql(_) => (query.replace("{cursor}", "@P1"), 1),
        }
    }

    /// One run of the query past `cursor`, as JSON rows. Without a cursor the
    /// `{cursor}` is bound to the lowest value of its kind, so the query still parses.
    /// The SQL is the operator's own, from the source definition; the only value
    /// that comes from data, the cursor, is bound, never spliced.
    pub(super) async fn run(
        &self,
        query: &str,
        cursor: Option<&Cursor>,
    ) -> Result<Vec<Map<String, Value>>> {
        let pool = self.pool().await?;
        let (sql, binds) = Self::sql(query, pool);
        let first = cursor.is_none();
        let cursor = cursor.cloned().unwrap_or(Cursor::Int(i64::MIN));
        let rows = match pool {
            Pool::Postgres(p) => {
                let mut q = sqlx::query(sqlx::AssertSqlSafe(sql.clone()));
                for _ in 0..binds {
                    q = match &cursor {
                        Cursor::Int(i) => q.bind(*i),
                        Cursor::Float(f) => q.bind(*f),
                        Cursor::Timestamp(t) => match instant(t) {
                            Some(at) => q.bind(at),
                            None => q.bind(t.clone()),
                        },
                        Cursor::Date(d) => match NaiveDate::parse_from_str(d, "%Y-%m-%d") {
                            Ok(d) => q.bind(d),
                            Err(_) => q.bind(d.clone()),
                        },
                        Cursor::Text(s) => q.bind(s.clone()),
                    };
                }
                q.fetch_all(p)
                    .await
                    .with_context(|| query_failed(&sql, first))?
                    .iter()
                    .map(pg_row)
                    .collect()
            }
            Pool::MySql(p) => {
                let mut q = sqlx::query(sqlx::AssertSqlSafe(sql.clone()));
                for _ in 0..binds {
                    q = match &cursor {
                        Cursor::Int(i) => q.bind(*i),
                        Cursor::Float(f) => q.bind(*f),
                        Cursor::Timestamp(t) => match instant(t) {
                            Some(at) => q.bind(at.naive_utc()),
                            None => q.bind(t.clone()),
                        },
                        Cursor::Date(d) => match NaiveDate::parse_from_str(d, "%Y-%m-%d") {
                            Ok(d) => q.bind(d),
                            Err(_) => q.bind(d.clone()),
                        },
                        Cursor::Text(s) => q.bind(s.clone()),
                    };
                }
                q.fetch_all(p)
                    .await
                    .with_context(|| query_failed(&sql, first))?
                    .iter()
                    .map(mysql_row)
                    .collect()
            }
            Pool::MsSql(config) => {
                let mut client = mssql_connect(config).await?;
                let param: Box<dyn tiberius::ToSql> = match &cursor {
                    Cursor::Int(i) => Box::new(*i),
                    Cursor::Float(f) => Box::new(*f),
                    Cursor::Timestamp(t) => match instant(t) {
                        Some(at) => Box::new(at.naive_utc()),
                        None => Box::new(t.clone()),
                    },
                    Cursor::Date(d) => match NaiveDate::parse_from_str(d, "%Y-%m-%d") {
                        Ok(d) => Box::new(d),
                        Err(_) => Box::new(d.clone()),
                    },
                    Cursor::Text(s) => Box::new(s.clone()),
                };
                let params: Vec<&dyn tiberius::ToSql> =
                    (0..binds).map(|_| param.as_ref()).collect();
                client
                    .query(sql.as_str(), &params)
                    .await
                    .with_context(|| query_failed(&sql, first))?
                    .into_first_result()
                    .await
                    .with_context(|| query_failed(&sql, first))?
                    .iter()
                    .map(mssql_row)
                    .collect()
            }
            Pool::Sqlite(p) => {
                let mut q = sqlx::query(sqlx::AssertSqlSafe(sql.clone()));
                for _ in 0..binds {
                    q = match &cursor {
                        Cursor::Int(i) => q.bind(*i),
                        Cursor::Float(f) => q.bind(*f),
                        Cursor::Timestamp(t) | Cursor::Date(t) | Cursor::Text(t) => {
                            q.bind(t.clone())
                        }
                    };
                }
                q.fetch_all(p)
                    .await
                    .with_context(|| query_failed(&sql, first))?
                    .iter()
                    .map(sqlite_row)
                    .collect()
            }
        };
        Ok(rows)
    }
}

/// Why a run failed. Before the first cursor the placeholder is bound as an
/// integer, which a timestamp or text column refuses; the fix is a cursor_start.
fn query_failed(sql: &str, first: bool) -> String {
    if first {
        format!(
            "running the query (no cursor yet, so {{cursor}} was bound as an integer; if the cursor column is not one, set cursor_start to a value of its kind, e.g. 1970-01-01T00:00:00Z): {sql}"
        )
    } else {
        format!("running the query: {sql}")
    }
}
