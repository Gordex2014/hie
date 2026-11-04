//! A database named by its parts (0.2.0 `connection`), and a session on it.
//!
//! [`Endpoint::open`] checks admission first, then opens the password's
//! revision, connects, and drops the value: it lives in memory only while
//! the connection is opened. Nothing is pooled; a session is opened for a
//! pass, a Test, a describe or a sample, and closed after it.
//!
//! Every failure is a [`DbError`]: the stage it happened at (connect,
//! authenticate, read) and a secret-safe sentence. A connection failure is
//! reduced to a fixed sentence; a driver's own message is kept only when
//! `Secrets::is_clean` finds none of the values HIE keeps in it.

use std::time::Duration;

use chrono::{DateTime, FixedOffset, NaiveDate, NaiveDateTime};
use serde_json::{Map, Value};
use sqlx::{ConnectOptions, Connection};
use tokio_util::compat::{Compat, TokioAsyncWriteCompatExt};

use super::rows::{mssql_row, pg_row, sqlite_row};
use crate::config::{ConnectionParts, DatabaseConnection};
use crate::connectors::Context;
use crate::secrets::SecretError;

/// How long a connection may take to open.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(20);
/// How long one statement may run.
const READ_TIMEOUT: Duration = Duration::from_secs(120);

/// The engines a connection by parts may name.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Engine {
    SqlServer,
    Postgres,
    Sqlite,
}

impl Engine {
    /// The engine a normalized name names.
    pub fn parse(name: &str) -> Option<Self> {
        match crate::config::normalize_engine(name).as_str() {
            "sqlserver" => Some(Engine::SqlServer),
            "postgres" => Some(Engine::Postgres),
            "sqlite" => Some(Engine::Sqlite),
            _ => None,
        }
    }

    /// The schema a table name without one is in.
    pub fn default_schema(self) -> &'static str {
        match self {
            Engine::SqlServer => "dbo",
            Engine::Postgres => "public",
            Engine::Sqlite => "main",
        }
    }

    /// An identifier, quoted.
    pub fn quote(self, name: &str) -> String {
        match self {
            Engine::SqlServer => format!("[{}]", name.replace(']', "]]")),
            _ => format!("\"{}\"", name.replace('"', "\"\"")),
        }
    }

    /// The `n`th (1-based) placeholder.
    pub fn placeholder(self, n: usize) -> String {
        match self {
            Engine::SqlServer => format!("@P{n}"),
            Engine::Postgres => format!("${n}"),
            Engine::Sqlite => format!("?{n}"),
        }
    }
}

/// Where a failure happened.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Stage {
    Connect,
    Authenticate,
    Read,
}

/// A failure, safe to answer and log: no value HIE keeps is in `message`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DbError {
    pub stage: Stage,
    pub message: String,
}

impl DbError {
    pub fn new(stage: Stage, message: impl Into<String>) -> Self {
        Self {
            stage,
            message: message.into(),
        }
    }
}

impl std::fmt::Display for DbError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let stage = match self.stage {
            Stage::Connect => "connecting to the database",
            Stage::Authenticate => "signing in to the database",
            Stage::Read => "reading the database",
        };
        write!(f, "{stage}: {}", self.message)
    }
}

impl std::error::Error for DbError {}

/// A value bound to a statement.
#[derive(Debug, Clone, PartialEq)]
pub enum Bind {
    Null,
    Int(i64),
    Float(f64),
    Decimal(String),
    Text(String),
    Timestamp(NaiveDateTime),
    /// An instant with its offset: `timestamptz` on PostgreSQL,
    /// `datetimeoffset` on SQL Server.
    TimestampTz(DateTime<FixedOffset>),
    Date(NaiveDate),
    Time(chrono::NaiveTime),
    Bool(bool),
    /// A UUID as text, bound as the engine's UUID type.
    Uuid(String),
}

/// A connection by its parts, with the password's revision resolved.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Endpoint {
    pub engine: Engine,
    pub host: Option<String>,
    pub port: Option<u16>,
    pub database: Option<String>,
    pub user: Option<String>,
    /// The secret and the revision this endpoint uses.
    pub password: Option<(String, u32)>,
    pub encrypt: bool,
    pub trust_cert: bool,
}

impl Endpoint {
    /// The endpoint of a definition's connection, the password taking
    /// `revision` (resolved by the caller: the evidence's, or the current one).
    pub fn new(connection: &DatabaseConnection, revision: Option<u32>) -> Result<Self, String> {
        let engine = Engine::parse(&connection.engine).ok_or_else(|| {
            format!(
                "connection.engine {:?} is not sqlserver, postgres or sqlite",
                connection.engine
            )
        })?;
        let parts = connection.parts();
        let password = match (
            &connection.password,
            revision.or_else(|| connection.password.as_ref().and_then(|p| p.revision)),
        ) {
            (Some(p), Some(r)) => Some((p.secret.clone(), r)),
            (Some(_), None) => return Err("the password's revision is not known".into()),
            (None, _) => None,
        };
        Ok(Self {
            engine,
            host: parts.host,
            port: parts.port,
            database: parts.database,
            user: connection.user.clone(),
            password,
            encrypt: connection.encrypt.unwrap_or(true),
            trust_cert: connection.trust_cert.unwrap_or(false),
        })
    }

    /// The parts admission compares.
    pub fn parts(&self) -> ConnectionParts {
        ConnectionParts {
            engine: match self.engine {
                Engine::SqlServer => "sqlserver",
                Engine::Postgres => "postgres",
                Engine::Sqlite => "sqlite",
            }
            .into(),
            host: self.host.clone(),
            port: self.port,
            database: self.database.clone(),
        }
        .normalized()
    }

    /// Opens a session: admission, then the password, then the connection.
    pub async fn open(&self, ctx: &Context) -> Result<Session, DbError> {
        if let Err(refused) = ctx.admission.check(&self.parts()) {
            return Err(DbError::new(Stage::Connect, refused.to_string()));
        }
        let password = match &self.password {
            None => None,
            Some((secret, revision)) => {
                let secrets = ctx.secrets.as_ref().ok_or_else(|| {
                    DbError::new(
                        Stage::Authenticate,
                        "secrets are not available to this engine",
                    )
                })?;
                Some(secrets.reveal(secret, *revision).map_err(|e| {
                    DbError::new(
                        Stage::Authenticate,
                        match e {
                            SecretError::Unavailable => "secrets are not available to this engine",
                            SecretError::NotFound => "the password's secret does not exist",
                            SecretError::RevisionNotFound => {
                                "the password's revision does not exist any more"
                            }
                            _ => "the password could not be opened",
                        },
                    )
                })?)
            }
        };
        let password = password.as_ref().map(|p| p.expose()).unwrap_or_default();
        let clean = |text: &str| {
            !text.is_empty()
                && (password.is_empty() || !text.contains(password))
                && ctx.secrets.as_ref().is_none_or(|s| s.is_clean(text))
        };
        let opened = match self.engine {
            Engine::Sqlite => self.open_sqlite().await,
            Engine::Postgres => self.open_postgres(password).await,
            Engine::SqlServer => self.open_mssql(password).await,
        };
        opened.map_err(|e| e.made_safe(&clean))
    }

    async fn open_sqlite(&self) -> Result<Session, Failure> {
        let path = self.database.clone().unwrap_or_default();
        if path.is_empty() {
            return Err(Failure::fixed(
                Stage::Connect,
                "a sqlite connection names its database file",
            ));
        }
        let options = sqlx::sqlite::SqliteConnectOptions::new()
            .filename(&path)
            .read_only(true)
            .create_if_missing(false)
            .log_statements(log::LevelFilter::Off)
            .log_slow_statements(log::LevelFilter::Off, READ_TIMEOUT);
        let connection = tokio::time::timeout(
            CONNECT_TIMEOUT,
            sqlx::SqliteConnection::connect_with(&options),
        )
        .await
        .map_err(|_| Failure::fixed(Stage::Connect, "timed out"))?
        .map_err(|e| Failure::sqlx(Stage::Connect, e))?;
        Ok(Session {
            engine: Engine::Sqlite,
            conn: Conn::Sqlite(connection),
        })
    }

    async fn open_postgres(&self, password: &str) -> Result<Session, Failure> {
        use sqlx::postgres::{PgConnectOptions, PgSslMode};
        let mut options = PgConnectOptions::new_without_pgpass()
            .log_statements(log::LevelFilter::Off)
            .log_slow_statements(log::LevelFilter::Off, READ_TIMEOUT)
            .host(self.host.as_deref().unwrap_or("localhost"))
            .port(self.port.unwrap_or(5432))
            .ssl_mode(match (self.encrypt, self.trust_cert) {
                (false, _) => PgSslMode::Disable,
                (true, true) => PgSslMode::Require,
                (true, false) => PgSslMode::VerifyFull,
            });
        if let Some(database) = &self.database {
            options = options.database(database);
        }
        if let Some(user) = &self.user {
            options = options.username(user);
        }
        if !password.is_empty() {
            options = options.password(password);
        }
        let connection =
            tokio::time::timeout(CONNECT_TIMEOUT, sqlx::PgConnection::connect_with(&options))
                .await
                .map_err(|_| Failure::fixed(Stage::Connect, "timed out"))?
                .map_err(|e| Failure::sqlx(Stage::Connect, e))?;
        drop(options);
        Ok(Session {
            engine: Engine::Postgres,
            conn: Conn::Postgres(connection),
        })
    }

    async fn open_mssql(&self, password: &str) -> Result<Session, Failure> {
        let mut config = tiberius::Config::new();
        config.host(self.host.as_deref().unwrap_or("localhost"));
        config.port(self.port.unwrap_or(1433));
        if let Some(database) = &self.database {
            config.database(database);
        }
        let Some(user) = &self.user else {
            return Err(Failure::fixed(
                Stage::Authenticate,
                "SQL Server needs a user (SQL Server authentication)",
            ));
        };
        config.authentication(tiberius::AuthMethod::sql_server(user, password));
        if self.trust_cert {
            config.trust_cert();
        }
        config.encryption(if self.encrypt {
            tiberius::EncryptionLevel::Required
        } else {
            tiberius::EncryptionLevel::NotSupported
        });
        let connect = async {
            let tcp = tokio::net::TcpStream::connect(config.get_addr())
                .await
                .map_err(|e| Failure::io(Stage::Connect, &e))?;
            tcp.set_nodelay(true)
                .map_err(|e| Failure::io(Stage::Connect, &e))?;
            tiberius::Client::connect(config, tcp.compat_write())
                .await
                .map_err(Failure::tiberius)
        };
        let client = tokio::time::timeout(CONNECT_TIMEOUT, connect)
            .await
            .map_err(|_| Failure::fixed(Stage::Connect, "timed out"))??;
        Ok(Session {
            engine: Engine::SqlServer,
            conn: Conn::MsSql(Box::new(client)),
        })
    }
}

/// A failure before it is made safe: a fixed sentence, or a driver's message
/// that may be kept once checked.
struct Failure {
    stage: Stage,
    fixed: Option<&'static str>,
    driver: String,
}

impl Failure {
    fn fixed(stage: Stage, sentence: &'static str) -> Self {
        Self {
            stage,
            fixed: Some(sentence),
            driver: String::new(),
        }
    }

    fn io(stage: Stage, e: &std::io::Error) -> Self {
        use std::io::ErrorKind as K;
        match e.kind() {
            K::ConnectionRefused => Self::fixed(stage, "connection refused"),
            K::TimedOut => Self::fixed(stage, "timed out"),
            K::UnexpectedEof | K::ConnectionReset | K::ConnectionAborted | K::BrokenPipe => {
                Self::fixed(stage, "the server closed the connection")
            }
            _ => Self {
                stage,
                fixed: None,
                driver: e.to_string(),
            },
        }
    }

    fn sqlx(stage: Stage, e: sqlx::Error) -> Self {
        match e {
            sqlx::Error::Io(io) => Self::io(stage, &io),
            sqlx::Error::Tls(_) => Self::fixed(Stage::Connect, "TLS handshake failed"),
            sqlx::Error::PoolTimedOut => Self::fixed(stage, "timed out"),
            sqlx::Error::Database(db)
                if stage != Stage::Read
                    && matches!(db.code().as_deref(), Some("28P01" | "28000")) =>
            {
                Self::fixed(Stage::Authenticate, "authentication failed")
            }
            other => Self {
                stage,
                fixed: None,
                driver: other.to_string(),
            },
        }
    }

    fn tiberius(e: tiberius::error::Error) -> Self {
        use tiberius::error::Error as E;
        match &e {
            E::Io { kind, .. } => Self::io(Stage::Connect, &std::io::Error::from(*kind)),
            E::Tls(_) => Self::fixed(Stage::Connect, "TLS handshake failed"),
            E::Server(token)
                if matches!(token.code(), 18456 | 18452 | 18470 | 18486 | 18487 | 18488) =>
            {
                Self::fixed(Stage::Authenticate, "authentication failed")
            }
            E::Server(token) if token.code() == 4060 => Self {
                stage: Stage::Authenticate,
                fixed: None,
                driver: token.message().to_string(),
            },
            E::Routing { .. } => {
                Self::fixed(Stage::Connect, "the server redirected the connection")
            }
            other => Self {
                stage: Stage::Connect,
                fixed: None,
                driver: other.to_string(),
            },
        }
    }

    fn read_tiberius(e: tiberius::error::Error) -> Self {
        match &e {
            tiberius::error::Error::Server(token) => Self {
                stage: Stage::Read,
                fixed: None,
                driver: token.message().to_string(),
            },
            tiberius::error::Error::Io { kind, .. } => {
                Self::io(Stage::Read, &std::io::Error::from(*kind))
            }
            other => Self {
                stage: Stage::Read,
                fixed: None,
                driver: other.to_string(),
            },
        }
    }

    /// The failure as it may be shown: the driver's message only when clean.
    fn made_safe(self, clean: &(dyn Fn(&str) -> bool + Sync)) -> DbError {
        let message = match self.fixed {
            Some(sentence) => sentence.to_string(),
            None if clean(&self.driver) => self.driver,
            None => match self.stage {
                Stage::Connect => "the connection failed".to_string(),
                Stage::Authenticate => "authentication failed".to_string(),
                Stage::Read => "the read failed".to_string(),
            },
        };
        DbError::new(self.stage, message)
    }
}

enum Conn {
    Sqlite(sqlx::SqliteConnection),
    Postgres(sqlx::PgConnection),
    MsSql(Box<tiberius::Client<Compat<tokio::net::TcpStream>>>),
}

/// An open connection. Close it with [`Session::close`].
pub struct Session {
    engine: Engine,
    conn: Conn,
}

impl Session {
    pub fn engine(&self) -> Engine {
        self.engine
    }

    /// Runs one statement and answers its rows as JSON objects. A failure's
    /// message is checked by `clean` before it is kept.
    pub async fn fetch(
        &mut self,
        sql: &str,
        binds: &[Bind],
        clean: &(dyn Fn(&str) -> bool + Sync),
    ) -> Result<Vec<Map<String, Value>>, DbError> {
        let run = async {
            match &mut self.conn {
                Conn::Sqlite(c) => {
                    let mut q = sqlx::query(sqlx::AssertSqlSafe(sql.to_string()));
                    for b in binds {
                        q = match b {
                            Bind::Null => q.bind(None::<i64>),
                            Bind::Int(i) => q.bind(*i),
                            Bind::Float(f) => q.bind(*f),
                            Bind::Decimal(t) | Bind::Text(t) => q.bind(t.clone()),
                            Bind::Timestamp(t) => {
                                q.bind(t.format("%Y-%m-%dT%H:%M:%S%.f").to_string())
                            }
                            Bind::TimestampTz(t) => q.bind(t.to_rfc3339()),
                            Bind::Date(d) => q.bind(d.to_string()),
                            Bind::Time(t) => q.bind(t.to_string()),
                            Bind::Bool(b) => q.bind(*b),
                            Bind::Uuid(u) => q.bind(u.clone()),
                        };
                    }
                    q.fetch_all(&mut *c)
                        .await
                        .map(|rows| rows.iter().map(sqlite_row).collect())
                        .map_err(|e| Failure::sqlx(Stage::Read, e))
                }
                Conn::Postgres(c) => {
                    let mut q = sqlx::query(sqlx::AssertSqlSafe(sql.to_string()));
                    for b in binds {
                        q = match b {
                            Bind::Null => q.bind(None::<i64>),
                            Bind::Int(i) => q.bind(*i),
                            Bind::Float(f) => q.bind(*f),
                            // Exact text; the statement casts it (`$n::numeric`).
                            Bind::Decimal(t) => q.bind(t.clone()),
                            Bind::Text(t) => q.bind(t.clone()),
                            Bind::Timestamp(t) => q.bind(*t),
                            Bind::TimestampTz(t) => q.bind(*t),
                            Bind::Date(d) => q.bind(*d),
                            Bind::Time(t) => q.bind(*t),
                            Bind::Bool(b) => q.bind(*b),
                            Bind::Uuid(u) => match sqlx::types::Uuid::parse_str(u) {
                                Ok(u) => q.bind(u),
                                Err(_) => q.bind(u.clone()),
                            },
                        };
                    }
                    q.fetch_all(&mut *c)
                        .await
                        .map(|rows| rows.iter().map(pg_row).collect())
                        .map_err(|e| Failure::sqlx(Stage::Read, e))
                }
                Conn::MsSql(client) => {
                    let params: Vec<Box<dyn tiberius::ToSql>> = binds
                        .iter()
                        .map(|b| -> Box<dyn tiberius::ToSql> {
                            match b {
                                Bind::Null => Box::new(None::<i64>),
                                Bind::Int(i) => Box::new(*i),
                                Bind::Float(f) => Box::new(*f),
                                // Exact text; the statement casts it to the
                                // column's DECIMAL(p, s).
                                Bind::Decimal(t) => Box::new(t.clone()),
                                Bind::Text(t) => Box::new(t.clone()),
                                Bind::Timestamp(t) => Box::new(*t),
                                Bind::TimestampTz(t) => Box::new(*t),
                                Bind::Date(d) => Box::new(*d),
                                Bind::Time(t) => Box::new(*t),
                                Bind::Bool(b) => Box::new(*b),
                                Bind::Uuid(u) => match tiberius::Uuid::parse_str(u) {
                                    Ok(u) => Box::new(u),
                                    Err(_) => Box::new(u.clone()),
                                },
                            }
                        })
                        .collect();
                    let refs: Vec<&dyn tiberius::ToSql> =
                        params.iter().map(|p| p.as_ref()).collect();
                    let stream = client
                        .query(sql, &refs)
                        .await
                        .map_err(Failure::read_tiberius)?;
                    stream
                        .into_first_result()
                        .await
                        .map(|rows| rows.iter().map(mssql_row).collect())
                        .map_err(Failure::read_tiberius)
                }
            }
        };
        match tokio::time::timeout(READ_TIMEOUT, run).await {
            Ok(Ok(rows)) => Ok(rows),
            Ok(Err(failure)) => Err(failure.made_safe(clean)),
            Err(_) => Err(DbError::new(Stage::Read, "timed out")),
        }
    }

    /// Closes the connection; a failure to close is of no consequence.
    pub async fn close(self) {
        match self.conn {
            Conn::Sqlite(c) => drop(c.close().await),
            Conn::Postgres(c) => drop(c.close().await),
            Conn::MsSql(c) => drop(c.close().await),
        }
    }
}
