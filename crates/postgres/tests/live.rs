//! Live round-trips for the Postgres backend, driven through the `omnia:sql`
//! host boundary (`WasiSqlCtx`). This is the acceptance gate for the
//! `into_param` / `into_wasi_row` mapping pair: `into_wasi_row` cannot be
//! unit-tested (its input `tokio_postgres::Row` is unconstructable without a
//! real server), and only a real server proves it accepts what `PgType`
//! encodes. The `types.rs` / `sql.rs` unit tests remain the service-free CI
//! floor for the same mapping.
//!
//! The pool lifecycle tests prove idle connections are closed, a backend killed
//! server-side is replaced, and a connection the network silently stops
//! carrying fails a checkout in bounded time. Each tags its sessions with its
//! own `application_name`, so it counts and kills only its own.
//!
//! `#[ignore]`d so it never touches the network in CI. The README's "Live
//! tests" section has the full run recipe.

use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context as _, Result, bail};
use omnia::Backend;
use omnia_postgres::{Client, ConnectOptions, PoolEntry};
use omnia_wasi_sql::{Connection, DataType, WasiSqlCtx};
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::watch;
use tokio::task::JoinHandle;

async fn connect() -> Result<Arc<dyn Connection>> {
    let client = <Client as Backend>::connect().await?;
    client.open("default".to_owned()).await
}

fn postgres_url() -> Result<String> {
    std::env::var("POSTGRES_URL").context("POSTGRES_URL must be set")
}

async fn tagged_client(app: &str) -> Result<Client> {
    tagged_client_at(&postgres_url()?, app).await
}

async fn tagged_client_at(url: &str, app: &str) -> Result<Client> {
    let separator = if url.contains('?') { '&' } else { '?' };
    let options = ConnectOptions {
        default_pool: PoolEntry {
            name: "DEFAULT".to_owned(),
            uri: format!("{url}{separator}application_name={app}"),
            pool_size: 10,
        },
        additional_pools: vec![],
    };
    Client::connect_with(options).await
}

async fn sessions(app: &str) -> Result<i64> {
    let observer = tagged_client("omnia-live-observer").await?;
    let rows = observer
        .open("default".to_owned())
        .await?
        .query(
            "SELECT count(*) AS n FROM pg_stat_activity WHERE application_name = $1".to_owned(),
            vec![DataType::Str(Some(app.to_owned()))],
        )
        .await?;
    match &rows[0].fields[0].value {
        DataType::Int64(Some(n)) => Ok(*n),
        other => bail!("unexpected count {other:?}"),
    }
}

// A local TCP relay to the server that can be frozen: sockets stay open but no
// bytes pass, as when a network middlebox silently drops a connection. Only
// clients built on `url` go through it, so the server is never disturbed.
struct Relay {
    url: String,
    frozen: watch::Sender<bool>,
    accept: JoinHandle<()>,
}

impl Relay {
    // The relay port replaces the one in the URL's authority, since a `port`
    // query parameter would add a second port rather than override it.
    // `hostaddr` sends the TCP connection to the relay while TLS still
    // verifies the original host name.
    async fn start(url: &str) -> Result<Self> {
        let (scheme, rest) = url.split_once("://").context("POSTGRES_URL has no scheme")?;
        let (authority, tail) = rest.split_at(rest.find(['/', '?']).unwrap_or(rest.len()));
        let (userinfo, hostport) = match authority.rsplit_once('@') {
            Some((userinfo, hostport)) => (format!("{userinfo}@"), hostport),
            None => (String::new(), authority),
        };
        if hostport.starts_with('[') {
            bail!("the relay does not support IPv6 hosts");
        }
        let (host, port) = match hostport.rsplit_once(':') {
            Some((host, port)) => (host, port.parse::<u16>().context("invalid port")?),
            None => (hostport, 5432),
        };

        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let relay_port = listener.local_addr()?.port();
        let separator = if tail.contains('?') { '&' } else { '?' };
        let url =
            format!("{scheme}://{userinfo}{host}:{relay_port}{tail}{separator}hostaddr=127.0.0.1");

        let (frozen, gate) = watch::channel(false);
        let upstream = (host.to_owned(), port);
        let accept = tokio::spawn(async move {
            while let Ok((client, _)) = listener.accept().await {
                let upstream = upstream.clone();
                let gate = gate.clone();
                tokio::spawn(async move {
                    let Ok(server) = TcpStream::connect(upstream).await else { return };
                    let (client_read, client_write) = client.into_split();
                    let (server_read, server_write) = server.into_split();
                    tokio::join!(
                        pipe(client_read, server_write, gate.clone()),
                        pipe(server_read, client_write, gate),
                    );
                });
            }
        });
        Ok(Self { url, frozen, accept })
    }

    fn freeze(&self) {
        self.frozen.send_replace(true);
    }

    fn thaw(&self) {
        self.frozen.send_replace(false);
    }
}

impl Drop for Relay {
    fn drop(&mut self) {
        self.accept.abort();
    }
}

// Holds every read while frozen, so data neither arrives nor errors. Ends once
// either side closes or the relay is dropped.
async fn pipe(mut from: OwnedReadHalf, mut to: OwnedWriteHalf, mut gate: watch::Receiver<bool>) {
    let mut buf = vec![0; 16 * 1024];
    loop {
        if gate.wait_for(|frozen| !frozen).await.is_err() {
            return;
        }
        let n = match from.read(&mut buf).await {
            Ok(0) | Err(_) => return,
            Ok(n) => n,
        };
        if gate.wait_for(|frozen| !frozen).await.is_err() {
            return;
        }
        if to.write_all(&buf[..n]).await.is_err() {
            return;
        }
    }
}

// The generated `DataType` has no `PartialEq`; compare debug renderings.
fn assert_field(row: &omnia_wasi_sql::Row, name: &str, expected: &DataType) {
    let field = row
        .fields
        .iter()
        .find(|f| f.name == name)
        .unwrap_or_else(|| panic!("column '{name}' missing from row"));
    assert_eq!(format!("{:?}", field.value), format!("{expected:?}"), "column '{name}' round-trip");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "live: needs a reachable Postgres (POSTGRES_URL); run with --run-ignored"]
async fn scalar_types() -> Result<()> {
    let conn = connect().await?;

    let rows = conn
        .query(
            "SELECT $1::int4 AS i4, $2::int8 AS i8, $3::oid AS u32, $4::float4 AS f4, \
             $5::float8 AS f8, $6::text AS txt, $7::bool AS b, $8::bytea AS bin, \
             $9::int8 AS u64_clamped"
                .to_owned(),
            vec![
                DataType::Int32(Some(42)),
                DataType::Int64(Some(i64::MAX)),
                DataType::Uint32(Some(u32::MAX)),
                DataType::Float(Some(1.5)),
                DataType::Double(Some(std::f64::consts::E)),
                DataType::Str(Some("héllo, wörld".to_owned())),
                DataType::Boolean(Some(true)),
                DataType::Binary(Some(vec![0x00, 0x01, 0xFF])),
                DataType::Uint64(Some(100)),
            ],
        )
        .await?;

    assert_eq!(rows.len(), 1, "one row returned");
    let row = &rows[0];
    assert_field(row, "i4", &DataType::Int32(Some(42)));
    assert_field(row, "i8", &DataType::Int64(Some(i64::MAX)));
    assert_field(row, "u32", &DataType::Uint32(Some(u32::MAX)));
    assert_field(row, "f4", &DataType::Float(Some(1.5)));
    assert_field(row, "f8", &DataType::Double(Some(std::f64::consts::E)));
    assert_field(row, "txt", &DataType::Str(Some("héllo, wörld".to_owned())));
    assert_field(row, "b", &DataType::Boolean(Some(true)));
    assert_field(row, "bin", &DataType::Binary(Some(vec![0x00, 0x01, 0xFF])));
    // uint64 is clamped into int8 on the way in, so it comes back as Int64.
    assert_field(row, "u64_clamped", &DataType::Int64(Some(100)));
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "live: needs a reachable Postgres (POSTGRES_URL); run with --run-ignored"]
async fn temporal_and_json() -> Result<()> {
    let conn = connect().await?;

    let json = r#"{"a":[1,2,null],"b":{"nested":true}}"#;
    let rows = conn
        .query(
            "SELECT $1::date AS d, $2::time AS t, $3::timestamp AS ts, \
             $4::timestamptz AS tstz, $5::jsonb AS jb, $6::json AS j"
                .to_owned(),
            vec![
                DataType::Date(Some("2024-12-25".to_owned())),
                DataType::Time(Some("14:30:45.123456".to_owned())),
                // Naive format binds as `timestamp`; RFC3339 binds as
                // `timestamptz` (into_param's format detection).
                DataType::Timestamp(Some("2024-01-20 15:30:45.123".to_owned())),
                DataType::Timestamp(Some("2024-01-20T15:30:45+02:00".to_owned())),
                DataType::Str(Some(json.to_owned())),
                DataType::Str(Some(json.to_owned())),
            ],
        )
        .await?;

    assert_eq!(rows.len(), 1, "one row returned");
    let row = &rows[0];
    assert_field(row, "d", &DataType::Date(Some("2024-12-25".to_owned())));
    assert_field(row, "t", &DataType::Time(Some("14:30:45.123456".to_owned())));
    assert_field(row, "ts", &DataType::Timestamp(Some("2024-01-20 15:30:45.123".to_owned())));
    // The +02:00 offset is normalized to UTC by the tz-aware path.
    assert_field(row, "tstz", &DataType::Timestamp(Some("2024-01-20T13:30:45+00:00".to_owned())));

    // JSON returns re-serialized text; compare structurally, not byte-for-byte.
    let expected: serde_json::Value = serde_json::from_str(json)?;
    for col in ["jb", "j"] {
        let field = row.fields.iter().find(|f| f.name == col).expect("json column");
        let DataType::Str(Some(raw)) = &field.value else {
            panic!("column '{col}' should be Str(Some(..)), got {:?}", field.value);
        };
        let round_tripped: serde_json::Value = serde_json::from_str(raw)?;
        assert_eq!(round_tripped, expected, "column '{col}' structural round-trip");
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "live: needs a reachable Postgres (POSTGRES_URL); run with --run-ignored"]
async fn nulls() -> Result<()> {
    let conn = connect().await?;

    let rows = conn
        .query(
            "SELECT $1::int4 AS i4, $2::text AS txt, $3::timestamptz AS tstz, \
             $4::bytea AS bin, $5::jsonb AS jb"
                .to_owned(),
            vec![
                DataType::Int32(None),
                DataType::Str(None),
                DataType::Timestamp(None),
                DataType::Binary(None),
                DataType::Str(None),
            ],
        )
        .await?;

    assert_eq!(rows.len(), 1, "one row returned");
    let row = &rows[0];
    assert_field(row, "i4", &DataType::Int32(None));
    assert_field(row, "txt", &DataType::Str(None));
    assert_field(row, "tstz", &DataType::Timestamp(None));
    assert_field(row, "bin", &DataType::Binary(None));
    assert_field(row, "jb", &DataType::Str(None));
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "live: needs a reachable Postgres (POSTGRES_URL); run with --run-ignored"]
async fn uint64_overflow() -> Result<()> {
    let conn = connect().await?;

    let err = conn
        .query("SELECT $1::int8 AS n".to_owned(), vec![DataType::Uint64(Some(u64::MAX))])
        .await
        .expect_err("u64::MAX cannot clamp into int8");
    assert!(
        err.to_string().contains("exceeds i64::MAX"),
        "overflow error surfaces through the boundary: {err}"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "live: needs a reachable Postgres (POSTGRES_URL); run with --run-ignored"]
async fn exec_affected_counts() -> Result<()> {
    // A single Connection wraps one pooled session, so the temp table is
    // visible to every statement in this test and dropped on session close.
    let conn = connect().await?;

    let created = conn
        .exec("CREATE TEMP TABLE live_exec (id int4 PRIMARY KEY, label text)".to_owned(), vec![])
        .await?;
    assert_eq!(created, 0, "DDL affects no rows");

    let inserted = conn
        .exec(
            "INSERT INTO live_exec (id, label) VALUES ($1, $2), ($3, $4)".to_owned(),
            vec![
                DataType::Int32(Some(1)),
                DataType::Str(Some("one".to_owned())),
                DataType::Int32(Some(2)),
                DataType::Str(Some("two".to_owned())),
            ],
        )
        .await?;
    assert_eq!(inserted, 2, "both tuples inserted");

    let updated = conn
        .exec(
            "UPDATE live_exec SET label = label || '!' WHERE id <= $1".to_owned(),
            vec![DataType::Int32(Some(2))],
        )
        .await?;
    assert_eq!(updated, 2, "both rows updated");

    let rows = conn.query("SELECT id, label FROM live_exec ORDER BY id".to_owned(), vec![]).await?;
    assert_eq!(rows.len(), 2, "both rows read back");
    assert_field(&rows[0], "id", &DataType::Int32(Some(1)));
    assert_field(&rows[0], "label", &DataType::Str(Some("one!".to_owned())));
    assert_field(&rows[1], "id", &DataType::Int32(Some(2)));
    assert_field(&rows[1], "label", &DataType::Str(Some("two!".to_owned())));
    Ok(())
}

// --- Pool lifecycle ---

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "live: needs a reachable Postgres (POSTGRES_URL); run with --run-ignored"]
async fn idle_reap() -> Result<()> {
    let app = "omnia-live-idle-reap";
    let client = tagged_client(app).await?;

    let mut held = Vec::new();
    for _ in 0..5 {
        let conn = client.open("default".to_owned()).await?;
        conn.query("SELECT 1".to_owned(), vec![]).await?;
        held.push(conn);
    }
    drop(held);
    assert_eq!(sessions(app).await?, 5, "five idle sessions before the reap");

    // 60 s idle limit plus up to one 30 s sweep, with margin
    tokio::time::sleep(Duration::from_secs(100)).await;
    assert_eq!(sessions(app).await?, 0, "idle sessions closed server-side");

    drop(client);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "live: needs a reachable Postgres (POSTGRES_URL); run with --run-ignored"]
async fn terminated_backend() -> Result<()> {
    let app = "omnia-live-terminated";
    let client = tagged_client(app).await?;
    client.open("default".to_owned()).await?.query("SELECT 1".to_owned(), vec![]).await?;

    let observer = tagged_client("omnia-live-observer").await?;
    let killed = observer
        .open("default".to_owned())
        .await?
        .query(
            "SELECT pg_terminate_backend(pid, 5000) AS killed FROM pg_stat_activity \
             WHERE application_name = $1"
                .to_owned(),
            vec![DataType::Str(Some(app.to_owned()))],
        )
        .await?;
    assert!(!killed.is_empty(), "the idle pooled session was found");
    for row in &killed {
        assert_field(row, "killed", &DataType::Boolean(Some(true)));
    }

    let rows =
        client.open("default".to_owned()).await?.query("SELECT 1".to_owned(), vec![]).await?;
    assert_eq!(rows.len(), 1, "the next checkout gets a working connection");
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "live: needs a reachable Postgres (POSTGRES_URL); run with --run-ignored"]
async fn frozen_network() -> Result<()> {
    let relay = Relay::start(&postgres_url()?).await?;
    let client = tagged_client_at(&relay.url, "omnia-live-frozen").await?;
    let first = client.open("default".to_owned()).await?;
    let second = client.open("default".to_owned()).await?;
    drop((first, second));

    relay.freeze();
    let started = Instant::now();
    let checkout = client.open("default".to_owned()).await;
    let elapsed = started.elapsed();
    assert!(checkout.is_err(), "a silent network cannot hand out a connection");
    // two 0.5 s recycle checks then the 5 s create, never a TCP-timeout hang
    assert!(
        elapsed >= Duration::from_secs(5),
        "gave up before trying a new connection: {elapsed:?}"
    );
    assert!(elapsed < Duration::from_secs(15), "checkout not bounded: {elapsed:?}");

    relay.thaw();
    let rows =
        client.open("default".to_owned()).await?.query("SELECT 1".to_owned(), vec![]).await?;
    assert_eq!(rows.len(), 1, "the pool recovers once the network is back");
    Ok(())
}
