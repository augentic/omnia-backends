//! Live `omnia:sql` for the Postgres backend: the `omnia_test::contract::sql`
//! rows from a real guest, and the mappings, transactions, and pool
//! behaviour no guest row reaches.
//!
//! The round trips are the acceptance gate for the `into_param` /
//! `into_wasi_row` mapping pair, since only a real server proves it accepts
//! what `PgType` encodes. `batch_abandoned` is the transaction's gate: a
//! batch dropped part-way leaves nothing behind. `terminated_backend` is the
//! pool's gate: a session killed on the server is replaced at the next
//! checkout.
//!
//! `#[ignore]`d so it never touches the network in CI. The rows drop their
//! tables first, so run against a local database (`POSTGRES_URL`):
//! `cargo nextest run -p omnia-postgres --all-features --run-ignored all`.

#![cfg(not(target_arch = "wasm32"))]

use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use omnia::{Connect as _, ExitStatus, FromEnv as _};
use omnia_postgres::{Client, ConnectOptions};
use omnia_test::host::{Backends, Deployment};
use omnia_wasi_sql::{Connection, Error, Row, SqlClient, Statement, Value, WasiSql};

// Every sql program in `crates/test-programs` must have a matching test
// here; a new program without one fails to compile.
test_programs::foreach_sql!();

async fn connect() -> Result<Arc<dyn Connection>> {
    Client::connect().await?.open("default".to_owned()).await
}

// The generated `Value` has no `PartialEq`; compare debug renderings.
fn assert_field(row: &Row, name: &str, expected: &Value) {
    let field = row
        .fields
        .iter()
        .find(|f| f.name == name)
        .unwrap_or_else(|| panic!("column '{name}' missing from row"));
    assert_eq!(format!("{:?}", field.value), format!("{expected:?}"), "column '{name}' round-trip");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "live: drops the rows' tables in a local Postgres (POSTGRES_URL); run with --run-ignored"]
async fn sql_sql_db() -> Result<()> {
    let client = Client::connect().await?;
    let conn = client.open("default".to_owned()).await?;
    for table in ["batch_items", "rollback_items", "placeholder_items", "value_items"] {
        conn.execute(format!("DROP TABLE IF EXISTS {table}"), vec![]).await?;
    }
    drop(conn);
    let backends = Backends::defaults().await.sql(client);

    let status = Deployment::new()
        .guest("guest", test_programs::SQL_SQL_DB)
        .run_host::<WasiSql, _>(backends)
        .await?;
    assert_eq!(status, ExitStatus::SUCCESS, "guest failed");
    Ok(())
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
                Value::Int32(Some(42)),
                Value::Int64(Some(i64::MAX)),
                Value::Uint32(Some(u32::MAX)),
                Value::Float(Some(1.5)),
                Value::Double(Some(std::f64::consts::E)),
                Value::Str(Some("héllo, wörld".to_owned())),
                Value::Boolean(Some(true)),
                Value::Binary(Some(vec![0x00, 0x01, 0xFF])),
                Value::Uint64(Some(100)),
            ],
        )
        .await?;

    assert_eq!(rows.len(), 1, "one row returned");
    let row = &rows[0];
    assert_field(row, "i4", &Value::Int32(Some(42)));
    assert_field(row, "i8", &Value::Int64(Some(i64::MAX)));
    assert_field(row, "u32", &Value::Uint32(Some(u32::MAX)));
    assert_field(row, "f4", &Value::Float(Some(1.5)));
    assert_field(row, "f8", &Value::Double(Some(std::f64::consts::E)));
    assert_field(row, "txt", &Value::Str(Some("héllo, wörld".to_owned())));
    assert_field(row, "b", &Value::Boolean(Some(true)));
    assert_field(row, "bin", &Value::Binary(Some(vec![0x00, 0x01, 0xFF])));
    // uint64 is clamped into int8 on the way in, so it comes back as Int64.
    assert_field(row, "u64_clamped", &Value::Int64(Some(100)));
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
                Value::Date(Some("2024-12-25".to_owned())),
                Value::Time(Some("14:30:45.123456".to_owned())),
                // Naive format binds as `timestamp`; RFC3339 binds as
                // `timestamptz` (into_param's format detection).
                Value::Timestamp(Some("2024-01-20 15:30:45.123".to_owned())),
                Value::Timestamp(Some("2024-01-20T15:30:45+02:00".to_owned())),
                Value::Str(Some(json.to_owned())),
                Value::Str(Some(json.to_owned())),
            ],
        )
        .await?;

    assert_eq!(rows.len(), 1, "one row returned");
    let row = &rows[0];
    assert_field(row, "d", &Value::Date(Some("2024-12-25".to_owned())));
    assert_field(row, "t", &Value::Time(Some("14:30:45.123456".to_owned())));
    assert_field(row, "ts", &Value::Timestamp(Some("2024-01-20 15:30:45.123".to_owned())));
    // The +02:00 offset is normalized to UTC by the tz-aware path.
    assert_field(row, "tstz", &Value::Timestamp(Some("2024-01-20T13:30:45+00:00".to_owned())));

    // JSON returns re-serialized text; compare structurally, not byte-for-byte.
    let expected: serde_json::Value = serde_json::from_str(json)?;
    for col in ["jb", "j"] {
        let field = row.fields.iter().find(|f| f.name == col).expect("json column");
        let Value::Str(Some(raw)) = &field.value else {
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
                Value::Int32(None),
                Value::Str(None),
                Value::Timestamp(None),
                Value::Binary(None),
                Value::Str(None),
            ],
        )
        .await?;

    assert_eq!(rows.len(), 1, "one row returned");
    let row = &rows[0];
    assert_field(row, "i4", &Value::Int32(None));
    assert_field(row, "txt", &Value::Str(None));
    assert_field(row, "tstz", &Value::Timestamp(None));
    assert_field(row, "bin", &Value::Binary(None));
    assert_field(row, "jb", &Value::Str(None));
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "live: needs a reachable Postgres (POSTGRES_URL); run with --run-ignored"]
async fn uint64_overflow() -> Result<()> {
    let conn = connect().await?;

    let err = conn
        .query("SELECT $1::int8 AS n".to_owned(), vec![Value::Uint64(Some(u64::MAX))])
        .await
        .expect_err("u64::MAX cannot clamp into int8");
    assert!(matches!(err.downcast_ref::<Error>(), Some(Error::Invalid(_))), "{err}");
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "live: needs a reachable Postgres (POSTGRES_URL); run with --run-ignored"]
async fn execute_affected_counts() -> Result<()> {
    // A single Connection wraps one pooled session, so the temp table is
    // visible to every statement in this test and dropped on session close.
    let conn = connect().await?;

    let created = conn
        .execute("CREATE TEMP TABLE live_exec (id int4 PRIMARY KEY, label text)".to_owned(), vec![])
        .await?;
    assert_eq!(created, 0, "DDL affects no rows");

    let inserted = conn
        .execute(
            "INSERT INTO live_exec (id, label) VALUES ($1, $2), ($3, $4)".to_owned(),
            vec![
                Value::Int32(Some(1)),
                Value::Str(Some("one".to_owned())),
                Value::Int32(Some(2)),
                Value::Str(Some("two".to_owned())),
            ],
        )
        .await?;
    assert_eq!(inserted, 2, "both tuples inserted");

    let updated = conn
        .execute(
            "UPDATE live_exec SET label = label || '!' WHERE id <= $1".to_owned(),
            vec![Value::Int32(Some(2))],
        )
        .await?;
    assert_eq!(updated, 2, "both rows updated");

    let rows = conn.query("SELECT id, label FROM live_exec ORDER BY id".to_owned(), vec![]).await?;
    assert_eq!(rows.len(), 2, "both rows read back");
    assert_field(&rows[0], "id", &Value::Int32(Some(1)));
    assert_field(&rows[0], "label", &Value::Str(Some("one!".to_owned())));
    assert_field(&rows[1], "id", &Value::Int32(Some(2)));
    assert_field(&rows[1], "label", &Value::Str(Some("two!".to_owned())));
    Ok(())
}

// The one-session pool hands the abandoned batch's session to the follow-up
// insert, which the backend pid confirms, and a second pool reads only what
// committed. A session left inside the batch's transaction would hold the
// follow-up insert uncommitted.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "live: needs a reachable Postgres (POSTGRES_URL); run with --run-ignored"]
async fn batch_abandoned() -> Result<()> {
    let mut options = ConnectOptions::load_env()?;
    options.default_pool.pool_size = 1;
    let single = Client::connect_with(options).await?;
    let observer = connect().await?;
    observer.execute("DROP TABLE IF EXISTS abandoned_items".to_owned(), vec![]).await?;
    observer
        .execute("CREATE TABLE abandoned_items (id BIGINT PRIMARY KEY)".to_owned(), vec![])
        .await?;
    let pid = backend_pid(&single.open("default".to_owned()).await?).await?;

    let insert = "INSERT INTO abandoned_items (id) VALUES ($1)";
    let batch = single.open("default".to_owned()).await?.batch(vec![
        statement(insert, vec![Value::Int64(Some(1))]),
        statement("SELECT pg_sleep(1)", vec![]),
    ]);
    let abandoned = tokio::time::timeout(Duration::from_millis(250), batch).await;
    assert!(abandoned.is_err(), "the batch finished inside its timeout: {abandoned:?}");

    let conn = single.open("default".to_owned()).await?;
    assert_eq!(conn.execute(insert.to_owned(), vec![Value::Int64(Some(2))]).await?, 1);
    assert_eq!(backend_pid(&conn).await?, pid, "the follow-up ran on another session");
    drop(conn);

    let rows = observer.query("SELECT id FROM abandoned_items".to_owned(), vec![]).await?;
    assert_eq!(rows.len(), 1, "the observer read {rows:?}");
    assert_field(&rows[0], "id", &Value::Int64(Some(2)));
    observer.execute("DROP TABLE abandoned_items".to_owned(), vec![]).await?;
    Ok(())
}

async fn backend_pid(conn: &Arc<dyn Connection>) -> Result<i32> {
    let rows = conn.query("SELECT pg_backend_pid() AS pid".to_owned(), vec![]).await?;
    let Value::Int32(Some(pid)) = rows[0].fields[0].value else {
        panic!("pg_backend_pid() is int4, got {:?}", rows[0].fields[0].value);
    };
    Ok(pid)
}

fn statement(sql: &str, params: Vec<Value>) -> Statement {
    Statement {
        sql: sql.to_owned(),
        params,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "live: needs a reachable Postgres (POSTGRES_URL); run with --run-ignored"]
async fn error_kinds() -> Result<()> {
    let client = Client::connect().await?;

    let err = client.open("undeclared".to_owned()).await.expect_err("an undeclared pool opens");
    assert!(matches!(err.downcast_ref::<Error>(), Some(Error::NotFound(_))), "{err}");

    let conn = client.open("default".to_owned()).await?;
    let err = conn.query("SELEKT 1".to_owned(), vec![]).await.expect_err("a misspelt query runs");
    assert!(matches!(err.downcast_ref::<Error>(), Some(Error::Invalid(_))), "{err}");

    let err = conn
        .execute("SELECT $1::int4".to_owned(), vec![])
        .await
        .expect_err("a statement runs without its parameter");
    assert!(matches!(err.downcast_ref::<Error>(), Some(Error::Invalid(_))), "{err}");

    let err = conn
        .batch(vec![
            statement("SELECT 1", vec![]),
            statement("SELECT $1::date", vec![Value::Date(Some("not a date".to_owned()))]),
        ])
        .await
        .expect_err("a malformed date binds");
    let at_position = matches!(
        err.downcast_ref::<Error>(),
        Some(Error::Invalid(message)) if message.contains("statement 2 of the batch")
    );
    assert!(at_position, "{err}");
    Ok(())
}

// After `Client::connect` the pool holds one idle session, so the pid read
// here is the one the next checkout would reuse. The kill is issued from a
// second pool, which it leaves untouched.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "live: needs a reachable Postgres (POSTGRES_URL); run with --run-ignored"]
async fn terminated_backend() -> Result<()> {
    let client = Client::connect().await?;
    let pid = backend_pid(&client.open("default".to_owned()).await?).await?;

    let killed = connect()
        .await?
        .query(
            "SELECT pg_terminate_backend($1, 5000) AS killed".to_owned(),
            vec![Value::Int32(Some(pid))],
        )
        .await?;
    assert_field(&killed[0], "killed", &Value::Boolean(Some(true)));

    let rows =
        client.open("default".to_owned()).await?.query("SELECT 1".to_owned(), vec![]).await?;
    assert_eq!(rows.len(), 1, "the next checkout replaces the terminated session");
    Ok(())
}
