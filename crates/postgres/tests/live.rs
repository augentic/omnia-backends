//! Live round-trips for the Postgres backend, driven through the `omnia:sql`
//! host boundary (`WasiSqlCtx`). This is the acceptance gate for the
//! `into_param` / `into_wasi_row` mapping pair: `into_wasi_row` cannot be
//! unit-tested (its input `tokio_postgres::Row` is unconstructable without a
//! real server), and only a real server proves it accepts what `PgType`
//! encodes. The `types.rs` / `sql.rs` unit tests remain the service-free CI
//! floor for the same mapping.
//!
//! `#[ignore]`d so it never touches the network in CI. Run against a reachable
//! database (`POSTGRES_URL`):
//! `cargo nextest run -p omnia-postgres --run-ignored all`.

use std::sync::Arc;

use anyhow::Result;
use omnia::Backend;
use omnia_postgres::Client;
use omnia_wasi_sql::{Connection, DataType, WasiSqlCtx};

async fn connect() -> Result<Arc<dyn Connection>> {
    let client = <Client as Backend>::connect().await?;
    client.open("default".to_owned()).await
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
async fn scalar_type_round_trips() -> Result<()> {
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
async fn temporal_and_json_round_trips() -> Result<()> {
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
async fn null_round_trips() -> Result<()> {
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
async fn uint64_overflow_rejected_at_boundary() -> Result<()> {
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
