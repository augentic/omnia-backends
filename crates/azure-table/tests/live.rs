//! Live `omnia:docstore` for the Azure Table Storage backend: the
//! `omnia_test::contract::document` rows from a real guest, and the entity,
//! filter, and paging mappings no guest row reaches.
//!
//! Only the real service (Azurite or cloud) proves it accepts what the
//! document and filter mappings send. `#[ignore]`d so it never touches the
//! network in CI. The guest row empties its tables first, so run against a
//! storage account of your own (`AZURE_STORAGE_ACCOUNT` and
//! `AZURE_STORAGE_KEY`, plus `AZURE_TABLE_ENDPOINT` for Azurite):
//! `cargo nextest run -p omnia-azure-table --all-features --run-ignored all`.

#![cfg(not(target_arch = "wasm32"))]

use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::Result;
use omnia::{Connect, ExitStatus};
use omnia_azure_table::{Client, encode_id};
use omnia_test::host::{Backends, Deployment};
use omnia_wasi_docstore::{
    Condition, DocStoreClient, Document, Error, QueryOpts, SortField, Test, Value as Operand,
    WasiDocStore,
};
use serde_json::{Value, json};

// Every docstore program in `crates/test-programs` must have a matching test
// here; a new program without one fails to compile.
test_programs::foreach_docstore!();

const TABLE: &str = "omnialive";

async fn client() -> Result<Client> {
    let client = <Client as Connect>::connect().await?;
    client.ensure_table(TABLE).await?;
    Ok(client)
}

// A partition key unique to this test run, so reruns and residue from failed
// runs never collide.
fn unique_partition(tag: &str) -> String {
    let nanos = SystemTime::now().duration_since(UNIX_EPOCH).expect("clock").subsec_nanos();
    format!("{tag}-{}-{nanos}", std::process::id())
}

// the largest page the host asks for
const fn opts() -> QueryOpts {
    QueryOpts {
        order_by: Vec::new(),
        limit: 1000,
        continuation: None,
    }
}

fn test(field: &str, test: Test) -> Condition {
    Condition {
        field: field.to_owned(),
        test,
        negated: false,
    }
}

fn text(text: &str) -> Operand {
    Operand::Text(text.to_owned())
}

// The rows need their collections empty. A deleted table stays unusable
// until Azure finishes the delete, so the entities go one at a time.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "live: empties the rows' tables in Azure Table Storage (AZURE_STORAGE_ACCOUNT/KEY); run with --run-ignored"]
async fn docstore_document_db() -> Result<()> {
    let client = <Client as Connect>::connect().await?;
    for collection in ["insertexists", "portablequery", "optionalquery"] {
        let stored = client.query(collection.to_owned(), Vec::new(), opts()).await?;
        for document in stored.documents {
            client.delete(collection.to_owned(), document.id).await?;
        }
    }
    let backends = Backends::defaults().await.docstore(client);

    let status = Deployment::new()
        .guest("guest", test_programs::DOCSTORE_DOCUMENT_DB)
        .run_host::<WasiDocStore, _>(backends)
        .await?;
    assert_eq!(status, ExitStatus::SUCCESS, "guest failed");
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "live: needs Azure Table Storage (AZURE_STORAGE_ACCOUNT/KEY); run with --run-ignored"]
async fn insert_get_delete() -> Result<()> {
    let client = client().await?;

    let id = encode_id(&unique_partition("crud"), "row");
    let doc = Document {
        id: id.clone(),
        data: br#"{"hello":"world"}"#.to_vec(),
    };

    assert!(client.insert(TABLE.to_owned(), doc.clone()).await?, "a fresh id inserts");
    assert!(!client.insert(TABLE.to_owned(), doc).await?, "a stored id inserts again");
    let got = client.get(TABLE.to_owned(), id.clone()).await?.expect("document present");
    assert_eq!(got.id, id, "id round-trips through the boundary");

    assert!(client.delete(TABLE.to_owned(), id.clone()).await?, "first delete removes");
    assert!(!client.delete(TABLE.to_owned(), id.clone()).await?, "second delete is a miss");
    assert!(client.get(TABLE.to_owned(), id).await?.is_none(), "deleted document is gone");
    Ok(())
}

// The service accepts every mapped Edm shape and the annotations restore
// fidelity on the way back: Int64 beyond i32 range returns as a JSON number,
// doubles stay f64, nulls are omitted, nested values come back as their
// JSON-string serialization. A second `put` proves upsert semantics.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "live: needs Azure Table Storage (AZURE_STORAGE_ACCOUNT/KEY); run with --run-ignored"]
async fn edm_type() -> Result<()> {
    let client = client().await?;

    let id = encode_id(&unique_partition("edm"), "row");
    let big = 9_007_199_254_740_993_i64; // needs Edm.Int64: loses precision as f64
    let doc = Document {
        id: id.clone(),
        data: serde_json::to_vec(&json!({
            "Name": "Ada Lovelace",
            "Active": true,
            "Small": 42,
            "Big": big,
            "Rating": 4.5,
            "Missing": null,
            "Meta": {"x": 1},
            "Tags": ["a", "b"],
        }))?,
    };

    client.put(TABLE.to_owned(), doc).await?;
    let got = client.get(TABLE.to_owned(), id.clone()).await?.expect("document present");
    let body: Value = serde_json::from_slice(&got.data)?;

    assert_eq!(body["Name"], "Ada Lovelace");
    assert_eq!(body["Active"], true);
    assert_eq!(body["Small"], 42);
    assert_eq!(body["Big"], big, "Edm.Int64 annotation restores the number: {body}");
    assert!(body["Big"].is_i64(), "Int64 returns as a JSON number, not a string: {body}");
    let rating = body["Rating"].as_f64().expect("Edm.Double restores as f64");
    assert!((rating - 4.5).abs() < f64::EPSILON, "{body}");
    assert!(body.get("Missing").is_none(), "null fields are omitted: {body}");
    assert_eq!(body["Meta"].as_str(), Some(r#"{"x":1}"#), "nested object as JSON string");
    assert_eq!(body["Tags"].as_str(), Some(r#"["a","b"]"#), "array as JSON string");

    // Upsert: a second put replaces the entity.
    let updated = Document {
        id: id.clone(),
        data: serde_json::to_vec(&json!({"Name": "Grace Hopper"}))?,
    };
    client.put(TABLE.to_owned(), updated).await?;
    let got = client.get(TABLE.to_owned(), id.clone()).await?.expect("document present");
    let body: Value = serde_json::from_slice(&got.data)?;
    assert_eq!(body["Name"], "Grace Hopper", "put upserts");

    client.delete(TABLE.to_owned(), id).await?;
    Ok(())
}

// Server-side filter translation against the real OData layer: comparisons,
// groups, `in` expansion, and negation; `$top` produces a native
// continuation token that drains the partition without duplicates.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "live: needs Azure Table Storage (AZURE_STORAGE_ACCOUNT/KEY); run with --run-ignored"]
async fn filtered_query() -> Result<()> {
    let client = client().await?;
    let pk = unique_partition("query");
    let scoped = format!("{TABLE}/{pk}");

    for n in 1..=5 {
        let tier = if n % 2 == 0 { "even" } else { "odd" };
        let doc = Document {
            id: encode_id(&pk, &format!("row-{n}")),
            data: serde_json::to_vec(&json!({"Points": n, "Tier": tier}))?,
        };
        client.insert(TABLE.to_owned(), doc).await?;
    }

    // Comparison: Points > 3.
    let filter = vec![vec![test("Points", Test::Gt(Operand::Integer(3)))]];
    let result = client.query(scoped.clone(), filter, opts()).await?;
    assert_eq!(result.documents.len(), 2, "Points gt 3 matches rows 4 and 5");

    // Groups: Points >= 2 and (Tier in {even} or Points eq 5).
    let filter = vec![
        vec![test("Points", Test::Gte(Operand::Integer(2)))],
        vec![
            test("Tier", Test::In(vec![text("even")])),
            test("Points", Test::Eq(Operand::Integer(5))),
        ],
    ];
    let result = client.query(scoped.clone(), filter, opts()).await?;
    assert_eq!(result.documents.len(), 3, "rows 2, 4, and 5 are >= 2 and even or 5");

    // Negation: not (Tier eq odd).
    let odd = Condition {
        negated: true,
        ..test("Tier", Test::Eq(text("odd")))
    };
    let result = client.query(scoped.clone(), vec![vec![odd]], opts()).await?;
    assert_eq!(result.documents.len(), 2, "negation excludes the three odd rows");

    // Pagination: a limit below the partition size yields a native
    // continuation token; following it drains the rest without duplicates.
    let first = client.query(scoped.clone(), Vec::new(), QueryOpts { limit: 2, ..opts() }).await?;
    assert_eq!(first.documents.len(), 2, "limit caps the first page");
    let token = first.continuation.expect("a capped page carries a continuation token");
    let rest = client
        .query(
            scoped.clone(),
            Vec::new(),
            QueryOpts {
                continuation: Some(token),
                ..opts()
            },
        )
        .await?;
    assert_eq!(rest.documents.len(), 3, "the continuation drains the partition");
    let mut ids: Vec<String> =
        first.documents.into_iter().chain(rest.documents).map(|d| d.id).collect();
    ids.sort();
    ids.dedup();
    assert_eq!(ids.len(), 5, "pages never overlap");

    for n in 1..=5 {
        client.delete(TABLE.to_owned(), encode_id(&pk, &format!("row-{n}"))).await?;
    }
    Ok(())
}

// What the OData layer cannot serve is refused as `unsupported` before any
// data is pulled, never evaluated client-side or ignored.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "live: needs Azure Table Storage (AZURE_STORAGE_ACCOUNT/KEY); run with --run-ignored"]
async fn unsupported_query_shapes() -> Result<()> {
    let client = client().await?;

    for condition in [
        test("Name", Test::Contains("Ada".to_owned())),
        test("Name", Test::Null),
        test("Meta.x", Test::Eq(Operand::Integer(1))),
        test("Tags", Test::ArrayContains(text("a"))),
    ] {
        let shown = format!("{condition:?}");
        let filter = vec![vec![condition]];
        let err = client.query(TABLE.to_owned(), filter, opts()).await.expect_err(&shown);
        assert!(matches!(err.downcast_ref::<Error>(), Some(Error::Unsupported(_))), "{err:#}");
    }

    let sorted = QueryOpts {
        order_by: vec![SortField {
            field: "Name".to_owned(),
            descending: false,
        }],
        ..opts()
    };
    let err = client.query(TABLE.to_owned(), Vec::new(), sorted).await.expect_err("a sort ran");
    assert!(matches!(err.downcast_ref::<Error>(), Some(Error::Unsupported(_))), "{err:#}");
    Ok(())
}
