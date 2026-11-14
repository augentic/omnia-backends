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
    ComparisonOp, DocStoreClient, Document, Error, FilterTree, QueryOpts, ScalarValue, SortField,
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

const fn opts() -> QueryOpts {
    QueryOpts {
        order_by: Vec::new(),
        limit: None,
        offset: None,
        continuation: None,
    }
}

// The rows need their collections empty. A deleted table stays unusable
// until Azure finishes the delete, so the entities go one at a time.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "live: empties the rows' tables in Azure Table Storage (AZURE_STORAGE_ACCOUNT/KEY); run with --run-ignored"]
async fn docstore_document_db() -> Result<()> {
    let client = <Client as Connect>::connect().await?;
    for collection in ["insertexists", "portablequery", "optionalquery"] {
        let stored = client.query(collection.to_owned(), None, opts()).await?;
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
// combinators, and InList expansion; `$top` produces a native continuation
// token that drains the partition without duplicates.
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
    let filter = FilterTree::Compare {
        field: "Points".to_owned(),
        op: ComparisonOp::Gt,
        value: ScalarValue::Int32(3),
    };
    let result = client.query(scoped.clone(), Some(filter), opts()).await?;
    assert_eq!(result.documents.len(), 2, "Points gt 3 matches rows 4 and 5");

    // Combinator + InList: Points >= 2 and Tier in {even}.
    let filter = FilterTree::And(vec![
        FilterTree::Compare {
            field: "Points".to_owned(),
            op: ComparisonOp::Gte,
            value: ScalarValue::Int32(2),
        },
        FilterTree::InList {
            field: "Tier".to_owned(),
            values: vec![ScalarValue::Str("even".to_owned())],
        },
    ]);
    let result = client.query(scoped.clone(), Some(filter), opts()).await?;
    assert_eq!(result.documents.len(), 2, "rows 2 and 4 are even and >= 2");

    // Negation: not (Tier eq odd).
    let filter = FilterTree::Not(Box::new(FilterTree::Compare {
        field: "Tier".to_owned(),
        op: ComparisonOp::Eq,
        value: ScalarValue::Str("odd".to_owned()),
    }));
    let result = client.query(scoped.clone(), Some(filter), opts()).await?;
    assert_eq!(result.documents.len(), 2, "negation excludes the three odd rows");

    // Pagination: a limit below the partition size yields a native
    // continuation token; following it drains the rest without duplicates.
    let first = client
        .query(
            scoped.clone(),
            None,
            QueryOpts {
                limit: Some(2),
                ..opts()
            },
        )
        .await?;
    assert_eq!(first.documents.len(), 2, "limit caps the first page");
    let token = first.continuation.expect("a capped page carries a continuation token");
    let rest = client
        .query(
            scoped.clone(),
            None,
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
    let compare = |field: &str, value| FilterTree::Compare {
        field: field.to_owned(),
        op: ComparisonOp::Eq,
        value,
    };

    for filter in [
        FilterTree::Contains {
            field: "Name".to_owned(),
            pattern: "Ada".to_owned(),
        },
        compare("Name", ScalarValue::Null),
        compare("Meta.x", ScalarValue::Int32(1)),
    ] {
        let shown = format!("{filter:?}");
        let err = client.query(TABLE.to_owned(), Some(filter), opts()).await.expect_err(&shown);
        assert!(matches!(err.downcast_ref::<Error>(), Some(Error::Unsupported(_))), "{err:#}");
    }

    let sorted = QueryOpts {
        order_by: vec![SortField {
            field: "Name".to_owned(),
            descending: false,
        }],
        ..opts()
    };
    let skipped = QueryOpts {
        offset: Some(1),
        ..opts()
    };
    for options in [sorted, skipped] {
        let shown = format!("{options:?}");
        let err = client.query(TABLE.to_owned(), None, options).await.expect_err(&shown);
        assert!(matches!(err.downcast_ref::<Error>(), Some(Error::Unsupported(_))), "{err:#}");
    }

    let zero = QueryOpts {
        limit: Some(0),
        ..opts()
    };
    let err = client.query(TABLE.to_owned(), None, zero).await.expect_err("a zero limit ran");
    assert!(matches!(err.downcast_ref::<Error>(), Some(Error::Invalid(_))), "{err:#}");
    Ok(())
}
