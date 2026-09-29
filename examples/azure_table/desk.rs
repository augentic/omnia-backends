use std::env;

use dotenvy::dotenv;
use omnia::Backend;
use omnia_azure_table::store::document::{decode_id, encode_id};
use omnia_azure_table::{Client, ConnectOptions};
use omnia_wasi_docstore::{
    ComparisonOp, Document, FilterTree, QueryOpts, QueryResult, ScalarValue, WasiDocStoreCtx,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tracing_subscriber::filter::EnvFilter;
use tracing_subscriber::fmt;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

/// Demonstrates the serde pattern for explicit `@odata.type` annotations.
///
/// Azure Table cannot infer `Edm.Guid`, `Edm.DateTime`, or `Edm.Binary` from
/// raw JSON strings.  Include a sibling `{field}@odata.type` key in the
/// serialized output so that [`omnia_azure_table::store::document::flatten`] passes it through verbatim.
#[derive(Debug, Serialize, Deserialize)]
struct TypedRecord {
    #[serde(rename = "Name")]
    name: String,

    #[serde(rename = "SessionId")]
    session_id: String,
    #[serde(rename = "SessionId@odata.type")]
    session_id_type: String,

    #[serde(rename = "ExpiresAt")]
    expires_at: String,
    #[serde(rename = "ExpiresAt@odata.type")]
    expires_at_type: String,
}

const TABLE: &str = "testJsondb";
const PK_DESK: &str = "desk";
const PK_MOBILE: &str = "mobile";
const SCOPE_DESK: &str = "testJsondb/desk";
const SCOPE_MOBILE: &str = "testJsondb/mobile";

fn doc(pk: &str, rk: &str, data: Value) -> Document {
    Document {
        id: encode_id(pk, rk),
        data: serde_json::to_vec(&data).expect("serialize"),
    }
}

fn body(d: &Document) -> Value {
    serde_json::from_slice(&d.data).expect("deserialize")
}

/// Extract just the RowKey portion from composite document IDs for readable assertions.
fn row_keys(r: &QueryResult) -> Vec<&str> {
    r.documents.iter().map(|d| decode_id(&d.id).expect("valid composite id").1).collect()
}

fn cmp(field: &str, op: ComparisonOp, value: ScalarValue) -> FilterTree {
    FilterTree::Compare {
        field: field.into(),
        op,
        value,
    }
}

async fn q(
    client: &Client, collection: &str, filter: Option<FilterTree>, opts: QueryOpts,
) -> QueryResult {
    client.query(collection.into(), filter, opts).await.expect("query")
}

async fn q_err(client: &Client, filter: FilterTree) -> String {
    client
        .query(SCOPE_DESK.into(), Some(filter), QueryOpts::default())
        .await
        .unwrap_err()
        .to_string()
}

fn pass(label: &str) {
    println!("  PASS  {label}");
}

fn seed() -> Vec<(&'static str, &'static str, Value)> {
    vec![
        (
            PK_DESK,
            "alice",
            json!({
                "Name": "Alice Montgomery",
                "IsActive": true,
                "Points": 950,
                "Rating": 4.8,
                "Created": "2026-01-15T10:30:00Z",
                "Region": "US-West"
            }),
        ),
        (
            PK_DESK,
            "bob",
            json!({
                "Name": "Bob Burns",
                "IsActive": false,
                "Points": 200,
                "Rating": 3.2,
                "Created": "2026-02-20T14:00:00Z",
                "Region": "EU-North"
            }),
        ),
        (
            PK_DESK,
            "charlie",
            json!({
                "Name": "Charlie Delta",
                "IsActive": true,
                "Points": 3000,
                "LargeId": 9_007_199_254_740_993_i64,
                "Rating": 4.95,
                "Created": "2025-12-01T08:00:00Z",
                "Region": "US-West"
            }),
        ),
        (
            PK_MOBILE,
            "dana",
            json!({
                "Name": "Dana Null",
                "IsActive": true,
                "Points": 500,
                "Rating": 3.9,
                "Created": "2026-03-10T16:45:00Z"
            }),
        ),
        (
            PK_MOBILE,
            "eve",
            json!({
                "Name": "Eve Eavesdrop",
                "IsActive": false,
                "Points": 150,
                "Rating": 2.1,
                "Created": "2026-01-01T00:00:00Z",
                "Region": "AP-South"
            }),
        ),
    ]
}

#[tokio::main]
pub async fn main() {
    dotenv().ok();
    tracing_subscriber::registry().with(fmt::layer()).with(EnvFilter::from_default_env()).init();

    let opts = ConnectOptions {
        name: env::var("AZURE_STORAGE_ACCOUNT").expect("Set AZURE_STORAGE_ACCOUNT"),
        key: env::var("AZURE_STORAGE_KEY").expect("Set AZURE_STORAGE_KEY"),
        endpoint: env::var("AZURE_TABLE_ENDPOINT").unwrap_or_default(),
    };
    let client = Client::connect_with(opts).await.expect("connect");

    println!("Azure Table document store desk test");
    println!("============================\n");

    // ── Setup ────────────────────────────────────────────────────────
    let created = client.ensure_table(TABLE).await.expect("ensure_table");
    println!("Table '{TABLE}': {}", if created { "created" } else { "already exists" });

    let existing = q(&client, TABLE, None, QueryOpts::default()).await;
    for d in &existing.documents {
        client.delete(TABLE.into(), d.id.clone()).await.expect("cleanup delete");
    }
    if !existing.documents.is_empty() {
        println!("Cleaned {} leftover rows", existing.documents.len());
    }

    let records = seed();
    for (pk, rk, data) in &records {
        client
            .insert(TABLE.into(), doc(pk, rk, data.clone()))
            .await
            .unwrap_or_else(|e| panic!("insert {pk}/{rk}: {e}"));
    }
    println!("Seeded {} records (2 partitions)\n", records.len());

    // ── 1. Get round-trip ────────────────────────────────────────────
    let alice_id = encode_id(PK_DESK, "alice");
    let alice = client
        .get(TABLE.into(), alice_id.clone())
        .await
        .expect("get alice")
        .expect("alice missing");
    assert_eq!(alice.id, alice_id);
    let ab = body(&alice);
    assert_eq!(ab["Name"], "Alice Montgomery");
    assert_eq!(ab["IsActive"], true);
    assert_eq!(ab["Points"], 950);
    assert_eq!(ab["Rating"], 4.8);
    assert_eq!(ab["Created"], "2026-01-15T10:30:00Z");
    assert_eq!(ab["Region"], "US-West");
    pass("get round-trip (all fields verified)");

    // ── 2. Cross-partition query returns all 5 docs ──────────────────
    let r = q(&client, TABLE, None, QueryOpts::default()).await;
    assert_eq!(r.documents.len(), 5, "expected 5, got {}", r.documents.len());
    for d in &r.documents {
        assert!(d.id.contains('\0'), "composite id should contain \\0: {:?}", d.id);
    }
    pass("cross-partition query (5 records, all composite ids)");

    // ── 3. Partition-scoped query: desk ──────────────────────────────
    let r = q(&client, SCOPE_DESK, None, QueryOpts::default()).await;
    assert_eq!(r.documents.len(), 3, "desk partition should have 3");
    let rks = row_keys(&r);
    assert!(rks.contains(&"alice"));
    assert!(rks.contains(&"bob"));
    assert!(rks.contains(&"charlie"));
    pass("partition-scoped query: desk (3 records)");

    // ── 4. Partition-scoped query: mobile ────────────────────────────
    let r = q(&client, SCOPE_MOBILE, None, QueryOpts::default()).await;
    assert_eq!(r.documents.len(), 2, "mobile partition should have 2");
    let rks = row_keys(&r);
    assert!(rks.contains(&"dana"));
    assert!(rks.contains(&"eve"));
    pass("partition-scoped query: mobile (2 records)");

    // ── 5. Compare: Eq Boolean (within desk partition) ───────────────
    let r = q(
        &client,
        SCOPE_DESK,
        Some(cmp("IsActive", ComparisonOp::Eq, ScalarValue::Boolean(true))),
        QueryOpts::default(),
    )
    .await;
    assert_eq!(r.documents.len(), 2);
    pass("Compare: IsActive eq true in desk (2)");

    // ── 6. Compare: Gt Int32 (within desk partition) ─────────────────
    let r = q(
        &client,
        SCOPE_DESK,
        Some(cmp("Points", ComparisonOp::Gt, ScalarValue::Int32(500))),
        QueryOpts::default(),
    )
    .await;
    assert_eq!(r.documents.len(), 2);
    let rks = row_keys(&r);
    assert!(rks.contains(&"alice"));
    assert!(rks.contains(&"charlie"));
    pass("Compare: Points gt 500 in desk (2)");

    // ── 7. Compare: Lte Float64 (within desk partition) ──────────────
    let r = q(
        &client,
        SCOPE_DESK,
        Some(cmp("Rating", ComparisonOp::Lte, ScalarValue::Float64(3.9))),
        QueryOpts::default(),
    )
    .await;
    assert_eq!(r.documents.len(), 1);
    pass("Compare: Rating le 3.9 in desk (1)");

    // ── 8. Compare: Gte String (timestamp, cross-partition) ──────────
    let r = q(
        &client,
        TABLE,
        Some(cmp("Created", ComparisonOp::Gte, ScalarValue::Str("2026-02-01T00:00:00Z".into()))),
        QueryOpts::default(),
    )
    .await;
    assert_eq!(r.documents.len(), 2);
    let rks = row_keys(&r);
    assert!(rks.contains(&"bob"));
    assert!(rks.contains(&"dana"));
    pass("Compare: Created ge '2026-02-01' cross-partition (2)");

    // ── 9. InList (within desk partition) ────────────────────────────
    let r = q(
        &client,
        SCOPE_DESK,
        Some(FilterTree::InList {
            field: "Points".into(),
            values: vec![ScalarValue::Int32(200), ScalarValue::Int32(950)],
        }),
        QueryOpts::default(),
    )
    .await;
    assert_eq!(r.documents.len(), 2);
    let rks = row_keys(&r);
    assert!(rks.contains(&"alice"));
    assert!(rks.contains(&"bob"));
    pass("InList: Points in [200, 950] in desk (2)");

    // ── 10. NotInList (within desk partition) ────────────────────────
    let r = q(
        &client,
        SCOPE_DESK,
        Some(FilterTree::NotInList {
            field: "Name".into(),
            values: vec![
                ScalarValue::Str("Alice Montgomery".into()),
                ScalarValue::Str("Bob Burns".into()),
            ],
        }),
        QueryOpts::default(),
    )
    .await;
    assert_eq!(r.documents.len(), 1);
    pass("NotInList: Name not in [Alice, Bob] in desk (1)");

    // ── 11. Compare: Ne (within desk partition) ─────────────────────
    let r = q(
        &client,
        SCOPE_DESK,
        Some(cmp("Region", ComparisonOp::Ne, ScalarValue::Str("US-West".into()))),
        QueryOpts::default(),
    )
    .await;
    assert_eq!(r.documents.len(), 1);
    let rks = row_keys(&r);
    assert!(rks.contains(&"bob"));
    pass("Compare: Region ne 'US-West' in desk (1)");

    // ── 12. And (all server-side, within desk partition) ─────────────
    let r = q(
        &client,
        SCOPE_DESK,
        Some(FilterTree::And(vec![
            cmp("IsActive", ComparisonOp::Eq, ScalarValue::Boolean(true)),
            cmp("Points", ComparisonOp::Gt, ScalarValue::Int32(500)),
        ])),
        QueryOpts::default(),
    )
    .await;
    assert_eq!(r.documents.len(), 2);
    let rks = row_keys(&r);
    assert!(rks.contains(&"alice"));
    assert!(rks.contains(&"charlie"));
    pass("And: IsActive=true AND Points gt 500 in desk (2)");

    // ── 13. Edm.Int64 round-trip ────────────────────────────────────
    let charlie_id = encode_id(PK_DESK, "charlie");
    let charlie =
        client.get(TABLE.into(), charlie_id).await.expect("get charlie").expect("charlie missing");
    let cb = body(&charlie);
    assert_eq!(cb["LargeId"], 9_007_199_254_740_993_i64, "Edm.Int64 should round-trip as number");
    pass("Edm.Int64 round-trip (LargeId)");

    // ── 14. Duplicate insert fails ──────────────────────────────────
    let dup =
        client.insert(TABLE.into(), doc(PK_DESK, "alice", json!({"Name": "Duplicate"}))).await;
    assert!(dup.is_err(), "inserting duplicate id should fail");
    pass("duplicate insert rejected");

    // ── 15. Get non-existent returns None ────────────────────────────
    let missing = client
        .get(TABLE.into(), encode_id(PK_DESK, "no-such-id"))
        .await
        .expect("get non-existent should not error");
    assert!(missing.is_none(), "non-existent id should return None");
    pass("get non-existent returns None");

    // ── 16–20. Unsupported filters are rejected ─────────────────────
    let err = q_err(
        &client,
        FilterTree::Contains {
            field: "Name".into(),
            pattern: "Delta".into(),
        },
    )
    .await;
    assert!(err.contains("not supported"), "Contains should be rejected: {err}");
    pass("Contains rejected");

    let err = q_err(
        &client,
        FilterTree::StartsWith {
            field: "Name".into(),
            pattern: "Eve".into(),
        },
    )
    .await;
    assert!(err.contains("not supported"), "StartsWith should be rejected: {err}");
    pass("StartsWith rejected");

    let err = q_err(
        &client,
        FilterTree::EndsWith {
            field: "Name".into(),
            pattern: "Burns".into(),
        },
    )
    .await;
    assert!(err.contains("not supported"), "EndsWith should be rejected: {err}");
    pass("EndsWith rejected");

    let err = q_err(&client, FilterTree::IsNull("Region".into())).await;
    assert!(err.contains("not supported"), "IsNull should be rejected: {err}");
    pass("IsNull rejected");

    let err = q_err(&client, FilterTree::IsNotNull("Region".into())).await;
    assert!(err.contains("not supported"), "IsNotNull should be rejected: {err}");
    pass("IsNotNull rejected");

    // ── 21. Not (server-side, within desk partition) ─────────────────
    let r = q(
        &client,
        SCOPE_DESK,
        Some(FilterTree::Not(Box::new(cmp(
            "IsActive",
            ComparisonOp::Eq,
            ScalarValue::Boolean(true),
        )))),
        QueryOpts::default(),
    )
    .await;
    assert_eq!(r.documents.len(), 1);
    let rks = row_keys(&r);
    assert!(rks.contains(&"bob"));
    pass("Not: NOT(IsActive eq true) in desk (1)");

    // ── 22. Or (server-side, cross-partition) ────────────────────────
    let r = q(
        &client,
        TABLE,
        Some(FilterTree::Or(vec![
            cmp("Points", ComparisonOp::Eq, ScalarValue::Int32(200)),
            cmp("Points", ComparisonOp::Eq, ScalarValue::Int32(150)),
        ])),
        QueryOpts::default(),
    )
    .await;
    assert_eq!(r.documents.len(), 2);
    let rks = row_keys(&r);
    assert!(rks.contains(&"bob"));
    assert!(rks.contains(&"eve"));
    pass("Or: Points=200 OR Points=150 cross-partition (2)");

    // ── 23. Limit (within desk partition) ────────────────────────────
    let r = q(
        &client,
        SCOPE_DESK,
        None,
        QueryOpts {
            limit: Some(2),
            ..Default::default()
        },
    )
    .await;
    assert_eq!(r.documents.len(), 2);
    pass("limit=2");

    // ── 24. Offset rejected ─────────────────────────────────────────
    let err = client
        .query(
            SCOPE_DESK.into(),
            None,
            QueryOpts {
                offset: Some(1),
                limit: Some(2),
                ..Default::default()
            },
        )
        .await
        .unwrap_err()
        .to_string();
    assert!(err.contains("offset is not supported"), "offset should be rejected: {err}");
    pass("offset rejected");

    // ── 25. Edm.Guid + Edm.DateTime via serde annotations ───────────
    let typed = TypedRecord {
        name: "Frank Typed".into(),
        session_id: "a1b2c3d4-e5f6-7890-abcd-ef1234567890".into(),
        session_id_type: "Edm.Guid".into(),
        expires_at: "2026-12-31T23:59:59Z".into(),
        expires_at_type: "Edm.DateTime".into(),
    };
    let typed_json = serde_json::to_vec(&typed).expect("serialize TypedRecord");
    let frank_id = encode_id(PK_DESK, "frank");
    client
        .insert(
            TABLE.into(),
            Document {
                id: frank_id.clone(),
                data: typed_json,
            },
        )
        .await
        .expect("insert frank");

    let frank = client
        .get(TABLE.into(), frank_id.clone())
        .await
        .expect("get frank")
        .expect("frank missing");
    let fb = body(&frank);
    assert_eq!(fb["Name"], "Frank Typed");
    assert_eq!(fb["SessionId"], "a1b2c3d4-e5f6-7890-abcd-ef1234567890");
    assert_eq!(fb["ExpiresAt"], "2026-12-31T23:59:59Z");
    assert!(
        fb.get("SessionId@odata.type").is_none(),
        "odata annotations should be stripped on read"
    );
    pass("Edm.Guid + Edm.DateTime via serde annotations (round-trip)");

    client.delete(TABLE.into(), frank_id).await.expect("delete frank");

    // ── 26. Put + get (verify change) ────────────────────────────────
    client
        .put(
            TABLE.into(),
            doc(
                PK_DESK,
                "alice",
                json!({
                    "Name": "Alice Montgomery",
                    "IsActive": true,
                    "Points": 999,
                    "Rating": 4.8,
                    "Created": "2026-01-15T10:30:00Z",
                    "Region": "US-West"
                }),
            ),
        )
        .await
        .expect("put alice");
    let updated = client
        .get(TABLE.into(), encode_id(PK_DESK, "alice"))
        .await
        .expect("get alice after put")
        .expect("alice missing after put");
    assert_eq!(body(&updated)["Points"], 999);
    pass("put + get (verify Points changed to 999)");

    // ── 27. Round-trip update from cross-partition query ─────────────
    let all = q(&client, TABLE, None, QueryOpts::default()).await;
    let dana = all.documents.iter().find(|d| d.id == encode_id(PK_MOBILE, "dana")).expect("dana");
    let mut dana_body: Value = serde_json::from_slice(&dana.data).expect("parse dana");
    dana_body["Points"] = json!(777);
    let updated_dana = Document {
        id: dana.id.clone(),
        data: serde_json::to_vec(&dana_body).unwrap(),
    };
    client.put(TABLE.into(), updated_dana).await.expect("put dana from query result");
    let fetched_dana = client
        .get(TABLE.into(), encode_id(PK_MOBILE, "dana"))
        .await
        .expect("get dana")
        .expect("dana missing");
    assert_eq!(body(&fetched_dana)["Points"], 777);
    pass("cross-partition round-trip update (put from query result)");

    // ── 28. Round-trip delete from cross-partition query ─────────────
    let eve_id = encode_id(PK_MOBILE, "eve");
    let deleted = client.delete(TABLE.into(), eve_id.clone()).await.expect("delete eve");
    assert!(deleted, "eve should have existed");
    let gone = client.get(TABLE.into(), eve_id.clone()).await.expect("get eve after delete");
    assert!(gone.is_none(), "eve should be gone");
    pass("cross-partition round-trip delete (delete from query result)");

    // ── 29. Double-delete returns false ──────────────────────────────
    let again = client.delete(TABLE.into(), eve_id).await.expect("double-delete eve");
    assert!(!again, "double-delete should return false");
    pass("double-delete returns false");

    // ── 30. Same RowKey in different partitions ─────────────────────
    client
        .insert(
            TABLE.into(),
            doc(PK_MOBILE, "alice", json!({"Name": "Mobile Alice", "Points": 111})),
        )
        .await
        .expect("insert mobile/alice");
    let desk_alice = client
        .get(TABLE.into(), encode_id(PK_DESK, "alice"))
        .await
        .expect("get desk/alice")
        .expect("desk/alice missing");
    let mobile_alice = client
        .get(TABLE.into(), encode_id(PK_MOBILE, "alice"))
        .await
        .expect("get mobile/alice")
        .expect("mobile/alice missing");
    assert_ne!(desk_alice.id, mobile_alice.id, "composite ids must differ");
    assert_eq!(body(&desk_alice)["Name"], "Alice Montgomery");
    assert_eq!(body(&mobile_alice)["Name"], "Mobile Alice");
    client.delete(TABLE.into(), encode_id(PK_MOBILE, "alice")).await.expect("cleanup mobile/alice");
    pass("same RowKey in different partitions (distinct composite ids)");

    // ── 31. Bare RowKey (no separator) rejected for get ──────────────
    let err = client.get(TABLE.into(), "alice".into()).await;
    assert!(err.is_err(), "bare RowKey without partition separator should fail");
    pass("bare RowKey rejected for get");

    // ── 32. Table-only collection works with composite id ────────────
    let ok = client.get(TABLE.into(), encode_id(PK_DESK, "alice")).await;
    assert!(ok.is_ok(), "table-only collection should work with composite id");
    pass("table-only collection accepted for get with composite id");

    // ── Teardown ─────────────────────────────────────────────────────
    let remaining = q(&client, TABLE, None, QueryOpts::default()).await;
    for d in &remaining.documents {
        client.delete(TABLE.into(), d.id.clone()).await.expect("teardown delete");
    }
    println!("\nCleaned up {} rows. All tests passed!", remaining.documents.len());
}
