//! Conversion between [`Document`] and Azure Table entity JSON.
//!
//! Azure Table stores typed entity properties rather than raw JSON blobs.
//! Top-level JSON fields are flattened into entity properties so that
//! server-side `OData` `$filter` queries work. Nested objects and arrays
//! are serialized as JSON string properties.

use anyhow::{Context, anyhow};
use omnia_wasi_docstore::Document;
use serde_json::{Map, Value};

/// Azure Table system / `OData` metadata properties stripped during unflatten.
const SYSTEM_KEYS: &[&str] = &["PartitionKey", "RowKey", "Timestamp"];

/// Separator for composite document IDs. U+0000 is forbidden in Azure Table
/// partition and row keys (control characters U+0000–U+001F are disallowed),
/// so it is unambiguous.
const ID_SEP: char = '\0';

/// Encode a partition key and row key into a composite document ID.
#[must_use]
pub fn encode_id(partition_key: &str, row_key: &str) -> String {
    format!("{partition_key}{ID_SEP}{row_key}")
}

/// Decode a composite document ID into `(partition_key, row_key)`.
///
/// # Errors
///
/// Returns an error if `id` does not contain the `\0` separator.
pub fn decode_id(id: &str) -> anyhow::Result<(&str, &str)> {
    id.split_once(ID_SEP).ok_or_else(|| {
        anyhow!("invalid document id {id:?}: expected '{{PartitionKey}}\\0{{RowKey}}'")
    })
}

/// Build an Azure Table entity JSON body from a [`Document`].
///
/// The document's composite `id` (`{PartitionKey}\0{RowKey}`) is split to
/// recover both Azure Table keys. Top-level JSON fields become entity
/// properties. `OData` type annotations (`@odata.type`) are added for types
/// that Azure Table cannot infer from the JSON representation alone.
///
/// # Errors
///
/// Returns an error if the document id is not a valid composite id, the body
/// is not valid JSON, or the body is not a JSON object.
pub fn flatten(doc: &Document) -> anyhow::Result<Value> {
    let (pk, rk) = decode_id(&doc.id)?;
    let body: Value =
        serde_json::from_slice(&doc.data).context("document body is not valid JSON")?;
    let src = body.as_object().ok_or_else(|| anyhow!("document body must be a JSON object"))?;

    let mut entity = Map::new();
    entity.insert("PartitionKey".into(), Value::String(pk.to_owned()));
    entity.insert("RowKey".into(), Value::String(rk.to_owned()));

    for (key, value) in src {
        if is_metadata_key(key) {
            continue;
        }
        insert_typed_property(&mut entity, key, value)?;
    }

    Ok(Value::Object(entity))
}

/// Convert an Azure Table entity JSON (from a GET/query response) into a
/// [`Document`], stripping system and `OData` metadata properties.
///
/// The returned document's `id` is a composite `{PartitionKey}\0{RowKey}`
/// string that uniquely identifies the entity across all partitions.
///
/// Type annotations (`@odata.type`) are used to restore fidelity for
/// `Edm.Int64` (string → i64 number) and `Edm.Double` (ensure f64
/// representation). Nested objects and arrays that were serialized as JSON
/// strings during [`flatten`] are **not** automatically restored — they
/// remain as string values. See the crate README for details.
///
/// # Errors
///
/// Returns an error if the entity is not a JSON object or is missing
/// `PartitionKey` or `RowKey`.
pub fn unflatten(entity: &Value) -> anyhow::Result<Document> {
    let obj = entity.as_object().ok_or_else(|| anyhow!("entity must be a JSON object"))?;

    let pk = obj
        .get("PartitionKey")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("entity missing PartitionKey"))?;
    let rk = obj
        .get("RowKey")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("entity missing RowKey"))?;
    let id = encode_id(pk, rk);

    let mut data_map = Map::new();
    for (key, value) in obj {
        if is_metadata_key(key) {
            continue;
        }
        let restored = restore_typed_value(obj, key, value);
        data_map.insert(key.clone(), restored);
    }

    let data = serde_json::to_vec(&data_map).context("serializing document body")?;
    Ok(Document { id, data })
}

/// Use `@odata.type` annotations to restore type fidelity where Azure Table
/// serialization loses the original JSON type.
fn restore_typed_value(obj: &Map<String, Value>, key: &str, value: &Value) -> Value {
    let type_key = format!("{key}@odata.type");
    let Some(edm_type) = obj.get(&type_key).and_then(Value::as_str) else {
        return value.clone();
    };

    match edm_type {
        "Edm.Int64" => value
            .as_str()
            .and_then(|s| s.parse::<i64>().ok())
            .map_or_else(|| value.clone(), |n| Value::Number(n.into())),
        "Edm.Double" => match value {
            Value::Number(n) => n
                .as_f64()
                .map_or_else(|| value.clone(), |f| json_f64(f).unwrap_or_else(|| value.clone())),
            Value::String(s) => {
                s.parse::<f64>().ok().and_then(json_f64).unwrap_or_else(|| value.clone())
            }
            _ => value.clone(),
        },
        _ => value.clone(),
    }
}

/// Create a `serde_json::Value::Number` from an f64, returning `None` for
/// `NaN` / infinity which JSON cannot represent.
fn json_f64(f: f64) -> Option<Value> {
    serde_json::Number::from_f64(f).map(Value::Number)
}

fn is_metadata_key(key: &str) -> bool {
    SYSTEM_KEYS.contains(&key) || key.starts_with("odata.") || key.ends_with("@odata.type")
}

/// Insert a single user property into the entity map, adding `@odata.type`
/// annotations where Azure Table cannot infer the type from raw JSON.
fn insert_typed_property(
    entity: &mut Map<String, Value>, key: &str, value: &Value,
) -> anyhow::Result<()> {
    match value {
        Value::Null => {}
        Value::Bool(_) | Value::String(_) => {
            entity.insert(key.into(), value.clone());
        }
        Value::Number(n) => {
            if n.is_f64() && !n.is_i64() {
                entity.insert(key.into(), value.clone());
                entity.insert(format!("{key}@odata.type"), Value::String("Edm.Double".into()));
            } else if let Some(v) = n.as_i64() {
                if (i64::from(i32::MIN)..=i64::from(i32::MAX)).contains(&v) {
                    entity.insert(key.into(), value.clone());
                } else {
                    entity.insert(key.into(), Value::String(v.to_string()));
                    entity.insert(format!("{key}@odata.type"), Value::String("Edm.Int64".into()));
                }
            } else if let Some(u) = n.as_u64() {
                if let Ok(v) = i64::try_from(u) {
                    entity.insert(key.into(), Value::String(v.to_string()));
                    entity.insert(format!("{key}@odata.type"), Value::String("Edm.Int64".into()));
                } else {
                    return Err(anyhow!(
                        "numeric value for key `{key}` exceeds Azure Table integer range"
                    ));
                }
            } else {
                entity.insert(key.into(), value.clone());
            }
        }
        Value::Array(_) | Value::Object(_) => {
            let serialized =
                serde_json::to_string(value).context("serializing nested value to JSON string")?;
            entity.insert(key.into(), Value::String(serialized));
        }
    }
    Ok(())
}

// Unit tests cover the pure codec and rejection paths only. The happy-path
// flatten/annotation mappings (Edm.Int64/Double, nulls, nested-as-string) are
// proven against the real service by `tests/live.rs::edm_type_round_trip`.
#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    // ── encode_id / decode_id ────────────────────────────────────────

    #[test]
    fn encode_decode_id() {
        let id = encode_id("pk1", "rk1");
        let (pk, rk) = decode_id(&id).unwrap();
        assert_eq!(pk, "pk1");
        assert_eq!(rk, "rk1");
    }

    #[test]
    fn decode_id_bare_rowkey() {
        let err = decode_id("just-a-rowkey").unwrap_err().to_string();
        assert!(err.contains("invalid document id"), "{err}");
    }

    #[test]
    fn decode_id_special_chars() {
        let id = encode_id("tenant/a", "row'key");
        let (pk, rk) = decode_id(&id).unwrap();
        assert_eq!(pk, "tenant/a");
        assert_eq!(rk, "row'key");
    }

    // ── flatten ──────────────────────────────────────────────────────

    #[test]
    fn flatten_reserved_keys() {
        let doc = Document {
            id: encode_id("pk1", "r1"),
            data: serde_json::to_vec(&json!({
                "PartitionKey": "evil",
                "RowKey": "evil",
                "Timestamp": "evil",
                "Name": "Alice"
            }))
            .unwrap(),
        };
        let entity = flatten(&doc).unwrap();
        let obj = entity.as_object().unwrap();
        assert_eq!(obj["PartitionKey"], "pk1", "injected PartitionKey must not be overwritten");
        assert_eq!(obj["RowKey"], "r1", "injected RowKey must not be overwritten");
        assert_eq!(obj["Name"], "Alice");
    }

    #[test]
    fn flatten_u64_overflow_rejected() {
        let doc = Document {
            id: encode_id("pk1", "r1"),
            data: serde_json::to_vec(&json!({ "bigU": u64::MAX })).unwrap(),
        };
        let err = flatten(&doc).unwrap_err().to_string();
        assert!(err.contains("exceeds Azure Table integer range"), "{err}");
    }

    #[test]
    fn flatten_bare_rowkey() {
        let doc = Document {
            id: "no-separator".into(),
            data: serde_json::to_vec(&json!({"a": 1})).unwrap(),
        };
        let err = flatten(&doc).unwrap_err().to_string();
        assert!(err.contains("invalid document id"), "{err}");
    }

    // ── unflatten ────────────────────────────────────────────────────

    #[test]
    fn unflatten_int64() {
        let entity = json!({
            "PartitionKey": "pk1",
            "RowKey": "r1",
            "LargeId": "9007199254740993",
            "LargeId@odata.type": "Edm.Int64",
        });
        let doc = unflatten(&entity).unwrap();
        assert_eq!(doc.id, encode_id("pk1", "r1"));
        let body: Value = serde_json::from_slice(&doc.data).unwrap();
        assert_eq!(body["LargeId"], 9_007_199_254_740_993_i64);
    }

    #[test]
    fn unflatten_double() {
        let entity = json!({
            "PartitionKey": "pk1",
            "RowKey": "r1",
            "Rating": 3,
            "Rating@odata.type": "Edm.Double",
        });
        let doc = unflatten(&entity).unwrap();
        assert_eq!(doc.id, encode_id("pk1", "r1"));
        let body: Value = serde_json::from_slice(&doc.data).unwrap();
        assert!(body["Rating"].is_f64(), "expected f64, got {:?}", body["Rating"]);
        assert!((body["Rating"].as_f64().unwrap() - 3.0).abs() < f64::EPSILON);
    }

    #[test]
    fn unflatten_system_keys() {
        let entity = json!({
            "PartitionKey": "pk1",
            "RowKey": "r1",
            "Timestamp": "2026-01-01T00:00:00Z",
            "odata.etag": "W/\"datetime'...'\"",
            "Name": "Alice",
            "Name@odata.type": "Edm.String",
        });
        let doc = unflatten(&entity).unwrap();
        assert_eq!(doc.id, encode_id("pk1", "r1"));
        let body: Value = serde_json::from_slice(&doc.data).unwrap();
        let obj = body.as_object().unwrap();
        assert!(!obj.contains_key("PartitionKey"));
        assert!(!obj.contains_key("RowKey"));
        assert!(!obj.contains_key("Timestamp"));
        assert!(!obj.contains_key("odata.etag"));
        assert!(!obj.contains_key("Name@odata.type"));
        assert_eq!(obj["Name"], "Alice");
    }

    #[test]
    fn unflatten_missing_partition_key() {
        let entity = json!({
            "RowKey": "r1",
            "Name": "Alice",
        });
        let err = unflatten(&entity).unwrap_err().to_string();
        assert!(err.contains("missing PartitionKey"), "{err}");
    }
}
