//! Conversion between [`Document`] and Azure Table entity JSON.
//!
//! Azure Table stores typed entity properties rather than raw JSON blobs.
//! Top-level JSON fields are flattened into entity properties so that
//! server-side `OData` `$filter` queries work. Nested objects and arrays
//! are serialized as JSON string properties.

use anyhow::{Context, anyhow};
use omnia_wasi_docstore::Document;
use serde_json::{Map, Value};

const SYSTEM_KEYS: &[&str] = &["PartitionKey", "RowKey", "Timestamp"];

// U+0000 cannot appear in a partition or row key (control characters are
// disallowed), so it never collides with either.
const ID_SEP: char = '\0';

/// Encode a partition key and row key into a composite document id.
#[must_use]
pub fn encode_id(partition_key: &str, row_key: &str) -> String {
    format!("{partition_key}{ID_SEP}{row_key}")
}

/// Decode a composite document id into `(partition_key, row_key)`.
///
/// # Errors
///
/// Returns an error when `id` carries no separator.
pub fn decode_id(id: &str) -> anyhow::Result<(&str, &str)> {
    id.split_once(ID_SEP).ok_or_else(|| {
        anyhow!("invalid document id {id:?}: expected '{{PartitionKey}}\\0{{RowKey}}'")
    })
}

/// Build the entity JSON body for `doc`, its top-level fields as typed
/// properties.
///
/// # Errors
///
/// Returns an error when the id is not composite or the body is not a JSON
/// object.
pub fn flatten(doc: &Document) -> anyhow::Result<Value> {
    let (pk, rk) = decode_id(&doc.id)?;
    let body: Value =
        serde_json::from_slice(&doc.data).context("document body is not valid JSON")?;
    let src = body.as_object().ok_or_else(|| anyhow!("document body must be a JSON object"))?;

    let properties = src
        .iter()
        .filter(|(key, _)| !is_metadata_key(key))
        .map(|(key, value)| typed_property(key, value))
        .collect::<anyhow::Result<Vec<_>>>()?;
    let entity: Map<String, Value> = [
        ("PartitionKey".to_owned(), Value::String(pk.to_owned())),
        ("RowKey".to_owned(), Value::String(rk.to_owned())),
    ]
    .into_iter()
    .chain(properties.into_iter().flatten())
    .collect();

    Ok(Value::Object(entity))
}

/// Convert an entity JSON body into a [`Document`] with a composite id.
///
/// Metadata properties are stripped; `Edm.Int64` and `Edm.Double` are
/// restored from their annotations, and nested values stay the JSON strings
/// [`flatten`] made of them.
///
/// # Errors
///
/// Returns an error when the entity is not a JSON object with a
/// `PartitionKey` and `RowKey`.
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

fn json_f64(f: f64) -> Option<Value> {
    serde_json::Number::from_f64(f).map(Value::Number)
}

fn is_metadata_key(key: &str) -> bool {
    SYSTEM_KEYS.contains(&key) || key.starts_with("odata.") || key.ends_with("@odata.type")
}

// Annotate what the service cannot infer from raw JSON: doubles, and
// integers past the i32 range, which travel as strings.
fn typed_property(key: &str, value: &Value) -> anyhow::Result<Vec<(String, Value)>> {
    let plain = |value: Value| vec![(key.to_owned(), value)];
    let annotated = |value: Value, edm_type: &str| {
        vec![(key.to_owned(), value), (format!("{key}@odata.type"), Value::String(edm_type.into()))]
    };
    Ok(match value {
        Value::Null => Vec::new(),
        Value::Bool(_) | Value::String(_) => plain(value.clone()),
        Value::Number(n) => {
            if n.is_f64() && !n.is_i64() {
                annotated(value.clone(), "Edm.Double")
            } else if let Some(v) = n.as_i64() {
                if (i64::from(i32::MIN)..=i64::from(i32::MAX)).contains(&v) {
                    plain(value.clone())
                } else {
                    annotated(Value::String(v.to_string()), "Edm.Int64")
                }
            } else if let Some(u) = n.as_u64() {
                let v = i64::try_from(u).map_err(|_overflow| {
                    anyhow!("numeric value for key `{key}` exceeds Azure Table integer range")
                })?;
                annotated(Value::String(v.to_string()), "Edm.Int64")
            } else {
                plain(value.clone())
            }
        }
        Value::Array(_) | Value::Object(_) => {
            let serialized =
                serde_json::to_string(value).context("serializing nested value to JSON string")?;
            plain(Value::String(serialized))
        }
    })
}
