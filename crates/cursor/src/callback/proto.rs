//! Hand-written prost messages for `sdk.v1.SdkCustomToolCallbackService` —
//! the one service the bridge calls *into* this backend, and therefore the
//! one place the binary protobuf codec must be accepted alongside JSON.
//! Field tags mirror `sdk_custom_tool_callback_service.proto` verbatim.

use prost_types::value::Kind;
use serde_json::{Map, Value};

#[derive(Clone, PartialEq, prost::Message)]
pub struct CallCustomToolRequest {
    #[prost(string, tag = "1")]
    pub tool_name: String,
    /// Tool arguments as a JSON object.
    #[prost(message, optional, tag = "2")]
    pub args: Option<prost_types::Struct>,
    #[prost(string, optional, tag = "3")]
    pub tool_call_id: Option<String>,
    #[prost(string, tag = "4")]
    pub agent_id: String,
}

#[derive(Clone, PartialEq, prost::Message)]
pub struct CallCustomToolResponse {
    /// Tool result as a JSON object.
    #[prost(message, optional, tag = "1")]
    pub result: Option<prost_types::Struct>,
}

/// Decode a `google.protobuf.Struct` into the equivalent JSON object.
pub fn struct_to_value(fields: &prost_types::Struct) -> Value {
    Value::Object(
        fields.fields.iter().map(|(key, value)| (key.clone(), kind_to_value(value))).collect(),
    )
}

fn kind_to_value(value: &prost_types::Value) -> Value {
    match &value.kind {
        None | Some(Kind::NullValue(_)) => Value::Null,
        Some(Kind::NumberValue(number)) => {
            serde_json::Number::from_f64(*number).map_or(Value::Null, Value::Number)
        }
        Some(Kind::StringValue(text)) => Value::String(text.clone()),
        Some(Kind::BoolValue(flag)) => Value::Bool(*flag),
        Some(Kind::StructValue(fields)) => struct_to_value(fields),
        Some(Kind::ListValue(list)) => {
            Value::Array(list.values.iter().map(kind_to_value).collect())
        }
    }
}

/// Encode a JSON object as a `google.protobuf.Struct`.
pub fn value_to_struct(object: &Map<String, Value>) -> prost_types::Struct {
    prost_types::Struct {
        fields: object.iter().map(|(key, value)| (key.clone(), value_to_kind(value))).collect(),
    }
}

// `Struct` numbers are f64 by definition, so integers beyond 2^53 round —
// the same loss every protobuf JSON mapping accepts.
#[allow(clippy::cast_precision_loss)]
fn value_to_kind(value: &Value) -> prost_types::Value {
    let kind = match value {
        Value::Null => Kind::NullValue(0),
        Value::Bool(flag) => Kind::BoolValue(*flag),
        Value::Number(number) => Kind::NumberValue(number.as_f64().unwrap_or_default()),
        Value::String(text) => Kind::StringValue(text.clone()),
        Value::Array(items) => Kind::ListValue(prost_types::ListValue {
            values: items.iter().map(value_to_kind).collect(),
        }),
        Value::Object(object) => Kind::StructValue(value_to_struct(object)),
    };
    prost_types::Value { kind: Some(kind) }
}

// Deliberate unit tests: the Struct codec is pure translation (CI floor).
#[cfg(test)]
mod tests {
    use serde_json::{Value, json};

    use super::{struct_to_value, value_to_struct};

    #[test]
    fn struct_round_trip_preserves_nested_shapes() {
        let original = json!({
            "text": "hello",
            "count": 3.5,
            "flag": true,
            "none": null,
            "nested": { "list": [1.0, "two", false, null, { "deep": "yes" }] },
        });
        let Value::Object(object) = &original else { unreachable!() };
        let round_tripped = struct_to_value(&value_to_struct(object));
        assert_eq!(round_tripped, original);
    }

    #[test]
    fn empty_object_round_trips() {
        let Value::Object(object) = json!({}) else { unreachable!() };
        assert_eq!(struct_to_value(&value_to_struct(&object)), json!({}));
    }
}
