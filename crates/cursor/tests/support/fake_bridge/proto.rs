//! The `sdk.v1.SdkCustomToolCallbackService` messages the fake POSTs in the
//! binary protobuf codec, as `cursor-sdk-bridge` may. Field tags mirror
//! `sdk_custom_tool_callback_service.proto`.

use prost_types::value::Kind;
use serde_json::{Map, Value};

#[derive(Clone, PartialEq, prost::Message)]
pub struct CallCustomToolRequest {
    #[prost(string, tag = "1")]
    pub tool_name: String,
    #[prost(message, optional, tag = "2")]
    pub args: Option<prost_types::Struct>,
    #[prost(string, optional, tag = "3")]
    pub tool_call_id: Option<String>,
    #[prost(string, tag = "4")]
    pub agent_id: String,
}

#[derive(Clone, PartialEq, prost::Message)]
pub struct CallCustomToolResponse {
    #[prost(message, optional, tag = "1")]
    pub result: Option<prost_types::Struct>,
}

pub fn struct_to_value(fields: &prost_types::Struct) -> Value {
    Value::Object(
        fields.fields.iter().map(|(key, value)| (key.clone(), kind_to_value(value))).collect(),
    )
}

fn kind_to_value(value: &prost_types::Value) -> Value {
    match &value.kind {
        None | Some(Kind::NullValue(_)) => Value::Null,
        // Proto3's JSON mapping prints an integral double without a
        // fraction, so `42` survives the round trip as `42`.
        #[allow(clippy::cast_possible_truncation)]
        Some(Kind::NumberValue(number)) if number.fract() == 0.0 && number.abs() < 1e15 => {
            Value::from(*number as i64)
        }
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

pub fn value_to_struct(object: &Map<String, Value>) -> prost_types::Struct {
    prost_types::Struct {
        fields: object.iter().map(|(key, value)| (key.clone(), value_to_kind(value))).collect(),
    }
}

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
