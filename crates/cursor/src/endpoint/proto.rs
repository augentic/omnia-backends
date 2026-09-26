//! Hand-written prost messages for `sdk.v1.SdkCustomToolCallbackService` —
//! the one service the worker calls *into* this backend, and therefore the
//! one place the binary protobuf codec must be accepted alongside JSON.
//! Field tags mirror `sdk_custom_tool_callback_service.proto` verbatim.
//!
//! The fake `cursor-sdk-bridge` under `tests/support` includes this file by
//! path: it POSTs the same messages, and one codec on both sides keeps them
//! honest.

use prost_types::NullValue;
use prost_types::value::Kind;
use serde_json::{Map, Value};

// 2^53: every integer up to here is exactly an f64
const EXACT_INTEGER: f64 = 9_007_199_254_740_992.0;

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
        Some(Kind::NumberValue(number)) => number_to_value(*number),
        Some(Kind::StringValue(text)) => Value::String(text.clone()),
        Some(Kind::BoolValue(flag)) => Value::Bool(*flag),
        Some(Kind::StructValue(fields)) => struct_to_value(fields),
        Some(Kind::ListValue(list)) => {
            Value::Array(list.values.iter().map(kind_to_value).collect())
        }
    }
}

// A `Struct` number is always a double; proto3's JSON mapping prints an
// integral one without a fraction, so `42` must read back as `42`, not
// `42.0`, whichever codec the worker picked.
fn number_to_value(number: f64) -> Value {
    match number {
        #[allow(clippy::cast_possible_truncation, reason = "integral and within 2^53: exact")]
        integral if integral.fract() == 0.0 && integral.abs() <= EXACT_INTEGER => {
            Value::from(integral as i64)
        }
        real => serde_json::Number::from_f64(real).map_or(Value::Null, Value::Number),
    }
}

pub fn value_to_struct(object: &Map<String, Value>) -> prost_types::Struct {
    prost_types::Struct {
        fields: object.iter().map(|(key, value)| (key.clone(), value_to_kind(value))).collect(),
    }
}

// `Struct` numbers are f64 by definition, so integers beyond 2^53 round —
// the same loss every protobuf JSON mapping accepts.
fn value_to_kind(value: &Value) -> prost_types::Value {
    let kind = match value {
        Value::Null => Kind::NullValue(i32::from(NullValue::NullValue)),
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
