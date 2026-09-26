use std::error::Error;

use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Utc};
use tokio_postgres::types::private::BytesMut;
use tokio_postgres::types::{IsNull, ToSql, Type, to_sql_checked};

pub type Param = Box<dyn ToSql + Send + Sync>;
pub type ParamRef<'a> = &'a (dyn ToSql + Sync);

// A wasi-sql `DataType` as a `ToSql` parameter.
#[derive(Debug)]
pub enum PgType {
    Int32(Option<i32>),
    Int64(Option<i64>),
    Uint32(Option<u32>),
    Float(Option<f32>),
    Double(Option<f64>),
    Text(Option<String>),
    Bool(Option<bool>),
    Date(Option<NaiveDate>),
    Time(Option<NaiveTime>),
    Timestamp(Option<NaiveDateTime>),
    TimestampTz(Option<DateTime<Utc>>),
    Binary(Option<Vec<u8>>),
}

impl ToSql for PgType {
    to_sql_checked!();

    fn to_sql(
        &self, ty: &Type, out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn Error + Send + Sync>> {
        match self {
            Self::Int32(value) => write_optional(value.as_ref(), ty, out),
            Self::Int64(value) => write_optional(value.as_ref(), ty, out),
            Self::Uint32(value) => write_optional(value.as_ref(), ty, out),
            Self::Float(value) => write_optional(value.as_ref(), ty, out),
            Self::Double(value) => write_optional(value.as_ref(), ty, out),
            Self::Text(value) => {
                if *ty == Type::JSON || *ty == Type::JSONB {
                    match value.as_ref() {
                        Some(raw) => {
                            let parsed: serde_json::Value = serde_json::from_str(raw)?;
                            tokio_postgres::types::Json(parsed).to_sql(ty, out)
                        }
                        None => Ok(IsNull::Yes),
                    }
                } else {
                    write_optional(value.as_ref(), ty, out)
                }
            }
            Self::Bool(value) => write_optional(value.as_ref(), ty, out),
            Self::Date(value) => write_optional(value.as_ref(), ty, out),
            Self::Time(value) => write_optional(value.as_ref(), ty, out),
            Self::Timestamp(value) => write_optional(value.as_ref(), ty, out),
            Self::TimestampTz(value) => write_optional(value.as_ref(), ty, out),
            Self::Binary(value) => write_optional(value.as_ref(), ty, out),
        }
    }

    fn accepts(_: &Type) -> bool {
        true
    }

    fn encode_format(&self, _ty: &Type) -> tokio_postgres::types::Format {
        tokio_postgres::types::Format::Binary
    }
}

fn write_optional<T>(
    value: Option<&T>, ty: &Type, out: &mut BytesMut,
) -> Result<IsNull, Box<dyn Error + Send + Sync>>
where
    T: ToSql + Sync,
{
    value.map_or_else(|| Ok(IsNull::Yes), |inner| inner.to_sql(ty, out))
}

// Encoding of each variant, JSON re-parse included; that the server accepts
// the bytes is the live suite's to prove.
#[cfg(test)]
mod tests {
    use chrono::TimeZone;

    use super::*;

    fn assert_serializes_successfully(value: &PgType, ty: &Type) {
        let mut buf = BytesMut::new();
        let result = value.to_sql(ty, &mut buf);
        assert!(result.is_ok());
        assert!(matches!(result.unwrap(), IsNull::No));
        assert!(!buf.is_empty());
    }

    fn assert_serializes_as_null(value: &PgType, ty: &Type) {
        let mut buf = BytesMut::new();
        let result = value.to_sql(ty, &mut buf);
        assert!(result.is_ok());
        assert!(matches!(result.unwrap(), IsNull::Yes));
        assert!(buf.is_empty());
    }

    #[test]
    fn simple_types() {
        assert_serializes_successfully(&PgType::Int32(Some(42)), &Type::INT4);
        assert_serializes_successfully(&PgType::Int64(Some(i64::MAX)), &Type::INT8);
        assert_serializes_successfully(&PgType::Uint32(Some(u32::MAX)), &Type::INT8);
        assert_serializes_successfully(&PgType::Float(Some(std::f32::consts::PI)), &Type::FLOAT4);
        assert_serializes_successfully(&PgType::Double(Some(std::f64::consts::E)), &Type::FLOAT8);

        assert_serializes_successfully(
            &PgType::Text(Some("Hello, World!".to_string())),
            &Type::TEXT,
        );
        assert_serializes_successfully(&PgType::Bool(Some(true)), &Type::BOOL);
        assert_serializes_successfully(&PgType::Bool(Some(false)), &Type::BOOL);
    }

    #[test]
    fn temporal_types() {
        let date = NaiveDate::from_ymd_opt(2024, 12, 25).unwrap();
        assert_serializes_successfully(&PgType::Date(Some(date)), &Type::DATE);

        let time = NaiveTime::from_hms_opt(14, 30, 45).unwrap();
        assert_serializes_successfully(&PgType::Time(Some(time)), &Type::TIME);

        let dt = NaiveDate::from_ymd_opt(2024, 1, 20).unwrap();
        let time = NaiveTime::from_hms_milli_opt(15, 30, 45, 123).unwrap();
        let timestamp = NaiveDateTime::new(dt, time);
        assert_serializes_successfully(&PgType::Timestamp(Some(timestamp)), &Type::TIMESTAMP);

        let timestamptz = Utc.with_ymd_and_hms(2024, 1, 20, 15, 30, 45).unwrap();
        assert_serializes_successfully(&PgType::TimestampTz(Some(timestamptz)), &Type::TIMESTAMPTZ);
    }

    #[test]
    fn binary_types() {
        let data = vec![0x01, 0x02, 0x03, 0x04, 0xFF];
        assert_serializes_successfully(&PgType::Binary(Some(data)), &Type::BYTEA);

        let mut buf = BytesMut::new();
        let empty = PgType::Binary(Some(vec![]));
        let result = empty.to_sql(&Type::BYTEA, &mut buf);
        assert!(result.is_ok());
        assert!(matches!(result.unwrap(), IsNull::No));
    }

    #[test]
    fn json() {
        let valid_json = PgType::Text(Some(r#"{"name":"test","value":42}"#.to_string()));
        assert_serializes_successfully(&valid_json, &Type::JSON);

        let valid_jsonb =
            PgType::Text(Some(r#"{"array":[1,2,3],"nested":{"key":"value"}}"#.to_string()));
        assert_serializes_successfully(&valid_jsonb, &Type::JSONB);

        let invalid_json = PgType::Text(Some("not valid json".to_string()));
        let mut buf = BytesMut::new();
        assert!(invalid_json.to_sql(&Type::JSON, &mut buf).is_err());

        assert_serializes_as_null(&PgType::Text(None), &Type::JSON);
    }

    #[test]
    fn null_values() {
        assert_serializes_as_null(&PgType::Int32(None), &Type::INT4);
        assert_serializes_as_null(&PgType::Int64(None), &Type::INT8);
        assert_serializes_as_null(&PgType::Float(None), &Type::FLOAT4);
        assert_serializes_as_null(&PgType::Text(None), &Type::TEXT);
        assert_serializes_as_null(&PgType::Bool(None), &Type::BOOL);
        assert_serializes_as_null(&PgType::Date(None), &Type::DATE);
        assert_serializes_as_null(&PgType::Time(None), &Type::TIME);
        assert_serializes_as_null(&PgType::Timestamp(None), &Type::TIMESTAMP);
        assert_serializes_as_null(&PgType::TimestampTz(None), &Type::TIMESTAMPTZ);
        assert_serializes_as_null(&PgType::Binary(None), &Type::BYTEA);
    }

    #[test]
    fn pgtype_binary_format() {
        let value = PgType::Int32(Some(42));
        assert!(matches!(value.encode_format(&Type::INT4), tokio_postgres::types::Format::Binary));
    }
}
