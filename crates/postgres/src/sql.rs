use std::convert::TryFrom;
use std::str::FromStr;
use std::sync::Arc;

use anyhow::{Context, anyhow, bail};
use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Utc};
use deadpool_postgres::Object;
use futures::future::FutureExt;
use omnia_wasi_sql::{Connection, DataType, Field, FutureResult, Row, WasiSqlCtx};
use tokio_postgres::row::Row as PgRow;

use crate::Client;
use crate::types::{Param, ParamRef, PgType};

impl WasiSqlCtx for Client {
    fn open(&self, name: String) -> FutureResult<Arc<dyn Connection>> {
        tracing::debug!("getting connection {name}");

        let pool = match self.0.get(&name.to_ascii_uppercase()) {
            Some(p) => p.clone(),
            None => {
                return futures::future::ready(Err(anyhow!("unknown postgres pool '{name}'")))
                    .boxed();
            }
        };
        async move {
            let cnn = pool.get().await.context("issue getting connection")?;
            Ok(Arc::new(PostgresConnection(Arc::new(cnn))) as Arc<dyn Connection>)
        }
        .boxed()
    }
}

#[derive(Debug)]
pub struct PostgresConnection(Arc<Object>);

impl Connection for PostgresConnection {
    fn query(&self, query: String, params: Vec<DataType>) -> FutureResult<Vec<Row>> {
        tracing::debug!("query: {query}, params: {params:?}");
        let cnn = Arc::clone(&self.0);

        async move {
            let mut pg_params: Vec<Param> = Vec::new();
            for p in &params {
                pg_params.push(into_param(p)?);
            }
            let param_refs: Vec<ParamRef> =
                pg_params.iter().map(|b| b.as_ref() as ParamRef).collect();

            let pg_rows = cnn
                .query(&query, &param_refs)
                .await
                .inspect_err(|e| {
                    dbg!(e);
                })
                .context("query failed")?;
            tracing::debug!("query returned {} rows", pg_rows.len());

            let mut wasi_rows = Vec::new();
            for (idx, r) in pg_rows.iter().enumerate() {
                let row = match into_wasi_row(r, idx) {
                    Ok(row) => row,
                    Err(e) => {
                        tracing::error!("failed to convert row: {e:?}");
                        return Err(anyhow!("failed to convert row: {e:?}"));
                    }
                };
                wasi_rows.push(row);
            }

            Ok(wasi_rows)
        }
        .boxed()
    }

    fn exec(&self, query: String, params: Vec<DataType>) -> FutureResult<u32> {
        tracing::debug!("exec: {query}, params: {params:?}");
        let cnn = Arc::clone(&self.0);

        async move {
            let mut pg_params: Vec<Param> = Vec::new();
            for p in &params {
                pg_params.push(into_param(p)?);
            }
            let param_refs: Vec<ParamRef> =
                pg_params.iter().map(|b| b.as_ref() as ParamRef).collect();

            let affected = match cnn.execute(&query, &param_refs).await {
                Ok(count) => count,
                Err(e) => {
                    tracing::error!("exec failed: {e}");
                    return Err(anyhow!("exec failed: {e}"));
                }
            };
            Ok(u32::try_from(affected).unwrap_or(u32::MAX))
        }
        .boxed()
    }
}

fn parse_date(source: Option<&str>) -> anyhow::Result<Option<NaiveDate>> {
    source.map(|s| NaiveDate::from_str(s).context("invalid date format")).transpose()
}

fn parse_time(source: Option<&str>) -> anyhow::Result<Option<NaiveTime>> {
    source.map(|s| NaiveTime::from_str(s).context("invalid time format")).transpose()
}

fn parse_timestamp_naive(source: Option<&str>) -> anyhow::Result<Option<NaiveDateTime>> {
    source
        .map(|s| {
            NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.f")
                .context("invalid naive timestamp format")
        })
        .transpose()
}

fn into_param(value: &DataType) -> anyhow::Result<Param> {
    let pg_value = match value {
        DataType::Int32(v) => PgType::Int32(*v),
        DataType::Int64(v) => PgType::Int64(*v),
        DataType::Uint32(v) => PgType::Uint32(*v),
        DataType::Uint64(v) => {
            // postgres has no u64; anything past i64::MAX is rejected
            let converted = match v {
                Some(raw) => {
                    let clamped = i64::try_from(*raw).map_err(|err| {
                        anyhow!("uint64 value {raw} exceeds i64::MAX and cannot be stored: {err}")
                    })?;
                    Some(clamped)
                }
                None => None,
            };
            PgType::Int64(converted)
        }
        DataType::Float(v) => PgType::Float(*v),
        DataType::Double(v) => PgType::Double(*v),
        DataType::Str(v) => PgType::Text(v.clone()),
        DataType::Boolean(v) => PgType::Bool(*v),
        DataType::Date(v) => PgType::Date(parse_date(v.as_deref())?),
        DataType::Time(v) => PgType::Time(parse_time(v.as_deref())?),
        DataType::Timestamp(v) => {
            // RFC 3339 (with zone) first, else the naive form
            if let Some(s) = v.as_deref() {
                if let Ok(ts) = DateTime::parse_from_rfc3339(s) {
                    PgType::TimestampTz(Some(ts.with_timezone(&Utc)))
                } else {
                    PgType::Timestamp(parse_timestamp_naive(v.as_deref())?)
                }
            } else {
                PgType::Timestamp(None)
            }
        }
        DataType::Binary(v) => PgType::Binary(v.clone()),
    };

    Ok(Box::new(pg_value) as Param)
}

fn into_wasi_row(pg_row: &PgRow, idx: usize) -> anyhow::Result<Row> {
    let mut fields = Vec::new();
    for (i, col) in pg_row.columns().iter().enumerate() {
        let name = col.name().to_string();
        tracing::debug!("attempting to convert column '{name}' with type '{:?}'", col.type_());
        tracing::debug!("column type name: {}", col.type_().name());
        let value = match col.type_().name() {
            "int4" => {
                let v: Option<i32> = pg_row.try_get(i)?;
                DataType::Int32(v)
            }
            "int8" => {
                let v: Option<i64> = pg_row.try_get(i)?;
                DataType::Int64(v)
            }
            "oid" => {
                let v: Option<u32> = pg_row.try_get(i)?;
                DataType::Uint32(v)
            }
            "float4" => {
                let v: Option<f32> = pg_row.try_get(i)?;
                DataType::Float(v)
            }
            "float8" => {
                let v: Option<f64> = pg_row.try_get(i)?;
                DataType::Double(v)
            }
            "text" | "varchar" | "name" | "_char" => {
                let v: Option<String> = pg_row.try_get(i)?;
                DataType::Str(v)
            }
            "bool" => {
                let v: Option<bool> = pg_row.try_get(i)?;
                DataType::Boolean(v)
            }
            "date" => {
                let v: Option<NaiveDate> = pg_row.try_get(i)?;
                let formatted = v.map(|date| date.to_string());
                DataType::Date(formatted)
            }
            "time" => {
                let v: Option<NaiveTime> = pg_row.try_get(i)?;
                let formatted = v.map(|time| time.to_string());
                DataType::Time(formatted)
            }
            "timestamp" => {
                let v: Option<NaiveDateTime> = pg_row.try_get(i)?;
                let formatted = v.map(|dt| dt.to_string());
                DataType::Timestamp(formatted)
            }
            "timestamptz" => {
                let v: Option<DateTime<Utc>> = pg_row.try_get(i)?;
                let formatted = v.map(|dtz| dtz.to_rfc3339());
                DataType::Timestamp(formatted)
            }
            "json" | "jsonb" => {
                let v: Option<tokio_postgres::types::Json<serde_json::Value>> =
                    pg_row.try_get(i)?;
                let as_str = v.map(|json| json.0.to_string());
                DataType::Str(as_str)
            }
            "bytea" => {
                let v: Option<Vec<u8>> = pg_row.try_get(i)?;
                DataType::Binary(v)
            }
            other => {
                bail!("unsupported column type: {other}");
            }
        };
        tracing::debug!("converted column '{name}' to value '{:?}'", value);
        fields.push(Field { name, value });
    }

    Ok(Row {
        index: idx.to_string(),
        fields,
    })
}

// Parsing and parameter mapping. Row conversion needs a real database (a
// `tokio_postgres::Row` cannot be built by hand) and is covered by the live
// suite.
#[cfg(test)]
mod tests {
    use chrono::{Datelike, Timelike};

    use super::*;

    #[test]
    fn dates() {
        let valid = parse_date(Some("2024-12-25")).unwrap().unwrap();
        assert_eq!(valid.year(), 2024);
        assert_eq!(valid.month(), 12);
        assert_eq!(valid.day(), 25);

        assert!(parse_date(None).unwrap().is_none());
        parse_date(Some("invalid-date")).unwrap_err();
    }

    #[test]
    fn times() {
        let valid = parse_time(Some("14:30:45.123456")).unwrap().unwrap();
        assert_eq!(valid.hour(), 14);
        assert_eq!(valid.minute(), 30);
        assert_eq!(valid.second(), 45);

        assert!(parse_time(None).unwrap().is_none());
        parse_time(Some("25:00:00")).unwrap_err();
    }

    #[test]
    fn naive_timestamps() {
        let valid = parse_timestamp_naive(Some("2024-01-20 15:30:45.123")).unwrap().unwrap();
        assert_eq!(valid.year(), 2024);
        assert_eq!(valid.month(), 1);
        assert_eq!(valid.day(), 20);
        assert_eq!(valid.hour(), 15);

        assert!(parse_timestamp_naive(None).unwrap().is_none());
        parse_timestamp_naive(Some("2024/01/20 15:30:45")).unwrap_err();
    }

    #[test]
    fn into_param_valid() {
        // basic types
        into_param(&DataType::Int32(Some(42))).unwrap();
        into_param(&DataType::Int64(Some(i64::MAX))).unwrap();
        into_param(&DataType::Uint32(Some(u32::MAX))).unwrap();
        into_param(&DataType::Uint64(Some(100))).unwrap(); // within i64 range
        into_param(&DataType::Float(Some(std::f32::consts::PI))).unwrap();
        into_param(&DataType::Double(Some(std::f64::consts::E))).unwrap();
        into_param(&DataType::Str(Some("test".to_string()))).unwrap();
        into_param(&DataType::Boolean(Some(true))).unwrap();
        into_param(&DataType::Binary(Some(vec![0x01, 0x02]))).unwrap();

        // nulls
        into_param(&DataType::Int32(None)).unwrap();
        into_param(&DataType::Str(None)).unwrap();
    }

    #[test]
    fn into_param_invalid() {
        let uint64_overflow = DataType::Uint64(Some(u64::MAX));
        let result = into_param(&uint64_overflow);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("exceeds i64::MAX"));

        into_param(&DataType::Date(Some("invalid".to_string()))).unwrap_err();
        into_param(&DataType::Time(Some("25:00:00".to_string()))).unwrap_err();
        into_param(&DataType::Timestamp(Some("not a timestamp".to_string()))).unwrap_err();
    }

    #[test]
    fn into_param_timestamp() {
        let with_tz = DataType::Timestamp(Some("2024-01-20T15:30:45Z".to_string()));
        into_param(&with_tz).unwrap();

        let naive = DataType::Timestamp(Some("2024-01-20 15:30:45".to_string()));
        into_param(&naive).unwrap();
    }
}
