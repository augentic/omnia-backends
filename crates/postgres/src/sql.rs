use std::convert::TryFrom;
use std::str::FromStr;
use std::sync::Arc;

use anyhow::{Context, anyhow, bail};
use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Utc};
use deadpool_postgres::Object;
use futures::future::FutureExt;
use omnia::FutureResult;
use omnia_wasi_sql::{Connection, DataType, Field, Row, SqlClient};
use tokio_postgres::row::Row as PgRow;

use crate::Client;
use crate::types::{Param, ParamRef, PgType};

impl SqlClient for Client {
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
struct PostgresConnection(Arc<Object>);

impl Connection for PostgresConnection {
    fn query(&self, query: String, params: Vec<DataType>) -> FutureResult<Vec<Row>> {
        tracing::debug!("query: {query}, params: {params:?}");
        let cnn = Arc::clone(&self.0);

        async move {
            let pg_params = into_params(&params)?;
            let param_refs: Vec<ParamRef> =
                pg_params.iter().map(|b| b.as_ref() as ParamRef).collect();

            let pg_rows = cnn.query(&query, &param_refs).await.context("query failed")?;
            tracing::debug!("query returned {} rows", pg_rows.len());

            let mut wasi_rows = Vec::with_capacity(pg_rows.len());
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
            let pg_params = into_params(&params)?;
            let param_refs: Vec<ParamRef> =
                pg_params.iter().map(|b| b.as_ref() as ParamRef).collect();

            let affected = match cnn.execute(&query, &param_refs).await {
                Ok(count) => count,
                Err(e) => {
                    tracing::error!("exec failed: {e}");
                    return Err(anyhow!("exec failed: {e}"));
                }
            };
            u32::try_from(affected).context("affected row count exceeds u32::MAX")
        }
        .boxed()
    }
}

fn into_params(params: &[DataType]) -> anyhow::Result<Vec<Param>> {
    params.iter().map(into_param).collect()
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
