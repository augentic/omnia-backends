use std::io;
use std::str::FromStr;
use std::sync::Arc;

use anyhow::Context as _;
use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Utc};
use deadpool_postgres::{GenericClient, Object, PoolError};
use futures::future::FutureExt;
use omnia::FutureResult;
use omnia_wasi_sql::{Connection, Error, Field, Row, SqlClient, Statement, Value};
use tokio::sync::RwLock;
use tokio_postgres::error::SqlState;
use tokio_postgres::row::Row as PgRow;
use tokio_postgres::types::{Json, WrongType};

use crate::Client;
use crate::types::{Param, ParamRef, PgType};

impl SqlClient for Client {
    fn open(&self, name: String) -> FutureResult<Arc<dyn Connection>> {
        tracing::debug!("getting connection {name}");
        let pool = self.0.get(&name.to_ascii_uppercase()).cloned();

        async move {
            let Some(pool) = pool else {
                return Err(Error::NotFound(format!("database `{name}` is not declared")).into());
            };
            let session = pool.get().await.map_err(|err| checkout_failed(err, &name))?;
            Ok(Arc::new(PostgresConnection(Arc::new(RwLock::new(session)))) as Arc<dyn Connection>)
        }
        .boxed()
    }
}

// A pool that hands out no session is `unavailable`, unless the server
// refused the session it opened.
fn checkout_failed(err: PoolError, name: &str) -> anyhow::Error {
    let context = format!("failed to get a connection to database `{name}`");
    match err {
        PoolError::Backend(err) => classify(err, context),
        PoolError::Timeout(_) | PoolError::Closed => {
            Error::Unavailable(format!("{context}: {err}")).into()
        }
        err => anyhow::Error::new(err).context(context),
    }
}

// The kind a guest sees for a Postgres failure, by SQLSTATE class where the
// server sent one. A constraint violation, like anything unrecognised, stays
// `other`, as it does on `SqlDefault`.
fn classify(err: tokio_postgres::Error, context: String) -> anyhow::Error {
    let kind = err.code().map_or_else(|| client_kind(&err), sqlstate_kind);
    let err = anyhow::Error::new(err).context(context);
    match kind {
        Some(kind) => kind(format!("{err:#}")).into(),
        None => err,
    }
}

fn sqlstate_kind(state: &SqlState) -> Option<fn(String) -> Error> {
    let code = state.code();
    match code.get(..2)? {
        "28" => Some(Error::Denied),
        "42" if code == "42501" => Some(Error::Denied),
        "22" | "42" => Some(Error::Invalid),
        "0A" => Some(Error::Unsupported),
        "08" | "53" | "57" => Some(Error::Unavailable),
        _ => None,
    }
}

// Without a SQLSTATE, a value the column type refuses is `invalid`, as is a
// request that does not encode, such as SQL text holding a NUL. A connection
// that closed or failed in transit is `unavailable`.
fn client_kind(err: &tokio_postgres::Error) -> Option<fn(String) -> Error> {
    let source = std::error::Error::source(err);
    if source.is_some_and(|source| source.is::<WrongType>() || source.is::<serde_json::Error>()) {
        return Some(Error::Invalid);
    }
    match source.and_then(|source| source.downcast_ref::<io::Error>()) {
        Some(io) if io.kind() == io::ErrorKind::InvalidInput => Some(Error::Invalid),
        Some(_) => Some(Error::Unavailable),
        None if err.is_closed() => Some(Error::Unavailable),
        None => None,
    }
}

// `query` and `execute` share the session, and `batch` holds it alone, so no
// other statement lands inside the batch's transaction.
#[derive(Debug)]
struct PostgresConnection(Arc<RwLock<Object>>);

impl Connection for PostgresConnection {
    fn query(&self, sql: String, params: Vec<Value>) -> FutureResult<Vec<Row>> {
        tracing::debug!("query: {sql}, params: {params:?}");
        let session = Arc::clone(&self.0);

        async move {
            let params = into_params(&params, "the query")?;
            let rows = {
                let session = session.read_owned().await;
                let statement = prepare(&*session, &sql, &params, "the query").await?;
                session
                    .query(&statement, &refs(&params))
                    .await
                    .map_err(|err| classify(err, "failed to run the query".to_owned()))?
            };
            tracing::debug!("query returned {} rows", rows.len());
            rows.iter().map(into_wasi_row).collect()
        }
        .boxed()
    }

    fn execute(&self, sql: String, params: Vec<Value>) -> FutureResult<u64> {
        tracing::debug!("execute: {sql}, params: {params:?}");
        let session = Arc::clone(&self.0);

        async move {
            let params = into_params(&params, "the statement")?;
            let session = session.read_owned().await;
            execute(&*session, &sql, &params, "the statement").await
        }
        .boxed()
    }

    fn batch(&self, statements: Vec<Statement>) -> FutureResult<Vec<u64>> {
        tracing::debug!("batch of {} statements", statements.len());
        let session = Arc::clone(&self.0);

        async move {
            let bound = statements
                .iter()
                .enumerate()
                .map(|(index, statement)| {
                    let subject = format!("statement {} of the batch", index + 1);
                    let params = into_params(&statement.params, &subject)?;
                    Ok((statement.sql.as_str(), params, subject))
                })
                .collect::<anyhow::Result<Vec<_>>>()?;
            transact(&mut *session.write_owned().await, &bound).await
        }
        .boxed()
    }
}

// A value that does not convert is the caller's to fix, and it fails before
// anything reaches the server.
fn into_params(params: &[Value], subject: &str) -> anyhow::Result<Vec<Param>> {
    params
        .iter()
        .enumerate()
        .map(|(index, value)| {
            into_param(value).map_err(|err| {
                let position = index + 1;
                let message = format!("failed to bind parameter ${position} of {subject}: {err:#}");
                Error::Invalid(message).into()
            })
        })
        .collect()
}

fn into_param(value: &Value) -> anyhow::Result<Param> {
    let pg_value = match value {
        Value::Int32(v) => PgType::Int32(*v),
        Value::Int64(v) => PgType::Int64(*v),
        Value::Uint32(v) => PgType::Uint32(*v),
        Value::Uint64(v) => {
            let converted = v
                .map(|raw| {
                    i64::try_from(raw).with_context(|| {
                        format!("uint64 value {raw} exceeds i64::MAX and cannot be stored")
                    })
                })
                .transpose()?;
            PgType::Int64(converted)
        }
        Value::Float(v) => PgType::Float(*v),
        Value::Double(v) => PgType::Double(*v),
        Value::Str(v) => PgType::Text(v.clone()),
        Value::Boolean(v) => PgType::Bool(*v),
        Value::Date(v) => PgType::Date(parse_date(v.as_deref())?),
        Value::Time(v) => PgType::Time(parse_time(v.as_deref())?),
        Value::Timestamp(v) => parse_timestamp(v.as_deref())?,
        Value::Binary(v) => PgType::Binary(v.clone()),
    };

    Ok(Box::new(pg_value) as Param)
}

fn parse_date(source: Option<&str>) -> anyhow::Result<Option<NaiveDate>> {
    source.map(|s| NaiveDate::from_str(s).context("invalid date format")).transpose()
}

fn parse_time(source: Option<&str>) -> anyhow::Result<Option<NaiveTime>> {
    source.map(|s| NaiveTime::from_str(s).context("invalid time format")).transpose()
}

// RFC 3339, with its offset, binds as `timestamptz`, and the naive form as
// `timestamp`.
fn parse_timestamp(source: Option<&str>) -> anyhow::Result<PgType> {
    let Some(s) = source else {
        return Ok(PgType::Timestamp(None));
    };
    if let Ok(ts) = DateTime::parse_from_rfc3339(s) {
        return Ok(PgType::TimestampTz(Some(ts.with_timezone(&Utc))));
    }
    let naive = NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.f")
        .context("invalid timestamp format")?;
    Ok(PgType::Timestamp(Some(naive)))
}

// tokio-postgres reports a parameter count mismatch without a kind, so the
// count is checked against the prepared statement.
async fn prepare(
    client: &impl GenericClient, sql: &str, params: &[Param], subject: &str,
) -> anyhow::Result<tokio_postgres::Statement> {
    let statement = client
        .prepare(sql)
        .await
        .map_err(|err| classify(err, format!("failed to prepare {subject}")))?;
    let expected = statement.params().len();
    if expected != params.len() {
        let given = params.len();
        let message =
            format!("failed to bind {subject}: it takes {expected} parameters, not {given}");
        return Err(Error::Invalid(message).into());
    }
    Ok(statement)
}

fn refs(params: &[Param]) -> Vec<ParamRef<'_>> {
    params.iter().map(|param| param.as_ref() as ParamRef).collect()
}

fn into_wasi_row(pg_row: &PgRow) -> anyhow::Result<Row> {
    let mut fields = Vec::with_capacity(pg_row.len());
    for (i, column) in pg_row.columns().iter().enumerate() {
        let value = match column.type_().name() {
            "int4" => Value::Int32(pg_row.try_get(i)?),
            "int8" => Value::Int64(pg_row.try_get(i)?),
            "oid" => Value::Uint32(pg_row.try_get(i)?),
            "float4" => Value::Float(pg_row.try_get(i)?),
            "float8" => Value::Double(pg_row.try_get(i)?),
            "text" | "varchar" | "name" | "_char" => Value::Str(pg_row.try_get(i)?),
            "bool" => Value::Boolean(pg_row.try_get(i)?),
            "date" => {
                let v: Option<NaiveDate> = pg_row.try_get(i)?;
                Value::Date(v.map(|date| date.to_string()))
            }
            "time" => {
                let v: Option<NaiveTime> = pg_row.try_get(i)?;
                Value::Time(v.map(|time| time.to_string()))
            }
            "timestamp" => {
                let v: Option<NaiveDateTime> = pg_row.try_get(i)?;
                Value::Timestamp(v.map(|dt| dt.to_string()))
            }
            "timestamptz" => {
                let v: Option<DateTime<Utc>> = pg_row.try_get(i)?;
                Value::Timestamp(v.map(|dtz| dtz.to_rfc3339()))
            }
            "json" | "jsonb" => {
                let v: Option<Json<serde_json::Value>> = pg_row.try_get(i)?;
                Value::Str(v.map(|json| json.0.to_string()))
            }
            "bytea" => Value::Binary(pg_row.try_get(i)?),
            other => {
                let name = column.name();
                let message = format!("column `{name}` has type `{other}`, which is not mapped");
                return Err(Error::Unsupported(message).into());
            }
        };
        fields.push(Field {
            name: column.name().to_owned(),
            value,
        });
    }

    Ok(Row { fields })
}

async fn execute(
    client: &impl GenericClient, sql: &str, params: &[Param], subject: &str,
) -> anyhow::Result<u64> {
    let statement = prepare(client, sql, params, subject).await?;
    client
        .execute(&statement, &refs(params))
        .await
        .map_err(|err| classify(err, format!("failed to run {subject}")))
}

// A transaction dropped before it commits rolls back, so a batch abandoned
// part-way leaves nothing behind and the session returns to the pool clean.
async fn transact(
    session: &mut Object, bound: &[(&str, Vec<Param>, String)],
) -> anyhow::Result<Vec<u64>> {
    let tx = session
        .transaction()
        .await
        .map_err(|err| classify(err, "failed to begin the batch".to_owned()))?;
    let mut counts = Vec::with_capacity(bound.len());
    for (sql, params, subject) in bound {
        counts.push(execute(&tx, sql, params, subject).await?);
    }
    tx.commit().await.map_err(|err| classify(err, "failed to commit the batch".to_owned()))?;
    Ok(counts)
}
