//! The `omnia:docstore` contract over the Table service REST API, with the
//! codecs it is built from.

pub mod document;
pub mod filter;
pub mod query;

use anyhow::{Context, anyhow, bail};
use base64ct::{Base64, Encoding};
use futures::future::FutureExt;
use hmac::{Hmac, KeyInit, Mac};
use omnia_wasi_docstore::{
    DocStoreClient, Document, Error, FilterTree, FutureResult, QueryOpts, QueryResult,
};
use reqwest::{Method, Response, StatusCode};
use serde_json::Value;
use sha2::Sha256;

use crate::Client;

const API_VERSION: &str = "2026-02-06";
const ACCEPT_HEADER: &str = "application/json;odata=fullmetadata";

impl Client {
    /// Create the named table when absent; `true` when this call created it.
    ///
    /// A write creates its table when the table does not exist yet, so this
    /// is needed only to create a table ahead of any write.
    ///
    /// # Errors
    ///
    /// - [`Error::Unavailable`] when the table is being deleted, or the
    ///   service does not answer.
    /// - [`Error::Invalid`] when the service refuses the name.
    pub async fn ensure_table(&self, table: &str) -> anyhow::Result<bool> {
        let uri = format!("{}/Tables", self.base_url);

        let response = self
            .signed_request(Method::POST, &uri)?
            .json(&serde_json::json!({"TableName": table}))
            .send()
            .await
            .map_err(|err| transport_failed(err, "create table"))?;

        if response.status().is_success() {
            return Ok(true);
        }
        let refusal = Refusal::read(response).await;
        if refusal.status == StatusCode::CONFLICT && !refusal.has_code(TABLE_BEING_DELETED) {
            return Ok(false);
        }
        Err(refusal.into_error("create table"))
    }

    // A collection is created by its first write, so a write whose table
    // does not exist yet creates the table and is sent once more.
    async fn write(
        &self, table: &str, method: Method, uri: &str, body: &Value,
    ) -> anyhow::Result<Response> {
        let response = self
            .signed_request(method.clone(), uri)?
            .json(body)
            .send()
            .await
            .map_err(|err| transport_failed(err, "write"))?;
        if response.status() != StatusCode::NOT_FOUND {
            return Ok(response);
        }

        self.ensure_table(table).await?;
        self.signed_request(method, uri)?
            .json(body)
            .send()
            .await
            .map_err(|err| transport_failed(err, "write"))
    }

    fn entity_uri(&self, table: &str, pk: &str, rk: &str) -> String {
        format!(
            "{}/{table}(PartitionKey='{epk}',RowKey='{erk}')",
            self.base_url,
            epk = escape_entity_key(pk),
            erk = escape_entity_key(rk),
        )
    }

    fn signed_request(&self, method: Method, uri: &str) -> anyhow::Result<reqwest::RequestBuilder> {
        let now = now_rfc1123();
        let auth = sign_request(&self.options.name, &self.hmac_key, &now, uri)?;
        let headers = azure_headers(&now, &auth)?;
        Ok(self.http.request(method, uri).headers(headers))
    }

    // One page of a query: the response body and the continuation token
    // for the next page, if any. A table that does not exist yet answers
    // with an empty last page.
    async fn fetch_page(
        &self, table: &str, odata_filter: Option<&str>, fetch_limit: Option<usize>,
        continuation: Option<&str>,
    ) -> anyhow::Result<(Value, Option<String>)> {
        let base_uri = format!("{}/{table}()", self.base_url);

        let mut query_params: Vec<String> = Vec::new();
        if let Some(f) = odata_filter {
            query_params.push(format!("$filter={}", urlencoding::encode(f)));
        }
        if let Some(limit) = fetch_limit {
            query_params.push(format!("$top={limit}"));
        }
        if let Some(cont) = continuation {
            let (next_partition, next_row) = query::decode_continuation(cont);
            query_params.push(format!("NextPartitionKey={}", urlencoding::encode(&next_partition)));
            if let Some(row) = next_row {
                query_params.push(format!("NextRowKey={}", urlencoding::encode(&row)));
            }
        }

        let uri = if query_params.is_empty() {
            base_uri
        } else {
            format!("{base_uri}?{}", query_params.join("&"))
        };

        let response = self
            .signed_request(Method::GET, &uri)?
            .send()
            .await
            .map_err(|err| transport_failed(err, "query"))?;

        if response.status() == StatusCode::NOT_FOUND {
            return Ok((Value::Null, None));
        }
        if !response.status().is_success() {
            return Err(Refusal::read(response).await.into_error("query"));
        }

        let continuation_partition = response
            .headers()
            .get("x-ms-continuation-NextPartitionKey")
            .and_then(|v| v.to_str().ok())
            .map(str::to_owned);
        let continuation_row = response
            .headers()
            .get("x-ms-continuation-NextRowKey")
            .and_then(|v| v.to_str().ok())
            .map(str::to_owned);

        let body: Value = response.json().await.map_err(|err| transport_failed(err, "query"))?;

        let token = query::encode_continuation(
            continuation_partition.as_deref(),
            continuation_row.as_deref(),
        );
        Ok((body, token))
    }
}

impl DocStoreClient for Client {
    fn get(&self, collection: String, id: String) -> FutureResult<Option<Document>> {
        let client = self.clone();
        async move {
            let (table, _scope_pk) = parse_collection(&collection)?;
            let (pk, rk) = document::decode_id(&id)?;
            let uri = client.entity_uri(&table, pk, rk);

            let response = client
                .signed_request(Method::GET, &uri)?
                .send()
                .await
                .map_err(|err| transport_failed(err, "get"))?;

            if response.status() == StatusCode::NOT_FOUND {
                return Ok(None);
            }
            if !response.status().is_success() {
                return Err(Refusal::read(response).await.into_error("get"));
            }

            let entity: Value =
                response.json().await.map_err(|err| transport_failed(err, "get"))?;

            Ok(Some(document::unflatten(&entity)?))
        }
        .boxed()
    }

    fn insert(&self, collection: String, doc: Document) -> FutureResult<bool> {
        let client = self.clone();
        async move {
            let (table, _scope_pk) = parse_collection(&collection)?;
            let uri = format!("{}/{table}", client.base_url);
            let body = document::flatten(&doc)?;

            let response = client.write(&table, Method::POST, &uri, &body).await?;
            if response.status().is_success() {
                return Ok(true);
            }
            let refusal = Refusal::read(response).await;
            if refusal.status == StatusCode::CONFLICT && refusal.has_code(ENTITY_ALREADY_EXISTS) {
                return Ok(false);
            }
            Err(refusal.into_error("insert"))
        }
        .boxed()
    }

    fn put(&self, collection: String, doc: Document) -> FutureResult<()> {
        let client = self.clone();
        async move {
            let (table, _scope_pk) = parse_collection(&collection)?;
            let (pk, rk) = document::decode_id(&doc.id)?;
            let uri = client.entity_uri(&table, pk, rk);
            let body = document::flatten(&doc)?;

            let response = client.write(&table, Method::PUT, &uri, &body).await?;
            if !response.status().is_success() {
                return Err(Refusal::read(response).await.into_error("put"));
            }
            Ok(())
        }
        .boxed()
    }

    fn delete(&self, collection: String, id: String) -> FutureResult<bool> {
        let client = self.clone();
        async move {
            let (table, _scope_pk) = parse_collection(&collection)?;
            let (pk, rk) = document::decode_id(&id)?;
            let uri = client.entity_uri(&table, pk, rk);

            let response = client
                .signed_request(Method::DELETE, &uri)?
                .header("If-Match", "*")
                .send()
                .await
                .map_err(|err| transport_failed(err, "delete"))?;

            if response.status() == StatusCode::NOT_FOUND {
                return Ok(false);
            }
            if !response.status().is_success() {
                return Err(Refusal::read(response).await.into_error("delete"));
            }
            Ok(true)
        }
        .boxed()
    }

    fn query(
        &self, collection: String, filter: Option<FilterTree>, options: QueryOpts,
    ) -> FutureResult<QueryResult> {
        let client = self.clone();
        async move {
            check_options(&options)?;
            let fetch_limit = options.limit.map(|l| l as usize);

            let (table, pk) = parse_collection(&collection)?;
            let user_filter = filter.as_ref().map(filter::to_odata).transpose()?;
            let odata_filter = build_odata_filter(pk.as_deref(), user_filter.as_deref());

            let mut all_documents: Vec<Document> = Vec::new();
            let mut next_continuation = options.continuation.clone();

            loop {
                let remaining = fetch_limit.map(|lim| lim - all_documents.len());

                let (body, continuation) = client
                    .fetch_page(
                        &table,
                        odata_filter.as_deref(),
                        remaining,
                        next_continuation.as_deref(),
                    )
                    .await?;

                if let Some(entries) = body.get("value").and_then(Value::as_array) {
                    for entity in entries {
                        all_documents.push(document::unflatten(entity)?);
                    }
                }

                let has_more_pages = continuation.is_some();
                next_continuation = continuation;

                let reached_limit = fetch_limit.is_some_and(|lim| all_documents.len() >= lim);

                if !has_more_pages || reached_limit {
                    break;
                }
            }

            Ok(QueryResult {
                documents: all_documents,
                continuation: next_continuation,
            })
        }
        .boxed()
    }
}

// --- Failures ---

const ENTITY_ALREADY_EXISTS: &str = "EntityAlreadyExists";
const TABLE_BEING_DELETED: &str = "TableBeingDeleted";

// A response the service refused, read once so its `odata.error.code` can
// both pick an outcome and name the failure.
struct Refusal {
    status: StatusCode,
    code: Option<String>,
    body: String,
}

impl Refusal {
    async fn read(response: Response) -> Self {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        let code = serde_json::from_str::<Value>(&body)
            .ok()
            .and_then(|error| Some(error.pointer("/odata.error/code")?.as_str()?.to_owned()));
        Self { status, code, body }
    }

    fn has_code(&self, code: &str) -> bool {
        self.code.as_deref() == Some(code)
    }

    // A table being deleted is transient whatever the status. Otherwise the
    // status decides, and an unrecognised one stays `other`.
    fn into_error(self, action: &str) -> anyhow::Error {
        let message = format!("Azure Table {action} failed ({}): {}", self.status, self.body);
        let kind: Option<fn(String) -> Error> = match self.status.as_u16() {
            _ if self.has_code(TABLE_BEING_DELETED) => Some(Error::Unavailable),
            400 | 413 => Some(Error::Invalid),
            401 | 403 => Some(Error::Denied),
            408 | 429 | 500..=599 => Some(Error::Unavailable),
            _ => None,
        };
        match kind {
            Some(kind) => kind(message).into(),
            None => anyhow!(message),
        }
    }
}

// A request that got no answer is `unavailable` when it could not connect or
// ran out of time.
fn transport_failed(err: reqwest::Error, action: &str) -> anyhow::Error {
    let unavailable = err.is_connect() || err.is_timeout();
    let err = anyhow::Error::new(err).context(format!("Azure Table {action} request failed"));
    if unavailable { Error::Unavailable(format!("{err:#}")).into() } else { err }
}

// --- Helpers ---

// A key lands inside an `OData` predicate (`PartitionKey='...'`): quotes
// are doubled per `OData`, then the whole percent-encoded for the URL.
fn escape_entity_key(value: &str) -> String {
    urlencoding::encode(&value.replace('\'', "''")).into_owned()
}

fn now_rfc1123() -> String {
    chrono::Utc::now().format("%a, %d %b %Y %H:%M:%S GMT").to_string()
}

// The `SharedKeyLite` scheme signs `/{account}{raw path}`. An Azurite URL
// already carries the account in its path (`/devstoreaccount1/Tables`), so
// the resource doubles it — deliberately: Azurite signs the same way.
// <https://learn.microsoft.com/en-us/rest/api/storageservices/authorize-with-shared-key>
fn sign_request(
    account_name: &str, hmac_key: &[u8], date_time: &str, uri: &str,
) -> anyhow::Result<String> {
    let uri_path = uri
        .split("://")
        .nth(1)
        .and_then(|after_scheme| after_scheme.find('/').map(|i| &after_scheme[i..]))
        .unwrap_or("/");
    let uri_path = uri_path.split('?').next().unwrap_or(uri_path);
    let resource = format!("/{account_name}{uri_path}");
    let string_to_sign = format!("{date_time}\n{resource}");
    let mut hmac = Hmac::<Sha256>::new_from_slice(hmac_key)
        .map_err(|e| anyhow!("HMAC initialization error: {e}"))?;
    hmac.update(string_to_sign.as_bytes());
    let signature = hmac.finalize().into_bytes();
    let encoded = Base64::encode_string(&signature);
    Ok(format!("SharedKeyLite {account_name}:{encoded}"))
}

fn azure_headers(date: &str, auth: &str) -> anyhow::Result<reqwest::header::HeaderMap> {
    let mut headers = reqwest::header::HeaderMap::new();
    headers.insert("x-ms-date", date.parse().context("invalid x-ms-date header value")?);
    headers.insert("x-ms-version", reqwest::header::HeaderValue::from_static(API_VERSION));
    headers.insert("Authorization", auth.parse().context("invalid Authorization header value")?);
    headers.insert("Accept", reqwest::header::HeaderValue::from_static(ACCEPT_HEADER));
    Ok(headers)
}

// A zero limit is `invalid`. The service returns entities in key order and
// has no `$skip`, so `order_by` and an offset are `unsupported`, never
// ignored or served client-side.
fn check_options(options: &QueryOpts) -> anyhow::Result<()> {
    if options.limit == Some(0) {
        bail!(Error::Invalid("query limit must be at least 1".to_owned()));
    }
    if !options.order_by.is_empty() {
        bail!(Error::Unsupported(
            "order_by is not supported by Azure Table — \
             entities come back in PartitionKey and RowKey order"
                .to_owned()
        ));
    }
    if options.offset.is_some_and(|offset| offset > 0) {
        bail!(Error::Unsupported(
            "offset is not supported by Azure Table — \
             use continuation tokens for pagination instead"
                .to_owned()
        ));
    }
    Ok(())
}

fn parse_collection(collection: &str) -> anyhow::Result<(String, Option<String>)> {
    match collection.split_once('/') {
        Some((table, pk)) if !table.is_empty() && !pk.is_empty() => {
            validate_table_name(table)?;
            Ok((table.to_owned(), Some(pk.to_owned())))
        }
        Some((table, _)) if !table.is_empty() => {
            bail!(Error::Invalid(format!(
                "collection '{collection}' has an empty partition key after '/'"
            )))
        }
        Some(_) => {
            bail!(Error::Invalid(format!("collection '{collection}' has an empty table name")))
        }
        None if !collection.is_empty() => {
            validate_table_name(collection)?;
            Ok((collection.to_owned(), None))
        }
        _ => bail!(Error::Invalid("collection must not be empty".to_owned())),
    }
}

fn validate_table_name(table: &str) -> anyhow::Result<()> {
    let len = table.len();
    let problem = if !(3..=63).contains(&len) {
        "length must be between 3 and 63 characters"
    } else if !table.starts_with(|c: char| c.is_ascii_alphabetic()) {
        "first character must be a letter"
    } else if !table.chars().all(|c| c.is_ascii_alphanumeric()) {
        "only ASCII letters and digits are allowed"
    } else if table.eq_ignore_ascii_case("tables") {
        "reserved table name"
    } else {
        return Ok(());
    };
    bail!(Error::Invalid(format!("invalid table name '{table}': {problem}")))
}

fn build_odata_filter(pk: Option<&str>, server_filter: Option<&str>) -> Option<String> {
    let mut parts: Vec<String> = Vec::new();
    if let Some(pk) = pk {
        parts.push(format!("PartitionKey eq '{}'", pk.replace('\'', "''")));
    }
    if let Some(sf) = server_filter {
        parts.push(sf.to_owned());
    }
    if parts.is_empty() { None } else { Some(parts.join(" and ")) }
}
