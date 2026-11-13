# omnia-azure-table

[![crates.io](https://img.shields.io/crates/v/omnia-azure-table.svg)](https://crates.io/crates/omnia-azure-table)
[![docs.rs](https://docs.rs/omnia-azure-table/badge.svg)](https://docs.rs/omnia-azure-table)

Azure Table Storage backend for the Omnia WASI runtime, serving `omnia:docstore` through `omnia-wasi-docstore`'s `DocStoreClient`.

Azure Table Storage is a `NoSQL` key-value store. This crate maps the document store model onto Azure Table entities: top-level JSON fields are flattened into entity properties so that server-side `OData` `$filter` queries work, while nested objects are serialized as JSON string properties.

MSRV: Rust 1.99

## Key Mapping

The document `id` names one entity. A plain id is the entity's `PartitionKey`
with an empty `RowKey`, so each such document is a partition of its own. A
composite `{PartitionKey}\0{RowKey}` id (null-byte separated) places several
documents in one partition. The `collection` string carries the table name and
an optional `/{PartitionKey}` suffix for query scoping.

| document store concept | Azure Table equivalent |
|----------------|------------------------|
| `collection` | Table name (+ optional `/{PartitionKey}` for query scoping) |
| plain `id` | `PartitionKey` = `id`, empty `RowKey` |
| composite `id` | `{PartitionKey}\0{RowKey}` |
| `document.data` | Flattened entity properties |

Examples:

- `get("users", "user-123")` → table=`users`, PK=`user-123`, RK=`""`.
- `get("users", "tenant-a\0user-123")` → table=`users`, PK=`tenant-a`, RK=`user-123`.

`pk` and `pk\0` name the same entity, and an entity with an empty `RowKey`
reads back with the plain id. An id holding a character Azure forbids in a key
(`/`, `\`, `#`, `?`, or a control character other than the separator) fails
with `invalid`.

A table-only collection (`"users"` without a `/`) is valid for all operations.
For `query()`, appending `/{PartitionKey}` scopes the scan to a single
partition. The partition key for point operations is always derived from the
`id`, never from the collection string.

A table is created on the first write to it, so a deployment needs no setup
step. Until then the table reads as empty: `get` returns nothing, `delete`
returns `false`, and `query` returns an empty page.

## Composite ID Format

Azure Table entities are keyed by `(PartitionKey, RowKey)`. A `RowKey` is only
unique within its partition — two different partitions can share the same
`RowKey`. To ensure that `Document.id` is globally unique and self-sufficient
for CRUD round-trips, this crate encodes both keys into a single string:

```text
{PartitionKey}\0{RowKey}
```

The null byte (`\0`, U+0000) is used as the separator because it is
[forbidden in Azure Table key fields](https://learn.microsoft.com/en-us/rest/api/storageservices/understanding-the-table-service-data-model#characters-disallowed-in-key-fields)
(control characters U+0000–U+001F are disallowed), making the split
unambiguous. The same separator is already used for continuation tokens.

Construct and parse ids with `omnia_azure_table::{encode_id, decode_id}`. An
empty `RowKey` gives the plain id:

```rust,ignore
use omnia_azure_table::{encode_id, decode_id};

let id = encode_id("tenant-a", "user-123");   // "tenant-a\0user-123"
let (pk, rk) = decode_id(&id).unwrap();       // ("tenant-a", "user-123")

let plain = encode_id("user-123", "");        // "user-123"
let (pk, rk) = decode_id(&plain).unwrap();    // ("user-123", "")
```

## `OData` Type Mapping

Top-level JSON fields are flattened into typed entity properties. The crate
automatically adds `@odata.type` annotations where Azure Table cannot infer
the type from the JSON representation alone.

| JSON value | Azure `OData` type | Annotation added? |
|------------|-------------------|-------------------|
| `true` / `false` | `Edm.Boolean` | No (inferred) |
| integer ≤ `i32` range | `Edm.Int32` | No (inferred) |
| integer > `i32` range | `Edm.Int64` | Yes |
| floating point | `Edm.Double` | Yes |
| string | `Edm.String` | No (inferred) |
| `null` | (skipped) | N/A |
| array / object | `Edm.String` (JSON-serialized) | No |

`Edm.DateTime`, `Edm.Guid`, and `Edm.Binary` require the document to include
explicit `@odata.type` annotations — the crate does not attempt to guess these
from string values.

## Supported Operations

| Operation | Description |
|-----------|-------------|
| `get` | Point read by `PartitionKey` + `RowKey` |
| `insert` | Insert a new entity; an id already stored is `already-exists`, and the stored entity is left untouched |
| `put` | Upsert entity |
| `delete` | Delete entity (returns whether it existed) |
| `query` | Filtered listing with `OData` `$filter` and pagination |
| `ensure_table` | Creates a table if it does not exist (admin helper) |

## Filter Support

| Filter or option | Supported | Notes |
|--------|-----------|-------|
| `Compare` (eq/ne/gt/gte/lt/lte) | Yes | Translated to `OData` operators |
| `Compare` with `null` | **No** | `unsupported`: a null property is omitted from the entity |
| `InList` / `NotInList` | Yes | Expanded to OR chains |
| `And` / `Or` / `Not` | Yes | Supported when all children are supported |
| `Contains` / `StartsWith` / `EndsWith` | **No** | `unsupported` |
| `IsNull` / `IsNotNull` | **No** | `unsupported` |
| Dotted field path (`meta.level`) | **No** | `unsupported`: nested values are stored as JSON strings |
| `order_by` | **No** | `unsupported`: entities come back in `PartitionKey` / `RowKey` order |
| `offset` | **No** | `unsupported`: use continuation tokens |
| `limit` | Yes | Sent as `$top`; zero is `invalid` |
| `continuation` | Yes | Native Azure Table continuation tokens |

What Azure Table cannot serve fails with `unsupported` rather than being
ignored or evaluated client-side, which could pull unbounded data from the
table service. Azure Table's `OData` `$filter` does not support string
functions or null checks, and there is no `$orderby` or `$skip`; see
[Querying tables and entities](https://learn.microsoft.com/en-us/rest/api/storageservices/querying-tables-and-entities#supported-query-options).

## Errors

A failure reaches the guest as one of the `omnia:docstore` error kinds.

| Kind | Failure |
|------|---------|
| `invalid` | A malformed collection, table name, or id; a body that is not a JSON object; a zero `limit`; or a request the service refuses with `400` or `413` |
| `denied` | A request the service refuses with `401` or `403` |
| `unsupported` | A query feature in the table above, or an integer beyond the `Edm.Int64` range |
| `unavailable` | A table still being deleted, a `408`, `429`, or `5xx`, or a connection that fails or times out |
| `other` | Anything else |

## Why not `azure_data_tables`?

The `azure_data_tables` crate is on a [legacy branch](https://github.com/Azure/azure-sdk-for-rust/tree/legacy) with no official replacement planned. This crate calls the REST API directly and leaves business object mapping to the WebAssembly guest.

## Configuration

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `AZURE_STORAGE_ACCOUNT` | yes | | Storage account name |
| `AZURE_STORAGE_KEY` | yes | | Storage account access key |
| `AZURE_TABLE_ENDPOINT` | no | `https://{account}.table.core.windows.net` | Table service endpoint URL |

Set `AZURE_TABLE_ENDPOINT` to override the default public-cloud URL. Common
values:

- **Azurite**: `http://127.0.0.1:10002/{account}`
- **Azure sovereign cloud**: the appropriate `table.core.*` URL for your region

## Usage

Bind the backend in your host's `runtime!` map — the guest `.wasm` is untouched
(see the [Production Backends guide](https://github.com/augentic/omnia/blob/main/docs/guides/production-backends.md)):

```rust,ignore
use omnia_azure_table::Client as AzureTable;
use omnia_wasi_docstore::WasiDocStore;

omnia::runtime!({
    hosts: {
        WasiDocStore: AzureTable,
    }
});
```

For direct or embedded use, connect it yourself:

```rust,ignore
use omnia::{Connect, FromEnv};
use omnia_azure_table::Client;

let options = omnia_azure_table::ConnectOptions::load_env()?;
let client = Client::connect_with(options).await?;

// Create the table ahead of the first write (admin helper, not part of omnia:docstore)
client.ensure_table("my_table").await?;
```

## Live tests

[`tests/live.rs`](tests/live.rs) runs against a real storage account or Azurite.

- `docstore_document_db` runs the `omnia_sdk::contract::document` rows from a
  real guest.
- The other tests cover what no guest row reaches: composite ids, `Edm` type
  fidelity, server-side filters with continuation paging, insert outcomes,
  and the shapes refused as `unsupported` or `invalid`.

`docstore_document_db` empties the rows' tables (`insertexists`,
`portablequery`, `optionalquery`) one entity at a time before it runs, so
point the suite at an account of your own. The tests are `#[ignore]`d so they
never run in CI; run them explicitly:

```bash
docker run -d --name azurite -p 10002:10002 mcr.microsoft.com/azure-storage/azurite \
  azurite-table --tableHost 0.0.0.0 --skipApiVersionCheck

AZURE_STORAGE_ACCOUNT=devstoreaccount1 \
AZURE_STORAGE_KEY='Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==' \
AZURE_TABLE_ENDPOINT=http://127.0.0.1:10002/devstoreaccount1 \
  cargo nextest run -p omnia-azure-table --all-features --run-ignored all
```

Against a cloud account, set `AZURE_STORAGE_ACCOUNT` and `AZURE_STORAGE_KEY`
to its own values and leave `AZURE_TABLE_ENDPOINT` unset.

## License

MIT OR Apache-2.0
