# omnia-azure-table

[![crates.io](https://img.shields.io/crates/v/omnia-azure-table.svg)](https://crates.io/crates/omnia-azure-table)
[![docs.rs](https://docs.rs/omnia-azure-table/badge.svg)](https://docs.rs/omnia-azure-table)

Azure Table Storage backend for the Omnia WASI runtime, implementing the `wasi-docstore` interface.

Azure Table Storage is a `NoSQL` key-value store. This crate maps the document store model onto Azure Table entities: top-level JSON fields are flattened into entity properties so that server-side `OData` `$filter` queries work, while nested objects are serialized as JSON string properties.

MSRV: Rust 1.95

## Key Mapping

The document `id` is a composite `{PartitionKey}\0{RowKey}` string (null-byte
separated) that uniquely identifies an entity across all partitions. The
`collection` string carries the table name and an optional `/{PartitionKey}`
suffix for query scoping.

| document store concept | Azure Table equivalent |
|----------------|------------------------|
| `collection` | Table name (+ optional `/{PartitionKey}` for query scoping) |
| `id` | `{PartitionKey}\0{RowKey}` (composite) |
| `document.data` | Flattened entity properties |

Example: `get("users", "tenant-a\0user-123")` → table=`users`, PK=`tenant-a`, RK=`user-123`.

A table-only collection (`"users"` without a `/`) is valid for all operations.
For `query()`, appending `/{PartitionKey}` scopes the scan to a single
partition. The partition key for point operations is always derived from the
composite `id`, never from the collection string.

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

Construct and parse composite IDs with the helpers in
`omnia_azure_table::store::document`:

```rust,ignore
use omnia_azure_table::store::document::{encode_id, decode_id};

let id = encode_id("tenant-a", "user-123");   // "tenant-a\0user-123"
let (pk, rk) = decode_id(&id).unwrap();       // ("tenant-a", "user-123")
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
| `insert` | Insert new entity (fails if exists) |
| `put` | Upsert entity |
| `delete` | Delete entity (returns whether it existed) |
| `query` | Filtered listing with `OData` `$filter` and pagination |
| `ensure_table` | Creates a table if it does not exist (admin helper) |

## Filter Support

| Filter | Supported | Notes |
|--------|-----------|-------|
| `Compare` (eq/ne/gt/gte/lt/lte) | Yes | Translated to `OData` operators |
| `InList` / `NotInList` | Yes | Expanded to OR chains |
| `And` / `Or` / `Not` | Yes | Supported when all children are supported |
| `Contains` / `StartsWith` / `EndsWith` | **No** | Rejected with error |
| `IsNull` / `IsNotNull` | **No** | Rejected with error |
| `offset` | **No** | Rejected with error — use continuation tokens |
| `continuation` | Yes | Native Azure Table continuation tokens |

Unsupported filters and query options return an error rather than silently
falling back to client-side evaluation, which could pull unbounded data from
the table service. Azure Table's `OData` `$filter` does not support string
functions or null checks, and there is no `$skip` — see
[Querying tables and entities](https://learn.microsoft.com/en-us/rest/api/storageservices/querying-tables-and-entities#supported-query-options).

> **Note:** `order_by` is ignored. Azure Table returns results in
> `PartitionKey` / `RowKey` order; there is no server-side `$orderby`.
> Callers that need a different sort order should sort after retrieval.

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

```rust,ignore
use omnia::{Backend, FromEnv};
use omnia_azure_table::Client;

let options = omnia_azure_table::ConnectOptions::load_env()?;
let client = Client::connect_with(options).await?;

// Create the table if needed (admin helper, not part of wasi-docstore)
client.ensure_table("my_table").await?;
```

## Live tests

[`tests/live.rs`](tests/live.rs) exercises the `wasi-docstore` boundary against a
real storage account (or Azurite). It is `#[ignore]`d so it never runs in CI; run
it explicitly:

```bash
AZURE_STORAGE_ACCOUNT=... AZURE_STORAGE_KEY=... \
  cargo nextest run -p omnia-azure-table --run-ignored all
```

## License

MIT OR Apache-2.0
