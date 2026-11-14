# Azure Table Storage Desk Test

Self-seeding desk test for the `omnia-azure-table` crate's `wasi-docstore` implementation. It creates the table, seeds five records across two partitions (`desk` and `mobile`) covering multiple `OData` types, exercises every filter variant, CRUD operation, and cross-partition identity, then cleans up after itself.

No manual data pre-seeding is required.

## Covered scenarios

- **CRUD**: insert, get round-trip, put (upsert), delete, double-delete, duplicate insert is already-exists, get non-existent returns None
- **Multi-partition identity**: same RowKey in different partitions coexists with distinct composite IDs; partition-scoped vs cross-partition queries return correct subsets
- **Cross-partition round-trip**: query all → put/delete using only the document's composite id (no partition key in the collection string)
- **Filters**: `Compare` (Eq/Ne/Gt/Gte/Lte across Boolean, Int32, Float64, String), `InList`, `NotInList`, `And`, `Or`, `Not`
- **Unsupported filter rejection**: `Contains`, `StartsWith`, `EndsWith`, `IsNull`, `IsNotNull` fail with `unsupported` (Azure Table `OData` does not support string functions or null checks)
- **Query options**: `offset`, `limit`, continuation tokens
- **OData type annotations**: `Edm.Int64` round-trip, `Edm.Guid` and `Edm.DateTime` via serde `#[serde(rename = "Field@odata.type")]` pattern
- **Plain ids**: an id without a partition separator round-trips as a partition of its own (empty RowKey); table-only collection accepted with composite id

## Running with Azurite

1. Start Azurite (table service on port 10002):

   ```bash
   azurite --tableHost 127.0.0.1 --tablePort 10002
   ```

   Or via Docker:

   ```bash
   docker run -d -p 10002:10002 mcr.microsoft.com/azure-storage/azurite \
     azurite-table --tableHost 0.0.0.0 --skipApiVersionCheck
   ```

2. Create an `.env` file in the workspace root (or export the variables):

   ```bash
   AZURE_STORAGE_ACCOUNT=devstoreaccount1
   AZURE_STORAGE_KEY=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==
   AZURE_TABLE_ENDPOINT=http://127.0.0.1:10002/devstoreaccount1
   ```

   These are the well-known Azurite development credentials.

3. Run the desk test:

   ```bash
   cargo run --example azure_table
   ```

## Running against Azure

Set the storage account name and key. Leave `AZURE_TABLE_ENDPOINT` unset (defaults to `https://{account}.table.core.windows.net`):

```bash
AZURE_STORAGE_ACCOUNT=myaccount \
AZURE_STORAGE_KEY=<base64-key> \
cargo run --example azure_table
```

## Expected output

```
Azure Table document store desk test
============================

Table 'testJsondb': created
Seeded 5 records (2 partitions)

  PASS  get round-trip (all fields verified)
  PASS  cross-partition query (5 records, all composite ids)
  PASS  partition-scoped query: desk (3 records)
  PASS  partition-scoped query: mobile (2 records)
  PASS  Compare: IsActive eq true in desk (2)
  ...
  PASS  cross-partition round-trip update (put from query result)
  PASS  cross-partition round-trip delete (delete from query result)
  PASS  same RowKey in different partitions (distinct composite ids)
  PASS  plain id round-trips (PartitionKey = id, empty RowKey)
  PASS  table-only collection accepted for get with composite id

Cleaned up N rows. All tests passed!
```
