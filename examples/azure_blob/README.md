# Azure Blob Storage Example

This is a small CLI that invokes the `azure_blob` backend directly so that some desk testing can be done.

## Prerequisites

An Azure Storage account with Blob Storage. Create one if needed:

```bash
# Create a resource group (skip if you already have one)
az group create --name my-resource-group --location westus2

# Create the storage account
az storage account create -n mystorageaccount -g my-resource-group --kind StorageV2

# Assign the Storage Blob Data Contributor role to your signed-in user
az role assignment create \
  --role "Storage Blob Data Contributor" \
  --assignee $(az ad signed-in-user show --query id -o tsv) \
  --scope $(az storage account show -n mystorageaccount -g my-resource-group --query id -o tsv)
```

## Configuration

The example requires the following environment variable (also loadable from `.env`):

```
AZURE_BLOB_ENDPOINT=https://mystorageaccount.blob.core.windows.net/
```

### Authentication

For local development, sign in via the Azure CLI:

```bash
az login
```

The backend will use `DeveloperToolsCredential` automatically.

For service principal authentication, also set:

```
AZURE_TENANT_ID=<tenant id>
AZURE_CLIENT_ID=<client id>
AZURE_CLIENT_SECRET=<client secret>
```

## What it does

The example is self-contained -- it creates its own container and blobs during the run and cleans up afterwards. No manual data seeding is required.

Operations exercised:

1. Create container `testaugenticblob`
2. Check container exists
3. Write `greeting.txt` (text) and `data.json` (JSON)
4. Read `greeting.txt` back and log content
5. List all blobs in the container
6. Check blob existence (`has_object`)
7. Get blob metadata (`object_info`) -- name, size, created_at
8. **Chunked stream read** -- write an 8 MiB blob, read it back using the example's streaming download helper, and verify byte-level integrity to confirm multi-chunk reassembly works correctly
9. Delete `greeting.txt` and confirm it no longer exists
10. Clean up: delete all blobs and the container

## Running

```bash
cargo run --example azure_blob
```

For debug-level tracing output:

```bash
RUST_LOG=debug cargo run --example azure_blob
```
