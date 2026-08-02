# omnia-azure-blob

[![crates.io](https://img.shields.io/crates/v/omnia-azure-blob.svg)](https://crates.io/crates/omnia-azure-blob)
[![docs.rs](https://docs.rs/omnia-azure-blob/badge.svg)](https://docs.rs/omnia-azure-blob)

Azure Blob Storage blobstore backend for the Omnia WASI runtime, implementing the `wasi-blobstore` interface.

Maps blobstore containers to Azure Blob containers and blobs to block blobs using the official `azure_storage_blob` SDK.

MSRV: Rust 1.95

## Configuration

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `AZURE_BLOB_ENDPOINT` | yes | | Storage account endpoint (e.g. `https://myaccount.blob.core.windows.net/`) |
| `AZURE_TENANT_ID` | no | | Azure AD tenant ID (for service principal auth) |
| `AZURE_CLIENT_ID` | no | | Azure AD client ID (for service principal auth) |
| `AZURE_CLIENT_SECRET` | no | | Azure AD client secret (for service principal auth) |

When service principal credentials are not provided, the backend falls back to
`DeveloperToolsCredential` which authenticates via Azure CLI (`az login`) or
Azure Developer CLI (`azd auth login`).

## Usage

```rust,ignore
use omnia::{Backend, FromEnv};
use omnia_azure_blob::Client;

let options = omnia_azure_blob::ConnectOptions::load_env()?;
let client = Client::connect_with(options).await?;
```

## Live tests

[`tests/live.rs`](tests/live.rs) exercises the `wasi-blobstore` boundary against
a real storage account (or Azurite): write/read/list/metadata round-trips plus
the ranged-read cases mirroring the `range_options` unit vectors. The tests are
`#[ignore]`d so they never run in CI; run them explicitly (authentication is
Entra ID only — service principal or developer tools):

```bash
AZURE_BLOB_ENDPOINT=https://<account>.blob.core.windows.net \
AZURE_TENANT_ID=... AZURE_CLIENT_ID=... AZURE_CLIENT_SECRET=... \
  cargo nextest run -p omnia-azure-blob --run-ignored all
```

## License

MIT OR Apache-2.0
