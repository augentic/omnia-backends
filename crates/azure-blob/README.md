# omnia-azure-blob

[![crates.io](https://img.shields.io/crates/v/omnia-azure-blob.svg)](https://crates.io/crates/omnia-azure-blob)
[![docs.rs](https://docs.rs/omnia-azure-blob/badge.svg)](https://docs.rs/omnia-azure-blob)

Azure Blob Storage blobstore backend for the Omnia WASI runtime, implementing the `wasi-blobstore` interface and the plugin loader's `PluginStore`.

Maps blobstore containers to Azure Blob containers and blobs to block blobs using the official `azure_storage_blob` SDK.

## Plugin store

`Client` also implements `omnia::PluginStore`, the digest-keyed store behind
omnia's registry acquirer, in a dedicated container the backend names itself:
`omnia-plugins`. Content blobs are keyed `content/<sha256:hex>` and shared
across registries; release records are keyed
`releases/<registry>/<package>-<version>.json`, scoped per registry. Writes
are verify-before-persist; a blob PUT is atomic on the service side.

Guest `wasi:blobstore` containers map one-to-one onto Azure containers, so a
guest container named `plugins` is simply the Azure container `plugins` —
never the store's `omnia-plugins`. A guest that names `omnia-plugins` itself
shares that Azure container; deployments that lend guests blobstore access on
the same storage account should treat the name as reserved.

MSRV: Rust 1.97

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

Bind the backend in your host's `runtime!` map — the guest `.wasm` is untouched
(see the [Production Backends guide](https://github.com/augentic/omnia/blob/main/docs/guides/production-backends.md)):

```rust,ignore
use omnia_azure_blob::Client as AzureBlob;
use omnia_wasi_blobstore::WasiBlobstore;

omnia::runtime!({
    hosts: {
        WasiBlobstore: AzureBlob,
    }
});
```

For direct or embedded use, connect it yourself:

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
