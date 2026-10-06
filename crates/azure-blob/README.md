# omnia-azure-blob

[![crates.io](https://img.shields.io/crates/v/omnia-azure-blob.svg)](https://crates.io/crates/omnia-azure-blob)
[![docs.rs](https://docs.rs/omnia-azure-blob/badge.svg)](https://docs.rs/omnia-azure-blob)

Azure Blob Storage blobstore backend for the Omnia WASI runtime, implementing the `wasi-blobstore` interface and the plugin loader's `PackageStore`.

Maps blobstore containers to Azure Blob containers and blobs to block blobs using the official `azure_storage_blob` SDK.

## Package store

`Client` also implements `omnia_plugin::PackageStore`, the store omnia's
registry acquirer reads before any registry and writes what it fetches to,
in a dedicated container the backend names itself: `omnia-plugins`. One
blob per release, named as omnia's `FsStore` files it
(`namespace_name@version.wasm`); a blob already there is never replaced, so
a stored release is final until it is deleted. The acquirer verifies what it
fetches before the write and what the store serves against a load's pin.

Guest `wasi:blobstore` containers map one-to-one onto Azure containers, so a
guest container named `plugins` is simply the Azure container `plugins` —
never the store's `omnia-plugins`. The name `omnia-plugins` is reserved: the
blobstore view refuses to create, open, delete, or probe it, so the store is
the container's only writer even when guests share the storage account.

MSRV: Rust 1.99

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
a real storage account (or Azurite): write/read/list/metadata round-trips, the
ranged-read cases mirroring the `range_options` unit vectors, and the package
store round-trip. The tests are `#[ignore]`d so they never run in CI; run them
explicitly (authentication is Entra ID only — service principal or developer
tools). [`tests/blobstore.rs`](tests/blobstore.rs) holds what the view refuses
before any request, such as the reserved `omnia-plugins` name, and runs in CI.

```bash
AZURE_BLOB_ENDPOINT=https://<account>.blob.core.windows.net \
AZURE_TENANT_ID=... AZURE_CLIENT_ID=... AZURE_CLIENT_SECRET=... \
  cargo nextest run -p omnia-azure-blob --run-ignored all
```

## License

MIT OR Apache-2.0
