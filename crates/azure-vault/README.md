# omnia-azure-vault

[![crates.io](https://img.shields.io/crates/v/omnia-azure-vault.svg)](https://crates.io/crates/omnia-azure-vault)
[![docs.rs](https://docs.rs/omnia-azure-vault/badge.svg)](https://docs.rs/omnia-azure-vault)

Azure Key Vault secrets backend for the Omnia WASI runtime, implementing the `wasi-vault` interface.

Manages secrets in Azure Key Vault using the official `azure_security_keyvault_secrets` SDK. Secrets are base64url-encoded and namespaced per locker identifier.

MSRV: Rust 1.97

## Configuration

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `AZURE_KEYVAULT_URL` | no | | Key Vault URL (omit to disable vault) |
| `AZURE_TENANT_ID` | no | | Tenant ID for service principal auth |
| `AZURE_CLIENT_ID` | no | | Client ID for service principal auth |
| `AZURE_CLIENT_SECRET` | no | | Client secret for service principal auth |

When no service principal credentials are set, `DeveloperToolsCredential` is used as a fallback.

## Usage

Bind the backend in your host's `runtime!` map — the guest `.wasm` is untouched
(see the [Production Backends guide](https://github.com/augentic/omnia/blob/main/docs/guides/production-backends.md)):

```rust,ignore
use omnia_azure_vault::Client as AzureVault;
use omnia_wasi_vault::WasiVault;

omnia::runtime!({
    hosts: {
        WasiVault: AzureVault,
    }
});
```

For direct or embedded use, connect it yourself:

```rust,ignore
use omnia::{Backend, FromEnv};
use omnia_azure_vault::Client;

let options = omnia_azure_vault::ConnectOptions::load_env()?;
let client = Client::connect_with(options).await?;
```

## Live tests

[`tests/live.rs`](tests/live.rs) exercises the `wasi-vault` boundary against a
real Key Vault. It is `#[ignore]`d so it never runs in CI; run it explicitly:

```bash
AZURE_KEYVAULT_URL=https://<vault>.vault.azure.net \
AZURE_TENANT_ID=... AZURE_CLIENT_ID=... AZURE_CLIENT_SECRET=... \
  cargo nextest run -p omnia-azure-vault --run-ignored all
```

## License

MIT OR Apache-2.0
