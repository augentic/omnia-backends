# omnia-azure-id

[![crates.io](https://img.shields.io/crates/v/omnia-azure-id.svg)](https://crates.io/crates/omnia-azure-id)
[![docs.rs](https://docs.rs/omnia-azure-id/badge.svg)](https://docs.rs/omnia-azure-id)

Azure Identity backend for the Omnia WASI runtime, implementing the `wasi-identity` interface.

Acquires Azure AD access tokens via Managed Identity credentials using the official `azure_identity` SDK.

MSRV: Rust 1.99

## Configuration

A guest asks for an identity by name, and its unnamed handle asks for
`default`. The backend serves the names a deployment declares and refuses
every other.

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `IDENTITY_NAMES` | no | `default` | Comma-separated identity names |
| `IDENTITY_CLIENT_ID` | no | | Client id of the user-assigned identity behind `default`; unset, `default` is the system-assigned identity |
| `IDENTITY_CLIENT_ID__<NAME>` | per name | | Client id of the user-assigned identity behind a name other than `default`, the name in ASCII uppercase |

A host whose guests use only its system-assigned identity needs none of them.
A guest that also asks for `reports` needs `IDENTITY_NAMES=default,reports`
and `IDENTITY_CLIENT_ID__REPORTS`.

Each identity's credential is created at connect and caches the tokens it
issues. Hosts that set `IDENTITY_ENDPOINT` and `IDENTITY_HEADER`, such as App
Service, Azure Functions, and Container Apps, are served through that
endpoint, and VMs through the instance metadata service. Connecting fails on
the managed-identity sources `azure_identity` does not support: Azure Arc,
Azure ML, Cloud Shell, and Service Fabric.

### Scopes

Azure managed identity issues a token for exactly one scope. A guest calls
`Identity::token` with one, such as `https://storage.azure.com/.default`.
`IdentityExt::access_token` asks for none, so it fails on this backend, as
does a request for two.

## Usage

Bind the backend in your host's `runtime!` map — the guest `.wasm` is untouched
(see the [Production Backends guide](https://github.com/augentic/omnia/blob/main/docs/guides/production-backends.md)):

```rust,ignore
use omnia_azure_id::Client as AzureId;
use omnia_wasi_identity::WasiIdentity;

omnia::runtime!({
    hosts: {
        WasiIdentity: AzureId,
    }
});
```

For direct or embedded use, connect it yourself:

```rust,ignore
use omnia::{Connect, FromEnv};
use omnia_azure_id::Client;

let options = omnia_azure_id::ConnectOptions::load_env()?;
let client = Client::connect_with(options).await?;
```

## Tests

**Guests through the runtime.** [`tests/identity.rs`](tests/identity.rs) runs a
guest from [`crates/test-programs`](../test-programs) through
`omnia_test::host::Deployment` over a fake App Service managed-identity
endpoint, [`tests/support`](tests/support), which the backend reaches through
`IDENTITY_ENDPOINT` as it reaches the real one. The guest asks for the unnamed,
a declared, and an undeclared identity, and the test asserts the client id
each token request carried. It needs no Azure, so it runs on every
`cargo nextest run -p omnia-azure-id --all-features`.

**The real service.** [`tests/live.rs`](tests/live.rs) asks real Azure AD for a
`default` token. It is `#[ignore]`d so it never runs in CI; run it by hand on
an Azure host with a managed identity (e.g. an Azure VM or App Service),
setting `IDENTITY_CLIENT_ID` when the identity is user-assigned:

```bash
cargo nextest run -p omnia-azure-id --run-ignored all
```

## License

MIT OR Apache-2.0
