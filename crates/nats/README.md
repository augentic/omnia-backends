# omnia-nats

[![crates.io](https://img.shields.io/crates/v/omnia-nats.svg)](https://crates.io/crates/omnia-nats)
[![docs.rs](https://docs.rs/omnia-nats/badge.svg)](https://docs.rs/omnia-nats)

NATS backend for the Omnia WASI runtime, implementing the `wasi-messaging`, `wasi-keyvalue`, and `wasi-blobstore` interfaces.

Uses `async-nats` with JetStream for key-value and object store capabilities. Supports JWT/NKey authentication.

MSRV: Rust 1.99

## Configuration

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `NATS_ADDR` | no | `demo.nats.io` | NATS server address |
| `NATS_TOPICS` | no | | Comma-separated subscription topics |
| `NATS_JWT` | no | | JWT for authentication |
| `NATS_SEED` | no | | `NKey` seed for signing |
| `NATS_KV_MAX_AGE_SECS` | no | | Bucket-wide max age for each JetStream KV bucket this client creates; unset, keys persist until deleted |

## Key-value

A bucket is a JetStream KV bucket created on first open with per-message TTL
enabled, which needs NATS server 2.11 or later:

- `set` with a TTL publishes the entry with a `Nats-TTL` header, and the
  server expires it; `cas` and `increment` carry the remaining TTL over to
  the value they write.
- A bucket created by an earlier release has per-message TTL disabled, so the
  server refuses a write with a TTL until `allow_msg_ttl` is enabled on the
  bucket's stream (or the bucket is recreated).

## Usage

Bind the backend in your host's `runtime!` map — the guest `.wasm` is untouched
(see the [Production Backends guide](https://github.com/augentic/omnia/blob/main/docs/guides/production-backends.md)):

```rust,ignore
use omnia_nats::Client as Nats;
use omnia_wasi_messaging::WasiMessaging;
use omnia_wasi_keyvalue::WasiKeyValue;
use omnia_wasi_blobstore::WasiBlobStore;

omnia::runtime!({
    hosts: {
        WasiMessaging: Nats,
        WasiKeyValue: Nats,
        WasiBlobStore: Nats,
    }
});
```

For direct or embedded use, connect it yourself:

```rust,ignore
use omnia_backend::{Connect, FromEnv};
use omnia_nats::Client;

let options = omnia_nats::ConnectOptions::load_env()?;
let client = Client::connect_with(options).await?;
```

## Live tests

[`tests/live.rs`](tests/live.rs) exercises the `wasi-messaging` and `wasi-keyvalue`
boundaries against a real server, per-key TTL included. It is `#[ignore]`d so it never
runs in CI; run it explicitly (the default `NATS_ADDR` is the public `demo.nats.io`):

```bash
# or run a local server with JetStream instead of the public demo instance:
# docker run -d --name nats -p 4222:4222 nats:latest -js

NATS_ADDR=demo.nats.io \
  cargo nextest run -p omnia-nats --run-ignored all
```

## License

MIT OR Apache-2.0
