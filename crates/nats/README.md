# omnia-nats

[![crates.io](https://img.shields.io/crates/v/omnia-nats.svg)](https://crates.io/crates/omnia-nats)
[![docs.rs](https://docs.rs/omnia-nats/badge.svg)](https://docs.rs/omnia-nats)

NATS backend for the Omnia WASI runtime, implementing the `wasi-messaging`, `wasi-keyvalue`, and `wasi-blobstore` interfaces.

Uses `async-nats` with JetStream for key-value and object store capabilities. Supports JWT/NKey authentication.

MSRV: Rust 1.97

## Configuration

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `NATS_ADDR` | no | `demo.nats.io` | NATS server address |
| `NATS_TOPICS` | no | | Comma-separated subscription topics |
| `NATS_JWT` | no | | JWT for authentication |
| `NATS_SEED` | no | | `NKey` seed for signing |

## Usage

Bind the backend in your host's `runtime!` map — the guest `.wasm` is untouched
(see the [Production Backends guide](https://github.com/augentic/omnia/blob/main/docs/guides/production-backends.md)):

```rust,ignore
use omnia_nats::Client as Nats;
use omnia_wasi_messaging::WasiMessaging;
use omnia_wasi_keyvalue::WasiKeyValue;
use omnia_wasi_blobstore::WasiBlobstore;

omnia::runtime!({
    hosts: {
        WasiMessaging: Nats,
        WasiKeyValue: Nats,
        WasiBlobstore: Nats,
    }
});
```

For direct or embedded use, connect it yourself:

```rust,ignore
use omnia::{Backend, FromEnv};
use omnia_nats::Client;

let options = omnia_nats::ConnectOptions::load_env()?;
let client = Client::connect_with(options).await?;
```

## Live tests

[`tests/live.rs`](tests/live.rs) exercises the `wasi-messaging` boundary against a
real server. It is `#[ignore]`d so it never runs in CI; run it explicitly (the
default `NATS_ADDR` is the public `demo.nats.io`):

```bash
# or run a local server with JetStream instead of the public demo instance:
# docker run -d --name nats -p 4222:4222 nats:latest -js

NATS_ADDR=demo.nats.io \
  cargo nextest run -p omnia-nats --run-ignored all
```

## License

MIT OR Apache-2.0
