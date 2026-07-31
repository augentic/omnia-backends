# omnia-nats

[![crates.io](https://img.shields.io/crates/v/omnia-nats.svg)](https://crates.io/crates/omnia-nats)
[![docs.rs](https://docs.rs/omnia-nats/badge.svg)](https://docs.rs/omnia-nats)

NATS backend for the Omnia WASI runtime, implementing the `wasi-messaging`, `wasi-keyvalue`, and `wasi-blobstore` interfaces.

Uses `async-nats` with JetStream for key-value and object store capabilities. Supports JWT/NKey authentication.

MSRV: Rust 1.95

## Configuration

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `NATS_ADDR` | no | `demo.nats.io` | NATS server address |
| `NATS_TOPICS` | no | | Comma-separated subscription topics |
| `NATS_JWT` | no | | JWT for authentication |
| `NATS_SEED` | no | | `NKey` seed for signing |

## Usage

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
NATS_ADDR=demo.nats.io \
  cargo nextest run -p omnia-nats --run-ignored all
```

## License

MIT OR Apache-2.0
