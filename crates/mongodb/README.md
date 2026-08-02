# omnia-mongodb

[![crates.io](https://img.shields.io/crates/v/omnia-mongodb.svg)](https://crates.io/crates/omnia-mongodb)
[![docs.rs](https://docs.rs/omnia-mongodb/badge.svg)](https://docs.rs/omnia-mongodb)

MongoDB blobstore backend for the Omnia WASI runtime, implementing the `wasi-blobstore` interface.

Maps blobstore containers to MongoDB collections using the official `mongodb` driver.

MSRV: Rust 1.97

## Configuration

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `MONGODB_URL` | yes | | MongoDB connection URI (must include a default database) |

## Usage

Bind the backend in your host's `runtime!` map — the guest `.wasm` is untouched
(see the [Production Backends guide](https://github.com/augentic/omnia/blob/main/docs/guides/production-backends.md)):

```rust,ignore
use omnia_mongodb::Client as MongoDb;
use omnia_wasi_blobstore::WasiBlobstore;

omnia::runtime!({
    hosts: {
        WasiBlobstore: MongoDb,
    }
});
```

For direct or embedded use, connect it yourself:

```rust,ignore
use omnia::{Backend, FromEnv};
use omnia_mongodb::Client;

let options = omnia_mongodb::ConnectOptions::load_env()?;
let client = Client::connect_with(options).await?;
```

## Live tests

[`tests/live.rs`](tests/live.rs) exercises the `wasi-blobstore` boundary against a
real MongoDB (containers map to collections). It is `#[ignore]`d so it never runs
in CI; run it explicitly:

```bash
docker run -d --name mongodb -p 27017:27017 mongo:latest

MONGODB_URL=mongodb://localhost:27017/omnia \
  cargo nextest run -p omnia-mongodb --run-ignored all
```

## License

MIT OR Apache-2.0
