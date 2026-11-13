# omnia-mongodb

[![crates.io](https://img.shields.io/crates/v/omnia-mongodb.svg)](https://crates.io/crates/omnia-mongodb)
[![docs.rs](https://docs.rs/omnia-mongodb/badge.svg)](https://docs.rs/omnia-mongodb)

MongoDB blobstore backend for the Omnia WASI runtime, implementing the `wasi-blobstore` interface.

Maps blobstore containers to MongoDB collections using the official `mongodb` driver.

- Only `create-container` makes a collection, and creating one that exists opens it.
- Opening a collection that does not exist fails with
  `omnia_wasi_blobstore::Error::missing_container`, which a guest's `BlobStore`
  reports as `ErrorKind::NotFound`. Each open costs one `listCollections`.
- Deleting an absent object or collection succeeds.

MSRV: Rust 1.99

## Configuration

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `MONGODB_URL` | yes | | MongoDB connection URI (must include a default database) |

## Usage

Bind the backend in your host's `runtime!` map — the guest `.wasm` is untouched
(see the [Production Backends guide](https://github.com/augentic/omnia/blob/main/docs/guides/production-backends.md)):

```rust,ignore
use omnia_mongodb::Client as MongoDb;
use omnia_wasi_blobstore::WasiBlobStore;

omnia::runtime!({
    hosts: {
        WasiBlobStore: MongoDb,
    }
});
```

For direct or embedded use, connect it yourself:

```rust,ignore
use omnia::{Connect, FromEnv};
use omnia_mongodb::Client;

let options = omnia_mongodb::ConnectOptions::load_env()?;
let client = Client::connect_with(options).await?;
```

## Live tests

[`tests/live.rs`](tests/live.rs) exercises the `wasi-blobstore` boundary against a
real MongoDB (containers map to collections): a write/read/delete round-trip and
the container rules above. The tests are `#[ignore]`d so they never run in CI; run
them explicitly:

```bash
docker run -d --name mongodb -p 27017:27017 mongo:latest

MONGODB_URL=mongodb://localhost:27017/omnia \
  cargo nextest run -p omnia-mongodb --run-ignored all
```

## License

MIT OR Apache-2.0
