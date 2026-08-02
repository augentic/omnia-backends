# omnia-postgres

[![crates.io](https://img.shields.io/crates/v/omnia-postgres.svg)](https://crates.io/crates/omnia-postgres)
[![docs.rs](https://docs.rs/omnia-postgres/badge.svg)](https://docs.rs/omnia-postgres)

`PostgreSQL` backend for the Omnia WASI runtime, implementing the `wasi-sql` interface.

Uses `deadpool-postgres` connection pooling with optional TLS via `rustls`. Supports multiple named pools for connecting to several databases from a single runtime.

MSRV: Rust 1.95

## Configuration

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `POSTGRES_URL` | yes | | Default pool connection URI |
| `POSTGRES_POOL_SIZE` | no | `10` | Default pool size |
| `POSTGRES_POOLS` | no | | Comma-separated extra pool names |
| `POSTGRES_URL__<NAME>` | per pool | | URI for named pool |
| `POSTGRES_POOL_SIZE__<NAME>` | no | inherited | Pool size for named pool |

## Usage

```rust,ignore
use omnia::{Backend, FromEnv};
use omnia_postgres::Client;

let options = omnia_postgres::ConnectOptions::load_env()?;
let client = Client::connect_with(options).await?;
```

## Live tests

[`tests/live.rs`](tests/live.rs) exercises the `wasi-sql` boundary against a real
database — the acceptance gate for the `into_param` / `into_wasi_row` mapping
pair: scalar, temporal, JSON, binary, and null round-trips, uint64 overflow
rejection, and `exec` affected counts. The tests are `#[ignore]`d so they never
run in CI; run them explicitly:

```bash
docker run -d --name postgres -e POSTGRES_PASSWORD=postgres -p 5432:5432 postgres:17

POSTGRES_URL='postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable' \
  cargo nextest run -p omnia-postgres --run-ignored all
```

## License

MIT OR Apache-2.0
