# omnia-postgres

[![crates.io](https://img.shields.io/crates/v/omnia-postgres.svg)](https://crates.io/crates/omnia-postgres)
[![docs.rs](https://docs.rs/omnia-postgres/badge.svg)](https://docs.rs/omnia-postgres)

`PostgreSQL` backend for the Omnia WASI runtime, serving `omnia:sql` and the `wasi:sql` draft through `omnia-wasi-sql`'s `SqlClient`.

Uses `deadpool-postgres` connection pooling with optional TLS via `rustls`. Supports multiple named pools for connecting to several databases from a single runtime.

MSRV: Rust 1.99

## Configuration

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `POSTGRES_URL` | yes | | Default pool connection URI |
| `POSTGRES_POOL_SIZE` | no | `10` | Default pool size |
| `POSTGRES_POOLS` | no | | Comma-separated extra pool names (matched case-insensitively; normalized to ASCII uppercase for environment variable suffixes) |
| `POSTGRES_URL__<NAME>` | per pool | | URI for named pool |
| `POSTGRES_POOL_SIZE__<NAME>` | no | inherited | Pool size for named pool |

Pool size is a ceiling. Connections open on demand and close after a minute
idle, so a quiet runtime holds few of the server's `max_connections`. Each
connection is checked with a round trip before it is handed out, so one the
server or network has dropped is replaced, not returned.

## Statements

Each connection the host opens holds one pooled session, and every statement
run through it lands on that session. The host opens one per `omnia:sql`
call, so session state such as a temporary table does not outlive the call.

- `execute` returns the rows the statement affected.
- `batch` runs its statements in one transaction and returns each statement's
  affected count. A failing statement rolls the whole batch back, and so does
  a batch dropped before it commits. Every parameter is converted before the
  transaction begins, and a failure names the statement's position in the
  batch.
- A `uint64` above `i64::MAX` cannot bind to any Postgres integer and fails
  with `invalid`.

## Errors

A failure reaches the guest as one of the `omnia:sql` error kinds, chosen by
the server's SQLSTATE class where it sent one.

| Kind | Failure |
|------|---------|
| `not-found` | A database name the deployment does not declare |
| `denied` | Class `28` (invalid authorization), and `42501` (insufficient privilege) |
| `invalid` | Classes `22` (data exception) and `42` (syntax or access rule), a value the column type refuses, or a statement given the wrong number of parameters |
| `unsupported` | Class `0A` (feature not supported), or a result column of a type the backend does not map |
| `unavailable` | Classes `08` (connection), `53` (insufficient resources) and `57` (operator intervention), a pool that times out or is closed, or a connection lost in transit |
| `other` | Anything else, including a constraint violation (class `23`) |

## Usage

Bind the backend in your host's `runtime!` map — the guest `.wasm` is untouched
(see the [Production Backends guide](https://github.com/augentic/omnia/blob/main/docs/guides/production-backends.md)):

```rust,ignore
use omnia_postgres::Client as Postgres;
use omnia_wasi_sql::WasiSql;

omnia::runtime!({
    hosts: {
        WasiSql: Postgres,
    }
});
```

For direct or embedded use, connect it yourself:

```rust,ignore
use omnia::{Connect, FromEnv};
use omnia_postgres::Client;

let options = omnia_postgres::ConnectOptions::load_env()?;
let client = Client::connect_with(options).await?;
```

## Live tests

[`tests/live.rs`](tests/live.rs) runs against a real database.

- `sql_sql_db` runs the `omnia_sdk::contract::sql` rows from a real guest.
- The round trips are the acceptance gate for the `into_param` /
  `into_wasi_row` mapping pair: scalar, temporal, JSON, binary, and null
  values, uint64 overflow, and `execute` affected counts.
- `batch_abandoned` drops a batch mid-transaction and checks that nothing it
  wrote committed and that its session returned to the pool clean.
- `error_kinds` checks the kinds a guest can cause itself.
- `terminated_backend` kills a pooled session on the server and checks that
  the next checkout replaces it.

`sql_sql_db` drops the rows' tables (`batch_items`, `rollback_items`,
`placeholder_items`, `value_items`) before it runs, and `batch_abandoned`
drops `abandoned_items`, so point the suite at a database of your own. The
tests are `#[ignore]`d so they never run in CI; run them explicitly:

```bash
docker run -d --name postgres -e POSTGRES_PASSWORD=postgres -p 5432:5432 postgres:17

POSTGRES_URL='postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable' \
  cargo nextest run -p omnia-postgres --all-features --run-ignored all
```

## License

MIT OR Apache-2.0
