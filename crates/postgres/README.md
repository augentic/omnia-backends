# omnia-postgres

[![crates.io](https://img.shields.io/crates/v/omnia-postgres.svg)](https://crates.io/crates/omnia-postgres)
[![docs.rs](https://docs.rs/omnia-postgres/badge.svg)](https://docs.rs/omnia-postgres)

`PostgreSQL` backend for the Omnia WASI runtime, implementing the `wasi-sql` interface.

Uses `deadpool-postgres` connection pooling with optional TLS via `rustls`. Supports multiple named pools for connecting to several databases from a single runtime.

MSRV: Rust 1.99

## Configuration

| Variable                     | Required | Default   | Description                                                                                                                    |
| ---------------------------- | -------- | --------- | ------------------------------------------------------------------------------------------------------------------------------ |
| `POSTGRES_URL`               | yes      |           | Default pool connection URI                                                                                                    |
| `POSTGRES_POOL_SIZE`         | no       | `10`      | Default pool size                                                                                                              |
| `POSTGRES_POOLS`             | no       |           | Comma-separated extra pool names (matched case-insensitively; normalized to ASCII uppercase for environment variable suffixes) |
| `POSTGRES_URL__<NAME>`       | per pool |           | URI for named pool                                                                                                             |
| `POSTGRES_POOL_SIZE__<NAME>` | no       | inherited | Pool size for named pool                                                                                                       |

Pool size is a ceiling, not a floor. A connection idle for more than a minute
is closed, so a quiet replica holds few connections against the server's
`max_connections`. Every connection is checked with a round trip before it is
handed out, so a connection the network or server dropped is replaced, not
returned.

Getting a connection is bounded by these limits, which add up within one attempt:

- 10 seconds waiting for a free slot when the pool is exhausted
- 0.5 seconds checking each idle connection before reuse
- 5 seconds opening a new connection

The whole attempt is capped at 15 seconds. If every pooled connection died, a
pool of up to 20 recovers within one attempt. A larger pool may fail the first
attempt, which still discards the dead connections it checked.

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
use omnia::{Backend, FromEnv};
use omnia_postgres::Client;

let options = omnia_postgres::ConnectOptions::load_env()?;
let client = Client::connect_with(options).await?;
```

## Live tests

[`tests/live.rs`](tests/live.rs) exercises the `wasi-sql` boundary against a real
database — the acceptance gate for the `into_param` / `into_wasi_row` mapping
pair: scalar, temporal, JSON, binary, and null round-trips, uint64 overflow
rejection, and `exec` affected counts. It also covers the pool lifecycle:

- `idle_reap` checks idle connections are closed server-side. It waits about 100 seconds.
- `terminated_backend` checks a session killed on the server is replaced.
- `frozen_network` routes its own connections through a local relay, then
  freezes the relay so the connections stay open but carry nothing. It checks a
  checkout fails in bounded time, then recovers once the relay thaws. Only its
  own connections are affected, so it runs alongside the other tests.

The tests are `#[ignore]`d so they never run in CI. To run them against a local
container, save this as a script and run it from the repository root. It needs
Docker and a free port 5432.

```bash
#!/usr/bin/env bash
set -euo pipefail

container=omnia-postgres-live
docker run -d --rm --name "$container" -e POSTGRES_PASSWORD=postgres \
  -p 5432:5432 postgres:17 >/dev/null
trap 'docker rm -f "$container" >/dev/null' EXIT

# TCP check skips the image's socket-only init server
until docker exec "$container" pg_isready -h 127.0.0.1 -U postgres >/dev/null 2>&1; do
  sleep 1
done

export POSTGRES_URL='postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable'
cargo nextest run -p omnia-postgres --all-features --run-ignored only
```

### Against a real server

The same tests run against a real server, such as Azure Database for
`PostgreSQL`. `frozen_network` keeps verifying the server's TLS certificate
through its relay.

```bash
POSTGRES_URL='postgres://user:pass@myserver.postgres.database.azure.com:5432/mydb?sslmode=require' \
  cargo nextest run -p omnia-postgres --all-features --run-ignored only
```

- Connect directly on port 5432, not through `PgBouncer` on 6432. `PgBouncer` keeps
  its own server connections open, so `idle_reap` would fail.
- `terminated_backend` needs Postgres 14 or later.
- Use a dev or test server. The tests write only a temporary table, but they
  open a few connections and end their own sessions.

Each test tags its sessions with an `application_name` starting `omnia-live-`.
To watch them while the tests run, connect with `psql "$POSTGRES_URL"` in
another terminal and run the query below. `\watch 5` reruns it every five
seconds. It is a `psql` command, so in pgAdmin drop it and rerun the query
by hand, or use the server's Dashboard tab.

```sql
SELECT application_name, state, count(*), max(now() - state_change) AS longest_in_state
FROM pg_stat_activity
WHERE application_name LIKE 'omnia-live-%'
GROUP BY 1, 2
ORDER BY 1, 2 \watch 5
```

## License

MIT OR Apache-2.0
