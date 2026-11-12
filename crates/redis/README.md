# omnia-redis

[![crates.io](https://img.shields.io/crates/v/omnia-redis.svg)](https://crates.io/crates/omnia-redis)
[![docs.rs](https://docs.rs/omnia-redis/badge.svg)](https://docs.rs/omnia-redis)

Redis key-value backend for the Omnia WASI runtime, implementing the `omnia:keyvalue` interface.

Uses the `redis` crate with a `ConnectionManager` for automatic reconnection and retry.
Keys are namespaced by bucket (`{bucket}:{key}`). Needs Redis 6.0 or later.

MSRV: Rust 1.99

## Key-value

- **Buckets.** A guest opens only the buckets `KEYVALUE_BUCKETS` declares, and any
  other name fails with `not-found`. A bucket name holds only ASCII letters, digits,
  `_`, and `-`, so no bucket's prefix reaches into another's keys.
- **TTLs.** An entry set without a TTL never expires. A TTL is rounded up to the next
  millisecond (`SET .. PX`), and one too long for `PX` is stored without expiry.
  - `set` replaces an entry's lifetime.
  - `compare-and-swap` and `increment` apply their TTL only to an entry they create.
    An update keeps the time the entry has left (`KEEPTTL`).
- **Counters.** `increment` is native `INCRBY`, so a counter reads back as decimal
  text. Incrementing a value that is not an integer, or past the `i64` range, fails
  with `invalid`.
- **Errors.** A refused, dropped, or timed-out connection, or a server that is loading
  or failing over, is `unavailable`. A failed authentication or an ACL refusal
  (`NOAUTH`, `WRONGPASS`, `NOPERM`) is `denied`. Anything else is `other`.

## Configuration

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `REDIS_URL` | no | `redis://localhost:6379` | Redis connection URL |
| `REDIS_MAX_RETRIES` | no | `3` | Maximum reconnection attempts |
| `REDIS_MAX_DELAY` | no | `1000` | Maximum retry delay in milliseconds |
| `KEYVALUE_BUCKETS` | no | `default` | Comma-separated buckets a guest may open |

## Usage

Bind the backend in your host's `runtime!` map — the guest `.wasm` is untouched
(see the [Production Backends guide](https://github.com/augentic/omnia/blob/main/docs/guides/production-backends.md)):

```rust,ignore
use omnia_redis::Client as Redis;
use omnia_wasi_keyvalue::WasiKeyValue;

omnia::runtime!({
    hosts: {
        WasiKeyValue: Redis,
    }
});
```

For direct or embedded use, connect it yourself:

```rust,ignore
use omnia::{Connect, FromEnv};
use omnia_redis::Client;

let options = omnia_redis::ConnectOptions::load_env()?;
let client = Client::connect_with(options).await?;
```

## Live tests

[`tests/live.rs`](tests/live.rs) exercises the `omnia:keyvalue` boundary against a
real Redis. `keyvalue_state_store` runs the shared `StateStore` rows from a real
guest, and the other tests cover the round trip and the atomics. Every test is
`#[ignore]`d so it never runs in CI.

The rows first empty the `default` and `second` buckets, so run them against a
local server only:

```bash
docker run -d --name redis -p 6379:6379 redis:7

REDIS_URL=redis://localhost:6379 \
  cargo nextest run -p omnia-redis --all-features --run-ignored all
```

## License

MIT OR Apache-2.0
