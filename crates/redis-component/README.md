# omnia-redis-component

Redis key-value backend for the Omnia WASI runtime as a backend package: a
Wasm component exporting `omnia:backend-keyvalue/keyvalue` that reaches Redis
only through the links the host dials for it (`omnia:backend/transport`).

The host pins one instance per deployment, calls `lifecycle.connect`, and
forwards every `wasi-keyvalue` request to the matching export. The package
speaks RESP2 over a pool of links, one command at a time per link, so commands
on different links interleave. Keys are namespaced by bucket (`{bucket}:{key}`),
`set` takes a millisecond TTL (`SET … PX`), `cas` is one `EVAL` of a Lua
compare-and-swap that preserves any remaining TTL (`KEEPTTL`), and `increment`
is the `wasi-keyvalue` contract: an 8-byte big-endian signed integer, added to
through `GET` and `cas` until the swap lands.

Natively the crate is the same commands over a scripted link, the RESP2 codec
([`resp`]) and the CAS script ([`CAS_SCRIPT`]), which the e2e fake shares with
the package.

MSRV: Rust 1.99

## Configuration

The deployment's entry grants the package its endpoint and its settings; the
package reads the settings once, at `connect`:

| Key        | Required | Default | Description                                      |
|------------|----------|---------|--------------------------------------------------|
| `links`    | no       | `4`     | Links the package holds open to the first endpoint |
| `password` | no       | —       | Sent with `AUTH` as each link opens              |

## Wiring

The package takes the `hosts:` row a native client takes, through
`omnia::backends::ComponentKeyValue` (omnia's `backends` feature, plus `jit`
to load the raw `.wasm`):

```rust
use omnia::backends::ComponentKeyValue;
use omnia_wasi_keyvalue::WasiKeyValue;

omnia::runtime!({
    hosts: {
        WasiKeyValue: ComponentKeyValue,
    }
});
```

The row connects from the environment:

| Variable                    | Description                                              |
|-----------------------------|----------------------------------------------------------|
| `KEYVALUE_PACKAGE`          | The package file (required)                              |
| `KEYVALUE_EGRESS`           | `host:port` of the Redis server (`localhost:6379`)       |
| `KEYVALUE_SETTING_LINKS`    | The `links` setting                                      |
| `KEYVALUE_SETTING_PASSWORD` | The `password` setting                                   |

Or carries its options: `WasiKeyValue: ComponentKeyValue(PackageOptions::new("redis.wasm").egress(["localhost:6379"]).setting("links", "8"))`.

## Building

```bash
cargo build -p omnia-redis-component --target wasm32-wasip2
```

The artifact is `target/wasm32-wasip2/debug/omnia_redis_component.wasm`. The
crate's own build script builds it for the e2e suites, which run the shared
`wasi-keyvalue` guest scenarios over it against an in-process fake Redis.

## Live tests

[`tests/live.rs`](tests/live.rs) runs the same guests over the package against
a real Redis at `REDIS_ENDPOINT` (`host:port`, default `localhost:6379`), with
`REDIS_PASSWORD` when the server requires one. It is `#[ignore]`d so it never
runs in CI:

```bash
docker run -d --name redis -p 6379:6379 redis:latest

REDIS_ENDPOINT=localhost:6379 \
  cargo nextest run -p omnia-redis-component --all-features --run-ignored all --test live
```

[`tests/bench.rs`](tests/bench.rs) is a measurement, not a test: the native
`omnia-redis` client and the package over the same fake, sequentially and under
a 64-way fan-out:

```bash
cargo nextest run -p omnia-redis-component --all-features --run-ignored all --test bench --no-capture
```

## License

MIT OR Apache-2.0
