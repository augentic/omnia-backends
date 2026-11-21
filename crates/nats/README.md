# omnia-nats

[![crates.io](https://img.shields.io/crates/v/omnia-nats.svg)](https://crates.io/crates/omnia-nats)
[![docs.rs](https://docs.rs/omnia-nats/badge.svg)](https://docs.rs/omnia-nats)

NATS backend for the Omnia WASI runtime, implementing the `wasi-messaging`, `wasi-keyvalue`, and `wasi-blobstore` interfaces.

Uses `async-nats` with JetStream for key-value and object store capabilities. Supports JWT/NKey authentication.

MSRV: Rust 1.100

## Key-value

- **Buckets.** A guest opens only the buckets `KEYVALUE_BUCKETS` declares, and any
  other name fails with `not-found`. A bucket name holds only ASCII letters, digits,
  `_`, and `-`. A declared bucket that does not exist yet is created when first
  opened, with a history of one and a size limit of 100 mebibytes.
- **Per-message TTL.** It needs NATS server 2.11 or later.
  - A bucket the backend creates has it from the start.
  - An existing bucket without it has it turned on when opened, which the server
    cannot undo.
  - If the server refuses, the bucket still opens, and a write with a TTL fails with
    `unsupported`.
- **Keys.** A key is not empty, neither starts nor ends with `.`, and holds only
  `[-/_=.a-zA-Z0-9]`, so every portable key qualifies. Any other key fails with
  `invalid`.
- **TTLs.** An entry set without a TTL never expires, unless `NATS_KV_MAX_AGE_SECS`
  caps it. A read checks the entry's deadline to the millisecond, rounded up, so it
  never sees an entry past it. The server removes the entry up to a second later,
  and `keys()` may list it until then.
  - A `set` that replaces the lifetime sets the TTL it names, and one that keeps it
    applies the TTL only to an entry it creates. An update keeps the time the entry
    has left.
  - `increment` applies its TTL only to an entry it creates.
- **Revisions.** An entry's revision is the sequence of the message that holds it, so
  every write makes a new one. A conditioned `set` or `delete`, a `set` that keeps the
  lifetime, and `increment` are writes conditioned on that sequence, retried while
  another writer gets in first. One whose condition fails returns the entry it found.
- **Counters.** A counter is decimal text, as Redis keeps one, and incrementing any
  other value, or past the `i64` range, fails with `invalid`.
- **Errors.** A timed-out or broken connection is `unavailable`. The server reports a
  permissions violation out of band, so it too surfaces as a timeout. Anything else is
  `other`.

## Blobstore

- A container is a JetStream object store, which only `create-container` makes.
  Creating one that exists opens it, whatever configuration made it.
- Opening a store that does not exist fails with
  `omnia_wasi_blobstore::missing_container`, which reaches a guest as
  the code `not-found`.
- Deleting an absent object or store succeeds.

## Configuration

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `NATS_ADDR` | no | `demo.nats.io` | NATS server address |
| `NATS_TOPICS` | no | | Comma-separated subscription topics |
| `NATS_TOPIC_PREFIX` | no | | Prefix prepended to every published and subscribed topic, and stripped from delivered subjects |
| `NATS_JWT` | no | | JWT for authentication |
| `NATS_SEED` | no | | `NKey` seed for signing |
| `NATS_KV_MAX_AGE_SECS` | no | | Max age for each JetStream KV bucket this client creates, capping every entry's lifetime, even one set without a TTL; unset, an entry lives until deleted or its TTL passes |
| `KEYVALUE_BUCKETS` | no | `default` | Comma-separated KV buckets a guest may open |

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
use omnia::{Connect, FromEnv};
use omnia_nats::Client;

let options = omnia_nats::ConnectOptions::load_env()?;
let client = Client::connect_with(options).await?;
```

## Live tests

[`tests/live.rs`](tests/live.rs) exercises the `wasi-messaging`, `omnia:keyvalue`,
and `wasi-blobstore` boundaries against a real server. `keyvalue_state_store` and
`blobstore_blob_store` run the shared `omnia_test::contract` `state` and `blob` rows from
a real guest, the latter covering the container rules above. The other key-value tests
cover the atomics under contention and a bucket gaining per-message TTL when
opened. Every test is `#[ignore]`d so it never runs in CI.

The state rows first empty the `default` and `second` buckets, and the blob rows
create object stores prefixed `omnia-live-<pid>`, so run a local server with
JetStream. The rows and `keyvalue_upgrade` refuse the default `demo.nats.io`:

```bash
nats-server -js -sd "$(mktemp -d)" &
# or: docker run -d --name nats -p 4222:4222 nats:latest -js

NATS_ADDR=localhost:4222 \
  cargo nextest run -p omnia-nats --all-features --run-ignored all
```

## License

MIT OR Apache-2.0
