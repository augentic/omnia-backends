# omnia-filesystem

Filesystem provider for `wasi:blobstore` and `wasi:keyvalue`: durable,
local, network-free storage over one shared root directory — the
local-first counterpart to the network-service backends (Azure Blob, Redis,
MongoDB, NATS). One `Client` serves both surfaces from disjoint subtrees of
the root (`blobstore/`, and `keyvalue/` with its `keyvalue-ttl/` deadlines),
so same-named containers and buckets never collide. A local package store for the plugin loader is
omnia's own `FsStore`, named by the deployment's `plugins.store`, not a
backend.

## Blobstore

- Containers map to subdirectories of the `blobstore/` subtree. Object names may contain `/`
  and map to nested paths beneath their container directory (sanitized:
  `..`, absolute paths, and empty segments are rejected), so clients encode
  their own sharding in names.
- A container is a directory only `create-container` makes, and creating
  one that exists opens it. `get-container` on a missing directory fails
  with `omnia_wasi_blobstore::Error::missing_container`, which a guest's
  `BlobStore` reports as `ErrorKind::NotFound`. So does a write through a
  handle on a container deleted since, so nothing recreates it.
- `delete-container` and `delete-object` succeed when their target is
  already absent.
- Writes are temp-file + atomic-rename: an object is either fully visible or
  absent, never torn, and concurrent same-name writes are benign (last
  rename wins).
- `object-info` reports size and created-at from file metadata.

## Keyvalue

- Buckets map to subdirectories of the `keyvalue/` subtree. A guest opens
  only the buckets `KEYVALUE_BUCKETS` declares, and any other name fails with
  `not-found`. A bucket name holds only ASCII letters, digits, `_`, and `-`.
- Keys may contain `/` and map to nested paths beneath their bucket
  directory, sanitized as above, so every portable key qualifies. Any other
  key fails with `invalid`. Keys that differ only in case collide on a
  case-insensitive filesystem, such as macOS's default.
- Writes are temp-file + atomic-rename, so a value is never observed torn. A
  value file holds only the caller's bytes.
- An entry's deadline lives at the same path under `keyvalue-ttl/`, as unix
  milliseconds rounded up, so it outlives the client that set it. An entry
  past its deadline reads as absent, and the next call on its key removes it.
  - `set` replaces an entry's lifetime.
  - `compare-and-swap` and `increment` apply their TTL only to an entry they
    create. An update keeps the time the entry has left.
- Every call on a key serializes on a per-key lock shared by every bucket
  handle opened from one client. Counter values are 8-byte big-endian `i64`,
  and incrementing any other value, or past the `i64` range, fails with
  `invalid`.
- A permission-denied I/O error is `denied`. Anything else is `other`.
- Scope: the lock serializes writers within one host process; a single
  process owns the root (the same assumption the blobstore makes).

## Configuration

| Variable           | Required | Default          | Description                                         |
| ------------------ | -------- | ---------------- | --------------------------------------------------- |
| `FILESYSTEM_ROOT`  | no       | `.omnia/storage` | Root directory of the store; created when absent.   |
| `KEYVALUE_BUCKETS` | no       | `default`        | Comma-separated key-value buckets a guest may open. |

## Usage

Bind the backend in your host's `runtime!` map — one `Client` can back both
`WasiBlobStore` and `WasiKeyValue`:

```rust,ignore
use omnia_filesystem::Client as Filesystem;
use omnia_wasi_blobstore::WasiBlobStore;
use omnia_wasi_keyvalue::WasiKeyValue;

omnia::runtime!({
    hosts: {
        WasiBlobStore: Filesystem,
        WasiKeyValue: Filesystem,
    }
});
```

Deployments that anchor the root themselves (rather than through the
environment) construct the backend with `Client::open(root)`, which declares
the `default` bucket alone. One that needs more buckets passes them with the
root, through `Client::connect_with(ConnectOptions { root, buckets })`.

## Tests

[`tests/blobstore.rs`](tests/blobstore.rs) and
[`tests/keyvalue.rs`](tests/keyvalue.rs) run the shared
`omnia_test::contract::blob` and `omnia_test::contract::state` rows from a real
guest, over a temporary root. The other tests cover what no guest reaches, such
as a write through a stale container handle and the names the backend refuses.
Nothing needs a service, so every test runs on
`cargo nextest run -p omnia-filesystem --all-features`, and in CI.
