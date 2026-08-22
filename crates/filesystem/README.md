# omnia-filesystem

Filesystem provider for `wasi:blobstore` and `wasi:keyvalue`: durable, local,
network-free storage over one shared root directory — the local-first
counterpart to the network-service backends (Azure Blob, Redis, MongoDB,
NATS). One `Client` serves both interfaces from disjoint subtrees of the
root (`blobstore/` and `keyvalue/`), so same-named containers and buckets
never collide.

## Blobstore

- Containers map to subdirectories of the `blobstore/` subtree. Object names may contain `/`
  and map to nested paths beneath their container directory (sanitized:
  `..`, absolute paths, and empty segments are rejected), so clients encode
  their own sharding in names.
- Writes are temp-file + atomic-rename: an object is either fully visible or
  absent, never torn, and concurrent same-name writes are benign (last
  rename wins).
- `object-info` reports size and created-at from file metadata.

## Keyvalue

- Buckets map to subdirectories of the `keyvalue/` subtree; keys may contain
  `/` and map to nested paths beneath their bucket directory, sanitized as
  above.
- Writes are temp-file + atomic-rename, so a value is never observed torn.
- The `wasi:keyvalue/atomics` surface (`swap`, `increment`) is native under a
  per-key lock shared by every bucket handle opened from one client: a stale
  swap never overwrites and returns a handle refreshed at the observed value.
  Counter values are 8-byte big-endian `i64`.
- Scope: the lock serializes writers within one host process; a single
  process owns the root (the same assumption the blobstore makes).

## Configuration

| Variable          | Description                                       |
| ----------------- | ------------------------------------------------- |
| `FILESYSTEM_ROOT` | Root directory of the store; created when absent. |

Deployments that anchor the root themselves (rather than through the
environment) construct the backend with `Client::open(root)`.
