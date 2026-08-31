# omnia-filesystem

Filesystem provider for `wasi:blobstore` and `wasi:keyvalue`, plus the
plugin loader's `PluginStore`: durable, local, network-free storage over one
shared root directory — the local-first counterpart to the network-service
backends (Azure Blob, Redis, MongoDB, NATS). One `Client` serves every
surface from disjoint subtrees of the root (`blobstore/`, `keyvalue/`, and
`plugins/`), so same-named containers and buckets never collide — and no
guest container or bucket name can reach the plugin store.

## Blobstore

- Containers map to subdirectories of the `blobstore/` subtree. Object names may contain `/`
  and map to nested paths beneath their container directory (sanitized:
  `..`, absolute paths, and empty segments are rejected), so clients encode
  their own sharding in names.
- Opening a container is an ensure: `get-container` creates the directory
  when absent, exactly as `create-container` does (and as keyvalue bucket
  opens do), so read paths never fail on a container nothing has written
  yet.
- Writes are temp-file + atomic-rename: an object is either fully visible or
  absent, never torn, and concurrent same-name writes are benign (last
  rename wins).
- `object-info` reports size and created-at from file metadata.

## Keyvalue

- Buckets map to subdirectories of the `keyvalue/` subtree; keys may contain
  `/` and map to nested paths beneath their bucket directory, sanitized as
  above.
- Writes are temp-file + atomic-rename, so a value is never observed torn.
- Writers (`set`, `delete`, `swap`, `increment`) serialize on a per-key lock
  shared by every bucket handle opened from one client. A stale swap never
  overwrites and returns a handle refreshed at the observed value. Counter
  values are 8-byte big-endian `i64`.
- Scope: the lock serializes writers within one host process; a single
  process owns the root (the same assumption the blobstore makes).

## Plugin store

`Client` also implements `omnia::PluginStore`, the digest-keyed store behind
omnia's registry acquirer, under the `plugins/` subtree:

- Content entries land at `plugins/content/<sha256:hex>` and are shared
  across registries — the digest is the identity. Writes are
  verify-before-persist (bytes that do not hash to their key are refused)
  and atomic (temp-file + rename), matching the blobstore's guarantees.
- Release records land at
  `plugins/releases/<registry>/<package>-<version>.json`, scoped per
  registry so an endpoint override is never answered from another
  registry's record.
- The subtree is a sibling of `blobstore/` and `keyvalue/`: guest storage
  cannot name its way into the plugin store, by construction.

## Configuration

| Variable          | Description                                       |
| ----------------- | ------------------------------------------------- |
| `FILESYSTEM_ROOT` | Root directory of the store; created when absent. |

Deployments that anchor the root themselves (rather than through the
environment) construct the backend with `Client::open(root)`.
