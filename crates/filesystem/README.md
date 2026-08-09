# omnia-filesystem

Filesystem provider for `wasi:blobstore`: a durable, local, network-free
object store over one root directory — the local-first counterpart to the
network-service backends (Azure Blob, MongoDB, NATS).

- Containers map to subdirectories of the root. Object names may contain `/`
  and map to nested paths beneath their container directory (sanitized:
  `..`, absolute paths, and empty segments are rejected), so clients encode
  their own sharding in names.
- Writes are temp-file + atomic-rename: an object is either fully visible or
  absent, never torn, and concurrent same-name writes are benign (last
  rename wins).
- `object-info` reports size and created-at from file metadata.

## Configuration

| Variable         | Description                                             |
| ---------------- | ------------------------------------------------------- |
| `BLOBSTORE_ROOT` | Root directory of the object tree; created when absent. |

Deployments that anchor the root themselves (rather than through the
environment) construct the backend with `Client::open(root)`.
