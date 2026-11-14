## 0.32.0

Unreleased

### Added

- `omnia-kafka` (`KAFKA_TOPIC_PREFIX`) and `omnia-nats`
  (`NATS_TOPIC_PREFIX`): optional topic prefix prepended on produce and
  subscribe and stripped from each delivered message's topic before it
  reaches the host, so guests speak logical topic names across environments.

### Changed

- **Breaking:** the Redis, NATS, and filesystem key-value backends implement
  the TTL-aware `Bucket` of `omnia:keyvalue`, under one set of rules:
  - an entry set without a TTL never expires;
  - a TTL is rounded up to the store's resolution, never down;
  - `set` replaces an entry's lifetime;
  - `compare_and_swap` and `increment` apply their TTL only to an entry they
    create, and an update keeps the time the entry has left;
  - a TTL longer than the store can hold is stored without expiry.
- **Breaking:** `omnia-redis` no longer expires every `set`, `increment`, and
  `swap` after a day. A key written without a TTL now lives until deleted, so
  a caller that relied on the day must pass a TTL. `r9k-omnia`'s
  validation-failure counter is one known case.
- **Breaking:** buckets are declared. Each of the three backends opens only
  the buckets `KEYVALUE_BUCKETS` names (comma-separated, default `default`),
  and fails any other with `not-found`. A name holds only ASCII letters,
  digits, `_`, and `-`. To migrate:
  - Redis: declare every bucket in use, the part of each key before its
    first `:`.
  - NATS: declare every KV bucket a guest opens. The backend creates a
    declared bucket that does not exist, and no other.
  - filesystem: declare every directory under `keyvalue/` a guest opens.
    `Client::open(root)` declares `default` alone.
  - Each `ConnectOptions` gains a `buckets` field, which breaks struct
    literals such as emery's.
- `omnia-nats` turns on per-message TTL for every declared bucket it opens.
  It creates a new bucket with it, and updates an existing one, which the
  server cannot undo. If the server refuses, a write with a TTL fails with
  `unsupported`.
- Key-value failures carry typed error kinds: a key or counter the store
  cannot hold is `invalid`, a refused credential or permission is `denied`,
  and a lost or timed-out connection is `unavailable`. Anything else stays
  `other`.
- **Breaking:** identities are declared. `omnia-azure-id` serves only the
  names `IDENTITY_NAMES` lists (comma-separated, default `default`), and
  fails any other with `no-such-identity`, which a guest's `Identity`
  reports as `ErrorKind::NotFound`. `default` is the host's system-assigned
  identity, or the user-assigned one whose client id `IDENTITY_CLIENT_ID`
  gives. Each other name takes its client id from
  `IDENTITY_CLIENT_ID__<NAME>`, and connecting fails without it. To migrate:
  - A guest that named an identity by its client id uses its unnamed handle,
    with `IDENTITY_CLIENT_ID` set to that id. Or the deployment declares the
    name it uses and maps it to the id.
  - `ConnectOptions` gains an `identities` map, so the unit literal no longer
    builds. Use `ConnectOptions::default()` or `load_env`.
- `omnia-azure-id` creates one credential per identity at connect, so a
  repeated request for a scope is answered from the credential's token
  cache. A managed-identity source `azure_identity` does not support now
  fails at connect, not at the first token request.
- **Breaking:** `omnia-postgres` implements `omnia:sql`. `execute` returns
  the affected row count, and `batch` runs its statements in one
  transaction. A failing statement, or a batch dropped before it commits,
  rolls the whole batch back. A database name the deployment does not
  declare fails with `not-found`.
- **Breaking:** `omnia-azure-table` accepts a plain document id, stored as
  the `PartitionKey` with an empty `RowKey`, and composite ids keep
  working. Other changes:
  - A missing table is created on the first write to it, and reads as
    empty until then.
  - A duplicate `insert` returns `already-exists` and leaves the stored
    entity untouched.
  - `order_by` fails with `unsupported` instead of being ignored.
  - A zero `limit` fails with `invalid`.
- Postgres and Azure Table failures carry typed error kinds. A malformed
  statement, value, or id is `invalid`, and a refused credential or
  permission is `denied`. A feature the service cannot serve is
  `unsupported`, and a lost or timed-out connection is `unavailable`.
  Anything else, including a Postgres constraint violation, stays `other`.
- **Breaking:** only `create-container` makes a blob container. The
  filesystem, MongoDB, Azure Blob, and NATS backends fail a call on an
  object in a container that does not exist with
  `omnia_wasi_blobstore::Error::missing_container`, which a guest's
  `BlobStore` reports as `ErrorKind::NotFound`. Creating a container that
  exists opens it, and deleting an absent object or container succeeds. To
  migrate:
  - filesystem and MongoDB: create each container a guest uses before it
    runs. Opening one no longer creates it, so a guest's first write to an
    uncreated container fails.
  - Azure Blob: a read in a container that does not exist fails, where it
    answered the object as absent.
- `omnia-azure-vault` deletes an absent secret as a no-op, where it
  failed.
- `omnia-azure-blob` reads a range that starts past the end of a blob as
  empty, as the other blob backends do, where it failed.

---

Release notes for previous releases can be found on the respective release
branches of the repository.

<!-- ARCHIVE_START -->
* [0.31.x](https://github.com/augentic/omnia-backends/blob/release-0.31.0/RELEASES.md)
* [0.30.x](https://github.com/augentic/omnia-backends/blob/release-0.30.0/RELEASES.md)
* [0.28.x](https://github.com/augentic/omnia-backends/blob/release-0.28.0/RELEASES.md)
* [0.27.x](https://github.com/augentic/omnia-backends/blob/release-0.27.0/RELEASES.md)
* [0.26.x](https://github.com/augentic/omnia-backends/blob/release-0.26.0/RELEASES.md)

- [0.25.x](https://github.com/augentic/omnia-backends/blob/release-0.25.0/RELEASES.md)
- [0.24.x](https://github.com/augentic/omnia-backends/blob/release-0.24.0/RELEASES.md)
- [0.23.x](https://github.com/augentic/omnia-backends/blob/release-0.23.0/RELEASES.md)
