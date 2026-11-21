## 0.32.0

Unreleased

### Added

- `omnia-kafka` (`KAFKA_TOPIC_PREFIX`) and `omnia-nats`
  (`NATS_TOPIC_PREFIX`): optional topic prefix prepended on produce and
  subscribe and stripped from each delivered message's topic before it
  reaches the host, so guests speak logical topic names across environments.

### Changed

- **Breaking:** MSRV is Rust 1.100. The guest-side members, `test-programs`
  and `examples`, build for `wasm32-wasip3`, because omnia 0.38 runs WASI 0.3
  guests alone.
- **Breaking:** the Redis, NATS, and filesystem key-value backends implement
  the revisioned, TTL-aware `Bucket` of `omnia:keyvalue`, under one set of
  rules:
  - an entry set without a TTL never expires;
  - a TTL is rounded up to the store's resolution, never down;
  - a `set` that replaces the lifetime sets its TTL, and one that keeps it
    applies its TTL only to an entry it creates, as `increment` does;
  - an update that keeps the lifetime keeps the time the entry has left;
  - a TTL longer than the store can hold is stored without expiry;
  - `get` answers an entry's revision with its value;
  - a `set` or `delete` conditioned on a revision, or on the key being
    absent, fails with the entry it found.

  A revision is a digest of the value on Redis and the filesystem, and the
  sequence of the message that holds the entry on NATS.
- **Breaking:** the NATS and filesystem backends keep a counter as decimal
  text, as Redis does, where they kept an 8-byte big-endian integer. A
  counter written by an earlier release fails its next `increment` with
  `invalid`, so delete or rewrite each one before upgrading. All three refuse
  a text that is not canonical, such as `007`, `-0`, or `+1`.
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
  fails any other with `not-found`. `default` is the host's system-assigned
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
  - A query's filter arrives as the groups of conditions `omnia:docstore`
    defines. Each group's conditions join with `or` and the groups with
    `and`, and a negated condition is wrapped in `not (...)`.
  - An `array-contains` test fails with `unsupported`, as the host refused
    it before.
- Postgres and Azure Table failures carry typed error kinds. A malformed
  statement, value, or id is `invalid`, and a refused credential or
  permission is `denied`. A feature the service cannot serve is
  `unsupported`, and a lost or timed-out connection is `unavailable`.
  Anything else, including a Postgres constraint violation, stays `other`.
- **Breaking:** only `create-container` makes a blob container. The
  filesystem, MongoDB, Azure Blob, and NATS backends fail a call on an
  object in a container that does not exist with
  `omnia_wasi_blobstore::missing_container`, which a guest's
  `BlobStore` reports with the code `not-found`. Creating a container that
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
- The filesystem, MongoDB, Azure Blob, and NATS backends implement the
  `Container` of `omnia:blobstore`:
  - a read takes a half-open range clamped to the blob, so an inverted
    range reads as empty, where it failed;
  - a listing answers the blobs under a prefix, with their metadata, in
    name order;
  - the metadata of an absent blob is `None`, where it failed.

  `omnia-mongodb` lists blobs without reading their data.
- `omnia-mongodb` dates a blob in seconds since the Unix epoch, as
  `omnia-wasi-blobstore` documents, where it wrote milliseconds. A blob written
  by an earlier release reads back in seconds too.
- **Breaking:** `omnia-genai` and `omnia-cursor` serve the `omnia:model` of
  omnia 0.38:
  - a request that opens the model `default` runs on `GENAI_MODEL` or
    `CURSOR_MODEL`, and any other name is the provider's model id;
  - a fault in the request is `invalid`;
  - a spent budget ends the completion as `exhausted`, naming the budget and
    carrying the tokens spent where the provider reported them;
  - a `worker` grant rides on the workspace lend it applies to.
- **Breaking:** `omnia-cursor` no longer passes MCP server grants to the
  agent, since `omnia:model` no longer carries them.
- `omnia-genai` reports the tokens every round of a completion spent, where
  it reported the last round's. Both model backends report the input tokens
  read from and written to the provider's prompt cache.
- `omnia-genai` sets the provider's reasoning effort from a request's
  `effort`. Genai's own levels, such as `minimal` or `xhigh`, pass through
  `other`, and any level genai does not know fails with `invalid`.

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
