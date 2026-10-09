# Kafka backend resilience

Thirteen defects in the Kafka consume and produce paths, the eight changes
that close them, and how each error condition is handled. Seven changes
are confined to `omnia-kafka`. One, the seam between the `wasi:messaging`
host and its backend, is in
[augentic/omnia](https://github.com/augentic/omnia). None changes the
`wasi:messaging` WIT, so no guest is recompiled.

## Acceptance criteria

1. **No silent loss.** A record is dropped only when the guest drops it on
   purpose (it returns `Ok` without acting on it), or when no guest could
   read it. Every drop the backend makes is logged at `error` and counted.
2. **Processing order per message key.** Records sharing a key are handled
   one at a time, in offset order.
3. **A short replay window.** A crash or restart replays a few hundred
   milliseconds of input. Committing that often must not mean one commit
   request per record.
4. **Throughput at least matching the legacy services**, and offset
   handling no weaker than theirs.

Duplicates on restart are inherent to periodic committing. Guests are
expected to tolerate a replayed record, as the legacy services already do.

## Context

The defects were observed on 29 Sep 2026 running the `trip-update` guest
in the `runtime` example on `omnia` 0.36 and `omnia-kafka` 0.30, against a
local broker. `KafkaMirror` copied two production topics
(`ppd-realtime-gtfs-vp.v2` and `ppd-realtime-r9k-station-event.v1`) onto
local `dev-` topics, in one of two modes:

- **Replay** (`--from <timestamp>`) writes historical records as fast as
  production returns them. The guest gets thousands of records at once,
  which reproduces the load defects (D7, D8) within seconds.
- **Tail** (no `--from`) forwards records at the real cadence. The same
  races show up as occasional single incidents.

### The legacy services

Throughout this document, **Node** means `at_realtime_common` (on a fork of
KafkaJS 1.15) together with the Node services that consume through it, such
as `at_realtime_vp_adapter` and `at_trip_update_enricher`. **Java** means
`at_schedule_adherence` (Kafka Streams 3.6.1).

|                      | Node                                                                                                        | Java                                                      |
| -------------------- | ----------------------------------------------------------------------------------------------------------- | --------------------------------------------------------- |
| Concurrency          | up to 12 partitions at once; up to 50 records in flight per partition, which is paused until its batch ends | one record at a time per partition                        |
| Offset eligible when | the handler calls `next()`; only the run of offsets with no gaps is resolved (`lastResolvedOffset`)         | `process()` returned and the producer flushed             |
| Commit cadence       | at the end of a fetch cycle, about once a second (`maxWaitTimeInMs: 1000`)                                  | `commit.interval.ms`, default 30 s                        |
| Uncaught failure     | an unhandled rejection; each service's handler exits with `process.exit(1)`                                 | Streams enters `ERROR`; `MainComponent` restarts Streams  |
| Publish              | app-level queue flushed every 100 ms; resolves once the broker acknowledges; drops the oldest at 25 000     | Streams record collector, flushed before every commit     |
| Partitioner          | KafkaJS default (murmur2 in JavaScript doubles)                                                             | Java default (`Utils.murmur2`)                            |

What shapes the design:

- **Node's handlers catch their own failures.** Each Node service wraps its
  processing in `try`/`catch` and calls `next()` whatever happened, so an
  upstream failure (Redis, HTTP, a failed publish) still commits the record.
  Only an error that escapes the handler reaches the process, which exits.
  Java's processors do the same: they catch Redis and HTTP failures and
  return, and only an exception that escapes `process()` restarts Streams.
- **Out-of-order completion needs a gap-free offset.** With many records in
  flight a later one can finish first. Node commits only the longest run
  with no gaps. Change 8 does the same and keeps Node's name for it.
- **Node's producer queue is not copied.** librdkafka has its own queue,
  batching and retries, so change 5 does not rebuild them.
- **Partition placement must stay compatible with Node.** Change 3 keeps
  the KafkaJS scheme as the default and adds the Java one as an option.

## The defects

| #   | defect                                                       | where it was     | severity          |
| --- | ------------------------------------------------------------ | ---------------- | ----------------- |
| D1  | Unframed payloads on registry topics are delivered corrupted | `registry.rs`    | data loss, live   |
| D2  | Partitioner disagrees with the Java producers on every key   | `partitioner.rs` | correctness       |
| D3  | The record key is dropped on the inbound path                | `messaging.rs`   | missing data      |
| D4  | A failed publish is reported to the guest as success         | `messaging.rs`   | silent loss       |
| D5  | A dead consumer is never noticed                             | `messaging.rs`   | silent stall      |
| D6  | In-flight producer records are lost on shutdown              | `lib.rs`         | loss on deploy    |
| D7  | Unbounded guest spawning exhausts the pool and drops records | `omnia` server   | loss under load   |
| D8  | No ordering: concurrent guests race on shared state          | `omnia` server   | stale output      |
| D9  | At-most-once consumption: failures are treated as consumed   | `messaging.rs`   | silent loss       |
| D10 | Offsets would commit before the output is durable            | both             | silent loss       |
| D11 | The schema cache serialises every decode                     | `registry.rs`    | stall             |
| D12 | The guest's returned error is discarded by the host          | `omnia` server   | invisible failure |
| D13 | A registry failure is silent and turns into wrong bytes      | `registry.rs`    | silent corruption |

**D1 — unframed payloads are corrupted.** The station-event topic has a
registered schema, but its producer writes plain JSON with no Confluent
header. The backend stripped the first five bytes of every payload on a
topic with a schema, without checking for the `0x00` magic byte, and on a
parse failure handed the guest the stripped bytes. The guest received
`tionId":...}` instead of `{"stationId":...}`.

**D2 — the partitioner disagrees with the Java producers.** The backend's
port of the KafkaJS hash does its arithmetic in JavaScript doubles; Kafka's
Java client uses wrapping 32-bit integers. They disagree on every key tried
(`1039-36302-36840-2-9f138052` goes to partition 7 under KafkaJS and 4
under Java), so a component replacing Java would place keys differently.

**D3 — the record key is dropped inbound.** Only the headers were copied
into the message metadata. The guest could not see the key, partition,
offset or timestamp, and the backend had nothing to order or track by.

**D4 — a failed publish looks like success.** When librdkafka refused to
queue a record, `send` logged it and returned `Ok(())`.

**D5 — a dead consumer is never noticed.** A fatal consumer error
(`KafkaError::MessageConsumptionFatal`) means librdkafka will deliver
nothing more. It was logged and filtered out like a transient one, and the
process sat idle looking healthy.

**D6 — the producer's queue is lost on shutdown.** Nothing flushed
librdkafka's outgoing queue when the process stopped.

**D7 — unbounded spawning exhausts the pool.** The host spawned a guest per
record with no limit. Past the instance pool's capacity (default 1000)
instantiation failed, and the record, already taken off the consumer, was
lost.

**D8 — guests race on shared state.** Two records for the same vehicle
could run in two guests at once, and the older position could be written
last. On a mirror replay about 55 such overwrites appeared within 500 ms.

**D9 — at-most-once consumption.** librdkafka's defaults stored an offset
the moment the record was handed over, before any guest ran. A guest that
failed, or never ran, lost its record.

**D10 — offsets would commit before the output is durable.** Once D9 is
fixed, a guest returning `Ok` could have its offset committed while its
published records were still in librdkafka's queue. A crash then loses the
output with no input left to regenerate it.

**D11 — the schema cache stalls every decode.** The cache lock was held
across the HTTP call to the registry, so a cold cache stalled every decode
in the process. With the lock released, the cache was still a `tokio`
mutex whose hit deep-copied the schema's JSON, and the validator was
recompiled on every call. On 8 Oct 2026 every `decode` and `encode` waited
about 86 ms for 0.3 ms of work under change 7's concurrency.

**D12 — the guest's returned error is discarded.** The host mapped the
guest's `result<_, error>` to `()`. A guest returning `Err` looked like one
returning `Ok`: nothing was logged or counted.

**D13 — a registry failure is silent and turns into wrong bytes.** Every
failure was logged at `debug` and the payload went on unchanged.

- A failed fetch on decode, and a framed payload that was not JSON or
  failed validation, handed the guest the framed bytes, header included.
  Only the second was counted.
- A failed fetch on encode, and a payload that was not JSON or failed
  validation, sent unframed JSON to a topic with a schema, which framed
  consumers then rejected. `send` returned `Ok` and nothing was counted.
- Schemas were looked up by topic, so a record was validated against the
  subject's latest schema, not the one it was written with.
- Concurrent misses each made their own request, and the registry client
  had no connect or request timeout.
- A failed fetch was not cached, so during an outage every record made its
  own request. The whole cache was wiped every hour.

## The changes

### Forces

- **Wire compatibility is preserved by default.** The partitioner stays
  `kafkajs` unless configured. Offset semantics do change: that is change 8.
- **`omnia` changes are minimal and Kafka-blind.** The host gains one seam,
  `Client::consume` over a `Handler`. Concurrency, ordering, offsets and the
  failure policy are the backend's.
- **Match Node unless there is a reason not to.** Most services in the
  realtime domain are Node.
- **A failure the guest reports is not committed.** A guest that returns
  `Err`, traps or times out ends the consumer, and the restart replays the
  record (Node and Java both behave this way for a failure their handler
  does not catch). A guest that wants to move past a failure catches it and
  returns `Ok`, as Node's handlers do.
- **A record no guest could read is dropped, logged and counted.** It is
  committed past, as Node does.
- **CI cannot stand up a broker or a registry.** Real-service tests are
  `#[ignore]`d live tests in `tests/live.rs`. Deterministic logic gets unit
  tests.

### 1. Framing — tell a framed payload from an unframed one

_Closes D1._

A payload is framed only when it is at least five bytes long and its first
byte is `0x00`. No UTF-8 JSON starts with `0x00`, so an unframed JSON
payload is never mistaken for a framed one. An unframed payload reaches the
guest byte-identical, with one `warn` per topic. The guest never receives
stripped bytes.

The sniff alone decides whether a record is decoded, and the topic needs no
registered subject: change 2 looks the schema up by the header's id. Two
consequences:

- The backend never learns whether a consumed topic has a subject, so the
  `warn` says only that the topic's payloads are unframed. It fires for the
  first unframed payload on each topic while a registry is configured.
- A consumed topic whose payloads are binary and can start with `0x00` must
  not be consumed with a registry configured.

Tests: `Payload::decode` unit tests (`encode_decode`, `wrong_magic_byte`,
`unframed_payload_is_not_decoded`), and the live test
`registry_unframed_payload`.

### 2. Schema registry — cache by id, fail loudly

_Closes D11 and D13._

**Decode looks the schema up by the id in the record's header.** An id
always names the same schema, so a fetched schema is kept for the life of
the process. Each record is validated against the schema it was written
with, not the subject's latest.

**Encode looks up the topic's latest schema on its first send.** The
subject is `<topic>-value`. The result is kept for the life of the process,
as Node does, so a new schema version is picked up on restart. A 404 is
kept as "no schema", logged at `warn` once, and the payload goes out
unframed.

**One fetch per key at a time.** All records needing the same schema id
(or the same subject, on encode) wait on one shared fetch.

- If it succeeds, the schema is cached and every waiter proceeds.
- If it fails, every waiter fails with it. The failure is logged once at
  `error`, with the registry's status and body. Each record it drops is
  logged once more with its coordinates (acceptance criterion 1) and
  counted, so the counter rises by the number of records affected. A send
  it refuses is counted and returned to the guest, which has the detail.
- A failure is not cached. The next record that needs the schema starts a
  new fetch.
- A schema the registry returns but the backend cannot use (not JSON, not a
  JSON Schema that compiles, no id on the subject's latest) is a failure
  like the others, with its own reason, `invalid_schema`.

**The registry client is bounded.** Each request, connecting included,
times out after 5 s. A transport failure (a refused or reset connection, DNS,
a timeout) is retried twice, after a jittered wait of up to 100 ms and
then 200 ms, as Node's registry client retries three times within 5 s. A
hung registry therefore fails a fetch within about 15 s, where Node's
client has no timeout and waits indefinitely. An HTTP error (429, 5xx) is
not retried: the client only retries what `reqwest` itself failed, and a
response with a status is a failed fetch at once. Nothing retries beyond
that: the next record that needs the schema starts a new fetch.

**A cache hit takes a lock only to clone an `Arc`.** Each entry holds the
schema id and its compiled validator, behind a `std::sync` lock. Nothing is
copied or compiled per record.

`KAFKA_REGISTRY_CACHE_TTL` and the cache cleaner go, since nothing expires.
So does the warning about a record "written with schema X, not the latest".

What happens on each failure is in [Error conditions](#error-conditions),
conditions 7 to 12.

**Changes.**

- `registry.rs`: a schema cache by id and a schema cache by subject, each a
  map of ready entries and in-flight shared futures. A
  `tokio::sync::OnceCell` does not fit, because it hands a failed fetch to
  the next waiter to retry instead of failing them all.
- `Registry::decode` returns the payload or a drop with its reason.
  `Delivery::handle` skips the guest for a drop, marks the record done, and
  counts it.
- `Registry::encode` returns the framed bytes, or an error that `send`
  returns to the guest.
- The `reqwest::Client` given to the registry client sets the timeouts;
  the client's `max_retries` is `2`, with `retries_wait_ms` of `100`.
- Unit tests over the single-flight cache with a scripted fetch: concurrent
  waiters share one fetch; a failure fails every waiter and is not cached; a
  caller dropped mid-fetch leaves the fetch for the next caller to join.
  The live test `registry_wire_format` gains a framed record naming an id
  the registry does not have, which is dropped and committed past.
  `registry_id_mismatch` registers a stricter second version (after setting
  the subject's compatibility to `NONE`, since under `BACKWARD` nothing can
  be valid under the old schema and invalid under the new) and checks that
  a record written under the older id validates against that id's schema
  while one framed under the newer id that fails it is dropped.
- The crate README drops `KAFKA_REGISTRY_CACHE_TTL`, describes conditions
  7 to 12 under Consuming and Producing, and adds the `reason` attribute to
  its counter table.

### 3. Partitioner — interoperate with the Java producers

_Closes D2._

`KAFKA_PARTITIONER` takes `kafkajs` (default) or `java`. The Java scheme is
Kafka's `Utils.murmur2` in wrapping 32-bit arithmetic, giving partition
`(h & 0x7fffffff) % count`. `metadata["partition"]` takes precedence over
both. The KafkaJS default stays because the Node producers use it.

Tests: the `partitioning` unit test over both schemes with known vectors,
and the live test `keyed_sends` under both schemes.

### 4. Keys on the wire

_Closes D3._

Each inbound record's metadata carries `key`, `partition`, `offset` and
`timestamp` alongside its headers. A send with no `metadata["key"]` goes
out with a null key. The delivery callback tolerates a null or non-UTF-8
key.

### 5. Errors — stop reporting failure as success

_Closes D4, D5 and D6._

- `send` returns librdkafka's refusal to queue a record (queue full, too
  large, invalid partition) to the guest, after one retry on a full queue.
  `send` returning `Ok` means queued, not delivered. Delivery is change 8's
  concern.
- A fatal consumer error ends the record stream with the error's detail.
  The consumer ends, the process exits, and the orchestrator restarts it.
  The consumer is not rebuilt in place. Transient errors are logged at
  `warn` and left to librdkafka.
- Dropping the client flushes the producer for up to 10 s. The consumer
  drops after it, and closing it commits the offsets already stored.

### 6. The handler seam — the one `omnia` change

_Closes D12. Enables changes 7 and 8. In `omnia`._

The backend owns the delivery loop and calls the host:

```rust
pub trait Client: Debug + Send + Sync + 'static {
    fn consume(&self, handler: Arc<dyn Handler>) -> FutureResult<()>;
    fn send(&self, topic: String, message: Message) -> FutureResult<()>;
    fn request(&self, ..) -> FutureResult<Message>;
}

pub trait Handler: Send + Sync + 'static {
    fn handle(&self, message: Message) -> BoxFuture<'static, Result<(), HandleError>>;
}

pub enum HandleError {
    /// No guest ran: the routed guest could not be loaded or instantiated.
    Unavailable(String),
    /// The guest returned `Err`.
    Rejected(types::Error),
    /// The guest trapped.
    Trapped(String),
    /// The guest ran past the deployment's guest timeout.
    TimedOut(Duration),
}
```

| outcome of `MessagingHandler::handle` | returns            |
| ------------------------------------- | ------------------ |
| guest returned `Ok`                   | `Ok(())`           |
| guest returned `Err`                  | `Err(Rejected)`    |
| guest trapped                         | `Err(Trapped)`     |
| guest ran past `GUEST_TIMEOUT_MS`     | `Err(TimedOut)`    |
| no route for the topic                | `Ok(())`           |
| guest could not be loaded or started  | `Err(Unavailable)` |

`handle` counts every message (`message_counter`) and logs and counts every
`Err` once (`processing_errors`). It holds one permit per running guest,
sized to `POOL_MAX_INSTANCES`, so this trigger alone cannot exhaust the
instance pool.

`server::run` calls `consume` and returns `Err` when it returns, whether
`consume` returned `Ok` or `Err`. The generated `main` runs every trigger
server under `try_join_all`, so this ends the process with a failure exit
code for the orchestrator to restart. NATS and the in-memory default hand
their stream to a shared `dispatch`, which runs one `handle` per message
and holds nothing back.

### 7. Bounded, ordered delivery

_Closes D7 and D8._

The backend's `consume` is the loop, and `handle` returning is when a
record is done.

- **64 slots.** A record takes a slot to run and returns it when `handle`
  returns. The loop takes a slot before it pulls, so nothing is pulled that
  cannot run.
- **A lane per key.** The order is fixed when the record is pulled. A record
  behind another for its key waits without a slot. A record with no key
  never waits.
- **4096 backlog places,** one per record pulled and not yet done. This is
  a memory ceiling for a stream with fewer distinct keys than slots.
  `max.partition.fetch.bytes` is lowered to 64 KiB so each partition's
  fetch contributes a short run of keys and the backlog never binds on an
  ordinary stream.

While no slot or place is free the loop stops polling, librdkafka's
prefetch fills to its own limits, and fetching stops. Nothing is lost: an
offset is stored only once its record is done.

**A failed outcome ends the loop.** When `handle` returns any `Err`, the
loop stops taking records, the other records in flight are abandoned, and
`consume` returns the error naming the record (`topic[partition]@offset`).
None of their offsets were stored, so the restart replays them from the
committed offset. A record that fails every time keeps restarting the
process until the guest is fixed, as in Java.

A hung guest holds its slot and lane for `GUEST_TIMEOUT_MS` (default 30 s)
and then fails as `TimedOut`, well inside librdkafka's
`max.poll.interval.ms` (300 s).

Tests: unit tests over the loop with scripted records and a scripted
handler (`keys_wait_only_on_themselves`,
`a_record_without_a_key_never_waits`, `the_record_past_the_bound_waits`,
`a_waiting_record_holds_the_resolved_offset`,
`a_failed_outcome_ends_the_loop`,
`a_revoked_partition_is_skipped_but_not_the_rest`,
`a_lane_is_forgotten_with_its_last_record`).

### 8. At-least-once offsets

_Closes D9 and D10._

An offset may be committed once the guest has returned `Ok` **and** every
message sent before that return has been acknowledged by the broker.
Records finish out of order, so the tracker remembers finished offsets
until the gap before them closes.

| setting                    | value                               | why                                                     |
| -------------------------- | ----------------------------------- | ------------------------------------------------------- |
| `enable.auto.offset.store` | `false`                             | the backend decides when a record is done               |
| `enable.auto.commit`       | `true`                              | librdkafka commits stored offsets on its own thread     |
| `auto.commit.interval.ms`  | `KAFKA_COMMIT_INTERVAL_MS`, default 200 | the replay window on a crash                        |
| `message.timeout.ms`       | 120 000                             | how long librdkafka retries a send before giving up     |

**The tracker.** Every `send` takes a number from one counter. When a
guest returns `Ok`, its record is marked done with the counter's current
value. The record is resolved once every send numbered below that value has
a delivery report. The **resolved offset** of a partition is the highest
offset with every record before it resolved, and it is the only offset
stored. A record is registered with the tracker when it is pulled, before
it joins its lane, so a later offset can never resolve past it.

A send that librdkafka refuses to queue reports itself to the tracker, and
so does a send that is never delivered, so neither stalls the counter.

**Rebalance.** When partitions are revoked, the tracker forgets them. A
record of a revoked partition still waiting its turn is skipped, and the
partition's new owner redelivers it. librdkafka commits stored offsets on
revoke and on close.

Flushing the producer before each commit, as Streams does, does not work
here: with 64 guests publishing, the queue is never empty. The send counter
asks a narrower question that one broker round trip answers.

Tests: tracker unit tests
(`out_of_order_completions_store_the_resolved_offset`,
`record_done_before_its_sends_are_delivered`,
`revoked_partition_drops_its_records`,
`enqueue_failure_does_not_stall_delivered`). The live test `at_least_once`
completes records out of order, publishes before completing one, and reads
the group's committed offset. It then fails one record and checks that
`consume` ends with the commit stopped before it.

## Error conditions

Each condition lists what the backend does, with the change and defect
behind it, and what Node and Java do. "Committed" means the record's
offset is stored, so the next commit moves past it. "Restart" means the
consumer ends, the process exits with a failure code, and the orchestrator
starts it again from the committed offset.

### Guest outcomes

**1. The guest hits an upstream failure, handles it, and returns `Ok`.**

- **Backend:** committed once the guest's earlier sends are delivered
  (change 8).
- **Node:** the handler catches the failure and calls `next()`. Committed.
- **Java:** the processor catches the failure and returns. Committed.

**2. The guest returns `Err`, traps, times out, or cannot be started.**

- **Backend:** not committed. The host logs it and counts
  `processing_errors` (change 6), then the backend ends the consumer
  (change 7). Restart, and the record is replayed.
- **Node:** the error escapes the handler, so `next()` is never called and
  the process exits on the unhandled rejection. Restart, and the record is
  replayed.
- **Java:** Streams enters `ERROR` and `MainComponent` restarts it. The
  record is replayed.

### Producing

**3. librdkafka refuses to queue a send (queue full, too large, invalid
partition).**

- **Backend:** one retry on a full queue, then `send` returns `Err` to the
  guest, logged and counted as `publish_refused` (change 5, D4). A guest
  that passes it up gets condition 2. A guest that catches it and returns
  `Ok` is committed, as in Node.
- **Node:** the publish queue drops its oldest message when full, rejecting
  that publish, and the handler catches the rejection and calls `next()`.
- **Java:** the default production handler fails Streams, which restarts.

**4. A queued send is never delivered (no broker acknowledgement within
120 s).**

- **Backend:** logged at `error` with the key and counted as
  `delivery_failures`. The send is treated as delivered, so the input
  record is still committed and the output is lost (change 8).
- **Node:** a failed batch goes back on the queue and is retried until it
  is delivered or dropped from a full queue. A handler awaiting the publish
  does not call `next()` until then.
- **Java:** the default production handler fails Streams, which restarts.

### The consumer

**5. librdkafka reports a fatal consumer error.**

- **Backend:** the consumer ends with the error's detail. Restart
  (change 5, D5).
- **Node:** an activity check every 45 s restarts an inactive consumer in
  the same process.
- **Java:** Streams enters `ERROR` and is restarted.

### Decoding

**6. A record without the Confluent header arrives on a topic that has a
registered schema.**

- **Backend:** delivered to the guest unchanged, with one `warn` per topic.
  The backend does not know the topic has a schema (change 1 and 2); the
  line says only that the topic's payloads are unframed (D1).
- **Node:** decoding fails, so it is logged at `error` and counted as
  `discarded_message_count`. The handler gets an empty value and calls
  `next()`, so it is committed.
- **Java:** only topics configured for the registry are decoded, so this
  topic's records are passed through raw.

**7. The registry cannot be reached when decoding (unreachable, timeout,
401, 403, 429, 5xx).**

- **Backend:** a transport failure is retried twice within the fetch; an
  HTTP error is not. If the fetch fails, every record waiting on it is
  dropped without running a guest, and committed. One `error` line per
  failed fetch with
  the status and body, one per dropped record with its coordinates;
  `discarded_messages{reason="registry_unavailable"}` rises by one per
  record (change 2, D13). A schema that comes back unusable is the same
  path under `reason="invalid_schema"`.
- **Node:** the registry library fetches by schema id, one request per id
  at a time, retrying three times within 5 s and with no request timeout.
  A failure is logged at `error` and counted as
  `discarded_message_count`. The handler gets an empty value and calls
  `next()`, so it is committed.
- **Java:** logged at `error`, not counted, and skipped. Committed.

**8. The registry does not have the schema id in the record's header
(404).**

- **Backend:** dropped without running a guest, and committed. Logged at
  `error` and counted as `discarded_messages{reason="unknown_schema"}`. Not
  cached, so the next record with that id asks again (change 2).
- **Node:** the same path as condition 7. Logged, counted as
  `discarded_message_count`, committed.
- **Java:** logged at `error` and skipped. Committed.

**9. A framed record's payload is not JSON, or fails its schema.**

- **Backend:** validated against the schema named by the record's header.
  On failure it is dropped without running a guest, and committed. Logged
  at `error` with the validation errors and counted as
  `discarded_messages{reason="invalid_payload"}` (change 2, D13).
- **Node:** logged at `error`, counted as `discarded_message_count`, and
  committed.
- **Java:** logged at `error`, not counted, and skipped. Committed.

### Encoding

**10. The topic has no registered subject (404 on `<topic>-value`).**

- **Backend:** sent unframed. The 404 is cached until restart, with one
  `warn` (change 2).
- **Node:** sent unframed, with a `warn`. Cached until restart.
- **Java:** the subject is looked up once at start; it is configured, not
  discovered.

**11. The registry cannot be reached when encoding.**

- **Backend:** not sent; `send` returns `Err`, which the guest handles as
  in condition 3. One `error` line per failed fetch;
  `publish_refused{reason="registry_unavailable"}` rises by one per send.
  Not cached, so the next send to the topic asks again (change 2, D13). A
  schema that comes back unusable is the same path under
  `reason="invalid_schema"`.
- **Node:** the publish fails. Nothing is cached, so the next publish to
  the topic asks again.
- **Java:** the lookup happens once when the processor starts; a failure
  restarts Streams.

**12. The payload sent is not JSON, or fails the topic's schema.**

- **Backend:** not sent; `send` returns `Err`. Logged at `error` with the
  validation errors and counted as
  `publish_refused{reason="invalid_payload"}` (change 2, D13).
- **Node:** the publish fails with "Cannot encode for topic …", not
  counted.
- **Java:** outgoing payloads are not validated.

## Observability

omnia turns tracing fields prefixed `monotonic_counter.` or `gauge.` into
OpenTelemetry instruments; the event's other fields become attributes.

```rust
tracing::info!(monotonic_counter.discarded_messages = 1, topic = %topic, reason = "unknown_schema");
```

### Counters

| counter              | emitted by    | attributes                    | counts                                                             | conditions |
| -------------------- | ------------- | ----------------------------- | ------------------------------------------------------------------ | ---------- |
| `message_counter`    | `omnia`       | `service`                     | every message handed to `handle`, including one with no route      | all        |
| `processing_errors`  | `omnia`       | `service`, `topic`, `error`   | every `Err` from `handle`                                          | 2          |
| `discarded_messages` | `omnia-kafka` | `topic`, `reason`             | a record dropped without running a guest                           | 7, 8, 9    |
| `publish_refused`    | `omnia-kafka` | `topic`, `reason`             | a send that returned `Err` to the guest                            | 3, 11, 12  |
| `delivery_failures`  | `omnia-kafka` | `key`                         | a queued send never delivered within 120 s                         | 4          |

`reason` values:

- `discarded_messages`: `registry_unavailable`, `unknown_schema`,
  `invalid_schema`, `invalid_payload`.
- `publish_refused`: `refused` (librdkafka would not queue it),
  `registry_unavailable`, `invalid_schema`, `invalid_payload`.

A record dropped by the backend never reaches `handle`, so it is in
neither `message_counter` nor `processing_errors`.

### Gauges

Emitted at `trace` from the delivery loop whenever a slot is taken or
returned, like the runtime's pool gauges. They are exported only when the
telemetry filter passes `trace` for `omnia_kafka`.

| gauge             | measures                                                                                              |
| ----------------- | ----------------------------------------------------------------------------------------------------- |
| `kafka_in_flight` | records with the host, out of 64. Pinned at 64 under lag: the slot count is the limit                 |
| `kafka_parked`    | records pulled and waiting behind their key, out of 4096. Climbing while in-flight is low: too few keys |

No error condition moves a gauge directly. A restart resets both to zero.

### Log lines

At `error`:

| line                                            | where              | condition | fields                                    |
| ----------------------------------------------- | ------------------ | --------- | ----------------------------------------- |
| `issue processing message`                      | `omnia`            | 2         | service, topic, the `HandleError`         |
| `failed to deliver message {key}`               | `omnia-kafka`      | 4         | key, librdkafka's error                   |
| `schema {id} fetch failed` / `{subject} fetch failed` | `omnia-kafka` | 7, 8, 11  | once per fetch: status, body              |
| `record {topic}[{partition}]@{offset} dropped`  | `omnia-kafka`      | 7, 8, 9   | once per record: reason, schema id, errors |
| `send to {topic} not encoded`                   | `omnia-kafka`      | 12        | schema id, errors                         |

A consumer that ends (conditions 2 and 5) ends the process with its error
on stderr: `record {topic}[{partition}]@{offset} failed; ending the
consumer`, or `fatal kafka consumer error: …`.

At `warn`: an unframed payload, once per topic (condition 6); a topic with
no subject on encode, once per topic (condition 10); a stored offset
refused; a transient consumer error; the producer flush on shutdown
failing.

## Guest impact

No guest code needs to change and the WIT is untouched. What a guest
author should know:

- **Returning `Err` replays the record.** An `Err`, a trap or a hang
  restarts the process and the record is delivered again. To move past a
  failure, catch it and return `Ok`, as Node's handlers do.
- **`send` can return an error:** librdkafka refused the message, the
  registry could not be reached, or the payload fails the topic's schema.
  Passing it up means condition 2. Catching it and returning `Ok` commits
  the record, as Node's handlers do; `trip-update` should do this in place
  of its `outcome?` in `vp.rs`.
- **A record no guest could read never arrives.** A framed record whose
  schema cannot be fetched, or whose payload is invalid, is dropped, logged
  and counted by the backend.
- **A record can be delivered twice** after a restart or rebalance.
  `trip-update` already discards a position older than the one it holds.
- **The handler's return is the completion.** Work spawned and not awaited
  before returning is abandoned. Await every send before returning.

## Throughput

Sustained rate is the in-flight bound divided by the handler's latency:
at 64 in flight and 20 ms a record, about 3 200 records a second, against
a feed of a few hundred. A hot key is the only serialisation; every other
key proceeds. The tracker adds one counter increment per send and one map
insert and remove per record, and waiting on delivery reports delays the
commit, not the next record. Java runs one record at a time per partition;
Node allows up to 600 handlers per replica, but those are promises waiting
on I/O rather than WASM instances. Measure records per second on the same
topic rather than infer from these numbers.

## Testing

- **`omnia`.** `crates/wasi-messaging/tests/messaging.rs` drives
  `MessagingHandler::handle` with one row per outcome: `Ok`
  (`messaging_produce_handle`), `Rejected` (`messaging_handle_err`),
  `Trapped` (`guest_trap`), `TimedOut` (`messaging_handle_sleep`, with a
  short `Deployment::guest_timeout`), no route (`no_route`) and
  `Unavailable` (`unloadable_guest`). `consumer_end_fails_server` checks a
  `consume` that returns fails the server.
- **`omnia-kafka` unit tests.** The loop, the lanes and the tracker over
  scripted records and a scripted handler; the partitioner; the wire
  format; the single-flight schema cache.
- **`omnia-kafka` live tests** (`#[ignore]`d, recipe in the crate README):
  `keyed_sends`, `registry_wire_format`, `registry_unframed_payload`,
  `registry_id_mismatch` and `at_least_once`, with the test playing the
  host's `Handler`.
- **End to end.** The `runtime` example running `trip-update` over a
  `KafkaMirror` replay, by hand: no pool errors, no out-of-order
  overwrites, committed offsets advancing.

## Deferred

- Encoding by per-topic configuration rather than registry lookup; the
  magic-byte sniff cannot work outbound.
- Picking up a new schema version for encode without a restart.
- Per-topic partition count and partitioner, in place of the global
  `KAFKA_PARTITION_COUNT` and `KAFKA_PARTITIONER`.
- An opt-in `send` that waits for delivery.
- A dead-letter topic, so a dropped record keeps its bytes.
- Pausing one partition, rather than restarting, when a record fails.
- `delivery_failures` carries the record key as an attribute, which is
  unbounded. Replacing it with `topic` (keeping the key in the log line)
  is a small follow-up.
- A streaming consumer interface so one guest instance keeps state across
  records.

## Conventions

`AGENTS.md` governs: run the crate's suite, `mise run lint` and
`mise run fmt-check`; real-service tests are `#[ignore]`d in
`tests/live.rs` with their run recipe in the crate README; comments follow
the register of `crates/cursor/src/{worker,pool,failure}.rs`.
