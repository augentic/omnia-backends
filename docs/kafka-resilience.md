# Kafka backend resilience

Twelve defects in the Kafka consume and produce paths, and the eight changes
that close them. Seven are confined to `omnia-kafka`; one, a few dozen
lines, is in [augentic/omnia](https://github.com/augentic/omnia). None
changes the `wasi:messaging` WIT, so no guest is recompiled.

## Acceptance criteria

1. **At-least-once delivery.** A record is dropped only when the guest
   deliberately drops it (as the legacy processor discards an out-of-date
   position). It is never lost to an infrastructure failure: a full pool, a
   trap, a timeout, a crash, or a deploy.
2. **Processing order per message key.** Records sharing a key are handled
   one at a time, in offset order.
3. **A short replay window.** A crash or restart replays a few hundred
   milliseconds of input, not Kafka Streams' 30 seconds. Committing that
   often must not mean one commit request per record; a handful of
   requests per second per consumer is the ceiling.
4. **Throughput at least matching the legacy services**, and offset
   handling no weaker than any of them. The comparison is in
   [Offsets against legacy](#offsets-against-legacy).

Duplicates on restart are inherent to periodic committing, so criterion 3 is
a bound on the window, not an elimination of it. Guests are expected to
tolerate a replayed record, as the legacy services already have to; the
window here is shorter than any of theirs, so a guest is no worse off.

## Context

The defects were observed on 29 Sep 2026 with this setup: the `runtime`
example running the `trip-update` guest on `omnia` 0.36 and `omnia-kafka`
0.30, against a local Kafka broker. `KafkaMirror` fed the local broker by
copying two production topics (`ppd-realtime-gtfs-vp.v2` and
`ppd-realtime-r9k-station-event.v1`) onto local `dev-` topics. The code
paths involved are unchanged in 0.37 / 0.31.

`KafkaMirror` can feed the local broker in two ways, and each shows a
different class of problem:

- **Replay** (`--from <timestamp>`): the mirror reads historical records
  from production and writes them locally as fast as production returns
  them. The vehicle feed's natural five-second gap between positions is
  lost, so the guest receives a burst of thousands of records at once. This
  overloads the host within seconds and reproduces the load problems (D7,
  D8) on demand.
- **Tail** (no `--from`): the mirror forwards production records as they
  arrive, at the real cadence. Load stays low, and the same races show up
  as occasional single incidents rather than a flood. This is the setting
  for seeing what a normal day looks like.

### The legacy services

Three services in `realtime/` consume and produce against these topics.

|                      | `at_realtime_common` (Node, KafkaJS 1.15 fork)                                                                                                                                                   | `at_schedule_adherence` (Java, Streams 3.6.1)                     | `at_dead_reckoning_adapter` (.NET, Confluent.Kafka) |
| -------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | ----------------------------------------------------------------- | --------------------------------------------------- |
| Concurrency          | up to 12 partitions at once; within each, one batch at a time (the partition is paused until its batch finishes) with up to 50 of the batch's records in flight — up to 600 handlers per replica | one record at a time per partition, one stream thread per replica | strictly sequential                                 |
| Offset eligible when | handler calls `next()`; contiguous prefix only (`lastResolvedOffset`)                                                                                                                            | `process()` returned **and** the producer flushed                 | `ProcessXMessage` returned                          |
| Commit cadence       | runner commit at the end of a later fetch cycle, ≤ ~1 s (`maxWaitTimeInMs: 1000`)                                                                                                                | `commit.interval.ms`, default 30 s                                | synchronous `Commit()` every 200 records            |
| Replay on crash      | ≤ one fetch cycle                                                                                                                                                                                | ≤ 30 s                                                            | ≤ 200 records                                       |
| Failed record        | handler throws → consumer abandoned and restarted                                                                                                                                                | task fails → Streams shuts down → `MainComponent` restarts it     | exception logged, record skipped, offset advances   |
| Publish              | app-level queue, 100 ms flush, drop-oldest at 25 000, 5 retries                                                                                                                                  | Streams record collector, flushed before every commit             | `ProduceAsync` then `Flush(10 s)` per message       |
| Partitioner          | KafkaJS default (f64 murmur2)                                                                                                                                                                    | Java default (`Utils.murmur2`)                                    | KafkaJS murmur2, run as JavaScript under Jint       |

Four things in that table shape the design below.

- **How the Node service commits when records finish out of order.** With
  50 records in flight per partition, a later record can finish before an
  earlier one. Node does not mark the later one as done; it keeps a sorted
  list of finished offsets and only marks the longest run with no gaps,
  starting from the oldest unfinished record. It calls that position
  `lastResolvedOffset`. This design has the same problem — up to 64 records
  are in flight and finish out of order — and change 8 uses the same
  solution under the same name.
- **The Node producer's queue should not be copied.** Node keeps its own
  queue in front of the Kafka client — 25 000 entries, flushed every 100 ms,
  oldest dropped when full, failed batches retried — because KafkaJS has no
  usable producer queue of its own. librdkafka has all of this built in, so
  change 5 does not reproduce it.
- **Java decides per topic whether a payload has a schema header.** The
  station-event topic is wired through `JsonByteDecoder`, which passes bytes
  through untouched; only the vehicle-position topic is decoded as Confluent
  wire format. That is a hard-coded list in the topology. Change 1 gets the
  same result by looking at the payload instead, so there is no list to keep
  up to date.
- **Partition placement must stay compatible with the Node services.** The
  .NET adapter went as far as embedding a JavaScript engine (Jint) to run
  KafkaJS's hash exactly, because any difference would put the same vehicle
  on a different partition from the Node producers. Change 3 therefore keeps
  the KafkaJS scheme as the default and adds the Java one as an option.

On the Node commit cadence: the `commitOffsetsIfNecessary()` call after each
batch does nothing as configured (it needs `autoCommitInterval` or
`autoCommitThreshold`, and neither is set). Offsets are committed by
KafkaJS at the end of its fetch cycle, up to about a second later.

## The defects

| #   | defect                                                       | where it is      | where it is fixed                        | severity          |
| --- | ------------------------------------------------------------ | ---------------- | ---------------------------------------- | ----------------- |
| D1  | Unframed payloads on registry topics are delivered corrupted | `registry.rs`    | `omnia-kafka`                            | data loss, live   |
| D2  | Partitioner disagrees with the Java producers on every key   | `partitioner.rs` | `omnia-kafka`                            | correctness       |
| D3  | The record key is dropped on the inbound path                | `messaging.rs`   | `omnia-kafka`                            | missing data      |
| D4  | A failed publish is reported to the guest as success         | `messaging.rs`   | `omnia-kafka`                            | silent loss       |
| D5  | A dead consumer is never noticed                             | `messaging.rs`   | `omnia-kafka`                            | silent stall      |
| D6  | In-flight producer records are lost on shutdown              | `lib.rs`         | `omnia-kafka`                            | loss on deploy    |
| D7  | Unbounded guest spawning exhausts the pool and drops records | `omnia` server   | `omnia-kafka`, by yielding at most 64    | loss under load   |
| D8  | No ordering: concurrent guests race on shared state          | `omnia` server   | `omnia-kafka`, by yielding one per key   | stale output      |
| D9  | At-most-once consumption: failures are treated as consumed   | `messaging.rs`   | `omnia-kafka`                            | silent loss       |
| D10 | Offsets would commit before the output is durable            | both             | `omnia-kafka`, over the `omnia` ack seam | silent loss       |
| D11 | The schema cache serialises every decode on a cold fetch     | `registry.rs`    | `omnia-kafka`                            | stall             |
| D12 | The guest's returned error is discarded by the host          | `omnia` server   | `omnia`, one line                        | invisible failure |

Each defect below is described the same way: what goes wrong, why the code
does it, and what the effect is.

**D1 — unframed payloads are corrupted.**
The station-event topic (`realtime-r9k-station-event.v1`) has a schema
registered in the Schema Registry, but its producer writes plain JSON with
no Confluent header. The backend assumes that any topic with a registered
schema carries framed payloads, so `Payload::decode` strips the first five
bytes without checking that the first byte is the `0x00` magic byte. The
stripped bytes are no longer valid JSON; the parse fails; and the failure
arm in `Registry::decode` hands the guest the _stripped_ buffer rather than
the original. The guest receives `tionId":...}` instead of
`{"stationId":...}`. Every message on this topic is corrupted today.

**D2 — the partitioner disagrees with the Java producers.**
When a guest sends a keyed message without choosing a partition, the
backend computes the partition itself, using a port of KafkaJS's hash.
KafkaJS does its arithmetic in JavaScript doubles, which lose precision
above 2^53; Kafka's Java client uses wrapping 32-bit integers. The two
hashes give different answers for every key tried (for example
`1039-36302-36840-2-9f138052` goes to partition 7 under KafkaJS and 4 under
Java). `at_schedule_adherence` uses the Java scheme, so a component
replacing it would put the same key on a different partition from the
service it replaces — and from any other Java producer on the topic.

**D3 — the record key is dropped inbound.**
When a record arrives, `from_kafka` copies its headers into the message
metadata and nothing else. The key, partition, offset and timestamp are
discarded. A guest cannot see the key of the record it is handling, even
though the outbound path reads `metadata["key"]` to decide where to send.
The backend also has no key to order on (change 7) and nothing to
acknowledge by (change 8).

**D4 — a failed publish looks like success.**
When librdkafka refuses to enqueue a record (queue full, message too large,
invalid partition), `send` logs the error and returns `Ok(())`. The guest
believes it published.

**D5 — a dead consumer is never noticed.**
The consumer stream yields two kinds of error. Transient ones (a broker
briefly unreachable, a rebalance in progress) librdkafka retries on its own;
the right response is to log and carry on, which `subscribe` does. Fatal
ones — reported as `KafkaError::MessageConsumptionFatal` — mean librdkafka
has given up on this consumer and will never deliver another record.
`subscribe` treats both the same way: it filters the error out with a log
line and keeps polling a stream that will stay empty forever. Nothing
recreates the consumer and nothing ends the process, so the component sits
idle looking healthy. The legacy services make the same transient/fatal
split and restart on the fatal side: Node through a 45-second heartbeat
check, Java when Streams enters its `ERROR` state, .NET by rebuilding the
consumer.

**D6 — the producer's queue is lost on shutdown.**
Records a guest has published sit in librdkafka's in-memory queue until a
broker acknowledges them. Nothing flushes that queue when the process stops:
there is no `Drop` on the client and no shutdown hook. Whatever is queued at
the moment of a deploy is discarded.

**D7 — unbounded spawning exhausts the pool.**
The host's server loop takes each record off the subscription and spawns a
task that instantiates a guest for it, with no limit on how many are in
flight. Wasmtime's pooling allocator has a fixed capacity
(`POOL_TOTAL_CORE_INSTANCES`, default 1000); the 1001st instantiation fails
with an allocator error. By then the record has already been taken off the
consumer, so it is simply lost. Raising the limit moves the ceiling without
creating backpressure. The backend's 1024-slot channel does not help
because the host empties it as fast as it fills.

**D8 — guests race on shared state.**
Kafka delivers records in order within a partition, and the backend
forwards them in that order. The host's server loop then spawns a task per
record, so two records for the same vehicle can be handled by two guest
instances at the same time. Both read the vehicle's state, both compute,
both write; whichever finishes last wins, and that can be the older
position. The order Kafka guaranteed is discarded by the host, not the
backend. The fix (change 7) is nonetheless in the backend: once it knows
when each record is done, it can withhold the next record for a key until
the previous one finishes, and the host never sees two at once. The legacy
services never hit this because they process one record at a time per
partition. On
a mirror replay about 55 such overwrites appeared within 500 ms; on a live
tail they still appear as isolated incidents seconds apart.

**D9 — at-most-once consumption.**
The backend does not configure offset handling, so librdkafka's defaults
apply: an offset is marked as consumed the moment the record is handed to
the application, and marked offsets are committed every five seconds. The
guest runs later, on a spawned task, and its result goes nowhere. If the
guest traps, times out, or is never instantiated at all, the record's offset
has already been marked and it is never seen again. The only redelivery is
a crash inside the five-second window, which is not a retry — it also
redelivers records that succeeded.

**D10 — offsets would commit before the output is durable.**
This one only appears once D9 is fixed. Suppose the guest returns `Ok` and
the host marks the offset. The trip update the guest published may still be
in librdkafka's queue, not yet on the broker. If the process crashes in that
window the output is lost, and the offset has already moved past the input
that would regenerate it. At-least-once consumption is undermined unless
publishing is accounted for before committing.

**D11 — the schema cache stalls every decode.**
`get_schema` takes the cache lock and then makes the HTTP call to the
Schema Registry while still holding it. On a cold cache, every decode in
the process — for every topic — waits behind that one request. Separately,
when the registry answers 404 the code caches `None` and returns an error,
so the first lookup for a schema-less topic logs an error and every later
one silently succeeds with "no schema".

**D12 — the guest's returned error is discarded.**
The guest's `handle` export returns `result<_, error>`. The host receives a
nested result — did the call succeed, and if so what did the guest return —
and `MessagingHandler::handle` maps the whole thing to `()`. A guest that
returns `Err` is indistinguishable from one that returns `Ok`: nothing is
logged and `processing_errors` is not incremented, because that counter
only sees traps, timeouts and load failures. Until this is fixed no failure
policy can tell a successful record from a failed one.

## The changes

### Forces

- **Wire compatibility is preserved by default.** The partitioner scheme
  stays `kafkajs` unless configured. Offset semantics _do_ change for every
  deployment — that is the point of change 8.
- **`omnia` changes are minimal and Kafka-blind.** The host gains one
  seam, the ack token, and nothing that encodes a Kafka policy.
  Concurrency bounds, ordering and offset handling are the backend's, and
  another backend that needs them does the same on its own side. If a
  backend floods the pool, that is that backend's problem.
- **A backend with no use for a capability does not implement it.** NATS
  acquires nothing it has to write.
- **CI cannot stand up a broker, a registry, or a guest pool.** Per
  `AGENTS.md`, real-service tests are `#[ignore]`d. Deterministic cores get
  unit tests; everything else is a live test.
- **Match the legacy services unless there is a reason not to.**
- **Upstream changes are cheap to consume but still cross a repo.** The
  root `Cargo.toml` already patches every `omnia-*` line to the
  `augentic/omnia` git main, so an `omnia` change is picked up without a
  release; see [Working on both repos](#working-on-both-repos).

### 1. Framing — tell a framed payload from an unframed one

_Closes D1. `omnia-kafka` only._

**Outcome.** A plain-JSON station event on a topic with a registered subject
reaches the guest byte-identical. A framed payload is still unwrapped and
validated. No configuration.

**Design.** Sniff the magic byte. The Confluent wire format reserves `0x00`
and no UTF-8 JSON begins with it, so the test is total. Static per-topic
configuration (what Java does) is correct but fails closed when a topic is
forgotten; subject presence (status quo, and what Node does) is wrong
because a registered subject does not imply framed producers. The sniff
cannot work on _encode_, where the backend has to be told which topics are
framed; encode is deferred.

**Changes.**

- `Payload::decode` returns `None` unless `len >= 5 && buffer[0] == 0`,
  without logging at error level.
- Every failure arm in `Registry::decode` returns the original `buffer`,
  never the stripped one.
- Unframed payloads skip validation: there is no schema id to resolve.
- Unit tests over `Payload::decode`: framed round-trips; unframed JSON of
  five bytes or more is `None`; under five bytes is `None`; a non-zero first
  byte is `None` at any length. The end-to-end claim is an `#[ignore]`d live
  test beside `registry_wire_format`.

### 2. Schema cache — stop serialising every decode

_Closes D11. `omnia-kafka` only._

**Changes.**

- Check the cache, drop the guard, fetch, re-lock, insert. Two concurrent
  cold fetches for one topic insert the same value; that is fine. A per-topic
  `OnceCell` coalesces them if it ever matters.
- A 404 is `Ok(None)` in both the cache and the return value.
- Per-message outcomes go to `debug`. One `warn` the first time an unframed
  payload appears on a topic that has a subject.
- Count payloads that are framed and still fail to decode
  (`discarded_messages`, the Node metric's name).

### 3. Partitioner — interoperate with the Java producers

_Closes D2. `omnia-kafka` only._

**Outcome.** A component can be configured to land keys where
`at_schedule_adherence` lands them. Existing deployments are unaffected.

**Design.** `KAFKA_PARTITIONER` with two values, `kafkajs` (default) and
`java`, implemented in Rust beside the existing scheme. Delegating to
librdkafka's `partitioner=murmur2_random` is less code but only verifiable
live, and `AGENTS.md` names the partitioner as deterministic logic that
should have a unit test. A per-topic map is deferred: no component produces to
topics owned by both populations.

The default is right and should not be "fixed": the Node fork's branch is
named `release/3.0.0` but its `package.json` says `1.15.0`, nothing passes
`createPartitioner`, and the .NET adapter embeds a JS engine to match it.

**Changes.**

- The enum on `ConnectOptions`, default `kafkajs`.
- Kafka's `Utils.murmur2` in `partitioner.rs`, wrapping `u32`/`i32`
  throughout: seed `0x9747b28c`, `m = 0x5bd1e995`, `r = 24`; per
  little-endian 4-byte chunk `k *= m; k ^= k >> r; k *= m; h *= m; h ^= k`;
  trailing bytes folded in then `h *= m`; finally
  `h ^= h >> 13; h *= m; h ^= h >> 15`. Partition is
  `(h & 0x7fffffff) % count`.
- `metadata["partition"]` keeps precedence over both schemes.
- Tests: `java_murmur2_vectors` from Kafka's `UtilsTest` (`"21"` →
  `-973932308`, `"foobar"` → `-790332482`, `"a-little-bit-long-string"` →
  `-985981536`, `"a-little-bit-longer-string"` → `-1486304829`, `"abc"` →
  `479470107`,
  `"lkjh234lh9fiuh90y23oiuhsafujhadof229phr9h19h89h8"` → `-58897971`), and
  a `java` table in `partitioning` over 12 partitions, verified
  independently:

  ```
  1039-36302-36840-2-9f138052  4     1182-07205-22440-2-0ad4507d  9
  1388-20023-36900-2-c9985f66  0     1010-98110-23700-2-295bfb4c  10
  1175-03505-36600-2-126dd9ca  1     1137-01302-23820-2-d560d49a  4
  1011-98208-36480-2-9eef750a  8     599999                       3
  233-75504-36900-2-609151bd   10
  ```

- `tests/live.rs::keyed_sends` gains a `java` run; the README configuration
  table gains the variable.

### 4. Keys on the wire — inbound and outbound

*Closes D3. `omnia-kafka` only.*

**Outcome.** A guest can read the key, partition, offset and timestamp of
the record it is handling. A guest that supplies no key produces a genuine
null key, not an empty one. The backend has something to order on (change
7) and something to ack by (change 8).

**Changes.**

- `from_kafka` builds `metadata` unconditionally and populates `key`,
  `partition`, `offset`, `timestamp` alongside the headers.
- `send` emits a null key when the guest supplied none. `.key()` changes
  `BaseRecord`'s key type parameter, so the two arms are different types; a
  helper taking `Option<&[u8]>` is the least painful shape.
- `Tracer::delivery` does `msg.key_view().unwrap().unwrap()` on both arms
  and will panic on librdkafka's callback thread for a null or non-UTF-8
  key. Make both arms tolerant.

### 5. Errors — stop reporting failure as success

*Closes D4, D5, D6. `omnia-kafka` only.*

**Outcome.** A guest learns when its publish was refused. A dead consumer
ends the process instead of logging forever. A deploy does not drop the
producer's queue.

**Design.** `producer.send(record)` returning `Ok` means *enqueued*, not
*delivered*. Propagate the enqueue error: it covers what a guest can act on
(`QueueFull`, oversized, invalid partition). Delivery is change 8's concern
and is tracked there, not awaited in `send`. The Node producer's queue,
flush timer, drop-oldest and retry are not ported — librdkafka provides all
of it (`queue.buffering.max.*`, `linger.ms`, `retries`,
`message.timeout.ms`), and refusing the newest on a full queue is the better
policy because the caller causing the pressure is the one that hears about
it.

A fatal consumer error has one correct answer: end the subscription. The
host then treats a stream that ends as a server error (change 6), the
process exits non-zero, and the orchestrator restarts it — the same
behaviour Node and Java implement in-process, with no in-process restart
code.

**Changes.**

- `send` returns the enqueue error. Count refused publishes
  (`publish_refused`). Optionally one bounded retry on `QueueFull` alone.
- `subscribe` matches `KafkaError::MessageConsumptionFatal`, logs it at
  `error` with the detail from `consumer.client().fatal_error()`, and drops
  the sender so the stream ends. Non-fatal errors stay `warn` and are
  skipped as now.
- `Client` wraps its handles in an `Arc<Inner>` whose `Drop` flushes the
  producer with a bounded timeout (10 s, as the .NET adapter). The consumer
  drops after it; with auto-commit on, `rd_kafka_consumer_close` commits
  stored offsets.

### 6. Acknowledgement — the one `omnia` change

*Closes D12. Enables changes 7 and 8. `omnia`.*

**Outcome.** A backend is told when the host has finished with each
message. The host loop itself is unchanged: it still spawns a task per
message, with no bound and no ordering. A backend that wants either
enforces it on its own side of the seam (change 7 does, for Kafka); a
backend that does not care gets today's behaviour. A guest's returned
`Err` is no longer silently discarded.

**Design.** An optional ack token on `Message`.

| option | verdict |
|---|---|
| `Delivery { message, ack }` as the `Subscriptions` item | changes every backend's `subscribe` |
| A second trait (`AckConsumer`) plus a defaulted accessor and a second subscription type | works; four new items and a branch in `server::run` for one bit of information |
| Defaulted `Client::ack(&self, ..)` method | the host must call it on every exit path; a missed path is a stalled partition, and the message has moved into the store table by the time the guest returns |
| Bounded, ordered dispatch in the host (lanes) | correct and generic, but a larger host change and a Kafka policy decided in `omnia`; the backend can do it itself once it has the token |
| **`pub ack: Option<Arc<dyn Ack>>` on `Message`** | **chosen** |

`Message` is `#[non_exhaustive]` so a field is not a breaking change, and it
already carries a host-side routing concern (`reply`) the guest never sees.
The token's contract is **call = the guest ran, or there was nothing to
run; drop without call = the host never ran a guest for it**. The backend
owns the token type, so it can tell the two apart (`Drop` on its
implementation) and decide what each means for the offset and for what it
yields next.

```rust
/// A backend's hook for learning the host is done with a message.
pub trait Ack: Debug + Send + Sync + 'static {
    /// The host ran the guest for this message, or had no guest to run.
    /// Dropping the token without calling this means it did neither.
    fn ack(&self);
}
```

**When the host acks.**

| outcome of `MessagingHandler::handle` | guest ran? | host                                                           |
| ------------------------------------- | ---------- | -------------------------------------------------------------- |
| guest returned `Ok`                   | yes        | ack                                                            |
| guest returned `Err`                  | yes        | ack; the error is now logged and counted (`processing_errors`) |
| trap                                  | yes        | ack; logged and counted as today                               |
| timeout                               | yes        | ack; logged and counted as today                               |
| no route for topic                    | n/a        | ack (nothing to do)                                            |
| load or instantiate failure           | no         | token dropped un-acked; logged and counted as today            |

If the guest ran, it has had its chance at the record whatever it did with
it; the host does not retry and does not hold anything back. If no guest
ran, the host says so by not acking and otherwise behaves as today; what
to do about it is the backend's decision (change 7).

**Changes — `omnia`.** All in
[`crates/wasi-messaging`](https://github.com/augentic/omnia/tree/main/crates/wasi-messaging);
a few dozen lines in total.

- `src/host/resource.rs`: the `Ack` trait and the `ack` field on `Message`.
  The existing `Clone, Debug, Default` derives still hold: `Arc` clones,
  `Ack: Debug`, and the default is `None`. `src/host.rs` re-exports `Ack`
  beside `Client` and `Message` so a backend can implement it.
- `src/host/server.rs`, `MessagingHandler::handle`: `let ack =
message.ack.take();` as the first line, before the message enters the
  store table. Call it on the no-route return and after the guest call
  resolves — on the `Ok`, `Err`, trap and timeout arms alike — and let it
  drop on the load, export-check, push and instantiate failures above the
  call. The ack lives in `handle`, not the server loop, because only
  `handle` knows whether a guest ran.
- Same function, one line: replace `.map(|_| ())` on the guest call with
  a flatten (`and_then`) so a guest `Err` becomes `handle`'s `Err`. The
  server loop already logs and counts that as `processing_errors`; no new
  counter.
- `server::run`, one line: a subscription stream that ends returns `Err`
  instead of `Ok(())`. A server's subscription never ends on purpose, and
  change 5 relies on this to turn a fatal consumer error into a process
  exit. Today the loop returns `Ok`, the lifecycle treats that as success,
  and if an HTTP server is also running the process stays up with a dead
  consumer.

Not changed: the spawn-per-message loop, `MessagingDefault`, the example,
`Options`.

**Effect on the other backends.** None. `omnia-nats` and `MessagingDefault`
build messages with `Message::new` and field assignment, so they compile
as is with `ack = None`, and the host runs them exactly as today: unbounded
spawning, no ordering. The one visible difference is the stream-end line,
and neither backend's stream ends while the runtime is alive (NATS
reconnects transparently; the default's stream ends only when the default
itself is dropped). The seam is not Kafka-specific: NATS JetStream has
per-message acks and could implement the same trait when that backend grows
a JetStream consumer.

### 7. Bounded, ordered yield

_Closes D7, D8. `omnia-kafka`._

**Outcome.** At most 64 records are with the host at once, so the guest
pool is never exhausted by this backend. Records sharing a key reach the
host one at a time, in offset order. Saturation slows the consumer instead
of losing records. Other backends are untouched; if one of them floods the
pool, that is its problem to solve the same way.

**Design.** The backend already has a forwarding task between librdkafka
and the `Subscriptions` stream the host reads. With the ack token it also
knows when each record it yielded is done. Those two facts are enough to
gate what it yields:

- Keep a count of records yielded and not yet acked. When it reaches 64,
  stop pulling from librdkafka until an ack arrives.
- Keep the set of keys with a record in flight. A record whose key is in
  the set is parked in a per-key queue instead of being yielded; when the
  in-flight record for that key acks, the next parked one is yielded in
  its place. A record with no key is never parked.
- Cap parked records at 4096 in total as a memory ceiling; at that point
  stop pulling as well. The cap must never be what binds: librdkafka queues
  one partition's fetch whole before the next partition's, and on a topic
  with few distinct keys per partition a fetch is a dozen keys repeated
  across hundreds of records, so parked outruns in-flight several times
  over before 64 distinct keys have been seen. Shrink `max.partition.fetch.bytes` from
  its 1 MiB default to 64 KiB so each partition contributes a short run of
  keys before the next is queued, and keep the cap well above a few such
  fetches.

| option                                                            | verdict                                                                                                              |
| ----------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------- |
| Spawn per record (status quo)                                     | the cause of D7 and D8                                                                                               |
| Raise `POOL_TOTAL_CORE_INSTANCES`                                 | moves the ceiling; no backpressure                                                                                   |
| Lanes in the host (previous draft)                                | a larger `omnia` change for a Kafka policy, and a record waits behind unrelated records that hashed to the same lane |
| One record at a time per partition (`pause`/`resume`, as Streams) | ordering for free, but 12 in flight for a 12-partition topic                                                         |
| **Gate in the forwarding task on the ack**                        | **chosen**: no host change beyond the token, waits only behind the same key                                          |
| The guest pulls from a stream                                     | deferred; see [Deferred](#deferred)                                                                                  |

The two guarantees follow directly. Ordering: a key has at most one record
with the host, so its records are handled one at a time, and parked
records leave the queue in the order they arrived, which is offset order
because a key lives on one partition. Bound: 64 records with the host
means at most 64 guests instantiated for this backend, against a pool of
1000 (`POOL_TOTAL_CORE_INSTANCES`) — only a guest taking more than 15 core
instances per instantiation could exhaust it, and nothing here comes near.
Neither number is exposed: the bound is fixed by the pool size, and the
parked cap is a memory bound (a few MiB) sized so that, with 64 KiB
partition fetches, the in-flight bound is reached first.

Backpressure is the forwarding task not polling. When it stops,
`rd_kafka_consumer_poll` stops, librdkafka's prefetch queue fills to its
own bounds (`queued.min.messages` per partition, `queued.max.messages.kbytes`
in total), and the fetch stops. Nothing in flight is lost:
parked and in-flight records are un-acked, and a crash refetches them from
the committed offset.

**What an ack, or its absence, means here.** The host does one of two
things with the token: calls `ack()`, or lets it go out of scope. Either
way the token is eventually destroyed, and the backend owns its type
(`KafkaAck`), so it can give it a `Drop` implementation. `ack()` sets a
flag; `Drop` checks it. Flag set: `ack()` already did the work, `Drop`
does nothing. Flag clear: `ack()` was never called, so `Drop` handles the
"no guest ran" case below. The host sees none of this; its only job is to
call `ack()` when a guest ran.

| host did                                  | backend does                                                                                                                     |
| ----------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------- |
| called `ack()`                            | release the slot and the key; mark the record acked in the tracker (change 8); yield the next parked record for that key, if any |
| dropped the token without calling `ack()` | release the slot and the key; do **not** mark the record; count `unacked_messages`; end the subscription                         |

A token dropped un-acked means the host never ran a guest for the record:
the component cannot be loaded (which will fail for every record) or an
instantiation failed (which cannot be pool exhaustion from this backend).
Ending the subscription makes `server::run` return `Err` (change 6), the
process exits non-zero, and the restart redelivers from the committed
offset. That is Streams' `SHUTDOWN_CLIENT`, and `at_schedule_adherence`
runs it. The instance pool is shared with the other triggers in the
process, so an HTTP burst could in principle take the last slots and make
a messaging instantiate fail transiently; that has not been observed, and
a bounded retry before ending the subscription is the refinement if it
ever is.

**Timeouts.** A hung guest holds its slot and key for `guest_timeout`,
which already exists (`GUEST_TIMEOUT_MS`, default 30 s, the same cap the
HTTP and WebSocket servers apply; `Deployment::guest_timeout` overrides it
per deployment). Keep the default: a hang costs one slot in 64 for 30 s,
and the forwarding task can be stalled at the bound for at most that long,
a tenth of `max.poll.interval.ms` (300 s), after which librdkafka would
leave the group. Only a guest that legitimately runs longer than 30 s per
record is a reason to raise it, and nothing here does.

**Changes — `omnia-kafka`.**

- `dispatch.rs`, beside the `Tracker` in `tracker.rs`: the in-flight count,
  the in-flight key set, the per-key parked queues, and two `const`s,
  `IN_FLIGHT: usize = 64` with a `//` tying it to
  `POOL_TOTAL_CORE_INSTANCES`, and `PARKED: usize = 4096` with a `//`
  saying why it must sit well above a partition fetch. The forwarding
  task consults it before yielding and waits on a `Notify` when either
  bound is hit; `KafkaAck::ack` and `KafkaAck::drop` update it and signal.
  One `Mutex` covers the count, the key set and the queues: the forwarding
  task admitting a new record for key _k_ and an ack releasing _k_ and
  yielding its next parked record race otherwise, and two records for _k_
  reach the host.
- The channel to the host (`subscribe`'s `mpsc`) can stay as it is;
  because the ack path yields the next parked record synchronously, and
  the bound guarantees at most 64 messages are ever outstanding on it,
  `try_send` cannot fail. A debug assertion says so.
- Unit tests over the gate alone: two keys interleaved yield in order with
  the second key never waiting on the first; a keyless record is never
  parked; the 65th record waits for an ack; a dozen keys repeated over
  1200 records park without stopping the pull; a dropped token ends the
  subscription; and, over gate and tracker together, a parked record holds
  the resolved offset back (offsets 3 and 6 acked while 5 is parked behind
  3 stores 3, not 6).
- Live test: part of `at_least_once` (change 8) — the test receives from
  `subscribe` without acking and asserts it sees exactly 64 messages and
  at most one per key, then acks one and sees the next.

### 8. At-least-once offsets

_Closes D9, D10. `omnia-kafka`._

**Outcome.** A record is dropped only when the guest deliberately drops it;
infrastructure failures redeliver it instead of losing it. On a crash, the
consumer replays at most a few hundred milliseconds of input.

**The problem in one paragraph.** Kafka tracks a consumer's progress with
one number per partition, the committed offset, and on restart the consumer
resumes from it. So the only question is: _when is it safe to move that
number past a record?_ The answer is: once the guest has finished with the
record **and** everything the guest published has reached the broker. If
records finished strictly in order, that would be a one-liner. They do
not — up to 64 are with the host at once (change 7) — so the rest of this
section is the bookkeeping needed to answer the question correctly when
they finish out of order.

**Alternatives considered.**

- _Kafka transactions_ (`send_offsets_to_transaction`): would make
  consume-process-produce atomic, but a guest's side effects reach stores
  (Redis, HTTP) that no Kafka transaction covers, the `wasi:messaging`
  interface has no notion of a producer and consumer sharing one, and
  `at_schedule_adherence` itself runs `at_least_once`. Declined.
- _Retry a failed record in place_: a record that always fails would block
  its partition forever. Rejected.
- _Dead-letter topic_: no legacy service runs one. Deferred.

**How librdkafka is configured.** librdkafka separates two steps that are
easy to confuse: _storing_ an offset (telling the client "this record is
done", in memory) and _committing_ (sending stored offsets to the broker).
By default both are automatic, and storing happens the moment a record is
handed to the application — which is D9. The documented fix is to take over
the store and leave the commit alone:

| setting                    | value              | why                                                                                                                                                    |
| -------------------------- | ------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `enable.auto.offset.store` | `false`            | the backend decides when a record is done                                                                                                              |
| `enable.auto.commit`       | `true` (default)   | librdkafka sends stored offsets to the broker on its own thread; one request per interval covering all partitions, however many records were processed |
| `auto.commit.interval.ms`  | `200` (new option) | the replay window on a crash                                                                                                                           |

Two details that bite: `Consumer::store_offset(topic, partition, offset)`
stores `offset + 1` itself, so pass the record's own offset; and librdkafka
only ever commits a _higher_ offset than the last one, so storing offsets
out of order silently loses the lower ones. That second point is why the
tracker exists.

**The tracker, by example.** Partition 3 has records at offsets 100, 101
and 102 (three different keys) with the host at once.

1. 101 finishes first. It cannot be stored: committing 101 would also
   commit 100, which has not finished. 101 is marked done and nothing is
   stored.
2. 100 finishes. Now 100 _and_ 101 are done with nothing unfinished before
   them, so offset 101 is stored. Node calls this position
   `lastResolvedOffset`; this document calls it the **resolved offset**:
   the highest offset with every record before it done.
3. 102 finishes. The resolved offset moves to 102 and is stored.

Now the producer side. When record 100's guest returned, it had called
`send` twice, and those two records may still be in librdkafka's outgoing
queue. Every `send` in the process takes a number from one counter; suppose
the counter stood at 57 when 100's guest returned, so both of its sends are
numbered below 57. Record 100 is not treated as done until every send
numbered below 57 has a delivery report from the broker. That includes
other guests' sends — conservative, but it costs one broker round trip and
never waits on anything that has not been sent yet.

So a record is **done** when its guest has returned _and_ the sends issued
before that return are delivered; the **resolved offset** is the highest
offset with every record before it done; and that is the only offset ever
stored.

**The tracker, as code.** One struct, behind a `Mutex`, touched by three
events.

```rust
struct Pending { acked: bool, sends_before: u64 }

struct Tracker {
    // per (topic, partition): offset → state, in offset order
    partitions: HashMap<(String, i32), BTreeMap<i64, Pending>>,
    // every send numbered below this has its delivery report
    delivered: u64,
    // deliveries that arrived out of order, above `delivered`
    out_of_order: BTreeSet<u64>,
}
```

| event                         | who                | what                                                                                                                                     |
| ----------------------------- | ------------------ | ---------------------------------------------------------------------------------------------------------------------------------------- |
| record pulled from librdkafka | forwarding task    | insert `offset → { acked: false }` **before** the gate decides whether to yield or park it: a parked record must already hold its place, or the resolved offset rolls past it while it waits. Attach a `KafkaAck { tracker, topic, partition, offset }` as `message.ack` when it is yielded |
| host acks the record          | `KafkaAck::ack`    | set `acked = true`, `sends_before =` the send counter's current value; release the record's slot and key (change 7)                      |
| host drops the token un-acked | `KafkaAck::drop`   | leave the entry as it is, so the partition's resolved offset stops before it; release the slot and key; end the subscription (change 7)  |
| broker reports a delivery     | `Tracer::delivery` | advance `delivered` (through `out_of_order` if needed); a `send` that failed to _enqueue_ reports itself here too, or `delivered` stalls |

After either of the last two events, for each partition: pop records from
the front while the front is acked and `sends_before <= delivered`; store
the last offset popped, if any.

On rebalance, a custom `ConsumerContext::pre_rebalance(Revoke)` drops the
revoked partitions from the map; a late ack for one is ignored. librdkafka
commits stored offsets on revoke and on close by itself. Guests already
running for a revoked partition finish and publish; every legacy service
produces that same duplicate on rebalance.

**Why not flush the producer instead.** Kafka Streams achieves "published
before committed" by flushing the producer before each commit, and the
first draft did the same. It cannot work here: `rd_kafka_flush` returns
only when the outgoing queue is _empty_, and with 64 guests publishing
continuously it never is, so the flush would time out every cycle and no
offset would ever be stored under load. The send counter asks a narrower
question — are the sends issued _before this point_ delivered — which is
answered after one round trip no matter how much is being published.

**When a send fails for good.** librdkafka retries a send until
`message.timeout.ms`, then reports failure. That failure is counted
(`delivery_failures`), logged with the key, and treated as "delivered" for
the counter so progress continues. The output is lost; the input offset is
not held back — the same choice the .NET adapter makes, and the only one
consistent with not stalling a partition. Set `message.timeout.ms` to
120 s (a fixed value, not an option), Apache Kafka's own producer default
(`delivery.timeout.ms`), instead of librdkafka's 300 s: long enough
to ride out a leader election or a rolling broker restart, while bounding
how long an unreachable broker stalls commits.

**The replay window.** On a crash the consumer resumes from the last
commit, so it replays up to `auto.commit.interval.ms` (200 ms) of records,
plus the few that were done but waiting on a delivery report or an earlier
record. Node replays up to a fetch cycle (~1 s), Java up to 30 s.

**Changes — `omnia-kafka`.**

- The three settings above; `auto.commit.interval.ms` as
  `KAFKA_COMMIT_INTERVAL_MS` on `ConnectOptions`, default 200. Also
  `max.partition.fetch.bytes` from 1 MiB to 64 KiB (fixed), for the gate
  (change 7). Nothing else in librdkafka's consumer config moves:
  `queued.min.messages` stays at its default because
  `queued.max.messages.kbytes` (64 MiB) already caps the prefetch.
- `tracker.rs`: `Tracker` and `KafkaAck`, with unit tests: out-of-order
  acks store only the resolved offset; a record acked before its sends are
  delivered stores once they are; a revoked partition's late ack is ignored;
  an enqueue failure does not stall `delivered`.
- `send` takes a number from the counter and passes it as the record's
  `DeliveryOpaque` (`usize`, which rdkafka accepts directly — no `Box`);
  `Tracer::delivery` reports it to the tracker. The
  tracker needs the consumer handle to call `store_offset`; a
  `OnceLock<Weak<StreamConsumer>>` set after the consumer is built is the
  simplest way, since the producer is created first.
- `StreamConsumer<Context>` with the rebalance hook.
- Live test, `tests/live.rs::at_least_once`, in the shape of `keyed_sends`:
  no guest and no host loop. The test drives `WasiMessagingCtx` directly,
  playing the host's part by calling each message's `ack` token itself —
  the token is the backend's whole contract with the host, so this is the
  backend's boundary. It produces records on a fresh topic, receives them
  from `subscribe`, acks them out of order, holds one token back un-acked
  (held, not dropped — a dropped token ends the subscription), publishes
  through `send` before acking another, and then reads the group's
  committed offset from an observer consumer (`committed()`). Expected:
  the commit stops just before the held record, moves past it once it is
  acked, and a record acked after a `send` commits only after that send is
  delivered. The same test covers change 7's gate (above). Dropping the
  client and reading `committed()` again covers the flush-on-shutdown path.
  That the host calls `ack` on every outcome where a guest ran is omnia's
  behaviour and is tested there ([Testing the ack
  seam](#testing-the-ack-seam)); the full chain over a real broker is the
  `runtime` example with `trip-update`, run by hand.

**The trade.** Replaying vehicle positions republishes stale trip updates,
and for a realtime feed a stale update can be worse than a missing one. It
is acceptable because the window is a few hundred milliseconds, the guest
already discards positions older than the one it has, and the alternative
is losing records to infrastructure failures.

## A record, end to end

With all eight changes in place:

1. **Backend** — librdkafka fetches the record. The forwarding task
   decodes it (change 1), copies key, partition and offset into metadata
   (change 4), and registers it in the tracker with an ack token attached
   (change 8).
2. **Backend** — if 64 records are already with the host, or another
   record with this key is, the task waits or parks it; nothing further is
   fetched while it waits (change 7). Otherwise it yields the record.
3. **Host** — the server loop spawns a task, which instantiates the guest
   and calls `handle`, exactly as today. Each `send` the guest makes is
   numbered and enqueued; an enqueue failure is returned to the guest
   (changes 5, 8).
4. **Host** — the handler returns, with `Ok`, `Err`, a trap or a timeout.
   `handle` calls `ack()` and returns; the server loop logs and counts
   anything but `Ok`, as today. If no guest ran at all, the token is
   dropped instead (change 6).
5. **Backend** — on ack: release the slot and key and yield the next parked
   record for the key (change 7); mark the record acked, and once the sends
   issued before that ack are delivered and every earlier record in the
   partition is done, store the resolved offset (change 8). On a dropped
   token: end the subscription; the process exits and restarts from the
   committed offset (change 7).
6. **librdkafka** — stored offsets are committed every 200 ms.

## Throughput

Sustained rate is in-flight bound ÷ handler latency. At 64 and 20 ms per
record that is about 3 200 records a second; the feed is a few hundred.
What bounds it:

|                                          | bounds                                                                     |
| ---------------------------------------- | -------------------------------------------------------------------------- |
| 64 in flight (constant)                  | how many guests run at once                                                |
| 4096 parked (constant)                   | memory ceiling on records waiting behind their key; sized never to bind    |
| `guest_timeout` (existing, default 30 s) | how long one hung guest holds its slot and its key                         |

Nothing new is exposed: the pool size and `GUEST_TIMEOUT_MS` keep their
defaults, and the two constants are sized under them.

A hot key is the only serialisation: its records go one at a time, at
1 ÷ latency per second, while every other key proceeds. There is no
head-of-line wait between unrelated keys. Change 8 adds nothing measurable:
one counter increment per send, one map insert and remove per record.
Waiting for delivery reports delays when an offset is _committed_, not
when the next record is _processed_.

Against legacy: `at_schedule_adherence` handles one record at a time per
partition, so 12 partitions is 12 in flight across the whole group; 64 is
more parallel. `at_realtime_common` allows up to 600 per replica, but
those are Node promises waiting on I/O, not WASM instances. The comparison
that matters is records per second on the same topic, which should be
measured rather than inferred from these numbers.

## Offsets against legacy

|                                        | Node                                    | Java                                          | .NET                                      | this design                                                |
| -------------------------------------- | --------------------------------------- | --------------------------------------------- | ----------------------------------------- | ---------------------------------------------------------- |
| A record counts as done when           | its handler called `next()`             | `process()` returned and the producer flushed | the handler returned                      | the guest returned and the sends before that are delivered |
| Out-of-order completion handled        | yes (`lastResolvedOffset`)              | n/a, one at a time                            | no, commits every 200th record regardless | yes (resolved offset)                                      |
| How often offsets are committed        | once per fetch cycle, ≤ ~1 s            | every 30 s                                    | every 200 records                         | every 200 ms                                               |
| Handler failed                         | consumer restarts, record replayed      | task restarts, record replayed                | skipped, offset advances                  | acked, counted, offset advances                            |
| Handler never ran                      | partition stalls until a 5-minute alarm | n/a                                           | n/a                                       | process exits, record replayed on restart                  |
| Output on the broker before the commit | not checked                             | yes                                           | yes                                       | yes                                                        |

Equal to or stronger than each legacy service on every row.

## Guest impact

No guest code needs to change and the WIT is untouched. Four things change
for a guest anyway:

- **`send` can now return an error.** It always had the signature; now a
  failure to enqueue actually comes back (change 5).
- **A record can be delivered twice** after a crash or rebalance. The
  `trip-update` guest already discards a position older than the one it
  holds, which covers this; confirm the same for any other guest (change 8).
- **Returning `Err` is logged and counted, and that is all.** It never
  causes redelivery, so there is no reason to swallow an error into a log
  line (changes 6, 8).
- **The handler's return is the ack.** Work spawned with
  `wit_bindgen::spawn_local` and not awaited before returning is abandoned,
  and the record is acknowledged anyway (changes 6, 8). A Kafka guest
  awaits its sends before returning.

## Implementing

### Working on both repos

The two repositories are checked out side by side as `omnia/` and
`omnia-backends/`, both at workspace version 0.37.0. The root `Cargo.toml`
of `omnia-backends` already has a `[patch.crates-io]` block pointing every
`omnia-*` crate at the `augentic/omnia` git main:

```toml
omnia-wasi-messaging = { git = "https://github.com/augentic/omnia" }
```

The work goes through three states of that block. In every state all
fourteen entries change together: a mix of sources resolves two copies of
`omnia-core` and nothing type-checks across them.

1. **Local, while developing and testing.** Both repos on a feature branch
   (`kafka-improvements` in each). Point the patch at the checkout:

   ```toml
   omnia-wasi-messaging = { path = "../omnia/crates/wasi-messaging" }
   ```

   The crate directory drops the `omnia-` prefix (`omnia` →
   `crates/omnia`, `omnia-core` → `crates/omnia-core`, `omnia-wasi-*` →
   `crates/wasi-*`). `cargo build --all-features` in `omnia-backends` then
   compiles the local `omnia` in place, and an edit in either repo is
   picked up by the next build with no commit. This state is never pushed.

2. **Pushed, for review and CI.** Once the `omnia` branch is pushed, point
   the patch at it:

   ```toml
   omnia-wasi-messaging = { git = "https://github.com/augentic/omnia", branch = "kafka-improvements" }
   ```

   then `cargo update -p omnia-wasi-messaging` (any one `omnia-*` crate
   re-resolves the whole git source) so `Cargo.lock` pins the branch head,
   and `mise run vet-regen` for the new git revision. This is what the
   `omnia-backends` pull request carries while the `omnia` one is open;
   `--locked` and `vet` pass because the lock and the audit name the
   commit. Further `omnia` pushes need the same `cargo update` to be seen.

3. **Merged.** After the `omnia` pull request lands on main, drop
   `branch = ..`, `cargo update -p omnia-wasi-messaging` again, and merge
   `omnia-backends`. The block is back to its present form.

Each transition should show in `git diff Cargo.lock` as the `omnia-*`
entries alone; anything else moving means a `cargo update` without `-p`.

### Counters

omnia emits counters as tracing fields with a `monotonic_counter.` prefix,
which the OpenTelemetry layer turns into instruments:

```rust
tracing::info!(monotonic_counter.publish_refused = 1, topic = %topic);
```

`server.rs` already does this for `message_counter` and
`processing_errors`, and both stay as they are; `processing_errors` simply
starts to include the guest's returned `Err` (change 6). The backend adds
four, in the same form:

| counter              | emitted by                       | counts                                                                             |
| -------------------- | -------------------------------- | ---------------------------------------------------------------------------------- |
| `message_counter`    | `omnia` (existing)               | every record handed to a guest                                                     |
| `processing_errors`  | `omnia` (existing)               | handler returned `Err`, trapped, timed out, or could not be loaded or instantiated |
| `discarded_messages` | `omnia-kafka` decode             | framed payload that failed to decode                                               |
| `publish_refused`    | `omnia-kafka` `send`             | librdkafka refused to enqueue                                                      |
| `delivery_failures`  | `omnia-kafka` `Tracer::delivery` | queued message not delivered within `message.timeout.ms`                           |
| `unacked_messages`   | `omnia-kafka` `KafkaAck::drop`   | token dropped without an ack: the host ran no guest for the record                 |

Two gauges from the gate (`dispatch.rs`), at `trace` level like the
runtime's pool gauges, emitted whenever either changes:

| gauge             | measures                                                                                                      |
| ----------------- | ------------------------------------------------------------------------------------------------------------- |
| `kafka_in_flight` | records with the host, out of `IN_FLIGHT`; pinned there under lag means the slot count is the limit           |
| `kafka_parked`    | records waiting behind a key in flight, out of `PARKED`; climbing while in-flight is low means too few keys |

### Testing the ack seam

The ack is called inside `MessagingHandler::handle`, which omnia's
`crates/wasi-messaging/tests/messaging.rs` already drives directly with a
constructed `Message`. So the test for change 6 is in that file's existing
style, with no server loop and no new guest program: a `RecordingAck`
(an `AtomicBool`, implementing `Ack`) defined in the test file, attached to
the `Message`, and checked after `handle` returns. Rows: guest returns `Ok`
→ acked; guest returns `Err` → acked and `handle` returns `Err`; no route
for the topic → acked; a message routed to a guest that does not exist →
`handle` returns `Err` and the token was **not** acked. The `Ok` row uses
the existing `produce_handle` guest. The `Err` row needs a guest whose
handler returns `Err`, and none exists (`produce_handle` would *trap* on a
wrong payload, which is a different row), so one small program is added to
omnia's `test-programs`: `programs/messaging/handle_err.rs`, returning
`Err(Error::Other(..))` for every message. That is the only
`test-programs` change in either repo. The stream-end line in
`server::run` is one `assert!(run(..).await.is_err())` over a
`MessagingDefault` dropped before the loop starts.

Nothing in `omnia-backends` outside `crates/kafka` changes. The backend's
unit tests cover the gate and the tracker in isolation; its live test
stands in for the host by calling the `ack` token itself (changes 7, 8).

### The crate README

[`crates/kafka/README.md`](../crates/kafka/README.md) today lists the
configuration variables and the live-test recipe and says nothing about
how the backend behaves. It is the one page an operator or guest author
reads, so it is updated in the same pull request as the behaviour, in
the register it already uses: short, factual, no design rationale (that is
this document). Four edits:

**Configuration table.** Two rows added, in the table's existing form:

| Variable                   | Required | Default   | Description                                                                                                                                          |
| -------------------------- | -------- | --------- | ---------------------------------------------------------------------------------------------------------------------------------------------------- |
| `KAFKA_PARTITIONER`        | no       | `kafkajs` | Key-to-partition scheme for sends without `metadata["partition"]`: `kafkajs` matches the Node and .NET producers, `java` matches Kafka's Java client |
| `KAFKA_COMMIT_INTERVAL_MS` | no       | `200`     | How often processed offsets are committed; bounds how much a restart replays                                                                         |

**A `## Behaviour` section** after Configuration, with these subsections.

- _Consuming._ Each record reaches the guest with `metadata` carrying
  `key`, `partition`, `offset`, `timestamp` and the record's headers. A
  payload on a topic with a registered schema is unwrapped and validated
  when it carries the Confluent wire-format header (first byte `0x00`),
  and passed through untouched when it does not. At most 64 records are
  being handled at once. Records sharing a key are handled one at a time
  in offset order; records with different keys run concurrently.
- _Offsets._ Delivery is at-least-once. A record's offset is eligible to
  commit once the guest's handler has returned — with `Ok`, with `Err`,
  by trapping, or by timing out — and every message the guest sent before
  returning has been acknowledged by the broker. Offsets are committed
  every `KAFKA_COMMIT_INTERVAL_MS`, so a crash or redeploy replays at most
  that much input plus the records still waiting on a broker
  acknowledgement; guests must tolerate seeing a record twice. A returned
  `Err` does not cause redelivery; it is logged and counted.
- _Producing._ `send` returns once librdkafka has queued the message; an
  `Err` means it was refused (queue full, too large, invalid partition)
  and nothing was sent. Delivery happens in the background with
  librdkafka's retries, up to 120 s; a message that cannot be delivered in
  that time is logged with its key and counted, and the record that
  produced it is still committed. A message with no `metadata["key"]` is
  sent with a null key. `metadata["partition"]` takes precedence over the
  partitioner.
- _Failure and shutdown._ A fatal consumer error (librdkafka has given up
  on the consumer) ends the subscription and the process exits non-zero
  for the orchestrator to restart; so does a record for which the host
  could not load or instantiate a guest, since every record would fail the
  same way. Transient errors are logged and retried by librdkafka. On
  shutdown the producer is flushed for up to 10 s and the consumer commits
  what it has stored.
- _Counters._ The table from [Counters](#counters), without the "emitted
  by" column.

**Live tests.** The paragraph gains one sentence per new test, in the
style of the existing ones: `keyed_sends` now runs under both partitioner
schemes; `registry_wire_format` is joined by an unframed-payload case; and
`at_least_once` acks received messages in various orders and checks what
is yielded and what is committed. The run recipe is unchanged.

**Nothing else.** No architecture essay, no legacy comparison, no mention
of the defects; those belong here.

## Order of work

All eight changes are built and tested locally, with the path patch from
[Working on both repos](#working-on-both-repos) in place, before anything
is pushed. The local gate is: both crates' suites green, `mise run lint`
and `mise run fmt-check` green in both repos, the `omnia-kafka` live tests
green against the local broker, and the `runtime` example running
`trip-update` over a `KafkaMirror` replay with no pool errors, no
out-of-order overwrites, and committed offsets advancing.

Within that local work the dependencies are:

| order    | repo          | changes                                                             | depends on      |
| -------- | ------------- | ------------------------------------------------------------------- | --------------- |
| any time | `omnia-kafka` | 1–5                                                                 | nothing         |
| first    | `omnia`       | 6                                                                   | nothing         |
| after 6  | `omnia-kafka` | 7 and 8, as one piece: the gate, the tracker, then the send counter | the `Ack` token |

7 and 8 are one piece because the gate's dropped-token policy ends the
subscription, and without the gate an un-acked token from pool exhaustion
would exit the process under load.

Only once the local gate passes: push the `omnia` branch, switch the patch
to `branch = "kafka-improvements"`, bump the lock, push `omnia-backends`,
and open the two pull requests. `omnia` merges first; `omnia-backends`
moves its patch back to main and merges second.

## Deferred

Each item is out of scope now and recorded so it is not rediscovered.

- `encode` framing by per-topic allowlist; the sniff cannot work outbound
  (change 1).
- Schema lookup by the id in the payload rather than the subject's latest
  version (change 2).
- Per-topic partition count and partitioner, in place of the global
  `KAFKA_PARTITION_COUNT` and `KAFKA_PARTITIONER` (change 3).
- An opt-in `send` that waits for delivery (change 5).
- A bounded retry of a dropped token before ending the subscription, if a
  transient instantiate failure is ever observed (change 7).
- A dead-letter topic for trapped and errored records (change 8).
- A streaming consumer interface so one guest instance keeps state across
  records; revisit if a guest is measured rebuilding something expensive per
  record.

## Conventions

`AGENTS.md` governs: run the crate's suite, `mise run lint` and
`mise run fmt-check`; real-service tests are `#[ignore]`d in `tests/live.rs`
with their run recipe in the crate README; comments follow the register of
`crates/cursor/src/{worker,pool,failure}.rs`.
