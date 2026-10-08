# omnia-kafka

[![crates.io](https://img.shields.io/crates/v/omnia-kafka.svg)](https://crates.io/crates/omnia-kafka)
[![docs.rs](https://docs.rs/omnia-kafka/badge.svg)](https://docs.rs/omnia-kafka)

Kafka messaging backend for the Omnia WASI runtime, implementing the `wasi-messaging` interface.

Provides a Kafka producer and consumer backed by `rdkafka`, with optional Confluent Schema Registry integration and custom partitioning.

MSRV: Rust 1.99

## Configuration

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `COMPONENT` | no | `omnia` | Client ID prefix; set it for a meaningful per-deployment client ID |
| `KAFKA_BROKERS` | yes | | Comma-separated broker addresses |
| `KAFKA_USERNAME` | no | | SASL username (enables `SASL_SSL`) |
| `KAFKA_PASSWORD` | no | | SASL password |
| `KAFKA_PARTITION_COUNT` | no | `12` | Partition count for custom partitioner |
| `KAFKA_PARTITIONER` | no | `kafkajs` | Key-to-partition scheme for sends without `metadata["partition"]`: `kafkajs` matches the Node and .NET producers, `java` matches Kafka's Java client |
| `KAFKA_TOPICS` | no | | Comma-separated topics for consumer |
| `KAFKA_CONSUMER_GROUP` | no | `wrt-kafka-consumer` | Consumer group ID |
| `KAFKA_COMMIT_INTERVAL_MS` | no | `200` | How often processed offsets are committed; bounds how much a restart replays |
| `KAFKA_REGISTRY_URL` | no | | Schema Registry URL |
| `KAFKA_REGISTRY_API_KEY` | no | | Schema Registry API key |
| `KAFKA_REGISTRY_API_SECRET` | no | | Schema Registry API secret |
| `KAFKA_REGISTRY_CACHE_TTL` | no | `3600` | Schema cache TTL in seconds |

## Behaviour

### Consuming

Each record reaches the guest with `metadata` carrying `key`, `partition`, `offset`, `timestamp` and the record's headers. A payload on a topic with a registered schema is unwrapped and validated when it carries the Confluent wire-format header (first byte `0x00`), and passed through untouched when it does not. At most 64 records are being handled at once. Records sharing a key are handled one at a time in offset order; records with different keys run concurrently.

### Offsets

Delivery is at-least-once. A record's offset is eligible to commit once the guest's handler has returned `Ok` and every message the guest sent before returning has been acknowledged by the broker. Offsets are committed every `KAFKA_COMMIT_INTERVAL_MS`, so a crash or redeploy replays at most that much input plus the records still waiting on a broker acknowledgement; guests must tolerate seeing a record twice.

### Producing

`send` returns once librdkafka has queued the message; an `Err` means it was refused (queue full, too large, invalid partition) and nothing was sent. Delivery happens in the background with librdkafka's retries, up to 120 s; a message that cannot be delivered in that time is logged with its key and counted, and the record that produced it is still committed. A message with no `metadata["key"]` is sent with a null key. `metadata["partition"]` takes precedence over the partitioner. `request` is not supported and returns an error.

### Failure and shutdown

A record the guest does not handle — its handler returned `Err`, trapped, or timed out, or the host could not load or instantiate a guest for it — ends the consumer, and the process exits non-zero for the orchestrator to restart. The record's offset was never stored, so the restart redelivers it, and everything after it, from the committed offset; a record that fails every time blocks its partition until the guest is fixed. A fatal consumer error (librdkafka has given up on the consumer) ends the consumer the same way. Transient errors are logged and retried by librdkafka. On shutdown the producer is flushed for up to 10 s and the consumer commits what it has stored.

### Counters

| Counter | Counts |
|---------|--------|
| `discarded_messages` | Framed payload that failed to decode |
| `publish_refused` | librdkafka refused to enqueue |
| `delivery_failures` | Queued message not delivered within 120 s |

### Gauges

Emitted at `trace` level, like the runtime's pool gauges, whenever either changes.

| Gauge | Measures |
|-------|----------|
| `kafka_in_flight` | Records with the host, out of 64. Pinned at 64 under lag: the slot count is the limit |
| `kafka_parked` | Records pulled and waiting behind a key already in flight, out of a 4096-record backlog. Climbing while `kafka_in_flight` is low: fewer distinct keys than slots |

## Usage

Bind the backend in your host's `runtime!` map — the guest `.wasm` is untouched
(see the [Production Backends guide](https://github.com/augentic/omnia/blob/main/docs/guides/production-backends.md)):

```rust,ignore
use omnia_kafka::Client as Kafka;
use omnia_wasi_messaging::WasiMessaging;

omnia::runtime!({
    hosts: {
        WasiMessaging: Kafka,
    }
});
```

For direct or embedded use, connect it yourself:

```rust,ignore
use omnia::{Backend, FromEnv};
use omnia_kafka::Client;

let options = omnia_kafka::ConnectOptions::load_env()?;
let client = Client::connect_with(options).await?;
```

## Live tests

[`tests/live.rs`](tests/live.rs) exercises the `wasi-messaging` boundary against a
real broker, with the test playing the host's `Handler`: keyed sends must
land on the partitions the configured partitioner predicts, under both the
`kafkajs` and `java` schemes, and (when a Schema Registry is reachable)
framed sends must carry the Confluent wire format and decode back through
`consume`, while an unframed payload on a topic that has a schema is
delivered unchanged and a message framed under a schema id other than the
topic's latest still decodes (the mismatch is only logged). `at_least_once`
completes delivered records in various orders and checks what is delivered
and what is committed, then fails one record and checks that the consumer
ends with the commit stopped before it. The tests are `#[ignore]`d so they
never run in CI; run them explicitly:

```bash
# One container provides both the broker and a schema registry:
docker run -d --name redpanda -p 9092:9092 -p 8081:8081 \
  redpandadata/redpanda:latest redpanda start --mode dev-container --smp 1 \
  --kafka-addr PLAINTEXT://0.0.0.0:9092 \
  --advertise-kafka-addr PLAINTEXT://localhost:9092 \
  --schema-registry-addr 0.0.0.0:8081

KAFKA_BROKERS=localhost:9092 KAFKA_REGISTRY_URL=http://localhost:8081 \
  cargo nextest run -p omnia-kafka --run-ignored all
```

## License

MIT OR Apache-2.0
