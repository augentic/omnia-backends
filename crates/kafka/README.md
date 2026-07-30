# omnia-kafka

[![crates.io](https://img.shields.io/crates/v/omnia-kafka.svg)](https://crates.io/crates/omnia-kafka)
[![docs.rs](https://docs.rs/omnia-kafka/badge.svg)](https://docs.rs/omnia-kafka)

Kafka messaging backend for the Omnia WASI runtime, implementing the `wasi-messaging` interface.

Provides a Kafka producer and consumer backed by `rdkafka`, with optional Confluent Schema Registry integration and custom partitioning.

MSRV: Rust 1.95

## Configuration

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `COMPONENT` | no | `omnia` | Client ID prefix; set it for a meaningful per-deployment client ID |
| `KAFKA_BROKERS` | yes | | Comma-separated broker addresses |
| `KAFKA_USERNAME` | no | | SASL username (enables `SASL_SSL`) |
| `KAFKA_PASSWORD` | no | | SASL password |
| `KAFKA_PARTITION_COUNT` | no | `12` | Partition count for custom partitioner |
| `KAFKA_TOPICS` | no | | Comma-separated topics for consumer |
| `KAFKA_CONSUMER_GROUP` | no | `wrt-kafka-consumer` | Consumer group ID |
| `KAFKA_REGISTRY_URL` | no | | Schema Registry URL |
| `KAFKA_REGISTRY_API_KEY` | no | | Schema Registry API key |
| `KAFKA_REGISTRY_API_SECRET` | no | | Schema Registry API secret |
| `KAFKA_REGISTRY_CACHE_TTL` | no | `3600` | Schema cache TTL in seconds |

## Usage

```rust,ignore
use omnia::Backend;
use omnia_kafka::Client;

let options = omnia_kafka::ConnectOptions::from_env()?;
let client = Client::connect_with(options).await?;
```

## Live tests

[`tests/live.rs`](tests/live.rs) exercises the `wasi-messaging` boundary against a
real broker. It is `#[ignore]`d so it never runs in CI; run it explicitly:

```bash
COMPONENT=omnia-live KAFKA_BROKERS=localhost:9092 \
  cargo nextest run -p omnia-kafka --run-ignored all
```

## License

MIT OR Apache-2.0
