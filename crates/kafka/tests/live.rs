//! Live tests for the Kafka backend, driven through the `omnia:messaging`
//! host boundary (`WasiMessagingCtx` + the `Client` producer/consumer proxy).
//! A raw `rdkafka` consumer observes landed partitions and wire bytes, since
//! the boundary `Message` deliberately does not expose them.
//!
//! `#[ignore]`d so it never touches the network in CI. Run against a reachable
//! broker (`KAFKA_BROKERS`, plus `KAFKA_REGISTRY_URL` for the registry
//! tests): `cargo nextest run -p omnia-kafka --run-ignored all`.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context as _, Result, anyhow};
use futures::StreamExt;
use omnia::Backend;
use omnia_kafka::{Client, ConnectOptions, ConsumerOptions, RegistryOptions};
use omnia_wasi_messaging::{
    Client as MessagingClient, Message, Metadata, Subscriptions, WasiMessagingCtx,
};
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::{ClientConfig, Message as _};

const RECV_TIMEOUT: Duration = Duration::from_mins(1);

// A produce case: payload, message metadata, and the expected partition.
type Case = (&'static str, Vec<(&'static str, &'static str)>, i32);

fn brokers() -> String {
    std::env::var("KAFKA_BROKERS").expect("KAFKA_BROKERS must be set for live tests")
}

fn unique(prefix: &str) -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock")
        .as_nanos();
    format!("{prefix}.{nanos}")
}

// Raw rdkafka config mirroring the backend's broker/SASL settings.
fn raw_config() -> ClientConfig {
    let mut config = ClientConfig::new();
    config.set("bootstrap.servers", brokers());
    if let (Ok(user), Ok(pass)) = (std::env::var("KAFKA_USERNAME"), std::env::var("KAFKA_PASSWORD"))
    {
        config.set("security.protocol", "SASL_SSL");
        config.set("sasl.mechanisms", "PLAIN");
        config.set("sasl.username", user);
        config.set("sasl.password", pass);
    }
    config
}

async fn create_topic(topic: &str, partitions: i32) -> Result<()> {
    let admin: AdminClient<DefaultClientContext> =
        raw_config().create().context("creating admin client")?;
    let results = admin
        .create_topics(
            &[NewTopic::new(topic, partitions, TopicReplication::Fixed(1))],
            &AdminOptions::new(),
        )
        .await
        .context("create_topics request")?;
    for result in results {
        result.map_err(|(name, err)| anyhow!("creating topic {name}: {err}"))?;
    }
    Ok(())
}

fn observer(topic: &str) -> Result<StreamConsumer> {
    let consumer: StreamConsumer = raw_config()
        .set("group.id", unique("omnia-live-observer"))
        .set("auto.offset.reset", "earliest")
        .set("enable.auto.commit", "false")
        .create()
        .context("creating observer consumer")?;
    consumer.subscribe(&[topic]).context("observer subscribe")?;
    Ok(consumer)
}

fn keyed_message(payload: &str, metadata: &[(&str, &str)]) -> Message {
    let mut message = Message::new(payload.as_bytes().to_vec());
    let mut md = Metadata::new();
    for (k, v) in metadata {
        md.inner.insert((*k).to_owned(), (*v).to_owned());
    }
    message.metadata = Some(md);
    message
}

// Keys and expected partitions come from the `KafkaJS` murmur2 vectors pinned
// in `partitioner.rs` (partition count 12); the broker landing them there
// proves `send` routes through the custom partitioner, not librdkafka's.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "live: needs a reachable Kafka broker (KAFKA_BROKERS); run with --run-ignored"]
async fn keyed_sends() -> Result<()> {
    let topic = unique("omnia.live.partitions");
    create_topic(&topic, 12).await?;

    let options = ConnectOptions {
        client_id: "omnia-live".to_owned(),
        brokers: brokers(),
        username: std::env::var("KAFKA_USERNAME").ok(),
        password: std::env::var("KAFKA_PASSWORD").ok(),
        partition_count: 12,
        consumer: None,
        registry: None,
    };
    let backend = Client::connect_with(options).await?;
    let producer: Arc<dyn MessagingClient> = WasiMessagingCtx::connect(&backend).await?;

    let cases: Vec<Case> = vec![
        ("kafkajs-vector-a", vec![("key", "1039-36302-36840-2-9f138052")], 7),
        ("kafkajs-vector-b", vec![("key", "1182-07205-22440-2-0ad4507d")], 3),
        ("kafkajs-vector-c", vec![("key", "599999")], 6),
        // Explicit partition metadata overrides the keyed partitioner.
        ("explicit-override", vec![("key", "599999"), ("partition", "9")], 9),
    ];

    for (payload, metadata, _) in &cases {
        producer.send(topic.clone(), keyed_message(payload, metadata)).await?;
    }

    let consumer = observer(&topic)?;
    let mut landed: HashMap<String, i32> = HashMap::new();
    while landed.len() < cases.len() {
        let msg = tokio::time::timeout(RECV_TIMEOUT, consumer.recv())
            .await
            .context("timed out waiting for produced messages")?
            .context("observer recv")?;
        let payload = String::from_utf8(msg.payload().unwrap_or_default().to_vec())?;
        landed.insert(payload, msg.partition());
    }

    for (payload, _, expected) in &cases {
        assert_eq!(
            landed.get(*payload),
            Some(expected),
            "'{payload}' should land on partition {expected}: {landed:?}"
        );
    }
    Ok(())
}

// A boundary client over a fresh single-partition topic whose value subject
// carries a permissive JSON schema, already subscribed: the backend consumer
// starts from the latest offset, so it is assigned before a caller produces.
struct RegistrySubscriber {
    client: Arc<dyn MessagingClient>,
    subscription: Subscriptions,
    topic: String,
    schema_id: i32,
}

impl RegistrySubscriber {
    async fn connect(prefix: &str) -> Result<Self> {
        use schema_registry_client::rest::client_config::ClientConfig as RegistryClientConfig;
        use schema_registry_client::rest::models::Schema;
        use schema_registry_client::rest::schema_registry_client::{
            Client as _, SchemaRegistryClient,
        };

        let url = std::env::var("KAFKA_REGISTRY_URL")
            .expect("KAFKA_REGISTRY_URL must be set for the registry live tests");
        let api_key = std::env::var("KAFKA_REGISTRY_API_KEY").unwrap_or_default();
        let api_secret = std::env::var("KAFKA_REGISTRY_API_SECRET").unwrap_or_default();

        let topic = unique(prefix);
        create_topic(&topic, 1).await?;

        let mut registry_config = RegistryClientConfig::new(vec![url.clone()]);
        registry_config.basic_auth = Some((api_key.clone(), Some(api_secret.clone())));
        let registry = SchemaRegistryClient::new(registry_config);
        let registered = registry
            .register_schema(
                &format!("{topic}-value"),
                &Schema::new(Some("JSON".to_owned()), r#"{"type":"object"}"#.to_owned()),
                false,
            )
            .await
            .map_err(|e| anyhow!("registering schema: {e:?}"))?;
        let schema_id = registered.id.ok_or_else(|| anyhow!("registered schema has no id"))?;

        let options = ConnectOptions {
            client_id: "omnia-live".to_owned(),
            brokers: brokers(),
            username: std::env::var("KAFKA_USERNAME").ok(),
            password: std::env::var("KAFKA_PASSWORD").ok(),
            partition_count: 1,
            consumer: Some(ConsumerOptions {
                topics: vec![topic.clone()],
                group_id: Some(unique("omnia-live-registry")),
            }),
            registry: Some(RegistryOptions {
                url,
                api_key,
                api_secret,
                cache_ttl_secs: 3600,
            }),
        };
        let backend = Client::connect_with(options).await?;
        let client: Arc<dyn MessagingClient> = WasiMessagingCtx::connect(&backend).await?;
        let subscription = client.subscribe().await?;
        tokio::time::sleep(Duration::from_secs(8)).await;

        Ok(Self {
            client,
            subscription,
            topic,
            schema_id,
        })
    }

    async fn next(&mut self) -> Result<Message> {
        tokio::time::timeout(RECV_TIMEOUT, self.subscription.next())
            .await
            .context("timed out waiting for boundary message")?
            .ok_or_else(|| anyhow!("subscription closed"))
    }
}

// Registers a JSON schema, sends through the boundary, and asserts both the
// Confluent wire layout on the raw bytes (magic byte + schema id + payload)
// and that the boundary subscriber hands back the decoded payload.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "live: needs Kafka + Schema Registry (KAFKA_BROKERS, KAFKA_REGISTRY_URL); run with --run-ignored"]
async fn registry_wire_format() -> Result<()> {
    let mut subscriber = RegistrySubscriber::connect("omnia.live.registry").await?;
    let topic = subscriber.topic.clone();

    let payload = br#"{"hello":"wire"}"#;
    let json = String::from_utf8(payload.to_vec())?;
    subscriber.client.send(topic.clone(), keyed_message(&json, &[("key", "wire")])).await?;

    // Raw bytes carry the Confluent wire format: magic byte 0, schema id
    // big-endian, then the JSON payload.
    let raw = observer(&topic)?;
    let msg = tokio::time::timeout(RECV_TIMEOUT, raw.recv())
        .await
        .context("timed out waiting for raw wire message")?
        .context("raw recv")?;
    let bytes = msg.payload().unwrap_or_default();
    assert!(bytes.len() > 5, "wire payload has the 5-byte header: {bytes:?}");
    assert_eq!(bytes[0], 0, "magic byte");
    let wire_id = i32::from_be_bytes([bytes[1], bytes[2], bytes[3], bytes[4]]);
    assert_eq!(wire_id, subscriber.schema_id, "schema id in wire header");
    assert_eq!(&bytes[5..], payload, "payload follows the header");

    // The boundary subscriber strips the header on the way back out.
    let received = subscriber.next().await?;
    assert_eq!(received.payload, payload, "decoded payload round-trips");
    assert_eq!(received.topic, topic, "topic round-trips");
    Ok(())
}

// A message framed under a schema id other than the topic's latest, as a
// producer still on an earlier version writes, reaches the subscriber with
// its header stripped: the mismatch is logged, never a dropped message.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "live: needs Kafka + Schema Registry (KAFKA_BROKERS, KAFKA_REGISTRY_URL); run with --run-ignored"]
async fn registry_id_mismatch() -> Result<()> {
    use rdkafka::producer::{FutureProducer, FutureRecord};

    let mut subscriber = RegistrySubscriber::connect("omnia.live.registry-mismatch").await?;

    let payload = br#"{"hello":"stale"}"#;
    let mut framed = vec![0u8];
    framed.extend(subscriber.schema_id.wrapping_add(1).to_be_bytes());
    framed.extend_from_slice(payload);

    let producer: FutureProducer = raw_config().create().context("creating raw producer")?;
    producer
        .send(
            FutureRecord::to(&subscriber.topic).key("stale").payload(&framed),
            Duration::from_secs(10),
        )
        .await
        .map_err(|(err, _)| anyhow!("raw send: {err}"))?;

    let received = subscriber.next().await?;
    assert_eq!(received.payload, payload, "the header is stripped despite the foreign id");
    Ok(())
}
