//! Live tests for the Kafka backend, driven through the `omnia:messaging`
//! host boundary: `WasiMessagingCtx` for the client, `Client::send` to
//! produce, and `Client::consume` with the test playing the host's `Handler`.
//! A raw `rdkafka` consumer observes landed partitions, wire bytes, and the
//! group's committed offsets.
//!
//! `#[ignore]`d so it never touches the network in CI. Run against a reachable
//! broker (`KAFKA_BROKERS`, plus `KAFKA_REGISTRY_URL` for the registry
//! tests): `cargo nextest run -p omnia-kafka --run-ignored all`.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context as _, Result, anyhow};
use futures::FutureExt as _;
use futures::future::BoxFuture;
use omnia::Backend;
use omnia_kafka::{Client, ConnectOptions, ConsumerOptions, PartitionerScheme, RegistryOptions};
use omnia_wasi_messaging::{
    Client as MessagingClient, HandleError, Handler, Message, Metadata, WasiMessagingCtx,
};
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::{ClientConfig, Message as _};
use schema_registry_client::rest::client_config::ClientConfig as RegistryClientConfig;
use schema_registry_client::rest::models::Schema;
use schema_registry_client::rest::schema_registry_client::{Client as _, SchemaRegistryClient};
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;

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

fn options(
    partition_count: i32, partitioner: PartitionerScheme, consumer: Option<ConsumerOptions>,
    registry: Option<RegistryOptions>,
) -> ConnectOptions {
    ConnectOptions {
        client_id: "omnia-live".to_owned(),
        brokers: brokers(),
        username: std::env::var("KAFKA_USERNAME").ok(),
        password: std::env::var("KAFKA_PASSWORD").ok(),
        partition_count,
        partitioner,
        commit_interval_ms: 200,
        consumer,
        registry,
    }
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

// The host's side of the seam. Every delivery reaches the test with the
// sender its outcome goes back through; a delivery the test never decides
// stays with the handler, as a guest that never returns would.
struct Recorder(mpsc::UnboundedSender<Delivery>);

struct Delivery {
    message: Message,
    outcome: oneshot::Sender<Result<(), HandleError>>,
}

impl Handler for Recorder {
    fn handle(&self, message: Message) -> BoxFuture<'static, Result<(), HandleError>> {
        let (outcome, decided) = oneshot::channel();
        let _ = self.0.send(Delivery { message, outcome });
        async move {
            match decided.await {
                Ok(outcome) => outcome,
                Err(_undecided) => std::future::pending().await,
            }
        }
        .boxed()
    }
}

impl Delivery {
    fn metadata(&self, name: &str) -> &str {
        let metadata = self.message.metadata.as_ref().expect("a record carries metadata");
        metadata.get(name).unwrap_or_else(|| panic!("a record carries `{name}`"))
    }

    fn key(&self) -> &str {
        self.metadata("key")
    }

    fn offset(&self) -> i64 {
        self.metadata("offset").parse().expect("offset")
    }

    fn payload(&self) -> &[u8] {
        &self.message.payload
    }

    fn complete(self, outcome: Result<(), HandleError>) -> Message {
        self.outcome.send(outcome).expect("the consumer is waiting on this record");
        self.message
    }
}

// A running `consume` over a `Recorder`. The backend consumer starts from
// the latest offset, so `start` returns once it is assigned and a caller
// can produce.
struct Consuming {
    deliveries: mpsc::UnboundedReceiver<Delivery>,
    task: JoinHandle<Result<()>>,
}

impl Consuming {
    async fn start(client: &Arc<dyn MessagingClient>) -> Self {
        let (recorder, deliveries) = mpsc::unbounded_channel();
        let task = tokio::spawn(client.consume(Arc::new(Recorder(recorder))));
        tokio::time::sleep(Duration::from_secs(8)).await;
        Self { deliveries, task }
    }

    async fn next(&mut self) -> Result<Delivery> {
        tokio::time::timeout(RECV_TIMEOUT, self.deliveries.recv())
            .await
            .context("timed out waiting for a record")?
            .context("the consumer ended")
    }

    // A consumer that has ended drops its handler and closes the channel;
    // that is nothing delivered too.
    async fn nothing(&mut self, within: Duration) -> bool {
        !matches!(tokio::time::timeout(within, self.deliveries.recv()).await, Ok(Some(_)))
    }
}

// Keys and expected partitions come from the `KafkaJS` murmur2 vectors pinned
// in `partitioner.rs` (partition count 12); the broker landing them there
// proves `send` routes through the custom partitioner, not librdkafka's.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "live: needs a reachable Kafka broker (KAFKA_BROKERS); run with --run-ignored"]
async fn keyed_sends() -> Result<()> {
    let kafkajs = vec![
        ("kafkajs-vector-a", vec![("key", "1039-36302-36840-2-9f138052")], 7),
        ("kafkajs-vector-b", vec![("key", "1182-07205-22440-2-0ad4507d")], 3),
        ("kafkajs-vector-c", vec![("key", "599999")], 6),
        // Explicit partition metadata overrides the keyed partitioner.
        ("explicit-override", vec![("key", "599999"), ("partition", "9")], 9),
    ];
    let java = vec![
        ("java-vector-a", vec![("key", "1039-36302-36840-2-9f138052")], 4),
        ("java-vector-b", vec![("key", "1182-07205-22440-2-0ad4507d")], 9),
        ("java-vector-c", vec![("key", "599999")], 3),
        ("explicit-override", vec![("key", "599999"), ("partition", "9")], 9),
    ];
    assert_keyed_sends(PartitionerScheme::KafkaJs, kafkajs).await?;
    assert_keyed_sends(PartitionerScheme::Java, java).await?;
    Ok(())
}

async fn assert_keyed_sends(scheme: PartitionerScheme, cases: Vec<Case>) -> Result<()> {
    let topic = unique("omnia.live.partitions");
    create_topic(&topic, 12).await?;

    let backend = Client::connect_with(options(12, scheme, None, None)).await?;
    let producer: Arc<dyn MessagingClient> = WasiMessagingCtx::connect(&backend).await?;

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

// The test's own registry client, for registering what the backend fetches.
fn registry_client() -> (SchemaRegistryClient, RegistryOptions) {
    let url = std::env::var("KAFKA_REGISTRY_URL")
        .expect("KAFKA_REGISTRY_URL must be set for the registry live tests");
    let api_key = std::env::var("KAFKA_REGISTRY_API_KEY").unwrap_or_default();
    let api_secret = std::env::var("KAFKA_REGISTRY_API_SECRET").unwrap_or_default();

    let mut config = RegistryClientConfig::new(vec![url.clone()]);
    config.basic_auth = Some((api_key.clone(), Some(api_secret.clone())));
    (
        SchemaRegistryClient::new(config),
        RegistryOptions {
            url,
            api_key,
            api_secret,
        },
    )
}

async fn register_schema(
    registry: &SchemaRegistryClient, topic: &str, schema: &str,
) -> Result<i32> {
    let registered = registry
        .register_schema(
            &format!("{topic}-value"),
            &Schema::new(Some("JSON".to_owned()), schema.to_owned()),
            false,
        )
        .await
        .map_err(|e| anyhow!("registering schema: {e:?}"))?;
    registered.id.ok_or_else(|| anyhow!("registered schema has no id"))
}

// Produce past the backend, which would frame a schema-backed topic.
async fn produce_raw(topic: &str, key: &str, payload: &[u8]) -> Result<()> {
    use rdkafka::producer::{FutureProducer, FutureRecord};

    let producer: FutureProducer = raw_config().create().context("creating raw producer")?;
    producer
        .send(FutureRecord::to(topic).payload(payload).key(key), Duration::from_secs(10))
        .await
        .map_err(|(error, _)| anyhow!("raw produce: {error}"))?;
    Ok(())
}

fn framed(schema_id: i32, payload: &[u8]) -> Vec<u8> {
    let mut bytes = vec![0u8];
    bytes.extend(schema_id.to_be_bytes());
    bytes.extend_from_slice(payload);
    bytes
}

// A boundary client over a fresh single-partition topic whose value subject
// carries a permissive JSON schema, already consuming.
struct RegistrySubscriber {
    client: Arc<dyn MessagingClient>,
    consuming: Consuming,
    registry: SchemaRegistryClient,
    topic: String,
    group: String,
    schema_id: i32,
}

impl RegistrySubscriber {
    async fn connect(prefix: &str) -> Result<Self> {
        let topic = unique(prefix);
        create_topic(&topic, 1).await?;

        let (registry, registry_options) = registry_client();
        let schema_id = register_schema(&registry, &topic, r#"{"type":"object"}"#).await?;

        let group = unique("omnia-live-registry");
        let options = options(
            1,
            PartitionerScheme::KafkaJs,
            Some(ConsumerOptions {
                topics: vec![topic.clone()],
                group_id: Some(group.clone()),
            }),
            Some(registry_options),
        );
        let backend = Client::connect_with(options).await?;
        let client: Arc<dyn MessagingClient> = WasiMessagingCtx::connect(&backend).await?;
        let consuming = Consuming::start(&client).await;

        Ok(Self {
            client,
            consuming,
            registry,
            topic,
            group,
            schema_id,
        })
    }

    async fn next(&mut self) -> Result<Delivery> {
        self.consuming.next().await
    }
}

// Registers a JSON schema, sends through the boundary, and asserts both the
// Confluent wire layout on the raw bytes (magic byte + schema id + payload)
// and that the boundary consumer hands back the decoded payload. A record
// framed under an id the registry does not have is dropped and committed
// past, and the record after it still arrives.
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

    // The boundary consumer strips the header on the way back out.
    let received = subscriber.next().await?;
    assert_eq!(received.offset(), 0);
    let received = received.complete(Ok(()));
    assert_eq!(received.payload, payload, "decoded payload round-trips");
    assert_eq!(received.topic, topic, "topic round-trips");

    // a record naming an unknown schema id never reaches the handler; the
    // one behind it does, and the commit moves past both
    produce_raw(&topic, "unknown", &framed(i32::MAX, br#"{"hello":"nobody"}"#)).await?;
    produce_raw(&topic, "known", &framed(subscriber.schema_id, br#"{"hello":"after"}"#)).await?;
    let received = subscriber.next().await?;
    assert_eq!(received.offset(), 2, "the record with the unknown id is dropped");
    assert_eq!(received.payload(), br#"{"hello":"after"}"#);
    received.complete(Ok(()));
    wait_offset(&subscriber.group, &topic, 3).await?;
    Ok(())
}

// A topic with a registered subject whose producer writes plain JSON. The
// guest must see those bytes unchanged.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "live: needs Kafka + Schema Registry (KAFKA_BROKERS, KAFKA_REGISTRY_URL); run with --run-ignored"]
async fn registry_unframed_payload() -> Result<()> {
    let mut subscriber = RegistrySubscriber::connect("omnia.live.unframed").await?;

    let payload = br#"{"stationId":"42"}"#;
    produce_raw(&subscriber.topic, "station", payload).await?;

    let received = subscriber.next().await?.complete(Ok(()));
    assert_eq!(received.payload, payload, "unframed JSON is delivered unchanged");
    Ok(())
}

fn group_offset(group: &str, topic: &str) -> Result<Option<i64>> {
    use rdkafka::consumer::BaseConsumer;
    use rdkafka::topic_partition_list::{Offset, TopicPartitionList};

    let consumer: BaseConsumer = raw_config()
        .set("group.id", group)
        .set("enable.auto.commit", "false")
        .create()
        .context("creating offset reader")?;
    let mut list = TopicPartitionList::new();
    list.add_partition(topic, 0);
    let fetched = consumer
        .committed_offsets(list, Duration::from_secs(10))
        .context("fetching committed offsets")?;
    let element = fetched
        .elements_for_topic(topic)
        .into_iter()
        .next()
        .ok_or_else(|| anyhow!("no committed offset for {topic}"))?;
    element.error().context("committed offset")?;
    Ok(match element.offset() {
        Offset::Offset(offset) => Some(offset),
        Offset::Invalid => None,
        other => return Err(anyhow!("unexpected committed offset {other:?}")),
    })
}

async fn wait_offset(group: &str, topic: &str, expected: i64) -> Result<()> {
    let start = std::time::Instant::now();
    let mut latest = None;
    while start.elapsed() < Duration::from_secs(15) {
        latest = group_offset(group, topic)?;
        if latest == Some(expected) {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    Err(anyhow!("committed offset is {latest:?}, expected {expected}"))
}

// The in-flight bound, per-key order, and at-least-once commits, with the
// test playing the host: it decides each record's outcome itself. A delivery
// report that lands before its offset is stored is `tracker`'s unit tests.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "live: needs a reachable Kafka broker (KAFKA_BROKERS); run with --run-ignored"]
async fn at_least_once() -> Result<()> {
    // matches `consume::IN_FLIGHT`
    const BOUND: usize = 64;

    let topic = unique("omnia.live.offsets");
    create_topic(&topic, 1).await?;
    let group = unique("omnia-live-offsets");
    let backend = Client::connect_with(options(
        1,
        PartitionerScheme::KafkaJs,
        Some(ConsumerOptions {
            topics: vec![topic.clone()],
            group_id: Some(group.clone()),
        }),
        None,
    ))
    .await?;
    let client: Arc<dyn MessagingClient> = WasiMessagingCtx::connect(&backend).await?;
    let mut consuming = Consuming::start(&client).await;

    // 64 distinct keys in flight, a second record for k0 waiting behind the
    // first, and one more record that waits until a slot frees.
    let mut produced = vec![("k0".to_owned(), "p0".to_owned()), ("k0".to_owned(), "p1".to_owned())];
    for index in 1..BOUND {
        produced.push((format!("k{index}"), format!("p{}", index + 1)));
    }
    produced.push(("k64".to_owned(), "p65".to_owned()));
    for (key, payload) in &produced {
        client.send(topic.clone(), keyed_message(payload, &[("key", key)])).await?;
    }

    let mut held = Vec::new();
    for _ in 0..BOUND {
        held.push(consuming.next().await?);
    }
    assert!(
        consuming.nothing(Duration::from_secs(1)).await,
        "the record past the in-flight bound waits for a completion"
    );

    let mut keys = HashMap::<&str, usize>::new();
    for record in &held {
        *keys.entry(record.key()).or_default() += 1;
    }
    assert!(keys.values().all(|count| *count == 1), "at most one record per key: {keys:?}");
    assert!(!held.iter().any(|record| record.payload() == b"p1"));

    // completing k0's head frees p1 behind it; a second completion frees a
    // slot for the record past the bound
    let index = held.iter().position(|record| record.payload() == b"p0").context("missing p0")?;
    let first = held.swap_remove(index);
    assert_eq!(first.offset(), 0);
    first.complete(Ok(()));
    held.pop().expect("a held record").complete(Ok(()));

    let mut freed = [consuming.next().await?, consuming.next().await?];
    freed.sort_by_key(Delivery::offset);
    assert_eq!(freed[0].payload(), b"p1", "k0's second record follows its first");
    assert_eq!(freed[0].key(), "k0");
    assert_eq!(freed[1].payload(), b"p65", "the record past the bound takes the freed slot");
    held.extend(freed);

    for record in held {
        record.complete(Ok(()));
    }
    let next_offset = i64::try_from(produced.len()).expect("produced");
    wait_offset(&group, &topic, next_offset).await?;

    // Five further records. Hold the second back, complete a later one first,
    // and publish once more before completing the record that should wait on
    // that send.
    let phase: Vec<(&str, &str)> =
        vec![("a", "a0"), ("b", "b0"), ("c", "c0"), ("d", "d0"), ("e", "e0")];
    for (key, payload) in &phase {
        client.send(topic.clone(), keyed_message(payload, &[("key", key)])).await?;
    }
    let mut phase_held = Vec::new();
    for _ in 0..phase.len() {
        phase_held.push(consuming.next().await?);
    }
    phase_held.sort_by_key(Delivery::offset);
    assert_eq!(phase_held[0].offset(), next_offset);
    let [a0, b0, c0, d0, e0] = phase_held.try_into().ok().context("five records")?;
    let (offset_b, offset_c, offset_e) = (b0.offset(), c0.offset(), e0.offset());

    d0.complete(Ok(()));
    a0.complete(Ok(()));
    wait_offset(&group, &topic, offset_b).await?;

    b0.complete(Ok(()));
    wait_offset(&group, &topic, offset_c).await?;

    client.send(topic.clone(), keyed_message("side", &[("key", "side")])).await?;
    let side = consuming.next().await?;
    assert_eq!(side.key(), "side");
    c0.complete(Ok(()));
    wait_offset(&group, &topic, offset_e).await?;

    e0.complete(Ok(()));
    wait_offset(&group, &topic, side.offset()).await?;
    let tail = side.offset() + 1;
    side.complete(Ok(()));
    wait_offset(&group, &topic, tail).await?;

    // a record the guest rejects ends the consumer with its offset uncommitted
    client.send(topic.clone(), keyed_message("f0", &[("key", "f")])).await?;
    let rejected = consuming.next().await?;
    assert_eq!(rejected.offset(), tail);
    rejected.complete(Err(HandleError::Rejected(omnia_wasi_messaging::Error::Other(
        "rejected f".to_owned(),
    ))));
    let ended = (&mut consuming.task).await.context("the consume task joins")?;
    let error = ended.expect_err("a rejected record ends the consumer");
    assert!(
        format!("{error:#}").contains(&format!("{topic}[0]@{tail}")),
        "unexpected error: {error:#}"
    );
    assert!(consuming.nothing(Duration::from_millis(100)).await);

    drop(client);
    drop(backend);
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert_eq!(group_offset(&group, &topic)?, Some(tail), "shutdown keeps the stored offset");
    Ok(())
}

// A record is validated against the schema its header names, not the
// subject's latest: one written under the earlier, permissive version still
// arrives after a stricter version is registered, and one framed under the
// stricter id that fails it is dropped and committed past. The subject's
// compatibility is set to `NONE` first, since under `BACKWARD` no payload
// can be valid under the old schema and invalid under the new.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "live: needs Kafka + Schema Registry (KAFKA_BROKERS, KAFKA_REGISTRY_URL); run with --run-ignored"]
async fn registry_id_mismatch() -> Result<()> {
    use schema_registry_client::rest::models::{CompatibilityLevel, ServerConfig};

    let mut subscriber = RegistrySubscriber::connect("omnia.live.registry-mismatch").await?;
    let topic = subscriber.topic.clone();

    let mut compatibility = ServerConfig::new();
    compatibility.compatibility = Some(CompatibilityLevel::None);
    subscriber
        .registry
        .update_config(&format!("{topic}-value"), &compatibility)
        .await
        .map_err(|e| anyhow!("setting compatibility: {e:?}"))?;
    let stricter =
        register_schema(&subscriber.registry, &topic, r#"{"type":"object","required":["v2"]}"#)
            .await?;
    assert_ne!(stricter, subscriber.schema_id);

    let payload = br#"{"hello":"stale"}"#;
    produce_raw(&topic, "invalid", &framed(stricter, payload)).await?;
    produce_raw(&topic, "stale", &framed(subscriber.schema_id, payload)).await?;

    let received = subscriber.next().await?;
    assert_eq!(received.offset(), 1, "the record failing its own schema is dropped");
    assert_eq!(received.payload(), payload, "the header is stripped under the older id");
    received.complete(Ok(()));
    wait_offset(&subscriber.group, &topic, 2).await?;
    Ok(())
}
