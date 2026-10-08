#![doc = include_str!("../README.md")]

mod consume;
mod messaging;
mod partitioner;
mod registry;
mod tracker;

use std::fmt::{self, Debug};
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::time::Duration;

use anyhow::{Context, Result};
use omnia::Backend;
use rand::random_range;
use rdkafka::consumer::Consumer;
use rdkafka::producer::{DeliveryResult, Producer, ProducerContext, ThreadedProducer};
use rdkafka::{ClientConfig, ClientContext, Message as _};
use tracing::instrument;

use crate::consume::{KafkaConsumer, KafkaContext, Shared};
use crate::partitioner::Partitioner;
use crate::registry::Registry;

const DEFAULT_GROUP: &str = "wrt-kafka-consumer";

// Apache Kafka's own producer default (`delivery.timeout.ms`): long enough
// to ride out a leader election or a rolling broker restart. librdkafka's is
// 5 minutes. A record's offset is held until its sends resolve, so this also
// bounds how long an unreachable broker stalls commits.
const MESSAGE_TIMEOUT: &str = "120000";

// How long shutdown waits for queued produces to reach the broker.
const PRODUCER_FLUSH: Duration = Duration::from_secs(10);

/// Kafka backend client with producer, optional consumer, and optional schema registry.
#[derive(Clone)]
pub struct Client {
    inner: Arc<Inner>,
}

// Fields drop in order: the producer flushes first, so a delivery report can
// still store an offset before the consumer's close commits what is stored.
struct Inner {
    producer: ThreadedProducer<Tracer>,
    partitioner: Partitioner,
    registry: Option<Registry>,
    shared: Arc<Shared>,
    consumer: Option<Arc<KafkaConsumer>>,
    consuming: AtomicBool,
}

impl Debug for Client {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("KafkaClient").finish()
    }
}

impl Drop for Inner {
    fn drop(&mut self) {
        if let Err(error) = self.producer.flush(PRODUCER_FLUSH) {
            tracing::warn!("producer flush on shutdown: {error}");
        }
    }
}

impl Backend for Client {
    type ConnectOptions = ConnectOptions;

    #[instrument]
    async fn connect_with(options: Self::ConnectOptions) -> Result<Self> {
        let mut config = ClientConfig::from(&options);
        config.set("message.timeout.ms", MESSAGE_TIMEOUT);

        let shared = Shared::new();
        let producer = config
            .create_with_context(Tracer {
                shared: Arc::clone(&shared),
            })
            .context("issue creating producer")?;

        let partitioner = Partitioner::new(options.partition_count, options.partitioner);
        let registry = options.registry.map(Registry::new);

        let consumer = if let Some(consumer_options) = options.consumer {
            let group_id = consumer_options.group_id.as_deref().unwrap_or(DEFAULT_GROUP);
            config.set("group.id", group_id);
            // the tracker decides when a record is done; librdkafka commits
            // the stored offsets on its own thread
            config.set("enable.auto.offset.store", "false");
            config.set("enable.auto.commit", "true");
            config.set("auto.commit.interval.ms", options.commit_interval_ms.to_string());
            // librdkafka queues a partition's fetch whole before the next
            // partition's. At the 1 MiB default one partition's few repeated
            // keys fill the backlog before any other partition is seen; at
            // 64 KiB each contributes a short run and the keys interleave. A
            // record larger than this is still fetched whole.
            config.set("max.partition.fetch.bytes", "65536");

            let consumer: KafkaConsumer = config
                .create_with_context(KafkaContext::new(Arc::clone(&shared)))
                .context("issue creating consumer")?;

            let topics = consumer_options.topics.iter().map(String::as_str).collect::<Vec<_>>();
            consumer.subscribe(&topics).context("issue subscribing to topics")?;
            tracing::debug!("subscribed to topics: {topics:?}");

            let consumer = Arc::new(consumer);
            shared.set_consumer(&consumer);
            Some(consumer)
        } else {
            None
        };

        Ok(Self {
            inner: Arc::new(Inner {
                producer,
                partitioner,
                registry,
                shared,
                consumer,
                consuming: AtomicBool::new(false),
            }),
        })
    }
}

#[expect(missing_docs, reason = "`FromEnv` has no docs")]
mod config {
    use fromenv::{FromEnv, ParseResult};

    use crate::partitioner::PartitionerScheme;

    /// Connection options for the Kafka backend.
    #[derive(Debug, Clone, FromEnv)]
    pub struct ConnectOptions {
        /// Client identifier prefix (suffixed with a random number). The
        /// runtime no longer writes `COMPONENT` into the process environment,
        /// so set it explicitly for per-deployment client ids.
        #[env(from = "COMPONENT", default = "omnia")]
        pub client_id: String,
        /// Comma-separated broker addresses.
        #[env(from = "KAFKA_BROKERS")]
        pub brokers: String,
        /// SASL username (enables `SASL_SSL` when set with password).
        #[env(from = "KAFKA_USERNAME")]
        pub username: Option<String>,
        /// SASL password.
        #[env(from = "KAFKA_PASSWORD")]
        pub password: Option<String>,
        /// Partition count for custom partitioner.
        #[env(from = "KAFKA_PARTITION_COUNT", default = "12")]
        pub partition_count: i32,
        /// Key-to-partition scheme when a send has no `metadata["partition"]`.
        #[env(from = "KAFKA_PARTITIONER", default = "kafkajs", with = partitioner)]
        pub partitioner: PartitionerScheme,
        /// How often processed offsets are committed, in milliseconds.
        #[env(from = "KAFKA_COMMIT_INTERVAL_MS", default = "200")]
        pub commit_interval_ms: u64,
        /// Optional consumer configuration.
        #[env(nested)]
        pub consumer: Option<ConsumerOptions>,
        /// Optional Schema Registry configuration.
        #[env(nested)]
        pub registry: Option<RegistryOptions>,
    }

    /// Kafka consumer configuration.
    #[derive(Debug, Clone, FromEnv)]
    pub struct ConsumerOptions {
        /// Comma-separated topics to subscribe to.
        #[env(from = "KAFKA_TOPICS", with = split)]
        pub topics: Vec<String>,
        /// Consumer group ID.
        #[env(from = "KAFKA_CONSUMER_GROUP")]
        pub group_id: Option<String>,
    }

    /// Confluent Schema Registry configuration.
    #[derive(Debug, Clone, FromEnv)]
    pub struct RegistryOptions {
        /// Schema Registry URL.
        #[env(from = "KAFKA_REGISTRY_URL")]
        pub url: String,
        /// Schema Registry API key.
        #[env(from = "KAFKA_REGISTRY_API_KEY")]
        pub api_key: String,
        /// Schema Registry API secret.
        #[env(from = "KAFKA_REGISTRY_API_SECRET")]
        pub api_secret: String,
        /// Schema cache TTL in seconds.
        #[env(from = "KAFKA_REGISTRY_CACHE_TTL", default = "3600")]
        pub cache_ttl_secs: u64,
    }

    #[expect(clippy::unnecessary_wraps, reason = "a `FromEnv` `with =` hook returns `ParseResult`")]
    fn split(s: &str) -> ParseResult<Vec<String>> {
        Ok(s.split(',').map(ToOwned::to_owned).collect())
    }

    fn partitioner(value: &str) -> ParseResult<PartitionerScheme> {
        match value {
            "kafkajs" => Ok(PartitionerScheme::KafkaJs),
            "java" => Ok(PartitionerScheme::Java),
            other => Err(UnknownPartitioner(other.to_owned()).into()),
        }
    }

    #[derive(Debug)]
    struct UnknownPartitioner(String);

    impl std::fmt::Display for UnknownPartitioner {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "unknown KAFKA_PARTITIONER `{}`", self.0)
        }
    }

    impl std::error::Error for UnknownPartitioner {}
}
pub use config::{ConnectOptions, ConsumerOptions, RegistryOptions};
pub use partitioner::PartitionerScheme;

impl From<&ConnectOptions> for ClientConfig {
    fn from(kafka: &ConnectOptions) -> Self {
        let mut config = Self::new();

        config.set("client.id", format!("{}-{}", kafka.client_id, random_range(1000..9999)));
        config.set("bootstrap.servers", &kafka.brokers);

        // sasl authentication
        if let Some(user) = &kafka.username
            && let Some(pass) = &kafka.password
        {
            config.set("security.protocol", "SASL_SSL");
            config.set("sasl.mechanisms", "PLAIN");
            config.set("sasl.username", user);
            config.set("sasl.password", pass);
        }

        config
    }
}

impl omnia::FromEnv for ConnectOptions {
    fn load_env() -> Result<Self> {
        Self::from_env().finalize().context("issue loading connection options")
    }
}

/// Kafka producer delivery callback that logs send results.
pub struct Tracer {
    shared: Arc<Shared>,
}

impl ClientContext for Tracer {}

impl ProducerContext for Tracer {
    type DeliveryOpaque = usize;

    fn delivery(
        &self, delivery_result: &DeliveryResult<'_>, delivery_opaque: Self::DeliveryOpaque,
    ) {
        let id = u64::try_from(delivery_opaque).unwrap_or(u64::MAX);
        match delivery_result {
            Ok(message) => {
                let key = record_key(message);
                tracing::debug!(
                    "sent message {key} in offset {offset} of partition {partition}",
                    offset = message.offset(),
                    partition = message.partition()
                );
            }
            Err((error, message)) => {
                let key = record_key(message);
                tracing::error!("failed to deliver message {key}: {error}");
                tracing::info!(monotonic_counter.delivery_failures = 1, key = %key);
            }
        }
        self.shared.note_delivered(id);
    }
}

fn record_key(message: &impl rdkafka::Message) -> String {
    match message.key_view::<str>() {
        Some(Ok(key)) => key.to_owned(),
        Some(Err(_)) => "<non-utf8>".to_owned(),
        None => "<null>".to_owned(),
    }
}
