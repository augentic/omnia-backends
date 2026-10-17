#![doc = include_str!("../README.md")]

mod consume;
mod messaging;
mod partitioner;
mod registry;
mod tracker;

use std::borrow::Cow;
use std::fmt::{self, Debug};
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::time::Duration;

use anyhow::{Context, Result, ensure};
use omnia::Backend;
use rand::random_range;
use rdkafka::consumer::Consumer;
use rdkafka::producer::{DeliveryResult, Producer, ProducerContext, ThreadedProducer};
use rdkafka::{ClientConfig, ClientContext, Message as _};
use tracing::instrument;

use crate::consume::{KafkaConsumer, KafkaContext, Shared};
use crate::partitioner::Partitioner;
use crate::registry::Registry;

// How long a produce retries before failing and releasing any offset waiting on it.
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
    producer: ThreadedProducer<DeliveryContext>,
    partitioner: Partitioner,
    registry: Option<Registry>,
    shared: Arc<Shared>,
    consumer: Option<Arc<KafkaConsumer>>,
    consuming: AtomicBool,
}

impl Debug for Client {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Client").finish_non_exhaustive()
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
        ensure!(
            options.username.is_some() == options.password.is_some(),
            "KAFKA_USERNAME and KAFKA_PASSWORD must be set together"
        );

        let mut config = ClientConfig::from(&options);

        let shared = Shared::new();
        let producer = config
            .clone()
            .set("message.timeout.ms", MESSAGE_TIMEOUT)
            .create_with_context(DeliveryContext {
                shared: Arc::clone(&shared),
            })
            .context("issue creating producer")?;

        let partitioner = Partitioner::new(options.partition_count, options.partitioner);
        let registry = options.registry.map(Registry::new).transpose()?;

        let consumer = if let Some(consumer_options) = options.consumer {
            ensure!(
                !consumer_options.topics.is_empty()
                    && consumer_options.topics.iter().all(|topic| !topic.trim().is_empty()),
                "KAFKA_TOPICS must contain at least one non-empty topic"
            );

            config.set("group.id", consumer_options.group_id);
            // the tracker stores resolved offsets for librdkafka to auto-commit
            config.set("enable.auto.offset.store", "false");
            config.set("enable.auto.commit", "true");
            config.set("auto.commit.interval.ms", options.commit_interval_ms.to_string());
            // 64 KiB so one partition's fetch cannot fill the backlog before the next
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
    use std::fmt;

    use fromenv::{FromEnv, ParseResult};

    use crate::partitioner::PartitionerScheme;

    /// Connection options for the Kafka backend.
    #[derive(Clone, FromEnv)]
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

    impl fmt::Debug for ConnectOptions {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.debug_struct("ConnectOptions")
                .field("client_id", &self.client_id)
                .field("brokers", &self.brokers)
                .field("username", &self.username)
                .field("password", &self.password.as_ref().map(|_| "<redacted>"))
                .field("partition_count", &self.partition_count)
                .field("partitioner", &self.partitioner)
                .field("commit_interval_ms", &self.commit_interval_ms)
                .field("consumer", &self.consumer)
                .field("registry", &self.registry)
                .finish()
        }
    }

    /// Kafka consumer configuration.
    #[derive(Debug, Clone, FromEnv)]
    pub struct ConsumerOptions {
        /// Comma-separated topics to subscribe to.
        #[env(from = "KAFKA_TOPICS", with = split)]
        pub topics: Vec<String>,
        /// Consumer group ID.
        #[env(from = "KAFKA_CONSUMER_GROUP", default = "wrt-kafka-consumer")]
        pub group_id: String,
    }

    /// Confluent Schema Registry configuration.
    #[derive(Clone, FromEnv)]
    pub struct RegistryOptions {
        /// Schema Registry URL.
        #[env(from = "KAFKA_REGISTRY_URL")]
        pub url: String,
        /// Optional Schema Registry API key.
        #[env(from = "KAFKA_REGISTRY_API_KEY")]
        pub api_key: Option<String>,
        /// Optional Schema Registry API secret.
        #[env(from = "KAFKA_REGISTRY_API_SECRET")]
        pub api_secret: Option<String>,
    }

    impl fmt::Debug for RegistryOptions {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.debug_struct("RegistryOptions")
                .field("url", &self.url)
                .field("api_key", &self.api_key)
                .field("api_secret", &self.api_secret.as_ref().map(|_| "<redacted>"))
                .finish()
        }
    }

    #[expect(clippy::unnecessary_wraps, reason = "a `FromEnv` `with =` hook returns `ParseResult`")]
    fn split(s: &str) -> ParseResult<Vec<String>> {
        Ok(s.split(',')
            .map(str::trim)
            .filter(|topic| !topic.is_empty())
            .map(ToOwned::to_owned)
            .collect())
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

struct DeliveryContext {
    shared: Arc<Shared>,
}

impl ClientContext for DeliveryContext {}

impl ProducerContext for DeliveryContext {
    type DeliveryOpaque = usize;

    fn delivery(
        &self, delivery_result: &DeliveryResult<'_>, delivery_opaque: Self::DeliveryOpaque,
    ) {
        match delivery_result {
            Ok(message) => tracing::debug!(
                "sent message {key} in offset {offset} of partition {partition}",
                key = record_key(message),
                offset = message.offset(),
                partition = message.partition()
            ),
            Err((error, message)) => {
                tracing::error!("failed to deliver message {}: {error}", record_key(message));
                tracing::info!(monotonic_counter.delivery_failures = 1, topic = message.topic());
            }
        }
        self.shared.note_delivered(delivery_opaque);
    }
}

// A key that is not UTF-8 is shown lossily, no key as `<null>`.
fn record_key(message: &impl rdkafka::Message) -> Cow<'_, str> {
    message.key().map_or(Cow::Borrowed("<null>"), String::from_utf8_lossy)
}
