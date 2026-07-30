#![doc = include_str!("../README.md")]

mod messaging;
mod partitioner;
mod registry;

use std::fmt::{self, Debug};
use std::sync::Arc;

use anyhow::{Context, Result};
use omnia::Backend;
use rand::random_range;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::producer::{DeliveryResult, ProducerContext, ThreadedProducer};
use rdkafka::{ClientConfig, ClientContext, Message as _};
use tracing::instrument;

use crate::partitioner::Partitioner;
use crate::registry::Registry;

const DEFAULT_GROUP: &str = "wrt-kafka-consumer";

/// Kafka backend client with producer, optional consumer, and optional schema registry.
#[derive(Clone)]
pub struct Client {
    producer: ThreadedProducer<Tracer>,
    partitioner: Partitioner,
    registry: Option<Registry>,
    consumer: Option<Arc<StreamConsumer>>,
}

impl Debug for Client {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("KafkaClient").finish()
    }
}

impl Backend for Client {
    type ConnectOptions = ConnectOptions;

    #[instrument]
    async fn connect_with(options: Self::ConnectOptions) -> Result<Self> {
        let mut config = ClientConfig::from(&options);

        // producer
        let producer = config.create_with_context(Tracer {}).context("issue creating producer")?;

        // custom partitioner and maybe schema registry
        let partitioner = Partitioner::new(options.partition_count);
        let registry = options.registry.map(Registry::new);

        // maybe consumer
        let consumer = if let Some(consumer_options) = options.consumer {
            let group_id = consumer_options.group_id.as_deref().unwrap_or(DEFAULT_GROUP);
            config.set("group.id", group_id);

            let consumer: StreamConsumer = config.create().context("issue creating consumer")?;

            // subscribe to topics
            let topics = consumer_options.topics.iter().map(String::as_str).collect::<Vec<_>>();
            consumer.subscribe(&topics).context("issue subscribing to topics")?;
            tracing::debug!("subscribed to topics: {topics:?}");

            Some(Arc::new(consumer))
        } else {
            None
        };

        Ok(Self {
            producer,
            partitioner,
            registry,
            consumer,
        })
    }
}

#[allow(missing_docs)]
mod config {
    use fromenv::{FromEnv, ParseResult};

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
        pub(crate) api_key: String,
        /// Schema Registry API secret.
        #[env(from = "KAFKA_REGISTRY_API_SECRET")]
        pub(crate) api_secret: String,
        /// Schema cache TTL in seconds.
        #[env(from = "KAFKA_REGISTRY_CACHE_TTL", default = "3600")]
        pub(crate) cache_ttl_secs: u64,
    }

    // The `FromEnv` `with =` hook requires a `ParseResult` return type.
    #[allow(clippy::unnecessary_wraps)]
    fn split(s: &str) -> ParseResult<Vec<String>> {
        Ok(s.split(',').map(ToOwned::to_owned).collect())
    }
}
pub use config::{ConnectOptions, ConsumerOptions, RegistryOptions};

impl From<&ConnectOptions> for ClientConfig {
    fn from(kafka: &ConnectOptions) -> Self {
        let mut config = Self::new();

        config.set("client.id", format!("{}-{}", kafka.client_id, random_range(1000..9999)));
        config.set("bootstrap.servers", &kafka.brokers);

        // SASL authentication
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
    fn from_env() -> Result<Self> {
        Self::from_env().finalize().context("issue loading connection options")
    }
}

/// Kafka producer delivery callback that logs send results.
pub struct Tracer;
impl ClientContext for Tracer {}
impl ProducerContext for Tracer {
    type DeliveryOpaque = ();

    fn delivery(&self, delivery_result: &DeliveryResult<'_>, (): Self::DeliveryOpaque) {
        match delivery_result {
            Ok(msg) => {
                let key: &str = msg.key_view().unwrap().unwrap();
                tracing::debug!(
                    "sent message {key} in offset {offset} of partition {partition}",
                    offset = msg.offset(),
                    partition = msg.partition()
                );
            }
            Err((err, message)) => {
                let key: &str = message.key_view().unwrap().unwrap();
                tracing::error!("Failed to send message {key}: {err}");
            }
        }
    }
}
