use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use anyhow::anyhow;
use futures::future::FutureExt;
use omnia_wasi_messaging::{
    Client, FutureResult, Handler, Message, Metadata, RequestOptions, WasiMessagingCtx,
};
use rdkafka::error::{KafkaError, RDKafkaErrorCode};
use rdkafka::message::ToBytes;
use rdkafka::producer::{BaseRecord, ThreadedProducer};

use crate::{Tracer, consume};

// One retry, long enough for a full queue to drain a slot.
const QUEUE_RETRY: Duration = Duration::from_millis(20);

impl WasiMessagingCtx for crate::Client {
    fn connect(&self) -> FutureResult<Arc<dyn Client>> {
        let client = self.clone();
        async move { Ok(Arc::new(client) as Arc<dyn Client>) }.boxed()
    }
}

impl Client for crate::Client {
    // A client consumes once: the loop ends only with a record it could not
    // deliver, and the process restarts to replay from the committed offset.
    fn consume(&self, handler: Arc<dyn Handler>) -> FutureResult<()> {
        let client = self.clone();

        async move {
            let Some(consumer) = &client.inner.consumer else {
                return Err(anyhow!("no topics to consume: the client has no consumer"));
            };
            if client.inner.consuming.swap(true, Ordering::AcqRel) {
                return Err(anyhow!("already consuming"));
            }
            let shared = Arc::clone(&client.inner.shared);
            let registry = client.inner.registry.clone();
            consume::run(consume::records(consumer), shared, registry, handler).await
        }
        .boxed()
    }

    fn send(&self, topic: String, message: Message) -> FutureResult<()> {
        let client = self.clone();

        async move {
            let payload = match &client.inner.registry {
                Some(registry) => match registry.encode(&topic, message.payload).await {
                    Ok(payload) => payload,
                    Err(rejection) => {
                        tracing::info!(
                            monotonic_counter.publish_refused = 1,
                            topic = %topic,
                            reason = rejection.reason()
                        );
                        return Err(anyhow!("send to {topic} refused: {rejection}"));
                    }
                },
                None => message.payload,
            };
            let metadata = message.metadata.unwrap_or_default();
            let key = metadata.get("key").cloned();
            let partition = explicit_partition(&metadata).or_else(|| {
                key.as_deref().map(|key| client.inner.partitioner.partition(key.as_bytes()))
            });

            let id = client.inner.shared.next_send_id();
            let opaque = usize::try_from(id).expect("send id fits a pointer");
            let mut record = BaseRecord::with_opaque_to(&topic, opaque)
                .payload(&payload)
                .timestamp(chrono::Utc::now().timestamp_millis());
            if let Some(partition) = partition {
                record = record.partition(partition);
            }
            // `.key()` changes the record's type, so each arm enqueues its own
            let queued = match &key {
                Some(key) => enqueue(&client.inner.producer, record.key(key.as_str())).await,
                None => enqueue(&client.inner.producer, record).await,
            };
            if let Err(error) = queued {
                // no delivery callback fires for a record that never queued
                client.inner.shared.note_delivered(id);
                tracing::info!(monotonic_counter.publish_refused = 1, topic = %topic, reason = "refused");
                return Err(anyhow!(error));
            }
            Ok(())
        }
        .boxed()
    }

    fn request(
        &self, _topic: String, _message: Message, _options: Option<RequestOptions>,
    ) -> FutureResult<Message> {
        async { Err(anyhow!("request-reply is not supported by the Kafka backend")) }.boxed()
    }
}

fn explicit_partition(metadata: &Metadata) -> Option<i32> {
    metadata
        .get("partition")
        .and_then(|value| value.parse().ok())
        .filter(|partition| *partition >= 0)
}

// A full queue hands the record back with the error, so the retry sends the
// very same one.
async fn enqueue<K, P>(
    producer: &ThreadedProducer<Tracer>, record: BaseRecord<'_, K, P, usize>,
) -> Result<(), KafkaError>
where
    K: ToBytes + Sync + ?Sized,
    P: ToBytes + Sync + ?Sized,
{
    let record = match producer.send(record) {
        Ok(()) => return Ok(()),
        Err((KafkaError::MessageProduction(RDKafkaErrorCode::QueueFull), record)) => record,
        Err((error, _)) => return Err(error),
    };
    tokio::time::sleep(QUEUE_RETRY).await;
    producer.send(record).map_err(|(error, _)| error)
}
