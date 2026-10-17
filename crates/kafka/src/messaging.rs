use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use anyhow::{Context as _, anyhow};
use futures::future::FutureExt;
use omnia_wasi_messaging::{
    Client, FutureResult, Handler, Message, Metadata, RequestOptions, WasiMessagingCtx,
};
use rdkafka::error::{KafkaError, RDKafkaErrorCode};
use rdkafka::message::ToBytes;
use rdkafka::producer::{BaseRecord, ThreadedProducer};

use crate::{DeliveryContext, consume};

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
            let key = metadata.get("key").map(String::as_str);
            let partition = explicit_partition(&metadata)
                .or_else(|| key.map(|key| client.inner.partitioner.partition(key.as_bytes())));

            let id = client.inner.shared.next_send_id();
            let mut record = BaseRecord::with_opaque_to(&topic, id.get()).payload(&payload);
            if let Some(partition) = partition {
                record = record.partition(partition);
            }
            // `.key()` changes the record's type, so each arm enqueues its own
            let queued = match key {
                Some(key) => enqueue(&client.inner.producer, record.key(key)).await,
                None => enqueue(&client.inner.producer, record).await,
            };
            if queued.is_ok() {
                id.queued();
            } else {
                tracing::info!(monotonic_counter.publish_refused = 1, topic = %topic, reason = "refused");
            }
            queued.with_context(|| format!("send to {topic} refused"))
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
    producer: &ThreadedProducer<DeliveryContext>, record: BaseRecord<'_, K, P, usize>,
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

// What a send reads from a consumed record's metadata, without a broker; a
// real send is `tests/live.rs::keyed_sends`.
#[cfg(test)]
mod tests {
    use rdkafka::Timestamp;
    use rdkafka::message::OwnedMessage;

    use super::*;

    #[test]
    fn forwarded_metadata() {
        let record = OwnedMessage::new(
            None,
            Some(b"k".to_vec()),
            "t".to_owned(),
            Timestamp::NotAvailable,
            3,
            7,
            None,
        );
        let message = consume::message(&record, Vec::new());
        let metadata = message.metadata.expect("a record carries metadata");

        assert_eq!(metadata.get("key").map(String::as_str), Some("k"));
        assert_eq!(metadata.get("source-partition").map(String::as_str), Some("3"));
        assert_eq!(explicit_partition(&metadata), None);
    }
}
