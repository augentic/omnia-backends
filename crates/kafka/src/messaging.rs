use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use anyhow::anyhow;
use futures::Stream;
use futures::future::FutureExt;
use futures::task::{Context, Poll};
use omnia_wasi_messaging::{
    Client, FutureResult, Message, RequestOptions, Subscriptions, WasiMessagingCtx,
};
use rdkafka::error::{KafkaError, RDKafkaErrorCode};
use rdkafka::producer::BaseRecord;
use tokio::sync::mpsc;

use crate::dispatch;

const CAPACITY: usize = 1024;

// One retry, long enough for a full queue to drain a slot.
const QUEUE_RETRY: Duration = Duration::from_millis(20);

impl WasiMessagingCtx for crate::Client {
    fn connect(&self) -> FutureResult<Arc<dyn Client>> {
        let client = self.clone();
        async move { Ok(Arc::new(client) as Arc<dyn Client>) }.boxed()
    }
}

impl Client for crate::Client {
    fn subscribe(&self) -> FutureResult<Subscriptions> {
        let client = self.clone();

        async move {
            let Some(consumer) = client.inner.consumer.clone() else {
                return Err(anyhow!("No topics specified"));
            };
            let mut forwarder = client.inner.forwarder.lock().expect("forwarder");
            if forwarder.is_some() {
                return Err(anyhow!("already subscribed"));
            }
            let registry = client.inner.registry.clone();
            let shared = Arc::clone(&client.inner.shared);

            let (sender, receiver) = mpsc::channel::<Message>(CAPACITY);
            shared.install_sender(sender);
            *forwarder = Some(dispatch::spawn(shared, consumer, registry));
            drop(forwarder);

            Ok(Box::pin(Subscriber { receiver }) as Subscriptions)
        }
        .boxed()
    }

    fn send(&self, topic: String, message: Message) -> FutureResult<()> {
        let client = self.clone();

        async move {
            let payload = if let Some(registry) = &client.inner.registry {
                registry.encode(&topic, message.payload).await
            } else {
                message.payload
            };

            let metadata = message.metadata.unwrap_or_default();
            let now = chrono::Utc::now().timestamp_millis();
            let key = metadata.get("key").cloned();
            let partition = explicit_partition(&metadata).or_else(|| {
                key.as_deref().map(|key| client.inner.partitioner.partition(key.as_bytes()))
            });

            let id = client.inner.shared.next_send_id();
            let opaque = usize::try_from(id).expect("send id fits a pointer");
            let key_bytes = key.as_deref().map(str::as_bytes);

            let mut attempt = enqueue(
                &client.inner.producer,
                &topic,
                &payload,
                key_bytes,
                partition,
                now,
                opaque,
            );
            if attempt.as_ref().is_err_and(queue_full) {
                tokio::time::sleep(QUEUE_RETRY).await;
                attempt = enqueue(
                    &client.inner.producer,
                    &topic,
                    &payload,
                    key_bytes,
                    partition,
                    now,
                    opaque,
                );
            }
            if let Err(error) = attempt {
                // no delivery callback will fire for a record that never queued
                client.inner.shared.note_delivered(id);
                tracing::info!(monotonic_counter.publish_refused = 1, topic = %topic);
                return Err(anyhow!(error));
            }
            Ok(())
        }
        .boxed()
    }

    fn request(
        &self, _topic: String, _message: Message, _options: Option<RequestOptions>,
    ) -> FutureResult<Message> {
        async move { unimplemented!() }.boxed()
    }
}

fn explicit_partition(metadata: &omnia_wasi_messaging::Metadata) -> Option<i32> {
    metadata
        .get("partition")
        .and_then(|value| value.parse().ok())
        .filter(|partition| *partition >= 0)
}

const fn queue_full(error: &KafkaError) -> bool {
    matches!(error, KafkaError::MessageProduction(RDKafkaErrorCode::QueueFull))
}

fn enqueue(
    producer: &rdkafka::producer::ThreadedProducer<crate::Tracer>, topic: &str, payload: &[u8],
    key: Option<&[u8]>, partition: Option<i32>, timestamp: i64, opaque: usize,
) -> Result<(), KafkaError> {
    let mut record =
        BaseRecord::with_opaque_to(topic, opaque).payload(payload).timestamp(timestamp);
    if let Some(partition) = partition {
        record = record.partition(partition);
    }
    // `.key()` changes the record's key type, so each arm maps its own error.
    if let Some(key) = key {
        producer.send(record.key(key)).map_err(|(error, _)| error)
    } else {
        producer.send(record).map_err(|(error, _)| error)
    }
}

#[derive(Debug)]
pub struct Subscriber {
    receiver: mpsc::Receiver<Message>,
}

impl Stream for Subscriber {
    type Item = Message;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.receiver.poll_recv(cx)
    }
}
