use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;

use anyhow::anyhow;
use futures::Stream;
use futures::future::FutureExt;
use futures::stream::StreamExt;
use futures::task::{Context, Poll};
use omnia_wasi_messaging::{
    Client, FutureResult, Message, Metadata, RequestOptions, Subscriptions, WasiMessagingCtx,
};
use rdkafka::Message as _;
use rdkafka::message::{Headers, OwnedMessage};
use rdkafka::producer::BaseRecord;
use tokio::sync::mpsc;

const CAPACITY: usize = 1024;

/// `wasi-messaging` implementation backed by Kafka via `rdkafka`.
impl WasiMessagingCtx for crate::Client {
    fn connect(&self) -> FutureResult<Arc<dyn Client>> {
        let client = self.clone();
        async move { Ok(Arc::new(client) as Arc<dyn Client>) }.boxed()
    }
}

/// Translate an incoming Kafka message into the host's [`Message`].
fn from_kafka(msg: &OwnedMessage, payload: Vec<u8>) -> Message {
    let metadata = msg.headers().map(|headers| {
        let mut md = HashMap::new();
        for h in headers.iter() {
            let bytes = h.value.unwrap_or_default();
            md.insert(h.key.to_string(), String::from_utf8_lossy(bytes).to_string());
        }
        Metadata { inner: md }
    });

    let mut message = Message::new(payload);
    message.topic = msg.topic().to_string();
    message.metadata = metadata;
    message
}

impl Client for crate::Client {
    fn subscribe(&self) -> FutureResult<Subscriptions> {
        let client = self.clone();

        async move {
            let Some(consumer) = client.consumer else {
                return Err(anyhow!("No topics specified"));
            };
            let registry = client.registry;

            // spawn a task to read messages and forward subscriber
            let (sender, receiver) = mpsc::channel::<Message>(CAPACITY);
            tokio::spawn(async move {
                consumer
                    .stream()
                    .filter_map(|res| async {
                        res.map_or_else(
                            |e| {
                                tracing::error!("kafka consumer error: {e}");
                                None
                            },
                            Some,
                        )
                    })
                    .for_each(|msg| {
                        let sender = sender.clone();
                        let registry = registry.clone();
                        async move {
                            let payload = msg.payload().unwrap_or_default().to_vec();
                            let decoded = if let Some(sr) = &registry {
                                sr.decode(msg.topic(), &payload).await
                            } else {
                                payload
                            };
                            let message = from_kafka(&msg.detach(), decoded);
                            if let Err(e) = sender.send(message).await {
                                tracing::error!("failed to send message to subscriber: {e}");
                            }
                        }
                    })
                    .await;
            });

            Ok(Box::pin(Subscriber { receiver }) as Subscriptions)
        }
        .boxed()
    }

    fn send(&self, topic: String, message: Message) -> FutureResult<()> {
        let client = self.clone();

        // TODO: add offset to header??

        async move {
            // schema registry validation when available
            let payload = if let Some(sr) = &client.registry {
                sr.encode(&topic, message.payload).await
            } else {
                message.payload
            };

            let metadata = message.metadata.unwrap_or_default();
            let now = chrono::Utc::now().timestamp_millis();

            let key = metadata.get("key").cloned().unwrap_or_default();
            let mut record =
                BaseRecord::to(&topic).payload(&payload).key(key.as_bytes()).timestamp(now);

            // partitioning
            let partition = metadata.get("partition").cloned().unwrap_or_default();
            let partition = partition.parse().unwrap_or(-1);
            if partition >= 0 {
                record = record.partition(partition);
            } else if let Some(key) = metadata.get("key") {
                let partition = client.partitioner.partition(key.as_bytes());
                record = record.partition(partition);
            }

            if let Err((e, _)) = client.producer.send(record) {
                tracing::error!("producer::error {e}");
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

/// Async stream of Kafka messages forwarded from a background consumer task.
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
