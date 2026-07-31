use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{Context, anyhow};
use futures::future::FutureExt;
use futures::stream::{self, StreamExt};
use omnia_wasi_messaging::{
    Client, FutureResult, Message, Metadata, Reply, RequestOptions, Subscriptions, WasiMessagingCtx,
};

/// `wasi-messaging` implementation backed by NATS.
impl WasiMessagingCtx for crate::Client {
    fn connect(&self) -> FutureResult<Arc<dyn Client>> {
        let client = self.clone();
        async move { Ok(Arc::new(client) as Arc<dyn Client>) }.boxed()
    }
}

/// Translate an incoming NATS message into the host's [`Message`].
fn from_nats(msg: async_nats::Message) -> Message {
    let metadata = msg.headers.as_ref().map(|headers| {
        let mut md = HashMap::new();
        for (k, v) in headers.iter() {
            let v_str = v.iter().map(ToString::to_string).collect::<Vec<String>>().join(", ");
            md.insert(k.to_string(), v_str);
        }
        Metadata { inner: md }
    });

    let mut message = Message::new(msg.payload.to_vec());
    message.topic = msg.subject.to_string();
    message.metadata = metadata;
    message.reply = msg.reply.map(|r| Reply { topic: r.to_string() });
    message
}

fn nats_headers(metadata: &Metadata) -> async_nats::HeaderMap {
    let mut headers = async_nats::HeaderMap::new();
    for (k, v) in metadata.iter() {
        headers.insert(k.as_str(), v.as_str());
    }
    headers
}

impl Client for crate::Client {
    fn subscribe(&self) -> FutureResult<Subscriptions> {
        let client = self.clone();

        async move {
            let Some(topics) = client.topics else {
                return Err(anyhow!("No topics specified"));
            };

            let mut subscribers = vec![];
            for t in &topics {
                let subscriber = client.inner.subscribe(t.clone()).await?;
                subscribers.push(subscriber);
            }

            tracing::info!("subscribed to {topics:?} topics");

            // process messages until terminated
            let stream = stream::select_all(subscribers).map(from_nats);
            Ok(Box::pin(stream) as Subscriptions)
        }
        .boxed()
    }

    fn send(&self, topic: String, message: Message) -> FutureResult<()> {
        let client = self.inner.clone();
        async move {
            match &message.metadata {
                None => client
                    .publish(topic, message.payload.into())
                    .await
                    .context("failed to publish")?,
                Some(metadata) => client
                    .publish_with_headers(topic, nats_headers(metadata), message.payload.into())
                    .await
                    .context("failed to publish")?,
            }

            Ok(())
        }
        .boxed()
    }

    fn request(
        &self, topic: String, message: Message, options: Option<RequestOptions>,
    ) -> FutureResult<Message> {
        let client = self.inner.clone();

        async move {
            let nats_headers = message.metadata.as_ref().map(nats_headers).unwrap_or_default();
            let timeout = options.and_then(|options| options.timeout);

            let request = async_nats::Request::new()
                .payload(message.payload.into())
                .headers(nats_headers)
                .timeout(timeout);

            let nats_msg =
                client.send_request(topic, request).await.context("failed to send request")?;
            Ok(from_nats(nats_msg))
        }
        .boxed()
    }
}
