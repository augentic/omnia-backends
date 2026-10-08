use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{Context, anyhow};
use futures::future::FutureExt;
use futures::stream::{self, StreamExt};
use omnia_wasi_messaging::{
    Client, FutureResult, Handler, Message, Metadata, Reply, RequestOptions, WasiMessagingCtx,
    dispatch,
};

impl WasiMessagingCtx for crate::Client {
    fn connect(&self) -> FutureResult<Arc<dyn Client>> {
        let client = self.clone();
        async move { Ok(Arc::new(client) as Arc<dyn Client>) }.boxed()
    }
}

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
    // NATS core has no acknowledgement: a message is finished with once the
    // handler returns, whatever it returned
    fn consume(&self, handler: Arc<dyn Handler>) -> FutureResult<()> {
        let client = self.clone();

        async move {
            let Some(topics) = client.topics else {
                return Err(anyhow!("No topics specified"));
            };

            let mut subscribers = Vec::with_capacity(topics.len());
            for topic in &topics {
                subscribers.push(client.inner.subscribe(topic.clone()).await?);
            }
            tracing::info!("subscribed to {topics:?} topics");

            let stream = stream::select_all(subscribers).map(from_nats);
            dispatch(Box::pin(stream), handler).await
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
