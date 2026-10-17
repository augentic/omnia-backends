use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{Context, anyhow};
use futures::future::FutureExt;
use futures::stream::{self, StreamExt};
use omnia_wasi_messaging::{
    Client as MessagingClient, FutureResult, Handler, Message, Metadata, Reply, RequestOptions,
    WasiMessagingCtx, dispatch,
};

use crate::Client;

impl WasiMessagingCtx for Client {
    fn connect(&self) -> FutureResult<Arc<dyn MessagingClient>> {
        let client = self.clone();
        async move { Ok(Arc::new(client) as Arc<dyn MessagingClient>) }.boxed()
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

impl MessagingClient for Client {
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

// Header and message translation between `omnia_wasi_messaging` and `async_nats`;
// live publish/subscribe is covered by `tests/live.rs`.
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn header_and_message_translation() {
        let mut md = Metadata::new();
        md.insert("x-trace".to_owned(), "abc-123".to_owned());
        let headers = nats_headers(&md);

        let nats_msg = async_nats::Message {
            subject: "orders.created".into(),
            reply: Some("inbox.1".into()),
            payload: b"hello".as_slice().into(),
            headers: Some(headers),
            status: None,
            description: None,
            length: 5,
        };

        let converted = from_nats(nats_msg);
        assert_eq!(converted.topic, "orders.created");
        assert_eq!(converted.payload, b"hello");
        assert_eq!(converted.reply.as_ref().map(|r| r.topic.as_str()), Some("inbox.1"));
        assert_eq!(
            converted.metadata.as_ref().and_then(|m| m.get("x-trace")).map(String::as_str),
            Some("abc-123")
        );
    }
}
