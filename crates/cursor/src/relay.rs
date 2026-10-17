//! Loopback relay for the bridge's API: the worker is pointed at it through
//! `CURSOR_BACKEND_URL`, and each call is forwarded to the backend the client
//! was configured with, over TLS where that backend is `https`.
//!
//! The bridge's runtime cannot stream a request body through a `CONNECT`
//! proxy, so its API never goes through the egress proxy: the relay is the
//! bridge's one route to Cursor, and the proxy carries the shell's commands
//! alone. The server binds `[::1]:0`, the `::1` loopback, which the sandbox's
//! `NO_PROXY` excepts and which the bridge does not read as `localhost` — a
//! backend at `localhost` or `127.0.0.1` makes it turn TLS verification off
//! for itself and every command it runs. It speaks HTTP/1.1, what the bridge
//! speaks to a plain-`http` backend.
//!
//! - A request's method, path, query and end-to-end headers go through, its
//!   body streamed as it arrives; the response comes back the same way.
//! - An upstream the relay cannot reach is `502`.
//! - Calls under `/v1/` are the REST API the bridge validates a model
//!   against, served by [`REST_API`] whatever the backend; the rest is the
//!   backend's.

use std::convert::Infallible;
use std::net::Ipv6Addr;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context as _, Result, ensure};
use bytes::Bytes;
use http::header::{
    CONNECTION, CONTENT_TYPE, HOST, HeaderMap, HeaderName, HeaderValue, PROXY_AUTHENTICATE,
    PROXY_AUTHORIZATION, TE, TRAILER, TRANSFER_ENCODING, UPGRADE,
};
use http::{Request, Response, StatusCode};
use http_body_util::combinators::BoxBody;
use http_body_util::{BodyExt as _, Full};
use hyper::body::Incoming;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper_util::rt::TokioIo;
use reqwest::Url;
use reqwest::redirect::Policy;
use tokio::net::TcpListener;
use tokio::task::JoinSet;
use tokio::time;

type Body = BoxBody<Bytes, Box<dyn std::error::Error + Send + Sync>>;

// Cursor's backend, the default `ConnectOptions::backend_url`.
pub const BACKEND_URL: &str = "https://api2.cursor.sh";
const REST_API: &str = "https://api.cursor.com";

// The bound loopback relay; dropping it stops serving, in-flight connections
// included.
#[derive(Debug)]
pub struct Relay {
    url: String,
    server: tokio::task::JoinHandle<()>,
}

impl Relay {
    // Bind on the IPv6 loopback, forwarding to `backend`, an `http` or
    // `https` URL; the client to the upstream is built once here.
    pub async fn bind(backend: &str) -> Result<Self> {
        let upstreams = Arc::new(Upstreams::new(backend)?);
        let listener = TcpListener::bind((Ipv6Addr::LOCALHOST, 0))
            .await
            .context("binding the relay listener on the IPv6 loopback")?;
        let addr = listener.local_addr().context("reading the relay address")?;

        // handle relay requests
        let server = tokio::spawn(async move {
            let mut connections = JoinSet::new();

            loop {
                let stream = match listener.accept().await {
                    Ok((stream, _)) => stream,
                    Err(error) => {
                        tracing::warn!(%error, "relay accept error");

                        // do not spin on a persistent failure
                        time::sleep(Duration::from_millis(100)).await;
                        continue;
                    }
                };

                while connections.try_join_next().is_some() {}

                let upstreams = Arc::clone(&upstreams);
                connections.spawn(async move {
                    let service = service_fn(move |request| {
                        let upstreams = Arc::clone(&upstreams);
                        async move { Ok::<_, Infallible>(upstreams.forward(request).await) }
                    });

                    if let Err(error) =
                        http1::Builder::new().serve_connection(TokioIo::new(stream), service).await
                    {
                        tracing::debug!(%error, "relay connection ended");
                    }
                });
            }
        });

        Ok(Self {
            url: format!("http://{addr}"),
            server,
        })
    }

    pub fn url(&self) -> &str {
        &self.url
    }
}

impl Drop for Relay {
    fn drop(&mut self) {
        self.server.abort();
    }
}

struct Upstreams {
    backend: String,
    client: reqwest::Client,
}

impl Upstreams {
    fn new(backend: &str) -> Result<Self> {
        let url = Url::parse(backend)
            .with_context(|| format!("the backend URL `{backend}` does not parse"))?;
        ensure!(
            matches!(url.scheme(), "http" | "https") && url.host_str().is_some(),
            "the backend URL `{backend}` must be an http or https URL naming a host"
        );

        let client = reqwest::Client::builder()
            .http1_only()
            .redirect(Policy::none())
            .build()
            .context("building the relay's upstream client")?;

        Ok(Self {
            backend: url.as_str().trim_end_matches('/').to_owned(),
            client,
        })
    }

    async fn forward(&self, request: Request<Incoming>) -> Response<Body> {
        let (parts, body) = request.into_parts();
        let target = parts.uri.path_and_query().map_or("/", |target| target.as_str());
        let upstream = if is_api(parts.uri.path()) { REST_API } else { &self.backend };
        let url = format!("{upstream}{target}");

        let sent = self
            .client
            .request(parts.method, &url)
            .headers(permitted(parts.headers))
            .body(reqwest::Body::wrap(body))
            .send()
            .await;

        match sent {
            Ok(response) => {
                let response: Response<reqwest::Body> = response.into();
                let (mut parts, body) = response.into_parts();
                parts.headers = permitted(parts.headers);
                Response::from_parts(parts, body.map_err(Into::into).boxed())
            }
            Err(error) => {
                let error = error.without_url();
                tracing::warn!(%upstream, %error, "relay upstream failed");
                bad_gateway(format!("relay: {upstream} did not answer: {error}"))
            }
        }
    }
}

fn is_api(path: &str) -> bool {
    path == "/v1" || path.starts_with("/v1/")
}

// Headers that describe the connection they arrived on, never forwarded
// (RFC 9110 §7.6.1), with `Host`, which names the relay, beside them.
const PROHIBITED: [HeaderName; 9] = [
    CONNECTION,
    HeaderName::from_static("keep-alive"),
    HeaderName::from_static("proxy-connection"),
    PROXY_AUTHENTICATE,
    PROXY_AUTHORIZATION,
    TE,
    TRAILER,
    TRANSFER_ENCODING,
    UPGRADE,
];

fn permitted(headers: HeaderMap) -> HeaderMap {
    let named: Vec<HeaderName> = headers
        .get_all(CONNECTION)
        .iter()
        .filter_map(|value| value.to_str().ok())
        .flat_map(|value| value.split(','))
        .filter_map(|name| HeaderName::from_bytes(name.trim().as_bytes()).ok())
        .collect();

    PROHIBITED.iter().chain(&named).chain([&HOST]).fold(headers, |mut kept, name| {
        kept.remove(name);
        kept
    })
}

fn bad_gateway(message: String) -> Response<Body> {
    let mut response = Response::new(Full::new(Bytes::from(message)));
    *response.status_mut() = StatusCode::BAD_GATEWAY;
    response.headers_mut().insert(CONTENT_TYPE, HeaderValue::from_static("text/plain"));
    response.map(|body| body.map_err(Into::into).boxed())
}

// The REST split alone; what the relay forwards, drops and streams is
// `tests/relay.rs`'s, over a backend the suite owns.
#[cfg(test)]
mod tests {
    use super::is_api;

    #[test]
    fn api_paths() {
        assert!(is_api("/v1/models"));
        assert!(is_api("/v1"));
        assert!(!is_api("/v10/models"));
        assert!(!is_api("/auth/exchange_user_api_key"));
        assert!(!is_api("/aiserver.v1.DashboardService/GetUserPrivacyMode"));
    }
}
