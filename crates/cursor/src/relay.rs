//! Loopback relay for the bridge's API: the worker is pointed at it through
//! `CURSOR_BACKEND_URL`, and each call is forwarded to the backend the client
//! was configured with, over TLS where that backend is `https`.
//!
//! The bridge's runtime cannot stream a request body through a `CONNECT`
//! proxy, so its API never goes through the egress proxy: the relay is the
//! bridge's one route to Cursor, and the proxy carries the shell's commands
//! alone. The server binds `[::1]:0`, the `::1` loopback, which the boundary's
//! `NO_PROXY` excepts and which the bridge does not read as `localhost` — a
//! backend at `localhost` or `127.0.0.1` makes it turn TLS verification off
//! for itself and every command it runs. It speaks HTTP/1.1, what the bridge
//! speaks to a plain-`http` backend. A request's method, path, query and
//! headers go through less the hop-by-hop set, its body streamed as it
//! arrives; the response comes back the same way; an upstream the relay
//! cannot reach is `502`. Calls under `/v1/` are the REST API the bridge
//! validates a model against, served by [`Relay::API`] whatever the backend;
//! the rest is the backend's.

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

type Body = BoxBody<Bytes, Box<dyn std::error::Error + Send + Sync>>;

// Headers that describe the connection they arrived on, never forwarded
// (RFC 9110 §7.6.1), with `Host`, which names the relay, beside them.
const HOP_BY_HOP: [HeaderName; 9] = [
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
const API_PREFIX: &str = "/v1/";

// The bound loopback relay; dropping it stops serving.
#[derive(Debug)]
pub struct Relay {
    url: String,
    server: tokio::task::JoinHandle<()>,
}

impl Relay {
    // The REST API the bridge's own client defaults to, which every call
    // under `/v1/` is forwarded to.
    pub const API: &str = "https://api.cursor.com";

    // Bind on the IPv6 loopback, forwarding to `backend`, an `http` or
    // `https` URL; the client to the upstream is built once here.
    pub async fn bind(backend: &str) -> Result<Self> {
        let upstreams = Arc::new(Upstreams::new(backend)?);
        let listener = TcpListener::bind((Ipv6Addr::LOCALHOST, 0))
            .await
            .context("binding the relay listener on the IPv6 loopback")?;
        let addr = listener.local_addr().context("reading the relay address")?;
        let server = tokio::spawn(serve(listener, upstreams));
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

// Accept until the `Relay` drops, one connection task per worker socket.
async fn serve(listener: TcpListener, upstreams: Arc<Upstreams>) {
    loop {
        let stream = match listener.accept().await {
            Ok((stream, _)) => stream,
            Err(error) => {
                tracing::warn!(%error, "relay accept error");

                // do not spin on a persistent failure such as fd exhaustion
                tokio::time::sleep(Duration::from_millis(100)).await;
                continue;
            }
        };

        let upstreams = Arc::clone(&upstreams);
        tokio::spawn(async move {
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
}

// Where each call goes, with the one client every call to either is made
// through: HTTP/1.1 as the bridge itself speaks, redirects handed back to
// the bridge, the host's own proxy settings and trust store honoured.
struct Upstreams {
    backend: String,
    api: String,
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
            backend: origin(url.as_str()),
            api: origin(Relay::API),
            client,
        })
    }

    async fn forward(&self, request: Request<Incoming>) -> Response<Body> {
        let (parts, body) = request.into_parts();
        let target = parts.uri.path_and_query().map_or("/", |target| target.as_str());
        let upstream = if is_api(parts.uri.path()) { &self.api } else { &self.backend };
        let url = format!("{upstream}{target}");

        let mut headers = parts.headers;
        strip_hop_by_hop(&mut headers);
        let sent = self
            .client
            .request(parts.method, &url)
            .headers(headers)
            .body(reqwest::Body::wrap(body))
            .send()
            .await;

        match sent {
            Ok(response) => {
                let response: Response<reqwest::Body> = response.into();
                let (mut parts, body) = response.into_parts();
                strip_hop_by_hop(&mut parts.headers);
                Response::from_parts(parts, body.map_err(Into::into).boxed())
            }
            Err(error) => {
                let error = error.without_url();
                tracing::warn!(%upstream, %error, "relay upstream failed");
                bad_gateway(&format!("relay: {upstream} did not answer: {error}"))
            }
        }
    }
}

fn is_api(path: &str) -> bool {
    path.starts_with(API_PREFIX) || path == API_PREFIX.trim_end_matches('/')
}

// The URL as the bridge concatenates a path onto it: no trailing slash.
fn origin(url: &str) -> String {
    url.trim_end_matches('/').to_owned()
}

// Drop the hop-by-hop set, whatever the `Connection` header names beside
// it, and `Host`.
fn strip_hop_by_hop(headers: &mut HeaderMap) {
    let named: Vec<HeaderName> = headers
        .get_all(CONNECTION)
        .iter()
        .filter_map(|value| value.to_str().ok())
        .flat_map(|value| value.split(','))
        .filter_map(|name| HeaderName::from_bytes(name.trim().as_bytes()).ok())
        .collect();
    for name in HOP_BY_HOP.iter().chain(&named).chain([&HOST]) {
        headers.remove(name);
    }
}

fn bad_gateway(message: &str) -> Response<Body> {
    let mut response = Response::new(Full::new(Bytes::from(message.to_owned())));
    *response.status_mut() = StatusCode::BAD_GATEWAY;
    response.headers_mut().insert(CONTENT_TYPE, HeaderValue::from_static("text/plain"));
    response.map(|body| body.map_err(Into::into).boxed())
}

// Pure translation — which upstream a path is for and what a header set
// loses; the server itself is exercised by `tests/relay.rs`.
#[cfg(test)]
mod tests {
    use http::header::{CONNECTION, CONTENT_LENGTH, HOST, TRANSFER_ENCODING};
    use http::{HeaderMap, HeaderName};

    use super::{is_api, origin, strip_hop_by_hop};

    #[test]
    fn api_paths() {
        assert!(is_api("/v1/models"));
        assert!(is_api("/v1"));
        assert!(!is_api("/v10/models"));
        assert!(!is_api("/auth/exchange_user_api_key"));
        assert!(!is_api("/aiserver.v1.DashboardService/GetUserPrivacyMode"));
    }

    #[test]
    fn origins() {
        assert_eq!(origin("https://api2.cursor.sh/"), "https://api2.cursor.sh");
        assert_eq!(origin("http://[::1]:8080/base/"), "http://[::1]:8080/base");
    }

    #[test]
    fn connection_named_headers() {
        let mut headers = HeaderMap::new();
        headers.insert(HOST, "[::1]:9".parse().unwrap());
        headers.insert(CONNECTION, "keep-alive, X-Session".parse().unwrap());
        headers.insert("x-session", "1".parse().unwrap());
        headers.insert(TRANSFER_ENCODING, "chunked".parse().unwrap());
        headers.insert(CONTENT_LENGTH, "2".parse().unwrap());
        headers.insert("x-kept", "yes".parse().unwrap());
        strip_hop_by_hop(&mut headers);
        let mut names: Vec<&str> = headers.keys().map(HeaderName::as_str).collect();
        names.sort_unstable();
        assert_eq!(names, ["content-length", "x-kept"]);
    }
}
