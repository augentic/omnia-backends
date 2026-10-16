//! The relay the bridge's API goes through: a worker is pointed at it in
//! place of the host's `CURSOR_BACKEND_URL`, and what it is sent reaches the
//! backend the client was configured with — path and query kept,
//! connection-specific headers dropped, bodies streamed both ways, an
//! unreachable backend `502`.
//! The relay's address is read back from the fake bridge's `CreateAgent`
//! record, as the only place the client tells it.

mod support;

use std::convert::Infallible;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use bytes::Bytes;
use http::header::{CONNECTION, HOST, PROXY_AUTHORIZATION, TE};
use http::{HeaderValue, Method, Request, Response, StatusCode};
use http_body_util::channel::Channel;
use http_body_util::combinators::BoxBody;
use http_body_util::{BodyExt as _, Full};
use hyper::body::Incoming;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper_util::client::legacy::Client as HyperClient;
use hyper_util::rt::{TokioExecutor, TokioIo};
use omnia::Backend as _;
use omnia_cursor::{Client, ConnectOptions};
use support::fake_bridge::{self, Config, History as _, Rpc, Spawnable};
use support::harness::{await_gone, connect, options, run_guest};
use tokio::net::TcpListener;
use tokio::sync::Notify;

type BoxError = Box<dyn std::error::Error + Send + Sync>;
type Body = BoxBody<Bytes, BoxError>;

// How long a chunk may take to cross the relay to the upstream.
const ARRIVAL: Duration = Duration::from_secs(5);

// --- The bridge's side ---

// The worker is told the relay, whatever the host's own `CURSOR_BACKEND_URL`
// says, since the relay is where the client forwards from.
#[tokio::test]
#[expect(unsafe_code, reason = "the host's value is planted in the process environment")]
async fn bridge_pointed_at_the_relay() {
    // SAFETY: nextest runs one test per process, and the variable is set
    // before the test spawns any thread of its own.
    unsafe { std::env::set_var("CURSOR_BACKEND_URL", "http://backend.invalid") };
    let fake = Spawnable::new(&Config::echo());
    let upstream = Upstream::serve().await;
    let (_client, relay) = relay_of(&fake, &upstream.url).await;

    assert!(relay.starts_with("http://[::1]:"), "the relay is on the IPv6 loopback: {relay}");
    assert_ne!(relay, "http://backend.invalid");
}

// `CURSOR_BACKEND_URL` is the client's to set, so a worker grant naming it
// is refused as the request's fault, before anything is spawned.
#[tokio::test]
async fn backend_url_refused_as_a_grant() {
    let fake = Spawnable::new(&Config::echo());
    let client = connect(ConnectOptions {
        shell_roots: vec![fake.home().to_path_buf()],
        ..options(1)
    })
    .await;
    let grant = ["env:CURSOR_BACKEND_URL=http://backend.invalid", "refused:is refused"];
    run_guest(test_programs::MODEL_WORKER, &grant, &client).await;

    assert!(fake.log().events.is_empty(), "a refused grant spawned: {}", fake.log().summary());
}

// A backend that is not an http(s) URL is refused at connect.
#[tokio::test]
async fn backend_url_must_be_http() {
    let fake = Spawnable::new(&Config::echo());
    fake_bridge::dummy_key();
    for bad in ["api2.cursor.sh", "ftp://api2.cursor.sh", "https://"] {
        let error = Client::connect_with(ConnectOptions {
            backend_url: bad.to_owned(),
            ..options(1)
        })
        .await
        .expect_err(bad);
        assert!(format!("{error:#}").contains(bad), "{bad}: {error:#}");
    }
    assert!(fake.log().events.is_empty(), "a refused option spawned a process");
}

// --- The backend's side ---

// Method, path, query and the end-to-end headers reach the backend; the
// connection-specific set, what `Connection` names, and the relay's own `Host` do
// not, and the backend's `Connection` is not handed back.
#[tokio::test]
async fn path_query_and_headers() {
    let fake = Spawnable::new(&Config::echo());
    let upstream = Upstream::serve().await;
    let (_client, relay) = relay_of(&fake, &upstream.url).await;

    let request = Request::builder()
        .method(Method::GET)
        .uri(format!("{relay}/a/b?x=1&y=two"))
        .header("x-kept", "yes")
        .header(CONNECTION, "x-session")
        .header("x-session", "1")
        .header(PROXY_AUTHORIZATION, "Basic abc")
        .header(TE, "trailers")
        .body(empty())
        .expect("a request");
    let response = http().request(request).await.expect("the relay answers");
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers().get("x-answered").map(HeaderValue::as_bytes), Some(&b"yes"[..]));
    assert!(response.headers().get(CONNECTION).is_none(), "{:?}", response.headers());

    let seen = upstream.seen();
    assert_eq!(seen.len(), 1, "{seen:?}");
    assert_eq!(seen[0].method, "GET");
    assert_eq!(seen[0].target, "/a/b?x=1&y=two");
    assert_eq!(seen[0].header("x-kept"), Some("yes"));
    assert_eq!(seen[0].header(HOST.as_str()), Some(upstream.authority.as_str()));
    for dropped in ["x-session", "connection", "proxy-authorization", "te"] {
        assert_eq!(seen[0].header(dropped), None, "{dropped}: {:?}", seen[0].headers);
    }
}

// A request body reaches the backend chunk by chunk, each before the next
// is sent: the bug class the relay sidesteps is a body held until it ends.
#[tokio::test]
async fn request_streams() {
    let fake = Spawnable::new(&Config::echo());
    let upstream = Upstream::serve().await;
    let (_client, relay) = relay_of(&fake, &upstream.url).await;

    let (mut sender, channel) = Channel::<Bytes, BoxError>::new(1);
    let request = Request::builder()
        .method(Method::POST)
        .uri(format!("{relay}/echo"))
        .body(channel.boxed())
        .expect("a request");
    let responding = tokio::spawn(http().request(request));

    for chunk in ["one", "two"] {
        sender.send_data(Bytes::from_static(chunk.as_bytes())).await.expect("the body is open");
        upstream.await_chunk(chunk).await;
        assert!(!upstream.seen()[0].ended, "the body ended before `{chunk}` was followed");
    }
    drop(sender);

    let response = responding.await.expect("the request task joins").expect("the relay answers");
    assert_eq!(response.status(), StatusCode::OK);
    let body = response.into_body().collect().await.expect("a body").to_bytes();
    assert_eq!(body, "onetwo");
}

// A response body comes back frame by frame: the first arrives while the
// backend still holds the second.
#[tokio::test]
async fn response_streams() {
    let fake = Spawnable::new(&Config::echo());
    let upstream = Upstream::serve().await;
    let (_client, relay) = relay_of(&fake, &upstream.url).await;

    let request = Request::builder()
        .uri(format!("{relay}/stream-response"))
        .body(empty())
        .expect("a request");
    let response = http().request(request).await.expect("the relay answers");
    assert_eq!(response.status(), StatusCode::OK);
    let mut body = response.into_body();

    assert_eq!(next_data(&mut body).await, Some(Bytes::from_static(b"first")));
    upstream.release();
    assert_eq!(next_data(&mut body).await, Some(Bytes::from_static(b"second")));
    assert_eq!(next_data(&mut body).await, None);
}

// A backend nothing listens at is answered `502`, naming it.
#[tokio::test]
async fn upstream_unreachable() {
    let fake = Spawnable::new(&Config::echo());
    let closed = {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("a loopback port");
        format!("http://{}", listener.local_addr().expect("an address"))
    };
    let (_client, relay) = relay_of(&fake, &closed).await;

    let request = Request::builder().uri(format!("{relay}/x")).body(empty()).expect("a request");
    let response = http().request(request).await.expect("the relay answers");
    assert_eq!(response.status(), StatusCode::BAD_GATEWAY);
    let body = response.into_body().collect().await.expect("a body").to_bytes();
    let body = String::from_utf8_lossy(&body);
    assert!(body.contains(&closed), "{body}");
}

// --- Support ---

// A client over `backend_url` with one completion run, so a worker was told
// the relay; the client is returned since the relay lives with it.
async fn relay_of(fake: &Spawnable, backend_url: &str) -> (Client, String) {
    let client = connect(ConnectOptions {
        backend_url: backend_url.to_owned(),
        ..options(1)
    })
    .await;
    run_guest(test_programs::MODEL_ECHO_TEXT, &[], &client).await;
    await_gone(fake).await;

    let log = fake.log();
    let created = log.saw(Rpc::CreateAgent);
    assert_eq!(created.len(), 1, "one worker was told the relay: {}", log.summary());
    (client, created[0].text("backendUrl").to_owned())
}

fn http() -> HyperClient<hyper_util::client::legacy::connect::HttpConnector, Body> {
    HyperClient::builder(TokioExecutor::new()).build_http()
}

fn empty() -> Body {
    Full::new(Bytes::new()).map_err(Into::into).boxed()
}

async fn next_data(body: &mut Incoming) -> Option<Bytes> {
    loop {
        let frame = body.frame().await?.expect("a frame");
        if let Ok(data) = frame.into_data() {
            return Some(data);
        }
    }
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

// One request as the backend saw it.
#[derive(Debug, Clone, Default)]
struct Seen {
    method: String,
    target: String,
    headers: Vec<(String, String)>,
    chunks: Vec<Bytes>,
    ended: bool,
}

impl Seen {
    fn header(&self, name: &str) -> Option<&str> {
        self.headers.iter().find(|(key, _)| key == name).map(|(_, value)| value.as_str())
    }
}

// The fake backend: every request recorded as it arrives, answered with its
// body echoed under `x-answered: yes` and `connection: close`, except
// `/stream-response`, answered in two frames with the second held until
// `release`.
struct Upstream {
    url: String,
    authority: String,
    seen: Arc<Mutex<Vec<Seen>>>,
    release: Arc<Notify>,
}

impl Upstream {
    async fn serve() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("a loopback port");
        let authority = listener.local_addr().expect("an address").to_string();
        let seen = Arc::new(Mutex::new(Vec::new()));
        let release = Arc::new(Notify::new());
        tokio::spawn(accept(listener, Arc::clone(&seen), Arc::clone(&release)));
        Self {
            url: format!("http://{authority}"),
            authority,
            seen,
            release,
        }
    }

    fn seen(&self) -> Vec<Seen> {
        lock(&self.seen).clone()
    }

    fn release(&self) {
        self.release.notify_one();
    }

    async fn await_chunk(&self, chunk: &str) {
        fake_bridge::poll(
            || self.seen().first().is_some_and(|seen| seen.chunks.iter().any(|c| c == chunk)),
            ARRIVAL,
            &format!("chunk `{chunk}` at the backend; saw {:?}", self.seen()),
        )
        .await;
    }
}

async fn accept(listener: TcpListener, seen: Arc<Mutex<Vec<Seen>>>, release: Arc<Notify>) {
    loop {
        let Ok((stream, _)) = listener.accept().await else { return };
        let (seen, release) = (Arc::clone(&seen), Arc::clone(&release));
        tokio::spawn(async move {
            let service = service_fn(move |request| {
                let (seen, release) = (Arc::clone(&seen), Arc::clone(&release));
                async move { Ok::<_, Infallible>(handle(request, &seen, release).await) }
            });
            let _ = http1::Builder::new().serve_connection(TokioIo::new(stream), service).await;
        });
    }
}

async fn handle(
    request: Request<Incoming>, seen: &Mutex<Vec<Seen>>, release: Arc<Notify>,
) -> Response<Body> {
    let (parts, mut body) = request.into_parts();
    let index = {
        let mut seen = lock(seen);
        seen.push(Seen {
            method: parts.method.to_string(),
            target: parts.uri.to_string(),
            headers: parts
                .headers
                .iter()
                .map(|(name, value)| {
                    (name.to_string(), String::from_utf8_lossy(value.as_bytes()).into_owned())
                })
                .collect(),
            ..Seen::default()
        });
        seen.len() - 1
    };

    let mut collected = Vec::new();
    while let Some(frame) = body.frame().await {
        if let Ok(data) = frame.expect("a frame").into_data() {
            collected.extend_from_slice(&data);
            lock(seen)[index].chunks.push(data);
        }
    }
    lock(seen)[index].ended = true;

    if parts.uri.path() == "/stream-response" {
        let (mut sender, channel) = Channel::<Bytes, BoxError>::new(1);
        tokio::spawn(async move {
            let _ = sender.send_data(Bytes::from_static(b"first")).await;
            release.notified().await;
            let _ = sender.send_data(Bytes::from_static(b"second")).await;
        });
        return Response::new(channel.boxed());
    }

    let mut response = Response::new(Full::new(Bytes::from(collected)).map_err(Into::into).boxed());
    response.headers_mut().insert("x-answered", "yes".parse().expect("a header value"));
    response.headers_mut().insert(CONNECTION, "close".parse().expect("a header value"));
    response
}
