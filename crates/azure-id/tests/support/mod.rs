//! A managed-identity endpoint in the App Service shape, served in-process on
//! loopback: what `IDENTITY_ENDPOINT` points `ManagedIdentityCredential` at
//! under test.
//!
//! The fake knows the host's system-assigned identity and the user-assigned
//! client ids it is given. It records every token request it receives and
//! answers one for an identity it knows with a token naming the identity and
//! the resource.

use std::collections::HashMap;
use std::convert::Infallible;
use std::future;
use std::net::{Ipv4Addr, SocketAddr};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use http_body_util::Full;
use hyper::body::Incoming;
use hyper::header::CONTENT_TYPE;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Method, Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use serde_json::{Value, json};
use tokio::net::TcpListener;
use tokio::task::JoinHandle;

const API_VERSION: &str = "2019-08-01";
const SECRET: &str = "fake-identity-header";
const LIFETIME_SECS: u64 = 3600;

/// The managed identity a token was asked for, by client id, and the resource.
pub type Asked = (Option<String>, String);

/// The fake, serving until dropped.
pub struct FakeIdentity {
    state: Arc<State>,
    endpoint: String,
    serving: JoinHandle<()>,
}

impl FakeIdentity {
    /// Serve the system-assigned identity and the user-assigned `client_ids` on loopback.
    pub async fn serve(client_ids: &[&str]) -> Self {
        let listener = TcpListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
            .await
            .expect("bind loopback");
        let addr = listener.local_addr().expect("local address");
        let state = Arc::new(State {
            assigned: client_ids.iter().map(|&id| id.to_owned()).collect(),
            asked: Mutex::default(),
        });
        let serving = tokio::spawn(accept(listener, Arc::clone(&state)));
        Self {
            state,
            endpoint: format!("http://{addr}/msi/token"),
            serving,
        }
    }

    /// Point the process at the fake, as App Service does, `vars` its only identity declaration.
    #[expect(unsafe_code, reason = "the backend reads its environment from the process")]
    pub fn plant(&self, vars: &[(&str, &str)]) {
        // SAFETY: nextest runs one test per process, and the variables are set
        // before the test spawns any thread of its own.
        unsafe {
            std::env::remove_var("IDENTITY_NAMES");
            std::env::remove_var("IDENTITY_CLIENT_ID");
            std::env::set_var("IDENTITY_ENDPOINT", &self.endpoint);
            std::env::set_var("IDENTITY_HEADER", SECRET);
            for (key, value) in vars {
                std::env::set_var(key, value);
            }
        }
    }

    /// Every token request received so far, in order.
    pub fn asked(&self) -> Vec<Asked> {
        self.state.asked.lock().expect("asked lock").clone()
    }
}

impl Drop for FakeIdentity {
    fn drop(&mut self) {
        self.serving.abort();
    }
}

struct State {
    assigned: Vec<String>,
    asked: Mutex<Vec<Asked>>,
}

impl State {
    fn answer(&self, request: &Request<Incoming>) -> Response<Full<Bytes>> {
        let query: HashMap<String, String> =
            url::form_urlencoded::parse(request.uri().query().unwrap_or_default().as_bytes())
                .into_owned()
                .collect();
        let client_id = query.get("client_id").cloned();
        let resource = query.get("resource").cloned().unwrap_or_default();
        self.asked.lock().expect("asked lock").push((client_id.clone(), resource.clone()));

        if request.method() != Method::GET {
            return reply(StatusCode::METHOD_NOT_ALLOWED, &json!({ "error": "not a GET" }));
        }
        let secret =
            request.headers().get("x-identity-header").and_then(|value| value.to_str().ok());
        if secret != Some(SECRET) {
            return reply(StatusCode::UNAUTHORIZED, &json!({ "error": "bad x-identity-header" }));
        }
        let version = query.get("api-version").map(String::as_str);
        if version != Some(API_VERSION) || resource.is_empty() {
            return reply(StatusCode::BAD_REQUEST, &json!({ "error": "bad query" }));
        }
        if client_id.as_ref().is_some_and(|id| !self.assigned.contains(id)) {
            return reply(StatusCode::BAD_REQUEST, &json!({ "error": "identity not found" }));
        }
        let identity = client_id.as_deref().unwrap_or("system");
        let now = SystemTime::now().duration_since(UNIX_EPOCH).expect("after the epoch");
        reply(
            StatusCode::OK,
            &json!({
                "access_token": format!("{identity}:{resource}"),
                "expires_on": (now.as_secs() + LIFETIME_SECS).to_string(),
                "token_type": "Bearer",
                "resource": resource,
            }),
        )
    }
}

async fn accept(listener: TcpListener, state: Arc<State>) {
    while let Ok((stream, _)) = listener.accept().await {
        let state = Arc::clone(&state);
        tokio::spawn(async move {
            let service = service_fn(move |request| {
                future::ready(Ok::<_, Infallible>(state.answer(&request)))
            });
            let _ = http1::Builder::new().serve_connection(TokioIo::new(stream), service).await;
        });
    }
}

fn reply(status: StatusCode, body: &Value) -> Response<Full<Bytes>> {
    Response::builder()
        .status(status)
        .header(CONTENT_TYPE, "application/json")
        .body(Full::new(Bytes::from(body.to_string())))
        .expect("a well-formed response")
}
