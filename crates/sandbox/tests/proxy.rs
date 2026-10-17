//! The egress proxy: what it admits, what it refuses, and the tunnel it
//! carries.

mod support;

use std::net::{IpAddr, SocketAddr};
use std::time::Duration;

use omnia_sandbox::{Child, Grant, Options};
use support::{connect, proxy_of};
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use tokio::net::{TcpListener, TcpStream};

// The proxy of a session around a private directory, admitting `hosts`,
// with the process that keeps it serving.
async fn proxy(hosts: &[&str]) -> (SocketAddr, Child) {
    let sandbox = connect(Options::default()).await;
    let grant = Grant {
        egress: hosts.iter().map(ToString::to_string).collect(),
        ..Grant::default()
    };
    proxy_of(support::granted(&sandbox, None, &grant).await).await
}

// One request to the proxy: the status line's code, the body, and the
// stream, which is the tunnel after a `200`.
async fn request(proxy: SocketAddr, request: &str) -> (u16, String, TcpStream) {
    let mut stream = TcpStream::connect(proxy).await.expect("the proxy listens");
    stream.write_all(request.as_bytes()).await.expect("the request is written");
    let mut head = Vec::new();
    while !head.ends_with(b"\r\n\r\n") {
        let byte = stream.read_u8().await.expect("a response head");
        head.push(byte);
    }
    let head = String::from_utf8_lossy(&head).into_owned();
    let status = head
        .split_whitespace()
        .nth(1)
        .and_then(|code| code.parse().ok())
        .unwrap_or_else(|| panic!("a status line, not {head:?}"));
    let length: usize = head
        .lines()
        .find_map(|line| line.strip_prefix("content-length: "))
        .and_then(|length| length.parse().ok())
        .unwrap_or(0);
    let mut body = vec![0; length];
    stream.read_exact(&mut body).await.expect("the body");
    (status, String::from_utf8_lossy(&body).into_owned(), stream)
}

async fn connect_to(proxy: SocketAddr, target: &str) -> (u16, String, TcpStream) {
    request(proxy, &format!("CONNECT {target} HTTP/1.1\r\nHost: {target}\r\n\r\n")).await
}

// An echo server on the host's loopback, answering one connection.
async fn echo_server() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("an echo server");
    let address = listener.local_addr().expect("its address");
    tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.expect("a connection");
        let mut heard = [0; 4];
        stream.read_exact(&mut heard).await.expect("four bytes");
        stream.write_all(&heard).await.expect("echoed");
    });
    address
}

// A server on the host's loopback that accepts and then holds each
// connection open, saying nothing.
async fn silent_server() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("a silent server");
    let address = listener.local_addr().expect("its address");
    tokio::spawn(async move {
        loop {
            let (stream, _) = listener.accept().await.expect("a connection");
            tokio::spawn(async move {
                let _held = stream;
                std::future::pending::<()>().await;
            });
        }
    });
    address
}

fn ip(text: &str) -> IpAddr {
    text.parse().expect("an address")
}

// An address of the host's own beyond the special ranges, where it has one:
// the one a connected datagram socket is bound to, which sends nothing.
fn own_address() -> Option<IpAddr> {
    let socket = std::net::UdpSocket::bind("0.0.0.0:0").ok()?;
    socket.connect("93.184.215.14:80").ok()?;
    let address = socket.local_addr().ok()?.ip();
    (!address.is_loopback() && !address.is_unspecified()).then_some(address)
}

// --- The proxy ---

#[tokio::test]
async fn unlisted_host() {
    let (proxy, _child) = proxy(&["127.0.0.1", "registry.example.test"]).await;
    let (status, body, _) = connect_to(proxy, "example.com:443").await;
    assert_eq!(status, 403, "{body}");
    assert!(body.contains("not on the allowlist"), "{body}");

    // a listed name is admitted whether or not it resolves: an unreachable one is a 502
    let (status, _, _) = connect_to(proxy, "registry.example.test:443").await;
    assert_eq!(status, 502);
}

#[tokio::test]
async fn suffix_pattern() {
    let (proxy, _child) = proxy(&["*.example.test"]).await;
    for target in ["example.test:443", "a.example.test:443", "b.a.example.test:443"] {
        let (status, body, _) = connect_to(proxy, target).await;
        assert_eq!(status, 502, "{target} is admitted: {body}");
    }
    for target in ["notexample.test:443", "example.test.evil:443", "test:443"] {
        let (status, body, _) = connect_to(proxy, target).await;
        assert_eq!(status, 403, "{target} is refused: {body}");
    }
}

#[tokio::test]
async fn loopback_resolution() {
    let server = echo_server().await;
    let (proxy, _child) = proxy(&["localhost"]).await;
    let (status, body, _) = connect_to(proxy, &format!("localhost:{}", server.port())).await;
    assert_eq!(status, 403, "{body}");
    assert!(body.contains("loopback"), "{body}");
}

#[tokio::test]
async fn loopback_literal() {
    let server = echo_server().await;
    let (proxy, _child) = proxy(&["127.0.0.1"]).await;
    let (status, _, mut tunnel) = connect_to(proxy, &format!("127.0.0.1:{}", server.port())).await;
    assert_eq!(status, 200);

    tunnel.write_all(b"ping").await.expect("bytes into the tunnel");
    let mut echoed = [0; 4];
    tunnel.read_exact(&mut echoed).await.expect("bytes back");
    assert_eq!(&echoed, b"ping");
}

// The proxy is one session's. Once the session's process is gone, a tunnel
// it carried is closed with it rather than left to drain on its own.
#[tokio::test]
async fn session_drop() {
    let server = silent_server().await;
    let (proxy, child) = proxy(&["127.0.0.1"]).await;
    let (status, _, mut tunnel) = connect_to(proxy, &format!("127.0.0.1:{}", server.port())).await;
    assert_eq!(status, 200);

    drop(child);

    let mut byte = [0; 1];
    let closed = tokio::time::timeout(Duration::from_secs(5), tunnel.read(&mut byte)).await;
    assert!(matches!(closed, Ok(Ok(0) | Err(_))), "the tunnel stayed open: {closed:?}");
    let refused = tokio::time::timeout(Duration::from_secs(5), TcpStream::connect(proxy)).await;
    assert!(matches!(refused, Ok(Err(_))), "the proxy still listens: {refused:?}");
}

#[tokio::test]
async fn denied_literals() {
    let mut rows = vec![
        ("169.254.169.254".to_owned(), "a cloud metadata address"),
        ("fd00:ec2::254".to_owned(), "a cloud metadata address"),
        ("::ffff:100.100.100.200".to_owned(), "a cloud metadata address"),
        ("fe80::1".to_owned(), "a link-local address"),
        ("0.0.0.0".to_owned(), "an unspecified address"),
    ];
    rows.extend(own_address().map(|address| (address.to_string(), "an address of the host's own")));
    let listed: Vec<&str> = rows.iter().map(|(address, _)| address.as_str()).collect();
    let (proxy, _child) = proxy(&listed).await;
    for (address, class) in &rows {
        let target = match ip(address) {
            IpAddr::V4(_) => format!("{address}:80"),
            IpAddr::V6(_) => format!("[{address}]:80"),
        };
        let (status, body, _) = connect_to(proxy, &target).await;
        assert_eq!(status, 403, "{target}: {body}");
        assert!(body.contains(class), "{target}: {body}");
    }
}

#[tokio::test]
async fn plain_http() {
    let (proxy, _child) = proxy(&["127.0.0.1", "example.com"]).await;
    let (status, body, _) =
        request(proxy, "GET http://example.com/ HTTP/1.1\r\nHost: example.com\r\n\r\n").await;
    assert_eq!(status, 405, "{body}");

    let (status, body, _) = request(proxy, "CONNECT example.com HTTP/1.1\r\n\r\n").await;
    assert_eq!(status, 400, "{body}");
}

#[tokio::test]
async fn proxy_env() {
    let sandbox = connect(Options::default()).await;
    let output = support::run(support::session(&sandbox, None).await, "env").await;
    let env = String::from_utf8_lossy(&output.stdout).into_owned();
    let proxy = env
        .lines()
        .find_map(|line| line.strip_prefix("HTTPS_PROXY="))
        .unwrap_or_else(|| panic!("HTTPS_PROXY is set: {env}"));
    assert!(proxy.starts_with("http://127.0.0.1:"), "a loopback proxy: {proxy}");
    for name in ["HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "http_proxy", "https_proxy", "all_proxy"]
    {
        assert!(env.contains(&format!("{name}={proxy}\n")), "{name}: {env}");
    }
    for name in ["NO_PROXY", "no_proxy"] {
        assert!(env.contains(&format!("{name}=127.0.0.1,localhost,::1,[::1]\n")), "{name}: {env}");
    }
}

// --- Inside the session (macOS) ---

// With the kernel closing the network to loopback, the proxy is the one way
// out. Nothing resolves inside, a direct connect fails, and a tunnel to a
// listed host carries bytes.
#[cfg(target_os = "macos")]
#[tokio::test]
async fn confined_egress() {
    let workspace = tempfile::tempdir().expect("a workspace");
    let tree = workspace.path().join("tree");
    std::fs::create_dir_all(&tree).expect("the tree");
    support::probe(&tree);
    let server = echo_server().await;
    let sandbox = connect(Options::default()).await;
    let grant = Grant {
        egress: vec!["127.0.0.1".to_owned()],
        ..Grant::default()
    };
    let bound = Duration::from_secs(20);

    let control = support::probe_outside(&tree, &["resolve", "localhost"]);
    assert!(control.status.success(), "the resolver answers outside");

    let session = support::granted(&sandbox, Some(&tree), &grant).await;
    let output = support::sh(session, "./probe resolve example.com")
        .output(bound)
        .await
        .expect("the probe answers");
    assert!(!output.status.success(), "a name resolved inside: {output:?}");

    let session = support::granted(&sandbox, Some(&tree), &grant).await;
    let script = "env -u HTTPS_PROXY -u https_proxy -u HTTP_PROXY -u http_proxy -u ALL_PROXY \
                  -u all_proxy curl -sS --max-time 10 https://example.com";
    let output = support::sh(session, script).output(bound).await.expect("curl answers");
    assert!(!output.status.success(), "a direct connect reached out: {output:?}");

    let session = support::granted(&sandbox, Some(&tree), &grant).await;
    let output = support::sh(session, &format!("./probe tunnel 127.0.0.1:{}", server.port()))
        .output(bound)
        .await
        .expect("the probe answers");
    assert!(output.status.success(), "the tunnel: {}", String::from_utf8_lossy(&output.stderr));
}
