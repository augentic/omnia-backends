//! The egress proxy: what it admits, what it refuses, and the tunnel it
//! carries; and the vetting every resolved address goes through.

mod support;

use std::net::{IpAddr, SocketAddr};

use omnia_confine::egress::{Refusal, own_addresses};
use omnia_confine::{Confine, Options};
use support::connect;
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use tokio::net::{TcpListener, TcpStream};

async fn proxy(hosts: &[&str]) -> Confine {
    connect(Options {
        egress: hosts.iter().map(ToString::to_string).collect(),
        ..Options::default()
    })
    .await
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

fn ip(text: &str) -> IpAddr {
    text.parse().expect("an address")
}

// --- Vetting ---

#[test]
fn vetting_table() {
    let table = [
        ("0.0.0.0", Some(Refusal::Unspecified)),
        ("::", Some(Refusal::Unspecified)),
        ("127.0.0.1", Some(Refusal::Loopback)),
        ("127.8.9.10", Some(Refusal::Loopback)),
        ("::1", Some(Refusal::Loopback)),
        ("169.254.169.254", Some(Refusal::Metadata)),
        ("fd00:ec2::254", Some(Refusal::Metadata)),
        ("100.100.100.200", Some(Refusal::Metadata)),
        ("169.254.1.1", Some(Refusal::LinkLocal)),
        ("fe80::1", Some(Refusal::LinkLocal)),
        ("224.0.0.1", Some(Refusal::Multicast)),
        ("ff02::1", Some(Refusal::Multicast)),
        ("255.255.255.255", Some(Refusal::Broadcast)),
        // IPv4 carried in IPv6 is classed as the IPv4 it carries
        ("::ffff:127.0.0.1", Some(Refusal::Loopback)),
        ("::ffff:169.254.169.254", Some(Refusal::Metadata)),
        ("::127.0.0.1", Some(Refusal::Loopback)),
        ("::ffff:0.0.0.0", Some(Refusal::Unspecified)),
        ("::ffff:93.184.215.14", None),
        // public addresses pass
        ("93.184.215.14", None),
        ("2606:2800:21f:cb07:6820:80da:af6b:8b2c", None),
        // a private range is the operator's to list: an internal registry is a legitimate host
        ("10.0.0.1", None),
        ("192.168.1.1", None),
        ("fd12::1", None),
    ];
    for (address, expected) in table {
        assert_eq!(Refusal::of(ip(address), &[]), expected, "{address}");
    }
}

#[test]
fn own_addresses_refused() {
    let own = own_addresses().expect("the host's interfaces");
    assert!(own.contains(&ip("127.0.0.1")), "a loopback interface among {own:?}");
    let public = ip("93.184.215.14");
    assert_eq!(Refusal::of(public, &[public]), Some(Refusal::Own));
    assert_eq!(Refusal::of(ip("::ffff:93.184.215.14"), &[public]), Some(Refusal::Own));
    assert_eq!(Refusal::of(public, &own), None);
}

// --- The proxy ---

#[tokio::test]
async fn unlisted_host_refused() {
    let confine = proxy(&["127.0.0.1", "registry.example.test"]).await;
    let (status, body, _) = connect_to(confine.egress().address(), "example.com:443").await;
    assert_eq!(status, 403, "{body}");
    assert!(body.contains("not on the allowlist"), "{body}");

    // a listed name is admitted whether or not it resolves: an unreachable one is a 502
    let (status, _, _) = connect_to(confine.egress().address(), "registry.example.test:443").await;
    assert_eq!(status, 502);
}

#[tokio::test]
async fn suffix_matches_beneath() {
    let confine = proxy(&["*.example.test"]).await;
    for target in ["example.test:443", "a.example.test:443", "b.a.example.test:443"] {
        let (status, body, _) = connect_to(confine.egress().address(), target).await;
        assert_eq!(status, 502, "{target} is admitted: {body}");
    }
    for target in ["notexample.test:443", "example.test.evil:443", "test:443"] {
        let (status, body, _) = connect_to(confine.egress().address(), target).await;
        assert_eq!(status, 403, "{target} is refused: {body}");
    }
}

#[tokio::test]
async fn listed_name_resolving_to_loopback_refused() {
    let server = echo_server().await;
    let confine = proxy(&["localhost"]).await;
    let (status, body, _) =
        connect_to(confine.egress().address(), &format!("localhost:{}", server.port())).await;
    assert_eq!(status, 403, "{body}");
    assert!(body.contains("loopback"), "{body}");
}

#[tokio::test]
async fn tunnel_to_loopback_literal() {
    let server = echo_server().await;
    let confine = proxy(&["127.0.0.1"]).await;
    let (status, _, mut tunnel) =
        connect_to(confine.egress().address(), &format!("127.0.0.1:{}", server.port())).await;
    assert_eq!(status, 200);

    tunnel.write_all(b"ping").await.expect("bytes into the tunnel");
    let mut echoed = [0; 4];
    tunnel.read_exact(&mut echoed).await.expect("bytes back");
    assert_eq!(&echoed, b"ping");
}

#[tokio::test]
async fn plain_get_refused() {
    let confine = proxy(&["127.0.0.1", "example.com"]).await;
    let (status, body, _) = request(
        confine.egress().address(),
        "GET http://example.com/ HTTP/1.1\r\nHost: example.com\r\n\r\n",
    )
    .await;
    assert_eq!(status, 405, "{body}");

    let (status, body, _) =
        request(confine.egress().address(), "CONNECT example.com HTTP/1.1\r\n\r\n").await;
    assert_eq!(status, 400, "{body}");
}

#[tokio::test]
async fn proxy_in_the_environment() {
    let confine = proxy(&[]).await;
    let boundary = support::boundary(&confine, None).await;
    let command = support::sh(&boundary, "env");
    let output = boundary.output(command, std::time::Duration::from_secs(5)).await.expect("env");
    let env = String::from_utf8_lossy(&output.stdout).into_owned();
    let proxy = format!("http://{}", confine.egress().address());
    for name in ["HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "http_proxy", "https_proxy", "all_proxy"]
    {
        assert!(env.contains(&format!("{name}={proxy}\n")), "{name}: {env}");
    }
    for name in ["NO_PROXY", "no_proxy"] {
        assert!(env.contains(&format!("{name}=127.0.0.1,localhost,::1\n")), "{name}: {env}");
    }
}

// --- Inside the boundary (macOS) ---

// With the kernel closing the network to loopback, the proxy is the one
// way out: nothing resolves inside and a direct connect fails, while a
// tunnel to a listed host carries bytes.
#[cfg(target_os = "macos")]
#[tokio::test]
async fn egress_through_the_proxy_alone() {
    let lend = tempfile::tempdir().expect("a lend");
    let tree = lend.path().join("tree");
    std::fs::create_dir_all(&tree).expect("the tree");
    support::probe(&tree);
    let server = echo_server().await;
    let confine = connect(Options {
        egress: vec!["127.0.0.1".to_owned()],
        substrate: omnia_confine::Substrate::Auto,
        ..Options::default()
    })
    .await;
    let bound = std::time::Duration::from_secs(20);

    let control = support::probe_outside(&tree, &["resolve", "localhost"]);
    assert!(control.status.success(), "the resolver answers outside");

    let boundary = support::boundary(&confine, Some(&tree)).await;
    let command = support::sh(&boundary, "./probe resolve example.com");
    let output = boundary.output(command, bound).await.expect("the probe answers");
    assert!(!output.status.success(), "a name resolved inside: {output:?}");

    let boundary = support::boundary(&confine, Some(&tree)).await;
    let script = "env -u HTTPS_PROXY -u https_proxy -u HTTP_PROXY -u http_proxy -u ALL_PROXY \
                  -u all_proxy curl -sS --max-time 10 https://example.com";
    let command = support::sh(&boundary, script);
    let output = boundary.output(command, bound).await.expect("curl answers");
    assert!(!output.status.success(), "a direct connect reached out: {output:?}");

    let boundary = support::boundary(&confine, Some(&tree)).await;
    let command = support::sh(&boundary, &format!("./probe tunnel 127.0.0.1:{}", server.port()));
    let output = boundary.output(command, bound).await.expect("the probe answers");
    assert!(output.status.success(), "the tunnel: {}", String::from_utf8_lossy(&output.stderr));
}
