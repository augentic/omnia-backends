//! The valve: the one way out of the boundary, a loopback `CONNECT` proxy
//! admitting the hosts the clearance named and nothing else.

use std::convert::Infallible;
use std::fmt;
use std::mem::MaybeUninit;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context as _, Result};
use bytes::Bytes;
use http::{Method, Request, Response, StatusCode};
use http_body_util::Full;
use hyper::body::Incoming;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper_util::rt::TokioIo;
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;
use tokio::time::timeout;

const CONNECT_BOUND: Duration = Duration::from_secs(10);
// The cloud metadata endpoints beside the link-local one: AWS over IPv6
// and Alibaba Cloud.
const METADATA: [IpAddr; 2] = [
    IpAddr::V6(Ipv6Addr::new(0xfd00, 0xec2, 0, 0, 0, 0, 0, 0x254)),
    IpAddr::V4(Ipv4Addr::new(100, 100, 100, 200)),
];

/// The bound valve; dropping it stops serving.
#[derive(Debug)]
pub struct Valve {
    address: SocketAddr,
    server: JoinHandle<()>,
}

impl Valve {
    pub(crate) async fn bind(hosts: &[String]) -> Result<Self> {
        let listener =
            TcpListener::bind(("127.0.0.1", 0)).await.context("binding the egress proxy")?;
        let address = listener.local_addr().context("reading the egress proxy's address")?;
        let allowlist = Arc::new(Allowlist(hosts.iter().map(|host| Entry::parse(host)).collect()));
        tracing::debug!(%address, hosts = ?allowlist, "egress proxy bound");
        let server = tokio::spawn(serve(listener, allowlist));
        Ok(Self { address, server })
    }

    /// The proxy's address, which every boundary's `HTTPS_PROXY` names.
    #[must_use]
    pub const fn address(&self) -> SocketAddr {
        self.address
    }
}

impl Drop for Valve {
    fn drop(&mut self) {
        self.server.abort();
    }
}

/// Why an address a listed host names or resolves to is not connected.
///
/// A name on the allowlist is resolved once and every address it answers
/// is held to these classes; a listed IP literal is held to them as
/// written, loopback alone admitted. So no listed host leads the tunnel
/// back to the host itself, its network segment, or a cloud's metadata
/// service.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum Refusal {
    /// `0.0.0.0` or `::`.
    Unspecified,
    /// `127.0.0.0/8` or `::1`.
    Loopback,
    /// `169.254.0.0/16` or `fe80::/10`.
    LinkLocal,
    /// A multicast group.
    Multicast,
    /// `255.255.255.255`.
    Broadcast,
    /// A cloud instance metadata endpoint.
    Metadata,
    /// An address one of the host's own interfaces holds.
    Own,
}

impl Refusal {
    /// The class `address` falls in, if any, against `own`, the host's
    /// own addresses.
    ///
    /// A v4 address carried in v6, mapped (`::ffff:a.b.c.d`) or
    /// v4-compatible (`::a.b.c.d`), is classed as the v4 address it
    /// carries.
    #[must_use]
    pub fn of(address: IpAddr, own: &[IpAddr]) -> Option<Self> {
        let address = match address {
            IpAddr::V6(v6) if !v6.is_unspecified() && !v6.is_loopback() => {
                v6.to_ipv4().map_or(address, IpAddr::V4)
            }
            other => other,
        };
        let class = match address {
            IpAddr::V4(v4) => Self::of_v4(v4),
            IpAddr::V6(v6) => Self::of_v6(v6),
        };
        class.or_else(|| own.contains(&address).then_some(Self::Own))
    }

    fn of_v4(address: Ipv4Addr) -> Option<Self> {
        if address.is_unspecified() {
            Some(Self::Unspecified)
        } else if address.is_loopback() {
            Some(Self::Loopback)
        } else if address.is_broadcast() {
            Some(Self::Broadcast)
        } else if address == Ipv4Addr::new(169, 254, 169, 254)
            || METADATA.contains(&IpAddr::V4(address))
        {
            Some(Self::Metadata)
        } else if address.is_link_local() {
            Some(Self::LinkLocal)
        } else if address.is_multicast() {
            Some(Self::Multicast)
        } else {
            None
        }
    }

    fn of_v6(address: Ipv6Addr) -> Option<Self> {
        if address.is_unspecified() {
            Some(Self::Unspecified)
        } else if address.is_loopback() {
            Some(Self::Loopback)
        } else if METADATA.contains(&IpAddr::V6(address)) {
            Some(Self::Metadata)
        } else if address.is_unicast_link_local() {
            Some(Self::LinkLocal)
        } else if address.is_multicast() {
            Some(Self::Multicast)
        } else {
            None
        }
    }
}

impl fmt::Display for Refusal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Unspecified => "an unspecified address",
            Self::Loopback => "a loopback address",
            Self::LinkLocal => "a link-local address",
            Self::Multicast => "a multicast address",
            Self::Broadcast => "the broadcast address",
            Self::Metadata => "a cloud metadata address",
            Self::Own => "an address of the host's own",
        })
    }
}

/// The addresses the host's own interfaces hold right now.
///
/// # Errors
///
/// Returns an error when the interfaces cannot be enumerated.
#[expect(unsafe_code, reason = "`getifaddrs` has no safe wrapper among the crate's dependencies")]
pub fn own_addresses() -> Result<Vec<IpAddr>> {
    let mut head = MaybeUninit::<*mut libc::ifaddrs>::uninit();
    // SAFETY: `getifaddrs` writes `head` as it succeeds, a list `freeifaddrs`
    // releases; the list is walked before it is released and never after.
    unsafe {
        if libc::getifaddrs(head.as_mut_ptr()) != 0 {
            return Err(std::io::Error::last_os_error()).context("enumerating the interfaces");
        }
        let head = head.assume_init();
        let mut addresses = Vec::new();
        let mut entry = head;
        while !entry.is_null() {
            let current = &*entry;
            if !current.ifa_addr.is_null() {
                match i32::from((*current.ifa_addr).sa_family) {
                    libc::AF_INET => {
                        let socket = &*current.ifa_addr.cast::<libc::sockaddr_in>();
                        let octets = socket.sin_addr.s_addr.to_ne_bytes();
                        addresses.push(IpAddr::V4(Ipv4Addr::from(octets)));
                    }
                    libc::AF_INET6 => {
                        let socket = &*current.ifa_addr.cast::<libc::sockaddr_in6>();
                        addresses.push(IpAddr::V6(Ipv6Addr::from(socket.sin6_addr.s6_addr)));
                    }
                    _ => {}
                }
            }
            entry = current.ifa_next;
        }
        libc::freeifaddrs(head);
        Ok(addresses)
    }
}

#[derive(Debug, Clone, Default)]
struct Allowlist(Vec<Entry>);

impl Allowlist {
    fn admits(&self, host: &str) -> Option<Admission> {
        let host = host.trim_matches(['[', ']']).to_ascii_lowercase();
        let literal = host.parse::<IpAddr>().ok();
        self.0.iter().find_map(|entry| match (entry, literal) {
            (Entry::Literal(listed), Some(asked)) if *listed == asked => {
                Some(Admission::Literal(asked))
            }
            (Entry::Exact(name), None) if *name == host => Some(Admission::Name),
            (Entry::Suffix(suffix), None)
                if host == *suffix
                    || host
                        .strip_suffix(suffix.as_str())
                        .is_some_and(|rest| rest.ends_with('.')) =>
            {
                Some(Admission::Name)
            }
            _ => None,
        })
    }
}

// One allowlist entry as the ask spelled it.
#[derive(Debug, Clone)]
enum Entry {
    // A host name, matched whole and without regard to case.
    Exact(String),
    // `*.suffix`: the suffix and every name beneath it.
    Suffix(String),
    // An IP literal, vetted as written: how an ask names a loopback mirror.
    Literal(IpAddr),
}

impl Entry {
    fn parse(text: &str) -> Self {
        let text = text.trim().trim_matches(['[', ']']).to_ascii_lowercase();
        match (text.parse::<IpAddr>(), text.strip_prefix("*.")) {
            (Ok(address), _) => Self::Literal(address),
            (Err(_), Some(suffix)) => Self::Suffix(suffix.to_owned()),
            (Err(_), None) => Self::Exact(text),
        }
    }
}

// What the allowlist says of one `CONNECT` host.
enum Admission {
    // A listed name: resolve and vet every address.
    Name,
    // A listed literal: vet as written.
    Literal(IpAddr),
}

// Accept until the `Valve` drops, one connection task per client socket.
async fn serve(listener: TcpListener, allowlist: Arc<Allowlist>) {
    loop {
        let stream = match listener.accept().await {
            Ok((stream, _)) => stream,
            Err(error) => {
                tracing::warn!(%error, "egress accept error");

                // do not spin on a persistent failure such as fd exhaustion
                tokio::time::sleep(Duration::from_millis(100)).await;
                continue;
            }
        };

        let allowlist = Arc::clone(&allowlist);
        tokio::spawn(async move {
            let service = service_fn(move |request| {
                let allowlist = Arc::clone(&allowlist);
                async move { Ok::<_, Infallible>(handle(request, &allowlist).await) }
            });
            let served = http1::Builder::new()
                .serve_connection(TokioIo::new(stream), service)
                .with_upgrades()
                .await;
            if let Err(error) = served {
                tracing::debug!(%error, "egress connection ended");
            }
        });
    }
}

// `CONNECT host:port` alone: the host held to the allowlist and every
// address vetted, a name resolved once and each answer classed, a literal
// classed as written, the tunnel opened before the client hears `200`, so
// a refusal is a status and never a closed tunnel.
async fn handle(request: Request<Incoming>, allowlist: &Allowlist) -> Response<Full<Bytes>> {
    if request.method() != Method::CONNECT {
        return answer(StatusCode::METHOD_NOT_ALLOWED, "only CONNECT is proxied");
    }
    let Some((host, port)) = request
        .uri()
        .authority()
        .and_then(|authority| Some((authority.host().to_owned(), authority.port_u16()?)))
    else {
        return answer(StatusCode::BAD_REQUEST, "CONNECT needs host:port");
    };

    let admitted = match allowlist.admits(&host) {
        None => Err("is not on the allowlist".to_owned()),
        Some(Admission::Literal(address)) => {
            literal(address, port).map_err(|refusal| format!("is {refusal}"))
        }
        Some(Admission::Name) => {
            vetted(&host, port).await.map_err(|refusal| format!("resolves to {refusal}"))
        }
    };
    let addresses = match admitted {
        Ok(addresses) => addresses,
        Err(why) => {
            tracing::warn!(host, port, why, "egress refused");
            return answer(StatusCode::FORBIDDEN, &format!("`{host}` {why}"));
        }
    };
    let upstream = match connect(addresses).await {
        Ok(upstream) => upstream,
        Err(error) => {
            tracing::warn!(host, port, error = format!("{error:#}"), "egress unreachable");
            return answer(StatusCode::BAD_GATEWAY, &format!("`{host}:{port}` is unreachable"));
        }
    };

    tracing::debug!(host, port, "egress tunnel opened");
    tokio::spawn(async move {
        match hyper::upgrade::on(request).await {
            Ok(upgraded) => {
                let mut client = TokioIo::new(upgraded);
                let mut upstream = upstream;
                if let Err(error) = tokio::io::copy_bidirectional(&mut client, &mut upstream).await
                {
                    tracing::debug!(%error, "egress tunnel ended");
                }
            }
            Err(error) => tracing::debug!(%error, "egress upgrade failed"),
        }
    });
    Response::new(Full::default())
}

// A listed literal as written, unless it is of a refused class; loopback
// is admitted, the mirror a literal exists to name.
fn literal(address: IpAddr, port: u16) -> std::result::Result<Vec<SocketAddr>, Refusal> {
    match Refusal::of(address, &own()) {
        None | Some(Refusal::Loopback) => Ok(vec![SocketAddr::new(address, port)]),
        Some(refusal) => Err(refusal),
    }
}

// The addresses a listed name resolved to that pass the vetting; `Err`
// names what the last refused one was when none pass.
async fn vetted(host: &str, port: u16) -> std::result::Result<Vec<SocketAddr>, Refusal> {
    let own = own();
    let mut admitted = Vec::new();
    let mut refused = None;
    if let Ok(resolved) = tokio::net::lookup_host((host, port)).await {
        for address in resolved {
            match Refusal::of(address.ip(), &own) {
                Some(refusal) => refused = Some(refusal),
                None => admitted.push(address),
            }
        }
    }
    match (admitted.is_empty(), refused) {
        (true, Some(refusal)) => Err(refusal),
        _ => Ok(admitted),
    }
}

// The host's own addresses, or none where they cannot be read.
fn own() -> Vec<IpAddr> {
    own_addresses().unwrap_or_else(|error| {
        tracing::warn!(error = format!("{error:#}"), "egress cannot read the host's addresses");
        Vec::new()
    })
}

async fn connect(addresses: Vec<SocketAddr>) -> Result<TcpStream> {
    let mut last = anyhow::anyhow!("the name resolved to nothing");
    for address in addresses {
        match timeout(CONNECT_BOUND, TcpStream::connect(address)).await {
            Ok(Ok(stream)) => return Ok(stream),
            Ok(Err(error)) => last = anyhow::Error::from(error).context(format!("{address}")),
            Err(_elapsed) => {
                last = anyhow::anyhow!("{address} did not answer within {CONNECT_BOUND:?}");
            }
        }
    }
    Err(last)
}

fn answer(status: StatusCode, reason: &str) -> Response<Full<Bytes>> {
    let mut response = Response::new(Full::from(Bytes::from(format!("{reason}\n"))));
    *response.status_mut() = status;
    response
}
