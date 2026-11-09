//! A RESP2 Redis served in-process on loopback: the endpoint the entry grants
//! the package under test.
//!
//! The fake keeps one map with lazy TTL expiry and answers `GET`, `SET`
//! (`PX`, `EX`, `KEEPTTL`), `DEL`, `EXISTS`, `SCAN`, and the one `EVAL` whose
//! script is the package's compare-and-swap, emulated natively. Served with a
//! password, it refuses every command on a link until that link's `AUTH`. It
//! records every command with the ordinal of the link it arrived on.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use omnia_redis_component::CAS_SCRIPT;
use omnia_redis_component::resp::{Decoder, Frame};
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;

/// One command as the fake received it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Recorded {
    /// The ordinal of the link it arrived on, in accept order.
    pub connection: usize,
    /// The command's arguments, its name first.
    pub args: Vec<Vec<u8>>,
}

impl Recorded {
    /// The command name, upper-cased.
    pub fn name(&self) -> String {
        command_name(&self.args)
    }

    /// The key the command addresses, when it has one.
    pub fn key(&self) -> Option<String> {
        key_of(&self.args).map(|key| String::from_utf8_lossy(key).into_owned())
    }
}

struct Entry {
    value: Vec<u8>,
    expires: Option<Instant>,
}

type Data = HashMap<Vec<u8>, Entry>;

struct State {
    password: Option<String>,
    data: Mutex<Data>,
    commands: Mutex<Vec<Recorded>>,
    connections: AtomicUsize,
}

/// The fake, serving until dropped.
pub struct FakeRedis {
    state: Arc<State>,
    addr: SocketAddr,
    serving: JoinHandle<()>,
}

impl FakeRedis {
    /// Serve on an ephemeral loopback port, answering every command.
    pub async fn serve() -> Self {
        Self::serve_with(None).await
    }

    /// Serve on an ephemeral loopback port, refusing a link's commands until
    /// it sends `AUTH password`.
    pub async fn serve_secured(password: &str) -> Self {
        Self::serve_with(Some(password.to_owned())).await
    }

    async fn serve_with(password: Option<String>) -> Self {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.expect("bind loopback");
        let addr = listener.local_addr().expect("local address");
        let state = Arc::new(State {
            password,
            data: Mutex::new(HashMap::new()),
            commands: Mutex::new(Vec::new()),
            connections: AtomicUsize::new(0),
        });
        let serving = tokio::spawn(accept(listener, Arc::clone(&state)));
        Self { state, addr, serving }
    }

    /// The `host:port` the entry grants the package.
    pub fn endpoint(&self) -> String {
        self.addr.to_string()
    }

    /// Every command received so far, in order.
    pub fn commands(&self) -> Vec<Recorded> {
        self.state.commands.lock().expect("commands lock").clone()
    }

    /// Links accepted so far.
    pub fn connections(&self) -> usize {
        self.state.connections.load(Ordering::SeqCst)
    }
}

impl Drop for FakeRedis {
    fn drop(&mut self) {
        self.serving.abort();
    }
}

async fn accept(listener: TcpListener, state: Arc<State>) {
    while let Ok((stream, _)) = listener.accept().await {
        let ordinal = state.connections.fetch_add(1, Ordering::SeqCst);
        tokio::spawn(connection(stream, ordinal, Arc::clone(&state)));
    }
}

async fn connection(mut stream: TcpStream, ordinal: usize, state: Arc<State>) {
    let mut decoder = Decoder::new();
    let mut buffer = vec![0_u8; 64 * 1024];
    let mut authenticated = state.password.is_none();
    loop {
        let read = match stream.read(&mut buffer).await {
            Ok(0) | Err(_) => return,
            Ok(read) => read,
        };
        decoder.push(&buffer[..read]);
        loop {
            let frame = match decoder.next_frame() {
                Ok(Some(frame)) => frame,
                Ok(None) => break,
                Err(error) => {
                    let reply = Frame::Error(format!("ERR Protocol error: {error}"));
                    let _ = stream.write_all(&reply.encode()).await;
                    return;
                }
            };
            let Some(args) = command_args(frame) else {
                let reply = Frame::Error("ERR Protocol error: expected an array".to_owned());
                if stream.write_all(&reply.encode()).await.is_err() {
                    return;
                }
                continue;
            };
            state.record(ordinal, &args);
            let reply = state.answer(&args, &mut authenticated);
            if stream.write_all(&reply.encode()).await.is_err() {
                return;
            }
        }
    }
}

// a command is an array of bulk strings, its name first
fn command_args(frame: Frame) -> Option<Vec<Vec<u8>>> {
    let items = frame.into_array().ok()?;
    let args: Option<Vec<Vec<u8>>> =
        items.into_iter().map(|item| item.into_bulk().ok().flatten()).collect();
    args.filter(|args| !args.is_empty())
}

fn command_name(args: &[Vec<u8>]) -> String {
    String::from_utf8_lossy(&args[0]).to_ascii_uppercase()
}

// the key a command addresses: its first argument, or EVAL's first KEYS entry
fn key_of(args: &[Vec<u8>]) -> Option<&[u8]> {
    match command_name(args).as_str() {
        "EVAL" => args.get(3).map(Vec::as_slice),
        "AUTH" | "SCAN" | "PING" | "CLIENT" => None,
        _ => args.get(1).map(Vec::as_slice),
    }
}

impl State {
    fn record(&self, connection: usize, args: &[Vec<u8>]) {
        self.commands.lock().expect("commands lock").push(Recorded {
            connection,
            args: args.to_vec(),
        });
    }

    // the AUTH gate first, as Redis applies it, then the command
    fn answer(&self, args: &[Vec<u8>], authenticated: &mut bool) -> Frame {
        if command_name(args) == "AUTH" {
            return match (&self.password, &args[1..]) {
                (None, _) => Frame::Error(
                    "ERR AUTH <password> called without any password configured for the \
                     default user. Are you sure your configuration is correct?"
                        .to_owned(),
                ),
                (Some(password), [given]) if given == password.as_bytes() => {
                    *authenticated = true;
                    Frame::Simple("OK".to_owned())
                }
                (Some(_), _) => Frame::Error(
                    "WRONGPASS invalid username-password pair or user is disabled.".to_owned(),
                ),
            };
        }
        if !*authenticated {
            return Frame::Error("NOAUTH Authentication required.".to_owned());
        }
        self.execute(args)
    }

    fn execute(&self, args: &[Vec<u8>]) -> Frame {
        let name = command_name(args);
        let data = &mut *self.data.lock().expect("data lock");
        match (name.as_str(), &args[1..]) {
            ("PING", []) => Frame::Simple("PONG".to_owned()),
            ("CLIENT", [sub, ..]) if sub.eq_ignore_ascii_case(b"SETINFO") => {
                Frame::Simple("OK".to_owned())
            }
            ("GET", [key]) => Frame::Bulk(live(data, key).map(|entry| entry.value.clone())),
            ("SET", [key, value, options @ ..]) => match set_options(options) {
                Ok(ttl) => {
                    let expires = match ttl {
                        Ttl::None => None,
                        Ttl::After(duration) => Some(Instant::now() + duration),
                        Ttl::Keep => live(data, key).and_then(|entry| entry.expires),
                    };
                    data.insert(
                        key.clone(),
                        Entry {
                            value: value.clone(),
                            expires,
                        },
                    );
                    Frame::Simple("OK".to_owned())
                }
                Err(error) => Frame::Error(error),
            },
            ("DEL", keys) if !keys.is_empty() => {
                let removed = keys
                    .iter()
                    .filter(|key| {
                        let existed = live(data, key).is_some();
                        data.remove(key.as_slice());
                        existed
                    })
                    .count();
                Frame::Integer(i64::try_from(removed).expect("a small count"))
            }
            ("EXISTS", [key]) => Frame::Integer(i64::from(live(data, key).is_some())),
            ("SCAN", [_cursor, options @ ..]) => scan(data, options),
            ("EVAL", [script, numkeys, key, has_expected, expected, value])
                if numkeys.as_slice() == b"1" =>
            {
                if script.as_slice() == CAS_SCRIPT.as_bytes() {
                    cas(data, key, has_expected.as_slice() == b"1", expected, value)
                } else {
                    Frame::Error("ERR unknown script".to_owned())
                }
            }
            _ => {
                Frame::Error(format!("ERR unknown command '{name}', or wrong number of arguments"))
            }
        }
    }
}

// the entry under `key` if it has not expired; an expired one is dropped on the way
fn live<'a>(data: &'a mut Data, key: &[u8]) -> Option<&'a Entry> {
    let expired =
        data.get(key).is_some_and(|entry| entry.expires.is_some_and(|at| at <= Instant::now()));
    if expired {
        data.remove(key);
    }
    data.get(key)
}

enum Ttl {
    None,
    After(Duration),
    Keep,
}

fn set_options(options: &[Vec<u8>]) -> Result<Ttl, String> {
    let mut ttl = Ttl::None;
    let mut options = options.iter();
    while let Some(option) = options.next() {
        let unit = match String::from_utf8_lossy(option).to_ascii_uppercase().as_str() {
            "KEEPTTL" => {
                ttl = Ttl::Keep;
                continue;
            }
            "PX" => Duration::from_millis,
            "EX" => Duration::from_secs,
            other => return Err(format!("ERR syntax error: {other}")),
        };
        let amount = options
            .next()
            .and_then(|amount| String::from_utf8_lossy(amount).parse::<u64>().ok())
            .ok_or_else(|| "ERR value is not an integer or out of range".to_owned())?;
        if amount == 0 {
            return Err("ERR invalid expire time in 'set' command".to_owned());
        }
        ttl = Ttl::After(unit(amount));
    }
    Ok(ttl)
}

// one page holding every live key the `MATCH prefix*` pattern admits
fn scan(data: &mut Data, options: &[Vec<u8>]) -> Frame {
    let pattern = options
        .chunks(2)
        .find(|pair| String::from_utf8_lossy(&pair[0]).eq_ignore_ascii_case("MATCH"))
        .and_then(|pair| pair.get(1))
        .map_or(b"*".as_slice(), Vec::as_slice);
    let Some(prefix) = pattern.strip_suffix(b"*") else {
        return Frame::Error("ERR the fake matches `prefix*` patterns only".to_owned());
    };
    let now = Instant::now();
    data.retain(|_, entry| entry.expires.is_none_or(|at| at > now));
    let mut keys: Vec<&Vec<u8>> = data.keys().filter(|key| key.starts_with(prefix)).collect();
    keys.sort();
    let keys = keys.into_iter().map(|key| Frame::Bulk(Some(key.clone()))).collect();
    Frame::Array(Some(vec![Frame::Bulk(Some(b"0".to_vec())), Frame::Array(Some(keys))]))
}

// the CAS script, natively: swap when the current value is the expected one, keeping the TTL
fn cas(data: &mut Data, key: &[u8], has_expected: bool, expected: &[u8], value: &[u8]) -> Frame {
    let current = live(data, key).map(|entry| (entry.value.clone(), entry.expires));
    let matches = match &current {
        Some((observed, _)) => has_expected && observed.as_slice() == expected,
        None => !has_expected,
    };
    if matches {
        let expires = current.and_then(|(_, expires)| expires);
        data.insert(
            key.to_vec(),
            Entry {
                value: value.to_vec(),
                expires,
            },
        );
        return cas_reply(1, 0, Vec::new());
    }
    match current {
        Some((observed, _)) => cas_reply(0, 1, observed),
        None => cas_reply(0, 0, Vec::new()),
    }
}

fn cas_reply(swapped: i64, present: i64, observed: Vec<u8>) -> Frame {
    Frame::Array(Some(vec![
        Frame::Integer(swapped),
        Frame::Integer(present),
        Frame::Bulk(Some(observed)),
    ]))
}
