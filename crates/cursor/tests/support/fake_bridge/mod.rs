//! A protocol-faithful fake `cursor-sdk-bridge`, in two mounts.
//!
//! [`FakeBridge::serve`] binds it in-process on loopback for attach mode:
//! the test holds the server directly and can release requests it has
//! parked. [`Spawnable`] stands it up as the `fake-cursor-sdk-bridge`
//! binary (`main.rs`, this same module) for spawn mode: the client starts
//! one process per lease, each does the ready-line handshake, and all of
//! them append to one JSONL log the test folds back into per-process
//! histories. Faults and the reply script are one [`Config`], written as a
//! file beside the binary the client is pointed at.
//!
//! The fake answers `sdk.v1` the way the real bridge does — bearer-checked
//! Connect JSON, `agent-<n>` ids counted per process so two processes hand
//! out the same id, `Send` as an enveloped run stream, `CallCustomTool`
//! posted back to the client's own endpoint — and nothing else.

pub mod log;
mod proto;
mod server;

use std::path::{Path, PathBuf};
use std::sync::{Arc, Once};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::net::TcpListener;
use tokio::task::JoinHandle;

#[allow(unused_imports, reason = "the suites' side of the module")]
pub use self::log::{Event, History, Kind, Log, Process, Rpc};
use self::server::{Callback, Server};
#[allow(unused_imports, reason = "the suites' side of the module")]
pub use self::server::{EXIT_ON_CREATE, MARKERS};

const SCRIPT_FILE: &str = "script.json";
const LOG_FILE: &str = "log.jsonl";
const BIN_NAME: &str = "fake-cursor-sdk-bridge";

/// Where in an agent's lifecycle a [`Fault::Park`] or [`Fault::Hang`]
/// holds the request.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Point {
    /// The handshake's first RPC (hang only).
    Ping,
    /// The graceful exit request (hang only).
    Shutdown,
    CreateAgent,
    /// The `Send` RPC itself, before its stream opens: no run id exists.
    Send,
    /// Mid-run: the stream is open and its first event, carrying the run
    /// id, has gone out.
    Stream,
    CloseAgent,
    DeleteAgent,
}

/// One way the fake misbehaves.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Fault {
    /// Hold requests at `Point` until the test releases them (in-process).
    Park(Point),
    /// Hold requests at `Point` forever.
    Hang(Point),
    /// `exit(EXIT_ON_CREATE)` as the nth `CreateAgent` (1-based) begins.
    ExitOnCreate(usize),
    /// Two marker lines to stderr, then `SIGKILL` ourselves as the nth
    /// `Send` begins.
    KillOnSend(usize),
    /// Open the nth `Send`'s stream, deliver the run id, then reset it.
    ResetStream(usize),
    /// Two marker lines to stderr, then exit with this code before the
    /// ready line.
    ExitBeforeReady(i32),
    /// Never print a ready line; sleep.
    NeverReady,
    /// Print a ready line naming a port nothing listens on.
    ReadyThenRefused,
    /// Print a ready line naming this URL instead of the bound one.
    ReadyUrl(String),
    /// Print the ready line with the token inline (`authToken`) rather
    /// than in a file.
    InlineToken,
    /// `CreateAgent` answers `agentId: ""`.
    EmptyId,
    /// `CloseAgent` fails `500 internal`.
    CloseFails,
    /// Answer `Shutdown`, then stay up this many milliseconds before
    /// exiting: the lease's slot stays held for that long.
    LingerOnShutdown(u64),
}

/// A fault and the spawned process it targets: 1-based in start order
/// (`Client::connect`'s probe is process 0), or every process but the
/// probe when unset. In-process, only unselected faults apply.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Injected {
    pub fault: Fault,
    pub process: Option<usize>,
}

impl Injected {
    pub const fn applies(&self, process: Option<usize>) -> bool {
        match (self.process, process) {
            (None, None) => true,
            (None, Some(number)) => number != 0,
            (Some(selected), Some(number)) => selected == number,
            (Some(_), None) => false,
        }
    }
}

/// How the fake frames its `CallCustomTool` POST.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum Codec {
    #[default]
    Json,
    /// JSON split across chunks, as a streaming callback client frames it.
    JsonChunked,
    Proto,
}

/// What a [`Script::Paced`] run does once its frames are out.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Then {
    Finish,
    Hang,
}

/// How the fake answers each `Send`.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum Script {
    /// The last user turn of the prompt comes back as the answer.
    #[default]
    Echo,
    /// `Send` number n on an agent is answered with `replies[n]`; the last
    /// reply repeats.
    Replies(Vec<String>),
    /// POST `CallCustomTool` for `name` to the callback endpoint and answer
    /// with the tool's result.
    Tool { name: String, args: Value, codec: Codec },
    /// Stream one activity frame every `every_ms` for `frames` frames, then
    /// finish (as `Echo`) or hang.
    Paced { every_ms: u64, frames: usize, then: Then },
}

/// The script and faults one fake — one process, or the in-process server
/// — runs with.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Config {
    pub script: Script,
    pub faults: Vec<Injected>,
}

impl Config {
    pub fn echo() -> Self {
        Self::default()
    }

    pub fn replies<S: Into<String>>(replies: impl IntoIterator<Item = S>) -> Self {
        Self {
            script: Script::Replies(replies.into_iter().map(Into::into).collect()),
            faults: Vec::new(),
        }
    }

    /// A tool script calling `name` with `{}` in the JSON codec.
    pub fn tool(name: &str) -> Self {
        Self {
            script: Script::Tool {
                name: name.to_owned(),
                args: json!({}),
                codec: Codec::Json,
            },
            faults: Vec::new(),
        }
    }

    pub const fn paced(every_ms: u64, frames: usize, then: Then) -> Self {
        Self {
            script: Script::Paced {
                every_ms,
                frames,
                then,
            },
            faults: Vec::new(),
        }
    }

    /// The tool script's codec.
    #[must_use]
    pub const fn codec(mut self, codec: Codec) -> Self {
        if let Script::Tool { codec: current, .. } = &mut self.script {
            *current = codec;
        }
        self
    }

    /// A fault for every process but the probe (and the in-process server).
    #[must_use]
    pub fn fault(mut self, fault: Fault) -> Self {
        self.faults.push(Injected { fault, process: None });
        self
    }

    /// A fault for spawned process `process` alone.
    #[must_use]
    pub fn fault_on(mut self, process: usize, fault: Fault) -> Self {
        self.faults.push(Injected {
            fault,
            process: Some(process),
        });
        self
    }
}

/// Give the client the `CURSOR_API_KEY` it reads — at connect, in
/// `CreateAgent`, in `DeleteAgent` — when the environment has none. Once
/// per process, before any runtime thread exists.
pub fn dummy_key() {
    static SET: Once = Once::new();
    SET.call_once(|| {
        if std::env::var_os("CURSOR_API_KEY").is_none() {
            // SAFETY: set once, never unset, before the tests spawn threads.
            unsafe { std::env::set_var("CURSOR_API_KEY", "test-key") }
        }
    });
}

/// The fake served in-process on loopback: the attach-mode bridge, with
/// the test holding the server's parks and record directly.
pub struct FakeBridge {
    server: Arc<Server>,
    url: String,
    serving: JoinHandle<()>,
}

impl FakeBridge {
    /// Bind `127.0.0.1:0` and serve `config`.
    pub async fn serve(config: Config) -> Self {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.expect("bind loopback");
        let url = format!("http://{}", listener.local_addr().expect("local address"));
        let server = Server::new(config, None, None, log::Recorder::in_memory());
        let serving = tokio::spawn(Arc::clone(&server).serve(listener));
        Self { server, url, serving }
    }

    pub fn url(&self) -> &str {
        &self.url
    }

    pub fn token(&self) -> &str {
        self.server.token()
    }

    /// Everything recorded so far.
    pub fn log(&self) -> Log {
        Log::from_events(self.server.recorder().events())
    }

    /// Requests held at `point` right now.
    pub fn parked(&self, point: Point) -> usize {
        self.server.parks().parked(point)
    }

    /// Wait until `count` requests are held at `point`. The bound is
    /// generous: a guest's component is compiled on the way here, under
    /// whatever load the rest of the suite puts on the machine.
    ///
    /// # Panics
    ///
    /// Panics when they have not arrived in time.
    pub async fn await_parked(&self, point: Point, count: usize) {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
        while self.parked(point) < count {
            assert!(
                tokio::time::Instant::now() < deadline,
                "timed out waiting for {count} request(s) parked at {point:?}; have {} — {}",
                self.parked(point),
                self.log().summary()
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    /// Release the earliest request held at `point`.
    pub fn release_one(&self, point: Point) -> bool {
        self.server.parks().release_one(point)
    }

    pub fn release_all(&self) {
        self.server.parks().release_all();
    }
}

impl Drop for FakeBridge {
    fn drop(&mut self) {
        self.serving.abort();
    }
}

/// The fake as a binary the client spawns: a directory holding the script,
/// the shared log, and a link to `fake-cursor-sdk-bridge`, which finds
/// both beside the path it was started through.
pub struct Spawnable {
    home: tempfile::TempDir,
}

impl Spawnable {
    /// Lay out `config` for the binary to pick up.
    #[allow(
        clippy::option_env_unwrap,
        reason = "set for the suites; unset in the binary's own build, which never gets here"
    )]
    pub fn new(config: &Config) -> Self {
        let home =
            tempfile::Builder::new().prefix("fake-bridge-").tempdir().expect("a home directory");
        std::fs::write(
            home.path().join(SCRIPT_FILE),
            serde_json::to_vec_pretty(config).expect("a config serializes"),
        )
        .expect("writing the script");
        let target = option_env!("CARGO_BIN_EXE_fake-cursor-sdk-bridge")
            .expect("the fake binary is built alongside the suites (feature `fake-bridge`)");
        std::os::unix::fs::symlink(target, home.path().join(BIN_NAME))
            .expect("linking the fake binary");
        Self { home }
    }

    /// What `bridge_bin` is set to.
    pub fn bin(&self) -> String {
        self.home.path().join(BIN_NAME).to_string_lossy().into_owned()
    }

    /// Everything every process recorded so far.
    pub fn log(&self) -> Log {
        Log::read(&self.home.path().join(LOG_FILE))
    }
}

/// Where the spawned binary finds its script and log: beside the path it
/// was started through.
pub struct Home(PathBuf);

impl Home {
    /// From `argv[0]`.
    pub fn of(program: &str) -> Self {
        let path = Path::new(program);
        Self(path.parent().map_or_else(|| PathBuf::from("."), Path::to_path_buf))
    }

    /// The config, or a plain echo when no script was laid out.
    pub fn config(&self) -> Config {
        std::fs::read(self.0.join(SCRIPT_FILE))
            .ok()
            .and_then(|bytes| serde_json::from_slice(&bytes).ok())
            .unwrap_or_default()
    }

    pub fn log_path(&self) -> PathBuf {
        self.0.join(LOG_FILE)
    }
}

/// The spawned binary's whole life: number ourselves through the log,
/// handshake or fail it as scripted, serve until `Shutdown`.
pub async fn run_spawned(args: Vec<String>) {
    let home = Home::of(args.first().map_or(BIN_NAME, String::as_str));
    let config = home.config();
    let recorder = log::Recorder::to_file(&home.log_path());
    let process = recorder.process();

    let mut state_root = None;
    let mut callback_url = None;
    let mut callback_token = None;
    let mut arguments = args.iter().skip(1);
    while let Some(argument) = arguments.next() {
        match argument.as_str() {
            "--state-root" => state_root = arguments.next().cloned(),
            "--tool-callback-url" => callback_url = arguments.next().cloned(),
            "--tool-callback-auth-token" => callback_token = arguments.next().cloned(),
            _ => {}
        }
    }
    let callback = callback_url.zip(callback_token).map(|(url, token)| Callback { url, token });
    let mut ready_event = callback.as_ref().map_or_else(
        || json!({}),
        |callback| json!({ "callbackUrl": callback.url, "callbackToken": callback.token }),
    );
    let server = Server::new(config, Some(process), callback, recorder);
    // The fake's own bearer token: what the ready line carries, for a test
    // proving it never reaches a log.
    ready_event["token"] = Value::String(server.token().to_owned());

    if let Some(code) = server.find(|fault| match fault {
        Fault::ExitBeforeReady(code) => Some(*code),
        _ => None,
    }) {
        server::write_markers();
        std::process::exit(code);
    }
    if server.has(&Fault::NeverReady) {
        std::future::pending::<()>().await;
    }

    let listener = TcpListener::bind(("127.0.0.1", 0)).await.expect("bind loopback");
    let bound = format!("http://{}", listener.local_addr().expect("local address"));
    let url = server
        .find(|fault| match fault {
            Fault::ReadyUrl(url) => Some(url.clone()),
            _ => None,
        })
        .unwrap_or(bound);

    let mut ready = json!({
        "schemaVersion": 1,
        "serverVersion": "fake",
        "pid": std::process::id(),
        "transport": "tcp",
        "protocol": "connect",
        "url": url,
    });
    if server.has(&Fault::InlineToken) {
        ready["authToken"] = Value::String(server.token().to_owned());
    } else {
        let root = state_root.map_or_else(std::env::temp_dir, PathBuf::from);
        std::fs::create_dir_all(&root).expect("the state root exists");
        let token_file = root.join("auth-token");
        std::fs::write(&token_file, server.token()).expect("writing the token file");
        ready["authTokenFile"] = Value::String(token_file.to_string_lossy().into_owned());
    }

    if server.has(&Fault::ReadyThenRefused) {
        drop(listener);
    } else {
        tokio::spawn(Arc::clone(&server).serve(listener));
    }
    eprintln!("cursor-sdk-bridge ready {ready}");
    server.recorder().record(Kind::Ready, None, ready_event);

    server.shutdown_requested().await;
    // Let the `Shutdown` reply reach the client before the process goes —
    // or, when scripted, hold the slot for longer.
    let linger = server
        .find(|fault| match fault {
            Fault::LingerOnShutdown(ms) => Some(*ms),
            _ => None,
        })
        .unwrap_or(0);
    tokio::time::sleep(Duration::from_millis(linger.max(50))).await;
    std::process::exit(0);
}

/// Poll `ready` every few milliseconds until it holds or `within` passes.
///
/// # Panics
///
/// Panics naming `what` when the bound passes first.
pub async fn poll(mut ready: impl FnMut() -> bool, within: Duration, what: &str) {
    let deadline = tokio::time::Instant::now() + within;
    while !ready() {
        assert!(tokio::time::Instant::now() < deadline, "timed out waiting for {what}");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}
