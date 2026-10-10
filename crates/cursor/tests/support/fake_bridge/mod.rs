//! A protocol-faithful fake `cursor-sdk-bridge`: bearer-checked Connect JSON,
//! `agent-<n>` ids counted per process so two processes hand out the same
//! id, `Send` as an enveloped run stream, `CallCustomTool` posted back to
//! the client's own endpoint — and nothing else.
//!
//! [`Spawnable`] stands it up as the process the client spawns: a home
//! holding the [`Config`] (script and faults), the shared JSONL log every
//! spawned process appends to, and a `cursor-sdk-bridge` symlink to the
//! `fake-cursor-sdk-bridge` binary (`main.rs`, this same module), put first
//! on the test process's `PATH`. A [`Fault::Park`] holds a request until the
//! test releases it: the process records the park and waits for the release
//! count the test writes beside the log to pass its ticket. The suites' side
//! — [`Spawnable`] and the liveness probe — is `cfg(test)`, since the binary
//! is built against the crate's `[dependencies]` alone.

mod log;
// The crate's own callback codec, by path: one codec on both sides of the
// callback keeps the fake honest.
#[path = "../../../src/endpoint/proto.rs"]
mod proto;
mod server;

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Once};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::net::TcpListener;

use self::log::Kind;
#[cfg(test)]
pub use self::log::alive;
#[allow(unused_imports, reason = "the suites' side of the module")]
pub use self::log::{Event, History, Log, Process, Rpc};
use self::server::{Callback, Server};

const SCRIPT_FILE: &str = "script.json";
const LOG_FILE: &str = "log.jsonl";
const RELEASES_FILE: &str = "releases.json";
// The name the client spawns a worker by.
const BIN_NAME: &str = "cursor-sdk-bridge";
// A secret the test process carries, as a co-located backend's credential
// would be, for a row to assert it never reaches a worker.
pub const PLANTED_SECRET: &str = "OMNIA_TEST_SECRET";

/// Where in an agent's lifecycle a [`Fault::Park`] or [`Fault::Hang`] holds the request.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Point {
    /// The handshake's first RPC (hang only).
    Ping,
    /// The graceful exit request (hang only).
    Shutdown,
    CreateAgent,
    /// The `Send` RPC itself, before its stream opens: no run id yet.
    Send,
    /// Mid-run: the stream is open and its first event, carrying the run id, is out.
    Stream,
    CloseAgent,
    DeleteAgent,
}

/// One way the fake misbehaves.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Fault {
    /// Hold requests at `Point` until the test releases them.
    Park(Point),
    /// Hold requests at `Point` forever.
    Hang(Point),
    /// `exit(EXIT_ON_CREATE)` as the nth `CreateAgent` (1-based) begins.
    ExitOnCreate(usize),
    /// Two marker lines to stderr, then `SIGKILL` ourselves as the nth `Send` begins.
    KillOnSend(usize),
    /// Open the nth `Send`'s stream, deliver the run id, then reset it.
    ResetStream(usize),
    /// Two marker lines to stderr, then exit with this code before the ready line.
    ExitBeforeReady(i32),
    /// Never print a ready line; sleep.
    NeverReady,
    /// Print a ready line naming a port nothing listens on.
    ReadyThenRefused,
    /// Print a ready line naming this URL instead of the bound one.
    ReadyUrl(String),
    /// Print the ready line with the token inline (`authToken`), not in a file.
    InlineToken,
    /// `CreateAgent` answers `agentId: ""`.
    EmptyId,
    /// `CloseAgent` fails `500 internal`.
    CloseFails,
    /// Answer `Shutdown`, then stay up this many milliseconds: the slot stays held.
    LingerOnShutdown(u64),
    /// Hold the run's answer until another spawned process has recorded this RPC.
    WaitForPeer(Rpc),
    /// Fork a `sleep` inheriting our stderr before the ready line, and leave it running.
    Grandchild,
}

/// A fault and the process it targets: 1-based in start order, or every process when unset.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Injected {
    pub fault: Fault,
    pub process: Option<usize>,
}

impl Injected {
    pub fn applies(&self, process: usize) -> bool {
        self.process.is_none_or(|selected| selected == process)
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
    /// `Send` number n on an agent is answered with `replies[n]`; the last reply repeats.
    Replies(Vec<String>),
    /// POST `CallCustomTool` for `name` to the callback endpoint and answer with its result.
    Tool { name: String, args: Value, codec: Codec },
    /// One activity frame every `every_ms` for `frames` frames, then finish as `Echo` or hang.
    Paced { every_ms: u64, frames: usize, then: Then },
    /// One `tool_call` frame starting each call, as the bridge spells it, then silence.
    Started(Vec<Call>),
}

/// A tool call the fake starts and never ends.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Call {
    pub name: String,
    pub args: Value,
}

/// The script and faults one fake process runs with.
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

    /// A run that starts the tool `name` with `args` and never ends it.
    pub fn started(name: &str, args: Value) -> Self {
        Self {
            script: Script::Started(vec![Call {
                name: name.to_owned(),
                args,
            }]),
            faults: Vec::new(),
        }
    }

    /// A further tool the run starts beside the first, in the same silence.
    #[must_use]
    pub fn beside(mut self, name: &str, args: Value) -> Self {
        if let Script::Started(calls) = &mut self.script {
            calls.push(Call {
                name: name.to_owned(),
                args,
            });
        }
        self
    }

    /// The tool script's codec.
    #[must_use]
    pub const fn codec(mut self, codec: Codec) -> Self {
        if let Script::Tool { codec: current, .. } = &mut self.script {
            *current = codec;
        }
        self
    }

    /// A fault for every spawned process.
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

/// Give the client a `CURSOR_API_KEY` when the environment has none, and
/// plant [`PLANTED_SECRET`] beside it; once per process, before any runtime
/// thread exists.
#[expect(unsafe_code, reason = "the key is read from the process environment")]
pub fn dummy_key() {
    static SET: Once = Once::new();
    SET.call_once(|| {
        if std::env::var_os("CURSOR_API_KEY").is_none() {
            // SAFETY: set once, never unset, before the tests spawn threads.
            unsafe { std::env::set_var("CURSOR_API_KEY", "test-key") }
        }
        // SAFETY: as above.
        unsafe { std::env::set_var(PLANTED_SECRET, "hunter2") }
    });
}

/// The fake as the process the client spawns: a home holding the script, log
/// and releases, and the `cursor-sdk-bridge` link on this test process's `PATH`.
#[cfg(test)]
pub struct Spawnable {
    home: tempfile::TempDir,
}

#[cfg(test)]
impl Spawnable {
    /// Lay out `config` and put the fake first on `PATH`, where a spawned fake
    /// finds its home; `PATH` is process-wide, so one `Spawnable` per test
    /// process (nextest's one test each).
    #[expect(unsafe_code, reason = "the client finds the bridge through PATH")]
    pub fn new(config: &Config) -> Self {
        let home =
            tempfile::Builder::new().prefix("fake-bridge-").tempdir().expect("a home directory");
        std::fs::write(
            home.path().join(SCRIPT_FILE),
            serde_json::to_vec_pretty(config).expect("a config serializes"),
        )
        .expect("writing the script");
        std::os::unix::fs::symlink(
            env!("CARGO_BIN_EXE_fake-cursor-sdk-bridge"),
            home.path().join(BIN_NAME),
        )
        .expect("linking the fake binary");

        let mut path = home.path().as_os_str().to_owned();
        if let Some(rest) = std::env::var_os("PATH") {
            path.push(":");
            path.push(rest);
        }
        // SAFETY: set before the test spawns any thread of its own, and read
        // only by the client's spawns and their children from here on.
        unsafe { std::env::set_var("PATH", path) };
        Self { home }
    }

    /// Everything every process recorded so far.
    pub fn log(&self) -> Log {
        Log::read(&self.home.path().join(LOG_FILE))
    }

    /// Requests held at `point` right now: parked and not yet released; a run
    /// that left a `Stream` park through `CancelRun` still counts.
    pub fn parked(&self, point: Point) -> usize {
        self.log().parked(point).saturating_sub(self.released().count(point))
    }

    /// Wait until `count` requests are held at `point`; the bound is generous
    /// because a guest's component is compiled on the way here.
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

    /// Release the earliest request held at `point`; `false` when none is.
    pub fn release_one(&self, point: Point) -> bool {
        let mut releases = self.released();
        if releases.count(point) >= self.log().parked(point) {
            return false;
        }
        releases.release(point, 1);
        releases.write(&self.releases_path());
        true
    }

    pub fn release_all(&self) {
        let log = self.log();
        let mut releases = self.released();
        for point in log.parked_points() {
            let held = log.parked(point).saturating_sub(releases.count(point));
            releases.release(point, held);
        }
        releases.write(&self.releases_path());
    }

    fn released(&self) -> Releases {
        Releases::read(&self.releases_path())
    }

    fn releases_path(&self) -> PathBuf {
        self.home.path().join(RELEASES_FILE)
    }
}

/// How many parked requests at each point the test has released, in
/// ticket order; the test writes it, every spawned process polls it.
#[derive(Debug, Default, Serialize, Deserialize)]
pub struct Releases(BTreeMap<String, usize>);

impl Releases {
    pub fn read(path: &Path) -> Self {
        std::fs::read(path)
            .ok()
            .and_then(|bytes| serde_json::from_slice(&bytes).ok())
            .unwrap_or_default()
    }

    pub fn count(&self, point: Point) -> usize {
        self.0.get(&format!("{point:?}")).copied().unwrap_or(0)
    }

    fn release(&mut self, point: Point, count: usize) {
        *self.0.entry(format!("{point:?}")).or_insert(0) += count;
    }

    // Written whole and renamed into place, so a process never reads half.
    fn write(&self, path: &Path) {
        let staged = path.with_extension("json.tmp");
        std::fs::write(&staged, serde_json::to_vec(self).expect("releases serialize"))
            .expect("writing the releases");
        std::fs::rename(&staged, path).expect("publishing the releases");
    }
}

/// Where the spawned binary finds its script, log, and releases.
pub struct Home(PathBuf);

impl Home {
    /// Where the client found us: the first directory on `PATH` holding a
    /// `cursor-sdk-bridge`, which `Spawnable::new` laid the link in. The
    /// client passes a worker `PATH` and nothing of its own, so no variable
    /// of the fake's could carry the home.
    pub fn on_path() -> Self {
        let path = std::env::var_os("PATH").expect("PATH reaches the worker");
        let home = std::env::split_paths(&path)
            .find(|dir| dir.join(BIN_NAME).exists())
            .expect("the fake is on PATH");
        Self(home)
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

    pub fn releases_path(&self) -> PathBuf {
        self.0.join(RELEASES_FILE)
    }
}

/// The spawned binary's whole life: number ourselves through the log,
/// handshake or fail it as scripted, serve until `Shutdown`.
pub async fn run_spawned(args: Vec<String>) {
    let home = Home::on_path();
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
    let server = Server::new(config, process, callback, recorder, home.releases_path());

    // the fake's own token rides in the ready event, for the row proving it is never logged
    ready_event["token"] = Value::String(server.token().to_owned());

    // a child in our group holding the stderr pipe, as a real agent would, that nothing reaps
    #[allow(clippy::zombie_processes, reason = "the fault is a child nobody waits for")]
    if server.has(&Fault::Grandchild) {
        let child = std::process::Command::new("sleep")
            .arg("600")
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::null())
            .spawn()
            .expect("forking a grandchild");
        server.recorder().record(Kind::Forked, None, json!({ "pid": child.id() }));
    }

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
    server::stderr_line(format_args!("cursor-sdk-bridge ready {ready}"));
    server.recorder().record(Kind::Ready, None, ready_event);

    server.shutdown_requested().await;

    // let the shutdown reply reach the client before exiting, or linger as scripted
    let linger = server
        .find(|fault| match fault {
            Fault::LingerOnShutdown(ms) => Some(*ms),
            _ => None,
        })
        .unwrap_or(0);
    tokio::time::sleep(Duration::from_millis(linger.max(50))).await;
    std::process::exit(0);
}

/// Poll `ready` every few milliseconds until it holds, panicking naming `what`
/// once `within` passes.
pub async fn poll(mut ready: impl FnMut() -> bool, within: Duration, what: &str) {
    let deadline = tokio::time::Instant::now() + within;
    while !ready() {
        assert!(tokio::time::Instant::now() < deadline, "timed out waiting for {what}");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}
