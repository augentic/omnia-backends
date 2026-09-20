//! What the fake records — one event per handshake step, RPC, and tool
//! callback — and the test-side views that fold the record back into
//! per-process histories.
//!
//! In-process the record is a `Vec` behind a mutex. A spawned fake appends
//! JSONL lines to the file `FAKE_BRIDGE_LOG` names under `flock`, so several
//! processes share one log and number themselves through it: the probe
//! `Client::connect` spawns is process 0, each lease's process counts up
//! from 1.

use std::collections::HashSet;
use std::fs::{File, OpenOptions};
use std::io::Write as _;
use std::os::fd::AsRawFd as _;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, PoisonError};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

/// One `sdk.v1` procedure the fake answers.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Rpc {
    Ping,
    GetVersion,
    Shutdown,
    CreateAgent,
    Send,
    CancelRun,
    CloseAgent,
    DeleteAgent,
}

/// What an [`Event`] records.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Kind {
    /// The process started and claimed its number.
    Started,
    /// The ready line went out; `arg` carries the callback URL and token
    /// the process was started with.
    Ready,
    /// One RPC arrived (recorded on arrival, before any hang or park).
    Rpc(Rpc),
    /// The fake posted `CallCustomTool`; `arg` carries the status and the
    /// bearer token it used.
    Callback,
}

/// One recorded moment, timestamped in epoch microseconds so events from
/// different processes order against each other and against the test.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Event {
    pub pid: u32,
    pub process: usize,
    pub ts: u64,
    pub kind: Kind,
    pub agent: Option<String>,
    pub arg: Value,
}

impl Event {
    pub const fn rpc(&self) -> Option<Rpc> {
        match self.kind {
            Kind::Rpc(rpc) => Some(rpc),
            _ => None,
        }
    }

    pub fn at(&self) -> SystemTime {
        UNIX_EPOCH + Duration::from_micros(self.ts)
    }

    /// A string field of `arg`.
    pub fn text(&self, key: &str) -> &str {
        self.arg.get(key).and_then(Value::as_str).unwrap_or_default()
    }
}

pub fn now_micros() -> u64 {
    let since = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default();
    u64::try_from(since.as_micros()).unwrap_or(u64::MAX)
}

/// The fake's side of the record.
pub struct Recorder {
    pid: u32,
    process: usize,
    memory: Mutex<Vec<Event>>,
    file: Option<PathBuf>,
}

impl Recorder {
    /// An in-process record, as process 0.
    pub fn in_memory() -> Self {
        Self {
            pid: std::process::id(),
            process: 0,
            memory: Mutex::new(Vec::new()),
            file: None,
        }
    }

    /// A file-backed record for a spawned process, which claims the next
    /// process number under the log's lock and announces itself.
    pub fn to_file(path: &Path) -> Self {
        let pid = std::process::id();
        let file = lock(path);
        let started = Log::read(path).events.iter().filter(|e| e.kind == Kind::Started).count();
        let recorder = Self {
            pid,
            process: started,
            memory: Mutex::new(Vec::new()),
            file: Some(path.to_path_buf()),
        };
        let event = recorder.event(Kind::Started, None, Value::Null);
        append(&file, &event);
        recorder.memory.lock().unwrap_or_else(PoisonError::into_inner).push(event);
        drop(file);
        recorder
    }

    pub const fn process(&self) -> usize {
        self.process
    }

    pub fn record(&self, kind: Kind, agent: Option<&str>, arg: Value) {
        let event = self.event(kind, agent, arg);
        if let Some(path) = &self.file {
            append(&lock(path), &event);
        }
        self.memory.lock().unwrap_or_else(PoisonError::into_inner).push(event);
    }

    pub fn events(&self) -> Vec<Event> {
        self.memory.lock().unwrap_or_else(PoisonError::into_inner).clone()
    }

    fn event(&self, kind: Kind, agent: Option<&str>, arg: Value) -> Event {
        Event {
            pid: self.pid,
            process: self.process,
            ts: now_micros(),
            kind,
            agent: agent.map(ToOwned::to_owned),
            arg,
        }
    }
}

// Open the log for appending and take its exclusive lock; the lock is
// released when the returned handle drops.
fn lock(path: &Path) -> File {
    let file =
        OpenOptions::new().create(true).append(true).open(path).unwrap_or_else(|error| {
            panic!("opening the fake bridge log {}: {error}", path.display())
        });
    // SAFETY: `flock` on a descriptor this handle owns; no memory is involved.
    let locked = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX) };
    assert_eq!(locked, 0, "locking the fake bridge log");
    file
}

fn append(mut file: &File, event: &Event) {
    let mut line = serde_json::to_vec(event).expect("an event serializes");
    line.push(b'\n');
    file.write_all(&line).expect("appending to the fake bridge log");
    file.flush().expect("flushing the fake bridge log");
}

/// Every event recorded, from memory or from the shared log file.
#[derive(Clone, Debug, Default)]
pub struct Log {
    pub events: Vec<Event>,
}

impl Log {
    /// Read the JSONL log; a line still being written is skipped.
    pub fn read(path: &Path) -> Self {
        let contents = std::fs::read_to_string(path).unwrap_or_default();
        Self {
            events: contents.lines().filter_map(|line| serde_json::from_str(line).ok()).collect(),
        }
    }

    pub const fn from_events(events: Vec<Event>) -> Self {
        Self { events }
    }

    /// The processes that wrote to the log, by number.
    pub fn processes(&self) -> Vec<Process> {
        let mut numbers: Vec<usize> = self.events.iter().map(|e| e.process).collect();
        numbers.sort_unstable();
        numbers.dedup();
        numbers.into_iter().filter_map(|number| self.process(number)).collect()
    }

    pub fn process(&self, number: usize) -> Option<Process> {
        let events: Vec<Event> =
            self.events.iter().filter(|e| e.process == number).cloned().collect();
        let pid = events.first()?.pid;
        Some(Process { number, pid, events })
    }

    /// Every process but the probe (`Client::connect`'s handshake spawn).
    pub fn workers(&self) -> Vec<Process> {
        self.processes().into_iter().filter(|process| process.number != 0).collect()
    }
}

/// One spawned process's history (the whole record in-process).
#[derive(Clone, Debug)]
pub struct Process {
    pub number: usize,
    pub pid: u32,
    pub events: Vec<Event>,
}

impl Process {
    /// Whether the process's last recorded RPC was `rpc`.
    pub fn ended_with(&self, rpc: Rpc) -> bool {
        self.last_rpc() == Some(rpc)
    }

    pub fn last_rpc(&self) -> Option<Rpc> {
        self.events.iter().rev().find_map(Event::rpc)
    }

    /// Whether a process with this pid still exists.
    pub fn alive(&self) -> bool {
        let Ok(pid) = i32::try_from(self.pid) else {
            return false;
        };
        // SAFETY: signal 0 probes for existence and delivers nothing.
        unsafe { libc::kill(pid, 0) == 0 }
    }

    /// The callback URL and bearer token the process was started with.
    pub fn ready(&self) -> Option<(String, String)> {
        let ready = self.events.iter().find(|e| e.kind == Kind::Ready)?;
        Some((ready.text("callbackUrl").to_owned(), ready.text("callbackToken").to_owned()))
    }

    /// The process's own bearer token, as its ready line carried it.
    pub fn bridge_token(&self) -> Option<String> {
        let ready = self.events.iter().find(|e| e.kind == Kind::Ready)?;
        Some(ready.text("token").to_owned())
    }
}

/// Queries shared by the whole log and one process's slice of it.
pub trait History {
    fn history(&self) -> &[Event];

    fn saw(&self, rpc: Rpc) -> Vec<&Event> {
        self.history().iter().filter(|e| e.rpc() == Some(rpc)).collect()
    }

    fn count(&self, rpc: Rpc) -> usize {
        self.saw(rpc).len()
    }

    /// The RPCs that named `agent`, in arrival order.
    fn sequence(&self, agent: &str) -> Vec<Rpc> {
        self.history()
            .iter()
            .filter(|e| e.agent.as_deref() == Some(agent))
            .filter_map(Event::rpc)
            .collect()
    }

    /// Every agent id a `CreateAgent` handed out, in order.
    fn agents(&self) -> Vec<String> {
        self.saw(Rpc::CreateAgent).into_iter().filter_map(|e| e.agent.clone()).collect()
    }

    /// The most agents created and not yet closed or deleted at once.
    fn peak_live(&self) -> usize {
        let mut live = HashSet::new();
        let mut peak = 0;
        for event in self.history() {
            let Some(agent) = &event.agent else {
                continue;
            };
            match event.rpc() {
                Some(Rpc::CreateAgent) => {
                    live.insert(agent.clone());
                    peak = peak.max(live.len());
                }
                Some(Rpc::CloseAgent | Rpc::DeleteAgent) => {
                    live.remove(agent);
                }
                _ => {}
            }
        }
        peak
    }

    fn callbacks(&self) -> Vec<&Event> {
        self.history().iter().filter(|e| e.kind == Kind::Callback).collect()
    }

    /// The RPC sequence per agent, for a failure message.
    fn summary(&self) -> Value {
        let agents = self.agents();
        json!({
            "agents": agents.iter().map(|a| json!({ a: self.sequence(a) })).collect::<Vec<_>>(),
            "rpcs": self.history().iter().filter_map(Event::rpc).collect::<Vec<_>>(),
        })
    }
}

impl History for Log {
    fn history(&self) -> &[Event] {
        &self.events
    }
}

impl History for Process {
    fn history(&self) -> &[Event] {
        &self.events
    }
}
