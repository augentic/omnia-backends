//! What the fake records — one event per handshake step, RPC, park, and
//! tool callback — and the test-side views that fold the record back into
//! per-process histories.
//!
//! Every spawned fake appends JSONL lines to the one log in its home under
//! `flock`, so several processes share it and number themselves through it:
//! the probe `Client::connect` spawns is process 0, each lease's process
//! counts up from 1.

use std::collections::HashSet;
use std::fs::{File, OpenOptions};
use std::io::Write as _;
use std::os::fd::AsRawFd as _;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use super::Point;

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
    /// A request is held at this point until the test releases it.
    Parked(Point),
    /// The fake posted `CallCustomTool`; `arg` carries the status and the
    /// bearer token it used.
    Callback,
    /// The process forked a child of its own; `arg` carries its pid.
    Forked,
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
    file: PathBuf,
}

impl Recorder {
    /// The record for a spawned process, which claims the next process
    /// number under the log's lock and announces itself.
    pub fn to_file(path: &Path) -> Self {
        let file = lock(path);
        let started = Log::read(path).events.iter().filter(|e| e.kind == Kind::Started).count();
        let recorder = Self {
            pid: std::process::id(),
            process: started,
            file: path.to_path_buf(),
        };
        append(&file, &recorder.event(Kind::Started, None, Value::Null));
        drop(file);
        recorder
    }

    pub const fn process(&self) -> usize {
        self.process
    }

    /// The shared JSONL log every spawned process appends to.
    pub fn log_path(&self) -> &Path {
        &self.file
    }

    pub fn record(&self, kind: Kind, agent: Option<&str>, arg: Value) {
        append(&lock(&self.file), &self.event(kind, agent, arg));
    }

    /// Record a park at `point` and take its ticket: how many requests
    /// parked there before it, across every process.
    pub fn park(&self, point: Point) -> usize {
        let file = lock(&self.file);
        let ticket = Log::read(&self.file).parked(point);
        append(&file, &self.event(Kind::Parked(point), None, Value::Null));
        ticket
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

/// Every event recorded in the shared log file.
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

    /// Requests ever parked at `point`, released or not.
    pub fn parked(&self, point: Point) -> usize {
        self.events.iter().filter(|e| e.kind == Kind::Parked(point)).count()
    }

    /// Every point a request was parked at.
    pub fn parked_points(&self) -> Vec<Point> {
        let mut points = Vec::new();
        for event in &self.events {
            if let Kind::Parked(point) = event.kind
                && !points.contains(&point)
            {
                points.push(point);
            }
        }
        points
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

/// One spawned process's history.
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
        alive(self.pid)
    }

    /// The pids of the children the process forked, in order.
    pub fn forked(&self) -> Vec<u32> {
        self.events
            .iter()
            .filter(|e| e.kind == Kind::Forked)
            .filter_map(|e| e.arg.get("pid").and_then(Value::as_u64))
            .filter_map(|pid| u32::try_from(pid).ok())
            .collect()
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

/// Whether a process with `pid` is still running: a pid that answers a
/// probe, and is not a zombie waiting on a parent that may never reap it
/// (a forked child, reparented to a pid 1 that does not).
pub fn alive(pid: u32) -> bool {
    let Ok(signed) = i32::try_from(pid) else {
        return false;
    };
    // SAFETY: signal 0 probes for existence and delivers nothing.
    if unsafe { libc::kill(signed, 0) } != 0 {
        return false;
    }
    !zombie(pid)
}

#[cfg(target_os = "linux")]
fn zombie(pid: u32) -> bool {
    std::fs::read_to_string(format!("/proc/{pid}/stat")).ok().and_then(|stat| proc_state(&stat))
        == Some('Z')
}

// elsewhere, what is reparented to pid 1 is reaped as it exits
#[cfg(not(target_os = "linux"))]
const fn zombie(_pid: u32) -> bool {
    false
}

// The state field of `/proc/<pid>/stat` follows the parenthesised command
// name, which may itself hold spaces and parentheses: split from the right.
#[cfg(any(target_os = "linux", test))]
fn proc_state(stat: &str) -> Option<char> {
    let (_, after_name) = stat.rsplit_once(") ")?;
    after_name.chars().next()
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
    ///
    /// Spawned fakes mint `agent-1` independently, so the live set is keyed
    /// by process as well as id: a workspace-wide scan must not collapse
    /// concurrent leases, and one process's close must not retire another's.
    fn peak_live(&self) -> usize {
        let mut live = HashSet::new();
        let mut peak = 0;
        for event in self.history() {
            let Some(agent) = event.agent.as_deref() else {
                continue;
            };
            let key = (event.process, agent);
            match event.rpc() {
                Some(Rpc::CreateAgent) => {
                    live.insert(key);
                    peak = peak.max(live.len());
                }
                Some(Rpc::CloseAgent | Rpc::DeleteAgent) => {
                    live.remove(&key);
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

#[cfg(test)]
mod tests {
    use serde_json::Value;

    use super::{Event, History as _, Kind, Log, Rpc, proc_state};

    #[test]
    fn command_name_holding_the_separator() {
        assert_eq!(proc_state("42 (sleep) S 1 42 42 0 -1"), Some('S'));
        assert_eq!(proc_state("42 (a) b) Z 1 42 42 0 -1"), Some('Z'));
        assert_eq!(proc_state("42 (no state"), None);
    }

    fn rpc(process: usize, rpc: Rpc, agent: &str) -> Event {
        Event {
            pid: 1,
            process,
            ts: 0,
            kind: Kind::Rpc(rpc),
            agent: Some(agent.to_owned()),
            arg: Value::Null,
        }
    }

    #[test]
    fn same_id_across_processes() {
        // The `lease_waits` shape: three spawned processes each mint `agent-1`.
        let concurrent = Log::from_events(vec![
            rpc(1, Rpc::CreateAgent, "agent-1"),
            rpc(2, Rpc::CreateAgent, "agent-1"),
            rpc(3, Rpc::CreateAgent, "agent-1"),
        ]);
        assert_eq!(concurrent.peak_live(), 3);

        // One process closing `agent-1` must not retire the others.
        let staggered = Log::from_events(vec![
            rpc(1, Rpc::CreateAgent, "agent-1"),
            rpc(2, Rpc::CreateAgent, "agent-1"),
            rpc(1, Rpc::CloseAgent, "agent-1"),
            rpc(3, Rpc::CreateAgent, "agent-1"),
        ]);
        assert_eq!(staggered.peak_live(), 2);

        // Sequential reuse of the same id in one process is one live agent.
        let reused = Log::from_events(vec![
            rpc(1, Rpc::CreateAgent, "agent-1"),
            rpc(1, Rpc::CloseAgent, "agent-1"),
            rpc(1, Rpc::CreateAgent, "agent-1"),
        ]);
        assert_eq!(reused.peak_live(), 1);
    }
}
