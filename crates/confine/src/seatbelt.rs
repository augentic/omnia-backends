//! The Seatbelt substrate: a profile `sandbox-exec` applies to the program
//! before it runs, so the kernel holds the boundary on macOS.

// Lifted from heel 0.3.0, `src/platform/macos/{mod,profile}.rs` and
// `templates/sandbox.txt`, Copyright (c) 2025 Lexo Liu, MIT OR Apache-2.0:
// the launcher by absolute path, the three-pass rule order, and every path
// resolved before the kernel sees it.

use std::collections::BTreeSet;
use std::ffi::OsString;
use std::fmt::Write as _;
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{Context as _, Result, ensure};
use tokio::process::Command;
use tokio::time::timeout;

// Named by absolute path, so no `PATH` of the operator's can stand in for it.
pub const SANDBOX_EXEC: &str = "/usr/bin/sandbox-exec";

const BASE: &str = include_str!("seatbelt/base.sbpl");

// `(deny default)` leaves `process-info` open on recent macOS, so the
// same-sandbox allow is restated after the deny for the last match to keep
// it. `system-fcntl` 80 and 110 (`F_MAKECOMPRESSED`, `F_TRANSFEREXTENTS`)
// change a file through a read-only descriptor.
const PROTECTIONS: &str = "
; --- protections ---
(deny process-info*)
(allow process-info* (target same-sandbox))
(deny mach-priv-task-port)
(deny mach-lookup (xpc-service-name-prefix \"\"))
(deny system-fcntl (fcntl-command 80 110))
(allow ipc-posix-shm)
";

const DEVICES: &str = "
; --- devices ---
(allow file-read*
  (literal \"/dev/null\")
  (literal \"/dev/zero\")
  (literal \"/dev/random\")
  (literal \"/dev/urandom\")
  (literal \"/dev/dtracehelper\"))
(allow file-write* (literal \"/dev/null\") (literal \"/dev/zero\"))
(allow file-write-data file-ioctl (literal \"/dev/dtracehelper\"))
(allow file-read-data file-test-existence file-write-data (subpath \"/dev/fd\"))
(allow file-read-metadata (literal \"/dev\"))
";

// Loopback alone: the egress proxy and the callback endpoint are there,
// and a tree's own tests serve and connect there. No unix socket is
// reachable, so no name resolves inside: every way out goes through the
// proxy by name.
const NETWORK: &str = "
; --- network ---
(deny network*)
(allow network-outbound (remote ip \"localhost:*\"))
(allow network-bind network-inbound (local ip \"*:*\"))
";

// The system a process runs on: the runtime, the toolchains installed
// system-wide, and the timezone database `localtime` resolves into. Each is
// skipped when the host lacks it.
const SYSTEM: [&str; 11] = [
    "/usr",
    "/bin",
    "/sbin",
    "/System",
    "/Library/Frameworks",
    "/Library/Developer/CommandLineTools",
    "/etc",
    "/var/select",
    "/var/db/timezone",
    "/opt/homebrew",
    "/usr/local",
];
// The symlinks every resolution passes through; `realpath` fails without
// their metadata.
const NODES: [&str; 3] = ["/tmp", "/etc", "/var"];

const PROBE: &str = "(version 1)(allow default)";
const PROBE_BOUND: Duration = Duration::from_secs(10);

// The Seatbelt profile's fixed part, settled at connect: the system roots
// the host has.
#[derive(Debug)]
pub struct Seatbelt {
    system: Vec<PathBuf>,
}

// Where one process is put, every path canonical, and the directories its
// ask was granted to read.
pub struct Place<'a> {
    pub tree: &'a Path,
    pub scratch: &'a Path,
    pub program: &'a Path,
    pub read: &'a [PathBuf],
}

impl Seatbelt {
    pub async fn probe() -> Result<Self> {
        ensure!(Path::new(SANDBOX_EXEC).exists(), "`{SANDBOX_EXEC}` is not available");
        let output = timeout(
            PROBE_BOUND,
            Command::new(SANDBOX_EXEC).args(["-p", PROBE, "/usr/bin/true"]).output(),
        )
        .await
        .with_context(|| format!("`{SANDBOX_EXEC}` did not answer within {PROBE_BOUND:?}"))?
        .with_context(|| format!("running `{SANDBOX_EXEC}`"))?;
        ensure!(
            output.status.success(),
            "`{SANDBOX_EXEC}` cannot apply a profile: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        );

        let system = SYSTEM.iter().filter_map(|root| std::fs::canonicalize(root).ok()).collect();
        Ok(Self { system })
    }

    // `sandbox-exec -p <profile> -D<KEY>=<path>.. -- <program>`: the profile
    // names every path through a parameter, so a path holding a quote or a
    // glob character cannot change what it means.
    pub fn command(&self, place: &Place<'_>) -> Result<Command> {
        let scratch = canonical(place.scratch)?;
        let program = canonical(place.program)?;
        let mut params = Params::default();
        let mut profile = String::with_capacity(BASE.len() + 4096);
        profile.push_str(BASE);
        profile.push_str(PROTECTIONS);

        // the system, read
        profile.push_str("\n; --- the system ---\n(allow file-read*");
        for (index, root) in self.system.iter().enumerate() {
            let _ =
                write!(profile, "\n  (subpath {})", params.add(format!("SYSTEM_{index}"), root));
        }
        profile.push_str(")\n");
        profile.push_str(DEVICES);
        let _ = writeln!(
            profile,
            "(allow file-read* (literal {}))",
            params.add("PROGRAM".to_owned(), &program)
        );

        // the grants
        profile.push_str("\n; --- the grants ---\n");
        for (index, root) in place.read.iter().enumerate() {
            let _ = writeln!(
                profile,
                "(allow file-read* (subpath {}))",
                params.add(format!("ROOT_{index}"), root)
            );
        }
        let tree = params.add("TREE".to_owned(), place.tree);
        let home = params.add("SCRATCH".to_owned(), &scratch);
        let _ =
            writeln!(profile, "(allow file-read* file-write* (subpath {tree}) (subpath {home}))");
        let _ = writeln!(
            profile,
            "(deny file-write-unlink (require-all (vnode-type DIRECTORY) (literal {tree})))"
        );

        // the engine's own directories beneath the tree, closed after its grant
        let git = params.add("TREE_GIT".to_owned(), &place.tree.join(".git"));
        let emery = params.add("TREE_EMERY".to_owned(), &place.tree.join(".emery"));
        let _ = writeln!(
            profile,
            "(deny file-read* file-write* (literal {git}) (subpath {git}) (literal {emery}) \
             (subpath {emery}))"
        );

        // the root, which a process reads to start, and the metadata a
        // resolution reads on its way to a granted path
        profile.push_str("(allow file-read* (literal \"/\"))\n(allow file-read-metadata");
        for node in NODES {
            let _ = write!(profile, " (literal \"{node}\")");
        }
        let granted = [place.tree, &scratch, &program]
            .into_iter()
            .chain(place.read.iter().map(PathBuf::as_path))
            .chain(self.system.iter().map(PathBuf::as_path));
        let above_tree = ancestors([place.tree]);
        let mut listed = Vec::new();
        for (index, ancestor) in ancestors(granted).iter().enumerate() {
            let param = params.add(format!("ANCESTOR_{index}"), ancestor);
            if above_tree.contains(ancestor) {
                listed.push(param.clone());
            }
            let _ = write!(profile, "\n  (literal {param})");
        }
        profile.push_str(")\n");

        // the tree's ancestors, listed: a Bun program (`cursor-sdk-bridge`)
        // reads every directory on the way to its working directory before
        // it reads its environment, and runs with none when one is closed;
        // an allow with no filter would open every file, so a tree with
        // nothing above it lists nothing
        if !listed.is_empty() {
            profile.push_str("(allow file-read-data");
            for param in &listed {
                let _ = write!(profile, "\n  (literal {param})");
            }
            profile.push_str(")\n");
        }
        profile.push_str(NETWORK);

        let mut command = Command::new(SANDBOX_EXEC);
        command.arg("-p").arg(profile);
        for (key, path) in params.0 {
            let mut definition = OsString::from(format!("-D{key}="));
            definition.push(path);
            command.arg(definition);
        }
        command.arg("--").arg(program);
        Ok(command)
    }
}

#[derive(Default)]
struct Params(Vec<(String, PathBuf)>);

impl Params {
    fn add(&mut self, key: String, path: &Path) -> String {
        let reference = format!("(param \"{key}\")");
        self.0.push((key, path.to_owned()));
        reference
    }
}

// Every proper ancestor but the root, once each.
fn ancestors<'a>(paths: impl IntoIterator<Item = &'a Path>) -> BTreeSet<PathBuf> {
    paths
        .into_iter()
        .flat_map(|path| path.ancestors().skip(1))
        .filter(|ancestor| ancestor.parent().is_some())
        .map(Path::to_owned)
        .collect()
}

fn canonical(path: &Path) -> Result<PathBuf> {
    std::fs::canonicalize(path).with_context(|| format!("resolving {}", path.display()))
}

// The profile's rendering for a tree no boundary row can lend: one directly
// under the root, with nothing above it to list. What a process at a lent
// tree sees is `tests/boundary.rs`.
#[cfg(test)]
mod tests {
    use std::ffi::OsStr;
    use std::path::Path;

    use super::{Place, Seatbelt};

    // A `file-read-data` allow with no filter would open every file, the
    // denied `.git` among them, since the last matching rule settles a read.
    #[tokio::test]
    async fn tree_under_root() {
        let scratch = tempfile::tempdir().expect("a scratch");
        let seatbelt = Seatbelt::probe().await.expect("sandbox-exec answers");
        let command = seatbelt
            .command(&Place {
                tree: Path::new("/Users"),
                scratch: scratch.path(),
                program: Path::new("/usr/bin/true"),
                read: &[],
            })
            .expect("the profile renders");

        let profile = command
            .as_std()
            .get_args()
            .skip_while(|arg| *arg != OsStr::new("-p"))
            .nth(1)
            .expect("a profile follows -p")
            .to_string_lossy();
        assert!(!profile.contains("(allow file-read-data)"), "{profile}");
    }
}
