//! Pins each Git operation to host policy.
//!
//! A writable repository can configure programs that Git executes on the
//! host:
//!
//! - Hooks and file-system monitors.
//! - Signing programs, filters, and merge drivers.
//! - Credential and SSH helpers.
//!
//! An operation lists the repository's configuration once and builds its
//! pins from that listing. Every later process reads the same listing back
//! through the frozen copy rather than the repository's own file, which a
//! guest can rewrite meanwhile. Command-line configuration takes precedence
//! over the copy: dangerous keys are disabled, and the SSH command,
//! credential prompt, credential helpers, and signing programs come only
//! from the operator's environment and host scopes. An operation is refused
//! when its listing fails, or when a repository key cannot be named safely
//! as a command-line pin.
//!
//! Transport pack commands are pinned separately because near-side `-c`
//! settings do not reach the far side. Local commands carry the same pins,
//! while remote commands use Git's defaults instead of repository overrides.

use std::env;
use std::ffi::{OsStr, OsString};
use std::os::unix::ffi::{OsStrExt as _, OsStringExt as _};

use anyhow::Result;
use omnia_wasi_vcs::{Error, Place};

use crate::Client;

// Keys forced on every operation whatever scope set them: each names a
// program git would run on a commit, a merge, a status, a fetch, or a push,
// or a source it would read, and none is wanted from an automated backend.
// Hooks are reached through `.git/hooks` or `core.hooksPath`, so `/dev/null`
// sends git to a directory with none; the alternate-refs command, run by a
// fetch over a repository with alternates and by the far side of a push, is
// emptied, since git can run no empty command and skips the alternates' refs
// instead. A bundle URI names a file or a URL that git copies into the
// object directory. A fetch reads one from the repository and a clone takes
// one the far side advertises, so the fetch's is emptied and the clone asks
// for none. Automatic garbage collection and maintenance never start: either
// would outlive the operation, and would rewrite `packed-refs` behind the
// frozen copy.
const FORCED: [&str; 12] = [
    "core.hooksPath=/dev/null",
    "core.fsmonitor=false",
    "core.alternateRefsCommand=",
    "commit.gpgsign=false",
    "push.gpgsign=false",
    "merge.verifySignatures=false",
    "submodule.recurse=false",
    "fetch.recurseSubmodules=no",
    "fetch.bundleURI=",
    "transfer.bundleURI=false",
    "gc.auto=0",
    "maintenance.auto=false",
];

// Keys naming a signing program, as git lists them: each is the host's, or
// none. Nothing here signs, so a signing program is pinned for whatever
// path to one a later git may add.
const SIGNING_PROGRAMS: [&str; 4] =
    ["gpg.program", "gpg.openpgp.program", "gpg.x509.program", "gpg.ssh.program"];

// The `http.*` keys that size or pace a transfer and shape nothing of where
// it goes or whom it trusts.
const ALLOWED_HTTP_KEYS: [&str; 7] = [
    "postbuffer",
    "lowspeedlimit",
    "lowspeedtime",
    "maxrequests",
    "minsessions",
    "useragent",
    "version",
];

// The configuration git applies at one place, every entry labelled with
// whose it is, in the order git read it.
#[derive(Debug)]
pub struct Listing {
    settings: Vec<Setting>,
}

impl Listing {
    // Read the configuration at `at`. A listing that fails is refused: no
    // pin could be built from it, and the forced set alone would leave
    // every program the repository names to run.
    pub async fn read(client: &Client, at: &Place) -> Result<Self> {
        let args = ["config", "--show-scope", "--list", "-z"];
        let output = client.git(Some(at), args, &[]).await?;
        let shown = at.path().display();
        if !output.status.success() {
            let stderr = output.stderr.trim();
            let message = format!("{shown}: cannot read the repository's configuration: {stderr}");
            return Err(Error::Other(message).into());
        }
        let settings = parse(&output.stdout).map_err(|key| {
            let message = format!(
                "{shown}: the repository sets {key}, which is not UTF-8 and no pin can name"
            );
            Error::Other(message)
        })?;
        Ok(Self { settings })
    }

    // The repository's own entries, each key with its value as git wrote it.
    pub fn repo_entries(&self) -> impl Iterator<Item = (&str, Option<&[u8]>)> {
        self.settings.iter().filter(|s| s.is_repo()).map(|s| (s.key.as_str(), s.value.as_deref()))
    }

    // The last host-scope value, as git's own precedence would read it.
    fn host_value(&self, key: &str) -> Option<&[u8]> {
        self.settings.iter().rfind(|s| s.is_host() && s.key == key)?.value.as_deref()
    }

    // The SSH command and credential prompt the operator would get from a
    // repository that set neither: the environment first, then the host's
    // configuration, then git's own fallback. git reads each variable before
    // the key it stands for, so no value of the repository's own applies,
    // and none is ever emptied, which would leave git no ssh to run.
    fn operator_env(&self) -> Vec<(&'static str, OsString)> {
        let ssh = env::var_os("GIT_SSH_COMMAND")
            .or_else(|| self.host_value("core.sshcommand").map(os))
            .or_else(|| env::var_os("GIT_SSH").map(|program| os(&shell_quote(program.as_bytes()))))
            .unwrap_or_else(|| OsString::from("ssh"));
        let askpass = env::var_os("GIT_ASKPASS")
            .or_else(|| self.host_value("core.askpass").map(os))
            .or_else(|| env::var_os("SSH_ASKPASS"))
            .unwrap_or_default();
        vec![("GIT_SSH_COMMAND", ssh), ("GIT_ASKPASS", askpass)]
    }
}

fn os(bytes: &[u8]) -> OsString {
    OsStr::from_bytes(bytes).to_owned()
}

// The `-c` overrides and the environment that hold one repository's
// operations to host policy, and the first setting the repository made of
// how a transport runs, if any, which no pin can undo.
#[derive(Debug)]
pub struct Pins {
    overrides: Vec<OsString>,
    env: Vec<(&'static str, OsString)>,
    blocked_transport_key: Option<String>,
}

impl Pins {
    pub fn build(listing: &Listing) -> Self {
        let settings = &listing.settings;

        // a URL rewrite, an HTTP setting, or a proxy the repository set would
        // carry the operator's credentials anywhere or over anything, and is
        // config no `-c` can put back to unset; the first is named for the
        // transport operations to refuse the repository on
        let blocked_transport_key = settings
            .iter()
            .find(|s| s.is_repo() && s.is_transport_override())
            .map(|s| s.key.clone());

        let mut overrides: Vec<OsString> = FORCED.iter().map(OsString::from).collect();

        // a signing program is the host's, or none
        for key in SIGNING_PROGRAMS {
            overrides.push(pin(key, Some(listing.host_value(key).unwrap_or_default())));
        }

        // a clean, smudge, or merge driver the repository defined is emptied,
        // so git runs no program: a filter passes content through, and a
        // merge driver leaves its path in conflict
        for setting in settings.iter().filter(|s| s.is_repo() && s.is_driver()) {
            overrides.push(pin(&setting.key, Some(b"")));
        }

        // every credential helper is reset, the repository's included, and
        // the host's are added back in the order git read them, so an
        // operator's auth survives and a planted helper does not
        overrides.push(pin("credential.helper", Some(b"")));
        for setting in settings.iter().filter(|s| s.is_host() && s.is_helper()) {
            overrides.push(pin(&setting.key, setting.value.as_deref()));
        }

        Self {
            overrides,
            env: listing.operator_env(),
            blocked_transport_key,
        }
    }

    // The forced set alone, with no environment held: what a read is held to
    // that reaches no remote, signs nothing, and asks for no credential.
    pub fn forced() -> Self {
        Self {
            overrides: FORCED.iter().map(OsString::from).collect(),
            env: Vec::new(),
            blocked_transport_key: None,
        }
    }

    // `-c <pin>` for each override, to lead a subcommand's arguments.
    pub fn args(&self) -> impl Iterator<Item = &OsStr> {
        self.overrides.iter().flat_map(|pin| [OsStr::new("-c"), pin.as_os_str()])
    }

    // The variables to set on every process, ahead of what the operation
    // itself sets.
    pub fn env(&self) -> impl Iterator<Item = (&str, &OsStr)> {
        self.env.iter().map(|(key, value)| (*key, value.as_os_str()))
    }

    // The first key the repository itself set of how a transport runs — a
    // URL rewrite, an HTTP setting, a proxy — as git lists it.
    pub fn blocked_transport_key(&self) -> Option<&str> {
        self.blocked_transport_key.as_deref()
    }
}

// `<key>=<value>` as one `-c` argument, or the bare key, which git reads as
// a key with no value.
fn pin(key: &str, value: Option<&[u8]>) -> OsString {
    let mut pin = key.as_bytes().to_vec();
    if let Some(value) = value {
        pin.push(b'=');
        pin.extend_from_slice(value);
    }
    OsString::from_vec(pin)
}

// Where a setting came from, cut to the one distinction that matters: the
// host's policy, which an operator owns, or the repository's, which a guest
// does.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Scope {
    Host,
    Repo,
}

#[derive(Debug)]
struct Setting {
    scope: Scope,
    key: String,
    value: Option<Vec<u8>>,
}

impl Setting {
    const fn is_repo(&self) -> bool {
        matches!(self.scope, Scope::Repo)
    }

    const fn is_host(&self) -> bool {
        matches!(self.scope, Scope::Host)
    }

    // `filter.<name>.clean` / `smudge` / `process` or `merge.<name>.driver`.
    fn is_driver(&self) -> bool {
        let Some((named, last)) = self.key.rsplit_once('.') else { return false };
        (named.starts_with("filter.") && matches!(last, "clean" | "smudge" | "process"))
            || (named.starts_with("merge.") && last == "driver")
    }

    // `credential.helper` or `credential.<url>.helper`.
    fn is_helper(&self) -> bool {
        self.key.starts_with("credential.")
            && self.key.rsplit_once('.').is_some_and(|(_, last)| last == "helper")
    }

    // `url.<base>.insteadOf` / `pushInsteadOf`; `http.*` and `http.<url>.*`
    // beyond the tuning keys; `remote.<name>.proxy` / `proxyAuthMethod`.
    fn is_transport_override(&self) -> bool {
        let Some((named, last)) = self.key.rsplit_once('.') else { return false };
        let rewrite = named.starts_with("url.") && matches!(last, "insteadof" | "pushinsteadof");
        let http =
            (named == "http" || named.starts_with("http.")) && !ALLOWED_HTTP_KEYS.contains(&last);
        let proxy = named.starts_with("remote.") && matches!(last, "proxy" | "proxyauthmethod");
        rewrite || http || proxy
    }
}

// `config --show-scope --list -z` is `<scope>\0<key>\n<value>\0` per setting,
// and `<scope>\0<key>\0` for a key with no value at all, which git reads as
// true where an empty value is false; a key holds no newline and a value no
// NUL, so the first `\n` splits the pair and the trailing NUL ends the list.
// A pin names a key as text, so a repository key in any other bytes — a
// subsection git lets hold anything but a newline — is the error, spelled
// as text reads it, since a pin that cannot name it would leave the program
// it points at running. Host keys read lossily; every value is kept as git
// wrote it, for the frozen copy to write back and a pin to name.
fn parse(listing: &[u8]) -> Result<Vec<Setting>, String> {
    let mut tokens = listing.split(|&byte| byte == 0);
    let mut settings = Vec::new();
    while let (Some(scope), Some(pair)) = (tokens.next(), tokens.next()) {
        if scope.is_empty() {
            break;
        }
        let scope = if matches!(scope, b"local" | b"worktree") { Scope::Repo } else { Scope::Host };
        let (key, value) = pair
            .iter()
            .position(|&byte| byte == b'\n')
            .map_or((pair, None), |split| (&pair[..split], Some(pair[split + 1..].to_vec())));
        let key = match str::from_utf8(key) {
            Ok(key) => key.to_owned(),
            Err(_) if scope == Scope::Repo => return Err(String::from_utf8_lossy(key).into_owned()),
            Err(_) => String::from_utf8_lossy(key).into_owned(),
        };
        settings.push(Setting { scope, key, value });
    }
    Ok(settings)
}

// What the far side of a local transport is held to, since a repository
// beneath the mount is as much the guest's as the place: no hooks, no
// alternate-refs command, no file-system monitor, and no update of a branch
// a working copy has checked out, which `receive.denyCurrentBranch=
// updateInstead` would write through the copy's own filters.
const LOCAL_PACK_PINS: [&str; 4] = [
    "core.hooksPath=/dev/null",
    "core.alternateRefsCommand=",
    "core.fsmonitor=false",
    "receive.denyCurrentBranch=refuse",
];

impl Client {
    // The protocol allow-list and the pack command for a transport over
    // `address`, the command named so the repository's own
    // `remote.<name>.uploadPack` or `receivePack` — which no `-c` outranks —
    // is never the program the operator's ssh carries to a host: for a
    // local transport, this git wrapped to hold the far side to
    // `LOCAL_PACK_PINS`; for a remote one, git's own default.
    pub(crate) fn transport_args(&self, address: &str, push: bool) -> (&'static str, String) {
        let operation = if push { "receive-pack" } else { "upload-pack" };
        if is_local(address) {
            ("file", self.pack_arg(operation))
        } else {
            ("http:https:ssh", format!("--{operation}=git-{operation}"))
        }
    }

    // `git -c … <operation>` as one `--upload-pack` or `--receive-pack`
    // argument: the near side's `-c` pins reach no process the far side
    // starts, so its own ride the command. git splits the value with shell
    // quoting for a local transport, so the binary is single-quoted.
    fn pack_arg(&self, operation: &str) -> String {
        let binary = String::from_utf8_lossy(&shell_quote(self.binary.as_bytes())).into_owned();
        let mut pins = String::new();
        for pin in LOCAL_PACK_PINS {
            pins.push_str(" -c ");
            pins.push_str(pin);
        }
        format!("--{operation}={binary}{pins} {operation}")
    }
}

// git's own rule: a `file://` URL, or an address with no scheme and no
// scp-like `host:path`, is a local path; everything else crosses a network.
fn is_local(address: &str) -> bool {
    if let Some(scheme) = address.split_once("://") {
        return scheme.0.eq_ignore_ascii_case("file");
    }
    match (address.find(':'), address.find('/')) {
        (Some(colon), Some(slash)) => colon > slash,
        (Some(_), None) => false,
        (None, _) => true,
    }
}

fn shell_quote(value: &[u8]) -> Vec<u8> {
    let mut quoted = vec![b'\''];
    for &byte in value {
        if byte == b'\'' {
            quoted.extend_from_slice(b"'\\''");
        } else {
            quoted.push(byte);
        }
    }
    quoted.push(b'\'');
    quoted
}
