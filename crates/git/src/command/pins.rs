//! The command-line config that holds one operation to host policy, so the
//! repository's own configuration cannot choose a program git runs, on
//! either side of a transport.
//!
//! A guest with a writable mount owns everything beneath `.git`. Left to the
//! repository, a planted hook, an `fsmonitor`, a signing program, a clean or
//! smudge filter, a merge driver, or a credential helper would run on the
//! host the moment the next operation touched the tree. Each such key is
//! pinned on the command line, where git reads it last and the repository
//! cannot reach: a fixed set is forced off, and the keys an operator carries
//! for real — an SSH command, a credential prompt, a credential helper, a
//! signing program — are kept from the host's own configuration (system,
//! global, or this command line) and dropped from the repository's. A key
//! the repository spells in bytes a pin cannot name refuses the operation.
//!
//! The far side of a transport starts no process the near side's `-c` pins
//! reach, so the pack command is named on the command line too: a local
//! one wrapped to carry pins of its own, a remote one git's default rather
//! than whatever `remote.<name>.uploadPack` or `receivePack` the repository
//! set.

use anyhow::Result;
use omnia_wasi_vcs::{Error, Place};

use crate::Client;

// Keys forced on every operation whatever scope set them: each names a
// program git would run on a commit, a merge, a status, a fetch, or a push,
// and none is wanted from an automated backend. Hooks are reached through
// `.git/hooks` or `core.hooksPath`, so `/dev/null` sends git to a directory
// with none; the alternate-refs command, run by a fetch over a repository
// with alternates and by the far side of a push, is emptied, since git can
// run no empty command and skips the alternates' refs instead.
const FORCED: [&str; 8] = [
    "core.hooksPath=/dev/null",
    "core.fsmonitor=false",
    "core.alternateRefsCommand=",
    "commit.gpgsign=false",
    "push.gpgsign=false",
    "merge.verifySignatures=false",
    "submodule.recurse=false",
    "fetch.recurseSubmodules=no",
];

// Keys naming one program the host may carry for real, as git lowercases
// them in a listing and as they are pinned: the repository's value gives way
// to the host's, or to none. Nothing here signs, so a signing program is
// pinned for whatever path to one a later git may add.
const HOST_PINNED_KEYS: [(&str, &str); 6] = [
    ("core.sshcommand", "core.sshCommand"),
    ("core.askpass", "core.askPass"),
    ("gpg.program", "gpg.program"),
    ("gpg.openpgp.program", "gpg.openpgp.program"),
    ("gpg.x509.program", "gpg.x509.program"),
    ("gpg.ssh.program", "gpg.ssh.program"),
];

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

// The `-c` overrides that hold one repository's operations to host policy,
// and the first setting the repository made of how a transport runs, if
// any, which no pin can undo.
#[derive(Debug)]
pub struct Pins {
    pins: Vec<String>,
    blocked_transport_key: Option<String>,
}

impl Pins {
    // Read the repository's configuration with its scopes and build the
    // overrides. An unreadable listing leaves the forced set alone to apply.
    pub async fn read(client: &Client, at: &Place) -> Result<Self> {
        let args = ["config", "--show-scope", "--list", "-z"];
        let output = client.git(Some(at), args, &[]).await?;
        if !output.status.success() {
            return Ok(Self::forced());
        }
        let settings = parse(&output.stdout).map_err(|key| {
            let shown = at.path().display();
            let message = format!(
                "{shown}: the repository sets {key}, which is not UTF-8 and no pin can name"
            );
            Error::Other(message)
        })?;
        Ok(Self::build(&settings))
    }

    // The forced set alone: what a place holding no repository yet — an
    // `init` or a `clone` destination — is held to, having no configuration
    // of its own to read.
    pub fn forced() -> Self {
        Self::build(&[])
    }

    fn build(settings: &[Setting]) -> Self {
        // a URL rewrite, an HTTP setting, or a proxy the repository set would
        // carry the operator's credentials anywhere or over anything, and is
        // config no `-c` can put back to unset; the first is named for the
        // transport operations to refuse the repository on
        let blocked_transport_key = settings
            .iter()
            .find(|s| s.is_repo() && s.is_transport_override())
            .map(|s| s.key.clone());

        let mut pins: Vec<String> = FORCED.iter().map(|pin| (*pin).to_owned()).collect();

        // an SSH command or a credential prompt the repository set is replaced
        // by the host's own, or emptied so git falls back to its environment
        for (key, pin) in HOST_PINNED_KEYS {
            if repo_set(settings, key) {
                let host = host_value(settings, key).unwrap_or_default();
                pins.push(format!("{pin}={host}"));
            }
        }

        // a clean, smudge, or merge driver the repository defined is emptied,
        // so git runs no program and falls back to its built-in merge
        for setting in settings.iter().filter(|s| s.is_repo() && s.is_driver()) {
            pins.push(format!("{}=", setting.key));
        }

        // every credential helper the repository added is reset and the
        // host's helpers for the same key re-added, so an operator's auth
        // survives and a planted helper does not
        for key in repo_credential_keys(settings) {
            pins.push(format!("{key}="));
            for value in host_values(settings, key) {
                pins.push(format!("{key}={value}"));
            }
        }

        Self {
            pins,
            blocked_transport_key,
        }
    }

    // `-c <pin>` for each override, to lead a subcommand's arguments.
    pub fn args(&self) -> impl Iterator<Item = &str> {
        self.pins.iter().flat_map(|pin| ["-c", pin.as_str()])
    }

    // The first key the repository itself set of how a transport runs — a
    // URL rewrite, an HTTP setting, a proxy — as git lists it.
    pub fn blocked_transport_key(&self) -> Option<&str> {
        self.blocked_transport_key.as_deref()
    }
}

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
        let binary = shell_quote(&self.binary);
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

fn shell_quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "'\\''"))
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
    value: String,
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

// `config --show-scope --list -z` is `<scope>\0<key>\n<value>\0` per setting;
// a key holds no newline and a value holds no NUL, so the first `\n` splits
// the pair and the trailing NUL ends the list. A pin names a key as text, so
// a repository key in any other bytes — a subsection git lets hold anything
// but a newline — is the error, spelled as text reads it, since a pin that
// cannot name it would leave the program it points at running; host keys and
// every value read lossily, as only a host value is ever pinned back.
fn parse(listing: &[u8]) -> Result<Vec<Setting>, String> {
    let mut tokens = listing.split(|&byte| byte == 0);
    let mut settings = Vec::new();
    while let (Some(scope), Some(pair)) = (tokens.next(), tokens.next()) {
        if scope.is_empty() {
            break;
        }
        let scope = if matches!(scope, b"local" | b"worktree") { Scope::Repo } else { Scope::Host };
        let split = pair.iter().position(|&byte| byte == b'\n').unwrap_or(pair.len());
        let (key, value) = (&pair[..split], pair.get(split + 1..).unwrap_or_default());
        let key = match str::from_utf8(key) {
            Ok(key) => key.to_owned(),
            Err(_) if scope == Scope::Repo => return Err(String::from_utf8_lossy(key).into_owned()),
            Err(_) => String::from_utf8_lossy(key).into_owned(),
        };
        settings.push(Setting {
            scope,
            key,
            value: String::from_utf8_lossy(value).into_owned(),
        });
    }
    Ok(settings)
}

fn repo_set(settings: &[Setting], key: &str) -> bool {
    settings.iter().any(|s| s.is_repo() && s.key == key)
}

// The last host-scope value, as git's own precedence would read it.
fn host_value<'a>(settings: &'a [Setting], key: &str) -> Option<&'a str> {
    settings.iter().rfind(|s| s.is_host() && s.key == key).map(|s| s.value.as_str())
}

fn host_values<'a>(settings: &'a [Setting], key: &'a str) -> impl Iterator<Item = &'a str> {
    settings.iter().filter(move |s| s.is_host() && s.key == key).map(|s| s.value.as_str())
}

// The credential-helper keys the repository set, each once and in order: the
// top-level list and any per-URL list (`credential.<url>.helper`).
fn repo_credential_keys(settings: &[Setting]) -> Vec<&str> {
    let mut keys: Vec<&str> = Vec::new();
    for setting in settings.iter().filter(|s| s.is_repo() && s.is_helper()) {
        if !keys.contains(&setting.key.as_str()) {
            keys.push(&setting.key);
        }
    }
    keys
}

// The listing parser, the pin rules, and the address rule are pure over
// text; the hardening suite proves the pins hold against a salted repository
// and its far side.
#[cfg(test)]
mod tests {
    use super::{Pins, Setting, is_local, parse};

    fn settings(listing: &str) -> Vec<Setting> {
        parse(listing.as_bytes()).expect("a listing in UTF-8")
    }

    // the forced pins, then whatever the settings add
    fn pins(listing: &str) -> Vec<String> {
        Pins::build(&settings(listing)).args().map(str::to_owned).collect::<Vec<_>>()
    }

    #[test]
    fn no_repo_config() {
        let listing = "global\0user.name\nT\0global\0core.sshcommand\nhostssh\0";
        assert_eq!(
            pins(listing),
            [
                "-c",
                "core.hooksPath=/dev/null",
                "-c",
                "core.fsmonitor=false",
                "-c",
                "core.alternateRefsCommand=",
                "-c",
                "commit.gpgsign=false",
                "-c",
                "push.gpgsign=false",
                "-c",
                "merge.verifySignatures=false",
                "-c",
                "submodule.recurse=false",
                "-c",
                "fetch.recurseSubmodules=no",
            ],
            "a host-only SSH command is left to apply on its own"
        );
    }

    #[test]
    fn ssh_with_host() {
        let listing = "global\0core.sshcommand\nhostssh\0local\0core.sshcommand\nreposs\0";
        assert!(pins(listing).windows(2).any(|w| w == ["-c", "core.sshCommand=hostssh"]));
    }

    #[test]
    fn ssh_without_host() {
        let listing = "local\0core.sshcommand\nreposs\0";
        assert!(pins(listing).windows(2).any(|w| w == ["-c", "core.sshCommand="]));
    }

    #[test]
    fn askpass() {
        let listing = "local\0core.askpass\n/tmp/evil\0";
        assert!(pins(listing).windows(2).any(|w| w == ["-c", "core.askPass="]));
    }

    #[test]
    fn signers() {
        let listing = "global\0gpg.program\nhostgpg\0local\0gpg.program\n/tmp/evil\0\
                       local\0gpg.ssh.program\n/tmp/evil\0";
        let pins = pins(listing);
        assert!(pins.windows(2).any(|w| w == ["-c", "gpg.program=hostgpg"]));
        assert!(pins.windows(2).any(|w| w == ["-c", "gpg.ssh.program="]));
    }

    #[test]
    fn drivers() {
        let listing = "local\0filter.lfs.smudge\ngit-lfs\0local\0merge.evil.driver\nx\0\
                       local\0filter.lfs.clean\ngit-lfs\0";
        let pins = pins(listing);
        for driver in ["filter.lfs.smudge=", "merge.evil.driver=", "filter.lfs.clean="] {
            assert!(pins.windows(2).any(|w| w == ["-c", driver]), "{driver} not emptied");
        }
    }

    #[test]
    fn credential_helpers() {
        let listing = "global\0credential.helper\nhostcred\0local\0credential.helper\nrepocred\0";
        let pins = pins(listing);
        let reset = pins.windows(2).position(|w| w == ["-c", "credential.helper="]).expect("reset");
        let readd =
            pins.windows(2).position(|w| w == ["-c", "credential.helper=hostcred"]).expect("readd");
        assert!(reset < readd, "the host helper is re-added after the list is reset");
        assert!(
            !pins.windows(2).any(|w| w == ["-c", "credential.helper=repocred"]),
            "the repository's helper is never re-added"
        );
    }

    #[test]
    fn per_url_credential() {
        let listing = "local\0credential.https://evil.example.helper\n!sh\0";
        assert!(
            pins(listing)
                .windows(2)
                .any(|w| w == ["-c", "credential.https://evil.example.helper="]),
            "a per-URL helper the repository set is reset"
        );
    }

    #[test]
    fn url_rewrite() {
        let listing = "global\0url.ssh://h/.insteadof\nhttps://h/\0\
                       local\0url./tmp/m.pushinsteadof\nhttps://github.com/\0";
        let pins = Pins::build(&settings(listing));
        assert_eq!(pins.blocked_transport_key(), Some("url./tmp/m.pushinsteadof"));
        assert!(
            !pins.args().any(|arg| arg.contains("insteadof")),
            "a rewrite is refused on, never pinned"
        );
    }

    #[test]
    fn http_override() {
        for key in [
            "http.proxy",
            "http.sslverify",
            "http.sslcainfo",
            "http.https://github.com/.sslverify",
            "http.https://github.com/.proxy",
            "http.cookiefile",
            "http.curloptresolve",
            "remote.origin.proxy",
        ] {
            let listing = format!("local\0{key}\nx\0");
            let pins = Pins::build(&settings(&listing));
            assert_eq!(pins.blocked_transport_key(), Some(key), "{key}");
        }
    }

    #[test]
    fn http_tuning() {
        let listing = "local\0http.postbuffer\n524288000\0local\0http.lowspeedlimit\n0\0\
                       local\0http.https://h/.postbuffer\n1\0";
        assert_eq!(Pins::build(&settings(listing)).blocked_transport_key(), None);
    }

    #[test]
    fn host_transport() {
        let listing = "global\0url.ssh://h/.insteadof\nhttps://h/\0global\0http.proxy\nhost\0\
                       system\0http.sslverify\nfalse\0";
        assert_eq!(Pins::build(&settings(listing)).blocked_transport_key(), None);
    }

    #[test]
    fn empty_value() {
        let listing = "local\0foo.bar\n\0global\0user.name\nT\0";
        assert_eq!(settings(listing).len(), 2);
    }

    #[test]
    fn non_utf8_repo_key() {
        let listing = b"local\0filter.\xff.clean\ntouch pwned\0";
        assert_eq!(parse(listing).expect_err("unnameable"), "filter.\u{FFFD}.clean");
    }

    #[test]
    fn non_utf8_host_key() {
        let listing = b"global\0filter.\xff.clean\nx\0local\0user.name\n\xff\0";
        let settings = parse(listing).expect("the host's key and a value are the operator's");
        assert_eq!(settings.len(), 2);
        assert_eq!(settings[1].value, "\u{FFFD}");
    }

    #[test]
    fn addresses() {
        assert!(is_local("file:///srv/repo.git"));
        assert!(is_local("/srv/repo.git"));
        assert!(is_local("./repo"));
        assert!(is_local("../origin.git"));
        assert!(!is_local("https://host/repo.git"));
        assert!(!is_local("ssh://host/repo.git"));
        assert!(!is_local("git@host:owner/repo.git"));
        assert!(!is_local("ext::sh -c payload"));
    }
}
