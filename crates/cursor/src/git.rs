//! Git as a worker runs it in a lent tree: held to host policy, reading
//! nothing the shell can change.
//!
//! The bridge runs git in a lent tree outside the shell's sandbox, so the
//! tree may choose nothing of what that git runs or reads. [`Pins`] is the
//! command-line config every git of a worker's reads last, above the
//! repository's own and anything it includes: a fixed set of keys forced
//! off, and each program key the repository set taken back to the host's
//! value or none. [`Confinement`] is what one lend asks: the pins its
//! repository calls for, and every path git reads config from, which the
//! sandbox holds still. A lend nothing here can hold is refused.

use std::collections::BTreeSet;
use std::ffi::{OsStr, OsString};
use std::os::unix::ffi::OsStrExt as _;
use std::path::{Component, Path, PathBuf};
use std::process::{Output, Stdio};

use anyhow::{Context as _, Result, bail, ensure};
use tokio::process::Command;

use crate::worker::Worker;

// Keys forced on every git of a worker's, whatever scope set them: each
// names a program git would run on a status, a diff or a fetch, and none is
// wanted from an agent's tree. Hooks are reached through `.git/hooks` or
// `core.hooksPath`, so `/dev/null` sends git to a directory with none; the
// alternate-refs command is emptied, since git runs no empty command and
// skips the alternates' refs instead.
const FORCED: [(&str, &str); 3] = [
    ("core.hooksPath", "/dev/null"),
    ("core.fsmonitor", "false"),
    ("core.alternateRefsCommand", ""),
];

// Keys naming one program the host may carry for real, as git lowercases
// them in a listing and as they are pinned: the repository's value gives
// way to the host's, or to none. Nothing here signs, so a signing program
// is pinned for whatever path to one a later git may add.
const HOST_KEPT: [(&str, &str); 6] = [
    ("core.sshcommand", "core.sshCommand"),
    ("core.askpass", "core.askPass"),
    ("gpg.program", "gpg.program"),
    ("gpg.openpgp.program", "gpg.openpgp.program"),
    ("gpg.x509.program", "gpg.x509.program"),
    ("gpg.ssh.program", "gpg.ssh.program"),
];

// The first git that reads `GIT_CONFIG_COUNT` and `--path-format`; older
// ones ignore the pins.
const MIN_VERSION: (u32, u32) = (2, 31);

// The command-line config every git of a worker's reads last, and the
// directories past which none looks for a repository.
#[derive(Clone, Debug)]
pub struct Pins {
    config: Vec<(String, String)>,
    ceilings: OsString,
}

impl Pins {
    // Every worker's: the forced keys, and no ceiling.
    pub fn forced() -> Self {
        Self {
            config: FORCED
                .iter()
                .map(|(key, value)| ((*key).to_owned(), (*value).to_owned()))
                .collect(),
            ceilings: OsString::new(),
        }
    }

    // The variables that carry the pins: `GIT_CONFIG_COUNT` with its
    // `GIT_CONFIG_KEY_n` and `GIT_CONFIG_VALUE_n` pairs, which git 2.31 and
    // later read as command-line config, and `GIT_CEILING_DIRECTORIES` where
    // there is one.
    pub fn variables(&self) -> impl Iterator<Item = (OsString, OsString)> + '_ {
        let count = ("GIT_CONFIG_COUNT".into(), self.config.len().to_string().into());
        let pairs = self.config.iter().enumerate().flat_map(|(index, (key, value))| {
            [
                (format!("GIT_CONFIG_KEY_{index}").into(), key.clone().into()),
                (format!("GIT_CONFIG_VALUE_{index}").into(), value.clone().into()),
            ]
        });
        let ceilings = (!self.ceilings.is_empty())
            .then(|| ("GIT_CEILING_DIRECTORIES".into(), self.ceilings.clone()));
        std::iter::once(count).chain(pairs).chain(ceilings)
    }

    // The forced keys, then whatever the repository's settings call for.
    fn build(settings: &[Setting]) -> Result<Vec<(String, String)>> {
        let mut config = Self::forced().config;
        let repo = |setting: &&Setting| setting.scope == Scope::Repo;

        // a program `git diff` runs in place of its own, or over a file it
        // compares, cannot be emptied without ending every diff
        if let Some(setting) = settings.iter().find(|s| repo(s) && is_diff_program(&s.key)) {
            bail!(
                "the repository sets {}, naming a program git diff would run from the tree, \
                 which a shell lend cannot turn off",
                setting.key
            );
        }

        // an SSH command, a credential prompt or a signing program the
        // repository set is replaced by the host's own, or emptied
        for (listed, pin) in HOST_KEPT {
            if settings.iter().any(|s| repo(&s) && s.key == listed) {
                let host = host_values(settings, listed).pop().unwrap_or_default();
                config.push((pin.to_owned(), host));
            }
        }

        // a clean, smudge or merge driver the repository defined is emptied,
        // so git runs no program and carries on; a filter the repository
        // requires is no longer required, so an emptied one is skipped
        // rather than fatal
        let mut required = BTreeSet::new();
        for setting in settings.iter().filter(|s| repo(s) && is_driver(&s.key)) {
            config.push((setting.key.clone(), String::new()));
            if let Some(filter) = setting.key.strip_prefix("filter.") {
                let (name, _) = filter.rsplit_once('.').expect("a driver key has a last segment");
                required.insert(name.to_owned());
            }
        }
        config.extend(
            required
                .into_iter()
                .map(|name| (format!("filter.{name}.required"), "false".to_owned())),
        );

        // every credential helper the repository added is reset and the
        // host's helpers for the same key re-added
        for key in repo_credential_keys(settings) {
            config.push((key.clone(), String::new()));
            config
                .extend(host_values(settings, &key).into_iter().map(|value| (key.clone(), value)));
        }

        Ok(config)
    }
}

// What one shell lend asks: the pins its repository calls for, and the paths
// the sandbox holds still so that git reads nothing the shell wrote. The
// paths are the agent's working directory, the tree's `.git`, the
// repository's own directories where they lie elsewhere, and every file git
// reads config from — each as named and as the kernel resolves it, present
// or not, so neither the file nor a link to it can be replaced or written
// through. `.gitattributes` is not held: it selects among the drivers config
// defines, and once the pins apply none of those is the tree's.
pub struct Confinement {
    pub pins: Pins,
    pub frozen: Vec<PathBuf>,
}

impl Confinement {
    // Read what the lend of `tree`, worked from `cwd`, asks of git and of the
    // sandbox. Refused: a repository with submodules, whose own repositories
    // nothing here reads; a repository key no pin can spell or turn off; an
    // include path only git could resolve.
    pub async fn read(tree: &Path, cwd: &Path) -> Result<Self> {
        Self::try_read(tree, cwd)
            .await
            .with_context(|| format!("lending {} with the shell", tree.display()))
    }

    async fn try_read(tree: &Path, cwd: &Path) -> Result<Self> {
        let layout = Layout::read(tree).await?;
        let base = layout.as_ref().map_or(tree, |layout| layout.toplevel.as_path());
        if let Some(layout) = &layout {
            refuse_submodules(&layout.toplevel).await?;
        }
        let settings = listing(tree, base).await?;
        let config = Pins::build(&settings)?;

        // the repository's directories are held whole; the config files
        // besides are each held as named and as resolved
        let mut held = Held::new([cwd.to_owned(), tree.join(".git")]);
        if let Some(layout) = &layout {
            held.hold_dir(layout.git_dir.clone());
            held.hold_dir(layout.common_dir.clone());
        }
        for setting in &settings {
            let Some(origin) = &setting.origin else { continue };
            if setting.scope == Scope::Repo || origin.starts_with(tree) {
                held.hold_file(origin).await;
            }
            if setting.scope == Scope::Repo && is_include(&setting.key) {
                held.hold_file(&include_target(&setting.value, origin)?).await;
            }
        }

        // no git of the worker's looks above the repository, or above a tree
        // or working directory in none, where the shell may write a `.git`
        let ceilings: BTreeSet<&Path> = [base, cwd].into_iter().filter_map(Path::parent).collect();
        let ceilings = std::env::join_paths(ceilings).context("a ceiling path holds a `:`")?;

        Ok(Self {
            pins: Pins { config, ceilings },
            frozen: held.into_paths(),
        })
    }
}

// Refuse a host whose git would ignore the pins, so a lend with the shell
// never runs on a tree's hooks unnoticed.
pub async fn check_version() -> Result<()> {
    let output = Command::new("git")
        .arg("--version")
        .env_clear()
        .envs(Worker::environment(&Pins::forced()))
        .stdin(Stdio::null())
        .output()
        .await
        .context("running `git --version`; shell_roots needs git on PATH")?;
    let stdout = String::from_utf8_lossy(&output.stdout);
    let version = parse_version(&stdout)
        .with_context(|| format!("unrecognised `git --version` output: {}", stdout.trim()))?;
    let (major, minor) = MIN_VERSION;
    ensure!(
        version >= MIN_VERSION,
        "shell_roots needs git {major}.{minor} or later, which reads the pins; found {}",
        stdout.trim()
    );
    Ok(())
}

// Git in `tree`, as the worker runs it.
async fn git(tree: &Path, args: &[&str]) -> Result<Output> {
    Command::new("git")
        .args(args)
        .current_dir(tree)
        .env_clear()
        .envs(Worker::environment(&Pins::forced()))
        .stdin(Stdio::null())
        .output()
        .await
        .with_context(|| format!("running `git {}` in {}", args.join(" "), tree.display()))
}

// The repository `tree` lies in: its own directory, the one it shares as a
// linked worktree, and its top level, which may lie above a lent
// subdirectory.
struct Layout {
    git_dir: PathBuf,
    common_dir: PathBuf,
    toplevel: PathBuf,
}

impl Layout {
    // `None` where `tree` lies in no repository; a repository git refuses
    // for any other reason refuses the lend.
    async fn read(tree: &Path) -> Result<Option<Self>> {
        let args = [
            "rev-parse",
            "--path-format=absolute",
            "--git-dir",
            "--git-common-dir",
            "--show-toplevel",
        ];
        let output = git(tree, &args).await?;
        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            if stderr.contains("not a git repository") {
                return Ok(None);
            }
            bail!("git refuses the tree: {}", stderr.trim());
        }
        let mut lines = output
            .stdout
            .split(|byte| *byte == b'\n')
            .map(|line| PathBuf::from(OsStr::from_bytes(line)));
        let (Some(git_dir), Some(common_dir), Some(toplevel)) =
            (lines.next(), lines.next(), lines.next())
        else {
            bail!("unrecognised `git rev-parse` output");
        };
        Ok(Some(Self {
            git_dir,
            common_dir,
            toplevel,
        }))
    }
}

// A `git status` or `git diff` in a repository with submodules runs git in
// each, on that one's own configuration, which nothing here reads or holds.
async fn refuse_submodules(toplevel: &Path) -> Result<()> {
    let output = git(toplevel, &["ls-files", "-z", "--stage"]).await?;
    if !output.status.success() {
        bail!("listing the index failed: {}", String::from_utf8_lossy(&output.stderr).trim());
    }
    let gitlink = output.stdout.split(|byte| *byte == 0).any(|entry| entry.starts_with(b"160000 "));
    ensure!(
        !gitlink,
        "the repository has submodules, whose own repositories a shell lend cannot hold still"
    );
    Ok(())
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
    // the file the setting was read from, as git names it: absolute, or
    // relative to the repository's top level
    origin: Option<PathBuf>,
    key: String,
    value: String,
}

// The configuration git reads in `tree`, in the worker's environment, so
// the answer is the one the bridge's git acts on.
async fn listing(tree: &Path, base: &Path) -> Result<Vec<Setting>> {
    let output = git(tree, &["config", "--show-scope", "--show-origin", "--list", "-z"]).await?;
    if !output.status.success() {
        bail!("listing git config failed: {}", String::from_utf8_lossy(&output.stderr).trim());
    }
    parse(&output.stdout, base)
}

// `config --show-scope --show-origin --list -z` is `<scope>\0<origin>\0<key>\n<value>\0`
// per setting; a key holds no newline and a value holds no NUL, so the first
// `\n` splits the pair. A pin names a key as text, so a repository key in
// any other bytes — a subsection git lets hold anything but a newline — is
// the error, since a pin that cannot name it would leave the program it
// points at running; host keys and every value read lossily, as only a host
// value is ever pinned back.
fn parse(listing: &[u8], base: &Path) -> Result<Vec<Setting>> {
    let mut tokens = listing.split(|&byte| byte == 0);
    let mut settings = Vec::new();
    while let (Some(scope), Some(origin), Some(pair)) =
        (tokens.next(), tokens.next(), tokens.next())
    {
        if scope.is_empty() {
            break;
        }
        let scope = if matches!(scope, b"local" | b"worktree") { Scope::Repo } else { Scope::Host };
        let origin = origin.strip_prefix(b"file:").map(|path| base.join(OsStr::from_bytes(path)));
        let split = pair.iter().position(|&byte| byte == b'\n').unwrap_or(pair.len());
        let (key, value) = (&pair[..split], pair.get(split + 1..).unwrap_or_default());
        let key = match str::from_utf8(key) {
            Ok(key) => key.to_owned(),
            Err(_) if scope == Scope::Repo => bail!(
                "the repository sets {}, which is not UTF-8 and no pin can name",
                String::from_utf8_lossy(key)
            ),
            Err(_) => String::from_utf8_lossy(key).into_owned(),
        };
        settings.push(Setting {
            scope,
            origin,
            key,
            value: String::from_utf8_lossy(value).into_owned(),
        });
    }
    Ok(settings)
}

fn host_values(settings: &[Setting], key: &str) -> Vec<String> {
    settings
        .iter()
        .filter(|s| s.scope == Scope::Host && s.key == key)
        .map(|s| s.value.clone())
        .collect()
}

// `diff.external`, or `diff.<driver>.command` / `textconv`.
fn is_diff_program(key: &str) -> bool {
    key == "diff.external"
        || key
            .strip_prefix("diff.")
            .and_then(|rest| rest.rsplit_once('.'))
            .is_some_and(|(_, last)| matches!(last, "command" | "textconv"))
}

// `filter.<name>.clean` / `smudge` / `process` or `merge.<name>.driver`.
fn is_driver(key: &str) -> bool {
    let Some((named, last)) = key.rsplit_once('.') else { return false };
    (named.starts_with("filter.") && matches!(last, "clean" | "smudge" | "process"))
        || (named.starts_with("merge.") && last == "driver")
}

// The credential-helper keys the repository set, each once and in order: the
// top-level list and any per-URL list (`credential.<url>.helper`).
fn repo_credential_keys(settings: &[Setting]) -> Vec<String> {
    let mut keys: Vec<String> = Vec::new();
    for setting in settings.iter().filter(|s| s.scope == Scope::Repo && is_helper(&s.key)) {
        if !keys.contains(&setting.key) {
            keys.push(setting.key.clone());
        }
    }
    keys
}

// `credential.helper` or `credential.<url>.helper`.
fn is_helper(key: &str) -> bool {
    key.starts_with("credential.") && key.rsplit_once('.').is_some_and(|(_, last)| last == "helper")
}

// `include.path` or `includeIf.<condition>.path`.
fn is_include(key: &str) -> bool {
    key == "include.path"
        || key
            .strip_prefix("includeif.")
            .and_then(|rest| rest.rsplit_once('.'))
            .is_some_and(|(_, last)| last == "path")
}

// The file an include names, as git opens it: `~/` is the worker's home, a
// relative path is relative to the file the include was found in. A path
// git expands for another user is left to git, so it refuses the lend.
fn include_target(value: &str, origin: &Path) -> Result<PathBuf> {
    if let Some(rest) = value.strip_prefix("~/") {
        let home = std::env::var_os("HOME").context("an include under `~` needs HOME")?;
        return Ok(PathBuf::from(home).join(rest));
    }
    ensure!(
        !value.starts_with('~'),
        "the repository includes `{value}`, a path git expands for another user, which a shell \
         lend cannot hold still"
    );
    let path = Path::new(value);
    if path.is_absolute() {
        return Ok(path.to_owned());
    }
    Ok(origin.parent().unwrap_or(origin).join(path))
}

// The directories held whole and the files held besides, each file as named
// and as resolved; a file under a held directory is held already.
struct Held {
    dirs: Vec<PathBuf>,
    files: BTreeSet<PathBuf>,
}

impl Held {
    fn new(dirs: impl IntoIterator<Item = PathBuf>) -> Self {
        let mut held = Self {
            dirs: Vec::new(),
            files: BTreeSet::new(),
        };
        for dir in dirs {
            held.hold_dir(dir);
        }
        held
    }

    fn hold_dir(&mut self, dir: PathBuf) {
        if !self.dirs.contains(&dir) {
            self.dirs.push(dir);
        }
    }

    async fn hold_file(&mut self, path: &Path) {
        for path in [normalized(path), resolved(path).await] {
            if !self.dirs.iter().any(|dir| path.starts_with(dir)) {
                self.files.insert(path);
            }
        }
    }

    fn into_paths(self) -> Vec<PathBuf> {
        self.dirs.into_iter().chain(self.files).collect()
    }
}

// `path` with its `.` and `..` components folded away, as the sandbox names
// a file to hold.
fn normalized(path: &Path) -> PathBuf {
    let mut out = PathBuf::new();
    for component in path.components() {
        match component {
            Component::CurDir => {}
            Component::ParentDir => {
                out.pop();
            }
            other => out.push(other),
        }
    }
    out
}

// The path the kernel resolves `path` to, or would once it exists: its
// longest existing prefix canonicalised, the rest appended as named.
async fn resolved(path: &Path) -> PathBuf {
    let mut rest: Vec<OsString> = Vec::new();
    let mut base = path;
    loop {
        if let Ok(canonical) = tokio::fs::canonicalize(base).await {
            return rest.iter().rev().fold(canonical, |path, name| path.join(name));
        }
        match (base.file_name(), base.parent()) {
            (Some(name), Some(parent)) => {
                rest.push(name.to_owned());
                base = parent;
            }
            _ => return path.to_owned(),
        }
    }
}

// The major and minor of a `git --version` line, whatever follows them.
fn parse_version(output: &str) -> Option<(u32, u32)> {
    let version = output.trim().strip_prefix("git version ")?;
    let mut parts = version.split(['.', ' ', '-']).map(str::parse::<u32>);
    Some((parts.next()?.ok()?, parts.next()?.ok()?))
}

// The listing's parse and the pins it builds; what the pins and the frozen
// paths do to a real lend is `tests/model.rs`.
#[cfg(test)]
mod tests {
    use std::path::{Path, PathBuf};

    use super::{MIN_VERSION, Pins, Setting, include_target, normalized, parse, parse_version};

    fn settings(listing: &str) -> Vec<Setting> {
        parse(listing.as_bytes(), Path::new("/repo")).expect("a listing in UTF-8")
    }

    fn pins(listing: &str) -> Vec<(String, String)> {
        Pins::build(&settings(listing)).expect("nothing refused")
    }

    fn pin(key: &str, value: &str) -> (String, String) {
        (key.to_owned(), value.to_owned())
    }

    #[test]
    fn versions() {
        assert_eq!(parse_version("git version 2.50.1 (Apple Git-155)\n"), Some((2, 50)));
        assert_eq!(parse_version("git version 2.31.0"), Some((2, 31)));
        assert_eq!(parse_version("git version 2.30.2\n"), Some((2, 30)));
        assert_eq!(parse_version("git version 3.0.0-rc1"), Some((3, 0)));
        assert_eq!(parse_version("git: command not found"), None);
        assert_eq!(parse_version(""), None);
        assert!((2, 30) < MIN_VERSION && (3, 0) >= MIN_VERSION);
    }

    #[test]
    fn listing_parsed() {
        let listing = "global\0file:/home/u/.gitconfig\0user.name\nu\0\
                       local\0file:.git/config\0foo.bar\n\0\
                       local\0file:.git/../.gitconfig\0diff.x.xfuncname\n^fn\0\
                       command\0command line:\0core.hookspath\n/dev/null\0";
        let settings = settings(listing);
        assert_eq!(settings.len(), 4);
        assert_eq!(settings[0].origin.as_deref(), Some(Path::new("/home/u/.gitconfig")));
        assert_eq!(settings[1].origin.as_deref(), Some(Path::new("/repo/.git/config")));
        assert_eq!(settings[1].value, "", "the empty value parses without eating the next setting");
        assert_eq!(settings[2].origin.as_deref(), Some(Path::new("/repo/.git/../.gitconfig")));
        assert_eq!(settings[3].origin, None);
    }

    #[test]
    fn forced_without_repo_config() {
        let listing = "global\0file:/h/.gitconfig\0user.name\nT\0\
                       global\0file:/h/.gitconfig\0core.sshcommand\nhostssh\0";
        assert_eq!(
            pins(listing),
            [
                pin("core.hooksPath", "/dev/null"),
                pin("core.fsmonitor", "false"),
                pin("core.alternateRefsCommand", ""),
            ],
            "a host-only SSH command is left to apply on its own"
        );
        assert_eq!(Pins::forced().config, pins(""));
    }

    #[test]
    fn repo_programs_take_the_host_value_or_empty() {
        let listing = "global\0file:/h/.gitconfig\0core.sshcommand\nhostssh\0\
                       local\0file:.git/config\0core.sshcommand\nreposs\0\
                       local\0file:.git/config\0core.askpass\n/tmp/evil\0\
                       global\0file:/h/.gitconfig\0gpg.program\nhostgpg\0\
                       local\0file:.git/config\0gpg.program\n/tmp/evil\0\
                       local\0file:.git/config\0gpg.ssh.program\n/tmp/evil\0";
        let pins = pins(listing);
        assert!(pins.contains(&pin("core.sshCommand", "hostssh")));
        assert!(pins.contains(&pin("core.askPass", "")));
        assert!(pins.contains(&pin("gpg.program", "hostgpg")));
        assert!(pins.contains(&pin("gpg.ssh.program", "")));
    }

    #[test]
    fn repo_drivers_emptied() {
        let listing = "local\0file:.git/config\0filter.lfs.smudge\ngit-lfs\0\
                       local\0file:.git/config\0filter.lfs.required\ntrue\0\
                       local\0file:.git/config\0merge.evil.driver\nx\0\
                       local\0file:.git/config\0filter.lfs.clean\ngit-lfs\0";
        let pins = pins(listing);
        for driver in ["filter.lfs.smudge", "merge.evil.driver", "filter.lfs.clean"] {
            assert!(pins.contains(&pin(driver, "")), "{driver} not emptied: {pins:?}");
        }
        assert!(pins.contains(&pin("filter.lfs.required", "false")), "{pins:?}");
    }

    #[test]
    fn repo_credential_reset_then_host_readded() {
        let listing = "global\0file:/h/.gitconfig\0credential.helper\nhostcred\0\
                       local\0file:.git/config\0credential.helper\nrepocred\0\
                       local\0file:.git/config\0credential.https://evil.example.helper\n!sh\0";
        let pins = pins(listing);
        let reset = pins.iter().position(|p| *p == pin("credential.helper", "")).expect("reset");
        let readd =
            pins.iter().position(|p| *p == pin("credential.helper", "hostcred")).expect("readd");
        assert!(reset < readd, "the host helper is re-added after the list is reset");
        assert!(!pins.contains(&pin("credential.helper", "repocred")), "never re-added");
        assert!(pins.contains(&pin("credential.https://evil.example.helper", "")));
    }

    #[test]
    fn repo_diff_program_refused() {
        for key in ["diff.external", "diff.x.command", "diff.x.textconv"] {
            let listing = format!("local\0file:.git/config\0{key}\n/tmp/evil\0");
            let error = Pins::build(&settings(&listing)).expect_err(key);
            assert!(error.to_string().contains(&format!("sets {key}")), "{error}");
        }
        let host = "global\0file:/h/.gitconfig\0diff.external\n/usr/bin/difft\0";
        assert_eq!(pins(host), Pins::forced().config, "the host's own is its policy");
    }

    #[test]
    fn repo_key_not_utf8() {
        let listing = b"local\0file:.git/config\0filter.\xff.clean\ntouch pwned\0";
        let error = parse(listing, Path::new("/repo")).expect_err("unnameable");
        assert!(error.to_string().contains("filter.\u{FFFD}.clean"), "{error}");
        let host = b"global\0file:/h/.gitconfig\0filter.\xff.clean\nx\0local\0file:.git/config\0user.name\n\xff\0";
        let settings =
            parse(host, Path::new("/repo")).expect("the host's key and a value read lossily");
        assert_eq!(settings[1].value, "\u{FFFD}");
    }

    #[test]
    fn include_targets() {
        let origin = Path::new("/repo/.git/config");
        assert_eq!(
            include_target("../team.gitconfig", origin).unwrap(),
            Path::new("/repo/.git/../team.gitconfig")
        );
        assert_eq!(
            include_target("/etc/team.gitconfig", origin).unwrap(),
            Path::new("/etc/team.gitconfig")
        );
        let home = std::env::var_os("HOME").map(PathBuf::from).expect("HOME");
        assert_eq!(
            include_target("~/.gitconfig-work", origin).unwrap(),
            home.join(".gitconfig-work")
        );
        let error = include_target("~other/.gitconfig", origin).expect_err("another user's home");
        assert!(error.to_string().contains("another user"), "{error}");
    }

    #[test]
    fn normalized_paths() {
        assert_eq!(
            normalized(Path::new("/repo/.git/../team.gitconfig")),
            Path::new("/repo/team.gitconfig")
        );
        assert_eq!(normalized(Path::new("/repo/./a/../../b")), Path::new("/b"));
        assert_eq!(normalized(Path::new("/../x")), Path::new("/x"));
    }

    #[test]
    fn variables_carry_the_pins() {
        let pins = Pins {
            config: vec![pin("a.b", "1"), pin("c.d", "")],
            ceilings: "/tmp:/work".into(),
        };
        let variables: Vec<(String, String)> = pins
            .variables()
            .map(|(name, value)| (name.into_string().unwrap(), value.into_string().unwrap()))
            .collect();
        assert_eq!(
            variables,
            [
                pin("GIT_CONFIG_COUNT", "2"),
                pin("GIT_CONFIG_KEY_0", "a.b"),
                pin("GIT_CONFIG_VALUE_0", "1"),
                pin("GIT_CONFIG_KEY_1", "c.d"),
                pin("GIT_CONFIG_VALUE_1", ""),
                pin("GIT_CEILING_DIRECTORIES", "/tmp:/work"),
            ]
        );
        assert!(!Pins::forced().variables().any(|(name, _)| name == "GIT_CEILING_DIRECTORIES"));
    }
}
