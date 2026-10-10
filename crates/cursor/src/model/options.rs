//! Translate one host-validated request into a [`Turn`]: guest function
//! tools become SDK custom tools, MCP grants ride inline as `mcp_servers`,
//! the workspace becomes the agent's `cwd`, and the prompt gains the
//! format's final-answer instruction and a hint naming the granted MCP
//! servers.

use std::collections::{BTreeMap, BTreeSet};
use std::ffi::OsStr;
use std::os::unix::ffi::OsStrExt as _;
use std::path::{Path, PathBuf};
use std::process::Stdio;

use anyhow::{Context as _, Result, bail};
use omnia_wasi_model::{Format, Mcp, Request, Tool};
use serde_json::{Value, json};
use tempfile::TempDir;
use tokio::process::Command;

use crate::protocol::{
    AgentOperationOptions, AgentOptions, CustomToolDefinition, LocalAgentOptions, McpServerConfig,
    ModelSelection, SandboxOptions, ToolList,
};
use crate::worker::Worker;

// The built-in tools a lent workspace grants: the read-only four and the
// custom-tool channel. `edit` and `delete` are withheld, so the tree is
// written through the guest's tools alone.
const LENT_TOOLS: [&str; 5] = ["glob", "grep", "ls", "mcp", "read"];

// The tool a lend under one of the operator's `shell_roots` adds, which the
// bridge bounds by each command's own timeout. It runs as the worker's user,
// so it is never granted without the bridge's sandbox: a lend is a preopen,
// not a trust decision, and a prompt the tree itself injects would
// otherwise name a command the host runs.
const SHELL_TOOL: &str = "shell";

// The file the bridge reads, from the agent's `cwd` alone, into the sandbox
// policy of every shell command: its write paths and network allows join
// the operator's.
const SANDBOX_POLICY: &str = ".cursor/sandbox.json";

// What the deployment fixes for every completion: the model a request
// leaves unset, the key, and the host trees whose lends get the sandboxed
// shell.
#[derive(Clone, Copy)]
pub struct Defaults<'a> {
    pub model: &'a str,
    pub api_key: &'a str,
    pub shell_roots: &'a [PathBuf],
}

// The built-in tools a workspace grants: none for a private one, the
// read-only set for a lend, and the sandboxed shell besides for a lend
// under one of the operator's roots.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Tools {
    None,
    ReadOnly,
    Shell,
}

// Everything one completion derives from the request: the agent to create
// and the prompt to send it.
pub struct Turn {
    pub agent: AgentSpec,
    pub prompt: Prompt,
}

pub struct Prompt {
    pub text: String,
    pub format: Format,
    pub check: bool,
}

// The agent one completion creates: the `CreateAgent` options, the pin
// every later call on the agent repeats, and the workspace they point into.
pub struct AgentSpec {
    pub options: AgentOptions,
    pub operation: AgentOperationOptions,
    pub workspace: Workspace,
}

impl Turn {
    pub async fn prepare(
        request: &Request, lent: Option<&Path>, defaults: Defaults<'_>,
    ) -> Result<Self> {
        if request.generation.is_some() {
            tracing::debug!("request.generation is ignored (CreateAgent has no sampling controls)");
        }

        let workspace = Workspace::new(lent, defaults.shell_roots).await?;
        let roots = workspace.roots()?;
        let tools = workspace.tools();
        let operation = AgentOperationOptions {
            cwd: roots[0].clone(),
            api_key: defaults.api_key.to_owned(),
        };
        let options = agent_options(request, roots, tools, defaults)?;
        let text = with_mcp_hint(&request.mcp_servers(), request.to_string());
        let text = with_tree_hint(workspace.hinted()?, text);
        Ok(Self {
            agent: AgentSpec {
                options,
                operation,
                workspace,
            },
            prompt: Prompt {
                text,
                format: request.format.clone(),
                check: request.check,
            },
        })
    }
}

// The agent's working directory and the tree it works in: one and the same
// for a read-only lend and for the private empty directory of a
// references-only completion, two for a lend that gets the shell. The
// bridge reads the sandbox policy of every shell command from the agent's
// `cwd`, and a lent tree is the guest's to write for as long as the run
// lasts, so the shell's `cwd` is a private directory carrying the policy
// that admits the tree as a write path and keeps the directory itself
// read-only; whatever the tree says of its own confinement is never read,
// and no command rewrites what the directory says.
pub enum Workspace {
    Private(TempDir),
    Lent(PathBuf),
    Shell { tree: PathBuf, cwd: PathBuf, _policy: TempDir },
}

impl Workspace {
    async fn new(lent: Option<&Path>, shell_roots: &[PathBuf]) -> Result<Self> {
        let Some(path) = lent else {
            return Ok(Self::Private(private("omnia-cursor-cwd-")?));
        };
        tokio::fs::create_dir_all(path)
            .await
            .with_context(|| format!("creating {}", path.display()))?;
        let tree = tokio::fs::canonicalize(path)
            .await
            .with_context(|| format!("canonicalizing {}", path.display()))?;
        if !under(&tree, shell_roots).await {
            return Ok(Self::Lent(tree));
        }

        let includes = config_includes(&tree).await?;
        let policy = private("omnia-cursor-policy-")?;
        let cwd = tokio::fs::canonicalize(policy.path())
            .await
            .with_context(|| format!("canonicalizing {}", policy.path().display()))?;
        let path = cwd.join(SANDBOX_POLICY);
        tokio::fs::create_dir_all(path.parent().expect("the policy has a parent"))
            .await
            .with_context(|| format!("creating {}", path.display()))?;
        let policy_json = sandbox_policy(utf8(&cwd)?, utf8(&tree)?, &includes);
        tokio::fs::write(&path, policy_json.to_string())
            .await
            .with_context(|| format!("writing {}", path.display()))?;
        Ok(Self::Shell {
            tree,
            cwd,
            _policy: policy,
        })
    }

    // The directory the agent works in, shown to the model and in the logs.
    pub fn tree(&self) -> &Path {
        match self {
            Self::Private(dir) => dir.path(),
            Self::Lent(tree) | Self::Shell { tree, .. } => tree,
        }
    }

    const fn tools(&self) -> Tools {
        match self {
            Self::Private(_) => Tools::None,
            Self::Lent(_) => Tools::ReadOnly,
            Self::Shell { .. } => Tools::Shell,
        }
    }

    // The agent's `cwd` list: its working directory first, then the tree
    // where that is not the tree itself.
    fn roots(&self) -> Result<Vec<String>> {
        let mut roots = vec![];
        if let Self::Shell { cwd, .. } = self {
            roots.push(utf8(cwd)?.to_owned());
        }
        roots.push(utf8(self.tree())?.to_owned());
        Ok(roots)
    }

    // The tree the prompt must name, since the agent is not started in it.
    fn hinted(&self) -> Result<Option<&str>> {
        match self {
            Self::Shell { tree, .. } => utf8(tree).map(Some),
            Self::Private(_) | Self::Lent(_) => Ok(None),
        }
    }
}

// `tempfile` has no async API; one `mkdir` under the temp root is not worth
// a blocking thread.
fn private(prefix: &str) -> Result<TempDir> {
    tempfile::Builder::new()
        .prefix(prefix)
        .tempdir()
        .with_context(|| format!("creating a private {prefix}* directory"))
}

// The wire carries a path as a string, so one that is not UTF-8 cannot
// name the workspace to the worker.
fn utf8(path: &Path) -> Result<&str> {
    path.to_str().with_context(|| format!("invalid workspace path {}", path.display()))
}

// The policy the shell's `cwd` carries: the tree is a write path; read-only
// within it are `.git`, so no command alters the repository or what git
// reads from it, and every config file git includes from the tree, so none
// gains a hook, filter or diff command the bridge's own git would run; and
// the `cwd` is read-only in full. The sandbox's writable root is the `cwd`,
// and the bridge reads this file again for every command, so the directory
// is held read-only by its own policy rather than by the protection of
// `.cursor` the bridge adds only where it finds a git checkout. The network
// is left to the operator's own policy.
fn sandbox_policy(cwd: &str, tree: &str, includes: &[String]) -> Value {
    let protected: Vec<String> = [cwd.to_owned(), format!("{tree}/.git")]
        .into_iter()
        .chain(includes.iter().cloned())
        .collect();
    json!({
        "type": "workspace_readwrite",
        "additionalReadwritePaths": [tree],
        "additionalReadonlyPaths": protected,
    })
}

// The config files git reads from inside `tree` besides those under
// `.git`: an `include.path` in the repository's config names one, and git
// resolves the path, its conditions and its own includes. Asked of git in
// the worker's environment, so the answer is the one the bridge's git acts
// on; where git cannot read the tree, the lend fails rather than guess.
async fn config_includes(tree: &Path) -> Result<Vec<String>> {
    let output = Command::new("git")
        .args(["config", "--list", "--show-origin", "-z"])
        .current_dir(tree)
        .env_clear()
        .envs(Worker::environment())
        .stdin(Stdio::null())
        .output()
        .await
        .with_context(|| format!("listing git config in {}", tree.display()))?;
    if !output.status.success() {
        bail!(
            "listing git config in {} failed: {}",
            tree.display(),
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }

    // keep the files under the tree, as git names them, outside `.git`
    let dot_git = tree.join(".git");
    let mut includes = BTreeSet::new();
    for origin in config_origins(&output.stdout) {
        let Ok(path) = tokio::fs::canonicalize(tree.join(origin)).await else {
            continue;
        };
        if path.starts_with(tree) && !path.starts_with(&dot_git) {
            includes.insert(utf8(&path)?.to_owned());
        }
    }
    Ok(includes.into_iter().collect())
}

// The `file:` origins of `git config --list --show-origin -z` output, which
// alternates an origin and a `key\nvalue`, each NUL-terminated.
fn config_origins(output: &[u8]) -> impl Iterator<Item = &Path> {
    output
        .split(|byte| *byte == 0)
        .step_by(2)
        .filter_map(|origin| origin.strip_prefix(b"file:"))
        .map(|path| Path::new(OsStr::from_bytes(path)))
}

// Whether the canonical `path` is at or beneath one of `roots`, each
// resolved as the lend was so a symlinked root still matches; a root that
// does not exist holds no tree.
async fn under(path: &Path, roots: &[PathBuf]) -> bool {
    for root in roots {
        if tokio::fs::canonicalize(root).await.is_ok_and(|root| path.starts_with(root)) {
            return true;
        }
    }
    false
}

fn agent_options(
    request: &Request, roots: Vec<String>, tools: Tools, defaults: Defaults<'_>,
) -> Result<AgentOptions> {
    let mut custom_tools = BTreeMap::new();
    let mut mcp_servers = BTreeMap::new();

    // guest function tools become custom tools; mcp grants ride inline
    for tool in &request.tools {
        match tool {
            Tool::Function(function) => {
                let input_schema: Value =
                    serde_json::from_str(&function.parameters).with_context(|| {
                        format!("function tool `{}` parameters is not valid JSON", function.name)
                    })?;
                custom_tools.insert(
                    function.name.clone(),
                    CustomToolDefinition {
                        description: (!function.description.is_empty())
                            .then(|| function.description.clone()),
                        input_schema,
                    },
                );
            }

            Tool::Mcp(mcp) => {
                mcp_servers.insert(mcp.name.clone(), McpServerConfig::streamable_http(&mcp.url));
            }
        }
    }

    let model = request.model.as_deref().unwrap_or(defaults.model).to_owned();
    let mut names: Vec<String> = match tools {
        Tools::None => Vec::new(),
        Tools::ReadOnly | Tools::Shell => LENT_TOOLS.iter().map(ToString::to_string).collect(),
    };
    let shell = tools == Tools::Shell;
    if shell {
        names.push(SHELL_TOOL.to_owned());
    }

    Ok(AgentOptions {
        model: ModelSelection { id: model },
        api_key: defaults.api_key.to_owned(),
        local: LocalAgentOptions {
            cwd: roots,
            sandbox_options: shell.then_some(SandboxOptions { enabled: true }),
            custom_tools,
        },
        mcp_servers,
        tools: Some(ToolList { names }),
    })
}

// Prepend the tree an agent not started in it is to work in. The sandbox
// starts every command in the agent's `cwd`, whatever working directory
// the call names, so the `cd` has to be the command's own.
fn with_tree_hint(tree: Option<&str>, prompt: String) -> String {
    match tree {
        Some(tree) => format!(
            "The project is `{tree}`. Every shell command starts outside it, in a directory that \
             holds nothing of the project, so begin each one with `cd {tree} && `. Read and \
             search in the project.\n\n{prompt}"
        ),
        None => prompt,
    }
}

// Prepend a hint naming the granted MCP servers and tool allowlist, so the
// agent prefers them over assumptions.
fn with_mcp_hint(servers: &[&Mcp], prompt: String) -> String {
    if servers.is_empty() {
        return prompt;
    }
    let lines: Vec<String> = servers
        .iter()
        .map(|server| {
            if server.tools.is_empty() {
                format!("- `{}`", server.name)
            } else {
                format!("- `{}` (use only: {})", server.name, server.tools.join(", "))
            }
        })
        .collect();

    format!(
        "The following read-only MCP servers are available. Consult their tools and resources for \
         authoritative reference material before answering, and prefer that material over \
         assumptions:\n{}\n\n{prompt}",
        lines.join("\n")
    )
}

// The lent/private workspace wire distinction and the shell's policy; tool,
// MCP and model mapping is accepted by `tests/live.rs`.
#[cfg(test)]
mod tests {
    use std::path::Path;

    use omnia_wasi_model::{Format, Grants, Mcp, Message, Request, Role, Tool};
    use serde_json::json;

    use super::{Defaults, Tools, agent_options, sandbox_policy, with_mcp_hint, with_tree_hint};

    const DEFAULTS: Defaults<'static> = Defaults {
        model: "auto",
        api_key: "test-key",
        shell_roots: &[],
    };

    fn roots(roots: &[&str]) -> Vec<String> {
        roots.iter().map(ToString::to_string).collect()
    }

    #[test]
    fn workspace_shapes() {
        let options =
            agent_options(&request(), roots(&["/workspace"]), Tools::ReadOnly, DEFAULTS).unwrap();
        assert_eq!(
            options.tools.as_ref().map(|t| t.names.clone()),
            Some(vec![
                "glob".to_string(),
                "grep".to_string(),
                "ls".to_string(),
                "mcp".to_string(),
                "read".to_string()
            ]),
            "a lent workspace grants the read-only tools and the custom-tool channel"
        );
        assert!(options.local.sandbox_options.is_none(), "nothing runs that needs confining");
        assert_eq!(options.api_key, "test-key");

        let options =
            agent_options(&request(), roots(&["/private"]), Tools::None, DEFAULTS).unwrap();
        assert_eq!(options.tools.as_ref().map(|t| t.names.as_slice()), Some(&[][..]));
    }

    #[test]
    fn shell_lend() {
        let options =
            agent_options(&request(), roots(&["/policy", "/workspace"]), Tools::Shell, DEFAULTS)
                .unwrap();
        assert_eq!(
            options.tools.as_ref().map(|t| t.names.clone()),
            Some(vec![
                "glob".to_string(),
                "grep".to_string(),
                "ls".to_string(),
                "mcp".to_string(),
                "read".to_string(),
                "shell".to_string()
            ]),
            "the shell joins the read-only tools"
        );
        assert!(
            options.local.sandbox_options.as_ref().is_some_and(|sandbox| sandbox.enabled),
            "and never without the sandbox"
        );
        assert_eq!(options.local.cwd, roots(&["/policy", "/workspace"]));
    }

    #[test]
    fn shell_policy() {
        let includes = roots(&["/work/tree/.gitconfig"]);
        assert_eq!(
            sandbox_policy("/tmp/policy", "/work/tree", &includes),
            json!({
                "type": "workspace_readwrite",
                "additionalReadwritePaths": ["/work/tree"],
                "additionalReadonlyPaths": ["/tmp/policy", "/work/tree/.git", "/work/tree/.gitconfig"],
            })
        );
    }

    #[test]
    fn config_origins() {
        let output = b"file:.git/config\0core.bare\nfalse\0\
                       file:.git/../.gitconfig\0diff.x.command\n/bin/echo a\nb\0\
                       command line:\0core.hookspath\n/dev/null\0\
                       file:/home/u/.gitconfig\0user.name\nu\0";
        let origins: Vec<&Path> = super::config_origins(output).collect();
        assert_eq!(
            origins,
            [".git/config", ".git/../.gitconfig", "/home/u/.gitconfig"].map(Path::new)
        );
        assert_eq!(super::config_origins(b"").count(), 0);
    }

    #[test]
    fn tree_hint() {
        assert_eq!(with_tree_hint(None, "hello".to_owned()), "hello");
        let hinted = with_tree_hint(Some("/work/tree"), "hello".to_owned());
        assert!(hinted.starts_with("The project is `/work/tree`."), "{hinted}");
        assert!(hinted.contains("`cd /work/tree && `"), "{hinted}");
        assert!(hinted.ends_with("\n\nhello"), "{hinted}");
    }

    #[test]
    fn mcp_hint_skipped() {
        assert_eq!(with_mcp_hint(&[], "hello".to_owned()), "hello");
    }

    #[test]
    fn mcp_hint_names_servers() {
        let open = Mcp {
            name: "docs".to_owned(),
            tools: vec![],
            url: "http://127.0.0.1:9/mcp".to_owned(),
        };
        let limited = Mcp {
            name: "search".to_owned(),
            tools: vec!["lookup".to_owned(), "get".to_owned()],
            url: "http://127.0.0.1:9/mcp".to_owned(),
        };
        let prompt = with_mcp_hint(&[&open, &limited], "Q".to_owned());
        assert!(prompt.contains("- `docs`\n"), "open server is named: {prompt}");
        assert!(
            prompt.contains("- `search` (use only: lookup, get)"),
            "allowlist rides in the hint: {prompt}"
        );
        assert!(prompt.ends_with("\n\nQ"), "original prompt is preserved: {prompt}");
    }

    #[test]
    fn private_workspace_keeps() {
        let mut request = request();
        request.tools = vec![Tool::Mcp(Mcp {
            name: "docs".to_owned(),
            tools: vec![],
            url: "http://127.0.0.1:9/mcp".to_owned(),
        })];
        let options = agent_options(&request, roots(&["/private"]), Tools::None, DEFAULTS).unwrap();
        assert!(options.mcp_servers.contains_key("docs"));
        assert_eq!(options.tools.as_ref().map(|t| t.names.as_slice()), Some(&[][..]));
    }

    fn request() -> Request {
        Request {
            model: None,
            system: None,
            messages: vec![Message {
                role: Role::User,
                content: "hello".to_owned(),
            }],
            generation: None,
            format: Format::Text,
            tools: vec![],
            grants: Grants { workspace: None },
            check: false,
        }
    }
}
