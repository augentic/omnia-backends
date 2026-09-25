//! Translate one host-validated request into a [`Turn`]: guest function
//! tools become SDK custom tools, MCP grants ride inline as `mcp_servers`,
//! the workspace becomes the agent's `cwd`, and the prompt gains the
//! format's final-answer instruction and a hint naming the granted MCP
//! servers.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use anyhow::{Context as _, Result};
use omnia_wasi_model::{Format, Mcp, Request, Tool};
use serde_json::Value;

use crate::protocol::{
    AgentOperationOptions, AgentOptions, CustomToolDefinition, LocalAgentOptions, McpServerConfig,
    ModelSelection, ToolList,
};

/// Everything one completion derives from the request: the agent to create
/// and the prompt to send it.
pub struct Turn {
    pub agent: AgentSpec,
    pub prompt: Prompt,
}

/// The prompt to send the agent: its text, the format steering candidate
/// extraction, and whether the guest checks the answer.
pub struct Prompt {
    pub text: String,
    pub format: Format,
    pub check: bool,
}

/// The agent one completion creates: the `CreateAgent` options, the pin
/// every later call on the agent repeats, and the workspace they point into.
pub struct AgentSpec {
    pub options: AgentOptions,
    pub operation: AgentOperationOptions,
    pub workspace: Workspace,
}

impl Turn {
    /// Translate the request against the lent workspace path, pinning the
    /// client's default model and API key into the agent options.
    ///
    /// # Errors
    ///
    /// Returns an error when the workspace cannot be prepared or the request
    /// does not map onto agent options.
    pub async fn prepare(
        request: &Request, lent: Option<&Path>, default_model: &str, api_key: &str,
    ) -> Result<Self> {
        if request.generation.is_some() {
            tracing::debug!("request.generation is ignored (CreateAgent has no sampling controls)");
        }

        let workspace = Workspace::new(lent).await?;
        let cwd = workspace.cwd()?;
        let options = agent_options(request, &cwd, workspace.is_lent(), default_model, api_key)?;
        let operation = AgentOperationOptions {
            cwd,
            api_key: api_key.to_owned(),
        };
        let text = with_mcp_hint(&request.mcp_servers(), request.to_string());
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

/// The agent's working directory: the lent tree, or a private empty one for
/// references-only completions.
pub enum Workspace {
    Lent(PathBuf),
    Private(tempfile::TempDir),
}

impl Workspace {
    // Create and canonicalize the lent tree; when none is lent, create a
    // private empty temporary directory instead.
    async fn new(lent: Option<&Path>) -> Result<Self> {
        match lent {
            Some(path) => {
                tokio::fs::create_dir_all(path)
                    .await
                    .with_context(|| format!("creating {}", path.display()))?;
                let path = tokio::fs::canonicalize(path)
                    .await
                    .with_context(|| format!("canonicalizing {}", path.display()))?;
                Ok(Self::Lent(path))
            }
            // `tempfile` has no async API; one `mkdir` under the temp root
            // is not worth a blocking thread
            None => Ok(Self::Private(
                tempfile::Builder::new()
                    .prefix("omnia-cursor-cwd-")
                    .tempdir()
                    .context("creating a private workspace")?,
            )),
        }
    }

    fn path(&self) -> &Path {
        match self {
            Self::Lent(path) => path,
            Self::Private(dir) => dir.path(),
        }
    }

    // The wire carries the path as a string, so one that is not UTF-8 cannot
    // name the workspace to the worker.
    fn cwd(&self) -> Result<String> {
        let path = self.path();
        path.to_str()
            .map(ToOwned::to_owned)
            .with_context(|| format!("invalid workspace path {}", path.display()))
    }

    const fn is_lent(&self) -> bool {
        matches!(self, Self::Lent(_))
    }
}

// Translate `request` into the `CreateAgent` options for an agent run in
// `workspace`.
fn agent_options(
    request: &Request, cwd: &str, lent: bool, default_model: &str, api_key: &str,
) -> Result<AgentOptions> {
    let mut custom_tools = BTreeMap::new();
    let mut mcp_servers = BTreeMap::new();

    // translate guest tools into custom tools
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

    // request.model, else the client's default (CURSOR_MODEL at connect, else auto)
    let model = request.model.as_deref().unwrap_or(default_model).to_owned();

    Ok(AgentOptions {
        model: ModelSelection { id: model },
        api_key: api_key.to_owned(),
        local: LocalAgentOptions {
            cwd: vec![cwd.to_owned()],
            source: lent.then(|| "SETTING_SOURCE_PROJECT".to_owned()),
            custom_tools,
        },
        mcp_servers,
        tools: if lent { None } else { Some(ToolList { names: Vec::new() }) },
    })
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

// The lent/private workspace wire distinction (CI floor). Tool/MCP/model
// mapping is accepted by `tests/live.rs`.
#[cfg(test)]
mod tests {
    use omnia_wasi_model::{Format, Grants, Mcp, Message, Request, Role, Tool};

    use super::{agent_options, with_mcp_hint};

    #[test]
    fn workspace_shapes() {
        let options = agent_options(&request(), "/workspace", true, "auto", "test-key").unwrap();
        assert!(options.tools.is_none());
        assert_eq!(options.local.source.as_deref(), Some("SETTING_SOURCE_PROJECT"));
        assert_eq!(options.api_key, "test-key");

        let options = agent_options(&request(), "/private", false, "auto", "test-key").unwrap();
        assert_eq!(options.tools.as_ref().map(|t| t.names.as_slice()), Some(&[][..]));
        assert!(options.local.source.is_none());
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
        let options = agent_options(&request, "/private", false, "auto", "test-key").unwrap();
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
