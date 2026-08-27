//! Translate one gate-validated request into a [`Turn`]: guest function
//! tools become SDK custom tools, MCP grants ride inline as `mcp_servers`,
//! the workspace becomes the agent's `cwd`, and the prompt gains a hint
//! naming the granted MCP servers.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::{env, fs};

use anyhow::{Context as _, Result};
use omnia_wasi_model::{Format, Mcp, Request, Tool};
use serde_json::Value;

use crate::bridge::{
    AgentOptions, CustomToolDefinition, LocalAgentOptions, McpServerConfig, ModelSelection,
    ToolList,
};

/// Everything one completion derives from the request: the `CreateAgent`
/// options, the workspace they point into, the opening prompt, and the
/// format gate.
pub struct Turn {
    pub options: AgentOptions,
    pub workspace: Workspace,
    pub prompt: String,
    pub format: Format,
}

impl Turn {
    /// Translate the request against the lent workspace path.
    ///
    /// # Errors
    ///
    /// Returns an error when the workspace cannot be prepared or the request
    /// does not map onto agent options.
    pub fn prepare(request: &Request, lent: Option<&Path>, default_model: &str) -> Result<Self> {
        let workspace = Workspace::new(lent)?;
        let options = AgentOptions::from_request(request, &workspace, default_model)?;
        let prompt = with_mcp_hint(&request.mcp_servers(), request.to_string());
        Ok(Self {
            options,
            workspace,
            prompt,
            format: request.format.clone(),
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
    /// The lent tree, created and canonicalized — or a private empty
    /// temporary directory when no workspace is lent.
    ///
    /// # Errors
    ///
    /// Returns an error when the lent path cannot be created or canonicalized,
    /// or when the private directory cannot be created.
    pub fn new(lent: Option<&Path>) -> Result<Self> {
        match lent {
            Some(path) => {
                fs::create_dir_all(path).with_context(|| format!("creating {}", path.display()))?;
                let path = path
                    .canonicalize()
                    .with_context(|| format!("canonicalizing {}", path.display()))?;
                Ok(Self::Lent(path))
            }
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

    const fn is_lent(&self) -> bool {
        matches!(self, Self::Lent(_))
    }
}

impl AgentOptions {
    pub fn from_request(
        request: &Request, workspace: &Workspace, default_model: &str,
    ) -> Result<Self> {
        let mut custom_tools = BTreeMap::new();
        let mut mcp_servers = BTreeMap::new();

        // translate guest tools into custom tools
        for tool in &request.tools {
            match tool {
                Tool::Function(function) => {
                    let input_schema: Value = serde_json::from_str(&function.parameters)
                        .with_context(|| {
                            format!(
                                "function tool `{}` parameters is not valid JSON",
                                function.name
                            )
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
                    mcp_servers
                        .insert(mcp.name.clone(), McpServerConfig::streamable_http(&mcp.url));
                }
            }
        }

        // request model -> env var -> default model
        let model = request.model.as_deref().unwrap_or(default_model).to_owned();
        let api_key = env::var("CURSOR_API_KEY").context("missing CURSOR_API_KEY")?;

        Ok(Self {
            model: ModelSelection { id: model },
            api_key,
            local: LocalAgentOptions {
                cwd: vec![workspace.path().display().to_string()],
                source: workspace.is_lent().then(|| "SETTING_SOURCE_PROJECT".to_owned()),
                custom_tools,
            },
            mcp_servers,
            tools: if workspace.is_lent() { None } else { Some(ToolList { names: Vec::new() }) },
        })
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

// The lent/private workspace wire distinction (CI floor). Tool/MCP/model
// mapping is accepted by `tests/live.rs`.
#[cfg(test)]
mod tests {
    use omnia_wasi_model::{Format, Grants, Message, Request, Role};

    use super::Workspace;
    use crate::bridge::AgentOptions;

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
        }
    }

    fn lent() -> Workspace {
        Workspace::Lent(std::env::temp_dir())
    }

    fn private() -> Workspace {
        Workspace::Private(tempfile::tempdir().expect("temp cwd"))
    }

    #[test]
    fn workspace_shapes() {
        let options = AgentOptions::from_request(&request(), &lent(), "auto").unwrap();
        assert!(options.tools.is_none());
        assert_eq!(options.local.source.as_deref(), Some("SETTING_SOURCE_PROJECT"));

        let options = AgentOptions::from_request(&request(), &private(), "auto").unwrap();
        assert_eq!(options.tools.as_ref().map(|t| t.names.as_slice()), Some(&[][..]));
        assert!(options.local.source.is_none());
    }
}
