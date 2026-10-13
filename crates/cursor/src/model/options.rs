//! Translate one host-validated request into a [`Turn`]: guest function
//! tools become SDK custom tools, MCP grants ride inline as `mcp_servers`,
//! the boundary's working directory becomes the agent's `cwd` and what it
//! grants the agent's tool list, and the prompt gains the format's
//! final-answer instruction and a hint naming the granted MCP servers.

use std::collections::BTreeMap;

use anyhow::{Context as _, Result};
use omnia_confine::Boundary;
use omnia_wasi_model::{Format, Mcp, Request, Tool};
use serde_json::Value;

use crate::protocol::{
    AgentOperationOptions, AgentOptions, CustomToolDefinition, LocalAgentOptions, McpServerConfig,
    ModelSelection, ToolList,
};

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

// The agent one completion creates: the `CreateAgent` options and the pin
// every later call on the agent repeats.
pub struct AgentSpec {
    pub options: AgentOptions,
    pub operation: AgentOperationOptions,
}

// What the agent may do in its working directory: nothing of a private one,
// read a lent tree, and run a shell in a lent tree under a shell root.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Access {
    Private,
    Read,
    Shell,
}

impl Access {
    pub const fn of(boundary: &Boundary) -> Self {
        if boundary.shell() {
            Self::Shell
        } else if boundary.lent() {
            Self::Read
        } else {
            Self::Private
        }
    }

    const fn lent(self) -> bool {
        !matches!(self, Self::Private)
    }

    fn tools(self) -> Vec<String> {
        let names: &[&str] = match self {
            Self::Private => &[],
            Self::Read => &["glob", "grep", "ls", "mcp", "read"],
            Self::Shell => &["glob", "grep", "ls", "mcp", "read", "shell"],
        };
        names.iter().map(ToString::to_string).collect()
    }
}

impl Turn {
    pub fn prepare(
        request: &Request, cwd: &str, access: Access, default_model: &str, api_key: &str,
    ) -> Result<Self> {
        if request.generation.is_some() {
            tracing::debug!("request.generation is ignored (CreateAgent has no sampling controls)");
        }

        let options = agent_options(request, cwd, access, default_model, api_key)?;
        let operation = AgentOperationOptions {
            cwd: cwd.to_owned(),
            api_key: api_key.to_owned(),
        };
        let text = with_mcp_hint(&request.mcp_servers(), request.to_string());
        Ok(Self {
            agent: AgentSpec { options, operation },
            prompt: Prompt {
                text,
                format: request.format.clone(),
                check: request.check,
            },
        })
    }
}

fn agent_options(
    request: &Request, cwd: &str, access: Access, default_model: &str, api_key: &str,
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

    let model = request.model.as_deref().unwrap_or(default_model).to_owned();

    Ok(AgentOptions {
        model: ModelSelection { id: model },
        api_key: api_key.to_owned(),
        local: LocalAgentOptions {
            cwd: vec![cwd.to_owned()],
            source: access.lent().then(|| "SETTING_SOURCE_PROJECT".to_owned()),
            custom_tools,
        },
        mcp_servers,
        tools: Some(ToolList {
            names: access.tools(),
        }),
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

// The access-to-tool-list wire distinction; tool, MCP and model mapping is
// accepted by `tests/live.rs`.
#[cfg(test)]
mod tests {
    use omnia_wasi_model::{Format, Grants, Mcp, Message, Request, Role, Tool};

    use super::{Access, agent_options, with_mcp_hint};

    fn names(access: Access) -> Vec<String> {
        agent_options(&request(), "/workspace", access, "auto", "test-key")
            .unwrap()
            .tools
            .expect("a tool list")
            .names
    }

    #[test]
    fn access_shapes() {
        let options =
            agent_options(&request(), "/workspace", Access::Read, "auto", "test-key").unwrap();
        assert_eq!(
            options.tools.as_ref().map(|t| t.names.clone()),
            Some(vec![
                "glob".to_string(),
                "grep".to_string(),
                "ls".to_string(),
                "mcp".to_string(),
                "read".to_string()
            ]),
            "a lent workspace grants read-only tools plus the custom-tool channel"
        );
        assert_eq!(options.local.source.as_deref(), Some("SETTING_SOURCE_PROJECT"));
        assert_eq!(options.api_key, "test-key");

        assert_eq!(names(Access::Shell).last().map(String::as_str), Some("shell"));
        assert_eq!(names(Access::Shell).len(), 6, "the shell joins the read-only tools");

        let options =
            agent_options(&request(), "/private", Access::Private, "auto", "test-key").unwrap();
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
        let options =
            agent_options(&request, "/private", Access::Private, "auto", "test-key").unwrap();
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
            grants: Grants { workspace: None, worker: None },
            check: false,
        }
    }
}
