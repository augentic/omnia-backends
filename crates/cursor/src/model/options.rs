//! Translate one host-validated request into a [`Turn`]: guest function
//! tools become SDK custom tools, the session's working directory becomes
//! the agent's `cwd` and its access the agent's tool list, and the prompt
//! gains the format's final-answer instruction.

use std::collections::BTreeMap;

use anyhow::{Context as _, Result};
use omnia_sandbox::{Access, Session};
use omnia_wasi_model::{Format, Request, Tool};
use serde_json::Value;

use crate::protocol::{
    AgentOperationOptions, AgentOptions, CustomToolDefinition, LocalAgentOptions, ModelSelection,
    ToolList,
};

// Everything one completion derives from the request: the agent to create
// and the prompt to send it.
pub struct Turn {
    pub agent: AgentSpec,
    pub prompt: Prompt,
}

impl Turn {
    pub fn prepare(
        request: &Request, session: &Session, model: &str, api_key: &str,
    ) -> Result<Self> {
        if request.generation.is_some() {
            tracing::debug!("request.generation is ignored (CreateAgent has no sampling controls)");
        }

        let path = session.cwd()?;
        let cwd = path
            .to_str()
            .with_context(|| format!("working directory {} is not UTF-8", path.display()))?;
        let options = agent_options(request, cwd, session.access(), model, api_key)?;
        let operation = AgentOperationOptions {
            cwd: cwd.to_owned(),
            api_key: api_key.to_owned(),
        };
        Ok(Self {
            agent: AgentSpec { options, operation },
            prompt: Prompt {
                text: request.to_string(),
                format: request.format.clone(),
                check: request.check,
            },
        })
    }
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

fn agent_options(
    request: &Request, cwd: &str, access: Access, model: &str, api_key: &str,
) -> Result<AgentOptions> {
    let mut custom_tools = BTreeMap::new();
    for Tool::Function(function) in &request.tools {
        let input_schema: Value =
            serde_json::from_str(&function.parameters).with_context(|| {
                format!("function tool `{}` parameters is not valid JSON", function.name)
            })?;
        custom_tools.insert(
            function.name.clone(),
            CustomToolDefinition {
                description: function.description.clone(),
                input_schema,
            },
        );
    }

    Ok(AgentOptions {
        model: ModelSelection { id: model.to_owned() },
        api_key: api_key.to_owned(),
        local: LocalAgentOptions {
            cwd: vec![cwd.to_owned()],
            source: access.is_lent().then(|| "SETTING_SOURCE_PROJECT".to_owned()),
            custom_tools,
        },
        tools: Some(ToolList { names: tools(access) }),
    })
}

// The agent's tools for what it may do in its working directory: nothing of
// a private one, read a lent workspace, and run a shell under a shell root.
fn tools(access: Access) -> Vec<String> {
    let names: &[&str] = match access {
        Access::Private => &[],
        Access::Read => &["glob", "grep", "ls", "mcp", "read"],
        Access::Shell => &["glob", "grep", "ls", "mcp", "read", "shell"],
    };
    names.iter().map(ToString::to_string).collect()
}
