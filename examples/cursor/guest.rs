//! # Cursor example — session guest
//!
//! A `wasi:cli/command` guest that asks the model one question and declares
//! one function tool, `widget_lifecycle`. When the bridge-managed Cursor
//! agent calls the tool, the guest answers over the session's `results`
//! stream with a `ToolResult` — the extra information the agent needs.
//!
//! The raw `omnia:model/completion` bindings are used so the `calls` and
//! `results` streams are visible; most guests use `omnia_guest::model`'s
//! `Model::complete_with`, which runs the same loop behind a closure.

#![cfg(target_arch = "wasm32")]

use omnia_wasi_model::completion::{
    self, Format, Function, Grants, Message, Role, Tool, ToolCall, ToolResult, WorkspaceGrant,
};
use omnia_wasi_model::wit_stream;
use serde_json::json;
use wasip3::filesystem::preopens;

struct CliGuest;
wasip3::cli::command::export!(CliGuest);

impl wasip3::exports::cli::run::Guest for CliGuest {
    async fn run() -> Result<(), ()> {
        // pass the `.` mount as the agent's working tree
        let directories = preopens::get_directories();
        let workspace = directories.iter().find_map(|(dir, name)| {
            (name == ".").then_some(WorkspaceGrant {
                root: dir,
                subpath: String::new(),
            })
        });

        // generate model request
        let request = completion::Request {
            model: None,
            system: Some(
                "Answer strictly from the `lifecycle` tool. Do not guess.".to_string(),
            ),
            messages: vec![Message {
                role: Role::User,
                content: "Call the `lifecycle` tool and state the stages an item moves \
                          through, in order."
                    .to_string(),
            }],
            generation: None,
            format: Format::Text,
            tools: vec![Tool::Function(Function {
                name: "lifecycle".to_string(),
                description: "Lifecycle stages".to_string(),
                parameters: json!({ "type": "object", "properties": {} }).to_string(),
            })],
            grants: Grants { workspace },
        };

        // create a `ToolResult` stream for writing tool results to
        let (mut writer, reader) = wit_stream::new::<ToolResult>();

        // make the model request, passing the request and the stream reader
        let session = match completion::create(request, reader).await {
            Ok(session) => session,
            Err(error) => {
                println!("error: {error:?}");
                return Ok(());
            }
        };

        // get the tool calls stream and the reply future from the session
        let completion::Session { mut calls, reply } = session;

        // process tool calls, replying with tool results to the stream shared with the host
        let tool_loop = async {
            while let Some(call) = calls.next().await {
                println!("{call:?}");
                let ToolCall { id, name, .. } = call;

                let output = match name.as_str() {
                    "lifecycle" => Ok(json!({
                        "stages": ["draft", "assembled", "shipped"],
                        "note": "Lifecycle never moves backwards.",
                    })
                    .to_string()),
                    _ => Err(format!("Unknown tool: {name}")),
                };

                // write the response to the stream
                let _ = writer.write_one(ToolResult { id, output }).await;
            }
        };

        // run the tool loop and the reply together until the reply is ready
        let ((), outcome) = futures::join!(tool_loop, async { reply.await });
        match outcome {
            Ok(reply) => println!("{}", reply.answer),
            Err(error) => println!("error: {error:?}"),
        }

        Ok(())
    }
}
