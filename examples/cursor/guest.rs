//! # Cursor example — session guest
//!
//! A `wasi:cli/command` reactor that **imports** `omnia:model/completion` and
//! calls `create` once when the host drives `wasi:cli/run`. The prompt
//! declares a `widget_lifecycle` function tool; the guest answers each
//! `tool-call` with a `ToolResult` so the cursor backend can feed that extra
//! information back to the agent.
//!
//! It reads `wasi:filesystem/preopens` and lends the `.` mount (the
//! `[[mount]]` in `config.toml`) through `grants.workspace`; the host
//! resolves it to the node-local working tree the spawned agent edits.

#![cfg(target_arch = "wasm32")]

use std::future::IntoFuture;

use omnia_wasi_model::completion::{self, Format, Grants, Tool, WorkspaceGrant};
use omnia_wasi_model::prompt::Sections;
use omnia_wasi_model::wit_stream;
use serde_json::json;
use wasip3::filesystem::preopens;

struct CliGuest;
wasip3::cli::command::export!(CliGuest);

impl wasip3::exports::cli::run::Guest for CliGuest {
    #[omnia_wasi_otel::instrument(name = "cursor_example_run")]
    async fn run() -> Result<(), ()> {
        // `directories` must outlive `create` — the lent `workspace` borrows
        // one of its descriptors.
        let directories = preopens::get_directories();
        let workspace = directories.iter().find_map(|(dir, name)| {
            (name == ".").then_some(WorkspaceGrant {
                root: dir,
                subpath: String::new(),
            })
        });

        tracing::info!(
            workspace = workspace.is_some(),
            tool = "widget_lifecycle",
            "cursor example completion"
        );

        let (system, messages) = Sections {
            role: Some("a terse technical writer".to_string()),
            task: "Call the `widget_lifecycle` tool and state the stages a widget moves \
                   through, in order."
                .to_string(),
            ..Sections::default()
        }
        .channels(Some("You answer strictly from the `widget_lifecycle` tool. Do not guess."));

        let request = completion::Request {
            model: None,
            system,
            messages,
            generation: None,
            format: Format::Text,
            tools: vec![Tool::Function(completion::Function {
                name: "widget_lifecycle".to_string(),
                description: "Return the ordered lifecycle stages a widget moves through."
                    .to_string(),
                parameters: json!({ "type": "object", "properties": {} }).to_string(),
            })],
            grants: Grants { workspace },
        };

        let (mut results, results_rx) = wit_stream::new::<completion::ToolResult>();
        let answer = match completion::create(request, results_rx).await {
            Ok(session) => {
                // "callbacks" from cursorbackend while processing the request
                let completion::Session { mut calls, reply } = session;
                let calls_loop = async {
                    while let Some(call) = calls.next().await {
                        tracing::info!(tool = call.name, "cursor example tool call");
                        let output = match call.name.as_str() {
                            "widget_lifecycle" => Ok(json!({
                                "stages": ["draft", "assembled", "shipped"],
                                "note": "Widgets never move backwards.",
                            })
                            .to_string()),
                            other => Err(format!("unknown tool `{other}`")),
                        };

                        // write the output to cursor backend's results stream
                        let _ =
                            results.write_one(completion::ToolResult { id: call.id, output }).await;
                    }
                };

                match futures::join!(calls_loop, IntoFuture::into_future(reply)) {
                    ((), Ok(reply)) => {
                        tracing::info!("cursor example answered");
                        reply.answer
                    }
                    ((), Err(error)) => {
                        tracing::warn!(?error, "cursor example completion failed");
                        format!("error: {error:?}")
                    }
                }
            }

            Err(error) => {
                tracing::warn!(?error, "cursor example completion failed");
                format!("error: {error:?}")
            }
        };

        println!("{answer}");
        Ok(())
    }
}
