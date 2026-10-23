//! Exercises typed completion failures from a guest.
//!
//! The first operator argument is a substring expected in the error detail.
//! Remaining arguments configure the scenario:
//!
//! - `tools` declares the `lookup` tool.
//! - `tool:<name>` declares a function tool with that name.
//! - `check` rejects every candidate and requires one before the failure.
//! - `nudged` requires failure after a JSON nudge but before a candidate.
//! - `budget` expects `budget-exhausted` instead of `backend`.
//! - `invalid` expects `invalid-request` instead of `backend`.
//! - `without:<text>` requires the detail to omit `text`.

#![cfg(target_arch = "wasm32")]

use omnia_sdk::model::{
    CHECK_TOOL, Error, Format, Function, Model as _, Request, Tool, ToolCall, WasiModel,
};
use test_programs::{arguments, lend, lookup, user};

omnia_sdk::command!(scenario);

async fn scenario() {
    let arguments = arguments();
    let (needle, flags) = arguments.split_first().expect("the needle is the first argument");
    let mut tools = if flags.iter().any(|flag| flag == "tools") { vec![lookup()] } else { vec![] };
    tools.extend(flags.iter().filter_map(|flag| flag.strip_prefix("tool:")).map(|name| {
        Tool::Function(
            Function::builder().name(name).description("test tool").parameters("{}").build(),
        )
    }));
    let check = flags.iter().any(|flag| flag == "check");
    let nudged = flags.iter().any(|flag| flag == "nudged");
    let expected = if flags.iter().any(|flag| flag == "budget") {
        "budget-exhausted"
    } else if flags.iter().any(|flag| flag == "invalid") {
        "invalid-request"
    } else {
        "backend"
    };
    let absent: Vec<&str> = flags.iter().filter_map(|flag| flag.strip_prefix("without:")).collect();
    let format = if nudged { Format::Json } else { Format::Text };

    let mut candidates = 0_usize;
    let error = WasiModel
        .complete_with(
            Request::builder()
                .messages(vec![user("hi")])
                .maybe_workspace(lend())
                .tools(tools)
                .format(format)
                .check(check || nudged)
                .build(),
            |call: ToolCall| {
                if call.name == CHECK_TOOL {
                    candidates += 1;
                }
                async move {
                    Err::<String, String>(if call.name == CHECK_TOOL {
                        format!(
                            "the scenario rejects every candidate; this was `{}`",
                            call.arguments
                        )
                    } else {
                        format!("tool `{}` has no handler in this scenario", call.name)
                    })
                }
            },
        )
        .await
        .expect_err("the backend fails the completion");
    let ((Error::Backend(detail), "backend")
    | (Error::BudgetExhausted(detail), "budget-exhausted")
    | (Error::InvalidRequest(detail), "invalid-request")) = (&error, expected)
    else {
        panic!("expected {expected}, got {error:?}");
    };
    assert!(detail.contains(needle), "expected {needle:?} in the detail: {detail}");
    for text in absent {
        assert!(!detail.contains(text), "{text:?} must not reach the detail: {detail}");
    }
    if check {
        assert!(candidates >= 1, "the failure struck before any candidate reached the check");
    }
    if nudged {
        assert_eq!(candidates, 0, "a candidate reached the check before the failure struck");
    }
}
