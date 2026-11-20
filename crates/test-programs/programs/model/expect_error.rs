//! Exercises typed completion failures from a guest.
//!
//! The first operator argument is a substring expected in the error detail.
//! Remaining arguments configure the scenario:
//!
//! - `tools` declares the `lookup` tool.
//! - `tool:<name>` declares a function tool with that name.
//! - `check` rejects every candidate and requires one before the failure.
//! - `nudged` requires failure after a JSON nudge but before a candidate.
//! - `budget` expects the completion exhausted instead of failed as `other`.
//! - `invalid` expects `invalid` instead of `other`.
//! - `without:<text>` requires the detail to omit `text`.

#![cfg(target_arch = "wasm32")]

use std::future::ready;

use omnia_sdk::model::{Candidate, Format, Request, ToolCall};
use test_programs::{arguments, lend, lookup, tool, user};

omnia_sdk::command::export!(scenario);

async fn scenario() {
    let arguments = arguments();
    let (needle, flags) = arguments.split_first().expect("the needle is the first argument");
    let mut tools = if flags.iter().any(|flag| flag == "tools") { vec![lookup()] } else { vec![] };
    tools.extend(
        flags
            .iter()
            .filter_map(|flag| flag.strip_prefix("tool:"))
            .map(|name| tool(name, "test tool")),
    );
    let check = flags.iter().any(|flag| flag == "check");
    let nudged = flags.iter().any(|flag| flag == "nudged");
    let budget = flags.iter().any(|flag| flag == "budget");
    let expected = if flags.iter().any(|flag| flag == "invalid") { "invalid" } else { "other" };
    let absent: Vec<&str> = flags.iter().filter_map(|flag| flag.strip_prefix("without:")).collect();
    let format = if nudged { Format::Json } else { Format::Text };

    let request = Request::builder()
        .messages(vec![user("hi")])
        .maybe_workspace(lend())
        .tools(tools)
        .format(format)
        .build();
    let unhandled = |call: ToolCall| {
        ready(Err::<String, String>(format!(
            "tool `{}` has no handler in this scenario",
            call.name
        )))
    };

    let mut candidates = 0_usize;
    let outcome = if check || nudged {
        let reject = |candidate: Candidate| {
            candidates += 1;
            ready(Err(format!(
                "the scenario rejects every candidate; this was `{}`",
                candidate.answer
            )))
        };
        request.complete_checked(unhandled, reject).await
    } else {
        request.complete_with(unhandled).await
    };
    let detail = match (outcome, budget) {
        (Ok(Err(exhausted)), true) => exhausted.detail,
        (Err(error), false) if error.code() == expected => error.description().to_owned(),
        (outcome, true) => panic!("expected the completion exhausted, got {outcome:?}"),
        (outcome, false) => panic!("expected `{expected}`, got {outcome:?}"),
    };
    assert!(detail.contains(needle.as_str()), "expected {needle:?} in the detail: {detail}");
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
