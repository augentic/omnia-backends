//! A completion the backend fails: the guest sees a `backend` failure whose
//! detail carries the needle the deployment named. The first operator
//! argument is the needle; further arguments are flags — `tools` declares
//! `lookup`, `check` asks for a check and rejects every candidate it is
//! offered (so the failure must strike after at least one has reached the
//! guest), and `without:<text>` asserts the detail does not carry `text`.

#![cfg(target_arch = "wasm32")]

use omnia_sdk::model::{CHECK_TOOL, Error, Model as _, Request, ToolCall, WasiModel};
use test_programs::{arguments, lookup, user};

omnia_sdk::command!(scenario);

async fn scenario() {
    let arguments = arguments();
    let (needle, flags) = arguments.split_first().expect("the needle is the first argument");
    let tools = if flags.iter().any(|flag| flag == "tools") { vec![lookup()] } else { vec![] };
    let check = flags.iter().any(|flag| flag == "check");
    let absent: Vec<&str> = flags.iter().filter_map(|flag| flag.strip_prefix("without:")).collect();

    let mut candidates = 0_usize;
    let error = WasiModel
        .complete_with(
            Request::builder().messages(vec![user("hi")]).tools(tools).check(check).build(),
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
    let Error::Backend(detail) = &error else {
        panic!("expected a backend failure, got {error:?}");
    };
    assert!(detail.contains(needle), "expected {needle:?} in the detail: {detail}");
    for text in absent {
        assert!(!detail.contains(text), "{text:?} must not reach the detail: {detail}");
    }
    if check {
        assert!(candidates >= 1, "the failure struck before any candidate reached the check");
    }
}
