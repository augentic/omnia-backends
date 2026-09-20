//! A completion the backend fails: the guest sees a `backend` failure whose
//! detail carries the needle the deployment named. The first operator
//! argument is the needle; further arguments are flags — `tools` declares
//! `lookup`, and `without:<text>` asserts the detail does not carry `text`.

#![cfg(target_arch = "wasm32")]

use omnia_sdk::model::{Error, Model as _, Request, WasiModel};
use test_programs::{arguments, lookup, user};

omnia_sdk::command!(scenario);

async fn scenario() {
    let arguments = arguments();
    let (needle, flags) = arguments.split_first().expect("the needle is the first argument");
    let tools = if flags.iter().any(|flag| flag == "tools") { vec![lookup()] } else { vec![] };
    let absent: Vec<&str> = flags.iter().filter_map(|flag| flag.strip_prefix("without:")).collect();

    let error = WasiModel
        .complete(Request::builder().messages(vec![user("hi")]).tools(tools).build())
        .await
        .expect_err("the backend fails the completion");
    let Error::Backend(detail) = &error else {
        panic!("expected a backend failure, got {error:?}");
    };
    assert!(detail.contains(needle), "expected {needle:?} in the detail: {detail}");
    for text in absent {
        assert!(!detail.contains(text), "{text:?} must not reach the detail: {detail}");
    }
}
