//! A completion over a lent workspace: the first operator argument names
//! the lend (`.` or `./<path>` beneath the `.` mount), and the echo answers.

#![cfg(target_arch = "wasm32")]

use omnia_sdk::model::Request;
use test_programs::{arguments, user};

omnia_sdk::command::export!(scenario);

async fn scenario() {
    let arguments = arguments();
    let lend = arguments.first().expect("the lend is the first argument");
    let reply = Request::builder()
        .messages(vec![user("hi")])
        .workspace(lend.as_str())
        .build()
        .complete()
        .await
        .expect("echo answers over the lend");
    assert_eq!(reply.answer, "hi");
}
