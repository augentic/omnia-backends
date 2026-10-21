//! A backend call to an undeclared tool is the model's slip, answered and
//! never reaching the guest.

#![cfg(target_arch = "wasm32")]

use omnia_sdk::model::{Model as _, Request, WasiModel};
use test_programs::{lend, user};

omnia_sdk::command!(scenario);

async fn scenario() {
    // No tools declared, so no handler: the backend answers the `lookup`
    // call itself and the model's next reply is the answer.
    let reply = WasiModel
        .complete(Request::builder().messages(vec![user("hi")]).maybe_workspace(lend()).build())
        .await
        .expect("an undeclared tool is answered, not failed");
    assert_eq!(
        reply.answer,
        "tool failed: `lookup` is not declared; this request declares no tools"
    );
}
