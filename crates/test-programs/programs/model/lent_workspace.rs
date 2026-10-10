//! A completion lending the guest's `.` mount answers as one lending nothing
//! does; what the lend changes is read off the bridge's `CreateAgent`.

#![cfg(target_arch = "wasm32")]

use omnia_sdk::model::{Model as _, Request, WasiModel};
use test_programs::user;

omnia_sdk::command!(scenario);

async fn scenario() {
    let reply = WasiModel
        .complete(Request::builder().messages(vec![user("hi")]).workspace(".").build())
        .await
        .expect("echo answers");
    assert_eq!(reply.answer, "hi");
}
