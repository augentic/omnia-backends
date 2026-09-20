//! N completions pending at once, each answered with its own seam: the
//! pool runs them together and no answer reaches the wrong completion.

#![cfg(target_arch = "wasm32")]

use futures::future::join_all;
use omnia_sdk::model::{Model as _, Request, WasiModel};
use test_programs::{fanout_width, seam, user};

omnia_sdk::command!(scenario);

async fn scenario() {
    let width = fanout_width();
    let replies = join_all((0..width).map(|index| async move {
        WasiModel
            .complete(Request::builder().messages(vec![user(&seam(index))]).build())
            .await
            .unwrap_or_else(|error| panic!("completion {index} failed: {error}"))
    }))
    .await;

    for (index, reply) in replies.iter().enumerate() {
        assert!(
            reply.answer.contains(&seam(index)),
            "completion {index} got another completion's answer: {:?}",
            reply.answer
        );
    }
}
