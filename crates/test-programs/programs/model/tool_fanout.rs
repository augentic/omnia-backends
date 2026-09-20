//! N tool-calling completions pending at once: each declares `lookup` and
//! answers it with its own seam, so a callback routed to the wrong
//! completion shows up as the wrong seam in an answer.

#![cfg(target_arch = "wasm32")]

use futures::future::join_all;
use omnia_sdk::model::{Model as _, Request, WasiModel};
use test_programs::{fanout_width, lookup, seam, user};

omnia_sdk::command!(scenario);

async fn scenario() {
    let width = fanout_width();
    let replies = join_all((0..width).map(|index| async move {
        let request = Request::builder().messages(vec![user("hi")]).tools(vec![lookup()]).build();
        let mut calls = 0;
        let reply = WasiModel
            .complete_with(request, |call| {
                calls += 1;
                assert_eq!(call.name, "lookup");
                async move { Ok::<_, String>(seam(index)) }
            })
            .await
            .unwrap_or_else(|error| panic!("completion {index} failed: {error}"));
        (calls, reply)
    }))
    .await;

    for (index, (calls, reply)) in replies.iter().enumerate() {
        assert_eq!(*calls, 1, "completion {index} answered one tool call");
        assert!(
            reply.answer.contains(&seam(index)),
            "completion {index} got another completion's tool result: {:?}",
            reply.answer
        );
    }
}
