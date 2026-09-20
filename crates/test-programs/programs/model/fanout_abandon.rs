//! N completions pending at once; the first to answer wins and the rest are
//! dropped where they stand. The host side asserts the losers leak nothing —
//! wherever the backend had got to with them.

#![cfg(target_arch = "wasm32")]

use futures::future::select_all;
use omnia_sdk::model::{Model as _, Request, WasiModel};
use test_programs::{fanout_width, seam, user};

omnia_sdk::command!(scenario);

async fn scenario() {
    let width = fanout_width();
    let pending: Vec<_> = (0..width)
        .map(|index| {
            Box::pin(async move {
                let reply = WasiModel
                    .complete(Request::builder().messages(vec![user(&seam(index))]).build())
                    .await;
                (index, reply)
            })
        })
        .collect();

    let ((index, reply), _, losers) = select_all(pending).await;
    let reply = reply.unwrap_or_else(|error| panic!("the winner {index} failed: {error}"));
    assert!(reply.answer.contains(&seam(index)), "the winner got {:?}", reply.answer);
    drop(losers);
}
