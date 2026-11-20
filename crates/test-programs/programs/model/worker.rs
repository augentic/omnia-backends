//! A completion carrying a worker grant over the scenario's lend. The
//! operator arguments spell the grant — `egress:<host>`, `read:<root>`,
//! `env:<NAME>=<value>`, any number of each — and `refused:<needle>` expects
//! the host to refuse the request as `invalid` with the needle in its
//! detail; without it the echo answers.

#![cfg(target_arch = "wasm32")]

use omnia_sdk::model::{Request, WorkerGrant};
use test_programs::{arguments, fails_with, lend, user};

omnia_sdk::command::export!(scenario);

async fn scenario() {
    let mut grant = WorkerGrant::default();
    let mut refused = None;
    for argument in arguments() {
        let (kind, value) = argument.split_once(':').expect("an argument is `<kind>:<value>`");
        match kind {
            "egress" => grant.egress.push(value.to_owned()),
            "read" => grant.read.push(value.to_owned()),
            "env" => {
                let (name, value) = value.split_once('=').expect("an env entry is `NAME=value`");
                grant.env.push((name.to_owned(), value.to_owned()));
            }
            "refused" => refused = Some(value.to_owned()),
            other => panic!("unknown argument kind `{other}`"),
        }
    }

    let request =
        Request::builder().messages(vec![user("hi")]).maybe_workspace(lend()).worker(grant).build();
    let outcome = request.complete().await;
    match (outcome, refused) {
        (Ok(Ok(reply)), None) => assert_eq!(reply.answer, "hi"),
        (Err(error), Some(needle)) => fails_with(error, "invalid", &needle),
        (outcome, None) => panic!("expected the echo's answer, got {outcome:?}"),
        (outcome, Some(needle)) => panic!("expected invalid with {needle:?}, got {outcome:?}"),
    }
}
