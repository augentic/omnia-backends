//! A completion carrying a worker grant over the scenario's lend. The
//! operator arguments spell the grant — `egress:<host>`, `read:<root>`,
//! `env:<NAME>=<value>`, any number of each — and `refused:<needle>` expects
//! the host to refuse the request as `invalid-request` with the needle in
//! its detail; without it the echo answers.

#![cfg(target_arch = "wasm32")]

use omnia_sdk::model::{Error, Model as _, Request, WasiModel, WorkerGrant};
use test_programs::{arguments, lend, user};

omnia_sdk::command!(scenario);

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
    let outcome = WasiModel.complete(request).await;
    match (outcome, refused) {
        (Ok(reply), None) => assert_eq!(reply.answer, "hi"),
        (Err(Error::InvalidRequest(detail)), Some(needle)) => {
            assert!(detail.contains(&needle), "expected {needle:?} in the detail: {detail}");
        }
        (outcome, None) => panic!("expected the echo's answer, got {outcome:?}"),
        (outcome, Some(needle)) => {
            panic!("expected invalid-request with {needle:?}, got {outcome:?}")
        }
    }
}
