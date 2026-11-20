//! Every candidate is rejected: the backend's round budget ends the
//! completion as exhausted, carrying the guest's own correction.

#![cfg(target_arch = "wasm32")]

use omnia_sdk::model::{Budget, Question};
use test_programs::{Verdict, lent};

omnia_sdk::command::export!(scenario);

async fn scenario() {
    let exhausted = lent(Question::<Verdict>::new("verdict"))
        .ask("judge this", None, Verdict::passing)
        .await
        .expect("the completion runs")
        .expect_err("no candidate passes the check");
    assert_eq!(exhausted.budget, Budget::Responses, "unexpected: {exhausted:?}");
    let correction = &exhausted.detail;
    assert!(correction.contains("## Previous answer (rejected)"), "unexpected: {correction}");
    assert!(correction.contains("verdict must be `pass`"), "unexpected: {correction}");
}
