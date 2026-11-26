//! The guest's `hosts` grant held over each address a transport dials, from
//! a guest whose list reaches `listed.invalid` and `localhost` alone:
//!
//! - an address outside the list, or one no host can be read from, is
//!   `denied` before anything connects;
//! - a fetch is held to the URL it dials, and a push to every one;
//! - a redirect is `denied`, even back to the host it came from;
//! - a listed host is dialled, and a local address reaches no host.
//!
//! The operator arguments are the origin's `file://` URL and the port of an
//! HTTP host on `localhost` that redirects every request back to itself. The
//! host seeds the remotes of `remotes`, and asserts what its ssh and its
//! HTTP host were sent and that the local push landed.

#![cfg(target_arch = "wasm32")]

use std::fs;

use omnia_sdk::vcs;
use test_programs::{arguments, fails_with};

omnia_sdk::command::export!(scenario);

const REMOTES: &str = "./remotes";
const ELSEWHERE: &str = "elsewhere.invalid";
const UNANSWERED: &str = "Could not read from remote repository";

async fn scenario() {
    let arguments = arguments();
    let [url, port] = arguments.as_slice() else {
        panic!("the origin's URL and the redirecting port are the arguments");
    };

    // a clone of a host outside the list, or of an address no host can be
    // read from; the HTTP host on the port would count any that connected
    let denied = [
        ("ssh://elsewhere.invalid/x.git".to_owned(), ELSEWHERE),
        ("git+ssh://elsewhere.invalid/x.git".to_owned(), ELSEWHERE),
        ("elsewhere.invalid:x.git".to_owned(), ELSEWHERE),
        (format!("https::http://elsewhere.localhost:{port}/x.git"), "elsewhere.localhost"),
        (format!("http://elsewhere.localhost:{port}/x.git"), "elsewhere.localhost"),
        (format!("http://127.0.0.1:{port}/x.git"), "127.0.0.1"),
        (format!("http://a@b@localhost:{port}/x.git"), "names no host"),
        (format!("http://local%68ost:{port}/x.git"), "names no host"),
    ];
    for (index, (address, needle)) in denied.iter().enumerate() {
        let place = format!("./denied-{index}");
        let refused = vcs::clone(address, &place, None).await.expect_err("the clone is denied");
        fails_with(refused, "denied", needle);
    }

    // a remote outside the list, for a fetch and a push, and a push that
    // dials it beside or instead of a listed URL
    let remotes = vcs::open(REMOTES).await.expect("the seeded repository");
    let fetched = remotes.fetch("elsewhere").await.expect_err("the fetch is denied");
    fails_with(fetched, "denied", ELSEWHERE);
    for remote in ["elsewhere", "mirrored", "doubled"] {
        let pushed = remotes.push(remote, "main").await.expect_err("the push is denied");
        fails_with(pushed, "denied", ELSEWHERE);
    }

    // a listed host is dialled, and answers nothing
    let listed = vcs::clone("ssh://listed.invalid/x.git", "./listed", None).await;
    fails_with(listed.expect_err("nothing answers"), "unavailable", UNANSWERED);
    let fetched = remotes.fetch("doubled").await.expect_err("nothing answers");
    fails_with(fetched, "unavailable", UNANSWERED);

    // a redirect, even back to the listed host it came from
    let redirect = format!("http://localhost:{port}/x.git");
    let redirected = vcs::clone(&redirect, "./redirected", None).await;
    fails_with(redirected.expect_err("the redirect is denied"), "denied", "no redirect");

    // a local address reaches no host, so the list does not hold it
    let local = vcs::clone(url, "./local", None).await.expect("clone").expect("a new workspace");
    fs::write("local/hosted.txt", "hosted\n").expect("writing a change");
    local.commit("hosted".to_owned()).await.expect("commit").expect("a commit");
    local.push("origin", "main").await.expect("push").expect("the push lands");
}
