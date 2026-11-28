//! Rearranging the mount while a backend holds a lend of it.
//!
//! - The operator arguments are the lend, a directory beneath the `.` mount,
//!   and the target its name is relinked to, relative to the mount.
//! - The guest lends the directory and declares a `swap` tool.
//! - The first `swap` call moves the lend aside to `held` and lays its name
//!   as a link to the target. A later call changes nothing, so a restarted
//!   completion finds the swap already made.
//! - The backend answers once the swap is done.

#![cfg(target_arch = "wasm32")]

use std::sync::atomic::{AtomicBool, Ordering};

use omnia_sdk::model::Request;
use omnia_sdk::wasip3::filesystem::preopens;
use test_programs::{arguments, tool, user};

omnia_sdk::command::export!(scenario);

async fn scenario() {
    let arguments = arguments();
    let [lend, target] = arguments.as_slice() else {
        panic!("the arguments are the lend and the link target: {arguments:?}");
    };
    let directories = preopens::get_directories();
    let root = directories
        .iter()
        .find(|(_, name)| name == ".")
        .map(|(dir, _)| dir)
        .expect("host must mount `.`");

    let swapped = AtomicBool::new(false);
    let request = Request::builder()
        .messages(vec![user("swap")])
        .tools(vec![tool("swap", "rearrange the mount")])
        .workspace(format!("./{lend}"))
        .build();
    request
        .complete_with(|call| {
            assert_eq!(call.name, "swap");
            let first = !swapped.swap(true, Ordering::Relaxed);
            async move {
                if first {
                    root.rename_at(lend.clone(), root, "held".to_owned())
                        .await
                        .expect("move the lent directory aside");
                    root.symlink_at(target.clone(), lend.clone())
                        .await
                        .expect("link the lent name to the target");
                }
                Ok::<_, String>("swapped".to_owned())
            }
        })
        .await
        .expect("the completion runs")
        .expect("the backend answers after the swap");
    assert!(swapped.load(Ordering::Relaxed), "the backend called `swap`");
}
