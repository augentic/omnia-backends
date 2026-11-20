//! The `omnia_test::contract::blob` rows, all at once, so a backend's
//! `omnia:blobstore` meets the contract. The first argument scopes every
//! container name, `contract` when none is given, so a live store reruns
//! them under fresh names. One object of the program's own, `raw` in
//! `<scope>-host`, is left for the host to read back.

#![cfg(target_arch = "wasm32")]

use omnia_sdk::blobstore::admin;
use omnia_test::contract::blob;
use test_programs::arguments;

omnia_sdk::command::export!(scenario);

async fn scenario() {
    let scope = arguments().into_iter().next().unwrap_or_else(|| "contract".to_owned());
    let host = admin::create_container(format!("{scope}-host"))
        .await
        .expect("create the host's container");
    host.write("raw", b"raw").await.expect("write the host's object");

    futures::join!(
        blob::objects(&scope),
        blob::absent(&scope),
        blob::attributes(&scope),
        blob::delete_absent(&scope),
        blob::uncreated(&scope),
        blob::containers(&scope),
        blob::range(&scope),
        blob::listing(&scope),
        blob::copy_rename(&scope),
        blob::clear(&scope),
    );
}
