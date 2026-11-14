//! The `omnia_test::contract::blob` rows over `WasiProvider`, all at once,
//! so a backend's `wasi:blobstore` meets the contract the native double does.
//! The first argument scopes every container name, `contract` when none is
//! given, so a live store reruns them under fresh names. One object of the
//! program's own, `raw` in `<scope>-host`, is left for the host to read back.

#![cfg(target_arch = "wasm32")]

use omnia_sdk::{BlobStore as _, WasiProvider};
use omnia_test::contract::blob;
use test_programs::arguments;

omnia_sdk::api::command::export!(scenario);

async fn scenario() {
    let store = WasiProvider;
    let scope = arguments().into_iter().next().unwrap_or_else(|| "contract".to_owned());
    let host = format!("{scope}-host");

    store.create_container(&host).await.expect("create the host's container");
    store.put(&host, "raw", b"raw").await.expect("put the host's object");

    futures::join!(
        blob::objects(&store, &scope),
        blob::absent(&store, &scope),
        blob::delete_absent(&store, &scope),
        blob::uncreated(&store, &scope),
        blob::containers(&store, &scope),
        blob::range(&store, &scope),
        blob::listing(&store, &scope),
        blob::copy_rename(&store, &scope),
        blob::clear(&store, &scope),
    );
}
