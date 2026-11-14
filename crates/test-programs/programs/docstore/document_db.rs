//! The `omnia_test::contract::document` rows over `WasiProvider`, all at
//! once, so a backend's `omnia:docstore` meets the `DocumentDb` contract the
//! native doubles do.

#![cfg(target_arch = "wasm32")]

use omnia_sdk::WasiProvider;
use omnia_test::contract::document;

omnia_sdk::api::command::export!(scenario);

async fn scenario() {
    let db = WasiProvider;

    futures::join!(
        document::insert_exists(&db),
        document::portable_query(&db),
        document::filter_limits(&db),
        document::optional_query(&db),
    );
}
