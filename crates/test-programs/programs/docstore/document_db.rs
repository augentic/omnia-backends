//! The `omnia_test::contract::document` rows, all at once, so a backend's
//! `omnia:docstore` meets the contract.

#![cfg(target_arch = "wasm32")]

use omnia_test::contract::document;

omnia_sdk::api::command::export!(scenario);

async fn scenario() {
    futures::join!(
        document::insert_exists(),
        document::portable_query(),
        document::filter_limits(),
        document::optional_query(),
    );
}
