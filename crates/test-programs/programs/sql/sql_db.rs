//! The `omnia_test::contract::sql` rows over `WasiProvider`, all at once, so
//! a backend's `omnia:sql` meets the `SqlDb` contract the native doubles do.

#![cfg(target_arch = "wasm32")]

use omnia_sdk::WasiProvider;
use omnia_test::contract::sql;

omnia_sdk::api::command::export!(scenario);

async fn scenario() {
    let db = WasiProvider;

    futures::join!(
        sql::batch(&db),
        sql::batch_rollback(&db),
        sql::placeholders(&db),
        sql::values(&db),
    );
}
