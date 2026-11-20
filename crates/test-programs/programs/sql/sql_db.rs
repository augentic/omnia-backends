//! The `omnia_test::contract::sql` rows over the `default` database, all at
//! once, so a backend's `omnia:sql` meets the contract.

#![cfg(target_arch = "wasm32")]

use omnia_sdk::sql::{DEFAULT, store};
use omnia_test::contract::sql;

omnia_sdk::command::export!(scenario);

async fn scenario() {
    let db = store::open(DEFAULT.to_owned()).await.expect("open the default database");

    futures::join!(
        sql::batch(&db),
        sql::batch_rollback(&db),
        sql::placeholders(&db),
        sql::values(&db),
    );
}
