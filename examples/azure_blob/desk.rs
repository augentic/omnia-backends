use std::env;

use dotenvy::dotenv;
use omnia::Backend;
use omnia_azure_blob::{Client, ConnectOptions, CredentialOptions};
use omnia_wasi_blobstore::WasiBlobstoreCtx;
use tracing_subscriber::filter::EnvFilter;
use tracing_subscriber::fmt;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

const CONTAINER: &str = "testaugenticblob";

#[tokio::main]
pub async fn main() {
    let _ = dotenv();
    tracing_subscriber::registry().with(fmt::layer()).with(EnvFilter::from_default_env()).init();

    tracing::info!("Azure Blob Storage backend desk-test");

    let endpoint = env::var("AZURE_BLOB_ENDPOINT").expect("Set AZURE_BLOB_ENDPOINT env variable");

    let credential = match (
        env::var("AZURE_TENANT_ID").ok(),
        env::var("AZURE_CLIENT_ID").ok(),
        env::var("AZURE_CLIENT_SECRET").ok(),
    ) {
        (Some(tenant_id), Some(client_id), Some(client_secret)) => Some(CredentialOptions {
            tenant_id,
            client_id,
            client_secret,
        }),
        _ => None,
    };

    let cnn_opts = ConnectOptions { endpoint, credential };

    let client = Client::connect_with(cnn_opts).await.expect("Failed to connect");

    // --- ensure container exists (idempotent) ---
    let exists = client
        .container_exists(CONTAINER.to_string())
        .await
        .expect("Failed to check container existence");
    let container = if exists {
        tracing::info!("container already exists, reusing: {CONTAINER}");
        client.get_container(CONTAINER.to_string()).await.expect("Failed to get container")
    } else {
        tracing::info!("creating container: {CONTAINER}");
        client.create_container(CONTAINER.to_string()).await.expect("Failed to create container")
    };
    tracing::info!("container exists: true");

    // --- write blobs ---
    tracing::info!("writing greeting.txt");
    container
        .write_data("greeting.txt".to_string(), b"hello world".to_vec().into())
        .await
        .expect("Failed to write greeting.txt");

    tracing::info!("writing data.json");
    container
        .write_data("data.json".to_string(), br#"{"name":"Alice","age":30}"#.to_vec().into())
        .await
        .expect("Failed to write data.json");

    // --- get_data ---
    let data = container
        .get_data("greeting.txt".to_string(), 0, u64::MAX)
        .await
        .expect("Failed to get greeting.txt");
    tracing::info!(
        "greeting.txt content: {:?}",
        data.map(|d| String::from_utf8_lossy(&d).to_string())
    );

    // --- list_objects ---
    let names = container.list_objects().await.expect("Failed to list objects");
    tracing::info!("blobs in container: {names:?}");

    // --- has_object ---
    let has = container
        .has_object("greeting.txt".to_string())
        .await
        .expect("Failed to check object existence");
    tracing::info!("has greeting.txt: {has}");

    // --- object_info ---
    let info =
        container.object_info("greeting.txt".to_string()).await.expect("Failed to get object info");
    tracing::info!(
        "greeting.txt info: name={}, size={}, created_at={}",
        info.name,
        info.size,
        info.created_at
    );

    // --- chunked stream read ---
    const CHUNK_TEST_SIZE: usize = 8 * 1024 * 1024; // 8 MiB -- exceeds the 4 MiB default chunk
    const CHUNK_TEST_BLOB: &str = "chunk-test.bin";

    tracing::info!(
        "writing {CHUNK_TEST_BLOB} ({} MiB) to exercise chunked download",
        CHUNK_TEST_SIZE / (1024 * 1024)
    );

    let test_data: Vec<u8> = (0..CHUNK_TEST_SIZE).map(|i| (i % 256) as u8).collect();
    container
        .write_data(CHUNK_TEST_BLOB.to_string(), test_data.clone().into())
        .await
        .expect("Failed to write chunk-test blob");

    let read_back = container
        .get_data(CHUNK_TEST_BLOB.to_string(), 0, u64::MAX)
        .await
        .expect("Failed to read chunk-test blob")
        .expect("chunk-test blob should exist");

    assert_eq!(read_back.len(), CHUNK_TEST_SIZE, "round-tripped size mismatch");
    assert_eq!(
        read_back, test_data,
        "round-tripped content mismatch -- chunked reassembly may be broken"
    );
    tracing::info!("chunked read OK: {CHUNK_TEST_SIZE} bytes verified");

    let info = container
        .object_info(CHUNK_TEST_BLOB.to_string())
        .await
        .expect("Failed to get chunk-test blob info");
    assert_eq!(info.size, CHUNK_TEST_SIZE as u64, "object_info size mismatch");
    tracing::info!("chunk-test blob size from object_info: {}", info.size);

    // --- delete_object ---
    tracing::info!("deleting greeting.txt");
    container
        .delete_object("greeting.txt".to_string())
        .await
        .expect("Failed to delete greeting.txt");

    let has = container
        .has_object("greeting.txt".to_string())
        .await
        .expect("Failed to check object existence");
    tracing::info!("has greeting.txt after delete: {has}");

    // --- cleanup: delete all blobs then container ---
    let remaining = container.list_objects().await.expect("Failed to list objects for cleanup");
    for blob in &remaining {
        tracing::info!("cleaning up {blob}");
        container.delete_object(blob.clone()).await.expect("Failed to delete blob");
    }

    tracing::info!("deleting container: {CONTAINER}");
    client.delete_container(CONTAINER.to_string()).await.expect("Failed to delete container");

    tracing::info!("done");
}
