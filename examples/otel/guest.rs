//! # OpenTelemetry example — HTTP guest
//!
//! Answers `POST /` with a greeting. Along the way it opens spans through
//! `tracing` and through the OpenTelemetry API, and records a counter, an
//! up-down counter, a histogram, and a gauge through the OpenTelemetry
//! metrics API. See `README.md`.

#![cfg(target_arch = "wasm32")]

use axum::routing::post;
use axum::{Json, Router};
use opentelemetry::trace::{TraceContextExt, Tracer};
use opentelemetry::{KeyValue, global};
use serde_json::{Value, json};

const SCOPE: &str = "otel-example";

omnia_sdk::http::export!(router);

fn router() -> Router {
    Router::new().route("/", post(greet))
}

async fn greet(Json(request): Json<Value>) -> Json<Value> {
    // count the request as in flight
    let meter = global::meter(SCOPE);
    let in_flight = meter.i64_up_down_counter("in_flight").with_unit("{request}").build();
    in_flight.add(1, &[]);

    // read the name inside a `tracing` span
    let name = tracing::info_span!("parse").in_scope(|| {
        let name = request["text"].as_str().unwrap_or("World").to_owned();
        tracing::info!(%name, "parsed the request");
        name
    });

    // compose the reply inside an OpenTelemetry API span
    let reply = global::tracer(SCOPE).in_span("compose", |cx| {
        cx.span().set_attribute(KeyValue::new("greeting.name", name.clone()));
        json!({ "message": format!("Hello, {name}!") })
    });

    // record the request
    let requests = meter.u64_counter("requests").with_unit("{request}").build();
    requests.add(1, &[KeyValue::new("route", "/")]);
    let name_length = meter.u64_histogram("name_length").with_unit("{char}").build();
    name_length.record(name.chars().count() as u64, &[]);
    let reply_size = meter.u64_gauge("reply_size").with_unit("By").build();
    reply_size.record(reply.to_string().len() as u64, &[]);
    in_flight.add(-1, &[]);

    Json(reply)
}
