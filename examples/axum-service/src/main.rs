//! E-11 `axum-service` — an HTTP service over a queue: push, status and
//! cancel by id through endpoints (spec 2.1.2.1, 2.1.2.14, 2.1.2.15).
//!
//! ```text
//! cargo run --manifest-path examples/axum-service/Cargo.toml
//!
//! curl -X POST localhost:3000/exports -H 'content-type: application/json' \
//!      -d '{"id": "orders-2026-10", "table": "orders"}'
//! curl localhost:3000/exports/orders-2026-10
//! curl -X DELETE localhost:3000/exports/orders-2026-10
//! ```
//!
//! Ctrl-C stops the server and then the queue, and prints the shutdown
//! report.

use std::sync::Arc;
use std::time::Duration;

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::Deserialize;
use serde_json::{Value, json};
use taskcraft::codec::IdentityCodec;
use taskcraft::{
    Cancel, CancelOutcome, CancellationToken, InMemorySource, Monitor, PushOutcome, Queue,
    QueueHandle, Task, TaskId, task_fn,
};

type Exports = QueueHandle<String>;

/// An export: a minute of work, stopping early when cancelled.
async fn export(table: String, cancel: Cancel) {
    println!("exporting {table}");
    tokio::select! {
        () = cancel.cancelled() => println!("export of {table} cancelled"),
        () = tokio::time::sleep(Duration::from_secs(60)) => println!("exported {table}"),
    }
}

#[derive(Deserialize)]
struct NewExport {
    id: String,
    table: String,
}

async fn push(
    State(exports): State<Exports>,
    Json(new): Json<NewExport>,
) -> (StatusCode, Json<Value>) {
    let task = Task::new(new.table).with_id(new.id);
    match exports.push(task).await {
        Ok(PushOutcome::Enqueued { id }) => (
            StatusCode::ACCEPTED,
            Json(json!({ "id": id.as_str(), "outcome": "enqueued" })),
        ),
        Ok(PushOutcome::AlreadyRunning { id, state }) => (
            StatusCode::OK,
            Json(
                json!({ "id": id.as_str(), "outcome": "already running", "state": state.as_str() }),
            ),
        ),
        Ok(other) => (
            StatusCode::CONFLICT,
            Json(json!({ "id": other.id().as_str(), "outcome": format!("{other:?}") })),
        ),
        Err(error) => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({ "error": error.to_string() })),
        ),
    }
}

async fn status(
    State(exports): State<Exports>,
    Path(id): Path<String>,
) -> (StatusCode, Json<Value>) {
    match exports.status(&TaskId::new(id.as_str())) {
        Some(status) => (
            StatusCode::OK,
            Json(json!({
                "id": id,
                "state": status.state().as_str(),
                "attempt": status.attempt(),
            })),
        ),
        // Without a task store, finished tasks are unknown.
        None => (
            StatusCode::NOT_FOUND,
            Json(json!({ "id": id, "state": "unknown" })),
        ),
    }
}

async fn cancel(
    State(exports): State<Exports>,
    Path(id): Path<String>,
) -> (StatusCode, Json<Value>) {
    let outcome = exports.cancel(&TaskId::new(id.as_str())).await;
    let (code, text) = match outcome {
        CancelOutcome::CancelRequested => (StatusCode::ACCEPTED, "cancel requested"),
        CancelOutcome::Cancelled => (StatusCode::OK, "cancelled"),
        CancelOutcome::AlreadyFinished => (StatusCode::OK, "already finished"),
        _ => (StatusCode::NOT_FOUND, "unknown"),
    };
    (code, Json(json!({ "id": id, "outcome": text })))
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let queue = Queue::builder(
        "exports",
        Arc::new(InMemorySource::default()),
        IdentityCodec::new(),
        task_fn(export),
    )
    .concurrency(2)
    .no_recovery()
    .build()?;
    let stop = CancellationToken::new();
    let (monitor, exports) = Monitor::new().register(queue)?;
    let monitor = tokio::spawn(monitor.run(stop.clone()));

    let app = Router::new()
        .route("/exports", post(push))
        .route("/exports/{id}", get(status).delete(cancel))
        .with_state(exports);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:3000").await?;
    println!("listening on http://127.0.0.1:3000");
    axum::serve(listener, app)
        .with_graceful_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await?;

    stop.cancel();
    let report = monitor.await??;
    println!(
        "queue stopped: completed={}, cancelled={}, aborted={}",
        report.completed(),
        report.cancelled(),
        report.aborted()
    );
    Ok(())
}
