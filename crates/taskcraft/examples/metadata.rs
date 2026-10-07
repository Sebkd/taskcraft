//! E-7 `metadata` — two scenarios of one application with their own
//! metadata types; the registry that names them; values a worker does not
//! know survive re-encoding (spec 2.3.17, 2.7.2).
//!
//! ```text
//! cargo run -p taskcraft --example metadata
//! ```

use std::sync::Arc;

use serde::{Deserialize, Serialize};
use std::sync::Mutex;

use taskcraft::codec::{Codec, IdentityCodec, JsonCodec};
use taskcraft::observe::{Event, Observer};
use taskcraft::{
    CancellationToken, InMemorySource, Meta, MetadataRegistry, Monitor, Queue, Task, TaskState,
    task_fn,
};

/// Prints failures and remembers them.
#[derive(Default)]
struct Failures(Mutex<Vec<String>>);

impl Observer for Failures {
    fn on_event(&self, event: &Event<'_>) {
        if let Event::Finished {
            task_id,
            state: TaskState::Failed,
            reason,
            ..
        } = event
        {
            let reason = reason.map(ToString::to_string).unwrap_or_default();
            println!("  {task_id} failed: {reason}");
            if let Ok(mut failures) = self.0.lock() {
                failures.push(task_id.as_str().to_owned());
            }
        }
    }
}

/// Scenario "mail": who the letter goes to.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct Recipient {
    email: String,
}

/// Scenario "export": how urgent the export is.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
struct Priority(u8);

/// Requires a recipient: a task without one fails before the handler runs.
async fn send_mail(subject: String, Meta(to): Meta<Recipient>) {
    println!("  mail {subject:?} to {}", to.email);
}

/// Priority is optional here.
async fn export(table: String, priority: Option<Meta<Priority>>) {
    let priority = priority.map_or(0, |Meta(Priority(p))| p);
    println!("  export {table} at priority {priority}");
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // In process, metadata stays typed: no registry needed.
    let mail = Queue::builder(
        "mail",
        Arc::new(InMemorySource::default()),
        IdentityCodec::new(),
        task_fn(send_mail),
    )
    .no_recovery()
    .build()?;
    let exports = Queue::builder(
        "exports",
        Arc::new(InMemorySource::default()),
        IdentityCodec::new(),
        task_fn(export),
    )
    .no_recovery()
    .build()?;
    let stop = CancellationToken::new();
    let failures = Arc::new(Failures::default());
    let monitor = Monitor::new().observer(Arc::clone(&failures));
    let (monitor, mail_handle) = monitor.register(mail)?;
    let (monitor, export_handle) = monitor.register(exports)?;
    let running = tokio::spawn(monitor.run(stop.clone()));

    let letter = Task::new("invoice".to_owned()).with_meta(Recipient {
        email: "billing@example.com".into(),
    });
    let _ = mail_handle.push(letter).await?;
    let unaddressed = Task::new("lost letter".to_owned()).with_id("lost");
    let _ = mail_handle.push(unaddressed).await?;
    let _ = export_handle
        .push(Task::new("orders".to_owned()).with_meta(Priority(5)))
        .await?;
    let _ = export_handle.push(Task::new("audit".to_owned())).await?;
    while mail_handle.live_tasks() + export_handle.live_tasks() > 0 {
        tokio::task::yield_now().await;
    }
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    stop.cancel();
    running.await??;
    if *failures.0.lock().map_err(|_| "poisoned")? != ["lost"] {
        return Err("only the unaddressed letter should fail".into());
    }

    // Outside the process, metadata is named by the registry.
    let registry = MetadataRegistry::new()
        .register::<Recipient>("mail.recipient")?
        .register::<Priority>("export.priority")?;
    let codec = JsonCodec::new(registry);
    let task = Task::new("orders".to_owned()).with_meta(Priority(7));
    let bytes: Vec<u8> = Codec::<String, Vec<u8>>::encode(&codec, task)?;
    println!("stored: {}", String::from_utf8_lossy(&bytes));

    // An older worker knows fewer names: the unknown value is kept as is
    // and written back unchanged.
    let older = JsonCodec::new(MetadataRegistry::new().register::<Recipient>("mail.recipient")?);
    let read: Task<String> = older.decode(bytes)?;
    let again: Vec<u8> = older.encode(read)?;
    let again = String::from_utf8_lossy(&again).into_owned();
    println!("re-encoded by an older worker: {again}");
    if !again.contains(r#""export.priority":7"#) {
        return Err("the unknown value was lost".into());
    }
    Ok(())
}
