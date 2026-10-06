//! Without a subscriber the library stays silent and installs none of its
//! own (spec 2.9, change criterion 6).

use std::sync::Arc;

use taskcraft::{CancellationToken, IdentityCodec, InMemorySource, Monitor, Queue, Task, task_fn};

#[tokio::test]
async fn no_subscriber_is_installed() {
    let source = Arc::new(InMemorySource::<u32>::default());
    let queue = Queue::builder(
        "quiet",
        Arc::clone(&source),
        IdentityCodec::new(),
        task_fn(|_: u32| async {}),
    )
    .no_recovery()
    .build()
    .unwrap();
    let handle = queue.handle();
    let stop = CancellationToken::new();
    let running = tokio::spawn(Monitor::new().register(queue).unwrap().run(stop.clone()));
    let _ = handle.push(Task::new(1)).await.unwrap();
    while !source.is_empty() || handle.live_tasks() > 0 {
        tokio::task::yield_now().await;
    }
    stop.cancel();
    running.await.unwrap().unwrap();
    assert!(!tracing::dispatcher::has_been_set());
}

/// The library prints nothing itself: no print macros in its sources.
#[test]
fn library_sources_print_nothing() {
    let src = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let mut pending = vec![src];
    while let Some(dir) = pending.pop() {
        for entry in std::fs::read_dir(dir).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                pending.push(path);
            } else {
                let text = std::fs::read_to_string(&path).unwrap();
                for banned in ["println!", "eprintln!", "print!(", "eprint!(", "dbg!("] {
                    assert!(!text.contains(banned), "{banned} in {}", path.display());
                }
            }
        }
    }
}
