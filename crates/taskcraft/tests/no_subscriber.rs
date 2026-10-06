//! Without a subscriber the library stays silent and installs none of its
//! own (spec 2.9, change criterion 6).

// Test helpers may unwrap and panic (invariant 1.3.19 covers library code).
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

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

/// Invariant 1.3.19: no assertions in library code — clippy has no lint
/// for them, and a failed one is a panic. `debug_assert!` stays allowed.
#[test]
fn library_sources_do_not_assert() {
    let crates = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("..");
    for entry in std::fs::read_dir(crates).unwrap() {
        let mut pending = vec![entry.unwrap().path().join("src")];
        while let Some(dir) = pending.pop() {
            let Ok(entries) = std::fs::read_dir(&dir) else {
                continue;
            };
            for entry in entries {
                let path = entry.unwrap().path();
                if path.is_dir() {
                    pending.push(path);
                    continue;
                }
                let text = std::fs::read_to_string(&path).unwrap();
                // Library code ends where the unit tests begin.
                let library = text.split("#[cfg(test)]").next().unwrap_or_default();
                for (n, line) in library.lines().enumerate() {
                    let code = line.trim_start();
                    if code.starts_with("//") {
                        continue;
                    }
                    for banned in ["assert!(", "assert_eq!(", "assert_ne!("] {
                        let found = code
                            .match_indices(banned)
                            .any(|(i, _)| !code[..i].ends_with("debug_"));
                        assert!(!found, "{banned} in {}:{}", path.display(), n + 1);
                    }
                }
            }
        }
    }
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
