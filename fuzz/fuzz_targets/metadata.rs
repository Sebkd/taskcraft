//! A task envelope whose metadata is any JSON object: decoding and resolving
//! known and unknown names give values or errors, never a panic.
#![no_main]

use libfuzzer_sys::fuzz_target;
use taskcraft::codec::{Codec, JsonCodec};
use taskcraft::{MetadataRegistry, Task, TraceParent};

#[derive(Clone, serde::Serialize, serde::Deserialize)]
struct Priority(u8);

fuzz_target!(|data: &[u8]| {
    let Ok(metadata) = serde_json::from_slice::<serde_json::Map<String, serde_json::Value>>(data)
    else {
        return;
    };
    let envelope = serde_json::json!({
        "id": "t", "args": 0, "metadata": metadata, "attempt": 0, "retries": 0,
    });
    let registry = MetadataRegistry::new()
        .register::<Priority>("report.priority")
        .expect("a valid registration");
    let codec = JsonCodec::new(registry.clone());
    let bytes = serde_json::to_vec(&envelope).expect("a JSON value serialises");
    if let Ok(task) = Codec::<u32, Vec<u8>>::decode(&codec, bytes) {
        let task: Task<u32> = task;
        let _ = task.metadata().resolve::<TraceParent>(&registry);
        let _ = task.metadata().resolve::<Priority>(&registry);
    }
});
