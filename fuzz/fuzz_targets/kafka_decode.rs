//! `KafkaJsonCodec::decode` on a message built from any bytes: the body and
//! up to four headers, split at 0xff; then the metadata it carries.
#![no_main]

use libfuzzer_sys::fuzz_target;
use taskcraft::codec::Codec;
use taskcraft::{MetadataRegistry, TaskId, TraceParent};
use taskcraft_kafka::{KafkaJsonCodec, KafkaMessage};

#[derive(Clone, serde::Serialize, serde::Deserialize)]
struct Region(String);

const HEADERS: [&str; 4] = ["billing.region", "trace_parent", "unknown", "billing.region"];

fuzz_target!(|data: &[u8]| {
    let registry = MetadataRegistry::new()
        .register::<Region>("billing.region")
        .expect("a valid registration");
    let mut parts = data.split(|b| *b == 0xff);
    let body = parts.next().map(<[u8]>::to_vec);
    let mut message = KafkaMessage::new(TaskId::new("t"), None, body);
    for (name, value) in HEADERS.iter().zip(parts) {
        message = message.with_header(*name, Some(value.to_vec()));
    }
    let codec = KafkaJsonCodec::<serde_json::Value>::new(registry.clone());
    if let Ok(task) = codec.decode(message) {
        let _ = task.metadata().resolve::<TraceParent>(&registry);
        let _ = task.metadata().resolve::<Region>(&registry);
    }
});
