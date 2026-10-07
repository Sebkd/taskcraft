//! `JsonCodec::decode` on any bytes: an error, never a panic or a hang.
#![no_main]

use libfuzzer_sys::fuzz_target;
use taskcraft::codec::{Codec, JsonCodec};
use taskcraft::{MetadataRegistry, Task};

fuzz_target!(|data: &[u8]| {
    let codec = JsonCodec::new(MetadataRegistry::new());
    let _: Result<Task<serde_json::Value>, _> =
        Codec::<serde_json::Value, Vec<u8>>::decode(&codec, data.to_vec());
    let _: Result<Task<u32>, _> = Codec::<u32, Vec<u8>>::decode(&codec, data.to_vec());
});
