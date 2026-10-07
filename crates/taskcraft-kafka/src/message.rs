//! A consumed message and the codec that turns it into a task.

use std::marker::PhantomData;

use serde::de::DeserializeOwned;
use serde_json::{Map, Value};
use taskcraft::codec::{Codec, CodecError};
use taskcraft::{MetadataRegistry, Task, TaskId};

/// A message read from the topic, owned and detached from the client.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KafkaMessage {
    pub(crate) id: TaskId,
    pub(crate) key: Option<Vec<u8>>,
    pub(crate) payload: Option<Vec<u8>>,
    pub(crate) headers: Vec<(String, Option<Vec<u8>>)>,
    pub(crate) partition: i32,
    pub(crate) offset: i64,
}

impl KafkaMessage {
    /// A message, as the source builds it; for codec tests.
    #[must_use]
    pub fn new(id: TaskId, key: Option<Vec<u8>>, payload: Option<Vec<u8>>) -> Self {
        Self {
            id,
            key,
            payload,
            headers: Vec::new(),
            partition: 0,
            offset: 0,
        }
    }

    /// The same message with one more header.
    #[must_use]
    pub fn with_header(mut self, key: impl Into<String>, value: Option<Vec<u8>>) -> Self {
        self.headers.push((key.into(), value));
        self
    }

    /// The task id the source's id function gave the message.
    #[must_use]
    pub fn id(&self) -> &TaskId {
        &self.id
    }

    /// The message key.
    #[must_use]
    pub fn key(&self) -> Option<&[u8]> {
        self.key.as_deref()
    }

    /// The message body.
    #[must_use]
    pub fn payload(&self) -> Option<&[u8]> {
        self.payload.as_deref()
    }

    /// The headers in their order on the message.
    pub fn headers(&self) -> impl Iterator<Item = (&str, Option<&[u8]>)> {
        self.headers.iter().map(|(k, v)| (k.as_str(), v.as_deref()))
    }

    /// The partition.
    #[must_use]
    pub fn partition(&self) -> i32 {
        self.partition
    }

    /// The offset within the partition.
    #[must_use]
    pub fn offset(&self) -> i64 {
        self.offset
    }
}

/// The codec for task arguments written to the topic as JSON. Headers named
/// in the registry become the task's metadata (spec 2.12); other headers are
/// left alone.
///
/// A header value is JSON; a value that is not JSON but is UTF-8 is taken as
/// a JSON string.
#[derive(Debug, Clone)]
pub struct KafkaJsonCodec<Args> {
    registry: MetadataRegistry,
    _args: PhantomData<fn() -> Args>,
}

impl<Args> KafkaJsonCodec<Args> {
    /// A codec reading metadata headers named in `registry`.
    #[must_use]
    pub fn new(registry: MetadataRegistry) -> Self {
        Self {
            registry,
            _args: PhantomData,
        }
    }
}

impl<Args> Codec<Args, KafkaMessage> for KafkaJsonCodec<Args>
where
    Args: DeserializeOwned + Send + 'static,
{
    /// The Kafka source does not take tasks back: pushes and defer are not
    /// supported, so nothing is ever encoded.
    fn encode(&self, _: Task<Args>) -> Result<KafkaMessage, CodecError> {
        Err(CodecError::Encode(
            "the kafka source does not accept tasks".to_owned(),
        ))
    }

    fn decode(&self, message: KafkaMessage) -> Result<Task<Args>, CodecError> {
        let payload = message
            .payload
            .ok_or_else(|| CodecError::Decode("message has no body".to_owned()))?;
        let args =
            serde_json::from_slice(&payload).map_err(|e| CodecError::Decode(e.to_string()))?;
        let mut metadata = Map::new();
        for (name, value) in message.headers {
            if !self.registry.is_registered(&name) {
                continue;
            }
            let Some(bytes) = value else { continue };
            let value = serde_json::from_slice(&bytes).or_else(|_| {
                String::from_utf8(bytes).map(Value::String).map_err(|_| {
                    CodecError::Decode(format!("header {name} is neither JSON nor UTF-8"))
                })
            })?;
            metadata.insert(name, value);
        }
        let mut task = Task::new(args).with_id(message.id);
        *task.metadata_mut() = self.registry.decode(metadata);
        Ok(task)
    }
}

#[cfg(test)]
mod tests {
    use serde::{Deserialize, Serialize};

    use super::*;

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    struct Region(String);

    fn registry() -> MetadataRegistry {
        MetadataRegistry::new()
            .register::<Region>("billing.region")
            .unwrap()
    }

    fn codec() -> KafkaJsonCodec<u32> {
        KafkaJsonCodec::new(registry())
    }

    /// Change criterion 3: a header under a registered name is the task's
    /// metadata of that type.
    #[test]
    fn registered_headers_become_metadata() {
        let message = KafkaMessage::new(TaskId::new("k"), None, Some(b"7".to_vec()))
            .with_header("billing.region", Some(br#""eu""#.to_vec()))
            .with_header("content-type", Some(b"json".to_vec()));
        let task = codec().decode(message).unwrap();
        assert_eq!((task.id().as_str(), *task.args()), ("k", 7));
        let region = task.metadata().resolve::<Region>(&registry()).unwrap();
        assert_eq!(region, Some(Region("eu".into())));
        let encoded = registry().encode(task.metadata()).unwrap();
        assert_eq!(encoded.len(), 1, "unregistered header left alone");
    }

    #[test]
    fn plain_text_header_is_a_json_string() {
        let message = KafkaMessage::new(TaskId::new("k"), None, Some(b"1".to_vec()))
            .with_header("billing.region", Some(b"eu".to_vec()));
        let task = codec().decode(message).unwrap();
        let region = task.metadata().resolve::<Region>(&registry()).unwrap();
        assert_eq!(region, Some(Region("eu".into())));
    }

    #[test]
    fn bad_bodies_are_poison() {
        let empty = KafkaMessage::new(TaskId::new("k"), None, None);
        assert!(matches!(codec().decode(empty), Err(CodecError::Decode(_))));
        let wrong = KafkaMessage::new(TaskId::new("k"), None, Some(b"\"x\"".to_vec()));
        assert!(matches!(codec().decode(wrong), Err(CodecError::Decode(_))));
        assert!(codec().encode(Task::new(1)).is_err());
    }
}
