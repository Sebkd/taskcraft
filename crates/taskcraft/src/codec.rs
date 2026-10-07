//! Codecs: turning a source's raw message into a task and back (spec 2.5).

use std::marker::PhantomData;

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use crate::error::MetadataError;
use crate::metadata::MetadataRegistry;
use crate::source::StoreMessage;
use crate::task::{AckPoint, Task, TaskId, TaskParts};

/// A message that could not be turned into a task, or a task that could not
/// be turned into a message.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum CodecError {
    /// The message is not a valid task. Retrying will not help: it is a
    /// poison message.
    #[error("message could not be decoded: {0}")]
    Decode(String),
    /// The task could not be encoded.
    #[error("task could not be encoded: {0}")]
    Encode(String),
    /// Metadata could not be encoded, for example an unregistered type.
    #[error(transparent)]
    Metadata(#[from] MetadataError),
}

/// Turns a source's messages into tasks and tasks into messages.
pub trait Codec<Args, M>: Send + Sync + 'static {
    /// Encodes a task for the source.
    ///
    /// # Errors
    ///
    /// [`CodecError::Encode`] or [`CodecError::Metadata`].
    fn encode(&self, task: Task<Args>) -> Result<M, CodecError>;

    /// Decodes a message from the source.
    ///
    /// # Errors
    ///
    /// [`CodecError::Decode`] when the message is not a valid task.
    fn decode(&self, message: M) -> Result<Task<Args>, CodecError>;
}

/// The codec for sources that keep tasks as they are, such as the in-memory
/// source. Metadata stays typed.
#[derive(Debug)]
pub struct IdentityCodec<Args>(PhantomData<fn(Args) -> Args>);

impl<Args> IdentityCodec<Args> {
    /// The identity codec.
    #[must_use]
    pub const fn new() -> Self {
        Self(PhantomData)
    }
}

impl<Args> Default for IdentityCodec<Args> {
    fn default() -> Self {
        Self::new()
    }
}

impl<Args> Clone for IdentityCodec<Args> {
    fn clone(&self) -> Self {
        Self::new()
    }
}

impl<Args: Send + 'static> Codec<Args, Task<Args>> for IdentityCodec<Args> {
    fn encode(&self, task: Task<Args>) -> Result<Task<Args>, CodecError> {
        Ok(task)
    }

    fn decode(&self, message: Task<Args>) -> Result<Task<Args>, CodecError> {
        Ok(message)
    }
}

/// The JSON envelope of a task.
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Envelope<A> {
    id: TaskId,
    args: A,
    metadata: Map<String, Value>,
    attempt: u32,
    retries: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    ack_point: Option<AckPoint>,
}

/// The default codec: a task as a JSON object with its id, arguments,
/// metadata (named through the registry), attempt counters and ack point.
///
/// Metadata is not parsed on decode; handlers parse it on lookup.
#[derive(Debug, Clone)]
pub struct JsonCodec {
    registry: MetadataRegistry,
}

impl JsonCodec {
    /// A JSON codec naming metadata through `registry`.
    #[must_use]
    pub fn new(registry: MetadataRegistry) -> Self {
        Self { registry }
    }

    /// The metadata registry.
    #[must_use]
    pub fn registry(&self) -> &MetadataRegistry {
        &self.registry
    }
}

impl<Args> Codec<Args, Vec<u8>> for JsonCodec
where
    Args: Serialize + DeserializeOwned + Send + 'static,
{
    fn encode(&self, task: Task<Args>) -> Result<Vec<u8>, CodecError> {
        let parts = task.into_parts();
        let envelope = Envelope {
            metadata: self.registry.encode(&parts.metadata)?,
            id: parts.id,
            args: parts.args,
            attempt: parts.attempt,
            retries: parts.retries,
            ack_point: parts.ack_point,
        };
        serde_json::to_vec(&envelope).map_err(|e| CodecError::Encode(e.to_string()))
    }

    fn decode(&self, message: Vec<u8>) -> Result<Task<Args>, CodecError> {
        let envelope: Envelope<Args> =
            serde_json::from_slice(&message).map_err(|e| CodecError::Decode(e.to_string()))?;
        Ok(Task::from_parts(TaskParts {
            id: envelope.id,
            args: envelope.args,
            metadata: self.registry.decode(envelope.metadata),
            attempt: envelope.attempt,
            retries: envelope.retries,
            ack_point: envelope.ack_point,
            accepted_at: None,
        }))
    }
}

/// The built-in codec of a task store: the same JSON envelope, as a
/// [`StoreMessage`].
impl<Args> Codec<Args, StoreMessage> for JsonCodec
where
    Args: Serialize + DeserializeOwned + Send + 'static,
{
    fn encode(&self, task: Task<Args>) -> Result<StoreMessage, CodecError> {
        Codec::<Args, Vec<u8>>::encode(self, task).map(StoreMessage::from_bytes)
    }

    fn decode(&self, message: StoreMessage) -> Result<Task<Args>, CodecError> {
        Codec::<Args, Vec<u8>>::decode(self, message.into_bytes())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
    struct Priority(u8);

    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    struct Report {
        month: String,
    }

    fn codec() -> JsonCodec {
        JsonCodec::new(
            MetadataRegistry::new()
                .register::<Priority>("report.priority")
                .unwrap(),
        )
    }

    #[test]
    fn json_round_trip_keeps_every_field() {
        let mut parts = Task::new(Report {
            month: "2026-10".into(),
        })
        .with_id("r-1")
        .with_meta(Priority(5))
        .with_ack_point(AckPoint::OnCompletion)
        .into_parts();
        parts.attempt = 3;
        parts.retries = 2;
        let codec = codec();

        let bytes: Vec<u8> = codec.encode(Task::from_parts(parts)).unwrap();
        let back: Task<Report> = codec.decode(bytes).unwrap();

        assert_eq!(back.id().as_str(), "r-1");
        assert_eq!(back.args().month, "2026-10");
        assert_eq!((back.attempt(), back.retries()), (3, 2));
        assert_eq!(back.ack_point(), Some(AckPoint::OnCompletion));
        assert_eq!(
            back.metadata()
                .resolve::<Priority>(codec.registry())
                .unwrap(),
            Some(Priority(5))
        );
        assert!(back.metadata().get::<Priority>().is_none(), "parsed lazily");
    }

    #[test]
    fn unregistered_metadata_fails_encoding() {
        let task = Task::new(1_u8).with_meta(7_i64);
        let err = Codec::<u8, Vec<u8>>::encode(&codec(), task).unwrap_err();
        assert!(matches!(
            err,
            CodecError::Metadata(MetadataError::UnregisteredType { .. })
        ));
    }

    #[test]
    fn broken_messages_are_decode_errors() {
        let codec = codec();
        for bad in [
            &b"not json"[..],
            br#"{"id":"x","args":1,"metadata":[],"attempt":0,"retries":0}"#,
            br#"{"id":"x","args":1,"metadata":{},"retries":0}"#,
            br#"{"id":"x","args":"one","metadata":{},"attempt":0,"retries":0}"#,
        ] {
            let result: Result<Task<u8>, _> = codec.decode(bad.to_vec());
            assert!(matches!(result, Err(CodecError::Decode(_))), "{bad:?}");
        }
    }

    #[test]
    fn identity_keeps_metadata_typed() {
        let codec = IdentityCodec::new();
        let task = codec.encode(Task::new(()).with_meta(Priority(1))).unwrap();
        let task = codec.decode(task).unwrap();
        assert_eq!(task.metadata().get::<Priority>(), Some(&Priority(1)));
    }
}
