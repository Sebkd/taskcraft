//! The Kafka source: a consumer of one topic within a consumer group.

use std::collections::BTreeMap;
use std::fmt;
use std::panic::{RefUnwindSafe, UnwindSafe};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::{Duration, Instant};

use rdkafka::consumer::{
    BaseConsumer, CommitMode, Consumer, ConsumerContext, Rebalance, StreamConsumer,
};
use rdkafka::error::{KafkaError, KafkaResult, RDKafkaErrorCode};
use rdkafka::message::{Headers, Message};
use rdkafka::{ClientConfig, ClientContext, Offset, TopicPartitionList};
use taskcraft::TaskId;
use taskcraft::error::ConfigError;
use taskcraft::source::{
    AckPointSupport, Capabilities, Notice, Notices, Polled, Source, WakeHandle, WakeSignal,
};
use tokio::sync::mpsc;
use tracing::warn;

use crate::commits::{Commits, KafkaReceipt};
use crate::message::KafkaMessage;

/// What the Kafka source can fail with.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum KafkaSourceError {
    /// A setting of the source is out of range.
    #[error(transparent)]
    Config(#[from] ConfigError),
    /// The client failed: creating the consumer, reading or committing.
    #[error(transparent)]
    Client(KafkaClientError),
    /// No broker is reachable, or the client hit a fatal error.
    #[error("broker unavailable: {0}")]
    Broker(String),
}

/// A failure of the Kafka client. Its cause — the client's own error — is
/// reachable through [`source`](std::error::Error::source); its type is not
/// part of this API, so a client update does not change it.
#[derive(Debug)]
pub struct KafkaClientError(
    // Unwind-safe like the client's error it replaces in 0.3.
    Box<dyn std::error::Error + Send + Sync + UnwindSafe + RefUnwindSafe>,
);

impl fmt::Display for KafkaClientError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl std::error::Error for KafkaClientError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&*self.0)
    }
}

impl From<KafkaClientError> for KafkaSourceError {
    fn from(error: KafkaClientError) -> Self {
        Self::Client(error)
    }
}

/// Turns a client result into the source's: the client's error becomes the
/// cause of [`KafkaSourceError::Client`].
trait OrClient<T> {
    fn client(self) -> Result<T, KafkaSourceError>;
}

impl<T> OrClient<T> for Result<T, KafkaError> {
    fn client(self) -> Result<T, KafkaSourceError> {
        self.map_err(|error| KafkaSourceError::Client(KafkaClientError(Box::new(error))))
    }
}

/// Derives a task id from a message (spec 2.6). The message's own
/// [`id`](KafkaMessage::id) is a generated one at that point; returning
/// `None` keeps it.
pub type TaskIdFn = Arc<dyn Fn(&KafkaMessage) -> Option<TaskId> + Send + Sync>;

/// The default id: the message key as text; no key, no id.
fn key_as_id(message: &KafkaMessage) -> Option<TaskId> {
    message
        .key()
        .map(|key| TaskId::new(String::from_utf8_lossy(key)))
}

type Shared<T> = Arc<Mutex<T>>;

fn lock<T>(shared: &Mutex<T>) -> MutexGuard<'_, T> {
    // Every critical section is a few infallible map calls.
    shared.lock().unwrap_or_else(PoisonError::into_inner)
}

/// Client callbacks: rebalances, errors and commit results.
struct Callbacks {
    topic: String,
    commits: Shared<Commits>,
    failure: Shared<Option<String>>,
    notify: mpsc::UnboundedSender<Notice>,
}

impl ClientContext for Callbacks {
    fn error(&self, error: KafkaError, reason: &str) {
        let code = error.rdkafka_error_code();
        if matches!(
            code,
            Some(RDKafkaErrorCode::AllBrokersDown | RDKafkaErrorCode::Fatal)
        ) {
            // Surfaces as a source error on the next poll (rule 2.3.12).
            *lock(&self.failure) = Some(format!("{error}: {reason}"));
        } else {
            warn!(
                event = "source",
                action = "client_error",
                "kafka client error: topic={}, error={:?}",
                self.topic,
                format!("{error}: {reason}")
            );
        }
    }
}

impl ConsumerContext for Callbacks {
    fn pre_rebalance(&self, _: &BaseConsumer<Self>, rebalance: &Rebalance<'_>) {
        if let Rebalance::Revoke(partitions) = rebalance {
            lock(&self.commits).revoke(partitions.elements().iter().map(|e| e.partition()));
        }
    }

    fn commit_callback(&self, result: KafkaResult<()>, offsets: &TopicPartitionList) {
        let Err(error) = result else {
            return;
        };
        for element in offsets.elements() {
            let Offset::Offset(next) = element.offset() else {
                continue;
            };
            warn!(
                event = "source",
                action = "commit_failed",
                "offset commit failed: topic={}, partition={}, offset={}, error={:?}",
                self.topic,
                element.partition(),
                next,
                error.to_string()
            );
            lock(&self.commits).commit_failed(element.partition(), next, Instant::now());
        }
        // Counted as a source error by the worker (change kafka-commit-errors).
        let _ = self.notify.send(Notice::SourceError(format!(
            "offset commit failed: {error}"
        )));
    }
}

/// Builds a [`KafkaSource`].
#[must_use]
pub struct KafkaSourceBuilder {
    brokers: String,
    topic: String,
    group: String,
    properties: BTreeMap<String, String>,
    task_id: TaskIdFn,
    max_wait: Duration,
}

impl KafkaSourceBuilder {
    /// Sets a client property (librdkafka configuration). Automatic offset
    /// commit and offset store stay off whatever is set here: offsets are
    /// committed only on ack.
    pub fn property(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.properties.insert(key.into(), value.into());
        self
    }

    /// How a task id is derived from a message. Default: the key; a message
    /// without a key gets a generated id, and then a redelivery is not
    /// recognised as a duplicate.
    pub fn task_id<F>(mut self, f: F) -> Self
    where
        F: Fn(&KafkaMessage) -> Option<TaskId> + Send + Sync + 'static,
    {
        self.task_id = Arc::new(f);
        self
    }

    /// The longest a poll waits for a message before answering "empty for
    /// now". Default 1 s.
    pub fn max_wait(mut self, wait: Duration) -> Self {
        self.max_wait = wait;
        self
    }

    /// The client configuration the source runs with.
    pub(crate) fn client_config(&self) -> ClientConfig {
        let mut config = ClientConfig::new();
        config
            .set("bootstrap.servers", &self.brokers)
            .set("group.id", &self.group)
            .set("auto.offset.reset", "earliest");
        for (key, value) in &self.properties {
            config.set(key, value);
        }
        config
            .set("enable.auto.commit", "false")
            .set("enable.auto.offset.store", "false");
        config
    }

    /// Creates the consumer and subscribes it to the topic. The broker is
    /// contacted later, by polls.
    ///
    /// # Errors
    ///
    /// [`KafkaSourceError::Config`] for a zero `max_wait`;
    /// [`KafkaSourceError::Client`] when the client rejects its
    /// configuration.
    pub fn build(self) -> Result<KafkaSource, KafkaSourceError> {
        if self.max_wait.is_zero() {
            return Err(ConfigError::InvalidDuration {
                reason: "duration must be positive",
            }
            .into());
        }
        let commits = Shared::default();
        let failure = Shared::default();
        let (notify, notices) = Notices::channel();
        let callbacks = Callbacks {
            topic: self.topic.clone(),
            commits: Arc::clone(&commits),
            failure: Arc::clone(&failure),
            notify,
        };
        let consumer: StreamConsumer<Callbacks> = self
            .client_config()
            .create_with_context(callbacks)
            .client()?;
        consumer.subscribe(&[&self.topic]).client()?;
        Ok(KafkaSource {
            consumer,
            topic: self.topic,
            group: self.group,
            commits,
            failure,
            notices: Mutex::new(Some(notices)),
            task_id: self.task_id,
            warned: AtomicBool::new(false),
            wake: WakeHandle::new(),
            max_wait: self.max_wait,
        })
    }
}

impl fmt::Debug for KafkaSourceBuilder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("KafkaSourceBuilder")
            .field("brokers", &self.brokers)
            .field("topic", &self.topic)
            .field("group", &self.group)
            .field("properties", &self.properties.keys().collect::<Vec<_>>())
            .field("max_wait", &self.max_wait)
            .finish_non_exhaustive()
    }
}

/// A log source reading one topic as a member of a consumer group (spec 2.6).
///
/// - An ack commits the partition's offset by the commit boundary: a task not
///   acknowledged yet holds back the commit of every later task of its
///   partition (rule 2.3.9 p. 5).
/// - Messages of a partition come in order; the queue runs them in order
///   only with concurrency 1 and `RetryPolicy::hold_slot` (see the crate
///   documentation).
/// - Partitions are shared between the processes of a group by Kafka; after
///   a rebalance, unfinished deliveries of a revoked partition commit
///   nothing and reach another consumer again.
/// - Pushes and defer are not supported; tasks are written to the topic by
///   producers outside the library.
/// - A poll waits up to `max_wait` for a message; an empty answer wakes the
///   worker at once, so with the default poll strategy the worker keeps
///   long-polling.
pub struct KafkaSource {
    consumer: StreamConsumer<Callbacks>,
    topic: String,
    group: String,
    commits: Shared<Commits>,
    failure: Shared<Option<String>>,
    notices: Mutex<Option<Notices>>,
    task_id: TaskIdFn,
    warned: AtomicBool,
    wake: WakeHandle,
    max_wait: Duration,
}

impl KafkaSource {
    /// Starts building a source for `topic` within consumer group `group`,
    /// with the bootstrap `brokers` (`host:port`, comma-separated).
    pub fn builder(
        brokers: impl Into<String>,
        topic: impl Into<String>,
        group: impl Into<String>,
    ) -> KafkaSourceBuilder {
        KafkaSourceBuilder {
            brokers: brokers.into(),
            topic: topic.into(),
            group: group.into(),
            properties: BTreeMap::new(),
            task_id: Arc::new(key_as_id),
            max_wait: Duration::from_secs(1),
        }
    }

    /// The topic.
    #[must_use]
    pub fn topic(&self) -> &str {
        &self.topic
    }
}

impl fmt::Debug for KafkaSource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("KafkaSource")
            .field("topic", &self.topic)
            .field("group", &self.group)
            .field("max_wait", &self.max_wait)
            .finish_non_exhaustive()
    }
}

impl KafkaSource {
    /// Commits again the boundaries the broker refused, once their pause is
    /// over: without it a partition whose tasks are all done would wait for
    /// a new message to commit (change kafka-commit-retry).
    fn retry_commits(&self) -> Result<(), KafkaSourceError> {
        let due = lock(&self.commits).retry(Instant::now());
        if due.is_empty() {
            return Ok(());
        }
        let mut offsets = TopicPartitionList::new();
        for (partition, next) in due {
            offsets
                .add_partition_offset(&self.topic, partition, Offset::Offset(next))
                .client()?;
        }
        self.consumer.commit(&offsets, CommitMode::Async).client()?;
        Ok(())
    }
}

impl Source for KafkaSource {
    type Message = KafkaMessage;
    type Receipt = KafkaReceipt;
    type Error = KafkaSourceError;

    fn capabilities(&self) -> Capabilities {
        Capabilities::new(AckPointSupport::QueueOnly)
    }

    async fn poll(&self) -> Result<Polled<KafkaMessage, KafkaReceipt>, KafkaSourceError> {
        let failure = lock(&self.failure).take();
        if let Some(failure) = failure {
            return Err(KafkaSourceError::Broker(failure));
        }
        self.retry_commits()?;
        let Ok(received) = tokio::time::timeout(self.max_wait, self.consumer.recv()).await else {
            self.wake.wake();
            return Ok(Polled::Empty);
        };
        let received = received.client()?;
        let headers = received
            .headers()
            .map(|headers| {
                headers
                    .iter()
                    .map(|h| (h.key.to_owned(), h.value.map(<[u8]>::to_vec)))
                    .collect()
            })
            .unwrap_or_default();
        let mut message = KafkaMessage {
            id: TaskId::generate(),
            key: received.key().map(<[u8]>::to_vec),
            payload: received.payload().map(<[u8]>::to_vec),
            headers,
            partition: received.partition(),
            offset: received.offset(),
        };
        drop(received);
        match (self.task_id)(&message) {
            Some(id) => message.id = id,
            None => {
                if !self.warned.swap(true, Ordering::Relaxed) {
                    warn!(
                        event = "source",
                        action = "key_missing",
                        "message without key, task id generated: topic={}",
                        self.topic
                    );
                }
            }
        }
        let receipt = lock(&self.commits).delivered(message.partition, message.offset);
        Ok(Polled::Task { message, receipt })
    }

    async fn ack(&self, receipt: KafkaReceipt) -> Result<(), KafkaSourceError> {
        let next = lock(&self.commits).acked(receipt);
        let Some(next) = next else {
            return Ok(());
        };
        let mut offsets = TopicPartitionList::new();
        offsets
            .add_partition_offset(&self.topic, receipt.partition, Offset::Offset(next))
            .client()?;
        self.consumer.commit(&offsets, CommitMode::Async).client()?;
        Ok(())
    }

    fn subscribe(&self) -> Option<WakeSignal> {
        Some(self.wake.subscribe())
    }

    fn notices(&self) -> Option<Notices> {
        lock(&self.notices).take()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Change driver-errors-wrapped: the client's error is the cause, with
    /// its own message; the type is reached by downcasting.
    #[test]
    fn client_error_keeps_the_client_error_as_its_cause() {
        use std::error::Error as _;

        let original = KafkaError::Canceled;
        let error = Err::<(), _>(KafkaError::Canceled).client().unwrap_err();
        assert_eq!(error.to_string(), original.to_string());
        let cause = error.source().and_then(|e| e.downcast_ref::<KafkaError>());
        assert!(matches!(cause, Some(KafkaError::Canceled)), "{cause:?}");
    }

    /// Change criterion 1: auto commit stays off whatever the consumer sets.
    #[test]
    fn auto_commit_is_forced_off() {
        let builder = KafkaSource::builder("localhost:9092", "jobs", "workers")
            .property("enable.auto.commit", "true")
            .property("enable.auto.offset.store", "true")
            .property("auto.offset.reset", "latest")
            .property("client.id", "worker-1");
        let config = builder.client_config();
        assert_eq!(config.get("enable.auto.commit"), Some("false"));
        assert_eq!(config.get("enable.auto.offset.store"), Some("false"));
        assert_eq!(config.get("auto.offset.reset"), Some("latest"));
        assert_eq!(config.get("client.id"), Some("worker-1"));
        assert_eq!(config.get("group.id"), Some("workers"));
    }

    fn callbacks() -> (Callbacks, Notices) {
        let (notify, notices) = Notices::channel();
        let callbacks = Callbacks {
            topic: "jobs".to_owned(),
            commits: Shared::default(),
            failure: Shared::default(),
            notify,
        };
        (callbacks, notices)
    }

    fn offsets(partition: i32, next: i64) -> TopicPartitionList {
        let mut list = TopicPartitionList::new();
        list.add_partition_offset("jobs", partition, Offset::Offset(next))
            .unwrap();
        list
    }

    /// Change criteria 1 and 2: a failed commit is a source error, and the
    /// boundary goes out again on the next ack.
    #[tokio::test]
    async fn failed_commit_is_reported_and_retried() {
        let (callbacks, mut notices) = callbacks();
        let (first, second) = {
            let mut commits = lock(&callbacks.commits);
            (commits.delivered(0, 1), commits.delivered(0, 2))
        };
        assert_eq!(lock(&callbacks.commits).acked(first), Some(2));

        let failed = KafkaError::ConsumerCommit(RDKafkaErrorCode::RequestTimedOut);
        callbacks.commit_callback(Err(failed), &offsets(0, 2));
        let notice = notices.recv().await.unwrap();
        assert!(
            matches!(&notice, Notice::SourceError(text) if text.contains("offset commit failed")),
            "{notice:?}"
        );
        assert_eq!(lock(&callbacks.commits).acked(second), Some(3));
    }

    /// Change criterion 3: a successful commit changes nothing.
    #[tokio::test]
    async fn successful_commit_is_silent() {
        let (callbacks, mut notices) = callbacks();
        callbacks.commit_callback(Ok(()), &offsets(0, 2));
        drop(callbacks);
        assert!(notices.recv().await.is_none(), "no notice");
    }

    #[test]
    fn default_id_is_the_key() {
        let keyed = KafkaMessage::new(TaskId::new("gen"), Some(b"job-1".to_vec()), None);
        assert_eq!(key_as_id(&keyed), Some(TaskId::new("job-1")));
        let keyless = KafkaMessage::new(TaskId::new("gen"), None, None);
        assert_eq!(key_as_id(&keyless), None);
    }

    #[test]
    fn zero_wait_is_rejected() {
        let built = KafkaSource::builder("localhost:9092", "jobs", "workers")
            .max_wait(Duration::ZERO)
            .build();
        assert!(matches!(built, Err(KafkaSourceError::Config(_))));
    }

    #[tokio::test]
    async fn source_is_a_log_source_without_push_or_defer() {
        let source = KafkaSource::builder("localhost:9092", "jobs", "workers")
            .build()
            .unwrap();
        let caps = source.capabilities();
        assert_eq!(caps.ack_point_support(), AckPointSupport::QueueOnly);
        assert!(!caps.supports_defer());
        assert_eq!(source.topic(), "jobs");
    }
}
