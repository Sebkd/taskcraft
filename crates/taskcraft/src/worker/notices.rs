//! Notices from the source: cancel requests, lost leases, background errors
//! (rule 2.3.20).

use std::sync::Arc;

use tokio::task::JoinHandle;
use tracing::{info, warn};

use crate::backend::Backend;
use crate::observe::{Event, Observers};
use crate::registry::TaskRegistry;
use crate::source::Notice;
use crate::status::FinishReason;

/// Cancel requests and lost leases from the source (rule 2.3.20 pp. 5, 7),
/// when the source sends notices.
pub(super) fn spawn_listener<S: Backend>(
    source: &S,
    tasks: &Arc<TaskRegistry>,
    queue: &Arc<str>,
    observers: &Observers,
) -> Option<JoinHandle<()>> {
    source.notices().map(|notices| {
        tokio::spawn(listen(
            notices,
            Arc::clone(tasks),
            Arc::clone(queue),
            observers.clone(),
        ))
    })
}

/// Applies the source's notices to the registry: a cancel request or a lost
/// lease sets the task's cancel flag with its reason.
async fn listen(
    mut notices: crate::source::Notices,
    tasks: Arc<TaskRegistry>,
    queue: Arc<str>,
    observers: Observers,
) {
    while let Some(notice) = notices.recv().await {
        match notice {
            Notice::CancelRequested(id) => {
                if tasks.cancel(&id).is_some() {
                    info!(
                        event = "task",
                        action = "cancel_requested",
                        "task cancel requested: queue={}, task_id={}",
                        queue,
                        id
                    );
                }
            }
            Notice::LeaseLost(id) => {
                if tasks.cancel_with(&id, FinishReason::LeaseLost).is_some() {
                    warn!(
                        event = "lease",
                        action = "lost",
                        "lease lost: queue={}, task_id={}",
                        queue,
                        id
                    );
                    observers.emit(&Event::LeaseLost {
                        queue: &queue,
                        task_id: &id,
                    });
                }
            }
            Notice::SourceError(_) => {
                observers.emit(&Event::SourceFailed { queue: &queue });
            }
            Notice::TakenOver {
                task_id,
                previous_owner,
            } => {
                warn!(
                    event = "lease",
                    action = "taken_over",
                    "task taken over after lease expiry: queue={}, task_id={}, previous_owner={}",
                    queue,
                    task_id,
                    previous_owner
                );
                observers.emit(&Event::LeaseTakenOver {
                    queue: &queue,
                    task_id: &task_id,
                });
            }
        }
    }
}
