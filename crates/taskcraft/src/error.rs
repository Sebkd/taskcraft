//! Error types of the task model.

use crate::state::TaskState;

/// A configuration mistake caught while building queues or registries.
///
/// The messages match section 2.10 of the specification.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum ConfigError {
    /// The stable name is already registered for another metadata type.
    #[error("duplicate metadata name: {name}")]
    DuplicateMetadataName {
        /// The repeated name.
        name: String,
    },
    /// The metadata type is already registered under another name.
    #[error("duplicate metadata name: type {type_name} is already registered")]
    DuplicateMetadataType {
        /// The repeated type.
        type_name: &'static str,
    },
    /// The name is reserved by the library.
    #[error("reserved metadata name: {name}")]
    ReservedMetadataName {
        /// The reserved name.
        name: String,
    },
}

/// A failure to encode or parse task metadata.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum MetadataError {
    /// The task carries a metadata type the registry does not know, so it
    /// cannot be stored outside the process.
    #[error("unregistered metadata type: {type_name}")]
    UnregisteredType {
        /// The unregistered type.
        type_name: &'static str,
    },
    /// A typed value could not be encoded.
    #[error("metadata {name} could not be encoded: {reason}")]
    Encode {
        /// The stable name of the value.
        name: String,
        /// Why encoding failed.
        reason: String,
    },
    /// A stored value could not be parsed into its registered type. No
    /// default is substituted.
    #[error("metadata {name} could not be parsed as {type_name}: {reason}")]
    Unparsable {
        /// The stable name of the value.
        name: String,
        /// The registered type.
        type_name: &'static str,
        /// Why parsing failed.
        reason: String,
    },
}

/// A task state change that the lifecycle table (spec 2.4.1) does not allow.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("transition {from} -> {to} is not allowed")]
pub struct InvalidTransition {
    /// The state the task is in; it is left unchanged.
    pub from: TaskState,
    /// The requested state.
    pub to: TaskState,
}
