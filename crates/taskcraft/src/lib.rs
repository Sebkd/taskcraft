//! # taskcraft
//!
//! Background task queue for Rust on the tokio runtime.
//!
//! **This release only reserves the crate name.** It contains no API yet; the
//! first working version will be `0.1.0`.
//!
//! The target behaviour is specified in
//! [`openspec/specs/taskcraft/taskcraft.md`](https://github.com/Sebkd/taskcraft/blob/master/openspec/specs/taskcraft/taskcraft.md):
//! a source poll that tells "empty for now" apart from "closed", handler
//! outcomes classified as data rather than by error type, retries that release
//! their concurrency slot, named resource pools, status and cancellation by
//! task id, and a shutdown that wakes every sleeper at once.
