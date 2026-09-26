//! The plain-data flattening the Elixir and Ruby bindings share
//! (`docs/decisions/0010-embeddable-clients.md`, decision 4).
//!
//! Only plain data crosses an FFI boundary: integers, strings, and maps and
//! lists of them. Where a `trellis` type carries structure both hosts can't
//! represent without inventing semantics for it, it is flattened here, in
//! Rust, once, so each binding crate only turns these plain values into host
//! terms. Errors cross as a `(code, message)` pair and nothing else.
//!
//! This crate holds no Rustler or Magnus types and exports no C ABI (ADR-0010
//! decision 1). A binding depends on it and on `trellis` directly.
//!
//! | `trellis` value | Crosses as | Here |
//! |---|---|---|
//! | any error's `code()` + `Display` | `(code, message)` | [`PlainError`] |
//! | [`trellis::Definition`] | summary fields + column name → type name | [`PlainDefinition`] |
//! | [`trellis::DefinitionSummary`] | summary fields | [`PlainDefinitionSummary`] |
//! | [`trellis::DefinitionStatus`] | status word + backfill failure | [`PlainDefinitionStatus`] |
//! | `SystemTime` | epoch microseconds | [`epoch_micros`] / [`system_time_from_epoch_micros`] |
//! | [`trellis::TransformStatus`] / [`trellis::QuarantineState`] | their `as_str()` word | [`transform_status`] / [`quarantine_state`] |
//! | [`trellis::QuarantineTarget`] | `transform` / `transform.column` | [`quarantine_address`] |
//! | `sample_quarantined`'s `(src_table, key)` cursor | an opaque string | [`next_cursor`] / [`decode_cursor`] |

mod cursor;
mod definition;
mod error;
mod quarantine;
mod status;
mod time;

pub use cursor::{decode_cursor, encode_cursor, next_cursor};
pub use definition::{
    PlainBackfillFailure, PlainDefinition, PlainDefinitionStatus, PlainDefinitionSummary,
};
pub use error::{CodedError, ERROR_CODES, PlainError};
pub use quarantine::quarantine_address;
pub use status::{
    quarantine_state, quarantine_state_names, transform_status, transform_status_names,
};
pub use time::{epoch_micros, system_time_from_epoch_micros};
