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
//! | [`trellis::Applied`] | its kind + that kind's plain fields, or `unknown` | [`PlainApplied`] |
//! | [`trellis::RelationshipDefinition`] | fields + cardinality word + warning messages | [`PlainRelationship`] |
//! | [`trellis::RelationshipSummary`] | fields + cardinality word + creation time | [`PlainRelationshipSummary`] |
//! | [`trellis::QuarantineEntry`] | address + state word + pause time/error | [`PlainQuarantineEntry`] |
//! | [`trellis::PoisonEntry`] | fields + poison time | [`PlainPoisonEntry`] |
//! | a page of [`trellis::PoisonSample`] | the rows + the next page's cursor | [`PlainSamplePage`] |
//! | `watermark_token`'s `PgLsn` | an opaque string | [`encode_watermark`] / [`decode_watermark`] |
//!
//! One piece of shared logic isn't flattening: [`require_transform_statement`]
//! is the check a binding's `define` makes before calling `apply`, so that a
//! `DROP` or `PAUSE` handed to `define` is refused rather than carried out. It
//! asks [`trellis::statement_kind`], so the form is `apply`'s own parser's
//! answer, and only the refusal's wording lives here.

mod applied;
mod cursor;
mod definition;
mod error;
mod quarantine;
mod relationship;
mod statement;
mod status;
mod time;
mod watermark;

pub use applied::PlainApplied;

pub use cursor::{decode_cursor, encode_cursor, next_cursor};
pub use definition::{
    PlainBackfillFailure, PlainDefinition, PlainDefinitionStatus, PlainDefinitionSummary,
};
pub use error::{CodedError, ERROR_CODES, PlainError};
pub use quarantine::{
    PlainPoisonEntry, PlainPoisonSample, PlainQuarantineEntry, PlainSamplePage, quarantine_address,
};
pub use relationship::{
    PlainRelationship, PlainRelationshipSummary, relationship_cardinality_names,
};
pub use statement::require_transform_statement;
pub use status::{
    quarantine_state, quarantine_state_names, transform_status, transform_status_names,
};
pub use time::{epoch_micros, system_time_from_epoch_micros};
pub use watermark::{decode_watermark, encode_watermark};
