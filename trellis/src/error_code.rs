//! A small, stable set of coarse error categories (`docs/decisions/0008-public-api-design.md`,
//! decision 3) that every error type in this crate can report via a `code()`
//! method, independent of whichever internal Rust enum variant actually
//! produced the failure.
//!
//! A future FFI boundary (issue #87) needs something with a stable shape to
//! match on; a host language reimplementing a `match` over every internal
//! enum this crate has — and keeps adding — isn't that. [`ErrorCode`] is the
//! stable shape: a fixed, small set of categories a host language can match
//! on once and never touch again, while the wrapped Rust type underneath is
//! free to keep growing new variants that fold into one of these.
//!
//! This module intentionally depends on nothing else in the crate (only
//! `tokio_postgres`, for [`classify_pg_error`]'s `SqlState` inspection) so
//! every error type, at any layer, can report a code without creating a
//! dependency-direction problem — `error.rs`, `client.rs`, `defs::catalog`,
//! and everything each of those wraps, all sit "above" this module.

use std::fmt;

/// A coarse category for any error this crate can produce. Deliberately
/// small — on the order of the variants below, not one per internal error
/// enum — and expected to grow rarely: adding a new internal error variant
/// should fold into one of these existing categories, not add a new one.
///
/// `#[non_exhaustive]`: unlike [`crate::defs::TransformStatus`] or
/// [`crate::defs::RelationshipCardinality`], which round-trip through a
/// Postgres `check` constraint and so are exhaustively matched against a
/// closed set of persisted strings, this type's whole purpose is to be
/// matched on from outside this crate (eventually outside Rust entirely) as
/// the taxonomy evolves — so a match on it must already tolerate an
/// unrecognized category rather than assume the set is closed forever.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum ErrorCode {
    /// Input text failed to parse — a `TRANSFORM`/`RELATIONSHIP` statement,
    /// or previously-persisted text that's expected to re-parse.
    Parse,
    /// Structurally valid input was semantically rejected: type mismatches,
    /// unresolved references, cycles, or a construct this crate doesn't
    /// support.
    Validation,
    /// Reaching or using Postgres failed at the connection/transport/pool
    /// layer, or applying migrations failed.
    Connectivity,
    /// The operation collides with existing state: a name already declared,
    /// a uniqueness violation, a singleton lock already held, an instance
    /// identity mismatch.
    Conflict,
    /// Something the caller (or a persisted record) named — a table, a
    /// slot, a row — does not exist.
    NotFound,
    /// Anything else: engine-internal failures, "should not happen"
    /// invariants, IO failures, and Postgres errors with no more specific
    /// category. The catch-all so a new internal variant always has
    /// somewhere to land without forcing a new category onto this enum.
    Internal,
}

impl ErrorCode {
    /// A stable, lowercase `snake_case` name for this category — the form a
    /// host language across a future FFI boundary would key off of.
    pub fn as_str(self) -> &'static str {
        match self {
            ErrorCode::Parse => "parse",
            ErrorCode::Validation => "validation",
            ErrorCode::Connectivity => "connectivity",
            ErrorCode::Conflict => "conflict",
            ErrorCode::NotFound => "not_found",
            ErrorCode::Internal => "internal",
        }
    }
}

impl fmt::Display for ErrorCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Classifies a raw `tokio_postgres::Error` the way every error variant in
/// this crate that wraps one directly (typically named `Db` or `Connect`)
/// wants: a connection/protocol-level failure — [`tokio_postgres::Error::as_db_error`]
/// is `None`, meaning the call never reached the server to get a SQLSTATE at
/// all — is [`ErrorCode::Connectivity`]; a server-side `DbError` is
/// classified by its `SqlState` where this crate's call sites can actually
/// hit one with an obvious category (`unique_violation`/`foreign_key_violation`
/// -> [`ErrorCode::Conflict`], `undefined_table` -> [`ErrorCode::NotFound`],
/// matching the same SQLSTATEs [`crate::staging::quarantine`] already keys
/// off of for its own classification), and [`ErrorCode::Internal`]
/// otherwise — most other server-side failures at this crate's call sites
/// (a malformed generated statement, an unexpected type error) are engine
/// bugs, not a condition a caller can act on differently.
///
/// The exception to "a `DbError` reached the server, so it isn't
/// connectivity" is the server telling us the connection itself is gone or
/// unavailable (issue #340): SQLSTATE class `08` (connection exception) and
/// `57P01`/`57P02`/`57P03` (admin shutdown, which is what
/// `pg_terminate_backend` sends; crash shutdown; cannot connect now) are
/// [`ErrorCode::Connectivity`]. Otherwise the same fault reads differently
/// depending on whether the server's `FATAL` or the socket close reached the
/// caller first.
pub fn classify_pg_error(err: &tokio_postgres::Error) -> ErrorCode {
    match err.as_db_error() {
        Some(db_err) => classify_sqlstate(db_err.code()),
        None => ErrorCode::Connectivity,
    }
}

/// [`classify_pg_error`]'s mapping for a server-side error's SQLSTATE.
fn classify_sqlstate(code: &tokio_postgres::error::SqlState) -> ErrorCode {
    use tokio_postgres::error::SqlState;

    if code.code().starts_with("08")
        || *code == SqlState::ADMIN_SHUTDOWN
        || *code == SqlState::CRASH_SHUTDOWN
        || *code == SqlState::CANNOT_CONNECT_NOW
    {
        ErrorCode::Connectivity
    } else if *code == SqlState::UNIQUE_VIOLATION || *code == SqlState::FOREIGN_KEY_VIOLATION {
        ErrorCode::Conflict
    } else if *code == SqlState::UNDEFINED_TABLE {
        ErrorCode::NotFound
    } else {
        ErrorCode::Internal
    }
}

#[cfg(test)]
mod tests {
    use tokio_postgres::error::SqlState;

    use super::*;

    /// Issue #340: the server reporting a dead or unavailable connection is
    /// connectivity, whichever connection it happened to (a producer
    /// session's `57P01` must match the walsender's transport-level drop).
    #[test]
    fn connection_loss_sqlstates_classify_as_connectivity() {
        for code in [
            SqlState::ADMIN_SHUTDOWN,
            SqlState::CRASH_SHUTDOWN,
            SqlState::CANNOT_CONNECT_NOW,
            SqlState::CONNECTION_EXCEPTION,
            SqlState::CONNECTION_FAILURE,
            SqlState::CONNECTION_DOES_NOT_EXIST,
            SqlState::SQLCLIENT_UNABLE_TO_ESTABLISH_SQLCONNECTION,
            SqlState::PROTOCOL_VIOLATION,
        ] {
            assert_eq!(
                classify_sqlstate(&code),
                ErrorCode::Connectivity,
                "{}",
                code.code()
            );
        }
    }

    #[test]
    fn other_sqlstates_keep_their_classification() {
        assert_eq!(
            classify_sqlstate(&SqlState::UNIQUE_VIOLATION),
            ErrorCode::Conflict
        );
        assert_eq!(
            classify_sqlstate(&SqlState::FOREIGN_KEY_VIOLATION),
            ErrorCode::Conflict
        );
        assert_eq!(
            classify_sqlstate(&SqlState::UNDEFINED_TABLE),
            ErrorCode::NotFound
        );
        // Same `57` class as admin shutdown, but a cancelled statement, not a
        // lost connection.
        assert_eq!(
            classify_sqlstate(&SqlState::QUERY_CANCELED),
            ErrorCode::Internal
        );
        assert_eq!(
            classify_sqlstate(&SqlState::SYNTAX_ERROR),
            ErrorCode::Internal
        );
    }
}
