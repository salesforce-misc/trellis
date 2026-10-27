//! Trellis's error type for the connectivity and migration layer.
//!
//! Kept as a plain enum implementing [`std::error::Error`] (no `thiserror`/
//! `anyhow`) per the crate's dependency policy: pull in only what's on the
//! approved list for this issue. [`Error::code`] additionally reports a
//! stable [`crate::ErrorCode`] category for this error, alongside the
//! existing `Display`-driven message — see `docs/decisions/0008-public-api-design.md`,
//! decision 3.

use std::fmt;

use crate::error_code::{self, ErrorCode};

/// Errors that can occur while resolving configuration, connecting to
/// Postgres, or running migrations.
#[derive(Debug)]
#[non_exhaustive]
pub enum Error {
    /// The engine's own configuration (DSN, schema name, ...) was invalid.
    Config(String),
    /// Building the connection pool failed (e.g. an unparsable DSN).
    BuildPool(deadpool_postgres::BuildError),
    /// Acquiring a connection from the pool failed.
    Pool(deadpool_postgres::PoolError),
    /// A direct Postgres protocol/connection error.
    Connect(tokio_postgres::Error),
    /// Applying migrations failed.
    Migrate(refinery::Error),
    /// The configured schema already belongs to a different Trellis
    /// instance (a mismatched identity marker), an incompatible one (an
    /// instance format version newer than this build understands), or a
    /// pre-existing, non-Trellis schema with no marker at all. See
    /// `crate::identity`.
    IncompatibleInstance(String),
}

impl Error {
    /// This error's stable, coarse [`ErrorCode`] category — see
    /// `docs/decisions/0008-public-api-design.md`, decision 3. The message itself is still
    /// only available via `Display`/`to_string()`; this is purely the
    /// category alongside it.
    pub fn code(&self) -> ErrorCode {
        match self {
            // Invalid configuration (DSN, schema name, ...) is a rejected
            // input, the same category a definition's own validation
            // failure reports.
            Error::Config(_) => ErrorCode::Validation,
            Error::BuildPool(_) | Error::Pool(_) => ErrorCode::Connectivity,
            Error::Connect(err) => error_code::classify_pg_error(err),
            // A migration failure is an trellis/deployment problem, not a
            // reachability one.
            Error::Migrate(_) => ErrorCode::Internal,
            // A mismatched/incompatible/foreign schema marker is this
            // instance colliding with existing state.
            Error::IncompatibleInstance(_) => ErrorCode::Conflict,
        }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Config(msg) => write!(f, "invalid trellis configuration: {msg}"),
            Error::BuildPool(err) => write!(f, "failed to build connection pool: {err}"),
            Error::Pool(err) => write!(f, "failed to acquire a connection: {err}"),
            Error::Connect(err) => write!(f, "postgres connection error: {err}"),
            Error::Migrate(err) => write!(f, "failed to apply migrations: {err}"),
            Error::IncompatibleInstance(msg) => write!(f, "refusing to attach: {msg}"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::Config(_) => None,
            Error::BuildPool(err) => Some(err),
            Error::Pool(err) => Some(err),
            Error::Connect(err) => Some(err),
            Error::Migrate(err) => Some(err),
            Error::IncompatibleInstance(_) => None,
        }
    }
}

impl From<deadpool_postgres::BuildError> for Error {
    fn from(err: deadpool_postgres::BuildError) -> Self {
        Error::BuildPool(err)
    }
}

impl From<deadpool_postgres::PoolError> for Error {
    fn from(err: deadpool_postgres::PoolError) -> Self {
        Error::Pool(err)
    }
}

impl From<tokio_postgres::Error> for Error {
    fn from(err: tokio_postgres::Error) -> Self {
        Error::Connect(err)
    }
}

impl From<refinery::Error> for Error {
    fn from(err: refinery::Error) -> Self {
        Error::Migrate(err)
    }
}

/// Writes `err`'s message, plus the underlying `DbError` detail if one is
/// attached.
///
/// `tokio_postgres::Error`'s own `Display` only ever prints its bare
/// error-kind text (e.g. `"db error"`) — the actual Postgres message
/// (severity, text, and any `DETAIL`/`HINT`) lives on the `DbError` its
/// `source()` carries, not in its own `Display`. Every `Display` impl in
/// this crate that wraps a `tokio_postgres::Error` needs this, or a caller
/// only ever sees the useless "db error" stand-in instead of what actually
/// went wrong.
pub fn write_pg_error(f: &mut fmt::Formatter<'_>, err: &tokio_postgres::Error) -> fmt::Result {
    write!(f, "{err}")?;
    if let Some(db_err) = err.as_db_error() {
        write!(f, ": {db_err}")?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn config_error_is_validation() {
        assert_eq!(
            Error::Config("bad dsn".to_string()).code(),
            ErrorCode::Validation
        );
    }

    #[test]
    fn incompatible_instance_is_conflict() {
        assert_eq!(
            Error::IncompatibleInstance("schema already claimed".to_string()).code(),
            ErrorCode::Conflict
        );
    }
}
