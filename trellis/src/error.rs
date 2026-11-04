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
    /// instance (a mismatched identity marker), or is a pre-existing,
    /// non-Trellis schema with no marker at all. See `crate::identity`.
    IncompatibleInstance(String),
    /// Another Trellis version is live on the instance, so `migrate` changes
    /// nothing (ADR-0023, version handover).
    OtherVersionLive(crate::handover::OtherVersionLive),
    /// This binary's version is below the catalog floor, the oldest Trellis
    /// version that can read the catalog (ADR-0024), so it refuses to attach.
    /// The remedy is a version at or above `floor`.
    BelowCatalogFloor {
        /// The floor the catalog records.
        floor: String,
        /// This binary's version.
        this_version: String,
    },
    /// The catalog's migration history doesn't fit this binary's migrations:
    /// a shared version with another name or checksum, or a gap below this
    /// binary's newest migration.
    MigrationHistory(String),
    /// `migrate` couldn't restore a definition a schema dump queued
    /// (`trellis::dump`): the text resolves to other tables than the dump
    /// recorded, or its define refused. The definitions before it in the
    /// queue are restored, and it and the rest stay queued for the next
    /// `migrate`.
    RestoreDefinition {
        /// The transform's qualified target, as the dump recorded it.
        target: String,
        /// The refusal's category.
        code: ErrorCode,
        /// Why it was refused, and the remedy.
        message: String,
    },
    /// A schema dump's `pg_dump` output still names this instance's capture
    /// functions after the filter (`trellis::dump::filter_pg_dump`), so it
    /// would fail to load: a form of the output the filter doesn't know.
    SchemaDump(String),
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
            Error::OtherVersionLive(_) => ErrorCode::VersionHandover,
            Error::BelowCatalogFloor { .. } => ErrorCode::VersionFloor,
            // Like a refinery refusal: a deployment problem.
            Error::MigrationHistory(_) => ErrorCode::Internal,
            Error::RestoreDefinition { code, .. } => *code,
            Error::SchemaDump(_) => ErrorCode::Internal,
        }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Config(msg) => write!(f, "invalid trellis configuration: {msg}"),
            Error::BuildPool(err) => write!(f, "failed to build connection pool: {err}"),
            Error::Pool(err) => {
                write!(f, "failed to acquire a connection: ")?;
                write_pool_error(f, err)
            }
            Error::Connect(err) => {
                write!(f, "postgres connection error: ")?;
                write_pg_error(f, err)
            }
            Error::Migrate(err) => write!(f, "failed to apply migrations: {err}"),
            Error::IncompatibleInstance(msg) => write!(f, "refusing to attach: {msg}"),
            Error::OtherVersionLive(seen) => write!(f, "refusing to migrate: {seen}"),
            Error::BelowCatalogFloor {
                floor,
                this_version,
            } => write!(
                f,
                "refusing to attach: the catalog floor is {floor}, the oldest Trellis version \
                 that can read this catalog, and this binary is version {this_version}; run \
                 Trellis {floor} or later"
            ),
            Error::MigrationHistory(msg) => write!(f, "refusing to attach: {msg}"),
            Error::RestoreDefinition {
                target, message, ..
            } => write!(
                f,
                "can't restore transform {target} from the schema dump: {message}"
            ),
            Error::SchemaDump(msg) => write!(f, "can't write the schema dump: {msg}"),
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
            Error::OtherVersionLive(seen) => Some(seen),
            Error::BelowCatalogFloor { .. }
            | Error::MigrationHistory(_)
            | Error::RestoreDefinition { .. }
            | Error::SchemaDump(_) => None,
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
        // A lock that wasn't granted (a wait that ran out `lock_timeout`, or
        // a `NOWAIT` refused) is an ordinary condition, not a fault, and the
        // server's own text, which already says which of the two it was,
        // doesn't say what to do about it.
        if *db_err.code() == tokio_postgres::error::SqlState::LOCK_NOT_AVAILABLE {
            write!(
                f,
                " (another transaction held a lock this statement needed; retry once that \
                 transaction ends)"
            )?;
        }
    }
    Ok(())
}

/// Writes a failed pool checkout's message, with the server's own text
/// when the server refused the connection or the new connection's session
/// setup.
///
/// `deadpool`'s `PoolError` displays the `tokio_postgres::Error` it carries
/// (`Backend`, or a `post_create` hook's `HookError::Backend`) with `{}`,
/// which reads only "db error": a connection the server refused (no such
/// database or role, a rejected password) lost its `FATAL` text. The
/// wrapper error types in this crate that hold a pool checkout's [`Error`]
/// print it through [`Error`]'s `Display`, so this is the one place that
/// text is restored.
fn write_pool_error(f: &mut fmt::Formatter<'_>, err: &deadpool_postgres::PoolError) -> fmt::Result {
    match err {
        deadpool_postgres::PoolError::Backend(err) => {
            write!(f, "Error occurred while creating a new object: ")?;
            write_pg_error(f, err)
        }
        deadpool_postgres::PoolError::PostCreateHook(deadpool_postgres::HookError::Backend(
            err,
        )) => {
            write!(f, "setting up a new connection's session failed: ")?;
            write_pg_error(f, err)
        }
        other => write!(f, "{other}"),
    }
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
