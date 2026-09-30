//! Errors the backfill markers, their discharge and the orphan sweep can
//! produce. The module and type keep the `intake` name until F (#625)
//! deletes what is left of them (issue #622 plan, Q6).
//!
//! Same hand-rolled-enum convention as [`crate::staging::error::StagingError`]
//! and [`crate::error::Error`], with `From` impls so the call sites can use
//! `?` against the crates they compose.

use std::fmt;

use crate::error_code::{self, ErrorCode};
use crate::staging::StagingError;

/// Failure modes of the backfill markers and their discharge.
#[derive(Debug)]
pub enum IntakeError {
    /// The staging ring's blind append, or the transaction it runs in,
    /// failed. Composes [`StagingError`] via `From` rather than
    /// duplicating its variants.
    Staging(StagingError),
    /// A raw `tokio_postgres` error that isn't already wrapped by
    /// [`IntakeError::Staging`].
    Db(tokio_postgres::Error),
    /// A `src_table` string wasn't the `"schema.table"` shape backfill
    /// enumeration needs to split and quote.
    InvalidTableName(String),
    /// [`super::markers::qualify`] was asked to join a schema or table
    /// name that itself contains a literal `.` — legal as a quoted Postgres
    /// identifier, but this crate's `"schema.table"` joined-string
    /// representation has no way to tell that dot apart from the separator.
    /// Rejected loudly at construction rather than silently mis-split later
    /// by [`super::markers::split_qualified`].
    DottedIdentifierComponent { component: String },
    /// A table a marker's discharge enumerates has no identity key to stage
    /// its rows under: no primary key, and not an aggregate target with
    /// grouping columns (issue #308).
    NoIdentityKey { table: String },
    /// A `pg_snapshot` value read back from Postgres (the current snapshot a
    /// backfill marker's fence is checked against) wasn't in the
    /// `"xmin:xmax:xip..."` text form this decodes.
    InvalidSnapshot(String),
    /// A `defs::catalog` lookup or build failed. Composed via `From` rather
    /// than flattened, matching
    /// [`IntakeError::Staging`]'s convention. `Box`ed: `CatalogError`
    /// itself carries a `Backfill(IntakeError)` variant, so an unboxed
    /// cycle between the two enums would make both infinite-sized.
    Catalog(Box<crate::defs::catalog::CatalogError>),
    /// Issue #330: reporting the target rows a resume's discharge deleted
    /// (`super::resume_orphans`) through the target-mutation seam failed.
    /// `Box`ed for the same reason as [`IntakeError::Catalog`]:
    /// `ApplyError` carries an `Intake(IntakeError)` variant.
    Propagation(Box<crate::staging::ApplyError>),
    /// Issue #518: a discharge's sweep (`super::resume_orphans`) found a
    /// swept target inconsistent with its definition in the catalog: an
    /// aggregate target whose key lacks one of its grouping columns (its
    /// primary key altered by hand, say), or a `GROUP BY` relationship its
    /// source no longer declares as to-one. Returned rather than panicked, so
    /// the marker fails and backs off (issue #407) with this on status
    /// instead of taking down the maintenance loop.
    UnsweepableTarget { target: String, reason: String },
}

impl IntakeError {
    /// This error's stable, coarse [`ErrorCode`] category (`docs/decisions/0008-public-api-design.md`,
    /// decision 3). Delegates to [`StagingError::code`] for
    /// [`IntakeError::Staging`] and [`error_code::classify_pg_error`] for a
    /// raw Postgres error; every other variant is a data-integrity condition
    /// this crate can't attribute to a specific caller mistake, so it reports
    /// [`ErrorCode::Internal`].
    pub fn code(&self) -> ErrorCode {
        match self {
            IntakeError::Staging(err) => err.code(),
            IntakeError::Db(err) => error_code::classify_pg_error(err),
            IntakeError::Catalog(err) => err.code(),
            IntakeError::Propagation(err) => err.code(),
            IntakeError::InvalidTableName(_)
            | IntakeError::DottedIdentifierComponent { .. }
            | IntakeError::NoIdentityKey { .. }
            | IntakeError::InvalidSnapshot(_)
            | IntakeError::UnsweepableTarget { .. } => ErrorCode::Internal,
        }
    }
}

impl fmt::Display for IntakeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            IntakeError::Staging(err) => write!(f, "{err}"),
            IntakeError::Catalog(err) => write!(f, "{err}"),
            IntakeError::Propagation(err) => write!(f, "{err}"),
            IntakeError::Db(err) => {
                write!(f, "backfill database error: ")?;
                crate::error::write_pg_error(f, err)
            }
            IntakeError::InvalidTableName(name) => {
                write!(f, "expected a \"schema.table\" name, got {name:?}")
            }
            IntakeError::DottedIdentifierComponent { component } => write!(
                f,
                "identifier component {component:?} contains a literal '.', which this crate's \
                 \"schema.table\" joined-string representation cannot round-trip; refusing to \
                 build an ambiguous src_table"
            ),
            IntakeError::NoIdentityKey { table } => write!(
                f,
                "can't enumerate {table}: it has no primary key (or, for an aggregate target, \
                 grouping key) to stage its rows under"
            ),
            IntakeError::InvalidSnapshot(text) => {
                write!(f, "could not parse pg_snapshot text {text:?}")
            }
            IntakeError::UnsweepableTarget { target, reason } => {
                write!(f, "can't sweep {target}'s unbacked rows: {reason}")
            }
        }
    }
}

impl std::error::Error for IntakeError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            IntakeError::Staging(err) => Some(err),
            IntakeError::Db(err) => Some(err),
            IntakeError::Catalog(err) => Some(err),
            // The `ApplyError` itself, not its `Box`: a `Box<ApplyError>` as
            // the source would hide the `ApplyError` from `downcast_ref`, and
            // from `quarantine::classify`'s innermost-error rule with it.
            IntakeError::Propagation(err) => Some(err.as_ref()),
            IntakeError::InvalidTableName(_)
            | IntakeError::DottedIdentifierComponent { .. }
            | IntakeError::NoIdentityKey { .. }
            | IntakeError::InvalidSnapshot(_)
            | IntakeError::UnsweepableTarget { .. } => None,
        }
    }
}

impl From<StagingError> for IntakeError {
    fn from(err: StagingError) -> Self {
        IntakeError::Staging(err)
    }
}

impl From<crate::defs::catalog::CatalogError> for IntakeError {
    fn from(err: crate::defs::catalog::CatalogError) -> Self {
        IntakeError::Catalog(Box::new(err))
    }
}

impl From<crate::staging::ApplyError> for IntakeError {
    fn from(err: crate::staging::ApplyError) -> Self {
        IntakeError::Propagation(Box::new(err))
    }
}

impl From<tokio_postgres::Error> for IntakeError {
    fn from(err: tokio_postgres::Error) -> Self {
        IntakeError::Db(err)
    }
}
