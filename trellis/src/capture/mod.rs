//! Trigger capture (ADR-0002, "Capture by statement triggers"; epic #556
//! milestone C, #622).
//!
//! A captured table carries four `AFTER … FOR EACH STATEMENT` triggers:
//! insert, update and delete with transition tables, and truncate. Each one
//! appends the statement's changes to the active ring segment inside the
//! writer's own transaction. So a ring row's `row_txid` (the ring's `DEFAULT
//! pg_current_xact_id()`) is the source commit's `xid8`: invariant I0,
//! exact identity, which D (#623) relies on from its first ledger write.
//!
//! This module is built in parts:
//!
//! - [`columns`] decides which columns a table's triggers image, from the
//!   catalog (C2).
//! - [`sql`] generates the functions and triggers for one table (C2).
//! - [`install`] installs, widens, narrows and uninstalls them under
//!   `locks::DdlRetry`, and reads back what is installed from the catalog
//!   (C3).
//! - [`reconcile`] is the staging worker's pass that calls it for every
//!   table, and decides which waiting definitions a discharge may dispatch
//!   (C5).
//!
//! # What the images must equal
//!
//! Every key, op, image and `group_key` a trigger writes must equal the
//! golden fixture `tests/capture_parity.rs` checks. Of note:
//!
//! - **Narrow images.** A trigger images only the primary key and the
//!   columns some reader needs ([`columns`]).
//! - **Whole images.** A transition table carries the whole row, so an image
//!   always has every column it names, TOASTed or not.
//! - **A key move is a delete plus an insert.** A statement trigger sees an
//!   update's old and new rows as two sets, which [`sql`] pairs by primary
//!   key. A row whose key changed has no partner, so it becomes a delete of
//!   the old key and an insert of the new one.
//! - **`lsn` and `origin_lsn`** are `pg_current_wal_insert_lsn()` when the
//!   trigger runs, not the commit LSN. Per-key order stays `(lsn,
//!   change_id)`. A second writer of the same key runs its trigger only
//!   after the first commits, because it waits on the row lock (#565 E4).
//! - **`src_changed`** is `clock_timestamp()` when the statement's trigger
//!   runs, which is change time, not commit time (#622 plan Q8).
//!
//! # Known limitation until D (#623)
//!
//! A nested write to the same key reorders its images. Suppose an
//! application `AFTER ROW` trigger, or a self-referencing cascade, rewrites a
//! row its own statement wrote. The nested statement's capture runs first,
//! so the ring holds the newer image at the lower `lsn`. A GROUP BY target
//! then subtracts and adds the wrong images. A 1-1 target is unaffected.
//! Under OLD+NEW images no image shape fixes this; D's NEW-only apply with a
//! re-read image does.

// A few items (the parity helpers, the spec accessors) only the tests reach,
// and a build without `internals` would flag them as dead.
#![cfg_attr(not(feature = "internals"), allow(dead_code))]

pub mod columns;
pub mod install;
pub mod reconcile;
pub mod sql;

use std::fmt;

/// Why a table's capture spec couldn't be built.
#[derive(Debug)]
pub enum CaptureError {
    /// The table has no primary key. Trigger capture keys every ring row by
    /// the primary key, so a table without one can't be captured.
    NoPrimaryKey { table: String },
    /// The table doesn't exist.
    UnknownTable { table: String },
    /// A column some reader needs, or a key column the spec names, isn't a
    /// column of the table. A renamed or dropped column lands here while a
    /// definition that isn't paused still reads it, until the reconcile
    /// pass, or the drain on the capture function's marker, pauses that
    /// definition (C6, `staging::schema_change`).
    MissingColumn { table: String, column: String },
    /// A table name that isn't a `schema.table` identity with exactly one
    /// separating `.` (see `intake::markers::qualify`).
    InvalidTableName(String),
    /// Reading the catalog failed.
    Catalog(crate::defs::catalog::CatalogError),
    /// Parking the backfill marker an install or widen commits with failed
    /// ([`install`]).
    Marker(crate::intake::IntakeError),
    /// A catalog read, or the capture DDL, failed. A lock timeout on the
    /// table doesn't land here: once [`install`]'s retries stop at their
    /// deadline, the operation returns [`install::Progress::Waiting`].
    Db(tokio_postgres::Error),
}

impl fmt::Display for CaptureError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CaptureError::NoPrimaryKey { table } => {
                write!(
                    f,
                    "table {table} has no primary key, so it can't be captured"
                )
            }
            CaptureError::UnknownTable { table } => write!(f, "table {table} does not exist"),
            CaptureError::MissingColumn { table, column } => write!(
                f,
                "capture of {table} needs column {column:?}, which the table doesn't have"
            ),
            CaptureError::InvalidTableName(name) => {
                write!(f, "{name:?} is not a schema.table identity")
            }
            CaptureError::Catalog(e) => write!(f, "reading the catalog for capture: {e}"),
            CaptureError::Marker(e) => write!(f, "parking the capture's backfill marker: {e}"),
            CaptureError::Db(e) => {
                write!(f, "capture DDL or catalog read failed: ")?;
                crate::error::write_pg_error(f, e)
            }
        }
    }
}

impl std::error::Error for CaptureError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            CaptureError::Catalog(e) => Some(e),
            CaptureError::Marker(e) => Some(e),
            CaptureError::Db(e) => Some(e),
            _ => None,
        }
    }
}

impl From<crate::defs::catalog::CatalogError> for CaptureError {
    fn from(e: crate::defs::catalog::CatalogError) -> Self {
        CaptureError::Catalog(e)
    }
}

impl From<crate::intake::IntakeError> for CaptureError {
    fn from(e: crate::intake::IntakeError) -> Self {
        CaptureError::Marker(e)
    }
}

impl From<tokio_postgres::Error> for CaptureError {
    fn from(e: tokio_postgres::Error) -> Self {
        CaptureError::Db(e)
    }
}
