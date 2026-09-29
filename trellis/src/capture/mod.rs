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
//!   (C3). The staging worker's reconcile pass that calls it is C5.
//!
//! Until C5 lands, nothing at runtime calls this module. The tests drive it
//! by hand.
//!
//! # What the images must equal
//!
//! Until C8 deletes it, intake is the reference: every key, op, image and
//! `group_key` a trigger writes must be byte-identical to what intake stages
//! for the same write (`intake::tuple_to_json`, `intake::extract_key`,
//! `intake::touched_group_key`), restricted to the columns the trigger
//! images. `tests/capture_parity.rs` checks that against intake, and against
//! a checked-in golden fixture that outlives intake.
//!
//! Three differences are by design:
//!
//! - **Narrow images.** A trigger images only the primary key and the
//!   columns some reader needs ([`columns`]). Intake images every column.
//! - **No omitted TOAST columns.** Intake leaves out an unchanged TOASTed
//!   column of an update's new tuple. A transition table carries the whole
//!   row, so a trigger image always has every column it names.
//! - **A key move is a delete plus an insert.** A statement trigger sees an
//!   update's old and new rows as two sets, which [`sql`] pairs by primary
//!   key. A row whose key changed has no partner, so it becomes a delete of
//!   the old key and an insert of the new one. Intake stages one `update`
//!   keyed by the new key, whose old image carries the old key.
//!
//! Two more are in the metadata, not the images:
//!
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

// C5 (reconcile) is the caller. Until it lands, only the tests reach this
// module, and a build without `internals` would flag every item as dead.
#![cfg_attr(not(feature = "internals"), allow(dead_code))]

pub mod columns;
pub mod install;
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
    /// column of the table. A renamed or dropped read column lands here until
    /// C6 handles schema changes.
    MissingColumn { table: String, column: String },
    /// A table name that isn't a `schema.table` identity with exactly one
    /// separating `.` (see `intake::publication::qualify`).
    InvalidTableName(String),
    /// Reading the catalog failed.
    Catalog(crate::defs::catalog::CatalogError),
    /// Parking the backfill marker an install or widen commits with failed
    /// ([`install`]).
    Marker(crate::intake::IntakeError),
    /// A catalog read, or the capture DDL, failed. A lock timeout on a user
    /// table lands here too ([`crate::locks::is_lock_not_available`] tells it
    /// apart), once [`install`]'s retries stop at their deadline.
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
            CaptureError::Db(e) => write!(f, "capture DDL or catalog read failed: {e}"),
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
