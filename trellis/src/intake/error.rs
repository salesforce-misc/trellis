//! Errors intake (issue #7) can produce: connecting/streaming the
//! replication feed, decoding it, and the checked REPLICA IDENTITY FULL
//! requirement.
//!
//! Same hand-rolled-enum convention as [`crate::staging::error::StagingError`]
//! and [`crate::error::Error`], with `From` impls so intake's call sites can
//! use `?` against the crates it composes.

use std::fmt;

use super::pgoutput::DecodeError;
use crate::error_code::{self, ErrorCode};
use crate::staging::StagingError;

/// Failure modes specific to intake.
#[derive(Debug)]
pub enum IntakeError {
    /// The replication transport (`pgwire_replication`) failed to connect,
    /// authenticate, or stream. A `PgWireError`, flattened to a `String` at
    /// the boundary rather than depending on its internal shape.
    Transport(String),
    /// The staging ring's blind append, or the transaction it runs in,
    /// failed. Composes [`StagingError`] via `From` rather than
    /// duplicating its variants.
    Staging(StagingError),
    /// A raw `tokio_postgres` error from the linchpin transaction (the
    /// watermark UPDATE, the NOTIFY, or the transaction's own
    /// BEGIN/COMMIT/ROLLBACK) that isn't already wrapped by
    /// [`IntakeError::Staging`].
    Db(tokio_postgres::Error),
    /// The `pgoutput` decoder rejected a message (see [`DecodeError`]).
    /// Unreachable from a well-formed server, and named as a non-retried
    /// condition: reconnecting would only replay into the same wedge.
    Decode(DecodeError),
    /// A transform definition needs the old row image (see
    /// `intake::replica_identity`) but the source table's replica identity
    /// doesn't guarantee one. `statement` is the exact `ALTER TABLE ...
    /// REPLICA IDENTITY FULL;` an operator must run themselves.
    ReplicaIdentityRequired { table: String, statement: String },
    /// A decoded tuple had no usable value for one of its key columns (null,
    /// unchanged, or absent) — no key to stage a change under. A well-formed
    /// server always sends key values, so like [`IntakeError::Decode`] this
    /// is a named, unretried wedge, not a transient condition.
    MissingKeyValue { table: String },
    /// A source transaction's buffered change count exceeded the hard cap
    /// (issue #8's memory-bounding story). Named with the xid and the tables
    /// touched so far, so this reads as a diagnosable stall rather than an
    /// opaque OOM crash-loop.
    TransactionTooLarge {
        xid: u32,
        cap: usize,
        tables: Vec<String>,
    },
    /// The txn-buffer spill file (issue #8) failed to write or read back.
    Io(std::io::Error),
    /// A replication slot this instance has previously confirmed work
    /// against (a `replication_progress` row exists for it) is missing or
    /// invalidated. Changes between `last_confirmed_lsn` and any new slot's
    /// start position are unrecoverable by streaming, so [`super::Intake::connect`]
    /// refuses rather than resume silently into the gap. A `Client`'s staging
    /// setup handles the loss before it gets here (issue #310,
    /// [`super::slot_loss::pause_if_slot_lost`]: pause every transform the
    /// slot fed and recreate the slot), so this surfaces only for `Intake`
    /// connected directly, or a slot lost between that setup and the connect.
    SlotLost {
        slot: String,
        last_confirmed_lsn: u64,
    },
    /// A `src_table` string wasn't the `"schema.table"` shape publication
    /// reconciliation and backfill enumeration need to split and quote.
    InvalidTableName(String),
    /// [`super::publication::qualify`] was asked to join a schema or table
    /// name that itself contains a literal `.` — legal as a quoted Postgres
    /// identifier, but this crate's `"schema.table"` joined-string
    /// representation has no way to tell that dot apart from the separator.
    /// Rejected loudly at construction rather than silently mis-split later
    /// by [`super::publication::split_qualified`].
    DottedIdentifierComponent { component: String },
    /// A `pg_snapshot` value read back from Postgres (the backfill fence, or
    /// the current snapshot compared against it) wasn't in the
    /// `"xmin:xmax:xip..."` text form this decodes.
    InvalidSnapshot(String),
    /// `slot` has no `replication_progress` row at connect time.
    /// [`super::publication::initial_snapshot_handshake`] seeds this row (at
    /// the slot's own consistent point) as part of creating the slot, so a
    /// missing row here means that handshake never ran, or ran against a
    /// different database than intake is connecting to — a precondition
    /// violation, not a fresh-slot state. Left unrepaired: the linchpin's
    /// watermark `UPDATE` (issue #31) matches zero rows forever while the
    /// slot is still acked, so WAL reclaims ahead of a watermark that never
    /// persists, with nothing to say why.
    MissingProgressRow { slot: String },
    /// `slot` exists in `pg_replication_slots` but has no
    /// `replication_progress` row — `pg_create_logical_replication_slot`
    /// persists the slot to disk the instant it returns, independent of the
    /// transaction it was called in, so a crash between that call and
    /// [`super::publication::initial_snapshot_handshake`]'s own commit
    /// leaves the slot behind with no seed row and no backfill. Re-running
    /// the handshake would die at slot-create with "already exists," and
    /// dropping the slot automatically is a destructive, WAL-retention-losing
    /// action this crate never takes on its own — so this names the exact
    /// operator recovery instead.
    OrphanedSlot { slot: String },
    /// Issue #133: a `defs::catalog` lookup failed — the only catalog
    /// dependency this module has is the outbound-relationship cache
    /// (`relationships_from_table`) that populates the ring's `group_key`
    /// column. Composed via `From` rather than flattened, matching
    /// [`IntakeError::Staging`]'s convention. `Box`ed: `CatalogError`
    /// itself carries a `Backfill(IntakeError)` variant, so an unboxed
    /// cycle between the two enums would make both infinite-sized.
    Catalog(Box<crate::defs::catalog::CatalogError>),
    /// Issue #330: reporting the target rows a resume's discharge deleted
    /// (`super::resume_orphans`) through the target-mutation seam failed.
    /// `Box`ed for the same reason as [`IntakeError::Catalog`]:
    /// `ApplyError` carries an `Intake(IntakeError)` variant.
    Propagation(Box<crate::staging::ApplyError>),
}

impl IntakeError {
    /// This error's stable, coarse [`ErrorCode`] category (`docs/decisions/0008-public-api-design.md`,
    /// decision 3). Delegates to [`StagingError::code`] for
    /// [`IntakeError::Staging`] and [`error_code::classify_pg_error`] for a
    /// raw Postgres error. [`IntakeError::ReplicaIdentityRequired`] is an
    /// actionable configuration precondition on the source table
    /// (`ErrorCode::Validation`, the same category
    /// [`crate::defs::validate::ValidationError`] itself uses for a
    /// rejected-but-fixable definition); [`IntakeError::MissingProgressRow`],
    /// [`IntakeError::OrphanedSlot`], and [`IntakeError::SlotLost`] are all
    /// "an expected record isn't there" -> [`ErrorCode::NotFound`]; every
    /// other variant is a protocol/data-integrity or resource-limit
    /// condition this crate can't attribute to a specific caller mistake, so
    /// it reports [`ErrorCode::Internal`].
    pub fn code(&self) -> ErrorCode {
        match self {
            IntakeError::Transport(_) => ErrorCode::Connectivity,
            IntakeError::Staging(err) => err.code(),
            IntakeError::Db(err) => error_code::classify_pg_error(err),
            IntakeError::Catalog(err) => err.code(),
            IntakeError::Propagation(err) => err.code(),
            IntakeError::ReplicaIdentityRequired { .. } => ErrorCode::Validation,
            IntakeError::MissingProgressRow { .. }
            | IntakeError::OrphanedSlot { .. }
            | IntakeError::SlotLost { .. } => ErrorCode::NotFound,
            IntakeError::Decode(_)
            | IntakeError::MissingKeyValue { .. }
            | IntakeError::TransactionTooLarge { .. }
            | IntakeError::Io(_)
            | IntakeError::InvalidTableName(_)
            | IntakeError::DottedIdentifierComponent { .. }
            | IntakeError::InvalidSnapshot(_) => ErrorCode::Internal,
        }
    }
}

impl fmt::Display for IntakeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            IntakeError::Transport(msg) => write!(f, "replication transport error: {msg}"),
            IntakeError::Staging(err) => write!(f, "{err}"),
            IntakeError::Catalog(err) => write!(f, "{err}"),
            IntakeError::Propagation(err) => write!(f, "{err}"),
            IntakeError::Db(err) => {
                write!(f, "intake database error: ")?;
                crate::error::write_pg_error(f, err)
            }
            IntakeError::Decode(err) => write!(f, "pgoutput decode error: {err}"),
            IntakeError::ReplicaIdentityRequired { table, statement } => write!(
                f,
                "table {table} needs its old row image for a derivation that requires it; run \
                 this against the source database first: {statement}"
            ),
            IntakeError::MissingKeyValue { table } => write!(
                f,
                "decoded a change for {table} with no usable value for one of its key columns"
            ),
            IntakeError::TransactionTooLarge { xid, cap, tables } => write!(
                f,
                "transaction {xid} exceeded the hard cap of {cap} buffered changes across \
                 tables [{}] — a diagnosable stall, not an OOM crash-loop; investigate what is \
                 producing this transaction before retrying",
                tables.join(", ")
            ),
            IntakeError::Io(err) => write!(f, "intake spill file error: {err}"),
            IntakeError::SlotLost {
                slot,
                last_confirmed_lsn,
            } => write!(
                f,
                "replication slot {slot} is missing or invalidated; changes after confirmed \
                 position {last_confirmed_lsn} are unrecoverable by streaming — refusing to \
                 resume silently into the gap (a restarted staging worker pauses every transform \
                 the slot fed and recreates it; resume each to rebuild it by a fresh backfill)"
            ),
            IntakeError::InvalidTableName(name) => {
                write!(f, "expected a \"schema.table\" name, got {name:?}")
            }
            IntakeError::DottedIdentifierComponent { component } => write!(
                f,
                "identifier component {component:?} contains a literal '.', which this crate's \
                 \"schema.table\" joined-string representation cannot round-trip; refusing to \
                 build an ambiguous src_table"
            ),
            IntakeError::InvalidSnapshot(text) => {
                write!(f, "could not parse pg_snapshot text {text:?}")
            }
            IntakeError::MissingProgressRow { slot } => write!(
                f,
                "no replication_progress row for slot {slot}; initial_snapshot_handshake is \
                 expected to seed one when the slot is created — refusing to start rather than \
                 stage work with no durable watermark to advance"
            ),
            IntakeError::OrphanedSlot { slot } => write!(
                f,
                "replication slot {slot} exists but has no replication_progress row — it was \
                 created but never fully initialized (a crash during initial_snapshot_handshake, \
                 before its seed row and backfill committed); run \
                 `SELECT pg_drop_replication_slot('{slot}')` and retry the handshake"
            ),
        }
    }
}

impl std::error::Error for IntakeError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            IntakeError::Staging(err) => Some(err),
            IntakeError::Db(err) => Some(err),
            IntakeError::Decode(err) => Some(err),
            IntakeError::Io(err) => Some(err),
            IntakeError::Catalog(err) => Some(err),
            IntakeError::Propagation(err) => Some(err),
            IntakeError::Transport(_)
            | IntakeError::ReplicaIdentityRequired { .. }
            | IntakeError::MissingKeyValue { .. }
            | IntakeError::TransactionTooLarge { .. }
            | IntakeError::SlotLost { .. }
            | IntakeError::InvalidTableName(_)
            | IntakeError::DottedIdentifierComponent { .. }
            | IntakeError::InvalidSnapshot(_)
            | IntakeError::MissingProgressRow { .. }
            | IntakeError::OrphanedSlot { .. } => None,
        }
    }
}

impl From<std::io::Error> for IntakeError {
    fn from(err: std::io::Error) -> Self {
        IntakeError::Io(err)
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

impl From<DecodeError> for IntakeError {
    fn from(err: DecodeError) -> Self {
        IntakeError::Decode(err)
    }
}

impl From<pgwire_replication::PgWireError> for IntakeError {
    fn from(err: pgwire_replication::PgWireError) -> Self {
        IntakeError::Transport(err.to_string())
    }
}
