//! The 1-1 side ledger (ADR-0002): one row per source key a 1-1
//! target has applied, recording the ordering state (`applied_lsn`,
//! `applied_seg`, `basis`, `tombstone`) that decides whether a later change
//! for the key is applied or skipped. The target row holds the values.
//!
//! Every 1-1 target gets one, created beside it in the registration
//! transaction (`<target>__ledger`, in the target's schema) and dropped with
//! it. An aggregate keeps its partials in the group basis's hidden table
//! instead (`staging::group_basis`).
//!
//! The bookkeeping columns are `__`-prefixed so they can't collide with a
//! target column, which validation keeps out of that prefix
//! ([`super::validate::RESERVED_COLUMN_PREFIX`]).

use crate::pool::quote_ident;

/// The suffix a target's ledger table adds to the target's name. Validation
/// refuses a target named with it ([`super::validate::ValidationError::ReservedTargetSuffix`]),
/// so one target's ledger can never be another target.
pub const LEDGER_SUFFIX: &str = "__ledger";

/// The longest target name whose ledger table name still fits Postgres's
/// 63-byte identifier limit (`NAMEDATALEN - 1`). A longer name would be
/// silently truncated, onto the target's own name at the limit.
pub const MAX_TARGET_NAME_LEN: usize = 63 - LEDGER_SUFFIX.len();

/// The reserved suffixes of the tables Trellis keeps beside a target.
pub const RESERVED_TARGET_SUFFIXES: [&str; 1] = [LEDGER_SUFFIX];

/// The source row's key, encoded exactly as the ring's `key`
/// ([`super::ddl::pk_key_sql_expr`]).
pub(crate) const KEY_COLUMN: &str = "__from_key";
/// The `lsn` of the last change applied to the entry. A Re-derive leaves it
/// unchanged, so the build writes null.
pub(crate) const APPLIED_LSN_COLUMN: &str = "__applied_lsn";
/// The tombstone GC's watermark: while the entry is a tombstone, a
/// ring segment (`seg_seq`) at or above every batch that can hold a change
/// the entry would refuse. Only a write that leaves the entry a tombstone
/// sets it ([`tombstone_seg_sql`]); a live entry's is whatever its last
/// tombstone left, or null, and nothing reads it.
pub(crate) const APPLIED_SEG_COLUMN: &str = "__applied_seg";
/// The `pg_current_snapshot()` of the read that last wrote the entry, taken
/// in the same statement as that read (ADR-0002 I1). A change whose
/// transaction is visible in it is already counted.
pub(crate) const BASIS_COLUMN: &str = "__basis";
/// Whether the key's last applied change deleted it.
pub(crate) const TOMBSTONE_COLUMN: &str = "__tombstone";

/// The value a write gives an entry's [`APPLIED_SEG_COLUMN`], as SQL:
/// `greatest(old, seg)` when the write leaves the entry a tombstone
/// (`tombstone`, the SQL of the entry's new `__tombstone`), the old value
/// otherwise. `old` is the entry's current value (`None` for an insert,
/// which has none) and `seg` the segment the write stamps: a page's latest
/// segment, or for a Re-derive the newest one its read sees, if that is
/// newer.
///
/// **Only a tombstone carries the stamp.** The GC
/// (`staging::retire::collect_tombstones`) reads `applied_seg` only on
/// tombstones, through a partial index keyed on it. An update that changes
/// an indexed column can't be HOT, so if every write moved it, no Apply to a
/// live entry could be. Left alone on a live entry, it changes only when an
/// entry is, or becomes, a tombstone, which changes `__tombstone` (an
/// indexed column too) or is already the rare case.
///
/// **Why a live entry's stale or null stamp is enough** (ADR-0002 I4: a
/// tombstone outlives every batch that could hold a change it refuses). The
/// stamp only has to cover the entry's state once the write that makes or
/// keeps it a tombstone is done, and each such write stamps what that state
/// needs, whatever the entry carried while it was live:
///
/// - **An Apply of a change D** (a delete) stamps at least D's batch, since a
///   page's latest segment holds every batch it drains. The entry then
///   refuses (I2) the changes at or below D's `lsn` and those visible in its
///   `basis`. The first are D's same-key predecessors, which committed before
///   D's trigger ran, so they are in D's batch or an earlier one. For the
///   second, D was applied only because it is not visible in `basis` (or
///   there is none): a change C that is visible completed before that
///   snapshot was taken and D had not, so C completed before D. D's batch's
///   fence sees D, so it sees C too: C is in D's batch or an earlier one. So
///   D's batch covers everything, including a `basis` an earlier Re-derive
///   wrote while the entry was live, unstamped.
/// - **A Re-derive** stamps at least the newest segment its read's snapshot
///   sees, which holds every change that snapshot saw: everything its
///   `basis` refuses, and the change behind the entry's `applied_lsn`, which
///   an earlier page applied and committed before this one took the entry
///   lock and read.
/// - **`greatest`** keeps any higher stamp the entry already had: a stale one
///   (an earlier tombstone's, kept through a revival) only delays the GC.
///
/// A revival (a tombstone becoming live again) keeps the old stamp, and the
/// next write that makes the entry a tombstone stamps it afresh, as above.
/// A GC batch re-checks `__tombstone` and the stamp on the row it locks, so a
/// page that revives a tombstone under it keeps the entry.
pub(crate) fn tombstone_seg_sql(old: Option<&str>, tombstone: &str, seg: &str) -> String {
    match old {
        Some(old) => format!("case when {tombstone} then greatest({old}, {seg}) else {old} end"),
        None => format!("case when {tombstone} then {seg} end"),
    }
}

/// A target's ledger table name.
pub(crate) fn ledger_table_name(target: &str) -> String {
    format!("{target}{LEDGER_SUFFIX}")
}

/// A target's ledger, schema-qualified and quoted for SQL text, from the
/// target's schema and bare name.
pub(crate) fn qualified_ledger_table(target_schema: &str, target: &str) -> String {
    format!(
        "{}.{}",
        quote_ident(target_schema),
        quote_ident(&ledger_table_name(target))
    )
}

/// The ordering-state columns every ledger ends with.
fn ordering_state_columns() -> String {
    format!(
        "{} pg_lsn, {} bigint, {} pg_snapshot, {} boolean not null default false",
        quote_ident(APPLIED_LSN_COLUMN),
        quote_ident(APPLIED_SEG_COLUMN),
        quote_ident(BASIS_COLUMN),
        quote_ident(TOMBSTONE_COLUMN),
    )
}

/// A 1-1 target's ledger DDL: the key and the ordering state,
/// with no values (the target row holds them), as a statement to append to
/// the target's own. Created empty: the 1-1 build's writes are absolute, and
/// the Re-derives that follow it stamp the entries. The partial
/// index is the tombstone GC's (`staging::retire::collect_tombstones`), keyed
/// on `applied_seg`, so that a GC batch seeks the
/// collectable tombstones rather than walking every one above the drained
/// prefix. An Apply to a live entry leaves `applied_seg` alone
/// ([`tombstone_seg_sql`]), so it changes no indexed column and can be a HOT
/// update. The key sorts in `"C"`: every lock is taken in key order,
/// and a byte comparison is far cheaper than a locale's.
pub(crate) fn one_to_one_ledger_ddl(qualified_ledger: &str) -> String {
    format!(
        "; create table {qualified_ledger} ({} text collate \"C\" primary key, {}); \
         create index on {qualified_ledger} ({}) where {}",
        quote_ident(KEY_COLUMN),
        ordering_state_columns(),
        quote_ident(APPLIED_SEG_COLUMN),
        quote_ident(TOMBSTONE_COLUMN),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_one_to_one_ledger_holds_only_the_key_and_the_ordering_state() {
        assert_eq!(
            one_to_one_ledger_ddl(r#""public"."t__ledger""#),
            r#"; create table "public"."t__ledger" ("__from_key" text collate "C" primary key, "__applied_lsn" pg_lsn, "__applied_seg" bigint, "__basis" pg_snapshot, "__tombstone" boolean not null default false); create index on "public"."t__ledger" ("__applied_seg") where "__tombstone""#
        );
    }

    #[test]
    fn a_ledger_name_derives_from_the_target_and_fits_the_identifier_limit() {
        assert_eq!(ledger_table_name("totals"), "totals__ledger");
        assert_eq!(
            qualified_ledger_table("app", "totals"),
            r#""app"."totals__ledger""#
        );
        assert_eq!(
            ledger_table_name(&"x".repeat(MAX_TARGET_NAME_LEN)).len(),
            63
        );
    }
}
