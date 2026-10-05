//! Generates one table's capture functions and triggers (#622 C2).
//!
//! Each captured table gets one function per event (insert, update, delete,
//! truncate) and one `AFTER … FOR EACH STATEMENT` trigger per function, plus
//! a `BEFORE INSERT OR UPDATE OR DELETE … FOR EACH STATEMENT` begin trigger
//! and function that only mark where a statement's span starts (#623 D8a). The
//! statement shape follows the #565 spike's `capture_sql.py`: one `INSERT …
//! SELECT` from the transition table into the active ring slot, chosen by a
//! `CASE` over the four static `seg_N` inserts so every arm keeps a cached
//! plan.
//!
//! # What every function does
//!
//! - **It runs as its owner.** Each function is `SECURITY DEFINER`, so the
//!   application's role needs no privilege on Trellis's schema. Its owner is
//!   the one Trellis role, which owns the ring and does every other Trellis
//!   operation too (#622 plan Q3; see [`super::install`]'s "The Trellis
//!   role").
//! - **It doesn't trust the writer's session.** `search_path` is pinned to
//!   `pg_catalog, pg_temp`, and every ring and mirror reference is
//!   schema-qualified. The five output settings Trellis pins everywhere
//!   ([`crate::pool::DETERMINISTIC_TEXT_OUTPUT_GUCS`]) are `SET` clauses on
//!   the function, parsed from that constant so there is one list. Without
//!   them an image would depend on the application session's `DateStyle`,
//!   `TimeZone`, `bytea_output`, `IntervalStyle` and `extra_float_digits`
//!   (#565 E3). Literals in the body never contain a backslash, so
//!   `standard_conforming_strings` can't change their meaning either.
//! - **It renders a value with the type's output function.** That is what
//!   `format('%s', col)` calls, and what the rest of Trellis compares a
//!   value's text against; `tests/capture_parity.rs` pins it against a golden
//!   fixture. `::text` is not the same: for example,
//!   it strips `char(n)` padding. A `NULL` stays a JSON `null`, because
//!   `format('%s', NULL)` is an empty string.
//! - **It reads the active slot the way every ring writer must** (#597): from
//!   `ring_slot_mirror` with `pg_sequence_last_value`, in the same expression
//!   that assigns the writer's xid. See `staging::append::active_ring_slot`
//!   for why both halves matter at every isolation level.
//! - **It images the live row** (#623 D8a): when another write to the table
//!   ran in the statement's span, every row the statement wrote is re-read
//!   by primary key, so a nested write to the same key leaves the later ring
//!   row carrying the final values (see [`ring_select`]). Otherwise the
//!   transition row is the live row, and it reads no relation, so a
//!   `SERIALIZABLE` writer takes no predicate lock on the table (see
//!   [`function_body`]). An update row whose imaged columns didn't change is
//!   dropped either way.
//! - **It skips a statement that changed nothing.** An `UPDATE … WHERE false`
//!   still fires a statement trigger. Returning early keeps it from
//!   assigning an xid it doesn't need.
//! - **It never fails the statement over a renamed or dropped column** (#622
//!   C6). The same guard query counts the columns the function images; on a
//!   miss it writes a [`SCHEMA_CHANGED_OP`] marker and images what's left
//!   (see [`function_body`]).
//! - **It stamps `lsn = origin_lsn = pg_current_wal_insert_lsn()`** and
//!   `src_changed = clock_timestamp()`, both read once per statement.
//!
//! The function records its column set in `COMMENT ON FUNCTION`, as JSON
//! ([`comment_text`]). C3 reads the installed state back from the catalog
//! that way, and `pg_dump` carries it (#644).
//!
//! # Names
//!
//! A function lives in the instance's schema, and its name is derived from
//! the captured table plus a hash of the table's qualified name, so no two
//! tables collide ([`function_name`]). A trigger is named after the instance
//! schema ([`trigger_name`]), so two instances can capture one table
//! without replacing each other's triggers.

use std::collections::BTreeSet;

use crate::defs::ddl::composite_key_escape_sql;
use crate::pool::{DETERMINISTIC_TEXT_OUTPUT_GUCS, quote_ident};
use crate::staging::append::{RING_SIZE, SCHEMA_CHANGED_SENTINEL_KEY, TRUNCATE_SENTINEL_KEY};

use super::CaptureError;

/// The ring `op` of the marker a capture function writes when a column it
/// images has been renamed or dropped (#622 C6), keyed by
/// [`SCHEMA_CHANGED_SENTINEL_KEY`]. Its `new_image` is `{"missing": [<column
/// names, sorted>], "key_missing": <bool>}`, and it has no `old_image` or
/// `group_key`.
pub const SCHEMA_CHANGED_OP: &str = "schema_changed";

/// The transition-table names the insert, update and delete triggers
/// declare with `REFERENCING`.
const OLD_ROWS: &str = "trellis_old";
const NEW_ROWS: &str = "trellis_new";

/// The ring columns a capture insert writes. Everything else (`hop_gen`,
/// `row_txid`, `change_id`, `appended_at`, `route`, `retry_count`,
/// `relationship_id`) comes from the ring tables' own defaults, exactly as
/// for `staging::append::append`.
const RING_COLUMNS: &str =
    "src_table, key, op, lsn, old_image, new_image, origin_lsn, src_changed, group_key";

/// Postgres truncates a longer identifier, so every generated name stays
/// within this many bytes.
const MAX_IDENT_BYTES: usize = 63;

/// `jsonb_build_object` takes at most 100 arguments (`FUNC_MAX_ARGS`), so a
/// wider image is built from several calls joined with `||`.
const MAX_PAIRS_PER_BUILD: usize = 50;

/// What one table's capture functions image.
///
/// Built by [`super::columns::capture_spec`] from the catalog, or by hand in
/// a test. [`CaptureSpec::new`] checks the invariants the generator relies
/// on.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CaptureSpec {
    /// The captured table's unquoted `schema.table` identity, the same
    /// spelling `intake::markers::qualify` builds and the ring's
    /// `src_table` holds.
    table: String,
    /// The primary key's columns, in the key's declared order
    /// (`ddl::pk_key_sql_expr`'s "declared-order convention").
    key: Vec<String>,
    /// Every column an image carries, sorted. Includes every key column.
    columns: Vec<String>,
    /// The columns whose values make up the ring's `group_key` (every
    /// outbound relationship's `from_col`, #133), in the table's physical
    /// column order. The order is load-bearing: it is the element order every
    /// ring row's `group_key` array has had (issue #133), and a reader
    /// comparing arrays relies on it.
    group_key: Vec<String>,
}

impl CaptureSpec {
    /// Checks that `table` has exactly one `.`, that `key` is non-empty, and
    /// that every key and group-key column is also an image column.
    pub fn new(
        table: impl Into<String>,
        key: Vec<String>,
        columns: impl IntoIterator<Item = String>,
        group_key: Vec<String>,
    ) -> Result<Self, CaptureError> {
        let table = table.into();
        split_table(&table)?;
        if key.is_empty() {
            return Err(CaptureError::NoPrimaryKey { table });
        }
        let mut columns: BTreeSet<String> = columns.into_iter().collect();
        columns.extend(key.iter().cloned());
        if let Some(column) = group_key.iter().find(|c| !columns.contains(*c)) {
            return Err(CaptureError::MissingColumn {
                table,
                column: column.clone(),
            });
        }
        Ok(CaptureSpec {
            table,
            key,
            columns: columns.into_iter().collect(),
            group_key,
        })
    }

    pub fn table(&self) -> &str {
        &self.table
    }

    pub fn key(&self) -> &[String] {
        &self.key
    }

    pub fn columns(&self) -> &[String] {
        &self.columns
    }

    pub fn group_key(&self) -> &[String] {
        &self.group_key
    }
}

/// One of the five statement triggers capture installs on a table, each with
/// its own function: the four events it images into the ring, and
/// [`CaptureEvent::Begin`], the `BEFORE` statement trigger that marks where a
/// statement's span starts (see "The live re-read is conditional" on
/// [`function_body`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CaptureEvent {
    Insert,
    Update,
    Delete,
    Truncate,
    Begin,
}

impl CaptureEvent {
    /// Every trigger and function capture installs, checks and removes.
    pub const ALL: [CaptureEvent; 5] = [
        CaptureEvent::Insert,
        CaptureEvent::Update,
        CaptureEvent::Delete,
        CaptureEvent::Truncate,
        CaptureEvent::Begin,
    ];

    /// The events whose functions write ring rows.
    pub const CAPTURED: [CaptureEvent; 4] = [
        CaptureEvent::Insert,
        CaptureEvent::Update,
        CaptureEvent::Delete,
        CaptureEvent::Truncate,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            CaptureEvent::Insert => "insert",
            CaptureEvent::Update => "update",
            CaptureEvent::Delete => "delete",
            CaptureEvent::Truncate => "truncate",
            CaptureEvent::Begin => "begin",
        }
    }

    /// The trigger's `REFERENCING` clause. A truncate or `BEFORE` trigger
    /// can't have transition tables.
    fn referencing(self) -> String {
        match self {
            CaptureEvent::Insert => format!(" referencing new table as {NEW_ROWS}"),
            CaptureEvent::Update => {
                format!(" referencing old table as {OLD_ROWS} new table as {NEW_ROWS}")
            }
            CaptureEvent::Delete => format!(" referencing old table as {OLD_ROWS}"),
            CaptureEvent::Truncate | CaptureEvent::Begin => String::new(),
        }
    }
}

/// The `set <name> to <value>` clauses of
/// [`crate::pool::DETERMINISTIC_TEXT_OUTPUT_GUCS`], one per setting. The same
/// text is valid both as a session statement and as a `CREATE FUNCTION`
/// `SET` clause, so the generator and a test that pins a session use it
/// as-is.
pub fn pinned_output_settings() -> Vec<&'static str> {
    DETERMINISTIC_TEXT_OUTPUT_GUCS
        .split(';')
        .map(str::trim)
        .filter(|clause| !clause.is_empty())
        .inspect(|clause| {
            assert!(
                clause.starts_with("set ") && clause.contains(" to "),
                "DETERMINISTIC_TEXT_OUTPUT_GUCS clause {clause:?} is not `set <name> to <value>`"
            )
        })
        .collect()
}

/// Planner settings every capture function runs (and so plans) under, as
/// `SET` clauses (#623 D8a).
///
/// PL/pgSQL plans each statement once per session, often on the first
/// write to a table that is still empty or that a `VACUUM` last saw empty.
/// Costed against an empty table, a sequential scan beats the primary-key
/// index for the live re-read, and the cached plan would then scan the whole
/// table for every captured row once it has grown. With sequential scans
/// off, the probe is an index scan from the first plan on. The function's
/// other reads are transition tables (not sequential scans) and catalog
/// probes that use their indexes anyway.
pub const PLANNER_SETTINGS: &[&str] = &["set enable_seqscan to 'off'"];

/// The unquoted name of `table`'s capture function for `event`, in the
/// instance schema: `cap_<event>_<table>_<hash>`, where `<table>` is the
/// table's name cut to fit and `<hash>` is 16 hex digits of a hash of the
/// whole qualified name. The hash keeps `a_b.c` and `a.b_c`, or two long
/// names sharing a prefix, apart.
pub fn function_name(table: &str, event: CaptureEvent) -> Result<String, CaptureError> {
    let (_, name) = split_table(table)?;
    let hash = format!("{:016x}", fnv1a64(table.as_bytes()));
    let fixed = "cap_".len() + event.as_str().len() + 2 + hash.len();
    let name = truncate_to(name, MAX_IDENT_BYTES - fixed);
    Ok(format!("cap_{}_{name}_{hash}", event.as_str()))
}

/// The unquoted name of the instance's `event` trigger on a captured table:
/// `<schema>_capture_<event>`, so the default instance's insert trigger is
/// `trellis_capture_insert`. A schema too long for that to fit keeps a
/// prefix of its name plus a hash of the whole name.
pub fn trigger_name(schema: &str, event: CaptureEvent) -> String {
    let suffix = format!("_capture_{}", event.as_str());
    if schema.len() + suffix.len() <= MAX_IDENT_BYTES {
        return format!("{schema}{suffix}");
    }
    let hash = format!("{:016x}", fnv1a64(schema.as_bytes()));
    let prefix = truncate_to(schema, MAX_IDENT_BYTES - suffix.len() - 1 - hash.len());
    format!("{prefix}_{hash}{suffix}")
}

/// Every statement that installs `spec`'s capture in instance schema
/// `schema`, in order: per event, the function, the revoke of `PUBLIC`'s
/// `EXECUTE` ([`revoke_ddl`]), its comment, the trigger and `ENABLE ALWAYS`
/// (which keeps capture on for a session in `session_replication_role =
/// replica`).
///
/// C3 runs these in one retried transaction under `locks::DdlRetry`, with the
/// join marker. `CREATE OR REPLACE TRIGGER` needs PostgreSQL 14.
pub fn install_statements(schema: &str, spec: &CaptureSpec) -> Result<Vec<String>, CaptureError> {
    let mut statements = Vec::with_capacity(CaptureEvent::ALL.len() * 5);
    for event in CaptureEvent::ALL {
        statements.push(function_ddl(schema, spec, event)?);
        statements.push(revoke_ddl(schema, spec, event)?);
        statements.push(comment_ddl(schema, spec, event)?);
        statements.extend(trigger_ddl(schema, spec, event)?);
    }
    Ok(statements)
}

/// Every statement that replaces `spec`'s capture functions without touching
/// the triggers, in order: per event, the function, the revoke of `PUBLIC`'s
/// `EXECUTE` and the comment. A widen or narrow runs these (C3,
/// `capture::install`); `CREATE OR REPLACE FUNCTION` keeps the function's
/// owner and privileges, and the revoke is repeated anyway so the statements
/// stand on their own.
pub fn function_statements(schema: &str, spec: &CaptureSpec) -> Result<Vec<String>, CaptureError> {
    let mut statements = Vec::with_capacity(CaptureEvent::ALL.len() * 3);
    for event in CaptureEvent::ALL {
        statements.push(function_ddl(schema, spec, event)?);
        statements.push(revoke_ddl(schema, spec, event)?);
        statements.push(comment_ddl(schema, spec, event)?);
    }
    Ok(statements)
}

/// Every statement that removes `table`'s capture in instance schema
/// `schema`, in order: the five triggers, then the five functions, each `if
/// exists`, so a partial install is removed as well. With `table_exists`
/// false (the application dropped the table, and its triggers with it) only
/// the functions are dropped.
pub fn uninstall_statements(
    schema: &str,
    table: &str,
    table_exists: bool,
) -> Result<Vec<String>, CaptureError> {
    let quoted = quoted_table(table)?;
    let mut statements = Vec::with_capacity(CaptureEvent::ALL.len() * 2);
    if table_exists {
        for event in CaptureEvent::ALL {
            statements.push(format!(
                "drop trigger if exists {} on {quoted}",
                quote_ident(&trigger_name(schema, event))
            ));
        }
    }
    for event in CaptureEvent::ALL {
        statements.push(format!(
            "drop function if exists {}()",
            qualified_function_name(schema, table, event)?
        ));
    }
    Ok(statements)
}

/// `ALTER FUNCTION … OWNER TO` `owner` for each of `table`'s capture
/// functions: the functions run as their owner (`SECURITY DEFINER`), which
/// #622's plan (Q3) makes the role that owns the ring, whoever installs them
/// (issue #701).
pub fn owner_statements(
    schema: &str,
    table: &str,
    owner: &str,
) -> Result<Vec<String>, CaptureError> {
    CaptureEvent::ALL
        .iter()
        .map(|event| {
            Ok(format!(
                "alter function {}() owner to {}",
                qualified_function_name(schema, table, *event)?,
                quote_ident(owner)
            ))
        })
        .collect()
}

/// `REVOKE ALL ON FUNCTION … FROM PUBLIC` for `spec`'s `event` capture
/// function.
///
/// A new function is executable by `PUBLIC`. For a `SECURITY DEFINER`
/// capture function that would let any role with `USAGE` on the instance
/// schema attach it to a table of its own with `CREATE TRIGGER` and forge
/// ring rows for the captured table. A trigger fires without an `EXECUTE`
/// check on the writer, so revoking it costs the application nothing; only
/// `CREATE TRIGGER` checks it, and the owner keeps it.
pub fn revoke_ddl(
    schema: &str,
    spec: &CaptureSpec,
    event: CaptureEvent,
) -> Result<String, CaptureError> {
    Ok(format!(
        "revoke all on function {}() from public",
        qualified_function(schema, spec, event)?
    ))
}

/// `CREATE OR REPLACE FUNCTION` for `spec`'s `event` capture function in
/// instance schema `schema`.
pub fn function_ddl(
    schema: &str,
    spec: &CaptureSpec,
    event: CaptureEvent,
) -> Result<String, CaptureError> {
    let function = qualified_function(schema, spec, event)?;
    let body = function_body(schema, spec, event);
    let tag = dollar_tag(&body);
    // The begin function renders and reads nothing, so it skips the
    // settings' save and restore on every statement.
    let settings: String = match event {
        CaptureEvent::Begin => String::new(),
        _ => pinned_output_settings()
            .iter()
            .chain(PLANNER_SETTINGS)
            .map(|clause| format!("\n    {clause}"))
            .collect(),
    };
    Ok(format!(
        "create or replace function {function}()\n    returns trigger\n    language plpgsql\n    \
         security definer\n    set search_path = pg_catalog, pg_temp{settings}\nas {tag}\n{body}{tag}"
    ))
}

/// `COMMENT ON FUNCTION` recording [`comment_text`].
pub fn comment_ddl(
    schema: &str,
    spec: &CaptureSpec,
    event: CaptureEvent,
) -> Result<String, CaptureError> {
    Ok(format!(
        "comment on function {}() is {}",
        qualified_function(schema, spec, event)?,
        string_constant(&comment_text(spec, event))
    ))
}

/// What a capture function's comment records, as a JSON object: the table,
/// the event, and the key, image and group-key columns. C3's
/// `installed(table)` reads it back to decide whether the installed
/// functions match the spec the catalog now asks for.
pub fn comment_text(spec: &CaptureSpec, event: CaptureEvent) -> String {
    let list = |names: &[String]| {
        let items: Vec<String> = names
            .iter()
            .map(|n| crate::intake::json_string(n))
            .collect();
        format!("[{}]", items.join(","))
    };
    format!(
        "{{\"trellis_capture\":1,\"table\":{},\"event\":{},\"key\":{},\"columns\":{},\"group_key\":{}}}",
        crate::intake::json_string(&spec.table),
        crate::intake::json_string(event.as_str()),
        list(&spec.key),
        list(&spec.columns),
        list(&spec.group_key),
    )
}

/// `CREATE OR REPLACE TRIGGER` plus `ALTER TABLE … ENABLE ALWAYS TRIGGER` for
/// `spec`'s `event` trigger.
pub fn trigger_ddl(
    schema: &str,
    spec: &CaptureSpec,
    event: CaptureEvent,
) -> Result<Vec<String>, CaptureError> {
    let table = quoted_table(&spec.table)?;
    let trigger = quote_ident(&trigger_name(schema, event));
    let function = qualified_function(schema, spec, event)?;
    let timing = match event {
        CaptureEvent::Begin => "before insert or update or delete".to_string(),
        _ => format!("after {}", event.as_str()),
    };
    Ok(vec![
        format!(
            "create or replace trigger {trigger} {timing} on {table}{} \
             for each statement execute function {function}()",
            event.referencing(),
        ),
        format!("alter table {table} enable always trigger {trigger}"),
    ])
}

fn qualified_function(
    schema: &str,
    spec: &CaptureSpec,
    event: CaptureEvent,
) -> Result<String, CaptureError> {
    qualified_function_name(schema, &spec.table, event)
}

fn qualified_function_name(
    schema: &str,
    table: &str,
    event: CaptureEvent,
) -> Result<String, CaptureError> {
    Ok(format!(
        "{}.{}",
        quote_ident(schema),
        quote_ident(&function_name(table, event)?)
    ))
}

/// What `pg_proc.prosrc` holds for the function [`function_ddl`] creates:
/// the text between the dollar quotes. C3's `installed` compares the two to
/// tell a function this build would generate from one an older generator,
/// or a hand edit, left behind.
pub(crate) fn function_source(schema: &str, spec: &CaptureSpec, event: CaptureEvent) -> String {
    format!("\n{}", function_body(schema, spec, event))
}

/// The PL/pgSQL body, from `#variable_conflict` through `end;`.
///
/// The insert and delete statements name the variables `l` and `ts`
/// unqualified in a `SELECT … FROM` the transition table, which carries every
/// column of the captured table. Under PL/pgSQL's default
/// (`plpgsql.variable_conflict = error`), a captured table with a column named
/// `l` or `ts` would make every write to it fail with "column reference is
/// ambiguous", and under a server-wide `use_column` the ring would silently
/// get that column's value as its `lsn`. `#variable_conflict use_variable`
/// pins the resolution to the variables. Every column reference in the body
/// is qualified by its alias, so nothing needs the other resolution.
///
/// # A renamed or dropped column never fails the write (#622 C6)
///
/// The static ring inserts name every imaged column. Once one of them is
/// renamed or dropped, planning them fails, and with them the application's
/// statement. So the insert, update and delete functions first count, in
/// one `pg_attribute` probe, how many of their imaged columns the table
/// still has. The probe rides in the empty-statement guard's query, which
/// the function runs anyway (#622 plan Q2(c); C4's
/// `trigger+column-check-guard` measured it at +3.4–5 µs per single-row
/// statement). On a miss the function takes [`schema_changed_branch`]
/// instead of the static inserts. PL/pgSQL plans a statement only when it
/// first runs it, so the stale inserts are never planned.
///
/// The check is sound per statement because `ALTER TABLE … RENAME` and `DROP
/// COLUMN` take `ACCESS EXCLUSIVE`: no captured statement runs across one,
/// and a statement after one in the same transaction probes the new catalog.
/// The truncate function images nothing, so it has no probe.
///
/// # The live re-read is conditional (#623 D8a)
///
/// The live re-read ([`ring_select`]) is an index probe of the source table
/// inside the application's write, and under `SERIALIZABLE` every probe takes
/// a SIREAD lock on the key's btree leaf page. Concurrent serializable
/// writers on neighbouring keys (every insert of an auto-increment id lands
/// on the rightmost leaf) then form the rw-conflict chains SSI cancels with
/// `40001`, writes that would commit without capture. So a function re-reads
/// only when the live row can differ from the transition row, and otherwise
/// images the transition tables, reading no relation at all.
///
/// The live version of a key this statement wrote can differ from its
/// transition row only if some other write to the same table changed it
/// after this statement's row change and before this capture runs: a nested
/// statement (an application trigger, an FK cascade, a function the
/// statement calls) or a sibling event of the same statement (a writable
/// CTE, `MERGE`, `INSERT … ON CONFLICT DO UPDATE`). Every such write is a
/// statement on this table, so its own capture function runs before this one
/// returns. The table's plain-table check (`defs::catalog::change_keyed`)
/// rules out partitions and inheritance children, whose writes would fire
/// another table's triggers.
///
/// So each instance keeps, per table, a transaction-local setting
/// ([`span_setting`]) holding `{seq, open, start}`:
///
/// - `seq` counts this transaction's capture calls that staged rows;
/// - the [`CaptureEvent::Begin`] function, a `BEFORE` statement trigger,
///   increments `open`, and when `open` was 0 sets `start` to `seq`;
/// - each capture call decrements `open`, re-reads if `seq <> start`, and
///   then increments `seq` if its statement changed any row.
///
/// While any statement on the table is between its `BEFORE` and its capture
/// `open` stays above 0, so `start` was set no later than the start of the
/// statement now capturing, and any capture call that staged rows since then
/// has moved `seq`. Every inexact case errs towards re-reading: a `start`
/// older than the statement (a nested or sibling statement's), a `BEFORE`
/// with no matching capture (several `ModifyTable` nodes share one `AFTER`
/// call), or no `start` at all (`-1`: the begin trigger was disabled or
/// missing, which the C9 audit reports). A subtransaction that rolls back
/// restores the setting with the writes it undid. The setting is the
/// application's to overwrite, as it could disable the triggers; doing so
/// can only cost it its own derived data, and the re-read itself never reads
/// another transaction's row either way.
///
/// One write reaches the table without a capture call of its own: the
/// update a foreign key's action makes on it (`ON UPDATE CASCADE`, `SET
/// NULL` or `SET DEFAULT`, or `ON DELETE SET NULL` or `SET DEFAULT`).
/// Postgres runs it in the trigger query level of the statement whose row
/// fired the action, not in one of its own. So if that level already fired
/// the table's `BEFORE UPDATE` statement trigger, it fires none, and its rows
/// join the transition tables of the table's update queued there that hasn't
/// fired yet: one capture call for both. A row both updates changed (a
/// self-referencing key that cascades, or two cascading keys on one row) is
/// then in the transition tables twice, and the join can pair an old version
/// with an intermediate new one and emit that pair last. Such a pairing
/// needs a key that occurs twice among the old rows, since the intermediate
/// version is an old row too, so the update function also re-reads whenever
/// a key does (`capture_ssi.rs` pins it). An `ON DELETE CASCADE` merges the
/// same way, harmlessly: a row is deleted once, and no action inserts.
///
/// The schema-changed branch ([`schema_changed_branch`]) always re-reads:
/// it is rare, and its `EXECUTE` builds one statement shape.
fn function_body(schema: &str, spec: &CaptureSpec, event: CaptureEvent) -> String {
    let span = text_expr(&span_setting(schema, &spec.table));
    let span_state = format!(
        "coalesce(nullif(pg_catalog.current_setting({span}, true), ''), '{{0,0,-1}}')::bigint[]"
    );
    if event == CaptureEvent::Begin {
        return format!(
            "declare\n    st bigint[];\nbegin\n    \
             -- #623 D8a: the statement-span state (see capture::sql's function_body).\n    \
             st := {span_state};\n    \
             if st[2] > 0 then\n        st[2] := st[2] + 1;\n    \
             else\n        st := array[st[1], 1, st[1]];\n    end if;\n    \
             perform pg_catalog.set_config({span}, cast(st as text), true);\n    \
             return null;\nend;\n"
        );
    }
    let mirror = text_expr(&format!(
        "{}.{}",
        quote_ident(schema),
        quote_ident("ring_slot_mirror")
    ));
    let rows = match event {
        CaptureEvent::Insert | CaptureEvent::Update => Some(NEW_ROWS),
        CaptureEvent::Delete => Some(OLD_ROWS),
        CaptureEvent::Truncate | CaptureEvent::Begin => None,
    };
    let mut body = String::from(
        "#variable_conflict use_variable\n\
         declare\n    slot smallint;\n    l pg_lsn;\n    ts timestamptz;\n",
    );
    if rows.is_some() {
        body.push_str(
            "    present bigint;\n    have text[];\n    st bigint[];\n    reread boolean;\n",
        );
    }
    body.push_str("begin\n");
    if let Some(rows) = rows {
        body.push_str(&format!(
            "    -- #622 C6: how many imaged columns the table still has, or -1 for a\n    \
             -- statement that changed nothing.\n    \
             present := case when exists (select 1 from {rows})\n        \
             then {} else -1 end;\n    \
             -- #623 D8a: re-read only if another capture of this table staged rows\n    \
             -- since this statement's span started.\n    \
             st := {span_state};\n    \
             reread := st[3] <> st[1];\n    \
             st[2] := case when st[2] > 0 then st[2] - 1 else 0 end;\n    \
             if present >= 0 then\n        st[1] := st[1] + 1;\n    end if;\n    \
             perform pg_catalog.set_config({span}, cast(st as text), true);\n    \
             if present < 0 then\n        return null;\n    end if;\n",
            column_probe("count(*)", &spec.columns),
        ));
    }
    body.push_str(&format!(
        "    -- #597: the xid is assigned in the same expression that reads the mirror.\n    \
         slot := case when pg_current_xact_id() is not null\n        \
         then pg_sequence_last_value(({mirror})::regclass)::smallint end;\n    \
         l := pg_current_wal_insert_lsn();\n    \
         ts := clock_timestamp();\n"
    ));
    if rows.is_some() {
        body.push_str(&schema_changed_branch(schema, spec, event));
    }
    if event == CaptureEvent::Update {
        // After the schema-changed branch, which handles a renamed key.
        body.push_str(&format!(
            "    -- #623 D8a: an FK action's update shares these transition tables, and\n    \
             -- a row updated twice leaves two old versions of one key.\n    \
             if not reread then\n        \
             reread := exists (select 1 from {OLD_ROWS} o\n            \
             group by {} collate \"C\" having count(*) > 1);\n    \
             end if;\n",
            key_expr(spec, "o")
        ));
    }
    body.push_str("    case slot\n");
    for slot in 0..RING_SIZE {
        let ring = format!(
            "{}.{}",
            quote_ident(schema),
            quote_ident(&format!("seg_{slot}"))
        );
        let insert = |live: bool| {
            format!(
                "insert into {ring} ({RING_COLUMNS})\n{}",
                ring_select(spec, event, Render::Static, live)
            )
        };
        if rows.is_some() {
            body.push_str(&format!(
                "    when {slot} then\n        if reread then\n        {};\n        \
                 else\n        {};\n        end if;\n",
                insert(true),
                insert(false)
            ));
        } else {
            body.push_str(&format!(
                "    when {slot} then\n        {};\n",
                insert(false)
            ));
        }
    }
    body.push_str(
        "    else\n        raise exception 'trellis capture: ring_slot_mirror returned %', slot;\n    \
         end case;\n    return null;\nend;\n",
    );
    body
}

/// The transaction-local setting holding instance `schema`'s statement-span
/// state for `table` (see "The live re-read is conditional" on
/// [`function_body`]): `trellis_capture.s_<hash>`, a custom setting any
/// session may set. Two tables whose hashes collide share one state, which
/// only makes both re-read more often.
pub(crate) fn span_setting(schema: &str, table: &str) -> String {
    let mut bytes = schema.as_bytes().to_vec();
    bytes.push(0);
    bytes.extend_from_slice(table.as_bytes());
    format!("trellis_capture.s_{:016x}", fnv1a64(&bytes))
}

/// A subquery over the table's live columns (`tg_relid`) named in `columns`,
/// selecting `select`: `count(*)` for the guard's probe, or the names.
fn column_probe(select: &str, columns: &[String]) -> String {
    let names: Vec<String> = columns.iter().map(|c| text_expr(c)).collect();
    format!(
        "(select {select} from pg_catalog.pg_attribute a\n            \
         where a.attrelid = tg_relid and a.attnum > 0 and not a.attisdropped\n              \
         and a.attname = any (array[{}]::name[]))",
        names.join(", ")
    )
}

/// The miss path of [`function_body`]'s column probe (#622 C6), taken when
/// some column the function images is gone. It never fails the statement:
///
/// 1. It appends one [`SCHEMA_CHANGED_OP`] marker for the table, whose
///    `new_image` names the missing columns. The drain pauses every
///    definition that reads one of them before it applies anything later
///    (`staging::schema_change`), and the staging worker's reconcile then
///    regenerates the functions over the columns the remaining readers need.
/// 2. If every key column is still there, it images the statement's rows over
///    the columns that are left, with the same shape as the static inserts
///    but built at run time and run with `EXECUTE`. PL/pgSQL registers the
///    transition tables for the whole trigger call, so dynamic SQL reads
///    them too. Definitions that don't read a missing column keep applying.
///    If a key column is gone, it writes the marker only: every row would be
///    keyed wrongly, and every definition on the table pauses.
///
/// Neither statement is in an `EXCEPTION` block, so the path takes no
/// subtransaction (#622 plan Q2).
fn schema_changed_branch(schema: &str, spec: &CaptureSpec, event: CaptureEvent) -> String {
    let all: Vec<String> = spec.columns.iter().map(|c| text_expr(c)).collect();
    let key: Vec<String> = spec.key.iter().map(|c| text_expr(c)).collect();
    let schema_lit = text_expr(&quote_ident(schema));
    let ring = format!("{schema_lit} || '.' || pg_catalog.quote_ident('seg_' || slot)");
    let key_present = format!("array[{}]::text[] <@ have", key.join(", "));
    let marker = format!(
        "pg_catalog.jsonb_build_object('missing', pg_catalog.to_jsonb((\
         select coalesce(pg_catalog.array_agg(m.c order by m.c), '{{}}') \
         from pg_catalog.unnest(array[{}]::text[]) as m(c) where m.c <> all (have))), \
         'key_missing', not ({key_present}))",
        all.join(", ")
    );
    let marker_sql = format!(
        "insert into %s ({RING_COLUMNS}) \
         values ($1, $2, '{SCHEMA_CHANGED_OP}', $3, null, $4, $3, $5, null)"
    );

    // The static select, with the column-dependent parts as `format()`
    // arguments (see [`Render::Dynamic`]). Every `%` the select holds, such
    // as `format('%s', …)`'s, is doubled first.
    let template = format!(
        "insert into {} ({RING_COLUMNS}) {}",
        placeholder(1),
        ring_select(spec, event, Render::Dynamic, true)
    )
    .replace('\n', " ")
    .replace('%', "%%");
    let template = (1..=7).fold(template, |t, n| {
        t.replace(&placeholder(n), &format!("%{n}$s"))
    });

    // `alias`'s fragment per column, `render`ed, joined by `sep` over the
    // columns the table still has.
    let fragments =
        |alias: &str, render: fn(&str, &str) -> String, columns: &[String], sep: &str| {
            let rows: Vec<String> = columns
                .iter()
                .enumerate()
                .map(|(i, c)| format!("({i}, {}, {})", text_expr(c), text_expr(&render(alias, c))))
                .collect();
            format!(
                "(select pg_catalog.string_agg(f.frag, {} order by f.i) \
             from (values {}) as f(i, c, frag) where f.c = any (have))",
                text_expr(sep),
                rows.join(", ")
            )
        };
    // Only the aliases the event reads (its transition tables and the live
    // table `t`) have arguments; the others are `null`, which `format()`
    // never reads.
    let reads = |alias: &str| match event {
        CaptureEvent::Insert => alias != "o",
        CaptureEvent::Delete => alias != "n",
        CaptureEvent::Update => true,
        CaptureEvent::Truncate | CaptureEvent::Begin => false,
    };
    let image_arg = |alias: &str| {
        if !reads(alias) {
            return "null".to_string();
        }
        fragments(alias, image_pair, &spec.columns, " || ")
    };
    let group_arg = |alias: &str| {
        if !reads(alias) || spec.group_key.is_empty() {
            return "null".to_string();
        }
        format!(
            "'array[' || coalesce({}, '') || ']::text[]'",
            fragments(alias, value_text, &spec.group_key, ", ")
        )
    };
    format!(
        "    if present <> {count} then\n        \
         -- #622 C6: an imaged column was renamed or dropped.\n        \
         have := array{names};\n        \
         execute pg_catalog.format({marker_sql}, {ring})\n            \
         using {src}, {marker_key}, l, {marker}, ts;\n        \
         if not ({key_present}) then\n            return null;\n        end if;\n        \
         execute pg_catalog.format({template},\n                {ring},\n                \
         {img_o},\n                {img_n},\n                {img_t},\n                \
         {gk_o},\n                {gk_n},\n                {gk_t})\n            \
         using l, ts;\n        \
         return null;\n    end if;\n",
        count = spec.columns.len(),
        names = column_probe("a.attname::text", &spec.columns),
        marker_sql = text_expr(&marker_sql),
        src = text_expr(&spec.table),
        marker_key = text_expr(SCHEMA_CHANGED_SENTINEL_KEY),
        template = text_expr(&template),
        img_o = image_arg("o"),
        img_n = image_arg("n"),
        img_t = image_arg("t"),
        gk_o = group_arg("o"),
        gk_n = group_arg("n"),
        gk_t = group_arg("t"),
    )
}

/// A token [`Render::Dynamic`] leaves where [`schema_changed_branch`] puts
/// `format()` argument `n`. A NUL can't occur in an identifier or in the
/// generated SQL, so it can't collide.
fn placeholder(n: usize) -> String {
    format!("\u{0}{n}\u{0}")
}

/// `alias`'s `column` as a one-pair image: [`image_expr`]'s pair, alone.
fn image_pair(alias: &str, column: &str) -> String {
    format!(
        "jsonb_build_object({}, {})",
        text_expr(column),
        value_text(alias, column)
    )
}

/// How [`ring_select`] renders the parts that depend on which columns the
/// table has, and the statement's `lsn` and change time.
#[derive(Clone, Copy)]
enum Render {
    /// The static inserts: every spec column named, and `l` and `ts` the
    /// function's variables.
    Static,
    /// [`schema_changed_branch`]'s template: [`placeholder`]s for the images
    /// of `o`, `n` and `t` (2, 3, 4) and their group-key arrays (5, 6, 7),
    /// and `$1`/`$2` for `l` and `ts`. It has no skip-no-op filter, so an
    /// update in the branch stages every row it changed.
    Dynamic,
}

/// The aliases [`ring_select`] reads: the old transition table, the new one,
/// and the live table re-read by primary key.
const ALIASES: [&str; 3] = ["o", "n", "t"];

fn alias_index(alias: &str) -> usize {
    ALIASES
        .iter()
        .position(|a| *a == alias)
        .expect("ring_select reads only o, n and t")
}

impl Render {
    fn lsn(self) -> &'static str {
        match self {
            Render::Static => "l",
            Render::Dynamic => "$1",
        }
    }

    fn ts(self) -> &'static str {
        match self {
            Render::Static => "ts",
            Render::Dynamic => "$2",
        }
    }

    fn image(self, spec: &CaptureSpec, alias: &str) -> String {
        match self {
            Render::Static => image_expr(spec, alias),
            Render::Dynamic => placeholder(2 + alias_index(alias)),
        }
    }

    fn group_array(self, spec: &CaptureSpec, alias: &str) -> String {
        match self {
            Render::Static => group_key_array(spec, alias),
            Render::Dynamic => placeholder(5 + alias_index(alias)),
        }
    }
}

/// The rows a capture insert writes: a `SELECT` over the event's transition
/// tables (a `VALUES` row for truncate), in [`RING_COLUMNS`] order.
///
/// # The image is the live row (#623 D8a, D's Q8)
///
/// With `live` (see "The live re-read is conditional" on [`function_body`]
/// for when), every row a statement wrote is re-read from the live table `t`
/// by primary key, and its `new_image` is that row, not the transition
/// table's. Without it, [`transition_select`] images the transition tables. An
/// application `AFTER ROW` trigger that rewrites the row its own statement
/// wrote runs its nested statement, and that statement's capture, before
/// this statement's capture; the transition table still holds the outer
/// statement's version, so imaging it would put the older values at the
/// higher `lsn` (#680). The live row is the one the transaction ends the
/// statement with. If it is gone, a nested write deleted or re-keyed it, and
/// the row staged is a delete of the key, imaging the transition row as its
/// `old_image`. A delete's key is re-read too: a nested write that put the
/// key back stages it as an update to the live row.
///
/// The re-read never sees another transaction's change. Each row it joins
/// was written (or deleted) by this statement, so this transaction holds its
/// row lock, or for a new key its unique-index entry, until it commits, and
/// the key's live version, if any, is one this transaction wrote. Two more
/// conditions keep the probe on that version:
///
/// - **It was written by this transaction**: `age(t.xmin) <= 0`. Under
///   `REPEATABLE READ` and `SERIALIZABLE` the snapshot predates the
///   statement, so a version of the key that another transaction deleted or
///   re-keyed after the snapshot, and that this statement then re-created,
///   is still visible beside this transaction's own (#623 D8a review). `age`
///   measures against the transaction's own xid, or the next xid as of its
///   first call in the transaction, so it is `<= 0` for every version this
///   transaction or its subtransactions wrote, and positive for one
///   committed before the snapshot.
/// - **Its key renders the same**: the typed `=` finds the version through
///   the index, but the ring keys by the key's text, and a type's `=` can be
///   looser (`numeric` `1.0 = 1.00`). Without the text check, a key move
///   from `1.0` to `1.00` would find the new row for the old key and never
///   stage the old key's delete.
///
/// `tests/capture_reread.rs` checks this at every isolation level.
///
/// The old image and the `group_key` are unchanged (old ∪ new, plus the live
/// row's): the relationship readers still need the old join keys until E
/// (#624).
///
/// # An update that changed nothing read stages nothing
///
/// A paired update row is dropped when every imaged column is the same
/// before and after, compared by `record_image_ne` over the typed values: a
/// binary comparison that works for every type, including those with no
/// equality operator (`json`, `point`, `xml`), and that is never looser than
/// the images' text, since a value's output text is a function of its
/// binary form under the pinned settings. A statement trigger can't take a
/// `WHEN` clause when it has transition tables, so the filter is here.
fn ring_select(spec: &CaptureSpec, event: CaptureEvent, render: Render, live: bool) -> String {
    let src = text_expr(&spec.table);
    let (l, ts) = (render.lsn(), render.ts());
    let indent = "        ";
    if !live {
        return transition_select(spec, event, render);
    }
    let table = quoted_table(&spec.table).expect("CaptureSpec::new checked the table name");
    let first_key = quote_ident(&spec.key[0]);
    let live = format!("t.{first_key} is not null");
    // `t`, read by the primary key's typed equality. The `limit 1` keeps the
    // lateral subquery from being flattened into a join, so the plan is a
    // probe per captured row whatever the table's size was when PL/pgSQL
    // planned the statement (it plans once per session); see
    // [`PLANNER_SETTINGS`] for the probe's index scan. `of` is the typed
    // key's source per key column.
    // `key` is the row's own ring key, which the live version's must match
    // as text; `age(t.xmin) <= 0` keeps the probe on a version this
    // transaction wrote (see "The image is the live row" above).
    let live_join = |of: &dyn Fn(&str) -> String, key: &str| {
        let on: Vec<String> = spec
            .key
            .iter()
            .map(|c| format!("t.{} = {}", quote_ident(c), of(&quote_ident(c))))
            .collect();
        format!(
            "{indent}left join lateral (select * from {table} t where {}\n{indent}    \
             and {} = {key} collate \"C\" and pg_catalog.age(t.xmin) <= 0 limit 1) t on true",
            on.join(" and "),
            key_expr(spec, "t"),
        )
    };
    match event {
        CaptureEvent::Insert => format!(
            "{indent}select {src}, case when {live} then {} else {} end,\n{indent}    \
             case when {live} then 'insert' else 'delete' end,\n{indent}    \
             {l}, case when not ({live}) then {} end, case when {live} then {} end, \
             {l}, {ts}, {}\n{indent}from {NEW_ROWS} n\n{}",
            key_expr(spec, "t"),
            key_expr(spec, "n"),
            render.image(spec, "n"),
            render.image(spec, "t"),
            group_key_expr(spec, &["n", "t"], render),
            live_join(&|c| format!("n.{c}"), &key_expr(spec, "n")),
        ),
        CaptureEvent::Delete => format!(
            "{indent}select {src}, case when {live} then {} else {} end,\n{indent}    \
             case when {live} then 'update' else 'delete' end,\n{indent}    \
             {l}, {}, case when {live} then {} end, {l}, {ts}, {}\n{indent}from {OLD_ROWS} o\n{}",
            key_expr(spec, "t"),
            key_expr(spec, "o"),
            render.image(spec, "o"),
            render.image(spec, "t"),
            group_key_expr(spec, &["o", "t"], render),
            live_join(&|c| format!("o.{c}"), &key_expr(spec, "o")),
        ),
        // A statement trigger sees the update's old and new rows as two sets
        // with no pairing, so they are paired on the key text. A row whose
        // key changed has no partner: it becomes a delete of its old key and
        // an insert of its new one. Pairing on the rendered key rather than
        // the key columns' `=` keeps the pairing on the same identity the
        // ring keys by, and `collate "C"` makes that a byte comparison.
        CaptureEvent::Update => {
            let old = format!("o.{first_key} is not null");
            let new = format!("n.{first_key} is not null");
            let changed = update_changed_filter(spec, render);
            format!(
                "{indent}select {src},\n{indent}    \
                 case when {live} then {} when {new} then {} else {} end,\n{indent}    \
                 case when not ({live}) then 'delete' when not ({old}) then 'insert' \
                 else 'update' end,\n{indent}    \
                 {l}, case when {old} then {} when not ({live}) then {} end,\n{indent}    \
                 case when {live} then {} end, {l}, {ts}, {}\n{indent}from {OLD_ROWS} o\n\
                 {indent}full join {NEW_ROWS} n on {} = {} collate \"C\"\n{}{changed}",
                key_expr(spec, "t"),
                key_expr(spec, "n"),
                key_expr(spec, "o"),
                render.image(spec, "o"),
                render.image(spec, "n"),
                render.image(spec, "t"),
                group_key_expr(spec, &["o", "n", "t"], render),
                key_expr(spec, "o"),
                key_expr(spec, "n"),
                live_join(
                    &|c| format!("coalesce(n.{c}, o.{c})"),
                    &format!(
                        "case when {new} then {} else {} end",
                        key_expr(spec, "n"),
                        key_expr(spec, "o")
                    ),
                ),
            )
        }
        CaptureEvent::Truncate | CaptureEvent::Begin => transition_select(spec, event, render),
    }
}

/// [`ring_select`] without the live re-read: every row imaged from its
/// transition table, which is the live row when no other write to the table
/// ran in the statement's span (see "The live re-read is conditional" on
/// [`function_body`]). The update's pairing and skip-no-op filter are the
/// same as the re-read's.
fn transition_select(spec: &CaptureSpec, event: CaptureEvent, render: Render) -> String {
    let src = text_expr(&spec.table);
    let (l, ts) = (render.lsn(), render.ts());
    let indent = "        ";
    let first_key = quote_ident(&spec.key[0]);
    match event {
        CaptureEvent::Insert => format!(
            "{indent}select {src}, {}, 'insert', {l}, null, {}, {l}, {ts}, {}\n{indent}from {NEW_ROWS} n",
            key_expr(spec, "n"),
            render.image(spec, "n"),
            group_key_expr(spec, &["n"], render),
        ),
        CaptureEvent::Delete => format!(
            "{indent}select {src}, {}, 'delete', {l}, {}, null, {l}, {ts}, {}\n{indent}from {OLD_ROWS} o",
            key_expr(spec, "o"),
            render.image(spec, "o"),
            group_key_expr(spec, &["o"], render),
        ),
        CaptureEvent::Update => {
            let old = format!("o.{first_key} is not null");
            let new = format!("n.{first_key} is not null");
            format!(
                "{indent}select {src}, case when {new} then {} else {} end,\n{indent}    \
                 case when not ({new}) then 'delete' when not ({old}) then 'insert' \
                 else 'update' end,\n{indent}    \
                 {l}, case when {old} then {} end, case when {new} then {} end, {l}, {ts}, {}\n\
                 {indent}from {OLD_ROWS} o\n\
                 {indent}full join {NEW_ROWS} n on {} = {} collate \"C\"{}",
                key_expr(spec, "n"),
                key_expr(spec, "o"),
                render.image(spec, "o"),
                render.image(spec, "n"),
                group_key_expr(spec, &["o", "n"], render),
                key_expr(spec, "o"),
                key_expr(spec, "n"),
                update_changed_filter(spec, render),
            )
        }
        CaptureEvent::Truncate => format!(
            "{indent}values ({src}, {}, 'truncate', {l}, null, null, {l}, {ts}, null)",
            text_expr(TRUNCATE_SENTINEL_KEY)
        ),
        CaptureEvent::Begin => unreachable!("the begin function writes no ring row"),
    }
}

/// The update's skip-no-op filter (see "An update that changed nothing read
/// stages nothing" on [`ring_select`]): keep a row that is unpaired or whose
/// imaged columns' binary images differ. [`Render::Dynamic`] has none.
fn update_changed_filter(spec: &CaptureSpec, render: Render) -> String {
    let indent = "        ";
    let first_key = quote_ident(&spec.key[0]);
    match render {
        Render::Static => {
            let row = |alias: &str| {
                let cols: Vec<String> = spec
                    .columns
                    .iter()
                    .map(|c| format!("{alias}.{}", quote_ident(c)))
                    .collect();
                format!("row({})", cols.join(", "))
            };
            format!(
                "\n{indent}where not (o.{first_key} is not null) or not (n.{first_key} is not null)\n{indent}    \
                 or pg_catalog.record_image_ne({}, {})",
                row("o"),
                row("n")
            )
        }
        Render::Dynamic => String::new(),
    }
}

/// `alias`'s ring key. A single-column key is the column's text verbatim; a
/// composite key joins its parts, each with #200's separator escape, on
/// U+001F in declared order ([`crate::defs::ddl::join_pk_key`]). Primary-key
/// columns are never `NULL`, so #110's null encoding never applies.
fn key_expr(spec: &CaptureSpec, alias: &str) -> String {
    let parts: Vec<String> = spec
        .key
        .iter()
        .map(|c| format!("format('%s', {alias}.{})", quote_ident(c)))
        .collect();
    match parts.as_slice() {
        [single] => single.clone(),
        _ => format!(
            "array_to_string(array[{}], chr(31))",
            parts
                .iter()
                .map(|p| composite_key_escape_sql(p))
                .collect::<Vec<_>>()
                .join(", ")
        ),
    }
}

/// One column's value as an image carries it: the output function's text,
/// or `NULL`.
///
/// The null test is `num_nulls`, not `IS NULL`. On a composite value `IS
/// NULL` is also true when every field is null, so `ROW(NULL, NULL)` would be
/// imaged as `NULL` where its output function prints `(,)`. `num_nulls` asks
/// only whether the value itself is null, for every type.
fn value_text(alias: &str, column: &str) -> String {
    let col = format!("{alias}.{}", quote_ident(column));
    format!("case when num_nulls({col}) = 1 then null else format('%s', {col}) end")
}

/// `alias`'s image: a JSON object of every image column's text.
fn image_expr(spec: &CaptureSpec, alias: &str) -> String {
    spec.columns
        .chunks(MAX_PAIRS_PER_BUILD)
        .map(|chunk| {
            let pairs: Vec<String> = chunk
                .iter()
                .map(|c| format!("{}, {}", text_expr(c), value_text(alias, c)))
                .collect();
            format!("jsonb_build_object({})", pairs.join(", "))
        })
        .collect::<Vec<_>>()
        .join(" || ")
}

/// `alias`'s group-key column values, `NULL`s included, in the spec's
/// (physical) order.
fn group_key_array(spec: &CaptureSpec, alias: &str) -> String {
    let values: Vec<String> = spec
        .group_key
        .iter()
        .map(|c| value_text(alias, c))
        .collect();
    format!("array[{}]::text[]", values.join(", "))
}

/// The ring's `group_key` for rows drawn from `aliases` (old, new, live):
/// the union of every non-null group-key value, in first-seen order, or
/// `NULL` when there are none.
fn group_key_expr(spec: &CaptureSpec, aliases: &[&str], render: Render) -> String {
    if spec.group_key.is_empty() {
        return "null::text[]".to_string();
    }
    let arrays: Vec<String> = aliases
        .iter()
        .map(|a| render.group_array(spec, a))
        .collect();
    distinct_non_null(&arrays.join(" || "))
}

/// The distinct non-null elements of `array`, in first-seen order; `NULL`
/// when there are none (`array_agg` over no rows).
fn distinct_non_null(array: &str) -> String {
    format!(
        "(select array_agg(g.v order by g.i) from (select u.v, min(u.i) as i \
         from unnest({array}) with ordinality as u(v, i) where u.v is not null \
         group by u.v) g)"
    )
}

/// `s` as a SQL text expression that means the same under any
/// `standard_conforming_strings`: a quoted literal with `'` doubled, and a
/// control character or backslash spliced in as `chr(n)`.
fn text_expr(s: &str) -> String {
    let mut parts: Vec<String> = Vec::new();
    let mut literal = String::new();
    for c in s.chars() {
        if c.is_control() || c == '\\' {
            if !literal.is_empty() {
                parts.push(format!("'{}'", std::mem::take(&mut literal)));
            }
            parts.push(format!("chr({})", c as u32));
        } else if c == '\'' {
            literal.push_str("''");
        } else {
            literal.push(c);
        }
    }
    if !literal.is_empty() || parts.is_empty() {
        parts.push(format!("'{literal}'"));
    }
    match parts.as_slice() {
        [one] => one.clone(),
        _ => format!("({})", parts.join(" || ")),
    }
}

/// `s` as a string constant (where SQL wants a literal, not an expression,
/// such as `COMMENT … IS`). An `E''` literal when `s` holds a backslash, so
/// the meaning doesn't depend on `standard_conforming_strings`.
fn string_constant(s: &str) -> String {
    let quoted = s.replace('\'', "''");
    if s.contains('\\') {
        format!("E'{}'", quoted.replace('\\', "\\\\"))
    } else {
        format!("'{quoted}'")
    }
}

/// A dollar-quote tag that doesn't occur in `body`.
fn dollar_tag(body: &str) -> String {
    let mut n = 0usize;
    loop {
        let tag = if n == 0 {
            "$trellis_capture$".to_string()
        } else {
            format!("$trellis_capture_{n}$")
        };
        if !body.contains(&tag) {
            return tag;
        }
        n += 1;
    }
}

fn split_table(table: &str) -> Result<(&str, &str), CaptureError> {
    match table.split_once('.') {
        Some((schema, name)) if !schema.is_empty() && !name.is_empty() && !name.contains('.') => {
            Ok((schema, name))
        }
        _ => Err(CaptureError::InvalidTableName(table.to_string())),
    }
}

pub(crate) fn quoted_table(table: &str) -> Result<String, CaptureError> {
    let (schema, name) = split_table(table)?;
    Ok(format!("{}.{}", quote_ident(schema), quote_ident(name)))
}

/// The longest prefix of `s` of at most `max` bytes that ends on a character
/// boundary.
fn truncate_to(s: &str, max: usize) -> &str {
    if s.len() <= max {
        return s;
    }
    let mut end = max;
    while !s.is_char_boundary(end) {
        end -= 1;
    }
    &s[..end]
}

/// 64-bit FNV-1a: a stable hash for generated names, with no dependency.
fn fnv1a64(bytes: &[u8]) -> u64 {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash
}

#[cfg(test)]
mod tests {
    use super::*;

    fn spec() -> CaptureSpec {
        CaptureSpec::new(
            "public.orders",
            vec!["id".to_string()],
            ["amount".to_string(), "customer_id".to_string()],
            vec!["customer_id".to_string()],
        )
        .unwrap()
    }

    fn composite() -> CaptureSpec {
        CaptureSpec::new(
            "shop.post_tags",
            vec!["post".to_string(), "tag".to_string()],
            ["weight".to_string()],
            vec![],
        )
        .unwrap()
    }

    fn ddl(spec: &CaptureSpec, event: CaptureEvent) -> String {
        function_ddl("trellis", spec, event).unwrap()
    }

    #[test]
    fn replacing_the_functions_touches_no_trigger_and_keeps_the_revoke() {
        let statements = function_statements("trellis", &spec()).unwrap();
        assert_eq!(statements.len(), 15);
        assert!(
            statements
                .iter()
                .all(|s| !s.starts_with("create or replace trigger")
                    && !s.starts_with("alter table"))
        );
        for event in CaptureEvent::ALL {
            let function = function_name("public.orders", event).unwrap();
            let revoke = statements
                .iter()
                .position(|s| s.starts_with("revoke") && s.contains(&function))
                .expect("a revoke per function");
            assert!(statements[revoke - 1].starts_with("create or replace function"));
            assert!(statements[revoke - 1].contains(&function));
        }
    }

    #[test]
    fn the_source_is_what_the_function_ddl_quotes() {
        for event in CaptureEvent::ALL {
            let ddl = ddl(&spec(), event);
            let source = function_source("trellis", &spec(), event);
            let tag = "$trellis_capture$";
            let quoted = &ddl[ddl.find(tag).unwrap() + tag.len()..ddl.rfind(tag).unwrap()];
            assert_eq!(quoted, source);
        }
    }

    #[test]
    fn an_uninstall_drops_triggers_before_functions_and_skips_them_for_a_dropped_table() {
        let statements = uninstall_statements("trellis", "public.orders", true).unwrap();
        assert_eq!(statements.len(), 10);
        assert!(statements[..5].iter().all(|s| {
            s.starts_with("drop trigger if exists \"trellis_capture_")
                && s.ends_with(" on \"public\".\"orders\"")
        }));
        assert!(
            statements[5..]
                .iter()
                .all(|s| s.starts_with("drop function if exists \"trellis\".\"cap_"))
        );
        let gone = uninstall_statements("trellis", "public.orders", false).unwrap();
        assert_eq!(gone, statements[5..]);
    }

    #[test]
    fn ownership_goes_to_the_named_role_quoted() {
        let statements = owner_statements("trellis", "public.orders", "Migrator").unwrap();
        assert_eq!(statements.len(), 5);
        assert!(
            statements
                .iter()
                .all(|s| s.starts_with("alter function \"trellis\".\"cap_")
                    && s.ends_with("() owner to \"Migrator\""))
        );
    }

    #[test]
    fn every_function_is_security_definer_with_a_pinned_search_path_and_output_settings() {
        for event in CaptureEvent::ALL {
            let sql = ddl(&spec(), event);
            assert!(sql.contains("security definer"), "{sql}");
            assert!(
                sql.contains("set search_path = pg_catalog, pg_temp"),
                "{sql}"
            );
            if event == CaptureEvent::Begin {
                // It renders nothing, so it saves no output settings.
                assert!(!sql.contains("set datestyle"), "{sql}");
                continue;
            }
            for clause in [
                "set datestyle to 'ISO, YMD'",
                "set bytea_output to 'hex'",
                "set extra_float_digits to 1",
                "set intervalstyle to 'postgres'",
                "set timezone to 'UTC'",
            ] {
                assert!(sql.contains(clause), "missing {clause:?} in\n{sql}");
            }
        }
    }

    #[test]
    fn the_live_reread_plans_as_an_index_probe_from_the_first_call() {
        for event in CaptureEvent::CAPTURED {
            assert!(
                ddl(&spec(), event).contains("\n    set enable_seqscan to 'off'\nas "),
                "{event:?}"
            );
        }
    }

    #[test]
    fn the_set_clauses_come_from_the_pools_pinned_settings() {
        let clauses = pinned_output_settings();
        // At least the five pinned before #672; the exact list grows with the pool's, and every
        // clause is checked below, so the count must not pin a merge order with #672.
        assert!(clauses.len() >= 5, "{clauses:?}");
        let sql = ddl(&spec(), CaptureEvent::Insert);
        for clause in clauses {
            assert!(sql.contains(clause), "missing {clause:?}");
        }
    }

    #[test]
    fn every_ring_and_mirror_reference_is_schema_qualified() {
        for event in CaptureEvent::CAPTURED {
            let sql = ddl(&spec(), event);
            for slot in 0..RING_SIZE {
                assert!(
                    sql.contains(&format!("insert into \"trellis\".\"seg_{slot}\" (")),
                    "slot {slot} unqualified in\n{sql}"
                );
            }
            // The schema-changed branch's two dynamic inserts name the ring
            // as `%s`/`%1$s`, the quoted schema plus the slot's table.
            let dynamic = if event == CaptureEvent::Truncate {
                0
            } else {
                2
            };
            // A re-read and a transition-only insert per slot (#623 D8a).
            let per_slot = if event == CaptureEvent::Truncate {
                1
            } else {
                2
            };
            assert_eq!(
                sql.matches("insert into ").count(),
                per_slot * RING_SIZE as usize + dynamic,
                "every ring insert qualified:\n{sql}"
            );
            if dynamic > 0 {
                assert_eq!(
                    sql.matches("'\"trellis\"' || '.' || pg_catalog.quote_ident('seg_' || slot)")
                        .count(),
                    2,
                    "{sql}"
                );
            }
            assert!(
                sql.contains(
                    "pg_sequence_last_value(('\"trellis\".\"ring_slot_mirror\"')::regclass)"
                ),
                "{sql}"
            );
            assert!(!sql.contains("segment_pointer"), "{sql}");
        }
    }

    #[test]
    fn the_mirror_is_read_inside_the_xid_case() {
        let sql = ddl(&spec(), CaptureEvent::Update);
        let case = sql
            .find("case when pg_current_xact_id() is not null")
            .expect("xid case");
        let read = sql.find("pg_sequence_last_value(").expect("mirror read");
        let end = sql[case..].find(" end;").expect("case end") + case;
        assert!(case < read && read < end, "{sql}");
    }

    #[test]
    fn a_statement_that_changed_nothing_returns_before_taking_an_xid() {
        for (event, rows) in [
            (CaptureEvent::Insert, NEW_ROWS),
            (CaptureEvent::Update, NEW_ROWS),
            (CaptureEvent::Delete, OLD_ROWS),
        ] {
            let sql = ddl(&spec(), event);
            let guard = sql
                .find(&format!(
                    "present := case when exists (select 1 from {rows})\n        then "
                ))
                .unwrap_or_else(|| panic!("no empty-statement guard in\n{sql}"));
            let exit = sql
                .find("    if present < 0 then\n        return null;\n    end if;\n")
                .expect("the guard returns");
            assert!(guard < exit && exit < sql.find("pg_current_xact_id").unwrap());
        }
        assert!(!ddl(&spec(), CaptureEvent::Truncate).contains("present"));
    }

    /// #622 C6: the guard's query counts the imaged columns the table still
    /// has, and a miss takes the schema-changed branch before any static
    /// insert: the marker first, then (only with the key intact) the images
    /// over the columns left, by `EXECUTE`, with no `EXCEPTION` block.
    #[test]
    fn a_missing_column_takes_the_marker_branch_before_the_static_inserts() {
        for (event, rows) in [
            (CaptureEvent::Insert, NEW_ROWS),
            (CaptureEvent::Update, NEW_ROWS),
            (CaptureEvent::Delete, OLD_ROWS),
        ] {
            let sql = ddl(&spec(), event);
            let at = |needle: &str| {
                sql.find(needle)
                    .unwrap_or_else(|| panic!("no {needle:?} in\n{sql}"))
            };
            let probe = at(&format!(
                "present := case when exists (select 1 from {rows})\n        \
                 then (select count(*) from pg_catalog.pg_attribute a\n            \
                 where a.attrelid = tg_relid and a.attnum > 0 and not a.attisdropped\n              \
                 and a.attname = any (array['amount', 'customer_id', 'id']::name[])) \
                 else -1 end;"
            ));
            let miss = at("    if present <> 3 then\n");
            let marker = at("''schema_changed''");
            let key_check =
                at("if not (array['id']::text[] <@ have) then\n            return null;");
            let images = at("execute pg_catalog.format('insert into %1$s (");
            let statics = at("    case slot\n");
            assert!(
                probe < miss
                    && miss < marker
                    && marker < key_check
                    && key_check < images
                    && images < statics,
                "{sql}"
            );
            assert!(at("pg_current_xact_id") < miss, "the xid comes first");
            assert_eq!(
                sql.matches("exception").count(),
                sql.matches("raise exception").count(),
                "no EXCEPTION block, so no subtransaction:\n{sql}"
            );
            // The dynamic template keeps the static select's `format('%s', …)`
            // as a literal `%%s` for `format()` to pass through.
            assert!(sql.contains("format(''%%s'', "), "{sql}");
            assert!(!sql[images..statics].contains(", l, ts"), "{sql}");
        }
    }

    /// #623 D8a: each slot has a re-read insert and a transition-only one,
    /// chosen by `reread`, which is read from the span state before this
    /// call counts itself; the transition insert reads no relation.
    #[test]
    fn the_reread_is_gated_on_the_span_state() {
        let span = span_setting("trellis", "public.orders");
        assert!(span.starts_with("trellis_capture.s_"), "{span}");
        assert_ne!(span, span_setting("other", "public.orders"));
        assert_ne!(span, span_setting("trellis", "public.order"));
        for event in [
            CaptureEvent::Insert,
            CaptureEvent::Update,
            CaptureEvent::Delete,
        ] {
            let sql = ddl(&spec(), event);
            let at = |needle: &str| {
                sql.find(needle)
                    .unwrap_or_else(|| panic!("no {needle:?} in\n{sql}"))
            };
            let read = at("reread := st[3] <> st[1];");
            let counted = at("st[1] := st[1] + 1;");
            let saved = at(&format!(
                "perform pg_catalog.set_config('{span}', cast(st as text), true);"
            ));
            assert!(read < counted && counted < saved && saved < at("if present < 0 then"));
            for slot in 0..RING_SIZE {
                let arm = &sql[at(&format!("    when {slot} then\n"))..];
                let reread = arm.find("        if reread then\n").expect("the gate");
                let otherwise = arm.find("        else\n").expect("the transition insert");
                let end = arm.find("        end if;\n").expect("end if");
                assert!(
                    arm[reread..otherwise].contains("left join lateral"),
                    "{sql}"
                );
                assert!(!arm[otherwise..end].contains("lateral"), "{sql}");
                assert!(
                    !arm[otherwise..end].contains("\"public\".\"orders\""),
                    "{sql}"
                );
            }
        }
        let begin = ddl(&spec(), CaptureEvent::Begin);
        assert!(
            begin.contains(&format!("pg_catalog.current_setting('{span}', true)")),
            "{begin}"
        );
        assert!(begin.contains("st := array[st[1], 1, st[1]];"), "{begin}");
        assert!(!begin.contains("insert into"), "{begin}");
        assert!(!ddl(&spec(), CaptureEvent::Truncate).contains("reread"));

        // An update also re-reads when a key repeats among its old rows (an
        // FK action's merged update), checked after the schema-changed branch.
        let update = ddl(&spec(), CaptureEvent::Update);
        let repeated = update
            .find("reread := exists (select 1 from trellis_old o")
            .expect("the repeated-key check");
        assert!(update.find("-- #622 C6: an imaged column").expect("branch") < repeated);
        assert!(repeated < update.find("    case slot\n").expect("case"));
        assert!(
            update[repeated..].contains("having count(*) > 1"),
            "{update}"
        );
        for event in [CaptureEvent::Insert, CaptureEvent::Delete] {
            assert!(
                !ddl(&spec(), event).contains("having count(*)"),
                "{event:?}"
            );
        }
    }

    #[test]
    fn the_body_resolves_a_name_clash_with_a_column_to_the_variable() {
        for event in CaptureEvent::CAPTURED {
            let sql = ddl(&spec(), event);
            let directive = sql
                .find("\n#variable_conflict use_variable\ndeclare\n")
                .unwrap_or_else(|| panic!("no #variable_conflict directive in\n{sql}"));
            assert!(directive < sql.find("begin\n").unwrap(), "{sql}");
        }
    }

    #[test]
    fn lsn_and_origin_lsn_are_the_insert_lsn_and_src_changed_is_clock_time() {
        let sql = ddl(&spec(), CaptureEvent::Insert);
        assert!(sql.contains("l := pg_current_wal_insert_lsn();"), "{sql}");
        assert!(sql.contains("ts := clock_timestamp();"), "{sql}");
        assert!(
            sql.contains(&format!(
                "insert into \"trellis\".\"seg_0\" ({RING_COLUMNS})"
            )),
            "{sql}"
        );
        assert!(
            sql.contains("then 'insert' else 'delete' end,\n            l, case when "),
            "{sql}"
        );
        assert!(sql.contains(", l, ts, "), "{sql}");
    }

    #[test]
    fn values_render_through_format_with_null_kept_null() {
        let sql = ddl(&spec(), CaptureEvent::Insert);
        assert!(
            sql.contains(
                "'amount', case when num_nulls(n.\"amount\") = 1 then null \
                 else format('%s', n.\"amount\") end"
            ),
            "{sql}"
        );
        assert!(
            !sql.contains(" is null then null"),
            "`IS NULL` is true for a composite whose fields are all null:\n{sql}"
        );
        assert!(
            !sql.contains("::text,"),
            "no ::text rendering of a value:\n{sql}"
        );
    }

    #[test]
    fn a_single_column_key_is_the_columns_text_verbatim() {
        let sql = ddl(&spec(), CaptureEvent::Insert);
        assert!(
            sql.contains(
                "select 'public.orders', case when t.\"id\" is not null \
                 then format('%s', t.\"id\") else format('%s', n.\"id\") end,"
            ),
            "{sql}"
        );
    }

    #[test]
    fn a_composite_key_joins_escaped_parts_in_declared_order() {
        let sql = ddl(&composite(), CaptureEvent::Delete);
        let expected = format!(
            "array_to_string(array[{}, {}], chr(31))",
            composite_key_escape_sql("format('%s', o.\"post\")"),
            composite_key_escape_sql("format('%s', o.\"tag\")"),
        );
        assert!(sql.contains(&expected), "{sql}");
    }

    #[test]
    fn an_update_full_joins_old_and_new_on_the_key() {
        let sql = ddl(&spec(), CaptureEvent::Update);
        assert!(sql.contains("from trellis_old o\n"), "{sql}");
        assert!(
            sql.contains(
                "full join trellis_new n on format('%s', o.\"id\") = format('%s', n.\"id\") \
                 collate \"C\""
            ),
            "{sql}"
        );
        assert!(
            sql.contains(
                "case when not (t.\"id\" is not null) then 'delete' \
                 when not (o.\"id\" is not null) then 'insert' else 'update' end"
            ),
            "a key move is a delete plus an insert, and a key gone from the live \
             table is a delete:\n{sql}"
        );
    }

    /// #623 D8a: every event re-reads the rows it wrote from the live table
    /// by the primary key's typed equality, keeps only a version this
    /// transaction wrote whose key renders as the row's own, and images the
    /// live row.
    #[test]
    fn every_event_images_the_live_row_read_by_primary_key() {
        for (event, on, key) in [
            (
                CaptureEvent::Insert,
                "t.\"id\" = n.\"id\"",
                "format('%s', n.\"id\")",
            ),
            (
                CaptureEvent::Delete,
                "t.\"id\" = o.\"id\"",
                "format('%s', o.\"id\")",
            ),
            (
                CaptureEvent::Update,
                "t.\"id\" = coalesce(n.\"id\", o.\"id\")",
                "case when n.\"id\" is not null then format('%s', n.\"id\") \
                 else format('%s', o.\"id\") end",
            ),
        ] {
            let sql = ddl(&spec(), event);
            assert!(
                sql.contains(&format!(
                    "left join lateral (select * from \"public\".\"orders\" t \
                     where {on}\n            and format('%s', t.\"id\") = {key} collate \"C\" \
                     and pg_catalog.age(t.xmin) <= 0 limit 1) t on true"
                )),
                "a probe per row, never a join the planner could hash:\n{sql}"
            );
            assert!(
                sql.contains(
                    "case when t.\"id\" is not null then jsonb_build_object('amount', \
                     case when num_nulls(t.\"amount\") = 1"
                ),
                "the new image is the live row's:\n{sql}"
            );
        }
        let composite = ddl(&composite(), CaptureEvent::Insert);
        assert!(
            composite.contains("where t.\"post\" = n.\"post\" and t.\"tag\" = n.\"tag\"\n"),
            "{composite}"
        );
    }

    /// #623 D8a: an update row whose imaged columns are all unchanged,
    /// compared by binary image over the typed values, stages nothing. A key
    /// move always stages.
    #[test]
    fn an_update_that_changed_no_imaged_column_stages_nothing() {
        let sql = ddl(&spec(), CaptureEvent::Update);
        assert!(
            sql.contains(
                "where not (o.\"id\" is not null) or not (n.\"id\" is not null)\n            \
                 or pg_catalog.record_image_ne(row(o.\"amount\", o.\"customer_id\", o.\"id\"), \
                 row(n.\"amount\", n.\"customer_id\", n.\"id\"));"
            ),
            "{sql}"
        );
        assert!(!sql.contains(" is distinct from "), "{sql}");
    }

    #[test]
    fn the_group_key_is_the_distinct_union_of_old_new_and_live() {
        let update = ddl(&spec(), CaptureEvent::Update);
        assert!(
            update.contains(
                "unnest(array[case when num_nulls(o.\"customer_id\") = 1 then null \
                 else format('%s', o.\"customer_id\") end]::text[] || \
                 array[case when num_nulls(n.\"customer_id\") = 1"
            ),
            "{update}"
        );
        assert!(
            update.contains(
                "]::text[] || array[case when num_nulls(t.\"customer_id\") = 1 then null"
            ),
            "{update}"
        );
        assert!(
            update.contains("where u.v is not null group by u.v"),
            "{update}"
        );
        let insert = ddl(&spec(), CaptureEvent::Insert);
        assert!(
            insert.contains("unnest(array[case when num_nulls(n.\"customer_id\") = 1"),
            "{insert}"
        );
        let plain = ddl(&composite(), CaptureEvent::Update);
        assert!(plain.contains(", l, ts, null::text[]\n"), "{plain}");
    }

    #[test]
    fn truncate_stages_one_sentinel_row() {
        let sql = ddl(&spec(), CaptureEvent::Truncate);
        assert!(
            sql.contains(
                "values ('public.orders', (chr(31) || 'trellis-truncate-sentinel'), 'truncate', \
                 l, null, null, l, ts, null)"
            ),
            "{sql}"
        );
    }

    #[test]
    fn a_wide_image_is_split_across_build_calls() {
        let columns: Vec<String> = (0..120).map(|i| format!("c{i:03}")).collect();
        let wide =
            CaptureSpec::new("public.wide", vec!["c000".to_string()], columns, vec![]).unwrap();
        let image = image_expr(&wide, "n");
        assert_eq!(image.matches("jsonb_build_object(").count(), 3);
        assert_eq!(image.matches(" || jsonb_build_object(").count(), 2);
    }

    #[test]
    fn literals_never_depend_on_standard_conforming_strings() {
        assert_eq!(text_expr("plain"), "'plain'");
        assert_eq!(text_expr("it's"), "'it''s'");
        assert_eq!(text_expr(""), "''");
        assert_eq!(text_expr("a\\b"), "('a' || chr(92) || 'b')");
        assert_eq!(text_expr("\u{1}"), "chr(1)");
        assert_eq!(string_constant("a'b"), "'a''b'");
        assert_eq!(string_constant("a\\b"), "E'a\\\\b'");
    }

    #[test]
    fn identifiers_are_quoted() {
        let spec = CaptureSpec::new(
            "My Schema.Odd\"Table",
            vec!["Id".to_string()],
            ["select".to_string()],
            vec![],
        )
        .unwrap();
        let sql = ddl(&spec, CaptureEvent::Insert);
        assert!(sql.contains("n.\"select\""), "{sql}");
        assert!(sql.contains("format('%s', n.\"Id\")"), "{sql}");
        assert!(sql.contains("select 'My Schema.Odd\"Table',"), "{sql}");
        let trigger = trigger_ddl("trellis", &spec, CaptureEvent::Insert).unwrap();
        assert!(
            trigger[0].contains(" on \"My Schema\".\"Odd\"\"Table\" "),
            "{trigger:?}"
        );
    }

    #[test]
    fn the_dollar_tag_avoids_the_body() {
        assert_eq!(dollar_tag("begin end"), "$trellis_capture$");
        assert_eq!(
            dollar_tag("a \"$trellis_capture$\" b"),
            "$trellis_capture_1$"
        );
    }

    #[test]
    fn triggers_carry_transition_tables_and_are_enabled_always() {
        let spec = spec();
        let insert = trigger_ddl("trellis", &spec, CaptureEvent::Insert).unwrap();
        assert_eq!(
            insert[0],
            format!(
                "create or replace trigger \"trellis_capture_insert\" after insert on \
                 \"public\".\"orders\" referencing new table as trellis_new for each statement \
                 execute function \"trellis\".\"{}\"()",
                function_name("public.orders", CaptureEvent::Insert).unwrap()
            )
        );
        assert_eq!(
            insert[1],
            "alter table \"public\".\"orders\" enable always trigger \"trellis_capture_insert\""
        );
        let update = trigger_ddl("trellis", &spec, CaptureEvent::Update).unwrap();
        assert!(update[0].contains(
            "after update on \"public\".\"orders\" referencing old table as trellis_old \
             new table as trellis_new for each statement"
        ));
        let delete = trigger_ddl("trellis", &spec, CaptureEvent::Delete).unwrap();
        assert!(delete[0].contains("referencing old table as trellis_old for each statement"));
        let truncate = trigger_ddl("trellis", &spec, CaptureEvent::Truncate).unwrap();
        assert!(truncate[0].contains("after truncate on \"public\".\"orders\" for each statement"));
        let begin = trigger_ddl("trellis", &spec, CaptureEvent::Begin).unwrap();
        assert!(begin[0].contains(
            "\"trellis_capture_begin\" before insert or update or delete on \"public\".\"orders\" \
             for each statement"
        ));
        assert!(begin[1].ends_with("enable always trigger \"trellis_capture_begin\""));
        assert_eq!(install_statements("trellis", &spec).unwrap().len(), 25);
    }

    #[test]
    fn public_cannot_execute_a_capture_function() {
        let spec = spec();
        let statements = install_statements("trellis", &spec).unwrap();
        for event in CaptureEvent::ALL {
            let function = format!(
                "\"trellis\".\"{}\"()",
                function_name("public.orders", event).unwrap()
            );
            let create = statements
                .iter()
                .position(|s| s.starts_with(&format!("create or replace function {function}")))
                .unwrap_or_else(|| panic!("no create for {event:?}"));
            assert_eq!(
                statements[create + 1],
                format!("revoke all on function {function} from public"),
                "the revoke follows the create, in the same transaction"
            );
        }
    }

    #[test]
    fn names_fit_postgres_and_tell_tables_and_instances_apart() {
        let a = function_name("a_b.c", CaptureEvent::Insert).unwrap();
        let b = function_name("a.b_c", CaptureEvent::Insert).unwrap();
        assert_ne!(a, b);
        assert!(a.starts_with("cap_insert_c_"), "{a}");
        let long = format!("public.{}", "é".repeat(60));
        for event in CaptureEvent::ALL {
            let name = function_name(&long, event).unwrap();
            assert!(name.len() <= MAX_IDENT_BYTES, "{name}");
        }
        assert_eq!(
            trigger_name("trellis", CaptureEvent::Truncate),
            "trellis_capture_truncate"
        );
        assert_ne!(
            trigger_name("trellis", CaptureEvent::Insert),
            trigger_name("tenant_b", CaptureEvent::Insert)
        );
        let x = "x".repeat(70);
        let y = format!("{}y", "x".repeat(69));
        let (tx, ty) = (
            trigger_name(&x, CaptureEvent::Truncate),
            trigger_name(&y, CaptureEvent::Truncate),
        );
        assert!(tx.len() <= MAX_IDENT_BYTES && ty.len() <= MAX_IDENT_BYTES);
        assert_ne!(tx, ty);
    }

    #[test]
    fn the_comment_records_the_column_set() {
        let spec = spec();
        assert_eq!(
            comment_text(&spec, CaptureEvent::Update),
            "{\"trellis_capture\":1,\"table\":\"public.orders\",\"event\":\"update\",\
             \"key\":[\"id\"],\"columns\":[\"amount\",\"customer_id\",\"id\"],\
             \"group_key\":[\"customer_id\"]}"
        );
        let sql = comment_ddl("trellis", &spec, CaptureEvent::Update).unwrap();
        assert!(
            sql.starts_with("comment on function \"trellis\".\"cap_update_orders_"),
            "{sql}"
        );
    }

    #[test]
    fn a_spec_needs_a_key_a_qualified_table_and_imaged_group_key_columns() {
        assert!(matches!(
            CaptureSpec::new("public.t", vec![], ["a".to_string()], vec![]),
            Err(CaptureError::NoPrimaryKey { .. })
        ));
        assert!(matches!(
            CaptureSpec::new("t", vec!["id".to_string()], [], vec![]),
            Err(CaptureError::InvalidTableName(_))
        ));
        assert!(matches!(
            CaptureSpec::new(
                "public.t",
                vec!["id".to_string()],
                [],
                vec!["fk".to_string()]
            ),
            Err(CaptureError::MissingColumn { .. })
        ));
        let spec = CaptureSpec::new("public.t", vec!["id".to_string()], [], vec![]).unwrap();
        assert_eq!(spec.columns(), ["id".to_string()]);
    }
}
