//! Generates one table's capture functions and triggers (#622 C2).
//!
//! Each captured table gets one function per event (insert, update, delete,
//! truncate) and one `AFTER … FOR EACH STATEMENT` trigger per function. The
//! statement shape follows the #565 spike's `capture_sql.py`: one `INSERT …
//! SELECT` from the transition table into the active ring slot, chosen by a
//! `CASE` over the four static `seg_N` inserts so every arm keeps a cached
//! plan.
//!
//! # What every function does
//!
//! - **It runs as its owner.** Each function is `SECURITY DEFINER`, so the
//!   application's role needs no privilege on Trellis's schema. It is owned
//!   by whichever role creates it, which C3 makes the role that owns the
//!   Trellis schema (#622 plan Q3). A dedicated least-privilege capture role
//!   is deferred.
//! - **It doesn't trust the writer's session.** `search_path` is pinned to
//!   `pg_catalog, pg_temp`, and every ring and mirror reference is
//!   schema-qualified. The five output settings Trellis pins everywhere
//!   ([`crate::pool::DETERMINISTIC_TEXT_OUTPUT_GUCS`]) are `SET` clauses on
//!   the function, parsed from that constant so there is one list. Without
//!   them an image would depend on the application session's `DateStyle`,
//!   `TimeZone`, `bytea_output`, `IntervalStyle` and `extra_float_digits`
//!   (#565 E3). Literals in the body never contain a backslash, so
//!   `standard_conforming_strings` can't change their meaning either.
//! - **It renders a value the way intake does.** `format('%s', col)` calls
//!   the type's output function, which is what `pgoutput` sends and what
//!   `intake::tuple_to_json` writes. `::text` is not the same: for example,
//!   it strips `char(n)` padding. A `NULL` stays a JSON `null`, because
//!   `format('%s', NULL)` is an empty string.
//! - **It reads the active slot the way every ring writer must** (#597): from
//!   `ring_slot_mirror` with `pg_sequence_last_value`, in the same expression
//!   that assigns the writer's xid. See `staging::append::active_ring_slot`
//!   for why both halves matter at every isolation level.
//! - **It skips a statement that changed nothing.** An `UPDATE … WHERE false`
//!   still fires a statement trigger. Returning early keeps it from
//!   assigning an xid it doesn't need.
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
use crate::staging::append::{RING_SIZE, TRUNCATE_SENTINEL_KEY};

use super::CaptureError;

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
    /// spelling `intake::publication::qualify` builds and the ring's
    /// `src_table` holds.
    table: String,
    /// The primary key's columns, in the key's declared order
    /// (`ddl::pk_key_sql_expr`'s "declared-order convention").
    key: Vec<String>,
    /// Every column an image carries, sorted. Includes every key column.
    columns: Vec<String>,
    /// The columns whose values make up the ring's `group_key` (every
    /// outbound relationship's `from_col`, #133), in the table's physical
    /// column order. The order is load-bearing: `intake::touched_group_key`
    /// collects values in `pgoutput`'s column order, so the array's element
    /// order matches only if this does.
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

/// One of the four statement events a table is captured on.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CaptureEvent {
    Insert,
    Update,
    Delete,
    Truncate,
}

impl CaptureEvent {
    pub const ALL: [CaptureEvent; 4] = [
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
        }
    }

    /// The trigger's `REFERENCING` clause. A truncate trigger can't have
    /// transition tables.
    fn referencing(self) -> String {
        match self {
            CaptureEvent::Insert => format!(" referencing new table as {NEW_ROWS}"),
            CaptureEvent::Update => {
                format!(" referencing old table as {OLD_ROWS} new table as {NEW_ROWS}")
            }
            CaptureEvent::Delete => format!(" referencing old table as {OLD_ROWS}"),
            CaptureEvent::Truncate => String::new(),
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
    let settings: String = pinned_output_settings()
        .iter()
        .map(|clause| format!("\n    {clause}"))
        .collect();
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
    Ok(vec![
        format!(
            "create or replace trigger {trigger} after {} on {table}{} \
             for each statement execute function {function}()",
            event.as_str(),
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
    Ok(format!(
        "{}.{}",
        quote_ident(schema),
        quote_ident(&function_name(&spec.table, event)?)
    ))
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
fn function_body(schema: &str, spec: &CaptureSpec, event: CaptureEvent) -> String {
    let mirror = text_expr(&format!(
        "{}.{}",
        quote_ident(schema),
        quote_ident("ring_slot_mirror")
    ));
    let mut body = String::from(
        "#variable_conflict use_variable\n\
         declare\n    slot smallint;\n    l pg_lsn;\n    ts timestamptz;\nbegin\n",
    );
    match event {
        CaptureEvent::Insert | CaptureEvent::Update => body.push_str(&format!(
            "    if not exists (select 1 from {NEW_ROWS}) then\n        return null;\n    end if;\n"
        )),
        CaptureEvent::Delete => body.push_str(&format!(
            "    if not exists (select 1 from {OLD_ROWS}) then\n        return null;\n    end if;\n"
        )),
        CaptureEvent::Truncate => {}
    }
    body.push_str(&format!(
        "    -- #597: the xid is assigned in the same expression that reads the mirror.\n    \
         slot := case when pg_current_xact_id() is not null\n        \
         then pg_sequence_last_value(({mirror})::regclass)::smallint end;\n    \
         l := pg_current_wal_insert_lsn();\n    \
         ts := clock_timestamp();\n    \
         case slot\n"
    ));
    for slot in 0..RING_SIZE {
        let ring = format!(
            "{}.{}",
            quote_ident(schema),
            quote_ident(&format!("seg_{slot}"))
        );
        body.push_str(&format!(
            "    when {slot} then\n        insert into {ring} ({RING_COLUMNS})\n{};\n",
            ring_select(spec, event)
        ));
    }
    body.push_str(
        "    else\n        raise exception 'trellis capture: ring_slot_mirror returned %', slot;\n    \
         end case;\n    return null;\nend;\n",
    );
    body
}

/// The rows a capture insert writes: a `SELECT` over the event's transition
/// tables (a `VALUES` row for truncate), in [`RING_COLUMNS`] order.
fn ring_select(spec: &CaptureSpec, event: CaptureEvent) -> String {
    let src = text_expr(&spec.table);
    let indent = "        ";
    match event {
        CaptureEvent::Insert => format!(
            "{indent}select {src}, {}, 'insert', l, null, {}, l, ts, {}\n{indent}from {NEW_ROWS} n",
            key_expr(spec, "n"),
            image_expr(spec, "n"),
            group_key_expr(spec, &["n"]),
        ),
        CaptureEvent::Delete => format!(
            "{indent}select {src}, {}, 'delete', l, {}, null, l, ts, {}\n{indent}from {OLD_ROWS} o",
            key_expr(spec, "o"),
            image_expr(spec, "o"),
            group_key_expr(spec, &["o"]),
        ),
        // A statement trigger sees the update's old and new rows as two sets
        // with no pairing, so they are paired on the key text. A row whose
        // key changed has no partner: it becomes a delete of its old key and
        // an insert of its new one. Pairing on the rendered key rather than
        // the key columns' `=` keeps the pairing on the same identity the
        // ring keys by, and `collate "C"` makes that a byte comparison.
        CaptureEvent::Update => {
            let side = |alias: &str, rows: &str| {
                let group = if spec.group_key.is_empty() {
                    String::new()
                } else {
                    format!(", {} as gk", group_key_array(spec, alias))
                };
                format!(
                    "(select {} as k, {} as img{group} from {rows} {alias})",
                    key_expr(spec, alias),
                    image_expr(spec, alias),
                )
            };
            let group = if spec.group_key.is_empty() {
                "null::text[]".to_string()
            } else {
                distinct_non_null("o.gk || n.gk")
            };
            format!(
                "{indent}select {src}, coalesce(n.k, o.k),\n{indent}    \
                 case when n.k is null then 'delete' when o.k is null then 'insert' \
                 else 'update' end,\n{indent}    \
                 l, o.img, n.img, l, ts, {group}\n{indent}from {} o\n{indent}full join {} n \
                 on o.k = n.k collate \"C\"",
                side("o", OLD_ROWS),
                side("n", NEW_ROWS),
            )
        }
        CaptureEvent::Truncate => format!(
            "{indent}values ({src}, {}, 'truncate', l, null, null, l, ts, null)",
            text_expr(TRUNCATE_SENTINEL_KEY)
        ),
    }
}

/// `alias`'s ring key: `intake::extract_key`'s encoding. A single-column key
/// is the column's text verbatim; a composite key joins its parts, each with
/// #200's separator escape, on U+001F in declared order
/// ([`crate::defs::ddl::join_pk_key`]). Primary-key columns are never `NULL`,
/// so #110's null encoding never applies.
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

/// One column's value as intake renders it: the output function's text, or
/// `NULL`.
///
/// The null test is `num_nulls`, not `IS NULL`. On a composite value `IS
/// NULL` is also true when every field is null, so `ROW(NULL, NULL)` would be
/// imaged as `NULL` where `pgoutput` sends its text, `(,)`. `num_nulls` asks
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

/// The ring's `group_key` for rows drawn from `aliases` (old before new):
/// `intake::touched_group_key`'s union of every non-null group-key value, in
/// first-seen order, or `NULL` when there are none.
fn group_key_expr(spec: &CaptureSpec, aliases: &[&str]) -> String {
    if spec.group_key.is_empty() {
        return "null::text[]".to_string();
    }
    let arrays: Vec<String> = aliases.iter().map(|a| group_key_array(spec, a)).collect();
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

fn quoted_table(table: &str) -> Result<String, CaptureError> {
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
    fn every_function_is_security_definer_with_a_pinned_search_path_and_output_settings() {
        for event in CaptureEvent::ALL {
            let sql = ddl(&spec(), event);
            assert!(sql.contains("security definer"), "{sql}");
            assert!(
                sql.contains("set search_path = pg_catalog, pg_temp"),
                "{sql}"
            );
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
    fn the_set_clauses_come_from_the_pools_pinned_settings() {
        let clauses = pinned_output_settings();
        assert_eq!(clauses.len(), 5);
        let sql = ddl(&spec(), CaptureEvent::Insert);
        for clause in clauses {
            assert!(sql.contains(clause), "missing {clause:?}");
        }
    }

    #[test]
    fn every_ring_and_mirror_reference_is_schema_qualified() {
        for event in CaptureEvent::ALL {
            let sql = ddl(&spec(), event);
            for slot in 0..RING_SIZE {
                assert!(
                    sql.contains(&format!("insert into \"trellis\".\"seg_{slot}\" (")),
                    "slot {slot} unqualified in\n{sql}"
                );
            }
            assert_eq!(
                sql.matches("insert into ").count(),
                RING_SIZE as usize,
                "one insert per ring slot, all qualified:\n{sql}"
            );
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
                .find(&format!("if not exists (select 1 from {rows}) then"))
                .unwrap_or_else(|| panic!("no empty-statement guard in\n{sql}"));
            assert!(guard < sql.find("pg_current_xact_id").unwrap());
        }
        assert!(!ddl(&spec(), CaptureEvent::Truncate).contains("if not exists"));
    }

    #[test]
    fn the_body_resolves_a_name_clash_with_a_column_to_the_variable() {
        for event in CaptureEvent::ALL {
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
            sql.contains("'insert', l, null, jsonb_build_object("),
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
            sql.contains("select 'public.orders', format('%s', n.\"id\"), 'insert'"),
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
        assert!(
            sql.contains("(select format('%s', o.\"id\") as k, jsonb_build_object("),
            "{sql}"
        );
        assert!(sql.contains("from trellis_old o) o"), "{sql}");
        assert!(sql.contains("from trellis_new n) n"), "{sql}");
        assert!(sql.contains("full join"), "{sql}");
        assert!(sql.contains("on o.k = n.k collate \"C\""), "{sql}");
        assert!(
            sql.contains(
                "case when n.k is null then 'delete' when o.k is null then 'insert' \
                 else 'update' end"
            ),
            "a key move is a delete plus an insert:\n{sql}"
        );
        assert!(sql.contains("coalesce(n.k, o.k)"), "{sql}");
    }

    #[test]
    fn the_group_key_is_the_distinct_union_of_old_then_new() {
        let update = ddl(&spec(), CaptureEvent::Update);
        assert!(
            update.contains(", array[case when num_nulls(o.\"customer_id\") = 1"),
            "{update}"
        );
        assert!(
            update.contains("unnest(o.gk || n.gk) with ordinality"),
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
        assert!(
            plain.contains("l, o.img, n.img, l, ts, null::text[]"),
            "{plain}"
        );
        assert!(!plain.contains(" as gk"), "{plain}");
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
        assert_eq!(install_statements("trellis", &spec).unwrap().len(), 20);
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
