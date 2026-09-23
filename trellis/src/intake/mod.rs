//! Intake (issue #7, stage 01): the logical-replication consumer that turns
//! a committed source-database change into a durable row in the staging
//! ring, and only then tells the replication slot it may move. See
//! docs/staging-and-claiming/01-intake-and-lsn-confirmation.md.
//!
//! - [`pgoutput`] is the hand-rolled, pure-bytes `pgoutput` decoder.
//! - [`replica_identity`] is the checked REPLICA IDENTITY FULL requirement.
//! - [`spill`] bounds a transaction's memory footprint (issue #8):
//!   [`spill::TxnBuffer`] spills past a threshold and enforces the hard cap.
//! - [`publication`] is the slot/publication lifecycle (issue #8):
//!   reconciling the publication in place, the backfill marker and its
//!   transaction fence, the initial snapshot handshake, and the loud
//!   startup error on slot loss.
//! - [`stage_and_advance`] is the linchpin: stage + watermark + notify, in
//!   one transaction.
//! - [`Intake`] wires the linchpin to a live replication stream via
//!   `pgwire_replication`.
//!
//! Still deferred (issue #8's scope decision): the `pgoutput` v2 streaming
//! path. The design doc calls the spill file a stopgap whose structural fix
//! is the protocol's own streaming mode, staging chunks provisionally by xid
//! before commit. That needs its own side-table machinery and a settled
//! minimum-PostgreSQL-version floor, so it isn't built yet; [`spill::TxnBuffer`]
//! is the seam it would replace.

pub mod error;
pub mod pgoutput;
pub mod publication;
pub mod replica_identity;
pub mod slot_loss;
pub mod spill;

pub use error::IntakeError;
pub use replica_identity::{
    ResolvedPlan, SourceGuarantee, require_replica_identity_full, required_source_guarantees,
};

use std::time::{Duration, Instant, SystemTime};

use tokio_postgres::Transaction;
use tokio_postgres::types::PgLsn;

use pgoutput::{ColumnValue, Message, Relation, RelationCache};

use crate::defs::catalog;
use crate::pool::Pool;
#[cfg(any(test, feature = "internals"))]
use crate::staging::append;
use crate::staging::session::ProducerSession;
use crate::staging::{CdcOp, StagedChange, StagedWatermark};

/// The Postgres epoch (2000-01-01T00:00:00Z) as microseconds since the Unix
/// epoch — `pgoutput` timestamps count from 2000, not 1970. A bare constant
/// rather than a `chrono`/`time` dependency.
const PG_EPOCH_OFFSET_MICROS: i64 = 946_684_800_000_000;

/// Converts a `pgoutput` commit timestamp to a [`SystemTime`] for
/// [`StagedChange::Cdc::src_changed`]. Saturates to [`SystemTime::UNIX_EPOCH`]
/// on an implausible negative value rather than panicking — this is
/// provenance metadata, not something correctness depends on.
fn pg_commit_time_to_system_time(commit_time_micros: i64) -> SystemTime {
    let unix_micros = commit_time_micros.saturating_add(PG_EPOCH_OFFSET_MICROS);
    if unix_micros <= 0 {
        return SystemTime::UNIX_EPOCH;
    }
    SystemTime::UNIX_EPOCH + Duration::from_micros(unix_micros as u64)
}

/// The linchpin (docs/staging-and-claiming/01-intake-and-lsn-confirmation.md):
/// stage `changes` into the active ring segment, advance
/// `replication_progress`'s watermark for `slot` to `end_lsn` (monotonically —
/// a lower or equal value is a no-op), and `pg_notify` `wake_channel`, all as
/// statements inside `txn`. Callers commit `txn` themselves.
///
/// Takes an already-open [`Transaction`] rather than opening its own, like
/// [`append::append`]. That also lets the durability tests drive the crash
/// points directly against this function: begin a transaction, call this,
/// then commit it, drop it uncommitted, or force it to fail.
///
/// Does **not** advance the in-memory position or send a Standby Status
/// Update — that is the caller's job, and only after this function's effects
/// are durably committed (see [`Intake::commit_transaction`]).
#[cfg(any(test, feature = "internals"))]
pub async fn stage_and_advance(
    txn: &Transaction<'_>,
    changes: &[StagedChange],
    slot: &str,
    wake_channel: &str,
    end_lsn: PgLsn,
) -> Result<(), IntakeError> {
    append::append(txn, changes).await?;
    advance_watermark_and_notify(txn, slot, wake_channel, end_lsn).await
}

/// The watermark-advance-and-notify half of the linchpin, factored out so
/// [`spill::TxnBuffer::stage_and_advance`] can run it once after appending
/// possibly-many chunks, instead of duplicating this SQL.
pub(crate) async fn advance_watermark_and_notify(
    txn: &Transaction<'_>,
    slot: &str,
    wake_channel: &str,
    end_lsn: PgLsn,
) -> Result<(), IntakeError> {
    // Monotonic guard: a replay of an already-confirmed end_lsn re-stages
    // duplicate rows (accepted — the fold collapses them) but must not
    // regress the watermark.
    txn.execute(
        "update replication_progress set confirmed_lsn = $1 \
         where slot_name = $2 and confirmed_lsn < $1",
        &[&end_lsn, &slot],
    )
    .await?;
    // Transactional: a listener only ever wakes to already-visible staged
    // rows, because this NOTIFY commits atomically with them.
    txn.execute("select pg_notify($1, '')", &[&wake_channel])
        .await?;
    Ok(())
}

/// Reads `slot`'s durable watermark, if any — a missing row means either
/// [`publication::initial_snapshot_handshake`] never ran for `slot`, which
/// [`Intake::connect`] rejects as [`IntakeError::MissingProgressRow`] (issue
/// #31, finding 2), or (once a row exists) that the row is present but the
/// slot itself has since gone missing or invalid, which
/// [`publication::require_slot_healthy`] treats distinctly.
async fn fetch_confirmed_lsn(
    client: &impl tokio_postgres::GenericClient,
    slot: &str,
) -> Result<Option<PgLsn>, IntakeError> {
    let row = client
        .query_opt(
            "select confirmed_lsn from replication_progress where slot_name = $1",
            &[&slot],
        )
        .await?;
    Ok(row.map(|r| r.get(0)))
}

/// Builds the [`StagedChange::Cdc::key`] string from `relation`'s key
/// columns, joined with the ASCII unit separator — the
/// same collision-free delimiter the ring's `route` column uses (see
/// `V3__staging_ring.sql`), so a key containing a comma can't collide with
/// another row's. Joined through [`crate::defs::ddl::join_pk_key`] rather
/// than a local `join`, so this producer and the SQL-side producer
/// (`ddl::pk_key_sql_expr`) provably share one separator.
///
/// `primary_key` is the source table's actual primary-key column names, **in
/// the primary key's own declared order**, looked up from `pg_catalog` (see
/// [`primary_key_columns`]), and is `Some` for any relation whose replica
/// identity makes its primary key the authoritative row identity — `REPLICA
/// IDENTITY FULL` (issue #56: `pgoutput` sets the `is_key` flag on *every*
/// column there, so `col.is_key` can't be trusted to mean "part of the key")
/// and `REPLICA IDENTITY DEFAULT` (where the replica identity simply *is* the
/// primary key, so the flags agree with it but carry no ordering
/// information). When `Some`, the parts are emitted in `primary_key`'s order,
/// making this agree with [`crate::defs::ddl::pk_key_sql_expr`]'s
/// declared-order convention — see that function's "declared-order
/// convention" section, and issue #163: this used to walk `relation.columns`
/// (`pgoutput`'s *physical* column order) filtered to key membership, which
/// silently produced a differently-ordered key than the one the consuming
/// side (`ddl::split_pk_key`, via `staging::apply::read_live_rows_batch`)
/// decodes with for any table whose `PRIMARY KEY (...)` clause happens to
/// list its columns in a different order than they're physically declared
/// (`create table t (tag text, post int, primary key (post, tag))`).
///
/// `None` — `REPLICA IDENTITY USING INDEX` or `NOTHING` — keeps the original
/// `is_key`-flag, physical-order behavior, deliberately: under `USING INDEX`
/// the flagged columns are that *index's*, not necessarily the primary key's,
/// so the primary key is not the right ordering to normalize onto, and
/// `pgoutput` exposes no ordering for the identity index itself. So the
/// physical-vs-declared divergence issue #163 describes survives, in
/// principle, for a multi-column `USING INDEX` identity listed out of
/// physical order (`NOTHING` stages no key at all — it fails the "at least
/// one key column" check below). A strictly narrower residue than what #163
/// found, since the two replica identities this crate's front door actually
/// *requires* for the tables whose composite keys get decoded again
/// downstream (an aggregate source, a relationship's projected endpoints)
/// are both FULL — but not nothing. Closing it means introspecting
/// `pg_index.indisreplident`'s own `indkey` order here rather than the
/// primary key's, which no reachable path needs today.
///
/// `None` also covers a DEFAULT-identity relation whose primary key the
/// catalog can no longer resolve — a table dropped since the change was
/// written to the WAL, say — where the flags remain the only surviving
/// description of its key; see `handle_xlog_data`'s `Relation` arm.
///
/// Issue #110's NULL-safe key encoding deliberately does **not** apply here:
/// every part is its column's raw text. This function can never produce a
/// `NULL` component in the first place — `key_value` below already rejects
/// anything but [`ColumnValue::Text`] with [`IntakeError::MissingKeyValue`],
/// and the key columns it reads are a real `PRIMARY KEY`'s
/// ([`primary_key_columns`] filters on `pg_index.indisprimary`, so Postgres
/// itself guarantees them `NOT NULL`) or a replica identity's, likewise
/// never `NULL`. The SQL-side producer (`ddl::pk_key_sql_expr`) renders
/// those same not-null columns raw for exactly that reason
/// ([`crate::defs::ddl::PrimaryKeyColumn::nullable`] is `false` for them),
/// so the two agree byte-for-byte.
///
/// Routing this side through [`crate::defs::ddl::encode_key_part`] instead
/// would not merely be redundant, it would corrupt data: for a 1-1
/// definition the staged key text is bound *directly* in as the target
/// table's own literal primary-key value by `staging::apply::apply_target`,
/// while `defs::backfill`, `staging::quarantine::recompute_column` and
/// `defs::oracle::recompute` all write/read that same column from the raw
/// source value — so a key column whose text genuinely contains a U+0001
/// would land under the escaped (doubled) text on the incremental path and
/// the raw text on every other one. See `ddl::encode_key_part`'s
/// "`PrimaryKeyColumn::nullable` selects the encoding" section. Only a
/// genuinely nullable key — an aggregate target's `GROUP BY` columns, which
/// never reach this function — takes the encoded form
/// (`staging::apply_aggregate::derive_group_key`).
///
/// Issue #200's separator escape, by contrast, *does* apply here, and comes
/// for free: [`crate::defs::ddl::join_pk_key`] applies it to a multi-part
/// key, exactly as `ddl::pk_key_sql_expr` applies its SQL twin
/// (`ddl::composite_key_escape_sql`) to a multi-column one, so a key column
/// whose value genuinely contains U+001F/U+001E still decodes at the right
/// arity. A single-column key stays verbatim on both sides — see
/// `ddl::push_escaped_composite_key_part`'s "why arity 1 is exempt", which
/// turns on the same write-path argument the U+0001 discussion above makes.
fn extract_key(
    relation: &Relation,
    tuple: &[ColumnValue],
    primary_key: Option<&[String]>,
) -> Result<String, IntakeError> {
    let missing_key = || IntakeError::MissingKeyValue {
        table: relation.name.clone(),
    };
    let key_value = |i: usize| match tuple.get(i) {
        Some(ColumnValue::Text(t)) => Ok(t.as_str()),
        _ => Err(missing_key()),
    };

    let mut parts = Vec::new();
    match primary_key {
        // Issue #163: `primary_key`'s order, not `relation.columns`' —
        // a column the publication doesn't carry at all is treated exactly
        // like a missing value below (there's no identity to build without
        // it), the same outcome the old membership filter reached by simply
        // never finding it.
        Some(pk) => {
            for name in pk {
                let i = relation
                    .columns
                    .iter()
                    .position(|col| &col.name == name)
                    .ok_or_else(missing_key)?;
                parts.push(key_value(i)?);
            }
        }
        None => {
            for (i, col) in relation.columns.iter().enumerate() {
                if !col.is_key {
                    continue;
                }
                parts.push(key_value(i)?);
            }
        }
    }
    if parts.is_empty() {
        return Err(missing_key());
    }
    Ok(crate::defs::ddl::join_pk_key(parts))
}

/// Looks up `namespace.name`'s actual primary-key column names from
/// `pg_catalog`, in the primary key's own **declared** column order
/// (`array_position(i.indkey, a.attnum)`, matching
/// `defs::ddl::source_primary_key` exactly — see
/// `defs::ddl::pk_key_sql_expr`'s "declared-order convention" section) — the
/// source of
/// truth [`extract_key`] falls back to under `REPLICA IDENTITY FULL`, where
/// `pgoutput`'s per-column `is_key` flag is set on every column and so can't
/// tell the key columns apart from the rest (issue #56), and under `REPLICA
/// IDENTITY DEFAULT`, where it supplies the ordering `pgoutput`'s
/// physical-order columns don't (issue #163). Unlike
/// `defs::ddl::source_primary_key`, this supports a composite primary key —
/// intake's key derivation only ever needs the column *names*, never a
/// single column's type — and returns an empty `Vec` rather than an error
/// for a table with no primary key at all, since a FULL-identity table
/// without one is still decodable; it will simply fail
/// [`extract_key`]'s "at least one key column" check downstream, with the
/// same [`IntakeError::MissingKeyValue`] a truly keyless DEFAULT-identity
/// table would.
async fn primary_key_columns(
    client: &tokio_postgres::Client,
    namespace: &str,
    name: &str,
) -> Result<Vec<String>, IntakeError> {
    let qualified = format!(
        "{}.{}",
        crate::pool::quote_ident(namespace),
        crate::pool::quote_ident(name)
    );
    let rows = client
        .query(
            "select a.attname::text \
             from pg_index i \
             join pg_attribute a \
               on a.attrelid = i.indrelid and a.attnum = any(i.indkey) \
             where i.indrelid = pg_catalog.to_regclass($1) and i.indisprimary \
             order by array_position(i.indkey, a.attnum)",
            &[&qualified],
        )
        .await?;
    Ok(rows.into_iter().map(|row| row.get(0)).collect())
}

/// Renders one tuple as a JSON object `{"column": value}` — hand-built text,
/// no JSON library dependency (see `staging::append`). A
/// [`ColumnValue::Unchanged`] column (an untouched TOASTed value `pgoutput`
/// didn't resend) is *omitted* entirely, not written as `null`; the apply
/// half re-reads current source state for it.
fn tuple_to_json(relation: &Relation, tuple: &[ColumnValue]) -> String {
    let mut fields = Vec::with_capacity(tuple.len());
    for (name, value) in pgoutput::named_columns(relation, tuple) {
        match value {
            ColumnValue::Null => fields.push(format!("{}:null", json_string(name))),
            ColumnValue::Text(text) => {
                fields.push(format!("{}:{}", json_string(name), json_string(text)))
            }
            ColumnValue::Unchanged => {}
        }
    }
    format!("{{{}}}", fields.join(","))
}

/// A minimal, dependency-free JSON string-literal encoder (quote, backslash,
/// control characters) — just enough for column names and `pgoutput`'s
/// text-format column values, not a general serializer.
pub(crate) fn json_string(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if (c as u32) < 0x20 => out.push_str(&format!("\\u{:04x}", c as u32)),
            c => out.push(c),
        }
    }
    out.push('"');
    out
}

/// Builds one CDC [`StagedChange`] from a decoded `pgoutput` DML message.
/// `lsn`/`src_changed` are left unset here — they are the transaction's
/// commit position/time, not known until the `Commit` message arrives — and
/// are stamped onto every buffered change by [`Intake::commit_transaction`].
///
/// `group_key_cols` (issue #133) is `relation`'s outbound-relationship
/// `from_col` names — [`Intake::handle_xlog_data`]'s
/// [`GroupKeyColumns`]-cached lookup, passed in rather than looked up here
/// because this function stays pure/synchronous (no `pool`, no `.await`):
/// see [`touched_group_key`] for what it does with them.
fn cdc_change(
    relation: &Relation,
    op: CdcOp,
    old: Option<&[ColumnValue]>,
    new: Option<&[ColumnValue]>,
    primary_key: Option<&[String]>,
    group_key_cols: &[String],
) -> Result<StagedChange, IntakeError> {
    let key_tuple = new.or(old).ok_or_else(|| IntakeError::MissingKeyValue {
        table: relation.name.clone(),
    })?;
    let key = extract_key(relation, key_tuple, primary_key)?;
    let src_table = publication::qualify(&relation.namespace, &relation.name)?;
    let group_key = touched_group_key(relation, old, new, group_key_cols);
    Ok(StagedChange::Cdc {
        src_table,
        key,
        op,
        lsn: None,
        old_image: old.map(|t| tuple_to_json(relation, t)),
        new_image: new.map(|t| tuple_to_json(relation, t)),
        origin_lsn: None,
        src_changed: None,
        hop_gen: 0,
        group_key,
    })
}

/// Issue #133: the union of `group_key_cols`' values across `old` and `new`
/// — the real, pre-fold "which join-key values did this row's own change
/// touch" signal [`StagedChange::Cdc::group_key`] carries into the ring (see
/// that field's doc comment for the merge rule it feeds and why it has to
/// come from here, not from the folded image). Reads straight off the
/// still-typed `ColumnValue` tuples, exactly where they're already in scope
/// — no JSON re-parse of the encoded images needed. `None` when
/// `group_key_cols` is empty (this table isn't any relationship's
/// from-side, the overwhelmingly common case) or neither tuple carries a
/// non-null, non-unchanged value for any of them.
fn touched_group_key(
    relation: &Relation,
    old: Option<&[ColumnValue]>,
    new: Option<&[ColumnValue]>,
    group_key_cols: &[String],
) -> Option<Vec<String>> {
    if group_key_cols.is_empty() {
        return None;
    }
    let mut values: Vec<String> = Vec::new();
    for tuple in [old, new].into_iter().flatten() {
        for (name, value) in pgoutput::named_columns(relation, tuple) {
            if let ColumnValue::Text(text) = value
                && group_key_cols.iter().any(|col| col == name)
                && !values.iter().any(|v| v == text)
            {
                values.push(text.clone());
            }
        }
    }
    if values.is_empty() {
        None
    } else {
        Some(values)
    }
}

/// Stamps every buffered change with its transaction's commit position/time
/// — not known until the `Commit` message arrives, so this runs at staging
/// time rather than at buffering time. `pub(crate)` so
/// [`spill::TxnBuffer::stage_and_advance`] can apply it per chunk during
/// replay, identically to the non-spilled tail.
pub(crate) fn stamp_commit_metadata(
    changes: &mut [StagedChange],
    lsn: PgLsn,
    changed_at: SystemTime,
) {
    for change in changes {
        match change {
            StagedChange::Cdc {
                lsn: row_lsn,
                src_changed,
                ..
            }
            | StagedChange::Truncate {
                lsn: row_lsn,
                src_changed,
                ..
            } => {
                *row_lsn = Some(lsn);
                *src_changed = Some(changed_at);
            }
            StagedChange::Recompute { .. } => {}
            // Issue #134: never produced by intake — only Phase 3
            // (`staging::apply::apply_and_mark_drained_many`) stages this
            // variant, directly through an open `Transaction`, never
            // through this buffered/spilled intake path. `lsn`/`src_changed`
            // are set explicitly by that call site instead of here.
            StagedChange::RelationshipReverseDeferred { .. } => {}
        }
    }
}

/// Connection parameters for [`Intake::connect`]: the replication transport
/// (`pgwire_replication`) plus the DSN [`ProducerSession`] opens its own
/// dedicated connection to for the linchpin transaction.
#[derive(Debug, Clone)]
pub struct IntakeConfig {
    /// The DSN for the linchpin/staging connection, opened via
    /// [`ProducerSession`] (which enforces `synchronous_commit = on` and
    /// the producer singleton lock).
    pub dsn: String,
    /// The Trellis schema `search_path` is pinned to.
    pub schema: String,
    /// Unix socket directory or TCP host for the *replication* connection.
    /// A value starting with `/` is treated as a socket directory, matching
    /// libpq convention (see `pgwire_replication::ReplicationConfig::unix`).
    pub host: String,
    pub port: u16,
    pub user: String,
    pub password: String,
    pub database: String,
    /// The (already-created — see this module's doc comment) logical
    /// replication slot to stream from.
    pub slot: String,
    /// The (already-created) publication to subscribe to.
    pub publication: String,
    /// The channel the linchpin's `pg_notify` wakes.
    pub wake_channel: String,
    /// Past this many buffered changes, one transaction's head spills to a
    /// temp file (issue #8). See [`spill::DEFAULT_SPILL_THRESHOLD`].
    pub spill_threshold: usize,
    /// The hard cap on one transaction's total buffered changes (issue #8).
    /// See [`spill::DEFAULT_HARD_CAP`].
    pub hard_cap: usize,
    /// Issue #274 (epic #269): batches several source transactions into one
    /// ring transaction instead of one ring transaction per source commit.
    /// `None` is the un-grouped escape hatch — every source commit opens,
    /// uses, and commits its own ring transaction exactly as intake did
    /// before this field existed. See [`GroupCommitConfig`]'s own doc
    /// comment for the shipped (`Some`) default and why it is the default.
    pub group_commit: Option<GroupCommitConfig>,
}

/// Issue #274 (epic #269) "intake group-commit": bounds for batching several
/// source transactions' changes into one ring transaction instead of one
/// ring transaction per source commit. A batch flushes (commits) once either
/// bound is crossed — `max_rows` buffered across the open batch, or
/// `max_delay` elapsed since the batch's first transaction was buffered —
/// whichever comes first. The row bound protects memory/lock-hold duration
/// under a burst of many small transactions; the delay bound keeps a lone
/// transaction (or the tail of a burst) from waiting indefinitely for enough
/// siblings to fill the row bound, especially at low source commit rates.
///
/// This is the shipped default (see [`Default`] below and
/// [`crate::ClientOptions::group_commit`]'s own doc comment) — #266's B4
/// found the un-grouped one-ring-transaction-per-source-commit path walls at
/// ~17k rows/sec when the source commits one row at a time, which is the
/// shape closest to real application traffic. `None` (on [`IntakeConfig`]/
/// [`crate::ClientOptions`]) is kept only as an explicit escape hatch, not
/// the default.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GroupCommitConfig {
    pub max_rows: usize,
    pub max_delay: Duration,
}

impl Default for GroupCommitConfig {
    /// The issue's own suggested starting point: "~1000 rows / 5 ms bound".
    fn default() -> Self {
        Self {
            max_rows: 1000,
            max_delay: Duration::from_millis(5),
        }
    }
}

impl IntakeConfig {
    /// Builds the transport config for the replication connection —
    /// including, as of issue #246, the same output-GUC pins the pool
    /// applies at [`crate::pool::session_bootstrap`]. Before
    /// `pgwire-replication` 0.4.1 the walsender had no way to receive a
    /// startup `options` parameter at all, so `DateStyle`/`bytea_output`/
    /// `extra_float_digits`/`IntervalStyle` (and, now, `TimeZone`) could
    /// silently render CDC-decoded text differently from the pool's on any
    /// server whose GUCs had been customized away from stock defaults — see
    /// [`crate::pool::DETERMINISTIC_TEXT_OUTPUT_GUCS`]'s doc comment for the
    /// full history. `with_options` sends exactly the same startup `options`
    /// parameter `libpq`'s `options`/`PGOPTIONS` would on a normal
    /// connection, which PostgreSQL honors on a replication connection too.
    fn replication_config(&self) -> pgwire_replication::ReplicationConfig {
        let options = crate::pool::deterministic_text_output_options();
        if self.host.starts_with('/') {
            pgwire_replication::ReplicationConfig::unix(
                self.host.clone(),
                self.port,
                self.user.clone(),
                self.password.clone(),
                self.database.clone(),
                self.slot.clone(),
                self.publication.clone(),
            )
            .with_options(options)
        } else {
            pgwire_replication::ReplicationConfig::new(
                self.host.clone(),
                self.user.clone(),
                self.password.clone(),
                self.database.clone(),
                self.slot.clone(),
                self.publication.clone(),
            )
            .with_port(self.port)
            .with_options(options)
        }
    }
}

/// Issue #133's `src_table -> from_col` cache: the outbound-relationship
/// column names [`touched_group_key`] needs, so [`Intake::handle_xlog_data`]
/// never does a live catalog lookup per row. Keyed by the **bare** table
/// name (`relation.name`, no schema) — `relationship_definitions.from_table`
/// is itself persisted bare (`catalog::create_relationship`'s own doc
/// comment: "a relationship endpoint gaining its own persisted qualified
/// identity is... future work"), the same pre-ADR-0007 convention
/// `staging::apply::catalog_source_key` already works around for this exact
/// table. A schema-qualified lookup key here would silently never match and
/// leave `group_key` permanently unpopulated for every real relationship.
///
/// Refreshed lazily, per `src_table`, whenever an entry is missing or older
/// than [`GROUP_KEY_CACHE_TTL`] — a plain refresh-on-miss/refresh-on-stale
/// map, not a push-invalidation channel. That's a deliberate "keep it
/// simple" choice (matching this crate's existing periodic-refresh
/// conventions, e.g. `client::maintenance_loop`'s own due-time throttle,
/// rather than inventing a general cache-invalidation mechanism for one
/// column): a relationship created after intake starts becomes visible
/// within one TTL window, not a restart, which is the liveness bar this
/// needs to clear — sub-second pickup was never a requirement. Correctness
/// never depends on this cache being fresh, either: a stale or empty entry
/// only ever *under*-populates `group_key` (a from-side row whose real
/// outbound relationship this cache hasn't observed yet just gets `None`,
/// exactly like every producer wrote before #133), which costs guard (b) a
/// spurious pass in precisely the fold-erasure window #133 exists to close
/// — it can never cause guard (b), or any of the other three guards, to
/// reject something that should have applied.
struct GroupKeyColumns {
    pool: Pool,
    entries: std::collections::HashMap<String, (Vec<String>, Instant)>,
}

/// How long a [`GroupKeyColumns`] entry stays fresh before the next lookup
/// for that `src_table` re-queries the catalog — see that struct's own doc
/// comment for why a coarse, refresh-on-miss cache is the right amount of
/// machinery here.
const GROUP_KEY_CACHE_TTL: Duration = Duration::from_secs(30);

impl GroupKeyColumns {
    fn new(pool: Pool) -> Self {
        Self {
            pool,
            entries: std::collections::HashMap::new(),
        }
    }

    /// `src_table`'s outbound-relationship `from_col` names, sorted and
    /// deduped — empty (not an error) for a table with no outbound
    /// relationship at all, the overwhelmingly common case.
    async fn columns_for(&mut self, src_table: &str) -> Result<&[String], IntakeError> {
        let stale = match self.entries.get(src_table) {
            Some((_, refreshed_at)) => refreshed_at.elapsed() > GROUP_KEY_CACHE_TTL,
            None => true,
        };
        if stale {
            let rels = catalog::relationships_from_table(&self.pool, src_table).await?;
            let mut cols: Vec<String> = rels.into_iter().map(|r| r.def.from_col).collect();
            cols.sort();
            cols.dedup();
            self.entries
                .insert(src_table.to_string(), (cols, Instant::now()));
        }
        Ok(&self
            .entries
            .get(src_table)
            .expect("just inserted or already fresh above")
            .0)
    }
}

/// The consumer (issue #7's "single stager"): owns the replication
/// connection and the dedicated staging connection ([`ProducerSession`],
/// which enforces the producer singleton), decodes `pgoutput` bytes, buffers
/// a transaction's changes until its `Commit`, and runs [`stage_and_advance`].
pub struct Intake {
    replication: pgwire_replication::ReplicationClient,
    session: ProducerSession,
    slot: String,
    wake_channel: String,
    relations: RelationCache,
    /// The actual source-table primary-key column names, in the key's own
    /// declared order, for every `REPLICA IDENTITY FULL` or `DEFAULT`
    /// relation seen so far, keyed by `relation_id` — [`extract_key`]'s
    /// override for issue #56 (FULL: the `is_key` flags are useless there)
    /// and its declared-order normalization for issue #163 (DEFAULT: the
    /// flags are right but carry no ordering). Populated on each `Relation`
    /// message; still never consulted for `USING INDEX`/`NOTHING`, where the
    /// primary key is not the row identity at all, nor for a DEFAULT-identity
    /// relation whose primary key the current catalog can't resolve at all
    /// (see `handle_xlog_data` and [`extract_key`]'s own doc comment).
    primary_keys: std::collections::HashMap<i32, Vec<String>>,
    /// Issue #133: `src_table -> from_col` column names, refreshed
    /// lazily/on-miss — see [`GroupKeyColumns`]'s own doc comment.
    group_key_columns: GroupKeyColumns,
    buffer: spill::TxnBuffer,
    spill_threshold: usize,
    hard_cap: usize,
    /// The xid of the transaction currently being buffered, if any — used
    /// only to name the hard-cap error and the spill file.
    current_xid: Option<u32>,
    /// Set on `Begin`, cleared after a successful `Commit` — the guard a
    /// keepalive's watermark advance must never straddle (see
    /// [`Self::advance_watermark_on_keepalive`]).
    in_txn: bool,
    /// Issue #312: tables whose writes in the transaction currently being
    /// decoded were already propagated inside that transaction, as named by
    /// its [`PROPAGATED_TABLES_MESSAGE_PREFIX`] message. Their changes are
    /// dropped rather than buffered. Cleared on `Begin` and `Commit`.
    propagated_in_txn: std::collections::HashSet<String>,
    /// This instance's [`propagated_tables_prefix`]. A message under any
    /// other prefix, including another instance's, is ignored.
    propagated_prefix: String,
    /// The in-memory half of the watermark, advanced only after its durable
    /// write returns — mirrors the linchpin's own acknowledgment discipline
    /// for the keepalive path, which has no `pgwire_replication` metrics
    /// counter to read it back from.
    last_confirmed: PgLsn,
    /// Rate-limits the keepalive persist (it is itself a WAL-generating
    /// write) — see [`KEEPALIVE_PERSIST_INTERVAL`].
    last_keepalive_persist: Instant,
    /// Issue #132, epic #127, guard (a)'s own in-process "staged-through"
    /// watermark — distinct from `last_confirmed` above (this struct's
    /// mirror of the *durable*, throttled watermark). Advanced right after
    /// every successful [`stage_and_advance`] commit
    /// ([`Self::commit_transaction`]) and on every keepalive with **no**
    /// throttle at all ([`Self::advance_watermark_on_keepalive`]) — see
    /// [`StagedWatermark`]'s own doc comment for why this tracks the
    /// source's write frontier far more tightly than `last_confirmed`/
    /// `replication_progress.confirmed_lsn` do. Constructed by the caller
    /// (see [`Self::connect`]'s parameter) and shared, via `Clone`, with
    /// whatever runs guard (a)'s check on the apply/drain side — see
    /// `client.rs`'s own wiring.
    watermark: StagedWatermark,
    /// Issue #274: `None` means every source commit still opens, uses, and
    /// commits its own ring transaction exactly as before this field
    /// existed — see [`Self::commit_transaction`]'s branch.
    group_commit: Option<GroupCommitConfig>,
    /// The currently-open group's buffered transactions, each with its own
    /// `end_lsn`/`changed_at` (needed so [`spill::TxnBuffer::append_only`]
    /// stamps every row correctly) — only the *last* entry's `end_lsn` is
    /// used for the group's single watermark advance/ack when it flushes.
    /// Always empty when `group_commit` is `None`.
    pending: Vec<(spill::TxnBuffer, PgLsn, SystemTime)>,
    /// Sum of every buffered `TxnBuffer::len()` in `pending` — checked
    /// against `GroupCommitConfig::max_rows` on every push so the flush
    /// decision doesn't need to re-walk `pending` each time.
    pending_rows: usize,
    /// Set when `pending` goes from empty to non-empty; cleared (`None`)
    /// once the group flushes. [`Self::run`] races this against the next
    /// replication event so a partially-filled batch still flushes promptly
    /// even if no new source commit arrives to trigger it synchronously.
    batch_deadline: Option<Instant>,
}

/// Prefix of the transactional logical-decoding message an apply
/// transaction emits to name the tables whose writes it propagates
/// downstream itself (issue #312).
///
/// A chain's intermediate hop is a target Trellis writes and also a source a
/// downstream transform reads, so it sits in the publication. Each write to
/// it would then reach the ring twice: once as the `Recompute` (or captured
/// delete) that `staging::apply`'s step 4 stages inside the writing
/// transaction, and again as the CDC copy intake decodes afterward. A 1-1
/// reader absorbs that. An aggregate reader does not when the two land in
/// different batches: the in-transaction `Recompute` re-derives the group
/// from live state, which already holds the write, and the later CDC delta
/// adds it again.
///
/// The in-transaction copy is the one to keep. It commits with the write, so
/// it is never late, and it carries the upstream origin that read-your-writes
/// convergence depends on; the CDC copy's origin is the hop's own later
/// commit. So the applying transaction names the tables it propagated, and
/// intake drops that transaction's changes to exactly those tables. Changes
/// to any other table in the transaction, and writes to the same tables by
/// anything other than an apply (a direct backfill build, say), still stream
/// as usual.
///
/// The message is transactional, so it is decoded only if the apply commits,
/// and it is emitted before the apply's first target write, so intake sees it
/// before any change it covers.
///
/// A logical-decoding message reaches every slot in the database, not just
/// this instance's, so the full prefix ends with the emitting instance's
/// schema ([`propagated_tables_prefix`]). Another instance may read one of
/// this instance's hops as a source, and it has no in-transaction copy of
/// the write, so its intake must keep the CDC. The apply appends the schema
/// in SQL as `current_schema()`, which the pool's `search_path` pins to the
/// instance schema.
pub(crate) const PROPAGATED_TABLES_MESSAGE_PREFIX: &str = "trellis.propagated:";

/// The full [`PROPAGATED_TABLES_MESSAGE_PREFIX`] an apply in the instance
/// owning `schema` emits.
fn propagated_tables_prefix(schema: &str) -> String {
    format!("{PROPAGATED_TABLES_MESSAGE_PREFIX}{schema}")
}

/// Encodes qualified table names as the content of a
/// [`PROPAGATED_TABLES_MESSAGE_PREFIX`] message: NUL-separated, since NUL is
/// the one character a Postgres identifier cannot contain.
pub(crate) fn encode_propagated_tables<'a>(tables: impl IntoIterator<Item = &'a str>) -> Vec<u8> {
    let mut content = Vec::new();
    for (i, table) in tables.into_iter().enumerate() {
        if i > 0 {
            content.push(0);
        }
        content.extend_from_slice(table.as_bytes());
    }
    content
}

/// The inverse of [`encode_propagated_tables`]. Empty and non-UTF-8 segments
/// are skipped: neither can name a table [`publication::qualify`] produces.
fn decode_propagated_tables(content: &[u8]) -> impl Iterator<Item = String> + '_ {
    content
        .split(|b| *b == 0)
        .filter(|segment| !segment.is_empty())
        .filter_map(|segment| std::str::from_utf8(segment).ok().map(str::to_string))
}

/// Whether `relation`'s changes in the current transaction were already
/// propagated inside it — see [`PROPAGATED_TABLES_MESSAGE_PREFIX`].
fn already_propagated(
    propagated: &std::collections::HashSet<String>,
    relation: &Relation,
) -> Result<bool, IntakeError> {
    if propagated.is_empty() {
        return Ok(false);
    }
    Ok(propagated.contains(&publication::qualify(&relation.namespace, &relation.name)?))
}

/// How often a quiet stream's keepalive-driven watermark advance may persist
/// (issue #8's quiet-stream problem, guard (d)). The persist is itself a
/// WAL-generating write, so unthrottled it would loop on its own writes
/// against a server that echoes empty-transaction keepalives.
const KEEPALIVE_PERSIST_INTERVAL: Duration = Duration::from_secs(10);

impl Intake {
    /// Opens both connections: the producer session (enforcing its session
    /// guards) and the replication stream (issuing `START_REPLICATION`
    /// against the already-existing slot/publication named in `config`).
    ///
    /// Before opening the replication stream, checks that `config.slot` has
    /// a `replication_progress` row (issue #31, finding 2's precondition:
    /// [`publication::initial_snapshot_handshake`] seeds this row at slot
    /// creation, so a missing row here is never a legitimate fresh-slot
    /// state — it means that handshake never ran, and staging into a slot
    /// with no durable watermark lets WAL reclaim ahead of work that never
    /// persists, silently) and that the slot itself is still healthy — see
    /// [`publication::require_slot_healthy`].
    ///
    /// `watermark` (issue #132, epic #127, guard (a)) is constructed by the
    /// caller — before this call, per that issue's own wiring note — and
    /// shared (via `Clone`) with whatever runs guard (a)'s check on the
    /// apply/drain side; see `client.rs`. Seeded here to `last_confirmed`
    /// (the durably-persisted position this connection is about to resume
    /// replication from) rather than left at whatever the caller
    /// constructed it with: a fresh [`StagedWatermark::new`] starts at LSN
    /// 0, and without this seed a restart would make guard (a) reject every
    /// relationship reverse record until intake re-streamed all the way
    /// back past `last_confirmed` — safe (guard (a) fails closed) but
    /// needlessly conservative, since `last_confirmed` is already a
    /// durably-proven "staged at least this far" position. [`StagedWatermark::advance`]'s
    /// own monotonic guard makes this seed a no-op if the caller already
    /// passed in something at least this fresh (e.g. a shared watermark
    /// surviving an in-process `Intake` restart that never dropped the
    /// `Arc`).
    /// `pool` (issue #133) backs [`GroupKeyColumns`]'s catalog cache only —
    /// a `Pool` is cheap to `Clone` (`Arc`-backed) and distinct from
    /// `session`'s dedicated producer connection, so this never competes
    /// with (or is required for) the producer singleton lock
    /// [`ProducerSession`] enforces.
    pub async fn connect(
        config: &IntakeConfig,
        watermark: StagedWatermark,
        pool: Pool,
    ) -> Result<Self, IntakeError> {
        let session = ProducerSession::connect(&config.dsn, &config.schema).await?;
        let last_confirmed = fetch_confirmed_lsn(session.client(), &config.slot)
            .await?
            .ok_or_else(|| IntakeError::MissingProgressRow {
                slot: config.slot.clone(),
            })?;
        publication::require_slot_healthy(session.client(), &config.slot, last_confirmed).await?;
        // Improvement-plan task E3 (generative-suite-e-hard) uncovered this via a
        // simulated in-process client crash-and-restart: without an explicit
        // `start_lsn`, `pgwire_replication`'s default (`Lsn(0)`) resumes from
        // the *replication slot's own* server-tracked `confirmed_flush_lsn`,
        // which only advances when a Standby Status Update actually reaches
        // the server — an async, batched/periodic acknowledgment
        // (`Intake::commit_transaction`'s `update_applied_lsn` only updates
        // the in-memory value pgwire_replication reports "on its next" status
        // update, per that function's own doc comment). `replication_progress.confirmed_lsn`
        // (`last_confirmed`, fetched above) is strictly durable and at least as
        // fresh — it is persisted in the *same* database transaction as the
        // staged rows themselves (`stage_and_advance`/`advance_watermark_and_notify`).
        // A crash between "stage a transaction" and "the next Standby Status
        // Update actually reaching Postgres" therefore leaves the slot's own
        // position stale; a fresh connection that trusted it (the previous
        // behavior here) would have Postgres *redeliver* one or more
        // already-staged-and-fully-applied transactions. The ring's fold only
        // collapses such a duplicate when it lands before the original
        // segment seals (`intake::stage_and_advance`'s "Monotonic guard" doc
        // comment) — once draining has already applied it to a target, a
        // redelivered duplicate is a second, independent delta, which
        // silently double-counts an `Aggregate` target's `SUM`/`COUNT` (and,
        // more rarely, can regress a `OneToOne` target if the duplicate's
        // stale image lands after a genuinely newer write to the same key —
        // observed directly via `generative`'s new restart-lifecycle property
        // and hand-built pin). Passing our own durably-persisted
        // `last_confirmed` explicitly closes this: it can never be *behind*
        // the slot's own tracked position (it only ever advances after a
        // commit durably records it), so resuming from it can redeliver
        // nothing already staged, while still replaying anything genuinely
        // unstaged at crash time — no lost work, no duplicate processing.
        let replication_config = config
            .replication_config()
            .with_start_lsn(pgwire_replication::Lsn::from(u64::from(last_confirmed)));
        let replication =
            pgwire_replication::ReplicationClient::connect(replication_config).await?;
        watermark.advance(last_confirmed);
        Ok(Self {
            replication,
            session,
            slot: config.slot.clone(),
            wake_channel: config.wake_channel.clone(),
            relations: RelationCache::new(),
            primary_keys: std::collections::HashMap::new(),
            group_key_columns: GroupKeyColumns::new(pool),
            buffer: spill::TxnBuffer::new(config.spill_threshold, config.hard_cap),
            spill_threshold: config.spill_threshold,
            hard_cap: config.hard_cap,
            current_xid: None,
            in_txn: false,
            propagated_in_txn: std::collections::HashSet::new(),
            propagated_prefix: propagated_tables_prefix(&config.schema),
            last_confirmed,
            // Zero-initialized rather than `Instant::now()`, so the very
            // first keepalive after connecting is never held back by the
            // rate limit.
            last_keepalive_persist: Instant::now() - KEEPALIVE_PERSIST_INTERVAL,
            watermark,
            group_commit: config.group_commit,
            pending: Vec::new(),
            pending_rows: 0,
            batch_deadline: None,
        })
    }

    /// Runs the consumer loop until the replication stream ends (cleanly,
    /// e.g. after a configured `stop_at_lsn`) or errors.
    pub async fn run(&mut self) -> Result<(), IntakeError> {
        loop {
            // Issue #274: when a group-commit batch is open (`batch_deadline`
            // is `Some`), race the next replication event against that
            // deadline so a partially-filled batch still flushes promptly
            // even if no new source commit arrives to trigger
            // `commit_transaction`'s own row-bound check. Safe to race:
            // `pgwire_replication::ReplicationClient::recv` is an
            // `mpsc::Receiver::recv().await` over a channel a decoupled
            // background worker task fills (verified directly against this
            // crate's pinned `pgwire-replication` version:
            // `ReplicationClient::recv` is `self.rx.recv().await` where `rx`
            // is a `tokio::sync::mpsc::Receiver` and the worker that feeds it
            // runs in its own `tokio::spawn`ed task) — dropping this future
            // on the timer branch's timeout (`tokio::select!`'s default
            // behavior) never discards an already-decoded event, since
            // `mpsc::Receiver::recv` is documented cancel-safe: the channel
            // buffer holds the event for the next call regardless of which
            // branch of this select won.
            let event = match self.batch_deadline {
                Some(deadline) => {
                    tokio::select! {
                        event = self.replication.recv() => event?,
                        _ = tokio::time::sleep_until(deadline.into()) => {
                            self.flush_pending_group().await?;
                            continue;
                        }
                    }
                }
                None => self.replication.recv().await?,
            };
            match event {
                Some(event) => self.handle_event(event).await?,
                None => break,
            }
        }
        Ok(())
    }

    /// Processes one [`pgwire_replication::ReplicationEvent`]. Exposed at
    /// this granularity (rather than only via [`run`](Self::run)) so tests
    /// can drive one event at a time.
    pub async fn handle_event(
        &mut self,
        event: pgwire_replication::ReplicationEvent,
    ) -> Result<(), IntakeError> {
        use pgwire_replication::ReplicationEvent;
        match event {
            ReplicationEvent::Begin { xid, .. } => {
                // Defensive: Commit already clears the buffer, so nothing
                // should be carried over from a prior transaction.
                self.buffer.clear();
                self.propagated_in_txn.clear();
                self.current_xid = Some(xid);
                self.in_txn = true;
            }
            ReplicationEvent::XLogData { data, .. } => {
                self.handle_xlog_data(data.as_ref()).await?;
            }
            ReplicationEvent::Commit {
                end_lsn,
                commit_time_micros,
                ..
            } => {
                self.commit_transaction(end_lsn, commit_time_micros).await?;
                self.in_txn = false;
                self.propagated_in_txn.clear();
                // Defensive: `handle_xlog_data` falls back to xid 0 if this
                // is unset, so a stray XLogData arriving between this Commit
                // and the next Begin — unreachable under normal protocol
                // ordering — can't silently reuse the transaction that just
                // committed.
                self.current_xid = None;
            }
            ReplicationEvent::KeepAlive { wal_end, .. } => {
                self.advance_watermark_on_keepalive(wal_end).await?;
            }
            ReplicationEvent::Message {
                transactional: true,
                prefix,
                content,
                ..
            } if prefix == self.propagated_prefix => {
                self.propagated_in_txn
                    .extend(decode_propagated_tables(&content));
            }
            ReplicationEvent::Message { .. } => {}
            ReplicationEvent::StoppedAt { .. } => {}
        }
        Ok(())
    }

    async fn handle_xlog_data(&mut self, data: &[u8]) -> Result<(), IntakeError> {
        let xid = self.current_xid.unwrap_or(0);
        match pgoutput::decode(data)? {
            Message::Relation(relation) => {
                // REPLICA IDENTITY FULL (issue #56): `is_key` is set on every
                // column for this relation, so `extract_key` can't trust it
                // — look up the source table's actual primary key once per
                // relation and cache it alongside the relation itself.
                //
                // REPLICA IDENTITY DEFAULT (issue #163): the flags *are*
                // trustworthy (the identity is exactly the primary key), but
                // they arrive in `pgoutput`'s physical column order, which
                // Postgres allows to differ from the order the `PRIMARY KEY
                // (...)` clause declares — and every consumer of an encoded
                // composite key decodes in *declared* order (see
                // `defs::ddl::pk_key_sql_expr`'s "declared-order convention"
                // section). Caching the real key here lets `extract_key`
                // normalize onto that one order instead of silently emitting a
                // differently-ordered key for such a table. One extra catalog
                // round trip per `Relation` message (not per change), and
                // byte-identical keys for the overwhelmingly common table
                // whose two orders already coincide.
                //
                // The lookup reads the *current* catalog on intake's own
                // session, while this `Relation` message comes out of the WAL
                // at a possibly much older position — logical decoding
                // resolves a relation against a historic snapshot, so a table
                // that has since been dropped (or had its primary key
                // replaced) still gets decoded while the lookup finds
                // nothing. An empty result therefore means "can't normalize",
                // not "this relation has no key":
                //
                // - under DEFAULT, fall back to `pgoutput`'s flags (they
                //   *are* the primary key's columns as of the change's own
                //   LSN, merely unordered), which is exactly the pre-#163
                //   behavior and the only ordering information that still
                //   exists for such a relation. Overriding with an empty key
                //   instead would fail `extract_key`'s "at least one key
                //   column" check and so take the whole intake loop down
                //   (`IntakeError::MissingKeyValue` propagates out of `run`)
                //   for a change that staged fine before.
                // - under FULL, keep overriding unconditionally: the flags
                //   are set on *every* column there, so falling back would
                //   key the row by its entire contents — issue #56's bug —
                //   and failing loudly is the right outcome instead.
                if matches!(relation.replica_identity, b'f' | b'd') {
                    let pk = primary_key_columns(
                        self.session.client(),
                        &relation.namespace,
                        &relation.name,
                    )
                    .await?;
                    if pk.is_empty() && relation.replica_identity == b'd' {
                        self.primary_keys.remove(&relation.relation_id);
                    } else {
                        self.primary_keys.insert(relation.relation_id, pk);
                    }
                } else {
                    self.primary_keys.remove(&relation.relation_id);
                }
                self.relations.record(relation);
            }
            Message::Insert { relation_id, new } => {
                let relation = self.relations.get(relation_id)?;
                if already_propagated(&self.propagated_in_txn, relation)? {
                    return Ok(());
                }
                // Issue #133: `group_key_cols` before `pk`, so the
                // `.await` below (the cache's only possible catalog round
                // trip — a plain map read on a fresh entry) happens before
                // any other field borrow is live. `relation.name` (bare),
                // not a schema-qualified identity — see
                // `GroupKeyColumns::columns_for`'s own doc comment for why
                // `relationship_definitions.from_table` is keyed bare.
                let group_key_cols = self.group_key_columns.columns_for(&relation.name).await?;
                let pk = self.primary_keys.get(&relation_id).map(Vec::as_slice);
                self.buffer.push(
                    cdc_change(
                        relation,
                        CdcOp::Insert,
                        None,
                        Some(&new),
                        pk,
                        group_key_cols,
                    )?,
                    xid,
                )?;
            }
            Message::Update {
                relation_id,
                old,
                new,
            } => {
                let relation = self.relations.get(relation_id)?;
                if already_propagated(&self.propagated_in_txn, relation)? {
                    return Ok(());
                }
                let old_tuple = old.as_ref().map(|(_, tuple)| tuple.as_slice());
                let group_key_cols = self.group_key_columns.columns_for(&relation.name).await?;
                let pk = self.primary_keys.get(&relation_id).map(Vec::as_slice);
                self.buffer.push(
                    cdc_change(
                        relation,
                        CdcOp::Update,
                        old_tuple,
                        Some(&new),
                        pk,
                        group_key_cols,
                    )?,
                    xid,
                )?;
            }
            Message::Delete {
                relation_id, old, ..
            } => {
                let relation = self.relations.get(relation_id)?;
                if already_propagated(&self.propagated_in_txn, relation)? {
                    return Ok(());
                }
                let group_key_cols = self.group_key_columns.columns_for(&relation.name).await?;
                let pk = self.primary_keys.get(&relation_id).map(Vec::as_slice);
                self.buffer.push(
                    cdc_change(
                        relation,
                        CdcOp::Delete,
                        Some(&old),
                        None,
                        pk,
                        group_key_cols,
                    )?,
                    xid,
                )?;
            }
            // No-ops in this slice — see `pgoutput::Message`'s doc comment.
            Message::Origin { .. } | Message::Type { .. } => {}
            // Issue #60: one truncate sentinel per named relation.
            // `relation_ids` already enumerates every truncated table *that
            // is in the publication* — Postgres pre-expands CASCADE
            // server-side before ever sending this message, so a cascaded
            // child not in the publication is simply absent here, and was
            // never replicated in the first place. We do not act on
            // `options` (CASCADE/RESTART IDENTITY) beyond that: this stage
            // only clears the listed relations, it does not walk a cascade
            // graph of its own.
            Message::Truncate { relation_ids, .. } => {
                for relation_id in relation_ids {
                    let relation = self.relations.get(relation_id)?;
                    if already_propagated(&self.propagated_in_txn, relation)? {
                        continue;
                    }
                    let src_table = publication::qualify(&relation.namespace, &relation.name)?;
                    self.buffer.push(
                        StagedChange::Truncate {
                            src_table,
                            lsn: None,
                            origin_lsn: None,
                            src_changed: None,
                        },
                        xid,
                    )?;
                }
            }
            Message::Begin { .. } | Message::Commit { .. } => {
                // Unreachable: `pgwire_replication` peels Begin/Commit off
                // before we see their XLogData bytes (see `pgoutput`'s module
                // doc). A no-op rather than `unreachable!()` so a broken
                // assumption can't panic the whole consumer.
            }
        }
        Ok(())
    }

    /// Runs the linchpin over this transaction's buffered changes, and —
    /// only after its `COMMIT` returns — advances the in-memory applied LSN
    /// and lets `pgwire_replication` report it as flushed on its next
    /// Standby Status Update. Confirms `end_lsn` (the position *after* the
    /// commit record), not `commit_lsn`, per the design doc.
    ///
    /// Issue #56: this is the root of the propagation span tree ADR-0009
    /// decision 3 calls for — the source-commit span every downstream hop
    /// (`staging::apply::compute`'s per-source evaluation,
    /// `staging::apply::apply_target`'s per-transform apply) traces back to
    /// in spirit, even though the two signals aren't wired together as
    /// parent/child spans: a source commit and the batch(es) it eventually
    /// lands in are separated by staging/fold/claim, crossing worker and
    /// even process boundaries, so there is no single in-process span this
    /// span could parent. `commit_time_micros` — this span's whole reason
    /// for existing — is the exact same origin timestamp
    /// `StagedChange::Cdc::src_changed`/`StagedChange::Truncate::src_changed`
    /// carry forward for #51/#52's `SystemTime`-based latency histograms
    /// (see [`stamp_commit_metadata`]); this span models the same journey
    /// as a `tracing` span rather than *computing* those histograms from it
    /// (see this module's own doc comment's "Still deferred" list — #51/#52
    /// predate `tracing` existing in this crate at all).
    #[tracing::instrument(
        name = "intake.commit_transaction",
        skip(self, end_lsn),
        fields(
            slot = %self.slot,
            end_lsn = end_lsn.as_u64(),
            changes = tracing::field::Empty,
        )
    )]
    async fn commit_transaction(
        &mut self,
        end_lsn: pgwire_replication::Lsn,
        commit_time_micros: i64,
    ) -> Result<(), IntakeError> {
        // Consumes the buffer (spilled chunks and all) — a fresh one takes
        // its place for the next transaction.
        let buffer = std::mem::replace(
            &mut self.buffer,
            spill::TxnBuffer::new(self.spill_threshold, self.hard_cap),
        );
        let change_count = buffer.len();
        tracing::Span::current().record("changes", change_count);
        let lsn = PgLsn::from(end_lsn.as_u64());
        let changed_at = pg_commit_time_to_system_time(commit_time_micros);

        let Some(group_commit) = self.group_commit else {
            return self
                .commit_single(buffer, change_count, end_lsn, lsn, changed_at)
                .await;
        };

        // Issue #274: buffer this transaction into the open group instead of
        // committing it alone — the ack/watermark-advance is deferred to the
        // whole group's own single flush ([`Self::flush_pending_group`]).
        if self.pending.is_empty() {
            self.batch_deadline = Some(Instant::now() + group_commit.max_delay);
        }
        self.pending_rows += change_count;
        self.pending.push((buffer, lsn, changed_at));

        if self.pending_rows >= group_commit.max_rows {
            self.flush_pending_group().await?;
        }
        Ok(())
    }

    /// The original, ungrouped commit path: one ring transaction for exactly
    /// this one source transaction's buffer. Used directly when
    /// `group_commit` is `None` (issue #274's escape hatch).
    async fn commit_single(
        &mut self,
        buffer: spill::TxnBuffer,
        change_count: usize,
        end_lsn: pgwire_replication::Lsn,
        lsn: PgLsn,
        changed_at: SystemTime,
    ) -> Result<(), IntakeError> {
        let txn = self.session.transaction().await?;
        match buffer
            .stage_and_advance(&txn, &self.slot, &self.wake_channel, lsn, changed_at)
            .await
        {
            Ok(()) => {
                txn.commit().await?;
            }
            Err(err) => {
                // Explicit, though dropping `txn` would roll back just as
                // surely. Leaves `confirmed` untouched; the caller's next
                // connection resumes at the old position and the server
                // replays.
                let _ = txn.rollback().await;
                tracing::error!(
                    slot = %self.slot,
                    changes = change_count,
                    error = %err,
                    "failed to stage a committed transaction"
                );
                return Err(err);
            }
        }
        tracing::debug!(changes = change_count, "staged a committed transaction");

        // The acknowledgment: strictly after the commit returned, never
        // inside it.
        self.replication.update_applied_lsn(end_lsn);
        self.last_confirmed = lsn;
        // Issue #132, epic #127: the in-process staged-through watermark
        // advances here too, right after the same commit that durably
        // staged `buffer`'s rows — this is the primary advance point
        // `StagedWatermark`'s own doc comment describes ("advanced right
        // after each stage_and_advance commit").
        self.watermark.advance(lsn);
        Ok(())
    }

    /// Issue #274: commits every buffered transaction in `self.pending` as
    /// **one** ring transaction — each appends with its own
    /// `end_lsn`/`changed_at` (append order across transactions doesn't
    /// matter for correctness; see [`spill::TxnBuffer::append_only`]'s doc
    /// comment), then the group's single [`advance_watermark_and_notify`]
    /// call, using the group's *last* transaction's `end_lsn` — exactly the
    /// LSN a single ungrouped commit of that same last transaction would
    /// have advanced to, since watermark advance is idempotent/monotonic
    /// (`advance_watermark_and_notify`'s own "Monotonic guard"). A no-op if
    /// `pending` is empty (both call sites already check this, but staying
    /// defensive here costs nothing).
    async fn flush_pending_group(&mut self) -> Result<(), IntakeError> {
        if self.pending.is_empty() {
            return Ok(());
        }
        let group = std::mem::take(&mut self.pending);
        let group_rows = self.pending_rows;
        self.pending_rows = 0;
        self.batch_deadline = None;

        let (_, last_lsn, _) = *group
            .last()
            .expect("checked non-empty above, and nothing else drains `pending`");

        let txn = self.session.transaction().await?;
        let mut result = Ok(());
        for (buffer, lsn, changed_at) in group {
            if let Err(err) = buffer.append_only(&txn, lsn, changed_at).await {
                result = Err(err);
                break;
            }
        }
        if result.is_ok() {
            result =
                advance_watermark_and_notify(&txn, &self.slot, &self.wake_channel, last_lsn).await;
        }
        match result {
            Ok(()) => {
                txn.commit().await?;
            }
            Err(err) => {
                let _ = txn.rollback().await;
                tracing::error!(
                    slot = %self.slot,
                    changes = group_rows,
                    error = %err,
                    "failed to stage a grouped batch of committed transactions"
                );
                return Err(err);
            }
        }
        tracing::debug!(
            changes = group_rows,
            "staged a grouped batch of committed transactions"
        );

        // Same acknowledgment discipline as `commit_single`: strictly after
        // the commit returned, to the group's last (most recent) end_lsn.
        self.replication
            .update_applied_lsn(pgwire_replication::Lsn::from(u64::from(last_lsn)));
        self.last_confirmed = last_lsn;
        self.watermark.advance(last_lsn);
        Ok(())
    }

    /// The quiet-stream watermark advance (issue #8): a stream carrying no
    /// watched changes still needs the watermark to advance on keepalive
    /// frames, with four load-bearing guards — see "The quiet-stream
    /// problem" in the design doc. Also where issue #132's in-process
    /// [`StagedWatermark`] gets its own keepalive-driven advance, ahead of
    /// (and unthrottled by) those same four guards — see the inline
    /// comment at its call below for why the two watermarks intentionally
    /// diverge.
    async fn advance_watermark_on_keepalive(
        &mut self,
        wal_end: pgwire_replication::Lsn,
    ) -> Result<(), IntakeError> {
        // Guard (a): never mid-transaction. A keepalive's `wal_end` can sit
        // past the commit record of a transaction whose changes are still
        // only buffered here; confirming it would tell the source those
        // changes are durably staged when they are not.
        if self.in_txn {
            return Ok(());
        }
        // Issue #274's own extension of guard (a): a still-open group-commit
        // batch has *decoded* one or more committed source transactions but
        // not yet staged them (that only happens at
        // `flush_pending_group`) — exactly the same hazard `in_txn` guards
        // against, just spanning several already-committed source
        // transactions instead of one still-open one. A keepalive's
        // `wal_end` routinely sits past every pending transaction's own
        // `end_lsn` (it is decoded from a later WAL position), so without
        // this guard a keepalive arriving inside the batch's `max_delay`
        // window could advance both watermarks — in-memory and persisted —
        // past rows that are still only sitting in `self.pending`, ahead of
        // `Self::run`'s own flush-on-deadline. Re-verified against the
        // current `pending`/`batch_deadline` design (not carried over from
        // any prior reference): `self.last_confirmed`'s own guard (b) below
        // does *not* catch this on its own, since `last_confirmed` is only
        // updated by `commit_single`/`flush_pending_group`, so it still sits
        // behind every pending transaction's LSN while a batch is open.
        if !self.pending.is_empty() {
            return Ok(());
        }
        let candidate = PgLsn::from(wal_end.as_u64());

        // Issue #132, epic #127: the in-process staged-through watermark
        // advances on *every* non-mid-transaction keepalive, with no
        // throttle — deliberately ahead of (and independent from) the
        // persisted-watermark guards below, which exist to rate-limit a
        // real WAL-generating write. `StagedWatermark::advance` is its own
        // monotonic no-op below `candidate`'s already-published value, so
        // this is safe to call unconditionally here regardless of where
        // `last_confirmed`/the persisted `confirmed_lsn` currently sit.
        self.watermark.advance(candidate);

        // Guard (b), the in-memory half: never regress. (The SQL
        // `confirmed_lsn < $1` guard below covers the persisted half.)
        if candidate <= self.last_confirmed {
            return Ok(());
        }
        // Guard (d): the persist below is itself a WAL-generating write —
        // unthrottled, intake would loop on its own writes against a server
        // that echoes empty-transaction keepalives.
        if self.last_keepalive_persist.elapsed() < KEEPALIVE_PERSIST_INTERVAL {
            return Ok(());
        }

        let txn = self.session.transaction().await?;
        txn.execute(
            "update replication_progress set confirmed_lsn = $1 \
             where slot_name = $2 and confirmed_lsn < $1",
            &[&candidate, &self.slot],
        )
        .await?;
        txn.commit().await?;
        self.last_keepalive_persist = Instant::now();

        // Guard (c): the in-memory value (and the report to the slot) only
        // advances once the persist above has returned — a crash in between
        // leaves the table ahead of the slot, a harmless re-stream, never a
        // gap.
        self.last_confirmed = candidate;
        self.replication.update_applied_lsn(wal_end);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pgoutput::ColumnInfo;

    #[test]
    fn propagated_tables_round_trip_through_the_message_content() {
        let content = encode_propagated_tables(["public.h1", "odd schema.a,b"]);
        let decoded: Vec<String> = decode_propagated_tables(&content).collect();
        assert_eq!(decoded, vec!["public.h1", "odd schema.a,b"]);
        assert_eq!(decode_propagated_tables(&[]).count(), 0);
    }

    #[test]
    fn only_a_named_relation_counts_as_already_propagated() {
        let widgets = relation(vec![("id", true)]);
        let mut propagated = std::collections::HashSet::new();
        assert!(!already_propagated(&propagated, &widgets).unwrap());
        propagated.insert("public.gadgets".to_string());
        assert!(!already_propagated(&propagated, &widgets).unwrap());
        propagated.insert("public.widgets".to_string());
        assert!(already_propagated(&propagated, &widgets).unwrap());
    }

    fn relation(columns: Vec<(&str, bool)>) -> Relation {
        Relation {
            relation_id: 1,
            namespace: "public".into(),
            name: "widgets".into(),
            replica_identity: b'd',
            columns: columns
                .into_iter()
                .map(|(name, is_key)| ColumnInfo {
                    name: name.into(),
                    type_oid: 25,
                    type_modifier: -1,
                    is_key,
                })
                .collect(),
        }
    }

    #[test]
    fn extract_key_joins_only_key_columns_in_definition_order() {
        let r = relation(vec![("tenant", true), ("id", true), ("payload", false)]);
        let tuple = vec![
            ColumnValue::Text("t1".into()),
            ColumnValue::Text("42".into()),
            ColumnValue::Text("hello".into()),
        ];
        assert_eq!(extract_key(&r, &tuple, None).unwrap(), "t1\u{1f}42");
    }

    /// A primary-key value that genuinely contains a U+0001 is staged
    /// **verbatim**, not escaped: `ddl::pk_key_sql_expr` renders a not-null
    /// key column raw too, and — decisively — a 1-1 definition's
    /// `staging::apply::apply_target` binds this very text in as the target
    /// table's own literal primary-key value, where `defs::backfill`,
    /// `staging::quarantine::recompute_column` and `defs::oracle::recompute`
    /// all write and read the raw source value. Escaping here doubled the
    /// U+0001 on the incremental path only, so the source row's target row
    /// was written under a key no other path could find — duplicating it on
    /// re-derive and orphaning it on delete. See
    /// `ddl::encode_key_part`'s "`PrimaryKeyColumn::nullable` selects the
    /// encoding" section, and the end-to-end
    /// `trellis/tests/one_to_one_control_char_pk.rs`.
    #[test]
    fn extract_key_keeps_a_control_character_in_a_key_value_verbatim() {
        let r = relation(vec![("id", true), ("payload", false)]);
        let tuple = vec![
            ColumnValue::Text("a\u{1}b".into()),
            ColumnValue::Text("hello".into()),
        ];
        assert_eq!(extract_key(&r, &tuple, None).unwrap(), "a\u{1}b");

        let pk = vec!["tenant".to_string(), "id".to_string()];
        let r = relation(vec![("tenant", true), ("id", true)]);
        let tuple = vec![
            ColumnValue::Text("\u{1}".into()),
            ColumnValue::Text("a\u{1}\u{1}b".into()),
        ];
        assert_eq!(
            extract_key(&r, &tuple, Some(&pk)).unwrap(),
            "\u{1}\u{1f}a\u{1}\u{1}b"
        );
    }

    /// Issue #200's counterpart of the test above, and the one place the two
    /// escape layers meet on this side: a U+0001 is still verbatim (a real
    /// `PRIMARY KEY` is `NOT NULL`, so there is no NULL layer at all here),
    /// but a genuine U+001F/U+001E in a *composite* key's component is
    /// escaped by `ddl::join_pk_key`, so the staged key still decodes at
    /// arity 2 instead of tripping `MalformedCompositeKey`. At arity 1
    /// nothing is escaped, since the staged text doubles as a 1-1 target's
    /// literal primary-key value.
    #[test]
    fn extract_key_escapes_a_separator_valued_component_only_in_a_composite_key() {
        use crate::defs::ddl;

        let r = relation(vec![("id", true), ("payload", false)]);
        let tuple = vec![
            ColumnValue::Text("a\u{1f}\u{1e}b".into()),
            ColumnValue::Text("hello".into()),
        ];
        assert_eq!(
            extract_key(&r, &tuple, None).unwrap(),
            "a\u{1f}\u{1e}b",
            "an arity-1 key is the column's own text, verbatim"
        );

        let pk = vec!["tenant".to_string(), "id".to_string()];
        let r = relation(vec![("tenant", true), ("id", true)]);
        let tuple = vec![
            ColumnValue::Text("t\u{1f}1".into()),
            ColumnValue::Text("a\u{1e}\u{1}b".into()),
        ];
        let key = extract_key(&r, &tuple, Some(&pk)).unwrap();
        // The consumer side decodes it back at the right arity — a real
        // `PRIMARY KEY`'s columns are not-null, so `split_pk_key` applies
        // only issue #200's un-escape, never issue #110's decode.
        let pk_columns: Vec<ddl::PrimaryKeyColumn> = ["tenant", "id"]
            .iter()
            .map(|name| ddl::PrimaryKeyColumn {
                name: (*name).to_string(),
                data_type: "text".to_string(),
                nullable: false,
            })
            .collect();
        assert_eq!(
            ddl::split_pk_key(&pk_columns, "widgets", &key)
                .expect("decodes at arity 2 despite the embedded separator")
                .iter()
                .map(Option::as_deref)
                .collect::<Vec<_>>(),
            vec![Some("t\u{1f}1"), Some("a\u{1e}\u{1}b")]
        );
    }

    #[test]
    fn extract_key_ignores_is_key_under_full_replica_identity_and_uses_the_real_primary_key() {
        // Under REPLICA IDENTITY FULL, pgoutput marks every column `is_key`
        // (issue #56) — simulated here by setting `is_key: true` on all
        // three columns, exactly as a real FULL relation message decodes.
        let mut r = relation(vec![("id", true), ("amount", true), ("rate", true)]);
        r.replica_identity = b'f';
        let tuple = vec![
            ColumnValue::Text("1".into()),
            ColumnValue::Text("10.00".into()),
            ColumnValue::Text("1.50".into()),
        ];
        let pk = vec!["id".to_string()];
        assert_eq!(extract_key(&r, &tuple, Some(&pk)).unwrap(), "1");
    }

    /// Issue #163: `create table t (tag text, post int, primary key (post,
    /// tag))` — the primary key's *declared* order `(post, tag)` is the
    /// reverse of the table's physical column order `(tag, post)`, and
    /// `pgoutput` hands us the physical one. The staged key must follow the
    /// declared order, because that is the order every consumer decodes with
    /// (`ddl::split_pk_key`, whose `pk` slice comes from
    /// `ddl::source_primary_key`'s `array_position(i.indkey, a.attnum)`
    /// sort) and the order the SQL-side producer `ddl::pk_key_sql_expr`
    /// re-renders it in. Before this fix the two silently disagreed for
    /// exactly this table shape: a live re-fetch would have looked up
    /// `post = 'rust'`/`tag = '7'`.
    #[test]
    fn extract_key_orders_composite_parts_by_the_primary_keys_declared_order() {
        let r = relation(vec![("tag", true), ("post", true), ("payload", false)]);
        let tuple = vec![
            ColumnValue::Text("rust".into()),
            ColumnValue::Text("7".into()),
            ColumnValue::Text("hello".into()),
        ];
        let pk = vec!["post".to_string(), "tag".to_string()];
        assert_eq!(extract_key(&r, &tuple, Some(&pk)).unwrap(), "7\u{1f}rust");
    }

    /// The same declared-order normalization under `REPLICA IDENTITY FULL`,
    /// where `pgoutput` additionally marks *every* column `is_key` (issue
    /// #56) — so neither the flags nor the column order carry usable
    /// information and both have to come from the catalog lookup.
    #[test]
    fn extract_key_orders_composite_parts_by_declared_order_under_full_replica_identity() {
        let mut r = relation(vec![("tag", true), ("post", true), ("payload", true)]);
        r.replica_identity = b'f';
        let tuple = vec![
            ColumnValue::Text("rust".into()),
            ColumnValue::Text("7".into()),
            ColumnValue::Text("hello".into()),
        ];
        let pk = vec!["post".to_string(), "tag".to_string()];
        assert_eq!(extract_key(&r, &tuple, Some(&pk)).unwrap(), "7\u{1f}rust");
    }

    /// A primary-key column the publication doesn't carry at all leaves no
    /// derivable row identity, so this fails loudly with the same
    /// `MissingKeyValue` a missing *value* raises — rather than silently
    /// staging a short, lower-arity key that `ddl::split_pk_key` would later
    /// reject as `MalformedCompositeKey` (or, worse, that would collide with
    /// a different row's).
    #[test]
    fn extract_key_rejects_a_primary_key_column_absent_from_the_relation() {
        let r = relation(vec![("tag", true), ("payload", false)]);
        let tuple = vec![
            ColumnValue::Text("rust".into()),
            ColumnValue::Text("hello".into()),
        ];
        let pk = vec!["post".to_string(), "tag".to_string()];
        match extract_key(&r, &tuple, Some(&pk)) {
            Err(IntakeError::MissingKeyValue { table }) => assert_eq!(table, "widgets"),
            other => panic!("expected MissingKeyValue, got {other:?}"),
        }
    }

    #[test]
    fn extract_key_rejects_a_missing_key_value() {
        let r = relation(vec![("id", true)]);
        let tuple = vec![ColumnValue::Null];
        match extract_key(&r, &tuple, None) {
            Err(IntakeError::MissingKeyValue { table }) => assert_eq!(table, "widgets"),
            other => panic!("expected MissingKeyValue, got {other:?}"),
        }
    }

    #[test]
    fn tuple_to_json_omits_unchanged_and_encodes_null_and_text() {
        let r = relation(vec![("id", true), ("payload", false), ("note", false)]);
        let tuple = vec![
            ColumnValue::Text("1".into()),
            ColumnValue::Null,
            ColumnValue::Unchanged,
        ];
        assert_eq!(tuple_to_json(&r, &tuple), r#"{"id":"1","payload":null}"#);
    }

    #[test]
    fn tuple_to_json_escapes_quotes_and_backslashes() {
        let r = relation(vec![("payload", false)]);
        let tuple = vec![ColumnValue::Text("she said \"hi\"\\ok".into())];
        assert_eq!(
            tuple_to_json(&r, &tuple),
            r#"{"payload":"she said \"hi\"\\ok"}"#
        );
    }

    #[test]
    fn cdc_change_builds_insert_with_no_old_image() {
        let r = relation(vec![("id", true), ("payload", false)]);
        let new = vec![ColumnValue::Text("1".into()), ColumnValue::Text("a".into())];
        let change = cdc_change(&r, CdcOp::Insert, None, Some(&new), None, &[]).unwrap();
        match change {
            StagedChange::Cdc {
                src_table,
                key,
                op,
                old_image,
                new_image,
                group_key,
                ..
            } => {
                assert_eq!(src_table, "public.widgets");
                assert_eq!(key, "1");
                assert_eq!(op, CdcOp::Insert);
                assert!(old_image.is_none());
                assert_eq!(new_image.unwrap(), r#"{"id":"1","payload":"a"}"#);
                assert!(
                    group_key.is_none(),
                    "empty group_key_cols must never populate group_key"
                );
            }
            other => panic!("expected Cdc, got {other:?}"),
        }
    }

    #[test]
    fn cdc_change_builds_delete_with_no_new_image() {
        let r = relation(vec![("id", true)]);
        let old = vec![ColumnValue::Text("9".into())];
        let change = cdc_change(&r, CdcOp::Delete, Some(&old), None, None, &[]).unwrap();
        match change {
            StagedChange::Cdc {
                key,
                new_image,
                old_image,
                ..
            } => {
                assert_eq!(key, "9");
                assert!(new_image.is_none());
                assert_eq!(old_image.unwrap(), r#"{"id":"9"}"#);
            }
            other => panic!("expected Cdc, got {other:?}"),
        }
    }

    /// Issue #133: a from-side row's `group_key` unions its outbound
    /// relationship column's value from *both* images when they differ (a
    /// re-point in this one change) — the exact signal guard (b) needs even
    /// before any fold ever runs.
    #[test]
    fn cdc_change_populates_group_key_from_old_and_new_images() {
        let r = relation(vec![("id", true), ("post", false), ("tag", false)]);
        let old = vec![
            ColumnValue::Text("30".into()),
            ColumnValue::Text("3".into()),
            ColumnValue::Text("rust".into()),
        ];
        let new = vec![
            ColumnValue::Text("30".into()),
            ColumnValue::Text("2".into()),
            ColumnValue::Text("rust".into()),
        ];
        let group_key_cols = vec!["post".to_string()];
        let change = cdc_change(
            &r,
            CdcOp::Update,
            Some(&old),
            Some(&new),
            None,
            &group_key_cols,
        )
        .unwrap();
        match change {
            StagedChange::Cdc { group_key, .. } => {
                assert_eq!(
                    group_key.unwrap(),
                    vec!["3".to_string(), "2".to_string()],
                    "must union the old-image and new-image touched values, in that order"
                );
            }
            other => panic!("expected Cdc, got {other:?}"),
        }
    }

    /// An unchanged `group_key_cols` value (present, identical, in both
    /// images) must not be duplicated in the union.
    #[test]
    fn cdc_change_dedups_an_unchanged_group_key_value_across_old_and_new() {
        let r = relation(vec![("id", true), ("post", false)]);
        let old = vec![
            ColumnValue::Text("30".into()),
            ColumnValue::Text("3".into()),
        ];
        let new = vec![
            ColumnValue::Text("30".into()),
            ColumnValue::Text("3".into()),
        ];
        let group_key_cols = vec!["post".to_string()];
        let change = cdc_change(
            &r,
            CdcOp::Update,
            Some(&old),
            Some(&new),
            None,
            &group_key_cols,
        )
        .unwrap();
        match change {
            StagedChange::Cdc { group_key, .. } => {
                assert_eq!(group_key.unwrap(), vec!["3".to_string()]);
            }
            other => panic!("expected Cdc, got {other:?}"),
        }
    }

    #[test]
    fn pg_commit_time_converts_from_postgres_epoch() {
        // 0 micros since 2000-01-01 == that instant in Unix time.
        let t = pg_commit_time_to_system_time(0);
        let unix = t.duration_since(SystemTime::UNIX_EPOCH).unwrap();
        assert_eq!(unix.as_secs(), 946_684_800);
    }
}
