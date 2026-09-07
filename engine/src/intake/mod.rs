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
pub mod spill;

pub use error::IntakeError;
pub use replica_identity::{needs_old_image, require_replica_identity_full};

use std::time::{Duration, Instant, SystemTime};

use tokio_postgres::Transaction;
use tokio_postgres::types::PgLsn;

use pgoutput::{ColumnValue, Message, Relation, RelationCache};

use crate::staging::append;
use crate::staging::session::ProducerSession;
use crate::staging::{CdcOp, StagedChange};

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
/// columns, in definition order, joined with the ASCII unit separator — the
/// same collision-free delimiter the ring's `route` column uses (see
/// `V3__staging_ring.sql`), so a key containing a comma can't collide with
/// another row's.
///
/// `primary_key` is the source table's actual primary-key column names,
/// looked up from `pg_catalog` (see [`primary_key_columns`]), and is only
/// ever `Some` for a `REPLICA IDENTITY FULL` relation. Under FULL,
/// `pgoutput` sets the `is_key` flag on *every* column (issue #56), so
/// `col.is_key` can't be trusted to mean "part of the key" there — `Some`
/// overrides it with the real primary key instead. `None` (every other
/// replica identity) keeps the original `is_key`-flag behavior unchanged.
fn extract_key(
    relation: &Relation,
    tuple: &[ColumnValue],
    primary_key: Option<&[String]>,
) -> Result<String, IntakeError> {
    let mut parts = Vec::new();
    for (i, col) in relation.columns.iter().enumerate() {
        let is_key = match primary_key {
            Some(pk) => pk.iter().any(|name| name == &col.name),
            None => col.is_key,
        };
        if !is_key {
            continue;
        }
        match tuple.get(i) {
            Some(ColumnValue::Text(t)) => parts.push(t.as_str()),
            _ => {
                return Err(IntakeError::MissingKeyValue {
                    table: relation.name.clone(),
                });
            }
        }
    }
    if parts.is_empty() {
        return Err(IntakeError::MissingKeyValue {
            table: relation.name.clone(),
        });
    }
    Ok(parts.join("\u{1f}"))
}

/// Looks up `namespace.name`'s actual primary-key column names from
/// `pg_catalog`, in the primary key's own column order — the source of
/// truth [`extract_key`] falls back to under `REPLICA IDENTITY FULL`, where
/// `pgoutput`'s per-column `is_key` flag is set on every column and so can't
/// tell the key columns apart from the rest (issue #56). Unlike
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
fn json_string(s: &str) -> String {
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
fn cdc_change(
    relation: &Relation,
    op: CdcOp,
    old: Option<&[ColumnValue]>,
    new: Option<&[ColumnValue]>,
    primary_key: Option<&[String]>,
) -> Result<StagedChange, IntakeError> {
    let key_tuple = new.or(old).ok_or_else(|| IntakeError::MissingKeyValue {
        table: relation.name.clone(),
    })?;
    let key = extract_key(relation, key_tuple, primary_key)?;
    let src_table = publication::qualify(&relation.namespace, &relation.name)?;
    Ok(StagedChange::Cdc {
        src_table,
        source_relation_oid: Some(relation.relation_id),
        key,
        op,
        lsn: None,
        old_image: old.map(|t| tuple_to_json(relation, t)),
        new_image: new.map(|t| tuple_to_json(relation, t)),
        origin_lsn: None,
        src_changed: None,
        hop_gen: 0,
        group_key: None,
    })
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
}

impl IntakeConfig {
    fn replication_config(&self) -> pgwire_replication::ReplicationConfig {
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
        }
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
    /// The actual source-table primary-key column names for every
    /// `REPLICA IDENTITY FULL` relation seen so far, keyed by
    /// `relation_id` — [`extract_key`]'s override for issue #56. Populated
    /// on each `Relation` message and never consulted for any other
    /// replica identity (see `handle_xlog_data`).
    primary_keys: std::collections::HashMap<u32, Vec<String>>,
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
    /// The in-memory half of the watermark, advanced only after its durable
    /// write returns — mirrors the linchpin's own acknowledgment discipline
    /// for the keepalive path, which has no `pgwire_replication` metrics
    /// counter to read it back from.
    last_confirmed: PgLsn,
    /// Rate-limits the keepalive persist (it is itself a WAL-generating
    /// write) — see [`KEEPALIVE_PERSIST_INTERVAL`].
    last_keepalive_persist: Instant,
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
    pub async fn connect(config: &IntakeConfig) -> Result<Self, IntakeError> {
        let session = ProducerSession::connect(&config.dsn, &config.schema).await?;
        let last_confirmed = fetch_confirmed_lsn(session.client(), &config.slot)
            .await?
            .ok_or_else(|| IntakeError::MissingProgressRow {
                slot: config.slot.clone(),
            })?;
        publication::require_slot_healthy(session.client(), &config.slot, last_confirmed).await?;
        let replication =
            pgwire_replication::ReplicationClient::connect(config.replication_config()).await?;
        Ok(Self {
            replication,
            session,
            slot: config.slot.clone(),
            wake_channel: config.wake_channel.clone(),
            relations: RelationCache::new(),
            primary_keys: std::collections::HashMap::new(),
            buffer: spill::TxnBuffer::new(config.spill_threshold, config.hard_cap),
            spill_threshold: config.spill_threshold,
            hard_cap: config.hard_cap,
            current_xid: None,
            in_txn: false,
            last_confirmed,
            // Zero-initialized rather than `Instant::now()`, so the very
            // first keepalive after connecting is never held back by the
            // rate limit.
            last_keepalive_persist: Instant::now() - KEEPALIVE_PERSIST_INTERVAL,
        })
    }

    /// Runs the consumer loop until the replication stream ends (cleanly,
    /// e.g. after a configured `stop_at_lsn`) or errors.
    pub async fn run(&mut self) -> Result<(), IntakeError> {
        while let Some(event) = self.replication.recv().await? {
            self.handle_event(event).await?;
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
                if relation.replica_identity == b'f' {
                    let pk = primary_key_columns(
                        self.session.client(),
                        &relation.namespace,
                        &relation.name,
                    )
                    .await?;
                    self.primary_keys.insert(relation.relation_id, pk);
                } else {
                    self.primary_keys.remove(&relation.relation_id);
                }
                self.relations.record(relation);
            }
            Message::Insert { relation_id, new } => {
                let relation = self.relations.get(relation_id)?;
                let pk = self.primary_keys.get(&relation_id).map(Vec::as_slice);
                self.buffer.push(
                    cdc_change(relation, CdcOp::Insert, None, Some(&new), pk)?,
                    xid,
                )?;
            }
            Message::Update {
                relation_id,
                old,
                new,
            } => {
                let relation = self.relations.get(relation_id)?;
                let old_tuple = old.as_ref().map(|(_, tuple)| tuple.as_slice());
                let pk = self.primary_keys.get(&relation_id).map(Vec::as_slice);
                self.buffer.push(
                    cdc_change(relation, CdcOp::Update, old_tuple, Some(&new), pk)?,
                    xid,
                )?;
            }
            Message::Delete {
                relation_id, old, ..
            } => {
                let relation = self.relations.get(relation_id)?;
                let pk = self.primary_keys.get(&relation_id).map(Vec::as_slice);
                self.buffer.push(
                    cdc_change(relation, CdcOp::Delete, Some(&old), None, pk)?,
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
                    let src_table = publication::qualify(&relation.namespace, &relation.name)?;
                    self.buffer.push(
                        StagedChange::Truncate {
                            src_table,
                            source_relation_oid: Some(relation.relation_id),
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
        let lsn = PgLsn::from(end_lsn.as_u64());
        let changed_at = pg_commit_time_to_system_time(commit_time_micros);

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
                return Err(err);
            }
        }

        // The acknowledgment: strictly after the commit returned, never
        // inside it.
        self.replication.update_applied_lsn(end_lsn);
        self.last_confirmed = lsn;
        Ok(())
    }

    /// The quiet-stream watermark advance (issue #8): a stream carrying no
    /// watched changes still needs the watermark to advance on keepalive
    /// frames, with four load-bearing guards — see "The quiet-stream
    /// problem" in the design doc.
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
        let candidate = PgLsn::from(wal_end.as_u64());
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
        let change = cdc_change(&r, CdcOp::Insert, None, Some(&new), None).unwrap();
        match change {
            StagedChange::Cdc {
                src_table,
                source_relation_oid,
                key,
                op,
                old_image,
                new_image,
                ..
            } => {
                assert_eq!(src_table, "public.widgets");
                assert_eq!(source_relation_oid, Some(1));
                assert_eq!(key, "1");
                assert_eq!(op, CdcOp::Insert);
                assert!(old_image.is_none());
                assert_eq!(new_image.unwrap(), r#"{"id":"1","payload":"a"}"#);
            }
            other => panic!("expected Cdc, got {other:?}"),
        }
    }

    #[test]
    fn cdc_change_builds_delete_with_no_new_image() {
        let r = relation(vec![("id", true)]);
        let old = vec![ColumnValue::Text("9".into())];
        let change = cdc_change(&r, CdcOp::Delete, Some(&old), None, None).unwrap();
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

    #[test]
    fn pg_commit_time_converts_from_postgres_epoch() {
        // 0 micros since 2000-01-01 == that instant in Unix time.
        let t = pg_commit_time_to_system_time(0);
        let unix = t.duration_since(SystemTime::UNIX_EPOCH).unwrap();
        assert_eq!(unix.as_secs(), 946_684_800);
    }
}
