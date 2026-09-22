//! The engine knobs the streaming scenarios vary, and the one place they're
//! mapped onto [`ClientOptions`].
//!
//! Every field here is an existing [`ClientOptions`] field — this harness
//! measures `main` as it is, and deliberately carries no knob the engine
//! doesn't already have.
//!
//! ## Extension point (epic #269)
//!
//! Later children of #269 add engine options of their own (a seal mode, an
//! intake group-commit config). Wiring one up is meant to be three edits and
//! no new plumbing:
//!
//! 1. add the field to [`EngineTuning`] (with its stock value in
//!    [`EngineTuning::default`], so every existing scenario keeps measuring
//!    stock behavior),
//! 2. set the corresponding [`ClientOptions`] field in
//!    [`EngineTuning::client_options`],
//! 3. read a CLI flag for it in [`super::cli`]'s `tuning`, which is the single
//!    function every scenario's flags go through.
//!
//! Scenarios themselves take `&EngineTuning` and never touch
//! [`ClientOptions`] directly, so none of them need editing to gain a knob —
//! which is the point: a later child measuring its own engine change against
//! this baseline should not be re-plumbing the harness.

use std::time::Duration;

use trellis::ClientOptions;

/// [`ClientOptions::poll_interval`]'s own stock default (200 ms) — the idle
/// `LISTEN` backstop under `NOTIFY` delivery.
pub const STOCK_POLL_INTERVAL: Duration = Duration::from_millis(200);

/// [`ClientOptions::maintenance_interval`]'s own stock default (300 ms) — the
/// seal cadence, and the only place a sealed (claimable) batch is ever
/// created. Issue #266's H1.
pub const STOCK_MAINTENANCE_INTERVAL: Duration = Duration::from_millis(300);

/// [`ClientOptions::reconcile_interval`]'s own stock default (5 s), and what
/// the single-hop scenarios run at: they have no intermediate hops for the
/// reconcile pass to publish, so it makes no difference to them and the stock
/// value is the honest one.
pub const STOCK_RECONCILE_INTERVAL: Duration = Duration::from_secs(5);

/// What the multi-hop latency ladder runs at — longer than any single run, so
/// the reconcile pass never fires and intermediate hop tables never join the
/// CDC publication.
///
/// The reference harness this was ported from used the same value as a
/// workaround for #267 (propagation staged `src_table` bare while intake
/// staged it fully-qualified, so a live 2+ hop chain deadlocked the moment an
/// intermediate hop joined the publication). **#267 landed on `main` (PR
/// #282), and a chain measured at the stock 5 s reconcile no longer deadlocks
/// — verified while porting this.** The interval stays long for a different,
/// still-current reason, which the port measured directly:
///
/// An intermediate hop table is both a propagation target and the next
/// transform's source, so once it is published, each of its writes is staged
/// **twice** — once in-transaction by apply's downstream `Recompute`, and
/// again by CDC decoding the very same write. #267's fix makes the two
/// spellings coalesce instead of conflicting, so this is now merely duplicated
/// work rather than a deadlock, but it still:
///
/// * breaks #266's metric cross-check outright — `trellis_changes_applied_total`
///   stops being comparable to rows committed (measured: 137 applied changes
///   for 100 committed rows at depth 2), and
/// * changes what is being measured — per-hop latency over two concurrent
///   propagation paths, with roughly twice the ring load, rather than over the
///   one path the hop-count ladder is about (measured: 185 ms/hop published vs
///   304 ms/hop unpublished, at the same stock tick).
///
/// Keeping the reconcile pass quiet keeps propagation purely on the
/// in-transaction `Recompute` path, which needs no publication membership.
/// This is sound for these scenarios specifically because they never write
/// directly to an intermediate hop table — the only writer is the hop above.
/// `--reconcile-interval-ms` overrides it, which is how the two numbers above
/// were obtained; the duplicate-staging cost itself is a real finding about
/// chained transforms and belongs in its own issue, not in a benchmark
/// default.
pub const ISOLATED_PROPAGATION_RECONCILE_INTERVAL: Duration = Duration::from_secs(3600);

/// One scenario's engine configuration. [`Default`] is stock `main` in every
/// field, so a scenario that overrides nothing measures the shipped defaults.
#[derive(Debug, Clone)]
pub struct EngineTuning {
    /// How many drain workers the client runs
    /// ([`ClientOptions::application_threads`]).
    pub application_threads: usize,
    /// The idle-wake backstop ([`ClientOptions::poll_interval`]).
    pub poll_interval: Duration,
    /// The seal cadence ([`ClientOptions::maintenance_interval`]).
    pub maintenance_interval: Duration,
    /// How often the publication/backfill reconcile pass runs
    /// ([`ClientOptions::reconcile_interval`]) — see
    /// [`STOCK_RECONCILE_INTERVAL`] for why this is a knob at all.
    pub reconcile_interval: Duration,
    /// Intake's group-commit batching bounds
    /// ([`ClientOptions::group_commit`], issue #274). `Some` (matching
    /// `ClientOptions::default()`) is stock `main`'s shipped, default-on
    /// behavior; `None` measures the un-grouped escape hatch instead.
    pub group_commit: Option<trellis::GroupCommitConfig>,
}

impl Default for EngineTuning {
    fn default() -> Self {
        Self {
            // Not `ClientOptions::default()`'s 0: every streaming scenario
            // needs at least one drain worker to make progress at all, and
            // `install_definition`'s Backfilling -> Live flip needs one even
            // for an empty chunk queue (see `chain::install_chain_hops`).
            // 4 is what the reference harness's latency ladder measured
            // with; the throughput scenarios override it to 8
            // (`SEG_BUCKETS`' worth of claim parallelism per batch).
            application_threads: 4,
            poll_interval: STOCK_POLL_INTERVAL,
            maintenance_interval: STOCK_MAINTENANCE_INTERVAL,
            reconcile_interval: STOCK_RECONCILE_INTERVAL,
            group_commit: Some(trellis::GroupCommitConfig::default()),
        }
    }
}

impl EngineTuning {
    /// The multi-hop latency ladder's defaults: stock in every field except
    /// the reconcile interval — see
    /// [`ISOLATED_PROPAGATION_RECONCILE_INTERVAL`] for why that one isn't.
    pub fn multi_hop() -> Self {
        Self {
            reconcile_interval: ISOLATED_PROPAGATION_RECONCILE_INTERVAL,
            ..Default::default()
        }
    }

    /// This tuning as [`ClientOptions`], for a staging-worker client
    /// publishing `source_tables`.
    pub fn client_options(&self, source_tables: Vec<String>) -> ClientOptions {
        ClientOptions {
            staging_worker: true,
            application_threads: self.application_threads,
            source_tables,
            poll_interval: self.poll_interval,
            maintenance_interval: self.maintenance_interval,
            reconcile_interval: self.reconcile_interval,
            group_commit: self.group_commit,
            ..Default::default()
        }
    }
}
