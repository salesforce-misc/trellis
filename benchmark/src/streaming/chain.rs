//! The N-hop 1-1 chain builder: `<prefix>_src -> <prefix>_h1 -> ... ->
//! <prefix>_hN`, every hop a plain passthrough (`SELECT val AS val`) so the
//! chain's own shape costs as little compute as possible — the latency ladder
//! is measuring hop *count*, not per-hop transform complexity.
//!
//! Goes through [`install_definition`] — the real, product-facing front
//! door — not the direct, ring-bypassing `backfill_definition`
//! [`crate::scenario`] uses. That is the whole point of this harness.
//!
//! Every table is created explicitly qualified into `public`: this crate's
//! raw connections pin `search_path` to `trellis, public` (matching the
//! engine's own integration tests — see [`crate::scenario::connect_raw`]),
//! under which an *unqualified* `CREATE TABLE` lands in `trellis`, the first
//! schema on that path, not `public`. Explicit qualification sidesteps that
//! rather than relying on it.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use tokio_postgres::Client as RawClient;
use trellis::Pool;
use trellis::dev::defs::{ValueType, install_definition};

/// The id [`warm_up`] sends through the chain. Negative, and never reused by
/// [`crate::streaming::load`] (whose ids start at 1), so it can never collide
/// with — or be miscounted among — the load generator's own rows.
pub const WARM_UP_ID: i64 = -1;

pub fn numeric_columns(names: &[&str]) -> HashMap<String, ValueType> {
    names
        .iter()
        .map(|n| (n.to_string(), ValueType::Numeric))
        .collect()
}

/// One programmatically-built N-hop chain: the source table's bare name and
/// each hop's bare target-table name, in hop order. Every table physically
/// lives in `public` (see the module doc comment).
pub struct Chain {
    pub source: String,
    pub hops: Vec<String>,
}

impl Chain {
    /// The terminal hop — the only transform
    /// `trellis_end_to_end_latency_seconds` ever fires for (ADR-0009:
    /// end-to-end latency is keyed by terminal transform).
    pub fn terminal(&self) -> &str {
        self.hops
            .last()
            .expect("a chain always has at least one hop")
    }
}

/// Creates `public.<prefix>_src (id bigint primary key, val numeric)`, bare —
/// no transforms yet.
///
/// Split out from [`install_chain_hops`] because
/// [`trellis::ClientOptions::source_tables`] (and the `ALTER PUBLICATION ADD
/// TABLE` a staging-worker `Client::start` issues against it) needs this
/// table to physically exist *before* the client starts, while the chain's
/// transforms can only be installed *after* — see [`install_chain_hops`].
pub async fn create_chain_source_table(raw: &RawClient, prefix: &str) -> String {
    let source = format!("{prefix}_src");
    raw.batch_execute(&format!(
        "create table public.{source} (id bigint primary key, val numeric)"
    ))
    .await
    .unwrap_or_else(|e| panic!("create chain source table public.{source}: {e}"));
    source
}

/// Installs `depth` chained 1-1 transforms on top of `source` (already
/// created by [`create_chain_source_table`]).
///
/// Must be called with a `Client` (staging worker, at least one application
/// thread) already running against `pool`'s database: with no source rows
/// yet, each hop's backfill enumerates zero chunks, but flipping
/// `Backfilling` -> `Live` (ADR-0007's "Backgrounding and resumability"
/// amendment) still needs a running drain worker to claim and finish that
/// empty chunk queue — there is no synchronous fallback path.
pub async fn install_chain_hops(pool: &Pool, source: &str, depth: usize) -> Chain {
    assert!(depth >= 1, "a chain needs at least one hop");
    let prefix = source
        .strip_suffix("_src")
        .expect("source table name must be `<prefix>_src`, from create_chain_source_table");

    let columns = numeric_columns(&["id", "val"]);
    let mut hops = Vec::with_capacity(depth);
    let mut current_source = format!("public.{source}");

    for i in 1..=depth {
        let target = format!("{prefix}_h{i}");
        let source_text = format!("TRANSFORM {target} FROM {current_source} SELECT val AS val");
        let def = install_definition(pool, &source_text, &columns, "public")
            .await
            .unwrap_or_else(|e| panic!("install_definition({source_text:?}) failed: {e}"));
        assert_eq!(
            def.def.target, target,
            "install_definition must keep the bare target name"
        );
        hops.push(target);
        current_source = format!("public.{}", hops.last().expect("just pushed"));
    }

    Chain {
        source: source.to_string(),
        hops,
    }
}

/// Polls `transform_definitions.status` for `target` (bare name; the catalog
/// stores it fully qualified) until it reads `'live'`, or panics at
/// `deadline`. Needs a running `Client` with at least one application thread,
/// or it blocks forever — see [`install_chain_hops`].
pub async fn wait_for_live(raw: &RawClient, target: &str, deadline: Instant) {
    loop {
        let status: Option<String> = raw
            .query_opt(
                "select status from transform_definitions where target_table = $1",
                &[&format!("public.{target}")],
            )
            .await
            .expect("read transform_definitions.status")
            .map(|row| row.get(0));
        if status.as_deref() == Some("live") {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "transform public.{target} never reached 'live' status in time \
             (last observed: {status:?})"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// [`wait_for_live`] for every hop in `chain`, within `timeout` overall.
pub async fn wait_for_chain_live(raw: &RawClient, chain: &Chain, timeout: Duration) {
    let deadline = Instant::now() + timeout;
    for target in &chain.hops {
        wait_for_live(raw, target, deadline).await;
    }
}

/// Polls until `predicate_sql` (a `select 1 ... ` returning at most one row)
/// matches, or panics at `deadline` with `what` in the message.
async fn wait_for_row(raw: &RawClient, predicate_sql: &str, what: &str, deadline: Instant) {
    loop {
        let seen = raw
            .query_opt(predicate_sql, &[])
            .await
            .unwrap_or_else(|e| panic!("poll {what}: {e}"))
            .is_some();
        if seen {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "{what} never landed in time — the pipeline isn't flowing end to end \
             (check publication membership and transform status before trusting any \
             measurement from this run)"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Sends one reserved-id row ([`WARM_UP_ID`]) through the whole chain and
/// waits for it to reach the terminal hop — proving CDC intake -> ring ->
/// seal -> claim -> fold -> apply is actually flowing (in particular that
/// every intermediate hop has joined the publication via the periodic
/// `reconcile_source_tables` pass) before a measurement window opens.
pub async fn warm_up(raw: &RawClient, chain: &Chain, timeout: Duration) {
    raw.execute(
        &format!(
            "insert into public.{} (id, val) values ($1::bigint, $1::numeric)",
            chain.source
        ),
        &[&WARM_UP_ID],
    )
    .await
    .expect("insert warm-up row");

    wait_for_row(
        raw,
        &format!(
            "select 1 from public.{} where id = {WARM_UP_ID}",
            chain.terminal()
        ),
        "the warm-up row",
        Instant::now() + timeout,
    )
    .await;
}

/// Inserts one reserved-id row into an aggregate scenario's source table and
/// waits for its group to appear in `terminal`. Same discipline as
/// [`warm_up`], for a topology that isn't a [`Chain`].
pub async fn warm_up_aggregate(
    raw: &RawClient,
    source: &str,
    terminal: &str,
    group_column: &str,
    timeout: Duration,
) {
    raw.execute(
        &format!(
            "insert into public.{source} (id, {group_column}, val) \
             values ({WARM_UP_ID}, {WARM_UP_ID}, {WARM_UP_ID})"
        ),
        &[],
    )
    .await
    .expect("insert aggregate warm-up row");

    wait_for_row(
        raw,
        &format!("select 1 from public.{terminal} where {group_column} = {WARM_UP_ID}"),
        "the aggregate warm-up row",
        Instant::now() + timeout,
    )
    .await;
}

/// The independent oracle verdict for a 1-1 chain (every scenario carries
/// one, per #266: "an oracle check per run so a fast-but-wrong result
/// fails").
#[derive(Debug, Clone, Copy)]
pub struct ChainOracle {
    /// Rows in the source table (including [`WARM_UP_ID`]).
    pub source_rows: i64,
    /// Rows in the terminal hop's target table.
    pub terminal_rows: i64,
    /// Terminal rows whose `val` disagrees with the source row of the same
    /// `id`, or that have no source row at all. Must be `0`: the chain is a
    /// pure passthrough, so every landed row's value is fully determined.
    pub mismatched_rows: i64,
    /// Whether the chain converged completely (`terminal_rows ==
    /// source_rows`). Deliberately **not** part of [`ChainOracle::ok`]: a
    /// throughput probe that intentionally ran past saturation has a real
    /// backlog, and reporting that as a correctness failure would hide the
    /// distinction between "slow" and "wrong".
    pub fully_converged: bool,
}

impl ChainOracle {
    /// Correct as far as it got: nothing landed with a wrong value, and
    /// nothing landed that shouldn't have.
    pub fn ok(&self) -> bool {
        self.mismatched_rows == 0 && self.terminal_rows <= self.source_rows
    }
}

/// Checks the terminal hop's target against the source table directly — an
/// exact per-row value comparison in SQL, independent of anything the engine
/// computed or of any metric this harness scraped.
///
/// The chain is a passthrough at every hop, so `terminal.val` must equal
/// `source.val` for the same `id`, with no arithmetic to re-derive. That also
/// means this one check covers *every* hop: a wrong value introduced at hop 3
/// of 10 propagates to the terminal, where this sees it.
pub async fn check_chain_oracle(raw: &RawClient, chain: &Chain) -> ChainOracle {
    let source = &chain.source;
    let terminal = chain.terminal();

    let source_rows: i64 = raw
        .query_one(&format!("select count(*) from public.{source}"), &[])
        .await
        .expect("count chain source rows")
        .get(0);
    let terminal_rows: i64 = raw
        .query_one(&format!("select count(*) from public.{terminal}"), &[])
        .await
        .expect("count chain terminal rows")
        .get(0);
    let mismatched_rows: i64 = raw
        .query_one(
            &format!(
                "select count(*) from public.{terminal} t \
                 left join public.{source} s on s.id = t.id \
                 where s.id is null or s.val is distinct from t.val"
            ),
            &[],
        )
        .await
        .expect("compare chain terminal against source oracle")
        .get(0);

    ChainOracle {
        source_rows,
        terminal_rows,
        mismatched_rows,
        fully_converged: terminal_rows == source_rows,
    }
}
