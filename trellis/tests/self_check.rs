//! Integration tests for `Trellis::self_check` (issue #174, ADR-0013's
//! production recompute audit).
//!
//! A real, ephemeral Postgres instance per test (`testkit::TestCluster`).
//! Definitions and the audit itself go through the public `Trellis` facade.
//! Everything else "reaches past the mechanism, inserts directly", the way
//! `trellis/tests/column_quarantine.rs` seeds its own scenarios: corrupting a
//! target row, seeding `column_status`, and building the converged target in
//! the first place. [`converged_fixture`] stages CDC rows by hand and drains
//! them through the engine's own apply path instead of running a live
//! `Client` and waiting for it to converge (issue #299). See that helper for
//! why the wait was the expensive part.
//!
//! Only
//! `self_check_reports_not_caught_up_rather_than_a_false_divergence_for_a_lagging_target`
//! runs a live `Client`, because a genuinely lagging pipeline is its subject.
//! It never waits for convergence.
//!
//! The comparison's pure logic (page alignment, divergence classification,
//! the re-check's filter) is unit-tested in `staging::self_check::tests`
//! (`trellis/src/staging/self_check.rs`). These tests cover what needs
//! Postgres: that the rendered recompute SQL runs and agrees with what the
//! engine persisted, the keyset paging, and the paused-column read.
//!
//! **A deliberately out-of-scope race** (judgment call, flagged rather than
//! silently skipped): ADR-0013's re-check-on-divergence design also guards
//! against a *sub-transaction* race — a brand-new commit landing in the
//! narrow window between `self_check`'s own internal `await_converged`
//! succeeding and the `REPEATABLE READ` transaction it opens immediately
//! after actually establishing its snapshot. That window is microseconds
//! wide with no artificial delay hook in the production code to widen it
//! (adding one purely for this test wasn't judged worth the production-code
//! complexity), so it isn't reproduced deterministically here.
//! `self_check_reports_not_caught_up_rather_than_a_false_divergence_for_a_lagging_target`
//! below covers the coarser, reliably-reproducible half of the same
//! guarantee (a target that hasn't even begun to catch up must never be
//! compared at all), and `staging::self_check::tests`'
//! `divergence_identity_ignores_a_cells_persisted_and_recomputed_text` and
//! `reproduced_keeps_only_divergences_seen_on_both_passes` unit-test the
//! re-check's own matching logic directly.

use std::time::Duration;

use testkit::{TestCluster, TestDatabase};
use tokio_postgres::types::PgLsn;
use tokio_postgres::{Client, NoTls};
use trellis::config::DEFAULT_SCHEMA;
use trellis::staging::{StagedWatermark, apply, has_pending, retire_drained_segments, seal};
use trellis::{
    Config, Divergence, SelfCheckMode, SelfCheckOutcome, SelfCheckScope, Trellis, TrellisOptions,
};

/// Connects directly to `dsn` (bypassing `trellis::Pool`), matching
/// `trellis/tests/app_converge.rs`'s own helper of the same name.
async fn connect_raw(dsn: &str) -> Client {
    let (client, connection) = tokio_postgres::connect(dsn, NoTls).await.expect("connect");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
        .batch_execute(&format!("set search_path to {DEFAULT_SCHEMA}, public; set datestyle to 'ISO, YMD'; set bytea_output to 'hex'"))
        .await
        .expect("set search_path");
    client
}

/// Bounds `self_check`'s own internal convergence await. Every
/// [`converged_fixture`] target is already caught up when `self_check`
/// starts, so that await returns on its first poll and this is never
/// actually spent. It is a ceiling, not a wait.
const GENEROUS_TIMEOUT: Duration = Duration::from_secs(30);

/// Seals the active segment and drains it through the engine's own apply
/// path, repeating until nothing is pending — the hand-driven stand-in for a
/// running `Client`'s drain workers that `defs_backfill_chunk_queue.rs`
/// uses.
async fn drain_to_quiescence(pool: &trellis::Pool, client: &mut Client) {
    // No live `Intake` here (rows are staged by hand below), so there is no
    // real staged watermark to hold apply back. A saturated one never does.
    let watermark = StagedWatermark::saturated();
    for _ in 0..16 {
        let outcome = seal::seal_phase1(client).await.expect("seal phase 1");
        seal::seal_phase2(client, outcome.sealed_seg_seq, "wake")
            .await
            .expect("seal phase 2");
        while apply::drain_once(
            pool,
            outcome.sealed_seg_seq,
            "self_check_test",
            1,
            "trellis_self_check_test",
            &watermark,
        )
        .await
        .expect("drain_once")
        .is_some()
        {}
        retire_drained_segments(client)
            .await
            .expect("retire drained segments");
        if !has_pending(client).await.expect("has_pending") {
            return;
        }
    }
    panic!("pipeline did not reach quiescence within 16 seal/drain rounds");
}

/// A converged 1-1 target, built with no live `Client` and no wall-clock
/// wait.
///
/// Creates `source` from `source_ddl`, defines `transform` over it (still
/// empty, so the definition goes live immediately), then for each `(key,
/// image)` pair inserts the source row and stages the matching CDC insert
/// by hand, the same text-valued JSON image intake would stage for it
/// (`intake::tuple_to_json`). The ring is then drained through the engine's
/// real apply path, so every target value is one the engine itself computed
/// and wrote.
///
/// What this skips is the live `Client`: CDC intake and its
/// `replication_progress` advance. That advance is what made these tests
/// slow. On a quiet stream `confirmed_lsn` only reaches a fresh
/// `pg_current_wal_lsn()` token on intake's keepalive-driven persist, paced
/// at 10s (`Trellis::await_converged`'s doc comment), so every await cost
/// about 10s. Here the progress row is seeded directly, as
/// `quarantine.rs`/`apply_aggregate.rs` seed theirs, at the highest
/// possible LSN: intake has confirmed everything, and whether the target is
/// caught up rests on the ring alone. `self_check`'s own convergence await
/// still runs its real predicate; it just finds nothing pending.
async fn converged_fixture(
    db: &TestDatabase,
    source_ddl: &str,
    source: &str,
    transform: &str,
    rows: &[(&str, &str)],
) -> (Trellis, Client) {
    let mut raw = connect_raw(db.dsn()).await;
    raw.batch_execute(source_ddl)
        .await
        .expect("create source table");

    let trellis = Trellis::connect(
        Config::from_dsn(db.dsn().to_string()).expect("valid dsn"),
        TrellisOptions::default(),
    )
    .await
    .expect("connect");
    trellis.apply(transform).await.expect("define");
    let status: String = raw
        .query_one(
            "select status from transform_definitions \
             where split_part(source_table, '.', 2) = $1",
            &[&source],
        )
        .await
        .expect("read definition status")
        .get(0);
    assert_eq!(
        status, "live",
        "a definition over an empty source must go live without a backfill, or apply \
         would exclude the CDC rows staged below"
    );

    let active: i16 = raw
        .query_one("select ring_slot from segment_pointer", &[])
        .await
        .expect("read segment pointer")
        .get(0);
    let src_table = format!("{DEFAULT_SCHEMA}.{source}");
    for (key, image) in rows {
        raw.execute(
            &format!("insert into {source} select * from jsonb_populate_record(null::{source}, $1::text::jsonb)"),
            &[image],
        )
        .await
        .unwrap_or_else(|e| panic!("insert source row {key}: {e}"));
        raw.execute(
            &format!(
                "insert into seg_{active} (src_table, key, op, lsn, new_image, hop_gen) \
                 values ($1, $2, 'insert', $3, $4::text::jsonb, 0)"
            ),
            &[&src_table, key, &PgLsn::from(1u64), image],
        )
        .await
        .unwrap_or_else(|e| panic!("stage cdc {key}: {e}"));
    }
    drain_to_quiescence(&db.pool, &mut raw).await;

    raw.execute(
        "insert into replication_progress (slot_name, confirmed_lsn) \
         values ('self_check_test', 'FFFFFFFF/FFFFFFFF')",
        &[],
    )
    .await
    .expect("seed replication_progress");

    (trellis, raw)
}

/// A correctly-converged 1-1 target must report
/// [`SelfCheckOutcome::Converged`] — no divergence — the steady-state case
/// every other test in this file is a variation on.
#[tokio::test]
async fn converged_target_reports_no_divergence() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let (trellis, _raw) = converged_fixture(
        &db,
        "create table widgets (id integer primary key, price integer, tax integer)",
        "widgets",
        "TRANSFORM widget_totals FROM widgets SELECT price + tax AS total",
        &[
            ("1", r#"{"id":"1","price":"10","tax":"1"}"#),
            ("2", r#"{"id":"2","price":"20","tax":"2"}"#),
        ],
    )
    .await;

    let report = trellis
        .self_check(
            "widget_totals",
            SelfCheckScope {
                after: None,
                limit: 100,
            },
            SelfCheckMode::Standard,
            GENEROUS_TIMEOUT,
        )
        .await
        .expect("self_check");

    assert!(
        matches!(report.outcome, SelfCheckOutcome::Converged),
        "expected Converged, got {:?}",
        report.outcome
    );
    assert_eq!(report.rows_compared, 2);
    assert_eq!(
        report.next_after, None,
        "neither side hit the limit, so this page reached the end of the keyspace"
    );

    trellis.shutdown().await.expect("shutdown");
}

/// Issue #109's typed literals, through the production audit path.
///
/// `self_check` is the one place a *production* SQL renderer
/// (`staging::self_check::render_leaf`) turns a calculated-field expression
/// back into text, then diffs its `::text` rendering against the persisted
/// target column byte-for-byte with no normalization whatsoever
/// (`compare_once`). This test is what covers that renderer's typed-literal
/// arm end-to-end: it proves the rendered SQL is valid Postgres, that it
/// means the same thing as the value the engine actually persisted, and that
/// the audit therefore reports `Converged` rather than tripping over a shape
/// it doesn't understand.
///
/// What it deliberately does **not** prove is the canonical-form rule
/// `defs::typed_literal` imposes. Both legs of `compare_once` are rendered
/// by Postgres, in the same session, with `date_out` applied to each — so a
/// non-canonical literal would be normalized identically on both sides and
/// cancel out. The comparisons that genuinely depend on canonical form are
/// the ones where a *Rust*-produced string meets a Postgres-produced one:
/// the generative suite's `evaluator_vs_sql` leg, and
/// `defs_typed_literals.rs`'s own
/// `evaluator_and_sql_oracle_agree_on_every_literal`.
///
/// Goes through the public `define` front door, so it also pins that the
/// whole grammar addition is reachable by an ordinary embedder and not just
/// by the engine-internal `parse`/`install_definition` pair.
#[tokio::test]
async fn a_converged_target_of_typed_literals_reports_no_divergence() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let (trellis, raw) = converged_fixture(
        &db,
        "create table widgets (id integer primary key, price integer)",
        "widgets",
        "TRANSFORM widget_stamps FROM widgets \
         SELECT DATE '2024-01-01' AS effective_on, \
                TIMESTAMP '2024-03-05 12:34:56.5' AS recorded_at, \
                CAST('\\x0102ff' AS bytea) AS tag",
        &[
            ("1", r#"{"id":"1","price":"10"}"#),
            ("2", r#"{"id":"2","price":"20"}"#),
        ],
    )
    .await;

    // The target's columns really are their own Postgres types, not text —
    // the "computed 1-1 target" role, asserted here through the same public
    // path an embedder would use to build it.
    for (column, expected) in [
        ("effective_on", "date"),
        ("recorded_at", "timestamp without time zone"),
        ("tag", "bytea"),
    ] {
        let data_type: String = raw
            .query_one(
                "select data_type from information_schema.columns \
                 where table_name = 'widget_stamps' and column_name = $1",
                &[&column],
            )
            .await
            .unwrap_or_else(|e| panic!("introspect {column}: {e}"))
            .get(0);
        assert_eq!(
            data_type, expected,
            "{column} must be a real {expected} column"
        );
    }

    let report = trellis
        .self_check(
            "widget_stamps",
            SelfCheckScope {
                after: None,
                limit: 100,
            },
            SelfCheckMode::Standard,
            GENEROUS_TIMEOUT,
        )
        .await
        .expect("self_check");

    assert!(
        matches!(report.outcome, SelfCheckOutcome::Converged),
        "a target built entirely from typed literals must audit clean, got {:?}",
        report.outcome
    );
    assert_eq!(report.rows_compared, 2);

    trellis.shutdown().await.expect("shutdown");
}

/// A target row corrupted directly via raw SQL (bypassing the engine
/// entirely, the way `trellis/tests/quarantine.rs`/`column_quarantine.rs`
/// seed their own scenarios) must be caught: `self_check` reports a
/// [`Divergence::Cell`] whose `persisted` half is the corrupted value and
/// whose `recomputed` half is the value the source data actually implies.
/// Run under [`SelfCheckMode::Standard`] (not [`SelfCheckMode::Strict`]) —
/// with nothing else writing to `widgets`/`widget_totals` after the
/// corruption, this also proves the re-check pass doesn't spuriously erase a
/// genuine, stable divergence.
#[tokio::test]
async fn self_check_detects_a_divergence_seeded_by_directly_corrupting_a_target_row() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let (trellis, raw) = converged_fixture(
        &db,
        "create table widgets (id integer primary key, price integer, tax integer)",
        "widgets",
        "TRANSFORM widget_totals FROM widgets SELECT price + tax AS total",
        &[
            ("1", r#"{"id":"1","price":"10","tax":"1"}"#),
            ("2", r#"{"id":"2","price":"20","tax":"2"}"#),
        ],
    )
    .await;

    // Directly corrupt row 1's persisted total — the correct value is 11
    // (10 + 1); the engine never wrote 9999, this test does.
    raw.execute("update widget_totals set total = 9999 where id = '1'", &[])
        .await
        .expect("corrupt target row");

    let report = trellis
        .self_check(
            "widget_totals",
            SelfCheckScope {
                after: None,
                limit: 100,
            },
            SelfCheckMode::Standard,
            GENEROUS_TIMEOUT,
        )
        .await
        .expect("self_check");

    match report.outcome {
        SelfCheckOutcome::Diverged(divergences) => {
            assert_eq!(
                divergences,
                vec![Divergence::Cell {
                    key: "1".to_string(),
                    column: "total".to_string(),
                    persisted: Some("9999".to_string()),
                    recomputed: Some("11".to_string()),
                }]
            );
        }
        other => panic!("expected Diverged, got {other:?}"),
    }

    trellis.shutdown().await.expect("shutdown");
}

/// A target that has real, unapplied source work pending — i.e. is merely
/// lagging, not wrong — must report [`SelfCheckOutcome::NotCaughtUp`], never
/// [`SelfCheckOutcome::Diverged`] (ADR-0013: "the correctness promise is
/// conditional on being caught up ... self_check must never report a
/// merely-lagging target as diverged"). Starts `running` staging-only (zero
/// drain workers, mirroring `trellis/tests/app_converge.rs`'s own timeout
/// test), so the inserted row is genuinely staged and sealed but never
/// applied — deterministic, not a timing coincidence: nothing in this test
/// could possibly converge before `self_check`'s own short timeout expires.
#[tokio::test]
async fn self_check_reports_not_caught_up_rather_than_a_false_divergence_for_a_lagging_target() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;

    db.pool
        .get()
        .await
        .expect("connection")
        .batch_execute("create table widgets (id integer primary key, price integer)")
        .await
        .expect("create source table");

    let definer = Trellis::connect(
        Config::from_dsn(db.dsn().to_string()).expect("valid dsn"),
        TrellisOptions::default(),
    )
    .await
    .expect("connect definer");
    definer
        .apply("TRANSFORM widget_prices FROM widgets SELECT price AS price")
        .await
        .expect("define");
    definer.shutdown().await.expect("shutdown definer");

    // Staging only: CDC intake + ring maintenance, but zero drain workers —
    // nothing will ever apply this row.
    let running = Trellis::connect(
        Config::from_dsn(db.dsn().to_string()).expect("valid dsn"),
        TrellisOptions {
            staging: true,
            drain_threads: 0,
            ..Default::default()
        },
    )
    .await
    .expect("connect running (staging only)");

    let raw = connect_raw(db.dsn()).await;
    raw.execute("insert into widgets (id, price) values (1, 9)", &[])
        .await
        .expect("insert source row");

    let report = running
        .self_check(
            "widget_prices",
            SelfCheckScope {
                after: None,
                limit: 100,
            },
            SelfCheckMode::Standard,
            Duration::from_millis(150),
        )
        .await
        .expect("self_check");

    assert!(
        matches!(report.outcome, SelfCheckOutcome::NotCaughtUp),
        "a target with real, unapplied work pending must report NotCaughtUp rather than \
         diverging on a row that legitimately hasn't landed yet — got {:?}",
        report.outcome
    );
    assert_eq!(
        report.rows_compared, 0,
        "NotCaughtUp must short-circuit before any comparison runs"
    );

    running.shutdown().await.expect("shutdown running");
}

/// A currently-paused column's deliberately-stale persisted value must be
/// excluded from the comparison entirely (ADR-0013: "auditing it would
/// report a false divergence on exactly the targets an operator is most
/// likely to be inspecting"). Seeds `column_status` directly via raw SQL,
/// the way `trellis/tests/column_quarantine.rs` seeds its own pause
/// scenarios, rather than driving a real column fuse trip end to end.
#[tokio::test]
async fn self_check_excludes_a_paused_column_from_the_comparison() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let (trellis, raw) = converged_fixture(
        &db,
        "create table widgets (id integer primary key, price integer, tax integer)",
        "widgets",
        "TRANSFORM widget_view FROM widgets SELECT price AS price, tax AS tax",
        &[("1", r#"{"id":"1","price":"10","tax":"1"}"#)],
    )
    .await;

    // Corrupt `tax` (a stand-in for "the value it was frozen to when its
    // fuse tripped") and mark it paused — `self_check` must not report this
    // as a divergence, since it's *supposed* to be stale while paused.
    raw.execute("update widget_view set tax = -1 where id = '1'", &[])
        .await
        .expect("corrupt the soon-to-be-paused column");
    raw.execute(
        "insert into column_status (transform_table, column_name, last_error, local_fuse) \
         values ('widget_view', 'tax', 'seeded directly for self_check test', true)",
        &[],
    )
    .await
    .expect("seed column_status");

    let report = trellis
        .self_check(
            "widget_view",
            SelfCheckScope {
                after: None,
                limit: 100,
            },
            SelfCheckMode::Standard,
            GENEROUS_TIMEOUT,
        )
        .await
        .expect("self_check");

    assert!(
        matches!(report.outcome, SelfCheckOutcome::Converged),
        "a paused column's stale value must be excluded from comparison, not reported as a \
         divergence — got {:?}",
        report.outcome
    );

    trellis.shutdown().await.expect("shutdown");
}

/// Regression, keyset page alignment: the recompute read and the persisted
/// read are two independently-`LIMIT`ed queries, so as soon as a real
/// divergence makes their key sets differ, their pages end at *different*
/// keys. Here the target is missing row `2` and the limit is 3, so the
/// recompute page is `{1,2,3}` while the persisted page is `{1,3,4}`.
/// Diffing those raw would report key `4` as an `ExtraRow` purely because it
/// fell off the recompute side's page — a deterministic false divergence
/// (it reproduces on the re-check pass, so the ADR's re-check can't filter
/// it), and `next_after` would then skip past `4` entirely, so the following
/// page would never compare it honestly either. Only the genuine
/// `MissingRow { 2 }` may be reported, and `next_after` must land on `3`.
///
/// `staging::self_check`'s own
/// `diff_page_does_not_invent_a_divergence_from_the_two_sides_ending_at_different_keys`
/// unit-tests the alignment itself. This end of it covers the real keyset
/// SQL, and that the next page picks up exactly where this one stopped.
#[tokio::test]
async fn a_bounded_page_does_not_invent_a_divergence_from_the_two_sides_ending_at_different_keys() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let (trellis, raw) = converged_fixture(
        &db,
        "create table widgets (id integer primary key, price integer)",
        "widgets",
        "TRANSFORM widget_prices FROM widgets SELECT price AS price",
        &[
            ("1", r#"{"id":"1","price":"10"}"#),
            ("2", r#"{"id":"2","price":"20"}"#),
            ("3", r#"{"id":"3","price":"30"}"#),
            ("4", r#"{"id":"4","price":"40"}"#),
            ("5", r#"{"id":"5","price":"50"}"#),
        ],
    )
    .await;

    // Delete one persisted row directly, bypassing the engine — a genuine
    // MissingRow, seeded the same way the corruption test above seeds its
    // own Cell divergence.
    raw.execute("delete from widget_prices where id = '2'", &[])
        .await
        .expect("delete a target row");

    let report = trellis
        .self_check(
            "widget_prices",
            SelfCheckScope {
                after: None,
                limit: 3,
            },
            SelfCheckMode::Standard,
            GENEROUS_TIMEOUT,
        )
        .await
        .expect("self_check");

    match report.outcome {
        SelfCheckOutcome::Diverged(divergences) => assert_eq!(
            divergences,
            vec![Divergence::MissingRow {
                key: "2".to_string()
            }],
            "only the genuine missing row may be reported; a key that merely fell off one \
             side's bounded page is not a divergence"
        ),
        other => panic!("expected Diverged with the missing row, got {other:?}"),
    }
    assert_eq!(
        report.next_after,
        Some("3".to_string()),
        "the page ends at the lower of the two sides' last keys, so key 4 is picked up whole \
         by the next call rather than skipped"
    );

    // And the next page genuinely continues from there, with no gap: keys 4
    // and 5 are both intact, so it converges.
    let next = trellis
        .self_check(
            "widget_prices",
            SelfCheckScope {
                after: report.next_after.clone(),
                limit: 3,
            },
            SelfCheckMode::Standard,
            GENEROUS_TIMEOUT,
        )
        .await
        .expect("self_check page 2");
    assert!(
        matches!(next.outcome, SelfCheckOutcome::Converged),
        "expected the second page to converge, got {:?}",
        next.outcome
    );
    assert_eq!(next.rows_compared, 2, "keys 4 and 5, neither skipped");
    assert_eq!(next.next_after, None, "end of the keyspace");

    trellis.shutdown().await.expect("shutdown");
}

/// A zero (or negative) `limit` compares nothing at all, so reporting a
/// cheerful `Converged` over an empty page would be an audit that silently
/// checked nothing — refuse instead.
#[tokio::test]
async fn self_check_refuses_a_non_positive_limit_rather_than_vacuously_converging() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;

    db.pool
        .get()
        .await
        .expect("connection")
        .batch_execute("create table widgets (id integer primary key, price integer)")
        .await
        .expect("create source table");

    let trellis = Trellis::connect(
        Config::from_dsn(db.dsn().to_string()).expect("valid dsn"),
        TrellisOptions::default(),
    )
    .await
    .expect("connect");
    trellis
        .apply("TRANSFORM widget_prices FROM widgets SELECT price AS price")
        .await
        .expect("define");

    let err = trellis
        .self_check(
            "widget_prices",
            SelfCheckScope {
                after: None,
                limit: 0,
            },
            SelfCheckMode::Standard,
            GENEROUS_TIMEOUT,
        )
        .await
        .expect_err("a zero limit must be refused");
    assert_eq!(err.code(), trellis::ErrorCode::Validation);
    assert!(
        err.to_string().contains("positive"),
        "expected a limit-specific message, got {err}"
    );

    trellis.shutdown().await.expect("shutdown");
}

/// ADR-0013's "1-1 first, then aggregates and relationships": an aggregate
/// target is out of scope for this slice, and `self_check` must *refuse* it
/// rather than silently run a plain keyset comparison that would mis-audit
/// the group-key space (every group would read as a divergence). A tool that
/// silently mis-audits an unsupported shape is worse than one that declines.
#[tokio::test]
async fn self_check_refuses_an_aggregate_target_rather_than_mis_auditing_it() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;

    db.pool
        .get()
        .await
        .expect("connection")
        .batch_execute(
            "create table orders (id integer primary key, region text); \
             alter table orders replica identity full",
        )
        .await
        .expect("create source table");

    let trellis = Trellis::connect(
        Config::from_dsn(db.dsn().to_string()).expect("valid dsn"),
        TrellisOptions::default(),
    )
    .await
    .expect("connect");
    trellis
        .apply("TRANSFORM region_counts FROM orders GROUP BY region SELECT region AS region, COUNT(*) AS n")
        .await
        .expect("define aggregate transform");

    let err = trellis
        .self_check(
            "region_counts",
            SelfCheckScope {
                after: None,
                limit: 100,
            },
            SelfCheckMode::Standard,
            GENEROUS_TIMEOUT,
        )
        .await
        .expect_err("an aggregate target must be refused, not audited");
    assert_eq!(err.code(), trellis::ErrorCode::Validation);
    assert!(
        err.to_string().contains("aggregate"),
        "expected a scope-specific message naming the aggregate shape, got {err}"
    );

    trellis.shutdown().await.expect("shutdown");
}
