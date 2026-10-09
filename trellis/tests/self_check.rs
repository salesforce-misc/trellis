//! Integration tests for `Trellis::self_check` (issue #174, ADR-0013's
//! production recompute audit).
//!
//! A real, ephemeral Postgres instance per test (`testkit::TestCluster`).
//! Definitions and the audit itself go through the public `Trellis` facade.
//! Everything else "reaches past the mechanism, inserts directly", the way
//! `trellis/tests/column_quarantine.rs` seeds its own scenarios: corrupting a
//! target row, seeding `column_status`, and building the converged target in
//! the first place. [`converged_fixture`] installs the source's capture
//! triggers, lets them stage the rows it writes, and drains them through the
//! engine's own apply path instead of running a live `Client` and waiting
//! for it to converge (issue #299). See that helper for why the wait was
//! the expensive part.
//!
//! The capture-audit tests at the end (#622 C9) break a converged target's
//! capture from the catalog side: a disabled or dropped trigger, a function
//! handed to another owner, a revoked privilege, a partition or inheritance
//! hierarchy. No staging worker runs to repair any of it, so each report is
//! read once, with nothing to wait for.
//!
//! Only
//! `self_check_reports_not_caught_up_rather_than_a_false_divergence_for_a_lagging_target`
//! runs a live `Client`, because a genuinely lagging pipeline is its subject.
//! It never waits for convergence.
//!
//! The comparison's pure logic (divergence classification, the re-check's
//! filter) is unit-tested in `staging::self_check::tests`
//! (`trellis/src/staging/self_check.rs`). These tests cover what needs
//! Postgres: that the rendered recompute SQL runs and agrees with what the
//! engine persisted, the keyset paging and its page alignment (under the
//! key's collation, #782), and the paused-column read.
//!
//! **A deliberately out-of-scope race** (judgment call, flagged rather than
//! silently skipped): ADR-0013's re-check-on-divergence design also guards
//! against a *sub-transaction* race — a brand-new commit landing in the
//! narrow window between `self_check`'s own internal `await_converged`
//! succeeding and the statement it runs immediately after actually
//! establishing its snapshot. That window is microseconds
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
use tokio_postgres::{Client, NoTls};
use trellis::config::DEFAULT_SCHEMA;
use trellis::defs::hierarchy::Hierarchy;
use trellis::staging::{StagedWatermark, apply, has_pending, retire_drained_segments, seal};
use trellis::{
    CaptureFault, Config, Divergence, SelfCheckMode, SelfCheckOutcome, SelfCheckScope,
    TransformStatus, Trellis, TrellisOptions,
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

/// The convergence deadline for the #592 test that blocks the wait on a
/// lock and terminates it. Never spent: the test terminates the wait within
/// its 30s lookup bound. It only has to outlast that bound, so the wait is
/// still running when the terminate lands, since #596 enforces the deadline
/// mid-poll.
const LOCKED_WAIT_TIMEOUT: Duration = Duration::from_secs(600);

/// Seals the active segment and drains it through the engine's own apply
/// path, repeating until nothing is pending — the hand-driven stand-in for a
/// running `Client`'s drain workers that `defs_backfill_chunk_queue.rs`
/// uses.
async fn drain_to_quiescence(pool: &trellis::Pool, client: &mut Client) {
    // Capture triggers stage in the writer's own transaction, so there is
    // no staged watermark to hold apply back. A saturated one never does.
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

/// Installs `table`'s capture triggers with one capture reconcile pass, the
/// staging worker's, which also parks the table's join marker.
async fn capture(client: &mut Client, table: &str) {
    capture_tables(client, &[table]).await;
}

/// [`capture`] for several tables in one pass. The pass reconciles to exactly
/// the list it is given, so every table a test captures goes in one call.
async fn capture_tables(client: &mut Client, tables: &[&str]) {
    let tables: Vec<String> = tables.iter().map(|t| t.to_string()).collect();
    let outcome = trellis::capture::reconcile::reconcile(
        client,
        DEFAULT_SCHEMA,
        &tables,
        std::time::Instant::now() + Duration::from_secs(5),
    )
    .await
    .expect("capture pass");
    assert!(
        outcome.failed.is_empty() && outcome.waiting.is_empty(),
        "{outcome:?}"
    );
}

/// A converged 1-1 target, built with no live `Client` and no wall-clock
/// wait.
///
/// Creates `source` from `source_ddl`, defines `transform` over it (still
/// empty, so the definition goes live immediately), installs the source's
/// capture triggers, then inserts each `(key, image)` pair's source row,
/// which the triggers stage. The ring is then drained through the engine's
/// real apply path, so every target value is one the engine itself computed
/// and wrote.
///
/// What this skips is the live `Client`, whose staging worker would also
/// repair a trigger a test breaks, so whether the target is caught up
/// rests on the ring alone. `self_check`'s own convergence await still runs
/// its real predicate; it just finds nothing pending.
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
    // No staging worker runs here: stand in for its capture pass, which
    // installs the source's capture triggers, and for its build, which
    // takes a definition over an empty source straight to `live` (#625 F8a:
    // a plain 1-1 over a captured table is the Re-derive build's, whose plan
    // job finds no rows).
    let src_table = format!("{DEFAULT_SCHEMA}.{source}");
    capture(&mut raw, &src_table).await;
    trellis::intake::markers::settle_registrations(&db.pool).await;
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

    // The capture triggers stage each insert.
    for (key, image) in rows {
        raw.execute(
            &format!("insert into {source} select * from jsonb_populate_record(null::{source}, $1::text::jsonb)"),
            &[image],
        )
        .await
        .unwrap_or_else(|e| panic!("insert source row {key}: {e}"));
    }
    drain_to_quiescence(&db.pool, &mut raw).await;

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

/// A definition that isn't `live` isn't compared (#625 F7, comment 2): a
/// rebuild a repair starts is visible from the call's return, so the audit
/// says so rather than comparing a target the rebuild is about to change, and
/// once the rebuild is done the same audit compares and converges. Nothing is
/// awaited for the `NotLive` report, so it needs no drain and no timeout.
#[tokio::test]
async fn self_check_reports_a_definition_under_a_rebuild_as_not_live() {
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
        ],
    )
    .await;
    let check = || async {
        trellis
            .self_check(
                "widget_prices",
                SelfCheckScope {
                    after: None,
                    limit: 100,
                },
                SelfCheckMode::Standard,
                GENEROUS_TIMEOUT,
            )
            .await
            .expect("self_check")
    };

    trellis
        .request_backfill("widgets")
        .await
        .expect("request a rebuild");
    let report = check().await;
    assert!(
        matches!(
            report.outcome,
            SelfCheckOutcome::NotLive(TransformStatus::Backfilling)
        ),
        "expected NotLive(Backfilling), got {:?}",
        report.outcome
    );
    assert_eq!(report.rows_compared, 0, "nothing was compared");
    assert_eq!(report.next_after, None);

    trellis::intake::markers::settle_builds(&db.pool).await;
    drain_to_quiescence(&db.pool, &mut { raw }).await;
    let report = check().await;
    assert!(
        matches!(report.outcome, SelfCheckOutcome::Converged),
        "expected Converged once live, got {:?}",
        report.outcome
    );
    assert_eq!(report.rows_compared, 2);

    trellis.shutdown().await.expect("shutdown");
}

/// Likewise a frozen definition: its target is deliberately not maintained.
#[tokio::test]
async fn self_check_reports_a_paused_definition_as_not_live() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let (trellis, _raw) = converged_fixture(
        &db,
        "create table widgets (id integer primary key, price integer)",
        "widgets",
        "TRANSFORM widget_prices FROM widgets SELECT price AS price",
        &[("1", r#"{"id":"1","price":"10"}"#)],
    )
    .await;
    trellis
        .apply("PAUSE TRANSFORM widget_prices")
        .await
        .expect("pause");

    let report = trellis
        .self_check(
            "widget_prices",
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
        matches!(
            report.outcome,
            SelfCheckOutcome::NotLive(TransformStatus::Paused)
        ),
        "expected NotLive(Paused), got {:?}",
        report.outcome
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
    // The definition goes `live` over the empty source before the staging
    // worker starts: with no drain workers it could never run a build, and a
    // definition that isn't `live` is not compared (#625 F7).
    let mut setup = connect_raw(db.dsn()).await;
    capture(&mut setup, &format!("{DEFAULT_SCHEMA}.widgets")).await;
    trellis::intake::markers::settle_registrations(&db.pool).await;

    // Staging only: capture + ring maintenance, but zero drain workers —
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

/// Issue #592: a database failure during `self_check`'s convergence wait
/// must come back as `Err`, not as the benign
/// [`SelfCheckOutcome::NotCaughtUp`]. Otherwise a caller polling `self_check`
/// keeps seeing "not caught up" while the audit can't run at all.
///
/// A raw session holds an `ACCESS EXCLUSIVE` lock on `poison_held`, which
/// the wait's `converged_through` query reads, so that query blocks on the
/// lock. None of `self_check`'s earlier reads touch `poison_held`, and nothing
/// else in this fixture reads it (the drain only writes it when it parks a
/// poisoned key). The test finds the blocked backend in `pg_locks`, terminates
/// it, and only then releases the lock.
///
/// `await_converged` bounds each poll by the remaining deadline (issue #596),
/// so a blocked wait that outlives it ends in `ConvergenceTimeout`, which
/// this test would read as `NotCaughtUp`. So the wait's deadline,
/// [`LOCKED_WAIT_TIMEOUT`], is far longer than the lookup loop's own 30s
/// bound: the terminate always lands well inside it.
#[tokio::test]
async fn self_check_reports_a_connection_lost_during_its_convergence_wait_as_an_error() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let (trellis, raw) = converged_fixture(
        &db,
        "create table widgets (id integer primary key, price integer)",
        "widgets",
        "TRANSFORM widget_prices FROM widgets SELECT price AS price",
        &[("1", r#"{"id": "1", "price": "9"}"#)],
    )
    .await;

    raw.batch_execute("begin; lock table poison_held in access exclusive mode")
        .await
        .expect("lock poison_held");

    let audit = tokio::spawn(async move {
        let result = trellis
            .self_check(
                "widget_prices",
                SelfCheckScope {
                    after: None,
                    limit: 100,
                },
                SelfCheckMode::Strict,
                LOCKED_WAIT_TIMEOUT,
            )
            .await;
        (trellis, result)
    });

    // Wait for the convergence query to queue behind the lock. This loop
    // only paces the lookup: the query stays blocked until the terminate
    // below, so how long this takes doesn't change the outcome.
    let mut blocked = None;
    for _ in 0..600 {
        let pids: Vec<i32> = raw
            .query(
                "select pid from pg_locks \
                 where relation = 'poison_held'::regclass and not granted",
                &[],
            )
            .await
            .expect("read pg_locks")
            .into_iter()
            .map(|row| row.get(0))
            .collect();
        if !pids.is_empty() {
            assert_eq!(pids.len(), 1, "only self_check's wait reads poison_held");
            blocked = Some(pids[0]);
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let pid = blocked.expect("self_check's convergence wait never blocked on poison_held");

    let terminated: bool = raw
        .query_one("select pg_terminate_backend($1)", &[&pid])
        .await
        .expect("terminate the waiting backend")
        .get(0);
    assert!(terminated, "pg_terminate_backend({pid}) failed");
    raw.batch_execute("rollback")
        .await
        .expect("release the lock");

    let (trellis, result) = audit.await.expect("self_check task");
    let err = match result {
        Ok(report) => panic!(
            "a connection lost during the convergence wait must be an error, not a report; \
             got {:?}",
            report.outcome
        ),
        Err(err) => err,
    };
    assert_eq!(
        err.code(),
        trellis::ErrorCode::Connectivity,
        "a terminated backend is a connectivity failure, got {err}"
    );

    trellis.shutdown().await.expect("shutdown");
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
/// The alignment is done in SQL (`staging::self_check::page_sql`), so this
/// covers it, and that the next page picks up exactly where this one
/// stopped. The tests under "Paging under the key's collation" below cover
/// it under a key collation that isn't byte order.
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
        .batch_execute("create table orders (id integer primary key, region text)")
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

/// #622 C9: [`converged_fixture`]'s `widget_totals` over `widgets`, with its
/// capture installed and nothing diverged.
async fn widgets_fixture(db: &TestDatabase) -> (Trellis, Client) {
    converged_fixture(
        db,
        "create table widgets (id integer primary key, price integer, tax integer)",
        "widgets",
        "TRANSFORM widget_totals FROM widgets SELECT price + tax AS total",
        &[("1", r#"{"id":"1","price":"10","tax":"1"}"#)],
    )
    .await
}

/// Audits `widget_totals` once, under `Strict`: nothing writes, and no
/// staging worker runs.
async fn audit_widgets(trellis: &Trellis) -> trellis::SelfCheckReport {
    trellis
        .self_check(
            "widget_totals",
            SelfCheckScope {
                after: None,
                limit: 100,
            },
            SelfCheckMode::Strict,
            GENEROUS_TIMEOUT,
        )
        .await
        .expect("self_check")
}

/// The capture faults `report` carries, or a panic naming what it carried
/// instead. A capture fault is reported before, and instead of, any
/// comparison, so no row was compared.
fn capture_faults(report: &trellis::SelfCheckReport) -> Vec<CaptureFault> {
    let SelfCheckOutcome::Diverged(divergences) = &report.outcome else {
        panic!("expected capture faults, got {:?}", report.outcome);
    };
    assert_eq!(report.rows_compared, 0, "{report:?}");
    divergences
        .iter()
        .map(|d| match d {
            Divergence::Capture(fault) => fault.clone(),
            other => panic!("expected only capture faults, got {other:?} in {divergences:?}"),
        })
        .collect()
}

const WIDGETS: &str = "trellis.widgets";

/// #622 C9 (acceptance A5): a capture trigger an operator disabled by name
/// is reported, and so is one set to plain `ENABLE`, which stops firing in
/// a `session_replication_role = replica` session. The target row is
/// corrupted as well, to show the comparison that would find it never runs.
#[tokio::test]
async fn a_disabled_capture_trigger_is_reported_before_any_comparison() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let (trellis, raw) = widgets_fixture(&db).await;
    assert!(
        matches!(
            audit_widgets(&trellis).await.outcome,
            SelfCheckOutcome::Converged
        ),
        "the fixture starts converged"
    );

    raw.batch_execute(
        "alter table widgets disable trigger trellis_capture_update; \
         alter table widgets enable trigger trellis_capture_delete; \
         alter table widgets disable trigger trellis_capture_begin; \
         update widget_totals set total = 0",
    )
    .await
    .expect("disable and re-enable capture triggers, and corrupt the target");

    let report = audit_widgets(&trellis).await;
    assert_eq!(
        capture_faults(&report),
        vec![
            CaptureFault::TriggerNotAlways {
                table: WIDGETS.to_string(),
                trigger: "trellis_capture_update".to_string(),
                enabled: "D".to_string(),
            },
            CaptureFault::TriggerNotAlways {
                table: WIDGETS.to_string(),
                trigger: "trellis_capture_delete".to_string(),
                enabled: "O".to_string(),
            },
            // #623 D8a: without it every capture re-reads.
            CaptureFault::TriggerNotAlways {
                table: WIDGETS.to_string(),
                trigger: "trellis_capture_begin".to_string(),
                enabled: "D".to_string(),
            },
        ]
    );

    trellis.shutdown().await.expect("shutdown");
}

/// #622 C9: a dropped capture trigger is reported.
#[tokio::test]
async fn a_dropped_capture_trigger_is_reported() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let (trellis, raw) = widgets_fixture(&db).await;

    raw.batch_execute("drop trigger trellis_capture_insert on widgets")
        .await
        .expect("drop a capture trigger");

    assert_eq!(
        capture_faults(&audit_widgets(&trellis).await),
        vec![CaptureFault::MissingTrigger {
            table: WIDGETS.to_string(),
            trigger: "trellis_capture_insert".to_string(),
        }]
    );

    trellis.shutdown().await.expect("shutdown");
}

/// #622 C9 (acceptance A5): the Trellis role losing a privilege the capture
/// functions use is reported.
///
/// The test cluster's Trellis role is a superuser, which holds every
/// privilege whatever is revoked, so the test hands the ring to an ordinary
/// role first. The functions still belong to the superuser then, which is
/// reported as mis-owned: they belong to the ring's owner (issue #701), and
/// the schema's owner doesn't count. Once they belong to the new role too,
/// and it holds exactly the grants the audit expects, the target converges;
/// revoking `INSERT` on one ring segment, or `SELECT` on the source, which
/// the functions re-read (#623 D8a), is then reported.
#[tokio::test]
async fn a_revoked_capture_privilege_and_a_mis_owned_function_are_reported() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let (trellis, raw) = widgets_fixture(&db).await;
    let role = "self_check_capture_owner";
    raw.batch_execute(&format!(
        "do $$ begin create role {role}; \
         exception when duplicate_object then null; end $$; \
         alter table trellis.seg_0 owner to {role}; \
         alter table trellis.seg_1 owner to {role}; \
         alter table trellis.seg_2 owner to {role}; \
         alter table trellis.seg_3 owner to {role}; \
         alter sequence trellis.staging_change_id_seq owner to {role}; \
         alter sequence trellis.ring_slot_mirror owner to {role}"
    ))
    .await
    .expect("hand the ring to an ordinary role");

    let functions: Vec<String> = trellis::capture::sql::CaptureEvent::ALL
        .iter()
        .map(|event| {
            format!(
                "trellis.{}",
                trellis::capture::sql::function_name(WIDGETS, *event).expect("function name")
            )
        })
        .collect();
    assert_eq!(
        capture_faults(&audit_widgets(&trellis).await),
        functions
            .iter()
            .map(|function| CaptureFault::FunctionOwner {
                table: WIDGETS.to_string(),
                function: function.clone(),
                owner: "postgres".to_string(),
                expected: role.to_string(),
            })
            .collect::<Vec<_>>(),
    );

    for function in &functions {
        raw.batch_execute(&format!("alter function {function}() owner to {role}"))
            .await
            .expect("hand a capture function to the role");
    }
    raw.batch_execute(&format!(
        "grant usage on schema trellis to {role}; grant select on {WIDGETS} to {role}"
    ))
    .await
    .expect("grant the role the schema and the source, the things it doesn't own");
    let report = audit_widgets(&trellis).await;
    assert!(
        matches!(report.outcome, SelfCheckOutcome::Converged),
        "with the functions owned by the ring's owner, and its grants, capture is whole: {:?}",
        report.outcome
    );

    raw.batch_execute(&format!("revoke insert on trellis.seg_0 from {role}"))
        .await
        .expect("revoke insert on a ring segment");
    assert_eq!(
        capture_faults(&audit_widgets(&trellis).await),
        vec![CaptureFault::MissingPrivilege {
            role: role.to_string(),
            privilege: "INSERT".to_string(),
            object: "trellis.seg_0".to_string(),
        }]
    );

    raw.batch_execute(&format!(
        "grant insert on trellis.seg_0 to {role}; revoke select on {WIDGETS} from {role}"
    ))
    .await
    .expect("revoke select on the source");
    assert_eq!(
        capture_faults(&audit_widgets(&trellis).await),
        vec![CaptureFault::MissingPrivilege {
            role: role.to_string(),
            privilege: "SELECT".to_string(),
            object: WIDGETS.to_string(),
        }]
    );

    trellis.shutdown().await.expect("shutdown");
}

/// #622 C9: a source attached as a partition after its definition was
/// accepted is reported. A write through the partitioned parent fires the
/// parent's statement triggers, not the source's.
#[tokio::test]
async fn a_source_attached_as_a_partition_is_reported() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let (trellis, raw) = widgets_fixture(&db).await;

    raw.batch_execute(
        "create table widgets_all (id integer not null, price integer, tax integer) \
             partition by range (id); \
         alter table widgets_all attach partition widgets \
             for values from (minvalue) to (maxvalue)",
    )
    .await
    .expect("attach the source as a partition");

    assert_eq!(
        capture_faults(&audit_widgets(&trellis).await),
        vec![CaptureFault::Hierarchy(Hierarchy::Partition {
            table: WIDGETS.to_string(),
            parent: "trellis.widgets_all".to_string(),
        })]
    );

    trellis.shutdown().await.expect("shutdown");
}

/// #622 C9: a source that joined an inheritance hierarchy after its
/// definition was accepted is reported, as a child and as a parent.
#[tokio::test]
async fn a_source_in_an_inheritance_hierarchy_is_reported() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let (trellis, raw) = widgets_fixture(&db).await;

    raw.batch_execute(
        "create table widgets_base (id integer, price integer, tax integer); \
         alter table widgets inherit widgets_base",
    )
    .await
    .expect("make the source an inheritance child");
    assert_eq!(
        capture_faults(&audit_widgets(&trellis).await),
        vec![CaptureFault::Hierarchy(Hierarchy::InheritanceChild {
            table: WIDGETS.to_string(),
            parent: "trellis.widgets_base".to_string(),
        })]
    );

    raw.batch_execute(
        "alter table widgets no inherit widgets_base; \
         create table widgets_more (extra text) inherits (widgets)",
    )
    .await
    .expect("make the source an inheritance parent instead");
    assert_eq!(
        capture_faults(&audit_widgets(&trellis).await),
        vec![CaptureFault::Hierarchy(Hierarchy::InheritanceParent {
            table: WIDGETS.to_string(),
            child: "trellis.widgets_more".to_string(),
        })]
    );

    trellis.shutdown().await.expect("shutdown");
}

/// #622 C9 review: a capture trigger re-pointed at some other function, and
/// a capture function switched to `SECURITY INVOKER`, are reported. The
/// re-pointed trigger is set back to `ENABLE ALWAYS`, so the only thing wrong
/// with it is the function it calls.
#[tokio::test]
async fn a_repointed_capture_trigger_and_an_invoker_capture_function_are_reported() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let (trellis, raw) = widgets_fixture(&db).await;
    let function = |event| {
        format!(
            "trellis.{}",
            trellis::capture::sql::function_name(WIDGETS, event).expect("function name")
        )
    };
    let update = function(trellis::capture::sql::CaptureEvent::Update);

    raw.batch_execute(&format!(
        "create function public.not_capture() returns trigger language plpgsql \
             as $$ begin return null; end $$; \
         create or replace trigger trellis_capture_insert after insert on widgets \
             referencing new table as trellis_new \
             for each statement execute function public.not_capture(); \
         alter table widgets enable always trigger trellis_capture_insert; \
         alter function {update}() security invoker"
    ))
    .await
    .expect("re-point a capture trigger and make a capture function an invoker");

    assert_eq!(
        capture_faults(&audit_widgets(&trellis).await),
        vec![
            CaptureFault::WrongFunction {
                table: WIDGETS.to_string(),
                trigger: "trellis_capture_insert".to_string(),
                function: function(trellis::capture::sql::CaptureEvent::Insert),
            },
            CaptureFault::NotSecurityDefiner {
                table: WIDGETS.to_string(),
                function: update,
            },
        ]
    );

    trellis.shutdown().await.expect("shutdown");
}

/// #622 C9 review: the audit covers the to-side of a relationship the
/// definition reads through, not only its source. `self_check` doesn't
/// compare a relationship-enriched target yet, so a healthy capture ends in
/// that refusal, and a broken to-side capture is reported ahead of it.
#[tokio::test]
async fn a_relationship_to_sides_capture_is_audited_too() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut raw = connect_raw(db.dsn()).await;
    raw.batch_execute(
        "create table makers (id integer primary key, name text); \
         create table widgets (id integer primary key, maker_id integer)",
    )
    .await
    .expect("create tables");
    let trellis = Trellis::connect(
        Config::from_dsn(db.dsn().to_string()).expect("valid dsn"),
        TrellisOptions::default(),
    )
    .await
    .expect("connect");
    trellis
        .apply("RELATIONSHIP maker FROM widgets.maker_id TO makers.id")
        .await
        .expect("relationship");
    trellis
        .apply("TRANSFORM widget_makers FROM widgets SELECT maker.name AS maker_name")
        .await
        .expect("define");
    capture_tables(&mut raw, &[WIDGETS, "trellis.makers"]).await;
    trellis::intake::markers::discharge_registrations(&db.pool)
        .await
        .expect("dispatch the build");
    let self_check = || {
        trellis.self_check(
            "widget_makers",
            SelfCheckScope {
                after: None,
                limit: 100,
            },
            SelfCheckMode::Strict,
            GENEROUS_TIMEOUT,
        )
    };
    let healthy = self_check().await;
    assert!(
        healthy.as_ref().is_err_and(|e| e
            .to_string()
            .contains("doesn't audit a relationship-enriched target")),
        "a healthy capture reaches the comparison, which refuses the relationship path: \
         {healthy:?}"
    );

    raw.batch_execute("alter table makers disable trigger trellis_capture_update")
        .await
        .expect("disable the to-side's update capture");
    assert_eq!(
        capture_faults(&self_check().await.expect("self_check")),
        vec![CaptureFault::TriggerNotAlways {
            table: "trellis.makers".to_string(),
            trigger: "trellis_capture_update".to_string(),
            enabled: "D".to_string(),
        }]
    );

    trellis.shutdown().await.expect("shutdown");
}

/// #622 C9 review: which definitions and tables the audit looks at. A
/// definition sourced from this instance's own target is fed by the
/// target-mutation seam, which installs no triggers, so its source isn't
/// audited. A paused definition is frozen and reads nothing, so it isn't
/// audited either, whatever its capture looks like.
#[tokio::test]
async fn a_seam_fed_source_and_a_paused_definition_are_not_audited() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let (trellis, raw) = widgets_fixture(&db).await;
    let audit = |target: &'static str| {
        let pool = db.pool.clone();
        async move {
            let def = trellis::defs::catalog::definition_by_target(&pool, target)
                .await
                .expect("read definition")
                .expect("definition exists");
            let client = pool.get().await.expect("connection");
            let faults = trellis::staging::capture_audit::audit(&**client, DEFAULT_SCHEMA, &def)
                .await
                .expect("audit");
            (def.status, faults)
        }
    };

    trellis
        .apply("TRANSFORM widget_totals_copy FROM widget_totals SELECT total AS total")
        .await
        .expect("define a reader of the target");
    trellis::intake::markers::discharge_registrations(&db.pool)
        .await
        .expect("dispatch the build");
    let (status, faults) = audit("widget_totals_copy").await;
    assert!(
        !matches!(
            status,
            trellis::defs::model::TransformStatus::WaitingToBackfill
        ),
        "the reader of the target is dispatched, so it is audited: {status:?}"
    );
    assert_eq!(
        faults,
        vec![],
        "its seam-fed source has no triggers to audit"
    );

    raw.batch_execute("drop trigger trellis_capture_insert on widgets")
        .await
        .expect("drop a capture trigger");
    let missing = vec![CaptureFault::MissingTrigger {
        table: WIDGETS.to_string(),
        trigger: "trellis_capture_insert".to_string(),
    }];
    assert_eq!(audit("widget_totals").await.1, missing, "while it's live");
    trellis
        .apply("PAUSE TRANSFORM widget_totals")
        .await
        .expect("pause");
    assert_eq!(
        audit("widget_totals").await,
        (trellis::defs::model::TransformStatus::Paused, vec![]),
        "once it's paused"
    );

    trellis.shutdown().await.expect("shutdown");
}

// ---------------------------------------------------------------------------
// Paging under the key's collation (issue #782)
// ---------------------------------------------------------------------------

/// Twelve source rows keyed `k01`, `K02`, `k03`, … (lower case for an odd
/// number, upper case for an even one). The test database's default
/// collation is ICU `en-US` (`testkit::cluster`), which orders them by their
/// digits; `"C"` (byte order) puts every `K…` before every `k…`.
fn mixed_case_words() -> Vec<(String, String)> {
    (1..=12)
        .map(|i| {
            let key = format!("{}{i:02}", if i % 2 == 0 { 'K' } else { 'k' });
            let image = format!(r#"{{"id":"{key}","v":"{i}"}}"#);
            (key, image)
        })
        .collect()
}

/// A converged `word_values` 1-1 target over [`mixed_case_words`], its
/// source key and its target key both under the database's default
/// collation.
async fn words_fixture(db: &TestDatabase) -> (Trellis, Client) {
    let words = mixed_case_words();
    let rows: Vec<(&str, &str)> = words
        .iter()
        .map(|(key, image)| (key.as_str(), image.as_str()))
        .collect();
    converged_fixture(
        db,
        "create table words (id text primary key, v integer)",
        "words",
        "TRANSFORM word_values FROM words SELECT v AS v",
        &rows,
    )
    .await
}

/// Re-collates the source key of [`words_fixture`] to `"C"`. The target key
/// keeps the default collation it was created with, so the two sides' keys
/// now order differently.
async fn recollate_words(raw: &Client) {
    raw.batch_execute(r#"alter table words alter column id type text collate "C""#)
        .await
        .expect("re-collate the source key");
}

/// Every page of an audit of `target`, `limit` keys at a time, from the
/// start of the keyspace to its end: every divergence any page reported, and
/// the sum of the pages' `rows_compared`.
async fn audit_every_page(trellis: &Trellis, target: &str, limit: i64) -> (Vec<Divergence>, i64) {
    let mut divergences = Vec::new();
    let mut compared = 0;
    let mut after = None;
    for _ in 0..100 {
        let report = trellis
            .self_check(
                target,
                SelfCheckScope { after, limit },
                SelfCheckMode::Standard,
                GENEROUS_TIMEOUT,
            )
            .await
            .expect("self_check");
        compared += report.rows_compared;
        match report.outcome {
            SelfCheckOutcome::Converged => {}
            SelfCheckOutcome::Diverged(found) => divergences.extend(found),
            other => panic!("expected a comparison, got {other:?}"),
        }
        if report.next_after.is_none() {
            return (divergences, compared);
        }
        after = report.next_after;
    }
    panic!("the audit of {target} did not reach the end of its keyspace in 100 pages");
}

/// A source key re-collated after define orders differently from its 1-1
/// target's key. An audit paged three keys at a time must still compare
/// every key exactly once and find nothing, rather than read each side's
/// page in its own order and report the keys the two pages don't share.
#[tokio::test]
async fn a_re_collated_source_key_pages_without_false_divergences() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let (trellis, raw) = words_fixture(&db).await;
    recollate_words(&raw).await;

    let (divergences, compared) = audit_every_page(&trellis, "word_values", 3).await;
    assert_eq!(divergences, vec![], "nothing diverged");
    assert_eq!(compared, 12, "every key compared once");

    trellis.shutdown().await.expect("shutdown");
}

/// A target row missing at a page boundary, under a key collation that
/// isn't byte order (`en-US`: `k01 < K02 < k03 < K04`), is reported, and
/// nothing else is. `k03` ends the source side's first three-key page, and
/// the target side's page runs on to `K04` in its place. The page has to end
/// at `k03` by the key's own ordering for `k03` to be reported and `K04` not
/// to be.
#[tokio::test]
async fn a_missing_row_at_a_page_boundary_is_reported_under_the_key_s_collation() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let (trellis, raw) = words_fixture(&db).await;
    raw.execute("delete from word_values where id = 'k03'", &[])
        .await
        .expect("delete a target row");

    let (divergences, compared) = audit_every_page(&trellis, "word_values", 3).await;
    assert_eq!(
        divergences,
        vec![Divergence::MissingRow {
            key: "k03".to_string()
        }]
    );
    assert_eq!(compared, 12, "every key compared once");

    trellis.shutdown().await.expect("shutdown");
}

/// After the source key is re-collated to `"C"` (`K02 < K04 < K06 < K08 <
/// … < k01`), a target row missing at a page boundary of that order (`K06`)
/// and a target row with no source row are each reported, and nothing else
/// is: comparing the two sides under one ordering hides no real divergence.
#[tokio::test]
async fn real_divergences_across_page_boundaries_are_reported_after_a_re_collation() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let (trellis, raw) = words_fixture(&db).await;
    recollate_words(&raw).await;
    raw.batch_execute(
        "delete from word_values where id = 'K06'; \
         insert into word_values (id, v) values ('k07x', 7)",
    )
    .await
    .expect("seed a missing and an extra target row");

    let (mut divergences, compared) = audit_every_page(&trellis, "word_values", 3).await;
    divergences.sort_by_key(|d| format!("{d:?}"));
    assert_eq!(
        divergences,
        vec![
            Divergence::ExtraRow {
                key: "k07x".to_string()
            },
            Divergence::MissingRow {
                key: "K06".to_string()
            },
        ]
    );
    assert_eq!(compared, 13, "every key of either side compared once");

    trellis.shutdown().await.expect("shutdown");
}

/// An extra target row, `k03a`, sorting right after a page boundary and
/// before the next source key (`en-US`: `k03 < k03a < K04`, where `"C"` puts
/// it after every `K…`). The first page ends at `k03` on both sides; the
/// second starts with `k03a` on the persisted side only, which pushes that
/// side's page end down to `k05`, so the recompute side's `K06` is left to
/// the third page. The extra row is reported, and nothing else.
#[tokio::test]
async fn an_extra_target_row_just_past_a_page_boundary_is_reported_under_the_key_s_collation() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let (trellis, raw) = words_fixture(&db).await;
    raw.execute("insert into word_values (id, v) values ('k03a', 3)", &[])
        .await
        .expect("insert an extra target row");

    let (divergences, compared) = audit_every_page(&trellis, "word_values", 3).await;
    assert_eq!(
        divergences,
        vec![Divergence::ExtraRow {
            key: "k03a".to_string()
        }]
    );
    assert_eq!(compared, 13, "every key of either side compared once");

    trellis.shutdown().await.expect("shutdown");
}

/// A source key re-collated after define to a *nondeterministic* collation
/// (case-insensitive here; define refuses one, #638, but a later `alter`
/// isn't refused). Under it, `k03` and an extra target row `K03` compare
/// equal, so paging by it would let a page's `limit` cut between the two and
/// the next page's `> k03` skip whichever was cut: the extra row is never
/// compared, or `k03` is reported missing when it isn't. Distinct keys must
/// never tie in the order the pages are read in.
#[tokio::test]
async fn a_re_collation_to_a_nondeterministic_collation_hides_no_row_at_a_page_boundary() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let (trellis, raw) = words_fixture(&db).await;
    raw.batch_execute(
        "create collation case_insensitive \
           (provider = icu, locale = 'und-u-ks-level2', deterministic = false); \
         alter table words alter column id type text collate case_insensitive; \
         insert into word_values (id, v) values ('K03', 3)",
    )
    .await
    .expect("re-collate the source key and seed a tying extra target row");

    let (divergences, compared) = audit_every_page(&trellis, "word_values", 3).await;
    assert_eq!(
        divergences,
        vec![Divergence::ExtraRow {
            key: "K03".to_string()
        }]
    );
    assert_eq!(compared, 13, "every key of either side compared once");

    trellis.shutdown().await.expect("shutdown");
}

/// The target has an extra row, `K00`, ahead of every source key, so only
/// the persisted side's three-key page ends early, at `K02` (`en-US`: `K00 <
/// k01 < K02 < k03`). The page ends there, and the recompute side's `k03` is
/// left to the next page rather than reported missing.
#[tokio::test]
async fn a_page_ends_at_the_lower_of_the_two_sides_last_keys_in_the_key_s_order() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let (trellis, raw) = words_fixture(&db).await;
    raw.execute("insert into word_values (id, v) values ('K00', 0)", &[])
        .await
        .expect("insert an extra target row");

    let report = trellis
        .self_check(
            "word_values",
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
            vec![Divergence::ExtraRow {
                key: "K00".to_string()
            }]
        ),
        other => panic!("expected Diverged with the extra row, got {other:?}"),
    }
    assert_eq!(report.next_after, Some("K02".to_string()));
    assert_eq!(report.rows_compared, 3, "K00, k01 and K02");

    trellis.shutdown().await.expect("shutdown");
}

/// The lines of `explain`'s text that read a table by a sequential scan.
fn seq_scans(explained: &str) -> Vec<&str> {
    explained
        .lines()
        .filter(|line| line.contains("Seq Scan"))
        .collect()
}

/// While the source and target keys share a collation, as a 1-1 target's
/// key is created with its source's, both sides of a page are read by their
/// primary-key index: naming the key's own collation costs no index.
#[tokio::test]
async fn a_page_reads_both_sides_by_their_key_index() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let (trellis, _raw) = words_fixture(&db).await;

    let explained =
        trellis::staging::self_check::explain_page(&db.pool, "word_values", Some("k03"), 3)
            .await
            .expect("explain the page");
    assert_eq!(seq_scans(&explained), Vec::<&str>::new(), "{explained}");
    assert!(explained.contains("words_pkey"), "{explained}");
    assert!(explained.contains("word_values_pkey"), "{explained}");

    trellis.shutdown().await.expect("shutdown");
}

/// After the source key is re-collated, the page is compared under the
/// source key's new collation: the source is still read by its index, and
/// the target, whose index orders by its own collation, by a scan. This is
/// the documented cost of auditing a target whose key no longer orders like
/// its source's (`staging::self_check::page_collation`).
#[tokio::test]
async fn a_page_after_a_re_collation_reads_the_source_by_its_key_index() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let (trellis, raw) = words_fixture(&db).await;
    recollate_words(&raw).await;

    let explained =
        trellis::staging::self_check::explain_page(&db.pool, "word_values", Some("K04"), 3)
            .await
            .expect("explain the page");
    assert!(explained.contains("words_pkey"), "{explained}");
    assert_eq!(
        seq_scans(&explained).len(),
        1,
        "only the target is scanned: {explained}"
    );
    assert!(!explained.contains("word_values_pkey"), "{explained}");

    trellis.shutdown().await.expect("shutdown");
}
