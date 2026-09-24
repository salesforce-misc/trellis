//! Proves the manual backend now builds a definition's target through the
//! direct, set-based backfill path (issue #63 M3) rather than the ring
//! enumeration `create_definition` used to stage.
//!
//! The backend seam always creates a source table empty and installs its
//! definitions in the same `install` call, so a definition's backfill normally
//! runs over an empty source. To exercise the direct build over *populated*
//! data — the whole point of M3 — this drives `install` in two phases against
//! one backend: first install the source table alone and seed rows into it,
//! then install the definition, whose backfill must find and build those
//! already-present rows directly. A final live update proves the ring still
//! folds a post-build CDC delta onto the directly-built row (the build/CDC
//! fence).
//!
//! **Why the mid-test `quiesce` is load-bearing.** Phase 1's `install` starts
//! the engine client (its source-table set is non-empty), which creates the
//! replication slot; the seed inserts that follow are therefore captured by
//! CDC and staged into the ring as `Recompute` markers. Without draining them
//! first, those markers stay *pending* — and the moment phase 2 persists the
//! definition, the applier folds them onto the freshly-created target,
//! computing the very same values the direct build would. That masks M3
//! completely: the test would still pass even if `install_definition`'s
//! `backfill_definition` call were a no-op (the CDC fold, not the direct
//! build, would be doing the work). Quiescing *before* the definition exists
//! drains those seed markers while no definition references the source, so
//! they fold into nothing and leave the ring empty. After that, the direct
//! build is the *only* thing that can populate the target — which is exactly
//! the real M3 production scenario (a new definition installed over a source
//! already live under CDC with pre-existing rows), and what makes this test
//! actually discriminate: no-op the direct build and it fails.

use std::time::{Duration, Instant};

use generative::backend::{Backend, ManualBackend};
use generative::model::{NamePool, Op, OpOutcome, Program, Table};
use testkit::TestCluster;
use trellis::dev::defs::ast::{
    Expr, FieldDef, KeySpace, Operator, Predicate, TransformDef, ValueType,
};
use trellis::dev::staging::{await_converged, watermark_token};

#[tokio::test]
async fn direct_backfill_builds_the_target_from_preexisting_source_rows() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;

    let mut pool = NamePool::new();
    let source = Table::new(&mut pool, &[ValueType::Numeric, ValueType::Numeric]);
    let a = source.columns[1].name.clone();
    let b = source.columns[2].name.clone();
    let target_name = pool.next_table_name();

    let def = TransformDef {
        target: target_name.clone(),
        source: source.name.clone(),
        key_space: KeySpace::OneToOne,
        fields: vec![FieldDef {
            name: "total".to_string(),
            expr: Expr::BinaryOp {
                op: Operator::Add,
                lhs: Box::new(Expr::Column(a.clone())),
                rhs: Box::new(Expr::Column(b.clone())),
            },
        }],
        predicate: Predicate::True,
        explicit_source_schema: None,
        explicit_target_schema: None,
    };

    let mut backend = ManualBackend::connect(db.dsn())
        .await
        .expect("connect manual backend");

    // Phase 1: install only the source table (no definition yet), then seed two
    // rows into it. These rows exist in the source *before* the definition is
    // created, so the definition's backfill — not CDC — is what must build them
    // into the target.
    let source_only = Program {
        tables: vec![source.clone()],
        relationships: Vec::new(),
        defs: vec![],
        def_install_after_op: vec![],
        ops: vec![],
        restart_after_ops: vec![],
        scale_out_after_ops: vec![],
    };
    backend
        .install(&source_only)
        .await
        .expect("install source table");

    for (pk, av, bv) in [("1", "10.00", "1.50"), ("2", "20.00", "2.00")] {
        backend
            .apply(&Op::Insert {
                table: source.name.clone(),
                row: vec![
                    (source.pk_col.clone(), Some(pk.to_string())),
                    (a.clone(), Some(av.to_string())),
                    (b.clone(), Some(bv.to_string())),
                ],
                expect: OpOutcome::Succeeds,
            })
            .await
            .expect("seed source row");
    }

    // Drain the CDC markers the seed inserts staged, *before* any definition
    // exists to fold them onto. This is what forces the target to be built by
    // the direct backfill alone in phase 2 rather than by a still-pending CDC
    // fold — see this module's doc comment. Remove it and the test silently
    // stops testing M3 (it would pass even against a no-op direct build).
    backend
        .quiesce()
        .await
        .expect("quiesce to drain seed CDC before the definition exists");

    // Phase 2: install the definition against the now-populated source. With
    // the seed markers already drained, its target can only be built by the
    // direct backfill.
    let def_only = Program {
        tables: vec![],
        relationships: Vec::new(),
        defs: vec![def],
        def_install_after_op: vec![0],
        ops: vec![],
        restart_after_ops: vec![],
        scale_out_after_ops: vec![],
    };
    backend
        .install(&def_only)
        .await
        .expect("install definition (runs the direct backfill)");
    backend.quiesce().await.expect("quiesce after backfill");

    let snapshot = backend.snapshot().await.expect("snapshot");
    let target = snapshot.get(&target_name).unwrap_or_else(|| {
        panic!("target table {target_name:?} missing from snapshot: {snapshot:?}")
    });
    assert_eq!(
        target.len(),
        2,
        "the direct backfill must have built both pre-existing source rows: {target:?}"
    );
    assert_eq!(
        target["1"]["total"],
        Some("11.50".to_string()),
        "row 1's backfilled total must be 10.00 + 1.50"
    );
    assert_eq!(
        target["2"]["total"],
        Some("22.00".to_string()),
        "row 2's backfilled total must be 20.00 + 2.00"
    );

    // A live update after the build must fold onto the directly-built row via
    // the ring — the build/CDC fence the direct path relies on.
    backend
        .apply(&Op::Update {
            table: source.name.clone(),
            pk: "1".to_string(),
            changes: vec![(a.clone(), Some("100.00".to_string()))],
            expect: OpOutcome::Succeeds,
        })
        .await
        .expect("apply post-backfill update");
    backend.quiesce().await.expect("quiesce after update");

    let snapshot = backend.snapshot().await.expect("snapshot after update");
    let target = &snapshot[&target_name];
    assert_eq!(
        target["1"]["total"],
        Some("101.50".to_string()),
        "a post-backfill CDC update must converge onto the directly-built row (100.00 + 1.50)"
    );
}

/// Advisory-lock key gating the backfill chunk write in
/// `quiesce_blocks_until_a_slow_backfill_actually_reaches_live`. Advisory
/// locks are per-database and each test runs in its own isolated database,
/// so the value only has to be distinct within this test.
const GATE_LOCK_KEY: i64 = 300;

/// How long after observing ring convergence the gate stays closed. It only
/// has to comfortably exceed the skew between the releaser's convergence
/// observation and `quiesce`'s own (both poll with a 250ms backoff ceiling),
/// so that a `quiesce` which only waits on the ring reliably returns before
/// the release. It only affects how reliably the test catches that
/// regression. A correct `quiesce` passes whatever the margin is, because the
/// backfill cannot commit until the unlock, and `released_at` is recorded
/// before the unlock is sent.
const RELEASE_MARGIN: Duration = Duration::from_secs(1);

/// public-api-design review gap: [`ManualBackend::quiesce`] used to only
/// await CDC-ring convergence — it had no awareness of a still-`Backfilling`
/// direct-build definition's `defs::chunk_queue` work at all (docs/decisions/0007's
/// "Backgrounding and resumability" amendment put that work entirely outside
/// the ring). Every other test in this file only happened to pass because a
/// large, unrelated ring-seal age-gate stall gave a real running drain
/// worker enough wall-clock time to finish a small backfill before `quiesce`
/// returned and the test snapshotted state — not because `quiesce` actually
/// waited for it.
///
/// This proves `quiesce` now really does wait, with no such coincidence to
/// lean on and no manual chunk-queue driving of its own (this backend's
/// normal, real running application worker does the actual claim/execute/
/// finish, exactly like production): a `before insert ... for each
/// statement` trigger, attached to the target table the instant it's
/// created (via a `ddl_command_end` event trigger, so there is no window
/// between the table existing and the slow trigger being on it for a real
/// app worker to race past), makes the definition's one backfill chunk write
/// block on an advisory lock this test holds on a separate connection.
///
/// **Why an event gate, not a fixed `pg_sleep`.** The ring-convergence half
/// of `quiesce` alone stalls for ~10s on this exact scenario (measured with
/// `GENERATIVE_QUIESCE_TIMING=1`; presumably the seal age-gate stall that
/// `local_docs/transit-comparison.md` §3.3 describes), independent of how
/// slow the chunk write is. A fixed sleep shorter than that stall lets a
/// `quiesce` that only waits on ring convergence pass by the very
/// coincidence this test exists to rule out. Issue #300 proposed shrinking
/// the old `pg_sleep(13.0)` to 1-2s, but a 1.5s sleep was measured to pass
/// even with the definition-status wait deleted from `quiesce`. So the gate is
/// released on an *event*, not a clock: a helper task waits for the ring to
/// converge on its own connection, lets [`RELEASE_MARGIN`] pass, and only
/// then releases the lock. A `quiesce` that really waits on the backfill
/// cannot return before that release, however long or short the ring stall
/// is. One that only waits on the ring returns about when the helper sees
/// convergence, a full margin before the release, and fails the ordering
/// assertion below.
#[tokio::test]
async fn quiesce_blocks_until_a_slow_backfill_actually_reaches_live() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;

    let mut pool = NamePool::new();
    let source = Table::new(&mut pool, &[ValueType::Numeric, ValueType::Numeric]);
    let a = source.columns[1].name.clone();
    let b = source.columns[2].name.clone();
    let target_name = pool.next_table_name();

    let def = TransformDef {
        target: target_name.clone(),
        source: source.name.clone(),
        key_space: KeySpace::OneToOne,
        fields: vec![FieldDef {
            name: "total".to_string(),
            expr: Expr::BinaryOp {
                op: Operator::Add,
                lhs: Box::new(Expr::Column(a.clone())),
                rhs: Box::new(Expr::Column(b.clone())),
            },
        }],
        predicate: Predicate::True,
        explicit_source_schema: None,
        explicit_target_schema: None,
    };

    let mut backend = ManualBackend::connect(db.dsn())
        .await
        .expect("connect manual backend");

    // Phase 1, exactly like `direct_backfill_builds_the_target_from_preexisting_source_rows`
    // above: install only the source table, seed rows into it, then quiesce
    // *before* the definition exists so the seed CDC markers drain into
    // nothing rather than being available for the applier to (mis)use in
    // place of the direct build.
    let source_only = Program {
        tables: vec![source.clone()],
        relationships: Vec::new(),
        defs: vec![],
        def_install_after_op: vec![],
        ops: vec![],
        restart_after_ops: vec![],
        scale_out_after_ops: vec![],
    };
    backend
        .install(&source_only)
        .await
        .expect("install source table");

    for (pk, av, bv) in [("1", "10.00", "1.50"), ("2", "20.00", "2.00")] {
        backend
            .apply(&Op::Insert {
                table: source.name.clone(),
                row: vec![
                    (source.pk_col.clone(), Some(pk.to_string())),
                    (a.clone(), Some(av.to_string())),
                    (b.clone(), Some(bv.to_string())),
                ],
                expect: OpOutcome::Succeeds,
            })
            .await
            .expect("seed source row");
    }
    backend
        .quiesce()
        .await
        .expect("quiesce to drain seed CDC before the definition exists");

    // Close the gate before anything can write to the target. The lock is
    // session-level, held by `gate` until the releaser task below unlocks it
    // (or, if that task panics, until `gate` is dropped and its session ends,
    // so a failure can't leave the backfill wedged).
    let (gate, gate_conn) = tokio_postgres::connect(db.dsn(), tokio_postgres::NoTls)
        .await
        .expect("connect gate session");
    tokio::spawn(gate_conn);
    gate.batch_execute(&format!("select pg_advisory_lock({GATE_LOCK_KEY})"))
        .await
        .expect("close the backfill gate");

    // Deterministically block the definition's backfill chunk write
    // without racing this backend's already-running application worker: a
    // `ddl_command_end` event trigger fires *inside* the same DDL command
    // that creates the target table (`install`'s `create_target_table`
    // call, below), attaching a statement-level `before insert` trigger to
    // it before that command even commits — so by the time the backfill
    // chunk is enqueued (a separate, later transaction), the target table
    // already carries the gated-write trigger. There is no window in which a
    // real app worker could claim and finish the chunk before the gate is
    // in place.
    backend
        .execute_raw(&format!(
            "create function _slow_backfill_write() returns trigger as $$ \
             begin perform pg_advisory_xact_lock({GATE_LOCK_KEY}); return null; end; \
             $$ language plpgsql"
        ))
        .await
        .expect("create the slow-write trigger function");
    backend
        .execute_raw(&format!(
            "create function _attach_slow_backfill_trigger() returns event_trigger as $$ \
             declare obj record; \
             begin \
               for obj in select * from pg_event_trigger_ddl_commands() loop \
                 if obj.object_type = 'table' \
                    and obj.object_identity = 'public.{target_name}' then \
                   execute format( \
                     'create trigger _slow_backfill_write_trigger before insert on %s \
                      for each statement execute function _slow_backfill_write()', \
                     obj.objid::regclass); \
                 end if; \
               end loop; \
             end; \
             $$ language plpgsql"
        ))
        .await
        .expect("create the ddl-attach event trigger function");
    backend
        .execute_raw(
            "create event trigger _slow_backfill_ddl_trigger on ddl_command_end \
             when tag in ('CREATE TABLE') \
             execute function _attach_slow_backfill_trigger()",
        )
        .await
        .expect("create the ddl event trigger");

    // Phase 2: install the definition. Its target table is created (and,
    // via the event trigger above, instantly gated), then its one backfill
    // chunk is enqueued — and, since this backend has a real running
    // application worker, claimed and executed automatically, blocking on
    // the gate.
    let def_only = Program {
        tables: vec![],
        relationships: Vec::new(),
        defs: vec![def],
        def_install_after_op: vec![0],
        ops: vec![],
        restart_after_ops: vec![],
        scale_out_after_ops: vec![],
    };
    backend
        .install(&def_only)
        .await
        .expect("install definition (enqueues its one, now gated, backfill chunk)");

    let (observer, observer_conn) = tokio_postgres::connect(db.dsn(), tokio_postgres::NoTls)
        .await
        .expect("connect ring observer");
    tokio::spawn(observer_conn);
    // Same instance-schema resolution `ManualBackend::connect` uses, so the
    // observer's convergence read sees the backend's own ring tables.
    let instance_schema = trellis::Config::from_dsn(db.dsn())
        .expect("resolve instance schema")
        .schema()
        .to_string();
    observer
        .batch_execute(&format!("set search_path to {instance_schema}, public"))
        .await
        .expect("point the observer at the instance schema");
    let releaser = tokio::spawn(async move {
        let token = watermark_token(&observer)
            .await
            .expect("observer watermark token");
        await_converged(&observer, token, Duration::from_secs(30))
            .await
            .expect("the ring must converge while the backfill chunk is still gated");
        tokio::time::sleep(RELEASE_MARGIN).await;
        let released_at = Instant::now();
        gate.batch_execute(&format!("select pg_advisory_unlock({GATE_LOCK_KEY})"))
            .await
            .expect("open the backfill gate");
        released_at
    });

    backend
        .quiesce()
        .await
        .expect("quiesce must wait for the gated backfill chunk to actually finish");
    let returned_at = Instant::now();
    let released_at = releaser.await.expect("releaser task");
    assert!(
        returned_at >= released_at,
        "quiesce returned {:?} before the backfill gate was even opened — it only waited on \
         ring convergence, not on the backfill",
        released_at - returned_at
    );

    let snapshot = backend.snapshot().await.expect("snapshot");
    let target = snapshot.get(&target_name).unwrap_or_else(|| {
        panic!("target table {target_name:?} missing from snapshot: {snapshot:?}")
    });
    assert_eq!(
        target.len(),
        2,
        "the slow backfill must have fully finished before quiesce returned: {target:?}"
    );
    assert_eq!(target["1"]["total"], Some("11.50".to_string()));
    assert_eq!(target["2"]["total"], Some("22.00".to_string()));
}
