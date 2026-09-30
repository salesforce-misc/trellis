//! Improvement-plan task D3, negative control.
//!
//! `generative::run::run_convergence` already does read-your-own-writes
//! (RYOW), asserted incrementally: apply → quiesce → snapshot → compare,
//! once per op (`generative/src/run/mod.rs`). That's a real property, not a
//! new one this file adds — what's missing is proof that `quiesce()` is
//! doing real, load-bearing work, rather than being a no-op that happens to
//! pass because this suite's test conditions always converge fast enough
//! anyway. Without that proof, a `quiesce()` that silently degraded into
//! `Ok(())` immediately would make every convergence property in this crate
//! pass just as often — nothing would ever demonstrate that skipping it
//! *could* observe stale state.
//!
//! This test reads target state immediately after `backend.apply(op)`,
//! **without** calling `quiesce()` first, and demonstrates the read can
//! legitimately return state that does not yet reflect the just-applied op.
//!
//! **Why this is structurally very unlikely to be flaky, not just usually
//! lucky.** `ManualBackend::apply` returns as soon as the raw source
//! statement's own transaction commits — nothing about that commit touches
//! the engine beyond the capture trigger's ring row. Reflecting it in the
//! target requires, at minimum: *sealing* the
//! active segment — which only happens on the staging worker's fixed
//! `maintenance_interval` tick (**300ms** by default,
//! `trellis::client::ClientOptions::maintenance_interval`, and this backend
//! never overrides it — see `ManualBackend::install`) — then claiming,
//! folding, and applying that batch. Every one of those is a separate
//! asynchronous hop, several of them gated behind a fixed poll that only
//! fires once every 300ms *regardless of load*. The instant this test's
//! `apply()` call returns, without ever `.await`ing anything else, none of
//! that chain has had a chance to run — for the read immediately afterward
//! to already reflect the change would require the entire chain completing
//! in under the time it takes this test's own next `await` (the snapshot
//! read's own network round trip) to resolve, which is not realistically
//! possible on the fixed-300ms-tick design described above.
//!
//! That said: this is still fundamentally a race, observing a timing window
//! rather than a structural guarantee, so — per this task's own instructions
//! — it's built to fail loudly rather than flake quietly if that
//! expectation is ever wrong: it retries the same check against several
//! independent updates (a fresh, distinct value each time) and only requires
//! staleness to be observed on **at least one** of them, not every one. The
//! probability of every single attempt racing the ~300ms tick and losing is
//! astronomically lower than any one attempt doing so.

use generative::backend::{Backend, ManualBackend};
use generative::generate::{
    DefShape, RelFieldKind, RelFieldSpec, TableSpec, build_program,
    build_program_multi_with_relationships,
};
use generative::model::{Op, OpOutcome};
use generative::run::check_program;
use testkit::TestCluster;
use trellis::{Config, Pool};

/// How many distinct updates to race against `quiesce()` before concluding
/// the negative control couldn't demonstrate staleness at all (see the
/// module doc comment on why this is a reliability mitigation, not a
/// tolerance for expected flakiness).
const ATTEMPTS: usize = 10;

#[tokio::test(flavor = "multi_thread")]
async fn reading_without_quiesce_can_observe_state_that_has_not_caught_up_yet() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;

    let program = build_program(&[(Some(1), Some(2))], &[]);
    let seed = &program.ops[0];
    let source_table = &program.tables[0].name;
    let c1_col = &program.tables[0].columns[1].name;
    let target_table = &program.defs[0].target;

    let mut backend = ManualBackend::connect(db.dsn())
        .await
        .expect("connect manual backend");
    let pool = Pool::new(&Config::from_dsn(db.dsn().to_string()).expect("config")).expect("pool");

    backend.install(&program).await.expect("install program");
    backend.apply(seed).await.expect("seed insert must succeed");
    // Let the seed itself fully converge first, so every attempt below
    // starts from a known, already-settled baseline — the staleness we're
    // trying to observe must come from the *update* we race, not leftover
    // catch-up from the seed.
    backend.quiesce().await.expect("quiesce after seed");

    let mut saw_stale_read = false;
    for attempt in 0..ATTEMPTS {
        // A fresh, distinct value every attempt, so "the target hasn't
        // caught up" is unambiguous (not indistinguishable from a
        // coincidentally-already-correct value): total = c1 + c2 = c1 + 2,
        // and no earlier attempt's target value can equal this attempt's.
        let new_c1 = 100 + attempt as i64;
        let update = Op::Update {
            table: source_table.clone(),
            pk: "1".to_string(),
            changes: vec![(c1_col.clone(), Some(new_c1.to_string()))],
            expect: OpOutcome::Succeeds,
        };

        backend
            .apply(&update)
            .await
            .expect("update must succeed against the live seeded row");

        // No `quiesce()` here — this is the whole point. Read immediately.
        let snapshot = backend
            .snapshot()
            .await
            .expect("snapshot immediately after apply, no quiesce");

        let source_reflects_the_write = snapshot
            .get(source_table)
            .and_then(|rows| rows.get("1"))
            .and_then(|cols| cols.get(c1_col))
            .cloned()
            .flatten()
            == Some(new_c1.to_string());
        assert!(
            source_reflects_the_write,
            "the source statement itself must have committed synchronously by the time \
             apply() returned (attempt {attempt}): {snapshot:#?}"
        );

        let expected_total = (new_c1 + 2).to_string();
        let target_already_reflects_it = snapshot
            .get(target_table)
            .and_then(|rows| rows.get("1"))
            .and_then(|cols| cols.get("total"))
            .cloned()
            .flatten()
            == Some(expected_total);

        if !target_already_reflects_it {
            saw_stale_read = true;
            break;
        }
        // This attempt's read raced quiesce and (implausibly) lost — the
        // pipeline caught up before we even asked. Try again with a fresh
        // value; see the module doc comment.
    }

    assert!(
        saw_stale_read,
        "expected at least one of {ATTEMPTS} immediate (no-quiesce) reads to observe a target \
         that had not yet caught up with its source update — if every single attempt already \
         converged, either this environment is unrealistically fast or `quiesce()`/the \
         maintenance pipeline changed in a way that makes this negative control worth \
         re-examining"
    );

    // Leave the database in a clean, fully-converged state, and confirm it:
    // the negative control's whole point is that quiesce is real work, so
    // show it actually finishes the job.
    backend.quiesce().await.expect("final quiesce");
    let snapshot = backend.snapshot().await.expect("final snapshot");
    let divergence = check_program(&pool, &program, &snapshot)
        .await
        .expect("oracle check must run");
    assert!(
        divergence.is_empty(),
        "after a real quiesce, the target must have fully caught up: {divergence:?}"
    );
}

/// Read-your-own-writes across a **relationship** (issue #34, ADR-0006).
///
/// The positive half of the property, on the shape that makes it hardest:
/// the write lands on the *related* row, not on the row whose derived value
/// changes. Trellis has to resolve — from the dependency graph — which
/// definitions read the changed table, find the referencing rows by join
/// key, and re-derive them (ADR-0006's "reverse" direction), all
/// asynchronously. So `quiesce()`'s promise here covers a write the client
/// never made against the target's own source table at all.
///
/// Both cardinalities are exercised in one program: `d0` reads `t1` through
/// a to-one enrichment, `d1` reads `t1` through a to-many aggregate. One
/// update to a single `t1` row must, after one `quiesce()`, be visible in
/// both.
#[tokio::test(flavor = "multi_thread")]
async fn awaiting_after_a_write_to_a_related_row_makes_the_enrichment_visible() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;

    let spec = |seeds: Vec<(Option<i64>, Option<i64>)>, fks: Vec<Option<String>>| {
        let n = seeds.len();
        TableSpec {
            seed_values: seeds,
            text_values: vec![None; n],
            bool_values: vec![None; n],
            uuid_values: vec![None; n],
            grain_values: vec![None; n],
            rel_fk_values: fks,
            mutates: Vec::new(),
        }
    };

    let program = build_program_multi_with_relationships(
        // `t1`'s key column is `k1` (derived from its pk), so `t0`'s first
        // row joins to it both ways: `t0.fk = "k1"` for the to-one
        // direction, and `t1.fk = "k1"` (matching `t0`'s own key) for the
        // to-many one.
        &[
            spec(vec![(Some(1), Some(2))], vec![Some("k1".to_string())]),
            spec(vec![(Some(10), Some(0))], vec![Some("k1".to_string())]),
        ],
        &[(0, DefShape::OneToOne), (0, DefShape::OneToOne)],
        &[None, None],
        &[
            Some(RelFieldSpec {
                to_table: 1,
                kind: RelFieldKind::ToOneBare,
            }),
            Some(RelFieldSpec {
                to_table: 1,
                kind: RelFieldKind::ToManyAggregate(generative::generate::RelAggregateFn::Sum),
            }),
        ],
    );

    let related_table = program.tables[1].name.clone();
    let related_c1 = program.tables[1].columns[1].name.clone();

    let mut backend = ManualBackend::connect(db.dsn())
        .await
        .expect("connect manual backend");
    let pool = Pool::new(&Config::from_dsn(db.dsn().to_string()).expect("config")).expect("pool");

    backend.install(&program).await.expect("install program");
    for op in &program.ops {
        backend.apply(op).await.expect("seed op must succeed");
    }
    backend.quiesce().await.expect("quiesce after seeding");

    // The write: a *related* row changes. Nothing touches `t0`, yet both of
    // `t0`'s targets must move.
    let update = Op::Update {
        table: related_table,
        pk: "1".to_string(),
        changes: vec![(related_c1, Some("777".to_string()))],
        expect: OpOutcome::Succeeds,
    };
    backend
        .apply(&update)
        .await
        .expect("update to the related row must succeed");
    backend
        .quiesce()
        .await
        .expect("quiesce must cover reverse propagation, not just forward");

    let snapshot = backend.snapshot().await.expect("snapshot after quiesce");
    assert_eq!(
        snapshot[&program.defs[0].target]["1"]["rel_enrich"],
        Some("777".to_string()),
        "the to-one enrichment must reflect the related row's new value after quiesce: \
         {snapshot:#?}"
    );
    assert_eq!(
        snapshot[&program.defs[1].target]["1"]["rel_agg"],
        Some("777".to_string()),
        "the to-many aggregate must reflect the related row's new value after quiesce: \
         {snapshot:#?}"
    );

    // And the full oracle agrees, not just the two cells spelled out above.
    let divergence = check_program(&pool, &program, &snapshot)
        .await
        .expect("oracle check must run");
    assert!(
        divergence.is_empty(),
        "after quiesce, every relationship-enriched target must match the oracle: {divergence:?}"
    );
}
