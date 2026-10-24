//! #1179: `self_check`'s capture audit over a partitioned source. A root the
//! reconcile pass records is no hierarchy fault, and the audit reads the five
//! capture triggers of every recorded node, naming a node whose triggers were
//! disabled (ADR-0017, "Every node carries capture").

#[path = "support/drain_driver.rs"]
mod drain_driver;

use std::time::{Duration, Instant};

use drain_driver::Driver;
use trellis::capture::sql::{CaptureEvent, trigger_name};
use trellis::config::DEFAULT_SCHEMA;
use trellis::defs::ValueType;
use trellis::integer::IntWidth;
use trellis::staging::capture_audit::CaptureFault;
use trellis::staging::self_check::{
    Divergence, SelfCheckMode, SelfCheckOutcome, SelfCheckScope, check_page,
};

const SOURCE: &str = "public.items";
const AGG: &str =
    "TRANSFORM agg FROM public.items GROUP BY g WITHOUT SOURCE INDEX SELECT SUM(v) AS total";
const TREE: &str = "create table public.items (id integer not null, g integer not null, \
        v integer not null, primary key (g, id)) partition by list (g); \
     create table public.items_a partition of public.items for values in (1, 2); \
     create table public.items_b partition of public.items for values in (3, 4)";

fn int4() -> ValueType {
    ValueType::Integer(IntWidth::Int4)
}

async fn check(d: &Driver, target: &str) -> Vec<Divergence> {
    let page = check_page(
        d.pool(),
        target,
        SelfCheckScope {
            after: None,
            limit: 100,
        },
        SelfCheckMode::Standard,
        Duration::from_secs(30),
    )
    .await
    .expect("a page of the check");
    match page.outcome {
        SelfCheckOutcome::Converged => Vec::new(),
        SelfCheckOutcome::Diverged(d) => d,
        other => panic!("unexpected outcome {other:?}"),
    }
}

/// A driver over `ddl` with `definitions` live, after one reconcile pass has
/// recorded every partitioned source.
async fn start(ddl: &str, definitions: &[&str], sources: &[&str]) -> Driver {
    let mut d = Driver::start(
        ddl,
        &[
            ("id", int4()),
            ("g", int4()),
            ("v", int4()),
            ("k", int4()),
            ("w", int4()),
        ],
        definitions,
        sources,
    )
    .await;
    pass(&mut d).await;
    d
}

/// One reconcile pass, which records each partitioned source's tree.
async fn pass(d: &mut Driver) {
    let desired = trellis::defs::tables_to_capture(d.pool())
        .await
        .expect("tables to capture");
    trellis::capture::reconcile::reconcile(
        &mut d.ctl,
        DEFAULT_SCHEMA,
        &desired,
        Instant::now() + Duration::from_secs(5),
    )
    .await
    .expect("pass");
}

/// `ALTER TABLE … {action} TRIGGER` for each of `events`' capture triggers.
fn on_triggers(table: &str, action: &str, events: &[CaptureEvent]) -> String {
    events
        .iter()
        .map(|e| {
            format!(
                "alter table {table} {action} trigger {};",
                trigger_name(DEFAULT_SCHEMA, *e)
            )
        })
        .collect()
}

#[tokio::test]
async fn a_recorded_partitioned_source_is_clean_and_a_leafs_disabled_trigger_is_named() {
    let mut d = start(TREE, &[AGG], &[SOURCE]).await;
    d.user()
        .await
        .batch_execute("insert into public.items values (1, 1, 5), (2, 3, 7)")
        .await
        .expect("seed");
    d.settle().await;

    assert_eq!(check(&d, "agg").await, [], "a complying partitioned source");

    let trigger = trigger_name(DEFAULT_SCHEMA, CaptureEvent::ALL[0]);
    d.user()
        .await
        .batch_execute(&format!(
            "alter table public.items_b disable trigger {trigger}"
        ))
        .await
        .expect("disable");
    let found = check(&d, "agg").await;
    assert!(
        matches!(
            found.as_slice(),
            [Divergence::Capture(CaptureFault::TriggerNotAlways { table, .. })]
                if table == "public.items_b"
        ),
        "{found:?}"
    );
}

/// A middle partitioned node and a `DEFAULT` leaf carry the five triggers
/// too: one dropped from the middle and one disabled on the `DEFAULT` are
/// each named, and nothing else is.
#[tokio::test]
async fn a_middle_nodes_dropped_trigger_and_a_defaults_disabled_one_are_named() {
    let mut d = start(
        "create table public.items (id integer not null, g integer not null, \
             v integer not null, primary key (g, id)) partition by list (g); \
         create table public.items_a partition of public.items for values in (1, 2) \
             partition by list (g); \
         create table public.items_a1 partition of public.items_a for values in (1); \
         create table public.items_a2 partition of public.items_a for values in (2); \
         create table public.items_rest partition of public.items default",
        &[AGG],
        &[SOURCE],
    )
    .await;
    d.user()
        .await
        .batch_execute("insert into public.items values (1, 1, 5), (2, 2, 7), (3, 9, 1)")
        .await
        .expect("seed");
    d.settle().await;
    assert_eq!(check(&d, "agg").await, [], "a complying multi-level tree");

    let [first, second, ..] = CaptureEvent::ALL;
    d.user()
        .await
        .batch_execute(&format!(
            "drop trigger {} on public.items_a; {}",
            trigger_name(DEFAULT_SCHEMA, first),
            on_triggers("public.items_rest", "disable", &[second]),
        ))
        .await
        .expect("break the middle and the default");
    let found = check(&d, "agg").await;
    assert!(
        matches!(
            found.as_slice(),
            [
                Divergence::Capture(CaptureFault::MissingTrigger { table: middle, .. }),
                Divergence::Capture(CaptureFault::TriggerNotAlways { table: default, .. }),
            ] if middle == "public.items_a" && default == "public.items_rest"
        ),
        "{found:?}"
    );
}

/// The audit reads the nodes the pass recorded that are still in the tree.
/// A leaf detached since the pass is no node of the source, whatever its
/// triggers, and one attached since is bare until the next pass installs its
/// triggers: neither is a fault. Once the pass has run, the new leaf's
/// triggers are audited like any other's.
#[tokio::test]
async fn a_leaf_detached_or_attached_since_the_pass_is_no_fault() {
    let mut d = start(TREE, &[AGG], &[SOURCE]).await;
    d.user()
        .await
        .batch_execute(&format!(
            "alter table public.items detach partition public.items_b; {} \
             create table public.items_c (id integer not null, g integer not null, \
                 v integer not null, primary key (g, id)); \
             alter table public.items attach partition public.items_c for values in (5, 6)",
            CaptureEvent::ALL
                .iter()
                .map(|e| format!(
                    "drop trigger {} on public.items_b;",
                    trigger_name(DEFAULT_SCHEMA, *e)
                ))
                .collect::<String>()
        ))
        .await
        .expect("detach a bare leaf, attach a bare one");
    assert_eq!(
        check(&d, "agg").await,
        [],
        "neither the detached nor the attached leaf is a recorded node in the tree"
    );

    pass(&mut d).await;
    d.user()
        .await
        .batch_execute(&on_triggers(
            "public.items_c",
            "disable",
            &[CaptureEvent::ALL[0]],
        ))
        .await
        .expect("disable the new leaf's trigger");
    let found = check(&d, "agg").await;
    assert!(
        matches!(
            found.as_slice(),
            [Divergence::Capture(CaptureFault::TriggerNotAlways { table, .. })]
                if table == "public.items_c"
        ),
        "{found:?}"
    );
}

/// A `JOIN` reading a partitioned root as its joined side. Its build refuses
/// a partitioned side until #1111 (J6), so the definition never goes live
/// here and `self_check` would skip it; the audit runs on the definition as
/// if it were live. It is clean, and a leaf's disabled trigger is named, as
/// for an aggregate.
#[tokio::test]
async fn a_join_reading_a_partitioned_side_audits_clean_and_names_a_leafs_disabled_trigger() {
    let cluster = testkit::TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut raw = drain_driver::connect(db.dsn()).await;
    raw.batch_execute(&format!(
        "{TREE}; create table public.facts (id integer primary key, k integer, w integer); \
         create index on public.facts (k)"
    ))
    .await
    .expect("the tables");
    let trellis = trellis::Trellis::connect(
        trellis::Config::from_dsn(db.dsn().to_string()).expect("valid dsn"),
        trellis::TrellisOptions::default(),
    )
    .await
    .expect("connect");
    trellis
        .apply(
            "TRANSFORM j FROM public.facts JOIN public.items ON facts.k = items.g \
             GROUP BY items.g WITHOUT SOURCE INDEX SELECT items.g AS g, COUNT(*) AS n, SUM(facts.w) AS sw",
        )
        .await
        .expect("define a JOIN over the partitioned side");
    let desired = trellis::defs::tables_to_capture(&db.pool)
        .await
        .expect("tables to capture");
    let outcome = trellis::capture::reconcile::reconcile(
        &mut raw,
        DEFAULT_SCHEMA,
        &desired,
        Instant::now() + Duration::from_secs(5),
    )
    .await
    .expect("pass");
    assert!(outcome.failed.is_empty(), "{:?}", outcome.failed);
    assert!(
        trellis::capture::partitions::recorded(&raw, SOURCE)
            .await
            .expect("read the recorded tree")
            .is_some(),
        "the pass records the JOIN's partitioned side"
    );
    let mut def = trellis::defs::catalog::definition_by_target(&db.pool, "j")
        .await
        .expect("read the definition")
        .expect("the definition");
    def.status = trellis::defs::TransformStatus::Live;
    let audit = || trellis::staging::capture_audit::audit(&raw, DEFAULT_SCHEMA, &def);
    assert_eq!(audit().await.expect("audit"), [], "a complying JOIN side");

    raw.batch_execute(&on_triggers(
        "public.items_a",
        "disable",
        &[CaptureEvent::ALL[0]],
    ))
    .await
    .expect("disable");
    let found = audit().await.expect("audit");
    assert!(
        matches!(
            found.as_slice(),
            [CaptureFault::TriggerNotAlways { table, .. }] if table == "public.items_a"
        ),
        "{found:?}"
    );
}
