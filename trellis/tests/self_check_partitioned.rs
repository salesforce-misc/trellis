//! #1179: `self_check`'s capture audit over a partitioned source. A root the
//! reconcile pass records is no hierarchy fault, and the audit reads the five
//! capture triggers of every recorded node, naming a node whose triggers were
//! disabled (ADR-0017, "Every node carries capture").

#[path = "support/drain_driver.rs"]
mod drain_driver;

use std::time::{Duration, Instant};

use drain_driver::Driver;
use trellis::config::DEFAULT_SCHEMA;
use trellis::defs::ValueType;
use trellis::integer::IntWidth;
use trellis::staging::capture_audit::CaptureFault;
use trellis::staging::self_check::{
    Divergence, SelfCheckMode, SelfCheckOutcome, SelfCheckScope, check_page,
};

const SOURCE: &str = "public.items";
const AGG: &str = "TRANSFORM agg FROM public.items GROUP BY g SELECT SUM(v) AS total";
const TREE: &str = "create table public.items (id integer not null, g integer not null, \
        v integer not null, primary key (g, id)) partition by list (g); \
     create table public.items_a partition of public.items for values in (1, 2); \
     create table public.items_b partition of public.items for values in (3, 4)";

fn int4() -> ValueType {
    ValueType::Integer(IntWidth::Int4)
}

async fn check(d: &Driver) -> Vec<Divergence> {
    let page = check_page(
        d.pool(),
        "agg",
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

#[tokio::test]
async fn a_recorded_partitioned_source_is_clean_and_a_leafs_disabled_trigger_is_named() {
    trellis::dev::staging::set_group_basis(true);
    trellis::defs::partition_tree::open_gate_for_tests();
    let mut d = Driver::start(
        TREE,
        &[("id", int4()), ("g", int4()), ("v", int4())],
        &[AGG],
        &[SOURCE],
    )
    .await;
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
    d.user()
        .await
        .batch_execute("insert into public.items values (1, 1, 5), (2, 3, 7)")
        .await
        .expect("seed");
    d.settle().await;

    assert_eq!(check(&d).await, [], "a complying partitioned source");

    let trigger = trellis::capture::sql::trigger_name(
        DEFAULT_SCHEMA,
        trellis::capture::sql::CaptureEvent::ALL[0],
    );
    d.user()
        .await
        .batch_execute(&format!(
            "alter table public.items_b disable trigger {trigger}"
        ))
        .await
        .expect("disable");
    let found = check(&d).await;
    assert!(
        matches!(
            found.as_slice(),
            [Divergence::Capture(CaptureFault::TriggerNotAlways { table, .. })]
                if table == "public.items_b"
        ),
        "{found:?}"
    );
}
