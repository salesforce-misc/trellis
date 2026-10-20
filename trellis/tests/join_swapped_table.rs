//! A live `JOIN` whose joined table is swapped for a copy (#1177): the
//! staging worker's pass reinstalls capture on the recreated table and its
//! re-read reaches the orphan sweep, which has no arm for a `JOIN` until its
//! build (J4, #1109). The pass fails that marker and backs it off rather than
//! panicking the worker. Since #707 the swap is first a disruption of capture
//! that pauses the `JOIN`: the test resumes it and steps the passes after.
//!
//! The group-basis dev knob is on before the driver starts (a `JOIN` applies
//! only behind it); the pass is stepped by hand and nothing polls (#297).

#[path = "support/drain_driver.rs"]
mod drain_driver;

use std::time::Duration;

use drain_driver::Driver;
use trellis::config::DEFAULT_SCHEMA;
use trellis::defs::ValueType;
use trellis::integer::IntWidth;

const J: &str = "TRANSFORM j2 FROM public.ja LEFT JOIN public.jb ON ja.k = jb.k \
     GROUP BY ja.g SELECT ja.g AS g, COUNT(*) AS n, SUM(jb.y) AS sy";

#[tokio::test]
async fn a_swapped_joined_table_does_not_panic_the_pass() {
    trellis::dev::staging::set_group_basis(true);
    let mut d = Driver::start(
        "create table public.ja (id integer primary key, g text, k text); \
         create table public.jb (id integer primary key, k text, y integer)",
        &[
            ("id", ValueType::Integer(IntWidth::Int4)),
            ("g", ValueType::Text),
            ("k", ValueType::Text),
        ],
        &[J],
        &["public.ja", "public.jb"],
    )
    .await;
    d.user()
        .await
        .batch_execute(
            "insert into public.ja values (1, 'a', 'k1'); \
             insert into public.jb values (1, 'k1', 10)",
        )
        .await
        .expect("seed");
    d.settle().await;

    d.ctl
        .batch_execute(
            "create table public.jb_new (like public.jb including all); \
             insert into public.jb_new select id, k, y + 1 from public.jb; \
             alter table public.jb rename to jb_old; \
             alter table public.jb_new rename to jb",
        )
        .await
        .expect("swap");

    let pool = d.pool().clone();
    // The swap is a disruption of Trellis's capture (#707): the first pass
    // pauses the `JOIN` and installs nothing. A resume re-validates it
    // against the table now at the name, and the passes after it reinstall
    // capture on the recreated table, which is where the orphan sweep meets
    // a `JOIN` (#1177).
    trellis::client::reconcile_pass(
        &mut d.ctl,
        &pool,
        DEFAULT_SCHEMA,
        "join_swapped_table_wake",
        Duration::from_secs(2),
    )
    .await
    .expect("reconcile pass");
    let status = d
        .rows("select status from transform_definitions where target_table = 'public.j2'")
        .await;
    assert_eq!(
        status,
        vec!["(paused)".to_string()],
        "the swap pauses the JOIN"
    );
    let definer = trellis::Trellis::connect(
        trellis::Config::from_dsn(d.db.dsn().to_string()).expect("valid dsn"),
        trellis::TrellisOptions::default(),
    )
    .await
    .expect("connect a definer");
    definer
        .apply("RESUME TRANSFORM j2")
        .await
        .expect("the resume re-validates against the swapped table");

    // Two passes: the second skips the backed-off marker, so it fails once.
    for _ in 0..2 {
        trellis::client::reconcile_pass(
            &mut d.ctl,
            &pool,
            DEFAULT_SCHEMA,
            "join_swapped_table_wake",
            Duration::from_secs(2),
        )
        .await
        .expect("reconcile pass");
    }

    // The passes after the resume rebuild the `JOIN` rather than park a
    // marker the orphan sweep would fail (#1177 is the guard if one is
    // parked): nothing is left failing on the swapped table.
    let rows = d
        .rows("select attempts from pending_backfill where table_name = 'public.jb' and last_error is not null")
        .await;
    assert!(rows.is_empty(), "no failing marker is left: {rows:?}");
}
