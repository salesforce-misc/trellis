//! A live `JOIN` whose joined table is swapped for a copy (#1177): the
//! staging worker's pass reinstalls capture on the recreated table and its
//! re-read reaches the orphan sweep, which has no arm for a `JOIN` until its
//! build (J4, #1109). The pass fails that marker and backs it off rather than
//! panicking the worker.
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

    let rows = d
        .rows(
            "select attempts, last_error from pending_backfill \
             where table_name = 'public.jb'",
        )
        .await;
    assert_eq!(
        rows.len(),
        1,
        "the swapped table's marker stays parked: {rows:?}"
    );
    assert!(
        rows[0].starts_with("(1,") && rows[0].contains("JOIN target is rebuilt"),
        "failed once and backed off, with the reason recorded: {rows:?}"
    );
}
