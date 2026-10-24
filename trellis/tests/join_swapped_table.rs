//! A live `JOIN` whose joined table is swapped for a copy (#1177): the
//! staging worker's pass reinstalls capture on the recreated table. Since
//! #707 the swap is first a disruption of capture that pauses the `JOIN`: the
//! test resumes it and steps the passes after, neither of which may fail.
//!
//! The pass is stepped by hand and nothing polls (#297).

#[path = "support/drain_driver.rs"]
mod drain_driver;

use drain_driver::Driver;
use trellis::config::DEFAULT_SCHEMA;
use trellis::defs::ValueType;
use trellis::integer::IntWidth;

const J: &str = "TRANSFORM j2 FROM public.ja LEFT JOIN public.jb ON ja.k = jb.k \
     GROUP BY ja.g WITHOUT SOURCE INDEX SELECT ja.g AS g, COUNT(*) AS n, SUM(jb.y) AS sy";

#[tokio::test]
async fn a_swapped_joined_table_does_not_panic_the_pass() {
    let mut d = Driver::start(
        "create table public.ja (id integer primary key, g text, k text); \
         create table public.jb (id integer primary key, k text, y integer); \
         create index on public.ja (k); create index on public.jb (k)",
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
    // capture on the recreated table (#1177).
    trellis::client::reconcile_pass(&mut d.ctl, &pool, DEFAULT_SCHEMA)
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

    // Two passes after the resume: neither fails.
    for _ in 0..2 {
        trellis::client::reconcile_pass(&mut d.ctl, &pool, DEFAULT_SCHEMA)
            .await
            .expect("reconcile pass");
    }
}
