//! A live `JOIN` target across a schema change of either table (#1110 J5,
//! ADR-0019): a widening that changes only the catalog is applied to the
//! target and the hidden tables in place by the staging worker's pass, and
//! the keys a too-long value held before the pass are released and
//! recomputed through the `JOIN`'s release.
//!
//! The group-basis dev knob is on before the driver starts (a `JOIN`
//! applies only behind it); the pipeline and the pass are stepped by hand
//! (`support/drain_driver.rs`), and nothing polls for convergence (#297).

#[path = "support/drain_driver.rs"]
mod drain_driver;

use std::time::Duration;

use drain_driver::Driver;
use trellis::config::DEFAULT_SCHEMA;
use trellis::defs::ValueType;
use trellis::integer::IntWidth;

const DDL: &str = "create table public.a (id integer primary key, g varchar(5), \
                       k varchar(5), x integer); \
                   create table public.b (id integer primary key, k varchar(5), \
                       h varchar(5), y integer); \
                   create index on public.a (k); create index on public.b (k)";

const LEFT: &str = "TRANSFORM l FROM public.a LEFT JOIN public.b ON a.k = b.k \
     GROUP BY a.g, b.h \
     SELECT a.g AS g, b.h AS h, COUNT(*) AS n, SUM(a.x) AS sx, SUM(b.y) AS sy";

const ORACLE: &str = "select a.g, b.h, count(*)::bigint as n, sum(a.x)::bigint as sx, \
                             sum(b.y)::bigint as sy \
                      from public.a left join public.b on a.k = b.k group by a.g, b.h";

async fn start() -> Driver {
    trellis::dev::staging::set_group_basis(true);
    Driver::start(
        DDL,
        &[
            ("id", ValueType::Integer(IntWidth::Int4)),
            ("g", ValueType::Text),
            ("k", ValueType::Text),
            ("x", ValueType::Integer(IntWidth::Int4)),
        ],
        &[LEFT],
        &["public.a", "public.b"],
    )
    .await
}

async fn write(d: &Driver, sql: &str) {
    d.user().await.batch_execute(sql).await.expect(sql);
}

/// The target's rows that differ from the oracle's, either way.
async fn diverged(d: &Driver) -> Vec<String> {
    d.rows(&format!(
        "(select g, h, n, sx, sy from public.l except {ORACLE}) \
         union all ({ORACLE} except select g, h, n, sx, sy from public.l)"
    ))
    .await
}

async fn column_type(d: &Driver, table: &str, column: &str) -> String {
    d.ctl
        .query_one(
            "select pg_catalog.format_type(atttypid, atttypmod) from pg_catalog.pg_attribute \
             where attrelid = pg_catalog.to_regclass($1) and attname = $2",
            &[&table, &column],
        )
        .await
        .unwrap_or_else(|e| panic!("{table}.{column}: {e}"))
        .get(0)
}

/// Seals what was written, evicts its one failing key at a threshold of
/// one, then drains the rest.
async fn hold_the_failing_key(d: &mut Driver) {
    use trellis::staging::quarantine::{IsolationOutcome, isolate_and_evict};
    use trellis::staging::{BucketFilter, claim, fold};
    let seg = d.seal().await;
    let folded = {
        let mut conn = d.pool().get().await.expect("pool");
        let txn = conn.transaction().await.expect("txn");
        claim::claim(&txn, seg, "w", 1).await.expect("claim");
        let folded = fold::fold(&txn, seg, BucketFilter::all())
            .await
            .expect("fold");
        txn.commit().await.expect("commit the claim");
        folded
    };
    let outcome = isolate_and_evict(d.pool(), seg, "w", drain_driver::WAKE, &folded, 1, false)
        .await
        .expect("isolation");
    assert!(
        matches!(outcome, IsolationOutcome::Evicted { evicted: 1, .. }),
        "{outcome:?}"
    );
    d.drain(seg, "w").await;
}

async fn pass(d: &mut Driver) {
    let pool = d.pool().clone();
    trellis::client::reconcile_pass(
        &mut d.ctl,
        &pool,
        DEFAULT_SCHEMA,
        "join_schema_changes_wake",
        Duration::from_secs(2),
    )
    .await
    .expect("reconcile pass");
}

/// Both `GROUP BY` columns and the `ON` pair widen from `varchar(5)` to
/// `varchar(20)`. Values too long for the old type, written before the
/// staging worker's pass on either table, fail their pages and are held;
/// the pass re-types the target and `P`, `M`, `J` and `T` in place, pauses
/// nothing, and releases the held keys, whose release recomputes their
/// cells. Longer values written after it apply as any change does.
#[tokio::test]
async fn a_catalog_only_widening_of_either_table_is_retyped_in_place_on_a_live_join() {
    let mut d = start().await;
    write(
        &d,
        "insert into public.a values (1, 'g1', 'k1', 1), (2, 'g2', 'k2', 2); \
         insert into public.b values (1, 'k1', 'h1', 10), (2, 'k2', 'h2', 20)",
    )
    .await;
    d.settle().await;
    assert_eq!(diverged(&d).await, Vec::<String>::new());

    d.ctl
        .batch_execute(
            "alter table public.a alter column g type varchar(20), \
                                  alter column k type varchar(20); \
             alter table public.b alter column k type varchar(20), \
                                  alter column h type varchar(20)",
        )
        .await
        .expect("widen");
    // One key too long for each table's old type, each alone in its batch,
    // held at a threshold of one as the drain's isolation would after its
    // deaths.
    for sql in [
        "insert into public.a values (3, 'group-three', 'k1', 3)",
        "insert into public.b values (4, 'key-four', 'h-four-long', 40)",
    ] {
        write(&d, sql).await;
        hold_the_failing_key(&mut d).await;
    }
    let held: i64 = d
        .ctl
        .query_one("select count(*) from poison_held", &[])
        .await
        .expect("held keys")
        .get(0);
    assert_eq!(held, 2, "each value too long for the old type is held");

    pass(&mut d).await;
    let status: String = d
        .ctl
        .query_one(
            "select status from transform_definitions where target_table = 'public.l'",
            &[],
        )
        .await
        .expect("status")
        .get(0);
    assert_eq!(status, "live");
    let id: i64 = d
        .ctl
        .query_one(
            "select id from transform_definitions where target_table = 'public.l'",
            &[],
        )
        .await
        .expect("id")
        .get(0);
    let hidden = |part: &str| format!("{DEFAULT_SCHEMA}.join_{id}_{part}");
    for (table, column) in [
        ("public.l".to_string(), "g"),
        ("public.l".to_string(), "h"),
        (hidden("p"), "g"),
        (hidden("p"), "k"),
        (hidden("m"), "k"),
        (hidden("m"), "h"),
        (hidden("j"), "k"),
        (hidden("t"), "g"),
        (hidden("t"), "h"),
    ] {
        assert_eq!(
            column_type(&d, &table, column).await,
            "character varying(20)",
            "{table}.{column}"
        );
    }
    let held: i64 = d
        .ctl
        .query_one("select count(*) from poison_held", &[])
        .await
        .expect("held keys")
        .get(0);
    assert_eq!(held, 0, "the pass released every held key");
    d.settle().await;
    assert_eq!(diverged(&d).await, Vec::<String>::new());

    write(
        &d,
        "insert into public.a values (5, 'a-longer-group', 'a-longer-key', 5); \
         insert into public.b values (5, 'a-longer-key', 'a-longer-h', 50); \
         update public.b set h = 'h-two-longer' where id = 2",
    )
    .await;
    d.settle().await;
    assert_eq!(diverged(&d).await, Vec::<String>::new());
}
