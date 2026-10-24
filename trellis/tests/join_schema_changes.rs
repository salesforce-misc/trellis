//! A live `JOIN` target across a schema change of either table (#1110 J5,
//! ADR-0019): its copies of a `varchar(n)` column are unbounded (#1207), so a
//! widening needs no re-type and no value waits for the staging worker's
//! pass. No table a `JOIN` or a group-basis aggregate creates holds a bounded
//! `varchar`.
//!
//! The group-basis dev knob is on before the driver starts (a `JOIN`
//! applies only behind it); the pipeline and the pass are stepped by hand
//! (`support/drain_driver.rs`), and nothing polls for convergence (#297).

#[path = "support/drain_driver.rs"]
mod drain_driver;
#[path = "support/varchar_columns.rs"]
mod varchar_columns;

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
    WITHOUT SOURCE INDEX SELECT a.g AS g, b.h AS h, COUNT(*) AS n, SUM(a.x) AS sx, SUM(b.y) AS sy";

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
/// `varchar(20)` (#1207). The target and the hidden `P`, `M`, `J` and `T`
/// hold their copies of these columns as unbounded `varchar`, so a value
/// the old type couldn't hold, and a value of only trailing spaces past it
/// (which a `varchar(5)` copy would truncate into the group `'g1'`), is
/// applied by the first page that reads it, before any pass on either
/// table: nothing is held, the oracle agrees, and the pass that follows
/// re-types and pauses nothing.
#[tokio::test]
async fn a_widened_varchar_of_either_table_is_applied_before_any_pass_on_a_live_join() {
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
    // No pass in between: a group that differs from `'g1'` only by trailing
    // spaces, a longer group and a longer key, and a longer `h` on each table.
    write(
        &d,
        "insert into public.a values (3, 'g1      ', 'k1', 3), \
                                     (4, 'a-longer-group', 'a-longer-key', 4); \
         insert into public.b values (3, 'a-longer-key', 'a-longer-h', 30); \
         update public.b set h = 'h-two-longer' where id = 2",
    )
    .await;
    d.settle().await;
    let held: i64 = d
        .ctl
        .query_one("select count(*) from poison_held", &[])
        .await
        .expect("held keys")
        .get(0);
    assert_eq!(held, 0, "nothing fails: the copies hold any length");
    assert_eq!(diverged(&d).await, Vec::<String>::new());

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
    assert_eq!(
        status, "live",
        "the pass has nothing to re-type or pause for"
    );
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
            "character varying",
            "{table}.{column}"
        );
    }
    write(
        &d,
        "insert into public.a values (5, 'g1       ', 'k2', 5); \
         update public.a set x = 9 where id = 3",
    )
    .await;
    d.settle().await;
    assert_eq!(diverged(&d).await, Vec::<String>::new());
}

/// Rule 1 of #1207's decision, over the tables a `JOIN` and a group-basis
/// aggregate create: a `JOIN` grouped by `varchar(n)` columns of both tables
/// over a `varchar(n)` `ON` pair, another grouped by a `varchar(n)` key, and
/// a group-basis aggregate grouped by two `varchar(n)` columns. None of
/// their columns has a length: the targets, `P`, `M`, `J`, `T`, `agg_<id>`,
/// and the working tables a page leaves in its session.
#[tokio::test]
async fn no_table_a_join_or_group_basis_creates_holds_a_bounded_varchar() {
    let mut d = Driver::start(
        "create table public.a (id varchar(5) primary key, g varchar(5), \
                                k varchar(5), x integer); \
         create table public.b (id varchar(5) primary key, k varchar(5), \
                                h varchar(5), y integer); \
         create index on public.a (k); create index on public.b (k); \
         create index on public.a (g, k)",
        &[
            ("id", ValueType::Text),
            ("g", ValueType::Text),
            ("k", ValueType::Text),
            ("x", ValueType::Integer(IntWidth::Int4)),
        ],
        &[
            LEFT,
            "TRANSFORM by_id FROM public.a JOIN public.b ON a.k = b.k \
             GROUP BY a.id WITHOUT SOURCE INDEX SELECT a.id AS id, COUNT(*) AS n",
            "TRANSFORM per_gk FROM public.a GROUP BY g, k \
            WITHOUT SOURCE INDEX SELECT g AS g, k AS k, COUNT(*) AS n, SUM(x) AS sx",
        ],
        &["public.a", "public.b"],
    )
    .await;
    write(
        &d,
        "insert into public.a values ('1', 'g1', 'k1', 1), ('2', 'g2', 'k2', 2); \
         insert into public.b values ('1', 'k1', 'h1', 10)",
    )
    .await;
    d.settle().await;
    pass(&mut d).await;
    d.settle().await;

    let columns = varchar_columns::read(&d.ctl, &["public.a", "public.b"]).await;
    let bounded: Vec<&String> = columns.iter().filter(|c| c.contains('(')).collect();
    assert_eq!(bounded, Vec::<&String>::new(), "{columns:?}");
    for copy in ["public.l.g ", "public.l.h ", "public.by_id.id "] {
        assert!(
            columns.iter().any(|c| c.starts_with(copy)),
            "{copy}is a copy: {columns:?}"
        );
    }
    for hidden in ["_p.", "_m.", "_j.", "_t."] {
        assert!(
            columns
                .iter()
                .any(|c| c.starts_with("trellis.join_") && c.contains(hidden)),
            "{hidden} holds copies: {columns:?}"
        );
    }
}
