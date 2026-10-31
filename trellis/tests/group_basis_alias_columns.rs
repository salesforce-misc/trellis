//! A source column named like an alias in Trellis's generated SQL (`r`, `s`,
//! `e`, `p`, `h`, `t`, ...) must be an ordinary column: the aggregate path
//! (build, apply, recompute, `self_check`, rebuild) reads the source's columns
//! only where it names them, never through an alias of its own (#1456).

#[path = "support/drain_driver.rs"]
mod drain_driver;

use std::time::Duration;

use drain_driver::Driver;
use trellis::config::Config;
use trellis::defs::ValueType;
use trellis::defs::chunk_queue;
use trellis::integer::IntWidth;
use trellis::staging::build::{self, WorkerOptions};
use trellis::staging::self_check::{
    SelfCheckMode, SelfCheckOutcome, SelfCheckScope, check_page, compare_page,
};
use trellis::{Trellis, TrellisOptions};

fn int4() -> ValueType {
    ValueType::Integer(IntWidth::Int4)
}

/// Every name the generated SQL uses as a table alias, a CTE or a CTE column,
/// each a source column here.
const ALIASES: &[&str] = &[
    "r",
    "s",
    "e",
    "p",
    "h",
    "t",
    "u",
    "i",
    "a",
    "x",
    "b",
    "f",
    "n",
    "c",
    "d",
    "k",
    "l",
    "m",
    "o",
    "q",
    "j",
    "w",
    "y",
    "z",
    "sd",
    "raw",
    "sides",
    "bad",
    "ev",
    "hinted",
    "staged",
    "up",
    "sg",
    "rg",
    "pg",
    "snap",
    "lost",
    "cur",
    "vis",
    "nv",
    "ins",
    "written",
    "deleted",
    "page",
    "unc",
    "hid",
    "hid0",
    "he",
    "implied",
    "kept",
    "rows",
    "src",
    "ph",
    "g2",
    "v2",
    "__i",
    "__txid",
    "__lsn",
    "__sgn",
    "__hc",
    "__n",
    "__rn",
    "__c1",
    "__s1",
    "__a1",
    "__bad",
    "__seg",
    "__basis",
    "__changed",
    "__nn",
    "__meta",
    "__on",
    "__k",
    "__v",
    "__f",
    "__w",
    "__visible",
    "__kind",
    "__key",
    "__o",
    "__x",
    "__nf0",
    "__vf0",
    "__tc",
    "__snap",
    "__stamp",
    "__zero",
    "__matches",
    "__dn",
    "__nc1",
    "__oc1",
    "__rc1",
    "__dc1",
    "__ns1",
    "__sum_1",
    "__count_1",
    "__trellis_none",
    // The ring's columns, read beside a populated image.
    "src_table",
    "key",
    "op",
    "lsn",
    "old_image",
    "new_image",
    "transition_images",
    "row_txid",
    "origin_lsn",
    "src_changed",
    "img",
    // The keys of the ring's marker objects (a node truncate's, a schema
    // change's), which are not images of a source row.
    "node",
    "oid",
    "missing",
    "key_missing",
    // The capture function's PL/pgSQL variables and trigger variables, read
    // beside the transition tables' columns.
    "slot",
    "ts",
    "present",
    "have",
    "st",
    "reread",
    "tr",
    "tg_relid",
    "tg_op",
    "tg_name",
    "tg_table_name",
    "new_rows",
    "old_rows",
];

fn ddl() -> String {
    let mut columns = vec![
        "id integer primary key".to_string(),
        "g integer".to_string(),
        "v integer".to_string(),
    ];
    columns.extend(ALIASES.iter().map(|a| format!("\"{a}\" integer")));
    format!(
        "create table public.src ({}); create index on public.src (g)",
        columns.join(", ")
    )
}

fn columns() -> Vec<(&'static str, ValueType)> {
    let mut columns = vec![("id", int4()), ("g", int4()), ("v", int4())];
    columns.extend(ALIASES.iter().map(|a| (*a, int4())));
    columns
}

/// `(id, g, v)` and every alias column set to `v + <position>`.
fn row(id: i32, g: Option<i32>, v: i32) -> String {
    let g = g.map_or("null".to_string(), |g| g.to_string());
    let rest: Vec<String> = (0..ALIASES.len())
        .map(|i| (v + i as i32).to_string())
        .collect();
    format!("({id}, {g}, {v}, {})", rest.join(", "))
}

const AGG: &str = "TRANSFORM agg FROM public.src GROUP BY g WITHOUT SOURCE INDEX \
    SELECT g AS g, COUNT(*) AS n, SUM(r) AS sum_r, SUM(s + e) AS sum_se, \
    COUNT(p) AS cnt_p, SUM(v) AS total";

/// The same groups under names Trellis's SQL also uses as aliases: the group
/// column is a source column named `h`, and the fields are named `r`, `s`, ...
const ALIASED: &str = "TRANSFORM aliased FROM public.src GROUP BY h WITHOUT SOURCE INDEX \
    SELECT h AS h, COUNT(*) AS r, SUM(v) AS s, COUNT(p) AS t, SUM(e) AS u, SUM(r) AS up, \
    COUNT(*) AS cur, SUM(s) AS nv, COUNT(*) AS sg";
const ALIASED_ORACLE: &str = "select h, count(*), sum(v), count(p), sum(e), sum(r), count(*), \
    sum(s), count(*) from public.src group by h order by h nulls last";
const ALIASED_TARGET: &str = "select h, r, s, t, u, up, cur, nv, sg from public.aliased \
    order by h nulls last";

/// `g, n, sum_r, sum_se, cnt_p, total` as the source says them.
const ORACLE: &str = "select g, count(*), sum(r), sum(s + e), count(p), sum(v) from public.src \
    group by g order by g nulls last";
const TARGET: &str =
    "select g, n, sum_r, sum_se, cnt_p, total from public.agg order by g nulls last";

async fn step(d: &mut Driver, sql: &str) {
    d.user().await.batch_execute(sql).await.expect(sql);
    let batch = d.seal().await;
    d.drain(batch, "step").await;
}

/// `sql`'s rows as text. (`Driver::rows` aliases its subquery `r`, a field name
/// here.)
async fn rows(d: &Driver, sql: &str) -> Vec<String> {
    d.ctl
        .query(&format!("select __q::text from ({sql}) __q"), &[])
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
        .into_iter()
        .map(|row| row.get(0))
        .collect()
}

async fn assert_matches_source(d: &Driver, when: &str) {
    assert_eq!(
        rows(d, TARGET).await,
        rows(d, ORACLE).await,
        "the target is the source's GROUP BY {when}"
    );
    assert_eq!(
        rows(d, ALIASED_TARGET).await,
        rows(d, ALIASED_ORACLE).await,
        "the aliased-name target is the source's GROUP BY {when}"
    );
}

async fn start() -> Driver {
    // The first rows are seeded, so the definition's build runs over a table
    // with rows.
    let seed = format!(
        "{}; insert into public.src values {}, {}, {}",
        ddl(),
        row(1, Some(1), 10),
        row(2, Some(1), 20),
        row(3, Some(2), 30)
    );
    Driver::start(&seed, &columns(), &[AGG, ALIASED], &["public.src"]).await
}

const OPTIONS: WorkerOptions = WorkerOptions {
    chunk_rows: 1_000,
    drain_batch_cap: usize::MAX,
    heartbeat_interval: Duration::from_secs(5),
    reclaim_ttl: Duration::from_secs(60),
};

async fn request_backfill_and_rebuild(d: &Driver) {
    let trellis = Trellis::connect(
        Config::from_dsn(d.db.dsn().to_string()).expect("valid dsn"),
        TrellisOptions::default(),
    )
    .await
    .expect("connect a Trellis");
    trellis
        .request_backfill("src")
        .await
        .expect("request a rebuild");
    d.run_frozen(&[], |pool| async move {
        let claimed = chunk_queue::claim_chunks_of(
            &**pool.get().await.expect("pool"),
            "rebuild",
            1,
            &[chunk_queue::KIND_SWEEP],
        )
        .await
        .expect("claim the rebuild");
        assert_eq!(claimed.len(), 1, "one rebuild job to claim");
        build::run_claimed(&pool, &claimed[0], "rebuild", &OPTIONS).await;
        Ok(())
    })
    .await
    .finish()
    .await;
}

async fn assert_self_check_clean(d: &Driver) {
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
    assert!(
        matches!(page.outcome, SelfCheckOutcome::Converged),
        "self_check: {:?}",
        page.outcome
    );
}

/// The build over seeded rows, then inserts, group moves, deletes and updates
/// through apply (staging, naming groups, the hinted recompute), the check and
/// a rebuild, over a source whose every column is named like a generated alias.
#[tokio::test]
async fn an_aggregate_over_columns_named_like_generated_aliases_builds_applies_checks_and_rebuilds()
{
    let mut d = start().await;
    assert_matches_source(&d, "after the build").await;

    step(
        &mut d,
        &format!(
            "insert into public.src values {}, {}, {}",
            row(4, Some(2), 40),
            row(5, Some(3), 50),
            row(6, None, 60)
        ),
    )
    .await;
    assert_matches_source(&d, "after inserts").await;

    step(&mut d, "update public.src set g = 3 where id = 2").await;
    step(
        &mut d,
        "update public.src set r = r + 100, v = v + 1 where id = 3",
    )
    .await;
    step(&mut d, "delete from public.src where id in (5, 6)").await;
    assert_matches_source(&d, "after updates and deletes").await;
    assert_self_check_clean(&d).await;

    // Changes the drain hasn't applied: the comparison subtracts the ring's
    // rows from the source, as weighted rows over the source's columns.
    d.user()
        .await
        .batch_execute(
            "update public.src set r = r + 1, g = 2 where id = 1; delete from public.src where id = 3",
        )
        .await
        .expect("unapplied changes");
    let unapplied = compare_page(
        d.pool(),
        "agg",
        SelfCheckScope {
            after: None,
            limit: 100,
        },
    )
    .await
    .expect("a comparison");
    assert_eq!(
        (unapplied.divergences, unapplied.uncertain_keys),
        (vec![], vec![]),
        "the ring's unapplied rows are subtracted from the source"
    );
    let batch = d.seal().await;
    d.drain(batch, "apply").await;
    assert_matches_source(&d, "after the last changes").await;

    // Image-less changes (Re-derive) recompute their keys' groups, with and
    // without a prior image.
    d.stage_recomputes("public.src", &["1", "3"]).await;
    let batch = d.seal().await;
    d.drain(batch, "recompute").await;
    assert_matches_source(&d, "after a re-derive").await;
    let prior = r#"{"id": 1, "g": 2, "v": 10, "h": 5, "r": 1, "s": 2, "e": 3, "p": 4}"#.to_string();
    d.stage_recomputes_with_prior("public.src", &[("1", Some(prior))])
        .await;
    let batch = d.seal().await;
    d.drain(batch, "recompute-prior").await;
    assert_matches_source(&d, "after a re-derive with a prior image").await;

    request_backfill_and_rebuild(&d).await;
    assert_matches_source(&d, "after a rebuild").await;
}

// ---------------------------------------------------------------- JOIN

const JOIN_BASE_A: &[&str] = &["id", "ga", "t", "k", "x"];
const JOIN_BASE_B: &[&str] = &["id", "t", "k", "gb", "y"];

/// `base`'s columns, then every alias not already one of them.
fn side_columns(base: &[&'static str]) -> Vec<&'static str> {
    let mut columns = base.to_vec();
    columns.extend(ALIASES.iter().filter(|a| !base.contains(a)));
    columns
}

fn side_ddl(table: &str, base: &[&'static str]) -> String {
    let columns: Vec<String> = side_columns(base)
        .iter()
        .map(|c| {
            if *c == "id" {
                "id integer primary key".to_string()
            } else {
                format!("\"{c}\" integer")
            }
        })
        .collect();
    format!(
        "create table public.{table} ({}); create index on public.{table} (t, k)",
        columns.join(", ")
    )
}

/// A row of `base`'s shape: the named base columns, every other column
/// `seed + position`.
fn side_row(base: &[&'static str], base_values: &[Option<i32>], seed: i32) -> String {
    let values: Vec<String> = side_columns(base)
        .iter()
        .enumerate()
        .map(|(i, _)| match base_values.get(i) {
            Some(Some(v)) => v.to_string(),
            Some(None) => "null".to_string(),
            None => (seed + i as i32).to_string(),
        })
        .collect();
    format!("({})", values.join(", "))
}

fn row_a(id: i32, ga: i32, t: i32, k: i32, x: i32) -> String {
    side_row(
        JOIN_BASE_A,
        &[Some(id), Some(ga), Some(t), Some(k), Some(x)],
        id,
    )
}

fn row_b(id: i32, t: i32, k: i32, gb: i32, y: i32) -> String {
    side_row(
        JOIN_BASE_B,
        &[Some(id), Some(t), Some(k), Some(gb), Some(y)],
        100 + id,
    )
}

const LEFT_JOIN: &str = "TRANSFORM lj FROM public.ja LEFT JOIN public.jb \
    ON ja.t = jb.t AND ja.k = jb.k GROUP BY ja.ga, jb.gb WITHOUT SOURCE INDEX \
    SELECT ja.ga AS ga, jb.gb AS gb, COUNT(*) AS n, SUM(ja.x) AS sx, SUM(jb.y) AS sy, \
    SUM(ja.r) AS sr, COUNT(jb.s) AS cs";
const LEFT_JOIN_ORACLE: &str = "select ja.ga, jb.gb, count(*), sum(ja.x), sum(jb.y), sum(ja.r), \
    count(jb.s) from public.ja left join public.jb on ja.t = jb.t and ja.k = jb.k \
    group by ja.ga, jb.gb order by ja.ga nulls last, jb.gb nulls last";
const LEFT_JOIN_TARGET: &str =
    "select ga, gb, n, sx, sy, sr, cs from public.lj order by ga nulls last, gb nulls last";

const INNER_JOIN: &str = "TRANSFORM ij FROM public.ja JOIN public.jb ON ja.k = jb.k \
    GROUP BY ja.ga WITHOUT SOURCE INDEX \
    SELECT ja.ga AS ga, COUNT(*) AS n, SUM(ja.x) AS sx, SUM(jb.y) AS sy";
const INNER_JOIN_ORACLE: &str = "select ja.ga, count(*), sum(ja.x), sum(jb.y) from public.ja \
    join public.jb on ja.k = jb.k group by ja.ga order by ja.ga nulls last";
const INNER_JOIN_TARGET: &str = "select ga, n, sx, sy from public.ij order by ga nulls last";

async fn assert_joins_match_source(d: &Driver, when: &str) {
    assert_eq!(
        rows(d, LEFT_JOIN_TARGET).await,
        rows(d, LEFT_JOIN_ORACLE).await,
        "the LEFT JOIN target is the join's GROUP BY {when}"
    );
    assert_eq!(
        rows(d, INNER_JOIN_TARGET).await,
        rows(d, INNER_JOIN_ORACLE).await,
        "the inner JOIN target is the join's GROUP BY {when}"
    );
}

/// A `JOIN` aggregate over two tables whose columns are all named like
/// generated aliases: the build, the sides' partials, their cells, the target,
/// the check and a rebuild.
#[tokio::test]
async fn a_join_over_columns_named_like_generated_aliases_builds_applies_checks_and_rebuilds() {
    let ddl = format!(
        "{}; {}; insert into public.ja values {}, {}, {}; insert into public.jb values {}, {}, {}",
        side_ddl("ja", JOIN_BASE_A),
        side_ddl("jb", JOIN_BASE_B),
        row_a(1, 1, 1, 1, 10),
        row_a(2, 1, 1, 2, 5),
        row_a(3, 2, 2, 3, 7),
        row_b(1, 1, 1, 10, 100),
        row_b(2, 1, 1, 20, 4),
        row_b(3, 2, 2, 10, 8),
    );
    let mut columns_a: Vec<(&str, ValueType)> = Vec::new();
    for c in side_columns(JOIN_BASE_A)
        .into_iter()
        .chain(side_columns(JOIN_BASE_B))
    {
        if !columns_a.iter().any(|(n, _)| *n == c) {
            columns_a.push((c, int4()));
        }
    }
    let mut d = Driver::start(
        &ddl,
        &columns_a,
        &[LEFT_JOIN, INNER_JOIN],
        &["public.ja", "public.jb"],
    )
    .await;
    assert_joins_match_source(&d, "after the build").await;

    step(
        &mut d,
        &format!(
            "insert into public.ja values {}, {}; insert into public.jb values {}, {}",
            row_a(4, 3, 3, 3, 1),
            row_a(5, 4, 9, 9, 2),
            row_b(4, 3, 3, 30, 6),
            row_b(5, 3, 3, 31, 7)
        ),
    )
    .await;
    assert_joins_match_source(&d, "after inserts on both sides").await;
    step(
        &mut d,
        "update public.ja set ga = 5, r = r + 100 where id = 1; \
         update public.jb set gb = 11, k = 3 where id = 2; \
         delete from public.jb where id = 3; delete from public.ja where id = 5",
    )
    .await;
    assert_joins_match_source(&d, "after updates and deletes").await;

    // Changes on both sides the drain hasn't applied.
    d.user()
        .await
        .batch_execute(
            "update public.ja set x = x + 1, r = r + 1 where id = 1; \
             update public.jb set y = y + 1, gb = 12 where id = 1; \
             delete from public.ja where id = 2",
        )
        .await
        .expect("unapplied changes");
    for target in ["lj", "ij"] {
        let unapplied = compare_page(
            d.pool(),
            target,
            SelfCheckScope {
                after: None,
                limit: 100,
            },
        )
        .await
        .expect("a comparison");
        assert_eq!(
            (unapplied.divergences, unapplied.uncertain_keys),
            (vec![], vec![]),
            "{target}: the ring's unapplied rows are subtracted"
        );
    }
    let batch = d.seal().await;
    d.drain(batch, "apply").await;
    assert_joins_match_source(&d, "after the last changes").await;

    for target in ["lj", "ij"] {
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
        assert!(
            matches!(page.outcome, SelfCheckOutcome::Converged),
            "{target} self_check: {:?}",
            page.outcome
        );
    }

    // The rebuild in place: each side's sweep job.
    let trellis = Trellis::connect(
        Config::from_dsn(d.db.dsn().to_string()).expect("valid dsn"),
        TrellisOptions::default(),
    )
    .await
    .expect("connect a Trellis");
    for table in ["ja", "jb"] {
        trellis
            .request_backfill(table)
            .await
            .expect("request a rebuild");
        trellis::staging::build::settle_builds(d.pool()).await;
    }
    assert_joins_match_source(&d, "after a rebuild").await;
}

// ------------------------------------------------------------ partitions

/// A partitioned source (`p0`..`p3` for the groups 0-3 .. 12-15) whose other
/// columns are all named like generated aliases.
fn partitioned_ddl() -> String {
    let others: String = ALIASES
        .iter()
        .filter(|a| !["id", "g", "v"].contains(a))
        .map(|a| format!(", \"{a}\" integer"))
        .collect();
    let leaves: String = (0..4)
        .map(|i| {
            format!(
                "create table public.src_p{i} partition of public.src \
                 for values from ({}) to ({});",
                i * 4,
                i * 4 + 4
            )
        })
        .collect();
    format!(
        "create table public.src (id integer not null, g integer not null, \
         v integer not null{others}, primary key (g, id)) partition by range (g); {leaves}"
    )
}

/// Ten rows per group 0-15, every alias column set from `id`.
fn partitioned_seed(from: i32, to: i32) -> String {
    let aliased: String = ALIASES
        .iter()
        .enumerate()
        .map(|(i, _)| format!(", i + {i}"))
        .collect();
    format!(
        "insert into public.src select i, i % 16, i{aliased} \
         from generate_series({from}, {to}) i"
    )
}

/// The aggregate is correct against the source's own `GROUP BY`.
async fn assert_partitioned_matches(d: &Driver, when: &str) {
    assert_eq!(
        rows(
            d,
            "select g, n, sum_r, cnt_p, total from public.agg order by g"
        )
        .await,
        rows(
            d,
            "select g, count(*), sum(r), count(p), sum(v) from public.src group by g order by g"
        )
        .await,
        "the target is the source's GROUP BY {when}"
    );
}

/// A partitioned source with alias-named columns: the seed through capture, a
/// leaf's `TRUNCATE` (its correction reads the node), and a `DROP` of another
/// (its correction reads the ring's images through the ring `s` alias). The
/// truncate's ring marker (`{node, oid}`) is no image of a source row: read as
/// one, it would fill the source's integer `node` column with the leaf's name.
#[tokio::test]
async fn a_partitioned_aggregate_over_columns_named_like_generated_aliases_is_corrected() {
    use trellis::capture::reconcile::reconcile;
    use trellis::config::DEFAULT_SCHEMA;
    use trellis::staging::partition_correction::{self, Options};

    let definition = "TRANSFORM agg FROM public.src GROUP BY g WITHOUT SOURCE INDEX \
        SELECT g AS g, COUNT(*) AS n, SUM(r) AS sum_r, COUNT(p) AS cnt_p, SUM(v) AS total";
    let mut d = Driver::start(
        &partitioned_ddl(),
        &columns(),
        &[definition],
        &["public.src"],
    )
    .await;

    async fn pass(d: &mut Driver) {
        let desired = trellis::defs::tables_to_capture(d.pool())
            .await
            .expect("tables to capture");
        let outcome = reconcile(
            &mut d.ctl,
            DEFAULT_SCHEMA,
            &desired,
            std::time::Instant::now() + Duration::from_secs(5),
        )
        .await
        .expect("pass");
        assert!(outcome.failed.is_empty(), "{:?}", outcome.failed);
    }
    // As the pass, the seal, the engine (before any drain) and a drain.
    async fn correct(d: &mut Driver) {
        pass(d).await;
        let batch = d.seal().await;
        partition_correction::run_pending(d.pool(), &Options::default())
            .await
            .expect("run the pending corrections");
        d.drain(batch, "after").await;
        d.settle().await;
    }

    pass(&mut d).await;
    d.ctl
        .batch_execute(&partitioned_seed(1, 160))
        .await
        .expect("seed");
    d.settle().await;
    assert_partitioned_matches(&d, "after the seed").await;

    // Undrained changes in two leaves, then a TRUNCATE of one and a DROP of the
    // other.
    d.ctl
        .batch_execute(
            "update public.src_p2 set r = r + 1000 where id % 3 = 0; \
             delete from public.src_p1 where id % 5 = 0; \
             truncate public.src_p2",
        )
        .await
        .expect("truncate");
    correct(&mut d).await;
    assert_partitioned_matches(&d, "after a truncate").await;

    d.ctl
        .batch_execute("update public.src_p1 set r = r + 5; drop table public.src_p1")
        .await
        .expect("drop");
    correct(&mut d).await;
    assert_partitioned_matches(&d, "after a drop").await;
}
