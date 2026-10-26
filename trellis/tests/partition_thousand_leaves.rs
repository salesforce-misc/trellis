//! ADR-0017 P10 (issue #1068): a partitioned source at the supported limit
//! of 1,000 leaves.
//!
//! The default path runs one cheap check: a 101-leaf source, past
//! [`SINGLE_TRANSACTION_TABLES`], defines, installs capture node by node,
//! backfills, and stays exact under writes named on the root and on its
//! leaves. The `measure_` tests are `#[ignore]`d measurements that print
//! `MEASURE` lines: run them one at a time, alone on the machine and under
//! a memory cap. Nothing waits for convergence: the
//! pass, the ring and the correction engine are driven by hand.

#[path = "support/drain_driver.rs"]
mod drain_driver;

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use drain_driver::{Driver, connect};
use testkit::TestCluster;
use tokio_postgres::Client;
use trellis::capture::install::{self, Progress, SINGLE_TRANSACTION_TABLES};
use trellis::capture::partitions;
use trellis::capture::reconcile::PassOutcome;
use trellis::capture::sql::CaptureSpec;
use trellis::config::DEFAULT_SCHEMA;
use trellis::defs::{ValueType, install_definition};
use trellis::staging::partition_correction::{self, Options};

const SOURCE: &str = "public.items";
const DEFINITION: &str = "TRANSFORM agg FROM public.items GROUP BY g WITHOUT SOURCE INDEX SELECT SUM(v) AS total, COUNT(*) AS n";

fn columns() -> Vec<(&'static str, ValueType)> {
    vec![
        ("id", ValueType::Numeric),
        // As define types it from the source (`integer`), so the routing
        // and sweep searches' casts are no-ops an index serves.
        ("g", ValueType::Integer(trellis::integer::IntWidth::Int4)),
        ("v", ValueType::Numeric),
    ]
}

struct Fx {
    d: Driver,
}

impl Fx {
    async fn start(ddl: &str) -> Fx {
        let (schema, seed) = drain_driver::split_seed(ddl);
        let d = Driver::start(&schema, &columns(), &[DEFINITION], &[SOURCE]).await;
        let mut fx = Fx { d };
        // The first sight of the source records its tree; the rows follow.
        fx.pass().await;
        if !seed.trim_matches(';').trim().is_empty() {
            fx.exec(&seed).await;
            fx.d.settle().await;
        }
        fx
    }

    /// The capture half of one staging-worker pass.
    async fn pass(&mut self) -> PassOutcome {
        let desired = trellis::defs::tables_to_capture(self.d.pool())
            .await
            .expect("tables to capture");
        let outcome = trellis::capture::reconcile::reconcile(
            &mut self.d.ctl,
            DEFAULT_SCHEMA,
            &desired,
            Instant::now() + Duration::from_secs(30),
        )
        .await
        .expect("pass");
        assert!(outcome.failed.is_empty(), "{:?}", outcome.failed);
        outcome
    }

    async fn wrong_groups(&self) -> Vec<String> {
        self.d
            .rows(
                "with want as (select g, sum(v) as total, count(*) as n \
                               from public.items group by g), \
                      have as (select g, total, n from public.agg) \
                 select coalesce(w.g, h.g)::text || ': ' || coalesce(h.total::text, '-') \
                        || '/' || coalesce(h.n::text, '-') || ' vs ' \
                        || coalesce(w.total::text, '-') || '/' || coalesce(w.n::text, '-') \
                 from want w full join have h on h.g = w.g \
                 where w.total is distinct from h.total or w.n is distinct from h.n \
                 order by 1 limit 20",
            )
            .await
    }

    async fn exec(&self, sql: &str) {
        self.d
            .ctl
            .batch_execute(sql)
            .await
            .unwrap_or_else(|e| panic!("{sql}: {e}"));
    }

    async fn int(&self, sql: &str) -> i64 {
        self.d
            .ctl
            .query_one(sql, &[])
            .await
            .unwrap_or_else(|e| panic!("{sql}: {e}"))
            .get(0)
    }
}

/// `public.items` partitioned by range on `g` into `leaves` leaves of ten
/// groups each, with `per_group` rows in each group.
fn range_tree(leaves: usize, per_group: usize) -> String {
    let mut ddl = String::from(
        "create table public.items (id integer not null, g integer not null, \
         v integer not null, primary key (g, id)) partition by range (g);",
    );
    for i in 0..leaves {
        ddl.push_str(&format!(
            "create table public.items_p{i} partition of public.items \
             for values from ({}) to ({});",
            i * 10,
            i * 10 + 10
        ));
    }
    ddl.push_str(&format!(
        "insert into public.items select i, i % {groups}, i % 7 \
         from generate_series(1, {rows}) i; analyze public.items;",
        groups = leaves * 10,
        rows = leaves * 10 * per_group,
    ));
    ddl
}

/// Every table carrying the instance's capture triggers, with how many.
async fn triggers_per_node(client: &Client) -> Vec<i64> {
    client
        .query(
            "select count(*) from pg_trigger t \
             where t.tgname like 'trellis\\_capture\\_%' and not t.tgisinternal \
             group by t.tgrelid",
            &[],
        )
        .await
        .expect("read triggers")
        .into_iter()
        .map(|row| row.get(0))
        .collect()
}

/// A 101-leaf source is past the single-transaction install, so it defines,
/// installs capture root first and then node by node, backfills, and stays
/// exact under writes named on the root and on a leaf.
#[tokio::test]
async fn a_hundred_and_one_leaf_source_installs_node_by_node_and_stays_exact() {
    const LEAVES: usize = 101;
    const { assert!(LEAVES + 1 > SINGLE_TRANSACTION_TABLES) };
    let mut fx = Fx::start(&range_tree(LEAVES, 2)).await;
    // The root and every leaf carry the five triggers.
    let nodes = triggers_per_node(&fx.d.ctl).await;
    assert_eq!(nodes.len(), LEAVES + 1, "{nodes:?}");
    assert!(nodes.iter().all(|n| *n == 5), "{nodes:?}");
    // The first pass records the tree and queues nothing.
    fx.pass().await;
    assert!(
        partitions::pending_corrections(&fx.d.ctl, SOURCE)
            .await
            .expect("the queue")
            .is_empty()
    );
    assert_eq!(fx.wrong_groups().await, Vec::<String>::new());

    // Through the root, into the first and the last leaf; naming a middle
    // leaf directly; an update that moves a row between leaves; a delete
    // named on the last leaf.
    let user = fx.d.user().await;
    user.batch_execute(
        "insert into public.items values (100001, 3, 5), (100002, 1009, 6); \
         insert into public.items_p50 values (100003, 505, 7); \
         update public.items set g = 1008 where id = 100001; \
         delete from public.items_p100 where g = 1001;",
    )
    .await
    .expect("writes");
    fx.d.settle().await;
    assert_eq!(fx.wrong_groups().await, Vec::<String>::new());
}

/// `--ignored`: how much of the lock table one statement over a partitioned
/// root takes, and the leaf count at which a plain `count(*)` over the root
/// runs the default lock table out. Leaves are added 250 at a time, each
/// batch in its own transaction.
#[tokio::test]
#[ignore = "a measurement: run it alone, with a memory cap"]
async fn measure_the_lock_table_limit_of_one_statement() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = connect(db.dsn()).await;
    let capacity: i64 = client
        .query_one(
            "select current_setting('max_locks_per_transaction')::bigint \
                 * (current_setting('max_connections')::bigint \
                    + current_setting('max_prepared_transactions')::bigint)",
            &[],
        )
        .await
        .expect("capacity")
        .get(0);
    let settings: String = client
        .query_one(
            "select string_agg(name || '=' || setting, ' ' order by name) from pg_settings \
             where name in ('max_locks_per_transaction', 'max_connections', \
                            'max_prepared_transactions', 'server_version')",
            &[],
        )
        .await
        .expect("settings")
        .get(0);
    eprintln!("MEASURE lock_table_slots={capacity} {settings}");
    client
        .batch_execute(
            "create table public.items (id integer not null, g integer not null, \
             v integer not null, primary key (g, id)) partition by range (g)",
        )
        .await
        .expect("root");
    let mut leaves = 0;
    loop {
        let mut batch = String::new();
        for i in leaves..leaves + 250 {
            batch.push_str(&format!(
                "create table public.items_p{i} partition of public.items \
                 for values from ({}) to ({});",
                i * 10,
                i * 10 + 10
            ));
        }
        client.batch_execute(&batch).await.expect("leaves");
        leaves += 250;
        let txn = "begin; select count(*) from public.items; \
             select count(*) from pg_locks where pid = pg_backend_pid() \
                 and locktype = 'relation';";
        match client.simple_query(txn).await {
            Ok(messages) => {
                let locks = messages
                    .iter()
                    .filter_map(|m| match m {
                        tokio_postgres::SimpleQueryMessage::Row(row) => {
                            row.get(0).map(str::to_string)
                        }
                        _ => None,
                    })
                    .nth(1)
                    .unwrap_or_default();
                client.batch_execute("commit").await.expect("commit");
                eprintln!("MEASURE leaves={leaves} statement_relation_locks={locks}");
            }
            Err(err) => {
                let _ = client.batch_execute("rollback").await;
                eprintln!(
                    "MEASURE leaves={leaves} refused: {} ({:?})",
                    err.as_db_error()
                        .map(|e| e.message().to_string())
                        .unwrap_or_else(|| err.to_string()),
                    err.code()
                );
                break;
            }
        }
        assert!(leaves < 20_000, "no lock table runs out by 20,000 leaves");
    }
}

/// The source of the 1,000-leaf measurement: `public.items` partitioned by
/// range on `g`, leaf `items_p0` holding 1,000,000 rows over the 50,000
/// groups `[0, 50000)`, and 999 more leaves of 1,000 groups each, each
/// holding 100 rows in one group (99,900 rows).
fn thousand_leaves() -> String {
    let mut ddl = String::from(
        "create table public.items (id integer not null, g integer not null, \
         v integer not null, primary key (g, id)) partition by range (g); \
         create table public.items_p0 partition of public.items \
             for values from (0) to (50000);",
    );
    for i in 1..1000 {
        ddl.push_str(&format!(
            "create table public.items_p{i} partition of public.items \
             for values from ({}) to ({});",
            50_000 + (i - 1) * 1000,
            50_000 + i * 1000
        ));
    }
    ddl.push_str(
        "insert into public.items select i, i % 50000, i % 97 \
             from generate_series(1, 1000000) i; \
         insert into public.items select 2000000 + i, 50000 + (i - 1) / 100 * 1000, 1 \
             from generate_series(1, 99900) i; \
         analyze public.items;",
    );
    ddl
}

fn ms(d: Duration) -> f64 {
    d.as_secs_f64() * 1e3
}

/// `--ignored`: the supported limit, measured on one 1,000-leaf source
/// (ADR-0017's figures): define, build and backfill; each pass's fingerprints
/// and sweep; the correction
/// of a dropped 1,000,000-row leaf; and a tree grown to 1,001 leaves.
#[tokio::test]
#[ignore = "a measurement: run it alone, with a memory cap"]
async fn measure_a_thousand_leaf_source() {
    let started = Instant::now();
    let mut fx = Fx::start(&thousand_leaves()).await;
    let phases: Vec<String> =
        fx.d.phases
            .iter()
            .map(|(name, d)| format!("{name}_s={:.2}", d.as_secs_f64()))
            .collect();
    eprintln!(
        "MEASURE start total_s={:.1} {} target_rows={} nodes_captured={}",
        started.elapsed().as_secs_f64(),
        phases.join(" "),
        fx.int("select count(*) from public.agg").await,
        triggers_per_node(&fx.d.ctl).await.len(),
    );
    assert_eq!(fx.wrong_groups().await, Vec::<String>::new());

    // The lock table every statement shares.
    let slots = fx
        .int(
            "select current_setting('max_locks_per_transaction')::bigint \
                 * (current_setting('max_connections')::bigint \
                    + current_setting('max_prepared_transactions')::bigint)",
        )
        .await;
    eprintln!("MEASURE lock_table_slots={slots}");

    // Each pass: the first records the tree; then steady passes with nothing
    // changed, and a deep check after a simulated restart.
    let t = Instant::now();
    fx.pass().await;
    eprintln!("MEASURE first_pass_ms={:.1}", ms(t.elapsed()));
    for i in 0..5 {
        let t = Instant::now();
        let outcome = fx.pass().await;
        let pass = t.elapsed();
        let sweep = outcome
            .partition_sweeps
            .iter()
            .find(|(t, _)| t == SOURCE)
            .map(|(_, s)| s.clone())
            .expect("the source was swept");
        eprintln!(
            "MEASURE steady_pass={i} pass_ms={:.1} deep_checks={} sweep_statements={} \
             sweep_read_ms={:.2} sweep_search_ms={:.2}",
            ms(pass),
            outcome.partition_deep_checks.len(),
            sweep.statements,
            ms(sweep.read),
            ms(sweep.search),
        );
    }
    let mut fingerprints = Vec::new();
    for _ in 0..20 {
        let t = Instant::now();
        partitions::read_fingerprints(&fx.d.ctl, SOURCE)
            .await
            .expect("fingerprints")
            .expect("the source");
        fingerprints.push(ms(t.elapsed()));
    }
    fingerprints.sort_by(f64::total_cmp);
    eprintln!(
        "MEASURE fingerprints_ms min={:.2} median={:.2} max={:.2}",
        fingerprints[0], fingerprints[10], fingerprints[19]
    );
    // The pass's other per-table catalog reads, each timed alone.
    let catalog = trellis::capture::columns::load_catalog(&fx.d.ctl)
        .await
        .expect("catalog");
    for _ in 0..3 {
        let t = Instant::now();
        trellis::capture::reconcile::installed_tables(&fx.d.ctl, DEFAULT_SCHEMA)
            .await
            .expect("installed tables");
        let installed_tables = t.elapsed();
        let t = Instant::now();
        install::installed(&fx.d.ctl, DEFAULT_SCHEMA, SOURCE)
            .await
            .expect("installed");
        let installed = t.elapsed();
        let t = Instant::now();
        trellis::capture::columns::capture_spec(&fx.d.ctl, &catalog, SOURCE)
            .await
            .expect("capture spec");
        eprintln!(
            "MEASURE pass_parts installed_tables_ms={:.1} installed_ms={:.1} capture_spec_ms={:.1}",
            ms(installed_tables),
            ms(installed),
            ms(t.elapsed()),
        );
    }
    for i in 0..3 {
        partitions::forget_checked(fx.d.db.name(), DEFAULT_SCHEMA);
        let t = Instant::now();
        let outcome = fx.pass().await;
        eprintln!(
            "MEASURE restart_pass={i} pass_ms={:.1} deep_checks={}",
            ms(t.elapsed()),
            outcome.partition_deep_checks.len()
        );
    }

    // A retention `DROP` of the 1,000,000-row leaf, corrected.
    async fn wal(fx: &Fx) -> i64 {
        fx.int("select (pg_current_wal_lsn() - '0/0'::pg_lsn)::bigint")
            .await
    }
    fx.exec("drop table public.items_p0").await;
    let t = Instant::now();
    let outcome = fx.pass().await;
    let detect = t.elapsed();
    let queued = partitions::pending_corrections(&fx.d.ctl, SOURCE)
        .await
        .expect("the queue");
    assert_eq!(queued.len(), 1, "{queued:?}");
    fx.d.settle().await;
    let before = wal(&fx).await;
    let t = Instant::now();
    let corrected = partition_correction::run_pending(fx.d.pool(), &Options::default())
        .await
        .expect("run the correction");
    let engine = t.elapsed();
    let middle = wal(&fx).await;
    let t = Instant::now();
    fx.d.settle().await;
    let settle = t.elapsed();
    let after = wal(&fx).await;
    eprintln!(
        "MEASURE correction leaf_rows=1000000 detect_pass_ms={:.1} deep_checks={} keys={} \
         pages={} engine_s={:.2} engine_wal_mb={:.0} settle_s={:.2} settle_wal_mb={:.0}",
        ms(detect),
        outcome.partition_deep_checks.len(),
        corrected.keys,
        corrected.pages,
        engine.as_secs_f64(),
        (middle - before) as f64 / 1e6,
        settle.as_secs_f64(),
        (after - middle) as f64 / 1e6,
    );
    assert_eq!(fx.wrong_groups().await, Vec::<String>::new());

    // Grown to 1,001 leaves: what the pass does with the defined reader,
    // and what define says to a new one.
    fx.exec(
        "create table public.items_p0 partition of public.items for values from (0) to (50000); \
         create table public.items_n1 partition of public.items for values from (-1000) to (0);",
    )
    .await;
    let leaves = fx
        .int("select count(*) from pg_partition_tree('public.items') where isleaf")
        .await;
    let desired = trellis::defs::tables_to_capture(fx.d.pool())
        .await
        .expect("tables to capture");
    let outcome = trellis::capture::reconcile::reconcile(
        &mut fx.d.ctl,
        DEFAULT_SCHEMA,
        &desired,
        Instant::now() + Duration::from_secs(30),
    )
    .await
    .expect("pass");
    let status: String =
        fx.d.ctl
            .query_one(
                "select status from transform_definitions \
                 where split_part(target_table, '.', 2) = 'agg'",
                &[],
            )
            .await
            .expect("status")
            .get(0);
    let failures =
        fx.d.rows("select kind || ': ' || error from capture_failures")
            .await;
    eprintln!(
        "MEASURE grown leaves={leaves} pass_failed={} status={status} capture_failures={failures:?}",
        outcome.failed.len()
    );
    let columns: HashMap<String, ValueType> = columns()
        .into_iter()
        .map(|(name, ty)| (name.to_string(), ty))
        .collect();
    let refused = install_definition(
        fx.d.pool(),
        "TRANSFORM agg2 FROM public.items GROUP BY g WITHOUT SOURCE INDEX SELECT COUNT(*) AS n",
        &columns,
        "public",
    )
    .await
    .expect_err("a 1,001-leaf source is refused at define");
    eprintln!("MEASURE define_at_1001: {refused}");
    assert!(refused.to_string().contains("1000 leaves"), "{refused}");
}

/// A capture spec for `public.r (id, v)`.
fn spec(partitioned: bool) -> CaptureSpec {
    CaptureSpec::new("public.r", vec!["id".to_string()], ["v".to_string()])
        .expect("valid spec")
        .with_partitioned(partitioned)
}

/// `public.r (id int primary key, v text)`: plain when `leaves` is 0,
/// otherwise hash partitioned on `id` into `leaves` leaves.
fn hash_tree(leaves: usize) -> String {
    if leaves == 0 {
        return "create table public.r (id int primary key, v text)".to_string();
    }
    let mut ddl =
        String::from("create table public.r (id int primary key, v text) partition by hash (id);");
    for i in 0..leaves {
        ddl.push_str(&format!(
            "create table public.r_{i} partition of public.r \
             for values with (modulus {leaves}, remainder {i});"
        ));
    }
    ddl
}

fn landed<T>(progress: Progress<T>) -> T {
    match progress {
        Progress::Done(value) => value,
        Progress::Waiting(wait) => panic!("an operation without a deadline landed: {wait}"),
    }
}

/// `--ignored`: installing capture on a 1,000-leaf tree with a writer
/// inserting through the root the whole time, node by node (as Trellis
/// installs a tree of more than [`SINGLE_TRANSACTION_TABLES`] tables) and in
/// one transaction, alternating, three rounds each. Prints the install's
/// time and the writer's worst commit and how many commits took over 50 ms.
#[tokio::test]
#[ignore = "a measurement: run it alone, with a memory cap"]
async fn measure_a_thousand_leaf_install_node_by_node_and_in_one_transaction() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect(db.dsn()).await;
    client.batch_execute(&hash_tree(1000)).await.expect("tree");
    let spec = spec(true);
    let mut next_id = 0i32;
    for round in 0..3 {
        for one_transaction in [false, true] {
            let stop = Arc::new(AtomicBool::new(false));
            let flag = stop.clone();
            let dsn = db.dsn().to_string();
            let first = next_id;
            let writer = tokio::spawn(async move {
                let writer = connect(&dsn).await;
                let insert = writer
                    .prepare("insert into public.r values ($1, 'w')")
                    .await
                    .expect("prepare");
                let mut commits = Vec::new();
                let mut id = first;
                while !flag.load(Ordering::Relaxed) {
                    id += 1;
                    let started = Instant::now();
                    writer.execute(&insert, &[&id]).await.expect("write");
                    commits.push(started.elapsed());
                }
                (commits, id)
            });
            tokio::time::sleep(Duration::from_millis(300)).await;
            let started = Instant::now();
            if one_transaction {
                landed(
                    install::install_in_one_transaction(&mut client, DEFAULT_SCHEMA, &spec, None)
                        .await
                        .expect("install"),
                );
            } else {
                assert!(landed(
                    install::install(&mut client, DEFAULT_SCHEMA, &spec, None)
                        .await
                        .expect("install")
                ));
            }
            let took = started.elapsed();
            tokio::time::sleep(Duration::from_millis(300)).await;
            stop.store(true, Ordering::Relaxed);
            let (mut commits, last) = writer.await.expect("writer");
            next_id = last;
            commits.sort();
            let worst = commits.last().copied().unwrap_or_default();
            let over = commits
                .iter()
                .filter(|c| **c > Duration::from_millis(50))
                .count();
            let p99 = commits[commits.len() * 99 / 100];
            eprintln!(
                "MEASURE install round={round} one_transaction={one_transaction} \
                 install_ms={:.0} writes={} worst_commit_ms={:.1} p99_ms={:.2} over_50ms={over}",
                ms(took),
                commits.len(),
                ms(worst),
                ms(p99),
            );
            let nodes = triggers_per_node(&client).await;
            assert_eq!(nodes.len(), 1001);
            landed(
                install::uninstall(&mut client, DEFAULT_SCHEMA, "public.r", None)
                    .await
                    .expect("uninstall"),
            );
        }
    }
}

/// `--ignored`: the mean single-row write on a plain table and on a
/// 1,000-leaf tree, each without and with capture: an `INSERT` and an
/// `UPDATE` by key, named on the root, and on the 1,000-leaf tree also named
/// on the leaf that holds the row. 3,000 of each per round, prepared and
/// autocommit, three rounds, the cases interleaved within a round.
#[tokio::test]
#[ignore = "a measurement: run it alone, with a memory cap"]
async fn measure_per_write_capture_cost_at_a_thousand_leaves() {
    const WRITES: i32 = 3000;
    let cluster = TestCluster::start();
    let mut cases = Vec::new();
    for (leaves, captured) in [(0usize, false), (0, true), (1000, false), (1000, true)] {
        let db = cluster.create_isolated_database().await;
        let mut client = connect(db.dsn()).await;
        client
            .batch_execute(&hash_tree(leaves))
            .await
            .expect("table");
        if captured {
            landed(
                install::install(&mut client, DEFAULT_SCHEMA, &spec(leaves > 0), None)
                    .await
                    .expect("install"),
            );
        }
        cases.push((leaves, captured, db, client));
    }
    let mut next = 0i32;
    for round in 0..3 {
        let mut line = Vec::new();
        for (leaves, captured, _db, client) in &cases {
            let mut targets = vec!["root"];
            if *leaves > 0 {
                targets.push("leaf");
            }
            for target in targets {
                let first = next;
                next += WRITES + 100;
                // Warm the plans.
                for id in first..first + 50 {
                    client
                        .execute("insert into public.r values ($1, 'x')", &[&id])
                        .await
                        .expect("warm");
                }
                let mut elapsed = Duration::ZERO;
                if target == "root" {
                    let insert = client
                        .prepare("insert into public.r values ($1, 'x')")
                        .await
                        .expect("prepare");
                    let update = client
                        .prepare("update public.r set v = 'y' where id = $1")
                        .await
                        .expect("prepare");
                    let started = Instant::now();
                    for id in first + 100..first + 100 + WRITES {
                        client.execute(&insert, &[&id]).await.expect("insert");
                        client.execute(&update, &[&id]).await.expect("update");
                    }
                    elapsed += started.elapsed();
                } else {
                    // Every write names leaf r_0: the ids hash to it.
                    let mut ids: Vec<i32> = Vec::new();
                    let mut lo = first + 100;
                    while ids.len() < WRITES as usize {
                        let window: Vec<i32> = client
                            .query(
                                "select i from generate_series($1::int, $1::int + 199999) i \
                                 where satisfies_hash_partition('public.r'::regclass, 1000, 0, i)",
                                &[&lo],
                            )
                            .await
                            .expect("ids")
                            .iter()
                            .map(|r| r.get(0))
                            .collect();
                        ids.extend(window);
                        lo += 200_000;
                    }
                    ids.truncate(WRITES as usize);
                    next = next.max(ids.last().copied().unwrap_or(next) + 1);
                    let insert = client
                        .prepare("insert into public.r_0 values ($1, 'x')")
                        .await
                        .expect("prepare");
                    let update = client
                        .prepare("update public.r_0 set v = 'y' where id = $1")
                        .await
                        .expect("prepare");
                    let started = Instant::now();
                    for id in &ids {
                        client.execute(&insert, &[id]).await.expect("insert");
                        client.execute(&update, &[id]).await.expect("update");
                    }
                    elapsed += started.elapsed();
                }
                let micros = elapsed.as_secs_f64() * 1e6 / f64::from(2 * WRITES);
                line.push(format!(
                    "leaves={leaves} capture={captured} named={target}: {micros:.1}us"
                ));
            }
        }
        eprintln!("MEASURE per_write round={round} {}", line.join(" | "));
    }
}
