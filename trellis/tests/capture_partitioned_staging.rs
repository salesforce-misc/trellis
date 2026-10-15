//! ADR-0017 P2b (issue #1060): capture of a partitioned source stages every
//! write exactly once under the root's identity, and stages nothing for a
//! table that has left the tree.
//!
//! The DML matrix writes to the root, a middle and a leaf with `INSERT`,
//! `UPDATE`, `DELETE`, `COPY`, `INSERT … ON CONFLICT` and `TRUNCATE`. After
//! every step the ring rows the step staged must be the expected ones, and
//! replaying the ring (last row per key wins, a delete removes the key) must
//! equal the source table: the oracle diff. The detach tests drive two
//! sessions with explicit transaction control, never a sleep.

use std::time::Instant;

use testkit::TestCluster;
use tokio_postgres::{Client, NoTls};
use trellis::capture::install::{self, Progress};
use trellis::capture::sql::CaptureSpec;
use trellis::config::DEFAULT_SCHEMA;

async fn connect(dsn: &str) -> Client {
    let (client, connection) = tokio_postgres::connect(dsn, NoTls).await.expect("connect");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
        .batch_execute(&format!(
            "set search_path to {DEFAULT_SCHEMA}, public; set lock_timeout = '10s'"
        ))
        .await
        .expect("set search_path");
    client
}

/// `public.r` partitioned by list on `region`: `m1` and `m2`, each hash
/// partitioned on `id` into two leaves.
const TREE: &str = "create table public.r (id int, region int, v text, \
        primary key (id, region)) partition by list (region); \
     create table public.m1 partition of public.r for values in (1) partition by hash (id); \
     create table public.m2 partition of public.r for values in (2) partition by hash (id); \
     create table public.m1_0 partition of public.m1 for values with (modulus 2, remainder 0); \
     create table public.m1_1 partition of public.m1 for values with (modulus 2, remainder 1); \
     create table public.m2_0 partition of public.m2 for values with (modulus 2, remainder 0); \
     create table public.m2_1 partition of public.m2 for values with (modulus 2, remainder 1)";

const RING: &str = "(select * from seg_0 union all select * from seg_1 \
                    union all select * from seg_2 union all select * from seg_3)";

fn spec() -> CaptureSpec {
    CaptureSpec::new(
        "public.r",
        vec!["id".to_string(), "region".to_string()],
        ["v".to_string()],
        Vec::new(),
    )
    .expect("valid spec")
    .with_partitioned(true)
}

async fn install_capture(client: &mut Client) {
    match install::install(client, DEFAULT_SCHEMA, &spec(), None)
        .await
        .expect("install")
    {
        Progress::Done(true) => {}
        other => panic!("install did not land: {other:?}"),
    }
}

async fn tree(db: &testkit::TestDatabase) -> Client {
    let mut client = connect(db.dsn()).await;
    client.batch_execute(TREE).await.expect("tree");
    install_capture(&mut client).await;
    client
}

/// Ring rows by source table and op, as `"table:op=n"`.
async fn ring_counts(client: &Client) -> Vec<String> {
    client
        .query(
            &format!(
                "select src_table || ':' || op || '=' || count(*) from {RING} s \
                 group by src_table, op order by 1"
            ),
            &[],
        )
        .await
        .expect("ring counts")
        .into_iter()
        .map(|row| row.get(0))
        .collect()
}

/// Replays the ring (the last row of each key wins, a delete removes the
/// key) and counts the rows that differ from the source table, both ways.
async fn oracle_diff(client: &Client) -> i64 {
    client
        .query_one(
            &format!(
                "with replay as ( \
                     select distinct on (key) key, op, new_image from {RING} s \
                     order by key, lsn desc, change_id desc), \
                 want as (select (new_image ->> 'id')::int id, (new_image ->> 'region')::int region, \
                                 new_image ->> 'v' v from replay where op <> 'delete'), \
                 have as (select id, region, v from public.r) \
                 select (select count(*) from (select * from want except select * from have) a) \
                      + (select count(*) from (select * from have except select * from want) b)"
            ),
            &[],
        )
        .await
        .expect("oracle")
        .get(0)
}

/// Runs `sql` and asserts the ring rows it staged, as `"table:op=n"`
/// entries, and a zero oracle diff. The ring is not cleared between steps,
/// so each step's replay builds on the previous steps'.
async fn step(client: &Client, sql: &str, expect: &[&str]) {
    let before = ring_counts(client).await;
    client.batch_execute(sql).await.expect(sql);
    let after = ring_counts(client).await;
    assert_eq!(oracle_diff(client).await, 0, "oracle diff after: {sql}");
    let mut expect: Vec<&str> = expect.to_vec();
    expect.sort_unstable();
    assert_eq!(
        subtract(&after, &before),
        expect,
        "ring rows staged by: {sql}"
    );
}

/// `after - before` over `"label=n"` entries.
fn subtract(after: &[String], before: &[String]) -> Vec<String> {
    let parse = |e: &String| {
        let (label, n) = e.rsplit_once('=').expect("label=n");
        (label.to_string(), n.parse::<i64>().expect("count"))
    };
    let before: std::collections::BTreeMap<String, i64> = before.iter().map(parse).collect();
    after
        .iter()
        .map(parse)
        .filter_map(|(label, n)| {
            let delta = n - before.get(&label).copied().unwrap_or(0);
            (delta != 0).then(|| format!("{label}={delta}"))
        })
        .collect()
}

/// The leaf a row with this key routes to, found by inserting it through
/// the root in a transaction that rolls back (so nothing is staged).
async fn leaf_of(client: &mut Client, id: i32, region: i32) -> String {
    let txn = client.transaction().await.expect("begin");
    txn.execute(
        "insert into public.r values ($1, $2, 'probe')",
        &[&id, &region],
    )
    .await
    .expect("probe insert");
    let leaf: String = txn
        .query_one(
            "select tableoid::regclass::text from public.r where id = $1 and region = $2",
            &[&id, &region],
        )
        .await
        .expect("probe route")
        .get(0);
    txn.rollback().await.expect("rollback");
    leaf
}

/// Writes `rows` (`id,region,v` lines) to a file the server reads.
fn csv(name: &str, rows: &str) -> String {
    let path = format!("{}/{name}.csv", env!("CARGO_TARGET_TMPDIR"));
    std::fs::write(&path, rows).expect("write the copy file");
    path
}

const INS: &str = "public.r:insert";
const UPD: &str = "public.r:update";
const DEL: &str = "public.r:delete";

#[tokio::test]
async fn writes_aimed_at_the_root_a_middle_or_a_leaf_each_stage_once() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = tree(&db).await;
    let leaf = leaf_of(&mut client, 3, 2).await;

    // INSERT.
    step(
        &client,
        "insert into public.r values (1, 1, 'a'), (2, 1, 'b'), (3, 2, 'c'), (4, 2, 'd')",
        &[&format!("{INS}=4")],
    )
    .await;
    step(
        &client,
        "insert into public.m1 values (5, 1, 'e'), (6, 1, 'f')",
        &[&format!("{INS}=2")],
    )
    .await;
    step(
        &client,
        &format!("delete from public.r where id = 3; insert into {leaf} values (3, 2, 'c')"),
        &[&format!("{INS}=1"), &format!("{DEL}=1")].map(String::as_str),
    )
    .await;

    // UPDATE.
    step(
        &client,
        "update public.r set v = v || 'r' where region = 1",
        &[&format!("{UPD}=4")],
    )
    .await;
    step(
        &client,
        "update public.m2 set v = v || 'm'",
        &[&format!("{UPD}=2")],
    )
    .await;
    step(
        &client,
        &format!("update {leaf} set v = v || 'l' where id = 3"),
        &[&format!("{UPD}=1")],
    )
    .await;
    // An update that changes nothing stages nothing.
    step(&client, "update public.r set v = v where id = 1", &[]).await;

    // DELETE.
    step(
        &client,
        "delete from public.r where id = 1",
        &[&format!("{DEL}=1")],
    )
    .await;
    step(
        &client,
        "delete from public.m1 where id = 5",
        &[&format!("{DEL}=1")],
    )
    .await;
    step(
        &client,
        &format!("delete from {leaf} where id = 3"),
        &[&format!("{DEL}=1")],
    )
    .await;

    // COPY.
    let file = csv("p2b_copy_root", "10,1,x\n11,2,y\n12,1,z\n");
    step(
        &client,
        &format!("copy public.r from '{file}' (format csv)"),
        &[&format!("{INS}=3")],
    )
    .await;
    let file = csv("p2b_copy_mid", "13,1,x\n14,1,y\n");
    step(
        &client,
        &format!("copy public.m1 from '{file}' (format csv)"),
        &[&format!("{INS}=2")],
    )
    .await;
    let leaf_15 = leaf_of(&mut client, 15, 1).await;
    let file = csv("p2b_copy_leaf", "15,1,x\n");
    step(
        &client,
        &format!("copy {leaf_15} from '{file}' (format csv)"),
        &[&format!("{INS}=1")],
    )
    .await;

    // INSERT … ON CONFLICT: a new row inserts, a conflicting row updates, a
    // conflicting row that does nothing stages nothing.
    step(
        &client,
        "insert into public.r values (10, 1, 'new'), (20, 1, 'fresh') \
         on conflict (id, region) do update set v = excluded.v",
        &[&format!("{INS}=1"), &format!("{UPD}=1")],
    )
    .await;
    step(
        &client,
        "insert into public.m1 values (10, 1, 'again'), (21, 1, 'fresh') \
         on conflict (id, region) do update set v = excluded.v",
        &[&format!("{INS}=1"), &format!("{UPD}=1")],
    )
    .await;
    step(
        &client,
        &format!(
            "insert into {leaf_15} values (15, 1, 'leaf') \
             on conflict (id, region) do update set v = excluded.v"
        ),
        &[&format!("{UPD}=1")],
    )
    .await;
    step(
        &client,
        "insert into public.r values (10, 1, 'ignored') on conflict do nothing",
        &[],
    )
    .await;
}

#[tokio::test]
async fn an_update_that_moves_a_row_between_partitions_stages_a_delete_and_an_insert() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = tree(&db).await;
    step(
        &client,
        "insert into public.r values (1, 1, 'a'), (2, 1, 'b'), (3, 1, 'c'), (4, 1, 'd')",
        &[&format!("{INS}=4")],
    )
    .await;
    // Region 1 to region 2, through the root: another middle.
    step(
        &client,
        "update public.r set region = 2 where id = 1",
        &[&format!("{DEL}=1"), &format!("{INS}=1")],
    )
    .await;
    // Through a middle: across its hash leaves.
    step(
        &client,
        "update public.m1 set id = id + 100 where id in (2, 3)",
        &[&format!("{DEL}=2"), &format!("{INS}=2")],
    )
    .await;
}

/// A `TRUNCATE` cascades to every partition under the table it names and
/// fires each one's statement trigger, so it stages one sentinel per table
/// of the subtree, all under the root's identity. The seal reads
/// `bool_or(op = 'truncate')`, so the repeats are harmless.
#[tokio::test]
async fn truncate_stages_one_sentinel_per_table_of_the_subtree_under_the_roots_identity() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = tree(&db).await;
    for (sql, n) in [
        ("truncate public.m2_0", 1),
        ("truncate public.m1", 3),
        ("truncate public.r", 7),
    ] {
        let before = ring_counts(&client).await;
        client.batch_execute(sql).await.expect(sql);
        let delta = subtract(&ring_counts(&client).await, &before);
        assert_eq!(delta, [format!("public.r:truncate={n}")], "{sql}");
    }
}

/// Every ring row staged so far, whatever its `src_table`.
async fn staged_total(client: &Client) -> i64 {
    client
        .query_one(&format!("select count(*) from {RING} s"), &[])
        .await
        .expect("count the ring")
        .get(0)
}

#[tokio::test]
async fn a_write_to_a_detached_table_that_still_carries_triggers_stages_nothing() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = tree(&db).await;
    step(
        &client,
        "insert into public.r values (1, 1, 'a'), (2, 1, 'b'), (3, 2, 'c'), (4, 2, 'd')",
        &[&format!("{INS}=4")],
    )
    .await;
    client
        .batch_execute("alter table public.r detach partition public.m2")
        .await
        .expect("detach a middle");
    // Its triggers stay (the reconcile pass drops them), and so does every
    // trigger of the leaves under it.
    let carrying: i64 = client
        .query_one(
            "select count(*) from pg_trigger t where t.tgrelid = 'public.m2_0'::regclass \
               and t.tgname like 'trellis\\_capture\\_%'",
            &[],
        )
        .await
        .expect("triggers")
        .get(0);
    assert_eq!(carrying, 5);

    let before = staged_total(&client).await;
    client
        .batch_execute("insert into public.m2 values (5, 2, 'x'); update public.m2 set v = 'y'")
        .await
        .expect("writes named on the detached middle");
    let leaf: String = client
        .query_one(
            "select tableoid::regclass::text from public.m2 where id = 5",
            &[],
        )
        .await
        .expect("route")
        .get(0);
    client
        .batch_execute(&format!(
            "update {leaf} set v = 'z'; delete from {leaf} where id = 5; \
             truncate public.m2_1; truncate public.m2"
        ))
        .await
        .expect("writes named on a detached leaf");
    assert_eq!(staged_total(&client).await, before, "nothing staged");
    // The root still captures. (The detached rows' own ring rows stay: the
    // correction for the rows that left the root is P3/P4's.)
    let counts = ring_counts(&client).await;
    client
        .batch_execute(
            "insert into public.r values (7, 1, 'g'); delete from public.r where region = 1",
        )
        .await
        .expect("writes to the root");
    assert_eq!(
        subtract(&ring_counts(&client).await, &counts),
        [format!("{DEL}=3"), format!("{INS}=1")]
    );
}

/// A `REPEATABLE READ` writer whose snapshot predates the `DETACH` still
/// sees the table in `pg_inherits`; the capture function must not.
#[tokio::test]
async fn a_repeatable_read_writer_with_a_snapshot_older_than_the_detach_stages_nothing() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = tree(&db).await;
    let writer = connect(db.dsn()).await;

    writer
        .batch_execute("begin isolation level repeatable read; select 1")
        .await
        .expect("take the snapshot");
    client
        .batch_execute("alter table public.r detach partition public.m2")
        .await
        .expect("detach, committed after the writer's snapshot");

    // The premise: the writer's snapshot still has the table attached.
    let attached_m2: i64 = writer
        .query_one(
            "select count(*) from pg_inherits where inhrelid = 'public.m2'::regclass",
            &[],
        )
        .await
        .expect("pg_inherits")
        .get(0);
    assert_eq!(attached_m2, 1, "an old snapshot still sees m2 attached");

    let before = staged_total(&client).await;
    writer
        .batch_execute(
            "insert into public.m2 values (2, 2, 'x'), (3, 2, 'y'); \
             update public.m2 set v = 'z'; \
             delete from public.m2 where id = 2",
        )
        .await
        .expect("writes through the detached middle");
    writer.batch_execute("commit").await.expect("commit");
    assert_eq!(staged_total(&client).await, before, "nothing staged");
}

/// An interrupted `DETACH … CONCURRENTLY` leaves the leaf detach-pending: it
/// still carries the triggers, `pg_partition_root` names the leaf itself,
/// and the reconcile pass does not list it for cleanup until the detach
/// completes (a writer with an old snapshot can still write through the
/// parent into it).
#[tokio::test]
async fn a_detach_pending_leaf_stages_nothing_and_is_not_cleaned_up_until_the_detach_completes() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = tree(&db).await;
    step(
        &client,
        "insert into public.r values (1, 1, 'a'), (2, 1, 'b'), (3, 1, 'c'), (4, 1, 'd')",
        &[&format!("{INS}=4")],
    )
    .await;
    let leaf: String = client
        .query_one(
            "select tableoid::regclass::text from public.r where id = 1",
            &[],
        )
        .await
        .expect("route")
        .get(0);

    // A transaction holding the leaf open stops the second phase of the
    // detach, and the statement timeout cancels it there: the first phase
    // has committed, which leaves the leaf pending.
    let holder = connect(db.dsn()).await;
    holder
        .batch_execute(&format!("begin; lock table {leaf} in access share mode"))
        .await
        .expect("hold the leaf");
    let detacher = connect(db.dsn()).await;
    detacher
        .batch_execute("set statement_timeout = '1500ms'")
        .await
        .expect("timeout");
    let interrupted = detacher
        .batch_execute(&format!(
            "alter table public.m1 detach partition {leaf} concurrently"
        ))
        .await;
    assert!(interrupted.is_err(), "the second phase waits on the holder");
    let pending: bool = client
        .query_one(
            &format!(
                "select inhdetachpending from pg_inherits where inhrelid = '{leaf}'::regclass"
            ),
            &[],
        )
        .await
        .expect("pending")
        .get(0);
    assert!(pending, "{leaf} is detach-pending");

    // A write named on the pending leaf stages nothing; the root's other
    // leaves still capture.
    let before = staged_total(&client).await;
    client
        .batch_execute(&format!("update {leaf} set v = 'z'"))
        .await
        .expect("write to the pending leaf");
    assert_eq!(staged_total(&client).await, before);
    client
        .batch_execute("insert into public.m2 values (9, 2, 'n')")
        .await
        .expect("write to a sibling");
    assert_eq!(staged_total(&client).await, before + 1);

    // The cleanup waits: the pending leaf is not an installed table of its
    // own, and its triggers stay.
    let tables = trellis::capture::reconcile::installed_tables(&client, DEFAULT_SCHEMA)
        .await
        .expect("installed tables");
    assert_eq!(tables.into_iter().collect::<Vec<_>>(), ["public.r"]);

    // Once the detach completes the leaf is listed, for the pass to strip.
    holder.batch_execute("commit").await.expect("release");
    client
        .batch_execute(&format!(
            "alter table public.m1 detach partition {leaf} finalize"
        ))
        .await
        .expect("finalize");
    let tables = trellis::capture::reconcile::installed_tables(&client, DEFAULT_SCHEMA)
        .await
        .expect("installed tables");
    assert_eq!(tables.len(), 2, "{tables:?}");
}

/// Per-write capture cost on a partitioned source against a plain table at
/// 10, 100 and 1,000 leaves: `cargo test -p trellis --test
/// capture_partitioned_staging measure_ -- --ignored --nocapture`.
#[tokio::test]
#[ignore = "a measurement; prints the mean single-row write time"]
async fn measure_per_write_capture_cost_against_a_plain_table() {
    const WRITES: usize = 3000;
    let cluster = TestCluster::start();
    let mut results = Vec::new();
    for (leaves, captured) in [
        (0usize, false),
        (0, true),
        (10, false),
        (10, true),
        (100, false),
        (100, true),
        (1000, false),
        (1000, true),
    ] {
        let db = cluster.create_isolated_database().await;
        let mut client = connect(db.dsn()).await;
        let ddl = if leaves == 0 {
            "create table public.r (id int primary key, v text)".to_string()
        } else {
            let mut ddl = String::from(
                "create table public.r (id int primary key, v text) partition by hash (id);",
            );
            for i in 0..leaves {
                ddl.push_str(&format!(
                    "create table public.r_{i} partition of public.r \
                     for values with (modulus {leaves}, remainder {i});"
                ));
            }
            ddl
        };
        client.batch_execute(&ddl).await.expect("table");
        let spec = CaptureSpec::new(
            "public.r",
            vec!["id".to_string()],
            ["v".to_string()],
            Vec::new(),
        )
        .expect("spec")
        .with_partitioned(leaves > 0);
        if captured {
            match install::install(&mut client, DEFAULT_SCHEMA, &spec, None)
                .await
                .expect("install")
            {
                Progress::Done(_) => {}
                other => panic!("{other:?}"),
            }
        }
        let insert = client
            .prepare("insert into public.r values ($1, 'x')")
            .await
            .expect("prepare");
        let update = client
            .prepare("update public.r set v = 'y' where id = $1")
            .await
            .expect("prepare");
        // Warm the function plans.
        for id in 0..50i32 {
            client.execute(&insert, &[&(-id - 1)]).await.expect("warm");
        }
        let started = Instant::now();
        for id in 0..WRITES as i32 {
            client.execute(&insert, &[&id]).await.expect("insert");
            client.execute(&update, &[&id]).await.expect("update");
        }
        let micros = started.elapsed().as_micros() as f64 / (2 * WRITES) as f64;
        results.push(format!(
            "leaves={leaves} capture={captured}: {micros:.1} us"
        ));
    }
    eprintln!("MEASURE {}", results.join(" | "));
}

/// A plain table detached from the tree is no partition at all, so
/// `pg_partition_root` reads NULL for it; once the root is dropped the root's
/// name reads NULL too. The check must still stage nothing for a write to
/// the detached table, which keeps the dropped root's triggers.
#[tokio::test]
async fn a_write_to_a_detached_table_after_its_root_is_dropped_stages_nothing() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = tree(&db).await;
    client
        .batch_execute(
            "alter table public.m2 detach partition public.m2_0; \
             alter table public.r detach partition public.m1; \
             drop table public.r",
        )
        .await
        .expect("detach a leaf and a middle, then drop the root");
    let before = staged_total(&client).await;
    client
        .batch_execute(
            "insert into public.m2_0 values (2, 2, 'x'); update public.m2_0 set v = 'y'; \
             insert into public.m1 values (1, 1, 'x'); delete from public.m1; \
             truncate public.m2_0",
        )
        .await
        .expect("writes to the detached tables");
    assert_eq!(staged_total(&client).await, before, "nothing staged");
}

/// A leaf under a middle whose `DETACH … CONCURRENTLY` is pending is not
/// listed for cleanup either: `pg_partition_ancestors` stops at the pending
/// link, so the leaf no longer reaches the captured root, but a writer with
/// an older snapshot can still write through the root into it.
#[tokio::test]
async fn the_leaves_of_a_detach_pending_middle_are_not_cleaned_up_until_the_detach_completes() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = tree(&db).await;
    step(
        &client,
        "insert into public.r values (1, 2, 'a'), (2, 2, 'b'), (3, 2, 'c'), (4, 2, 'd')",
        &[&format!("{INS}=4")],
    )
    .await;

    let holder = connect(db.dsn()).await;
    holder
        .batch_execute("begin; lock table public.m2 in access share mode")
        .await
        .expect("hold the middle");
    let detacher = connect(db.dsn()).await;
    detacher
        .batch_execute("set statement_timeout = '1500ms'")
        .await
        .expect("timeout");
    let interrupted = detacher
        .batch_execute("alter table public.r detach partition public.m2 concurrently")
        .await;
    assert!(interrupted.is_err(), "the second phase waits on the holder");
    let pending: bool = client
        .query_one(
            "select inhdetachpending from pg_inherits where inhrelid = 'public.m2'::regclass",
            &[],
        )
        .await
        .expect("pending")
        .get(0);
    assert!(pending, "public.m2 is detach-pending");

    // A write named on the pending middle or a leaf under it stages nothing.
    let before = staged_total(&client).await;
    client
        .batch_execute(
            "update public.m2 set v = 'm'; update public.m2_0 set v = 'x'; \
             update public.m2_1 set v = 'y'",
        )
        .await
        .expect("writes under the pending middle");
    assert_eq!(staged_total(&client).await, before);

    let tables = trellis::capture::reconcile::installed_tables(&client, DEFAULT_SCHEMA)
        .await
        .expect("installed tables");
    assert_eq!(tables.into_iter().collect::<Vec<_>>(), ["public.r"]);

    holder.batch_execute("commit").await.expect("release");
    client
        .batch_execute("alter table public.r detach partition public.m2 finalize")
        .await
        .expect("finalize");
    let tables = trellis::capture::reconcile::installed_tables(&client, DEFAULT_SCHEMA)
        .await
        .expect("installed tables");
    assert_eq!(
        tables.into_iter().collect::<Vec<_>>(),
        ["public.m2", "public.m2_0", "public.m2_1", "public.r"]
    );
}
