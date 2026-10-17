//! ADR-0017 P3 (issue #1061): the reconcile pass records a partitioned
//! source's tree, detects partition maintenance by two persisted fingerprints
//! and queues a correction request for each node it touches.
//!
//! The corrections are consumed by P4; these tests read the durable queue
//! (`partition_corrections`) directly. Every pass is stepped by hand and
//! nothing waits for convergence (#297). The gate that keeps partitioned
//! sources from being defined is opened for this process, so a transform can
//! read a partitioned root.

use std::time::{Duration, Instant};

use testkit::{TestCluster, TestDatabase};
use tokio_postgres::{Client, NoTls};
use trellis::capture::partitions::{self, PendingCorrection};
use trellis::capture::reconcile::PassOutcome;
use trellis::config::DEFAULT_SCHEMA;
use trellis::defs::TransformStatus;
use trellis::intake::markers;
use trellis::staging::apply::ApplyError;
use trellis::{CatalogError, Config, Trellis, TrellisError, TrellisOptions};

const SOURCE: &str = "public.items";

async fn connect(dsn: &str) -> Client {
    let (client, connection) = tokio_postgres::connect(dsn, NoTls).await.expect("connect");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
        .batch_execute(&format!("set search_path to {DEFAULT_SCHEMA}, public"))
        .await
        .expect("set search_path");
    client
}

struct Instance {
    db: TestDatabase,
    raw: Client,
    trellis: Trellis,
}

/// `public.items` partitioned by list on `order_id`:
///
/// - `items_a` for 1 and 2, itself hash-partitioned on `order_id` into
///   `items_a0` and `items_a1`;
/// - `items_b` for 3 and `items_c` for 4.
///
/// The primary key holds both columns a level partitions by, so a sub-level
/// on `amount` can be added later.
const TREE: &str = "create table public.items (id int not null, order_id int not null, \
        amount int not null default 0, primary key (order_id, id, amount)) \
        partition by list (order_id); \
     create table public.items_a partition of public.items for values in (1, 2) \
        partition by hash (order_id); \
     create table public.items_a0 partition of public.items_a \
        for values with (modulus 2, remainder 0); \
     create table public.items_a1 partition of public.items_a \
        for values with (modulus 2, remainder 1); \
     create table public.items_b partition of public.items for values in (3); \
     create table public.items_c partition of public.items for values in (4)";

const NODES: [&str; 6] = [
    "public.items",
    "public.items_a",
    "public.items_a0",
    "public.items_a1",
    "public.items_b",
    "public.items_c",
];

async fn instance(cluster: &TestCluster, tree: &str) -> Instance {
    trellis::defs::partition_tree::open_gate_for_tests();
    let db = cluster.create_isolated_database().await;
    let mut raw = connect(db.dsn()).await;
    raw.batch_execute(tree)
        .await
        .expect("the partitioned source");
    let trellis = Trellis::connect(
        Config::from_dsn(db.dsn().to_string()).expect("valid dsn"),
        TrellisOptions::default(),
    )
    .await
    .expect("connect");
    trellis
        .apply("TRANSFORM totals FROM public.items GROUP BY order_id SELECT COUNT(*) AS n")
        .await
        .expect("define an aggregate over the partitioned source");
    let outcome = pass(&mut raw, &db.pool).await;
    assert!(outcome.failed.is_empty(), "{:?}", outcome.failed);
    markers::settle_registrations(&db.pool).await;
    Instance { db, raw, trellis }
}

/// The capture half of one staging-worker pass.
async fn pass(raw: &mut Client, pool: &trellis::Pool) -> PassOutcome {
    let desired = trellis::defs::tables_to_capture(pool)
        .await
        .expect("read the tables to capture");
    trellis::capture::reconcile::reconcile(
        raw,
        DEFAULT_SCHEMA,
        &desired,
        Instant::now() + Duration::from_secs(5),
    )
    .await
    .expect("capture pass")
}

async fn clean_pass(it: &mut Instance) -> PassOutcome {
    let outcome = pass(&mut it.raw, &it.db.pool).await;
    assert!(outcome.failed.is_empty(), "{:?}", outcome.failed);
    outcome
}

async fn corrections(raw: &Client) -> Vec<PendingCorrection> {
    partitions::pending_corrections(raw, SOURCE)
        .await
        .expect("read the corrections")
}

/// The queue as `(kind, node, cause)`, in queue order.
async fn queued(raw: &Client) -> Vec<(String, String, String)> {
    corrections(raw)
        .await
        .into_iter()
        .map(|c| (c.kind, c.node_name, c.cause))
        .collect()
}

fn entry(kind: &str, node: &str, cause: &str) -> (String, String, String) {
    (kind.to_string(), node.to_string(), cause.to_string())
}

/// The test consumer: takes everything off the queue.
async fn drain(raw: &Client) -> Vec<PendingCorrection> {
    let all = corrections(raw).await;
    for correction in &all {
        partitions::complete_correction(raw, correction.id)
            .await
            .expect("complete");
    }
    all
}

async fn recorded_names(raw: &Client) -> Vec<String> {
    let mut names: Vec<String> = partitions::recorded(raw, SOURCE)
        .await
        .expect("read the record")
        .expect("a recorded tree")
        .nodes
        .into_iter()
        .map(|n| n.name)
        .collect();
    names.sort();
    names
}

/// The tables whose capture triggers are installed, among `public`'s.
async fn tables_with_triggers(raw: &Client) -> Vec<String> {
    raw.query(
        "select distinct c.relname::text from pg_catalog.pg_trigger t \
         join pg_catalog.pg_class c on c.oid = t.tgrelid \
         where c.relnamespace = 'public'::regnamespace and not t.tgisinternal \
         order by 1",
        &[],
    )
    .await
    .expect("triggers")
    .into_iter()
    .map(|r| format!("public.{}", r.get::<_, String>(0)))
    .collect()
}

async fn status(trellis: &Trellis) -> trellis::DefinitionStatus {
    trellis
        .status("totals")
        .await
        .expect("status")
        .expect("registered")
}

/// The first pass over a source records the tree and queues nothing (the
/// definition's build covers what is there), reads it in full once, and
/// every pass after that compares two fingerprints only, Trellis's own install
/// included.
#[tokio::test]
async fn the_first_pass_records_the_tree_and_later_passes_check_nothing() {
    let cluster = TestCluster::start();
    let mut it = instance(&cluster, TREE).await;

    assert_eq!(recorded_names(&it.raw).await, NODES);
    assert_eq!(tables_with_triggers(&it.raw).await, {
        let mut nodes: Vec<String> = NODES.iter().map(ToString::to_string).collect();
        nodes.sort();
        nodes
    });
    assert!(queued(&it.raw).await.is_empty());
    // Every non-root node's constraint text is recorded.
    let recorded = partitions::recorded(&it.raw, SOURCE)
        .await
        .expect("record")
        .expect("recorded");
    assert!(
        recorded
            .nodes
            .iter()
            .skip(1)
            .all(|n| n.constraint.is_some()),
        "{recorded:?}"
    );
    assert_eq!(
        recorded.keys.len(),
        2,
        "one key column on the root, one on items_a"
    );

    for _ in 0..2 {
        let outcome = clean_pass(&mut it).await;
        assert!(
            outcome.partition_deep_checks.is_empty(),
            "{:?}",
            outcome.partition_deep_checks
        );
    }
    it.trellis.shutdown().await.expect("shutdown");
}

/// A new leaf gets capture installed by the pass, and one `new_leaf`
/// correction; the pass after that finds nothing.
#[tokio::test]
async fn a_new_leaf_gets_capture_and_one_correction() {
    let cluster = TestCluster::start();
    let mut it = instance(&cluster, TREE).await;
    it.raw
        .batch_execute(
            "create table public.items_d partition of public.items for values in (5); \
             insert into public.items_d values (1, 5, 1)",
        )
        .await
        .expect("a new leaf, with a row written before capture");
    assert!(
        !tables_with_triggers(&it.raw)
            .await
            .contains(&"public.items_d".to_string()),
        "a new leaf is a bare node until the pass"
    );

    let outcome = clean_pass(&mut it).await;
    assert_eq!(outcome.partition_deep_checks, [SOURCE]);
    assert!(
        tables_with_triggers(&it.raw)
            .await
            .contains(&"public.items_d".to_string())
    );
    assert_eq!(
        queued(&it.raw).await,
        [entry("new_leaf", "public.items_d", "created")]
    );
    assert!(
        recorded_names(&it.raw)
            .await
            .contains(&"public.items_d".to_string())
    );

    let outcome = clean_pass(&mut it).await;
    assert!(outcome.partition_deep_checks.is_empty());
    assert_eq!(corrections(&it.raw).await.len(), 1, "not queued twice");
    it.trellis.shutdown().await.expect("shutdown");
}

/// A detached leaf queues its topmost gone node with the text recorded while
/// it was in the tree; so does a dropped one, and a detached middle queues
/// the middle alone.
#[tokio::test]
async fn a_gone_subtree_queues_its_topmost_node_with_the_recorded_text() {
    let cluster = TestCluster::start();
    let mut it = instance(&cluster, TREE).await;
    let before = partitions::recorded(&it.raw, SOURCE)
        .await
        .expect("record")
        .expect("recorded");
    let text_of = |name: &str| {
        before
            .nodes
            .iter()
            .find(|n| n.name == name)
            .and_then(|n| n.constraint.clone())
            .unwrap_or_else(|| panic!("{name} has a recorded text"))
    };

    it.raw
        .batch_execute(
            "alter table public.items detach partition public.items_b; \
             drop table public.items_c; \
             alter table public.items detach partition public.items_a",
        )
        .await
        .expect("detach, drop, detach a middle");
    let outcome = clean_pass(&mut it).await;
    assert_eq!(outcome.partition_deep_checks, [SOURCE]);

    let mut got = corrections(&it.raw).await;
    got.sort_by(|a, b| a.node_name.cmp(&b.node_name));
    let summary: Vec<(String, String, Option<String>)> = got
        .iter()
        .map(|c| (c.kind.clone(), c.node_name.clone(), c.constraint.clone()))
        .collect();
    assert_eq!(
        summary,
        [
            (
                "gone_node".to_string(),
                "public.items_a".to_string(),
                Some(text_of("public.items_a"))
            ),
            (
                "gone_node".to_string(),
                "public.items_b".to_string(),
                Some(text_of("public.items_b"))
            ),
            (
                "gone_node".to_string(),
                "public.items_c".to_string(),
                Some(text_of("public.items_c"))
            ),
        ]
    );
    // The leaves of the detached middle are not queued on their own.
    assert_eq!(
        recorded_names(&it.raw).await,
        ["public.items"],
        "the record is the tree that is left"
    );
    // The pass dropped capture from the tables that left the tree.
    assert_eq!(tables_with_triggers(&it.raw).await, ["public.items"]);
    it.trellis.shutdown().await.expect("shutdown");
}

/// A node that leaves the tree twice before the consumer takes the first
/// correction, under two bounds, queues a correction for each recorded text:
/// the groups its rows reached under the second bound route only by that one.
#[tokio::test]
async fn a_node_gone_twice_under_two_bounds_queues_both_texts() {
    let cluster = TestCluster::start();
    let mut it = instance(&cluster, TREE).await;
    it.raw
        .batch_execute("alter table public.items detach partition public.items_b")
        .await
        .expect("detach");
    clean_pass(&mut it).await;
    it.raw
        .batch_execute("alter table public.items attach partition public.items_b for values in (6)")
        .await
        .expect("attach under another bound");
    clean_pass(&mut it).await;
    it.raw
        .batch_execute("drop table public.items_b")
        .await
        .expect("drop");
    clean_pass(&mut it).await;

    let gone: Vec<Option<String>> = corrections(&it.raw)
        .await
        .into_iter()
        .filter(|c| c.kind == "gone_node" && c.node_name == "public.items_b")
        .map(|c| c.constraint)
        .collect();
    assert_eq!(gone.len(), 2, "{gone:?}");
    assert!(
        gone[0].as_deref().is_some_and(|t| t.contains('3')),
        "{gone:?}"
    );
    assert!(
        gone[1].as_deref().is_some_and(|t| t.contains('6')),
        "{gone:?}"
    );
    it.trellis.shutdown().await.expect("shutdown");
}

/// A leaf detached, modified and attached again under the same bound between
/// two passes is a re-attached leaf, which only the `pg_inherits` row's
/// `xmin` shows.
#[tokio::test]
async fn a_leaf_detached_and_attached_again_under_the_same_bound_is_found() {
    let cluster = TestCluster::start();
    let mut it = instance(&cluster, TREE).await;
    it.raw
        .batch_execute(
            "alter table public.items detach partition public.items_b; \
             insert into public.items_b values (7, 3, 7); \
             alter table public.items attach partition public.items_b for values in (3)",
        )
        .await
        .expect("detach, modify, re-attach");
    let outcome = clean_pass(&mut it).await;
    assert_eq!(outcome.partition_deep_checks, [SOURCE]);
    assert_eq!(
        queued(&it.raw).await,
        [entry("new_leaf", "public.items_b", "reattached")]
    );

    // A middle attached again corrects every leaf under it.
    drain(&it.raw).await;
    it.raw
        .batch_execute(
            "alter table public.items detach partition public.items_a; \
             alter table public.items attach partition public.items_a for values in (1, 2)",
        )
        .await
        .expect("re-attach a middle");
    clean_pass(&mut it).await;
    assert_eq!(
        queued(&it.raw).await,
        [
            entry("new_leaf", "public.items_a0", "reattached"),
            entry("new_leaf", "public.items_a1", "reattached"),
        ]
    );

    // Two levels down, only the lifecycle fingerprint's `xmin` shows it: the
    // compliance fingerprint covers the root's own `pg_inherits` rows alone,
    // and the bound's text is the same.
    drain(&it.raw).await;
    it.raw
        .batch_execute(
            "alter table public.items_a detach partition public.items_a0; \
             insert into public.items_a0 values (8, 2, 8); \
             alter table public.items_a attach partition public.items_a0 \
                for values with (modulus 2, remainder 0)",
        )
        .await
        .expect("re-attach a leaf under the middle");
    let outcome = clean_pass(&mut it).await;
    assert_eq!(outcome.partition_deep_checks, [SOURCE]);
    assert_eq!(
        queued(&it.raw).await,
        [entry("new_leaf", "public.items_a0", "reattached")]
    );
    it.trellis.shutdown().await.expect("shutdown");
}

/// A `pg_upgrade` keeps oids and bounds and resets every `xmin`, under a new
/// `system_identifier`: the pass records them again and corrects nothing.
#[tokio::test]
async fn a_pg_upgrade_rebaselines_the_recorded_xmins_and_corrects_nothing() {
    let cluster = TestCluster::start();
    let mut it = instance(&cluster, TREE).await;
    let live_id: String = it
        .raw
        .query_one(
            "select system_identifier from partition_sources where table_name = $1",
            &[&SOURCE],
        )
        .await
        .expect("the identifier is recorded")
        .get(0);
    assert!(!live_id.is_empty());
    // Fake the upgrade: another identifier and every recorded xmin reset.
    it.raw
        .execute(
            "update partition_sources set system_identifier = '1', nodes = jsonb_set(nodes, \
                 '{nodes}', (select jsonb_agg(case when n ? 'inherits_xmin' \
                     and n->'inherits_xmin' <> 'null'::jsonb \
                     then jsonb_set(n, '{inherits_xmin}', '\"1\"') else n end) \
                 from jsonb_array_elements(nodes->'nodes') n)) \
             where table_name = $1",
            &[&SOURCE],
        )
        .await
        .expect("fake a pg_upgrade");
    let outcome = clean_pass(&mut it).await;
    assert_eq!(outcome.partition_deep_checks, [SOURCE]);
    assert!(queued(&it.raw).await.is_empty());
    let row = it
        .raw
        .query_one(
            "select system_identifier, nodes::text from partition_sources where table_name = $1",
            &[&SOURCE],
        )
        .await
        .expect("recorded");
    assert_eq!(row.get::<_, String>(0), live_id);
    assert!(!row.get::<_, String>(1).contains("\"inherits_xmin\": \"1\""));
    // And the next pass has nothing to check.
    let outcome = clean_pass(&mut it).await;
    assert!(outcome.partition_deep_checks.is_empty());
    it.trellis.shutdown().await.expect("shutdown");
}

/// A `DEFAULT` whose siblings changed is treated as new, and its text is
/// fetched again.
#[tokio::test]
async fn a_default_whose_siblings_changed_is_treated_as_new() {
    let cluster = TestCluster::start();
    let mut it = instance(
        &cluster,
        "create table public.items (id int not null, order_id int not null, \
             amount int not null default 0, primary key (order_id, id)) \
             partition by list (order_id); \
         create table public.items_1 partition of public.items for values in (1); \
         create table public.items_x partition of public.items default",
    )
    .await;
    let text = |recorded: &partitions::Recorded, name: &str| {
        recorded
            .nodes
            .iter()
            .find(|n| n.name == name)
            .and_then(|n| n.constraint.clone())
            .expect("a recorded text")
    };
    let first = partitions::recorded(&it.raw, SOURCE)
        .await
        .unwrap()
        .unwrap();

    it.raw
        .batch_execute("create table public.items_2 partition of public.items for values in (2)")
        .await
        .expect("a sibling");
    clean_pass(&mut it).await;
    let mut got = queued(&it.raw).await;
    got.sort();
    assert_eq!(
        got,
        [
            entry("new_leaf", "public.items_2", "created"),
            entry("new_leaf", "public.items_x", "default_siblings"),
        ]
    );
    let second = partitions::recorded(&it.raw, SOURCE)
        .await
        .unwrap()
        .unwrap();
    assert_ne!(
        text(&first, "public.items_x"),
        text(&second, "public.items_x"),
        "the DEFAULT's text follows its siblings"
    );
    assert_eq!(
        text(&first, "public.items_1"),
        text(&second, "public.items_1"),
        "an unchanged leaf's text is carried"
    );

    // A sibling dropped changes it again.
    drain(&it.raw).await;
    it.raw
        .batch_execute("drop table public.items_2")
        .await
        .expect("drop the sibling");
    clean_pass(&mut it).await;
    let mut got = queued(&it.raw).await;
    got.sort();
    assert_eq!(
        got,
        [
            entry("gone_node", "public.items_2", "gone"),
            entry("new_leaf", "public.items_x", "default_siblings"),
        ]
    );
    it.trellis.shutdown().await.expect("shutdown");
}

/// An interrupted `DETACH … CONCURRENTLY` leaves the leaf pending: it has
/// left the tree for capture already, so it is queued gone; its triggers stay
/// until the detach completes, and finishing it queues nothing more.
#[tokio::test]
async fn a_detach_pending_leaf_is_queued_once_and_cleaned_up_after_the_detach_completes() {
    let cluster = TestCluster::start();
    let mut it = instance(&cluster, TREE).await;
    let holder = connect(it.db.dsn()).await;
    holder
        .batch_execute("begin; lock table public.items_b in access share mode")
        .await
        .expect("hold the leaf");
    let detacher = connect(it.db.dsn()).await;
    detacher
        .batch_execute("set statement_timeout = '1500ms'")
        .await
        .expect("timeout");
    let interrupted = detacher
        .batch_execute("alter table public.items detach partition public.items_b concurrently")
        .await;
    assert!(interrupted.is_err(), "the second phase waits on the holder");

    clean_pass(&mut it).await;
    assert_eq!(
        queued(&it.raw).await,
        [entry("gone_node", "public.items_b", "gone")]
    );
    assert!(
        tables_with_triggers(&it.raw)
            .await
            .contains(&"public.items_b".to_string()),
        "the pending leaf keeps its triggers until the detach completes"
    );
    assert!(
        !recorded_names(&it.raw)
            .await
            .contains(&"public.items_b".to_string())
    );

    holder.batch_execute("rollback").await.expect("release");
    it.raw
        .batch_execute("alter table public.items detach partition public.items_b finalize")
        .await
        .expect("finish the detach");
    let outcome = clean_pass(&mut it).await;
    assert_eq!(outcome.partition_deep_checks, [SOURCE]);
    assert_eq!(
        queued(&it.raw).await,
        [entry("gone_node", "public.items_b", "gone")],
        "finishing the detach queues nothing more"
    );
    assert!(
        !tables_with_triggers(&it.raw)
            .await
            .contains(&"public.items_b".to_string()),
        "cleaned up once the detach completed"
    );
    it.trellis.shutdown().await.expect("shutdown");
}

/// The queue and the fingerprints are committed together, so a restart
/// finds the corrections still queued, checks the source in full once more,
/// and queues nothing twice.
#[tokio::test]
async fn a_restart_finds_the_queued_corrections() {
    let cluster = TestCluster::start();
    let mut it = instance(&cluster, TREE).await;
    it.raw
        .batch_execute(
            "create table public.items_d partition of public.items for values in (5); \
             alter table public.items detach partition public.items_c",
        )
        .await
        .expect("maintenance");
    clean_pass(&mut it).await;
    let before = corrections(&it.raw).await;
    assert_eq!(before.len(), 2);

    let database: String = it
        .raw
        .query_one("select current_database()::text", &[])
        .await
        .unwrap()
        .get(0);
    partitions::forget_checked(&database, DEFAULT_SCHEMA);
    let outcome = clean_pass(&mut it).await;
    assert_eq!(
        outcome.partition_deep_checks,
        [SOURCE],
        "a restart checks the source in full"
    );
    assert_eq!(corrections(&it.raw).await, before, "nothing queued twice");
    let outcome = clean_pass(&mut it).await;
    assert!(outcome.partition_deep_checks.is_empty());

    // The consumer takes what is queued; a later pass doesn't bring it back.
    assert_eq!(drain(&it.raw).await.len(), 2);
    clean_pass(&mut it).await;
    assert!(corrections(&it.raw).await.is_empty());
    it.trellis.shutdown().await.expect("shutdown");
}

/// A pass whose install of a new leaf's triggers is still waiting on a lock
/// records nothing and queues nothing; the next pass finds the change again.
#[tokio::test]
async fn nothing_is_recorded_until_capture_covers_the_new_leaf() {
    let cluster = TestCluster::start();
    let mut it = instance(&cluster, TREE).await;
    // A writer holds the new leaf, so the install of its triggers waits.
    it.raw
        .batch_execute("create table public.items_d partition of public.items for values in (5)")
        .await
        .expect("a new leaf");
    let holder = connect(it.db.dsn()).await;
    holder
        .batch_execute("begin; lock table public.items_d in share row exclusive mode")
        .await
        .expect("hold the new leaf");
    let desired = trellis::defs::tables_to_capture(&it.db.pool)
        .await
        .expect("tables");
    let outcome = trellis::capture::reconcile::reconcile(
        &mut it.raw,
        DEFAULT_SCHEMA,
        &desired,
        Instant::now() + Duration::from_millis(300),
    )
    .await
    .expect("pass");
    assert_eq!(outcome.waiting.len(), 1, "{:?}", outcome.waiting);
    assert!(queued(&it.raw).await.is_empty(), "nothing queued yet");
    assert!(
        !recorded_names(&it.raw)
            .await
            .contains(&"public.items_d".to_string())
    );

    holder.batch_execute("rollback").await.expect("release");
    let outcome = clean_pass(&mut it).await;
    assert_eq!(outcome.partition_deep_checks, [SOURCE]);
    assert_eq!(
        queued(&it.raw).await,
        [entry("new_leaf", "public.items_d", "created")]
    );
    it.trellis.shutdown().await.expect("shutdown");
}

/// A new sub-level whose key isn't in the group key pauses the definition
/// with the reason; `RESUME` refuses until the tree complies.
#[tokio::test]
async fn a_sub_level_outside_the_group_key_pauses_the_reader() {
    let cluster = TestCluster::start();
    let mut it = instance(&cluster, TREE).await;
    it.raw
        .batch_execute(
            "create table public.items_d partition of public.items for values in (5) \
                partition by hash (amount); \
             create table public.items_d0 partition of public.items_d \
                for values with (modulus 1, remainder 0)",
        )
        .await
        .expect("a sub-level on another column");
    clean_pass(&mut it).await;
    let reported = status(&it.trellis).await;
    assert_eq!(reported.status, TransformStatus::Paused);
    let failure = reported.capture_failure.expect("a reason");
    assert_eq!(failure.source_table, SOURCE);
    assert!(
        failure.error.contains("\"amount\"") && failure.error.contains("GROUP BY"),
        "{failure:?}"
    );
    assert!(failure.error.contains("resume"), "{failure:?}");

    let err = it
        .trellis
        .apply("RESUME TRANSFORM totals")
        .await
        .expect_err("a resume while the source doesn't comply is refused");
    assert!(
        matches!(
            &err,
            TrellisError::Apply(ApplyError::ResumeRefused { reason, .. })
                if matches!(**reason, CatalogError::PartitionedSource(_))
        ),
        "{err:?}"
    );

    // The pass looks again every pass while the source doesn't comply, and
    // leaves capture as it is: the non-complying sub-level gets none.
    for _ in 0..2 {
        let outcome = clean_pass(&mut it).await;
        assert_eq!(outcome.partition_deep_checks, [SOURCE]);
        assert!(
            !tables_with_triggers(&it.raw)
                .await
                .contains(&"public.items_d0".to_string()),
            "a held source gets no capture"
        );
    }

    // Once the sub-level is gone, a resume rebuilds.
    it.raw
        .batch_execute("drop table public.items_d")
        .await
        .expect("remove the sub-level");
    it.trellis
        .apply("RESUME TRANSFORM totals")
        .await
        .expect("resume");
    clean_pass(&mut it).await;
    markers::settle_registrations(&it.db.pool).await;
    assert_eq!(status(&it.trellis).await.status, TransformStatus::Live);
    it.trellis.shutdown().await.expect("shutdown");
}

/// The root attached under another table pauses the definition, through the
/// hierarchy check, and gets no further capture.
#[tokio::test]
async fn the_root_attached_under_another_table_pauses_the_reader() {
    let cluster = TestCluster::start();
    let mut it = instance(&cluster, TREE).await;
    it.raw
        .batch_execute(
            "create table public.everything (id int not null, order_id int not null, \
                amount int not null default 0) partition by range (id); \
             alter table public.everything attach partition public.items \
                for values from (minvalue) to (maxvalue)",
        )
        .await
        .expect("attach the root under another table");
    clean_pass(&mut it).await;
    let reported = status(&it.trellis).await;
    assert_eq!(reported.status, TransformStatus::Paused);
    let failure = reported.capture_failure.expect("a reason");
    assert!(
        failure
            .error
            .contains("public.items became a partition of public.everything"),
        "{failure:?}"
    );
    it.trellis.shutdown().await.expect("shutdown");
}

/// A root renamed away and back between two passes: nothing staged under the
/// old name meanwhile, so the pass queues every leaf. A root replaced by
/// another table under the same name queues the old one gone and the new
/// leaves created.
#[tokio::test]
async fn a_renamed_root_is_noticed_and_a_replaced_one_is_a_new_tree() {
    let cluster = TestCluster::start();
    let mut it = instance(&cluster, TREE).await;
    it.raw
        .batch_execute("alter table public.items rename to items_old")
        .await
        .expect("rename away");
    let outcome = pass(&mut it.raw, &it.db.pool).await;
    assert_eq!(outcome.failed.len(), 1, "the source is missing by name");

    it.raw
        .batch_execute("alter table public.items_old rename to items")
        .await
        .expect("rename back");
    let outcome = clean_pass(&mut it).await;
    assert_eq!(outcome.partition_deep_checks, [SOURCE]);
    let mut got = queued(&it.raw).await;
    got.sort();
    assert_eq!(
        got,
        [
            entry("new_leaf", "public.items_a0", "renamed"),
            entry("new_leaf", "public.items_a1", "renamed"),
            entry("new_leaf", "public.items_b", "renamed"),
            entry("new_leaf", "public.items_c", "renamed"),
        ]
    );
    assert!(
        tables_with_triggers(&it.raw)
            .await
            .contains(&"public.items".to_string())
    );
    drain(&it.raw).await;

    // Renamed away while a plain table holds the name for a pass: the same.
    it.raw
        .batch_execute(
            "alter table public.items rename to items_old; \
             create table public.items (id int not null, order_id int not null, \
                amount int not null default 0, primary key (order_id, id, amount))",
        )
        .await
        .expect("rename away, a plain table takes the name");
    pass(&mut it.raw, &it.db.pool).await;
    it.raw
        .batch_execute(
            "drop table public.items; \
             alter table public.items_old rename to items",
        )
        .await
        .expect("rename back");
    clean_pass(&mut it).await;
    assert_eq!(
        queued(&it.raw).await.len(),
        4,
        "every leaf again: {:?}",
        queued(&it.raw).await
    );
    drain(&it.raw).await;

    // Replaced: the old tree moves aside, a new one takes the name.
    it.raw
        .batch_execute(
            "alter table public.items rename to items_old; \
             create table public.items (id int not null, order_id int not null, \
                amount int not null default 0, primary key (order_id, id, amount)) \
                partition by list (order_id); \
             create table public.items_n1 partition of public.items for values in (1, 2); \
             create table public.items_n2 partition of public.items for values in (3, 4)",
        )
        .await
        .expect("replace the root");
    clean_pass(&mut it).await;
    let mut got = queued(&it.raw).await;
    got.sort();
    assert_eq!(
        got,
        [
            entry("gone_node", "public.items", "gone"),
            entry("new_leaf", "public.items_n1", "created"),
            entry("new_leaf", "public.items_n2", "created"),
        ]
    );
    let tables = tables_with_triggers(&it.raw).await;
    assert!(
        ["public.items", "public.items_n1", "public.items_n2"]
            .iter()
            .all(|t| tables.contains(&t.to_string())),
        "{tables:?}"
    );
    it.trellis.shutdown().await.expect("shutdown");
}

/// Statements that change nothing about the tree don't make the pass read
/// it: the zero-false-positive criterion, on 1,000 leaves, with the deep
/// checks counted.
#[tokio::test]
async fn routine_maintenance_on_a_thousand_leaves_is_never_a_deep_check() {
    let cluster = TestCluster::start();
    let mut it = instance(&cluster, &thousand_leaves()).await;
    clean_pass(&mut it).await;

    let statements = [
        "insert into public.items select g, g, g from generate_series(1, 2000) g",
        "update public.items set amount = amount + 0 where id < 500",
        "delete from public.items where id > 1500",
        "vacuum public.items",
        "vacuum (analyze) public.items",
        "vacuum (freeze) public.items",
        "vacuum (full) public.items_0",
        "analyze public.items",
        "create index items_amount on public.items (amount)",
        "reindex index public.items_amount",
        "grant select on public.items to public",
        "alter table public.items_1 set (autovacuum_enabled = on)",
        "alter table public.items_1 set (fillfactor = 90)",
        "cluster public.items_2 using items_2_pkey",
    ];
    for sql in statements {
        it.raw
            .batch_execute(sql)
            .await
            .unwrap_or_else(|e| panic!("{sql}: {e}"));
        let outcome = clean_pass(&mut it).await;
        assert!(
            outcome.partition_deep_checks.is_empty(),
            "{sql} caused a deep check"
        );
    }
    assert!(queued(&it.raw).await.is_empty());

    // Freezing the catalog's own `pg_inherits` rows leaves their `xmin` as it
    // was (the column reports the raw value; freezing sets a flag), and so
    // does rewriting it.
    for sql in [
        "vacuum (freeze) pg_catalog.pg_inherits",
        "vacuum (full, freeze) pg_catalog.pg_inherits",
    ] {
        it.raw
            .batch_execute(sql)
            .await
            .unwrap_or_else(|e| panic!("{sql}: {e}"));
        let outcome = clean_pass(&mut it).await;
        assert!(
            outcome.partition_deep_checks.is_empty(),
            "{sql} caused a deep check"
        );
    }
    assert!(queued(&it.raw).await.is_empty());
    it.trellis.shutdown().await.expect("shutdown");
}

fn thousand_leaves() -> String {
    let mut sql = String::from(
        "create table public.items (id int not null, order_id int not null, \
             amount int not null default 0, primary key (order_id, id)) \
             partition by hash (order_id);",
    );
    for i in 0..1000 {
        sql.push_str(&format!(
            "create table public.items_{i} partition of public.items \
                for values with (modulus 1000, remainder {i});"
        ));
    }
    sql
}

/// What the two fingerprints cost per pass at 1,000 leaves. Run by hand:
/// `cargo test -p trellis --test capture_partition_pass -- --ignored
/// --nocapture measure_fingerprint`.
#[tokio::test]
#[ignore = "a measurement"]
async fn measure_fingerprint_cost_at_a_thousand_leaves() {
    let cluster = TestCluster::start();
    let it = instance(&cluster, &thousand_leaves()).await;
    let mut samples = Vec::new();
    for _ in 0..200 {
        let started = Instant::now();
        let fingerprints = partitions::read_fingerprints(&it.raw, SOURCE)
            .await
            .expect("fingerprints")
            .expect("exists");
        samples.push(started.elapsed());
        assert!(fingerprints.servable);
    }
    samples.sort();
    println!(
        "fingerprints at 1,000 leaves, 200 passes: min {:?}, median {:?}, p95 {:?}, max {:?}",
        samples[0],
        samples[samples.len() / 2],
        samples[samples.len() * 95 / 100],
        samples[samples.len() - 1]
    );
    let started = Instant::now();
    let mut raw = connect(it.db.dsn()).await;
    let outcome = pass(&mut raw, &it.db.pool).await;
    println!(
        "a whole unchanged pass over the 1,000-leaf source: {:?} (deep checks: {})",
        started.elapsed(),
        outcome.partition_deep_checks.len()
    );
}
