//! A table a logical-replication subscription replicates into isn't
//! supported, and is detected (issue #751, `trellis::defs::subscription`):
//! the apply worker fires only row-level triggers, so Trellis's statement
//! triggers never see the changes it applies.
//!
//! Every subscription here is real, made by `CREATE SUBSCRIPTION`, so the
//! check reads the catalog state Postgres itself writes. Most are made
//! against a publication in the same database, disabled and with no slot
//! (`slot_name = NONE`): that still connects to the publisher to list its
//! tables, filling `pg_subscription_rel`, and needs no `wal_level =
//! logical`. The one test that refreshes a subscription needs it enabled, so
//! it switches its cluster to `wal_level = logical` and subscribes across
//! two databases.
//!
//! Every check and every Trellis call runs as a non-superuser login role;
//! only creating a subscription needs the superuser.
//!
//! Nothing polls for convergence (#297): every pass is stepped by hand, and
//! no test waits for a subscription to apply anything.

use std::time::{Duration, Instant};

use testkit::{StopMode, TestCluster};
use tokio_postgres::{Client, NoTls};
use trellis::defs::TransformStatus;
use trellis::defs::subscription::{Subscribed, subscribed};
use trellis::intake::markers;
use trellis::{CaptureFault, CatalogError, Divergence, SelfCheckOutcome, TrellisError};

const SCHEMA: &str = trellis::config::DEFAULT_SCHEMA;

async fn connect(dsn: &str) -> Client {
    let (client, connection) = tokio_postgres::connect(dsn, NoTls).await.expect("connect");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
}

/// `CREATE SUBSCRIPTION name` to `publication`, published in the database
/// `conninfo` reaches, disabled and with no slot unless `options` says
/// otherwise. `options` is appended to the `WITH` list.
fn create_subscription(name: &str, conninfo: &str, publication: &str, options: &str) -> String {
    let with = if options.is_empty() {
        "create_slot = false, enabled = false, slot_name = NONE".to_string()
    } else {
        format!("create_slot = false, enabled = false, slot_name = NONE, {options}")
    };
    format!(
        "create subscription {name} connection '{conninfo}' publication {publication} with ({with})"
    )
}

/// Each `srrelid`'s `(subscription, srsubstate)`, as the admin reads them.
async fn subscription_rels(admin: &Client) -> Vec<(String, String, String)> {
    admin
        .query(
            "select r.srrelid::regclass::text, s.subname::text, r.srsubstate::text \
             from pg_subscription_rel r join pg_subscription s on s.oid = r.srsubid \
             order by 1, 2",
            &[],
        )
        .await
        .expect("read pg_subscription_rel")
        .into_iter()
        .map(|row| (row.get(0), row.get(1), row.get(2)))
        .collect()
}

fn found(table: &str, subscription: &str, enabled: bool) -> Option<Subscribed> {
    Some(Subscribed {
        table: table.to_string(),
        subscription: subscription.to_string(),
        enabled,
    })
}

/// The check against real subscription states, read as a non-superuser:
/// a table counts as replicated into whenever it has a `pg_subscription_rel`
/// row, whatever its `srsubstate` and whether or not the subscription is
/// enabled. A subscription created with `connect = false` lists no tables
/// until a refresh, a dropped one none at all, and one in another database
/// none here: `pg_subscription` is shared across the cluster, but
/// `pg_subscription_rel` isn't.
#[tokio::test]
async fn the_check_reads_real_subscription_states_as_a_non_superuser() {
    let cluster = TestCluster::start();
    let db = cluster.create_empty_database().await;
    let admin = connect(db.dsn()).await;
    admin
        .batch_execute(
            "create role sub_reader login; \
             create table public.t (id int primary key); \
             create table public.u (id int primary key); \
             create table public.w (id int primary key); \
             create publication pub for table public.t, public.u;",
        )
        .await
        .expect("tables, a publication and a non-superuser role");
    let reader = connect(&db.dsn().replace("user=postgres", "user=sub_reader")).await;
    let check = |table: &'static str| {
        let reader = &reader;
        async move { subscribed(reader, table).await.expect("check") }
    };

    admin
        .batch_execute(&create_subscription(
            "s_unconnected",
            db.dsn(),
            "pub",
            "connect = false",
        ))
        .await
        .expect("a subscription that never connected");
    assert_eq!(subscription_rels(&admin).await, vec![]);
    assert_eq!(check("public.t").await, None, "it lists no tables yet");

    admin
        .batch_execute(&create_subscription("s_waiting", db.dsn(), "pub", ""))
        .await
        .expect("a disabled subscription that will copy its tables");
    admin
        .batch_execute(&create_subscription(
            "s_ready",
            db.dsn(),
            "pub",
            "copy_data = false",
        ))
        .await
        .expect("a disabled subscription that won't copy");
    let rel = |table: &str, sub: &str, state: &str| {
        (table.to_string(), sub.to_string(), state.to_string())
    };
    assert_eq!(
        subscription_rels(&admin).await,
        vec![
            rel("t", "s_ready", "r"),
            rel("t", "s_waiting", "i"),
            rel("u", "s_ready", "r"),
            rel("u", "s_waiting", "i"),
        ],
        "a table waiting for its copy (i) and one ready (r)"
    );
    assert_eq!(check("public.t").await, found("public.t", "s_ready", false));
    assert_eq!(check("public.u").await, found("public.u", "s_ready", false));
    assert_eq!(check("public.w").await, None, "not published");
    assert_eq!(check("public.missing").await, None, "no such table");
    admin
        .batch_execute("drop subscription s_ready")
        .await
        .expect("drop one");
    assert_eq!(
        check("public.t").await,
        found("public.t", "s_waiting", false),
        "a table waiting for its initial copy counts"
    );
    admin
        .batch_execute("drop subscription s_waiting")
        .await
        .expect("drop the other");
    assert_eq!(check("public.t").await, None, "a dropped subscription");

    // Another database subscribes to its own `public.t`.
    let other = cluster.create_empty_database().await;
    let other_admin = connect(other.dsn()).await;
    other_admin
        .batch_execute(&format!(
            "create table public.t (id int primary key); \
             create publication pub for table public.t; \
             {}",
            create_subscription("s_elsewhere", other.dsn(), "pub", "")
        ))
        .await
        .expect("a subscription in another database");
    let listed: Vec<String> = reader
        .query("select subname::text from pg_subscription order by 1", &[])
        .await
        .expect("pg_subscription is readable, less subconninfo")
        .into_iter()
        .map(|row| row.get(0))
        .collect();
    assert_eq!(listed, ["s_elsewhere", "s_unconnected"]);
    assert_eq!(
        check("public.t").await,
        None,
        "it replicates into its own t"
    );
    other_admin
        .batch_execute("drop subscription s_elsewhere")
        .await
        .expect("drop it");
    admin
        .batch_execute("drop subscription s_unconnected")
        .await
        .expect("drop the unconnected one");
}

/// `ALTER SUBSCRIPTION … REFRESH PUBLICATION` after the publisher stops
/// publishing a table takes it out of `pg_subscription_rel`, so the check
/// stops finding it. A refresh needs an enabled subscription, which needs a
/// slot, so this cluster runs at `wal_level = logical` and the publication
/// is in another database (a subscription can't make its slot in its own
/// cluster, so the test makes it first).
#[tokio::test]
async fn a_refresh_that_drops_the_table_from_the_subscription_clears_the_check() {
    let cluster = TestCluster::start();
    {
        let admin = connect(&cluster.database_dsn("postgres")).await;
        admin
            .batch_execute("alter system set wal_level = logical")
            .await
            .expect("wal_level = logical");
    }
    cluster.restart(StopMode::Fast);
    let publisher = cluster.create_empty_database().await;
    let subscriber = cluster.create_empty_database().await;
    let pub_admin = connect(publisher.dsn()).await;
    pub_admin
        .batch_execute(
            "create table public.t (id int primary key); \
             create table public.u (id int primary key); \
             create publication pub for table public.t, public.u;",
        )
        .await
        .expect("a publication");
    // A statement of its own: a multi-statement batch is one transaction,
    // and a slot can't be made in one that has written.
    pub_admin
        .batch_execute("select pg_create_logical_replication_slot('s_live', 'pgoutput')")
        .await
        .expect("a slot for the subscription");
    let admin = connect(subscriber.dsn()).await;
    admin
        .batch_execute(&format!(
            "create role sub_reader login; \
             create table public.t (id int primary key); \
             create table public.u (id int primary key); \
             create subscription s_live connection '{}' publication pub \
               with (create_slot = false, slot_name = 's_live', copy_data = false);",
            publisher.dsn()
        ))
        .await
        .expect("an enabled subscription");
    let reader = connect(&subscriber.dsn().replace("user=postgres", "user=sub_reader")).await;
    assert_eq!(
        subscribed(&reader, "public.t").await.expect("check"),
        found("public.t", "s_live", true)
    );

    pub_admin
        .batch_execute("alter publication pub drop table public.t")
        .await
        .expect("stop publishing t");
    assert_eq!(
        subscribed(&reader, "public.t").await.expect("check"),
        found("public.t", "s_live", true),
        "the subscriber doesn't know until it refreshes"
    );
    admin
        .batch_execute("alter subscription s_live refresh publication")
        .await
        .expect("refresh");
    assert_eq!(subscribed(&reader, "public.t").await.expect("check"), None);
    assert_eq!(
        subscribed(&reader, "public.u").await.expect("check"),
        found("public.u", "s_live", true),
        "u is still published"
    );
    admin
        .batch_execute("drop subscription s_live")
        .await
        .expect("drop the subscription and its slot");
}

/// A database with a non-superuser login role, `sub_trellis`, that owns the
/// application's tables (it must, to install capture triggers) and runs
/// Trellis, and a publication of each table in the same database for the
/// tests' subscriptions to name.
struct Instance {
    db: testkit::TestDatabase,
    admin: Client,
    raw: Client,
    pool: trellis::Pool,
    trellis: trellis::Trellis,
}

async fn instance(cluster: &TestCluster) -> Instance {
    let db = cluster.create_empty_database().await;
    let admin = connect(db.dsn()).await;
    admin
        .batch_execute(&format!(
            "create role sub_trellis login; \
             grant create on database \"{}\" to sub_trellis; \
             grant create on schema public to sub_trellis; \
             create table public.p (id int primary key, name text); \
             create table public.c (id int primary key, pid int, amount int); \
             insert into public.p select i, 'n' || i from generate_series(1, 3) i; \
             insert into public.c select i, 1 + i % 3, i from generate_series(1, 6) i; \
             alter table public.p owner to sub_trellis; \
             alter table public.c owner to sub_trellis; \
             create publication pub_p for table public.p; \
             create publication pub_c for table public.c;",
            db.name()
        ))
        .await
        .expect("a login role owning the application's tables");
    assert!(db.dsn().contains("user=postgres"), "{}", db.dsn());
    let dsn = db.dsn().replace("user=postgres", "user=sub_trellis");
    let config = trellis::Config::with_schema(dsn.clone(), SCHEMA).expect("valid config");
    let pool = trellis::Pool::new(&config).expect("pool");
    trellis::migrate(&pool, &config)
        .await
        .expect("migrate as the login role");
    let raw = connect(&dsn).await;
    raw.batch_execute(&format!("set search_path to {SCHEMA}, public"))
        .await
        .expect("set search_path");
    let trellis = trellis::Trellis::connect(config, trellis::TrellisOptions::default())
        .await
        .expect("connect as the login role");
    Instance {
        db,
        admin,
        raw,
        pool,
        trellis,
    }
}

impl Instance {
    /// Subscribes to `publication`, disabled and with no slot.
    async fn subscribe(&self, name: &str, publication: &str) {
        self.admin
            .batch_execute(&create_subscription(name, self.db.dsn(), publication, ""))
            .await
            .expect("create a subscription");
    }

    async fn unsubscribe(&self, name: &str) {
        self.admin
            .batch_execute(&format!("drop subscription {name}"))
            .await
            .expect("drop the subscription");
    }
}

fn refused_for_subscription(result: Result<trellis::Applied, TrellisError>) -> Subscribed {
    match result {
        Err(TrellisError::Catalog(CatalogError::Subscribed(sub))) => sub,
        other => panic!("expected a subscription refusal, got {other:?}"),
    }
}

/// Defining refuses a source, or a relationship's to-side the definition
/// reads through, that a subscription replicates into, even a disabled one.
/// A definition that reads neither is accepted.
#[tokio::test]
async fn defining_refuses_a_table_a_subscription_replicates_into() {
    let cluster = TestCluster::start();
    let it = instance(&cluster).await;
    it.trellis
        .apply("RELATIONSHIP parent FROM c.pid TO p.id")
        .await
        .expect("declare a relationship");
    it.subscribe("s_p", "pub_p").await;

    let sub = refused_for_subscription(
        it.trellis
            .apply("TRANSFORM p_copy FROM public.p SELECT name AS name")
            .await,
    );
    assert_eq!(sub, found("public.p", "s_p", false).unwrap());
    let err = it
        .trellis
        .apply("TRANSFORM c_named FROM public.c SELECT amount AS amount, parent.name AS name")
        .await
        .expect_err("a to-side it replicates into");
    assert_eq!(err.code(), trellis::ErrorCode::Validation);
    assert!(
        err.to_string()
            .contains("public.p is replicated into by logical-replication subscription s_p"),
        "{err}"
    );
    it.trellis
        .apply("TRANSFORM c_copy FROM public.c SELECT amount AS amount")
        .await
        .expect("a definition that reads neither is accepted");

    it.unsubscribe("s_p").await;
    it.trellis
        .apply("TRANSFORM c_named FROM public.c SELECT amount AS amount, parent.name AS name")
        .await
        .expect("accepted once nothing replicates into the to-side");
    it.trellis.shutdown().await.expect("shutdown");
}

/// The capture half of one staging-worker pass.
async fn capture_pass(raw: &mut Client, pool: &trellis::Pool) {
    let desired = trellis::defs::tables_to_capture(pool)
        .await
        .expect("read the tables to capture");
    let outcome = trellis::capture::reconcile::reconcile(
        raw,
        SCHEMA,
        &desired,
        Instant::now() + Duration::from_secs(1),
    )
    .await
    .expect("capture pass");
    assert!(outcome.failed.is_empty(), "{:?}", outcome.failed);
}

async fn status(trellis: &trellis::Trellis, target: &str) -> trellis::DefinitionStatus {
    trellis
        .status(target)
        .await
        .expect("status")
        .unwrap_or_else(|| panic!("{target} is registered"))
}

async fn self_check(trellis: &trellis::Trellis, target: &str) -> SelfCheckOutcome {
    trellis
        .self_check(
            target,
            trellis::SelfCheckScope {
                after: None,
                limit: 100,
            },
            trellis::SelfCheckMode::Strict,
            Duration::from_secs(30),
        )
        .await
        .expect("self_check")
        .outcome
}

/// A subscription created after define that replicates into a live
/// definition's source: `self_check`'s capture audit reports it, though
/// every trigger is in place, and the staging worker's next capture pass
/// pauses the definition with the reason as its `capture_failure`. A
/// subscription to a table it doesn't read changes nothing. Once the
/// subscription is gone, resuming rebuilds the definition to `live`.
#[tokio::test]
async fn the_capture_pass_pauses_a_reader_once_a_subscription_replicates_into_it() {
    let cluster = TestCluster::start();
    let mut it = instance(&cluster).await;
    it.trellis
        .apply("TRANSFORM c_copy FROM public.c SELECT amount AS amount")
        .await
        .expect("define");
    capture_pass(&mut it.raw, &it.pool).await;
    markers::settle_registrations(&it.pool).await;
    assert_eq!(
        status(&it.trellis, "c_copy").await.status,
        TransformStatus::Live
    );

    it.subscribe("s_p", "pub_p").await;
    capture_pass(&mut it.raw, &it.pool).await;
    let reported = status(&it.trellis, "c_copy").await;
    assert_eq!(reported.status, TransformStatus::Live, "it doesn't read p");
    assert_eq!(reported.capture_failure, None);
    assert!(
        matches!(
            self_check(&it.trellis, "c_copy").await,
            SelfCheckOutcome::Converged
        ),
        "nothing replicates into c"
    );

    it.subscribe("s_c", "pub_c").await;
    let expected = found("public.c", "s_c", false).unwrap();
    match self_check(&it.trellis, "c_copy").await {
        SelfCheckOutcome::Diverged(divergences) => assert_eq!(
            divergences,
            vec![Divergence::Capture(CaptureFault::Subscribed(
                expected.clone()
            ))]
        ),
        other => panic!("expected the capture audit's fault, got {other:?}"),
    }
    capture_pass(&mut it.raw, &it.pool).await;
    let reported = status(&it.trellis, "c_copy").await;
    assert_eq!(reported.status, TransformStatus::Paused);
    let failure = reported.capture_failure.expect("the reason is reported");
    assert_eq!(failure.source_table, "public.c");
    assert!(failure.columns.is_empty(), "{failure:?}");
    assert!(
        failure.error.starts_with(&expected.to_string()) && failure.error.contains("resume"),
        "{failure:?}"
    );
    capture_pass(&mut it.raw, &it.pool).await;
    assert_eq!(
        status(&it.trellis, "c_copy").await.status,
        TransformStatus::Paused,
        "a later pass leaves it paused"
    );

    it.unsubscribe("s_c").await;
    it.unsubscribe("s_p").await;
    it.trellis
        .apply("RESUME TRANSFORM c_copy")
        .await
        .expect("resume");
    capture_pass(&mut it.raw, &it.pool).await;
    markers::settle_registrations(&it.pool).await;
    let reported = status(&it.trellis, "c_copy").await;
    assert_eq!(reported.status, TransformStatus::Live);
    assert_eq!(reported.capture_failure, None);
    let copied: i64 = it
        .raw
        .query_one("select count(*) from public.c_copy", &[])
        .await
        .expect("read the target")
        .get(0);
    assert_eq!(copied, 6, "the rebuild read every row");
    it.trellis.shutdown().await.expect("shutdown");
}

/// A table another definition targets isn't captured (the target-mutation
/// seam feeds it), and the seam doesn't see a subscription's writes to it
/// either, so the capture pass checks it too: a subscription into an
/// upstream's target pauses the definition sourced from it, and not the
/// upstream, which doesn't read its own target.
#[tokio::test]
async fn the_capture_pass_pauses_a_reader_of_a_seam_fed_table() {
    let cluster = TestCluster::start();
    let mut it = instance(&cluster).await;
    it.trellis
        .apply("TRANSFORM c_copy FROM public.c SELECT amount AS amount")
        .await
        .expect("define the upstream");
    capture_pass(&mut it.raw, &it.pool).await;
    markers::settle_registrations(&it.pool).await;
    it.trellis
        .apply("TRANSFORM c_again FROM public.c_copy SELECT amount AS amount")
        .await
        .expect("define one sourced from its target");
    capture_pass(&mut it.raw, &it.pool).await;
    markers::settle_registrations(&it.pool).await;

    it.admin
        .batch_execute("create publication pub_target for table public.c_copy")
        .await
        .expect("publish the upstream's target");
    it.subscribe("s_target", "pub_target").await;
    capture_pass(&mut it.raw, &it.pool).await;
    let reported = status(&it.trellis, "c_again").await;
    assert_eq!(reported.status, TransformStatus::Paused);
    let failure = reported.capture_failure.expect("the reason is reported");
    assert_eq!(failure.source_table, "public.c_copy");
    assert!(
        failure.error.contains("subscription s_target"),
        "{failure:?}"
    );
    let upstream = status(&it.trellis, "c_copy").await;
    assert_eq!(upstream.status, TransformStatus::Live);
    assert_eq!(upstream.capture_failure, None);
    it.unsubscribe("s_target").await;
    it.trellis.shutdown().await.expect("shutdown");
}
