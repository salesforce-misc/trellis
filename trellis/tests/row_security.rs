//! Row-level security that applies to the Trellis role isn't supported, and
//! is detected (`trellis::defs::row_security`).
//!
//! The first two tests check the catalog predicate against Postgres itself:
//! for every combination of table setting and role, the check must agree
//! with `row_security_active()` and with what a read actually sees. The rest
//! run Trellis as a non-superuser login role (a superuser bypasses RLS, and a
//! test cluster connects as one): defining refuses a table whose policies
//! apply, and the staging worker's capture pass pauses the readers of one
//! whose policies apply after define, which `self_check` reports too.
//!
//! Nothing polls for convergence: every pass is stepped by hand.

use std::time::{Duration, Instant};

use testkit::TestCluster;
use tokio_postgres::{Client, NoTls};
use trellis::defs::TransformStatus;
use trellis::defs::row_security::{Readers, RowSecurity, applying};
use trellis::intake::harness;
use trellis::staging::self_check::CheckPageExt;
use trellis::staging::{apply, seal};
use trellis::{CaptureFault, CatalogError, Divergence, SelfCheckOutcome, TrellisError};

const SCHEMA: &str = trellis::config::DEFAULT_SCHEMA;

async fn connect(dsn: &str) -> Client {
    let (client, connection) = tokio_postgres::connect(dsn, NoTls).await.expect("connect");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
}

/// The table settings the matrix runs through.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Setting {
    Disabled,
    Enabled,
    Forced,
}

/// Every role setting against every table setting: [`applying`] agrees with
/// `row_security_active()`, which is Postgres's own `check_enable_rls`, and
/// with a read of a table with no policy (default deny), which sees no row
/// exactly when the policies apply. The expected column makes the rules
/// explicit too: the owner, or a role inheriting from it, is exempt unless
/// FORCE is set; BYPASSRLS and superuser are always exempt, and BYPASSRLS
/// isn't inherited; a member that doesn't inherit isn't the owner.
#[tokio::test]
async fn the_check_matches_postgres_for_every_role_and_table_setting() {
    let cluster = TestCluster::start();
    let db = cluster.create_empty_database().await;
    let client = connect(db.dsn()).await;
    client
        .batch_execute(
            "create role rls_owner nologin; \
             create role rls_plain; \
             create role rls_bypass bypassrls; \
             create role rls_member in role rls_owner; \
             create role rls_member_noinherit noinherit in role rls_owner; \
             create role rls_bypass_group nologin bypassrls; \
             create role rls_in_bypass_group in role rls_bypass_group; \
             create table public.t (id int primary key); \
             insert into public.t select generate_series(1, 3); \
             alter table public.t owner to rls_owner; \
             grant select on public.t to public;",
        )
        .await
        .expect("roles and a table");

    // (role, whether the policies apply with RLS enabled, whether they apply
    // with FORCE as well, whether the role owns the table). `None` is the
    // superuser the session connected as.
    let roles: [(Option<&str>, bool, bool, bool); 7] = [
        (Some("rls_plain"), true, true, false),
        (Some("rls_bypass"), false, false, false),
        (Some("rls_owner"), false, true, true),
        (Some("rls_member"), false, true, true),
        (Some("rls_member_noinherit"), true, true, false),
        (Some("rls_in_bypass_group"), true, true, false),
        (None, false, false, false),
    ];
    for setting in [Setting::Disabled, Setting::Enabled, Setting::Forced] {
        client
            .batch_execute(match setting {
                Setting::Disabled => {
                    "alter table public.t disable row level security, \
                     no force row level security"
                }
                Setting::Enabled => {
                    "alter table public.t enable row level security, \
                     no force row level security"
                }
                Setting::Forced => {
                    "alter table public.t enable row level security, \
                     force row level security"
                }
            })
            .await
            .expect("set the table's RLS");
        for (role, when_enabled, when_forced, owns) in roles {
            match role {
                Some(role) => client.batch_execute(&format!("set role {role}")).await,
                None => client.batch_execute("reset role").await,
            }
            .expect("set role");
            let expected = match setting {
                Setting::Disabled => false,
                Setting::Enabled => when_enabled,
                Setting::Forced => when_forced,
            };
            let found = applying(&client, SCHEMA, "public.t", Readers::RingAndSession)
                .await
                .expect("check RLS");
            // No ring here, so all three check the session's role alone.
            assert_eq!(
                applying(&client, SCHEMA, "public.t", Readers::Session)
                    .await
                    .expect("check RLS as a seam-fed table"),
                found,
                "{role:?} with {setting:?}: as a seam-fed table"
            );
            let as_target = applying(&client, SCHEMA, "public.t", Readers::Target)
                .await
                .expect("check RLS as a target");
            assert_eq!(
                as_target,
                found.clone().map(|rls| RowSecurity {
                    target: true,
                    ..rls
                }),
                "{role:?} with {setting:?}: as a target"
            );
            let active: bool = client
                .query_one("select row_security_active('public.t')", &[])
                .await
                .expect("row_security_active")
                .get(0);
            let visible: i64 = client
                .query_one("select count(*) from public.t", &[])
                .await
                .expect("read the table")
                .get(0);
            let case = format!("{role:?} with {setting:?}");
            assert_eq!(found.is_some(), expected, "{case}: {found:?}");
            assert_eq!(active, expected, "{case}: row_security_active");
            assert_eq!(visible == 0, expected, "{case}: {visible} rows visible");
            if let Some(rls) = found {
                assert_eq!(
                    rls,
                    RowSecurity {
                        table: "public.t".to_string(),
                        role: role.expect("a superuser is exempt").to_string(),
                        owner_forced: owns,
                        target: false,
                    },
                    "{case}"
                );
            }
        }
        client
            .batch_execute("reset role")
            .await
            .expect("reset role");
    }
}

/// The capture functions run as the role that owns the ring, and re-read
/// the captured table, so the policies applying to that role
/// count even when the session's role is exempt. When both are subject, the
/// session's role is the one reported. [`Readers::Ring`], define's check,
/// asks about the ring's owner alone: the session's role may be a process's
/// that only defines transforms and never reads the table.
/// [`Readers::Session`] and [`Readers::Target`] ask about the session's role
/// alone, which reads a seam-fed table and writes a target.
#[tokio::test]
async fn the_ring_owner_is_checked_as_well_as_the_session_role() {
    let cluster = TestCluster::start();
    let db = cluster.create_empty_database().await;
    let client = connect(db.dsn()).await;
    client
        .batch_execute(
            "create role rls_ring nologin; \
             create role rls_bypass bypassrls; \
             create role rls_plain; \
             create schema rls_instance; \
             create table rls_instance.seg_0 (x int); \
             alter table rls_instance.seg_0 owner to rls_ring; \
             create table public.t (id int primary key); \
             insert into public.t select generate_series(1, 3); \
             alter table public.t enable row level security; \
             grant select on public.t to public; \
             grant usage on schema rls_instance to public; \
             create function rls_instance.visible() returns bigint \
               language sql security definer as 'select count(*) from public.t'; \
             alter function rls_instance.visible() owner to rls_ring;",
        )
        .await
        .expect("a ring owned by another role, and a table with RLS");

    client
        .batch_execute("set role rls_bypass")
        .await
        .expect("set role");
    // What makes the ring's owner matter: a `SECURITY DEFINER` function
    // reads as its owner, so the policies filter it whoever calls it.
    let as_caller: i64 = client
        .query_one("select count(*) from public.t", &[])
        .await
        .expect("read as the caller")
        .get(0);
    let as_definer: i64 = client
        .query_one("select rls_instance.visible()", &[])
        .await
        .expect("read as the function's owner")
        .get(0);
    assert_eq!((as_caller, as_definer), (3, 0));
    for readers in [Readers::Ring, Readers::RingAndSession] {
        let found = applying(&client, "rls_instance", "public.t", readers)
            .await
            .expect("check RLS")
            .expect("the ring's owner is subject to the policies");
        assert_eq!(found.role, "rls_ring", "{readers:?}");
    }
    for readers in [Readers::Session, Readers::Target] {
        assert!(
            applying(&client, "rls_instance", "public.t", readers)
                .await
                .expect("check RLS")
                .is_none(),
            "{readers:?} asks about the session's role alone: a seam-fed table's \
             reader, or a target's writer"
        );
    }
    assert!(
        applying(
            &client,
            "no_such_instance",
            "public.t",
            Readers::RingAndSession
        )
        .await
        .expect("check RLS")
        .is_none(),
        "an instance with no ring has no other role to check"
    );

    client
        .batch_execute("set role rls_plain")
        .await
        .expect("set role");
    let found = applying(&client, "rls_instance", "public.t", Readers::RingAndSession)
        .await
        .expect("check RLS")
        .expect("both roles are subject");
    assert_eq!(found.role, "rls_plain", "the session's role comes first");
    let found = applying(&client, "rls_instance", "public.t", Readers::Ring)
        .await
        .expect("check RLS")
        .expect("the ring's owner is subject");
    assert_eq!(found.role, "rls_ring", "only the ring's owner");
    client
        .batch_execute("reset role; alter role rls_ring bypassrls; set role rls_plain")
        .await
        .expect("exempt the ring's owner");
    assert!(
        applying(&client, "rls_instance", "public.t", Readers::Ring)
            .await
            .expect("check RLS")
            .is_none(),
        "define's check doesn't ask about the session's role"
    );
    assert!(
        applying(
            &client,
            "rls_instance",
            "public.missing",
            Readers::RingAndSession
        )
        .await
        .expect("check RLS")
        .is_none(),
        "no such table"
    );
}

/// A database with a non-superuser login role, `rls_trellis`, that owns the
/// application's tables (it must, to install capture triggers) and runs
/// Trellis. Returns the admin (superuser) session, a raw session and a pool
/// as `rls_trellis`, and the `Trellis` handle.
struct Instance {
    _db: testkit::TestDatabase,
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
            "create role rls_trellis login; \
             grant create on database \"{}\" to rls_trellis; \
             grant create on schema public to rls_trellis; \
             create table public.p (id int primary key, name text); \
             create table public.c (id int primary key, pid int, amount int); \
             insert into public.p select i, 'n' || i from generate_series(1, 3) i; \
             insert into public.c select i, 1 + i % 3, i from generate_series(1, 6) i; \
             alter table public.p owner to rls_trellis; \
             alter table public.c owner to rls_trellis;",
            db.name()
        ))
        .await
        .expect("a login role owning the application's tables");
    assert!(db.dsn().contains("user=postgres"), "{}", db.dsn());
    let dsn = db.dsn().replace("user=postgres", "user=rls_trellis");
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
        _db: db,
        admin,
        raw,
        pool,
        trellis,
    }
}

fn refused_for_row_security(result: Result<trellis::Applied, TrellisError>) -> RowSecurity {
    match result {
        Err(TrellisError::Catalog(CatalogError::RowSecurityApplies(rls))) => rls,
        other => panic!("expected a row-level security refusal, got {other:?}"),
    }
}

/// A `JOIN` from `c` to `p`, writing `target`.
fn c_join_p(target: &str) -> String {
    format!(
        "TRANSFORM {target} FROM public.c JOIN public.p ON c.pid = p.id GROUP BY p.name \
         WITHOUT SOURCE INDEX SELECT p.name AS name, SUM(c.amount) AS amount"
    )
}

/// Defining refuses a source, or the joined side of a `JOIN`, whose policies
/// apply to the Trellis role. Enabling RLS on a table the role owns, for the
/// application's other roles, is routine and refuses nothing.
#[tokio::test]
async fn defining_refuses_a_table_whose_policies_apply_to_the_trellis_role() {
    let cluster = TestCluster::start();
    let it = instance(&cluster).await;

    it.admin
        .batch_execute(
            "alter table public.c enable row level security; \
             alter table public.p enable row level security; \
             create policy app_only on public.c to public using (false);",
        )
        .await
        .expect("enable RLS on tables the Trellis role owns");
    it.trellis
        .apply(&c_join_p("c_named"))
        .await
        .expect("the owner is exempt without FORCE");

    it.admin
        .batch_execute("alter table public.c force row level security")
        .await
        .expect("force RLS on the source");
    let rls = refused_for_row_security(
        it.trellis
            .apply("TRANSFORM c_copy FROM public.c SELECT amount AS amount")
            .await,
    );
    assert_eq!(
        rls,
        RowSecurity {
            table: "public.c".to_string(),
            role: "rls_trellis".to_string(),
            owner_forced: true,
            target: false,
        }
    );
    let err = it
        .trellis
        .apply("TRANSFORM c_copy FROM public.c SELECT amount AS amount")
        .await
        .expect_err("still refused");
    assert_eq!(err.code(), trellis::ErrorCode::Validation);
    assert!(
        err.to_string().contains("NO FORCE ROW LEVEL SECURITY"),
        "{err}"
    );

    it.admin
        .batch_execute(
            "alter table public.c no force row level security; \
             alter table public.p owner to postgres; \
             grant select on public.p to rls_trellis;",
        )
        .await
        .expect("hand the joined side to another owner");
    let rls = refused_for_row_security(it.trellis.apply(&c_join_p("c_tier")).await);
    assert_eq!(
        rls,
        RowSecurity {
            table: "public.p".to_string(),
            role: "rls_trellis".to_string(),
            owner_forced: false,
            target: false,
        }
    );
    it.trellis
        .apply("TRANSFORM c_plain FROM public.c SELECT amount AS amount")
        .await
        .expect("a definition that doesn't read the joined side is accepted");

    it.admin
        .batch_execute("alter role rls_trellis bypassrls")
        .await
        .expect("exempt the role");
    it.trellis
        .apply(&c_join_p("c_tier"))
        .await
        .expect("a role with BYPASSRLS is exempt");
    it.trellis.shutdown().await.expect("shutdown");
}

/// The capture half of one staging-worker pass. Returns the waiting
/// definitions it found ready to build.
async fn capture_pass(raw: &mut Client, pool: &trellis::Pool) -> Vec<i64> {
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
    outcome.ready
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
        .self_check_to_end(
            target,
            trellis::SelfCheckMode::Strict,
            Duration::from_secs(30),
        )
        .await
        .expect("self_check")
        .outcome
}

/// RLS forced on a live definition's source after define: `self_check`'s
/// capture audit reports it, and the staging worker's next capture pass
/// pauses the definition with the reason as its `capture_failure`. Enabling
/// RLS without FORCE beforehand, on a table the role owns, changes nothing.
/// Once the role is exempted, resuming rebuilds the definition to `live`.
#[tokio::test]
async fn the_capture_pass_pauses_a_reader_once_the_policies_apply_and_self_check_reports_it() {
    let cluster = TestCluster::start();
    let mut it = instance(&cluster).await;
    it.trellis
        .apply("TRANSFORM c_copy FROM public.c SELECT amount AS amount")
        .await
        .expect("define");
    capture_pass(&mut it.raw, &it.pool).await;
    harness::settle_builds(&it.pool).await;
    assert_eq!(
        status(&it.trellis, "c_copy").await.status,
        TransformStatus::Live
    );

    it.admin
        .batch_execute(
            "alter table public.c enable row level security; \
             create policy app_only on public.c to public using (false);",
        )
        .await
        .expect("enable RLS on a table the Trellis role owns");
    capture_pass(&mut it.raw, &it.pool).await;
    let reported = status(&it.trellis, "c_copy").await;
    assert_eq!(
        reported.status,
        TransformStatus::Live,
        "routine: nothing fires"
    );
    assert_eq!(reported.capture_failure, None);
    assert!(
        matches!(
            self_check(&it.trellis, "c_copy").await,
            SelfCheckOutcome::Converged
        ),
        "the owner's reads aren't filtered"
    );

    it.admin
        .batch_execute("alter table public.c force row level security")
        .await
        .expect("force RLS");
    let expected = RowSecurity {
        table: "public.c".to_string(),
        role: "rls_trellis".to_string(),
        owner_forced: true,
        target: false,
    };
    match self_check(&it.trellis, "c_copy").await {
        SelfCheckOutcome::Diverged(divergences) => assert_eq!(
            divergences,
            vec![Divergence::Capture(CaptureFault::RowSecurity(
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

    it.admin
        .batch_execute("alter role rls_trellis bypassrls")
        .await
        .expect("exempt the role");
    it.trellis
        .apply("RESUME TRANSFORM c_copy")
        .await
        .expect("resume");
    capture_pass(&mut it.raw, &it.pool).await;
    harness::settle_builds(&it.pool).await;
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
/// seam feeds it), but its readers read it as the Trellis role all the same,
/// so the capture pass checks it too: RLS forced on an upstream target pauses
/// the definition sourced from it, for its reads. It pauses the upstream as
/// well, for its writes to the target.
#[tokio::test]
async fn the_capture_pass_pauses_a_reader_of_a_seam_fed_table() {
    let cluster = TestCluster::start();
    let mut it = instance(&cluster).await;
    it.trellis
        .apply("TRANSFORM c_copy FROM public.c SELECT amount AS amount")
        .await
        .expect("define the upstream");
    capture_pass(&mut it.raw, &it.pool).await;
    harness::settle_builds(&it.pool).await;
    it.trellis
        .apply("TRANSFORM c_again FROM public.c_copy SELECT amount AS amount")
        .await
        .expect("define one sourced from its target");
    capture_pass(&mut it.raw, &it.pool).await;
    harness::settle_builds(&it.pool).await;
    for target in ["c_copy", "c_again"] {
        assert_eq!(
            status(&it.trellis, target).await.status,
            TransformStatus::Live,
            "{target}"
        );
    }

    it.admin
        .batch_execute(
            "alter table public.c_copy enable row level security, force row level security",
        )
        .await
        .expect("force RLS on the upstream's target");
    capture_pass(&mut it.raw, &it.pool).await;
    let reported = status(&it.trellis, "c_again").await;
    assert_eq!(reported.status, TransformStatus::Paused);
    let failure = reported.capture_failure.expect("the reason is reported");
    assert_eq!(failure.source_table, "public.c_copy");
    assert!(
        failure.error.contains("FORCE ROW LEVEL SECURITY"),
        "{failure:?}"
    );
    assert!(
        failure.error.contains("Trellis's reads of it"),
        "{failure:?}"
    );
    // The upstream writes the forced target as the same role, so it
    // pauses too, for its writes.
    let upstream = status(&it.trellis, "c_copy").await;
    assert_eq!(upstream.status, TransformStatus::Paused);
    let failure = upstream.capture_failure.expect("the reason is reported");
    assert_eq!(failure.source_table, "public.c_copy");
    assert!(
        failure.error.contains("writes to it are filtered"),
        "{failure:?}"
    );
    it.trellis.shutdown().await.expect("shutdown");
}

/// Seals the ring and drains what it sealed through `pool`, as `pool`'s
/// role: a bounded stand-in for one drain thread's turn. Returns the drain's
/// error, if it failed.
async fn seal_and_drain(raw: &mut Client, pool: &trellis::Pool) -> Result<(), String> {
    let sealed = seal::seal_phase1(raw).await.expect("seal phase 1");
    seal::seal_phase2(raw, sealed.sealed_seg_seq, "wake")
        .await
        .expect("seal phase 2");
    for _ in 0..16 {
        match apply::drain_once(
            pool,
            sealed.sealed_seg_seq,
            "row_security_test",
            1,
            "trellis_row_security_test",
        )
        .await
        {
            Ok(Some(_)) => {}
            Ok(None) => return Ok(()),
            Err(err) => return Err(err.to_string()),
        }
    }
    panic!("the drain didn't finish in 16 turns");
}

async fn amount_of(admin: &Client, table: &str, id: i32) -> Option<String> {
    admin
        .query_one(
            &format!("select amount::text from {table} where id = $1"),
            &[&id],
        )
        .await
        .expect("read the target as the superuser")
        .get(0)
}

/// The quarantine's record of every key it has charged or evicted.
async fn poison_rows(admin: &Client) -> i64 {
    admin
        .query_one(&format!("select count(*) from {SCHEMA}.poison"), &[])
        .await
        .expect("count poison rows")
        .get(0)
}

/// Which role writes a target, and what happens when row-level
/// security applies to a role the catalog checks never look at.
///
/// Apply's writes are plain SQL on the draining connection, so they run as
/// that connection's login role, not as the ring's owner (no `SECURITY
/// DEFINER` function is involved). Here the ring's owner has `BYPASSRLS` and
/// a drain logged in as it updates the target. A login role that is a member
/// of it doesn't inherit `BYPASSRLS`, so the target's forced policies apply
/// to it. Neither define, nor the capture pass, nor `self_check` checks that
/// role: they run as the ring's owner, which is exempt.
///
/// The policy hides row 1, so with row-level security on, the member's
/// update of it would silently match nothing and leave the target stale.
/// Every Trellis connection runs with `row_security = off`, so the write
/// raises instead, and the drain pauses the definition that writes the
/// target as a halt, with the reason naming the table and the role, rather
/// than charging the page's keys to the quarantine. The rest of the page
/// commits.
#[tokio::test]
async fn a_drain_as_an_unchecked_role_pauses_the_writer_instead_of_skipping_rows() {
    let cluster = TestCluster::start();
    let mut it = instance(&cluster).await;
    it.trellis
        .apply("TRANSFORM c_copy FROM public.c SELECT amount AS amount")
        .await
        .expect("define");
    capture_pass(&mut it.raw, &it.pool).await;
    harness::settle_builds(&it.pool).await;
    assert_eq!(
        status(&it.trellis, "c_copy").await.status,
        TransformStatus::Live
    );
    it.admin
        .batch_execute(
            "alter role rls_trellis bypassrls; \
             create role rls_worker login in role rls_trellis; \
             alter table public.c_copy enable row level security, force row level security; \
             create policy hide_one on public.c_copy using (id <> 1) with check (true);",
        )
        .await
        .expect("a member login role the target's policies apply to");
    let dsn = it._db.dsn().replace("user=postgres", "user=rls_worker");
    let config = trellis::Config::with_schema(dsn, SCHEMA).expect("valid config");
    let worker = trellis::Pool::new(&config).expect("pool");

    // The catalog checks run as the ring's owner, which is exempt.
    capture_pass(&mut it.raw, &it.pool).await;
    assert_eq!(
        status(&it.trellis, "c_copy").await.status,
        TransformStatus::Live,
        "the capture pass, as the ring's owner, sees nothing to pause"
    );

    it.admin
        .batch_execute("update public.c set amount = 100 where id = 1")
        .await
        .expect("write the source");
    seal_and_drain(&mut it.raw, &it.pool)
        .await
        .expect("a drain as the ring's owner, which has BYPASSRLS, passes");
    assert_eq!(
        amount_of(&it.admin, "public.c_copy", 1).await,
        Some("100".to_string())
    );

    it.admin
        .batch_execute("update public.c set amount = 200 where id = 1")
        .await
        .expect("write the source");
    seal_and_drain(&mut it.raw, &worker)
        .await
        .expect("the member's drain pauses the writer and commits the rest of the page");
    assert_eq!(
        amount_of(&it.admin, "public.c_copy", 1).await,
        Some("100".to_string()),
        "the member's write didn't land"
    );
    let paused = status(&it.trellis, "c_copy").await;
    assert_eq!(paused.status, TransformStatus::Paused);
    let failure = paused.capture_failure.expect("the halt's record");
    assert_eq!(failure.kind, trellis::CaptureFailureKind::Halt);
    assert!(
        failure
            .error
            .contains("row-level security on target public.c_copy applies to role rls_worker"),
        "{}",
        failure.error
    );
    assert!(
        failure
            .error
            .contains("query would be affected by row-level security policy"),
        "carries Postgres's own message: {}",
        failure.error
    );
    assert!(
        failure.error.contains("docs/transforms.md"),
        "points to the docs: {}",
        failure.error
    );
    assert_eq!(
        poison_rows(&it.admin).await,
        0,
        "no key was charged for a failure that isn't any key's"
    );
    it.trellis.shutdown().await.expect("shutdown");
}

/// A refused read beside a key failure. The page holds a
/// change to `p`, whose policies apply to the drain's role and which the
/// drain reads for `p`'s reader, `p_copy`, and a change to key 1 of `c` that
/// one of `c`'s two readers, `c_cheap`, fails to write (its target refuses
/// the amount). The refusal halts `p_copy`, charging no key, and the page's
/// retries skip the refused table. So do isolation's probes: a bisection
/// probe holding `p`'s change would otherwise read the refused table again,
/// halt on its `42501` and leave key 1 uncharged on every drain.
/// (Attribution's probes skip the same way, but hold only key 1's record,
/// which reads nothing refused.) Key 1 is charged to `c_cheap` alone and, at
/// the death threshold, held for it, while `c_copy` applies it.
#[tokio::test]
async fn a_refused_read_halts_while_a_key_failure_beside_it_is_held_for_its_definition() {
    let cluster = TestCluster::start();
    let mut it = instance(&cluster).await;
    for ddl in [
        "TRANSFORM p_copy FROM public.p SELECT name AS name",
        "TRANSFORM c_cheap FROM public.c SELECT amount AS amount",
        "TRANSFORM c_copy FROM public.c SELECT amount AS amount",
    ] {
        it.trellis.apply(ddl).await.expect(ddl);
    }
    capture_pass(&mut it.raw, &it.pool).await;
    harness::settle_builds(&it.pool).await;
    for target in ["p_copy", "c_cheap", "c_copy"] {
        assert_eq!(
            status(&it.trellis, target).await.status,
            TransformStatus::Live,
            "{target}"
        );
    }
    it.admin
        .batch_execute(
            "alter role rls_trellis bypassrls; \
             create role rls_worker login in role rls_trellis; \
             alter table public.p enable row level security, force row level security; \
             create policy hide_two on public.p using (id <> 2); \
             alter table public.c_cheap add constraint cheap check (amount < 100);",
        )
        .await
        .expect("a member login role p's policies apply to, and a narrow target");
    let dsn = it._db.dsn().replace("user=postgres", "user=rls_worker");
    let config = trellis::Config::with_schema(dsn, SCHEMA).expect("valid config");
    let worker = trellis::Pool::new(&config).expect("pool");

    it.admin
        .batch_execute(
            "update public.p set name = 'renamed' where id = 2; \
             update public.c set amount = 500 where id = 1;",
        )
        .await
        .expect("write both sources");
    let sealed = seal::seal_phase1(&mut it.raw).await.expect("seal phase 1");
    seal::seal_phase2(&it.raw, sealed.sealed_seg_seq, "wake")
        .await
        .expect("seal phase 2");
    let mut failures = 0;
    loop {
        match apply::drain_once(
            &worker,
            sealed.sealed_seg_seq,
            "row_security_test",
            1,
            "trellis_row_security_test",
        )
        .await
        {
            Ok(Some(_)) => {}
            Ok(None) => break,
            Err(err) => {
                failures += 1;
                assert!(failures <= 20, "the page never committed: {err}");
            }
        }
    }
    assert_eq!(
        failures,
        trellis::staging::DEFAULT_DEATH_THRESHOLD as usize - 1,
        "each drain below the threshold charges key 1 once, and the one that crosses it \
         holds the key and commits the page"
    );

    let halted = status(&it.trellis, "p_copy").await;
    assert_eq!(halted.status, TransformStatus::Paused);
    let failure = halted.capture_failure.expect("the halt's record");
    assert_eq!(failure.kind, trellis::CaptureFailureKind::Halt);
    assert_eq!(failure.source_table, "public.p");
    let poisoned: Vec<(String, String)> = it
        .admin
        .query(
            &format!(
                "select split_part(d.target_table, '.', 2), p.key from {SCHEMA}.poison p \
                 join {SCHEMA}.transform_definitions d on d.id = p.transform_id order by 1, 2"
            ),
            &[],
        )
        .await
        .expect("read poison")
        .into_iter()
        .map(|row| (row.get(0), row.get(1)))
        .collect();
    assert_eq!(
        poisoned,
        vec![("c_cheap".to_string(), "1".to_string())],
        "key 1 is held for the definition whose write failed, and the refusal charged nothing"
    );
    assert_eq!(
        status(&it.trellis, "c_cheap").await.status,
        TransformStatus::Live
    );
    assert_eq!(
        amount_of(&it.admin, "public.c_copy", 1).await,
        Some("500".to_string()),
        "c_copy applied the change c_cheap failed on"
    );
    assert_eq!(
        amount_of(&it.admin, "public.c_cheap", 1).await,
        Some("1".to_string()),
        "c_cheap holds key 1"
    );
    it.trellis.shutdown().await.expect("shutdown");
}

/// defining refuses a target only when DDL around its creation makes the
/// policies apply to that role: here an event trigger that enables and
/// forces RLS on each new table. One that only enables it, a common
/// "RLS on every table" setup, leaves the owner exempt and refuses nothing.
#[tokio::test]
async fn defining_refuses_a_target_whose_policies_apply_to_the_session_once_created() {
    let cluster = TestCluster::start();
    let it = instance(&cluster).await;
    let event_trigger = |force: &str| {
        format!(
            "create or replace function public.rls_on_new_tables() returns event_trigger \
               language plpgsql as $$ \
             declare cmd record; \
             begin \
               for cmd in select * from pg_event_trigger_ddl_commands() \
                          where command_tag = 'CREATE TABLE' and schema_name = 'public' loop \
                 execute format('alter table %s enable row level security{force}', \
                                cmd.object_identity); \
               end loop; \
             end $$;"
        )
    };
    it.admin
        .batch_execute(&format!(
            "{} create event trigger rls_on_new_tables on ddl_command_end \
               when tag in ('CREATE TABLE') execute function public.rls_on_new_tables();",
            event_trigger("")
        ))
        .await
        .expect("an event trigger enabling RLS on new tables");
    it.trellis
        .apply("TRANSFORM c_copy FROM public.c SELECT amount AS amount")
        .await
        .expect("the target's owner is exempt without FORCE");
    let enabled: bool = it
        .admin
        .query_one(
            "select relrowsecurity from pg_class where oid = 'public.c_copy'::regclass",
            &[],
        )
        .await
        .expect("read the target's RLS")
        .get(0);
    assert!(enabled, "the event trigger ran on the target");

    it.admin
        .batch_execute(&event_trigger(", force row level security"))
        .await
        .expect("force RLS on new tables too");
    let rls = refused_for_row_security(
        it.trellis
            .apply("TRANSFORM c_more FROM public.c SELECT amount AS amount")
            .await,
    );
    assert_eq!(
        rls,
        RowSecurity {
            table: "public.c_more".to_string(),
            role: "rls_trellis".to_string(),
            owner_forced: true,
            target: true,
        }
    );
    let left: Option<String> = it
        .admin
        .query_one("select to_regclass('public.c_more')::text", &[])
        .await
        .expect("look for the target")
        .get(0);
    assert_eq!(left, None, "the refusal rolled the target back");

    it.admin
        .batch_execute("alter role rls_trellis bypassrls")
        .await
        .expect("exempt the role");
    it.trellis
        .apply("TRANSFORM c_more FROM public.c SELECT amount AS amount")
        .await
        .expect("a role with BYPASSRLS is exempt");
    it.trellis.shutdown().await.expect("shutdown");
}

/// RLS on a live definition's target after define. Enabling it on a
/// target the worker's role owns, for the application's readers, changes
/// nothing. Forcing it makes `self_check` report it, and so does handing the
/// table to another owner; the next capture pass pauses the definition that
/// writes it, with the reason as its `capture_failure`. Once the table is
/// the role's again, resuming rebuilds the definition to `live`.
#[tokio::test]
async fn the_capture_pass_pauses_the_writer_of_a_target_its_policies_apply_to() {
    let cluster = TestCluster::start();
    let mut it = instance(&cluster).await;
    it.trellis
        .apply("TRANSFORM c_copy FROM public.c SELECT amount AS amount")
        .await
        .expect("define");
    capture_pass(&mut it.raw, &it.pool).await;
    harness::settle_builds(&it.pool).await;
    assert_eq!(
        status(&it.trellis, "c_copy").await.status,
        TransformStatus::Live
    );

    it.admin
        .batch_execute(
            "alter table public.c_copy enable row level security; \
             create policy app_only on public.c_copy to public using (false);",
        )
        .await
        .expect("enable RLS on a target the Trellis role owns");
    capture_pass(&mut it.raw, &it.pool).await;
    let reported = status(&it.trellis, "c_copy").await;
    assert_eq!(
        reported.status,
        TransformStatus::Live,
        "routine: nothing fires"
    );
    assert_eq!(reported.capture_failure, None);
    assert!(
        matches!(
            self_check(&it.trellis, "c_copy").await,
            SelfCheckOutcome::Converged
        ),
        "the owner's writes and reads aren't filtered"
    );

    let mut expected = RowSecurity {
        table: "public.c_copy".to_string(),
        role: "rls_trellis".to_string(),
        owner_forced: true,
        target: true,
    };
    it.admin
        .batch_execute("alter table public.c_copy force row level security")
        .await
        .expect("force RLS");
    match self_check(&it.trellis, "c_copy").await {
        SelfCheckOutcome::Diverged(divergences) => assert_eq!(
            divergences,
            vec![Divergence::Capture(CaptureFault::RowSecurity(
                expected.clone()
            ))]
        ),
        other => panic!("expected the capture audit's fault, got {other:?}"),
    }

    it.admin
        .batch_execute(
            "alter table public.c_copy no force row level security; \
             alter table public.c_copy owner to postgres; \
             grant select, insert, update, delete on public.c_copy to rls_trellis;",
        )
        .await
        .expect("hand the target to another owner");
    expected.owner_forced = false;
    match self_check(&it.trellis, "c_copy").await {
        SelfCheckOutcome::Diverged(divergences) => assert_eq!(
            divergences,
            vec![Divergence::Capture(CaptureFault::RowSecurity(
                expected.clone()
            ))]
        ),
        other => panic!("expected the capture audit's fault, got {other:?}"),
    }
    capture_pass(&mut it.raw, &it.pool).await;
    let reported = status(&it.trellis, "c_copy").await;
    assert_eq!(reported.status, TransformStatus::Paused);
    let failure = reported.capture_failure.expect("the reason is reported");
    assert_eq!(failure.source_table, "public.c_copy");
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

    it.admin
        .batch_execute("alter table public.c_copy owner to rls_trellis")
        .await
        .expect("give the target back");
    it.trellis
        .apply("RESUME TRANSFORM c_copy")
        .await
        .expect("resume");
    capture_pass(&mut it.raw, &it.pool).await;
    harness::settle_builds(&it.pool).await;
    let reported = status(&it.trellis, "c_copy").await;
    assert_eq!(reported.status, TransformStatus::Live);
    assert_eq!(reported.capture_failure, None);
    let copied: i64 = it
        .admin
        .query_one("select count(*) from public.c_copy", &[])
        .await
        .expect("read the target")
        .get(0);
    assert_eq!(copied, 6, "the rebuild wrote every row");
    it.trellis.shutdown().await.expect("shutdown");
}

/// A target belongs to the role that defined it. When the workers log in as
/// a member of the ring's owner and define as that member too, the ring's
/// owner owns nothing of the target, so enabling RLS on it (for the
/// application's readers) applies the policies to the ring's owner. That
/// isn't a writer, so nothing fires: the capture pass, run as the
/// member, leaves the definition `live`, and `self_check` converges.
#[tokio::test]
async fn a_target_its_writer_owns_is_not_checked_for_the_ring_owner() {
    let cluster = TestCluster::start();
    let it = instance(&cluster).await;
    it.admin
        .batch_execute("create role rls_worker login in role rls_trellis")
        .await
        .expect("a login role that is a member of the ring's owner");
    let dsn = it._db.dsn().replace("user=postgres", "user=rls_worker");
    let config = trellis::Config::with_schema(dsn.clone(), SCHEMA).expect("valid config");
    let pool = trellis::Pool::new(&config).expect("pool");
    let mut raw = connect(&dsn).await;
    raw.batch_execute(&format!("set search_path to {SCHEMA}, public"))
        .await
        .expect("set search_path");
    let worker = trellis::Trellis::connect(config, trellis::TrellisOptions::default())
        .await
        .expect("connect as the member");
    worker
        .apply("TRANSFORM c_copy FROM public.c SELECT amount AS amount")
        .await
        .expect("define as the member");
    capture_pass(&mut raw, &pool).await;
    harness::settle_builds(&pool).await;
    assert_eq!(
        status(&worker, "c_copy").await.status,
        TransformStatus::Live
    );
    let owner: String = it
        .admin
        .query_one(
            "select pg_get_userbyid(relowner)::text from pg_class \
             where oid = 'public.c_copy'::regclass",
            &[],
        )
        .await
        .expect("the target's owner")
        .get(0);
    assert_eq!(owner, "rls_worker");

    it.admin
        .batch_execute(
            "alter table public.c_copy enable row level security; \
             create policy app_only on public.c_copy to public using (false);",
        )
        .await
        .expect("enable RLS on the target");
    let as_reader = applying(&raw, SCHEMA, "public.c_copy", Readers::RingAndSession)
        .await
        .expect("check RLS")
        .expect("the ring's owner doesn't own the target");
    assert_eq!(as_reader.role, "rls_trellis");
    assert_eq!(
        applying(&raw, SCHEMA, "public.c_copy", Readers::Target)
            .await
            .expect("check RLS"),
        None,
        "its writer owns it"
    );
    capture_pass(&mut raw, &pool).await;
    let reported = status(&worker, "c_copy").await;
    assert_eq!(reported.status, TransformStatus::Live);
    assert_eq!(reported.capture_failure, None);
    assert!(
        matches!(
            self_check(&worker, "c_copy").await,
            SelfCheckOutcome::Converged
        ),
        "nothing to report"
    );
    worker.shutdown().await.expect("shutdown");
    it.trellis.shutdown().await.expect("shutdown");
}

/// A table the target-mutation seam feeds (another definition's target) is
/// read only by plain SQL on worker connections, as their login role: no
/// capture function is installed on it, so the ring's owner never reads it.
/// When the workers log in as a member of the ring's owner and define as
/// that member, the member owns the upstream's target and the ring's owner
/// owns nothing of it, so enabling RLS on it for the application's readers
/// applies the policies to the ring's owner alone. That isn't a reader, so
/// nothing fires for a definition sourced from the target: defining one is
/// accepted, the capture pass leaves it `live`, and `self_check`'s capture
/// audit reports nothing. Forcing RLS, which applies to the member too,
/// still pauses it.
#[tokio::test]
async fn a_seam_fed_table_its_readers_own_is_not_checked_for_the_ring_owner() {
    let cluster = TestCluster::start();
    let it = instance(&cluster).await;
    it.admin
        .batch_execute("create role rls_worker login in role rls_trellis")
        .await
        .expect("a login role that is a member of the ring's owner");
    let dsn = it._db.dsn().replace("user=postgres", "user=rls_worker");
    let config = trellis::Config::with_schema(dsn.clone(), SCHEMA).expect("valid config");
    let pool = trellis::Pool::new(&config).expect("pool");
    let mut raw = connect(&dsn).await;
    raw.batch_execute(&format!("set search_path to {SCHEMA}, public"))
        .await
        .expect("set search_path");
    let worker = trellis::Trellis::connect(config, trellis::TrellisOptions::default())
        .await
        .expect("connect as the member");
    worker
        .apply("TRANSFORM c_copy FROM public.c SELECT amount AS amount")
        .await
        .expect("define the upstream as the member");
    capture_pass(&mut raw, &pool).await;
    harness::settle_builds(&pool).await;
    worker
        .apply("TRANSFORM c_again FROM public.c_copy SELECT amount AS amount")
        .await
        .expect("define one sourced from the target");
    capture_pass(&mut raw, &pool).await;
    harness::settle_builds(&pool).await;
    for target in ["c_copy", "c_again"] {
        assert_eq!(
            status(&worker, target).await.status,
            TransformStatus::Live,
            "{target}"
        );
    }

    it.admin
        .batch_execute(
            "alter table public.c_copy enable row level security; \
             create policy app_only on public.c_copy to public using (false);",
        )
        .await
        .expect("enable RLS on the upstream's target");
    assert_eq!(
        applying(&raw, SCHEMA, "public.c_copy", Readers::Ring)
            .await
            .expect("check RLS")
            .map(|rls| rls.role),
        Some("rls_trellis".to_string()),
        "the ring's owner doesn't own the target"
    );
    assert_eq!(
        applying(&raw, SCHEMA, "public.c_copy", Readers::Session)
            .await
            .expect("check RLS"),
        None,
        "its readers own it"
    );
    worker
        .apply("TRANSFORM c_more FROM public.c_copy SELECT amount AS amount")
        .await
        .expect("a source the seam feeds isn't checked for the ring's owner");
    capture_pass(&mut raw, &pool).await;
    harness::settle_builds(&pool).await;
    for target in ["c_copy", "c_again", "c_more"] {
        let reported = status(&worker, target).await;
        assert_eq!(reported.status, TransformStatus::Live, "{target}");
        assert_eq!(reported.capture_failure, None, "{target}");
    }
    // `self_check`'s capture audit, as the member: nothing to report.
    for target in ["c_again", "c_more"] {
        let def = trellis::defs::catalog::definition_by_target(&pool, target)
            .await
            .expect("read the definition")
            .expect("registered");
        let faults = trellis::staging::capture_audit::audit(&raw, SCHEMA, &def)
            .await
            .expect("audit");
        assert_eq!(faults, [], "{target}");
    }

    it.admin
        .batch_execute("alter table public.c_copy force row level security")
        .await
        .expect("force RLS on the upstream's target");
    capture_pass(&mut raw, &pool).await;
    for target in ["c_again", "c_more"] {
        let reported = status(&worker, target).await;
        assert_eq!(reported.status, TransformStatus::Paused, "{target}");
        let failure = reported.capture_failure.expect("the reason is reported");
        assert_eq!(failure.source_table, "public.c_copy", "{target}");
        assert!(failure.error.contains("rls_worker"), "{failure:?}");
        assert!(
            failure.error.contains("Trellis's reads of it"),
            "{failure:?}"
        );
    }
    worker.shutdown().await.expect("shutdown");
    it.trellis.shutdown().await.expect("shutdown");
}

/// A pool as `role`, for the drains and builds after the fix.
fn pool_as(it: &Instance, role: &str) -> trellis::Pool {
    let dsn = it
        ._db
        .dsn()
        .replace("user=postgres", &format!("user={role}"));
    let config = trellis::Config::with_schema(dsn, SCHEMA).expect("valid config");
    trellis::Pool::new(&config).expect("pool")
}

/// The halt record `target` is paused with, after a refused read of its source.
async fn halted(trellis: &trellis::Trellis, target: &str) -> trellis::CaptureFailure {
    let reported = status(trellis, target).await;
    assert_eq!(reported.status, TransformStatus::Paused, "{target}");
    let failure = reported
        .capture_failure
        .unwrap_or_else(|| panic!("{target} carries the halt's record"));
    assert_eq!(failure.kind, trellis::CaptureFailureKind::Halt, "{target}");
    failure
}

/// Every staged change to `public.c`, as (op, key), in staging order.
async fn ring_of_c(admin: &Client) -> Vec<(String, String)> {
    let segments = (0..trellis::staging::append::RING_SIZE)
        .map(|n| format!("select src_table, op, key, change_id from {SCHEMA}.seg_{n}"))
        .collect::<Vec<_>>()
        .join(" union all ");
    admin
        .query(
            &format!(
                "select op, key from ({segments}) ring where src_table = 'public.c' \
                 order by change_id"
            ),
            &[],
        )
        .await
        .expect("read the ring")
        .into_iter()
        .map(|row| (row.get(0), row.get(1)))
        .collect()
}

/// `c`'s rows and its 1-1 copy's, and its per-`pid` sums and counts and its
/// aggregate's, read as the superuser.
async fn source_and_targets(admin: &Client) -> [Vec<String>; 4] {
    let mut out: [Vec<String>; 4] = Default::default();
    for (slot, sql) in [
        "select id || ':' || amount from public.c order by id",
        "select id || ':' || amount from public.c_copy order by id",
        "select pid || ':' || sum(amount) || ':' || count(*) from public.c \
         group by pid order by pid",
        "select pid || ':' || total || ':' || n from public.c_totals order by pid",
    ]
    .into_iter()
    .enumerate()
    {
        out[slot] = admin
            .query(sql, &[])
            .await
            .expect(sql)
            .into_iter()
            .map(|row| row.get(0))
            .collect();
    }
    out
}

/// Capture's conditional live re-read runs as the ring's owner
/// under the writing session's `row_security`. Defines a 1-1 copy and an
/// aggregate of `c`, forces a policy hiding every row of `c` from the
/// ring's owner, then writes as the superuser (exempt, `row_security` on)
/// through two application triggers whose nested writes send the outer
/// statement's capture down the re-read: updating row 1 updates row 3, and
/// inserting row 100 rewrites it. The re-read finds neither row 1 nor row
/// 100, so the capture stages a delete for each, though both still exist.
/// Returns the instance with those changes staged and not yet drained.
async fn deletes_staged_for_rows_the_policy_hides(cluster: &TestCluster) -> Instance {
    let mut it = instance(cluster).await;
    for ddl in [
        "TRANSFORM c_copy FROM public.c SELECT amount AS amount",
        "TRANSFORM c_totals FROM public.c GROUP BY pid \
        WITHOUT SOURCE INDEX SELECT pid AS pid, SUM(amount) AS total, COUNT(*) AS n",
    ] {
        it.trellis.apply(ddl).await.expect(ddl);
    }
    capture_pass(&mut it.raw, &it.pool).await;
    harness::settle_builds(&it.pool).await;
    for target in ["c_copy", "c_totals"] {
        assert_eq!(
            status(&it.trellis, target).await.status,
            TransformStatus::Live,
            "{target}"
        );
    }
    it.admin
        .batch_execute(
            "create function public.app_touch() returns trigger language plpgsql as $$ \
             begin update public.c set amount = new.amount * 10 where id = new.id; \
             return null; end $$; \
             create trigger app_touch after insert on public.c \
               for each row execute function public.app_touch(); \
             create function public.app_other() returns trigger language plpgsql as $$ \
             begin if pg_catalog.pg_trigger_depth() < 2 and new.id = 1 then \
               update public.c set amount = amount + 1000 where id = 3; end if; \
             return null; end $$; \
             create trigger app_other after update on public.c \
               for each row execute function public.app_other(); \
             alter table public.c enable row level security, force row level security; \
             create policy hide_all on public.c to public using (false);",
        )
        .await
        .expect("application triggers, and policies that apply to the ring's owner");
    it.admin
        .batch_execute(
            "update public.c set amount = 22 where id = 2; \
             update public.c set amount = 11 where id = 1; \
             insert into public.c values (100, 1, 7);",
        )
        .await
        .expect("the application's writes succeed");

    let staged = [
        ("update", "2"),
        ("update", "3"),
        ("delete", "1"),
        ("update", "100"),
        ("delete", "100"),
    ]
    .map(|(op, key)| (op.to_string(), key.to_string()));
    assert_eq!(
        ring_of_c(&it.admin).await,
        staged,
        "the plain update and the nested writes stage their rows; the re-reads stage deletes"
    );
    let still_there: i64 = it
        .admin
        .query_one("select count(*) from public.c where id in (1, 100)", &[])
        .await
        .expect("read the source")
        .get(0);
    assert_eq!(still_there, 2, "rows 1 and 100 still exist");
    it
}

/// A delete capture staged for a row a policy hid from its re-read (above)
/// shows in no target when the drain's role is exempt: a 1-1 apply and an
/// aggregate apply re-read a delete's row live as the drain's role,
/// find it, and both targets match the source. The next capture pass then
/// pauses both readers for the row-level security.
#[tokio::test]
async fn a_delete_staged_for_a_row_the_policy_hides_is_repaired_by_an_exempt_drain() {
    let cluster = TestCluster::start();
    let mut it = deletes_staged_for_rows_the_policy_hides(&cluster).await;

    let exempt = pool_as(&it, "postgres");
    seal_and_drain(&mut it.raw, &exempt)
        .await
        .expect("a drain as the superuser, which is exempt, passes");
    let [source, copy, totals, aggregate] = source_and_targets(&it.admin).await;
    assert_eq!(copy, source, "the 1-1 target matches the source");
    assert_eq!(aggregate, totals, "the aggregate matches the source");
    for target in ["c_copy", "c_totals"] {
        assert_eq!(
            status(&it.trellis, target).await.status,
            TransformStatus::Live,
            "{target}: the drain halted nothing"
        );
    }

    capture_pass(&mut it.raw, &it.pool).await;
    let expected = RowSecurity {
        table: "public.c".to_string(),
        role: "rls_trellis".to_string(),
        owner_forced: true,
        target: false,
    };
    for target in ["c_copy", "c_totals"] {
        let reported = status(&it.trellis, target).await;
        assert_eq!(reported.status, TransformStatus::Paused, "{target}");
        let failure = reported
            .capture_failure
            .unwrap_or_else(|| panic!("{target} carries the reason"));
        assert_eq!(failure.kind, trellis::CaptureFailureKind::Capture);
        assert_eq!(failure.source_table, "public.c");
        assert!(
            failure.error.starts_with(&expected.to_string()),
            "{target}: {failure:?}"
        );
    }
    it.trellis.shutdown().await.expect("shutdown");
}

/// The same staged deletes drained as the ring's owner, which the policy
/// applies to: the drain's live re-read of `c` is refused, and it halts both
/// readers instead of applying the deletes.
#[tokio::test]
async fn a_delete_staged_for_a_row_the_policy_hides_halts_a_drain_the_policy_applies_to() {
    let cluster = TestCluster::start();
    let mut it = deletes_staged_for_rows_the_policy_hides(&cluster).await;
    let [_, copy_before, _, aggregate_before] = source_and_targets(&it.admin).await;

    seal_and_drain(&mut it.raw, &it.pool)
        .await
        .expect("the drain halts the readers and commits the page");
    for target in ["c_copy", "c_totals"] {
        let failure = halted(&it.trellis, target).await;
        assert!(
            failure
                .error
                .contains("row-level security on public.c applies to role rls_trellis"),
            "{target}: {}",
            failure.error
        );
    }
    let [_, copy, _, aggregate] = source_and_targets(&it.admin).await;
    assert_eq!(copy, copy_before, "the 1-1 target applied nothing");
    assert_eq!(aggregate, aggregate_before, "the aggregate applied nothing");
    assert_eq!(poison_rows(&it.admin).await, 0);
    it.trellis.shutdown().await.expect("shutdown");
}
