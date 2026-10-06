//! Row-level security that applies to the Trellis role isn't supported, and
//! is detected (issue #745, `trellis::defs::row_security`).
//!
//! The first two tests check the catalog predicate against Postgres itself:
//! for every combination of table setting and role, the check must agree
//! with `row_security_active()` and with what a read actually sees. The rest
//! run Trellis as a non-superuser login role (a superuser bypasses RLS, and a
//! test cluster connects as one): defining refuses a table whose policies
//! apply, and the staging worker's capture pass pauses the readers of one
//! whose policies apply after define, which `self_check` reports too.
//!
//! Nothing polls for convergence (#297): every pass is stepped by hand.

use std::time::{Duration, Instant};

use testkit::TestCluster;
use tokio_postgres::{Client, NoTls};
use trellis::defs::TransformStatus;
use trellis::defs::row_security::{Readers, RowSecurity, applying};
use trellis::intake::markers;
use trellis::staging::{StagedWatermark, apply, seal};
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
            // No ring here, so all three check the session's role alone
            // (#765).
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
/// the captured table (#623 D8a), so the policies applying to that role
/// count even when the session's role is exempt. When both are subject, the
/// session's role is the one reported. [`Readers::Ring`], define's check,
/// asks about the ring's owner alone: the session's role may be a process's
/// that only defines transforms and never reads the table.
/// [`Readers::Session`] and [`Readers::Target`] ask about the session's role
/// alone, which reads a seam-fed table and writes a target (#765).
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
             reader, or a target's writer (#765)"
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

/// Defining refuses a source, or a relationship's to-side the definition
/// reads through, whose policies apply to the Trellis role. Enabling RLS on
/// a table the role owns, for the application's other roles, is routine and
/// refuses nothing.
#[tokio::test]
async fn defining_refuses_a_table_whose_policies_apply_to_the_trellis_role() {
    let cluster = TestCluster::start();
    let it = instance(&cluster).await;
    it.trellis
        .apply("RELATIONSHIP parent FROM c.pid TO p.id")
        .await
        .expect("declare a relationship");

    it.admin
        .batch_execute(
            "alter table public.c enable row level security; \
             alter table public.p enable row level security; \
             create policy app_only on public.c to public using (false);",
        )
        .await
        .expect("enable RLS on tables the Trellis role owns");
    it.trellis
        .apply("TRANSFORM c_named FROM public.c SELECT amount AS amount, parent.name AS name")
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
        .expect("hand the to-side to another owner");
    let rls = refused_for_row_security(
        it.trellis
            .apply("TRANSFORM c_tier FROM public.c SELECT amount AS amount, parent.name AS name")
            .await,
    );
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
        .expect("a definition that doesn't read the to-side is accepted");

    it.admin
        .batch_execute("alter role rls_trellis bypassrls")
        .await
        .expect("exempt the role");
    it.trellis
        .apply("TRANSFORM c_tier FROM public.c SELECT amount AS amount, parent.name AS name")
        .await
        .expect("a role with BYPASSRLS is exempt");
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
    markers::settle_registrations(&it.pool).await;
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

/// Registration reads no source rows, with one exception: it seeds and
/// widens a to-one relationship's settled projection from the to-side as the
/// session's role, and a build reads the projection. So defining refuses a
/// to-one to-side whose policies apply to the session's role, even when the
/// ring's owner is exempt. Here the ring's owner has `BYPASSRLS`, which a
/// login role that is a member of it doesn't inherit.
///
/// Declaring the to-one relationship seeds that projection too, so it is
/// refused the same way: the session runs with `row_security = off` (issue
/// #766), and the seed would otherwise fail on Postgres's bare error.
#[tokio::test]
async fn defining_refuses_a_to_one_to_side_whose_policies_apply_to_the_session_role() {
    let cluster = TestCluster::start();
    let it = instance(&cluster).await;
    it.admin
        .batch_execute(
            "create role rls_definer login in role rls_trellis; \
             alter role rls_trellis bypassrls; \
             alter table public.p enable row level security, force row level security; \
             create policy hide_two on public.p using (id <> 2);",
        )
        .await
        .expect("a definer role the to-side's policies apply to");
    let dsn = it._db.dsn().replace("user=postgres", "user=rls_definer");
    let config = trellis::Config::with_schema(dsn, SCHEMA).expect("valid config");
    let definer = trellis::Trellis::connect(config, trellis::TrellisOptions::default())
        .await
        .expect("connect as the definer");
    let rls = refused_for_row_security(
        definer
            .apply("RELATIONSHIP parent FROM c.pid TO p.id")
            .await,
    );
    assert_eq!(
        rls,
        RowSecurity {
            table: "public.p".to_string(),
            role: "rls_definer".to_string(),
            owner_forced: true,
            target: false,
        }
    );
    it.trellis
        .apply("RELATIONSHIP parent FROM c.pid TO p.id")
        .await
        .expect("the ring's owner, with BYPASSRLS, declares it");
    let rls = refused_for_row_security(
        definer
            .apply("TRANSFORM c_named FROM public.c SELECT amount AS amount, parent.name AS name")
            .await,
    );
    assert_eq!(
        rls,
        RowSecurity {
            table: "public.p".to_string(),
            role: "rls_definer".to_string(),
            owner_forced: true,
            target: false,
        }
    );
    definer
        .apply("TRANSFORM c_copy FROM public.c SELECT amount AS amount")
        .await
        .expect("a definition that doesn't read the to-side is accepted");

    it.admin
        .batch_execute("alter role rls_definer bypassrls")
        .await
        .expect("exempt the definer");
    definer
        .apply("TRANSFORM c_named FROM public.c SELECT amount AS amount, parent.name AS name")
        .await
        .expect("accepted once the definer is exempt");
    let projection: String = it
        .admin
        .query_one(
            &format!("select projection_table from {SCHEMA}.relationship_projections"),
            &[],
        )
        .await
        .expect("the relationship's projection")
        .get(0);
    let seeded: Vec<(i32, Option<String>)> = it
        .admin
        .query(
            &format!("select id, name from {SCHEMA}.{projection} order by id"),
            &[],
        )
        .await
        .expect("read the projection")
        .into_iter()
        .map(|row| (row.get(0), row.get(1)))
        .collect();
    assert_eq!(
        seeded,
        (1..=3)
            .map(|i| (i, Some(format!("n{i}"))))
            .collect::<Vec<_>>(),
        "every to-side row seeds the projection, the definer's hidden one included"
    );
    definer.shutdown().await.expect("shutdown");
    it.trellis.shutdown().await.expect("shutdown");
}

/// A table another definition targets isn't captured (the target-mutation
/// seam feeds it), but its readers read it as the Trellis role all the same,
/// so the capture pass checks it too: RLS forced on an upstream target pauses
/// the definition sourced from it, for its reads. It pauses the upstream as
/// well, for its writes to the target (#765).
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
    // #765: the upstream writes the forced target as the same role, so it
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
    let watermark = StagedWatermark::saturated();
    for _ in 0..16 {
        match apply::drain_once(
            pool,
            sealed.sealed_seg_seq,
            "row_security_test",
            1,
            "trellis_row_security_test",
            &watermark,
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

/// Which role writes a target (#765), and what happens when row-level
/// security applies to a role the catalog checks never look at (#766).
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
    markers::settle_registrations(&it.pool).await;
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

/// The read half of the drain test above (#766): a relationship's to-side
/// whose policies apply to the drain's login role, but not to the ring's
/// owner the catalog checks look at. The drain's read of it raises, and the
/// drain pauses the definition that reads through the relationship, naming
/// the to-side, instead of reading the hidden parent as absent.
#[tokio::test]
async fn a_drain_as_an_unchecked_role_pauses_the_reader_of_a_hidden_to_side() {
    let cluster = TestCluster::start();
    let mut it = instance(&cluster).await;
    it.trellis
        .apply("RELATIONSHIP parent FROM c.pid TO p.id")
        .await
        .expect("declare a relationship");
    it.trellis
        .apply("TRANSFORM c_named FROM public.c SELECT amount AS amount, parent.name AS name")
        .await
        .expect("define");
    capture_pass(&mut it.raw, &it.pool).await;
    markers::settle_registrations(&it.pool).await;
    assert_eq!(
        status(&it.trellis, "c_named").await.status,
        TransformStatus::Live
    );
    it.admin
        .batch_execute(
            "alter role rls_trellis bypassrls; \
             create role rls_worker login in role rls_trellis; \
             alter table public.p enable row level security, force row level security; \
             create policy hide_two on public.p using (id <> 2);",
        )
        .await
        .expect("a member login role the to-side's policies apply to");
    let dsn = it._db.dsn().replace("user=postgres", "user=rls_worker");
    let config = trellis::Config::with_schema(dsn, SCHEMA).expect("valid config");
    let worker = trellis::Pool::new(&config).expect("pool");
    capture_pass(&mut it.raw, &it.pool).await;
    assert_eq!(
        status(&it.trellis, "c_named").await.status,
        TransformStatus::Live,
        "the capture pass, as the ring's owner, sees nothing to pause"
    );

    it.admin
        .batch_execute("update public.p set name = 'renamed' where id = 2")
        .await
        .expect("write the to-side");
    seal_and_drain(&mut it.raw, &worker)
        .await
        .expect("the member's drain pauses the reader and commits the rest of the page");
    let paused = status(&it.trellis, "c_named").await;
    assert_eq!(paused.status, TransformStatus::Paused);
    let failure = paused.capture_failure.expect("the halt's record");
    assert_eq!(failure.kind, trellis::CaptureFailureKind::Halt);
    assert_eq!(failure.source_table, "public.p");
    assert!(
        failure
            .error
            .contains("row-level security on public.p applies to role rls_worker"),
        "{}",
        failure.error
    );
    assert_eq!(poison_rows(&it.admin).await, 0);
    it.trellis.shutdown().await.expect("shutdown");
}

/// A refused read beside a key failure (#766 with #799). The page holds a
/// change to a to-side whose policies apply to the drain's role, and a change
/// to key 1 of `c` that one of `c`'s three readers, `c_cheap`, fails to write
/// (its target refuses the amount). The refusal halts the definition reading
/// through the relationship, charging no key, and the page's retries skip the
/// refused table. So do isolation's probes: a bisection probe holding the
/// to-side's change would otherwise read the refused table again, halt on its
/// `42501` and leave key 1 uncharged on every drain. (Attribution's probes
/// skip the same way, but hold only key 1's record, which reads nothing
/// refused.) Key 1 is charged to `c_cheap` alone and, at the death threshold,
/// held for it, while `c_copy` applies it.
#[tokio::test]
async fn a_refused_read_halts_while_a_key_failure_beside_it_is_held_for_its_definition() {
    let cluster = TestCluster::start();
    let mut it = instance(&cluster).await;
    it.trellis
        .apply("RELATIONSHIP parent FROM c.pid TO p.id")
        .await
        .expect("declare a relationship");
    for ddl in [
        "TRANSFORM c_named FROM public.c SELECT amount AS amount, parent.name AS name",
        "TRANSFORM c_cheap FROM public.c SELECT amount AS amount",
        "TRANSFORM c_copy FROM public.c SELECT amount AS amount",
    ] {
        it.trellis.apply(ddl).await.expect(ddl);
    }
    capture_pass(&mut it.raw, &it.pool).await;
    markers::settle_registrations(&it.pool).await;
    for target in ["c_named", "c_cheap", "c_copy"] {
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
        .expect("a member login role the to-side's policies apply to, and a narrow target");
    let dsn = it._db.dsn().replace("user=postgres", "user=rls_worker");
    let config = trellis::Config::with_schema(dsn, SCHEMA).expect("valid config");
    let worker = trellis::Pool::new(&config).expect("pool");

    it.admin
        .batch_execute(
            "update public.p set name = 'renamed' where id = 2; \
             update public.c set amount = 500 where id = 1;",
        )
        .await
        .expect("write the to-side and the source");
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
            &StagedWatermark::saturated(),
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

    let halted = status(&it.trellis, "c_named").await;
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
/// forces RLS on each new table (#765). One that only enables it, a common
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

/// RLS on a live definition's target after define (#765). Enabling it on a
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
    markers::settle_registrations(&it.pool).await;
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
    markers::settle_registrations(&it.pool).await;
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
/// isn't a writer, so nothing fires (#765): the capture pass, run as the
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
    markers::settle_registrations(&pool).await;
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
/// nothing fires, for a definition sourced from the target or one reading it
/// as a relationship's to-side: defining either is accepted, the capture
/// pass leaves them `live`, and `self_check`'s capture audit reports
/// nothing. Forcing RLS, which applies to the member too, still pauses them.
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
    markers::settle_registrations(&pool).await;
    worker
        .apply("RELATIONSHIP mirror FROM p.id TO c_copy.id")
        .await
        .expect("declare a relationship to the upstream's target");
    worker
        .apply("TRANSFORM c_again FROM public.c_copy SELECT amount AS amount")
        .await
        .expect("define one sourced from the target");
    worker
        .apply("TRANSFORM c_mirror FROM public.p SELECT mirror.amount AS mirrored")
        .await
        .expect("define one reading the target as a to-side");
    capture_pass(&mut raw, &pool).await;
    markers::settle_registrations(&pool).await;
    for target in ["c_copy", "c_again", "c_mirror"] {
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
    worker
        .apply("TRANSFORM c_mirror2 FROM public.p SELECT mirror.amount AS mirrored")
        .await
        .expect("nor is a to-side the seam feeds");
    capture_pass(&mut raw, &pool).await;
    markers::settle_registrations(&pool).await;
    for target in ["c_copy", "c_again", "c_mirror", "c_more", "c_mirror2"] {
        let reported = status(&worker, target).await;
        assert_eq!(reported.status, TransformStatus::Live, "{target}");
        assert_eq!(reported.capture_failure, None, "{target}");
    }
    // `self_check`'s capture audit, as the member: nothing to report.
    for target in ["c_again", "c_mirror"] {
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
    for target in ["c_again", "c_mirror"] {
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

/// Once every definition reading a to-side through a relationship is paused
/// for row-level security that applies to the drain's role, the drain still
/// keeps the relationship's settled projection current from the to-side's
/// changes, and every Trellis session runs with `row_security = off` (#766),
/// so that read is refused. The drain skips the table instead, as it does a
/// table whose key can't be used (#768), so the page commits rather than
/// failing forever, and nothing is charged to the quarantine. A resume
/// refreshes the projection, so the rebuilt target reads the change the
/// skip left out.
#[tokio::test]
async fn a_to_side_whose_readers_are_paused_for_row_security_drains_past_the_refusal() {
    let cluster = TestCluster::start();
    let mut it = instance(&cluster).await;
    it.trellis
        .apply("RELATIONSHIP parent FROM c.pid TO p.id")
        .await
        .expect("declare a relationship");
    it.trellis
        .apply("TRANSFORM c_named FROM public.c SELECT amount AS amount, parent.name AS name")
        .await
        .expect("define");
    capture_pass(&mut it.raw, &it.pool).await;
    markers::settle_registrations(&it.pool).await;
    it.admin
        .batch_execute(
            "alter table public.p enable row level security, force row level security; \
             create policy hide_two on public.p using (id <> 2);",
        )
        .await
        .expect("force RLS on the to-side");
    capture_pass(&mut it.raw, &it.pool).await;
    let paused = status(&it.trellis, "c_named").await;
    assert_eq!(paused.status, TransformStatus::Paused);
    assert_eq!(
        paused.capture_failure.expect("the pass's record").kind,
        trellis::CaptureFailureKind::Capture
    );

    it.admin
        .batch_execute("update public.p set name = 'renamed' where id = 2")
        .await
        .expect("write the to-side");
    seal_and_drain(&mut it.raw, &it.pool)
        .await
        .expect("the drain skips the refused to-side and commits the page");
    assert_eq!(poison_rows(&it.admin).await, 0);

    it.admin
        .batch_execute("alter role rls_trellis bypassrls")
        .await
        .expect("exempt the role");
    it.trellis
        .apply("RESUME TRANSFORM c_named")
        .await
        .expect("resume");
    capture_pass(&mut it.raw, &it.pool).await;
    markers::settle_registrations(&it.pool).await;
    assert_eq!(
        status(&it.trellis, "c_named").await.status,
        TransformStatus::Live
    );
    seal_and_drain(&mut it.raw, &it.pool)
        .await
        .expect("the drain applies the rebuild");
    let name: Option<String> = it
        .admin
        .query_one("select name from public.c_named where id = 1", &[])
        .await
        .expect("read the target")
        .get(0);
    assert_eq!(
        name.as_deref(),
        Some("renamed"),
        "the rebuild read the change"
    );
    it.trellis.shutdown().await.expect("shutdown");
}

/// The relationship's settled projection on `to_table`'s `label` for `id`,
/// read as the superuser.
async fn projected_label(admin: &Client, relationship: &str, id: i32) -> Option<String> {
    let projection: String = admin
        .query_one(
            &format!(
                "select rp.projection_table from {SCHEMA}.relationship_projections rp \
                 join {SCHEMA}.relationship_definitions rd on rd.id = rp.relationship_id \
                 where rd.name = $1"
            ),
            &[&relationship],
        )
        .await
        .expect("the relationship has a projection")
        .get(0);
    admin
        .query_opt(
            &format!("select label from {SCHEMA}.{projection} where id = $1"),
            &[&id],
        )
        .await
        .expect("read the projection")
        .and_then(|row| row.get(0))
}

/// The refused page's retry (#766) skips every table no unfrozen definition
/// reads, not just the refused one: here a healthy to-side, `q`, sharing the
/// page, whose only reader is paused. Its change never reaches its
/// relationship's projection, as for a table skipped for its key (#768), so
/// the next definition to read through the relationship must refresh the
/// projection from the table, or it would go live on the stale label.
#[tokio::test]
async fn a_healthy_to_side_skipped_beside_a_refused_one_is_refreshed_by_the_next_reader() {
    let cluster = TestCluster::start();
    let mut it = instance(&cluster).await;
    it.admin
        .batch_execute(
            "create table public.q (id int primary key, label text); \
             create table public.d (id int primary key, qid int); \
             insert into public.q values (1, 'old'); \
             insert into public.d values (1, 1); \
             alter table public.q owner to rls_trellis; \
             alter table public.d owner to rls_trellis;",
        )
        .await
        .expect("a second relationship's tables");
    for statement in [
        "RELATIONSHIP parent FROM c.pid TO p.id",
        "TRANSFORM c_named FROM public.c SELECT amount AS amount, parent.name AS name",
        "RELATIONSHIP tag FROM d.qid TO q.id",
        "TRANSFORM d_tagged FROM public.d SELECT tag.label AS label",
    ] {
        it.trellis.apply(statement).await.expect(statement);
    }
    capture_pass(&mut it.raw, &it.pool).await;
    markers::settle_registrations(&it.pool).await;
    it.trellis
        .apply("PAUSE TRANSFORM d_tagged")
        .await
        .expect("pause q's only reader");
    it.admin
        .batch_execute(
            "alter table public.p enable row level security, force row level security; \
             create policy hide_two on public.p using (id <> 2);",
        )
        .await
        .expect("force RLS on the to-side");
    capture_pass(&mut it.raw, &it.pool).await;
    assert_eq!(
        status(&it.trellis, "c_named").await.status,
        TransformStatus::Paused
    );

    it.admin
        .batch_execute(
            "update public.p set name = 'renamed' where id = 2; \
             update public.q set label = 'new' where id = 1;",
        )
        .await
        .expect("write both to-sides into one page");
    seal_and_drain(&mut it.raw, &it.pool)
        .await
        .expect("the retry skips both to-sides and commits the page");
    assert_eq!(
        projected_label(&it.admin, "tag", 1).await.as_deref(),
        Some("old"),
        "q's change was skipped with the refused table"
    );

    it.trellis
        .apply("TRANSFORM d_relabelled FROM public.d SELECT tag.label AS label")
        .await
        .expect("a new reader of the relationship");
    assert_eq!(
        projected_label(&it.admin, "tag", 1).await.as_deref(),
        Some("new"),
        "the define refreshed the projection the skip left stale"
    );
    capture_pass(&mut it.raw, &it.pool).await;
    markers::settle_registrations(&it.pool).await;
    seal_and_drain(&mut it.raw, &it.pool)
        .await
        .expect("drain after the define");
    let label: Option<String> = it
        .admin
        .query_one("select label from public.d_relabelled where id = 1", &[])
        .await
        .expect("read the new reader's target")
        .get(0);
    assert_eq!(label.as_deref(), Some("new"));
    it.trellis.shutdown().await.expect("shutdown");
}

/// The from-side half of the skip (#766). A relationship's from-side, `c`,
/// has policies that apply to the drain's role, and its to-side, `p`, has a
/// live reader of its own, `p_copy`. An image-less change to `p` (a
/// `recompute`, as a backfill, a release or a propagation hop stages)
/// re-derives the `c` rows that read it, so the drain looks those rows up in
/// `c`, and that read is refused. The refusal halts `c_named`, the only
/// definition reading `c`, and the retry skips the lookup as it skips `c`'s
/// own changes, so the page commits and `p_copy` applies its share. Without
/// the skip the retry is refused again on every pass, and the ring stalls
/// behind the page.
#[tokio::test]
async fn a_to_side_recompute_skips_a_from_side_whose_readers_are_halted_for_row_security() {
    let cluster = TestCluster::start();
    let mut it = instance(&cluster).await;
    it.trellis
        .apply("RELATIONSHIP parent FROM c.pid TO p.id")
        .await
        .expect("declare a relationship");
    for ddl in [
        "TRANSFORM c_named FROM public.c SELECT amount AS amount, parent.name AS name",
        "TRANSFORM p_copy FROM public.p SELECT name AS name",
    ] {
        it.trellis.apply(ddl).await.expect(ddl);
    }
    capture_pass(&mut it.raw, &it.pool).await;
    markers::settle_registrations(&it.pool).await;
    seal_and_drain(&mut it.raw, &it.pool)
        .await
        .expect("the registrations drain before any policy applies");
    for target in ["c_named", "p_copy"] {
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
             alter table public.c enable row level security, force row level security; \
             create policy hide_two on public.c using (id <> 2); \
             set session_replication_role = replica; \
             update public.p set name = 'renamed' where id = 2; \
             reset session_replication_role;",
        )
        .await
        .expect("a member login role the from-side's policies apply to, and an uncaptured write");
    let dsn = it._db.dsn().replace("user=postgres", "user=rls_worker");
    let config = trellis::Config::with_schema(dsn, SCHEMA).expect("valid config");
    let worker = trellis::Pool::new(&config).expect("pool");

    let ring_slot: i16 = it
        .raw
        .query_one("select ring_slot from segment_pointer", &[])
        .await
        .expect("read segment_pointer")
        .get(0);
    it.raw
        .execute(
            &format!(
                "insert into seg_{ring_slot} (src_table, key, op, lsn, old_image, new_image, \
                 hop_gen) values ('public.p', '2', 'recompute', null, null, null, 0)"
            ),
            &[],
        )
        .await
        .expect("stage an image-less change to the to-side");
    seal_and_drain(&mut it.raw, &worker)
        .await
        .expect("the drain halts the from-side's reader, skips the lookup and commits");

    let halted = status(&it.trellis, "c_named").await;
    assert_eq!(halted.status, TransformStatus::Paused);
    let failure = halted.capture_failure.expect("the halt's record");
    assert_eq!(failure.kind, trellis::CaptureFailureKind::Halt);
    assert_eq!(failure.source_table, "public.c");
    assert_eq!(
        status(&it.trellis, "p_copy").await.status,
        TransformStatus::Live
    );
    let name: Option<String> = it
        .admin
        .query_one("select name from public.p_copy where id = 2", &[])
        .await
        .expect("read the target")
        .get(0);
    assert_eq!(name.as_deref(), Some("renamed"), "p_copy applied the page");
    assert_eq!(poison_rows(&it.admin).await, 0);
    it.trellis.shutdown().await.expect("shutdown");
}
