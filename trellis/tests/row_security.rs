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
