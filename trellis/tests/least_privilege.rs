//! The whole Trellis lifecycle as a login role holding only the privileges
//! `docs/recommendations.md` ("One Trellis role") lists (issue #833).
//!
//! Each privilege is checked piecewise elsewhere (`capture_install.rs`,
//! `self_check.rs`, `row_security.rs`, `staging::halt`). This test runs the
//! lifecycle through the real staging and drain workers as one such role,
//! so the list is shown sufficient: migrate, define, build, drain,
//! `self_check`, `ALTER TRANSFORM`, `PAUSE`/`RESUME`, a widened key's pause
//! and resume (#767), `DROP TRANSFORM` and defining again, `DROP
//! RELATIONSHIP`, and the staging worker removing capture.
//!
//! The roles, as the doc's example sets them up:
//!
//! - `app_owner`, `NOLOGIN`, owns the source tables, the relationship's
//!   to-side included;
//! - `trellis`, `LOGIN`, a member of `app_owner` (inherited), holding
//!   [`trellis_grants`] and nothing else;
//! - `app`, `LOGIN`, the application: it writes the sources, reads the
//!   targets through the doc's default privileges, and holds nothing on the
//!   instance schema.
//!
//! The database revokes what `PUBLIC` holds by default (`CONNECT` and
//! `TEMPORARY` on the database, `USAGE` on `public`), so each of those is a
//! grant of its own. Relationship endpoints resolve through the connection's
//! `search_path`, which Trellis pins to the instance schema, the target
//! schema and `public`, so the sources stay in `public`. Dropping any one of
//! [`trellis_grants`] makes a step fail; that was checked by hand for #833.
//!
//! It runs the real background pipeline, so it polls `status` the way an
//! embedder does (`docs/embedding.md`, "Poll to `live`, don't wait"),
//! bounded at 60s per wait. The pipeline is the [`trellis::Client`] that
//! `TrellisOptions { staging: true, drain_threads: 2 }` starts, but with a
//! 200ms reconcile pass instead of the 5s default: most steps wait on a pass
//! (a capture install, a discharge, a pause or resume landing), and at 5s
//! they made up most of the test's time. Some worker failures are only logged and
//! retried, so the test also records every warning or error event logged
//! anywhere in the process, and fails on any that reads as a privilege
//! refusal.

use std::time::{Duration, Instant};

use testkit::TestCluster;
use tokio_postgres::{Client, NoTls};
use trellis::{
    ClientOptions, Config, DefinitionStatus, SelfCheckOutcome, TransformStatus, Trellis,
    TrellisOptions,
};

const SCHEMA: &str = trellis::config::DEFAULT_SCHEMA;
const TARGETS: &str = "trellis_targets";
const WAIT: Duration = Duration::from_secs(60);

async fn connect(dsn: &str) -> Client {
    let (client, connection) = tokio_postgres::connect(dsn, NoTls).await.expect("connect");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
}

/// Every `WARN` or `ERROR` event logged on any thread, so a privilege
/// refusal a background worker logs and retries is still seen.
mod logged {
    use std::sync::{Arc, Mutex, OnceLock};

    use tracing::field::{Field, Visit};
    use tracing_subscriber::layer::{Context, Layer, SubscriberExt};

    struct Fields(String);

    impl Visit for Fields {
        fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
            self.0.push_str(&format!("{}={value:?} ", field.name()));
        }
    }

    struct Recorder(Arc<Mutex<Vec<String>>>);

    impl<S: tracing::Subscriber> Layer<S> for Recorder {
        fn on_event(&self, event: &tracing::Event<'_>, _ctx: Context<'_, S>) {
            if *event.metadata().level() <= tracing::Level::WARN {
                let mut fields = Fields(format!("{} ", event.metadata().target()));
                event.record(&mut fields);
                self.0.lock().unwrap().push(fields.0);
            }
        }
    }

    static EVENTS: OnceLock<Arc<Mutex<Vec<String>>>> = OnceLock::new();

    /// Installs the process-wide recorder once. This file is its own test
    /// binary, so nothing else competes for the global default.
    pub fn install() {
        EVENTS.get_or_init(|| {
            let events = Arc::new(Mutex::new(Vec::new()));
            let subscriber = tracing_subscriber::registry().with(Recorder(events.clone()));
            tracing::subscriber::set_global_default(subscriber).expect("install the recorder");
            events
        });
    }

    /// The recorded events that read as a privilege refusal.
    pub fn privilege_refusals() -> Vec<String> {
        EVENTS
            .get()
            .expect("installed")
            .lock()
            .unwrap()
            .iter()
            .filter(|event| {
                let event = event.to_lowercase();
                event.contains("permission denied")
                    || event.contains("must be owner")
                    // `SqlState(E42501)`, as an error's `Debug` spells it;
                    // a bare `42501` could be part of a pid or a count.
                    || event.contains("e42501")
                    || event.contains("insufficient privilege")
            })
            .cloned()
            .collect()
    }
}

fn assert_no_privilege_refusal(step: &str) {
    let refusals = logged::privilege_refusals();
    assert!(
        refusals.is_empty(),
        "{step}: a worker logged a privilege refusal:\n{}",
        refusals.join("\n")
    );
}

/// `dsn`, logging in as `role`.
fn as_role(dsn: &str, role: &str) -> String {
    assert!(dsn.contains("user=postgres"), "{dsn}");
    dsn.replace("user=postgres", &format!("user={role}"))
}

/// The documented grants on the `trellis` role, one statement each, so a
/// grant can be left out to see which step needs it.
fn trellis_grants(database: &str) -> Vec<String> {
    vec![
        format!("grant create on database \"{database}\" to trellis"),
        "grant app_owner to trellis".to_string(),
        format!("grant create on schema {TARGETS} to trellis"),
        format!("grant usage on schema {TARGETS} to trellis"),
        "grant usage on schema public to trellis".to_string(),
        format!("grant connect on database \"{database}\" to trellis"),
        format!("grant temporary on database \"{database}\" to trellis"),
    ]
}

/// The roles, the source tables and the grants, as
/// `docs/recommendations.md` sets them up.
async fn set_up(admin: &Client, database: &str) {
    admin
        .batch_execute(&format!(
            "create role app_owner nologin; \
             create role trellis login; \
             create role app login; \
             revoke all on database \"{database}\" from public; \
             grant connect on database \"{database}\" to app; \
             revoke all on schema public from public; \
             grant usage on schema public to app; \
             create schema {TARGETS}; \
             create table public.customers (id int primary key, name text); \
             create table public.orders (id int primary key, customer_id int, amount int); \
             insert into public.customers select i, 'c' || i from generate_series(1, 3) i; \
             insert into public.orders select i, 1 + i % 3, i * 10 from generate_series(1, 9) i; \
             alter table public.customers owner to app_owner; \
             alter table public.orders owner to app_owner; \
             grant select, insert, update, delete on public.customers, public.orders to app; \
             grant usage on schema {TARGETS} to app; \
             alter default privileges for role trellis in schema {TARGETS} \
               grant select on tables to app;"
        ))
        .await
        .expect("the roles and sources");
    for grant in trellis_grants(database) {
        admin.batch_execute(&grant).await.expect(&grant);
    }
}

async fn apply(trellis: &Trellis, statement: &str) {
    trellis
        .apply(statement)
        .await
        .unwrap_or_else(|err| panic!("{statement}: {err}"));
}

async fn status(trellis: &Trellis, target: &str) -> DefinitionStatus {
    trellis
        .status(target)
        .await
        .expect("read status")
        .unwrap_or_else(|| panic!("{target} is registered"))
}

/// Polls `target` until it reports `wanted`, failing after [`WAIT`] with
/// whatever failure it reports and any privilege refusal logged.
async fn wait_for(trellis: &Trellis, target: &str, wanted: TransformStatus) -> DefinitionStatus {
    let deadline = Instant::now() + WAIT;
    loop {
        let reported = status(trellis, target).await;
        if reported.status == wanted {
            return reported;
        }
        assert!(
            Instant::now() < deadline,
            "{target} never reported {wanted:?}: {reported:?}\nrefusals logged:\n{}",
            logged::privilege_refusals().join("\n")
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// `PAUSE`, then `DROP`, which refuses a definition that isn't paused.
async fn drop_transform(trellis: &Trellis, target: &str) {
    apply(trellis, &format!("PAUSE TRANSFORM {target}")).await;
    wait_for(trellis, target, TransformStatus::Paused).await;
    apply(trellis, &format!("DROP TRANSFORM {target}")).await;
}

/// Waits for every change committed so far to reach the targets.
async fn converge(trellis: &Trellis) {
    let token = trellis.watermark_token().await.expect("watermark token");
    trellis
        .await_converged(token, WAIT)
        .await
        .expect("await_converged");
}

async fn rows(client: &Client, sql: &str) -> Vec<String> {
    let mut rows: Vec<String> = client
        .query(&format!("select t::text from ({sql}) t"), &[])
        .await
        .unwrap_or_else(|err| panic!("{sql}: {err}"))
        .into_iter()
        .map(|row| row.get(0))
        .collect();
    rows.sort();
    rows
}

/// Every target matches what its definition computes from the sources,
/// read as the application role, which the default privileges let read.
async fn assert_targets_match(app: &Client, admin: &Client, with_double: bool) {
    let copy_fields = if with_double {
        "id, amount, amount + amount as double_amount"
    } else {
        "id, amount"
    };
    assert_eq!(
        rows(
            app,
            &format!("select {copy_fields} from {TARGETS}.order_copy")
        )
        .await,
        rows(admin, &format!("select {copy_fields} from public.orders")).await,
        "order_copy"
    );
    assert_eq!(
        rows(
            app,
            &format!("select customer_id, n, total from {TARGETS}.order_totals")
        )
        .await,
        rows(
            admin,
            "select customer_id, count(*) as n, sum(amount) as total \
             from public.orders group by customer_id"
        )
        .await,
        "order_totals"
    );
    assert_eq!(
        rows(
            app,
            &format!("select id, customer_name from {TARGETS}.order_customers")
        )
        .await,
        rows(
            admin,
            "select o.id, c.name as customer_name \
             from public.orders o left join public.customers c on c.id = o.customer_id"
        )
        .await,
        "order_customers"
    );
}

async fn self_check_converges(trellis: &Trellis, target: &str) {
    let outcome = trellis
        .self_check(
            target,
            trellis::SelfCheckScope {
                after: None,
                limit: 1000,
            },
            trellis::SelfCheckMode::Strict,
            WAIT,
        )
        .await
        .unwrap_or_else(|err| panic!("self_check {target}: {err}"))
        .outcome;
    assert!(
        matches!(outcome, SelfCheckOutcome::Converged),
        "{target}: {outcome:?}"
    );
}

async fn capture_triggers(admin: &Client) -> i64 {
    admin
        .query_one(
            "select count(*) from pg_catalog.pg_trigger \
             where tgrelid in ('public.orders'::regclass, 'public.customers'::regclass) \
               and not tgisinternal",
            &[],
        )
        .await
        .expect("count capture triggers")
        .get(0)
}

#[tokio::test]
async fn the_documented_privileges_run_the_whole_lifecycle() {
    logged::install();
    let cluster = TestCluster::start();
    let db = cluster.create_empty_database().await;
    let admin = connect(db.dsn()).await;
    set_up(&admin, db.name()).await;

    let dsn = as_role(db.dsn(), "trellis");
    let config = Config::with_schema(dsn, SCHEMA)
        .expect("valid config")
        .with_target_schema(TARGETS)
        .expect("valid target schema");
    let pool = trellis::Pool::new(&config).expect("pool");
    trellis::migrate(&pool, &config)
        .await
        .expect("migrate as the Trellis role");
    let trellis = Trellis::connect(config.clone(), TrellisOptions::default())
        .await
        .expect("connect as the Trellis role");
    let workers = trellis::Client::start_with_config(
        config,
        ClientOptions {
            staging_worker: true,
            application_threads: 2,
            reconcile_interval: Duration::from_millis(200),
            ..ClientOptions::default()
        },
    )
    .expect("start the staging and drain workers as the Trellis role");
    let app = connect(&as_role(db.dsn(), "app")).await;

    // Define a 1-1, an aggregate and a to-one relationship transform.
    apply(
        &trellis,
        "RELATIONSHIP customer FROM orders.customer_id TO customers.id",
    )
    .await;
    apply(
        &trellis,
        "TRANSFORM order_copy FROM public.orders SELECT amount AS amount",
    )
    .await;
    apply(
        &trellis,
        "TRANSFORM order_totals FROM public.orders GROUP BY customer_id \
         SELECT customer_id AS customer_id, COUNT(*) AS n, SUM(amount) AS total",
    )
    .await;
    apply(
        &trellis,
        "TRANSFORM order_customers FROM public.orders SELECT customer.name AS customer_name",
    )
    .await;
    for target in ["order_copy", "order_totals", "order_customers"] {
        wait_for(&trellis, target, TransformStatus::Live).await;
    }
    // A definition the discharge flips `live` gets its rows from the ring
    // after the flip, so `live` alone doesn't promise them: a token does.
    converge(&trellis).await;
    assert_targets_match(&app, &admin, false).await;
    assert_no_privilege_refusal("define and build");

    // The application writes the sources; the drain carries every change.
    app.batch_execute(
        "insert into public.orders values (10, 1, 100), (11, 2, 110); \
         update public.orders set amount = amount + 1 where id in (1, 2); \
         delete from public.orders where id = 3; \
         update public.customers set name = 'renamed' where id = 2; \
         insert into public.customers values (4, 'c4'); \
         update public.orders set customer_id = 4 where id = 4;",
    )
    .await
    .expect("the application writes its sources");
    converge(&trellis).await;
    assert_targets_match(&app, &admin, false).await;
    // `self_check` audits plain 1-1 targets only, and runs its capture
    // audit with them.
    self_check_converges(&trellis, "order_copy").await;
    let refused = app
        .batch_execute(&format!(
            "insert into {TARGETS}.order_copy (id, amount) values (99, 1)"
        ))
        .await
        .expect_err("the application can't write a target");
    assert_eq!(
        refused.code(),
        Some(&tokio_postgres::error::SqlState::INSUFFICIENT_PRIVILEGE),
        "{refused:?}"
    );
    assert_no_privilege_refusal("drain and self_check");

    // ALTER TRANSFORM: a field build rewrites the new field across the rows.
    apply(
        &trellis,
        "ALTER TRANSFORM order_copy ADD amount + amount AS double_amount",
    )
    .await;
    wait_for(&trellis, "order_copy", TransformStatus::Live).await;
    app.batch_execute("update public.orders set amount = 7 where id = 5")
        .await
        .expect("write");
    converge(&trellis).await;
    assert_targets_match(&app, &admin, true).await;
    assert_no_privilege_refusal("ALTER TRANSFORM");

    // PAUSE and RESUME: the resume rebuilds what changed during the pause.
    apply(&trellis, "PAUSE TRANSFORM order_totals").await;
    wait_for(&trellis, "order_totals", TransformStatus::Paused).await;
    app.batch_execute(
        "insert into public.orders values (12, 3, 120); \
         update public.orders set amount = 1 where id = 6;",
    )
    .await
    .expect("write while paused");
    apply(&trellis, "RESUME TRANSFORM order_totals").await;
    wait_for(&trellis, "order_totals", TransformStatus::Live).await;
    converge(&trellis).await;
    assert_targets_match(&app, &admin, true).await;
    assert_no_privilege_refusal("PAUSE and RESUME");

    // A widened key (#767) pauses the definitions keyed by it; a resume
    // re-types Trellis's copies and rebuilds.
    admin
        .batch_execute("alter table public.orders alter column id type bigint")
        .await
        .expect("the owner widens the key");
    for target in ["order_copy", "order_customers"] {
        let reported = wait_for(&trellis, target, TransformStatus::Paused).await;
        assert!(reported.capture_failure.is_some(), "{target}: {reported:?}");
    }
    for target in ["order_copy", "order_customers"] {
        apply(&trellis, &format!("RESUME TRANSFORM {target}")).await;
    }
    for target in ["order_copy", "order_totals", "order_customers"] {
        wait_for(&trellis, target, TransformStatus::Live).await;
    }
    app.batch_execute("insert into public.orders values (3000000000, 1, 5)")
        .await
        .expect("a key above 2^31");
    converge(&trellis).await;
    assert_targets_match(&app, &admin, true).await;
    assert_no_privilege_refusal("widened key");

    // DROP, and defining again: the default privileges let the
    // application read the new table without a grant step.
    drop_transform(&trellis, "order_copy").await;
    apply(
        &trellis,
        "TRANSFORM order_copy FROM public.orders SELECT amount AS amount",
    )
    .await;
    wait_for(&trellis, "order_copy", TransformStatus::Live).await;
    converge(&trellis).await;
    assert_targets_match(&app, &admin, false).await;

    for target in ["order_customers", "order_totals", "order_copy"] {
        drop_transform(&trellis, target).await;
    }
    apply(&trellis, "DROP RELATIONSHIP orders.customer").await;
    assert!(trellis.definitions().await.expect("definitions").is_empty());
    let targets: i64 = admin
        .query_one(
            &format!("select count(*) from pg_catalog.pg_tables where schemaname = '{TARGETS}'"),
            &[],
        )
        .await
        .expect("count targets")
        .get(0);
    assert_eq!(targets, 0, "DROP TRANSFORM removes each target");

    // With nothing left reading the sources, the staging worker removes
    // their capture.
    let deadline = Instant::now() + WAIT;
    while capture_triggers(&admin).await > 0 {
        assert!(
            Instant::now() < deadline,
            "capture was never removed\nrefusals logged:\n{}",
            logged::privilege_refusals().join("\n")
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    app.batch_execute("insert into public.orders values (13, 1, 1)")
        .await
        .expect("the application writes without capture");
    assert_no_privilege_refusal("DROP");
    workers.shutdown().await.expect("shutdown");
    assert_no_privilege_refusal("shutdown");
}
