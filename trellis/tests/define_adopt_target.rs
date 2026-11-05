//! `ADOPT EXISTING TABLE`: a define that builds into an existing empty
//! table shaped exactly as define would create it, and refuses any other
//! (`trellis::defs::adopt`).
//!
//! Each key space adopts a table created by hand as define would create it,
//! with an index and a grant added, and the build fills it while the index
//! and the grant stay; a table carrying everything a user may add to any
//! target adopts too. Then one refusal per class of property the check
//! refuses, a table with rows, a taken ledger name and a missing privilege.
//! Last, what a user's unique index and another table's foreign key
//! referencing the target do to the rows Trellis writes, drained by hand
//! through `support/drain_driver.rs`. Builds run by hand
//! ([`build_everything`]); nothing waits for a background pass.

use testkit::TestCluster;
use tokio_postgres::Client;
use trellis::{Config, ErrorCode, Trellis, TrellisOptions};

async fn raw(dsn: &str) -> Client {
    let (client, connection) = tokio_postgres::connect(dsn, tokio_postgres::NoTls)
        .await
        .expect("connect");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
        .batch_execute(&format!(
            "set search_path to {}, public",
            trellis::config::DEFAULT_SCHEMA
        ))
        .await
        .expect("search_path");
    client
}

/// A define-only handle on `db`.
async fn handle(db: &testkit::TestDatabase) -> Trellis {
    Trellis::connect(
        Config::from_dsn(db.dsn().to_string()).expect("config"),
        TrellisOptions::default(),
    )
    .await
    .expect("connect")
}

/// Runs every queued build to completion by hand, the stand-in for a drain
/// worker.
async fn build_everything(trellis: &Trellis) {
    trellis::intake::harness::start_registered_builds(trellis.pool())
        .await
        .expect("dispatch registered definitions' builds");
    trellis::staging::build::settle_builds(trellis.pool()).await;
}

/// The sources every case reads: `s`, keyed by `id`, and `u`, joined to it.
const SOURCES: &str = "create table public.s (id bigint primary key, a numeric, r text); \
     create index on public.s (r); \
     insert into public.s values (1, 10, 'x'), (2, 20, 'x'), (3, 5, 'y'); \
     create table public.u (id bigint primary key, sid bigint, v text); \
     create index on public.u (sid); \
     insert into public.u values (1, 1, 'one'), (2, 2, 'two'), (3, 3, 'three'); \
     create role adopt_reader nologin";

/// The 1-1 definition the refusal cases adopt, and the table define would
/// create for it.
const ONE_TO_ONE: &str = "TRANSFORM t ADOPT EXISTING TABLE FROM s SELECT a AS a, r AS r";
const ONE_TO_ONE_TABLE: &str =
    "create table public.t (id bigint, a numeric, r text, primary key (id))";

async fn set_up(cluster: &TestCluster) -> (testkit::TestDatabase, Client, Trellis) {
    let db = cluster.create_isolated_database().await;
    let raw = raw(db.dsn()).await;
    raw.batch_execute(SOURCES).await.expect("sources");
    let trellis = handle(&db).await;
    (db, raw, trellis)
}

/// `table` holds `want` (each row rendered as text, in order), and still has
/// the index and the grant the test added before define.
async fn assert_adopted(raw: &Client, table: &str, order_by: &str, want: &[&str]) {
    let rows: Vec<String> = raw
        .query(
            &format!("select t::text from public.{table} t order by {order_by}"),
            &[],
        )
        .await
        .expect("read the target")
        .iter()
        .map(|row| row.get(0))
        .collect();
    assert_eq!(rows, want, "{table}");
    let index: bool = raw
        .query_one(
            "select exists (select from pg_indexes \
             where schemaname = 'public' and indexname = $1)",
            &[&format!("{table}_users_idx")],
        )
        .await
        .expect("read the index")
        .get(0);
    assert!(index, "{table}: the user's index survives");
    let granted: bool = raw
        .query_one(
            "select has_table_privilege('adopt_reader', $1, 'SELECT')",
            &[&format!("public.{table}")],
        )
        .await
        .expect("read the grant")
        .get(0);
    assert!(granted, "{table}: the user's grant survives");
}

/// Creates `table` with `create`, adds an index on `indexed` and a grant,
/// defines `statement` and builds it.
async fn adopt(
    raw: &Client,
    trellis: &Trellis,
    table: &str,
    create: &str,
    indexed: &str,
    statement: &str,
) {
    raw.batch_execute(&format!(
        "{create}; create index {table}_users_idx on public.{table} ({indexed}); \
         grant select on public.{table} to adopt_reader"
    ))
    .await
    .expect("the table to adopt");
    trellis
        .apply(statement)
        .await
        .unwrap_or_else(|err| panic!("{statement}: {err}"));
    build_everything(trellis).await;
}

#[tokio::test]
async fn each_key_space_adopts_an_empty_table_shaped_as_define_creates_it() {
    let cluster = TestCluster::start();
    let (_db, raw, trellis) = set_up(&cluster).await;

    adopt(&raw, &trellis, "t", ONE_TO_ONE_TABLE, "r", ONE_TO_ONE).await;
    assert_adopted(&raw, "t", "id", &["(1,10,x)", "(2,20,x)", "(3,5,y)"]).await;

    adopt(
        &raw,
        &trellis,
        "g",
        "create table public.g (r text, n bigint, total numeric, \
         unique nulls not distinct (r))",
        "total",
        "TRANSFORM g ADOPT EXISTING TABLE FROM s GROUP BY r \
         SELECT r AS r, COUNT(*) AS n, SUM(id) AS total",
    )
    .await;
    assert_adopted(&raw, "g", "r", &["(x,2,3)", "(y,1,3)"]).await;

    adopt(
        &raw,
        &trellis,
        "j",
        "create table public.j (r text, n bigint, unique nulls not distinct (r))",
        "n",
        "TRANSFORM j ADOPT EXISTING TABLE FROM s JOIN u ON s.id = u.sid GROUP BY s.r \
         SELECT s.r AS r, COUNT(*) AS n",
    )
    .await;
    assert_adopted(&raw, "j", "r", &["(x,2)", "(y,1)"]).await;

    adopt(
        &raw,
        &trellis,
        "o",
        "create table public.o (id bigint, v text, primary key (id))",
        "v",
        "TRANSFORM o ADOPT EXISTING TABLE FROM u JOIN s ON u.sid = s.id \
         SELECT s.r AS v",
    )
    .await;
    assert_adopted(&raw, "o", "id", &["(1,x)", "(2,x)", "(3,y)"]).await;

    // Afterwards an adopted target is an ordinary one: `DROP` drops it.
    for target in ["t", "g", "j", "o"] {
        trellis
            .apply(&format!("PAUSE TRANSFORM {target}"))
            .await
            .expect("pause");
        trellis
            .apply(&format!("DROP TRANSFORM {target}"))
            .await
            .expect("drop");
        let exists: bool = raw
            .query_one(
                "select to_regclass($1) is not null",
                &[&format!("public.{target}")],
            )
            .await
            .expect("look the table up")
            .get(0);
        assert!(!exists, "DROP drops the adopted {target}");
    }
}

/// Define refuses `statement` naming `code`, and registers nothing.
async fn assert_refused(raw: &Client, trellis: &Trellis, statement: &str, code: &str) {
    let err = trellis
        .apply(statement)
        .await
        .err()
        .unwrap_or_else(|| panic!("{statement}: accepted, expected {code}"));
    let message = err.to_string();
    assert!(
        message.contains(&format!("({code})")),
        "expected {code}: {message}"
    );
    let expected = if code == "missing" {
        ErrorCode::NotFound
    } else {
        ErrorCode::Validation
    };
    assert_eq!(err.code(), expected, "{message}");
    let defined: i64 = raw
        .query_one("select count(*) from transform_definitions", &[])
        .await
        .expect("count definitions")
        .get(0);
    assert_eq!(defined, 0, "{message}");
}

/// Each class of property the shape check refuses, on a 1-1 target: the
/// table is created as given, the define refused with the code naming the
/// property, and the table left as it was.
#[tokio::test]
async fn every_class_of_property_define_would_not_create_refuses_with_its_code() {
    let cluster = TestCluster::start();
    let (_db, raw, trellis) = set_up(&cluster).await;
    let cases = [
        ("missing", ""),
        (
            "not_a_table",
            "create view public.t as select 1::bigint as id",
        ),
        (
            "partitioned",
            "create table public.t (id bigint, a numeric, r text, primary key (id)) \
             partition by range (id)",
        ),
        (
            "inheritance",
            "create table public.p (id bigint); \
             create table public.t (id bigint, a numeric, r text, primary key (id)) \
             inherits (public.p)",
        ),
        (
            "unlogged",
            "create unlogged table public.t (id bigint, a numeric, r text, primary key (id))",
        ),
        (
            "missing_column",
            "create table public.t (id bigint, a numeric, primary key (id))",
        ),
        (
            "extra_column",
            "create table public.t (id bigint, a numeric, r text, note text, primary key (id))",
        ),
        (
            "column_order",
            "create table public.t (id bigint, r text, a numeric, primary key (id))",
        ),
        (
            "column_type",
            "create table public.t (id bigint, a numeric(10, 2), r text, primary key (id))",
        ),
        (
            "column_type",
            "create table public.t (id integer, a numeric, r text, primary key (id))",
        ),
        (
            "column_collation",
            "create table public.t (id bigint, a numeric, r text collate \"C\", \
             primary key (id))",
        ),
        (
            "column_nullability",
            "create table public.t (id bigint, a numeric not null, r text, primary key (id))",
        ),
        (
            "column_default",
            "create table public.t (id bigint, a numeric default 0, r text, primary key (id))",
        ),
        (
            "generated_column",
            "create table public.t (id bigint, a numeric, \
             r text generated always as ('x') stored, primary key (id))",
        ),
        (
            "identity_column",
            "create table public.t (id bigint generated always as identity, a numeric, \
             r text, primary key (id))",
        ),
        (
            "key",
            "create table public.t (id bigint not null, a numeric, r text, unique (id))",
        ),
        (
            "key",
            "create table public.t (id bigint, a numeric, r text, primary key (id) \
             deferrable)",
        ),
        // A user's unique constraint doesn't stand in for the key.
        (
            "key",
            "create table public.t (id bigint not null, a numeric, r text, unique (r))",
        ),
        (
            "key",
            "create table public.t (id bigint, a numeric, r text, primary key (id) include (r))",
        ),
        (
            "owned_sequence",
            &format!("{ONE_TO_ONE_TABLE}; create sequence public.t_seq owned by public.t.a"),
        ),
        (
            "not_empty",
            &format!("{ONE_TO_ONE_TABLE}; insert into public.t values (9, 9, 'z')"),
        ),
    ];
    for (code, create) in cases {
        raw.batch_execute(&format!(
            "do $$ begin \
               if exists (select from pg_views where viewname = 't') then \
                 drop view public.t; \
               end if; \
             end $$; \
             drop table if exists public.t, public.p cascade; {create}"
        ))
        .await
        .unwrap_or_else(|err| panic!("{code}: {create}: {err:?}"));
        assert_refused(&raw, &trellis, ONE_TO_ONE, code).await;
    }
}

/// The ledger beside a 1-1 target is Trellis's to create: a table already
/// under its name refuses the define, as it does without the opt-in.
#[tokio::test]
async fn a_taken_ledger_name_refuses_the_adopting_define() {
    let cluster = TestCluster::start();
    let (_db, raw, trellis) = set_up(&cluster).await;
    raw.batch_execute(&format!(
        "{ONE_TO_ONE_TABLE}; create table public.t__ledger (k text)"
    ))
    .await
    .expect("the table and a ledger");
    let err = trellis.apply(ONE_TO_ONE).await.expect_err("refused");
    assert_eq!(err.code(), ErrorCode::Conflict, "{err}");
    assert!(
        err.to_string()
            .contains("\"public.t__ledger\" already exists"),
        "{err}"
    );
}

/// Another definition's target is Trellis's already: adopting it refuses as
/// a taken name, even while it is empty.
#[tokio::test]
async fn another_definitions_target_refuses_the_adopting_define() {
    let cluster = TestCluster::start();
    let (_db, raw, trellis) = set_up(&cluster).await;
    raw.batch_execute("truncate public.s")
        .await
        .expect("an empty source");
    trellis
        .apply("TRANSFORM g FROM s GROUP BY r SELECT r AS r, COUNT(*) AS n")
        .await
        .expect("the first definition");
    let err = trellis
        .apply("TRANSFORM g ADOPT EXISTING TABLE FROM s GROUP BY r SELECT r AS r, COUNT(*) AS n")
        .await
        .expect_err("refused");
    assert_eq!(err.code(), ErrorCode::Conflict, "{err}");
    assert!(
        err.to_string().contains("\"public.g\" already exists"),
        "{err}"
    );
}

/// Without the opt-in an existing table refuses as it always has, empty and
/// shaped as define would create it or not.
#[tokio::test]
async fn without_the_opt_in_an_existing_table_still_refuses() {
    let cluster = TestCluster::start();
    let (_db, raw, trellis) = set_up(&cluster).await;
    raw.batch_execute(ONE_TO_ONE_TABLE).await.expect("table");
    let err = trellis
        .apply("TRANSFORM t FROM s SELECT a AS a, r AS r")
        .await
        .expect_err("refused");
    assert_eq!(err.code(), ErrorCode::Conflict, "{err}");
    assert!(err.to_string().contains("already exists"), "{err}");
}

/// A definition that adopts its target carries the release of the opt-in as
/// its floor.
#[tokio::test]
async fn an_adopting_definition_carries_the_opt_ins_floor() {
    let cluster = TestCluster::start();
    let (db, raw, _) = set_up(&cluster).await;
    raw.batch_execute(ONE_TO_ONE_TABLE).await.expect("table");
    let later = Trellis::connect(
        Config::from_dsn(db.dsn().to_string())
            .expect("config")
            .with_binary_version("0.3.0")
            .with_feature_since("AdoptTarget", "0.3.0"),
        TrellisOptions::default(),
    )
    .await
    .expect("connect");
    later.apply(ONE_TO_ONE).await.expect("adopted");
    let floor: String = raw
        .query_one(
            "select floor_version from transform_definitions where target_table = 'public.t'",
            &[],
        )
        .await
        .expect("floor")
        .get(0);
    assert_eq!(floor, "0.3.0");
}

/// The defining role must own the table (or be a member of its owner) and
/// hold every privilege Trellis writes it with; each refuses naming what's
/// missing. The roles follow `docs/recommendations.md` ("One Trellis role").
#[tokio::test]
async fn a_missing_privilege_or_ownership_refuses_naming_it() {
    const TARGETS: &str = "trellis_targets";
    let cluster = TestCluster::start();
    let db = cluster.create_empty_database().await;
    let admin = raw(db.dsn()).await;
    let database = db.name();
    admin
        .batch_execute(&format!(
            "create role app_owner nologin; \
             create role other_owner nologin; \
             create role trellis login; \
             create schema {TARGETS}; \
             create table public.s (id bigint primary key, a numeric, r text); \
             alter table public.s owner to app_owner; \
             grant create, connect, temporary on database \"{database}\" to trellis; \
             grant app_owner to trellis; \
             grant create, usage on schema {TARGETS} to trellis; \
             grant usage on schema public to trellis;"
        ))
        .await
        .expect("the roles and the source");
    assert!(db.dsn().contains("user=postgres"), "{}", db.dsn());
    let config = Config::with_schema(
        db.dsn().replace("user=postgres", "user=trellis"),
        trellis::config::DEFAULT_SCHEMA,
    )
    .expect("valid config")
    .with_target_schema(TARGETS)
    .expect("valid target schema");
    let pool = trellis::pool::Pool::new(&config).expect("pool");
    trellis::migrate::migrate_with_pool(&pool, &config)
        .await
        .expect("migrate as the Trellis role");
    let trellis = Trellis::connect(config, TrellisOptions::default())
        .await
        .expect("connect as the Trellis role");
    let table =
        format!("create table {TARGETS}.t (id bigint, a numeric, r text, primary key (id))");

    for (setup, code, named) in [
        (
            format!("{table}; alter table {TARGETS}.t owner to other_owner"),
            "not_owner",
            "other_owner",
        ),
        (
            format!(
                "{table}; alter table {TARGETS}.t owner to app_owner; \
                 revoke truncate on {TARGETS}.t from app_owner"
            ),
            "privilege",
            "TRUNCATE",
        ),
    ] {
        admin
            .batch_execute(&format!("drop table if exists {TARGETS}.t; {setup}"))
            .await
            .expect("the table");
        let err = trellis.apply(ONE_TO_ONE).await.expect_err("refused");
        let message = err.to_string();
        assert!(message.contains(&format!("({code})")), "{message}");
        assert!(message.contains(named), "{message}");
        assert_eq!(err.code(), ErrorCode::Validation, "{message}");
    }

    // A member that doesn't inherit the owner's privileges (PostgreSQL 16's
    // `WITH INHERIT FALSE`) can't alter or drop the table without SET ROLE,
    // which Trellis doesn't do.
    let version: i32 = admin
        .query_one("select current_setting('server_version_num')::int", &[])
        .await
        .expect("server version")
        .get(0);
    if version >= 160_000 {
        admin
            .batch_execute(&format!(
                "create role no_inherit_owner nologin; \
                 grant no_inherit_owner to trellis with inherit false; \
                 drop table {TARGETS}.t; {table}; \
                 alter table {TARGETS}.t owner to no_inherit_owner; \
                 grant all on {TARGETS}.t to trellis"
            ))
            .await
            .expect("the table");
        let err = trellis.apply(ONE_TO_ONE).await.expect_err("refused");
        let message = err.to_string();
        assert!(message.contains("(not_owner)"), "{message}");
        assert!(message.contains("no_inherit_owner"), "{message}");
    }

    // Owned by a role the defining role is a member of, with every
    // privilege: adopted.
    admin
        .batch_execute(&format!(
            "drop table {TARGETS}.t; {table}; alter table {TARGETS}.t owner to app_owner"
        ))
        .await
        .expect("the table");
    trellis.apply(ONE_TO_ONE).await.expect("adopted");
}

/// Everything a user may add to any target is accepted on the table adopted:
/// constraints of every kind beside the key (deferrable or not), a partial
/// expression unique index, triggers, a rule and another table's foreign key
/// referencing it. The build fills it, and each object stays.
#[tokio::test]
async fn everything_a_user_may_add_to_any_target_adopts() {
    let cluster = TestCluster::start();
    let (_db, raw, trellis) = set_up(&cluster).await;
    raw.batch_execute(
        "create table public.t (id bigint references public.s, a numeric, r text, \
           primary key (id), check (a > 0), unique (a) deferrable initially deferred, \
           exclude using btree (a with =)); \
         create unique index t_lower_r on public.t (lower(r)) where a > 100; \
         create function public.noop() returns trigger language plpgsql \
           as 'begin return new; end'; \
         create trigger t_noop before insert on public.t \
           for each row execute function public.noop(); \
         create constraint trigger t_later after insert on public.t deferrable \
           for each row execute function public.noop(); \
         create table public.child (tid bigint references public.t); \
         create rule t_nothing as on delete to public.t do instead nothing",
    )
    .await
    .expect("the table to adopt");
    trellis
        .apply(ONE_TO_ONE)
        .await
        .unwrap_or_else(|err| panic!("{ONE_TO_ONE}: {err}"));
    // A `DELETE` rule fails the build's deletes (the docs' "Target tables
    // are Trellis-owned"): adopted, it is the user's to drop.
    raw.batch_execute("drop rule t_nothing on public.t")
        .await
        .expect("drop the rule");
    build_everything(&trellis).await;
    let rows: Vec<String> = raw
        .query("select t::text from public.t t order by id", &[])
        .await
        .expect("read the target")
        .iter()
        .map(|row| row.get(0))
        .collect();
    assert_eq!(rows, ["(1,10,x)", "(2,20,x)", "(3,5,y)"]);
    let kept: i64 = raw
        .query_one(
            "select (select count(*) from pg_constraint where conrelid = 'public.t'::regclass) \
                  + (select count(*) from pg_constraint where confrelid = 'public.t'::regclass) \
                  + (select count(*) from pg_indexes where indexname = 't_lower_r') \
                  + (select count(*) from pg_trigger \
                     where tgrelid = 'public.t'::regclass and not tgisinternal)",
            &[],
        )
        .await
        .expect("count the user's objects")
        .get(0);
    // pkey, fkey, check, unique, exclusion and the constraint trigger's
    // constraint; child's foreign key; the index; two triggers.
    assert_eq!(kept, 6 + 1 + 1 + 2);
}

#[path = "support/drain_driver.rs"]
mod drain_driver;

/// A driver whose `public.nums` (keys 1 to 3, `x` equal to the key) is read
/// by `t`, adopted from a table that carries `extra` beside the key.
async fn adopted_with(extra: &str) -> drain_driver::Driver {
    drain_driver::Driver::start(
        &format!(
            "create table public.nums (id integer primary key, x integer); \
             insert into public.nums select g, g from generate_series(1, 3) g; \
             create table public.t (id integer, x integer, primary key (id)); {extra}"
        ),
        &[
            ("id", trellis::defs::ValueType::Numeric),
            ("x", trellis::defs::ValueType::Numeric),
        ],
        &["TRANSFORM t ADOPT EXISTING TABLE FROM public.nums SELECT x AS x"],
        &["public.nums"],
    )
    .await
}

/// Drains `batch` until it commits, as a drain worker retries it: each
/// failure charges the failing keys a death, so the page commits once they
/// are held. Bounded, not a wait for convergence.
async fn drain_until_held(d: &drain_driver::Driver, batch: i64) {
    for _ in 0..20 {
        match trellis::staging::apply::drain_once(d.pool(), batch, "worker", 1, drain_driver::WAKE)
            .await
        {
            Ok(Some(_)) => continue,
            Ok(None) => return,
            Err(_) => {}
        }
    }
    panic!("the batch never drained");
}

/// The keys `t` holds in quarantine, each rendered as a row.
async fn held(d: &drain_driver::Driver) -> Vec<String> {
    d.rows("select key from poison order by key").await
}

/// `t`'s status, read through the facade.
async fn status_of_t(d: &drain_driver::Driver) -> trellis::DefinitionStatus {
    let config = Config::with_schema(d.db.dsn().to_string(), trellis::config::DEFAULT_SCHEMA)
        .expect("config");
    Trellis::connect(config, TrellisOptions::default())
        .await
        .expect("connect")
        .status("t")
        .await
        .expect("read status")
        .expect("t is registered")
}

/// A user's unique index on an adopted target refuses a row Trellis writes
/// as on any target: the key is held, and the definition keeps running.
#[tokio::test]
async fn a_row_a_users_unique_index_refuses_holds_its_key() {
    let mut d = adopted_with("create unique index t_x on public.t (x)").await;
    d.user()
        .await
        .batch_execute("update public.nums set x = 3 where id = 2")
        .await
        .expect("write a value key 3 has");
    let batch = d.seal().await;
    drain_until_held(&d, batch).await;
    assert_eq!(held(&d).await, ["(2)"]);
    let status = status_of_t(&d).await;
    assert_eq!(status.status, trellis::TransformStatus::Live, "{status:?}");
    assert!(status.drain_failure.is_none(), "{status:?}");
    assert_eq!(
        d.rows("select * from public.t order by id").await,
        ["(1,1)", "(2,2)", "(3,3)"]
    );
}

/// An aggregate's build writes its groups together, so a user's unique
/// constraint that refuses one of them pauses the build with a
/// `backfill_failure` naming it, rather than holding a key.
#[tokio::test]
async fn a_users_unique_constraint_refusing_an_aggregates_build_pauses_it() {
    let cluster = TestCluster::start();
    let (_db, raw, trellis) = set_up(&cluster).await;
    raw.batch_execute(
        "create index on public.s (r, a); \
         create table public.g (r text, a numeric, n bigint, \
           unique nulls not distinct (r, a)); \
         alter table public.g add constraint g_one_per_r unique (r)",
    )
    .await
    .expect("the table to adopt");
    trellis
        .apply(
            "TRANSFORM g ADOPT EXISTING TABLE FROM s GROUP BY r, a \
             SELECT r AS r, a AS a, COUNT(*) AS n",
        )
        .await
        .expect("adopted");
    build_everything(&trellis).await;
    let status = trellis
        .status("g")
        .await
        .expect("read status")
        .expect("g is registered");
    assert_eq!(
        status.status,
        trellis::TransformStatus::Paused,
        "{status:?}"
    );
    let failure = status.backfill_failure.expect("the build's failure");
    assert!(failure.last_error.contains("g_one_per_r"), "{failure:?}");
}

/// Another table's foreign key referencing an adopted target: a delete of a
/// referenced row holds its key, a source `TRUNCATE` (which Trellis applies
/// as a delete of every row) holds its page back as a `drain_failure` until
/// the referencing rows are gone, and `DROP TRANSFORM` fails while the
/// foreign key exists.
#[tokio::test]
async fn a_foreign_key_referencing_an_adopted_target_fails_trellis_deletes_of_it() {
    let mut d = adopted_with("create table public.child (tid integer references public.t)").await;
    d.ctl
        .batch_execute("insert into public.child values (1)")
        .await
        .expect("reference key 1");
    let user = d.user().await;
    user.batch_execute("delete from public.nums where id = 1")
        .await
        .expect("delete key 1");
    let batch = d.seal().await;
    drain_until_held(&d, batch).await;
    assert_eq!(held(&d).await, ["(1)"]);

    user.batch_execute("truncate public.nums")
        .await
        .expect("truncate the source");
    let batch = d.seal().await;
    for _ in 0..3 {
        let err =
            trellis::staging::apply::drain_once(d.pool(), batch, "worker", 1, drain_driver::WAKE)
                .await
                .expect_err("the clear is refused");
        assert!(err.to_string().contains("child_tid_fkey"), "{err}");
    }
    let status = status_of_t(&d).await;
    assert_eq!(status.status, trellis::TransformStatus::Live, "{status:?}");
    let failure = status.drain_failure.expect("the page is held back");
    assert_eq!(failure.sqlstate.as_deref(), Some("23503"));
    assert_eq!(d.rows("select count(*) from public.t").await, ["(3)"]);

    d.ctl
        .batch_execute("delete from public.child")
        .await
        .expect("drop the reference");
    drain_until_held(&d, batch).await;
    assert_eq!(d.rows("select count(*) from public.t").await, ["(0)"]);
    assert!(status_of_t(&d).await.drain_failure.is_none());

    let trellis = Trellis::connect(
        Config::with_schema(d.db.dsn().to_string(), trellis::config::DEFAULT_SCHEMA)
            .expect("config"),
        TrellisOptions::default(),
    )
    .await
    .expect("connect");
    trellis.apply("PAUSE TRANSFORM t").await.expect("pause");
    let err = trellis
        .apply("DROP TRANSFORM t")
        .await
        .expect_err("the foreign key depends on the target");
    assert!(err.to_string().contains("child_tid_fkey"), "{err}");
    assert_eq!(
        d.rows("select count(*) from transform_definitions").await,
        ["(1)"]
    );
}
