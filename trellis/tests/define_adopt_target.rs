//! `ADOPT EXISTING TABLE`: a define that builds into an existing empty
//! table shaped exactly as define would create it, and refuses any other
//! (`trellis::defs::adopt`).
//!
//! Each key space adopts a table created by hand as define would create it,
//! with an index and a grant added, and the build fills it while the index
//! and the grant stay. Then one refusal per class of property the check
//! refuses, a table with rows, a taken ledger name and a missing privilege.
//! Builds run by hand ([`build_everything`]); nothing waits for a background
//! pass.

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
        (
            "check_constraint",
            "create table public.t (id bigint, a numeric, r text, primary key (id), \
             check (a > 0))",
        ),
        (
            "unique_constraint",
            "create table public.t (id bigint, a numeric, r text, primary key (id), \
             unique (r))",
        ),
        (
            "exclusion_constraint",
            "create table public.t (id bigint, a numeric, r text, primary key (id), \
             exclude using btree (r with =))",
        ),
        (
            "foreign_key",
            "create table public.t (id bigint references public.s, a numeric, r text, \
             primary key (id))",
        ),
        (
            "key",
            "create table public.t (id bigint, a numeric, r text, primary key (id) include (r))",
        ),
        (
            "foreign_key",
            &format!(
                "{ONE_TO_ONE_TABLE}; \
                 create table public.child (tid bigint references public.t)"
            ),
        ),
        (
            "unique_index",
            &format!("{ONE_TO_ONE_TABLE}; create unique index on public.t (r)"),
        ),
        (
            "trigger",
            &format!(
                "{ONE_TO_ONE_TABLE}; \
                 create function public.noop() returns trigger language plpgsql \
                   as 'begin return new; end'; \
                 create trigger t_noop before insert on public.t \
                   for each row execute function public.noop()"
            ),
        ),
        (
            "rule",
            &format!(
                "{ONE_TO_ONE_TABLE}; \
                 create rule t_nothing as on delete to public.t do instead nothing"
            ),
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
             drop table if exists public.t, public.p, public.child cascade; \
             drop function if exists public.noop(); {create}"
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
