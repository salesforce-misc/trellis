//! Issue #622 (C2), acceptance A1: a capture trigger stages exactly the
//! images its golden fixture records for the same write.
//!
//! One column per `docs/type-support.md` family (the #565 E3 matrix), with
//! edge values and an all-`NULL` row, goes through insert, update and delete.
//! Alongside it: a primary-key move, a composite primary key declared out of
//! physical order whose parts contain the separator bytes, control-character
//! primary keys, truncate, two relationship `from_col`s feeding `group_key`,
//! and a to-side table whose image the catalog narrows. `public.ctl` also has
//! unread columns named `l` and `ts`, the capture function's own variable
//! names, which must not clash with them.
//!
//! Every write runs in an application session whose `DateStyle`, `TimeZone`,
//! `bytea_output`, `IntervalStyle` and `extra_float_digits` all differ from
//! the settings Trellis pins. The generated triggers are installed by hand.
//! Every ring row a step stages must carry the writer's
//! `pg_current_xact_id()` as its `row_txid`. The writer's role has no
//! privilege on Trellis's schema, so the `SECURITY DEFINER` functions are
//! what reach the ring.
//!
//! Per step, table and key, the rows in `(lsn, change_id)` order (key, op,
//! both images restricted to the spec's columns, `group_key` and `hop_gen`)
//! must equal the golden fixture `tests/fixtures/capture_parity.txt`.
//! `lsn`/`src_changed` are not compared (statement position and time).
//!
//! C2 recorded the fixture from trigger rows that matched, row for row, what
//! intake decoded from the WAL for the same transactions, with two
//! normalizations: an update that moved a primary key compared as the
//! delete plus insert a trigger stages, and intake's images restricted to the
//! trigger's columns. Intake is gone (#622 C7), so the fixture is the oracle.
//! Regenerate it with `TRELLIS_BLESS_CAPTURE_PARITY=1` only for a change to
//! what capture images, and review the diff as a behaviour change.

use std::collections::BTreeMap;
use std::collections::HashMap;

use testkit::TestCluster;
use tokio_postgres::{Client, NoTls};
use trellis::capture::columns::{capture_spec, load_catalog};
use trellis::capture::sql::{CaptureSpec, install_statements};
use trellis::config::DEFAULT_SCHEMA;
use trellis::defs::ast::ValueType;
use trellis::defs::{create_relationship, install_definition, publication_tables};

const FIXTURE: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/tests/fixtures/capture_parity.txt"
);

/// The application session's settings: every one differs from Trellis's.
const HOSTILE: &str = "set datestyle to 'SQL, DMY'; set bytea_output to 'escape'; \
     set extra_float_digits to 0; set intervalstyle to 'sql_standard'; \
     set timezone to 'Asia/Kolkata'";

/// The role the application writes as.
const APP_ROLE: &str = "capture_parity_app";

/// `(column, type, values)`: row `i` of `public.fam` takes `values[i]`, or
/// `NULL` past the end. The #565 E3 matrix.
const FAMILIES: &[(&str, &str, &[&str])] = &[
    ("c_int2", "smallint", &["-32768", "7"]),
    ("c_int4", "integer", &["2147483647", "0"]),
    ("c_int8", "bigint", &["-9223372036854775808", "42"]),
    ("c_oid", "oid", &["4294967295", "1"]),
    (
        "c_num",
        "numeric",
        &[
            "1.50",
            "'NaN'",
            "'Infinity'",
            "-0.000",
            "123456789012345678901234567890.123",
        ],
    ),
    (
        "c_real",
        "real",
        &["1.1", "'NaN'", "'-Infinity'", "'-0'", "3.4028235e38"],
    ),
    (
        "c_dbl",
        "double precision",
        &["0.1", "'Infinity'", "'-0'", "1e-300", "12345678.901234567"],
    ),
    ("c_bool", "boolean", &["true", "false"]),
    (
        "c_uuid",
        "uuid",
        &["'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'"],
    ),
    (
        "c_text",
        "text",
        &[
            "'plain'",
            r#"E'quote" back\\ nl\n tab\t ctl\x01 uni ☃ 𝄞'"#,
            "''",
        ],
    ),
    ("c_vchar", "varchar(20)", &["'v'"]),
    ("c_char", "char(5)", &["'ab'"]),
    ("c_citext", "citext", &["'MiXeD'"]),
    ("c_bytea", "bytea", &[r"'\xdeadbeef'", "''"]),
    (
        "c_date",
        "date",
        &["'2024-02-29'", "'infinity'", "'0044-03-15 BC'"],
    ),
    ("c_time", "time", &["'23:59:59.999999'"]),
    ("c_timetz", "timetz", &["'12:00:00+05:30'"]),
    (
        "c_ts",
        "timestamp",
        &[
            "'2024-01-01 00:00:00'",
            "'infinity'",
            "'2024-06-01 12:34:56.789'",
        ],
    ),
    (
        "c_tstz",
        "timestamptz",
        &[
            "'2024-01-01 00:00:00+00'",
            "'-infinity'",
            "'2024-06-01 12:34:56.789+02'",
        ],
    ),
    (
        "c_intv",
        "interval",
        &["'1 day 02:03:04'", "'-1 mon'", "'1 year 2 months'"],
    ),
    (
        "c_jsonb",
        "jsonb",
        &[
            r#"'{"b": 1.50, "a": [1, "x", null], "n": {"t": true}}'"#,
            r#"'"str"'"#,
            "'null'",
        ],
    ),
    ("c_json", "json", &[r#"'{"b":1.50,  "a":[1]}'"#]),
    (
        "c_inet",
        "inet",
        &["'10.0.0.1'", "'10.0.0.0/8'", "'::1/128'"],
    ),
    ("c_cidr", "cidr", &["'10.0.0.0/8'"]),
    ("c_mac", "macaddr", &["'08:00:2b:01:02:03'"]),
    ("c_mac8", "macaddr8", &["'08:00:2b:01:02:03:04:05'"]),
    ("c_enum", "public.mood", &["'happy'"]),
    ("c_bit", "bit(4)", &["B'1010'"]),
    ("c_vbit", "bit varying", &["B'101'"]),
    ("c_iarr", "integer[]", &["'{1,2,NULL}'", "'{}'"]),
    ("c_tarr", "text[]", &[r#"'{"a b","c,d","q\""}'"#]),
    ("c_range", "int4range", &["'[1,10)'", "'empty'"]),
    (
        "c_comp",
        "public.pair",
        // `ROW(NULL, NULL)` is not a NULL value (its output is `(,)`),
        // though `IS NULL` is true of it.
        &["ROW(1, 'x y')::public.pair", "ROW(NULL, NULL)::public.pair"],
    ),
    ("c_point", "point", &["'(1.5,2)'"]),
    ("c_money", "money", &["12.34"]),
    ("c_xml", "xml", &[r#"'<a b="1">t</a>'"#]),
    ("c_tsv", "tsvector", &["'a fat cat'"]),
    ("c_tsq", "tsquery", &["'fat & cat'"]),
];

fn fam_rows() -> usize {
    FAMILIES.iter().map(|(_, _, v)| v.len()).max().unwrap()
}

fn value_types(pairs: &[(&str, ValueType)]) -> HashMap<String, ValueType> {
    pairs.iter().map(|(n, t)| (n.to_string(), *t)).collect()
}

fn strings(names: &[&str]) -> Vec<String> {
    names.iter().map(|s| s.to_string()).collect()
}

async fn connect(dsn: &str, setup: &str) -> Client {
    let (client, connection) = tokio_postgres::connect(dsn, NoTls).await.expect("connect");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client.batch_execute(setup).await.expect("session setup");
    client
}

/// One ring row, as the comparison sees it.
struct RingRow {
    txid: String,
    key: String,
    /// `to_jsonb(key)::text`, for the fixture.
    key_json: String,
    /// `[op, old_image, new_image, group_key, hop_gen]` with both images
    /// restricted to the spec's columns.
    doc: String,
    /// The same with the images unrestricted.
    raw_doc: String,
    lsn_is_origin: bool,
    has_lsn_and_time: bool,
}

/// Every ring row of `table` above `after`, in `(lsn, change_id)` order.
async fn ring_rows(raw: &Client, after: i64, table: &str, spec: &CaptureSpec) -> Vec<RingRow> {
    let ring = (0..4)
        .map(|s| format!("select * from {DEFAULT_SCHEMA}.seg_{s}"))
        .collect::<Vec<_>>()
        .join(" union all ");
    let restrict = |img: &str| {
        format!(
            "(select jsonb_object_agg(e.key, e.value) from jsonb_each({img}) e \
             where e.key = any($3::text[]))"
        )
    };
    let sql = format!(
        "select r.row_txid::text, r.key, to_jsonb(r.key)::text, \
                jsonb_build_array(r.op, {ro}, {rn}, r.group_key, r.hop_gen)::text, \
                jsonb_build_array(r.op, r.old_image, r.new_image, r.group_key, r.hop_gen)::text, \
                r.lsn is not distinct from r.origin_lsn, \
                r.lsn is not null and r.src_changed is not null \
         from ({ring}) r \
         where r.change_id > $1 and r.src_table = $2 \
         order by r.lsn, r.change_id",
        ro = restrict("r.old_image"),
        rn = restrict("r.new_image"),
    );
    let columns: Vec<String> = spec.columns().to_vec();
    raw.query(&sql, &[&after, &table, &columns])
        .await
        .unwrap_or_else(|e| panic!("read ring rows of {table}: {e}"))
        .into_iter()
        .map(|row| RingRow {
            txid: row.get(0),
            key: row.get(1),
            key_json: row.get(2),
            doc: row.get(3),
            raw_doc: row.get(4),
            lsn_is_origin: row.get(5),
            has_lsn_and_time: row.get(6),
        })
        .collect()
}

#[tokio::test]
async fn trigger_capture_stages_the_golden_images_under_a_foreign_session() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let raw = connect(
        db.dsn(),
        &format!("set search_path to {DEFAULT_SCHEMA}, public"),
    )
    .await;

    let fam_columns: Vec<String> = FAMILIES
        .iter()
        .map(|(name, ty, _)| format!("{name} {ty}"))
        .collect();
    raw.batch_execute(&format!(
        "create extension if not exists citext; \
         create type public.mood as enum ('sad', 'happy'); \
         create type public.pair as (a int, b text); \
         create table public.parent (id integer primary key, name text, unread text); \
         create table public.fam (id integer primary key, fk2 integer, fk integer, {}); \
         create table public.comp (tag text, post integer, weight numeric, note text, \
                                   primary key (post, tag)); \
         create table public.ctl (k text primary key, v text, l integer, ts text); \
         alter table public.parent replica identity full; \
         alter table public.fam replica identity full; \
         alter table public.comp replica identity full; \
         alter table public.ctl replica identity full",
        fam_columns.join(", ")
    ))
    .await
    .expect("create sources");

    create_relationship(&db.pool, "RELATIONSHIP fam_parent FROM fam.fk TO parent.id")
        .await
        .expect("relationship fk");
    create_relationship(
        &db.pool,
        "RELATIONSHIP fam_parent2 FROM fam.fk2 TO parent.id",
    )
    .await
    .expect("relationship fk2");
    for (text, columns) in [
        (
            "TRANSFORM fam_out FROM public.fam SELECT c_int4 AS c_int4",
            value_types(&[("id", ValueType::Numeric), ("c_int4", ValueType::Numeric)]),
        ),
        (
            "TRANSFORM fam_named FROM public.fam SELECT fam_parent.name AS parent_name",
            value_types(&[("id", ValueType::Numeric), ("fk", ValueType::Numeric)]),
        ),
        (
            "TRANSFORM comp_out FROM public.comp GROUP BY tag \
             SELECT tag AS tag, SUM(weight) AS w",
            value_types(&[
                ("tag", ValueType::Text),
                ("post", ValueType::Numeric),
                ("weight", ValueType::Numeric),
            ]),
        ),
        (
            "TRANSFORM ctl_out FROM public.ctl SELECT v AS v",
            value_types(&[("k", ValueType::Text), ("v", ValueType::Text)]),
        ),
    ] {
        install_definition(&db.pool, text, &columns, "public")
            .await
            .unwrap_or_else(|e| panic!("install {text:?}: {e}"));
        trellis::intake::publication::settle_registrations(&db.pool).await;
    }

    // The capture specs, from the catalog.
    let published = publication_tables(&db.pool)
        .await
        .expect("publication_tables");
    let mut sorted = published.clone();
    sorted.sort();
    assert_eq!(
        sorted,
        ["public.comp", "public.ctl", "public.fam", "public.parent"]
    );
    let client = db.pool.get().await.expect("pool client");
    let catalog = load_catalog(&**client, DEFAULT_SCHEMA)
        .await
        .expect("load capture catalog");
    let mut specs: BTreeMap<String, CaptureSpec> = BTreeMap::new();
    for table in &sorted {
        let spec = capture_spec(&**client, &catalog, table)
            .await
            .unwrap_or_else(|e| panic!("capture spec for {table}: {e}"));
        specs.insert(table.clone(), spec);
    }
    drop(client);

    // What the catalog asks each table to image.
    let fam = &specs["public.fam"];
    assert_eq!(fam.key(), strings(&["id"]));
    assert_eq!(fam.columns(), strings(&["c_int4", "fk", "fk2", "id"]));
    assert_eq!(
        fam.group_key(),
        strings(&["fk2", "fk"]),
        "group-key columns in physical order"
    );
    let parent = &specs["public.parent"];
    assert_eq!(
        parent.columns(),
        strings(&["id", "name"]),
        "unread is not imaged"
    );
    assert!(parent.group_key().is_empty());
    let comp = &specs["public.comp"];
    assert_eq!(comp.key(), strings(&["post", "tag"]), "declared key order");
    assert_eq!(comp.columns(), strings(&["post", "tag", "weight"]));
    assert_eq!(specs["public.ctl"].columns(), strings(&["k", "v"]));

    // `fam` images every family, so each one is compared, not just the
    // column a definition reads.
    let every_family = CaptureSpec::new(
        "public.fam",
        strings(&["id"]),
        FAMILIES
            .iter()
            .map(|(name, _, _)| name.to_string())
            .chain(strings(&["fk", "fk2"])),
        fam.group_key().to_vec(),
    )
    .expect("every-family spec");
    specs.insert("public.fam".to_string(), every_family);

    for spec in specs.values() {
        for statement in install_statements(DEFAULT_SCHEMA, spec).expect("generate") {
            raw.batch_execute(&statement)
                .await
                .unwrap_or_else(|e| panic!("install capture:\n{statement}\n{e}"));
        }
    }

    // The application role has no privilege on Trellis's schema: the
    // `SECURITY DEFINER` functions write the ring for it.
    raw.batch_execute(&format!(
        "do $$ begin create role {APP_ROLE}; \
             exception when duplicate_object then null; end $$; \
             grant usage on schema public to {APP_ROLE}; \
             grant all on all tables in schema public to {APP_ROLE}"
    ))
    .await
    .expect("create the application role");
    let mut writer = connect(db.dsn(), &format!("set role {APP_ROLE}; {HOSTILE}")).await;
    let usage: bool = writer
        .query_one(
            "select has_schema_privilege($1, 'usage')",
            &[&DEFAULT_SCHEMA],
        )
        .await
        .expect("check schema privilege")
        .get(0);
    assert!(!usage, "the writer can't reach the ring on its own");
    // Nor can it execute a capture function (so it can't attach one to a
    // table of its own), yet its writes below still fire them.
    let privileges = raw
        .query_one(
            "select count(*), \
                    count(*) filter (where pg_catalog.has_function_privilege($2, p.oid, 'execute')) \
             from pg_catalog.pg_proc p \
             join pg_catalog.pg_namespace s on s.oid = p.pronamespace \
             where s.nspname = $1 and p.proname like 'cap\\_%'",
            &[&DEFAULT_SCHEMA, &APP_ROLE],
        )
        .await
        .expect("check function privilege");
    let (functions, executable): (i64, i64) = (privileges.get(0), privileges.get(1));
    assert_eq!(functions, 4 * specs.len() as i64, "one function per event");
    assert_eq!(executable, 0, "PUBLIC can't execute a capture function");

    let rows = fam_rows();
    let fam_inserts: Vec<String> = (0..rows)
        .map(|i| {
            let id = i + 1;
            let values: Vec<&str> = FAMILIES
                .iter()
                .map(|(_, _, v)| v.get(i).copied().unwrap_or("NULL"))
                .collect();
            format!(
                "insert into public.fam values ({id}, {}, {}, {})",
                3 - (id % 3),
                id % 3 + 1,
                values.join(", ")
            )
        })
        .chain([format!("insert into public.fam (id) values ({})", rows + 1)])
        .collect();
    let steps: Vec<(&str, Vec<String>)> = vec![
        (
            "parents",
            strings(&[
                "insert into public.parent values (1, 'p1', 'x'), (2, 'p2', 'y'), (3, 'p3', 'z')",
            ]),
        ),
        ("every family, then an all-NULL row", fam_inserts),
        (
            "a multi-row update moving group keys",
            strings(&["update public.fam set c_vchar = coalesce(c_vchar, '') || 'u', fk = fk + 1"]),
        ),
        (
            "a delete of the all-NULL row",
            vec![format!("delete from public.fam where id = {}", rows + 1)],
        ),
        (
            "a delete carrying a full image",
            strings(&["delete from public.fam where id = 2"]),
        ),
        (
            "an update that matches nothing",
            strings(&["update public.fam set c_int4 = 1 where false"]),
        ),
        (
            "a to-side update, including a column no reader reads",
            strings(&["update public.parent set name = name || '!', unread = 'w' where id <= 2"]),
        ),
        (
            "composite keys containing the separator bytes",
            strings(&[
                r"insert into public.comp values ('plain', 1, 1.5, 'n'), (E'a\x1fb', 2, 2, 'n'), (E'c\x1ed', 3, 3, 'n')",
            ]),
        ),
        (
            "composite updates, one touching only an unread column",
            strings(&[
                r"update public.comp set weight = weight * 2 where post = 2",
                r"update public.comp set note = 'm' where post = 3",
            ]),
        ),
        (
            "a composite delete",
            strings(&["delete from public.comp where post = 1"]),
        ),
        (
            "control-character keys",
            strings(&[
                r"insert into public.ctl values ('plain', 'a'), (E'a\x01b', 'b'), (E'x\x1fy', 'c'), (E'tab\tk', 'd'), (E'nl\nk', 'e'), ('', 'f')",
            ]),
        ),
        (
            "two statements on one key in one transaction",
            strings(&[
                r"update public.ctl set v = 'b2' where k = E'a\x01b'",
                r"update public.ctl set v = 'b3' where k = E'a\x01b'",
            ]),
        ),
        (
            "a primary-key move",
            strings(&["update public.ctl set k = 'moved' where k = 'plain'"]),
        ),
        (
            "a delete of a control-character key",
            strings(&[r"delete from public.ctl where k = E'nl\nk'"]),
        ),
        ("truncate", strings(&["truncate public.ctl"])),
    ];

    let mut after: i64 = raw
        .query_one(
            &format!(
                "select coalesce(max(change_id), 0) from ({}) r",
                (0..4)
                    .map(|s| format!("select change_id from {DEFAULT_SCHEMA}.seg_{s}"))
                    .collect::<Vec<_>>()
                    .join(" union all ")
            ),
            &[],
        )
        .await
        .expect("ring high-water mark")
        .get(0);
    let mut golden: Vec<String> = Vec::new();

    for (step, statements) in &steps {
        let txn = writer.transaction().await.expect("begin");
        for statement in statements {
            txn.batch_execute(statement)
                .await
                .unwrap_or_else(|e| panic!("{step}: {statement}: {e}"));
        }
        let xid: String = txn
            .query_one("select pg_current_xact_id()::text", &[])
            .await
            .expect("writer xid")
            .get(0);
        txn.commit().await.expect("commit");

        golden.push(format!("# {step}"));
        let mut total = 0;
        for (table, spec) in &specs {
            let trigger = ring_rows(&raw, after, table, spec).await;
            total += trigger.len();
            for row in &trigger {
                assert_eq!(
                    row.txid, xid,
                    "{step}: every ring row is the writer's own ({table})"
                );
                assert_eq!(
                    row.doc, row.raw_doc,
                    "{step}: a trigger images exactly its spec's columns ({table})"
                );
                assert!(row.lsn_is_origin, "{step}: lsn = origin_lsn ({table})");
                assert!(
                    row.has_lsn_and_time,
                    "{step}: lsn and src_changed are set ({table})"
                );
            }
            let mut lines: Vec<(String, String)> = trigger
                .iter()
                .map(|r| (r.key.clone(), format!("{table}\t{}\t{}", r.key_json, r.doc)))
                .collect();
            // Per key, ring order; across keys, byte order of the key.
            lines.sort_by(|a, b| a.0.as_bytes().cmp(b.0.as_bytes()));
            golden.extend(lines.into_iter().map(|(_, line)| line));
        }
        after = raw
            .query_one(
                &format!(
                    "select coalesce(max(change_id), $1) from ({}) r",
                    (0..4)
                        .map(|s| format!("select change_id from {DEFAULT_SCHEMA}.seg_{s}"))
                        .collect::<Vec<_>>()
                        .join(" union all ")
                ),
                &[&after],
            )
            .await
            .expect("ring high-water mark")
            .get(0);
        if *step == "an update that matches nothing" {
            assert_eq!(total, 0, "a statement that changed nothing stages nothing");
        } else {
            assert!(total > 0, "{step}: nothing reached the ring");
        }
    }

    let golden = golden.join("\n") + "\n";
    if std::env::var_os("TRELLIS_BLESS_CAPTURE_PARITY").is_some() {
        std::fs::create_dir_all(std::path::Path::new(FIXTURE).parent().unwrap())
            .expect("fixture dir");
        std::fs::write(FIXTURE, &golden).expect("write fixture");
    } else {
        let expected = std::fs::read_to_string(FIXTURE).expect(
            "read tests/fixtures/capture_parity.txt (bless with TRELLIS_BLESS_CAPTURE_PARITY=1)",
        );
        assert!(
            expected == golden,
            "trigger rows differ from tests/fixtures/capture_parity.txt; got:\n{golden}"
        );
    }

    drop(writer);
}
