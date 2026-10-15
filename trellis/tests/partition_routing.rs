//! ADR-0017 P4a (issue #1062): a correction routes a group to a partition
//! node by evaluating the node's recorded `pg_get_partition_constraintdef`
//! text over the group, each column cast to the source key column's type and
//! collation, under pinned settings.
//!
//! The oracle is Postgres itself: rows are inserted through the root, and the
//! leaf each landed in (`tableoid`) is the truth. For every leaf, the
//! evaluated text must hold for exactly the rows that landed in it. The
//! matrix runs `RANGE`, `LIST` and `HASH` (and a `DEFAULT` beside the first
//! two) over every accepted key type, about 50,000 comparisons per kind and
//! type pair per run, with a seeded generator so a failure is the same
//! failure next time.

use testkit::TestCluster;
use tokio_postgres::{Client, NoTls};
use trellis::capture::partitions::{Key, pinned_settings};
use trellis::staging::partition_correction::{Route, RouteColumn, route_predicate};

async fn connect(cluster: &TestCluster) -> (testkit::TestDatabase, Client) {
    let db = cluster.create_empty_database().await;
    let (client, connection) = tokio_postgres::connect(db.dsn(), NoTls)
        .await
        .expect("connect");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    (db, client)
}

/// One accepted key type: how to generate values of it, and a wider or
/// narrower type to present the group's value in (a ledger's group column
/// need not have the source key column's type).
struct Ty {
    /// The key column's type as written in `create table`.
    sql: &'static str,
    /// A SQL expression generating one value of the type, per row.
    generate: &'static str,
    /// Wraps a value of the key's type into the group's type; the engine's
    /// cast must bring it back.
    present: &'static str,
}

const TYPES: &[Ty] = &[
    Ty {
        sql: "smallint",
        generate: "(random() * 600 - 100)::int::smallint",
        present: "{}::integer",
    },
    Ty {
        sql: "integer",
        generate: "(random() * 600 - 100)::int",
        present: "{}::bigint",
    },
    Ty {
        sql: "bigint",
        generate: "((random() * 600 - 100)::bigint * 1000003)",
        present: "{}::numeric",
    },
    Ty {
        sql: "numeric(10,2)",
        generate: "round((random() * 600 - 100)::numeric, 2)",
        present: "{}::numeric",
    },
    Ty {
        sql: "text",
        generate: "md5(floor(random() * 500)::text)",
        present: "{}::varchar",
    },
    Ty {
        sql: "character varying(10)",
        generate: "substr(md5(floor(random() * 500)::text), 1, 10)",
        present: "{}::text",
    },
    Ty {
        sql: "uuid",
        generate: "md5(floor(random() * 500)::text)::uuid",
        present: "{}::uuid",
    },
    Ty {
        sql: "date",
        generate: "date '2020-01-01' + floor(random() * 700)::int",
        present: "{}::timestamp",
    },
    Ty {
        sql: "timestamp",
        generate: "date_trunc('second', timestamp '2020-01-01' + random() * interval '700 days')",
        present: "{}::timestamp",
    },
    Ty {
        sql: "timestamptz",
        generate: "date_trunc('second', timestamptz '2020-01-01 00:00:00+00' \
                   + random() * interval '700 days')",
        present: "{}::timestamptz",
    },
    Ty {
        sql: "boolean",
        generate: "random() < 0.5",
        present: "{}::boolean",
    },
];

const ROWS: i64 = 10_000;

/// The partition clauses of one kind: bounds and list values are the data's
/// own quartiles and first values, so every leaf holds rows.
async fn clauses(client: &Client, ty: &Ty, kind: &str) -> Vec<String> {
    let boolean = ty.sql == "boolean";
    match kind {
        "hash" => (0..4)
            .map(|r| format!("for values with (modulus 4, remainder {r})"))
            .collect(),
        "list" => {
            let values: Vec<String> = if boolean {
                vec!["true".to_string()]
            } else {
                client
                    .query(
                        "select format('%L', k) from (select distinct k from src order by k \
                         limit 5) s",
                        &[],
                    )
                    .await
                    .expect("list values")
                    .iter()
                    .map(|r| r.get::<_, String>(0))
                    .collect()
            };
            let mut out = Vec::new();
            if values.len() > 1 {
                out.push(format!("for values in ({})", values[..2].join(", ")));
                out.push(format!("for values in ({})", values[2..].join(", ")));
            } else {
                out.push(format!("for values in ({})", values.join(", ")));
            }
            out.push("default".to_string());
            out
        }
        _ => {
            let bounds: Vec<String> = if boolean {
                vec!["true".to_string()]
            } else {
                client
                    .query(
                        "select format('%L', b) from unnest((select percentile_disc( \
                             array[0.25, 0.5, 0.75]) within group (order by k) from src)) b",
                        &[],
                    )
                    .await
                    .expect("range bounds")
                    .iter()
                    .map(|r| r.get::<_, String>(0))
                    .collect()
            };
            let mut out = vec![format!("for values from (minvalue) to ({})", bounds[0])];
            for pair in bounds.windows(2) {
                out.push(format!("for values from ({}) to ({})", pair[0], pair[1]));
            }
            out.push(format!(
                "for values from ({}) to (maxvalue)",
                bounds[bounds.len() - 1]
            ));
            out.push("default".to_string());
            out
        }
    }
}

/// The key column's recorded type and collation, as `capture::partitions`
/// records them.
async fn recorded_key(client: &mut Client, table: &str, column: &str) -> Key {
    let txn = client.transaction().await.expect("begin");
    txn.batch_execute(&pinned_settings()).await.expect("pin");
    let row = txn
        .query_one(
            "select p.partrelid::bigint, pg_catalog.format_type(a.atttypid, a.atttypmod), \
                    case when k.collid = 0 then '' else ( \
                        select n.nspname::text || '.' || co.collname::text \
                        from pg_catalog.pg_collation co \
                        join pg_catalog.pg_namespace n on n.oid = co.collnamespace \
                        where co.oid = k.collid) end \
             from pg_catalog.pg_partitioned_table p \
             cross join lateral unnest(p.partattrs::int2[], p.partcollation::oid[]) \
                 as k(attnum, collid) \
             join pg_catalog.pg_attribute a \
                 on a.attrelid = p.partrelid and a.attnum = k.attnum \
             where p.partrelid = ('public.' || $1::text)::regclass and a.attname = $2",
            &[&table, &column],
        )
        .await
        .expect("the recorded key");
    txn.commit().await.expect("commit");
    Key {
        node: row.get::<_, i64>(0) as u32,
        column: Some(column.to_string()),
        pg_type: row.get(1),
        collation: row.get(2),
    }
}

/// The leaves of `table`: `(name, oid)`.
async fn leaves(client: &Client, table: &str) -> Vec<(String, i64)> {
    client
        .query(
            "select pg_catalog.format('%I.%I', n.nspname, c.relname), c.oid::bigint \
             from pg_catalog.pg_partition_tree(('public.' || $1::text)::regclass) t \
             join pg_catalog.pg_class c on c.oid = t.relid \
             join pg_catalog.pg_namespace n on n.oid = c.relnamespace \
             where t.isleaf order by 1",
            &[&table],
        )
        .await
        .expect("leaves")
        .iter()
        .map(|r| (r.get(0), r.get(1)))
        .collect()
}

/// `pg_get_partition_constraintdef`, fetched under the pinned settings as
/// the reconcile pass fetches it.
async fn constraint_text(client: &mut Client, oid: i64) -> Option<String> {
    let txn = client.transaction().await.expect("begin");
    txn.batch_execute(&pinned_settings()).await.expect("pin");
    let text = txn
        .query_one(
            "select pg_get_partition_constraintdef($1::bigint::oid)",
            &[&oid],
        )
        .await
        .expect("text")
        .get(0);
    txn.commit().await.expect("commit");
    text
}

/// The rows of `table` for which the route's verdict differs from the
/// truth, which is "the row is in `leaf`": evaluated under the pinned
/// settings, as the engine evaluates it.
async fn disagreements(
    client: &mut Client,
    table: &str,
    leaf: &str,
    route: &Route,
    columns: &[RouteColumn],
) -> Result<i64, tokio_postgres::Error> {
    let predicate = route_predicate(route, columns);
    let txn = client.transaction().await?;
    txn.batch_execute(&pinned_settings()).await?;
    let row = txn
        .query_one(
            &format!(
                "select count(*) from public.{table} m \
                 where ({predicate}) is distinct from (m.tableoid = '{leaf}'::regclass)"
            ),
            &[],
        )
        .await?;
    txn.commit().await?;
    Ok(row.get(0))
}

async fn count_in(client: &Client, leaf: &str) -> i64 {
    client
        .query_one(&format!("select count(*) from {leaf}"), &[])
        .await
        .expect("count")
        .get(0)
}

#[tokio::test]
async fn routing_by_the_recorded_text_agrees_with_postgres_per_kind_and_type() {
    let cluster = TestCluster::start();
    let (_db, mut client) = connect(&cluster).await;
    let mut compared: i64 = 0;
    for ty in TYPES {
        client
            .batch_execute(&format!(
                "drop table if exists src; \
                 select setseed(0.4242); \
                 create table src as select {} as k from generate_series(1, {ROWS})",
                ty.generate
            ))
            .await
            .expect("source values");
        for kind in ["range", "list", "hash"] {
            let table = format!("m_{kind}");
            let clauses = clauses(&client, ty, kind).await;
            let mut ddl = format!(
                "drop table if exists {table}; \
                 create table {table} (k {} not null, v int) partition by {kind} (k);",
                ty.sql
            );
            for (i, clause) in clauses.iter().enumerate() {
                ddl.push_str(&format!(
                    "create table {table}_{i} partition of {table} {clause};"
                ));
            }
            ddl.push_str(&format!(
                "insert into {table} select k, 0 from src; analyze {table}"
            ));
            client
                .batch_execute(&ddl)
                .await
                .unwrap_or_else(|e| panic!("{kind} over {}: {e}\n{ddl}", ty.sql));

            let key = recorded_key(&mut client, &table, "k").await;
            let mut non_empty = 0;
            for (leaf, oid) in leaves(&client, &table).await {
                let text = constraint_text(&mut client, oid).await;
                let route = text.map_or(Route::Every, Route::Text);
                // The group's value in the group's own type, brought to the
                // key's by the cast.
                let value = ty.present.replace("{}", "m.k");
                let columns = [RouteColumn {
                    name: "k".to_string(),
                    value,
                    key: Some(key.clone()),
                }];
                let wrong = disagreements(&mut client, &table, &leaf, &route, &columns)
                    .await
                    .unwrap_or_else(|e| panic!("{kind} over {}, {leaf}: {e:?}", ty.sql));
                assert_eq!(
                    wrong, 0,
                    "{kind} over {}: {wrong} rows route differently than {leaf} holds them",
                    ty.sql
                );
                compared += ROWS;
                if count_in(&client, &leaf).await > 0 {
                    non_empty += 1;
                }
            }
            // Two values reach at most two leaves of a hash.
            let at_least = if ty.sql == "boolean" { 1 } else { 2 };
            assert!(
                non_empty >= at_least,
                "{kind} over {}: the data reaches only {non_empty} leaf(s), so the check is weak",
                ty.sql
            );
        }
    }
    assert!(
        compared >= 1_000_000,
        "compared {compared} group/leaf pairs"
    );
}

#[tokio::test]
async fn a_hash_text_raises_on_a_differently_typed_column_unless_the_group_is_cast() {
    let cluster = TestCluster::start();
    let (_db, mut client) = connect(&cluster).await;
    client
        .batch_execute(
            "create table h (k integer not null) partition by hash (k); \
             create table h0 partition of h for values with (modulus 2, remainder 0); \
             create table h1 partition of h for values with (modulus 2, remainder 1); \
             insert into h select g from generate_series(1, 200) g",
        )
        .await
        .expect("hash table");
    let key = recorded_key(&mut client, "h", "k").await;
    let first = leaves(&client, "h").await[0].1;
    let text = constraint_text(&mut client, first)
        .await
        .expect("a hash text");
    let route = Route::Text(text);
    let bigint = |key: Option<Key>| {
        [RouteColumn {
            name: "k".to_string(),
            value: "m.k::bigint".to_string(),
            key,
        }]
    };
    let raised = disagreements(&mut client, "h", "public.h0", &route, &bigint(None))
        .await
        .expect_err("the uncast bigint group raises against an integer key");
    let message = raised.as_db_error().map(|e| e.message().to_string());
    assert!(
        message
            .as_deref()
            .is_some_and(|m| m.contains("supplied value is of type")),
        "{message:?}"
    );
    assert_eq!(
        disagreements(&mut client, "h", "public.h0", &route, &bigint(Some(key)))
            .await
            .expect("the cast group routes"),
        0
    );
}

#[tokio::test]
async fn a_two_level_tree_routes_a_leaf_by_every_level_and_a_dropped_middle_by_its_own_text() {
    let cluster = TestCluster::start();
    let (_db, mut client) = connect(&cluster).await;
    client
        .batch_execute(
            "create table t (a integer not null, b text not null) partition by range (a); \
             create table t_lo partition of t for values from (0) to (100) \
                 partition by hash (b); \
             create table t_lo0 partition of t_lo for values with (modulus 2, remainder 0); \
             create table t_lo1 partition of t_lo for values with (modulus 2, remainder 1); \
             create table t_hi partition of t for values from (100) to (200) \
                 partition by list (b); \
             create table t_hi_x partition of t_hi for values in ('x', 'y'); \
             create table t_hi_d partition of t_hi default; \
             select setseed(0.77); \
             insert into t select (random() * 199)::int, \
                 (array['x', 'y', 'z', 'w', 'q', 'r'])[1 + floor(random() * 6)::int] \
             from generate_series(1, 3000)",
        )
        .await
        .expect("two-level tree");
    let keys = [
        recorded_key(&mut client, "t", "a").await,
        recorded_key(&mut client, "t_lo", "b").await,
    ];
    let columns = |keys: &[Key]| -> Vec<RouteColumn> {
        vec![
            RouteColumn {
                name: "a".to_string(),
                value: "m.a".to_string(),
                key: Some(keys[0].clone()),
            },
            RouteColumn {
                name: "b".to_string(),
                value: "m.b".to_string(),
                key: keys.get(1).cloned(),
            },
        ]
    };
    for (leaf, oid) in leaves(&client, "t").await {
        let text = constraint_text(&mut client, oid)
            .await
            .expect("a text for a leaf with siblings");
        let wrong = disagreements(&mut client, "t", &leaf, &Route::Text(text), &columns(&keys))
            .await
            .unwrap_or_else(|e| panic!("{leaf}: {e:?}"));
        assert_eq!(wrong, 0, "{leaf}");
    }
    // The middle's own text, which a correction of the dropped middle uses:
    // it holds for exactly the rows its leaves hold. A leaf's hash text
    // embeds the middle's oid, which is gone once the middle is dropped.
    let (middle, oid): (String, i64) = {
        let row = client
            .query_one("select 't_lo', 'public.t_lo'::regclass::oid::bigint", &[])
            .await
            .expect("middle");
        (row.get(0), row.get(1))
    };
    let text = constraint_text(&mut client, oid)
        .await
        .expect("middle text");
    let predicate = route_predicate(&Route::Text(text), &columns(&keys));
    let txn = client.transaction().await.expect("begin");
    txn.batch_execute(&pinned_settings()).await.expect("pin");
    let wrong: i64 = txn
        .query_one(
            &format!(
                "select count(*) from public.t m where ({predicate}) is distinct from \
                 (m.tableoid in ('public.t_lo0'::regclass, 'public.t_lo1'::regclass))"
            ),
            &[],
        )
        .await
        .expect("middle check")
        .get(0);
    txn.commit().await.expect("commit");
    assert_eq!(wrong, 0, "{middle} routes the rows of its leaves");
}

/// A session with another `DateStyle` and `TimeZone` evaluates a recorded
/// text the same as the pinned settings do, because the evaluation pins them.
#[tokio::test]
async fn the_pinned_settings_make_the_evaluation_independent_of_the_session() {
    let cluster = TestCluster::start();
    let (_db, mut client) = connect(&cluster).await;
    client
        .batch_execute(
            "create table d (k date not null, ts timestamptz not null) partition by range (k); \
             create table d0 partition of d for values from ('2020-01-01') to ('2020-03-01'); \
             create table d1 partition of d for values from ('2020-03-01') to ('2020-06-30'); \
             create table dd partition of d default; \
             insert into d select date '2020-01-01' + g, timestamptz '2020-01-01+00' \
                 from generate_series(0, 300) g",
        )
        .await
        .expect("date tree");
    let key = recorded_key(&mut client, "d", "k").await;
    // The text is recorded under the pinned settings, in a session that has
    // others.
    client
        .batch_execute("set datestyle = 'German, DMY'; set timezone = 'America/Los_Angeles'")
        .await
        .expect("other settings");
    for (leaf, oid) in leaves(&client, "d").await {
        let text = constraint_text(&mut client, oid).await;
        let route = text.map_or(Route::Every, Route::Text);
        let columns = [RouteColumn {
            name: "k".to_string(),
            value: "m.k".to_string(),
            key: Some(key.clone()),
        }];
        assert_eq!(
            disagreements(&mut client, "d", &leaf, &route, &columns)
                .await
                .expect("evaluate"),
            0,
            "{leaf} under German, DMY and Los Angeles"
        );
    }
}

/// A custom operator in `public` that a session path listing `public` first
/// would pick misroutes rows; the pinned `search_path` does not see it.
#[tokio::test]
async fn an_operator_in_public_does_not_misroute_under_the_pinned_search_path() {
    let cluster = TestCluster::start();
    let (_db, mut client) = connect(&cluster).await;
    client
        .batch_execute(
            "create table o (k integer not null) partition by range (k); \
             create table o0 partition of o for values from (minvalue) to (100); \
             create table o1 partition of o for values from (100) to (maxvalue); \
             insert into o select g from generate_series(1, 300) g; \
             create function public.reversed_ge(integer, integer) returns boolean \
                 language sql immutable as 'select $1 < $2'; \
             create operator public.>= (leftarg = integer, rightarg = integer, \
                 procedure = public.reversed_ge)",
        )
        .await
        .expect("shadow operator");
    let key = recorded_key(&mut client, "o", "k").await;
    let leaves = leaves(&client, "o").await;
    let (leaf, oid) = &leaves[1];
    let text = constraint_text(&mut client, *oid).await.expect("text");
    let columns = [RouteColumn {
        name: "k".to_string(),
        value: "m.k".to_string(),
        key: Some(key),
    }];
    // Pinned: right.
    assert_eq!(
        disagreements(&mut client, "o", leaf, &Route::Text(text.clone()), &columns)
            .await
            .expect("pinned"),
        0
    );
    // A session path that lists `public` ahead of the catalog: wrong, which
    // is why the evaluation pins its own.
    let predicate = route_predicate(&Route::Text(text), &columns);
    let wrong: i64 = {
        let txn = client.transaction().await.expect("begin");
        txn.batch_execute("set local search_path = public, pg_catalog")
            .await
            .expect("session path");
        let row = txn
            .query_one(
                &format!(
                    "select count(*) from public.o m where ({predicate}) is distinct from \
                     (m.tableoid = '{leaf}'::regclass)"
                ),
                &[],
            )
            .await
            .expect("unpinned");
        txn.commit().await.expect("commit");
        row.get(0)
    };
    assert!(
        wrong > 0,
        "the shadow operator misroutes an unpinned session"
    );
}

/// A collation named like a built-in in `public`, and an enum key: the
/// recorded key names the collation by schema, and the type is qualified, so
/// the pinned evaluation holds for both.
#[tokio::test]
async fn an_enum_key_and_a_collation_in_public_route_exactly() {
    let cluster = TestCluster::start();
    let (_db, mut client) = connect(&cluster).await;
    client
        .batch_execute(
            "create type public.shade as enum ('light', 'mid', 'dark'); \
             create table e (k public.shade not null) partition by range (k); \
             create table e0 partition of e for values from (minvalue) to ('mid'); \
             create table e1 partition of e for values from ('mid') to (maxvalue); \
             insert into e select (array['light', 'mid', 'dark'])[1 + g % 3]::public.shade \
                 from generate_series(1, 300) g",
        )
        .await
        .expect("enum tree");
    let key = recorded_key(&mut client, "e", "k").await;
    assert_eq!(key.pg_type, "public.shade".to_string());
    for (leaf, oid) in leaves(&client, "e").await {
        let text = constraint_text(&mut client, oid).await;
        let route = text.map_or(Route::Every, Route::Text);
        let columns = [RouteColumn {
            name: "k".to_string(),
            value: "m.k::text::public.shade".to_string(),
            key: Some(key.clone()),
        }];
        assert_eq!(
            disagreements(&mut client, "e", &leaf, &route, &columns)
                .await
                .expect("evaluate"),
            0,
            "{leaf}"
        );
    }

    let icu: i64 = client
        .query_one(
            "select count(*) from pg_catalog.pg_collation where collprovider = 'i'",
            &[],
        )
        .await
        .expect("icu")
        .get(0);
    if icu == 0 {
        eprintln!("no ICU collations in this build: the collation shadow case is skipped");
        return;
    }
    client
        .batch_execute(
            "create collation public.\"C\" (provider = icu, locale = 'und-u-kn-true', \
                 deterministic = true); \
             create table c (k text collate public.\"C\" not null) partition by range (k); \
             create table c0 partition of c for values from (minvalue) to ('m'); \
             create table c1 partition of c for values from ('m') to (maxvalue); \
             insert into c select chr(97 + g % 26) || g::text from generate_series(1, 300) g",
        )
        .await
        .expect("collation tree");
    let key = recorded_key(&mut client, "c", "k").await;
    assert_eq!(key.collation, "public.C");
    for (leaf, oid) in leaves(&client, "c").await {
        let text = constraint_text(&mut client, oid).await;
        let route = text.map_or(Route::Every, Route::Text);
        let columns = [RouteColumn {
            name: "k".to_string(),
            value: "m.k".to_string(),
            key: Some(key.clone()),
        }];
        assert_eq!(
            disagreements(&mut client, "c", &leaf, &route, &columns)
                .await
                .expect("evaluate"),
            0,
            "{leaf}"
        );
    }
}
