//! Define accepts the shape of a partitioned source and refuses every shape
//! routing can't serve (ADR-0017, issue #1058).
//!
//! A partitioned root of a servable shape defines; each accepted definition is
//! dropped again so the next check can reuse its name. Nothing polls (#297):
//! every check is a catalog read or an `apply` call.

use testkit::{TestCluster, TestDatabase};
use tokio_postgres::{Client, NoTls};
use trellis::defs::partition_tree::{
    KeyRole, PartitionRefusal, PartitionTree, Strategy, read_partition_tree, server_version_num,
};
use trellis::{CatalogError, Config, Trellis, TrellisError, TrellisOptions};

async fn connect(dsn: &str) -> Client {
    let (client, connection) = tokio_postgres::connect(dsn, NoTls).await.expect("connect");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
}

struct Instance {
    db: TestDatabase,
    raw: Client,
    trellis: Trellis,
}

async fn instance(cluster: &TestCluster, ddl: &str) -> Instance {
    let db = cluster.create_isolated_database().await;
    let raw = connect(db.dsn()).await;
    raw.batch_execute(ddl)
        .await
        .expect("the application's tables");
    let trellis = Trellis::connect(
        Config::from_dsn(db.dsn().to_string()).expect("valid dsn"),
        TrellisOptions::default(),
    )
    .await
    .expect("connect");
    Instance { db, raw, trellis }
}

const ORDER_LINES: &str = "create table public.order_line_items ( \
        id bigint not null, order_id bigint not null, product_id bigint, amount int, \
        primary key (order_id, id)) partition by hash (order_id); \
     create table public.oli_0 partition of public.order_line_items \
        for values with (modulus 4, remainder 0); \
     create table public.oli_1 partition of public.order_line_items \
        for values with (modulus 4, remainder 1); \
     create table public.oli_2 partition of public.order_line_items \
        for values with (modulus 4, remainder 2); \
     create table public.oli_3 partition of public.order_line_items \
        for values with (modulus 4, remainder 3);";

fn aggregate(source: &str, group_by: &str) -> String {
    format!(
        "TRANSFORM totals FROM {source} GROUP BY {group_by} WITHOUT SOURCE INDEX SELECT COUNT(*) AS n"
    )
}

fn is_relationship(statement: &str) -> bool {
    statement
        .trim_start()
        .to_ascii_uppercase()
        .starts_with("RELATIONSHIP")
}

/// The catalog error `apply` (or, for a relationship, the dev door's
/// `create_relationship`) refused with.
async fn refusal(it: &Instance, statement: &str) -> CatalogError {
    if is_relationship(statement) {
        return match trellis::dev::defs::create_relationship(it.trellis.pool(), statement).await {
            Err(err) => err,
            Ok(_) => panic!("{statement} was accepted"),
        };
    }
    match it.trellis.apply(statement).await {
        Err(TrellisError::Catalog(err)) => err,
        Err(other) => panic!("expected a catalog refusal of {statement}, got {other:?}"),
        Ok(_) => panic!("{statement} was accepted"),
    }
}

/// Defines `statement`, which must be accepted, then drops what it defined.
async fn assert_accepted(it: &Instance, statement: &str) {
    if is_relationship(statement) {
        if let Err(err) =
            trellis::dev::defs::create_relationship(it.trellis.pool(), statement).await
        {
            panic!("{statement} was refused: {err:?}");
        }
    } else if let Err(err) = it.trellis.apply(statement).await {
        panic!("{statement} was refused: {err:?}");
    }
    let mut words = statement.split_whitespace();
    let drops = match (words.next(), words.next(), words.next(), words.next()) {
        (Some("TRANSFORM"), Some(name), _, _) => {
            vec![
                format!("PAUSE TRANSFORM {name}"),
                format!("DROP TRANSFORM {name}"),
            ]
        }
        (Some("RELATIONSHIP"), Some(name), Some("FROM"), Some(from)) => {
            let table = from.split_once('.').map_or(from, |(t, _)| t);
            let address = format!("{table}.{name}");
            trellis::dev::defs::drop_relationship(it.trellis.pool(), &address)
                .await
                .unwrap_or_else(|err| panic!("drop relationship {address}: {err:?}"));
            return;
        }
        _ => panic!("no drop for {statement}"),
    };
    for drop in drops {
        it.trellis
            .apply(&drop)
            .await
            .unwrap_or_else(|err| panic!("{drop}: {err:?}"));
    }
}

/// The shape refusal's message, which must name `needle`.
fn assert_refused_naming(err: &CatalogError, needle: &str) -> PartitionRefusal {
    match err {
        CatalogError::PartitionedSource(refusal) => {
            assert!(refusal.to_string().contains(needle), "{refusal}");
            refusal.clone()
        }
        other => panic!("expected a partitioned-source refusal naming {needle}, got {other:?}"),
    }
}

async fn tree(it: &Instance, table: &str) -> PartitionTree {
    read_partition_tree(&it.raw, table)
        .await
        .expect("read the tree")
        .unwrap_or_else(|| panic!("{table} is a partitioned root"))
}

#[tokio::test]
async fn the_adr_example_defines_and_grouping_by_another_column_names_the_key() {
    let cluster = TestCluster::start();
    let it = instance(&cluster, ORDER_LINES).await;

    assert_accepted(&it, &aggregate("public.order_line_items", "order_id")).await;
    assert_accepted(
        &it,
        &aggregate("public.order_line_items", "order_id, product_id"),
    )
    .await;
    let err = refusal(&it, &aggregate("public.order_line_items", "product_id")).await;
    let refused = assert_refused_naming(&err, "\"order_id\"");
    assert!(matches!(
        refused,
        PartitionRefusal::KeyNotInGroup {
            role: KeyRole::GroupBy,
            ..
        }
    ));
}

#[tokio::test]
async fn the_reader_walks_hash_range_with_default_and_two_level_trees() {
    let cluster = TestCluster::start();
    let it = instance(
        &cluster,
        &format!(
            "{ORDER_LINES} \
             create table public.events (id bigint not null, at date not null, kind text not null, \
                primary key (at, id)) partition by range (at); \
             create table public.events_2026 partition of public.events \
                for values from ('2026-01-01') to ('2027-01-01'); \
             create table public.events_other partition of public.events default; \
             create table public.metrics (day date not null, device int not null, v int, \
                primary key (day, device)) partition by range (day); \
             create table public.metrics_q1 partition of public.metrics \
                for values from ('2026-01-01') to ('2026-04-01') partition by hash (device); \
             create table public.metrics_q1_0 partition of public.metrics_q1 \
                for values with (modulus 2, remainder 0); \
             create table public.metrics_q1_1 partition of public.metrics_q1 \
                for values with (modulus 2, remainder 1); \
             create table public.metrics_q2 partition of public.metrics \
                for values from ('2026-04-01') to ('2026-07-01') partition by hash (device); \
             create table public.metrics_q2_0 partition of public.metrics_q2 \
                for values with (modulus 1, remainder 0); \
             create table public.plain (id int primary key);"
        ),
    )
    .await;

    let hash = tree(&it, "public.order_line_items").await;
    assert_eq!(hash.leaf_count(), 4);
    assert_eq!(hash.depth(), 1);
    assert!(!hash.has_default());
    assert_eq!(hash.key_columns(), vec!["order_id"]);
    assert_eq!(hash.levels[0].strategy, Strategy::Hash);
    assert_eq!(hash.nodes[0].bound, None);
    assert!(
        hash.nodes[1]
            .bound
            .as_deref()
            .unwrap()
            .contains("modulus 4"),
        "{:?}",
        hash.nodes[1].bound
    );

    let range = tree(&it, "public.events").await;
    assert_eq!(range.leaf_count(), 2);
    assert!(range.has_default());
    assert_eq!(range.levels[0].strategy, Strategy::Range);
    assert_eq!(range.key_columns(), vec!["at"]);

    let two = tree(&it, "public.metrics").await;
    assert_eq!(two.leaf_count(), 3);
    assert_eq!(two.depth(), 2);
    assert_eq!(two.levels.len(), 3);
    assert_eq!(two.key_columns(), vec!["day", "device"]);
    let q1 = two
        .nodes
        .iter()
        .position(|n| n.name == "public.metrics_q1")
        .unwrap();
    let leaf = two
        .nodes
        .iter()
        .find(|n| n.name == "public.metrics_q1_0")
        .unwrap();
    assert_eq!(leaf.parent, Some(q1));
    assert_eq!(two.nodes[q1].parent, Some(0));

    // Not a partitioned root: a plain table, a partition, a missing table.
    for not_root in [
        "public.plain",
        "public.oli_0",
        "public.metrics_q1",
        "public.nope",
    ] {
        assert!(
            read_partition_tree(&it.raw, not_root)
                .await
                .unwrap()
                .is_none(),
            "{not_root}"
        );
    }
    assert!(server_version_num(&it.raw).await.unwrap() >= 150_000);
}

#[tokio::test]
async fn a_multi_level_tree_needs_the_key_of_every_level() {
    let cluster = TestCluster::start();
    let it = instance(
        &cluster,
        "create table public.metrics (day date not null, device int not null, v int, \
            primary key (day, device)) partition by range (day); \
         create table public.metrics_q1 partition of public.metrics \
            for values from ('2026-01-01') to ('2026-04-01') partition by hash (device); \
         create table public.metrics_q1_0 partition of public.metrics_q1 \
            for values with (modulus 1, remainder 0);",
    )
    .await;
    assert_accepted(&it, &aggregate("public.metrics", "day, device")).await;
    // The second level's column is the one named.
    assert_refused_naming(
        &refusal(&it, &aggregate("public.metrics", "day")).await,
        "\"device\"",
    );
    assert_refused_naming(
        &refusal(&it, &aggregate("public.metrics", "device")).await,
        "\"day\"",
    );
}

#[tokio::test]
async fn a_range_tree_with_a_default_partition_defines() {
    let cluster = TestCluster::start();
    let it = instance(
        &cluster,
        "create table public.events (id bigint not null, at date not null, \
            primary key (at, id)) partition by range (at); \
         create table public.events_2026 partition of public.events \
            for values from ('2026-01-01') to ('2027-01-01'); \
         create table public.events_other partition of public.events default;",
    )
    .await;
    assert_accepted(&it, &aggregate("public.events", "at")).await;
}

#[tokio::test]
async fn a_key_type_off_the_list_names_the_column() {
    let cluster = TestCluster::start();
    let it = instance(
        &cluster,
        "create type public.mood as enum ('a', 'b'); \
         create table public.t (k oid not null, id int not null, primary key (k, id)) \
            partition by hash (k); \
         create table public.t_0 partition of public.t \
            for values with (modulus 1, remainder 0); \
         create table public.u (k public.mood not null, id int not null, primary key (k, id)) \
            partition by list (k); \
         create table public.u_a partition of public.u for values in ('a');",
    )
    .await;
    for (table, column, pg_type) in [("public.t", "k", "oid"), ("public.u", "k", "mood")] {
        let err = refusal(&it, &aggregate(table, column)).await;
        let refused = assert_refused_naming(&err, "\"k\"");
        assert!(
            matches!(&refused, PartitionRefusal::UnsupportedKeyType { pg_type: t, .. } if t.ends_with(pg_type)),
            "{refused}"
        );
    }
}

#[tokio::test]
async fn every_listed_key_type_a_primary_key_can_hold_passes_the_type_check() {
    // `numeric` and `boolean` are listed too, but a source's primary key must
    // hold every partition-key column and isn't allowed those types, so a
    // table with one is refused for its key first; the unit tests cover them.
    let cluster = TestCluster::start();
    let types = [
        "smallint",
        "integer",
        "bigint",
        "text",
        "varchar(10)",
        "uuid",
        "date",
        "timestamp",
        "timestamptz",
    ];
    let mut ddl = String::new();
    for (i, ty) in types.iter().enumerate() {
        ddl.push_str(&format!(
            "create table public.k{i} (k {ty} not null, id int not null, \
                primary key (k, id)) partition by hash (k); \
             create table public.k{i}_0 partition of public.k{i} \
                for values with (modulus 1, remainder 0);"
        ));
    }
    let it = instance(&cluster, &ddl).await;
    for i in 0..types.len() {
        let table = format!("public.k{i}");
        assert_accepted(&it, &aggregate(&table, "k")).await;
    }
}

#[tokio::test]
async fn a_non_default_opclass_and_a_nondeterministic_collation_name_the_column() {
    let cluster = TestCluster::start();
    let it = instance(
        &cluster,
        "create table public.t (k text not null, id int not null, primary key (k, id)) \
            partition by range (k text_pattern_ops); \
         create table public.t_a partition of public.t for values from ('a') to ('b'); \
         create collation public.ci (provider = icu, locale = 'und-u-ks-level2', \
            deterministic = false); \
         create table public.c (k text collate public.ci not null, id int not null, \
            primary key (k, id)) partition by list (k); \
         create table public.c_a partition of public.c for values in ('a');",
    )
    .await;
    let err = refusal(&it, &aggregate("public.t", "k")).await;
    let refused = assert_refused_naming(&err, "\"k\"");
    assert!(matches!(
        refused,
        PartitionRefusal::NonDefaultOpclass { .. }
    ));

    let err = refusal(&it, &aggregate("public.c", "k")).await;
    let refused = assert_refused_naming(&err, "\"k\"");
    assert!(matches!(
        refused,
        PartitionRefusal::NondeterministicCollation { .. }
    ));
}

#[tokio::test]
async fn a_tree_over_the_leaf_limit_is_refused_stating_the_limit() {
    let cluster = TestCluster::start();
    let it = instance(
        &cluster,
        "create table public.big (k int not null, id int not null, primary key (k, id)) \
            partition by hash (k); \
         create table public.small (k int not null, id int not null, primary key (k, id)) \
            partition by hash (k); \
         do $$ begin \
            for i in 0..1000 loop \
                execute format('create table public.big_%s partition of public.big \
                    for values with (modulus 1001, remainder %s)', i, i); \
            end loop; \
            for i in 0..999 loop \
                execute format('create table public.small_%s partition of public.small \
                    for values with (modulus 1000, remainder %s)', i, i); \
            end loop; \
         end $$;",
    )
    .await;
    // 1,000 leaves is the limit, inclusive.
    assert_accepted(&it, &aggregate("public.small", "k")).await;
    let err = refusal(&it, &aggregate("public.big", "k")).await;
    let refused = assert_refused_naming(&err, "1001 partitions");
    assert!(refused.to_string().contains("1000 leaves"), "{refused}");
}

#[tokio::test]
async fn a_partition_a_hierarchy_and_a_one_to_one_from_stay_refused() {
    let cluster = TestCluster::start();
    let it = instance(
        &cluster,
        &format!(
            "{ORDER_LINES} \
             create table public.base (id int primary key, g int); \
             create table public.kid (extra int) inherits (public.base);"
        ),
    )
    .await;
    // A partition named as a source.
    for source in ["public.oli_0"] {
        match refusal(&it, &aggregate(source, "order_id")).await {
            CatalogError::SourceNotChangeKeyed { source_table } => {
                assert_eq!(source_table, source)
            }
            other => panic!("expected SourceNotChangeKeyed, got {other:?}"),
        }
    }
    // An inheritance parent.
    assert!(matches!(
        refusal(&it, &aggregate("public.base", "g")).await,
        CatalogError::SourceNotChangeKeyed { .. }
    ));
    // A 1-1 transform's FROM.
    let err = refusal(
        &it,
        "TRANSFORM copy FROM public.order_line_items SELECT amount AS amount",
    )
    .await;
    let refused = assert_refused_naming(&err, "public.order_line_items");
    assert!(matches!(refused, PartitionRefusal::OneToOneFrom { .. }));
    // A root that is itself a partition.
    it.raw
        .batch_execute(
            "create table public.top (order_id bigint not null, id bigint not null, \
                product_id bigint, amount int, primary key (order_id, id)) partition by range (order_id); \
             alter table public.top attach partition public.order_line_items \
                for values from (0) to (1000)",
        )
        .await
        .unwrap();
    assert!(matches!(
        refusal(&it, &aggregate("public.order_line_items", "order_id")).await,
        CatalogError::SourceNotChangeKeyed { .. }
    ));
}

#[tokio::test]
async fn a_to_many_to_side_follows_the_same_rules() {
    let cluster = TestCluster::start();
    let it = instance(
        &cluster,
        &format!(
            "{ORDER_LINES} \
             create table public.orders (id bigint primary key); \
             create table public.by_id (id bigint primary key, tag text) \
                partition by hash (id); \
             create table public.by_id_0 partition of public.by_id \
                for values with (modulus 1, remainder 0);"
        ),
    )
    .await;
    // To-many onto the partition key: passes the shape checks.
    assert_accepted(
        &it,
        "RELATIONSHIP lines FROM orders.id TO order_line_items.order_id",
    )
    .await;
    // To-many onto another column.
    assert_refused_naming(
        &refusal(
            &it,
            "RELATIONSHIP lines FROM orders.id TO order_line_items.product_id",
        )
        .await,
        "\"order_id\"",
    );
    // A to-one to-side stays refused.
    let err = refusal(&it, "RELATIONSHIP tag FROM orders.id TO by_id.id").await;
    let refused = assert_refused_naming(&err, "public.by_id");
    assert!(matches!(refused, PartitionRefusal::ToOneToSide { .. }));
    // A partitioned from-side stays refused as a plain endpoint.
    assert!(matches!(
        refusal(
            &it,
            "RELATIONSHIP owner FROM order_line_items.order_id TO orders.id"
        )
        .await,
        CatalogError::RelationshipEndpointNotChangeKeyed { .. }
    ));
    let _ = &it.db;
}

/// A partitioned root is held to the plain-table rule's primary key: one
/// keyed only by a unique index on not-null columns, which
/// `ddl::source_primary_key` would take, is refused before its shape is
/// read, as a source and as a to-many to-side.
#[tokio::test]
async fn a_partitioned_root_without_a_primary_key_is_refused_as_unkeyed() {
    let cluster = TestCluster::start();
    let it = instance(
        &cluster,
        "create table public.lines (id bigint not null, order_id bigint not null, \
            amount int, unique (order_id, id)) partition by hash (order_id); \
         create table public.lines_0 partition of public.lines \
            for values with (modulus 1, remainder 0); \
         create table public.orders (id bigint primary key);",
    )
    .await;
    match refusal(&it, &aggregate("public.lines", "order_id")).await {
        CatalogError::SourceNotChangeKeyed { source_table } => {
            assert_eq!(source_table, "public.lines")
        }
        other => panic!("expected SourceNotChangeKeyed, got {other:?}"),
    }
    assert!(matches!(
        refusal(&it, "RELATIONSHIP lines FROM orders.id TO lines.order_id").await,
        CatalogError::RelationshipEndpointNotChangeKeyed { .. }
    ));
    let _ = &it.db;
}

/// A `JOIN` side's partial key is its own `GROUP BY` columns plus its join
/// key (ADR-0019), and the group-key rule applies to it on either side: a
/// fact table partitioned on the column it joins by needs nothing in its
/// `GROUP BY`; one joined by another column needs the partition key grouped;
/// a dimension partitioned on a column outside its join key and `GROUP BY`
/// is refused, naming the column (#1111).
#[tokio::test]
async fn a_join_side_is_held_to_the_rule_over_its_partial_key() {
    let cluster = TestCluster::start();
    let it = instance(
        &cluster,
        &format!(
            "{ORDER_LINES} \
             create table public.orders (id bigint primary key, customer_id bigint, zone text); \
             create table public.products (id bigint primary key, name text); \
             create index on public.order_line_items (product_id); \
             create index on public.orders (customer_id); \
             create table public.customers (id bigint not null, region text not null, \
                primary key (region, id)) partition by list (region); \
             create table public.customers_eu partition of public.customers \
                for values in ('eu'); \
             create index on public.customers (id);"
        ),
    )
    .await;
    let join = |from: &str, on: &str, group_by: &str| {
        let fields: Vec<String> = group_by
            .split(", ")
            .map(|c| format!("{c} AS {}", c.split_once('.').map_or(c, |(_, b)| b)))
            .collect();
        format!(
            "TRANSFORM j FROM {from} ON {on} GROUP BY {group_by} WITHOUT SOURCE INDEX SELECT {}, COUNT(*) AS n",
            fields.join(", ")
        )
    };
    // The fact side joins by its partition key.
    assert_accepted(
        &it,
        &join(
            "order_line_items LEFT JOIN orders",
            "order_line_items.order_id = orders.id",
            "orders.zone",
        ),
    )
    .await;
    // It joins by another column: refused, unless the key is grouped.
    let err = refusal(
        &it,
        &join(
            "order_line_items JOIN products",
            "order_line_items.product_id = products.id",
            "products.name",
        ),
    )
    .await;
    let refused = assert_refused_naming(&err, "\"order_id\"");
    assert!(matches!(
        refused,
        PartitionRefusal::KeyNotInGroup {
            role: KeyRole::GroupBy,
            ..
        }
    ));
    assert_accepted(
        &it,
        &join(
            "order_line_items JOIN products",
            "order_line_items.product_id = products.id",
            "order_line_items.order_id, products.name",
        ),
    )
    .await;
    // The dimension side: its partition key grouped, or not.
    assert_accepted(
        &it,
        &join(
            "orders JOIN customers",
            "orders.customer_id = customers.id",
            "customers.region",
        ),
    )
    .await;
    let err = refusal(
        &it,
        &join(
            "orders LEFT JOIN customers",
            "orders.customer_id = customers.id",
            "orders.zone",
        ),
    )
    .await;
    assert_refused_naming(&err, "\"region\"");
    assert_refused_naming(&err, "public.customers");
    let _ = &it.db;
}
