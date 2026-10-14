//! The catalog reader for a declaratively partitioned table's tree, and the
//! define-time checks of whether the tree can be a source (ADR-0017).
//!
//! A partitioned root is a source only when its `GROUP BY` (or a to-many
//! relationship's to-side join key) holds every partition-key column at every
//! level, so each group lives in exactly one leaf and a lifecycle change
//! touches only the groups routing to one node. [`read_partition_tree`] walks
//! the tree through `pg_inherits` and `pg_partitioned_table`;
//! [`PartitionTree::refusal`] is the pure decision over what it read, so the
//! rules are testable without a database.
//!
//! The reader is shared: capture installation, the reconcile pass and the
//! corrections read the same [`PartitionTree`], so every consumer agrees on
//! what a node, a level and a key column are.
//!
//! # Interface
//!
//! - [`read_partition_tree`] returns `Some` for a partitioned root that is not
//!   itself a partition and is in no inheritance hierarchy, and `None` for any
//!   other table (including one that doesn't exist).
//! - A [`PartitionTree`] lists its [`PartitionNode`]s root first, then by
//!   depth and name. A node's `parent` is an index into that list. `levels`
//!   holds one [`PartitionLevel`] per partitioned node (the root, each
//!   middle), with one [`KeyColumn`] per partition-key position.
//! - [`PartitionTree::refusal`] returns the first [`PartitionRefusal`], in a
//!   fixed order: server version, leaf count, then each level top-down (an
//!   expression key, an unsupported type, a non-default operator class, a
//!   nondeterministic collation), then each level top-down again for a
//!   column missing from the key.

use std::collections::HashMap;
use std::fmt;

use tokio_postgres::GenericClient;

use crate::defs::ddl::regclass_arg;

/// The largest tree a definition may read, in leaves. No build statement
/// prunes, so each one plans, locks and opens every leaf; the default lock
/// table runs out for one statement somewhere above this.
pub const MAX_LEAVES: usize = 1_000;

/// The oldest server a partitioned source is read on.
pub const MIN_SERVER_VERSION_NUM: i32 = 150_000;

/// The partition-key column types a partitioned source accepts, as
/// `format_type` spells them.
pub const SUPPORTED_KEY_TYPES: &[&str] = &[
    "smallint",
    "integer",
    "bigint",
    "numeric",
    "text",
    "character varying",
    "uuid",
    "date",
    "timestamp without time zone",
    "timestamp with time zone",
    "boolean",
];

/// How a node partitions its children.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Strategy {
    Range,
    List,
    Hash,
}

/// One table in the tree.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionNode {
    /// The table's oid.
    pub oid: u32,
    /// Unquoted `schema.table`.
    pub name: String,
    /// Index into [`PartitionTree::nodes`] of the parent, `None` for the root.
    pub parent: Option<usize>,
    /// 0 for the root.
    pub depth: u32,
    /// Whether the node is partitioned itself (the root and each middle).
    pub partitioned: bool,
    /// Whether this is a `DEFAULT` partition.
    pub is_default: bool,
    /// The partition bound text (`pg_get_expr(relpartbound)`), `None` for the
    /// root.
    pub bound: Option<String>,
}

/// One partition-key position of a partitioned node.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeyColumn {
    /// The column name, `None` when the position is an expression.
    pub column: Option<String>,
    /// The column's type as `format_type` spells it (empty for an expression).
    pub pg_type: String,
    /// Whether the partition operator class is the type's default.
    pub default_opclass: bool,
    /// Whether the partition collation is nondeterministic.
    pub nondeterministic_collation: bool,
}

/// One partitioned node's key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionLevel {
    /// Index into [`PartitionTree::nodes`].
    pub node: usize,
    pub strategy: Strategy,
    pub key: Vec<KeyColumn>,
}

/// A partitioned root and everything under it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionTree {
    pub nodes: Vec<PartitionNode>,
    pub levels: Vec<PartitionLevel>,
}

impl PartitionTree {
    /// The root's `schema.table`.
    pub fn root(&self) -> &str {
        &self.nodes[0].name
    }

    /// The number of leaves (nodes that aren't partitioned).
    pub fn leaf_count(&self) -> usize {
        self.nodes.iter().filter(|n| !n.partitioned).count()
    }

    /// The deepest node's depth: 1 for a root of plain leaves.
    #[allow(dead_code)] // read by capture install and the reconcile pass (ADR-0017 P2/P3)
    pub fn depth(&self) -> u32 {
        self.nodes.iter().map(|n| n.depth).max().unwrap_or(0)
    }

    /// Whether a `DEFAULT` partition sits anywhere in the tree.
    #[allow(dead_code)] // read by capture install and the reconcile pass (ADR-0017 P2/P3)
    pub fn has_default(&self) -> bool {
        self.nodes.iter().any(|n| n.is_default)
    }

    /// Every partition-key column name at every level, each once, in the
    /// order first met top-down.
    #[allow(dead_code)] // read by capture install and the reconcile pass (ADR-0017 P2/P3)
    pub fn key_columns(&self) -> Vec<&str> {
        let mut out: Vec<&str> = Vec::new();
        for level in &self.levels {
            for key in &level.key {
                if let Some(column) = key.column.as_deref()
                    && !out.contains(&column)
                {
                    out.push(column);
                }
            }
        }
        out
    }

    /// The first reason this tree can't be read by a definition whose group
    /// key (or to-many join key) is `group_key`, or `None` when it can. See
    /// the module doc for the order of the checks.
    pub fn refusal(
        &self,
        server_version_num: i32,
        role: KeyRole,
        group_key: &[String],
    ) -> Option<PartitionRefusal> {
        if server_version_num < MIN_SERVER_VERSION_NUM {
            return Some(PartitionRefusal::ServerTooOld {
                table: self.root().to_string(),
                server_version_num,
            });
        }
        let leaves = self.leaf_count();
        if leaves > MAX_LEAVES {
            return Some(PartitionRefusal::TooManyLeaves {
                table: self.root().to_string(),
                leaves,
            });
        }
        for level in &self.levels {
            let table = self.nodes[level.node].name.clone();
            for (position, key) in level.key.iter().enumerate() {
                let Some(column) = key.column.clone() else {
                    return Some(PartitionRefusal::ExpressionKey {
                        table,
                        position: position + 1,
                    });
                };
                if !SUPPORTED_KEY_TYPES.contains(&key.pg_type.as_str()) {
                    return Some(PartitionRefusal::UnsupportedKeyType {
                        table,
                        column,
                        pg_type: key.pg_type.clone(),
                    });
                }
                if !key.default_opclass {
                    return Some(PartitionRefusal::NonDefaultOpclass { table, column });
                }
                if key.nondeterministic_collation {
                    return Some(PartitionRefusal::NondeterministicCollation { table, column });
                }
            }
        }
        for level in &self.levels {
            for key in &level.key {
                let column = key.column.as_deref().unwrap_or_default();
                if !group_key.iter().any(|g| g == column) {
                    return Some(PartitionRefusal::KeyNotInGroup {
                        table: self.nodes[level.node].name.clone(),
                        column: column.to_string(),
                        role,
                    });
                }
            }
        }
        None
    }
}

/// Which key the partition-key columns must all be in.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KeyRole {
    /// An aggregate's `GROUP BY`.
    GroupBy,
    /// A to-many relationship's to-side join key.
    JoinKey,
}

/// Why a partitioned source can't be read. Every message names the column or
/// the limit.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PartitionRefusal {
    ServerTooOld {
        table: String,
        server_version_num: i32,
    },
    TooManyLeaves {
        table: String,
        leaves: usize,
    },
    ExpressionKey {
        table: String,
        position: usize,
    },
    UnsupportedKeyType {
        table: String,
        column: String,
        pg_type: String,
    },
    NonDefaultOpclass {
        table: String,
        column: String,
    },
    NondeterministicCollation {
        table: String,
        column: String,
    },
    KeyNotInGroup {
        table: String,
        column: String,
        role: KeyRole,
    },
    /// A 1-1 transform's `FROM`.
    OneToOneFrom {
        table: String,
    },
    /// The to-side of a to-one relationship.
    ToOneToSide {
        table: String,
    },
}

impl fmt::Display for PartitionRefusal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PartitionRefusal::ServerTooOld {
                table,
                server_version_num,
            } => write!(
                f,
                "partitioned table {table} can't be read: Trellis reads partitioned tables on \
                 PostgreSQL 15 or later, and this server is version {server_version_num}"
            ),
            PartitionRefusal::TooManyLeaves { table, leaves } => write!(
                f,
                "partitioned table {table} has {leaves} partitions, and Trellis reads a \
                 partition tree of at most {MAX_LEAVES} leaves"
            ),
            PartitionRefusal::ExpressionKey { table, position } => write!(
                f,
                "partitioned table {table} is partitioned by an expression (key position \
                 {position}), which names no column; Trellis reads a partitioned table whose \
                 partition keys are plain columns"
            ),
            PartitionRefusal::UnsupportedKeyType {
                table,
                column,
                pg_type,
            } => write!(
                f,
                "partitioned table {table} is partitioned by column \"{column}\" of type \
                 {pg_type}, which isn't a supported partition-key type; supported types are {}",
                SUPPORTED_KEY_TYPES.join(", ")
            ),
            PartitionRefusal::NonDefaultOpclass { table, column } => write!(
                f,
                "partitioned table {table} partitions column \"{column}\" with a non-default \
                 operator class; Trellis reads only the type's default"
            ),
            PartitionRefusal::NondeterministicCollation { table, column } => write!(
                f,
                "partitioned table {table} partitions column \"{column}\" with a \
                 nondeterministic collation; Trellis routes groups to partitions by exact value"
            ),
            PartitionRefusal::KeyNotInGroup {
                table,
                column,
                role: KeyRole::GroupBy,
            } => write!(
                f,
                "partitioned table {table} is partitioned by \"{column}\", which the GROUP BY \
                 doesn't contain; a partitioned source's GROUP BY must contain every \
                 partition-key column at every level, so each group lives in one partition. \
                 Add \"{column}\" to the GROUP BY"
            ),
            PartitionRefusal::KeyNotInGroup {
                table,
                column,
                role: KeyRole::JoinKey,
            } => write!(
                f,
                "partitioned table {table} is partitioned by \"{column}\", which isn't the \
                 relationship's join column; a partitioned to-side's join column must be \
                 every partition-key column at every level, so each parent's children live in \
                 one partition"
            ),
            PartitionRefusal::OneToOneFrom { table } => write!(
                f,
                "partitioned table {table} can't be the source of a 1-1 transform; Trellis \
                 reads a partitioned table only as an aggregate's FROM or a to-many \
                 relationship's to-side"
            ),
            PartitionRefusal::ToOneToSide { table } => write!(
                f,
                "partitioned table {table} can't be the to-side of a to-one relationship; \
                 Trellis reads a partitioned table only as an aggregate's FROM or a to-many \
                 relationship's to-side"
            ),
        }
    }
}

/// Reads the partition tree under `table` (an unquoted `schema.table`). `None`
/// when it isn't a partitioned root: it doesn't exist, isn't partitioned, is
/// itself a partition, or is in an inheritance hierarchy.
pub async fn read_partition_tree(
    client: &impl GenericClient,
    table: &str,
) -> Result<Option<PartitionTree>, tokio_postgres::Error> {
    let arg = regclass_arg(table);
    let root_ok: bool = client
        .query_one(
            "select coalesce((select c.relkind = 'p' and not c.relispartition \
                    and not exists (select 1 from pg_catalog.pg_inherits h \
                                    where h.inhrelid = c.oid) \
                    and not exists (select 1 from pg_catalog.pg_inherits h \
                                    join pg_catalog.pg_class k on k.oid = h.inhrelid \
                                    where h.inhparent = c.oid and not k.relispartition) \
             from pg_catalog.pg_class c where c.oid = pg_catalog.to_regclass($1)), false)",
            &[&arg],
        )
        .await?
        .get(0);
    if !root_ok {
        return Ok(None);
    }

    let rows = client
        .query(
            "with recursive t(oid, parent, depth) as ( \
                 select pg_catalog.to_regclass($1)::oid, null::oid, 0 \
                 union all \
                 select i.inhrelid, t.oid, t.depth + 1 \
                 from t join pg_catalog.pg_inherits i on i.inhparent = t.oid) \
             select t.oid::bigint, n.nspname::text || '.' || c.relname::text, \
                    t.parent::bigint, t.depth, c.relkind = 'p', \
                    pg_catalog.pg_get_expr(c.relpartbound, c.oid) \
             from t join pg_catalog.pg_class c on c.oid = t.oid \
             join pg_catalog.pg_namespace n on n.oid = c.relnamespace \
             order by t.depth, 2",
            &[&arg],
        )
        .await?;
    let mut nodes: Vec<PartitionNode> = Vec::with_capacity(rows.len());
    let mut parents: Vec<Option<i64>> = Vec::with_capacity(rows.len());
    for row in &rows {
        let bound: Option<String> = row.get(5);
        nodes.push(PartitionNode {
            oid: row.get::<_, i64>(0) as u32,
            name: row.get(1),
            parent: None,
            depth: row.get::<_, i32>(3) as u32,
            partitioned: row.get(4),
            is_default: bound.as_deref() == Some("DEFAULT"),
            bound,
        });
        parents.push(row.get(2));
    }
    // By oid, so a tree well over the leaf limit is still read in linear time
    // before `refusal` turns it away.
    let index: HashMap<u32, usize> = nodes.iter().enumerate().map(|(i, n)| (n.oid, i)).collect();
    for (i, parent) in parents.into_iter().enumerate() {
        nodes[i].parent = parent.and_then(|p| index.get(&(p as u32)).copied());
    }

    let oids: Vec<i64> = nodes
        .iter()
        .filter(|n| n.partitioned)
        .map(|n| i64::from(n.oid))
        .collect();
    let key_rows = client
        .query(
            "select p.partrelid::bigint, p.partstrat::text, k.ord, \
                    a.attname::text, \
                    coalesce(pg_catalog.format_type(a.atttypid, null), ''), \
                    coalesce(o.opcdefault, true), \
                    coalesce(not co.collisdeterministic, false) \
             from pg_catalog.pg_partitioned_table p \
             cross join lateral unnest(p.partattrs::int2[], p.partclass::oid[], \
                                       p.partcollation::oid[]) \
                 with ordinality as k(attnum, opcid, collid, ord) \
             left join pg_catalog.pg_attribute a \
                 on a.attrelid = p.partrelid and a.attnum = k.attnum and k.attnum > 0 \
             left join pg_catalog.pg_opclass o on o.oid = k.opcid \
             left join pg_catalog.pg_collation co on co.oid = k.collid \
             where p.partrelid = any($1::bigint[]::oid[]) \
             order by 1, k.ord",
            &[&oids],
        )
        .await?;
    let mut levels: Vec<PartitionLevel> = Vec::new();
    for row in &key_rows {
        let oid = row.get::<_, i64>(0) as u32;
        let Some(&node) = index.get(&oid) else {
            continue;
        };
        let strategy = match row.get::<_, &str>(1) {
            "l" => Strategy::List,
            "h" => Strategy::Hash,
            _ => Strategy::Range,
        };
        let column: Option<String> = row.get(3);
        let key = KeyColumn {
            column,
            pg_type: row.get(4),
            default_opclass: row.get(5),
            nondeterministic_collation: row.get(6),
        };
        match levels.last_mut() {
            Some(level) if level.node == node => level.key.push(key),
            _ => levels.push(PartitionLevel {
                node,
                strategy,
                key: vec![key],
            }),
        }
    }
    // Level order is by node depth, root first.
    levels.sort_by_key(|l| (nodes[l.node].depth, l.node));
    Ok(Some(PartitionTree { nodes, levels }))
}

/// The server's `server_version_num`.
pub async fn server_version_num(client: &impl GenericClient) -> Result<i32, tokio_postgres::Error> {
    let setting: String = client
        .query_one("select current_setting('server_version_num')", &[])
        .await?
        .get(0);
    Ok(setting.parse().unwrap_or(0))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(column: &str, pg_type: &str) -> KeyColumn {
        KeyColumn {
            column: Some(column.to_string()),
            pg_type: pg_type.to_string(),
            default_opclass: true,
            nondeterministic_collation: false,
        }
    }

    fn node(name: &str, depth: u32, partitioned: bool) -> PartitionNode {
        PartitionNode {
            oid: 0,
            name: name.to_string(),
            parent: if depth == 0 { None } else { Some(0) },
            depth,
            partitioned,
            is_default: false,
            bound: None,
        }
    }

    /// A root by `keys`, with `leaves` plain leaves.
    fn flat(keys: Vec<KeyColumn>, leaves: usize) -> PartitionTree {
        let mut nodes = vec![node("public.t", 0, true)];
        for i in 0..leaves {
            nodes.push(node(&format!("public.t_{i}"), 1, false));
        }
        PartitionTree {
            nodes,
            levels: vec![PartitionLevel {
                node: 0,
                strategy: Strategy::Hash,
                key: keys,
            }],
        }
    }

    fn group(columns: &[&str]) -> Vec<String> {
        columns.iter().map(|c| c.to_string()).collect()
    }

    #[test]
    fn a_group_key_holding_the_partition_key_passes() {
        let tree = flat(vec![key("order_id", "bigint")], 4);
        assert_eq!(
            tree.refusal(150_000, KeyRole::GroupBy, &group(&["order_id", "status"])),
            None
        );
        assert_eq!(tree.leaf_count(), 4);
        assert_eq!(tree.depth(), 1);
        assert_eq!(tree.key_columns(), vec!["order_id"]);
    }

    #[test]
    fn a_group_key_missing_the_partition_key_names_the_column() {
        let tree = flat(vec![key("order_id", "bigint")], 4);
        let refusal = tree
            .refusal(160_000, KeyRole::GroupBy, &group(&["product_id"]))
            .unwrap();
        assert!(refusal.to_string().contains("\"order_id\""), "{refusal}");
        assert!(refusal.to_string().contains("GROUP BY"), "{refusal}");
    }

    #[test]
    fn every_level_of_a_multi_level_tree_must_be_in_the_key() {
        let mut tree = flat(vec![key("created_on", "date")], 2);
        tree.nodes[1].partitioned = true;
        tree.levels.push(PartitionLevel {
            node: 1,
            strategy: Strategy::Hash,
            key: vec![key("order_id", "bigint")],
        });
        assert_eq!(
            tree.refusal(
                150_000,
                KeyRole::GroupBy,
                &group(&["created_on", "order_id"])
            ),
            None
        );
        let refusal = tree
            .refusal(150_000, KeyRole::GroupBy, &group(&["created_on"]))
            .unwrap();
        assert!(refusal.to_string().contains("\"order_id\""), "{refusal}");
        assert!(refusal.to_string().contains("public.t_0"), "{refusal}");
        assert_eq!(tree.key_columns(), vec!["created_on", "order_id"]);
    }

    #[test]
    fn a_composite_key_needs_every_column() {
        let tree = flat(vec![key("a", "integer"), key("b", "integer")], 2);
        let refusal = tree
            .refusal(150_000, KeyRole::JoinKey, &group(&["a"]))
            .unwrap();
        assert!(refusal.to_string().contains("\"b\""), "{refusal}");
        assert!(refusal.to_string().contains("join column"), "{refusal}");
    }

    #[test]
    fn an_expression_key_names_the_table_and_position() {
        let mut expr = key("x", "integer");
        expr.column = None;
        let tree = flat(vec![key("a", "integer"), expr], 2);
        let refusal = tree
            .refusal(150_000, KeyRole::GroupBy, &group(&["a"]))
            .unwrap();
        assert_eq!(
            refusal,
            PartitionRefusal::ExpressionKey {
                table: "public.t".to_string(),
                position: 2
            }
        );
        assert!(refusal.to_string().contains("position 2"), "{refusal}");
    }

    #[test]
    fn an_unsupported_key_type_names_the_column_and_type() {
        let tree = flat(vec![key("code", "character")], 2);
        let refusal = tree
            .refusal(150_000, KeyRole::GroupBy, &group(&["code"]))
            .unwrap();
        let text = refusal.to_string();
        assert!(
            text.contains("\"code\"") && text.contains("character,"),
            "{text}"
        );
        for supported in SUPPORTED_KEY_TYPES {
            let tree = flat(vec![key("k", supported)], 2);
            assert_eq!(
                tree.refusal(150_000, KeyRole::GroupBy, &group(&["k"])),
                None,
                "{supported}"
            );
        }
    }

    #[test]
    fn a_non_default_opclass_and_a_nondeterministic_collation_name_the_column() {
        let mut opclass = key("k", "text");
        opclass.default_opclass = false;
        let refusal = flat(vec![opclass], 2)
            .refusal(150_000, KeyRole::GroupBy, &group(&["k"]))
            .unwrap();
        assert!(refusal.to_string().contains("\"k\""), "{refusal}");
        assert!(refusal.to_string().contains("operator class"), "{refusal}");

        let mut collation = key("k", "text");
        collation.nondeterministic_collation = true;
        let refusal = flat(vec![collation], 2)
            .refusal(150_000, KeyRole::GroupBy, &group(&["k"]))
            .unwrap();
        assert!(refusal.to_string().contains("\"k\""), "{refusal}");
        assert!(
            refusal.to_string().contains("nondeterministic"),
            "{refusal}"
        );
    }

    #[test]
    fn the_leaf_limit_is_inclusive_and_the_message_states_it() {
        let at = flat(vec![key("k", "integer")], MAX_LEAVES);
        assert_eq!(at.refusal(150_000, KeyRole::GroupBy, &group(&["k"])), None);
        let over = flat(vec![key("k", "integer")], MAX_LEAVES + 1);
        let refusal = over
            .refusal(150_000, KeyRole::GroupBy, &group(&["k"]))
            .unwrap();
        let text = refusal.to_string();
        assert!(text.contains("1001") && text.contains("1000"), "{text}");
    }

    #[test]
    fn a_server_before_15_is_refused_first() {
        let tree = flat(vec![key("k", "text")], MAX_LEAVES + 1);
        let refusal = tree
            .refusal(140_011, KeyRole::GroupBy, &group(&["x"]))
            .unwrap();
        assert!(refusal.to_string().contains("PostgreSQL 15"), "{refusal}");
    }
}
