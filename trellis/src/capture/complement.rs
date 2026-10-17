//! The every-pass sweep of a partitioned source's rows that route to no
//! current leaf (ADR-0017, P7 of epic #897).
//!
//! A leaf created, written and dropped between two reconcile passes changes
//! neither fingerprint the pass compares ([`super::partitions`]), so no
//! correction is queued for it, and the groups its rows reached stay counted.
//! Those groups lie where no current leaf takes a row: in the complement of
//! the tree's bounds. Every pass therefore looks for target groups there and,
//! when it finds any, queues a correction of the whole complement, which the
//! correction engine ([`crate::staging::partition_correction`]) runs like a
//! gone node's: it re-derives the keys of the live ledger entries whose group
//! is in it, through the normal page path.
//!
//! # The complement forms
//!
//! The complement is computed from each partitioned node's children's bounds
//! (`pg_get_expr(relpartbound)`), read live:
//!
//! - **`HASH`**: the remainders of the greatest modulus that no child covers,
//!   each a `satisfies_hash_partition` term. Nothing missing is no term.
//! - **`RANGE`**: the gaps between the children's bounds, ordered by the key
//!   columns' types and collations (the server ranks the values), with
//!   `MINVALUE` and `MAXVALUE`, plus a `NULL` in any key column, which no
//!   `RANGE` child takes.
//! - **`LIST`**: every value no child lists, `NULL` included unless one does.
//!
//! A partitioned node with a `DEFAULT` child has no complement: the
//! `DEFAULT` takes everything its siblings don't. Under a middle, the
//! complement is the middle's own route (its bound and its ancestors') and
//! its children's gaps. When no node has a complement (a `DEFAULT` under the
//! root, or every `HASH` remainder present) the sweep runs no statement.
//!
//! # Ordering against DDL
//!
//! The sweep runs in its own `READ COMMITTED` transaction under the pinned
//! settings ([`super::partitions::pinned_settings`]). It reads the lifecycle
//! fingerprint, **then** the bounds, finds and queues the hits, and reads the
//! fingerprint again in a new statement. If the tree changed, it rolls back
//! and starts over (up to [`ATTEMPTS`] times a pass). A leaf created after
//! the bounds were read, whose rows the stale complement would include, is
//! seen by the second read; reading the bounds before the first would let a
//! leaf created between the two reads pass both.
//!
//! The groups of a gone node whose correction is still queued (with a usable
//! text) are left out of the search: that correction covers them. The
//! queued complement itself is not narrowed by them, so the same complement
//! is queued once however long the gone node's correction takes.
//!
//! # What the sweep reads
//!
//! The groups of every aggregate that reads the source and applies. A to-many
//! relationship's target is corrected by recomputing its parents, which is
//! the to-many correction engine's (#1063).

use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};

use tokio_postgres::{Client, IsolationLevel};

use super::CaptureError;
use super::columns::CaptureCatalog;
use super::partitions::{self, Key};
use crate::defs::ast::KeySpace;
use crate::defs::ddl::{qualified_target_table_ident, regclass_arg};
use crate::defs::model::applying_sql;
use crate::defs::partition_tree::Strategy;
use crate::pool::quote_ident;

/// Times one pass starts the sweep over when the tree changes under it.
pub const ATTEMPTS: usize = 3;

/// One value of a partition bound, as `pg_get_expr(relpartbound)` prints it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Datum {
    MinValue,
    MaxValue,
    Null,
    /// The value's text: a quoted literal unquoted, a bare one as printed.
    Value(String),
}

/// A partition bound.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Bound {
    Default,
    Hash { modulus: u64, remainder: u64 },
    List(Vec<Datum>),
    Range { from: Vec<Datum>, to: Vec<Datum> },
}

/// Parses a bound as `pg_get_expr(relpartbound)` prints it, `None` for a
/// form it doesn't know.
pub fn parse_bound(text: &str) -> Option<Bound> {
    let text = text.trim();
    if text == "DEFAULT" {
        return Some(Bound::Default);
    }
    let rest = text.strip_prefix("FOR VALUES ")?;
    if let Some(rest) = rest.strip_prefix("WITH (") {
        let inner = rest.strip_suffix(')')?;
        let (modulus, remainder) = inner.split_once(',')?;
        let modulus = modulus
            .trim()
            .strip_prefix("modulus ")?
            .trim()
            .parse()
            .ok()?;
        let remainder = remainder
            .trim()
            .strip_prefix("remainder ")?
            .trim()
            .parse()
            .ok()?;
        return Some(Bound::Hash { modulus, remainder });
    }
    if let Some(rest) = rest.strip_prefix("IN ") {
        let (values, rest) = parse_list(rest)?;
        return rest.trim().is_empty().then_some(Bound::List(values));
    }
    let rest = rest.strip_prefix("FROM ")?;
    let (from, rest) = parse_list(rest)?;
    let rest = rest.trim_start().strip_prefix("TO ")?;
    let (to, rest) = parse_list(rest)?;
    (rest.trim().is_empty() && !from.is_empty() && from.len() == to.len())
        .then_some(Bound::Range { from, to })
}

/// Parses `(item, item, ...)` at the start of `s`, returning the items and
/// what follows the closing parenthesis.
fn parse_list(s: &str) -> Option<(Vec<Datum>, &str)> {
    let mut chars = s.strip_prefix('(')?.char_indices().peekable();
    let body = &s[1..];
    let mut items = Vec::new();
    loop {
        while chars.next_if(|(_, c)| c.is_whitespace()).is_some() {}
        let (start, first) = *chars.peek()?;
        if first == '\'' {
            chars.next();
            let mut value = String::new();
            loop {
                let (_, c) = chars.next()?;
                if c == '\'' {
                    if chars.next_if(|(_, c)| *c == '\'').is_some() {
                        value.push('\'');
                    } else {
                        break;
                    }
                } else {
                    value.push(c);
                }
            }
            items.push(Datum::Value(value));
        } else {
            let mut end = start;
            while let Some((i, c)) = chars.next_if(|(_, c)| *c != ',' && *c != ')') {
                end = i + c.len_utf8();
            }
            let token = body[start..end].trim();
            items.push(match token {
                "MINVALUE" => Datum::MinValue,
                "MAXVALUE" => Datum::MaxValue,
                "NULL" => Datum::Null,
                "" => return None,
                _ if token.contains(['\'', ' ', '(']) => return None,
                _ => Datum::Value(token.to_string()),
            });
        }
        while chars.next_if(|(_, c)| c.is_whitespace()).is_some() {}
        match chars.next()? {
            (_, ',') => {}
            (i, ')') => return Some((items, &body[i + 1..])),
            _ => return None,
        }
    }
}

/// The `hash` parent oids a constraint text or a complement evaluates
/// `satisfies_hash_partition` against. Evaluating one whose parent is gone
/// raises, so a text naming a dropped table can't be evaluated.
pub fn hash_parents(text: &str) -> Vec<u32> {
    const CALL: &str = "satisfies_hash_partition('";
    let mut out = Vec::new();
    let mut rest = text;
    while let Some(at) = rest.find(CALL) {
        rest = &rest[at + CALL.len()..];
        let digits: String = rest.chars().take_while(char::is_ascii_digit).collect();
        if rest[digits.len()..].starts_with("'::oid")
            && let Ok(oid) = digits.parse()
        {
            out.push(oid);
        }
    }
    out
}

/// One partition-key column of a partitioned node.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeyColumn {
    pub name: String,
    /// `format_type(atttypid, atttypmod)`.
    pub pg_type: String,
    /// The level's key collation (`partcollation`): the one Postgres
    /// compares the column under against this level's children's bounds.
    /// Schema-qualified, empty for a type without one.
    pub collation: String,
}

/// One node of the tree the sweep reads: every node reachable from the root
/// without crossing a pending detach.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TreeNode {
    pub oid: u32,
    /// Index of the parent, `None` for the root.
    pub parent: Option<usize>,
    /// `None` for the root.
    pub bound: Option<Bound>,
    /// For a partitioned node, its strategy and key.
    pub partition: Option<(Strategy, Vec<KeyColumn>)>,
}

/// A predicate over the source's key columns, simplified as it's built.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Pred {
    True,
    False,
    Sql(String),
}

impl Pred {
    fn and(parts: impl IntoIterator<Item = Pred>) -> Pred {
        let mut sql = Vec::new();
        for part in parts {
            match part {
                Pred::False => return Pred::False,
                Pred::True => {}
                Pred::Sql(s) => sql.push(s),
            }
        }
        match sql.len() {
            0 => Pred::True,
            1 => Pred::Sql(sql.pop().expect("one")),
            _ => Pred::Sql(format!("({})", sql.join(" and "))),
        }
    }

    fn or(parts: impl IntoIterator<Item = Pred>) -> Pred {
        let mut sql = Vec::new();
        for part in parts {
            match part {
                Pred::True => return Pred::True,
                Pred::False => {}
                Pred::Sql(s) => sql.push(s),
            }
        }
        match sql.len() {
            0 => Pred::False,
            1 => Pred::Sql(sql.pop().expect("one")),
            _ => Pred::Sql(format!("({})", sql.join(" or "))),
        }
    }

    /// The SQL: `true`, `false` or the predicate.
    pub fn sql(&self) -> &str {
        match self {
            Pred::True => "true",
            Pred::False => "false",
            Pred::Sql(s) => s,
        }
    }
}

/// The values of one `RANGE` key position that need a rank: (node index,
/// key position) and the distinct values.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RankRequest {
    pub node: usize,
    pub position: usize,
    pub key: KeyColumn,
    pub values: Vec<String>,
}

/// Each `RANGE` value's rank in its key column's order, by (node index, key
/// position): equal values share one.
pub type Ranks = HashMap<(usize, usize), HashMap<String, i64>>;

/// The values [`complement`] needs ranked.
pub fn rank_requests(nodes: &[TreeNode]) -> Vec<RankRequest> {
    let mut out = Vec::new();
    for (index, node) in nodes.iter().enumerate() {
        let Some((Strategy::Range, keys)) = &node.partition else {
            continue;
        };
        for (position, key) in keys.iter().enumerate() {
            let mut values: Vec<String> = Vec::new();
            for child in children(nodes, index) {
                if let Some(Bound::Range { from, to }) = &child.bound {
                    for datum in [from.get(position), to.get(position)].into_iter().flatten() {
                        if let Datum::Value(v) = datum
                            && !values.contains(v)
                        {
                            values.push(v.clone());
                        }
                    }
                }
            }
            if !values.is_empty() {
                out.push(RankRequest {
                    node: index,
                    position,
                    key: key.clone(),
                    values,
                });
            }
        }
    }
    out
}

fn children(nodes: &[TreeNode], parent: usize) -> impl Iterator<Item = &TreeNode> {
    nodes.iter().filter(move |n| n.parent == Some(parent))
}

/// The SQL of a cast to `pg_type` with `collation`, of `value` (SQL).
fn typed(value: &str, key: &KeyColumn) -> String {
    let mut cast = format!("(({value})::{}", key.pg_type);
    if !key.collation.is_empty() {
        cast.push_str(" collate ");
        cast.push_str(&qualified_collation(&key.collation));
    }
    cast.push(')');
    cast
}

/// `schema.collation` as recorded, quoted for a `collate` clause.
pub fn qualified_collation(recorded: &str) -> String {
    match recorded.split_once('.') {
        Some((schema, name)) => format!("{}.{}", quote_ident(schema), quote_ident(name)),
        None => quote_ident(recorded),
    }
}

/// `key`'s column under the node's own key collation. Levels of one tree can
/// partition the same column under different collations, and the search
/// presents each column once, so each comparison names the collation of the
/// level whose bound it reads.
fn collated(key: &KeyColumn) -> String {
    if key.collation.is_empty() {
        quote_ident(&key.name)
    } else {
        format!(
            "({} collate {})",
            quote_ident(&key.name),
            qualified_collation(&key.collation)
        )
    }
}

/// A literal of `key`'s type.
fn literal(value: &str, key: &KeyColumn) -> String {
    format!("'{}'::{}", value.replace('\'', "''"), key.pg_type)
}

/// The statement ranking every request's values, one row per value:
/// (request index, value, rank). Its parameters are each request's values
/// as `text[]`, in order.
pub fn rank_sql(requests: &[RankRequest]) -> String {
    requests
        .iter()
        .enumerate()
        .map(|(i, r)| {
            format!(
                "select {i}, v, pg_catalog.dense_rank() over (order by {}) \
                 from pg_catalog.unnest(${}::text[]) as v",
                typed("v", &r.key),
                i + 1
            )
        })
        .collect::<Vec<_>>()
        .join(" union all ")
}

/// A position in a `RANGE` key column's order.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum Pos {
    Min,
    Val(i64),
    Max,
}

/// What [`complement`] couldn't work out.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Unreadable(pub String);

/// The predicate, over the source's key column names, of the rows no current
/// leaf of `nodes` takes. [`Pred::False`] when every row has a leaf.
pub fn complement(nodes: &[TreeNode], ranks: &Ranks) -> Result<Pred, Unreadable> {
    let mut terms = Vec::new();
    for (index, node) in nodes.iter().enumerate() {
        if node.partition.is_none() {
            continue;
        }
        let gap = gap(nodes, ranks, index, true)?;
        if gap == Pred::False {
            continue;
        }
        terms.push(Pred::and([route(nodes, ranks, index)?, gap]));
    }
    Ok(Pred::or(terms))
}

/// The rows that route to node `index` from the root, through every bound
/// on the way.
fn route(nodes: &[TreeNode], ranks: &Ranks, index: usize) -> Result<Pred, Unreadable> {
    let mut parts = Vec::new();
    let mut at = index;
    while let Some(parent) = nodes[at].parent {
        parts.push(member(nodes, ranks, parent, &nodes[at])?);
        at = parent;
    }
    Ok(Pred::and(parts))
}

/// The rows of partitioned node `parent` that its child `child`'s bound
/// takes.
fn member(
    nodes: &[TreeNode],
    ranks: &Ranks,
    parent: usize,
    child: &TreeNode,
) -> Result<Pred, Unreadable> {
    let (strategy, keys) = nodes[parent]
        .partition
        .as_ref()
        .ok_or_else(|| Unreadable("a child of an unpartitioned node".to_string()))?;
    let bound = child
        .bound
        .as_ref()
        .ok_or_else(|| Unreadable(format!("node {} has no bound", child.oid)))?;
    match (strategy, bound) {
        (_, Bound::Default) => gap(nodes, ranks, parent, false),
        (Strategy::Hash, Bound::Hash { modulus, remainder }) => {
            Ok(hash_term(nodes[parent].oid, *modulus, *remainder, keys))
        }
        (Strategy::List, Bound::List(values)) => Ok(list_member(values, keys)),
        (Strategy::Range, Bound::Range { from, to }) => Ok(Pred::and([
            not_null(keys),
            at_or_after(keys, from),
            before(keys, to),
        ])),
        _ => Err(Unreadable(format!(
            "node {}'s bound doesn't match its parent's strategy",
            child.oid
        ))),
    }
}

/// The rows of partitioned node `index` no child takes. `with_default`
/// says whether a `DEFAULT` child counts: it takes every such row.
fn gap(
    nodes: &[TreeNode],
    ranks: &Ranks,
    index: usize,
    with_default: bool,
) -> Result<Pred, Unreadable> {
    let (strategy, keys) = nodes[index]
        .partition
        .as_ref()
        .ok_or_else(|| Unreadable("the gap of an unpartitioned node".to_string()))?;
    let mut bounds = Vec::new();
    for child in children(nodes, index) {
        match &child.bound {
            Some(Bound::Default) => {
                if with_default {
                    return Ok(Pred::False);
                }
            }
            Some(bound) => bounds.push(bound),
            None => return Err(Unreadable(format!("node {} has no bound", child.oid))),
        }
    }
    if bounds.is_empty() {
        return Ok(Pred::True);
    }
    match strategy {
        Strategy::Hash => {
            let mut covered: Vec<(u64, u64)> = Vec::new();
            for bound in &bounds {
                match bound {
                    Bound::Hash { modulus, remainder } if *modulus > 0 => {
                        covered.push((*modulus, *remainder));
                    }
                    _ => return Err(Unreadable("a HASH child without a modulus".to_string())),
                }
            }
            let greatest = covered.iter().map(|(m, _)| *m).max().unwrap_or(1);
            let missing = (0..greatest)
                .filter(|r| !covered.iter().any(|(m, rem)| r % m == *rem))
                .map(|r| hash_term(nodes[index].oid, greatest, r, keys));
            Ok(Pred::or(missing))
        }
        Strategy::List => {
            let mut values = Vec::new();
            for bound in &bounds {
                match bound {
                    Bound::List(list) => values.extend(list.iter().cloned()),
                    _ => return Err(Unreadable("a LIST child without a list".to_string())),
                }
            }
            Ok(Pred::Sql(format!(
                "not coalesce({}, false)",
                list_member(&values, keys).sql()
            )))
        }
        Strategy::Range => {
            let mut intervals = Vec::new();
            for bound in &bounds {
                match bound {
                    Bound::Range { from, to } => {
                        intervals.push((ranked(ranks, index, from)?, from, to));
                    }
                    _ => return Err(Unreadable("a RANGE child without a range".to_string())),
                }
            }
            intervals.sort_by(|a, b| a.0.cmp(&b.0));
            let mut terms =
                vec![Pred::or(keys.iter().map(|k| {
                    Pred::Sql(format!("{} is null", quote_ident(&k.name)))
                }))];
            let mut cursor: Option<&Vec<Datum>> = None;
            for (lower, from, to) in &intervals {
                match cursor {
                    None => {
                        if lower[0] != Pos::Min {
                            terms.push(before(keys, from));
                        }
                    }
                    Some(upper) => {
                        if ranked(ranks, index, upper)?.cmp(lower) == Ordering::Less {
                            terms.push(Pred::and([at_or_after(keys, upper), before(keys, from)]));
                        }
                    }
                }
                cursor = Some(to);
            }
            if let Some(upper) = cursor
                && upper[0] != Datum::MaxValue
            {
                terms.push(at_or_after(keys, upper));
            }
            Ok(Pred::or(terms))
        }
    }
}

/// `satisfies_hash_partition` of the rows of `parent` with `remainder` of
/// `modulus`.
fn hash_term(parent: u32, modulus: u64, remainder: u64, keys: &[KeyColumn]) -> Pred {
    let columns: Vec<String> = keys.iter().map(|k| quote_ident(&k.name)).collect();
    Pred::Sql(format!(
        "satisfies_hash_partition('{parent}'::oid, {modulus}, {remainder}, {})",
        columns.join(", ")
    ))
}

/// The rows a `LIST` of `values` takes (a `LIST` key is one column).
fn list_member(values: &[Datum], keys: &[KeyColumn]) -> Pred {
    let Some(key) = keys.first() else {
        return Pred::False;
    };
    let column = collated(key);
    let literals: Vec<String> = values
        .iter()
        .filter_map(|v| match v {
            Datum::Value(v) => Some(literal(v, key)),
            _ => None,
        })
        .collect();
    let null = values.contains(&Datum::Null);
    let listed = if literals.is_empty() {
        Pred::False
    } else {
        Pred::Sql(format!("{column} = any (array[{}])", literals.join(", ")))
    };
    let null = if null {
        Pred::Sql(format!("{column} is null"))
    } else {
        Pred::False
    };
    Pred::or([null, listed])
}

fn not_null(keys: &[KeyColumn]) -> Pred {
    Pred::and(
        keys.iter()
            .map(|k| Pred::Sql(format!("{} is not null", quote_ident(&k.name)))),
    )
}

/// The ranked form of a `RANGE` bound of node `index`.
fn ranked(ranks: &Ranks, index: usize, bound: &[Datum]) -> Result<Vec<Pos>, Unreadable> {
    bound
        .iter()
        .enumerate()
        .map(|(position, datum)| match datum {
            Datum::MinValue => Ok(Pos::Min),
            Datum::MaxValue => Ok(Pos::Max),
            Datum::Value(v) => ranks
                .get(&(index, position))
                .and_then(|r| r.get(v))
                .map(|r| Pos::Val(*r))
                .ok_or_else(|| Unreadable(format!("no rank for the bound value {v}"))),
            Datum::Null => Err(Unreadable("a NULL in a RANGE bound".to_string())),
        })
        .collect()
}

/// The leading values of a `RANGE` bound before its first `MINVALUE` or
/// `MAXVALUE`, and that one, if any.
fn split_bound(bound: &[Datum]) -> (usize, Option<&Datum>) {
    match bound
        .iter()
        .position(|d| matches!(d, Datum::MinValue | Datum::MaxValue))
    {
        Some(k) => (k, Some(&bound[k])),
        None => (bound.len(), None),
    }
}

/// `(columns) op (values)` over the first `n` key columns.
fn compare(keys: &[KeyColumn], bound: &[Datum], n: usize, op: &str) -> Pred {
    let columns: Vec<String> = keys[..n].iter().map(collated).collect();
    let values: Vec<String> = bound[..n]
        .iter()
        .zip(keys)
        .map(|(d, k)| match d {
            Datum::Value(v) => literal(v, k),
            _ => unreachable!("only the leading values are compared"),
        })
        .collect();
    if n == 1 {
        Pred::Sql(format!("{} {op} {}", columns[0], values[0]))
    } else {
        Pred::Sql(format!(
            "({}) {op} ({})",
            columns.join(", "),
            values.join(", ")
        ))
    }
}

/// The rows at or after a `RANGE` lower bound (inclusive, as a lower bound
/// is).
fn at_or_after(keys: &[KeyColumn], bound: &[Datum]) -> Pred {
    match split_bound(bound) {
        (n, None) => compare(keys, bound, n, ">="),
        (0, Some(Datum::MinValue)) => Pred::True,
        (0, Some(_)) => Pred::False,
        (n, Some(Datum::MinValue)) => compare(keys, bound, n, ">="),
        (n, Some(_)) => compare(keys, bound, n, ">"),
    }
}

/// The rows before a `RANGE` upper bound (exclusive, as an upper bound is).
fn before(keys: &[KeyColumn], bound: &[Datum]) -> Pred {
    match split_bound(bound) {
        (n, None) => compare(keys, bound, n, "<"),
        (0, Some(Datum::MinValue)) => Pred::False,
        (0, Some(_)) => Pred::True,
        (n, Some(Datum::MinValue)) => compare(keys, bound, n, "<"),
        (n, Some(_)) => compare(keys, bound, n, "<="),
    }
}

/// What one pass's sweep of a source did.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Sweep {
    /// Statements that searched a target for groups in the complement: 0
    /// when nothing routes nowhere (a `DEFAULT` under the root, every
    /// `HASH` remainder present) or nothing reads the source's groups.
    pub statements: usize,
    /// The sweep queued a correction of the complement.
    pub queued: bool,
    /// Times it read the tree: more than one when the tree changed under it.
    pub attempts: usize,
    /// The tree changed under every attempt, or a bound couldn't be read:
    /// the next pass sweeps again.
    pub skipped: bool,
    /// How long the last attempt's catalog reads took: the fingerprint, the
    /// bounds and keys, and the ranks of the `RANGE` values.
    pub read: Duration,
    /// How long its search of the targets took.
    pub search: Duration,
}

/// Sweeps `table` (an unquoted `schema.table`) for groups that route to no
/// current leaf, and queues their correction. `schema` is the engine's.
pub(crate) async fn sweep(
    client: &mut Client,
    schema: &str,
    catalog: &CaptureCatalog,
    table: &str,
) -> Result<Sweep, CaptureError> {
    let readers: Vec<i64> = catalog
        .definitions
        .iter()
        .filter(|r| {
            r.source == table
                && !r.capture_failed
                && matches!(r.def.key_space, KeySpace::Aggregate { .. })
        })
        .map(|r| r.id)
        .collect();
    let mut outcome = Sweep::default();
    if readers.is_empty() {
        return Ok(outcome);
    }
    while outcome.attempts < ATTEMPTS {
        outcome.attempts += 1;
        match attempt(client, schema, table, &readers, &mut outcome).await? {
            Attempt::Done => return Ok(outcome),
            Attempt::Changed => {}
            Attempt::Unreadable(why) => {
                crate::instance_log::warn!(table = %table, "the partition sweep can't read a bound: {why}");
                outcome.skipped = true;
                return Ok(outcome);
            }
        }
    }
    outcome.queued = false;
    outcome.skipped = true;
    Ok(outcome)
}

enum Attempt {
    Done,
    /// The tree changed under the attempt; it rolled back.
    Changed,
    Unreadable(String),
}

async fn attempt(
    client: &mut Client,
    schema: &str,
    table: &str,
    readers: &[i64],
    outcome: &mut Sweep,
) -> Result<Attempt, CaptureError> {
    let schema_ident = quote_ident(schema);
    let txn = client
        .build_transaction()
        .isolation_level(IsolationLevel::ReadCommitted)
        .start()
        .await?;
    txn.batch_execute(&partitions::pinned_settings()).await?;
    txn.batch_execute("set local standard_conforming_strings = on")
        .await?;
    let started = Instant::now();
    // The lifecycle catalog first, then the bounds: see the module doc.
    let Some(first) = partitions::read_fingerprints(&txn, table).await? else {
        return Ok(Attempt::Done);
    };
    let nodes = match read_tree(&txn, table).await? {
        Ok(nodes) => nodes,
        Err(Unreadable(why)) => return Ok(Attempt::Unreadable(why)),
    };
    let requests = rank_requests(&nodes);
    let ranks = read_ranks(&txn, &requests).await?;
    let complement = match complement(&nodes, &ranks) {
        Ok(pred) => pred,
        Err(Unreadable(why)) => return Ok(Attempt::Unreadable(why)),
    };
    outcome.statements = 0;
    outcome.queued = false;
    outcome.read = started.elapsed();
    outcome.search = Duration::ZERO;
    if complement == Pred::False {
        txn.commit().await?;
        return Ok(Attempt::Done);
    }
    let live: HashSet<u32> = nodes.iter().map(|n| n.oid).collect();
    // A gone node's queued correction covers its groups: every group for a
    // `DEFAULT` without a text, and otherwise those of its text, which is
    // evaluable while every `HASH` parent it names is in the tree.
    let mut every = false;
    let mut covered: Vec<String> = Vec::new();
    for row in txn
        .query(
            &format!(
                "select constraint_text, node_default from {schema_ident}.partition_corrections \
                 where table_name = $1 and kind = $2"
            ),
            &[&table, &partitions::Kind::GoneNode.as_str()],
        )
        .await?
    {
        match row.get::<_, Option<String>>(0) {
            None => every |= row.get::<_, bool>(1),
            Some(text) if hash_parents(&text).iter().all(|oid| live.contains(oid)) => {
                covered.push(text);
            }
            Some(_) => {}
        }
    }
    let targets: Vec<String> = txn
        .query(
            &format!(
                "select d.target_table from {schema_ident}.transform_definitions d \
                 where d.id = any($1) and {} order by d.id",
                applying_sql("d")
            ),
            &[&readers],
        )
        .await?
        .into_iter()
        .map(|row| row.get(0))
        .collect();
    // Test-only pause point (#1066): after every catalog read, before the
    // search. See `staging::interleave`.
    #[cfg(any(test, feature = "test-util"))]
    crate::staging::interleave::pause_at(
        &txn,
        crate::staging::interleave::PausePoint::AfterSweepCatalogRead,
        table,
    )
    .await?;
    if targets.is_empty() || every {
        txn.commit().await?;
        return Ok(Attempt::Done);
    }
    let keys = key_columns(&nodes);
    let mut search = complement.sql().to_string();
    for text in &covered {
        search.push_str(&format!(" and not coalesce(({text}), false)"));
    }
    let probes: Vec<String> = targets
        .iter()
        .map(|target| {
            let presented: Vec<String> = keys
                .iter()
                .map(|k| {
                    format!(
                        "{} as {}",
                        typed(&format!("t.{}", quote_ident(&k.name)), k),
                        quote_ident(&k.name)
                    )
                })
                .collect();
            format!(
                "exists (select 1 from (select {} from {} t) as __route where {search})",
                presented.join(", "),
                qualified_target_table_ident(target)
            )
        })
        .collect();
    outcome.statements = 1;
    let searching = Instant::now();
    let hit: bool = txn
        .query_one(&format!("select {}", probes.join(" or ")), &[])
        .await?
        .get(0);
    outcome.search = searching.elapsed();
    if hit {
        let root = i64::from(first.root_oid);
        txn.execute(
            &format!(
                "insert into {schema_ident}.partition_corrections \
                     (table_name, kind, node_oid, node_name, cause, constraint_text) \
                 values ($1, $2, $3, $1, $2, $4) \
                 on conflict (table_name, kind, node_oid, \
                              (md5(coalesce(constraint_text, '')))) \
                     where kind <> 'truncated' do nothing"
            ),
            &[
                &table,
                &partitions::Kind::Complement.as_str(),
                &root,
                &complement.sql(),
            ],
        )
        .await?;
        outcome.queued = true;
    }
    // Again, in a statement of its own: a leaf created since the first read
    // changes it, and the hits may be its rows.
    let again = partitions::read_fingerprints(&txn, table).await?;
    if again.as_ref().map(|f| &f.lifecycle) != Some(&first.lifecycle) {
        txn.rollback().await?;
        outcome.queued = false;
        return Ok(Attempt::Changed);
    }
    txn.commit().await?;
    Ok(Attempt::Done)
}

/// Every partition-key column at every level, each once.
fn key_columns(nodes: &[TreeNode]) -> Vec<KeyColumn> {
    let mut out: Vec<KeyColumn> = Vec::new();
    for (_, keys) in nodes.iter().filter_map(|n| n.partition.as_ref()) {
        for key in keys {
            if !out.iter().any(|k| k.name == key.name) {
                out.push(key.clone());
            }
        }
    }
    out
}

/// Reads the tree under `table`: every node reachable without crossing a
/// pending detach, its bound, and each partitioned node's strategy and key.
async fn read_tree(
    txn: &tokio_postgres::Transaction<'_>,
    table: &str,
) -> Result<Result<Vec<TreeNode>, Unreadable>, tokio_postgres::Error> {
    let rows = txn
        .query(
            "with recursive t(oid, parent) as ( \
                 select pg_catalog.to_regclass($1)::oid, null::oid \
                 union all \
                 select i.inhrelid, i.inhparent \
                 from t join pg_catalog.pg_inherits i on i.inhparent = t.oid \
                 where not i.inhdetachpending) \
             select t.oid::bigint, t.parent::bigint, \
                    pg_catalog.pg_get_expr(c.relpartbound, c.oid), p.partstrat::text \
             from t join pg_catalog.pg_class c on c.oid = t.oid \
             left join pg_catalog.pg_partitioned_table p on p.partrelid = t.oid",
            &[&regclass_arg(table)],
        )
        .await?;
    let index: HashMap<u32, usize> = rows
        .iter()
        .enumerate()
        .map(|(i, r)| (r.get::<_, i64>(0) as u32, i))
        .collect();
    let partitioned: Vec<i64> = rows
        .iter()
        .filter(|r| r.get::<_, Option<String>>(3).is_some())
        .map(|r| r.get(0))
        .collect();
    let keys = partitions::read_keys(txn, &partitioned).await?;
    let mut nodes = Vec::with_capacity(rows.len());
    for row in &rows {
        let oid = row.get::<_, i64>(0) as u32;
        let bound = match row.get::<_, Option<String>>(2) {
            None => None,
            Some(text) => match parse_bound(&text) {
                Some(bound) => Some(bound),
                None => return Ok(Err(Unreadable(format!("the bound {text}")))),
            },
        };
        let partition = match row.get::<_, Option<String>>(3).as_deref() {
            None => None,
            Some(strategy) => {
                let strategy = match strategy {
                    "h" => Strategy::Hash,
                    "l" => Strategy::List,
                    _ => Strategy::Range,
                };
                let mut columns = Vec::new();
                for key in keys.iter().filter(|k| k.node == oid) {
                    let Some(name) = &key.column else {
                        return Ok(Err(Unreadable(format!("an expression key on {oid}"))));
                    };
                    columns.push(key_column(name, key));
                }
                Some((strategy, columns))
            }
        };
        nodes.push(TreeNode {
            oid,
            parent: row
                .get::<_, Option<i64>>(1)
                .and_then(|p| index.get(&(p as u32)).copied()),
            bound,
            partition,
        });
    }
    Ok(Ok(nodes))
}

fn key_column(name: &str, key: &Key) -> KeyColumn {
    KeyColumn {
        name: name.to_string(),
        pg_type: key.pg_type.clone(),
        collation: key.level_collation.clone(),
    }
}

async fn read_ranks(
    txn: &tokio_postgres::Transaction<'_>,
    requests: &[RankRequest],
) -> Result<Ranks, tokio_postgres::Error> {
    let mut ranks = Ranks::new();
    if requests.is_empty() {
        return Ok(ranks);
    }
    let params: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> = requests
        .iter()
        .map(|r| &r.values as &(dyn tokio_postgres::types::ToSql + Sync))
        .collect();
    for row in txn.query(&rank_sql(requests), &params).await? {
        let request = &requests[row.get::<_, i32>(0) as usize];
        ranks
            .entry((request.node, request.position))
            .or_default()
            .insert(row.get(1), row.get(2));
    }
    Ok(ranks)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn value(v: &str) -> Datum {
        Datum::Value(v.to_string())
    }

    #[test]
    fn bounds_parse_as_the_server_prints_them() {
        assert_eq!(parse_bound("DEFAULT"), Some(Bound::Default));
        assert_eq!(
            parse_bound("FOR VALUES WITH (modulus 4, remainder 3)"),
            Some(Bound::Hash {
                modulus: 4,
                remainder: 3
            })
        );
        assert_eq!(
            parse_bound("FOR VALUES IN ('a', NULL, 'it''s')"),
            Some(Bound::List(vec![value("a"), Datum::Null, value("it's")]))
        );
        assert_eq!(
            parse_bound("FOR VALUES IN (true)"),
            Some(Bound::List(vec![value("true")]))
        );
        assert_eq!(
            parse_bound("FOR VALUES FROM (MINVALUE, MINVALUE) TO ('-5', 'it''s')"),
            Some(Bound::Range {
                from: vec![Datum::MinValue, Datum::MinValue],
                to: vec![value("-5"), value("it's")],
            })
        );
        assert_eq!(
            parse_bound("FOR VALUES FROM (10, 'x, y)') TO (20, MAXVALUE)"),
            Some(Bound::Range {
                from: vec![value("10"), value("x, y)")],
                to: vec![value("20"), Datum::MaxValue],
            })
        );
        assert_eq!(parse_bound("FOR VALUES IN ('a'::text)"), None);
        assert_eq!(parse_bound("FOR VALUES FROM (1) TO (2, 3)"), None);
        assert_eq!(parse_bound("something else"), None);
    }

    #[test]
    fn hash_parents_are_read_from_a_text() {
        assert_eq!(
            hash_parents(
                "(satisfies_hash_partition('16419'::oid, 4, 3, a, b) and \
                 satisfies_hash_partition('7'::oid, 2, 1, c))"
            ),
            vec![16419, 7]
        );
        assert!(hash_parents("((a >= 1) AND (a < 4))").is_empty());
    }

    fn key(name: &str, ty: &str) -> KeyColumn {
        KeyColumn {
            name: name.to_string(),
            pg_type: ty.to_string(),
            collation: String::new(),
        }
    }

    fn root(strategy: Strategy, keys: Vec<KeyColumn>) -> TreeNode {
        TreeNode {
            oid: 100,
            parent: None,
            bound: None,
            partition: Some((strategy, keys)),
        }
    }

    fn leaf(oid: u32, parent: usize, bound: &str) -> TreeNode {
        TreeNode {
            oid,
            parent: Some(parent),
            bound: Some(parse_bound(bound).expect("a bound")),
            partition: None,
        }
    }

    /// Ranks as the server would for integers.
    fn int_ranks(nodes: &[TreeNode]) -> Ranks {
        let mut ranks = Ranks::new();
        for r in rank_requests(nodes) {
            let mut values: Vec<i64> = r.values.iter().map(|v| v.parse().unwrap()).collect();
            values.sort();
            values.dedup();
            ranks.insert(
                (r.node, r.position),
                r.values
                    .iter()
                    .map(|v| {
                        let n: i64 = v.parse().unwrap();
                        (v.clone(), values.binary_search(&n).unwrap() as i64 + 1)
                    })
                    .collect(),
            );
        }
        ranks
    }

    #[test]
    fn a_hash_tree_with_every_remainder_has_no_complement() {
        let nodes = vec![
            root(Strategy::Hash, vec![key("g", "integer")]),
            leaf(1, 0, "FOR VALUES WITH (modulus 2, remainder 0)"),
            leaf(2, 0, "FOR VALUES WITH (modulus 4, remainder 1)"),
            leaf(3, 0, "FOR VALUES WITH (modulus 4, remainder 3)"),
        ];
        assert_eq!(complement(&nodes, &Ranks::new()), Ok(Pred::False));
    }

    #[test]
    fn a_hash_tree_missing_remainders_names_each_of_the_greatest_modulus() {
        let nodes = vec![
            root(Strategy::Hash, vec![key("g", "integer"), key("h", "text")]),
            leaf(1, 0, "FOR VALUES WITH (modulus 8, remainder 0)"),
            leaf(2, 0, "FOR VALUES WITH (modulus 4, remainder 2)"),
            leaf(3, 0, "FOR VALUES WITH (modulus 2, remainder 1)"),
        ];
        assert_eq!(
            complement(&nodes, &Ranks::new()).unwrap().sql(),
            "satisfies_hash_partition('100'::oid, 8, 4, \"g\", \"h\")",
            "remainder 4 of 8: 4 % 8 isn't 0, 4 % 4 isn't 2, 4 % 2 isn't 1"
        );
        let nodes = vec![
            root(Strategy::Hash, vec![key("g", "integer")]),
            leaf(1, 0, "FOR VALUES WITH (modulus 4, remainder 0)"),
            leaf(2, 0, "FOR VALUES WITH (modulus 4, remainder 2)"),
        ];
        assert_eq!(
            complement(&nodes, &Ranks::new()).unwrap().sql(),
            "(satisfies_hash_partition('100'::oid, 4, 1, \"g\") or \
             satisfies_hash_partition('100'::oid, 4, 3, \"g\"))"
        );
    }

    #[test]
    fn a_default_under_the_root_leaves_no_complement() {
        for strategy in [Strategy::Hash, Strategy::Range, Strategy::List] {
            let nodes = vec![
                root(strategy, vec![key("g", "integer")]),
                leaf(9, 0, "DEFAULT"),
            ];
            assert_eq!(complement(&nodes, &Ranks::new()), Ok(Pred::False));
        }
    }

    #[test]
    fn a_root_without_leaves_is_all_complement() {
        let nodes = vec![root(Strategy::List, vec![key("g", "integer")])];
        assert_eq!(complement(&nodes, &Ranks::new()), Ok(Pred::True));
    }

    #[test]
    fn a_list_complement_is_every_unlisted_value_and_null_unless_listed() {
        let nodes = vec![
            root(Strategy::List, vec![key("g", "text")]),
            leaf(1, 0, "FOR VALUES IN ('a', 'it''s')"),
            leaf(2, 0, "FOR VALUES IN ('b')"),
        ];
        assert_eq!(
            complement(&nodes, &Ranks::new()).unwrap().sql(),
            "not coalesce(\"g\" = any (array['a'::text, 'it''s'::text, 'b'::text]), false)"
        );
        let nodes = vec![
            root(Strategy::List, vec![key("g", "text")]),
            leaf(1, 0, "FOR VALUES IN ('a', NULL)"),
        ];
        assert_eq!(
            complement(&nodes, &Ranks::new()).unwrap().sql(),
            "not coalesce((\"g\" is null or \"g\" = any (array['a'::text])), false)"
        );
    }

    #[test]
    fn range_gaps_follow_the_ranked_order_not_the_text() {
        // Out of order, and "10" sorts before "4" as text.
        let nodes = vec![
            root(Strategy::Range, vec![key("g", "integer")]),
            leaf(2, 0, "FOR VALUES FROM (10) TO (20)"),
            leaf(1, 0, "FOR VALUES FROM (4) TO (8)"),
            leaf(3, 0, "FOR VALUES FROM (20) TO (MAXVALUE)"),
        ];
        let ranks = int_ranks(&nodes);
        assert_eq!(
            complement(&nodes, &ranks).unwrap().sql(),
            "(\"g\" is null or \"g\" < '4'::integer or \
             (\"g\" >= '8'::integer and \"g\" < '10'::integer))"
        );
    }

    #[test]
    fn range_minvalue_and_maxvalue_close_the_ends() {
        let nodes = vec![
            root(Strategy::Range, vec![key("g", "integer")]),
            leaf(1, 0, "FOR VALUES FROM (MINVALUE) TO (0)"),
            leaf(2, 0, "FOR VALUES FROM (0) TO (MAXVALUE)"),
        ];
        let ranks = int_ranks(&nodes);
        assert_eq!(
            complement(&nodes, &ranks).unwrap().sql(),
            "\"g\" is null",
            "only a NULL key routes nowhere"
        );
    }

    #[test]
    fn a_multi_column_range_compares_rows_and_reads_minvalue_and_maxvalue_per_column() {
        let nodes = vec![
            root(
                Strategy::Range,
                vec![key("a", "integer"), key("b", "integer")],
            ),
            leaf(1, 0, "FOR VALUES FROM (1, MINVALUE) TO (1, 5)"),
            leaf(2, 0, "FOR VALUES FROM (1, 7) TO (2, MAXVALUE)"),
            leaf(
                3,
                0,
                "FOR VALUES FROM (4, MINVALUE) TO (MAXVALUE, MAXVALUE)",
            ),
        ];
        let ranks = int_ranks(&nodes);
        assert_eq!(
            complement(&nodes, &ranks).unwrap().sql(),
            "((\"a\" is null or \"b\" is null) or \"a\" < '1'::integer or \
             ((\"a\", \"b\") >= ('1'::integer, '5'::integer) and \
             (\"a\", \"b\") < ('1'::integer, '7'::integer)) or \
             (\"a\" > '2'::integer and \"a\" < '4'::integer))"
        );
    }

    #[test]
    fn a_middle_contributes_its_route_and_its_own_gaps() {
        // A RANGE root on `a` with a HASH middle on `b` missing remainder 1,
        // and a DEFAULT middle on `c`, so the root has no gap of its own. The
        // DEFAULT middle's route is its siblings' complement.
        let nodes = vec![
            root(Strategy::Range, vec![key("a", "integer")]),
            TreeNode {
                oid: 200,
                parent: Some(0),
                bound: parse_bound("FOR VALUES FROM (0) TO (10)"),
                partition: Some((Strategy::Hash, vec![key("b", "integer")])),
            },
            leaf(201, 1, "FOR VALUES WITH (modulus 2, remainder 0)"),
            TreeNode {
                oid: 300,
                parent: Some(0),
                bound: Some(Bound::Default),
                partition: Some((Strategy::List, vec![key("c", "text")])),
            },
            leaf(301, 3, "FOR VALUES IN ('x')"),
        ];
        let ranks = int_ranks(&nodes);
        assert_eq!(
            complement(&nodes, &ranks).unwrap().sql(),
            "(((\"a\" is not null and \"a\" >= '0'::integer and \"a\" < '10'::integer) and \
             satisfies_hash_partition('200'::oid, 2, 1, \"b\")) or \
             ((\"a\" is null or \"a\" < '0'::integer or \"a\" >= '10'::integer) and \
             not coalesce(\"c\" = any (array['x'::text]), false)))"
        );
    }

    #[test]
    fn rank_requests_hold_each_ranges_distinct_values_per_position() {
        let nodes = vec![
            root(Strategy::Range, vec![key("a", "integer"), key("b", "text")]),
            leaf(1, 0, "FOR VALUES FROM (1, 'x') TO (2, MAXVALUE)"),
            leaf(2, 0, "FOR VALUES FROM (2, MAXVALUE) TO (3, 'x')"),
        ];
        let requests = rank_requests(&nodes);
        assert_eq!(requests.len(), 2);
        assert_eq!(requests[0].values, ["1", "2", "3"]);
        assert_eq!(requests[1].values, ["x"]);
        assert_eq!(
            rank_sql(&requests),
            "select 0, v, pg_catalog.dense_rank() over (order by ((v)::integer)) \
             from pg_catalog.unnest($1::text[]) as v union all \
             select 1, v, pg_catalog.dense_rank() over (order by ((v)::text)) \
             from pg_catalog.unnest($2::text[]) as v"
        );
    }

    /// Whether each probe row (a query over the source's columns) is in the
    /// complement the sweep computes for `public.items`, as the sweep reads
    /// and presents it, and whether Postgres finds no partition for it.
    async fn disagreements(client: &mut Client, ddl: &str, probes: &str) -> Vec<String> {
        client
            .batch_execute(&format!("drop table if exists public.items cascade; {ddl}"))
            .await
            .unwrap_or_else(|e| panic!("{ddl}: {e}"));
        let txn = client.transaction().await.expect("begin");
        txn.batch_execute(&partitions::pinned_settings())
            .await
            .expect("pinned");
        let nodes = read_tree(&txn, "public.items")
            .await
            .expect("read")
            .expect("readable");
        let ranks = read_ranks(&txn, &rank_requests(&nodes))
            .await
            .expect("ranks");
        let pred = complement(&nodes, &ranks).expect("a complement");
        let presented: Vec<String> = key_columns(&nodes)
            .iter()
            .map(|k| {
                format!(
                    "{} as {}",
                    typed(&format!("p.{}", quote_ident(&k.name)), k),
                    quote_ident(&k.name)
                )
            })
            .collect();
        let rows: Vec<(String, bool)> = txn
            .query(
                &format!(
                    "select pg_catalog.row_to_json(p)::text, \
                       (select ({}) is not distinct from true from (select {}) as __route) \
                     from ({probes}) as p",
                    pred.sql(),
                    presented.join(", ")
                ),
                &[],
            )
            .await
            .unwrap_or_else(|e| panic!("{}: {e}", pred.sql()))
            .iter()
            .map(|r| (r.get(0), r.get(1)))
            .collect();
        txn.commit().await.expect("commit");
        let mut wrong = Vec::new();
        for (json, in_complement) in rows {
            let routed = client
                .batch_execute(&format!(
                    "begin; insert into public.items \
                     select * from json_populate_record(null::public.items, '{}'); rollback",
                    json.replace('\'', "''")
                ))
                .await;
            let nowhere = match routed {
                Ok(()) => false,
                Err(e) => {
                    client.batch_execute("rollback").await.expect("rollback");
                    let message = e.as_db_error().map(|d| d.message().to_string());
                    assert!(
                        message
                            .as_deref()
                            .is_some_and(|m| m.contains("no partition")),
                        "{json}: {e}"
                    );
                    true
                }
            };
            if nowhere != in_complement {
                wrong.push(format!(
                    "{json}: complement {in_complement}, no partition {nowhere} in {}",
                    pred.sql()
                ));
            }
        }
        wrong
    }

    /// The complement agrees with Postgres's own routing, row by row, over
    /// every shape the sweep forms: multi-column `RANGE` with `MINVALUE` and
    /// `MAXVALUE`, a `"C"` text key, `timestamptz` and `numeric` bounds, `LIST`
    /// with and without `NULL`, `HASH` across mixed moduli, middles (`HASH`,
    /// `LIST` beside a `DEFAULT`, a partitioned `DEFAULT`), and levels that
    /// partition one column under different collations.
    #[tokio::test]
    async fn the_complement_holds_exactly_the_rows_postgres_routes_nowhere() {
        let cluster = testkit::TestCluster::start();
        let db = cluster.create_empty_database().await;
        let (mut client, connection) = tokio_postgres::connect(db.dsn(), tokio_postgres::NoTls)
            .await
            .expect("connect");
        tokio::spawn(async move {
            let _ = connection.await;
        });
        client
            .batch_execute("set timezone = 'UTC'; set datestyle = 'ISO, YMD'")
            .await
            .expect("settings");
        let cases = [
            (
                "create table public.items (a int, b int) partition by range (a, b); \
                 create table public.i0 partition of public.items \
                     for values from (minvalue, minvalue) to (-1, 2); \
                 create table public.i1 partition of public.items \
                     for values from (1, minvalue) to (1, 5); \
                 create table public.i2 partition of public.items \
                     for values from (1, 7) to (2, maxvalue); \
                 create table public.i3 partition of public.items \
                     for values from (3, 3) to (3, 9); \
                 create table public.i4 partition of public.items \
                     for values from (4, minvalue) to (maxvalue, maxvalue);",
                "select a, b from generate_series(-2, 12) a, generate_series(-2, 12) b \
                 union all select null, 3 union all select 3, null",
            ),
            (
                "create table public.items (a int, t text) \
                     partition by range (a, t collate \"C\"); \
                 create table public.i1 partition of public.items \
                     for values from (1, 'A') to (1, 'a'); \
                 create table public.i2 partition of public.items \
                     for values from (1, 'b') to (1, 'z'); \
                 create table public.i3 partition of public.items \
                     for values from (2, minvalue) to (3, 'M');",
                "select a, t from generate_series(0, 4) a, \
                 unnest(array['A', 'B', 'Z', 'a', 'b', 'c', 'm', 'M', 'z', 'zz', '', ' ', null]) t",
            ),
            (
                "create table public.items (ts timestamptz) partition by range (ts); \
                 create table public.i1 partition of public.items \
                     for values from ('2024-01-01 00:00+00') to ('2024-02-01 00:00+05'); \
                 create table public.i2 partition of public.items \
                     for values from ('2024-03-01') to (maxvalue);",
                "select ts from generate_series('2023-12-31'::timestamptz, '2024-03-02', \
                 '6 hours') ts union all select null",
            ),
            (
                "create table public.items (n numeric) partition by range (n); \
                 create table public.i1 partition of public.items \
                     for values from (-10.5) to (-1); \
                 create table public.i2 partition of public.items \
                     for values from (2.25) to (100);",
                "select (x / 4.0)::numeric as n from generate_series(-50, 420) x \
                 union all select null",
            ),
            (
                "create table public.items (t text) partition by list (t); \
                 create table public.i1 partition of public.items for values in ('a', 'it''s'); \
                 create table public.i2 partition of public.items for values in ('b', null);",
                "select unnest(array['a', 'b', 'c', 'it''s', 'A', null]) as t",
            ),
            (
                "create table public.items (t text) partition by list (t); \
                 create table public.i1 partition of public.items for values in ('a', 'it''s');",
                "select unnest(array['a', 'b', 'c', 'it''s', 'A', null]) as t",
            ),
            (
                "create table public.items (f boolean) partition by list (f); \
                 create table public.i1 partition of public.items for values in (true);",
                "select unnest(array[true, false, null]) as f",
            ),
            (
                "create table public.items (a int, t text) partition by hash (a, t); \
                 create table public.i1 partition of public.items \
                     for values with (modulus 8, remainder 0); \
                 create table public.i2 partition of public.items \
                     for values with (modulus 4, remainder 2); \
                 create table public.i3 partition of public.items \
                     for values with (modulus 2, remainder 1);",
                "select a, t from generate_series(0, 40) a, unnest(array['x', 'y', null]) t",
            ),
            (
                "create table public.items (a int, b int, c text) partition by range (a); \
                 create table public.m1 partition of public.items \
                     for values from (0) to (10) partition by hash (b); \
                 create table public.m1_0 partition of public.m1 \
                     for values with (modulus 2, remainder 0); \
                 create table public.m2 partition of public.items \
                     for values from (10) to (20) partition by list (c); \
                 create table public.m2_x partition of public.m2 for values in ('x'); \
                 create table public.m2_d partition of public.m2 default; \
                 create table public.m3 partition of public.items \
                     for values from (30) to (40) partition by list (c); \
                 create table public.m3_x partition of public.m3 for values in ('x', null); \
                 create table public.md partition of public.items default \
                     partition by list (c); \
                 create table public.md_y partition of public.md for values in ('y');",
                "select a, b, c from \
                 unnest(array[-1, 0, 5, 9, 10, 15, 20, 25, 30, 35, 40, null]) a, \
                 generate_series(0, 5) b, unnest(array['x', 'y', 'z', null]) c",
            ),
            (
                "create table public.items (t text collate \"C\") \
                     partition by range (t collate \"C\"); \
                 create table public.m1 partition of public.items \
                     for values from ('A') to ('z') partition by range (t collate \"en-US-x-icu\"); \
                 create table public.m1_0 partition of public.m1 for values from ('a') to ('c');",
                "select unnest(array['A', 'B', 'a', 'b', 'C', 'c', 'Z', 'y', null]) as t",
            ),
        ];
        for (ddl, probes) in cases {
            let wrong = disagreements(&mut client, ddl, probes).await;
            assert!(wrong.is_empty(), "{ddl}\n{}", wrong.join("\n"));
        }
    }
}
