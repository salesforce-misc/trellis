//! Which fields read which sibling fields by alias.
//!
//! Define time and the pause bookkeeping need this closure: a field that
//! reads a paused field by alias pauses with it.

use std::collections::{HashMap, HashSet};

use super::ast::TransformDef;

/// A field `f` reads sibling `g` when `f`'s expression names `g` and `g`
/// isn't `f` itself. A field naming itself reads its source column
/// (`c AS c`), the carve-out `validate`'s cycle detection makes, and a persisted definition passed `validate`, which refuses a field
/// naming itself that isn't a source column.
///
/// ADR-0003 freezes a paused field rather than nulling it, and a field that
/// reads a paused one by alias is paused too: rendering it would read the
/// paused field as absent and write NULL. [`Self::close`] is how every paused-field read
/// (`staging::quarantine::paused_columns_for` and its copies) takes that in,
/// and how a field build adds the fields that read the ones it rebuilds.
#[derive(Debug, Clone, Default)]
pub struct AliasReaders {
    readers: HashMap<String, Vec<String>>,
}

impl AliasReaders {
    /// `def`'s alias readers.
    pub fn of(def: &TransformDef) -> Self {
        let names: HashSet<&str> = def.fields.iter().map(|f| f.name.as_str()).collect();
        let mut readers: HashMap<String, Vec<String>> = HashMap::new();
        let aggregates = super::validate::aggregate_field_names(
            def.fields.iter().map(|f| (f.name.as_str(), &f.expr)),
        );
        for field in &def.fields {
            // A name in an aggregate's argument that matches an aggregate
            // field is a source column, not a read of that field.
            let mut columns = Vec::new();
            super::validate::collect_reads(
                &field.expr,
                &aggregates,
                false,
                &mut columns,
                &mut Vec::new(),
            );
            for column in columns {
                if column != field.name && names.contains(column.as_str()) {
                    let entry = readers.entry(column).or_default();
                    if !entry.contains(&field.name) {
                        entry.push(field.name.clone());
                    }
                }
            }
        }
        Self { readers }
    }

    /// The fields that read `field` by alias directly, in definition order.
    pub fn direct(&self, field: &str) -> &[String] {
        self.readers.get(field).map_or(&[], Vec::as_slice)
    }

    /// Adds to `fields` every field that reads one of them by alias,
    /// directly or through other fields.
    pub fn close(&self, fields: &mut HashSet<String>) {
        if self.readers.is_empty() {
            return;
        }
        let mut queue: Vec<String> = fields.iter().cloned().collect();
        while let Some(field) = queue.pop() {
            for reader in self.direct(&field) {
                if fields.insert(reader.clone()) {
                    queue.push(reader.clone());
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::defs::ast::{Expr, FieldDef, KeySpace, Operator, Predicate};

    fn def(fields: Vec<FieldDef>) -> TransformDef {
        TransformDef {
            target: "t".to_string(),
            source: "s".to_string(),
            key_space: KeySpace::OneToOne,
            fields,
            predicate: Predicate::True,
            without_source_index: false,
            explicit_source_schema: None,
            explicit_target_schema: None,
        }
    }

    fn col(name: &str) -> Expr {
        Expr::Column(name.to_string())
    }

    fn add(lhs: Expr, rhs: Expr) -> Expr {
        Expr::BinaryOp {
            op: Operator::Add,
            lhs: Box::new(lhs),
            rhs: Box::new(rhs),
        }
    }

    fn field(name: &str, expr: Expr) -> FieldDef {
        FieldDef {
            name: name.to_string(),
            expr,
        }
    }

    fn closed(d: &TransformDef, seeds: &[&str]) -> Vec<String> {
        let mut set: HashSet<String> = seeds.iter().map(|s| s.to_string()).collect();
        AliasReaders::of(d).close(&mut set);
        let mut out: Vec<String> = set.into_iter().collect();
        out.sort();
        out
    }

    /// A field reading a paused one by alias, directly or through a chain, is
    /// closed into the paused set; a field naming itself reads its source
    /// column, not itself, and an unrelated field stays out.
    #[test]
    fn alias_readers_close_over_direct_and_transitive_readers_only() {
        let d = def(vec![
            field("c3", col("c3")),
            field("derived", add(col("c3"), col("price"))),
            field("chained", add(col("derived"), col("derived"))),
            field("other", col("price")),
        ]);
        assert_eq!(closed(&d, &["c3"]), ["c3", "chained", "derived"]);
        assert_eq!(closed(&d, &["derived"]), ["chained", "derived"]);
        assert_eq!(closed(&d, &["other"]), ["other"]);
        assert_eq!(AliasReaders::of(&d).direct("c3"), ["derived"]);
        assert!(AliasReaders::of(&d).direct("chained").is_empty());
    }

    /// In an aggregate's argument, a name that matches an aggregate field
    /// reads the source column (`SUM(val) AS val, AVG(val) AS mean`), so
    /// `mean` doesn't pause with `val`; a name matching a non-aggregate
    /// field (`(id + 1) AS adj, SUM(adj) AS t`) is still a read by alias.
    #[test]
    fn an_aggregate_argument_reads_an_aggregate_fields_name_as_its_source_column() {
        let d = crate::defs::parse(
            "TRANSFORM t FROM s GROUP BY grp SELECT grp AS grp, SUM(val) AS val, \
             AVG(val) AS mean, (grp + 1) AS adj, SUM(adj) AS total",
        )
        .expect("parse");
        let readers = AliasReaders::of(&d);
        assert!(readers.direct("val").is_empty());
        assert_eq!(readers.direct("adj"), ["total"]);
        assert_eq!(closed(&d, &["val"]), ["val"]);
    }
}
