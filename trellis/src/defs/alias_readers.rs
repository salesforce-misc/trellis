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
