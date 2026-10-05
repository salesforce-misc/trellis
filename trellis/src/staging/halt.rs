//! The drain's halt (#663): what a [`FailureClass::Halting`] failure parks.
//!
//! A halting failure is structural, not one row's fault: a key the drain
//! can't use (`NoPrimaryKey`, `UnsupportedPrimaryKeyType`), a propagation
//! wave past the hop bound (`HopBoundExceeded`), or an aggregate target off
//! the ledger (`AggregateOffLedger`). Every key it touches reproduces it, so
//! retrying the page re-fails it forever, and quarantining a key blames one
//! for nobody's fault. Instead [`halt_closure`] pauses the definitions the
//! failure reaches, and the drain retries the page without them:
//!
//! - every definition that reads the halting table, as its source or as a
//!   relationship's to-side (`capture::columns::readers_of`). For a hop
//!   bound, the halting tables are those the wave ran away through, so their
//!   readers are the cycle's definitions. For an aggregate off the ledger,
//!   it's the definition that writes that target;
//! - and everything downstream of them, so no hop target goes quietly stale.
//!
//! Each is paused like a capture failure (`capture_failures`, with `kind`
//! `halt`). Its rows then drain with the page: a paused definition's share is
//! dropped, and a table with no reader that isn't frozen is skipped
//! (`apply::source_key_for_apply`, #768), so the page commits, its claims
//! are marked drained and its segments retire. Resuming a definition is the
//! rebuild it is for any pause (`quarantine::resume_transform`).
//!
//! One halt is one episode: the pauses and the halting-stop record commit in
//! one transaction, and only a call that newly paused something records the
//! stop. A peer that met the same failure, or a retry that meets it again,
//! finds the closure already frozen and records nothing.
//!
//! [`FailureClass::Halting`]: super::quarantine::FailureClass::Halting

use std::collections::{BTreeSet, HashSet};

use crate::capture::columns::{CaptureCatalog, load_catalog, readers_of};
use crate::defs::ddl::DdlError;
use crate::defs::model::TransformStatus;
use crate::pool::Pool;

use super::apply::ApplyError;
use super::quarantine;

/// Where a halting failure starts its closure.
enum Seed {
    /// Every definition reading these tables.
    Tables(Vec<String>),
    /// The definition writing this target.
    Target(String),
}

/// The tables or target `err`'s innermost [`ApplyError`] names, when it's a
/// halting one.
fn seed(err: &ApplyError) -> Option<Seed> {
    match quarantine::innermost_apply_error(err) {
        ApplyError::Ddl(DdlError::NoPrimaryKey { source_table })
        | ApplyError::Ddl(DdlError::UnsupportedPrimaryKeyType { source_table, .. }) => {
            Some(Seed::Tables(vec![source_table.clone()]))
        }
        ApplyError::HopBoundExceeded { tables, .. } => Some(Seed::Tables(tables.clone())),
        ApplyError::AggregateOffLedger { target } => Some(Seed::Target(target.clone())),
        _ => None,
    }
}

/// Pauses every definition that isn't frozen and that the halting failure
/// `err` reaches (see the module doc), and, if that paused any, records the
/// halting stop, all in one transaction. Returns the bare targets of the
/// definitions this call paused, in id order: empty when every one was
/// already frozen (a peer halted first), or when `err` isn't a halting
/// failure this module seeds a closure from.
pub async fn halt_closure(pool: &Pool, err: &ApplyError) -> Result<Vec<String>, ApplyError> {
    let Some(seed) = seed(err) else {
        return Ok(Vec::new());
    };
    let seed = match seed {
        Seed::Tables(tables) => {
            let mut qualified = Vec::with_capacity(tables.len());
            for table in tables {
                qualified.push(quarantine::qualified_src_table(pool, &table).await?);
            }
            Seed::Tables(qualified)
        }
        target => target,
    };

    let mut client = pool.get().await?;
    let txn = client.transaction().await?;
    let catalog = load_catalog(&*txn, pool.schema()).await?;
    let unfrozen: HashSet<i64> = txn
        .query(
            "select id from transform_definitions where status = any($1)",
            &[&TransformStatus::dispatchable()],
        )
        .await?
        .iter()
        .map(|row| row.get(0))
        .collect();
    let members = closure(&catalog, &seed, &unfrozen);

    let source_table = match &seed {
        Seed::Tables(tables) => tables.join(", "),
        Seed::Target(target) => target.clone(),
    };
    let error = format!(
        "the drain halted: {err}; fix the cause, then resume the definition to rebuild it, \
         or drop the definition"
    );
    let mut paused = Vec::new();
    for id in members {
        if crate::defs::lifecycle::pause_for_halt(&txn, id, &source_table, &error).await? {
            let reader = catalog.definitions.iter().find(|r| r.id == id);
            paused.push(reader.map_or_else(|| id.to_string(), |r| r.def.target.clone()));
        }
    }
    if !paused.is_empty() {
        quarantine::record_halting_stop(&txn, &err.to_string()).await?;
    }
    txn.commit().await?;
    Ok(paused)
}

/// The ids of the definitions in `unfrozen` that `seed` reaches: its
/// readers (or writer), then every reader of each one's target, to a
/// fixpoint. A frozen definition is left out and not followed, since it
/// applies nothing for its own readers to see.
fn closure(catalog: &CaptureCatalog, seed: &Seed, unfrozen: &HashSet<i64>) -> BTreeSet<i64> {
    let mut members = BTreeSet::new();
    let mut tables: Vec<String> = Vec::new();
    match seed {
        Seed::Tables(seeds) => tables.extend(seeds.iter().cloned()),
        Seed::Target(target) => {
            for reader in &catalog.definitions {
                if (reader.target == *target || reader.def.target == *target)
                    && unfrozen.contains(&reader.id)
                    && members.insert(reader.id)
                {
                    tables.push(reader.target.clone());
                }
            }
        }
    }
    let mut seen = HashSet::new();
    while let Some(table) = tables.pop() {
        if !seen.insert(table.clone()) {
            continue;
        }
        for id in readers_of(catalog, &table, &BTreeSet::new(), true) {
            if unfrozen.contains(&id)
                && members.insert(id)
                && let Some(reader) = catalog.definitions.iter().find(|r| r.id == id)
            {
                tables.push(reader.target.clone());
            }
        }
    }
    members
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use crate::capture::columns::CaptureReader;
    use crate::defs::ast::RelationshipDef;
    use crate::defs::model::{RelationshipCardinality, RelationshipDefinition};
    use crate::defs::parse;

    /// `(source, target, text)` per definition, ids from 1, with `author`
    /// (`posts.author` to `users.handle`) declared.
    fn catalog(definitions: &[(&str, &str, &str)]) -> CaptureCatalog {
        CaptureCatalog {
            definitions: definitions
                .iter()
                .enumerate()
                .map(|(i, (source, target, text))| CaptureReader {
                    id: i as i64 + 1,
                    source: source.to_string(),
                    def: parse(text).unwrap_or_else(|e| panic!("{text}: {e:?}")),
                    capture_failed: false,
                    target: target.to_string(),
                })
                .collect(),
            relationships: vec![RelationshipDefinition {
                id: 1,
                from_schema: "public".to_string(),
                to_schema: "public".to_string(),
                def: RelationshipDef {
                    name: "author".to_string(),
                    from_table: "posts".to_string(),
                    from_col: "author".to_string(),
                    to_table: "users".to_string(),
                    to_col: "handle".to_string(),
                },
                cardinality: RelationshipCardinality::ToOne,
                warnings: Vec::new(),
            }],
            projection_columns: HashMap::new(),
        }
    }

    fn all(catalog: &CaptureCatalog) -> HashSet<i64> {
        catalog.definitions.iter().map(|r| r.id).collect()
    }

    fn tables(tables: &[&str]) -> Seed {
        Seed::Tables(tables.iter().map(|t| t.to_string()).collect())
    }

    /// A key seed reaches the table's direct reader, the definition reading
    /// it as a relationship's to-side, and what reads either one's target,
    /// to any depth, and nothing else.
    #[test]
    fn a_key_seed_reaches_readers_relationship_readers_and_their_downstream() {
        let catalog = catalog(&[
            (
                "public.users",
                "public.users_copy",
                "TRANSFORM users_copy FROM users SELECT name AS name",
            ),
            (
                "public.posts",
                "public.posts_named",
                "TRANSFORM posts_named FROM posts SELECT author.name AS name",
            ),
            (
                "public.posts",
                "public.posts_plain",
                "TRANSFORM posts_plain FROM posts SELECT author AS author",
            ),
            (
                "public.posts_named",
                "public.named_copy",
                "TRANSFORM named_copy FROM posts_named SELECT name AS name",
            ),
            (
                "public.named_copy",
                "public.named_copy_2",
                "TRANSFORM named_copy_2 FROM named_copy SELECT name AS name",
            ),
            (
                "public.posts_plain",
                "public.plain_copy",
                "TRANSFORM plain_copy FROM posts_plain SELECT author AS author",
            ),
        ]);
        assert_eq!(
            closure(&catalog, &tables(&["public.users"]), &all(&catalog)),
            BTreeSet::from([1, 2, 4, 5])
        );
    }

    /// A hop bound's seed is the cycle's tables: every definition on the
    /// cycle, and what is downstream of it, whichever table the wave ran
    /// away through.
    #[test]
    fn a_hop_bound_seed_reaches_the_cycle_and_its_downstream() {
        let catalog = catalog(&[
            ("public.a", "public.b", "TRANSFORM b FROM a SELECT v AS v"),
            ("public.b", "public.c", "TRANSFORM c FROM b SELECT v AS v"),
            ("public.c", "public.a", "TRANSFORM a FROM c SELECT v AS v"),
            ("public.c", "public.d", "TRANSFORM d FROM c SELECT v AS v"),
            ("public.x", "public.y", "TRANSFORM y FROM x SELECT v AS v"),
        ]);
        for table in ["public.a", "public.b", "public.c"] {
            assert_eq!(
                closure(&catalog, &tables(&[table]), &all(&catalog)),
                BTreeSet::from([1, 2, 3, 4]),
                "{table}"
            );
        }
    }

    /// A frozen definition is neither a member nor followed: it applies
    /// nothing, so what reads its target reads nothing new.
    #[test]
    fn a_frozen_definition_is_left_out_and_not_followed() {
        let catalog = catalog(&[
            ("public.a", "public.b", "TRANSFORM b FROM a SELECT v AS v"),
            ("public.b", "public.c", "TRANSFORM c FROM b SELECT v AS v"),
            ("public.a", "public.e", "TRANSFORM e FROM a SELECT v AS v"),
        ]);
        let unfrozen = HashSet::from([2, 3]);
        assert_eq!(
            closure(&catalog, &tables(&["public.a"]), &unfrozen),
            BTreeSet::from([3])
        );
    }

    /// An aggregate off the ledger names its target bare: the seed is the
    /// definition writing it, and what is downstream of it.
    #[test]
    fn a_target_seed_reaches_its_writer_and_its_downstream() {
        let catalog = catalog(&[
            (
                "public.s",
                "public.agg",
                "TRANSFORM agg FROM s GROUP BY g SELECT g AS g, SUM(v) AS total",
            ),
            (
                "public.agg",
                "public.agg_copy",
                "TRANSFORM agg_copy FROM agg SELECT total AS total",
            ),
            ("public.s", "public.t", "TRANSFORM t FROM s SELECT v AS v"),
        ]);
        for target in ["agg", "public.agg"] {
            assert_eq!(
                closure(&catalog, &Seed::Target(target.to_string()), &all(&catalog)),
                BTreeSet::from([1, 2]),
                "{target}"
            );
        }
    }
}
