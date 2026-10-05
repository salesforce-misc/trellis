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
