//! The one seam every write to a Trellis-owned target table goes through
//! (issue #315).
//!
//! # Why a seam, not a publication
//!
//! A target table is not a member of this instance's CDC publication (see
//! `defs::catalog::publication_tables`; the one exception, a target that is
//! also a relationship endpoint, is documented there). A definition that reads another
//! definition's target (a chained hop) learns about that target's changes
//! only from the image-less `Recompute` rows the writer stages here, inside
//! the same transaction as the write itself. That gives three properties CDC
//! could not:
//!
//! - **The key is correct by construction.** Each staged key is the target's
//!   own row identity (`ddl::pk_key_sql_expr`, or the aggregate
//!   `derive_group_key` text that matches it), produced by the code that
//!   wrote the row. Nothing reconstructs a NULL-safe group key from raw
//!   `pgoutput` bytes, which `intake::extract_key` cannot do correctly for an
//!   aggregate target.
//! - **No replica-identity cost.** A target needs no `REPLICA IDENTITY FULL`,
//!   so its updates add no old-row images to WAL and no pressure on the slot.
//! - **Crash safety.** The staged rows commit or roll back with the write, so
//!   there is no window where a target changed but its downstream signal was
//!   lost, and no window where the signal exists without the change.
//!
//! It also closes the double-apply class #312 patched: a target change now
//! reaches the ring exactly once, not once from the apply and again from CDC.
//!
//! # Why it is structural
//!
//! Every target writer (`apply::apply_target`, `apply_aggregate::apply_aggregate_target`,
//! the truncate clears (`apply::clear_target`), `quarantine::recompute_column`,
//! `defs::backfill::backfill_altered_columns`) takes a `&mut TargetMutations`
//! and reports each physically-changed key into it. None of them returns the
//! changed keys to its caller, so a caller cannot forget to propagate them:
//! the only way a changed key leaves a writer is through [`TargetMutations::record`],
//! and the only way a [`TargetMutations`] leaves a transaction is through
//! [`TargetMutations::into_staged`] (or [`TargetMutations::flush`], which
//! calls it). A new writer has to take this parameter to compile against the
//! shared helpers, which is the point.
//!
//! The only target writes that bypass the seam are a definition's own
//! initial build (`defs::backfill::backfill_definition` and the chunk queue),
//! which run while that definition is not yet `live`. Nothing can read a
//! target in that state: `defs::catalog::create_definition_inner` refuses a
//! definition whose source is a non-`live` target (`CatalogError::TransformNotLive`),
//! and a target that goes `live` with readers already attached (a resumed
//! upstream) parks a catch-up marker for itself so those readers re-derive
//! from its rebuilt state.
//!
//! # What gets staged
//!
//! One `StagedChange::Recompute` per changed key, for every target at least
//! one `live` definition reads. The row is image-less, so a downstream
//! aggregate re-derives the affected group from live state and a downstream
//! 1-1 re-reads the row: both idempotent, whatever batch the signal lands in.
//!
//! Each row also carries the key's **prior image**: the target row as it
//! stood before this transaction first touched it (`None` if the transaction
//! created it). A downstream aggregate grouped by a non-key column of this
//! target needs it. When an upstream write moves a row from group A to group
//! B, the live re-read only names B, so without the prior image group A would
//! keep the row's stale contribution. With it, the aggregate re-derives both
//! A and B (see `apply_aggregate::accumulate_changes`). A deleted key's prior
//! image is how the aggregate finds the group it left.
//!
//! Writers capture the prior image with the same statement that already
//! row-locks the key before writing it (a `SELECT ... FOR UPDATE` pre-lock,
//! extended to return each row's image), or, for a truncate clear, with the
//! delete's own `RETURNING`.
//! Postgres 17's `RETURNING` cannot name the pre-update row, so the pre-lock
//! is the only place an update's old image can come from.
//!
//! [`TargetMutations::image_sql`] only returns an image expression for a
//! target that has a reader, so a terminal target (the common case) pays
//! nothing for the capture.

use std::collections::{BTreeMap, HashMap};
use std::time::SystemTime;

use tokio_postgres::Transaction;

use super::append::{self, StagedChange};
use super::apply::{ApplyError, MAX_HOP_GEN, earliest_src_changed, live_row_columns};

/// One changed key's accumulated state — see [`TargetMutations::record`].
#[derive(Debug, Clone)]
struct KeyMutation {
    prior_image: Option<String>,
    hop_gen: i32,
    src_changed: Option<SystemTime>,
}

/// What [`TargetMutations`] knows about one target, resolved once per
/// accumulator (so once per Phase 3 transaction).
#[derive(Debug, Clone)]
struct TargetInfo {
    /// Whether any `live` definition reads this target as its source.
    has_readers: bool,
    /// The target's live column list, only resolved when `has_readers`.
    image_columns: Vec<String>,
}

/// Every key one transaction changed in every target table it wrote, keyed
/// by the target's qualified identity (`transform_definitions.target_table`).
/// See the module doc comment.
#[derive(Debug, Default)]
#[must_use = "a TargetMutations must be staged (into_staged/flush) in the transaction that wrote it"]
pub struct TargetMutations {
    targets: HashMap<String, TargetInfo>,
    touched: BTreeMap<String, BTreeMap<String, KeyMutation>>,
    /// Test-only: treat every target as unread without asking the catalog,
    /// for unit tests that drive a writer against a bare database with no
    /// Trellis schema in it.
    #[cfg(test)]
    assume_unread: bool,
}

/// [`TargetMutations::into_staged`]'s output: the downstream `Recompute` rows
/// to append, and every target whose propagation would exceed
/// [`MAX_HOP_GEN`], with the worst hop generation seen.
pub(crate) struct Propagation {
    pub changes: Vec<StagedChange>,
    pub hop_bound_tables: Vec<String>,
    pub worst_hop_gen: i32,
}

impl TargetMutations {
    pub fn new() -> Self {
        Self::default()
    }

    /// A [`TargetMutations`] that never consults the catalog and treats every
    /// target as unread — see `assume_unread`.
    #[cfg(test)]
    pub(crate) fn assuming_unread() -> Self {
        Self {
            assume_unread: true,
            ..Self::default()
        }
    }

    async fn info(
        &mut self,
        txn: &Transaction<'_>,
        target: &str,
    ) -> Result<&TargetInfo, ApplyError> {
        #[cfg(test)]
        if self.assume_unread {
            self.targets
                .entry(target.to_string())
                .or_insert(TargetInfo {
                    has_readers: false,
                    image_columns: Vec::new(),
                });
        }
        if !self.targets.contains_key(target) {
            // Mirrors `catalog::dependents_of`'s own `status = 'live'`
            // filter: a reader that isn't live yet is excluded from applies
            // anyway, and catches up from its own backfill or catch-up marker.
            let has_readers: bool = txn
                .query_one(
                    "select exists (select 1 from transform_definitions \
                     where source_table = $1 and status = 'live')",
                    &[&target],
                )
                .await?
                .get(0);
            let image_columns = if has_readers {
                live_row_columns(txn, target).await?
            } else {
                Vec::new()
            };
            self.targets.insert(
                target.to_string(),
                TargetInfo {
                    has_readers,
                    image_columns,
                },
            );
        }
        Ok(&self.targets[target])
    }

    /// The SQL expression (over `alias`) that renders one of `target`'s rows
    /// as the prior-image text a writer should capture, or `None` when no
    /// `live` definition reads `target` and so nothing would ever consume it.
    /// Built with `apply::row_as_text_jsonb_sql`, the same rendering every
    /// other image in the ring uses (issue #248).
    pub async fn image_sql(
        &mut self,
        txn: &Transaction<'_>,
        target: &str,
        alias: &str,
    ) -> Result<Option<String>, ApplyError> {
        let info = self.info(txn, target).await?;
        Ok(info
            .has_readers
            .then(|| super::apply::row_as_text_jsonb_sql(alias, &info.image_columns)))
    }

    /// Records that this transaction physically changed (wrote or deleted)
    /// `key` in `target`. `prior_image` is the row before the write (see the
    /// module doc comment); pass `None` for a key the write created, or when
    /// [`Self::image_sql`] returned `None`.
    ///
    /// A key touched more than once in one transaction keeps its *first*
    /// prior image (the state every downstream consumer last saw), the
    /// highest `hop_gen`, and the earliest `src_changed`.
    pub fn record(
        &mut self,
        target: &str,
        key: String,
        prior_image: Option<String>,
        hop_gen: i32,
        src_changed: Option<SystemTime>,
    ) {
        self.touched
            .entry(target.to_string())
            .or_default()
            .entry(key)
            .and_modify(|m| {
                m.hop_gen = m.hop_gen.max(hop_gen);
                m.src_changed = earliest_src_changed(m.src_changed, src_changed);
            })
            .or_insert(KeyMutation {
                prior_image,
                hop_gen,
                src_changed,
            });
    }

    /// Every key recorded for `target`, in key order — for tests.
    #[cfg(test)]
    pub(crate) fn recorded_keys(&self, target: &str) -> Vec<String> {
        self.touched
            .get(target)
            .map(|keys| keys.keys().cloned().collect())
            .unwrap_or_default()
    }

    /// Turns every recorded key of a target with a `live` reader into its
    /// downstream `Recompute` row at `hop_gen + 1`. A key whose next hop
    /// would pass [`MAX_HOP_GEN`] is not staged; its target is reported in
    /// [`Propagation::hop_bound_tables`] instead, for the caller to fail the
    /// transaction with [`ApplyError::HopBoundExceeded`] (alongside any other
    /// hop-bounded rows it stages).
    pub(crate) async fn into_staged(
        mut self,
        txn: &Transaction<'_>,
    ) -> Result<Propagation, ApplyError> {
        let mut propagation = Propagation {
            changes: Vec::new(),
            hop_bound_tables: Vec::new(),
            worst_hop_gen: 0,
        };
        let touched = std::mem::take(&mut self.touched);
        for (target, keys) in touched {
            if !self.info(txn, &target).await?.has_readers {
                continue;
            }
            for (key, m) in keys {
                let next_hop = m.hop_gen + 1;
                if next_hop > MAX_HOP_GEN {
                    propagation.hop_bound_tables.push(target.clone());
                    propagation.worst_hop_gen = propagation.worst_hop_gen.max(next_hop);
                    continue;
                }
                propagation.changes.push(StagedChange::Recompute {
                    src_table: target.clone(),
                    key,
                    hop_gen: next_hop,
                    group_key: None,
                    src_changed: m.src_changed,
                    prior_image: m.prior_image,
                });
            }
        }
        Ok(propagation)
    }

    /// [`Self::into_staged`] plus the append, for a writer outside the drain
    /// path (which folds its own hop-bound check into a wider one).
    pub async fn flush(self, txn: &Transaction<'_>) -> Result<(), ApplyError> {
        let mut propagation = self.into_staged(txn).await?;
        if !propagation.hop_bound_tables.is_empty() {
            propagation.hop_bound_tables.sort();
            propagation.hop_bound_tables.dedup();
            return Err(ApplyError::HopBoundExceeded {
                hop_gen: propagation.worst_hop_gen,
                tables: propagation.hop_bound_tables,
            });
        }
        append::append(txn, &propagation.changes).await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_key_touched_twice_keeps_its_first_prior_image() {
        let mut m = TargetMutations::new();
        m.record(
            "public.t",
            "1".into(),
            Some("{\"g\":\"a\"}".into()),
            0,
            None,
        );
        m.record(
            "public.t",
            "1".into(),
            Some("{\"g\":\"b\"}".into()),
            3,
            None,
        );
        m.record("public.t", "2".into(), None, 1, None);
        assert_eq!(m.touched["public.t"].len(), 2);
        let key = &m.touched["public.t"]["1"];
        assert_eq!(key.prior_image.as_deref(), Some("{\"g\":\"a\"}"));
        assert_eq!(key.hop_gen, 3);
    }

    #[test]
    fn a_key_created_in_the_transaction_keeps_no_prior_image() {
        let mut m = TargetMutations::new();
        m.record("public.t", "1".into(), None, 0, None);
        m.record(
            "public.t",
            "1".into(),
            Some("{\"g\":\"a\"}".into()),
            0,
            None,
        );
        assert_eq!(m.touched["public.t"]["1"].prior_image, None);
    }
}
