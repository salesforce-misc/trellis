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
//! `defs::backfill::backfill_altered_columns`, and a rebuild's orphan delete,
//! `intake::resume_orphans`) takes a `&mut TargetMutations`
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
//! The seam only stages for readers that are already `live` when the writer
//! checks, so a *new* reader parks a catch-up on its source target once it
//! goes live, whichever way it was built (`defs::catalog::install_definition`,
//! `create_definition_inner`, `complete_direct_backfill`, or
//! `intake::publication`'s deferred-backfill flip). A write that raced the
//! reader's build reaches it through that catch-up: its fence is captured
//! after the reader is visibly live, so it waits out every writer that
//! checked before then.
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
//! extended to return each row's image), or, for a delete with no pre-lock (a
//! truncate clear, a rebuild's orphan delete), with the delete's own
//! `RETURNING`.
//! Postgres 17's `RETURNING` cannot name the pre-update row, so the pre-lock
//! is the only place an update's old image can come from.
//!
//! [`TargetMutations::image_sql`] only returns an image expression for a
//! target that has a reader, so a terminal target (the common case) pays
//! nothing for the capture.
//!
//! # The write token
//!
//! [`TargetMutations::into_staged`] also reads the transaction's **write
//! token**, `pg_current_wal_insert_lsn()`, into [`Propagation::write_token`]
//! (issue #401, step 1 of #375's direction 1). It orders one key's writes in
//! the same WAL space as CDC's commit LSNs, which is what lets the seam stand
//! in as a CDC-shaped feed for a target (steps 2 and 3, #402/#403). Nothing
//! stages it yet.
//!
//! Where it is read is the whole point. Two writers of one key serialize on
//! that key's row lock: the second acquires it only after the first commits,
//! and then writes WAL of its own. A token read after every lock and write
//! the transaction takes is therefore strictly above the previous holder's
//! commit LSN, so one key's tokens increase in commit order. `into_staged`
//! runs after every writer in the transaction has finished (it is the only
//! way the accumulator leaves the transaction), so reading it there is after
//! the last lock by construction. A token read *before* a lock is not
//! ordered against the previous holder's commit at all.
//!
//! The token is **pre-commit**: it is below the writer's own commit LSN, so it
//! is not a commit `end_lsn`. A consumer may rely on "token above a position
//! read by another transaction means the writer committed after that read",
//! and on per-key order, but never on "a token at or below X means the writer
//! had committed by X". It is never an `origin_lsn` either: a seam row's
//! origin stays the conservative "unknown" `await_converged` already gates on
//! (see `staging::converge::converged_through`).
//!
//! It is read only when this transaction stages something (some key of a
//! target a `live` definition reads), so a terminal target pays no extra
//! round trip.

use std::collections::{BTreeMap, HashMap};
use std::time::SystemTime;

use tokio_postgres::Transaction;
use tokio_postgres::types::PgLsn;

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
    /// Test-only: `Some(has_readers)` treats every target as read (or
    /// unread) without asking the catalog, for unit tests that drive a
    /// writer against a bare database with no Trellis schema in it.
    #[cfg(test)]
    assume_readers: Option<bool>,
}

/// [`TargetMutations::into_staged`]'s output: the downstream `Recompute` rows
/// to append, every target whose propagation would exceed [`MAX_HOP_GEN`]
/// with the worst hop generation seen, and the transaction's write token.
pub(crate) struct Propagation {
    pub changes: Vec<StagedChange>,
    pub hop_bound_tables: Vec<String>,
    pub worst_hop_gen: i32,
    /// `pg_current_wal_insert_lsn()`, read after every row lock and write
    /// this transaction took (see the module doc's "The write token"), or
    /// `None` when the transaction stages nothing. One token covers every
    /// key the transaction changed.
    pub write_token: Option<PgLsn>,
}

impl TargetMutations {
    pub fn new() -> Self {
        Self::default()
    }

    /// A [`TargetMutations`] that never consults the catalog and treats every
    /// target as unread — see `assume_readers`.
    #[cfg(test)]
    pub(crate) fn assuming_unread() -> Self {
        Self {
            assume_readers: Some(false),
            ..Self::default()
        }
    }

    /// A [`TargetMutations`] that never consults the catalog and treats every
    /// target as read (with no image columns) — see `assume_readers`.
    #[cfg(test)]
    pub(crate) fn assuming_read() -> Self {
        Self {
            assume_readers: Some(true),
            ..Self::default()
        }
    }

    async fn info(
        &mut self,
        txn: &Transaction<'_>,
        target: &str,
    ) -> Result<&TargetInfo, ApplyError> {
        #[cfg(test)]
        if let Some(has_readers) = self.assume_readers {
            self.targets
                .entry(target.to_string())
                .or_insert(TargetInfo {
                    has_readers,
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
    ///
    /// Also reads [`Propagation::write_token`]. The caller must not take any
    /// further row lock on a target after this (see the module doc's "The
    /// write token"); every caller calls it last, just before commit.
    pub(crate) async fn into_staged(
        mut self,
        txn: &Transaction<'_>,
    ) -> Result<Propagation, ApplyError> {
        let mut propagation = Propagation {
            changes: Vec::new(),
            hop_bound_tables: Vec::new(),
            worst_hop_gen: 0,
            write_token: None,
        };
        let touched = std::mem::take(&mut self.touched);
        for (target, keys) in touched {
            if !self.info(txn, &target).await?.has_readers {
                continue;
            }
            // Every writer in this transaction has already taken its row
            // locks and written, so this is after the last of them. Read
            // once, before the first staged row, so a row built here can
            // carry it.
            if propagation.write_token.is_none() {
                propagation.write_token = Some(read_write_token(txn).await?);
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

/// The current WAL insert position, as this transaction's write token — see
/// the module doc's "The write token". The same device #321's recompute
/// horizon uses.
async fn read_write_token(txn: &Transaction<'_>) -> Result<PgLsn, ApplyError> {
    Ok(txn
        .query_one("select pg_current_wal_insert_lsn()", &[])
        .await?
        .get(0))
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

    async fn connect(dsn: &str) -> tokio_postgres::Client {
        let (client, connection) = tokio_postgres::connect(dsn, tokio_postgres::NoTls)
            .await
            .expect("connect");
        tokio::spawn(async move {
            let _ = connection.await;
        });
        client
    }

    async fn wal_insert_lsn(client: &impl tokio_postgres::GenericClient) -> PgLsn {
        client
            .query_one("select pg_current_wal_insert_lsn()", &[])
            .await
            .expect("read the WAL insert position")
            .get(0)
    }

    /// Caution 1 of #375's direction 1: two writers of one key serialize on
    /// its row lock, so a token read after the lock orders the second writer
    /// above the first one's commit. Writer B is shaped like every real seam
    /// writer (`image_sql`, then the locking write, then `record`, then
    /// `into_staged`) and blocks on writer A's lock mid-write.
    ///
    /// - A's own token is below A's commit (caution 2: pre-commit).
    /// - A position B reads before its lock (where `image_sql` runs) is below
    ///   A's commit too, which is why the token must not be read there.
    /// - B's token is at or above a position read after A's commit returned,
    ///   so above A's commit record.
    #[tokio::test]
    async fn a_second_writer_of_a_key_takes_its_token_after_the_first_writers_commit() {
        let cluster = testkit::TestCluster::start();
        let db = cluster.create_isolated_database().await;
        let mut a = connect(db.dsn()).await;
        let mut b = connect(db.dsn()).await;
        a.batch_execute("create table t (id int primary key, v int); insert into t values (1, 0)")
            .await
            .expect("create and seed t");

        let txn_a = a.transaction().await.expect("begin A");
        let mut m_a = TargetMutations::assuming_read();
        txn_a
            .execute("update t set v = 1 where id = 1", &[])
            .await
            .expect("A's write");
        m_a.record("public.t", "1".into(), None, 0, None);
        let token_a = m_a
            .into_staged(&txn_a)
            .await
            .expect("stage A")
            .write_token
            .expect("A staged a row, so it read a token");

        let txn_b = b.transaction().await.expect("begin B");
        let mut m_b = TargetMutations::assuming_read();
        m_b.image_sql(&txn_b, "public.t", "t")
            .await
            .expect("B resolves its image expression before locking");
        let before_b_locks = wal_insert_lsn(&txn_b).await;
        let (after_a_commits, written_b) = {
            let write_b = txn_b.execute("update t set v = 2 where id = 1", &[]);
            tokio::pin!(write_b);
            assert!(
                tokio::time::timeout(std::time::Duration::from_millis(100), &mut write_b)
                    .await
                    .is_err(),
                "B's write waits on A's row lock"
            );
            txn_a.commit().await.expect("commit A");
            let after_a_commits = wal_insert_lsn(&a).await;
            (after_a_commits, write_b.await.expect("B's write"))
        };
        assert_eq!(written_b, 1);
        m_b.record("public.t", "1".into(), None, 0, None);
        let token_b = m_b
            .into_staged(&txn_b)
            .await
            .expect("stage B")
            .write_token
            .expect("B staged a row, so it read a token");
        txn_b.commit().await.expect("commit B");

        assert!(
            token_a < after_a_commits,
            "A's token ({token_a}) is pre-commit, below A's commit (<= {after_a_commits})"
        );
        assert!(
            before_b_locks < after_a_commits,
            "a position B read before its lock ({before_b_locks}) is not ordered after A's \
             commit (<= {after_a_commits})"
        );
        assert!(
            token_b >= after_a_commits,
            "B's token ({token_b}) is at or above a position read after A committed \
             ({after_a_commits})"
        );
    }

    /// The token is read at staging time, after every write the transaction
    /// made, not when the first key was recorded.
    #[tokio::test]
    async fn the_token_follows_the_transactions_last_write() {
        let cluster = testkit::TestCluster::start();
        let db = cluster.create_isolated_database().await;
        let mut client = connect(db.dsn()).await;
        client
            .batch_execute("create table t (id int primary key, v int)")
            .await
            .expect("create t");

        let txn = client.transaction().await.expect("begin");
        let mut m = TargetMutations::assuming_read();
        txn.execute("insert into t values (1, 0)", &[])
            .await
            .expect("first write");
        m.record("public.t", "1".into(), None, 0, None);
        let after_first_write = wal_insert_lsn(&txn).await;
        txn.execute(
            "insert into t select g, 0 from generate_series(2, 100) g",
            &[],
        )
        .await
        .expect("last write");
        for id in 2..=100 {
            m.record("public.t", id.to_string(), None, 0, None);
        }
        let after_last_write = wal_insert_lsn(&txn).await;
        let propagation = m.into_staged(&txn).await.expect("stage");
        let token = propagation.write_token.expect("rows staged, so a token");

        assert_eq!(propagation.changes.len(), 100);
        assert!(
            after_first_write < after_last_write,
            "the last write wrote WAL"
        );
        assert!(
            token >= after_last_write,
            "the token ({token}) is at or after the last write ({after_last_write})"
        );
    }

    /// A transaction that stages nothing pays no round trip for a token.
    #[tokio::test]
    async fn a_transaction_that_stages_nothing_reads_no_token() {
        let cluster = testkit::TestCluster::start();
        let db = cluster.create_isolated_database().await;
        let mut client = connect(db.dsn()).await;

        let txn = client.transaction().await.expect("begin");
        let mut unread = TargetMutations::assuming_unread();
        unread.record("public.t", "1".into(), None, 0, None);
        let propagation = unread.into_staged(&txn).await.expect("stage unread");
        assert!(propagation.changes.is_empty());
        assert_eq!(
            propagation.write_token, None,
            "a terminal target reads no token"
        );

        let untouched = TargetMutations::assuming_read();
        let propagation = untouched.into_staged(&txn).await.expect("stage untouched");
        assert_eq!(propagation.write_token, None, "no key changed, no token");
    }
}
