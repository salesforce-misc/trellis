//! Statements shared by the 1-1 side ledger (`super::one_to_one_ledger`) and
//! the writers around it: the plan settings every entry-keyed statement runs
//! under, the entry lock, ADR-0002 I2's apply predicate, and the build-start
//! check a page makes.

use tokio_postgres::Transaction;

use crate::defs::ledger as schema;
use crate::pool::quote_ident;

/// ADR-0002 I2 as SQL over the ledger entry `l`, the batch row `v` (its
/// `lsn` and `txid`) and the target's truncate floor `fl`: whether an Apply
/// changes the entry.
pub(super) fn apply_predicate() -> String {
    let basis = quote_ident(schema::BASIS_COLUMN);
    let applied = quote_ident(schema::APPLIED_LSN_COLUMN);
    format!(
        "(l.{basis} is null or not pg_visible_in_snapshot(v.__txid, l.{basis})) \
         and (l.{applied} is null or v.__lsn > l.{applied}) \
         and not exists (select 1 from fl where v.__lsn <= fl.floor)"
    )
}

/// The plan settings of a statement that reads or writes a ledger by its
/// entries' keys (#778): no sequential scan, so the ledger is reached
/// through its key's index, and the statement costs the keys it is given
/// rather than the ledger's size. [`ENTRY_PLAN_RESET`] puts them back after
/// the statement. They pin the entry lock ([`lock_statement`]) and a 1-1
/// page's entry update (`super::one_to_one_ledger::update_entries`). They also pin every read
/// of a source by a batch's keys (`super::apply::live_rows_query`), the
/// endpoint feed's re-read of a target by its keys
/// (`super::target_mutations::read_new_images`) and a composite-key 1-1
/// target's pre-lock (`super::apply::lock_composite_keys`), which a table's
/// lagging statistics price the same way.
///
/// Left to itself, the planner read the whole ledger for a page's few
/// thousand keys whenever it priced that cheaper, which it does while a
/// fast-growing ledger has no statistics yet, or ones from when it was
/// small: the lock as a sequential scan and a sort, and the entry update as
/// a hash join of the page's records to a sequential scan of the ledger.
/// The paged benchmark spent 200–345 ms a page on that scan, at up to
/// 1.15M entries, until `autoanalyze` caught up (#775).
///
/// The setting alone isn't enough for a join. With sequential scans off,
/// the planner hashed a scan of the ledger's whole key index instead. So
/// each statement that joins the ledger to its keys also restricts the
/// ledger to them (`l.key = any(<keys>)`), which bounds the ledger's side of
/// whatever join the planner picks. Hash and merge joins stay on: the
/// statements also join their CTEs to each other, where a nested loop would
/// be quadratic in the page.
/// A bitmap scan of the key's index is fine: it reads only the keys too.
pub(super) const ENTRY_PLAN_SETTINGS: &str = "set local enable_seqscan = off";

/// Undoes [`ENTRY_PLAN_SETTINGS`] for the rest of the transaction.
pub(super) const ENTRY_PLAN_RESET: &str = "set local enable_seqscan to default";

/// Runs `sql` under [`ENTRY_PLAN_SETTINGS`], and puts the settings back
/// after it: the one place a page's (and a build chunk's) ledger statements
/// keyed by entry run, and the plan their `explain` tests read. An error
/// leaves the settings on, but it also aborts `txn`, and a `set local` ends
/// with the transaction.
pub(crate) async fn query_by_entry_key(
    txn: &Transaction<'_>,
    sql: &str,
    params: &[&(dyn tokio_postgres::types::ToSql + Sync)],
) -> Result<Vec<tokio_postgres::Row>, tokio_postgres::Error> {
    txn.batch_execute(ENTRY_PLAN_SETTINGS).await?;
    let rows = txn.query(sql, params).await?;
    txn.batch_execute(ENTRY_PLAN_RESET).await?;
    Ok(rows)
}

/// The entry lock's `select … for update` on the ledger `ledger` (quoted,
/// qualified): every entry of the keys `$1` (`text[]`), in key order (I5).
/// One row per entry locked. Shared with the 1-1 ledger
/// (`super::one_to_one_ledger::lock_entries`).
pub(super) fn lock_statement(ledger: &str) -> String {
    let key = quote_ident(schema::KEY_COLUMN);
    format!("select 1 from {ledger} where {key} = any($1::text[]) order by {key} for update")
}

/// Whether a page whose lowest segment is `first_seg` may hold a change
/// committed before `target`'s Re-derive build started (#733): its batch
/// is at or below the target's `build_seg` (`super::build`'s start). Such a
/// page re-derives every key it holds for the target rather than applying
/// their changes (`super::apply`'s `settle_one_to_one_target`).
pub(crate) async fn page_may_predate_build(
    txn: &Transaction<'_>,
    target: &str,
    first_seg: i64,
) -> Result<bool, tokio_postgres::Error> {
    Ok(txn
        .query_one(
            "select coalesce($2 <= (select build_seg from transform_definitions \
                                    where target_table = $1), false)",
            &[&target, &first_seg],
        )
        .await?
        .get(0))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// ADR-0002 I2 evaluated by Postgres: whether an Apply of a change
    /// (`txid`, `lsn`) changes an entry (`basis`, `applied_lsn`) under the
    /// truncate `floor`.
    async fn applies(
        client: &tokio_postgres::Client,
        basis: Option<&str>,
        applied_lsn: Option<&str>,
        (txid, lsn): (&str, &str),
        floor: Option<&str>,
    ) -> bool {
        client
            .query_one(
                &format!(
                    "with l as (select $1::text::pg_snapshot as \"__basis\", \
                                       $2::text::pg_lsn as \"__applied_lsn\"), \
                          v as (select $3::text::xid8 as __txid, $4::text::pg_lsn as __lsn), \
                          fl as (select $5::text::pg_lsn as floor where $5::text is not null) \
                     select {} from l, v",
                    apply_predicate()
                ),
                &[&basis, &applied_lsn, &txid, &lsn, &floor],
            )
            .await
            .expect("evaluate the apply predicate")
            .get(0)
    }

    #[tokio::test]
    async fn the_apply_predicate_skips_what_the_basis_saw_older_lsns_and_the_truncated() {
        let cluster = testkit::TestCluster::start();
        let db = cluster.create_isolated_database().await;
        let (client, connection) = tokio_postgres::connect(db.dsn(), tokio_postgres::NoTls)
            .await
            .expect("connect");
        tokio::spawn(connection);
        let c = &client;
        // A basis taken with 102 in flight: 99 and 103 had committed, 106
        // hadn't started.
        let basis = Some("100:105:102");
        let change = |txid| (txid, "0/200");

        // A never-applied entry (a placeholder) takes any change.
        assert!(applies(c, None, None, change("99"), None).await);
        // Visible in the basis: the read already counted it.
        assert!(!applies(c, basis, None, change("99"), None).await);
        assert!(!applies(c, basis, None, change("103"), None).await);
        // In flight when the basis was taken (`xip`), or after it: applied.
        assert!(applies(c, basis, None, change("102"), None).await);
        assert!(applies(c, basis, None, change("106"), None).await);
        // `applied_lsn`: only a strictly newer change applies.
        assert!(!applies(c, None, Some("0/200"), change("106"), None).await);
        assert!(!applies(c, None, Some("0/300"), change("106"), None).await);
        assert!(applies(c, None, Some("0/100"), change("106"), None).await);
        // The truncate floor: a change at or below it predates the truncate.
        assert!(!applies(c, None, None, change("106"), Some("0/200")).await);
        assert!(applies(c, None, None, change("106"), Some("0/1FF")).await);
    }

    #[test]
    fn the_apply_predicate_is_visibility_then_lsn_then_the_floor() {
        assert_eq!(
            apply_predicate(),
            r#"(l."__basis" is null or not pg_visible_in_snapshot(v.__txid, l."__basis")) and (l."__applied_lsn" is null or v.__lsn > l."__applied_lsn") and not exists (select 1 from fl where v.__lsn <= fl.floor)"#
        );
    }
}
