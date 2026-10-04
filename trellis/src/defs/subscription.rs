//! A table a logical-replication subscription replicates into (issue #751).
//!
//! Trellis doesn't support reading a table that a subscription in its
//! database writes. Capture is statement-level triggers with transition
//! tables, and a subscription's apply worker fires only row-level triggers
//! for the inserts, updates and deletes it applies (Postgres's "Logical
//! Replication: Triggers"), even with the triggers `ENABLE ALWAYS`. So none
//! of those changes reach the ring, the targets silently diverge from the
//! source, and the capture audit's trigger checks all pass. The initial
//! table copy fires statement triggers (it runs like `COPY`), and so does a
//! replicated `TRUNCATE`, so they are captured, but every change after them
//! isn't.
//!
//! An ordinary session in `session_replication_role = replica` isn't
//! affected: it fires `ENABLE ALWAYS` statement triggers, so it is captured.
//! The apply worker is the only writer that skips them.
//!
//! # When a table counts as replicated into
//!
//! When it has a row in `pg_subscription_rel`, in any state and whether or
//! not its subscription is enabled. That catalog lists exactly the tables a
//! subscription applies changes to: the apply worker skips a table without
//! a row there. A row is added by `CREATE SUBSCRIPTION` (unless it is
//! created with `connect = false`) and by `ALTER SUBSCRIPTION … REFRESH
//! PUBLICATION` (or `ADD`/`SET PUBLICATION`, which refresh by default), and
//! removed by a refresh that no longer finds the table published, and by
//! `DROP SUBSCRIPTION`.
//!
//! - **Every `srsubstate`.** `i` (waiting to copy), `d` (copying), `f`, `s`
//!   (catching up, which applies changes the same way) and `r` (ready) all
//!   end in, or already are, changes Trellis doesn't see.
//! - **A disabled subscription.** Its slot keeps every change it hasn't
//!   applied, so enabling it applies them all, uncaptured. No refresh can
//!   add a table to a disabled subscription, so a disabled one only names a
//!   table if it did before it was disabled, or was created naming it.
//!
//! `pg_subscription_rel` is a per-database catalog, so a subscription in
//! another database never shows up here. The check reads it and
//! `pg_subscription`'s `subname` and `subenabled`, which any role can read
//! (only `subconninfo` is restricted), so it needs no privilege beyond
//! connecting.
//!
//! # Where it is checked
//!
//! Wherever row-level security is ([`super::row_security`]), on the same
//! tables: the definition's source and the to-side of every relationship it
//! reads through, captured or fed by the target-mutation seam.
//!
//! - **Defining** a transform refuses such a table
//!   ([`super::catalog::CatalogError::Subscribed`]).
//! - **The staging worker's capture pass** pauses every definition that
//!   reads one, recording why in `capture_failures`
//!   (`staging::schema_change::pause_readers_of_unsupported`). A
//!   subscription can be created, or refreshed to include the table, after
//!   define.
//! - **`self_check`'s capture audit** reports it
//!   (`staging::capture_audit::CaptureFault::Subscribed`).

use std::fmt;

use tokio_postgres::GenericClient;

use crate::defs::ddl::regclass_arg;

/// A subscription that replicates into a table Trellis reads, found by
/// [`subscribed`].
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct Subscribed {
    /// The table, as its unquoted `schema.table` identity.
    pub table: String,
    /// The subscription's name.
    pub subscription: String,
    /// Whether the subscription is enabled. A disabled one applies every
    /// change it missed once it is enabled.
    pub enabled: bool,
}

impl fmt::Display for Subscribed {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let Subscribed {
            table,
            subscription,
            enabled,
        } = self;
        let disabled = if *enabled {
            ""
        } else {
            " (disabled now, but enabling it applies every change it has missed)"
        };
        write!(
            f,
            "{table} is replicated into by logical-replication subscription {subscription}\
             {disabled}: its apply worker fires only row-level triggers, so Trellis's \
             statement-level capture triggers never see the inserts, updates and deletes it \
             applies. Trellis doesn't support that. Stop replicating into the table (remove it \
             from the publication on the publisher, then ALTER SUBSCRIPTION {subscription} \
             REFRESH PUBLICATION), or drop the subscription"
        )
    }
}

/// The subscription that replicates into `table` (an unquoted
/// `schema.table` identity), per the module doc's rule. `None` if none does,
/// or there is no such table. When several do, an enabled one is reported
/// first, then by name.
pub async fn subscribed(
    client: &impl GenericClient,
    table: &str,
) -> Result<Option<Subscribed>, tokio_postgres::Error> {
    let row = client
        .query_opt(
            "select s.subname::text, s.subenabled \
             from pg_catalog.pg_subscription_rel r \
             join pg_catalog.pg_subscription s on s.oid = r.srsubid \
             where r.srrelid = pg_catalog.to_regclass($1) \
             order by s.subenabled desc, s.subname \
             limit 1",
            &[&regclass_arg(table)],
        )
        .await?;
    Ok(row.map(|row| Subscribed {
        table: table.to_string(),
        subscription: row.get(0),
        enabled: row.get(1),
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_message_names_the_subscription_and_the_remedy() {
        let mut sub = Subscribed {
            table: "public.t".to_string(),
            subscription: "from_primary".to_string(),
            enabled: true,
        };
        let text = sub.to_string();
        assert!(
            text.starts_with(
                "public.t is replicated into by logical-replication subscription from_primary:"
            ),
            "{text}"
        );
        assert!(
            text.contains("ALTER SUBSCRIPTION from_primary REFRESH PUBLICATION"),
            "{text}"
        );
        assert!(!text.contains("disabled"), "{text}");
        sub.enabled = false;
        let text = sub.to_string();
        assert!(
            text.contains("from_primary (disabled now, but enabling it applies"),
            "{text}"
        );
    }
}
