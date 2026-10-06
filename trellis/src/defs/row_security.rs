//! Row-level security that applies to the Trellis role (issue #745).
//!
//! Trellis doesn't support a table whose row-level security policies apply
//! to its own role. Capture still sees every changed row, because transition
//! tables ignore RLS, but every path that *reads* a table runs as the Trellis
//! role, and the policies filter those reads: a build never sees a hidden
//! row, a per-key Re-derive reads a hidden key as gone and deletes its target
//! row, a relationship read finds a hidden parent absent, and `self_check`'s
//! recompute agrees with the wrong answer. So does a capture function's
//! re-read of the live row (#623 D8a), which runs as the function's owner,
//! the role that owns the ring.
//!
//! Whether a policy actually hides a row depends on the row, so the check is
//! whether policies *apply* at all. A policy that applies but hides nothing
//! (`USING (true)`) is refused like any other; exempt the role instead.
//!
//! # When policies apply to a role
//!
//! Postgres's own rule (`check_enable_rls`), read from the catalog for each
//! role that reads (see [`Readers`]):
//!
//! 1. The table has RLS enabled (`pg_class.relrowsecurity`). If not, nothing
//!    applies.
//! 2. A superuser, or a role with `BYPASSRLS` (`pg_roles.rolbypassrls`, its
//!    own attribute: a role doesn't inherit it from roles it is a member of),
//!    bypasses RLS, `FORCE` or not.
//! 3. The table's owner bypasses it unless `FORCE ROW LEVEL SECURITY` is set
//!    (`relforcerowsecurity`). Ownership is `has_privs_of_role`: the owner
//!    itself, or a role that inherits the owner's privileges through
//!    membership, which is what `pg_has_role(..., 'USAGE')` answers. A member
//!    without `INHERIT` isn't the owner here.
//! 4. Anyone else gets the policies.
//!
//! Every input is a catalog column any role can read, so the check needs no
//! privilege beyond connecting.
//!
//! # Where it is checked
//!
//! - **Defining** a transform refuses its source, and the to-side of every
//!   relationship it reads through, when the policies apply to the ring's
//!   owner ([`super::catalog::CatalogError::RowSecurityApplies`]). A to-one
//!   relationship's to-side is checked for the session's role too:
//!   registration seeds and widens the relationship's settled projection
//!   from it as that role, and a build reads the projection. Otherwise not
//!   the session's role: registration reads no source rows, and a process
//!   that only defines transforms needn't run as a role that could.
//!   Declaring a to-one relationship checks its to-side for the session's
//!   role alone: it registers no reader, but it seeds the projection from
//!   the to-side as that role, which `row_security = off` (below) would
//!   otherwise fail with Postgres's bare error.
//!   `ALTER TRANSFORM` adds no table to read: its `ADD`/`ALTER` refuse a
//!   relationship path. A source or to-side that is another definition's
//!   target is checked for the session's role alone ([`Readers::Session`]).
//!   No capture function reads it, so the ring's owner never does.
//! - **The staging worker's capture pass** pauses every definition that
//!   reads a table whose policies now apply to the ring's owner or to the
//!   worker's own role, recording why in `capture_failures`
//!   (`staging::schema_change::pause_readers_of_unsupported`): each
//!   captured table, and each table another definition's target is, which
//!   the target-mutation seam feeds, for the worker's own role alone
//!   ([`Readers::Session`]). RLS can be enabled or forced, a table
//!   handed to another owner, or a role's `BYPASSRLS` or membership taken
//!   away, after define.
//! - **`self_check`'s capture audit** reports it for the ring's owner or the
//!   caller's role, whose recompute it would filter, or the caller's role
//!   alone for a seam-fed table
//!   (`staging::capture_audit::CaptureFault::RowSecurity`).
//!
//! Drain threads in another process, running as another login role, aren't
//! seen from here: a deployment runs every worker as the Trellis role or a
//! member of it.
//!
//! # `row_security = off` (issue #766)
//!
//! The checks above are the friendly half. Behind them, every connection
//! Trellis opens sets `row_security = off` ([`crate::pool::ROW_SECURITY_OFF`]),
//! so a statement the policies would filter, as whatever role it runs as,
//! fails with `42501` instead of reading or writing the wrong rows. The drain
//! classifies that as halting (`staging::quarantine::classify`) and pauses
//! what it reaches, reading the refused tables off the catalog with
//! [`applying`] as its own role (`staging::halt`). That is what covers the
//! drain threads above.
//!
//! # A definition's target (issue #765)
//!
//! Trellis also *writes* each definition's target, and policies that apply
//! to the writer filter those writes: an `UPDATE` or `DELETE` skips the rows
//! they hide, leaving them as they were, and an `INSERT` fails their `WITH
//! CHECK`, raising in the drain. So the same rule applies to a target, for
//! the role that writes it ([`Readers::Target`]).
//!
//! **Which role writes.** Every target write (apply's, a build's, a
//! rebuild's orphan delete) is plain SQL on a Trellis connection, so it runs
//! as that connection's login role: the drain thread's or the worker's. No
//! capture function is involved (targets aren't captured), and nothing
//! switches role. So the ring's owner isn't checked for a target: in a
//! deployment whose workers log in as a member of it, the target belongs to
//! the role that defined it, the ring's owner may own nothing of it, and
//! checking it would refuse a target the workers write as its owner.
//!
//! **When it can fire.** Registration always creates the target, as the
//! session's role (#440: Trellis never adopts a table), with row-level
//! security off. So the policies only come to apply to its writer if
//! someone enables RLS and the writer doesn't own the table (another owner,
//! or a membership taken away), forces RLS on it, or takes away the
//! writer's `BYPASSRLS`. Enabling RLS on a target the writer owns, for the
//! application's roles that read it, fires nothing.
//!
//! - **Defining** checks the new target for the session's role right after
//!   creating it, which only fires when an event trigger turns RLS on for
//!   new tables and forces it, or hands the table to another owner.
//! - **The capture pass** checks each target for the worker's own role and
//!   pauses the definition that writes it. Its readers are checked as for
//!   any table another definition targets, above.
//! - **`self_check`'s capture audit** checks the definition's target for the
//!   caller's role, which reads it to compare.

use std::fmt;

use tokio_postgres::GenericClient;

use crate::capture::install::RING_OWNER;
use crate::defs::ddl::regclass_arg;

/// Row-level security that applies to a role Trellis reads `table` as, found
/// by [`applying`].
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct RowSecurity {
    /// The table, as its unquoted `schema.table` identity.
    pub table: String,
    /// The role its policies apply to.
    pub role: String,
    /// Whether `role` owns the table (itself, or through a role it inherits
    /// from), so that only `FORCE ROW LEVEL SECURITY` makes the policies
    /// apply to it. Otherwise it neither owns the table nor has `BYPASSRLS`.
    pub owner_forced: bool,
    /// Whether the table is a definition's target, which Trellis writes
    /// (issue #765, [`Readers::Target`]), rather than a table it reads.
    pub target: bool,
}

impl fmt::Display for RowSecurity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let RowSecurity {
            table,
            role,
            owner_forced,
            target,
        } = self;
        if *target {
            return if *owner_forced {
                write!(
                    f,
                    "row-level security on target {table} applies to role {role}: it owns the \
                     table, but the table has FORCE ROW LEVEL SECURITY, so Trellis's writes to it \
                     are filtered: its updates and deletes skip the rows the policies hide, and \
                     its inserts fail their WITH CHECK. Trellis doesn't support that. Exempt the role with \
                     ALTER TABLE {table} NO FORCE ROW LEVEL SECURITY, or give it BYPASSRLS"
                )
            } else {
                write!(
                    f,
                    "row-level security on target {table} applies to role {role}: it neither \
                     owns the table nor has BYPASSRLS, so Trellis's writes to it are filtered: \
                     its updates and deletes skip the rows the policies hide, and its inserts \
                     fail their WITH CHECK. Trellis doesn't support that. Exempt the role with ALTER ROLE \
                     {role} BYPASSRLS, or make it the table's owner again (without FORCE ROW \
                     LEVEL SECURITY)"
                )
            };
        }
        if *owner_forced {
            write!(
                f,
                "row-level security on {table} applies to role {role}: it owns the table, but \
                 the table has FORCE ROW LEVEL SECURITY, so Trellis's reads of it see only the \
                 rows its policies allow. Trellis doesn't support that. Exempt the role with \
                 ALTER TABLE {table} NO FORCE ROW LEVEL SECURITY, or give it BYPASSRLS"
            )
        } else {
            write!(
                f,
                "row-level security on {table} applies to role {role}: it neither owns the table \
                 nor has BYPASSRLS, so Trellis's reads of it see only the rows its policies \
                 allow. Trellis doesn't support that. Exempt the role with ALTER ROLE {role} \
                 BYPASSRLS, or make it the table's owner (without FORCE ROW LEVEL SECURITY)"
            )
        }
    }
}

/// Which roles [`applying`] checks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Readers {
    /// The role that owns the instance's ring, which ran the migrations and
    /// which the capture functions run as: the Trellis role. A staging
    /// worker or drain thread runs as it, or as a login role that is a
    /// member of it.
    Ring,
    /// The ring's owner and the session's role, for a check made by a
    /// session that reads the table itself: the staging worker's capture
    /// pass, or `self_check`'s recompute.
    RingAndSession,
    /// The session's role alone, for a table the target-mutation seam feeds
    /// (another definition's target) that a definition reads, as its source
    /// or a relationship's to-side. Only plain SQL on a worker connection
    /// reads one (a build, a Re-derive, a relationship read, the seam's own
    /// image reads), as its login role. No capture function is installed on
    /// it, so the ring's owner never reads it unless it is that role, and
    /// checking it would pause a reader in a deployment whose workers log in
    /// as a member of the ring's owner and define as that member: the member
    /// owns the upstream's target, and the ring's owner owns nothing of it.
    /// The session stands in for the workers, as for [`Readers::Target`].
    Session,
    /// The session's role alone, for a definition's target table (issue
    /// #765). Only the workers write a target (applies, builds, a rebuild's
    /// orphan delete), each as its connection's login role, and no capture
    /// function touches one (the target-mutation seam feeds its readers), so
    /// the ring's owner never writes one unless it is that role. The session
    /// stands in for the workers: the capture pass's is a worker's, and
    /// define's and `self_check`'s are assumed to be one. What it finds says
    /// so ([`RowSecurity::target`]).
    Target,
}

/// The row-level security that applies to a role Trellis reads `table` as
/// (see [`Readers`] and the module doc). `None` if none applies, or there is
/// no such table. When it applies to both roles, the session's is reported.
///
/// `schema` is the instance schema, whose ring's owner is checked, and
/// `table` an unquoted `schema.table` identity.
pub async fn applying(
    client: &impl GenericClient,
    schema: &str,
    table: &str,
    readers: Readers,
) -> Result<Option<RowSecurity>, tokio_postgres::Error> {
    let row = client
        .query_opt(
            &format!(
                "select r.rolname::text, \
                        pg_catalog.pg_has_role(r.oid, c.relowner, 'USAGE') \
                 from pg_catalog.pg_class c \
                 cross join pg_catalog.pg_roles r \
                 where c.oid = pg_catalog.to_regclass($2) \
                   and (($3 and r.rolname = current_user) or ($4 and r.oid = {RING_OWNER})) \
                   and c.relrowsecurity \
                   and not r.rolsuper and not r.rolbypassrls \
                   and (c.relforcerowsecurity \
                        or not pg_catalog.pg_has_role(r.oid, c.relowner, 'USAGE')) \
                 order by r.rolname <> current_user, r.rolname \
                 limit 1"
            ),
            &[
                &schema,
                &regclass_arg(table),
                &(readers != Readers::Ring),
                &matches!(readers, Readers::Ring | Readers::RingAndSession),
            ],
        )
        .await?;
    Ok(row.map(|row| RowSecurity {
        table: table.to_string(),
        role: row.get(0),
        owner_forced: row.get(1),
        target: readers == Readers::Target,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_message_names_the_remedy_for_each_reason() {
        let mut rls = RowSecurity {
            table: "public.t".to_string(),
            role: "trellis".to_string(),
            owner_forced: false,
            target: false,
        };
        let text = rls.to_string();
        assert!(
            text.contains("neither owns the table nor has BYPASSRLS"),
            "{text}"
        );
        assert!(text.contains("ALTER ROLE trellis BYPASSRLS"), "{text}");
        rls.owner_forced = true;
        let text = rls.to_string();
        assert!(text.contains("FORCE ROW LEVEL SECURITY"), "{text}");
        assert!(
            text.contains("ALTER TABLE public.t NO FORCE ROW LEVEL SECURITY"),
            "{text}"
        );
    }

    #[test]
    fn a_target_s_message_says_its_writes_are_filtered() {
        // Issue #765.
        let mut rls = RowSecurity {
            table: "public.t".to_string(),
            role: "trellis".to_string(),
            owner_forced: false,
            target: true,
        };
        let text = rls.to_string();
        assert!(text.contains("on target public.t"), "{text}");
        assert!(text.contains("writes to it are filtered"), "{text}");
        assert!(text.contains("ALTER ROLE trellis BYPASSRLS"), "{text}");
        rls.owner_forced = true;
        let text = rls.to_string();
        assert!(text.contains("writes to it are filtered"), "{text}");
        assert!(
            text.contains("ALTER TABLE public.t NO FORCE ROW LEVEL SECURITY"),
            "{text}"
        );
    }
}
