//! The drain's halt (#663): what a [`FailureClass::Halting`] failure parks.
//!
//! A halting failure is structural, not one row's fault: a key the drain
//! can't use (`NoPrimaryKey`, `UnsupportedPrimaryKeyType`), a propagation
//! wave past the hop bound (`HopBoundExceeded`), an aggregate target off
//! the ledger (`AggregateOffLedger`), a truncate with no ring `lsn`
//! (`TruncateWithoutLsn`), or Postgres refusing the drain's role a read or
//! write (`42501`, see below). Every key it touches reproduces it, so
//! retrying the page re-fails it forever, and quarantining a key blames one
//! for nobody's fault. Instead [`halt_closure`] pauses the definitions the
//! failure reaches, and the drain retries the page without them:
//!
//! - every definition that reads the halting table, as its source or as a
//!   relationship's to-side (`capture::columns::readers_of`). For a hop
//!   bound, the halting tables are those the wave ran away through, so their
//!   readers are the cycle's definitions. For a truncate with no `lsn`, it
//!   is the truncated table. For an aggregate off the ledger, it's the
//!   definition that writes that target;
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
//! # A refused read or write (issue #766)
//!
//! Every Trellis session runs with `row_security = off`
//! ([`crate::pool::ROW_SECURITY_OFF`]), so a statement that row-level
//! security would filter fails with `42501` instead, as does one the role
//! lacks a privilege for. Neither names a key, and Postgres's message names
//! the table bare (and in the server's `lc_messages`), so the seed is read
//! from the catalog instead, on the drain's own pooled connection and so as
//! its login role ([`refusals`]): each table an unfrozen definition reads
//! whose policies apply to that role ([`row_security::Readers::Session`]),
//! or that it can't read at all, seeds that table's readers, and each
//! target the role can't write, or whose policies apply to it
//! ([`row_security::Readers::Target`]), seeds its writer. Each definition's
//! record carries the reason it found and Postgres's own message. A `42501`
//! the catalog pins on no table (one about Trellis's own objects, say)
//! pauses nothing: [`super::apply`]'s halt then retries the page once and
//! surfaces the error, charging no key either way.
//!
//! A backfill marker's discharge that Postgres refuses the same way (a
//! build's planning, or a go-live catch-up's re-read, issue #813) halts
//! through the same attribution, on the discharge's own connection
//! ([`halt_refused_discharge`]), and retries the marker without the
//! definitions it paused. One the catalog pins on no table pauses nothing
//! there either, and the marker backs off as for any failed discharge.
//!
//! [`FailureClass::Halting`]: super::quarantine::FailureClass::Halting

use std::collections::{BTreeSet, HashSet};

use tokio_postgres::{GenericClient, Transaction};

use crate::capture::columns::{CaptureCatalog, load_catalog, readers_of};
use crate::defs::ddl::DdlError;
use crate::defs::model::TransformStatus;
use crate::defs::row_security;
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
        ApplyError::TruncateWithoutLsn { src_table } => Some(Seed::Tables(vec![src_table.clone()])),
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
    let seed = match seed(err) {
        Some(Seed::Tables(tables)) => {
            let mut qualified = Vec::with_capacity(tables.len());
            for table in tables {
                qualified.push(quarantine::qualified_src_table(pool, &table).await?);
            }
            Some(Seed::Tables(qualified))
        }
        Some(target) => Some(target),
        None if quarantine::is_insufficient_privilege(err) => None,
        None => return Ok(Vec::new()),
    };

    let mut client = pool.get().await?;
    let txn = client.transaction().await?;
    let paused = pause_closure(
        &txn,
        pool.schema(),
        seed,
        "the drain halted",
        &err.to_string(),
    )
    .await?;
    txn.commit().await?;
    Ok(paused)
}

/// [`halt_closure`] for a backfill marker's discharge (issue #813,
/// `intake::markers`): a registration's or a resume's planning, or a go-live
/// catch-up's read, that Postgres refused with `42501`. Runs the same
/// attribution ([`refusals`]) on the discharge's own connection, `client`,
/// and so as the role that was refused, and pauses the closure of each
/// table it names with kind `halt`, recording the halting stop if it paused
/// any, in one transaction. Returns the bare targets it paused; empty when
/// the catalog pins the refusal on no table, or every definition it reaches
/// was already frozen, and then the caller backs the marker off as for any
/// failed discharge.
///
/// `table` is the marker's and `err` the refusal's text, for the record's
/// sentence. The instance schema
/// is read from `client`'s `search_path`, which every Trellis session leads
/// with it.
pub(crate) async fn halt_refused_discharge(
    client: &mut tokio_postgres::Client,
    table: &str,
    err: &str,
) -> Result<Vec<String>, ApplyError> {
    let schema: String = client
        .query_one("select pg_catalog.current_schema()::text", &[])
        .await?
        .get(0);
    let txn = client.transaction().await?;
    let halted = format!("the backfill discharge of {table} was refused");
    let paused = pause_closure(&txn, &schema, None, &halted, err).await?;
    txn.commit().await?;
    Ok(paused)
}

/// Pauses, in `txn`, every unfrozen definition `seed` reaches, or, with no
/// seed (a refused read or write, `42501`), every one the [`refusals`] read
/// as `txn`'s role reaches; and records the halting stop if that paused any.
/// `halted` opens each record's sentence: what halted, and `err`, the
/// failure's text, follows it.
async fn pause_closure(
    txn: &Transaction<'_>,
    schema: &str,
    seed: Option<Seed>,
    halted: &str,
    err: &str,
) -> Result<Vec<String>, ApplyError> {
    let catalog = load_catalog(txn, schema).await?;
    let unfrozen: HashSet<i64> = txn
        .query(
            "select id from transform_definitions where status = any($1)",
            &[&TransformStatus::dispatchable()],
        )
        .await?
        .iter()
        .map(|row| row.get(0))
        .collect();

    // Each seed, with the table its records name and the sentence they carry.
    let episodes: Vec<(Seed, String, String)> = match seed {
        Some(seed) => {
            let source_table = match &seed {
                Seed::Tables(tables) => tables.join(", "),
                Seed::Target(target) => target.clone(),
            };
            let error = format!(
                "{halted}: {err}; fix the cause, then resume the definition to rebuild it, or \
                 drop the definition"
            );
            vec![(seed, source_table, error)]
        }
        None => refusals(txn, schema, &catalog, &unfrozen)
            .await?
            .into_iter()
            .map(|refusal| {
                let error = format!(
                    "{halted}: {err}; {}; then resume the definition to rebuild it, or drop the \
                     definition",
                    refusal.reason
                );
                (refusal.seed, refusal.table, error)
            })
            .collect(),
    };

    let mut paused = Vec::new();
    for (seed, source_table, error) in &episodes {
        for id in closure(&catalog, seed, &unfrozen) {
            if crate::defs::lifecycle::pause_for_halt(txn, id, source_table, error).await? {
                let reader = catalog.definitions.iter().find(|r| r.id == id);
                paused.push(reader.map_or_else(|| id.to_string(), |r| r.def.target.clone()));
            }
        }
    }
    if !paused.is_empty() {
        quarantine::record_halting_stop(txn, err).await?;
    }
    Ok(paused)
}

/// A table the drain's role was refused (issue #766), found by [`refusals`].
struct Refusal {
    /// The readers of the table, or the writer of the target.
    seed: Seed,
    /// The unquoted `schema.table` identity.
    table: String,
    /// What the catalog says the role can't do there, and how to fix it.
    reason: String,
}

/// Where the documentation on row-level security and Trellis's roles lives,
/// for a refusal's reason.
const ROW_SECURITY_DOCS: &str = "see \"Supported sources and targets\" in docs/transforms.md for the roles row-level security must not apply to";

/// Every table an unfrozen definition reads or writes that the session's
/// role (the drain's: `client` is one of its pool's connections) can't use:
/// it lacks the privilege, or row-level security applies to it (see the
/// module doc). Reads are checked for `SELECT`, and for policies
/// ([`row_security::Readers::Session`]); targets for `SELECT`, `INSERT`,
/// `UPDATE` and `DELETE`, and for policies ([`row_security::Readers::Target`]).
/// A privilege is only counted missing when the role holds it on no column
/// at all, so a column grant never pins the failure on the wrong table.
async fn refusals(
    client: &impl GenericClient,
    schema: &str,
    catalog: &CaptureCatalog,
    unfrozen: &HashSet<i64>,
) -> Result<Vec<Refusal>, ApplyError> {
    let live = || {
        catalog
            .definitions
            .iter()
            .filter(|r| unfrozen.contains(&r.id))
    };
    let mut read: BTreeSet<String> = live().map(|r| r.source.clone()).collect();
    for rel in &catalog.relationships {
        read.insert(rel.qualified_to_table());
    }
    let written: BTreeSet<String> = live().map(|r| r.target.clone()).collect();

    let mut refusals = Vec::new();
    for table in read {
        if !readers_of(catalog, &table, &BTreeSet::new(), true)
            .iter()
            .any(|id| unfrozen.contains(id))
        {
            continue;
        }
        let reason = match missing_privileges(client, &table, &["SELECT"]).await? {
            Some(missing) => Some(missing),
            None => row_security::applying(client, schema, &table, row_security::Readers::Session)
                .await?
                .map(|rls| format!("{rls}; {ROW_SECURITY_DOCS}")),
        };
        if let Some(reason) = reason {
            refusals.push(Refusal {
                seed: Seed::Tables(vec![table.clone()]),
                table,
                reason,
            });
        }
    }
    for table in written {
        let reason =
            match missing_privileges(client, &table, &["SELECT", "INSERT", "UPDATE", "DELETE"])
                .await?
            {
                Some(missing) => Some(missing),
                None => {
                    row_security::applying(client, schema, &table, row_security::Readers::Target)
                        .await?
                        .map(|rls| format!("{rls}; {ROW_SECURITY_DOCS}"))
                }
            };
        if let Some(reason) = reason {
            refusals.push(Refusal {
                seed: Seed::Target(table.clone()),
                table,
                reason,
            });
        }
    }
    Ok(refusals)
}

/// The sentence naming which of `privileges` the session's role lacks on
/// `table` (an unquoted `schema.table` identity), counting `USAGE` on its
/// schema too; `None` if it holds them all, or there is no such table.
/// Looked up by name rather than through `regclass`, whose cast itself
/// raises on a schema the role can't use. `SELECT`, `INSERT` and `UPDATE`
/// count as held when held on any column.
async fn missing_privileges(
    client: &impl GenericClient,
    table: &str,
    privileges: &[&str],
) -> Result<Option<String>, ApplyError> {
    let Some((nspname, relname)) = table.split_once('.') else {
        return Ok(None);
    };
    let row = client
        .query_opt(
            "select current_user::text, \
                    pg_catalog.has_schema_privilege(n.oid, 'USAGE'), \
                    array(select p from unnest($3::text[]) p \
                          where not case when p = 'DELETE' \
                                         then pg_catalog.has_table_privilege(c.oid, p) \
                                         else pg_catalog.has_any_column_privilege(c.oid, p) end) \
             from pg_catalog.pg_class c \
             join pg_catalog.pg_namespace n on n.oid = c.relnamespace \
             where n.nspname = $1 and c.relname = $2",
            &[&nspname, &relname, &privileges],
        )
        .await?;
    let Some(row) = row else {
        return Ok(None);
    };
    let role: String = row.get(0);
    let usage: bool = row.get(1);
    let missing: Vec<String> = row.get(2);
    Ok(if !usage {
        Some(format!(
            "role {role} lacks USAGE on schema {nspname}, so it can't reach {table}, which \
             Trellis reads or writes as that role. Grant it"
        ))
    } else if !missing.is_empty() {
        Some(format!(
            "role {role} lacks {} on {table}, which Trellis needs as that role. Grant it",
            missing.join(", ")
        ))
    } else {
        None
    })
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

    /// A truncate with no `lsn` (#774) halts every definition reading the
    /// truncated table.
    #[test]
    fn a_truncate_without_an_lsn_seeds_its_table() {
        let err = ApplyError::TruncateWithoutLsn {
            src_table: "public.posts".to_string(),
        };
        match seed(&err) {
            Some(Seed::Tables(tables)) => assert_eq!(tables, ["public.posts"]),
            _ => panic!("a truncate without an lsn seeds its table"),
        }
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

    /// Issue #766: what a `42501` halt seeds from, read off the catalog as the
    /// drain's own role. A source it can't select from seeds its readers; a
    /// target it can't write, or whose policies apply to it, seeds its
    /// writer; a table whose schema it can't use is named as such. A column
    /// grant counts as the privilege, and a frozen definition's tables aren't
    /// looked at.
    #[tokio::test]
    async fn refusals_name_each_table_the_role_cant_use_and_why() {
        let cluster = testkit::TestCluster::start();
        let db = cluster.create_empty_database().await;
        let (admin, connection) = tokio_postgres::connect(db.dsn(), tokio_postgres::NoTls)
            .await
            .expect("connect");
        tokio::spawn(async move {
            let _ = connection.await;
        });
        admin
            .batch_execute(
                "create role drainer login; \
                 create schema hidden; \
                 create table public.s (id int primary key, v int); \
                 create table public.t (id int primary key, v int); \
                 create table public.u (id int primary key, v int); \
                 create table public.w (id int primary key, v int); \
                 create table public.cols (id int primary key, v int); \
                 create table hidden.h (id int primary key, v int); \
                 create table public.f (id int primary key, v int); \
                 grant all on public.t, public.w to drainer; \
                 grant select (id, v) on public.cols to drainer; \
                 grant all on public.u to drainer; \
                 alter table public.w enable row level security; \
                 grant all on hidden.h to drainer;",
            )
            .await
            .expect("tables, and a role with some grants on them");
        let dsn = db.dsn().replace("user=postgres", "user=drainer");
        let (drainer, connection) = tokio_postgres::connect(&dsn, tokio_postgres::NoTls)
            .await
            .expect("connect as the drain's role");
        tokio::spawn(async move {
            let _ = connection.await;
        });

        let catalog = catalog(&[
            // 1: can't read `s`, can write `t`.
            ("public.s", "public.t", "TRANSFORM t FROM s SELECT v AS v"),
            // 2: reads `u`; `w`'s policies apply to the role.
            ("public.u", "public.w", "TRANSFORM w FROM u SELECT v AS v"),
            // 3: reads `cols` through a column grant; can't use `hidden`.
            (
                "public.cols",
                "hidden.h",
                "TRANSFORM h FROM cols SELECT v AS v",
            ),
            // 4: frozen, so `f` isn't looked at.
            ("public.f", "public.f2", "TRANSFORM f2 FROM f SELECT v AS v"),
        ]);
        let unfrozen = HashSet::from([1, 2, 3]);
        let found = refusals(&drainer, "trellis", &catalog, &unfrozen)
            .await
            .expect("read the catalog");
        let found: Vec<(String, bool, String)> = found
            .into_iter()
            .map(|r| (r.table, matches!(r.seed, Seed::Target(_)), r.reason))
            .collect();
        assert_eq!(found.len(), 3, "{found:#?}");
        assert_eq!((found[0].0.as_str(), found[0].1), ("public.s", false));
        assert!(
            found[0].2.contains("role drainer lacks SELECT on public.s"),
            "{}",
            found[0].2
        );
        assert_eq!((found[1].0.as_str(), found[1].1), ("hidden.h", true));
        assert!(
            found[1]
                .2
                .contains("role drainer lacks USAGE on schema hidden"),
            "{}",
            found[1].2
        );
        assert_eq!((found[2].0.as_str(), found[2].1), ("public.w", true));
        assert!(
            found[2]
                .2
                .contains("row-level security on target public.w applies to role drainer"),
            "{}",
            found[2].2
        );
        assert!(found[2].2.contains("docs/transforms.md"), "{}", found[2].2);
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
