//! Version handover (`docs/decisions/0023-version-handover.md`): two Trellis
//! versions never stage, drain or change an instance's state side by side.
//!
//! Every background process registers in `worker_registry` with the version of
//! its binary ([`BINARY_VERSION`], the exact crate version) and its roles, and
//! heartbeats there. Before it stages, drains or builds anything, and again
//! every standby poll, it reads the live rows and the staging-worker lock and
//! asks [`decide`]: while a process of another version is live it stands by,
//! and it takes over once none is. A state-changing call asks the same
//! question and refuses ([`OtherVersionLive`], [`crate::ErrorCode::VersionHandover`]),
//! and a `migrate` with migrations pending refuses while any process is live,
//! so no migration ever runs under a live binary of an older version.
//!
//! Two processes of a release before 0.2.0 have no version to compare: a
//! drain worker's row has none, and a staging-only leader registers nothing
//! at all. Both count as another version: a live row without a version, and
//! the staging lock held by a session no row names (rows name the session
//! they hold the lock on, `staging_pid`).
//!
//! [`decide`] is pure, so every case is tested without a database; [`observe`]
//! reads its input in one place.

use std::fmt;
use std::time::Duration;

use tokio_postgres::GenericClient;

use crate::pool::quote_ident;

/// This build's version: the crate version, compared as an exact string
/// (ADR-0023, rule 5).
pub(crate) const BINARY_VERSION: &str = env!("CARGO_PKG_VERSION");

/// How long a registry row counts as live after its last heartbeat: the
/// reclaim TTL, the same window `has_live_drain_workers` reads.
pub(crate) const LIVE_TTL: Duration = crate::staging::DEFAULT_RECLAIM_TTL;

/// The namespace the per-instance handover lock key is derived from (see
/// [`handover_lock_key`]).
const HANDOVER_LOCK_NAMESPACE: u64 = 0x7472_6c73_6876_6572;

/// The advisory lock every background process's pass takes for its
/// transaction, and `migrate` takes while it checks and runs migrations: so
/// two processes starting at once see each other's rows (the second stands by
/// rather than both taking over), and no process registers while a migration
/// it would have to wait for runs. Per instance, like the staging lock.
pub fn handover_lock_key(schema: &str) -> i64 {
    (crate::staging::session::fnv1a_64(schema) ^ HANDOVER_LOCK_NAMESPACE) as i64
}

/// One live `worker_registry` row, as [`decide`] reads it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct LiveRow {
    pub(crate) worker_id: String,
    /// `None` for a row written by a release before 0.2.0.
    pub(crate) version: Option<String>,
    /// The process stands by for another version: it changes nothing, so it
    /// doesn't hold anyone else back.
    pub(crate) standby: bool,
}

/// Who holds the instance's staging-worker lock.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum StagingLock {
    Free,
    /// Held by the session a registry row names as its `staging_pid`.
    Claimed {
        worker_id: String,
        version: Option<String>,
    },
    /// Held by a session no registry row names: a staging worker of a release
    /// before 0.2.0, which registers nothing.
    Unclaimed,
}

/// What [`decide`] decides from.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Observed {
    /// Every row whose heartbeat is within [`LIVE_TTL`].
    pub(crate) live: Vec<LiveRow>,
    pub(crate) staging_lock: StagingLock,
    /// Some migration this binary carries hasn't run on the instance.
    pub(crate) migrations_pending: bool,
    /// The instance has a registry to register in (it has been migrated).
    /// Not read by [`decide`].
    pub(crate) registry_exists: bool,
}

/// Who asks [`decide`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Asker {
    /// A background process (staging worker, drain threads): it waits.
    Background,
    /// A state-changing call: it refuses.
    Call,
    /// `migrate`: it refuses, and with migrations pending any live process
    /// holds it back, whatever its version.
    Migrate,
}

/// What a background process does, or whether a call goes ahead.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Verdict {
    /// Nothing holds this binary back: a background process stages, drains
    /// and builds; a call goes ahead; `migrate` runs the pending migrations.
    TakeOver,
    /// A background process waits, and asks again next standby poll.
    StandBy(StandBy),
    /// A call or `migrate` refuses.
    Refuse(OtherVersionLive),
}

/// Why a background process stands by.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum StandBy {
    /// A process of another version is live (or, with migrations pending,
    /// any process is).
    OtherVersion(OtherVersionLive),
    /// Nothing is live, but the instance hasn't been migrated to this
    /// binary's migrations yet: a background process never migrates, it
    /// waits for `migrate` (`trellis run` migrates before it starts one).
    MigrationsPending,
}

/// ADR-0023's whole decision: rules 2, 3 and 6 over what [`observe`] read.
///
/// Another version is live (rule 2) when a live row has a different version
/// or none, unless that process stands by itself, or when the staging lock is
/// held and no row of this version holds it. Versions are compared, never
/// worker ids, so a process's own row never holds it back. With migrations
/// pending (rule 3), every live process and a held staging lock hold back a
/// background process and `migrate`, whatever their version: only a binary
/// older than the migrations can be running then.
pub(crate) fn decide(own_version: &str, observed: &Observed, asker: Asker) -> Verdict {
    let migration_check = observed.migrations_pending && asker != Asker::Call;
    let mut processes: Vec<OtherProcess> = observed
        .live
        .iter()
        .filter(|row| {
            migration_check || (row.version.as_deref() != Some(own_version) && !row.standby)
        })
        .map(|row| OtherProcess {
            worker_id: row.worker_id.clone(),
            version: row.version.clone(),
        })
        .collect();
    let mut unregistered_staging_worker = false;
    match &observed.staging_lock {
        StagingLock::Free => {}
        StagingLock::Unclaimed => unregistered_staging_worker = true,
        StagingLock::Claimed { worker_id, version } => {
            let counts = migration_check || version.as_deref() != Some(own_version);
            if counts && !processes.iter().any(|p| &p.worker_id == worker_id) {
                processes.push(OtherProcess {
                    worker_id: worker_id.clone(),
                    version: version.clone(),
                });
            }
        }
    }
    if processes.is_empty() && !unregistered_staging_worker {
        return if asker == Asker::Background && observed.migrations_pending {
            Verdict::StandBy(StandBy::MigrationsPending)
        } else {
            Verdict::TakeOver
        };
    }
    let blocked = OtherVersionLive {
        this_version: own_version.to_string(),
        processes,
        unregistered_staging_worker,
        migrations_pending: migration_check,
    };
    match asker {
        Asker::Background => Verdict::StandBy(StandBy::OtherVersion(blocked)),
        Asker::Call | Asker::Migrate => Verdict::Refuse(blocked),
    }
}

/// Another Trellis version is live on the instance, so this binary changes
/// nothing until it is gone (ADR-0023, version handover). Carried by the
/// refusal of a state-changing call ([`crate::ErrorCode::VersionHandover`]) and
/// by [`crate::DefinitionStatus::version_handover`].
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct OtherVersionLive {
    /// This binary's version.
    pub this_version: String,
    /// The live processes holding this binary back, each by worker id with
    /// its version (`None`: a release before 0.2.0, which records none).
    pub processes: Vec<OtherProcess>,
    /// The staging-worker lock is held by a session no registered process
    /// names: a staging worker of a release before 0.2.0.
    pub unregistered_staging_worker: bool,
    /// Migrations this binary carries are pending, so every live process
    /// holds it back, whatever its version: migrations never run beside one.
    pub migrations_pending: bool,
}

/// One live process of another version (see [`OtherVersionLive`]).
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct OtherProcess {
    /// Its `worker_registry` id (`trellis-<host>-<pid>-<random>`).
    pub worker_id: String,
    /// Its binary's version, or `None` for a release before 0.2.0.
    pub version: Option<String>,
}

impl OtherVersionLive {
    /// The other versions seen, each once, in order: a release before 0.2.0
    /// reads "a release before 0.2.0".
    pub fn other_versions(&self) -> Vec<String> {
        let mut versions: Vec<String> = Vec::new();
        let mut push = |v: String| {
            if !versions.contains(&v) {
                versions.push(v);
            }
        };
        for process in &self.processes {
            push(version_name(process.version.as_deref()));
        }
        if self.unregistered_staging_worker {
            push(version_name(None));
        }
        versions
    }
}

fn version_name(version: Option<&str>) -> String {
    match version {
        Some(version) => version.to_string(),
        None => "a release before 0.2.0".to_string(),
    }
}

impl fmt::Display for OtherVersionLive {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.migrations_pending {
            write!(
                f,
                "migrations are pending and Trellis processes are live on this instance, and \
                 migrations never run beside a live process; this binary is version {}",
                self.this_version
            )?;
        } else {
            write!(
                f,
                "another Trellis version is live on this instance ({}); this binary is \
                 version {}",
                self.other_versions().join(", "),
                self.this_version
            )?;
        }
        let mut seen: Vec<String> = self
            .processes
            .iter()
            .map(|p| {
                format!(
                    "worker {} at {}",
                    p.worker_id,
                    version_name(p.version.as_deref())
                )
            })
            .collect();
        if self.unregistered_staging_worker {
            seen.push(
                "a staging worker that registers no version (a release before 0.2.0)".to_string(),
            );
        }
        write!(
            f,
            "; live: {}. It changes nothing until they stop: stop the other version's \
             processes. A process killed without a clean shutdown holds this back until its \
             registry row ages out, at most {} seconds",
            seen.join(", "),
            LIVE_TTL.as_secs()
        )
    }
}

impl std::error::Error for OtherVersionLive {}

/// Reads [`decide`]'s input for `schema`'s instance on `client`. Works on a
/// schema at any migration, or none: a row's version, role and standby are
/// read through `to_jsonb`, so a registry before migration V9 reads as rows
/// without versions. `with_pending` reads whether this binary's migrations
/// are pending (a background process and `migrate` need it; a call doesn't,
/// and reads `false`).
pub(crate) async fn observe(
    client: &impl GenericClient,
    schema: &str,
    with_pending: bool,
) -> Result<Observed, tokio_postgres::Error> {
    let migrations_pending = if with_pending {
        crate::migrate::migrations_pending(client, schema).await?
    } else {
        false
    };
    let registry = format!("{}.worker_registry", quote_ident(schema));
    let has_registry: bool = client
        .query_one("select to_regclass($1) is not null", &[&registry])
        .await?
        .get(0);
    // The staging lock's holder, by backend pid.
    let holder: Option<i32> = client
        .query_opt(
            "select pid from pg_locks \
             where locktype = 'advisory' and granted and objsubid = 1 \
               and database = (select oid from pg_database where datname = current_database()) \
               and ((classid::int8 << 32) | objid::int8) = $1 \
             limit 1",
            &[&crate::staging::session::producer_singleton_lock_key(
                schema,
            )],
        )
        .await?
        .and_then(|row| row.get(0));
    let mut live = Vec::new();
    let mut claimed = None;
    if has_registry {
        // A live row, or any row naming a session: the lock holder's row
        // counts whether or not its heartbeat is fresh, since a session that
        // still holds the lock is still its process's.
        let rows = client
            .query(
                &format!(
                    "select w.worker_id, j->>'version', \
                            coalesce((j->>'standby')::boolean, false), \
                            (j->>'staging_pid')::int4, \
                            w.last_seen > now() - (interval '1 second' * $1) \
                     from {registry} w, lateral to_jsonb(w) j \
                     where w.last_seen > now() - (interval '1 second' * $1) \
                        or j->>'staging_pid' is not null \
                     order by w.worker_id"
                ),
                &[&LIVE_TTL.as_secs_f64()],
            )
            .await?;
        for row in rows {
            let worker_id: String = row.get(0);
            let version: Option<String> = row.get(1);
            let staging_pid: Option<i32> = row.get(3);
            if holder.is_some() && staging_pid == holder && claimed.is_none() {
                claimed = Some((worker_id.clone(), version.clone()));
            }
            if row.get::<_, bool>(4) {
                live.push(LiveRow {
                    worker_id,
                    version,
                    standby: row.get(2),
                });
            }
        }
    }
    let staging_lock = match (holder, claimed) {
        (None, _) => StagingLock::Free,
        (Some(_), Some((worker_id, version))) => StagingLock::Claimed { worker_id, version },
        (Some(_), None) => StagingLock::Unclaimed,
    };
    Ok(Observed {
        live,
        staging_lock,
        migrations_pending,
        registry_exists: has_registry,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    const OWN: &str = "0.2.0";

    fn row(worker_id: &str, version: Option<&str>, standby: bool) -> LiveRow {
        LiveRow {
            worker_id: worker_id.to_string(),
            version: version.map(str::to_string),
            standby,
        }
    }

    fn observed(live: Vec<LiveRow>, staging_lock: StagingLock, pending: bool) -> Observed {
        Observed {
            live,
            staging_lock,
            migrations_pending: pending,
            registry_exists: true,
        }
    }

    fn claimed(worker_id: &str, version: Option<&str>) -> StagingLock {
        StagingLock::Claimed {
            worker_id: worker_id.to_string(),
            version: version.map(str::to_string),
        }
    }

    fn other(worker_id: &str, version: Option<&str>) -> OtherProcess {
        OtherProcess {
            worker_id: worker_id.to_string(),
            version: version.map(str::to_string),
        }
    }

    fn blocked(
        processes: Vec<OtherProcess>,
        unregistered: bool,
        pending: bool,
    ) -> OtherVersionLive {
        OtherVersionLive {
            this_version: OWN.to_string(),
            processes,
            unregistered_staging_worker: unregistered,
            migrations_pending: pending,
        }
    }

    const ALL: [Asker; 3] = [Asker::Background, Asker::Call, Asker::Migrate];

    /// The empty instance, and one where only this version runs (its own
    /// row among them, leading staging), take over for every asker.
    #[test]
    fn nothing_but_this_version_live_takes_over() {
        for asker in ALL {
            assert_eq!(
                decide(OWN, &observed(vec![], StagingLock::Free, false), asker),
                Verdict::TakeOver
            );
            let same = observed(
                vec![row("me", Some(OWN), false), row("peer", Some(OWN), false)],
                claimed("peer", Some(OWN)),
                false,
            );
            assert_eq!(decide(OWN, &same, asker), Verdict::TakeOver, "{asker:?}");
        }
    }

    /// Rule 2, first half: a live row of another version holds a background
    /// process back and refuses a call, naming the row.
    #[test]
    fn a_live_row_of_another_version_stands_by_and_refuses() {
        let seen = observed(
            vec![
                row("me", Some(OWN), false),
                row("old", Some("0.1.9"), false),
            ],
            StagingLock::Free,
            false,
        );
        let expected = blocked(vec![other("old", Some("0.1.9"))], false, false);
        assert_eq!(
            decide(OWN, &seen, Asker::Background),
            Verdict::StandBy(StandBy::OtherVersion(expected.clone()))
        );
        assert_eq!(
            decide(OWN, &seen, Asker::Call),
            Verdict::Refuse(expected.clone())
        );
        assert_eq!(
            decide(OWN, &seen, Asker::Migrate),
            Verdict::Refuse(expected)
        );
    }

    /// Rule 5: any difference in the string is another version, a newer one
    /// as much as an older one, and a build suffix too.
    #[test]
    fn the_match_is_exact() {
        for version in ["0.2.1", "0.1.0", "0.2.0-rc.1", "0.2", " 0.2.0"] {
            let seen = observed(
                vec![row("w", Some(version), false)],
                StagingLock::Free,
                false,
            );
            assert!(
                matches!(decide(OWN, &seen, Asker::Call), Verdict::Refuse(_)),
                "{version:?}"
            );
        }
    }

    /// The pre-0.2.0 drain worker: a live row with no version.
    #[test]
    fn a_live_row_without_a_version_is_another_version() {
        let seen = observed(vec![row("old", None, false)], StagingLock::Free, false);
        let expected = blocked(vec![other("old", None)], false, false);
        assert_eq!(
            decide(OWN, &seen, Asker::Background),
            Verdict::StandBy(StandBy::OtherVersion(expected.clone()))
        );
        assert_eq!(decide(OWN, &seen, Asker::Call), Verdict::Refuse(expected));
    }

    /// Rule 2, second half: the pre-0.2.0 staging-only leader holds the lock
    /// and registers nothing.
    #[test]
    fn the_staging_lock_held_by_no_row_is_another_version() {
        let seen = observed(
            vec![row("me", Some(OWN), false)],
            StagingLock::Unclaimed,
            false,
        );
        let expected = blocked(vec![], true, false);
        assert_eq!(
            decide(OWN, &seen, Asker::Background),
            Verdict::StandBy(StandBy::OtherVersion(expected.clone()))
        );
        assert_eq!(decide(OWN, &seen, Asker::Call), Verdict::Refuse(expected));
    }

    /// The lock held by another version's session counts even when its row
    /// isn't live any more (a leader whose heartbeat stalled), and is named
    /// once when its row is live too.
    #[test]
    fn the_staging_lock_held_by_another_versions_row_is_another_version() {
        let stale = observed(vec![], claimed("old", Some("0.3.0")), false);
        assert_eq!(
            decide(OWN, &stale, Asker::Call),
            Verdict::Refuse(blocked(vec![other("old", Some("0.3.0"))], false, false))
        );
        let live = observed(
            vec![row("old", Some("0.3.0"), false)],
            claimed("old", Some("0.3.0")),
            false,
        );
        assert_eq!(
            decide(OWN, &live, Asker::Call),
            Verdict::Refuse(blocked(vec![other("old", Some("0.3.0"))], false, false))
        );
    }

    /// The lock held by this version's session, whose row's heartbeat has
    /// stalled, holds nothing back.
    #[test]
    fn the_staging_lock_held_by_this_versions_row_is_not() {
        let seen = observed(vec![], claimed("peer", Some(OWN)), false);
        for asker in ALL {
            assert_eq!(decide(OWN, &seen, asker), Verdict::TakeOver, "{asker:?}");
        }
    }

    /// A process of another version that stands by itself changes nothing,
    /// so it holds nothing back: two versions never wait on each other.
    #[test]
    fn a_standby_of_another_version_is_not_live() {
        let seen = observed(
            vec![row("new", Some("0.3.0"), true), row("me", Some(OWN), false)],
            StagingLock::Free,
            false,
        );
        for asker in ALL {
            assert_eq!(decide(OWN, &seen, asker), Verdict::TakeOver, "{asker:?}");
        }
    }

    /// Rule 3: with migrations pending, any live process holds back a
    /// background process and `migrate`, a row of this version or one that
    /// stands by included, and so does a held staging lock.
    #[test]
    fn with_migrations_pending_any_live_process_holds_back_migrate_and_background() {
        let cases = [
            (
                observed(
                    vec![row("old", Some("0.1.0"), false)],
                    StagingLock::Free,
                    true,
                ),
                blocked(vec![other("old", Some("0.1.0"))], false, true),
            ),
            (
                observed(vec![row("old", None, false)], StagingLock::Free, true),
                blocked(vec![other("old", None)], false, true),
            ),
            (
                observed(vec![row("same", Some(OWN), true)], StagingLock::Free, true),
                blocked(vec![other("same", Some(OWN))], false, true),
            ),
            (
                observed(vec![], StagingLock::Unclaimed, true),
                blocked(vec![], true, true),
            ),
            (
                observed(vec![], claimed("lead", Some(OWN)), true),
                blocked(vec![other("lead", Some(OWN))], false, true),
            ),
        ];
        for (seen, expected) in cases {
            assert_eq!(
                decide(OWN, &seen, Asker::Background),
                Verdict::StandBy(StandBy::OtherVersion(expected.clone())),
                "{seen:?}"
            );
            assert_eq!(
                decide(OWN, &seen, Asker::Migrate),
                Verdict::Refuse(expected),
                "{seen:?}"
            );
        }
    }

    /// Rule 3 with nothing live: `migrate` runs the migrations, and a
    /// background process waits for it (it never migrates).
    #[test]
    fn with_migrations_pending_and_nothing_live_migrate_takes_over() {
        let seen = observed(vec![], StagingLock::Free, true);
        assert_eq!(decide(OWN, &seen, Asker::Migrate), Verdict::TakeOver);
        assert_eq!(
            decide(OWN, &seen, Asker::Background),
            Verdict::StandBy(StandBy::MigrationsPending)
        );
    }

    /// A call never reads pending migrations; if handed them, only rule 2
    /// applies to it.
    #[test]
    fn a_call_ignores_pending_migrations() {
        let seen = observed(vec![row("same", Some(OWN), false)], StagingLock::Free, true);
        assert_eq!(decide(OWN, &seen, Asker::Call), Verdict::TakeOver);
    }

    /// The refusal names this binary's version, every other version once,
    /// each worker id, and the TTL a killed process's row delays takeover by.
    #[test]
    fn the_message_names_the_versions_and_workers() {
        let refusal = blocked(
            vec![
                other("trellis-a-1-00", Some("0.1.9")),
                other("trellis-b-2-00", None),
                other("trellis-c-3-00", Some("0.1.9")),
            ],
            true,
            false,
        );
        assert_eq!(
            refusal.other_versions(),
            ["0.1.9", "a release before 0.2.0"]
        );
        let text = refusal.to_string();
        for needle in [
            "this binary is version 0.2.0",
            "(0.1.9, a release before 0.2.0)",
            "worker trellis-a-1-00 at 0.1.9",
            "worker trellis-b-2-00 at a release before 0.2.0",
            "a staging worker that registers no version",
            "at most 30 seconds",
        ] {
            assert!(text.contains(needle), "{needle:?} missing from {text}");
        }
        let pending = blocked(vec![other("w", Some("0.1.0"))], false, true).to_string();
        assert!(pending.contains("migrations are pending"), "{pending}");
    }
}
