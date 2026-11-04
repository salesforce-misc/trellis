//! The catalog floor (ADR-0024, rules 1 and 2): the oldest Trellis release
//! that can read this catalog, and the check every attach makes against it
//! before the migration runner touches the schema.
//!
//! The floor is one row of `catalog_floor`, which migration V11 creates at
//! `0.2.0`. A migration an older release can't read raises it in its own SQL,
//! to the greater of the stored floor and its own (see `crate::migrate`'s
//! "Raising the catalog floor"), so the applied history and the floor never
//! disagree.
//!
//! A binary at or above the floor attaches, and tolerates applied migrations
//! newer than any it carries: a newer release migrated the catalog, and the
//! floor says this one can still read it. Refinery's blanket `abort_missing`
//! would refuse those, so [`decide`] classifies the applied history first,
//! and the runner runs with `abort_missing` off only once it has passed. A gap
//! below the binary's newest migration, or a shared version whose name or
//! checksum differs, still refuses.
//!
//! [`decide`] is pure, so every case is tested without a database; [`check`]
//! reads its input. `migrate` runs [`check`] as it attaches; a handle that
//! connects without migrating doesn't yet (#1602).

use crate::error::Error;
use crate::pool::quote_ident;
use crate::release_version::ReleaseVersion;

/// The catalog floor this binary's migrations leave: the highest any of
/// them declares. Raise it with the migration that raises the stored floor;
/// a test checks the two agree.
///
/// A build carries its release's migrations, so it meets every floor they
/// declare whatever its version string says: a pre-release (`0.2.0-rc.1`)
/// or a development build that still carries the previous release's version
/// reads the catalog its own migrations leave. Only that catalog: once a
/// later build of the same release has run a migration this one lacks, the
/// floor no longer tells them apart, and the build's own version decides.
pub(crate) const CATALOG_FLOOR: &str = "0.2.0";

/// One migration, applied (a row of refinery's history) or carried by a
/// binary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct MigrationRecord {
    pub(crate) version: i32,
    pub(crate) name: String,
    /// Refinery's checksum, as its history stores it (a decimal `u64`).
    pub(crate) checksum: String,
}

/// What [`decide`] decides.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Verdict {
    /// The binary can read the catalog. `future` are the applied migrations
    /// newer than any it carries, in order.
    Attach {
        future: Vec<i32>,
    },
    Refuse(Refusal),
}

/// Why [`decide`] refuses an attach.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Refusal {
    /// The binary's version is below the catalog floor.
    BelowFloor { floor: String, this_version: String },
    /// The stored floor isn't a release version this binary can read.
    UnreadableFloor { floor: String },
    /// The binary's own version isn't semver.
    UnreadableVersion { this_version: String },
    /// A version both have, with another name or checksum.
    Divergent {
        applied: MigrationRecord,
        carried: MigrationRecord,
    },
    /// An applied migration the binary lacks, below its newest.
    NotCarried {
        applied: MigrationRecord,
        newest_carried: i32,
    },
    /// One of the binary's migrations isn't applied, below the newest applied.
    NotApplied {
        carried: MigrationRecord,
        newest_applied: i32,
    },
    /// Applied migrations newer than the binary's, on a catalog that records
    /// no floor to say this binary can read them.
    FutureWithoutFloor { versions: Vec<i32> },
}

impl Refusal {
    /// The error the attach returns.
    pub(crate) fn into_error(self) -> Error {
        match self {
            Refusal::BelowFloor {
                floor,
                this_version,
            } => Error::BelowCatalogFloor {
                floor,
                this_version,
            },
            other => Error::MigrationHistory(other.to_string()),
        }
    }
}

impl std::fmt::Display for Refusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Refusal::BelowFloor {
                floor,
                this_version,
            } => write!(
                f,
                "the catalog floor is {floor}: Trellis {floor} or later can read this \
                 catalog, and this binary is version {this_version}"
            ),
            Refusal::UnreadableFloor { floor } => write!(
                f,
                "the catalog floor {floor:?} is not a release version this binary can read"
            ),
            Refusal::UnreadableVersion { this_version } => {
                write!(f, "this binary's version {this_version:?} is not semver")
            }
            Refusal::Divergent { applied, carried } => write!(
                f,
                "migration V{} on this catalog differs from this binary's: the catalog ran \
                 {:?} with checksum {}, and this binary carries {:?} with checksum {}",
                applied.version, applied.name, applied.checksum, carried.name, carried.checksum
            ),
            Refusal::NotCarried {
                applied,
                newest_carried,
            } => write!(
                f,
                "migration V{} ({:?}) has run on this catalog, and this binary, whose \
                 migrations run to V{newest_carried}, doesn't carry it",
                applied.version, applied.name
            ),
            Refusal::NotApplied {
                carried,
                newest_applied,
            } => write!(
                f,
                "this binary's migration V{} ({:?}) hasn't run on this catalog, which has \
                 run migrations up to V{newest_applied}",
                carried.version, carried.name
            ),
            Refusal::FutureWithoutFloor { versions } => write!(
                f,
                "migrations {} have run on this catalog, this binary doesn't carry them, and \
                 the catalog records no floor saying which versions can read it",
                versions_list(versions)
            ),
        }
    }
}

fn versions_list(versions: &[i32]) -> String {
    versions
        .iter()
        .map(|v| format!("V{v}"))
        .collect::<Vec<_>>()
        .join(", ")
}

/// ADR-0024 rule 2 over what [`check`] read: whether a binary of
/// `own_version`, whose migrations are `carried` and leave the floor
/// `own_floor`, can attach to a catalog recording `catalog_floor` (`None`
/// before V11 has run) and the history `applied`.
///
/// The binary meets the floor when its version is at or above it, or when
/// the floor its own migrations leave is and the catalog has run no
/// migration the binary lacks (see [`CATALOG_FLOOR`]).
pub(crate) fn decide(
    own_version: &str,
    own_floor: &str,
    catalog_floor: Option<&str>,
    applied: &[MigrationRecord],
    carried: &[MigrationRecord],
) -> Verdict {
    if let Some(floor_text) = catalog_floor {
        let Some(floor) = ReleaseVersion::parse(floor_text) else {
            return Verdict::Refuse(Refusal::UnreadableFloor {
                floor: floor_text.to_string(),
            });
        };
        let Some(own) = ReleaseVersion::parse(own_version) else {
            return Verdict::Refuse(Refusal::UnreadableVersion {
                this_version: own_version.to_string(),
            });
        };
        // The floor this binary's migrations declare counts only on a catalog
        // they left: one that has run no migration this binary lacks. A
        // pre-release or development build shares its floor with the later
        // builds of that release, so a migration one of them added since
        // doesn't move the floor past it.
        let carries_every_applied = applied
            .iter()
            .all(|a| carried.iter().any(|c| c.version == a.version));
        let meets = own >= floor
            || (carries_every_applied
                && ReleaseVersion::parse(own_floor).is_some_and(|f| f >= floor));
        if !meets {
            return Verdict::Refuse(Refusal::BelowFloor {
                floor: floor_text.to_string(),
                this_version: own_version.to_string(),
            });
        }
    }

    for applied in applied {
        if let Some(carried) = carried.iter().find(|c| c.version == applied.version)
            && (carried.name != applied.name || carried.checksum != applied.checksum)
        {
            return Verdict::Refuse(Refusal::Divergent {
                applied: applied.clone(),
                carried: carried.clone(),
            });
        }
    }

    let newest_carried = carried.iter().map(|c| c.version).max();
    let newest_applied = applied.iter().map(|a| a.version).max();
    let mut future = Vec::new();
    for applied in applied {
        if carried.iter().any(|c| c.version == applied.version) {
            continue;
        }
        match newest_carried {
            Some(newest) if applied.version < newest => {
                return Verdict::Refuse(Refusal::NotCarried {
                    applied: applied.clone(),
                    newest_carried: newest,
                });
            }
            _ => future.push(applied.version),
        }
    }
    if let Some(newest) = newest_applied {
        for carried in carried {
            if carried.version < newest && !applied.iter().any(|a| a.version == carried.version) {
                return Verdict::Refuse(Refusal::NotApplied {
                    carried: carried.clone(),
                    newest_applied: newest,
                });
            }
        }
    }
    future.sort_unstable();
    if !future.is_empty() && catalog_floor.is_none() {
        return Verdict::Refuse(Refusal::FutureWithoutFloor { versions: future });
    }
    Verdict::Attach { future }
}

/// The stored catalog floor, or `None` when `schema` has no `catalog_floor`
/// table (V11 hasn't run) or no row in it.
pub(crate) async fn read_floor(
    client: &impl tokio_postgres::GenericClient,
    schema: &str,
) -> Result<Option<String>, tokio_postgres::Error> {
    let table = format!("{}.catalog_floor", quote_ident(schema));
    let exists: bool = client
        .query_one("select to_regclass($1) is not null", &[&table])
        .await?
        .get(0);
    if !exists {
        return Ok(None);
    }
    Ok(client
        .query_opt(&format!("select version from {table}"), &[])
        .await?
        .map(|row| row.get(0)))
}

/// Refinery's history on `schema`, empty when it has none (never migrated).
async fn read_history(
    client: &impl tokio_postgres::GenericClient,
    schema: &str,
) -> Result<Vec<MigrationRecord>, tokio_postgres::Error> {
    let table = format!("{}.refinery_schema_history", quote_ident(schema));
    let exists: bool = client
        .query_one("select to_regclass($1) is not null", &[&table])
        .await?
        .get(0);
    if !exists {
        return Ok(Vec::new());
    }
    Ok(client
        .query(
            &format!("select version, name, checksum from {table} order by version"),
            &[],
        )
        .await?
        .iter()
        .map(|row| MigrationRecord {
            version: row.get(0),
            name: row.get::<_, Option<String>>(1).unwrap_or_default(),
            checksum: row.get::<_, Option<String>>(2).unwrap_or_default(),
        })
        .collect())
}

/// Reads the catalog floor and the applied history of `schema` and runs
/// [`decide`] for a binary of `own_version` carrying `carried`, whose
/// migrations leave `own_floor`. Refuses with the matching [`Error`], or
/// logs the applied migrations newer than the binary's, once, and returns.
pub(crate) async fn check(
    client: &impl tokio_postgres::GenericClient,
    schema: &str,
    own_version: &str,
    own_floor: &str,
    carried: &[MigrationRecord],
) -> Result<(), Error> {
    let floor = read_floor(client, schema).await?;
    let applied = read_history(client, schema).await?;
    match decide(own_version, own_floor, floor.as_deref(), &applied, carried) {
        Verdict::Refuse(refusal) => Err(refusal.into_error()),
        Verdict::Attach { future } => {
            if !future.is_empty() {
                crate::instance_log::info!(
                    "migrations {} have run on this catalog and this binary (version \
                     {own_version}) doesn't carry them: a newer Trellis migrated it, and the \
                     catalog floor ({}) says this version can read it",
                    versions_list(&future),
                    floor.as_deref().unwrap_or("none"),
                );
            }
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn m(version: i32) -> MigrationRecord {
        MigrationRecord {
            version,
            name: format!("migration_{version}"),
            checksum: format!("{}", 1000 + version),
        }
    }

    fn ms(versions: impl IntoIterator<Item = i32>) -> Vec<MigrationRecord> {
        versions.into_iter().map(m).collect()
    }

    const OWN: &str = "0.2.0";

    fn attach(future: impl IntoIterator<Item = i32>) -> Verdict {
        Verdict::Attach {
            future: future.into_iter().collect(),
        }
    }

    #[test]
    fn a_fresh_catalog_attaches() {
        assert_eq!(decide(OWN, OWN, None, &[], &ms(1..=11)), attach([]));
    }

    #[test]
    fn a_catalog_this_binary_migrated_attaches() {
        assert_eq!(
            decide(OWN, OWN, Some("0.2.0"), &ms(1..=11), &ms(1..=11)),
            attach([])
        );
    }

    /// Before V11 the catalog has no floor, and the binary's own migrations
    /// are pending above the newest applied one.
    #[test]
    fn a_catalog_before_the_floor_with_migrations_pending_attaches() {
        assert_eq!(decide(OWN, OWN, None, &ms(1..=8), &ms(1..=11)), attach([]));
    }

    #[test]
    fn migrations_newer_than_the_binary_are_tolerated_at_or_above_the_floor() {
        assert_eq!(
            decide(OWN, OWN, Some("0.2.0"), &ms(1..=13), &ms(1..=11)),
            attach([12, 13])
        );
        assert_eq!(
            decide("0.3.0", OWN, Some("0.3.0"), &ms(1..=13), &ms(1..=11)),
            attach([12, 13]),
            "at the floor"
        );
    }

    #[test]
    fn a_binary_below_the_floor_refuses() {
        assert_eq!(
            decide(OWN, OWN, Some("0.3.0"), &ms(1..=13), &ms(1..=11)),
            Verdict::Refuse(Refusal::BelowFloor {
                floor: "0.3.0".to_string(),
                this_version: OWN.to_string(),
            })
        );
    }

    /// The floor refuses even with no migration the binary lacks: a newer
    /// release can raise it for a format it writes without a migration.
    #[test]
    fn the_floor_refuses_without_future_migrations() {
        assert!(matches!(
            decide(OWN, OWN, Some("0.3.0"), &ms(1..=11), &ms(1..=11)),
            Verdict::Refuse(Refusal::BelowFloor { .. })
        ));
    }

    /// A pre-release precedes its release: `0.3.0-rc.1` is below a `0.3.0`
    /// floor unless its own migrations leave that floor, and a pre-release of
    /// a later version is above it.
    #[test]
    fn pre_release_versions_compare_as_semver() {
        assert!(matches!(
            decide(
                "0.3.0-rc.1",
                "0.2.0",
                Some("0.3.0"),
                &ms(1..=12),
                &ms(1..=11)
            ),
            Verdict::Refuse(Refusal::BelowFloor { .. })
        ));
        assert_eq!(
            decide(
                "0.3.0-rc.1",
                "0.3.0",
                Some("0.3.0"),
                &ms(1..=12),
                &ms(1..=12)
            ),
            attach([])
        );
        assert_eq!(
            decide(
                "0.3.1-alpha",
                "0.2.0",
                Some("0.3.0"),
                &ms(1..=12),
                &ms(1..=11)
            ),
            attach([12])
        );
        assert_eq!(
            decide(
                "0.3.0+build.7",
                "0.2.0",
                Some("0.3.0"),
                &ms(1..=12),
                &ms(1..=11)
            ),
            attach([12])
        );
    }

    /// A development build that still carries the previous release's version
    /// reads the catalog its own migrations leave.
    #[test]
    fn a_binary_meets_the_floor_its_own_migrations_leave() {
        assert_eq!(
            decide("0.1.0", "0.2.0", Some("0.2.0"), &ms(1..=11), &ms(1..=11)),
            attach([])
        );
        assert!(matches!(
            decide("0.1.0", "0.1.0", Some("0.2.0"), &ms(1..=11), &ms(1..=11)),
            Verdict::Refuse(Refusal::BelowFloor { .. })
        ));
    }

    /// Only on the catalog its own migrations leave: `0.3.0-rc.1`, whose V12
    /// raised the floor to 0.3.0, meets that floor until `0.3.0` adds a V13.
    /// The floor stays 0.3.0, so it can't say whether rc.1 reads V13, and
    /// rc.1's own version, below 0.3.0, decides.
    #[test]
    fn its_own_migrations_floor_does_not_cover_a_later_builds_migrations() {
        assert_eq!(
            decide(
                "0.3.0-rc.1",
                "0.3.0",
                Some("0.3.0"),
                &ms(1..=12),
                &ms(1..=12)
            ),
            attach([])
        );
        assert_eq!(
            decide(
                "0.3.0-rc.1",
                "0.3.0",
                Some("0.3.0"),
                &ms(1..=13),
                &ms(1..=12)
            ),
            Verdict::Refuse(Refusal::BelowFloor {
                floor: "0.3.0".to_string(),
                this_version: "0.3.0-rc.1".to_string(),
            })
        );
        assert_eq!(
            decide("0.3.0", "0.3.0", Some("0.3.0"), &ms(1..=13), &ms(1..=12)),
            attach([13]),
            "a release at the floor still tolerates it"
        );
    }

    #[test]
    fn a_checksum_divergence_refuses() {
        let mut applied = ms(1..=11);
        applied[2].checksum = "99".to_string();
        assert_eq!(
            decide(OWN, OWN, Some("0.2.0"), &applied, &ms(1..=11)),
            Verdict::Refuse(Refusal::Divergent {
                applied: applied[2].clone(),
                carried: m(3),
            })
        );
    }

    #[test]
    fn a_name_divergence_refuses() {
        let mut applied = ms(1..=13);
        applied[10].name = "someone_elses_v11".to_string();
        assert!(matches!(
            decide(OWN, OWN, Some("0.2.0"), &applied, &ms(1..=11)),
            Verdict::Refuse(Refusal::Divergent { .. })
        ));
    }

    /// An applied migration the binary lacks, below its newest: not a newer
    /// release's migration but another line's.
    #[test]
    fn an_applied_migration_the_binary_lacks_below_its_newest_refuses() {
        let carried: Vec<_> = ms(1..=9).into_iter().chain([m(11)]).collect();
        assert_eq!(
            decide(OWN, OWN, Some("0.2.0"), &ms(1..=12), &carried),
            Verdict::Refuse(Refusal::NotCarried {
                applied: m(10),
                newest_carried: 11,
            })
        );
    }

    /// One of the binary's own migrations not applied, below the newest
    /// applied: refinery with `abort_missing` off would skip it, so the check
    /// refuses instead (rule 3: never skip one of its own).
    #[test]
    fn a_carried_migration_missing_below_the_newest_applied_refuses() {
        let applied: Vec<_> = ms(1..=10).into_iter().chain([m(12)]).collect();
        assert_eq!(
            decide(OWN, OWN, Some("0.2.0"), &applied, &ms(1..=11)),
            Verdict::Refuse(Refusal::NotApplied {
                carried: m(11),
                newest_applied: 12,
            })
        );
    }

    #[test]
    fn future_migrations_without_a_floor_refuse() {
        assert_eq!(
            decide(OWN, OWN, None, &ms(1..=12), &ms(1..=11)),
            Verdict::Refuse(Refusal::FutureWithoutFloor { versions: vec![12] })
        );
    }

    #[test]
    fn an_unreadable_floor_or_version_refuses() {
        assert_eq!(
            decide(OWN, OWN, Some("next"), &ms(1..=11), &ms(1..=11)),
            Verdict::Refuse(Refusal::UnreadableFloor {
                floor: "next".to_string()
            })
        );
        assert_eq!(
            decide("dev", OWN, Some("0.2.0"), &ms(1..=11), &ms(1..=11)),
            Verdict::Refuse(Refusal::UnreadableVersion {
                this_version: "dev".to_string()
            })
        );
    }

    /// The divergence message names the checksum, which the upgrade
    /// battery's control step matches.
    #[test]
    fn the_divergence_message_names_the_checksum() {
        let mut applied = ms(1..=3);
        applied[2].checksum = "99".to_string();
        let Verdict::Refuse(refusal) = decide(OWN, OWN, None, &applied, &ms(1..=3)) else {
            panic!("refuses");
        };
        assert!(refusal.to_string().contains("checksum"), "{refusal}");
    }
}
