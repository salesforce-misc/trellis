//! Applies Trellis's ordered SQL migrations via `refinery`.
//!
//! Migration files live in `trellis/migrations/` and are embedded into the
//! binary at compile time by [`refinery::embed_migrations!`] — nothing is
//! read from disk at runtime, so a deployed binary carries its own
//! migrations. Applied versions are tracked in refinery's own
//! `refinery_schema_history` table, created (like everything else Trellis
//! owns) in the schema configured on [`Config`] via `search_path`
//! (see [`crate::pool`]).
//!
//! Each migration runs in its own transaction (refinery's behavior, not
//! something built here): a failing migration rolls back cleanly and is
//! not recorded as applied, so the ledger never reflects a partial
//! migration.
//!
//! Before the runner ever touches the schema, [`crate::identity`] decides
//! whether attaching to it is safe at all — see that module for what
//! "safe" means.

mod embedded {
    #![allow(clippy::needless_raw_string_hashes)]
    refinery::embed_migrations!("migrations");
}

use crate::config::Config;
use crate::error::Error;
use crate::identity;
use crate::pool::Pool;

/// Ensures `config.schema()` exists and is safe to attach to (see
/// [`crate::identity`]), then applies any migrations not yet recorded as
/// applied. A no-op if everything is already up to date.
///
/// Deliberately public — a tier-2 composable primitive under ADR-0012, not a
/// leaked internal. [`crate::Trellis::migrate`] and
/// [`crate::BlockingTrellis::migrate`] delegate to it, and an embedder (or a
/// shared test bootstrap such as `testkit`'s) may call it directly to bring a
/// schema up to date without standing up a facade.
pub async fn migrate(pool: &Pool, config: &Config) -> Result<(), Error> {
    let mut client = pool.get().await?;
    let pg_client: &mut tokio_postgres::Client = &mut client;

    identity::prepare_attach(pg_client, config).await?;

    embedded::migrations::runner().run_async(pg_client).await?;

    // Always seed/reconcile the marker, not just on a "fresh attach" — see
    // `identity::seed_marker` for why: it's an idempotent upsert, so a
    // clean re-attach, a resumed crash between the runner creating
    // `trellis_instance` and this committing, and two processes racing a
    // first-ever attach are all a no-op here rather than three different
    // states to track separately.
    identity::seed_marker(pg_client, config).await?;

    Ok(())
}
