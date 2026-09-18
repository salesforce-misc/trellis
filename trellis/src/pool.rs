//! Connection pool management, built on `deadpool-postgres`.
//!
//! [`Pool::new`] wires up a `deadpool_postgres::Pool` from a resolved
//! [`Config`] and attaches a per-connection session bootstrap hook (via
//! deadpool's `post_create` hook). That hook pins `search_path` to the
//! configured Trellis schema followed by the configured target schema (see
//! `Config::target_schema`), so migrations and staging objects land in the
//! Trellis schema and transform target tables resolve without every query
//! needing to qualify table names, while [`crate::defs::ddl`] still
//! schema-qualifies target-table DDL explicitly (`search_path` only decides
//! where an *unqualified* `CREATE TABLE` lands, and that must be the
//! Trellis schema, not the target schema, for the rest of this module's
//! unqualified references to Trellis's own objects to keep resolving
//! correctly).
//!
//! This hook is also the seam intake will extend: it's the one place that
//! runs exactly once per physical connection, before it's ever handed to a
//! caller, which is where `synchronous_commit = on` will need to be enforced
//! for the staging ring to make its durability guarantees.
//! Nothing beyond `search_path` is enforced here yet.

use crate::config::Config;
use crate::error::Error;
use deadpool_postgres::{
    Hook, HookError, Manager, ManagerConfig, Pool as DeadpoolPool, RecyclingMethod, Runtime,
};
use std::str::FromStr;
use tokio_postgres::NoTls;

/// A pooled connection, handed out by [`Pool::get`].
pub type Client = deadpool_postgres::Client;

/// Trellis's connection pool.
///
/// Connections are unencrypted (`NoTls`) for now; TLS is out of scope for
/// this issue and can be layered on by swapping the `NoTls` connector below
/// for a real one once a TLS approach is chosen.
///
/// `Clone` is cheap: `deadpool_postgres::Pool` is itself `Arc`-backed, so a
/// clone shares the same underlying pool of physical connections rather than
/// opening a second one. [`crate::Client`]'s app-worker tasks each get their
/// own clone rather than a `&Pool` reference, since every worker runs in its
/// own spawned task with its own lifetime.
#[derive(Debug, Clone)]
pub struct Pool {
    inner: DeadpoolPool,
    /// A copy of the [`Config::target_schema`] this pool was built from —
    /// see [`Pool::target_schema`].
    target_schema: String,
}

impl Pool {
    /// Builds a pool from `config`. Fails only if `config.dsn` can't be
    /// parsed as a Postgres connection string or deadpool's builder rejects
    /// the resulting configuration; no network I/O happens here — the pool
    /// connects lazily on first `get()`.
    ///
    /// Sized and timed out per `config.pool_max_size()`/
    /// `config.pool_wait_timeout()` (issue #182) rather than deadpool's own
    /// defaults (`4 * num_cpus` connections, no wait timeout at all): with
    /// no cap and no timeout, a handful of concurrent callers each needing
    /// a *second* connection while their own transaction holds a first
    /// (`crate::staging::quarantine::trip_transform_fuse_if_crossed`'s
    /// `catalog::transforms_for_source` call) could exhaust a small box's
    /// pool and then [`Pool::get`] would wait forever — no error, no log,
    /// just every drain thread stuck. See [`crate::config::DEFAULT_POOL_MAX_SIZE`]/
    /// [`crate::config::DEFAULT_POOL_WAIT_TIMEOUT`] for the sizing/timeout
    /// reasoning.
    pub fn new(config: &Config) -> Result<Self, Error> {
        let pg_config = tokio_postgres::Config::from_str(config.dsn())
            .map_err(|err| Error::Config(format!("invalid database connection string: {err}")))?;

        let manager_config = ManagerConfig {
            recycling_method: RecyclingMethod::Fast,
        };
        let manager = Manager::from_config(pg_config, NoTls, manager_config);

        let schema = config.schema().to_string();
        let target_schema = config.target_schema().to_string();
        let inner = DeadpoolPool::builder(manager)
            .post_create(Hook::async_fn(move |client, _metrics| {
                let schema = schema.clone();
                let target_schema = target_schema.clone();
                Box::pin(async move {
                    session_bootstrap(client, &schema, &target_schema)
                        .await
                        .map_err(HookError::Backend)
                })
            }))
            .max_size(config.pool_max_size())
            // A `Runtime` is required for `wait_timeout` to take effect at
            // all — without one, `deadpool`'s `PoolBuilder::build` rejects
            // the config outright (`BuildError::NoRuntimeSpecified`) rather
            // than silently ignoring the timeout. `Runtime::Tokio1` matches
            // deadpool-postgres's own default feature (`rt_tokio_1`) and
            // every other async runtime this crate already assumes
            // (tokio_postgres::Client, the post_create hook above).
            .runtime(Runtime::Tokio1)
            .wait_timeout(Some(config.pool_wait_timeout()))
            .build()?;

        Ok(Self {
            inner,
            target_schema: config.target_schema().to_string(),
        })
    }

    /// Acquires a connection, waiting up to `config.pool_wait_timeout()`
    /// (see [`Pool::new`]) for one to become available if the pool is at
    /// capacity. Once that timeout elapses, returns
    /// `Err(Error::Pool(deadpool_postgres::PoolError::Timeout(_)))` — a
    /// clear, typed, loggable failure (categorized [`crate::ErrorCode::Connectivity`]
    /// via [`Error::code`]) instead of hanging indefinitely (issue #182).
    pub async fn get(&self) -> Result<Client, Error> {
        Ok(self.inner.get().await?)
    }

    /// The [`Config::target_schema`] this pool was built with (issue #73,
    /// ADR-0007) — the same value baked into every connection's own
    /// `search_path` by [`session_bootstrap`] above, and the same value
    /// every real caller of [`crate::defs::install_definition`] already
    /// passes as its own `target_schema` argument (that argument and this
    /// one are never meant to diverge; `install_definition` still takes its
    /// own explicit parameter rather than reading this one, so its
    /// target-table DDL and [`crate::defs::catalog::create_definition_inner`]'s
    /// persisted qualification are threaded from one guaranteed-identical
    /// value instead of two that merely happen to agree). Exposed so
    /// [`crate::defs::catalog::create_definition`]/
    /// [`crate::defs::catalog::create_definition_without_backfill`] — the
    /// ring-path entry points, which take no `target_schema` parameter of
    /// their own — can qualify a definition's target table the same way,
    /// without widening their public signature.
    pub(crate) fn target_schema(&self) -> &str {
        &self.target_schema
    }
}

/// Runs once per physical connection, right after it's established and
/// before it's returned to any caller.
///
/// Pins `search_path` to `schema` (first, so unqualified references to
/// Trellis's own objects always resolve there) followed by `target_schema`
/// (so unqualified reads/writes against a transform target table resolve
/// even when it lives outside both `schema` and `public`) and `public`
/// (Postgres's own default, kept last as a fallback for anything that
/// depends on it today). Beyond `search_path`, this is the seam intake will
/// use to enforce `synchronous_commit = on`.
async fn session_bootstrap(
    client: &mut tokio_postgres::Client,
    schema: &str,
    target_schema: &str,
) -> Result<(), tokio_postgres::Error> {
    client
        .batch_execute(&format!(
            "set search_path to {}, {}, public",
            quote_ident(schema),
            quote_ident(target_schema)
        ))
        .await
}

/// Quotes a Postgres identifier for safe interpolation into SQL text.
///
/// `config.schema` is operator-supplied (an environment variable), not
/// end-user input, but it still flows into SQL as text rather than a bind
/// parameter (identifiers can't be bound), so it's quoted defensively.
pub(crate) fn quote_ident(ident: &str) -> String {
    format!("\"{}\"", ident.replace('"', "\"\""))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quote_ident_escapes_embedded_quotes() {
        assert_eq!(quote_ident("trellis"), "\"trellis\"");
        assert_eq!(quote_ident("weird\"schema"), "\"weird\"\"schema\"");
    }

    #[test]
    fn unparsable_dsn_is_a_typed_config_error() {
        let config =
            Config::from_dsn("not a valid dsn").expect("schema is valid; only the DSN is bogus");
        match Pool::new(&config) {
            Err(Error::Config(_)) => {}
            other => panic!("expected a typed config error, got {other:?}"),
        }
    }
}
