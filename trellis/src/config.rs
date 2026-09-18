//! Configuration for connecting to Postgres.
//!
//! Config comes only from CLI args (passed in by the caller) and
//! environment variables — no config crate, no config files. Resolution
//! order for the DSN:
//!
//! 1. An explicit DSN passed by the caller (e.g. a `--database-url` CLI
//!    flag).
//! 2. The `TRELLIS_DATABASE_URL` environment variable, used verbatim.
//! 3. The standard libpq `PGHOST`/`PGPORT`/`PGUSER`/`PGPASSWORD`/
//!    `PGDATABASE` environment variables, assembled into a DSN.
//!
//! The Postgres schema Trellis manages its own objects under is likewise
//! resolved from `TRELLIS_SCHEMA`, defaulting to [`DEFAULT_SCHEMA`]. The
//! schema *transform target tables* are created under is a separate,
//! independently configurable setting — resolved from `TRELLIS_TARGET_SCHEMA`,
//! defaulting to [`DEFAULT_TARGET_SCHEMA`] (`public`) — deliberately decoupled
//! from the Trellis-managed catalog schema above (issue #15): defaulting
//! target tables into the same schema as the catalog made every new
//! transform a potential name collision with Trellis's own future
//! catalog/state objects.
//!
//! [`crate::Pool`]'s sizing/timeout (issue #182) is configured the same
//! way: `TRELLIS_POOL_MAX_SIZE` (default [`DEFAULT_POOL_MAX_SIZE`]) caps how
//! many physical connections the pool opens, and
//! `TRELLIS_POOL_WAIT_TIMEOUT_SECS` (default [`DEFAULT_POOL_WAIT_TIMEOUT`])
//! bounds how long [`crate::Pool::get`] waits for one to free up before
//! failing with a typed error, instead of deadpool's own defaults (a
//! box-size-dependent `max_size` and no wait timeout at all).

use crate::error::Error;
use std::fmt;
use std::time::Duration;

/// The default Postgres schema Trellis's own objects (staging tables, the
/// refinery migration ledger, and eventually the transform catalog) live
/// under. Kept separate from `public` so an install's footprint can be
/// inspected or dropped without touching application schemas that happen to
/// share the database. This is a deliberate but minimal choice for now —
/// later work may need finer-grained namespacing (e.g. per-workspace
/// schemas). See `docs/instance-identity.md`.
pub const DEFAULT_SCHEMA: &str = "trellis";

/// The default Postgres schema transform *target* tables are created
/// under — `public`, matching the default schema every other DDL statement
/// (e.g. a bare `CREATE TABLE`) would land in absent an explicit schema.
/// Deliberately distinct from [`DEFAULT_SCHEMA`]: target tables are
/// application data a user queries directly, not part of Trellis's own
/// managed footprint, so defaulting them into the Trellis instance schema
/// (as an earlier POC did) meant a high chance of future name conflicts as
/// Trellis grows its own catalog/state tables in that schema (issue #15).
pub const DEFAULT_TARGET_SCHEMA: &str = "public";

/// The default cap on how many physical connections [`crate::Pool`] will
/// ever open at once (issue #182).
///
/// `deadpool_postgres`'s own default (`2 * num_cpus`, per `deadpool`'s
/// `get_default_pool_max_size`) scales with the box this happens to run on,
/// not with this engine's actual concurrency pattern, and — paired with no
/// wait timeout — is how a handful of
/// concurrent evictions each needing a *second* pool connection while their
/// own transaction holds a first (`trip_transform_fuse_if_crossed`,
/// `crate::staging::quarantine`) could exhaust a small box's pool and hang
/// every drain thread involved forever. This fixed floor is sized well
/// above the concurrency any single [`crate::Trellis::connect`] connection
/// drives today (one staging worker plus a handful of `drain_threads`, each
/// normally holding at most one connection and occasionally a second),
/// with generous headroom for that occasional double-acquisition to happen
/// several times over without contending — while still being small enough
/// that exhausting it is a real, actionable signal rather than something
/// that only happens after hundreds of runaway workers. Override via
/// `TRELLIS_POOL_MAX_SIZE` if an operator's own `drain_threads` count needs
/// more.
pub const DEFAULT_POOL_MAX_SIZE: usize = 20;

/// The default timeout [`crate::Pool::get`] waits for a free connection
/// before failing with a typed [`crate::Error::Pool`] error, instead of
/// deadpool's default of waiting forever (issue #182). Long enough that a
/// normal, brief load spike (a burst of concurrent evictions, a slow
/// query holding a connection a bit longer than usual) doesn't spuriously
/// fail, but bounded so a genuinely exhausted pool surfaces as a loud,
/// diagnosable error within a bounded amount of time instead of a silent,
/// permanent stall. Override via `TRELLIS_POOL_WAIT_TIMEOUT_SECS`.
pub const DEFAULT_POOL_WAIT_TIMEOUT: Duration = Duration::from_secs(30);

/// Resolved connection configuration.
///
/// Every field but `dsn` is private and every constructor validates the
/// schema names via [`validate_schema_name`], so there is no way to hold a
/// `Config` whose schema hasn't been checked — see [`Config::with_schema`]
/// and [`Config::with_target_schema`].
#[derive(Debug, Clone)]
pub struct Config {
    /// A Postgres connection string, in either URL (`postgresql://...`) or
    /// libpq keyword/value (`host=... user=...`) form.
    dsn: String,
    /// The schema Trellis operates in. See [`DEFAULT_SCHEMA`].
    schema: String,
    /// The schema transform target tables are created under. See
    /// [`DEFAULT_TARGET_SCHEMA`].
    target_schema: String,
    /// The cap on physical connections [`crate::Pool`] opens. See
    /// [`DEFAULT_POOL_MAX_SIZE`].
    pool_max_size: usize,
    /// How long [`crate::Pool::get`] waits for a free connection before
    /// failing. See [`DEFAULT_POOL_WAIT_TIMEOUT`].
    pool_wait_timeout: Duration,
}

impl Config {
    /// Resolves configuration from an optional explicit DSN (typically a
    /// CLI flag the caller already parsed) and environment variables.
    pub fn resolve(cli_dsn: Option<String>) -> Result<Self, Error> {
        let dsn = match cli_dsn {
            Some(dsn) => dsn,
            None => Self::dsn_from_env(),
        };

        if dsn.trim().is_empty() {
            return Err(Error::Config(
                "no database connection string provided (pass a DSN, set \
                 TRELLIS_DATABASE_URL, or set PGHOST/PGDATABASE/...)"
                    .to_string(),
            ));
        }

        let schema = std::env::var("TRELLIS_SCHEMA").unwrap_or_else(|_| DEFAULT_SCHEMA.to_string());
        let target_schema = target_schema_from_env();
        let pool_max_size = pool_max_size_from_env()?;
        let pool_wait_timeout = pool_wait_timeout_from_env()?;
        Ok(Self::with_schema(dsn, schema)?
            .with_target_schema(target_schema)?
            .with_pool_max_size(pool_max_size)?
            .with_pool_wait_timeout(pool_wait_timeout))
    }

    /// Builds a [`Config`] from an explicit DSN, bypassing environment
    /// resolution entirely (the schema and target schema are still resolved
    /// from `TRELLIS_SCHEMA`/[`DEFAULT_SCHEMA`] and
    /// `TRELLIS_TARGET_SCHEMA`/[`DEFAULT_TARGET_SCHEMA`], and validated —
    /// likewise the pool sizing/timeout, from `TRELLIS_POOL_MAX_SIZE`/
    /// [`DEFAULT_POOL_MAX_SIZE`] and `TRELLIS_POOL_WAIT_TIMEOUT_SECS`/
    /// [`DEFAULT_POOL_WAIT_TIMEOUT`]). Useful for tests.
    pub fn from_dsn(dsn: impl Into<String>) -> Result<Self, Error> {
        let schema = std::env::var("TRELLIS_SCHEMA").unwrap_or_else(|_| DEFAULT_SCHEMA.to_string());
        let target_schema = target_schema_from_env();
        let pool_max_size = pool_max_size_from_env()?;
        let pool_wait_timeout = pool_wait_timeout_from_env()?;
        Ok(Self::with_schema(dsn, schema)?
            .with_target_schema(target_schema)?
            .with_pool_max_size(pool_max_size)?
            .with_pool_wait_timeout(pool_wait_timeout))
    }

    /// Builds a [`Config`] from an explicit DSN and schema, validating the
    /// schema via [`validate_schema_name`] and defaulting the target schema
    /// to [`DEFAULT_TARGET_SCHEMA`] (override via [`Config::with_target_schema`]).
    /// This is the one place a `Config` is actually constructed —
    /// [`Config::resolve`] and [`Config::from_dsn`] both resolve a schema
    /// and hand it to this — so there is no path to a `Config` carrying an
    /// unvalidated schema name.
    pub fn with_schema(dsn: impl Into<String>, schema: impl Into<String>) -> Result<Self, Error> {
        let schema = schema.into();
        validate_schema_name(&schema)?;
        Ok(Self {
            dsn: dsn.into(),
            schema,
            target_schema: DEFAULT_TARGET_SCHEMA.to_string(),
            pool_max_size: DEFAULT_POOL_MAX_SIZE,
            pool_wait_timeout: DEFAULT_POOL_WAIT_TIMEOUT,
        })
    }

    /// Returns `self` with its target schema overridden to `target_schema`,
    /// validated via [`validate_schema_name`] just like [`Config::with_schema`]
    /// validates the instance schema.
    pub fn with_target_schema(mut self, target_schema: impl Into<String>) -> Result<Self, Error> {
        let target_schema = target_schema.into();
        validate_schema_name(&target_schema)?;
        self.target_schema = target_schema;
        Ok(self)
    }

    /// Returns `self` with its connection pool's `max_size` overridden (issue
    /// #182) — see [`DEFAULT_POOL_MAX_SIZE`]. Rejects `0`: a pool that can
    /// never open a single connection isn't a smaller pool, it's a broken
    /// one, and letting it through here would only surface as a confusing
    /// wait-timeout on every single [`crate::Pool::get`] call instead of a
    /// clear configuration error now.
    pub fn with_pool_max_size(mut self, pool_max_size: usize) -> Result<Self, Error> {
        if pool_max_size == 0 {
            return Err(Error::Config(
                "pool_max_size must be at least 1".to_string(),
            ));
        }
        self.pool_max_size = pool_max_size;
        Ok(self)
    }

    /// Returns `self` with its connection pool's wait timeout overridden
    /// (issue #182) — see [`DEFAULT_POOL_WAIT_TIMEOUT`]. Unlike
    /// [`Config::with_pool_max_size`], every [`Duration`] (including
    /// [`Duration::ZERO`], which just makes [`crate::Pool::get`] fail
    /// immediately when no connection is already free — a legitimate, if
    /// unusual, choice) is a coherent value, so there is nothing to reject.
    pub fn with_pool_wait_timeout(mut self, pool_wait_timeout: Duration) -> Self {
        self.pool_wait_timeout = pool_wait_timeout;
        self
    }

    /// The Postgres connection string this instance was configured with.
    pub fn dsn(&self) -> &str {
        &self.dsn
    }

    /// The Postgres schema this instance is configured to operate in. See
    /// [`DEFAULT_SCHEMA`] and `docs/instance-identity.md`.
    pub fn schema(&self) -> &str {
        &self.schema
    }

    /// The Postgres schema transform target tables are created under. See
    /// [`DEFAULT_TARGET_SCHEMA`].
    pub fn target_schema(&self) -> &str {
        &self.target_schema
    }

    /// The cap [`crate::Pool`] places on physical connections. See
    /// [`DEFAULT_POOL_MAX_SIZE`].
    pub fn pool_max_size(&self) -> usize {
        self.pool_max_size
    }

    /// How long [`crate::Pool::get`] waits for a free connection before
    /// failing. See [`DEFAULT_POOL_WAIT_TIMEOUT`].
    pub fn pool_wait_timeout(&self) -> Duration {
        self.pool_wait_timeout
    }

    fn dsn_from_env() -> String {
        if let Ok(dsn) = std::env::var("TRELLIS_DATABASE_URL") {
            return dsn;
        }

        let host = std::env::var("PGHOST").unwrap_or_else(|_| "localhost".to_string());
        let port = std::env::var("PGPORT").unwrap_or_else(|_| "5432".to_string());
        let user = std::env::var("PGUSER").unwrap_or_else(|_| "postgres".to_string());
        let dbname = std::env::var("PGDATABASE").unwrap_or_else(|_| user.clone());

        match std::env::var("PGPASSWORD") {
            Ok(password) => format!("postgresql://{user}:{password}@{host}:{port}/{dbname}"),
            Err(_) => format!("postgresql://{user}@{host}:{port}/{dbname}"),
        }
    }
}

impl fmt::Display for Config {
    /// A short, human-readable summary of the resolved instance identity —
    /// the schema this instance operates in, and the schema its transform
    /// target tables are created under — for wherever configuration gets
    /// reported (logs, diagnostics). Deliberately omits the DSN, which may
    /// carry a password.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "trellis instance in schema {:?} (target tables in schema {:?})",
            self.schema, self.target_schema
        )
    }
}

/// Validates a candidate schema name — used for both the Trellis instance
/// schema (`TRELLIS_SCHEMA`) and the transform target schema
/// (`TRELLIS_TARGET_SCHEMA`).
///
/// Every place this name reaches SQL goes through [`crate::pool::quote_ident`]
/// as a quoted (delimited) identifier, so Postgres will accept almost any
/// character in it — over-restricting the character set here would reject
/// names Postgres itself is happy with. The real hazards a quoted identifier
/// doesn't protect against are:
///
/// - **Empty or all-whitespace.** Not a name at all — almost certainly a
///   misconfigured `TRELLIS_SCHEMA`/`TRELLIS_TARGET_SCHEMA` (e.g. set to
///   `" "`), not an intentional identity.
/// - **Longer than 63 bytes.** Postgres's `NAMEDATALEN` limit means longer
///   identifiers are *silently truncated*, not rejected — two distinct
///   configured names could collide under truncation without anyone
///   noticing, which is exactly the failure mode instance identity exists to
///   prevent.
/// - **An embedded NUL byte.** Postgres (via libpq) treats a C string's NUL
///   as its terminator, so a name containing one would be interpreted as
///   something shorter and different than what was configured.
fn validate_schema_name(name: &str) -> Result<(), Error> {
    if name.trim().is_empty() {
        return Err(Error::Config(
            "schema name must not be empty or all-whitespace".to_string(),
        ));
    }
    if name.contains('\0') {
        return Err(Error::Config(
            "schema name must not contain a NUL byte".to_string(),
        ));
    }
    if name.len() > 63 {
        return Err(Error::Config(format!(
            "schema name {name:?} is {} bytes long, exceeding Postgres's 63-byte \
             identifier limit (NAMEDATALEN); Postgres would silently truncate it, \
             which could collide with another schema name",
            name.len()
        )));
    }
    Ok(())
}

/// Resolves the target schema from `TRELLIS_TARGET_SCHEMA`, defaulting to
/// [`DEFAULT_TARGET_SCHEMA`] — the one place both [`Config::resolve`] and
/// [`Config::from_dsn`] read this env var, so they can't drift.
fn target_schema_from_env() -> String {
    std::env::var("TRELLIS_TARGET_SCHEMA").unwrap_or_else(|_| DEFAULT_TARGET_SCHEMA.to_string())
}

/// Resolves the pool's `max_size` from `TRELLIS_POOL_MAX_SIZE`, defaulting
/// to [`DEFAULT_POOL_MAX_SIZE`] (issue #182) — the one place both
/// [`Config::resolve`] and [`Config::from_dsn`] read this env var, so they
/// can't drift.
fn pool_max_size_from_env() -> Result<usize, Error> {
    match std::env::var("TRELLIS_POOL_MAX_SIZE") {
        Ok(raw) => raw.trim().parse::<usize>().map_err(|err| {
            Error::Config(format!(
                "TRELLIS_POOL_MAX_SIZE {raw:?} is not a valid positive integer: {err}"
            ))
        }),
        Err(_) => Ok(DEFAULT_POOL_MAX_SIZE),
    }
}

/// Resolves the pool's wait timeout from `TRELLIS_POOL_WAIT_TIMEOUT_SECS`
/// (whole seconds), defaulting to [`DEFAULT_POOL_WAIT_TIMEOUT`] (issue
/// #182) — the one place both [`Config::resolve`] and [`Config::from_dsn`]
/// read this env var, so they can't drift.
fn pool_wait_timeout_from_env() -> Result<Duration, Error> {
    match std::env::var("TRELLIS_POOL_WAIT_TIMEOUT_SECS") {
        Ok(raw) => {
            let secs: u64 = raw.trim().parse().map_err(|err| {
                Error::Config(format!(
                    "TRELLIS_POOL_WAIT_TIMEOUT_SECS {raw:?} is not a valid non-negative integer \
                     number of seconds: {err}"
                ))
            })?;
            Ok(Duration::from_secs(secs))
        }
        Err(_) => Ok(DEFAULT_POOL_WAIT_TIMEOUT),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn explicit_dsn_wins() {
        let config = Config::resolve(Some("postgresql://example/db".to_string())).unwrap();
        assert_eq!(config.dsn(), "postgresql://example/db");
        assert_eq!(config.schema(), DEFAULT_SCHEMA);
    }

    #[test]
    fn target_schema_defaults_to_public_not_the_instance_schema() {
        let config = Config::from_dsn("postgresql://example/db").unwrap();
        assert_eq!(config.target_schema(), DEFAULT_TARGET_SCHEMA);
        assert_eq!(DEFAULT_TARGET_SCHEMA, "public");
        assert_ne!(config.target_schema(), config.schema());
    }

    #[test]
    fn with_target_schema_overrides_the_default() {
        let config = Config::from_dsn("postgresql://example/db")
            .unwrap()
            .with_target_schema("analytics")
            .unwrap();
        assert_eq!(config.target_schema(), "analytics");
        // Overriding the target schema leaves the instance schema alone.
        assert_eq!(config.schema(), DEFAULT_SCHEMA);
    }

    #[test]
    fn invalid_target_schema_name_is_rejected() {
        let err = Config::from_dsn("postgresql://example/db")
            .unwrap()
            .with_target_schema("")
            .unwrap_err();
        assert!(matches!(err, Error::Config(_)));
    }

    #[test]
    fn empty_dsn_is_rejected() {
        let err = Config::resolve(Some(String::new())).unwrap_err();
        assert!(matches!(err, Error::Config(_)));
    }

    #[test]
    fn valid_schema_name_passes() {
        assert!(validate_schema_name("trellis").is_ok());
        assert!(validate_schema_name(DEFAULT_SCHEMA).is_ok());
    }

    #[test]
    fn empty_schema_name_is_rejected() {
        assert!(matches!(validate_schema_name(""), Err(Error::Config(_))));
    }

    #[test]
    fn whitespace_only_schema_name_is_rejected() {
        assert!(matches!(
            validate_schema_name("   \t  "),
            Err(Error::Config(_))
        ));
    }

    #[test]
    fn schema_name_over_namedatalen_is_rejected() {
        let too_long = "a".repeat(64);
        assert!(matches!(
            validate_schema_name(&too_long),
            Err(Error::Config(_))
        ));
        // Exactly at the limit is fine.
        let at_limit = "a".repeat(63);
        assert!(validate_schema_name(&at_limit).is_ok());
    }

    #[test]
    fn schema_name_with_embedded_nul_is_rejected() {
        assert!(matches!(
            validate_schema_name("trel\0lis"),
            Err(Error::Config(_))
        ));
    }

    // Deliberately not testing `TRELLIS_POOL_MAX_SIZE`/
    // `TRELLIS_POOL_WAIT_TIMEOUT_SECS` themselves below: every other test in
    // this module already assumes `TRELLIS_SCHEMA`/`TRELLIS_TARGET_SCHEMA`
    // are unset in the test environment rather than mutating process-global
    // env state (which `cargo test`'s multi-threaded default would race
    // across tests) — these two follow the same convention.
    #[test]
    fn pool_sizing_and_timeout_default_when_unset() {
        let config = Config::from_dsn("postgresql://example/db").unwrap();
        assert_eq!(config.pool_max_size(), DEFAULT_POOL_MAX_SIZE);
        assert_eq!(config.pool_wait_timeout(), DEFAULT_POOL_WAIT_TIMEOUT);
    }

    #[test]
    fn with_pool_max_size_overrides_the_default() {
        let config = Config::from_dsn("postgresql://example/db")
            .unwrap()
            .with_pool_max_size(7)
            .unwrap();
        assert_eq!(config.pool_max_size(), 7);
    }

    #[test]
    fn zero_pool_max_size_is_rejected() {
        let err = Config::from_dsn("postgresql://example/db")
            .unwrap()
            .with_pool_max_size(0)
            .unwrap_err();
        assert!(matches!(err, Error::Config(_)));
    }

    #[test]
    fn with_pool_wait_timeout_overrides_the_default() {
        let config = Config::from_dsn("postgresql://example/db")
            .unwrap()
            .with_pool_wait_timeout(Duration::from_millis(250));
        assert_eq!(config.pool_wait_timeout(), Duration::from_millis(250));
    }
}
