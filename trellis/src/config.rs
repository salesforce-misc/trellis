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
//!    `PGDATABASE` environment variables, plus the TLS ones (`PGSSLMODE`,
//!    `PGSSLROOTCERT`, `PGSSLCERT`, `PGSSLKEY`), assembled into a DSN.
//!
//! The Postgres schema Trellis manages its own objects under is likewise
//! resolved from `TRELLIS_SCHEMA`, defaulting to [`DEFAULT_SCHEMA`]. The
//! schema *transform target tables* are created under is a separate,
//! independently configurable setting — resolved from `TRELLIS_TARGET_SCHEMA`,
//! defaulting to [`DEFAULT_TARGET_SCHEMA`] (`public`) — deliberately decoupled
//! from the Trellis-managed catalog schema above: defaulting
//! target tables into the same schema as the catalog made every new
//! transform a potential name collision with Trellis's own future
//! catalog/state objects.
//!
//!
//! Pool sizing is not part of a [`Config`]: a handle's call pool is sized by
//! [`crate::TrellisOptions::max_call_pool_size`] and
//! [`crate::TrellisOptions::call_pool_wait_timeout`], and its background pool
//! by its roles (see [`crate::Pool::background`]).

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
/// Trellis grows its own catalog/state tables in that schema.
pub const DEFAULT_TARGET_SCHEMA: &str = "public";

/// The default cap on a handle's call pool
/// ([`crate::TrellisOptions::max_call_pool_size`]): how many physical
/// connections the handle's API calls (`status`, `apply`, `define`, ...) open
/// at once. Calls hold one connection each, two or three when a call nests
/// (`resume_column`), so the default leaves room for a few concurrent
/// callers. The background client's pool is sized by role instead
/// ([`crate::Pool::background`]).
///
/// Neither pool counts Trellis's dedicated sessions (the producer session, the
/// `LISTEN` connection, the claim keep-alive) or each paged drain's
/// `TEMP`-table session (see `ClientOptions::drain_batch_cap`), up to one per
/// drain thread while oversized shares page.
pub const DEFAULT_CALL_POOL_MAX_SIZE: usize = 20;

/// The default timeout [`crate::Pool::get`] waits for a free connection
/// before failing with a typed [`crate::Error::Pool`] error, instead of
/// deadpool's default of waiting forever. Long enough that a
/// normal, brief load spike doesn't spuriously fail, but bounded so a
/// genuinely exhausted pool surfaces as a loud, diagnosable error instead of
/// a silent, permanent stall. The call pool's value is
/// [`crate::TrellisOptions::call_pool_wait_timeout`].
pub const DEFAULT_CALL_POOL_WAIT_TIMEOUT: Duration = Duration::from_secs(30);

/// How long a pooled connection may sit unused before the pool closes it
/// (both pools; not configurable).
pub const POOL_IDLE_TIMEOUT: Duration = Duration::from_secs(60);

/// Resolved connection configuration.
///
/// Every field but `dsn` is private and every constructor validates the
/// schema names via `validate_schema_name`, so there is no way to hold a
/// `Config` whose schema hasn't been checked — see [`Config::with_schema`]
/// and [`Config::with_target_schema`].
///
/// `Debug` is written by hand rather than derived so a `{:?}` (a `tracing`
/// field, a panic message, an embedder's log line) can't print the DSN's
/// password. It shows a password-free DSN as written and
/// replaces any other with `<redacted>`.
#[derive(Clone)]
pub struct Config {
    /// A Postgres connection string, in either URL (`postgresql://...`) or
    /// libpq keyword/value (`host=... user=...`) form.
    dsn: String,
    /// The schema Trellis operates in. See [`DEFAULT_SCHEMA`].
    schema: String,
    /// The schema transform target tables are created under. See
    /// [`DEFAULT_TARGET_SCHEMA`].
    target_schema: String,
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
        Self::with_schema(dsn, schema)?.with_target_schema(target_schema)
    }

    /// Builds a [`Config`] from an explicit DSN, bypassing environment
    /// resolution entirely (the schema and target schema are still resolved
    /// from `TRELLIS_SCHEMA`/`DEFAULT_SCHEMA` and
    /// `TRELLIS_TARGET_SCHEMA`/`DEFAULT_TARGET_SCHEMA`, and validated).
    /// Useful for tests.
    pub fn from_dsn(dsn: impl Into<String>) -> Result<Self, Error> {
        let schema = std::env::var("TRELLIS_SCHEMA").unwrap_or_else(|_| DEFAULT_SCHEMA.to_string());
        let target_schema = target_schema_from_env();
        Self::with_schema(dsn, schema)?.with_target_schema(target_schema)
    }

    /// Builds a [`Config`] from an explicit DSN and schema, validating the
    /// schema via `validate_schema_name` and defaulting the target schema
    /// to `DEFAULT_TARGET_SCHEMA` (override via [`Config::with_target_schema`]).
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
        })
    }

    /// Returns `self` with its target schema overridden to `target_schema`,
    /// validated via `validate_schema_name` just like [`Config::with_schema`]
    /// validates the instance schema.
    pub fn with_target_schema(mut self, target_schema: impl Into<String>) -> Result<Self, Error> {
        let target_schema = target_schema.into();
        validate_schema_name(&target_schema)?;
        self.target_schema = target_schema;
        Ok(self)
    }

    /// The Postgres connection string this instance was configured with.
    pub fn dsn(&self) -> &str {
        &self.dsn
    }

    /// The Postgres schema this instance is configured to operate in. See
    /// `DEFAULT_SCHEMA` and `docs/instance-identity.md`.
    pub fn schema(&self) -> &str {
        &self.schema
    }

    /// The Postgres schema transform target tables are created under. See
    /// `DEFAULT_TARGET_SCHEMA`.
    pub fn target_schema(&self) -> &str {
        &self.target_schema
    }

    fn dsn_from_env() -> String {
        dsn_from_vars(|name| std::env::var(name).ok())
    }
}

/// The `PGSSL*` environment variables and the connection options they set.
const PG_TLS_VARS: [(&str, &str); 4] = [
    ("PGSSLMODE", "sslmode"),
    ("PGSSLROOTCERT", "sslrootcert"),
    ("PGSSLCERT", "sslcert"),
    ("PGSSLKEY", "sslkey"),
];

/// [`Config::dsn_from_env`], reading variables through `var` so tests need
/// not touch the process environment: `TRELLIS_DATABASE_URL` verbatim, else
/// a DSN from the `PG*` variables, `PGSSL*` included. Their values are
/// checked where every DSN's are, when it is parsed.
fn dsn_from_vars(var: impl Fn(&str) -> Option<String>) -> String {
    if let Some(dsn) = var("TRELLIS_DATABASE_URL") {
        return dsn;
    }

    let host = var("PGHOST").unwrap_or_else(|| "localhost".to_string());
    let port = var("PGPORT").unwrap_or_else(|| "5432".to_string());
    let user = var("PGUSER").unwrap_or_else(|| "postgres".to_string());
    let dbname = var("PGDATABASE").unwrap_or_else(|| user.clone());
    let password = var("PGPASSWORD");
    let mut dsn = keyword_value_dsn(&host, &port, &user, password.as_deref(), &dbname);
    for (name, option) in PG_TLS_VARS {
        if let Some(value) = var(name) {
            dsn.push_str(&format!(" {option}={}", quote_kv(&value)));
        }
    }
    dsn
}

/// `value` single-quoted for a key=value DSN, with `\` and `'`
/// backslash-escaped, so it reaches the connection exactly as given.
fn quote_kv(value: &str) -> String {
    format!("'{}'", value.replace('\\', r"\\").replace('\'', r"\'"))
}

/// Assembles a keyword/value DSN from the `PG*` environment values.
///
/// Every value is single-quoted with `\` and `'` backslash-escaped, so it
/// reaches the connection exactly as given. A URL would need each part
/// percent-encoded instead: interpolated raw, a `PGPASSWORD` holding `@`
/// moved the rest of the password into the host, and a `PGHOST` socket
/// directory (`/var/run/postgresql`) became part of the database name.
fn keyword_value_dsn(
    host: &str,
    port: &str,
    user: &str,
    password: Option<&str>,
    dbname: &str,
) -> String {
    let quoted = quote_kv;
    let mut dsn = format!(
        "host={} port={} user={} dbname={}",
        quoted(host),
        quoted(port),
        quoted(user),
        quoted(dbname),
    );
    if let Some(password) = password {
        dsn.push_str(" password=");
        dsn.push_str(&quoted(password));
    }
    dsn
}

impl fmt::Debug for Config {
    /// Destructures `self` so a new field can't be added without deciding
    /// here whether it's safe to print.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let Self {
            dsn,
            schema,
            target_schema,
        } = self;
        f.debug_struct("Config")
            .field("dsn", &RedactedDsn(dsn))
            .field("schema", schema)
            .field("target_schema", target_schema)
            .finish()
    }
}

/// `Debug`s a DSN without its password.
///
/// Rather than masking the password in place, which would mean
/// re-implementing the connection-string grammar and getting every quoting
/// and escaping rule right, this asks [`tokio_postgres::Config`]'s own
/// parser (the one [`crate::Pool`] connects with) whether the DSN carries a
/// password at all. A DSN that parses and carries none prints as written,
/// so the host, port, user and database stay visible for debugging. Any
/// other DSN prints as `<redacted>`. That includes one that fails to parse,
/// since there's no telling where its password would be. It also covers a
/// URL whose password holds an unencoded `@`, which the parser splits at
/// the first `@`, so part of the password would land in the host.
pub(crate) struct RedactedDsn<'a>(pub(crate) &'a str);

impl fmt::Debug for RedactedDsn<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match parse_dsn(self.0) {
            Ok(parsed) if parsed.get_password().is_none() => fmt::Debug::fmt(self.0, f),
            _ => f.write_str("<redacted>"),
        }
    }
}

/// Parses `dsn` with [`tokio_postgres::Config`]'s parser. Every place
/// Trellis parses a DSN goes through this.
///
/// On failure it reports a fixed message and drops tokio_postgres's error.
/// That error's `Debug` and `source()` name the offending
/// option, so a password holding an unquoted space
/// (`password=hunter2 xyzzy=1`) or a URL query value holding an unencoded
/// `&` would print its tail as an "unknown option". Nothing in a DSN that
/// fails to parse can be trusted not to be password material, which is the
/// same reason [`RedactedDsn`] prints one as `<redacted>`. The message names
/// the form the DSN looked like and what usually breaks it, without quoting
/// any of it.
pub(crate) fn parse_dsn(dsn: &str) -> Result<tokio_postgres::Config, Error> {
    parse_dsn_tls(dsn).map(|(config, _)| config)
}

/// [`parse_dsn`], plus the TLS options the DSN carried
/// ([`crate::tls::split_dsn`]); the pool builds its connector from them.
pub(crate) fn parse_dsn_tls(
    dsn: &str,
) -> Result<(tokio_postgres::Config, crate::tls::TlsSpec), Error> {
    let unparsable = || {
        let hint = if dsn.starts_with("postgres://") || dsn.starts_with("postgresql://") {
            "a postgres:// URL; percent-encode reserved characters (such as @ : / ? # & and \
             spaces) in the user, password and query values"
        } else {
            "a key=value connection string; single-quote any value that contains spaces, and \
             escape a quote or backslash inside it with a backslash"
        };
        Error::Config(format!(
            "the database connection string does not parse as {hint} (the parser's detail is \
             withheld because it can quote part of a password)"
        ))
    };
    // A DSN the TLS pre-pass cannot scan is refused here rather than handed
    // to `tokio_postgres`: its parser stops silently at some malformed input
    // and would then connect with a `sslmode` the pre-pass never saw.
    let (rest, tls) = crate::tls::split_dsn(dsn)?.ok_or_else(unparsable)?;
    let mut config: tokio_postgres::Config = rest.parse().map_err(|_| unparsable())?;
    tls.apply(&mut config)?;
    Ok((config, tls))
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

/// Test fixtures: DSNs that fail to parse and carry a
/// password whose tail tokio_postgres's parse error would name.
#[cfg(test)]
pub(crate) mod malformed_dsn_fixtures {
    /// `password=hunter2 xyzzy=1` meant a password of `hunter2 xyzzy=1`, but
    /// unquoted it parses as `password=hunter2` plus an unknown option
    /// `xyzzy`, which tokio_postgres's error names. The URL form does the
    /// same through a query parameter holding an unencoded `&`.
    pub(crate) const DSNS: [&str; 2] = [
        "host=db.example.com password=hunter2 xyzzy=1",
        "postgresql://db.example.com/app?password=hunter2&xyzzy=1",
    ];

    const FRAGMENTS: [&str; 2] = ["hunter2", "xyzzy"];

    /// Asserts that neither `err` nor anything on its `source()` chain puts
    /// a fragment of the password in its `Display` or `Debug`.
    pub(crate) fn assert_no_password_fragment(dsn: &str, err: &(dyn std::error::Error + 'static)) {
        let mut next = Some(err);
        while let Some(err) = next {
            let printed = format!("{err} / {err:?}");
            for fragment in FRAGMENTS {
                assert!(
                    !printed.contains(fragment),
                    "the error for {dsn:?} leaks {fragment:?}: {printed}"
                );
            }
            next = err.source();
        }
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

    /// The `PG*` values reach the connection exactly as given, whatever
    /// characters they hold: a `PGPASSWORD` with `@`, `:`, `/`, quotes or
    /// backslashes, and a `PGHOST` socket directory.
    // `Host::Unix` only exists on unix.
    #[cfg(unix)]
    #[test]
    fn env_dsn_carries_pg_values_through_verbatim() {
        let password = r#"p@ss:w/rd?#&'q'\ x"#;
        let dsn = keyword_value_dsn(
            "/var/run/postgresql",
            "5433",
            "o'brien@corp",
            Some(password),
            "my db",
        );
        let parsed: tokio_postgres::Config = dsn.parse().expect("the assembled DSN parses");
        assert_eq!(parsed.get_password(), Some(password.as_bytes()));
        assert_eq!(parsed.get_user(), Some("o'brien@corp"));
        assert_eq!(parsed.get_dbname(), Some("my db"));
        assert_eq!(parsed.get_ports(), [5433]);
        assert_eq!(
            parsed.get_hosts(),
            [tokio_postgres::config::Host::Unix(
                "/var/run/postgresql".into()
            )]
        );

        let parsed: tokio_postgres::Config =
            keyword_value_dsn("db.example.com", "5432", "alice", None, "app")
                .parse()
                .expect("the assembled DSN parses");
        assert_eq!(parsed.get_password(), None);
        assert_eq!(
            parsed.get_hosts(),
            [tokio_postgres::config::Host::Tcp("db.example.com".into())]
        );
    }

    /// The `PGSSL*` variables reach the DSN assembled from the `PG*` ones,
    /// quoted like the rest, and are checked like the same options written
    /// in a DSN. Read through a map, so no test touches the process
    /// environment.
    #[test]
    fn env_dsn_carries_the_pgssl_variables() {
        let vars = |pairs: &[(&str, &str)]| {
            let map: std::collections::HashMap<String, String> = pairs
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect();
            dsn_from_vars(move |name| map.get(name).cloned())
        };
        let dsn = vars(&[
            ("PGHOST", "db.example.com"),
            ("PGPASSWORD", "pw-secret"),
            ("PGSSLMODE", "verify-full"),
            ("PGSSLROOTCERT", "/etc/my certs/ca's.pem"),
            ("PGSSLCERT", "/etc/c.pem"),
            ("PGSSLKEY", "/etc/k.pem"),
        ]);
        let (config, tls) = parse_dsn_tls(&dsn).expect("parses");
        assert_eq!(config.get_password(), Some(&b"pw-secret"[..]));
        let tls = format!("{tls:?}");
        for expected in [
            "VerifyFull",
            "/etc/my certs/ca's.pem",
            "/etc/c.pem",
            "/etc/k.pem",
        ] {
            assert!(tls.contains(expected), "{expected}: {tls}");
        }

        // Without them the default is `prefer`, as for any DSN.
        let (config, _) = parse_dsn_tls(&vars(&[("PGHOST", "db")])).unwrap();
        assert_eq!(
            config.get_ssl_mode(),
            tokio_postgres::config::SslMode::Prefer
        );

        // A bad value is the same error as in a DSN, without the password.
        for (pairs, expected) in [
            (&[("PGSSLMODE", "allow")][..], "sslmode must be one of"),
            (&[("PGSSLMODE", "verify-ca")][..], "need sslrootcert"),
            (&[("PGSSLCERT", "/c.pem")][..], "given together"),
        ] {
            let mut pairs = pairs.to_vec();
            pairs.push(("PGPASSWORD", "pw-secret"));
            let err = parse_dsn_tls(&vars(&pairs)).unwrap_err().to_string();
            assert!(err.contains(expected), "{err}");
            assert!(!err.contains("pw-secret"), "{err}");
        }

        // `TRELLIS_DATABASE_URL` is used verbatim; the `PG*` variables,
        // `PGSSL*` included, do not apply to it.
        assert_eq!(
            vars(&[("TRELLIS_DATABASE_URL", "host=a"), ("PGSSLMODE", "disable")]),
            "host=a"
        );
    }

    /// The fixed parse-failure message still says which form
    /// the DSN looked like, so there's something to act on.
    #[test]
    fn unparsable_dsn_message_names_its_form() {
        let [key_value, url] = malformed_dsn_fixtures::DSNS;
        let message = parse_dsn(key_value).unwrap_err().to_string();
        assert!(message.contains("key=value"), "{message}");
        let message = parse_dsn(url).unwrap_err().to_string();
        assert!(message.contains("postgres:// URL"), "{message}");
        assert!(parse_dsn("host=db.example.com user=alice").is_ok());
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

    /// Neither `{:?}` nor `{}` on a [`Config`] may print the
    /// DSN's password, whichever form carries it.
    #[test]
    fn config_debug_and_display_never_print_the_password() {
        for dsn in [
            "postgresql://alice:s3cret@db.example.com:5432/app",
            "postgres://alice:s3cret@db.example.com/app?sslmode=disable",
            "postgresql://alice@db.example.com/app?password=s3cret&sslmode=disable",
            // An unencoded `@` in the password: the parser splits at the
            // first `@`, so the tail would otherwise surface as the host.
            "postgresql://alice:s3cret@s3cret@db.example.com/app",
            "host=db.example.com user=alice password=s3cret dbname=app",
            "host=db.example.com user=alice password = 's3cret with spaces' dbname=app",
            // Unparseable (an unknown keyword): redacted whole.
            "host=db.example.com sslpassword=s3cret",
            "host=db.example.com password='s3cret",
        ] {
            let config = Config::with_schema(dsn, "app_trellis").unwrap();
            let debug = format!("{config:?}");
            let pretty = format!("{config:#?}");
            let display = format!("{config}");
            for printed in [&debug, &pretty, &display] {
                assert!(!printed.contains("s3cret"), "{dsn:?} leaked: {printed}");
            }
            assert!(debug.contains("dsn: <redacted>"), "{debug}");
            // The rest of the configuration stays visible.
            assert!(debug.contains(r#"schema: "app_trellis""#), "{debug}");
        }
    }

    /// A DSN with no password has nothing to hide, so `{:?}` prints it as
    /// written, keeping the host, port, user and database visible.
    #[test]
    fn config_debug_prints_a_password_free_dsn_as_written() {
        for dsn in [
            "postgresql://alice@db.example.com:5432/app",
            "postgresql:///app?host=/tmp/sockets",
            "host=/tmp/sockets port=5433 user=postgres dbname=app",
        ] {
            let config = Config::from_dsn(dsn).unwrap();
            let debug = format!("{config:?}");
            assert!(debug.contains(&format!("dsn: {dsn:?}")), "{debug}");
        }
    }
}
