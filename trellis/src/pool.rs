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
//! Beyond `search_path`, the hook also pins the output GUCs that make a
//! value's text rendering depend on the value alone rather than on the
//! session reading it (see [`DETERMINISTIC_TEXT_OUTPUT_GUCS`]), and turns on
//! TCP keepalives at both ends so a partitioned connection's locks don't
//! outlive it by hours (see [`tcp_keepalive_gucs`], issue #364). The
//! dedicated, non-pooled connections get the same through
//! [`connect_dedicated`] and [`dedicated_session_setup`].

use crate::config::Config;
use crate::error::Error;
use deadpool_postgres::{
    Hook, HookError, Manager, ManagerConfig, Pool as DeadpoolPool, RecyclingMethod, Runtime,
};
use std::time::Duration;
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
    /// The connection settings the pool's connections are opened with,
    /// client keepalives included, kept so [`Pool::connect_unpooled`] can
    /// open one more the same way. `tokio_postgres::Config`'s `Debug`
    /// redacts the password (issue #591).
    pg_config: tokio_postgres::Config,
    /// A copy of the [`Config::schema`] this pool was built from — see
    /// [`Pool::schema`].
    schema: String,
    /// A copy of the [`Config::target_schema`] this pool was built from —
    /// see [`Pool::target_schema`]. [`Pool::connect_unpooled`] bootstraps
    /// its connection's `search_path` with it.
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
    /// defaults (`2 * num_cpus` connections, no wait timeout at all): with
    /// no cap and no timeout, a handful of concurrent callers each needing
    /// a *second* connection while their own transaction holds a first
    /// (`crate::staging::quarantine::trip_transform_fuse_if_crossed`'s
    /// `catalog::transforms_for_source` call) could exhaust a small box's
    /// pool and then [`Pool::get`] would wait forever — no error, no log,
    /// just every drain thread stuck. See [`crate::config::DEFAULT_POOL_MAX_SIZE`]/
    /// [`crate::config::DEFAULT_POOL_WAIT_TIMEOUT`] for the sizing/timeout
    /// reasoning.
    pub fn new(config: &Config) -> Result<Self, Error> {
        let mut pg_config = crate::config::parse_dsn(config.dsn())?;
        with_client_keepalives(&mut pg_config, DeadPeerDetection::KeepalivesOnly);

        let manager_config = ManagerConfig {
            recycling_method: RecyclingMethod::Fast,
        };
        let manager = Manager::from_config(pg_config.clone(), NoTls, manager_config);

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
            pg_config,
            schema: config.schema().to_string(),
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

    /// Opens one connection outside the pool, set up exactly like a pooled
    /// one ([`session_bootstrap`]), and spawns its driver. It closes when the
    /// returned client is dropped, and it never counts against
    /// `pool_max_size` or waits on it.
    ///
    /// For session state that must never reach a pooled connection: a paged
    /// drain's `TEMP` table (issue #620, [`crate::staging::page`]) lives on
    /// one of these, so however the drain ends (finished, failed, or its
    /// future dropped mid-page) the table goes with the session, and no later
    /// borrower of a pooled connection can inherit it.
    pub(crate) async fn connect_unpooled(&self) -> Result<tokio_postgres::Client, Error> {
        let (mut client, connection) = self.pg_config.connect(NoTls).await?;
        tokio::spawn(async move {
            if let Err(err) = connection.await {
                tracing::debug!(error = %err, "unpooled connection ended with an error");
            }
        });
        session_bootstrap(&mut client, &self.schema, &self.target_schema).await?;
        Ok(client)
    }

    /// The [`Config::schema`] this pool was built with: the instance's own
    /// catalog schema, first on every connection's `search_path`. Trellis's
    /// internal tables that DDL creates at runtime rather than a migration
    /// (a to-one relationship's settled parent projection, issue #435) are
    /// qualified with it explicitly.
    pub(crate) fn schema(&self) -> &str {
        &self.schema
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
    /// test-fixture entry points, which take no `target_schema` parameter of
    /// their own — can qualify a definition's target table the same way,
    /// without widening their public signature. Gated like its only callers.
    #[cfg(any(test, feature = "test-util"))]
    pub(crate) fn target_schema(&self) -> &str {
        &self.target_schema
    }
}

/// The GUCs that make a value's **text rendering** a function of the value
/// alone rather than of the session it's read on.
///
/// On the **output** side every one of these is already what a stock server
/// produces, so this changes nothing an unconfigured Postgres does — it makes
/// a guarantee Trellis was previously *assuming* into one it actually
/// enforces. A server, database or role with `ALTER ... SET datestyle`
/// applied (or `PGDATESTYLE` in the environment) silently breaks that
/// assumption today.
///
/// One caveat, because the claim above is not quite "these are the boot
/// values": `DateStyle`'s boot value is `ISO, MDY`, and the second field is
/// an *input* field-order preference that has no effect on output at all.
/// Pinning `YMD` rather than `MDY` therefore does change one thing on a
/// stock server — how an *ambiguous* all-numeric date literal typed into a
/// session is read (`'01/02/2024'::date` is 2024-01-02 under `MDY` and an
/// out-of-range error under `YMD`). That is deliberate and safe here: every
/// date text Trellis itself puts into SQL is canonical leading-4-digit ISO
/// (see the input note below), which parses identically under either field
/// order, and application SQL runs on the application's own connections,
/// not on these. `YMD` is chosen so that an ambiguous spelling *fails loudly*
/// on an engine connection instead of being silently reinterpreted.
///
/// Why Trellis needs it, concretely: a computed value travels through the
/// engine as **text** ([`crate::defs::eval::Value`]), and correctness is
/// established by comparing two independently-authored renderings of it —
/// the Rust evaluator's and Postgres's own (ADR-0013's continuous
/// cross-check; `docs/generative-test-suite.md`). For a
/// [`crate::defs::ValueType::Other`] value that comparison is **byte-exact**
/// (the generative suite's `Comparison::Exact`), so any GUC that changes how
/// Postgres spells a value turns a perfectly converged target into a
/// reported divergence. Under `DateStyle = 'SQL, MDY'`, for instance,
/// `('2024-01-01'::date)::text` renders `01/01/2024`, and nothing is
/// actually wrong.
///
/// The first four pinned here cover what issue #109's typed literals can
/// produce (`date`/`timestamp` under `DateStyle`, `bytea` under
/// `bytea_output`), what issue #112's float split added (`real`/`double
/// precision` under `extra_float_digits`), and what issue #113's temporal
/// families added (`interval` under `IntervalStyle`). **Every future type
/// family in epic #123 hits this same wall and should extend this one
/// constant** rather than pinning a GUC at its own call site. Keeping them
/// in a single list is also what lets the non-pooled connect sites below,
/// and — since issue #246 — the walsender's own startup `options` (see
/// [`deterministic_text_output_options`]), stay in step with the pooled
/// connection: every one of them derives from or interpolates this same
/// text, so a sixth GUC added here is automatically pinned everywhere else
/// too, with nothing to remember to update in a second place.
///
/// # `TimeZone` joined this constant once the walsender could be pinned (issue #246)
///
/// `timestamptz_out` renders in the session's `TimeZone`, so pinning it to
/// `'UTC'` is the obvious way to give `timestamptz` the same text-stability
/// `DateStyle` gave `date`/`timestamp` — and issue #113 confirmed that
/// alone would work (Trellis owns every connection it opens, so "blast
/// radius on other sessions" was never the objection). It nonetheless
/// declined the pin, for a reason that survives that argument:
///
/// **Trellis renders `timestamptz` on two backends, and issue #113 could
/// only control one.** The CDC half of a value's life is rendered by the
/// type's output function running in the **walsender**, under the
/// walsender's GUCs — verified live by peeking one slot from two sessions
/// with different `timezone` settings and getting two different wall
/// clocks for one instant, on a server whose own default was a third zone.
/// `pgwire_replication::ReplicationConfig` v0.4 offered no way to send
/// startup runtime parameters or to `SET` on the replication connection, so
/// the walsender kept the server/database/role default no matter what this
/// constant said, and pinning `'UTC'` here alone would have traded the two
/// renderers' *accidental* agreement (both falling back to the same server
/// default) for a *guaranteed* disagreement on every server whose default
/// is not UTC — strictly worse than the status quo.
///
/// Issue #246 closed that gap: `pgwire-replication` 0.4.1 added
/// `ReplicationConfig::with_options`, which sends the same startup
/// `options` parameter `libpq`'s `options`/`PGOPTIONS` already send on a
/// normal connection, and PostgreSQL honors it on a replication connection
/// too. [`crate::intake::IntakeConfig::replication_config`] now passes
/// [`deterministic_text_output_options`] — this same constant, reparsed
/// into the `-c name=value` shape `options` expects — so the walsender pins
/// every one of these GUCs exactly like the pool does, and `TimeZone` can
/// finally join them without introducing the asymmetry #113 declined.
/// [`crate::temporal::is_bijective_under_text`]/
/// [`crate::temporal::is_render_consistent`] pick the story up from here —
/// `timestamptz` is admitted to the key/`MIN`/`MAX` roles as of this issue.
///
/// The same asymmetry was latent for the four pinned before `TimeZone`, and
/// the reason it did not bite is worth restating because it *was* the rule
/// for adding a fifth, back when only the pool could be pinned: each
/// pinned value was **output-identical to a stock server's default** (`ISO`
/// output, `hex`, shortest-round-trip floats, `postgres` interval style),
/// so the unpinned walsender agreed with the pinned pool unless an operator
/// had deliberately reconfigured the server — `TimeZone` was the one value
/// here with no such stock default to lean on, which is exactly why it
/// waited for the walsender to become pinnable too rather than joining
/// under that older, weaker rule. Now that the walsender is genuinely
/// pinned rather than merely agreeing by accident, that rule is retired:
/// any future GUC extending this constant is pinned symmetrically on both
/// backends by construction, and does not need to pass an
/// output-identical-to-stock-defaults test first.
///
/// `extra_float_digits` deserves a word, because `1` is already Postgres
/// 12+'s default and pinning a default can look like a no-op. It isn't: the
/// GUC is settable per-database and per-role, and the *old* default (`0`,
/// pre-12) means "round to `FLT_DIG`/`DBL_DIG` significant digits", which
/// loses information — `0.1::float8 + 0.2::float8` renders as `0.3` under
/// `0` and `0.30000000000000004` under `1`. A value that renders lossily on
/// one connection and exactly on another cannot round-trip through the
/// text-carried staging ring, and [`crate::float::render`] reproduces the
/// `>= 1` spelling specifically.
///
/// Note these are *output*-side settings. Input is a separate question and
/// is handled separately: `crate::defs::typed_literal` requires a literal's
/// text to be in a spelling that parses to the same value under **any**
/// `DateStyle` (leading-4-digit-year ISO is unambiguous), so a definition
/// installed against one server stays correct if read on another.
/// `IntervalStyle` joins them for issue #113 and is the clearest case of
/// all: `interval_out` renders the *same* value as `1 year 2 mons 3 days
/// 04:05:06` under `postgres`, `+1-2 +3 +4:05:06` under `sql_standard`,
/// `@ 1 year 2 mons 3 days 4 hours 5 mins 6 secs` under `postgres_verbose`
/// and `P1Y2M3DT4H5M6S` under `iso_8601` (all four read off a live server).
/// `crate::temporal::Interval::render` reproduces the `postgres` spelling
/// specifically, which is both Postgres's default and what an interval
/// `SUM` has to write back.
///
/// `TimeZone` joined the other four for issue #246, once the walsender
/// could be pinned too (see the "`TimeZone` joined this constant" section
/// above) — `timestamptz_out` renders in `'UTC'` on every connection
/// Trellis opens, pool and walsender alike, making it a bijection on the
/// instant exactly the way `DateStyle` makes `date_out` a bijection on the
/// day number.
///
/// `lc_monetary` joined for issue #672. `cash_out` spells a `money` value
/// in the session's monetary locale: `1234.56::money` is `$1,234.56` under
/// `C`, `£1,234.56` under `en_GB.UTF-8` and `kr.1.234,56` under
/// `en_DK.UTF-8`, and `cash_in` reads input under the same locale. A
/// `money` column is a passthrough value (`docs/type-support.md`), so an
/// operator's `ALTER DATABASE ... SET lc_monetary` used to change its
/// staged text. `'C'` is the value every libc has, and it renders the same
/// `$1,234.56` a server initialized with `C.UTF-8` (the test clusters,
/// issue #667) or `en_US.UTF-8` defaults to.
///
/// `money` stores an integer count of the monetary locale's smallest unit,
/// scaled by its `frac_digits` at *input* time, so under a database whose
/// `lc_monetary` has `frac_digits` other than 2 (`ja_JP`'s is 0) the text
/// Trellis stages (`$12.35` for a value the application wrote as `¥1,235`)
/// is not what the application reads back. It is still a function of the
/// stored value alone, which is all this constant promises.
///
/// # `standard_conforming_strings`, the one *input* setting pinned here
///
/// Everything else here pins how a value is *rendered*. This one pins how
/// SQL text Trellis writes is *parsed*: [`quote_literal`],
/// [`crate::defs::typed_literal::render_sql`] and every `Expr::StringLiteral`
/// renderer escape a literal by doubling `'` and nothing else, which is
/// correct only when `standard_conforming_strings` is `on`. Under `off` a
/// backslash in an ordinary `'...'` literal starts an escape, so a column
/// named `a\b` quotes as the `jsonb_build_object` key `a<backspace>`, one
/// ending in `\` swallows the closing quote, and the canonical `bytea`
/// literal `'\x0102'` reads as a control character followed by `02`. `on`
/// has been the server default since Postgres 9.1, but an operator's
/// `ALTER DATABASE ... SET standard_conforming_strings = off` still reaches
/// every session that doesn't set it. It sits in this constant (issue #672)
/// because this is the one list every Trellis session applies; on the
/// walsender, which parses no SQL literal Trellis writes, it is harmless.
///
/// # Output settings deliberately *not* pinned
///
/// The rest of Postgres's locale and formatting settings were audited for
/// issue #672. None of them changes what a type's output function (the
/// `::text` cast, `pgoutput`'s decoding, `to_jsonb`) prints:
///
/// * `lc_numeric` and `lc_time` only feed `to_char`'s locale patterns (`D`,
///   `G`, `L`, the `TM` prefix). `numeric_out`, `float8out` and the
///   date/time output functions ignore them. Trellis never calls `to_char`,
///   and its grammar can't express it.
/// * `lc_messages` changes error message text only. Trellis classifies
///   errors by SQLSTATE, never by message.
/// * `lc_collate`/`lc_ctype` are fixed per database, not settable per
///   session. Ordering is handled with explicit `collate "C"` and the
///   deterministic-collation checks on key columns (issues #590, #638).
/// * `client_encoding` is fixed to `UTF8` by `tokio-postgres` itself.
/// * `xmloption`, `xmlbinary` and `timezone_abbreviations` change how
///   *input* is parsed (or, for `xmlbinary`, what the `xml*` constructor
///   functions build), not how a stored value is rendered, and no SQL
///   Trellis writes depends on them.
/// * `escape_string_warning` and `backslash_quote` only matter when
///   `standard_conforming_strings` is `off`, which it never is here.
///
/// [`deterministic_text_output_options`] parses this string back into the
/// `-c name=value` shape `pgwire_replication::ReplicationConfig::with_options`
/// expects, so both forms come from one source of truth — see that
/// function's own doc comment for why it parses rather than hand-duplicating
/// a second `-c`-shaped list here.
pub(crate) const DETERMINISTIC_TEXT_OUTPUT_GUCS: &str = "set datestyle to 'ISO, YMD'; \
     set bytea_output to 'hex'; set extra_float_digits to 1; set intervalstyle to 'postgres'; \
     set timezone to 'UTC'; set lc_monetary to 'C'; set standard_conforming_strings to 'on'";

/// [`DETERMINISTIC_TEXT_OUTPUT_GUCS`], reparsed into the Postgres startup
/// `options` parameter's `-c name=value -c name2=value2 ...` shape —
/// exactly what `libpq`'s own `options` connection parameter (or
/// `PGOPTIONS`) sends, and what PostgreSQL honors on a **replication**
/// connection too, not just a normal one.
///
/// This exists so [`crate::intake::IntakeConfig::replication_config`] can
/// pin the walsender to the same GUCs [`session_bootstrap`] pins the pool
/// to, **without hand-duplicating the GUC list into a second hardcoded
/// place** — issue #246 is precisely the bug that a second, independently
/// maintained list invites: add a GUC to one, forget the other, and the
/// pool/walsender renderings silently diverge again. Parsing
/// [`DETERMINISTIC_TEXT_OUTPUT_GUCS`]'s own `set X to 'Y'; ...` text instead
/// means a GUC added to that constant is automatically pinned on the
/// walsender the next time this function runs — there is no second list to
/// forget.
///
/// Each `set <guc> to <value>;` clause becomes one `-c <guc>=<value>` token.
/// Postgres's own `options` parser (`pg_split_opts`) splits on whitespace
/// and treats a backslash as an escape for the following character, so any
/// space *within* a value (`'ISO, YMD'`'s) is backslash-escaped rather than
/// passed through raw — a bare space there would otherwise be read as a
/// second, malformed `-c` token instead of part of this one's value.
/// [`with_options_matches_a_hand_written_expectation`] pins the exact
/// output for the constant as it stands today, so a future edit to
/// [`DETERMINISTIC_TEXT_OUTPUT_GUCS`] that doesn't parse the way this
/// function expects fails loudly in CI instead of silently shipping a
/// walsender that pins something other than what it meant to.
///
/// Escaping here only covers spaces (`' '` -> `'\ '`); it does not escape a
/// literal backslash in a value, so a future GUC value containing one would
/// be mangled by `pg_split_opts`'s own backslash-as-escape-character rule.
/// None of today's values contain a backslash, so this is a latent gap, not
/// a live bug — worth fixing if that ever changes.
// Only intake used this; C8 deletes it (issue #622).
#[allow(dead_code)]
pub(crate) fn deterministic_text_output_options() -> String {
    DETERMINISTIC_TEXT_OUTPUT_GUCS
        .split(';')
        .map(str::trim)
        .filter(|clause| !clause.is_empty())
        .map(|clause| {
            let rest = clause.strip_prefix("set ").unwrap_or_else(|| {
                panic!(
                    "DETERMINISTIC_TEXT_OUTPUT_GUCS clause {clause:?} does not start with \"set \""
                )
            });
            let (name, value) = rest.split_once(" to ").unwrap_or_else(|| {
                panic!("DETERMINISTIC_TEXT_OUTPUT_GUCS clause {clause:?} has no \" to \"")
            });
            let value = value.trim().trim_matches('\'');
            format!("-c {name}={}", value.replace(' ', "\\ "))
        })
        .collect::<Vec<_>>()
        .join(" ")
}

/// Runs once per physical connection, right after it's established and
/// before it's returned to any caller.
///
/// Pins `search_path` to `schema` (first, so unqualified references to
/// Trellis's own objects always resolve there) followed by `target_schema`
/// (so unqualified reads/writes against a transform target table resolve
/// even when it lives outside both `schema` and `public`) and `public`
/// (Postgres's own default, kept last as a fallback for anything that
/// depends on it today), plus [`DETERMINISTIC_TEXT_OUTPUT_GUCS`], the
/// server-side TCP keepalives ([`tcp_keepalive_gucs`], without the user
/// timeout: see [`DeadPeerDetection::KeepalivesOnly`]) and the session's
/// `lock_timeout` cap ([`crate::locks::session_lock_timeout_sql`], ADR-0002
/// I7). Beyond those, this
/// is the seam intake will use to enforce `synchronous_commit = on`.
async fn session_bootstrap(
    client: &mut tokio_postgres::Client,
    schema: &str,
    target_schema: &str,
) -> Result<(), tokio_postgres::Error> {
    client
        .batch_execute(&format!(
            "set search_path to {}, {}, public; {DETERMINISTIC_TEXT_OUTPUT_GUCS}; {}; {}",
            quote_ident(schema),
            quote_ident(target_schema),
            tcp_keepalive_gucs(DeadPeerDetection::KeepalivesOnly),
            crate::locks::session_lock_timeout_sql()
        ))
        .await
}

// ---------------------------------------------------------------------
// Dead-peer detection: TCP keepalives on both ends (issue #364)
// ---------------------------------------------------------------------

/// How long a connection sits with no traffic before the first keepalive
/// probe goes out. Applied on both ends; see [`tcp_keepalive_gucs`] for the
/// server's half and [`with_client_keepalives`] for the client's.
///
/// # Why every Trellis connection gets these, and why these numbers
///
/// Postgres frees a session's locks only when the backend learns its
/// client is gone. A process that exits closes its socket and the server
/// notices at once. A network partition closes nothing: the backend sits
/// idle on a socket whose peer can no longer answer until the kernel gives
/// up on it, which with Linux defaults (`tcp_keepalive_time` 7200s, then 9
/// probes 75s apart) is about 2h11m. The replication slot doesn't have
/// this problem, because `wal_sender_timeout` (60s by default) ends the
/// walsender on its own clock.
///
/// The producer session is where this hurt (issue #364): its backend holds
/// the instance's producer-singleton advisory lock
/// ([`crate::staging::session::producer_singleton_lock_key`]), so a
/// partitioned producer kept every restart refused with
/// `ProducerAlreadyRunning` for the whole two hours, staging nothing. But
/// any connection partitioned mid-transaction pins its row and advisory
/// locks the same way, so the pool and every dedicated connection get the
/// keepalives too. Only the dedicated ones also get [`TCP_USER_TIMEOUT`];
/// see [`DeadPeerDetection`] for why the pool doesn't.
///
/// Idle 10s, then up to 3 probes 5s apart, is a dead peer declared about
/// 25s after it last answered ([`TCP_USER_TIMEOUT`] matches that). That's
/// inside `wal_sender_timeout`'s default 60s, so after a partition the
/// producer lock is free before the slot is, and a restarting intake waits
/// on the slot rather than on the lock. The cost is one probe per idle
/// connection per 10s, and a connection is only given up on after it has
/// missed three probes in a row. A live peer's kernel answers probes even
/// when its application is busy or stalled, so keepalives never end a
/// healthy connection.
///
/// # Explicit settings win
///
/// There's no Trellis knob for these, but an operator's own settings are
/// respected, all-or-nothing per end (mixing schedules can leave a user
/// timeout shorter than the probing budget, which ends a connection on a
/// single lost probe):
///
/// - **Client end:** if the DSN sets any of `keepalives`, `keepalives_idle`,
///   `keepalives_interval`, `keepalives_retries` or `tcp_user_timeout`,
///   the client socket is left exactly as the DSN says.
/// - **Server end:** if any of the four `tcp_*` GUCs was set for this
///   connection specifically (the DSN's `options`/`PGOPTIONS`, or `ALTER
///   ROLE`/`ALTER DATABASE ... SET`), none of them is touched. A
///   server-wide value from `postgresql.conf` or the command line is
///   overridden, since it wasn't chosen with this client in mind.
const TCP_KEEPALIVE_IDLE: Duration = Duration::from_secs(10);

/// The gap between keepalive probes once [`TCP_KEEPALIVE_IDLE`] has passed.
const TCP_KEEPALIVE_INTERVAL: Duration = Duration::from_secs(5);

/// Unanswered probes before the connection is declared dead.
const TCP_KEEPALIVE_COUNT: u32 = 3;

/// How long data sent on the connection may go unacknowledged before the
/// kernel drops it: [`TCP_KEEPALIVE_IDLE`] + [`TCP_KEEPALIVE_COUNT`] ×
/// [`TCP_KEEPALIVE_INTERVAL`], the same 25s.
///
/// The keepalives alone don't cover a busy connection. Linux sends no
/// keepalive probe while the socket has unacknowledged data in flight, so a
/// partition that lands just after one end sent something is left to the
/// retransmission timeout instead, about 15 minutes under the default
/// `tcp_retries2`. `TCP_USER_TIMEOUT` bounds that case too, and when it's
/// set Linux also uses it in place of the probe count to decide when
/// keepalives have failed, which is why it matches the keepalive total.
///
/// Unlike the keepalives, it can end a healthy connection. See
/// [`DeadPeerDetection::KeepalivesOnly`].
const TCP_USER_TIMEOUT: Duration = Duration::from_secs(25);

/// `tokio_postgres`'s default keepalive idle time. A parsed DSN reporting
/// anything else set `keepalives_idle` itself.
const TOKIO_POSTGRES_DEFAULT_KEEPALIVE_IDLE: Duration = Duration::from_secs(2 * 60 * 60);

/// How much of the dead-peer schedule a connection gets.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum DeadPeerDetection {
    /// Keepalives only, for the pool. [`TCP_USER_TIMEOUT`] also fires on a
    /// live peer that stops reading: once the reader's receive buffer is
    /// full it advertises a zero window, and Linux (5.11+) aborts a socket
    /// whose window has stayed shut for the user timeout even though the
    /// peer keeps acknowledging the window probes. On the server end that
    /// kills a pooled connection whose caller streams a large result
    /// (`query_raw`, `copy_out`) and pauses more than 25s between rows. The
    /// pool is handed to embedders ([`crate::Trellis::pool`]) and Trellis
    /// can't bound how they read, so it goes without. A partition that
    /// strikes a pooled connection with data in flight is then noticed at
    /// the retransmission timeout (~15 minutes) as before; the idle case,
    /// the usual one, still drops from about 2h to 25s.
    KeepalivesOnly,
    /// Keepalives plus [`TCP_USER_TIMEOUT`], for Trellis's own dedicated
    /// connections (producer session, maintenance loop, heartbeat daemon,
    /// wake listener). Trellis runs every query on them, reads every result
    /// eagerly and never pipelines a bulk write, so neither end sits on a
    /// shut window.
    KeepalivesAndUserTimeout,
}

impl DeadPeerDetection {
    fn user_timeout(self) -> Option<Duration> {
        match self {
            Self::KeepalivesOnly => None,
            Self::KeepalivesAndUserTimeout => Some(TCP_USER_TIMEOUT),
        }
    }
}

/// The statement that makes the **server** probe this connection's client
/// and drop it once the client stops answering. This is the half that
/// frees a partitioned session's locks. They're user-settable GUCs, so no
/// privilege is needed. It leaves all four alone if any was set for this
/// connection specifically (see [`TCP_KEEPALIVE_IDLE`], "Explicit settings
/// win"); `pg_settings.source` tells those apart from a server-wide default.
///
/// Postgres ignores them on a unix-socket connection, which has no network
/// to partition, and reads them back as `0` there. On a server without
/// `TCP_USER_TIMEOUT` support (macOS) it logs that it can't set that one,
/// and the statement still succeeds.
pub(crate) fn tcp_keepalive_gucs(detection: DeadPeerDetection) -> String {
    let mut settings = vec![
        (
            "tcp_keepalives_idle",
            TCP_KEEPALIVE_IDLE.as_secs().to_string(),
        ),
        (
            "tcp_keepalives_interval",
            TCP_KEEPALIVE_INTERVAL.as_secs().to_string(),
        ),
        ("tcp_keepalives_count", TCP_KEEPALIVE_COUNT.to_string()),
    ];
    if let Some(user_timeout) = detection.user_timeout() {
        settings.push(("tcp_user_timeout", user_timeout.as_millis().to_string()));
    }
    let values = settings
        .iter()
        .map(|(name, value)| format!("('{name}', '{value}')"))
        .collect::<Vec<_>>()
        .join(", ");
    format!(
        "select pg_catalog.set_config(w.name, w.setting, false) \
         from (values {values}) as w(name, setting) \
         where not exists (\
             select from pg_catalog.pg_settings s \
             where s.name in ('tcp_keepalives_idle', 'tcp_keepalives_interval', \
                              'tcp_keepalives_count', 'tcp_user_timeout') \
               and s.source in ('client', 'database', 'user', 'database user'))"
    )
}

/// The **client's** half of [`tcp_keepalive_gucs`]: `tokio_postgres`'s own
/// keepalives, which default to 2h idle with OS-default probing. Leaves
/// `config` untouched if its DSN set any keepalive option itself (see
/// [`TCP_KEEPALIVE_IDLE`], "Explicit settings win").
///
/// Freeing the server's locks doesn't need this, but noticing the partition
/// does. A client waiting on a reply from a partitioned server sees nothing
/// arrive and, without its own probes, keeps waiting for the same two
/// hours. On the producer that's intake stuck mid-append instead of
/// failing, so the supervisor never restarts it. With these, the wait
/// fails after about 25s and the restart path takes over.
///
/// `tokio_postgres` sets the user timeout on Linux only and ignores it
/// elsewhere; the keepalive options work on Linux, macOS and Windows.
pub(crate) fn with_client_keepalives(
    config: &mut tokio_postgres::Config,
    detection: DeadPeerDetection,
) {
    let dsn_set_any = !config.get_keepalives()
        || config.get_keepalives_idle() != TOKIO_POSTGRES_DEFAULT_KEEPALIVE_IDLE
        || config.get_keepalives_interval().is_some()
        || config.get_keepalives_retries().is_some()
        || config.get_tcp_user_timeout().is_some();
    if dsn_set_any {
        return;
    }
    config
        .keepalives(true)
        .keepalives_idle(TCP_KEEPALIVE_IDLE)
        .keepalives_interval(TCP_KEEPALIVE_INTERVAL)
        .keepalives_retries(TCP_KEEPALIVE_COUNT);
    if let Some(user_timeout) = detection.user_timeout() {
        config.tcp_user_timeout(user_timeout);
    }
}

/// Opens a standalone (non-pooled) connection to `dsn` with the client-side
/// keepalives and user timeout set. The caller spawns or polls the returned
/// connection future, then runs [`dedicated_session_setup`] (or its own
/// superset of it) before using the client.
///
/// A DSN that doesn't parse is an [`Error::Config`] from
/// [`crate::config::parse_dsn`], never a `tokio_postgres::Error`, whose
/// `Debug` can quote part of the password (issue #608).
pub(crate) async fn connect_dedicated(
    dsn: &str,
) -> Result<
    (
        tokio_postgres::Client,
        tokio_postgres::Connection<tokio_postgres::Socket, tokio_postgres::tls::NoTlsStream>,
    ),
    Error,
> {
    let mut config = crate::config::parse_dsn(dsn)?;
    with_client_keepalives(&mut config, DeadPeerDetection::KeepalivesAndUserTimeout);
    Ok(config.connect(NoTls).await?)
}

/// The session setup a dedicated connection runs straight after
/// [`connect_dedicated`], mirroring what [`session_bootstrap`] does for a
/// pooled one: `search_path` pinned to `schema` then `public`,
/// [`DETERMINISTIC_TEXT_OUTPUT_GUCS`], [`tcp_keepalive_gucs`] with the
/// user timeout, and the `lock_timeout` cap
/// ([`crate::locks::session_lock_timeout_sql`]).
pub(crate) fn dedicated_session_setup(schema: &str) -> String {
    format!(
        "set search_path to {}, public; {DETERMINISTIC_TEXT_OUTPUT_GUCS}; {}; {}",
        quote_ident(schema),
        tcp_keepalive_gucs(DeadPeerDetection::KeepalivesAndUserTimeout),
        crate::locks::session_lock_timeout_sql()
    )
}

/// Quotes a Postgres identifier for safe interpolation into SQL text.
///
/// `config.schema` is operator-supplied (an environment variable), not
/// end-user input, but it still flows into SQL as text rather than a bind
/// parameter (identifiers can't be bound), so it's quoted defensively.
pub(crate) fn quote_ident(ident: &str) -> String {
    format!("\"{}\"", ident.replace('"', "\"\""))
}

/// Quotes a plain string as a Postgres string *literal* for safe
/// interpolation into SQL text — [`quote_ident`]'s counterpart for a value
/// rather than an identifier (doubling embedded `'` characters, the
/// `standard_conforming_strings = on` escaping that
/// [`DETERMINISTIC_TEXT_OUTPUT_GUCS`] pins on every connection this crate
/// opens, so a backslash needs no escaping).
///
/// Issue #248: a column name can't be bound as an ordinary query parameter
/// (it isn't a value), but it still needs to appear as a `jsonb_build_object`
/// *key* — a text literal, not a bare identifier — when
/// `staging::apply`/`staging::quarantine` build an explicit per-column
/// `jsonb_build_object('<col>', <col>::text, ...)` in place of `to_jsonb(t.*)`
/// (see `staging::apply::row_as_text_jsonb_sql`). The column names it quotes
/// there come from live `pg_catalog` introspection, not end-user input, but
/// they're quoted defensively anyway, matching [`quote_ident`]'s own stance
/// on `config.schema`.
pub(crate) fn quote_literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    #[test]
    fn quote_ident_escapes_embedded_quotes() {
        assert_eq!(quote_ident("trellis"), "\"trellis\"");
        assert_eq!(quote_ident("weird\"schema"), "\"weird\"\"schema\"");
    }

    /// Issue #364: the client probes on the same schedule the server does,
    /// with the user timeout only on dedicated connections. The server half
    /// is checked against a live TCP session in `tests/tcp_keepalives.rs`;
    /// `tokio_postgres` exposes no way to read the client's socket options
    /// back, so this checks the config instead.
    #[test]
    fn client_keepalives_match_the_server_schedule() {
        for (detection, user_timeout) in [
            (DeadPeerDetection::KeepalivesOnly, None),
            (
                DeadPeerDetection::KeepalivesAndUserTimeout,
                Some(&TCP_USER_TIMEOUT),
            ),
        ] {
            let mut config = tokio_postgres::Config::from_str("host=127.0.0.1").expect("parse dsn");
            with_client_keepalives(&mut config, detection);
            assert!(config.get_keepalives(), "{detection:?}");
            assert_eq!(config.get_keepalives_idle(), TCP_KEEPALIVE_IDLE);
            assert_eq!(
                config.get_keepalives_interval(),
                Some(TCP_KEEPALIVE_INTERVAL)
            );
            assert_eq!(config.get_keepalives_retries(), Some(TCP_KEEPALIVE_COUNT));
            assert_eq!(config.get_tcp_user_timeout(), user_timeout, "{detection:?}");
        }
        // The user timeout replaces the probe count on Linux once set, so
        // it has to equal the probing budget or it changes the schedule.
        assert_eq!(
            TCP_USER_TIMEOUT,
            TCP_KEEPALIVE_IDLE + TCP_KEEPALIVE_INTERVAL * TCP_KEEPALIVE_COUNT
        );
        // The "did the DSN set it?" check compares against this default.
        assert_eq!(
            tokio_postgres::Config::new().get_keepalives_idle(),
            TOKIO_POSTGRES_DEFAULT_KEEPALIVE_IDLE
        );
    }

    /// A DSN that sets any client keepalive option keeps its own settings,
    /// all of them: Trellis doesn't fill in the rest around it.
    #[test]
    fn client_keepalives_leave_a_dsn_that_sets_any_alone() {
        for dsn in [
            "host=127.0.0.1 keepalives=0",
            "host=127.0.0.1 keepalives_idle=60",
            "host=127.0.0.1 keepalives_interval=20",
            "host=127.0.0.1 keepalives_retries=7",
            "host=127.0.0.1 tcp_user_timeout=90",
            "postgresql://postgres@127.0.0.1/db?keepalives_idle=60",
        ] {
            let parsed = tokio_postgres::Config::from_str(dsn).expect("parse dsn");
            let mut config = parsed.clone();
            with_client_keepalives(&mut config, DeadPeerDetection::KeepalivesAndUserTimeout);
            assert_eq!(config.get_keepalives(), parsed.get_keepalives(), "{dsn}");
            assert_eq!(
                config.get_keepalives_idle(),
                parsed.get_keepalives_idle(),
                "{dsn}"
            );
            assert_eq!(
                config.get_keepalives_interval(),
                parsed.get_keepalives_interval(),
                "{dsn}"
            );
            assert_eq!(
                config.get_keepalives_retries(),
                parsed.get_keepalives_retries(),
                "{dsn}"
            );
            assert_eq!(
                config.get_tcp_user_timeout(),
                parsed.get_tcp_user_timeout(),
                "{dsn}"
            );
        }
    }

    #[test]
    fn quote_literal_escapes_embedded_quotes() {
        assert_eq!(quote_literal("created_at"), "'created_at'");
        assert_eq!(quote_literal("weird'column"), "'weird''column'");
    }

    /// Pins the exact `-c ...` string [`deterministic_text_output_options`]
    /// produces from today's [`DETERMINISTIC_TEXT_OUTPUT_GUCS`] — a change to
    /// either one that the parser doesn't expect (a new clause shape, a
    /// value containing a character the space-escaping doesn't handle) fails
    /// this test loudly instead of shipping a walsender silently pinned to
    /// something other than what [`DETERMINISTIC_TEXT_OUTPUT_GUCS`] says.
    #[test]
    fn with_options_matches_a_hand_written_expectation() {
        assert_eq!(
            deterministic_text_output_options(),
            "-c datestyle=ISO,\\ YMD -c bytea_output=hex -c extra_float_digits=1 \
             -c intervalstyle=postgres -c timezone=UTC -c lc_monetary=C \
             -c standard_conforming_strings=on"
        );
    }

    /// The two representations can never silently drift apart: this walks
    /// [`DETERMINISTIC_TEXT_OUTPUT_GUCS`]'s own clause count and asserts
    /// [`deterministic_text_output_options`] produced exactly one `-c` token
    /// per clause — a change that adds a sixth GUC to the SQL string but
    /// breaks the parser (rather than merely producing the wrong value,
    /// which the test above would already catch) still fails here.
    #[test]
    fn every_pinned_guc_produces_exactly_one_option_token() {
        let clause_count = DETERMINISTIC_TEXT_OUTPUT_GUCS
            .split(';')
            .map(str::trim)
            .filter(|c| !c.is_empty())
            .count();
        let token_count = deterministic_text_output_options().split(" -c ").count();
        assert_eq!(clause_count, token_count);
        assert_eq!(
            clause_count, 7,
            "expected seven pinned GUCs as of issue #672"
        );
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

    /// Issue #608: a DSN that doesn't parse gets a fixed message, not
    /// tokio_postgres's error, which can name part of the password.
    #[test]
    fn unparsable_dsn_error_carries_no_password_fragment() {
        for dsn in crate::config::malformed_dsn_fixtures::DSNS {
            let config = Config::from_dsn(dsn).expect("schema is valid; only the DSN is bogus");
            let err = Pool::new(&config).expect_err("the DSN doesn't parse");
            assert!(matches!(err, Error::Config(_)), "{err:?}");
            crate::config::malformed_dsn_fixtures::assert_no_password_fragment(dsn, &err);
        }
    }

    /// Issue #608: the dedicated connections (producer session, maintenance
    /// loop, wake listener, liveness daemon) parse the DSN themselves.
    #[tokio::test]
    async fn dedicated_connection_dsn_error_carries_no_password_fragment() {
        for dsn in crate::config::malformed_dsn_fixtures::DSNS {
            let err = connect_dedicated(dsn)
                .await
                .map(|_| ())
                .expect_err("the DSN doesn't parse");
            crate::config::malformed_dsn_fixtures::assert_no_password_fragment(dsn, &err);
        }
    }

    /// Issue #672: `money` renders the same on every kind of session Trellis
    /// opens itself (pooled, unpooled, dedicated) even when the database's
    /// own `lc_monetary` default says otherwise. The walsender, the fourth
    /// kind, is covered with the other pins in `tests/intake_guc_pinning.rs`.
    #[tokio::test]
    async fn money_renders_the_same_on_every_session_kind_under_a_hostile_lc_monetary() {
        let cluster = testkit::TestCluster::start();
        let db = cluster.create_isolated_database().await;
        let (raw, connection) = tokio_postgres::connect(db.dsn(), NoTls)
            .await
            .expect("connect unpinned");
        tokio::spawn(async move {
            let _ = connection.await;
        });
        let hostile = testkit::locale::hostile_lc_monetary(&raw).await;
        raw.batch_execute(&format!(
            "alter database {} set lc_monetary to '{hostile}'",
            quote_ident(db.name())
        ))
        .await
        .expect("apply hostile database lc_monetary");

        const PROBE: &str = "select 1234.56::money::text, (-1234.56)::money::text";
        let expected = (
            testkit::locale::PINNED_MONEY_TEXT.to_string(),
            "-$1,234.56".to_string(),
        );
        async fn render(client: &tokio_postgres::Client) -> (String, String) {
            let row = client.query_one(PROBE, &[]).await.expect("render money");
            (row.get(0), row.get(1))
        }

        // Control: a session started after the `ALTER DATABASE` that pins
        // nothing sees the hostile locale, or this test proves nothing.
        let (unpinned, connection) = tokio_postgres::connect(db.dsn(), NoTls)
            .await
            .expect("connect unpinned");
        tokio::spawn(async move {
            let _ = connection.await;
        });
        assert_ne!(
            render(&unpinned).await,
            expected,
            "hostile lc_monetary {hostile} must be in effect"
        );

        // A fresh pool: `db.pool` may hold connections opened before the
        // `ALTER DATABASE`.
        let config = Config::from_dsn(db.dsn().to_string()).expect("valid dsn");
        let pool = Pool::new(&config).expect("pool");
        let pooled = pool.get().await.expect("pooled connection");
        assert_eq!(render(&pooled).await, expected, "pooled");

        let unpooled = pool.connect_unpooled().await.expect("unpooled connection");
        assert_eq!(render(&unpooled).await, expected, "unpooled");

        let (dedicated, connection) = connect_dedicated(db.dsn()).await.expect("dedicated");
        tokio::spawn(async move {
            let _ = connection.await;
        });
        dedicated
            .batch_execute(&dedicated_session_setup(crate::config::DEFAULT_SCHEMA))
            .await
            .expect("dedicated session setup");
        assert_eq!(render(&dedicated).await, expected, "dedicated");
    }

    /// Issue #672 review: the SQL Trellis writes escapes a string literal by
    /// doubling `'` only ([`quote_literal`],
    /// [`crate::defs::typed_literal::render_sql`]), which assumes
    /// `standard_conforming_strings = on`. A database default of `off` must
    /// not reach any session Trellis opens itself: here a column named
    /// `a\b` would otherwise become the image key `a<backspace>`, one named
    /// `trail\` would swallow its closing quote, and the canonical `bytea`
    /// literal `'\x0102'` would read as three other bytes.
    #[tokio::test]
    async fn backslashes_in_generated_literals_survive_a_database_default_of_nonstandard_strings() {
        use crate::defs::ast::ValueType;
        use crate::defs::pg_type::PgType;

        let cluster = testkit::TestCluster::start();
        let db = cluster.create_isolated_database().await;
        let (raw, connection) = tokio_postgres::connect(db.dsn(), NoTls)
            .await
            .expect("connect unpinned");
        tokio::spawn(async move {
            let _ = connection.await;
        });
        raw.batch_execute(&format!(
            "create table t (id int primary key, \"a\\b\" text, \"trail\\\" text); \
             insert into t values (1, 'x', 'y'); \
             alter database {} set standard_conforming_strings to off",
            quote_ident(db.name())
        ))
        .await
        .expect("seed the table and apply a hostile database default");

        let probe = |columns: &[&str]| {
            let columns: Vec<String> = columns.iter().map(|c| c.to_string()).collect();
            format!(
                "select array_agg(e.key || '=' || e.value order by e.key collate \"C\"), \
                        ({})::text \
                 from t, jsonb_each_text({}) e",
                crate::defs::typed_literal::render_sql(ValueType::Other(PgType::Bytea), "\\x0102"),
                crate::staging::apply::row_as_text_jsonb_sql("t", &columns)
            )
        };
        async fn render(client: &tokio_postgres::Client, sql: &str) -> (Vec<String>, String) {
            let row = client.query_one(sql, &[]).await.expect("render the probe");
            (row.get(0), row.get(1))
        }

        // Control: an unpinned session started after the `ALTER DATABASE`
        // really does read backslashes as escapes. `trail\` is left out of
        // this probe because under `off` it doesn't parse at all.
        let (unpinned, connection) = tokio_postgres::connect(db.dsn(), NoTls)
            .await
            .expect("connect unpinned");
        tokio::spawn(async move {
            let _ = connection.await;
        });
        let (keys, bytea) = render(&unpinned, &probe(&["id", "a\\b"])).await;
        assert_ne!(
            keys,
            ["a\\b=x", "id=1"],
            "hostile default must be in effect"
        );
        assert_ne!(bytea, "\\x0102", "hostile default must be in effect");

        let full = probe(&["id", "a\\b", "trail\\"]);
        let expected = (
            vec![
                "a\\b=x".to_string(),
                "id=1".to_string(),
                "trail\\=y".to_string(),
            ],
            "\\x0102".to_string(),
        );

        let config = Config::from_dsn(db.dsn().to_string()).expect("valid dsn");
        let pool = Pool::new(&config).expect("pool");
        let pooled = pool.get().await.expect("pooled connection");
        assert_eq!(render(&pooled, &full).await, expected, "pooled");

        let unpooled = pool.connect_unpooled().await.expect("unpooled connection");
        assert_eq!(render(&unpooled, &full).await, expected, "unpooled");

        let (dedicated, connection) = connect_dedicated(db.dsn()).await.expect("dedicated");
        tokio::spawn(async move {
            let _ = connection.await;
        });
        dedicated
            .batch_execute(&dedicated_session_setup(crate::config::DEFAULT_SCHEMA))
            .await
            .expect("dedicated session setup");
        assert_eq!(render(&dedicated, &full).await, expected, "dedicated");
    }

    /// Issue #591: `Pool`'s derived `Debug` reaches the DSN only through
    /// `deadpool_postgres::Manager`'s `tokio_postgres::Config`, whose own
    /// `Debug` masks the password. This pins that, so a dependency bump
    /// that changes it fails here rather than in someone's logs.
    #[test]
    fn pool_debug_never_prints_the_password() {
        let config = Config::from_dsn("postgresql://alice:s3cret@db.example.com/app")
            .expect("schema is valid");
        let pool = Pool::new(&config).expect("the DSN parses; no I/O happens here");
        let printed = format!("{pool:?}");
        assert!(!printed.contains("s3cret"), "leaked: {printed}");
        assert!(printed.contains("db.example.com"), "{printed}");
    }
}
