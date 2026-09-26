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
use std::str::FromStr;
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
    /// A copy of the [`Config::schema`] this pool was built from — see
    /// [`Pool::schema`].
    schema: String,
    /// A copy of the [`Config::target_schema`] this pool was built from —
    /// see [`Pool::target_schema`]. Only the test-fixture registration
    /// entry points read it, so it is compiled out of production builds.
    #[cfg(any(test, feature = "test-util"))]
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
        let mut pg_config = tokio_postgres::Config::from_str(config.dsn())
            .map_err(|err| Error::Config(format!("invalid database connection string: {err}")))?;
        with_client_keepalives(&mut pg_config);

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
            schema: config.schema().to_string(),
            #[cfg(any(test, feature = "test-util"))]
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
/// [`deterministic_text_output_options`] parses this string back into the
/// `-c name=value` shape `pgwire_replication::ReplicationConfig::with_options`
/// expects, so both forms come from one source of truth — see that
/// function's own doc comment for why it parses rather than hand-duplicating
/// a second `-c`-shaped list here.
pub(crate) const DETERMINISTIC_TEXT_OUTPUT_GUCS: &str = "set datestyle to 'ISO, YMD'; \
     set bytea_output to 'hex'; set extra_float_digits to 1; set intervalstyle to 'postgres'; \
     set timezone to 'UTC'";

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
/// depends on it today), plus [`DETERMINISTIC_TEXT_OUTPUT_GUCS`] and the
/// server-side TCP keepalives ([`tcp_keepalive_gucs`]). Beyond those, this
/// is the seam intake will use to enforce `synchronous_commit = on`.
async fn session_bootstrap(
    client: &mut tokio_postgres::Client,
    schema: &str,
    target_schema: &str,
) -> Result<(), tokio_postgres::Error> {
    client
        .batch_execute(&format!(
            "set search_path to {}, {}, public; {DETERMINISTIC_TEXT_OUTPUT_GUCS}; {}",
            quote_ident(schema),
            quote_ident(target_schema),
            tcp_keepalive_gucs()
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
/// same settings.
///
/// Idle 10s, then up to 3 probes 5s apart, is a dead peer declared about
/// 25s after it last answered ([`TCP_USER_TIMEOUT`] matches that). That's
/// inside `wal_sender_timeout`'s default 60s, so after a partition the
/// producer lock is free before the slot is, and a restarting intake waits
/// on the slot rather than on the lock. The cost is one probe per idle
/// connection per 10s, and a connection is only given up on after it has
/// missed three probes in a row.
///
/// Not configurable. These settings override any `keepalives*` or
/// `tcp_user_timeout` in the DSN, and any server, database or role default
/// for the `tcp_*` GUCs.
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
/// partition that lands just after the server sent a reply (on a producer
/// streaming changes, the likely case) is left to the retransmission
/// timeout instead, about 15 minutes under the default `tcp_retries2`.
/// `TCP_USER_TIMEOUT` bounds that case too, and when it's set Linux also
/// uses it in place of the probe count to decide when keepalives have
/// failed, which is why it matches the keepalive total.
const TCP_USER_TIMEOUT: Duration = Duration::from_secs(25);

/// The `set` statements that make the **server** probe this connection's
/// client and drop it once the client stops answering. This is the half
/// that frees a partitioned session's locks. They're user-settable GUCs, so
/// no privilege is needed.
///
/// Postgres ignores them on a unix-socket connection, which has no network
/// to partition, and reads them back as `0` there. On a server without
/// `TCP_USER_TIMEOUT`/`TCP_KEEPCNT` support it logs that it can't set that
/// one, and the statement still succeeds.
pub(crate) fn tcp_keepalive_gucs() -> String {
    format!(
        "set tcp_keepalives_idle to {}; set tcp_keepalives_interval to {}; \
         set tcp_keepalives_count to {TCP_KEEPALIVE_COUNT}; set tcp_user_timeout to {}",
        TCP_KEEPALIVE_IDLE.as_secs(),
        TCP_KEEPALIVE_INTERVAL.as_secs(),
        TCP_USER_TIMEOUT.as_millis()
    )
}

/// The **client's** half of [`tcp_keepalive_gucs`]: `tokio_postgres`'s own
/// keepalives, which default to 2h idle with OS-default probing.
///
/// Freeing the server's locks doesn't need this, but noticing the partition
/// does. A client waiting on a reply from a partitioned server sees nothing
/// arrive and, without its own probes, keeps waiting for the same two
/// hours. On the producer that's intake stuck mid-append instead of
/// failing, so the supervisor never restarts it. With these, the wait
/// fails after about 25s and the restart path takes over.
pub(crate) fn with_client_keepalives(config: &mut tokio_postgres::Config) {
    config
        .keepalives(true)
        .keepalives_idle(TCP_KEEPALIVE_IDLE)
        .keepalives_interval(TCP_KEEPALIVE_INTERVAL)
        .keepalives_retries(TCP_KEEPALIVE_COUNT)
        .tcp_user_timeout(TCP_USER_TIMEOUT);
}

/// Opens a standalone (non-pooled) connection to `dsn` with the client-side
/// keepalives set. The caller spawns or polls the returned connection
/// future, then runs [`dedicated_session_setup`] (or its own superset of
/// it) before using the client.
pub(crate) async fn connect_dedicated(
    dsn: &str,
) -> Result<
    (
        tokio_postgres::Client,
        tokio_postgres::Connection<tokio_postgres::Socket, tokio_postgres::tls::NoTlsStream>,
    ),
    tokio_postgres::Error,
> {
    let mut config = tokio_postgres::Config::from_str(dsn)?;
    with_client_keepalives(&mut config);
    config.connect(NoTls).await
}

/// The session setup a dedicated connection runs straight after
/// [`connect_dedicated`], mirroring what [`session_bootstrap`] does for a
/// pooled one: `search_path` pinned to `schema` then `public`,
/// [`DETERMINISTIC_TEXT_OUTPUT_GUCS`], and [`tcp_keepalive_gucs`].
pub(crate) fn dedicated_session_setup(schema: &str) -> String {
    format!(
        "set search_path to {}, public; {DETERMINISTIC_TEXT_OUTPUT_GUCS}; {}",
        quote_ident(schema),
        tcp_keepalive_gucs()
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
/// rather than an identifier (doubling embedded `'` characters, the standard
/// `standard_conforming_strings = on` escaping every connection this crate
/// opens already uses).
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

    #[test]
    fn quote_ident_escapes_embedded_quotes() {
        assert_eq!(quote_ident("trellis"), "\"trellis\"");
        assert_eq!(quote_ident("weird\"schema"), "\"weird\"\"schema\"");
    }

    /// Issue #364: the client probes on the same schedule the server does.
    /// The server half is checked against a live TCP session in
    /// `tests/tcp_keepalives.rs`; `tokio_postgres` exposes no way to read
    /// the client's socket options back, so this checks the config instead.
    #[test]
    fn client_keepalives_match_the_server_schedule() {
        let mut config = tokio_postgres::Config::from_str("host=127.0.0.1 keepalives_idle=7200")
            .expect("parse dsn");
        with_client_keepalives(&mut config);
        assert!(config.get_keepalives());
        assert_eq!(config.get_keepalives_idle(), TCP_KEEPALIVE_IDLE);
        assert_eq!(
            config.get_keepalives_interval(),
            Some(TCP_KEEPALIVE_INTERVAL)
        );
        assert_eq!(config.get_keepalives_retries(), Some(TCP_KEEPALIVE_COUNT));
        assert_eq!(config.get_tcp_user_timeout(), Some(&TCP_USER_TIMEOUT));
        // The user timeout replaces the probe count on Linux once set, so
        // it has to equal the probing budget or it changes the schedule.
        assert_eq!(
            TCP_USER_TIMEOUT,
            TCP_KEEPALIVE_IDLE + TCP_KEEPALIVE_INTERVAL * TCP_KEEPALIVE_COUNT
        );
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
             -c intervalstyle=postgres -c timezone=UTC"
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
            clause_count, 5,
            "expected five pinned GUCs as of issue #246"
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
}
