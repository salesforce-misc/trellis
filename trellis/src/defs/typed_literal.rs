//! Typed-literal grammar for calculated fields (issue #109).
//!
//! # What this unlocks
//!
//! Before this module, a calculated-field expression could only *spell* a
//! `Numeric` (`1.5`) or a `Text` (`'hi'`) constant. Every other family the
//! OID registry recognizes ([`PgType`], issue #108) was passthrough-only:
//! `SELECT created AS created` copied a `date` through, but nothing in the
//! grammar could produce a `date` that wasn't already sitting on a source
//! column. `docs/type-support.md` calls that the gap between role 1
//! (ingest/passthrough) and role 5 (**computed 1-1 target**), and closing it
//! is what this module does.
//!
//! # The syntax, and why this one
//!
//! Two spellings, one meaning:
//!
//! ```text
//! DATE '2024-01-01'              -- Postgres's typed-literal form
//! CAST('2024-01-01' AS date)     -- standard SQL's cast-of-a-literal form
//! ```
//!
//! Both parse to the same [`super::ast::Expr::TypedLiteral`], because
//! Postgres itself treats them as the same constant — `EXPLAIN (VERBOSE)
//! SELECT CAST('2024-01-01' AS date), DATE '2024-01-01'` prints
//! `'2024-01-01'::date` for both. Since the SQL-rendering oracle
//! (`super::oracle`) is Postgres, accepting a spelling Postgres folds
//! identically is exactly what ADR-0004's "immutable subset of PostgreSQL"
//! philosophy asks for: we borrow SQL's spelling and the oracle can render
//! our AST back to text Postgres parses to the same node.
//!
//! Deliberately **not** accepted:
//!
//! * `<expr>::<type>` — Postgres-only sugar that adds a postfix operator to
//!   a precedence-climbing parser ADR-0004 explicitly flags as delicate
//!   ("re-verify before adding a second Boolean- or Numeric-returning
//!   operator at a different precedence tier"), while buying nothing over
//!   `CAST`. [`super::lexer`] rejects a `:` with a message naming the two
//!   accepted spellings.
//! * `CAST(<any non-literal expr> AS <type>)` — a **general** cast, i.e. a
//!   coercion lattice. That is a much larger design than this issue: every
//!   (source, target) pair needs its own volatility verdict and its own
//!   re-implemented evaluator arm, and most of the interesting pairs are not
//!   immutable (`timestamptz` → `date` reads the `TimeZone` GUC). It is also
//!   worth very little today, since [`super::registry`] grants
//!   [`ValueType::Other`] no operator or function at all — the only operands
//!   a general cast could have are a column or another literal. Each type
//!   family's own epic child (#113 temporal, #114 `bytea`, #115 `jsonb`)
//!   decides its own cast pairs; [`super::error::ParseError::UnsupportedCast`]
//!   says so by name.
//!
//! # Why the literal text must be canonical
//!
//! [`TYPED_LITERALS`] pairs each accepted type keyword with a checker that
//! [`super::validate`] runs at definition time, and those checkers are
//! strict to the point of pedantry: `DATE '2024-1-5'` is rejected even
//! though Postgres accepts it. Two independent reasons, both load-bearing:
//!
//! 1. **Immutability** (ADR-0004's actual admission bar). `pg_proc` says
//!    `date_in` and `timestamp_in` are **`STABLE`, not `IMMUTABLE`** — a
//!    fact worth checking rather than assuming, since `jsonb_in` and
//!    `byteain` right next to them *are* immutable. The reason is that they
//!    accept relative and session-dependent spellings: `DATE 'today'`
//!    evaluates to a different date tomorrow, and `TIMESTAMP 'now'` to a
//!    different instant every call. A checker that admits only an absolute
//!    ISO-8601 calendar spelling removes exactly that surface, which is what
//!    lets a `date`/`timestamp` constant be treated as immutable here
//!    without lying about `provolatile`.
//!
//! 2. **Round-trip identity.** A computed value travels as text
//!    ([`super::eval::Value::Other`] carries the literal verbatim) and is
//!    cast back to its native type on write. The Rust evaluator has no type
//!    library to normalize `2024-1-5` into `2024-01-05` the way `date_out`
//!    would, so the evaluator's text and the oracle's rendering would
//!    disagree on a non-canonical spelling — precisely the cross-check
//!    ADR-0013 exists to catch. Requiring the canonical spelling up front
//!    makes the two renderers agree by construction, rather than by adding a
//!    per-family normalizer this issue has no business writing.
//!
//! Half 2 has a dependency worth naming, because it is *not* a property of
//! the literal alone: "canonical" means canonical **under the session's
//! output GUCs**. Under `DateStyle = 'SQL, MDY'`, `('2024-01-01'::date)::text`
//! renders `01/01/2024`, and the two renderers would disagree again. That is
//! why [`crate::pool::DETERMINISTIC_TEXT_OUTPUT_GUCS`] pins `DateStyle` and
//! `bytea_output` on every connection Trellis opens — see that constant for
//! the full reasoning, and extend *it* (not this module) when a future type
//! family needs `TimeZone`/`IntervalStyle`/`extra_float_digits`.
//!
//! Half 1, by contrast, needs no help: a leading-4-digit-year ISO spelling
//! *parses* to the same value under every `DateStyle`, so a definition
//! installed against one server stays correct when read on another.
//!
//! This is the same "safe subset, gaps named explicitly" shape ADR-0004
//! already documents for `COALESCE`.
//!
//! # Which families are in the allowlist
//!
//! See [`TYPED_LITERALS`]. The allowlist is deliberately *not* every
//! [`PgType`]: a family earns a row only once both requirements above can be
//! met for it.

use super::ast::ValueType;
use super::pg_type::PgType;
use crate::float::{self, FloatWidth};

/// One type keyword the typed-literal grammar accepts, and the check its
/// literal text must pass.
#[derive(Clone, Copy)]
pub struct TypedLiteralSpec {
    /// The canonical, uppercased type keyword this literal is spelled with
    /// (`DATE`, `TIMESTAMP`, ...). Matched case-insensitively by
    /// [`super::parser`] against an already-uppercased identifier, so
    /// `date '...'`, `Date '...'` and `DATE '...'` are all the same.
    ///
    /// Always equal to the uppercase of [`super::ddl::pg_type_name`] for
    /// [`Self::value_type`], so the spelling a user writes is the spelling
    /// the oracle renders back — the `type_keyword_matches_value_type` test
    /// pins that. `DOUBLE PRECISION` is the one two-word keyword; see
    /// [`super::parser`], which rejoins the pair before looking it up.
    pub keyword: &'static str,
    /// The type this literal produces, which becomes the field's inferred
    /// [`ValueType`] and hence its target column's Postgres type
    /// (`super::ddl::pg_type_name`).
    ///
    /// Issue #109 declared this a [`PgType`], because every family in the
    /// allowlist then was a passthrough `ValueType::Other`. Issue #112
    /// widened it to a full [`ValueType`] — exactly the follow-up this
    /// module's "which families are in the allowlist" note predicted —
    /// because `real`/`double precision` are a first-class
    /// [`ValueType::Float`] *and* have no bare literal syntax of their own
    /// (`pg_typeof(1.5)` is `numeric`, not `double precision`), so they are
    /// the first type that both needs this grammar and isn't an `Other`.
    pub value_type: ValueType,
    /// Checks the literal's text is in this family's canonical Postgres
    /// output spelling, returning a human-readable description of the
    /// expected shape on failure. See this module's doc comment for why the
    /// bar is canonical form and not merely "Postgres would parse it".
    pub canonical: fn(&str) -> Result<(), &'static str>,
}

/// The type keywords a typed literal may name (issue #109).
///
/// Scoped to the families issue #109 names as its motivating cases
/// (`date`/`timestamp`/`bytea`) — enough to prove the mechanism end-to-end
/// across the three shapes it has to handle: a plain calendar value, a value
/// with optional sub-second precision, and a binary value whose canonical
/// text is an escape-prefixed hex string. Promoting a family here is
/// deliberately *not* the same as fully supporting it: each family's own
/// epic child (#113, #114) still owns its join-key, primary-key and
/// aggregate roles.
///
/// Families held back, and why:
///
/// * **`jsonb`** (#115) — `jsonb_in`/`jsonb_out` are genuinely `IMMUTABLE`,
///   so the volatility bar is met, but the *canonical form* bar is not
///   reachable here. Postgres stores `jsonb` parsed, not as text, and
///   `jsonb_out` re-renders it: object keys are re-sorted by length and then
///   bytewise (`{"a":1,"bb":2,"c":3}` renders as `{"a": 1, "c": 3, "bb":
///   2}`), duplicate keys collapse to the last, and numbers are renormalized
///   (`1e3` renders as `1000`). Checking a literal is already in that form
///   means implementing `jsonb_in` + `jsonb_out` in Rust — a real `jsonb`
///   value model, which is #115's job and which it needs anyway for
///   `jsonb_agg`. Adding `jsonb` here afterwards is one row in this table
///   plus that canonicalizer.
/// * **`timestamptz`** (#113, #246) — value comparison is immutable, and
///   *text rendering* now is too: issue #246 pins `TimeZone = 'UTC'` on
///   every connection Trellis opens, pool and walsender alike (see
///   [`crate::pool::DETERMINISTIC_TEXT_OUTPUT_GUCS`]), which is what let
///   `timestamptz` join the join/`GROUP BY`/primary-key and `MIN`/`MAX`
///   roles (`crate::temporal::is_bijective_under_text`/
///   `is_render_consistent`). It is *not* in this table yet regardless: a
///   typed literal here additionally needs its own canonical-form checker
///   (this module's narrower "one spelling, checked without a live
///   connection" bar — see `canonical_time`/`canonical_timetz` for what
///   that looks like for a sibling family), which #246 did not write. Adding
///   one is a natural follow-up, not a reopened investigation — `timetz`'s
///   playbook (require a fully-numeric offset, canonicalize the way
///   `timestamptz_out` does under the now-pinned `TimeZone`) is the
///   template — but it is new code this module doesn't have today, so it
///   stays out of #246's scope.
/// * **`interval`** (#113) — the one temporal family that stays out, and
///   for an *input*-side reason the other five don't have: `interval_in`
///   reads `IntervalStyle`, so the same literal text parses to different
///   values on different servers. `'-1 2:03:04'` is `-1 days +02:03:04`
///   under `IntervalStyle = 'postgres'` and `-1 day -2:03:04` under
///   `sql_standard`; `'1-2'` is `1 year 2 mons` under both but `P1Y2M`
///   under `iso_8601` output. This module's "parses to the same value under
///   any GUC" bar — the half that `crate::pool::DETERMINISTIC_TEXT_OUTPUT_GUCS`
///   explicitly cannot help with, since a definition installed against one
///   server must stay correct when read on another — is therefore
///   unreachable for `interval` without also pinning an input GUC, which
///   Trellis does not do. (`interval`'s *output* side is fine: #113 pins
///   `IntervalStyle` and `crate::temporal::Interval::render` reproduces the
///   `postgres` spelling exactly, which is what `SUM(interval)` needs.)
/// * **`bit`** (fixed-length) — decided by #118, and permanently excluded,
///   not merely waiting: Postgres's *default* typmod for a `bit` literal
///   cast with no explicit length is `bit(1)`, and [`render_sql`] always
///   emits the bare, unmodified type name — `'101'::bit` truncates to
///   `'1'` on a live server (`bit_in` applies the default-typmod
///   coercion), silently discarding data on the very round trip this
///   module's "why the literal text must be canonical" section demands.
///   `bit varying` (below) has no such trap: its bare default is genuinely
///   unconstrained. See `registry::AGGREGATE_FUNCTION_SPECS`'s `BIT_AND`/
///   `BIT_OR` doc comment for the same bare-`bit`-defaults-to-`bit(1)`
///   hazard recurring in the aggregate-target role, and
///   `validate::reject_unsupported_group_by_key_type`'s `VarBit` arm for
///   the `GROUP BY` key role's version of it.
///
/// `jsonb` (issue #115) *is* in the allowlist below now, despite an earlier
/// version of this doc comment predicting otherwise. `jsonb_in`/`jsonb_out`
/// are `IMMUTABLE` (`pg_proc.provolatile`), clearing bar 1 as before. Bar 2
/// (canonical form) turns out to need only a *checker*, not the
/// `jsonb_in`/`jsonb_out` reimplementation the earlier prediction assumed:
/// [`crate::jsonb::canonical_jsonb`] rejects a non-canonical spelling
/// outright (exponent notation, an out-of-order/duplicate object key, a
/// non-canonical string escape) rather than normalizing it, which sidesteps
/// ever having to reproduce `numeric`'s scale-tracking arithmetic — see that
/// module's doc comment for the full live evidence, including the
/// `jsonb_agg`/key-role findings this same investigation produced.
///
/// `inet`, `cidr`, `macaddr` and `macaddr8` (issue #116) *are* in the
/// allowlist below now, each clearing both bars: `inet_in`/`cidr_in`/
/// `macaddr_in`/`macaddr8_in` and their `_out` counterparts are all
/// `IMMUTABLE` (checked against `pg_proc.provolatile`, the same check
/// `crate::netaddr`'s module doc runs for the key/aggregate roles), and each
/// checker below accepts exactly the one spelling the type's *actual*
/// canonical renderer emits — `network_show` (`<col>::text`) for `inet`/
/// `cidr`, not `inet_out`'s host-elided form (`crate::netaddr::
/// canonical_inet`/`canonical_cidr`), and `macaddr_out`/`macaddr8_out`'s one
/// lowercase colon-grouped spelling for the other two
/// (`crate::netaddr::canonical_macaddr`/`canonical_macaddr8`).
/// * **`smallint`, `integer`, `bigint`** — these *do* have literal syntax of
///   their own since issue #111, and it is Postgres's own: a bare `5` is
///   `integer` and a bare `3000000000` is `bigint`, exactly as
///   `pg_typeof(5)` reports, so `INTEGER '5'` would be a second spelling of
///   something already spellable. A `smallint` constant is the one gap — it
///   has no bare spelling in Postgres either. Issue #112 did the widening
///   that gap was waiting on ([`TypedLiteralSpec::value_type`] is a full
///   [`ValueType`] now), so closing it is a one-row change plus a canonical
///   checker; it is left to a #111 follow-up rather than smuggled in here.
///
/// `oid` (#111) and `real`/`double precision` (#112) *are* in the allowlist
/// below. The floats are there out of necessity rather than convenience:
/// unlike the integers, a float constant has **no** bare spelling in
/// Postgres at all — `select pg_typeof(1.5)` is `numeric` and
/// `pg_typeof(1.5e0)` is `numeric` too, so without these two rows a
/// calculated field could not produce a float constant by any syntax.
/// * **`money`, `json`, `xml`, `tsvector`, `tsquery`** — excluded from the
///   whole epic by `docs/type-support.md` (locale-dependent text I/O, or no
///   useful immutable equality), so they can never earn a row here.
/// * **[`PgType::Unrecognized`]** — not a Postgres type keyword at all (it
///   renders as `text` in SQL), so it has nothing to spell.
pub const TYPED_LITERALS: &[TypedLiteralSpec] = &[
    TypedLiteralSpec {
        keyword: "DATE",
        value_type: ValueType::Other(PgType::Date),
        canonical: canonical_date,
    },
    TypedLiteralSpec {
        keyword: "TIMESTAMP",
        value_type: ValueType::Other(PgType::Timestamp),
        canonical: canonical_timestamp,
    },
    TypedLiteralSpec {
        keyword: "BYTEA",
        value_type: ValueType::Other(PgType::Bytea),
        canonical: canonical_bytea,
    },
    // Issue #113. `time_out` and `timetz_out` are the only two output
    // functions in the temporal block `pg_proc` marks **IMMUTABLE**
    // outright — `date_out`, `timestamp_out`, `timestamptz_out` and
    // `interval_out` are all `STABLE` — and they read no GUC at all,
    // verified by rendering the same values under `ISO`/`SQL`/`Postgres`/
    // `German` `DateStyle`s and under three session `TimeZone`s and getting
    // one answer each time. `time_in`/`timetz_in` are `STABLE` for the
    // usual reason (`TIME 'now'`, and `timetz`'s zone *abbreviations*
    // resolve against the session's timezone set), and the canonical
    // checkers below remove exactly that surface by admitting only a
    // fully-numeric offset.
    TypedLiteralSpec {
        keyword: "TIME",
        value_type: ValueType::Other(PgType::Time),
        canonical: canonical_time,
    },
    TypedLiteralSpec {
        keyword: "TIMETZ",
        value_type: ValueType::Other(PgType::TimeTz),
        canonical: canonical_timetz,
    },
    // Issue #111. `oid` clears both bars this module sets easily: `oidin`
    // and `oidout` are `IMMUTABLE` (no session state, no relative
    // spellings), and `oid_out`'s canonical form is plain unsigned decimal,
    // which needs no normalizer to check.
    TypedLiteralSpec {
        keyword: "OID",
        value_type: ValueType::Other(PgType::Oid),
        canonical: canonical_oid,
    },
    // Issue #112. `float4in`/`float8in` and `float4out`/`float8out` are all
    // `IMMUTABLE` (checked against `pg_proc.provolatile`), and the canonical
    // form is whatever `float4out`/`float8out` emits — which
    // `crate::float::render` reproduces exactly, so the checkers below are
    // simply `crate::float::parse`, the round-trip test itself.
    TypedLiteralSpec {
        keyword: "REAL",
        value_type: ValueType::Float(FloatWidth::Float4),
        canonical: canonical_float4,
    },
    TypedLiteralSpec {
        keyword: "DOUBLE PRECISION",
        value_type: ValueType::Float(FloatWidth::Float8),
        canonical: canonical_float8,
    },
    // Issue #116. Canonical form is `network_show`'s (`<col>::text`'s)
    // always-explicit-netmask spelling, not `inet_out`'s host-elided one —
    // see `crate::netaddr`'s module doc for why those two differ and which
    // one every other renderer this engine uses actually produces.
    TypedLiteralSpec {
        keyword: "INET",
        value_type: ValueType::Other(PgType::Inet),
        canonical: crate::netaddr::canonical_inet,
    },
    // `cidr_out` and the `network_show` cast agree unconditionally (a `cidr`
    // value's whole point is that the netmask matters, so neither renderer
    // ever elides it) — the extra bar over `INET` is `cidr_in`'s own
    // host-bits-must-be-zero requirement.
    TypedLiteralSpec {
        keyword: "CIDR",
        value_type: ValueType::Other(PgType::Cidr),
        canonical: crate::netaddr::canonical_cidr,
    },
    TypedLiteralSpec {
        keyword: "MACADDR",
        value_type: ValueType::Other(PgType::MacAddr),
        canonical: crate::netaddr::canonical_macaddr,
    },
    TypedLiteralSpec {
        keyword: "MACADDR8",
        value_type: ValueType::Other(PgType::MacAddr8),
        canonical: crate::netaddr::canonical_macaddr8,
    },
    // Issue #118. `varbit_in`/`varbit_out` are `IMMUTABLE` and read no GUC
    // (verified against `pg_proc.provolatile`), and `bit varying`'s bare
    // (no explicit length) default is genuinely unconstrained — unlike
    // fixed-length `bit`, whose bare default of `bit(1)` is why it is *not*
    // in this table (see this module's "which families are held back" note
    // above). `varbit_out`'s canonical form is the bit string's own `'0'`/
    // `'1'` characters, with no separator or padding beyond its own stored
    // bits, so the checker below needs no normalizer.
    TypedLiteralSpec {
        keyword: "VARBIT",
        value_type: ValueType::Other(PgType::VarBit),
        canonical: canonical_varbit,
    },
    // Issue #115. `jsonb_in`/`jsonb_out` are `IMMUTABLE`, and
    // [`crate::jsonb::canonical_jsonb`] is the canonical-form checker — see
    // this module's "which families are in the allowlist" note and
    // `crate::jsonb`'s own module doc for the live evidence.
    TypedLiteralSpec {
        keyword: "JSONB",
        value_type: ValueType::Other(PgType::Jsonb),
        canonical: crate::jsonb::canonical_jsonb,
    },
];

/// Looks up a typed-literal type keyword by its canonical uppercased name —
/// the [`TYPED_LITERALS`] counterpart to
/// [`super::registry::lookup_function`].
pub fn lookup_typed_literal(keyword: &str) -> Option<&'static TypedLiteralSpec> {
    TYPED_LITERALS.iter().find(|spec| spec.keyword == keyword)
}

/// Renders a typed literal back to Postgres SQL, for every renderer that
/// turns an [`super::ast::Expr`] into text: the oracle
/// ([`super::oracle::render_expr_sql`] and its relationship-aware siblings),
/// the direct-build backfill, and `staging::self_check`'s recompute leg.
/// Shared here so the five of them can't drift — ADR-0013's cross-check only
/// catches a renderer that disagrees with the *evaluator*, not two renderers
/// that agree with each other and are both wrong.
///
/// Emits the `'<text>'::<type>` spelling rather than the `<type> '<text>'`
/// one a user may have written. The two are the same constant to Postgres
/// (`EXPLAIN (VERBOSE)` prints `'...'::<type>` for both), `::` is what every
/// other literal arm in those renderers already emits (`'x'::text`,
/// `1::numeric`), and a trailing cast is unambiguous in every context an
/// operand can appear in.
///
/// Quote-escaping is the same `''` doubling the `Text` arms use. Backslashes
/// need no escaping: `standard_conforming_strings` has been `on` by default
/// since Postgres 9.1, so `'\x0102'` is six literal characters — which is
/// exactly what a canonical `bytea` literal has to be.
pub fn render_sql(value_type: ValueType, text: &str) -> String {
    format!(
        "'{}'::{}",
        text.replace('\'', "''"),
        super::ddl::pg_type_name(value_type)
    )
}

/// `real`'s canonical `float4out` rendering. Delegates to
/// [`crate::float::parse`], whose contract *is* "accepts exactly what
/// [`crate::float::render`] emits" — so there is one definition of canonical
/// float text in this crate rather than a parser here and a renderer there.
fn canonical_float4(text: &str) -> Result<(), &'static str> {
    canonical_float(text, FloatWidth::Float4)
}

/// `double precision`'s canonical `float8out` rendering — see
/// [`canonical_float4`].
fn canonical_float8(text: &str) -> Result<(), &'static str> {
    canonical_float(text, FloatWidth::Float8)
}

fn canonical_float(text: &str, width: FloatWidth) -> Result<(), &'static str> {
    const SHAPE: &str = "a value in the shortest round-tripping decimal form \
         `float4out`/`float8out` emits for this width (no `+` sign, no \
         trailing zeros, no `E`, exponent written as e.g. `1e+30`), or one \
         of `NaN`, `Infinity`, `-Infinity`";
    float::parse(text, width).map(|_| ()).map_err(|_| SHAPE)
}

/// Plain unsigned decimal in `0 ..= 4294967295`, the spelling `oid_out`
/// emits (issue #111).
///
/// `oid` is Postgres's *unsigned* 32-bit identifier: `oid_out` never emits a
/// sign, never pads, and never emits a leading zero, so a canonical literal
/// is just digits with no redundant leading `0`. A negative or
/// `+`-prefixed spelling is rejected even though `oidin` would accept
/// `-1` (wrapping it to `4294967295`) — a literal whose own text differs
/// from what the value renders back as is precisely what breaks the
/// evaluator/oracle round-trip this module's doc comment describes.
fn canonical_oid(text: &str) -> Result<(), &'static str> {
    const SHAPE: &str = "an unsigned decimal in 0..=4294967295, with no sign and no leading zeros";
    if text.is_empty() || !text.bytes().all(|b| b.is_ascii_digit()) {
        return Err(SHAPE);
    }
    if text.len() > 1 && text.starts_with('0') {
        return Err(SHAPE);
    }
    match text.parse::<u32>() {
        Ok(_) => Ok(()),
        Err(_) => Err(SHAPE),
    }
}

/// `YYYY-MM-DD`, the spelling `date_out` emits under the ISO `DateStyle`.
///
/// Rejects every relative/special spelling `date_in` also accepts
/// (`today`, `yesterday`, `tomorrow`, `now`, `epoch`, `infinity`,
/// `-infinity`) — the reason `date_in` is `STABLE` — as well as the
/// zero-padding-optional and `DateStyle`-ambiguous forms (`2024-1-5`,
/// `01/02/2024`) that would round-trip to different text than they were
/// written as, or to a different *date* under a different `DateStyle`.
fn canonical_date(text: &str) -> Result<(), &'static str> {
    let bytes = text.as_bytes();
    if bytes.len() != 10 || bytes[4] != b'-' || bytes[7] != b'-' {
        return Err(DATE_SHAPE);
    }
    let year = parse_fixed(&text[0..4]).ok_or(DATE_SHAPE)?;
    let month = parse_fixed(&text[5..7]).ok_or(DATE_SHAPE)?;
    let day = parse_fixed(&text[8..10]).ok_or(DATE_SHAPE)?;
    check_ymd(year, month, day)
}

/// `YYYY-MM-DD HH:MM:SS[.F[FFFFF]]`, the spelling `timestamp_out` emits
/// under the ISO `DateStyle`.
///
/// The fractional-seconds part is optional and, when present, must have no
/// trailing zero: Postgres renders `12:00:00.100` back as `12:00:00.1`, so
/// accepting the padded form would break round-trip identity. Six digits is
/// `timestamp`'s full microsecond resolution — a seventh is silently rounded
/// away by `timestamp_in`, so it can't round-trip either.
///
/// Seconds are capped at 59 rather than 60 for the same reason: Postgres
/// accepts `:60` and normalizes it into the next minute.
fn canonical_timestamp(text: &str) -> Result<(), &'static str> {
    let (date_part, time_part) = text.split_once(' ').ok_or(TIMESTAMP_SHAPE)?;
    canonical_date(date_part).map_err(|_| TIMESTAMP_SHAPE)?;

    let (hms, fraction) = match time_part.split_once('.') {
        Some((hms, fraction)) => (hms, Some(fraction)),
        None => (time_part, None),
    };
    let bytes = hms.as_bytes();
    if bytes.len() != 8 || bytes[2] != b':' || bytes[5] != b':' {
        return Err(TIMESTAMP_SHAPE);
    }
    let hour = parse_fixed(&hms[0..2]).ok_or(TIMESTAMP_SHAPE)?;
    let minute = parse_fixed(&hms[3..5]).ok_or(TIMESTAMP_SHAPE)?;
    let second = parse_fixed(&hms[6..8]).ok_or(TIMESTAMP_SHAPE)?;
    if hour > 23 || minute > 59 || second > 59 {
        return Err(TIMESTAMP_SHAPE);
    }

    if let Some(fraction) = fraction {
        if fraction.is_empty() || fraction.len() > 6 {
            return Err(TIMESTAMP_SHAPE);
        }
        if !fraction.bytes().all(|b| b.is_ascii_digit()) {
            return Err(TIMESTAMP_SHAPE);
        }
        if fraction.ends_with('0') {
            return Err(TIMESTAMP_SHAPE);
        }
    }
    Ok(())
}

/// `\x` followed by an even number of lowercase hex digits — the spelling
/// `byteaout` emits under the default `bytea_output = 'hex'`.
///
/// The empty value `\x` is valid (an empty `bytea`). Postgres's older
/// `escape` input format (`'\\000'`) is rejected: `byteaout` never *emits*
/// it under the default GUC, so it isn't canonical, and its backslash
/// doubling is a second, ambiguous spelling of values the hex form already
/// covers. Uppercase hex is rejected for the same round-trip reason as an
/// unpadded date — `byteaout` emits lowercase.
fn canonical_bytea(text: &str) -> Result<(), &'static str> {
    let Some(hex) = text.strip_prefix("\\x") else {
        return Err(BYTEA_SHAPE);
    };
    if !hex.len().is_multiple_of(2) {
        return Err(BYTEA_SHAPE);
    }
    if !hex
        .bytes()
        .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    {
        return Err(BYTEA_SHAPE);
    }
    Ok(())
}

/// A run of only `'0'`/`'1'` characters (any length, including empty) — the
/// spelling `varbit_out` emits (issue #118). Unlike every temporal/`bytea`
/// checker above, there is no relative/session-dependent spelling to reject
/// (`varbit_in` is `IMMUTABLE`, not `STABLE`) and no separate "canonical vs.
/// merely parseable" gap to close: every string of `0`s and `1`s `varbit_in`
/// accepts is already the exact string `varbit_out` renders back, so this
/// checker is the round-trip property itself, not an approximation of it.
fn canonical_varbit(text: &str) -> Result<(), &'static str> {
    const SHAPE: &str = "expected a canonical bit-varying literal: a run of only `0`/`1` characters \
         (e.g. '101', or '' for a zero-length value), the spelling `varbit_out` emits";
    if text.bytes().all(|b| b == b'0' || b == b'1') {
        Ok(())
    } else {
        Err(SHAPE)
    }
}

const DATE_SHAPE: &str = "expected a canonical ISO-8601 date literal, `YYYY-MM-DD` \
     (e.g. '2024-01-01'). Relative spellings Postgres also accepts ('today', 'now', \
     'infinity') are rejected: they are what makes `date_in` STABLE rather than \
     IMMUTABLE, and ADR-0004 admits only immutable constructs. Unpadded or \
     DateStyle-dependent spellings ('2024-1-5', '01/02/2024') are rejected because \
     they do not round-trip to the text they were written as";

const TIMESTAMP_SHAPE: &str = "expected a canonical ISO-8601 timestamp literal, \
     `YYYY-MM-DD HH:MM:SS` with optional `.` plus 1-6 fractional-second digits and no \
     trailing zero (e.g. '2024-01-01 12:00:00' or '2024-01-01 12:00:00.5'). Relative \
     spellings ('now', 'epoch', 'infinity') are rejected as non-immutable, and \
     non-canonical ones because they do not round-trip to the text they were written as";

/// `HH:MM:SS[.F[FFFFF]]`, the spelling `time_out` emits — under every
/// `DateStyle`, since `time_out` is `IMMUTABLE` and reads no GUC.
///
/// Same fractional-second rules as [`canonical_timestamp`] (1-6 digits, no
/// trailing zero, since `time_out` trims). `24:00:00` is accepted and is a
/// genuinely distinct value from `00:00:00` — Postgres's legal end-of-day
/// `time` — but `24:00:01` and `24:00:00.5` are rejected, because
/// `time_in` rejects them too (`time` is capped at exactly 24 hours).
///
/// Relative spellings `time_in` also accepts (`now`, `allballs`) are
/// rejected: `'now'::time` is a different value every call, which is what
/// makes `time_in` `STABLE`, and `'allballs'::time` renders back as
/// `00:00:00`, so it would not round-trip to the text it was written as.
fn canonical_time(text: &str) -> Result<(), &'static str> {
    canonical_time_of_day(text).ok_or(TIME_SHAPE)
}

/// [`canonical_time`] plus `timetz_out`'s mandatory numeric UTC offset,
/// `±HH[:MM[:SS]]`.
///
/// The offset's minute and second fields are **omitted when zero** by
/// `timetz_out` (`'12:00:00-00:00'::timetz` renders `12:00:00+00`, and
/// `'+05:30:00'` renders `+05:30`), so a padded spelling is rejected for
/// the same round-trip reason an unpadded date is. Postgres's range is
/// `-15:59:59 .. +15:59:59` — `'12:00:00+16'::timetz` errors — so the hour
/// field is capped at 15.
///
/// Zone *abbreviations* and names (`'12:00:00 EST'`, `'12:00:00
/// America/New_York'`) are rejected, and this is the load-bearing half of
/// why a `timetz` literal can be treated as immutable at all: resolving an
/// abbreviation is a lookup against the server's timezone set, which is
/// exactly what makes `timetz_in` `STABLE`. A numeric offset needs no
/// lookup — and is what `timetz_out` emits anyway.
fn canonical_timetz(text: &str) -> Result<(), &'static str> {
    let sign_at = text.rfind(['+', '-']).ok_or(TIMETZ_SHAPE)?;
    let (time_part, zone) = text.split_at(sign_at);
    canonical_time_of_day(time_part).ok_or(TIMETZ_SHAPE)?;

    let mut fields = zone[1..].split(':');
    let hours = fields
        .next()
        .filter(|field| field.len() == 2)
        .and_then(parse_fixed)
        .ok_or(TIMETZ_SHAPE)?;
    if hours > 15 {
        return Err(TIMETZ_SHAPE);
    }
    // `timetz_out` emits the *shortest* offset that still names the value,
    // dropping a trailing all-zero tail — and only a trailing one. Read off
    // a live server:
    //
    //   +05:00:00 -> +05        +05:30:00 -> +05:30
    //   +05:00:30 -> +05:00:30  +00:00:30 -> +00:00:30
    //
    // So a zero *minutes* field is canonical whenever the seconds field is
    // non-zero — `+05:00:30` is exactly what Postgres prints, and rejecting
    // it (as this did before the #113 review) refuses a literal the user
    // could read straight out of a `select`. The rule is therefore about
    // the *tail*, not about each field independently: seconds may appear
    // only if non-zero, and minutes only if minutes or seconds is.
    let minutes = match fields.next() {
        Some(field) => Some(zone_field(field)?),
        None => None,
    };
    let seconds = match fields.next() {
        Some(field) => Some(zone_field(field)?),
        None => None,
    };
    if fields.next().is_some() {
        return Err(TIMETZ_SHAPE);
    }
    if seconds == Some(0) {
        return Err(TIMETZ_SHAPE);
    }
    if minutes == Some(0) && seconds.is_none() {
        return Err(TIMETZ_SHAPE);
    }
    Ok(())
}

/// One two-digit `00..=59` field of a `timetz` UTC offset.
fn zone_field(field: &str) -> Result<u32, &'static str> {
    match Some(field)
        .filter(|field| field.len() == 2)
        .and_then(parse_fixed)
    {
        Some(value) if value <= 59 => Ok(value),
        _ => Err(TIMETZ_SHAPE),
    }
}

/// The shared `HH:MM:SS[.F[FFFFF]]` clock reading of [`canonical_time`] and
/// [`canonical_timetz`], returning `None` rather than a message so each
/// caller can blame its own type.
fn canonical_time_of_day(text: &str) -> Option<()> {
    let (hms, fraction) = match text.split_once('.') {
        Some((hms, fraction)) => (hms, Some(fraction)),
        None => (text, None),
    };
    let bytes = hms.as_bytes();
    if bytes.len() != 8 || bytes[2] != b':' || bytes[5] != b':' {
        return None;
    }
    let hour = parse_fixed(&hms[0..2])?;
    let minute = parse_fixed(&hms[3..5])?;
    let second = parse_fixed(&hms[6..8])?;
    if hour > 24 || minute > 59 || second > 59 {
        return None;
    }
    // `time`'s upper bound is exactly 24:00:00, not 24:59:59.
    if hour == 24 && (minute != 0 || second != 0 || fraction.is_some()) {
        return None;
    }
    if let Some(fraction) = fraction
        && (fraction.is_empty()
            || fraction.len() > 6
            || !fraction.bytes().all(|b| b.is_ascii_digit())
            || fraction.ends_with('0'))
    {
        return None;
    }
    Some(())
}

const TIME_SHAPE: &str = "expected a canonical time literal, `HH:MM:SS` with optional `.` \
     plus 1-6 fractional-second digits and no trailing zero (e.g. '13:45:00' or \
     '13:45:00.5'). `24:00:00` is Postgres's legal end-of-day value and is accepted; \
     anything past it is not. Relative spellings ('now', 'allballs') are rejected as \
     non-immutable, and non-canonical ones because they do not round-trip to the text \
     they were written as";

const TIMETZ_SHAPE: &str = "expected a canonical timetz literal, a `HH:MM:SS[.F…]` time \
     followed by a numeric UTC offset `+HH`, `+HH:MM` or `+HH:MM:SS` in -15:59:59..+15:59:59, \
     with zero minute/second offset fields omitted (e.g. '13:45:00+00' or \
     '13:45:00.5-05:30'). A zone abbreviation or name ('EST', 'America/New_York') is \
     rejected: resolving one is a lookup against the server's timezone set, which is what \
     makes `timetz_in` STABLE rather than IMMUTABLE";

const BYTEA_SHAPE: &str = "expected a canonical hex bytea literal, `\\x` followed by an \
     even number of lowercase hex digits (e.g. '\\x0102ff', or '\\x' for an empty value). \
     The legacy `escape` input format and uppercase hex are rejected because \
     `bytea_output = 'hex'` — Postgres's default — never emits them, so they do not \
     round-trip to the text they were written as";

/// Parses a fixed-width, fully zero-padded run of ASCII digits. `None` for
/// anything else, including a `+`/`-` sign or an embedded space, both of
/// which `str::parse::<u32>` would otherwise be lenient about in ways that
/// break the fixed-width canonical spellings above.
fn parse_fixed(text: &str) -> Option<u32> {
    if !text.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    text.parse().ok()
}

/// Rejects a syntactically well-formed but non-existent calendar date
/// (`2024-02-30`, `2023-02-29`, `2024-13-01`). Postgres rejects these too,
/// but at *apply* time on the target's `::date` cast — long after the
/// definition installed — so catching them here keeps a bad literal a
/// definition-time error like every other grammar rejection.
///
/// Year 0 is rejected: Postgres has no year zero (1 BC is followed by 1 AD),
/// and `date_in` errors on `0000-01-01`.
fn check_ymd(year: u32, month: u32, day: u32) -> Result<(), &'static str> {
    if year == 0 || !(1..=12).contains(&month) || day == 0 {
        return Err(DATE_SHAPE);
    }
    let max_day = match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        // Proleptic Gregorian, which is what Postgres uses for `date`.
        _ if year.is_multiple_of(4) && (!year.is_multiple_of(100) || year.is_multiple_of(400)) => {
            29
        }
        _ => 28,
    };
    if day > max_day {
        return Err(DATE_SHAPE);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every allowlisted keyword must be the uppercase of the Postgres type
    /// keyword the oracle renders back (`ddl::pg_type_name`), or a
    /// definition would parse under one spelling and render under another.
    #[test]
    fn type_keyword_matches_value_type() {
        for spec in TYPED_LITERALS {
            assert_eq!(
                spec.keyword,
                super::super::ddl::pg_type_name(spec.value_type).to_ascii_uppercase(),
                "{} must be spelled as its own pg type keyword",
                spec.keyword
            );
        }
    }

    /// The allowlist may only hold types that genuinely have no bare
    /// literal syntax of their own in this grammar — every passthrough
    /// [`ValueType::Other`] family (issue #109) plus the two floats (issue
    /// #112, whose bare spelling `1.5` is `numeric` in Postgres). A type
    /// with a bare spelling (`Numeric`, `Text`, `Integer`) must never gain a
    /// row, or one constant would have two syntaxes.
    #[test]
    fn every_allowlisted_type_lacks_a_bare_literal_syntax() {
        for spec in TYPED_LITERALS {
            assert!(
                matches!(spec.value_type, ValueType::Other(_) | ValueType::Float(_)),
                "{} has a bare literal syntax and must not be allowlisted",
                spec.keyword
            );
        }
    }

    /// `docs/type-support.md` excludes these from the whole epic, so no
    /// future edit may quietly add one.
    #[test]
    fn epic_excluded_families_are_absent() {
        for excluded in [
            PgType::Money,
            PgType::Json,
            PgType::Xml,
            PgType::TsVector,
            PgType::TsQuery,
            PgType::Unrecognized,
        ] {
            assert!(
                !TYPED_LITERALS
                    .iter()
                    .any(|spec| spec.value_type == ValueType::Other(excluded)),
                "{excluded} is excluded by docs/type-support.md and must not be spellable"
            );
        }
    }

    #[test]
    fn lookup_is_by_canonical_uppercase_keyword() {
        assert_eq!(
            lookup_typed_literal("DATE").map(|spec| spec.value_type),
            Some(ValueType::Other(PgType::Date))
        );
        assert!(lookup_typed_literal("date").is_none());
        assert!(lookup_typed_literal("TIMESTAMPTZ").is_none());
    }

    #[test]
    fn canonical_dates_are_accepted() {
        for text in ["2024-01-01", "0001-01-01", "9999-12-31", "2024-02-29"] {
            assert!(canonical_date(text).is_ok(), "{text} should be canonical");
        }
    }

    #[test]
    fn non_canonical_or_non_immutable_dates_are_rejected() {
        for text in [
            // The reason `date_in` is STABLE.
            "today",
            "yesterday",
            "tomorrow",
            "now",
            "epoch",
            "infinity",
            "-infinity",
            // Parses in Postgres, but doesn't round-trip to itself.
            "2024-1-5",
            "01/02/2024",
            "20240101",
            "2024-01-01 00:00:00",
            // Not a real calendar date.
            "2023-02-29",
            "2024-02-30",
            "2024-13-01",
            "2024-00-01",
            "2024-01-00",
            "0000-01-01",
            // Signs/whitespace `str::parse` would otherwise be lenient about.
            "+024-01-01",
            "2024- 1-01",
            "",
        ] {
            assert!(
                canonical_date(text).is_err(),
                "{text:?} must not be accepted as a date literal"
            );
        }
    }

    #[test]
    fn canonical_timestamps_are_accepted() {
        for text in [
            "2024-01-01 12:00:00",
            "2024-01-01 00:00:00",
            "2024-01-01 23:59:59",
            "2024-01-01 12:00:00.5",
            "2024-01-01 12:00:00.123456",
        ] {
            assert!(
                canonical_timestamp(text).is_ok(),
                "{text} should be canonical"
            );
        }
    }

    #[test]
    fn non_canonical_or_non_immutable_timestamps_are_rejected() {
        for text in [
            "now",
            "epoch",
            "infinity",
            // No time part: `timestamp '2024-01-01'` is legal Postgres but
            // renders back as `2024-01-01 00:00:00`.
            "2024-01-01",
            // `T` separator: legal input, renders back with a space.
            "2024-01-01T12:00:00",
            // Trailing zero: renders back as `.1`.
            "2024-01-01 12:00:00.10",
            "2024-01-01 12:00:00.0",
            // Beyond microsecond resolution: silently rounded.
            "2024-01-01 12:00:00.1234567",
            // Empty fraction.
            "2024-01-01 12:00:00.",
            // Normalized into the next minute by Postgres.
            "2024-01-01 12:00:60",
            "2024-01-01 24:00:00",
            "2024-01-01 12:60:00",
            // A timezone offset makes it a `timestamptz` spelling, which
            // `timestamp_in` silently discards.
            "2024-01-01 12:00:00+02",
            "2024-02-30 12:00:00",
            "",
        ] {
            assert!(
                canonical_timestamp(text).is_err(),
                "{text:?} must not be accepted as a timestamp literal"
            );
        }
    }

    #[test]
    fn canonical_byteas_are_accepted() {
        for text in ["\\x", "\\x00", "\\x0102ff", "\\xdeadbeef"] {
            assert!(canonical_bytea(text).is_ok(), "{text} should be canonical");
        }
    }

    #[test]
    fn non_canonical_byteas_are_rejected() {
        for text in [
            // Legacy `escape` input format.
            "\\000",
            "abc",
            // `byteaout` emits lowercase.
            "\\xDEADBEEF",
            "\\xAb",
            // Odd digit count / non-hex.
            "\\x0",
            "\\x0g",
            "\\x 01",
            "",
            "\\",
            "x0102",
        ] {
            assert!(
                canonical_bytea(text).is_err(),
                "{text:?} must not be accepted as a bytea literal"
            );
        }
    }

    #[test]
    fn canonical_varbits_are_accepted() {
        for text in ["", "0", "1", "101", "00000000", "10100101"] {
            assert!(
                canonical_varbit(text).is_ok(),
                "{text:?} should be canonical"
            );
        }
    }

    #[test]
    fn non_canonical_varbits_are_rejected() {
        for text in ["2", "01x", "0 1", "b101", "0b101", "-1", "true"] {
            assert!(
                canonical_varbit(text).is_err(),
                "{text:?} must not be accepted as a varbit literal"
            );
        }
    }

    #[test]
    fn varbit_is_looked_up_by_its_canonical_uppercase_keyword() {
        assert_eq!(
            lookup_typed_literal("VARBIT").map(|spec| spec.value_type),
            Some(ValueType::Other(PgType::VarBit))
        );
        assert!(lookup_typed_literal("varbit").is_none());
        assert!(lookup_typed_literal("BIT").is_none());
    }
}
