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

/// One type keyword the typed-literal grammar accepts, and the check its
/// literal text must pass.
#[derive(Clone, Copy)]
pub struct TypedLiteralSpec {
    /// The canonical, uppercased type keyword this literal is spelled with
    /// (`DATE`, `TIMESTAMP`, ...). Matched case-insensitively by
    /// [`super::parser`] against an already-uppercased identifier, so
    /// `date '...'`, `Date '...'` and `DATE '...'` are all the same.
    ///
    /// Always equal to the uppercase of [`PgType::sql_type_name`], so the
    /// spelling a user writes is the spelling the oracle renders back — the
    /// `type_keyword_matches_pg_type` test pins that.
    pub keyword: &'static str,
    /// The family this literal produces, which becomes the field's
    /// [`ValueType::Other`] and hence its target column's Postgres type
    /// (`super::ddl::pg_type_name`).
    pub pg_type: PgType,
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
/// * **`timestamptz`** (#113) — value comparison is immutable but *text
///   rendering* is not: `timestamptz_out` formats in the session's `TimeZone`,
///   so the same stored instant reaches the evaluator as different text on
///   two connections. `docs/type-support.md` flags this as the reason the
///   typed key index is the real unlock; it applies to a computed value
///   travelling as text just as much as to a key.
/// * **`time`, `timetz`, `interval`** (#113) — same canonical-form work as
///   `date`/`timestamp`, deferred with the rest of the temporal family
///   rather than half-landed here; `interval` additionally has no canonical
///   text at all (`'1 day'` and `'24 hours'` are `=` but render
///   differently), which is its own design question.
/// * **`oid`, `inet`, `cidr`, `macaddr`, `macaddr8`, `bit`, `varbit`** —
///   each waits for its own child (#111, #116, #118).
/// * **`money`, `json`, `xml`, `tsvector`, `tsquery`** — excluded from the
///   whole epic by `docs/type-support.md` (locale-dependent text I/O, or no
///   useful immutable equality), so they can never earn a row here.
/// * **[`PgType::Unrecognized`]** — not a Postgres type keyword at all (it
///   renders as `text` in SQL), so it has nothing to spell.
pub const TYPED_LITERALS: &[TypedLiteralSpec] = &[
    TypedLiteralSpec {
        keyword: "DATE",
        pg_type: PgType::Date,
        canonical: canonical_date,
    },
    TypedLiteralSpec {
        keyword: "TIMESTAMP",
        pg_type: PgType::Timestamp,
        canonical: canonical_timestamp,
    },
    TypedLiteralSpec {
        keyword: "BYTEA",
        pg_type: PgType::Bytea,
        canonical: canonical_bytea,
    },
];

/// Looks up a typed-literal type keyword by its canonical uppercased name —
/// the [`TYPED_LITERALS`] counterpart to
/// [`super::registry::lookup_function`].
pub fn lookup_typed_literal(keyword: &str) -> Option<&'static TypedLiteralSpec> {
    TYPED_LITERALS.iter().find(|spec| spec.keyword == keyword)
}

/// The [`ValueType`] a typed literal of `pg_type` carries. Always
/// [`ValueType::Other`] today: the allowlist holds only families that have
/// no first-class [`ValueType`] variant, which is the whole point — a family
/// that *does* have one (`Numeric`, `Text`, ...) already has literal syntax
/// of its own and needs nothing here.
pub fn value_type(pg_type: PgType) -> ValueType {
    ValueType::Other(pg_type)
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
pub fn render_sql(pg_type: PgType, text: &str) -> String {
    format!(
        "'{}'::{}",
        text.replace('\'', "''"),
        pg_type.sql_type_name()
    )
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
    /// keyword the oracle renders back (`PgType::sql_type_name`), or a
    /// definition would parse under one spelling and render under another.
    #[test]
    fn type_keyword_matches_pg_type() {
        for spec in TYPED_LITERALS {
            assert_eq!(
                spec.keyword,
                spec.pg_type.sql_type_name().to_ascii_uppercase(),
                "{} must be spelled as its own pg type keyword",
                spec.keyword
            );
        }
    }

    /// The allowlist may only hold families with no first-class
    /// [`ValueType`] variant — see [`value_type`].
    #[test]
    fn every_allowlisted_family_is_an_other_value_type() {
        for spec in TYPED_LITERALS {
            assert_eq!(value_type(spec.pg_type), ValueType::Other(spec.pg_type));
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
                !TYPED_LITERALS.iter().any(|spec| spec.pg_type == excluded),
                "{excluded} is excluded by docs/type-support.md and must not be spellable"
            );
        }
    }

    #[test]
    fn lookup_is_by_canonical_uppercase_keyword() {
        assert_eq!(
            lookup_typed_literal("DATE").map(|spec| spec.pg_type),
            Some(PgType::Date)
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
}
