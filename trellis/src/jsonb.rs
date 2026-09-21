//! Postgres's `jsonb` type (issue #115) — the typed-literal ("computed 1-1
//! target") canonical-form checker, and the shared evidence the `jsonb_agg`
//! aggregate role and the join/primary/`GROUP BY` key roles are decided
//! against.
//!
//! # Why there is no `ValueType::Jsonb`
//!
//! Same reasoning as [`crate::temporal`]/[`crate::netaddr`]'s module docs:
//! every role this issue grants stays a function of the family alone, which
//! [`PgType::Jsonb`] already carries (since issue #108), so it stays
//! `ValueType::Other(PgType::Jsonb)`.
//!
//! # `pg_cast`: no second renderer, unlike `boolean`/`inet`
//!
//! Per #119's playbook, checked live rather than assumed:
//!
//! ```sql
//! select castfunc::regproc from pg_cast
//! where castsource = 'jsonb'::regtype and casttarget = 'text'::regtype;
//! -- (0 rows)
//! ```
//!
//! `::text` is `jsonb_out` directly, and `jsonb_in`/`jsonb_out` are both
//! `IMMUTABLE` (`pg_proc.provolatile = 'i'`) — no relative spellings, no GUC
//! read, unlike `date_in`/`timestamp_in`.
//!
//! # Key order is canonicalized; embedded numbers are not — a fresh hazard shape
//!
//! `jsonb_out` re-sorts object keys by **byte length, then bytewise** (not
//! insertion order, not codepoint order):
//!
//! ```sql
//! select '{"bb":1,"a":2,"c":3,"ab":4}'::jsonb::text;
//! -- {"a": 2, "c": 3, "ab": 4, "bb": 1}
//! select '{"é":1,"e":2}'::jsonb::text;
//! -- {"e": 2, "é": 1}   -- "e" is 1 UTF-8 byte, "é" is 2
//! ```
//!
//! and duplicate keys collapse to the last occurrence
//! (`'{"a":1,"a":2}'::jsonb::text` is `{"a": 2}`). Both are exactly what a
//! canonical-form checker can reject outright — an out-of-order or
//! duplicate-keyed literal is simply not accepted, the same "reject rather
//! than guess" posture every other checker in [`super::defs::typed_literal`]
//! takes.
//!
//! What key-order canonicalization does *not* buy is a text-stable key role,
//! and this is the hazard worth stating plainly, because it looks solved
//! once key order is off the table: **embedded numbers are not
//! renormalized the way key order is.**
//!
//! ```sql
//! select '{"a":1}'::jsonb = '{"a":1.0}'::jsonb as eq,
//!        '{"a":1}'::jsonb::text, '{"a":1.0}'::jsonb::text;
//! --  eq | ...          | ...
//! -- ----+--------------+----------------
//! --  t  | {"a": 1}     | {"a": 1.0}
//! ```
//!
//! One value (`jsonb`'s `=` holds), two renderings — structurally the same
//! equivalence-class defect `real`/`double precision`'s `-0`/`0` and
//! `interval`'s `'1 day'`/`'24 hours'` have (`docs/type-support.md`), just
//! discovered one type deeper: a JSON number is stored and re-emitted
//! through `numeric`'s own scale-preserving rendering (`1.50` stays `1.50`,
//! `1.500` stays `1.500`, `1e2` renormalizes to the non-exponential `100`),
//! so any two literal spellings of one JSON number that differ in scale are
//! `jsonb`-equal but `::text`-distinct. `numeric` itself is exactly why this
//! is unsurprising in hindsight — `docs/type-support.md`'s own `numeric`
//! row already states `1.0`≠`1.00` under text match — but it had never been
//! checked whether embedding a number inside a composite container the
//! *container's own* key-order canonicalization might paper over it. It
//! does not: sorting keys and renormalizing numbers are independent axes,
//! and `jsonb_out` only does the first. This is why `jsonb` is **not**
//! added to `catalog::TEXT_STABLE_JOIN_KEY_TYPES` or admitted by
//! `validate::reject_unsupported_group_by_key_type` — the unlock is the
//! same #110 typed key index every other numeric-bearing family
//! (`numeric`/`real`/`double precision`/`timestamptz`) is waiting on, not a
//! new mechanism jsonb-specific.
//!
//! (`-0`/`-0.0` do get normalized away, to plain `0`/`0.0` — verified live —
//! so that particular pair is not itself a hazard; the general
//! arbitrary-scale case above is.)
//!
//! # The typed-literal canonical-form checker
//!
//! [`canonical_jsonb`] is a small recursive-descent validator, not a
//! transformer: it never rewrites a literal into canonical form (no
//! `jsonb_in`/`jsonb_out` reimplementation, contrary to what this module's
//! doc comment in `typed_literal.rs` once predicted would be needed) — it
//! only *checks* that the given text is already exactly what `jsonb_out`
//! would print for itself, matching every other checker in
//! [`super::defs::typed_literal`]. That turns out to be tractable *without*
//! reimplementing `numeric`'s scale-tracking arithmetic: since the checker
//! only has to reject non-canonical spellings rather than accept-and-
//! normalize them, it can simply refuse exponent notation outright (never
//! canonical output) and refuse a negative-zero spelling (`-0`, `-0.0`,
//! ...; verified live to always normalize to positive), and any exponent-
//! free JSON number that survives JSON's own grammar (which already bans a
//! leading zero the way `-01` etc. would need) is automatically canonical —
//! no digit-by-digit renormalization needed.
//!
//! The checker enforces, all verified against a live server:
//!
//! * **Whitespace** — none, except exactly one space after `:` and after
//!   `,` (`jsonb_out`'s `{"a": 1, "b": 2}`/`[1, 2, 3]` separators). No space
//!   after `{`/`[`, none before `}`/`]`, none anywhere else, no leading or
//!   trailing whitespace on the literal as a whole.
//! * **Object keys** — sorted by `(byte length, bytes)` ascending on their
//!   *decoded* content, no duplicates (see above).
//! * **Numbers** — no exponent (`1e2`/`1E2` rejected outright: `jsonb_out`
//!   never emits one), no negative zero, otherwise whatever JSON's own
//!   number grammar accepts is already canonical.
//! * **Strings** — `\"`, `\\`, `\n`, `\t`, `\r`, `\b`, `\f` are the only
//!   short escapes `jsonb_out` uses; every other control character (0x00
//!   through 0x1F, minus those seven) must be `\u00xx` in **lowercase**
//!   hex; anything at or above 0x20 — including the solidus `/` and every
//!   non-ASCII character, verified live for a direct UTF-8 byte, a `\uXXXX`
//!   escape, and a surrogate-pair emoji, all three of which `jsonb_out`
//!   renders as the raw UTF-8 bytes — must appear as its own literal UTF-8
//!   bytes, not an escape. A raw (unescaped) control byte is not merely
//!   non-canonical, it is invalid JSON, and `jsonb_in` itself rejects it the
//!   same way.
//!
//! # `jsonb_agg`'s `STABLE` marking: root-caused, not assumed
//!
//! `pg_proc.provolatile` for `jsonb_agg` is `s` (`STABLE`). The epic's own
//! framing lumped it in with `array_agg`/`string_agg` as "order-sensitive,
//! hence `STABLE`" — checked live, that assumption is wrong on its own
//! terms:
//!
//! ```sql
//! select proname, provolatile from pg_proc
//! where proname in ('array_agg','string_agg','jsonb_agg')
//!   and pronamespace = 'pg_catalog'::regnamespace;
//! --   array_agg  | i      (IMMUTABLE)
//! --   string_agg | i      (IMMUTABLE)
//! --   jsonb_agg  | s      (STABLE)
//! ```
//!
//! `array_agg`/`string_agg` are just as order-sensitive as `jsonb_agg`
//! (none of the three accepts an internal `ORDER BY` clause's *absence* as
//! grounds for `STABLE` — Postgres does not consider "output order depends
//! on scan order" a volatility concern at all), so order-sensitivity is
//! **not** why `jsonb_agg` alone is marked `STABLE`. The real reason,
//! verified live: `jsonb_agg` is polymorphic (`anyelement`) and converts
//! each row through the same machinery `to_jsonb()` uses, which for some
//! argument types reads a session GUC:
//!
//! ```sql
//! set timezone = 'UTC';              select jsonb_agg(v) from tstz;
//! -- ["2024-01-01T12:00:00+00:00"]
//! set timezone = 'America/New_York'; select jsonb_agg(v) from tstz;
//! -- ["2024-01-01T07:00:00-05:00"]
//!
//! select jsonb_agg(v::money) from (values (1.5)) t(v);
//! -- ["$1.50"]     -- lc_monetary-dependent, verified separately
//! ```
//!
//! Postgres cannot declare a polymorphic function's volatility per
//! instantiation — one `pg_proc` row covers every possible argument type —
//! so the whole function is marked `STABLE` to cover the argument types
//! (`timestamptz`, `money`, ...) where the conversion genuinely is. This is
//! the same shape `super::defs::typed_literal`'s module doc documents for
//! `date_in`/`timestamp_in`: `STABLE` because of a *specific, excludable*
//! hazard (there, relative spellings; here, a GUC-dependent conversion for
//! *some* possible argument types), not because of genuine non-determinism
//! in every call.
//!
//! **The exclusion is reachable, and cheap: restrict `JSONB_AGG`'s argument
//! to `jsonb` itself**, rather than accepting it polymorphically the way
//! Postgres's own grammar does. Converting an already-`jsonb` value through
//! `to_jsonb`-equivalent logic is the identity — no GUC lookup happens at
//! all, verified live (a `jsonb` column's `jsonb_agg` is unaffected by
//! `TimeZone`). With the argument type pinned this way,
//! `super::defs::registry::AGGREGATE_FUNCTION_SPECS`'s `JSONB_AGG` row
//! never reaches the hazardous instantiations at all — the same "each
//! aggregate/type pair is checked, not the family as a whole" discipline
//! `BIT_AND`/`BIT_OR`'s `Other` split already established.
//!
//! What is left, once the GUC hazard is excluded, is exactly the
//! ordering concern every order-sensitive aggregate has (and
//! `array_agg`/`string_agg` have too, without being marked `STABLE` for
//! it): without an `ORDER BY` inside the aggregate call — a grammar
//! extension this issue does not add — two recomputes of the same group
//! over the same Postgres heap layout will usually agree, but are not
//! *guaranteed* to (a `VACUUM`/`HOT` update can reshuffle physical row
//! order between them). This is a materially different kind of hazard than
//! `MIN`/`MAX(interval)`'s (`docs/type-support.md`): the *set* of
//! elements a Trellis-computed `jsonb_agg` produces is always correct — no
//! row is fabricated, dropped, or corrupted — only the *array's element
//! order* is not contractually stable across a recompute. `defs::
//! invertibility::classify` marks `JSONB_AGG` `RecomputeOnly`, on
//! `MIN`/`MAX`'s reasoning (a deleted row's contribution cannot be
//! subtracted from a folded array the way a running sum can), the same
//! resting place #120 ("non-numeric & order-sensitive aggregates") is
//! already tracking `array_agg`/`string_agg` toward — this issue does not
//! attempt that broader design, only wires `JSONB_AGG` up to the existing
//! `RecomputeOnly` fallback every other non-invertible aggregate in the
//! registry already gets for free.
//!
//! # `jsonb_agg` is the one aggregate in the registry that does not skip `NULL`
//!
//! Every other aggregate here (`SUM`/`AVG`/`MIN`/`MAX`/`BOOL_AND`/
//! `BOOL_OR`/`BIT_AND`/`BIT_OR`) follows Postgres's ordinary "a `NULL` row
//! contributes nothing, and an aggregate over zero non-`NULL` values is
//! itself `NULL`" rule. `jsonb_agg` does not — verified live, its transition
//! function is not `proisstrict`:
//!
//! ```sql
//! select jsonb_agg(v) from (values ('"a"'::jsonb),(NULL),('"b"'::jsonb)) t(v);
//! -- ["a", null, "b"]      -- the SQL NULL row becomes a JSON null element
//! select jsonb_agg(v) from (values (NULL::jsonb)) t(v);
//! -- [null]                 -- one row, one JSON null element -- not SQL NULL
//! select jsonb_agg(v) from (values (1)) t(v) where false;
//! -- NULL                   -- only a genuinely *empty* group is SQL NULL
//! ```
//!
//! `super::defs::eval`'s `JSONB_AGG` fold path threads each row's `Option<Value>`
//! through as a JSON element (`null` for `None`) instead of filtering `None`
//! out first, unlike every other aggregate's shared fold helper.
//!
//! # `MIN`/`MAX(jsonb)` — not attempted
//!
//! Out of this issue's explicit scope (the issue asks for the key,
//! computed-target and `jsonb_agg` roles only) and not investigated live
//! beyond a spot check that Postgres does give `jsonb` a real `<`/`>`
//! btree opclass — `crate::netaddr`'s and `crate::temporal`'s own
//! `MIN`/`MAX` work is a meaningfully sized undertaking on its own (a
//! comparator that reproduces `jsonb_cmp`'s type-then-structure ordering
//! byte-for-byte), left for a future pass rather than bolted on
//! speculatively here.

const SHAPE: &str = "expected a canonical jsonb literal: the exact text `jsonb_out` would \
     print for itself — object keys sorted by (byte length, then bytes) with no duplicates, \
     `\", \"`/`\": \"` as the only whitespace, no exponent or negative-zero numbers, and no \
     string escape `jsonb_out` itself would not use (e.g. `\\/` for `/`, or `\\u00e9` for a \
     literal non-ASCII character)";

/// Checks `text` is already in `jsonb_out`'s exact canonical spelling — see
/// this module's doc comment for the full rule set and the live evidence
/// behind each clause. A pure validator, never a normalizer: a
/// non-canonical (but perfectly valid) JSON spelling is rejected outright,
/// the same "reject rather than guess" posture
/// [`super::defs::typed_literal`]'s other checkers take.
pub fn canonical_jsonb(text: &str) -> Result<(), &'static str> {
    let parser = Parser {
        bytes: text.as_bytes(),
    };
    let mut pos = 0;
    parser.parse_value(&mut pos)?;
    if pos != parser.bytes.len() {
        return Err(SHAPE);
    }
    Ok(())
}

struct Parser<'a> {
    bytes: &'a [u8],
}

impl Parser<'_> {
    fn peek(&self, pos: usize) -> Option<u8> {
        self.bytes.get(pos).copied()
    }

    fn expect(&self, pos: &mut usize, byte: u8) -> Result<(), &'static str> {
        if self.peek(*pos) == Some(byte) {
            *pos += 1;
            Ok(())
        } else {
            Err(SHAPE)
        }
    }

    fn expect_literal(&self, pos: &mut usize, literal: &str) -> Result<(), &'static str> {
        let bytes = literal.as_bytes();
        if self.bytes[*pos..].starts_with(bytes) {
            *pos += bytes.len();
            Ok(())
        } else {
            Err(SHAPE)
        }
    }

    fn parse_value(&self, pos: &mut usize) -> Result<(), &'static str> {
        match self.peek(*pos) {
            Some(b'{') => self.parse_object(pos),
            Some(b'[') => self.parse_array(pos),
            Some(b'"') => self.parse_string(pos).map(|_| ()),
            Some(b't') => self.expect_literal(pos, "true"),
            Some(b'f') => self.expect_literal(pos, "false"),
            Some(b'n') => self.expect_literal(pos, "null"),
            Some(b'-' | b'0'..=b'9') => self.parse_number(pos),
            _ => Err(SHAPE),
        }
    }

    fn parse_object(&self, pos: &mut usize) -> Result<(), &'static str> {
        self.expect(pos, b'{')?;
        if self.peek(*pos) == Some(b'}') {
            *pos += 1;
            return Ok(());
        }
        let mut prev_key: Option<Vec<u8>> = None;
        loop {
            let key = self.parse_string(pos)?;
            if let Some(prev) = &prev_key
                && key_order(prev, &key) != std::cmp::Ordering::Less
            {
                // Out of order, or a duplicate (`Ordering::Equal`) — either
                // way `jsonb_out` would not print this literal back as
                // written (see this module's doc comment).
                return Err(SHAPE);
            }
            prev_key = Some(key);
            self.expect(pos, b':')?;
            self.expect(pos, b' ')?;
            self.parse_value(pos)?;
            match self.peek(*pos) {
                Some(b',') => {
                    *pos += 1;
                    self.expect(pos, b' ')?;
                }
                Some(b'}') => {
                    *pos += 1;
                    return Ok(());
                }
                _ => return Err(SHAPE),
            }
        }
    }

    fn parse_array(&self, pos: &mut usize) -> Result<(), &'static str> {
        self.expect(pos, b'[')?;
        if self.peek(*pos) == Some(b']') {
            *pos += 1;
            return Ok(());
        }
        loop {
            self.parse_value(pos)?;
            match self.peek(*pos) {
                Some(b',') => {
                    *pos += 1;
                    self.expect(pos, b' ')?;
                }
                Some(b']') => {
                    *pos += 1;
                    return Ok(());
                }
                _ => return Err(SHAPE),
            }
        }
    }

    /// Parses a JSON string starting at `*pos`, returning its *decoded*
    /// content bytes (escapes resolved) — needed both to validate each
    /// escape is the one `jsonb_out` itself would use, and, for an object
    /// key, to compare decoded byte length/content against the previous key
    /// (`jsonb_out` sorts on the decoded string, not its literal spelling —
    /// see this module's doc comment's `"é"` example).
    fn parse_string(&self, pos: &mut usize) -> Result<Vec<u8>, &'static str> {
        self.expect(pos, b'"')?;
        let mut out = Vec::new();
        loop {
            match self.peek(*pos) {
                None => return Err(SHAPE),
                Some(b'"') => {
                    *pos += 1;
                    return Ok(out);
                }
                Some(b'\\') => {
                    *pos += 1;
                    match self.peek(*pos) {
                        Some(b'"') => {
                            out.push(b'"');
                            *pos += 1;
                        }
                        Some(b'\\') => {
                            out.push(b'\\');
                            *pos += 1;
                        }
                        Some(b'n') => {
                            out.push(b'\n');
                            *pos += 1;
                        }
                        Some(b't') => {
                            out.push(b'\t');
                            *pos += 1;
                        }
                        Some(b'r') => {
                            out.push(b'\r');
                            *pos += 1;
                        }
                        Some(b'b') => {
                            out.push(0x08);
                            *pos += 1;
                        }
                        Some(b'f') => {
                            out.push(0x0c);
                            *pos += 1;
                        }
                        Some(b'u') => {
                            *pos += 1;
                            let cp = self.parse_hex4(pos)?;
                            // Canonical only for a control character with no
                            // short escape of its own (0x08/0x09/0x0a/0x0c/
                            // 0x0d must use `\b`/`\t`/`\n`/`\f`/`\r`
                            // instead) — see this module's doc comment.
                            if cp >= 0x20 || matches!(cp, 0x08 | 0x09 | 0x0a | 0x0c | 0x0d) {
                                return Err(SHAPE);
                            }
                            out.push(cp as u8);
                        }
                        // `\/` and any other escape are not canonical —
                        // `jsonb_out` never emits an escaped solidus, and
                        // no other backslash escape exists in JSON.
                        _ => return Err(SHAPE),
                    }
                }
                // A raw, unescaped control byte is not merely
                // non-canonical, it is invalid JSON — `jsonb_in` rejects it
                // too (verified live).
                Some(b) if b < 0x20 => return Err(SHAPE),
                Some(b) => {
                    out.push(b);
                    *pos += 1;
                }
            }
        }
    }

    /// Exactly four lowercase hex digits (`jsonb_out` never emits uppercase
    /// hex in a `\u00xx` escape), returning the parsed code point.
    fn parse_hex4(&self, pos: &mut usize) -> Result<u32, &'static str> {
        let bytes = self.bytes.get(*pos..*pos + 4).ok_or(SHAPE)?;
        if !bytes
            .iter()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(b))
        {
            return Err(SHAPE);
        }
        let text = std::str::from_utf8(bytes).map_err(|_| SHAPE)?;
        let value = u32::from_str_radix(text, 16).map_err(|_| SHAPE)?;
        *pos += 4;
        Ok(value)
    }

    /// A JSON number with no exponent (`jsonb_out` never emits one — any
    /// literal spelling one would round-trip through is rejected outright,
    /// not normalized) and no negative-zero spelling (`-0`/`-0.0`/...,
    /// which `jsonb`'s underlying `numeric` storage always normalizes to
    /// positive — verified live). Every other exponent-free spelling JSON's
    /// own number grammar accepts is already canonical, since `jsonb`
    /// preserves a number's input scale verbatim (`1.50` stays `1.50`).
    fn parse_number(&self, pos: &mut usize) -> Result<(), &'static str> {
        let start = *pos;
        if self.peek(*pos) == Some(b'-') {
            *pos += 1;
        }
        let digits_start = *pos;
        match self.peek(*pos) {
            Some(b'0') => *pos += 1,
            Some(b'1'..=b'9') => {
                while matches!(self.peek(*pos), Some(b'0'..=b'9')) {
                    *pos += 1;
                }
            }
            _ => return Err(SHAPE),
        }
        if self.peek(*pos) == Some(b'.') {
            *pos += 1;
            let frac_start = *pos;
            while matches!(self.peek(*pos), Some(b'0'..=b'9')) {
                *pos += 1;
            }
            if *pos == frac_start {
                return Err(SHAPE);
            }
        }
        if matches!(self.peek(*pos), Some(b'e' | b'E')) {
            return Err(SHAPE);
        }
        let negative = self.bytes[start] == b'-';
        let all_zero = self.bytes[digits_start..*pos]
            .iter()
            .all(|&b| b == b'0' || b == b'.');
        if negative && all_zero {
            return Err(SHAPE);
        }
        Ok(())
    }
}

/// `jsonb_out`'s object-key sort order: ascending by decoded byte length,
/// then bytewise — verified live (`"e"` sorts before `"é"`: 1 UTF-8 byte
/// against 2). `Ordering::Equal` means a duplicate key, which
/// [`Parser::parse_object`] also rejects via this same comparison.
fn key_order(a: &[u8], b: &[u8]) -> std::cmp::Ordering {
    a.len().cmp(&b.len()).then_with(|| a.cmp(b))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn canonical_literals_are_accepted() {
        for text in [
            "null",
            "true",
            "false",
            "0",
            "-1",
            "1.50",
            "1.500",
            "0.000000001",
            "-1.50",
            "100000000000000000000",
            "\"\"",
            "\"hi\"",
            "\"a/b\"",
            "\"caf\u{e9}\"",
            "\"\\n\\t\\r\\b\\f\\\"\\\\\"",
            "\"\\u0001\\u001f\"",
            "{}",
            "[]",
            "[1, 2, 3]",
            "{\"a\": 1, \"b\": 2}",
            "{\"a\": 2, \"c\": 3, \"ab\": 4, \"bb\": 1}",
            "{\"a\": 1, \"b\": [1, 2], \"c\": {\"x\": 1}}",
        ] {
            assert!(
                canonical_jsonb(text).is_ok(),
                "{text:?} should be canonical"
            );
        }
    }

    #[test]
    fn non_canonical_literals_are_rejected() {
        for text in [
            "",
            " ",
            "1 ",
            " 1",
            "01",
            "1.",
            ".1",
            "1e2",
            "1E2",
            "1.5e10",
            "-0",
            "-0.0",
            "-0.00",
            "{ \"a\": 1}",
            "{\"a\" : 1}",
            "{\"a\":1}",
            "{\"a\": 1,\"b\": 2}",
            "[1,2]",
            "[1, 2,3]",
            // out of order / duplicate keys
            "{\"b\": 1, \"a\": 2}",
            "{\"bb\": 1, \"a\": 2}",
            "{\"a\": 1, \"a\": 2}",
            // non-canonical string escapes
            "\"\\/\"",
            "\"\\u00e9\"",
            "\"\\u0008\"",
            "\"\\u0009\"",
            "\"\\u000a\"",
            "\"\\u000A\"",
            "\"\\u001F\"",
            // trailing garbage
            "1x",
            "{}x",
            "nul",
            "truee",
        ] {
            assert!(
                canonical_jsonb(text).is_err(),
                "{text:?} must not be accepted as a jsonb literal"
            );
        }
    }

    #[test]
    fn key_order_matches_live_postgres_byte_length_then_bytewise_rule() {
        assert_eq!(
            key_order(b"e", "\u{e9}".as_bytes()),
            std::cmp::Ordering::Less
        );
        assert_eq!(key_order(b"a", b"z"), std::cmp::Ordering::Less);
        assert_eq!(key_order(b"a", b"a"), std::cmp::Ordering::Equal);
        assert_eq!(key_order(b"bb", b"ab"), std::cmp::Ordering::Greater);
    }
}
