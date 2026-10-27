//! The join-key type allowlist: which Postgres types the engine can match
//! as keys by their `::text` rendering. A `JOIN`'s `ON` columns
//! ([`super::validate::validate_join`]), a 1-1 source's primary key
//! ([`super::ddl::source_primary_key`]) and the copied key types
//! ([`super::key_types`]) all gate on it.

use std::borrow::Cow;

use tokio_postgres::GenericClient;

/// Strips a `format_type` rendering's `(...)` type modifier, wherever it
/// sits, leaving the bare type name: `character varying(255)` and
/// `numeric(10,2)` become `character varying` and `numeric`, and
/// `timestamp(3) without time zone` becomes `timestamp without time zone`.
///
/// That last case is why this is a function rather than the
/// `split('(').next()` that would suffice if every type name carried its
/// modifier as a *suffix*, as the non-temporal names do. The SQL-standard
/// temporal names do not: `format_type` renders a `timestamp(3)` column as
/// `timestamp(3) without time zone`, which truncates to `timestamp` — a
/// string matching neither the unmodified `timestamp without time zone` in
/// [`TEXT_STABLE_JOIN_KEY_TYPES`] nor the same column declared without a
/// precision. Left alone, a sub-second-precision `timestamp` key would have
/// been silently refused.
///
/// A precision modifier never affects text-stability, incidentally: it
/// rounds on *input* (`'12:00:00.5678'::time(2)` stores `12:00:00.57`), and
/// what is stored still renders canonically.
pub(crate) fn base_type_name(pg_type: &str) -> Cow<'_, str> {
    let trimmed = pg_type.trim();
    let Some(open) = trimmed.find('(') else {
        return Cow::Borrowed(trimmed);
    };
    let Some(close) = trimmed[open..].find(')').map(|offset| open + offset) else {
        return Cow::Borrowed(trimmed);
    };
    let head = trimmed[..open].trim_end();
    let tail = trimmed[close + 1..].trim_start();
    if tail.is_empty() {
        return Cow::Borrowed(head);
    }
    Cow::Owned(format!("{head} {tail}"))
}

/// Postgres type base names (modifier already stripped by
/// [`base_type_name`]) whose equality is *text-stable* — `a::text = b::text`
/// agrees with the type's native typed `=` for every value. This is a
/// positive allowlist, and a per-type fact: `character`'s native `=` is
/// blank-padding-insensitive while its `::text` rendering is blank-padded, so
/// it is off the list even though `character varying` and `text` are on it.
///
/// Only the join key's *own* type matters for this list, not what it's
/// compared against, so allowed/rejected status is a per-type fact.
/// Anything not named here — `numeric` (`1.0::text` != `1.00::text` though
/// numerically equal), `real`/`double precision` (`-0` and `0`
/// are `=` in Postgres but render as `'-0'` and `'0'`, so text matching
/// splits one value across two keys; verified on a live server, and the
/// unlock would be a typed key index comparing decoded values through
/// `crate::float::compare`, not a new encoding), `character`/`citext`
/// (blank-padding or
/// case-insensitivity native to the type but not its `::text` form),
/// `interval` (`timestamp` is admitted alongside `date`/`time`/
/// `timetz` because every renderer agrees on it, and
/// `timestamptz` because `TimeZone` is pinned on every
/// renderer; see the temporal block below for why `interval` alone stays
/// off), `boolean`, `json`/`jsonb`, or any unknown type — is rejected as a
/// join key.
///
/// `bytea` joins the list below on the same "text-stability is
/// a property of the rendering" reasoning `oid` and four of the six temporal
/// families were admitted under — `docs/type-support.md` had it marked
/// `🎯 typed index`, and that turned out to be exactly the wrong prediction
/// again. `byteaout` under the already-pinned `bytea_output = 'hex'`
/// (`crate::pool::DETERMINISTIC_TEXT_OUTPUT_GUCS`) is a bijection: every
/// distinct byte string has exactly one canonical `\x`-prefixed, lowercase,
/// even-length hex spelling, and every such spelling names exactly one byte
/// string — there is no `bytea` analogue of float's `-0`/`0` or interval's
/// `'1 day'`/`'24 hours'`, because a fixed-width positional encoding cannot
/// produce two spellings of the same value. Verified on a live server: a
/// grid spanning the empty value, embedded `NUL` bytes, and every byte value
/// from `0x00` to `0xff` produces exactly as many distinct `::text` groups as
/// distinct values (`defs_bytea.rs`'s
/// `bytea_text_rendering_is_a_bijection_under_hex_output`). The type
/// matrix's scope note ("key role needs decoded comparison") assumed a typed
/// key index was the prerequisite; asked of a live server, it is
/// not — pinning `bytea_output` alone already makes raw `::text` matching
/// agree with `bytea`'s native `=` for every value.
pub(crate) const TEXT_STABLE_JOIN_KEY_TYPES: &[&str] = &[
    "smallint",
    "integer",
    "bigint",
    // `oid` is an *unsigned* 32-bit integer whose `oid_out`
    // rendering is canonical decimal — no sign, no leading zeros, no
    // padding — so `a::text = b::text` agrees with `oid`'s native `=` for
    // every value, exactly as it does for the three signed widths above.
    // It is not a `ValueType::Integer` (Postgres gives it no arithmetic at
    // all; see `pg_type::PgType::Oid`), but text-stability is a property of
    // the *rendering*, not of the operator set, so it belongs here.
    "oid",
    "uuid",
    "text",
    "character varying",
    // Four of the six temporal families, on the same
    // "text-stability is a property of the rendering" reasoning `oid` was
    // admitted under. `docs/type-support.md` had all six marked `🎯 typed
    // index`, assuming a typed key index was the prerequisite; for
    // these four it is not, and the evidence is per-family rather than
    // per-block — see `crate::temporal`'s module doc for the live queries.
    //
    // `date_out` under the already-pinned `DateStyle = 'ISO, YMD'`
    // (`crate::pool::DETERMINISTIC_TEXT_OUTPUT_GUCS`) is a bijection on its
    // values: the year field widens (`5874897-12-31`), the era is an
    // explicit ` BC` suffix, and `infinity`/`-infinity` have their own
    // spellings. `time_out` and `timetz_out` need no GUC at all — verified
    // identical under `ISO`, `SQL`, `Postgres` and `German` `DateStyle`s
    // and under three session `TimeZone`s.
    //
    // `timetz` is the one to double-take on, and it is safe for a reason
    // opposite to the obvious one: its `=` is *narrower* than "same instant
    // of day", not wider. `select '12:00:00+00'::timetz =
    // '17:30:00+05:30'::timetz` is **false** — `timetz_cmp_internal` sorts
    // by GMT-equivalent time and then by zone, so equality is identity on
    // the stored `(time, zone)` pair, which is exactly what `timetz_out`
    // prints.
    //
    // `timestamp without time zone` is a bijection under `::text` too, and
    // it would be refused if `::text` were not the engine's only renderer.
    // A bare `to_jsonb(t.*)` row body renders a `timestamp` through
    // `jsonb`'s ISO-8601 writer: `2024-06-15T12:34:56`, not
    // `2024-06-15 12:34:56`, and a key seeded once each way would become two
    // target rows for one Postgres group. The live-row reads therefore build
    // an explicit per-column `jsonb_build_object` rendered the same way
    // `::text` is (`staging::apply::row_as_text_jsonb_sql`), which is why
    // `timestamp` joins `date`/`time`/`timetz` here — see
    // `crate::temporal::is_render_consistent`.
    //
    // One remaining deliberate absence:
    //
    // * `interval` — `'24 hours'` and `'1 day'` are one value with two
    //   renderings. The float `-0`/`0` defect; no GUC and no renderer
    //   reconciliation fixes it, though a typed key index would.
    "date",
    "time without time zone",
    "time with time zone",
    "timestamp without time zone",
    // `timestamp with time zone` joins the four above.
    // `timestamptz_out` renders the stored instant in the session's `TimeZone`,
    // so it needs two independent properties to be text-stable — the
    // `to_jsonb`/`::text` reconciliation (which it shares with
    // `timestamp`) and, separately, `TimeZone` itself being pinned identically
    // everywhere Trellis renders a value. The pool pins
    // `crate::pool::DETERMINISTIC_TEXT_OUTPUT_GUCS` (which includes `TimeZone =
    // 'UTC'`) on every connection, and each capture function pins the same
    // settings as `SET` clauses (`crate::capture::sql`), so `timestamptz_out`
    // is a bijection on the instant in both. See
    // `crate::temporal::is_bijective_under_text`/ `is_render_consistent` for
    // the per-family verdict this list mirrors.
    "timestamp with time zone",
    // `byteaout` under the pinned `bytea_output = 'hex'` is a
    // bijection on its values — see the doc comment above for the live
    // evidence. `bytea` has no length/precision modifier, so
    // `base_type_name` never has anything to strip for it.
    "bytea",
    // `cidr`/`macaddr`/`macaddr8` join the list; `inet` — despite
    // sharing `cidr`'s `pg_cast` row — deliberately does not. Full account in
    // `crate::netaddr`'s module doc, condensed here:
    //
    // `macaddr`/`macaddr8` have no `pg_cast` row for `text` at all (checked
    // the same way `boolean`'s was checked), so their `::text` *is*
    // `macaddr_out`/`macaddr8_out`, and both are bijections — every accepted
    // input spelling (colon/hyphen/dot-grouped/bare hex) normalizes on
    // output to one canonical lowercase colon-separated form.
    //
    // `inet` and `cidr` share one `pg_cast` row (`pg_catalog.text(inet)`,
    // `prosrc = network_show`, reused for `cidr` because its on-disk
    // representation *is* an `inet` with host bits forced to zero) — but
    // whether that second renderer actually diverges from the type's own
    // output function is a per-type fact, not a per-row one. `cidr_out`
    // never omits the netmask (a `cidr` value's whole point is that the
    // network prefix matters), so `cidr_out(v)::text = v::text` holds for
    // every value — verified live across a v4/v6 grid including the
    // host-bits-zero-only values `cidr_in` accepts. `inet_out` — what a
    // capture trigger images, and what it builds a staged `key` from —
    // *does* diverge from `network_show`: it omits the `/prefixlen` suffix
    // exactly when the stored netmask covers the whole address
    // (`'192.168.1.5'::inet::text` via `inet_out` is `192.168.1.5`; via the
    // cast, `192.168.1.5/32`), the identical "one value, two renderers, no
    // arbiter" shape `boolean` has above — so a raw `{col}::text = $1`
    // match would silently mismatch `inet` exactly the way `boolean`
    // would. `inet` is refused here for that reason;
    // its `GROUP BY` key role is still admitted, the same split `boolean`
    // got — see `validate::reject_unsupported_group_by_key_type`'s `Inet`
    // arm.
    "cidr",
    "macaddr",
    "macaddr8",
    // `boolean` is deliberately *not* here, and the reason is
    // new — every other admission/refusal on this list turns on whether
    // `<type>_out` (the type's own output function, what capture images and
    // what `<col>::text` normally reduces to) is a bijection.
    // `boolout` *is* one (`'t'`/`'f'`, nothing else). The problem is that
    // `<col>::text` does not call `boolout` at all: `select castfunc::regproc
    // from pg_cast where castsource = 'boolean'::regtype and casttarget =
    // 'text'::regtype` names `pg_catalog.text(boolean)`, a *second*,
    // dedicated cast function Postgres ships only for `boolean`, which
    // renders the SQL-standard `'true'`/`'false'` instead of `boolout`'s
    // `'t'`/`'f'`. Checked against every other type on this list
    // (`smallint`/`integer`/`bigint`/`oid`/`uuid`/`text`/`character
    // varying`/the four temporal families/`bytea`) and none has a
    // `pg_cast` row for `text` at all — their `::text` *is* their output
    // function, which is exactly why raw-text matching has been sound for
    // all of them. `boolean` is the only type on the block with a second,
    // independent renderer, and it is silent: nothing here fails to type
    // or parse, two spellings of one value just stop comparing equal.
    //
    // That is live-load-bearing, not theoretical: a capture trigger images a
    // column with `format('%s', col)`, which calls `boolout`, and builds a
    // primary key's staged `key` text from that — `'t'`/`'f'`. A *bulk* key
    // lookup that casts the *bound parameter* to the column's native type
    // (`col = any($1::text[]::boolean[])`) calls `boolin` — an
    // input function far more permissive than `boolout` is strict, so it
    // parses `'t'`/`'f'` and `'true'`/`'false'` alike back to the same
    // native value and the divergence never surfaces there. But a
    // *scalar* single-key lookup in the older, unindexed
    // `{key}::text = $1` form would compare a live column's
    // `pg_catalog.text(boolean)` rendering (`'true'`/`'false'`) against a
    // CDC-derived `key` built from `boolout` (`'t'`/`'f'`) — and those two
    // strings are simply not equal. Admitting `boolean` here today would
    // silently mismatch exactly the shape `timestamp` would have under
    // `to_jsonb` (one value, two renderers, no arbiter), just from a
    // different renderer pair, since `boolean` is the only type with this
    // second cast.
    //
    // The `GROUP BY` key role does *not* have this problem at the final SQL
    // layer — see `validate::reject_unsupported_group_by_key_type`'s own
    // `Boolean` arm — because the group basis (`staging::group_basis`) parses
    // a group key's text into its native type, never a bare `col::text` comparison,
    // so it never touches `pg_catalog.text (boolean)` at all. Extending the SQL-layer
    // native-array-cast pattern to the remaining scalar lookup sites (or
    // a typed key index, which would subsume it) is what would let
    // `boolean` join this list; until then it stays off. See
    // `trellis/tests/defs_boolean.rs` for the live evidence and
    // `docs/type-support.md`'s "Boolean semantics" section.
    //
    // `bit` and `bit varying` join this list on the *original*
    // "text-stability is a property of the rendering" reasoning `oid`/
    // `bytea`/four of the six temporal families were admitted under —
    // checked against `pg_cast`, not assumed, as every family on this list
    // must be. `select
    // castfunc::regproc from pg_cast where castsource in
    // ('bit'::regtype, 'varbit'::regtype) and casttarget = 'text'::regtype`
    // returns **no rows at all** on a live Postgres 17: unlike `boolean`,
    // neither bit-string type has a second, dedicated `::text` cast — their
    // `::text` *is* `bit_out`/`varbit_out`, exactly like every other type on
    // this list except `boolean`. `bit_out`/`varbit_out` are `IMMUTABLE`,
    // read no GUC, and render a bit string as its literal `'0'`/`'1'`
    // characters with no separator, no padding beyond the value's own stored
    // bits, and no second spelling of any value — a fixed-alphabet,
    // fixed-width-per-symbol encoding has no `bytea`-hex-style ambiguity to
    // begin with, so it needs no positional-encoding argument the way
    // `bytea`'s bijection proof did either.
    //
    // Both spellings are admitted *here* — this role never asks a bare
    // `ValueType` to declare a brand-new column, it only ever compares an
    // already-existing one (`{col} = any($1::text[]::{ty}[])`, via
    // `staging::apply`'s `key_column_pg_type`, which introspects the
    // column's own concrete `format_type` — `bit(5)`, not bare `bit` — so a
    // fixed-length key's own width is never lost) or, for a 1-1 primary key,
    // copies the source's concrete introspected type verbatim
    // (`ddl::source_primary_key`). Fixed-length `bit` is *not* admitted as a
    // `GROUP BY` key for a completely different, DDL-only reason — see
    // `validate::reject_unsupported_group_by_key_type`'s `VarBit` arm.
    "bit",
    "bit varying",
    // `jsonb` is deliberately **absent**, and — unlike
    // `boolean`/`inet` — not because of a second, disagreeing `::text`
    // renderer: `select castfunc from pg_cast where castsource =
    // 'jsonb'::regtype and casttarget = 'text'::regtype` returns no rows,
    // so `::text` is `jsonb_out` directly, and `jsonb_out` genuinely
    // canonicalizes object key order (`{"b":1,"a":2}` and `{"a":2,"b":1}`
    // both render `{"a": 2, "b": 1}`). The hazard is one level deeper: an
    // embedded JSON *number* preserves its literal input scale the way
    // `numeric` itself does, so `'{"a":1}'::jsonb` and
    // `'{"a":1.0}'::jsonb` are `=` (verified live) but render as
    // `{"a": 1}` and `{"a": 1.0}` — one value, two renderings, the same
    // equivalence-class shape `real`/`double precision`'s `-0`/`0` and
    // `interval`'s `'1 day'`/`'24 hours'` have. See `crate::jsonb`'s
    // module doc for the full live evidence and `trellis/tests/
    // defs_jsonb.rs`. The unlock is a typed key index, exactly as it
    // is for `numeric`/`real`/`double precision`/`timestamptz` above —
    // `jsonb` needs no jsonb-specific mechanism, just the same one every
    // other numeric-bearing family here is waiting on.
];

/// [`TEXT_STABLE_JOIN_KEY_TYPES`] rendered for a user-facing error message,
/// so the "supported join key types are ..." list in
/// [`super::validate::ValidationError::JoinUnsupportedKeyType`]'s `Display`
/// cannot drift out of step with the allowlist it describes.
///
/// A hand-maintained second copy of the list would drift whenever a family
/// is admitted.
pub(crate) fn supported_join_key_types() -> String {
    TEXT_STABLE_JOIN_KEY_TYPES.join(", ")
}

/// Whether `pg_type` (a `format_type` rendering, e.g. `integer`, `character
/// varying(255)`) is in [`TEXT_STABLE_JOIN_KEY_TYPES`] — the single source of truth
/// both [`super::validate::validate_join`] (`JOIN` keys) and
/// [`super::ddl::source_primary_key`] (1-1 transform primary keys) gate on. Any
/// modifier (`(255)`, `(10,2)`, `(3)`) is stripped by [`base_type_name`] before
/// matching, so `varchar(255)` and `varchar(100)` are both recognized as `character
/// varying` — and `timestamp(3) without time zone` as `timestamp without time zone`
/// — rather than falling through to the catch-all rejection as distinct strings.
pub(crate) fn is_text_stable_join_key_type(pg_type: &str) -> bool {
    let base = base_type_name(pg_type);
    // A copy renders `bit` as the quoted `"bit"` ([`super::copy_type`]).
    TEXT_STABLE_JOIN_KEY_TYPES.contains(&base.trim_matches('"'))
}

/// The dynamic counterpart to [`TEXT_STABLE_JOIN_KEY_TYPES`]/
/// [`is_text_stable_join_key_type`], for enum types: `pg_type`
/// (a `format_type` rendering, the same string [`is_text_stable_join_key_type`]
/// tests) cannot be enumerated in a fixed list the way every other family
/// here can, because `CREATE TYPE ... AS ENUM` mints an arbitrary,
/// per-schema type with a dynamically-assigned OID — there is no fixed set
/// of enum type names to write down ahead of time. `to_regtype` resolves
/// `pg_type` the same way Postgres itself would resolve any other type name
/// reference (honoring `search_path`/schema-qualification), returning `NULL`
/// rather than erroring for anything it can't resolve as a bare type name —
/// including a modifier-bearing rendering like `numeric(10,2)` or an array
/// type like `text[]`, neither of which is a bare name `to_regtype` accepts
/// — so a `false` result here is always safe: it only ever means "not a
/// live enum type", never a hard error, for any input the static allowlist
/// above already rejects.
///
/// Admitting the result is safe on the same "text-stability is a property
/// of the rendering, not of a fixed list" grounds every other family on this
/// list is admitted under: `enumout` (what `<col>::text` calls
/// — verified live against `pg_cast`, `trellis/tests/defs_enum.rs`) renders
/// each value as its own label text verbatim, with no separator, no
/// escaping, and no GUC dependence. Unlike `bytea`'s hex encoding or
/// `bit`'s fixed alphabet, an enum value's identity *is* its label text (a
/// `pg_enum` row keyed by OID, `enumlabel` the payload) — there is no
/// deeper representation for two distinct values to collide under, so this
/// is a bijection by construction, not merely by the absence of a
/// discovered counterexample.
pub(crate) async fn is_enum_type_name(
    client: &impl GenericClient,
    pg_type: &str,
) -> Result<bool, tokio_postgres::Error> {
    let base = base_type_name(pg_type);
    let row = client
        .query_one(
            "select coalesce( \
                 (select typtype = 'e' from pg_catalog.pg_type where oid = to_regtype($1)), \
                 false \
             )",
            &[&base.as_ref()],
        )
        .await?;
    Ok(row.get(0))
}
