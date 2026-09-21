//! Postgres's **network-address** types — `inet`, `cidr`, `macaddr` and
//! `macaddr8` (issue #116) — given the ordering, key/`GROUP BY`, and
//! typed-literal roles `docs/type-support.md` marked in scope, on top of the
//! passthrough classification issue #108 gave them.
//!
//! # Why there is no `ValueType::NetAddr`
//!
//! Same reasoning as [`crate::temporal`]'s module doc: every role this
//! issue grants any of the four families is a function of the family alone,
//! which [`PgType`] already carries, so they stay `ValueType::Other(PgType::X)`.
//!
//! # The four families do *not* land the same way
//!
//! Issue #116's own scope note groups all four together and marks `MIN`/
//! `MAX` `⚠️` across the board. Checked against a live server per each of
//! #111–#119's playbooks (never assumed from the type's name), the four
//! diverge in three different directions — the epic's most differentiated
//! outcome yet:
//!
//! ```text
//!            join/PK/GROUP BY key   MIN/MAX
//! inet       GROUP BY only          ✅ (returns inet)
//! cidr       ✅ all three            ❌ (no such aggregate — see below)
//! macaddr    ✅ all three            ❌ (no such aggregate at all)
//! macaddr8   ✅ all three            ❌ (no such aggregate at all)
//! ```
//!
//! ## `pg_cast`: `inet`/`cidr` share a second renderer; `macaddr`/`macaddr8` don't
//!
//! Per #119's playbook, the load-bearing check is `pg_cast`, not the type's
//! name:
//!
//! ```sql
//! select castsource::regtype, castfunc::regproc
//! from pg_cast
//! where castsource in ('inet'::regtype, 'cidr'::regtype,
//!                       'macaddr'::regtype, 'macaddr8'::regtype)
//!   and casttarget = 'text'::regtype;
//! --  cidr | pg_catalog.text
//! --  inet | pg_catalog.text
//! ```
//!
//! `macaddr`/`macaddr8` have **no** `pg_cast` row for `text` at all — their
//! `::text` *is* `macaddr_out`/`macaddr8_out`, exactly the property every
//! type on `catalog::TEXT_STABLE_JOIN_KEY_TYPES` needs, and both output
//! functions are already a bijection: every accepted input spelling
//! (colon-, hyphen-, dot-grouped, or bare hex) normalizes to one canonical
//! lowercase colon-separated form on output, verified live across all four
//! input shapes. They join the allowlist exactly the way `bytea` did in
//! #114.
//!
//! `inet` and `cidr` share **one** `pg_cast` row — `pg_catalog.text(inet)`,
//! `prosrc = network_show` — reused for both source types because `cidr`'s
//! on-disk representation *is* an `inet` with host bits forced to zero.
//! Whether that second renderer actually diverges from the type's own
//! output function (`inet_out`/`cidr_out`) turns out to depend on which of
//! the two types it is:
//!
//! * **`inet` diverges, live and reachable — the exact `boolean` shape.**
//!   `inet_out` (what CDC/`pgoutput` decodes, and what `intake::extract_key`
//!   stores verbatim) omits the `/prefixlen` suffix exactly when the stored
//!   netmask covers the whole address (`'192.168.1.5'::inet::text` via
//!   `inet_out` is `192.168.1.5`), while `network_show` (the cast, what
//!   `<col>::text` and hence `staging::apply::row_as_text_jsonb_sql`'s live
//!   reads call) *always* prints it explicitly (`192.168.1.5/32`). Verified
//!   on a live server across a v4/v6 grid: the two renderers disagree on
//!   every "bare host address" value and agree on every value with an
//!   explicit sub-maximal netmask. One value, two renderers, no arbiter —
//!   `catalog::TEXT_STABLE_JOIN_KEY_TYPES`'s doc comment has `boolean`'s
//!   account of why that is fatal for the join/primary-key role
//!   specifically (`staging::apply::check_reverse_guards` and its scalar
//!   siblings still do raw `{col}::text = $1` matching), and the same
//!   reasoning applies here verbatim. `inet` is *not* added to that
//!   allowlist.
//! * **`cidr` does not diverge, anywhere.** `cidr_out` never omits the
//!   netmask — a `cidr` value's whole *point* is that the network prefix is
//!   significant, so both the low-level output function and `network_show`
//!   print it unconditionally. Verified live across the same grid (plus the
//!   host-bits-must-be-zero values `cidr_in` alone accepts):
//!   `cidr_out(v)::text = v::text` for every value tried, no exceptions. So
//!   unlike `inet`, `cidr`'s `::text` genuinely *is* (in effect) its own
//!   output function, and it joins `catalog::TEXT_STABLE_JOIN_KEY_TYPES`
//!   the same way `bytea`/`oid` did.
//!
//! `inet`'s `GROUP BY` key role is nonetheless admitted (see
//! [`canonicalize_group_key_text`] below) — the same split `boolean` got in
//! #119, for the same mechanical reason: `staging::apply_aggregate`'s
//! keyset match never does raw-text comparison, it casts the *bound array*
//! to the column's native type (`$1::text[]::inet[]`), and `inet_in` is
//! permissive enough to parse both spellings (`'192.168.1.5'` and
//! `'192.168.1.5/32'`) back to the identical stored value — verified live:
//! `'192.168.1.5'::inet = '192.168.1.5/32'::inet` is `true`. That reconciles
//! the *SQL* half of the `GROUP BY` role automatically. It does not reconcile
//! `staging::apply_aggregate::accumulate_changes`'s in-memory `GroupPlan`
//! bucketing, which compares `derive_group_key`'s text byte-for-byte with no
//! database in the loop — `boolean`'s exact live bug shape — so this issue
//! adds an `inet` arm to `canonicalize_group_key_part` alongside `boolean`'s,
//! closing the gap the same way #119 did.
//!
//! ## `to_jsonb`
//!
//! Checked live for all four (a table with one column of each type, one row,
//! comparing `<col>::text` against `to_jsonb(t.*) ->> '<col>'`):
//!
//! ```text
//!  column    ::text            to_jsonb
//!  inet      192.168.1.5/32    192.168.1.5      DIFFERS (matches inet_out, not the cast)
//!  cidr      192.168.1.0/24    192.168.1.0/24    same
//!  macaddr   08:00:2b:01:02:03 08:00:2b:01:02:03 same
//!  macaddr8  08:00:2b:...      08:00:2b:...      same
//! ```
//!
//! `to_jsonb` calls a value's own output function for every non-numeric,
//! non-datetime scalar type (the same fact #114 established for `bytea`) —
//! it never goes through `pg_cast`'s `text(inet)` override, so for `inet` it
//! *agrees with `inet_out`*, not with `<col>::text`. This is not a live
//! hazard for Trellis today: issue #248's `staging::apply::row_as_text_jsonb_sql`
//! replaced every bare `to_jsonb(t.*)` call site with an explicit per-column
//! `<col>::text`, so nothing in the engine actually calls bare `to_jsonb`
//! for a row body — the discrepancy above is bare-Postgres-`to_jsonb`
//! trivia, not a second internal renderer to reconcile, the same conclusion
//! #114 reached for `bytea`. It is recorded here because it is one more
//! confirmation that `inet_out` (⟵ CDC) and `<col>::text` (⟵ everything the
//! engine itself renders, post-#248) are the two renderers actually in
//! conflict, not `to_jsonb` and `<col>::text`.
//!
//! ## `MIN`/`MAX`
//!
//! Checked live, per family, against `pg_proc`/`pg_aggregate` rather than
//! assumed:
//!
//! * **`inet` has a real `min(inet)`/`max(inet)` aggregate that keeps its
//!   argument's type** (`select pg_typeof(min(v)) from (values
//!   ('10.0.0.1'::inet)) t(v)` is `inet`). [`compare`] reproduces
//!   `network_cmp`'s three-level tie-break (verified live across a grid
//!   spanning cross-family pairs, same-family overlapping-prefix pairs at
//!   different netmask lengths, and same-network/different-host-bits pairs)
//!   so the Rust-side fold ([`crate::defs::eval`]'s `reduce_netaddr_aggregate`)
//!   agrees with a server-side `min`/`max` byte-for-byte, the ADR-0013 bar.
//! * **`cidr` has no `min(cidr)`/`max(cidr)` of its own — only Postgres's
//!   own implicit upcast to `inet` reaches one, and that changes the
//!   result's type.** `cidr` is implicitly castable to `inet`
//!   (`castcontext = 'i'` in `pg_cast`), so `select pg_typeof(max(cidr_col))
//!   from ...` type-checks and returns a value — but `pg_typeof` reports
//!   **`inet`**, not `cidr`, because Postgres resolved the call by
//!   implicitly widening every argument and calling `max(inet)`. Every other
//!   family this epic has granted `MIN`/`MAX` keeps the *column's own*
//!   declared type (`docs/type-support.md`'s "`min`/`max` keep their
//!   argument's own type" rule, verified for integers/floats/temporal); a
//!   `cidr` computed field silently coming back typed `inet` would be the
//!   one exception, and would need `registry::aggregate_result_type` to
//!   *change* a `ValueType::Other(PgType::Cidr)` argument into a
//!   `ValueType::Other(PgType::Inet)` result — a type-changing aggregate no
//!   other family in the epic needed and this issue declines to introduce
//!   speculatively. Treated as `❌`, matching bytea's "no server-side
//!   construct to be a subset of" reasoning applied one level up: the
//!   construct that exists is for a *different* type than the one asked for.
//! * **`macaddr`/`macaddr8` have no `min`/`max` at all, for either type** —
//!   `select min(v) from (values ('08:00:2b:01:02:03'::macaddr)) t(v)` is
//!   `ERROR: function min(macaddr) does not exist` on a live Postgres 17,
//!   and `pg_proc` has no `min`/`max` row for either argument type. The
//!   exact `bytea` finding (#114): both have a full, `IMMUTABLE` btree
//!   opclass (`macaddr_ops`/`macaddr8_ops`; `<`/`>`/`=`/`ORDER BY` all work),
//!   but Postgres never wired the aggregate to it. `registry::
//!   aggregate_result_type` returns `None` for both, same as `bytea`.

use std::cmp::Ordering;
use std::fmt;
use std::net::IpAddr;

use crate::defs::pg_type::PgType;

/// A failure to parse Postgres's canonical `inet` text (either renderer:
/// `inet_out`'s host-elided form or `network_show`'s always-explicit one —
/// [`parse`] accepts both, since [`compare`]/[`canonicalize_group_key_text`]
/// both have to handle text that arrived via either path).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NetAddrParseError;

impl fmt::Display for NetAddrParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("not a canonical Postgres `inet` rendering")
    }
}

impl std::error::Error for NetAddrParseError {}

/// An `inet` value's address bits, family, and netmask length — enough to
/// reproduce `network_cmp`'s ordering. `bits` is left-aligned in a 128-bit
/// word (an IPv4 address occupies the top 32 bits, the rest zero) so a v4
/// and a v6 value share one representation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Addr {
    /// Matches Postgres's own `family(inet)` output (`4` or `6`) — verified
    /// live to also be `network_cmp`'s cross-family tie-break order
    /// (`'1.2.3.4'::inet < '::'::inet` is `true` for every v4/v6 pair
    /// tried), so no separate ordinal mapping is needed.
    family: u8,
    bits: u128,
    masklen: u8,
}

/// Parses `text` — either of `inet`'s two live renderers, `inet_out`'s
/// host-elided form (`192.168.1.5`) or `network_show`'s always-explicit one
/// (`192.168.1.5/32`) — into an [`Addr`]. A missing `/prefixlen` defaults to
/// the family's full width, exactly the rule `inet_in` itself uses (and
/// exactly why the two renderings above name the same value:
/// `'192.168.1.5'::inet = '192.168.1.5/32'::inet` is `true`).
///
/// Round-trips the address part through [`IpAddr`]'s own `Display` and
/// requires it match `text` verbatim — this is what makes the function
/// strict rather than merely `IpAddr`-parseable: Rust's IPv6 `Display` and
/// Postgres's `inet_out`/`network_show` agree on every grid tried live
/// *except* the deprecated IPv4-compatible form (`::192.168.1.1`, distinct
/// from the IPv4-*mapped* `::ffff:192.168.1.1`, which does agree), which
/// Postgres preserves dotted and Rust renders as pure hex
/// (`::c0a8:101`). Requiring the round-trip rejects that one form rather
/// than risk silently mis-ordering it — the same "reject rather than guess"
/// posture `defs::typed_literal`'s canonical checkers take.
fn parse(text: &str) -> Result<Addr, NetAddrParseError> {
    let (addr_part, mask_part) = match text.split_once('/') {
        Some((a, m)) => (a, Some(m)),
        None => (text, None),
    };
    let ip: IpAddr = addr_part.parse().map_err(|_| NetAddrParseError)?;
    if ip.to_string() != addr_part {
        return Err(NetAddrParseError);
    }
    let (family, bits, maxbits): (u8, u128, u8) = match ip {
        IpAddr::V4(v4) => (4, (u32::from(v4) as u128) << 96, 32),
        IpAddr::V6(v6) => (6, u128::from(v6), 128),
    };
    let masklen = match mask_part {
        Some(m) => {
            let n: u8 = m.parse().map_err(|_| NetAddrParseError)?;
            // No leading zeros / `+` — `m.parse::<u8>()` already forbids a
            // sign, but `"05"` would still parse to `5`; reject it so the
            // literal-checker use of this same parser (`canonical_inet`)
            // enforces a single spelling per value.
            if n > maxbits || m != n.to_string() {
                return Err(NetAddrParseError);
            }
            n
        }
        None => maxbits,
    };
    Ok(Addr {
        family,
        bits,
        masklen,
    })
}

/// Masks `bits` down to its top `prefix_len` bits, zeroing the rest — the
/// building block [`compare`] needs three times over (`network_cmp`'s
/// shared-prefix compare, and implicitly the host-bits-zero check
/// [`super::defs::typed_literal`]'s `cidr` checker runs).
fn mask_prefix(bits: u128, prefix_len: u8) -> u128 {
    if prefix_len == 0 {
        0
    } else {
        bits & (u128::MAX << (128 - u32::from(prefix_len)))
    }
}

/// Reproduces `network_cmp`'s ordering exactly — verified live across:
/// cross-family pairs (`family` alone decides, address-independent: `4 <
/// 6`, checked with values at both extremes of each family's range so no
/// pair could coincidentally tie), same-family pairs whose shared prefix
/// (`min` of the two netmask lengths) differs (decided by that masked
/// prefix, e.g. `10.2.0.0/16 > 10.1.2.0/24`), same-family pairs whose
/// shared prefix agrees but whose netmask lengths differ (decided by
/// netmask length, e.g. `10.1.0.0/16 < 10.1.2.0/24`), and same-network
/// pairs whose netmask lengths also agree, differing only in host bits
/// (decided by the full address, e.g. `10.1.2.3/24 < 10.1.2.99/24`) — the
/// one case the first two comparisons alone cannot resolve, since both stop
/// at the (equal, in this case) netmask length.
fn compare_addr(a: Addr, b: Addr) -> Ordering {
    if a.family != b.family {
        return a.family.cmp(&b.family);
    }
    let min_bits = a.masklen.min(b.masklen);
    match mask_prefix(a.bits, min_bits).cmp(&mask_prefix(b.bits, min_bits)) {
        Ordering::Equal => {}
        other => return other,
    }
    match a.masklen.cmp(&b.masklen) {
        Ordering::Equal => {}
        other => return other,
    }
    a.bits.cmp(&b.bits)
}

/// [`compare_addr`] over two renderings of `inet` text — `None` if either
/// side doesn't parse, the same defensive posture
/// [`crate::temporal::compare`] takes for its own family.
pub fn compare(a: &str, b: &str) -> Option<Ordering> {
    Some(compare_addr(parse(a).ok()?, parse(b).ok()?))
}

/// Whether `pg_type` has a real `MIN`/`MAX` Postgres aggregate that keeps
/// its own argument's type — `inet` only. See this module's doc comment for
/// why `cidr` (aggregate exists, but only via an implicit upcast that
/// changes the result's type) and `macaddr`/`macaddr8` (no such aggregate at
/// all, the exact `bytea` finding) are both `false`.
pub const fn supports_min_max(pg_type: PgType) -> bool {
    matches!(pg_type, PgType::Inet)
}

/// Normalizes one `GROUP BY` key part's `inet` text to a single spelling
/// per logical value, before it feeds `staging::apply_aggregate::
/// derive_group_key`'s in-memory dedup key — the `inet` counterpart to
/// issue #119's `canonicalize_group_key_part` `Boolean` arm, for the
/// identical reason: `inet_out` (CDC) and `<col>::text`/`network_show`
/// (this engine's own live reads, post-#248) spell a bare-host `inet` value
/// two different ways, and `accumulate_changes`'s `GroupPlan` bucketing
/// compares that text byte-for-byte with no database in the loop. Always
/// appends the default netmask when the text omits one — the direction that
/// matches `network_show`'s always-explicit spelling, since that is what
/// every *other* renderer in the engine already produces for a live `inet`
/// read.
///
/// A no-op (returns `text` unchanged) for any input that already carries an
/// explicit `/prefixlen`, or that fails to parse at all — a malformed value
/// reaching here is a defense-in-depth case `parse_value` would already have
/// rejected earlier, and this function's contract (matching `Boolean`'s
/// `canonicalize_group_key_part` arm) is to pass unparseable text through
/// verbatim rather than guess at it.
pub fn canonicalize_group_key_text(text: &str) -> String {
    if text.contains('/') {
        return text.to_string();
    }
    match text.parse::<IpAddr>() {
        Ok(IpAddr::V4(_)) => format!("{text}/32"),
        Ok(IpAddr::V6(_)) => format!("{text}/128"),
        Err(_) => text.to_string(),
    }
}

const INET_SHAPE: &str = "an address in Rust's canonical std::net form followed by a mandatory \
    /prefixlen, e.g. `192.168.1.5/32` or `2001:db8::1/128` — the form `<col>::text` renders \
    (`network_show`), not `inet_out`'s host-address-elided form";

/// `defs::typed_literal`'s canonical-form checker for an `INET` literal.
///
/// The bar is `network_show`'s spelling (mandatory explicit `/prefixlen`),
/// not `inet_out`'s (see this module's doc comment) — the same "canonical
/// means whichever renderer the engine's own round-trip actually uses"
/// principle `defs::typed_literal`'s module doc states, applied to the one
/// family in the epic where the two renderers genuinely differ.
pub fn canonical_inet(text: &str) -> Result<(), &'static str> {
    // [`parse`] alone is not enough: it deliberately accepts *both* live
    // renderings (a missing `/prefixlen` defaults to the family's full
    // width, exactly `inet_in`'s own rule, which is what makes it usable
    // for [`compare`]/[`canonicalize_group_key_text`]). A typed literal's
    // canonical form is narrower than "parses" — it must be the one
    // spelling `network_show` actually emits, which always carries the
    // suffix explicitly.
    if !text.contains('/') {
        return Err(INET_SHAPE);
    }
    parse(text).map(|_| ()).map_err(|_| INET_SHAPE)
}

const CIDR_SHAPE: &str = "an address in Rust's canonical std::net form followed by a mandatory \
    /prefixlen, with every host bit zero, e.g. `192.168.1.0/24` or `2001:db8::/32`";

/// `defs::typed_literal`'s canonical-form checker for a `CIDR` literal —
/// [`canonical_inet`]'s shape plus `cidr_in`'s own extra requirement (every
/// bit outside the netmask must be zero; `cidr_in` raises `invalid cidr
/// value: ... has bits set to right of mask` otherwise, a real Postgres
/// input-side check this function reproduces rather than only relying on
/// `cidr_in` to catch at write time — round-trip identity needs the
/// *literal's own text* to already be in the form `cidr_out` would echo
/// back).
pub fn canonical_cidr(text: &str) -> Result<(), &'static str> {
    // Same reasoning as `canonical_inet`'s explicit-`/` check: without it, a
    // bare host address with no `/` at all would default to the family's
    // full-width mask (`parse`'s rule, for `compare`/`canonicalize_group_key_text`'s
    // benefit), which trivially has zero host bits and would otherwise slip
    // through the check below.
    if !text.contains('/') {
        return Err(CIDR_SHAPE);
    }
    let addr = parse(text).map_err(|_| CIDR_SHAPE)?;
    if mask_prefix(addr.bits, addr.masklen) != addr.bits {
        return Err(CIDR_SHAPE);
    }
    Ok(())
}

/// `defs::typed_literal`'s canonical-form checker for a `MACADDR` literal —
/// six lowercase hex octet pairs, colon-separated, the one spelling
/// `macaddr_out` ever emits regardless of which of `macaddr_in`'s several
/// accepted input shapes (colon-, hyphen-, dot-grouped, or bare hex) a
/// value was written in.
pub fn canonical_macaddr(text: &str) -> Result<(), &'static str> {
    const SHAPE: &str =
        "six lowercase hex octet pairs separated by colons, e.g. `08:00:2b:01:02:03`";
    canonical_mac_groups(text, 6).ok_or(SHAPE)
}

/// [`canonical_macaddr`]'s `MACADDR8` twin — eight octet pairs instead of
/// six, `macaddr8_out`'s one spelling.
pub fn canonical_macaddr8(text: &str) -> Result<(), &'static str> {
    const SHAPE: &str =
        "eight lowercase hex octet pairs separated by colons, e.g. `08:00:2b:01:02:03:04:05`";
    canonical_mac_groups(text, 8).ok_or(SHAPE)
}

fn canonical_mac_groups(text: &str, groups: usize) -> Option<()> {
    let parts: Vec<&str> = text.split(':').collect();
    if parts.len() != groups {
        return None;
    }
    for part in &parts {
        let bytes = part.as_bytes();
        if bytes.len() != 2 {
            return None;
        }
        if !bytes
            .iter()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(b))
        {
            return None;
        }
    }
    Some(())
}

#[cfg(test)]
mod tests {
    use super::*;

    // ---------------------------------------------------------------
    // parse / compare
    // ---------------------------------------------------------------

    #[test]
    fn parse_accepts_both_live_renderers_of_a_bare_host_address() {
        let elided = parse("192.168.1.5").unwrap();
        let explicit = parse("192.168.1.5/32").unwrap();
        assert_eq!(elided, explicit);
    }

    #[test]
    fn parse_rejects_leading_zero_prefix_and_out_of_range_prefix() {
        assert!(parse("192.168.1.5/032").is_err());
        assert!(parse("192.168.1.5/33").is_err());
        assert!(parse("::1/129").is_err());
    }

    #[test]
    fn parse_rejects_the_deprecated_ipv4_compatible_form() {
        // Rust's `Ipv6Addr` Display renders `::192.168.1.1` as `::c0a8:101`
        // — see this module's doc comment on `parse`. The round-trip check
        // must catch the mismatch rather than silently accept a text this
        // module cannot safely order/canonicalize.
        assert!(parse("::192.168.1.1/128").is_err());
    }

    #[test]
    fn compare_orders_by_family_first_regardless_of_address_value() {
        // Verified live: `'1.2.3.4'::inet < '::'::inet` and
        // `'255.255.255.255'::inet < '::'::inet` are both `true`.
        assert_eq!(compare("1.2.3.4", "::").unwrap(), Ordering::Less);
        assert_eq!(compare("255.255.255.255", "::").unwrap(), Ordering::Less);
    }

    #[test]
    fn compare_breaks_a_same_family_tie_by_shared_prefix_then_masklen_then_full_address() {
        // Shared 16-bit prefix differs: `10.2.../16 > 10.1.../24`.
        assert_eq!(
            compare("10.2.0.0/16", "10.1.2.0/24").unwrap(),
            Ordering::Greater
        );
        // Shared 16-bit prefix agrees, masklen decides: `10.1.0.0/16 <
        // 10.1.2.0/24`.
        assert_eq!(
            compare("10.1.0.0/16", "10.1.2.0/24").unwrap(),
            Ordering::Less
        );
        // Prefix and masklen both agree (same /24 network); only the host
        // bits differ.
        assert_eq!(
            compare("10.1.2.3/24", "10.1.2.99/24").unwrap(),
            Ordering::Less
        );
    }

    #[test]
    fn compare_is_reflexively_equal_for_one_value_under_either_rendering() {
        assert_eq!(
            compare("192.168.1.5", "192.168.1.5/32").unwrap(),
            Ordering::Equal
        );
    }

    // ---------------------------------------------------------------
    // GROUP BY canonicalization
    // ---------------------------------------------------------------

    #[test]
    fn canonicalize_group_key_text_appends_the_default_netmask_when_absent() {
        assert_eq!(canonicalize_group_key_text("192.168.1.5"), "192.168.1.5/32");
        assert_eq!(canonicalize_group_key_text("::1"), "::1/128");
    }

    #[test]
    fn canonicalize_group_key_text_is_a_no_op_when_a_netmask_is_already_present() {
        assert_eq!(
            canonicalize_group_key_text("192.168.1.0/24"),
            "192.168.1.0/24"
        );
    }

    #[test]
    fn canonicalize_group_key_text_reconciles_both_live_renderers_to_one_spelling() {
        let via_cdc = canonicalize_group_key_text("192.168.1.5"); // inet_out
        let via_live_read = canonicalize_group_key_text("192.168.1.5/32"); // network_show
        assert_eq!(via_cdc, via_live_read);
    }

    // ---------------------------------------------------------------
    // typed-literal canonical checkers
    // ---------------------------------------------------------------

    #[test]
    fn canonical_inet_requires_the_explicit_netmask_form() {
        assert!(canonical_inet("192.168.1.5/32").is_ok());
        assert!(canonical_inet("2001:db8::1/128").is_ok());
        // `inet_out`'s elided form is not `network_show`'s canonical text.
        assert!(canonical_inet("192.168.1.5").is_err());
    }

    #[test]
    fn canonical_cidr_requires_zero_host_bits() {
        assert!(canonical_cidr("192.168.1.0/24").is_ok());
        assert!(canonical_cidr("192.168.1.5/24").is_err());
        assert!(canonical_cidr("2001:db8::/32").is_ok());
    }

    #[test]
    fn canonical_macaddr_requires_six_lowercase_colon_groups() {
        assert!(canonical_macaddr("08:00:2b:01:02:03").is_ok());
        assert!(canonical_macaddr("08:00:2B:01:02:03").is_err());
        assert!(canonical_macaddr("08-00-2b-01-02-03").is_err());
        assert!(canonical_macaddr("08:00:2b:01:02:03:04").is_err());
    }

    #[test]
    fn canonical_macaddr8_requires_eight_lowercase_colon_groups() {
        assert!(canonical_macaddr8("08:00:2b:01:02:03:04:05").is_ok());
        assert!(canonical_macaddr8("08:00:2b:01:02:03").is_err());
    }

    #[test]
    fn supports_min_max_admits_only_inet() {
        assert!(supports_min_max(PgType::Inet));
        for pg_type in [PgType::Cidr, PgType::MacAddr, PgType::MacAddr8] {
            assert!(!supports_min_max(pg_type), "{pg_type}");
        }
    }
}
