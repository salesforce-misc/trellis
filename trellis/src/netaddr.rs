//! Postgres's **network-address** types — `inet`, `cidr`, `macaddr` and
//! `macaddr8` (issue #116) — given the ordering, key/`GROUP BY`, and
//! typed-literal roles `docs/type-support.md` marked in scope, on top of the
//! passthrough classification issue #108 gave them.
//!
//! # Why there is no `ValueType::NetAddr`
//!
//! Same reasoning as [`crate::temporal`]'s module doc: every role this
//! issue grants any of the four families is a function of the family alone,
//! which `PgType` already carries, so they stay `ValueType::Other(PgType::X)`.
//!
//! # The four families do *not* land the same way
//!
//! Checked against a live server per each of #111-#119's playbooks (never
//! assumed from the type's name), the four diverge:
//!
//! ```text
//!            join/PK/GROUP BY key
//! inet       GROUP BY only
//! cidr       all three
//! macaddr    all three
//! macaddr8   all three
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
//! type on `join_key_types::TEXT_STABLE_JOIN_KEY_TYPES` needs, and both output
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
//!   `inet_out` (what capture images, and what a staged `key` is built from)
//!   omits the `/prefixlen` suffix exactly when the stored
//!   netmask covers the whole address (`'192.168.1.5'::inet::text` via
//!   `inet_out` is `192.168.1.5`), while `network_show` (the cast, what
//!   `<col>::text` and hence `staging::apply::row_as_text_jsonb_sql`'s live
//!   reads call) *always* prints it explicitly (`192.168.1.5/32`). Verified
//!   on a live server across a v4/v6 grid: the two renderers disagree on
//!   every "bare host address" value and agree on every value with an
//!   explicit sub-maximal netmask. One value, two renderers, no arbiter —
//!   `join_key_types::TEXT_STABLE_JOIN_KEY_TYPES`'s doc comment has `boolean`'s
//!   account of why that is fatal for the join/primary-key role
//!   specifically, and the same
//!   reasoning applies here verbatim. `inet` is *not* added to that
//!   allowlist.
//! * **`cidr` does not diverge, anywhere.** `cidr_out` never omits the
//!   netmask — a `cidr` value's whole *point* is that the network prefix is
//!   significant, so both the low-level output function and `network_show`
//!   print it unconditionally. Verified live across the same grid (plus the
//!   host-bits-must-be-zero values `cidr_in` alone accepts):
//!   `cidr_out(v)::text = v::text` for every value tried, no exceptions. So
//!   unlike `inet`, `cidr`'s `::text` genuinely *is* (in effect) its own
//!   output function, and it joins `join_key_types::TEXT_STABLE_JOIN_KEY_TYPES`
//!   the same way `bytea`/`oid` did.
//!
//! `inet`'s `GROUP BY` key role is nonetheless admitted — the same split
//! `boolean` got in #119, for the same mechanical reason: the group basis
//! (`staging::group_basis`) never compares a group key's text, it parses it
//! into the column's native type, and `inet_in` is permissive enough to parse both
//! spellings (`'192.168.1.5'` and `'192.168.1.5/32'`) back to the identical
//! stored value — verified live: `'192.168.1.5'::inet =
//! '192.168.1.5/32'::inet` is `true`.
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

use std::fmt;
use std::net::IpAddr;

/// A failure to parse Postgres's canonical `inet` text (either renderer:
/// `inet_out`'s host-elided form or `network_show`'s always-explicit one —
/// [`parse`] accepts both, since [`compare`] has to handle text that arrived
/// via either path).
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
    // for [`compare`]). A typed literal's
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
    // full-width mask (`parse`'s rule, for `compare`'s benefit), which trivially has zero host bits and would otherwise slip
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
}
