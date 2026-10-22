//! The one place a Trellis-created column's type becomes SQL text (ADR-0020,
//! #1289).
//!
//! A column Trellis creates from a source column, and every cast to such a
//! column's type, is the source column's type **OID** (and collation OID)
//! with **no modifier**, rendered schema-qualified:
//!
//! - **I-T1.** A name is a rendering of an OID, never an identity.
//!   `format_type` omits the schema of a type visible on the session's
//!   `search_path`, and Trellis's sessions put the Trellis and target schemas
//!   on it, so a name rendered in one session can resolve to another type in
//!   another. The rendering here never depends on the path: a `pg_catalog`
//!   type is `format_type(oid, -1)` (always visible, so never ambiguous), any
//!   other type is `quote_ident(schema).quote_ident(typname)`, an array of one
//!   that with `[]`.
//! - **I-T2.** The modifier is dropped (`-1`). Postgres rounds silently on
//!   plain assignment into `numeric(p,s)`, `timestamp(p)`, `timestamptz(p)`,
//!   `time(p)`, `timetz(p)` and `interval` columns, and errors or truncates
//!   for the string and bit types, so a copy that keeps the source's modifier
//!   loses values after a catalog-only widening that fires no trigger. A
//!   `format_type(oid, -1)` rendering of a bit string is the quoted `"bit"`,
//!   which is unlimited; the unquoted keyword is `bit(1)`.
//!
//! Nothing else formats type text for a column Trellis creates or a cast to
//! one: every builder reads the type through [`type_sql`] (inside a catalog
//! query) or [`render`] (from a recorded OID), and a test fails on a
//! `format_type(…, atttypmod)` that reaches DDL or a cast.

/// A SQL expression, over the `oid`-typed SQL expression `oid_expr`, for the
/// text of that type as a copy declares and casts to it: no modifier,
/// qualified. `NULL` for an OID with no `pg_type` row.
///
/// The expression is a scalar subquery with its own aliases, so it embeds in
/// any query.
pub(crate) fn type_sql(oid_expr: &str) -> String {
    format!(
        "(select case \
            when ct_t.typnamespace = 'pg_catalog'::pg_catalog.regnamespace \
                then pg_catalog.format_type(ct_t.oid, -1) \
            when ct_t.typcategory = 'A' and ct_e.oid is not null \
                then pg_catalog.quote_ident(ct_en.nspname) || '.' \
                     || pg_catalog.quote_ident(ct_e.typname) || '[]' \
            else pg_catalog.quote_ident(ct_n.nspname) || '.' \
                 || pg_catalog.quote_ident(ct_t.typname) end \
          from pg_catalog.pg_type ct_t \
          join pg_catalog.pg_namespace ct_n on ct_n.oid = ct_t.typnamespace \
          left join pg_catalog.pg_type ct_e on ct_e.oid = ct_t.typelem \
          left join pg_catalog.pg_namespace ct_en on ct_en.oid = ct_e.typnamespace \
          where ct_t.oid = ({oid_expr})::pg_catalog.oid)"
    )
}

/// A SQL expression for the qualified, quoted name of the collation whose
/// OID `oid_expr` is, for `collate <name>`: `NULL` for the database default
/// (OID 100) and for no collation (0).
pub(crate) fn collation_sql(oid_expr: &str) -> String {
    format!(
        "(select pg_catalog.quote_ident(ct_cn.nspname) || '.' \
                 || pg_catalog.quote_ident(ct_co.collname) \
          from pg_catalog.pg_collation ct_co \
          join pg_catalog.pg_namespace ct_cn on ct_cn.oid = ct_co.collnamespace \
          where ct_co.oid = ({oid_expr})::pg_catalog.oid and ct_co.oid <> 100)"
    )
}

/// The `bit` family's unlimited type. Unquoted, `bit` is `bit(1)`.
pub(crate) const UNLIMITED_BIT: &str = "\"bit\"";

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_type_expression_never_renders_a_modifier_or_reads_the_search_path() {
        let sql = type_sql("a.atttypid");
        assert!(sql.contains("format_type(ct_t.oid, -1)"), "{sql}");
        assert!(!sql.contains("atttypmod"), "{sql}");
        assert!(sql.contains("quote_ident(ct_n.nspname)"), "{sql}");
    }
}
