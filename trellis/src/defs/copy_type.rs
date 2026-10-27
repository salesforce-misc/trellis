//! The one place a Trellis-created column's type becomes SQL text (ADR-0020).
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
//! one: every builder reads the type through [`type_sql`] and the collation
//! through [`collation_sql`], inside the catalog query that finds the
//! column. A value-family column (a `GROUP BY` key's, an aggregate's) is
//! typed by `PgType::sql_type_name`, which has no modifier to render. The
//! test `format_type_is_called_only_where_listed` fails on a new
//! `format_type` call anywhere in the crate until it is listed there with
//! the reason it can't reach a column Trellis creates or a cast to one.

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
        assert!(
            sql.contains(concat!("format", "_type(ct_t.oid, -1)")),
            "{sql}"
        );
        assert!(!sql.contains("atttypmod"), "{sql}");
        assert!(sql.contains("quote_ident(ct_n.nspname)"), "{sql}");
    }

    /// Every `format_type` call in the crate, by file, and why none of them
    /// types a column Trellis creates or a cast to one (ADR-0020, rule 8's
    /// completeness): a new call fails here until it is listed with its
    /// reason, or goes through [`type_sql`] instead.
    const FORMAT_TYPE_CALLS: &[(&str, usize, &str)] = &[
        (
            "capture/baseline.rs",
            2,
            "a column's type with its modifier, for the reason a pause gives",
        ),
        (
            "capture/partitions.rs",
            1,
            "a partition key's own type, to rank the source's own bounds as \
             Postgres routes them",
        ),
        (
            "defs/catalog.rs",
            1,
            "a JOIN column's declared type, for the key allowlist and \
             messages; its copy is `copy_type`",
        ),
        (
            "defs/copies.rs",
            1,
            "`CopyType::display`, for messages; the copy is `CopyType::sql`",
        ),
        (
            "defs/copy_type.rs",
            1,
            "`type_sql` itself, with no modifier",
        ),
        (
            "defs/ddl.rs",
            1,
            "the type a refused primary key column declares, for the message",
        ),
        (
            "defs/key_types.rs",
            1,
            "`LiveColumn::display`, for the key allowlist and messages",
        ),
        (
            "defs/partition_tree.rs",
            1,
            "a partition key's type name for the partition key allowlist",
        ),
        (
            "staging/schema_change.rs",
            2,
            "the recorded and live types a pause's reason names",
        ),
    ];

    #[test]
    fn format_type_is_called_only_where_listed() {
        // Split so this file's own source doesn't count as a call.
        let needle = concat!("format", "_type(");
        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
        let mut found: Vec<(String, usize)> = Vec::new();
        let mut dirs = vec![root.clone()];
        while let Some(dir) = dirs.pop() {
            for entry in std::fs::read_dir(&dir).expect("read src") {
                let path = entry.expect("entry").path();
                if path.is_dir() {
                    dirs.push(path);
                    continue;
                }
                if path.extension().is_none_or(|e| e != "rs") {
                    continue;
                }
                let source = std::fs::read_to_string(&path).expect("read a source file");
                let calls: usize = source
                    .lines()
                    .filter(|line| !line.trim_start().starts_with("//"))
                    .map(|line| line.matches(needle).count())
                    .sum();
                if calls > 0 {
                    let relative = path
                        .strip_prefix(&root)
                        .expect("under src")
                        .to_string_lossy()
                        .replace('\\', "/");
                    found.push((relative, calls));
                }
            }
        }
        found.sort();
        let listed: Vec<(String, usize)> = FORMAT_TYPE_CALLS
            .iter()
            .map(|(file, calls, _)| ((*file).to_string(), *calls))
            .collect();
        assert_eq!(
            found, listed,
            "a format_type call that isn't listed: a column Trellis creates, or a cast to one, \
             takes its type from `type_sql` (ADR-0020); list any other call with its reason"
        );
    }

    /// Each class of type a source column can have, and a collation in a
    /// quoted schema, rendered in a session that sees every one of them by
    /// its bare name, resolves to the same type and collation, with no
    /// modifier, in a session whose `search_path` finds a decoy of the same
    /// name first (ADR-0020, I-T1).
    #[tokio::test]
    async fn a_rendering_resolves_to_the_same_type_whatever_the_search_path() {
        let cluster = testkit::TestCluster::start();
        let db = cluster.create_isolated_database().await;
        let (client, connection) = tokio_postgres::connect(db.dsn(), tokio_postgres::NoTls)
            .await
            .expect("connect");
        tokio::spawn(async move {
            let _ = connection.await;
        });
        client
            .batch_execute(
                "create schema \"My.Sch\"\"ema\"; \
                 create schema decoy; \
                 create type \"My.Sch\"\"ema\".\"Mood X\" as enum ('a', 'b'); \
                 create type public.mood as enum ('x'); \
                 create domain public.posint as integer check (value > 0); \
                 create collation \"My.Sch\"\"ema\".cc (provider = libc, locale = 'C'); \
                 create type decoy.\"Mood X\" as enum ('zzz'); \
                 create type decoy.mood as enum ('zzz'); \
                 create domain decoy.posint as text; \
                 create collation decoy.cc (provider = libc, locale = 'POSIX'); \
                 create table public.t ( \
                     a bit(5), b bit varying(8), c char(4), d \"char\", e name, \
                     f timetz(2), g interval day to second(3), h numeric(6,2), \
                     i \"My.Sch\"\"ema\".\"Mood X\", j \"My.Sch\"\"ema\".\"Mood X\"[], \
                     k posint, l mood, m timestamptz(0)[], n varchar(3)[], \
                     o text collate \"My.Sch\"\"ema\".cc, p text collate \"C\", q text, \
                     r bit, s varchar(9), u time(1), v timestamp(2))",
            )
            .await
            .expect("types");
        client
            .batch_execute("set search_path to public, \"My.Sch\"\"ema\"")
            .await
            .expect("every type visible");
        let rendered = client
            .query(
                &format!(
                    "select a.attname::text, a.atttypid, a.attcollation, {}, {} \
                     from pg_catalog.pg_attribute a \
                     where a.attrelid = 'public.t'::regclass and a.attnum > 0 \
                     order by a.attnum",
                    type_sql("a.atttypid"),
                    collation_sql("a.attcollation")
                ),
                &[],
            )
            .await
            .expect("render");
        assert_eq!(rendered.len(), 21);
        for path in ["decoy, public", "pg_catalog, pg_temp"] {
            client
                .batch_execute(&format!("set search_path to {path}"))
                .await
                .expect("another search_path");
            for row in &rendered {
                let column: String = row.get(0);
                let (type_oid, collation_oid): (u32, u32) = (row.get(1), row.get(2));
                let ty: String = row.get(3);
                let collation: Option<String> = row.get(4);
                let collate = collation
                    .map(|c| format!(" collate {c}"))
                    .unwrap_or_default();
                let declared = format!("{ty}{collate}");
                client
                    .batch_execute(&format!(
                        "drop table if exists pg_temp.copy; \
                         create temp table copy (c {declared})"
                    ))
                    .await
                    .unwrap_or_else(|err| panic!("{column} as {declared} under {path}: {err}"));
                let copied = client
                    .query_one(
                        "select atttypid, atttypmod, attcollation from pg_catalog.pg_attribute \
                         where attrelid = 'pg_temp.copy'::regclass and attname = 'c'",
                        &[],
                    )
                    .await
                    .expect("the copy");
                assert_eq!(
                    (
                        copied.get::<_, u32>(0),
                        copied.get::<_, i32>(1),
                        copied.get::<_, u32>(2)
                    ),
                    (type_oid, -1, collation_oid),
                    "{column} rendered {declared} resolves elsewhere under {path}"
                );
            }
        }
    }
}
