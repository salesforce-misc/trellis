//! Every column the engine matches as a key by its exact text
//! needs a deterministic collation. A nondeterministic one's `=` (and so its
//! `GROUP BY` and unique indexes) treats `'A'` and `'a'` as equal, so Postgres
//! would fold rows the engine keeps apart. Refused at define time, one test
//! per key class, and a deterministic non-default collation (`"C"`) still
//! passes in every one of them.
//!
//! The `JOIN` key twin of this rule is pinned in `defs_join_define.rs`.

use std::collections::HashMap;

use testkit::TestCluster;
use tokio::sync::{Mutex, MutexGuard};
use trellis::defs::join_one_to_one::set_enabled;
use trellis::defs::{
    CatalogError, KeyColumnRole, NondeterministicCollationTextFunction,
    NondeterministicKeyCollation, Statement, ValidationError, ValueType, alter_transform,
    install_definition, parse_statement,
};
use trellis::{Config, TransformStatus, Trellis, TrellisOptions};

/// Only ICU provides nondeterministic collations. Every test cluster's
/// default collation is ICU `en-US`, so a server without ICU can't run
/// these tests at all; a refusal here is a real failure, not a skip.
async fn create_case_insensitive_collation(pool: &trellis::pool::Pool) {
    pool.get()
        .await
        .expect("get connection")
        .batch_execute(
            "create collation case_insensitive \
             (provider = icu, locale = 'und-u-ks-level2', deterministic = false)",
        )
        .await
        .expect("create a nondeterministic ICU collation");
}

async fn batch(pool: &trellis::pool::Pool, sql: &str) {
    pool.get()
        .await
        .expect("get connection")
        .batch_execute(sql)
        .await
        .expect("set up tables");
}

fn columns(pairs: &[(&str, ValueType)]) -> HashMap<String, ValueType> {
    pairs
        .iter()
        .map(|(name, value_type)| (name.to_string(), *value_type))
        .collect()
}

/// Unwraps `err` as [`ValidationError::NondeterministicKeyCollation`] and
/// checks it names `key`, `table.column` and the `case_insensitive`
/// collation.
fn assert_refused(err: CatalogError, key: KeyColumnRole, table: &str, column: &str) {
    let message = err.to_string();
    match err {
        CatalogError::Validate(ValidationError::NondeterministicKeyCollation(refused)) => {
            assert_eq!(
                *refused,
                NondeterministicKeyCollation {
                    key,
                    table: table.to_string(),
                    column: column.to_string(),
                    collation: "case_insensitive".to_string(),
                }
            );
        }
        other => panic!("expected NondeterministicKeyCollation, got {other:?}"),
    }
    assert!(
        message.contains(&format!("{table}.{column} is "))
            && message.contains(r#"collation "case_insensitive" is nondeterministic"#),
        "{message}"
    );
}

/// Refused before any DDL: registration creates the target in the same
/// transaction, after every check.
async fn assert_no_target(pool: &trellis::pool::Pool, target: &str) {
    let exists: bool = pool
        .get()
        .await
        .expect("get connection")
        .query_one(
            "select pg_catalog.to_regclass($1) is not null",
            &[&format!("public.{target}")],
        )
        .await
        .expect("probe target")
        .get(0);
    assert!(!exists, "a refused definition must not leave its target");
}

/// A source whose primary key has a nondeterministic collation is refused:
/// apply keys every change by it, and a 1-1 target mirrors it as its key.
#[tokio::test]
async fn a_source_key_with_a_nondeterministic_collation_is_refused() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    create_case_insensitive_collation(&db.pool).await;
    batch(
        &db.pool,
        "create table codes (code text collate case_insensitive primary key, n integer)",
    )
    .await;
    let err = install_definition(
        &db.pool,
        "TRANSFORM code_copies FROM codes SELECT n AS n",
        &columns(&[("code", ValueType::Text), ("n", ValueType::Numeric)]),
        "public",
    )
    .await
    .expect_err("a nondeterministic source key must be refused");
    assert_refused(err, KeyColumnRole::SourceKey, "trellis.codes", "code");
    assert_no_target(&db.pool, "code_copies").await;
}

/// A `GROUP BY` over a source column with a nondeterministic collation is
/// refused: the target's group key is the value's exact text.
#[tokio::test]
async fn a_group_by_column_with_a_nondeterministic_collation_is_refused() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    create_case_insensitive_collation(&db.pool).await;
    batch(
        &db.pool,
        "create table sales (id bigint primary key, region text collate case_insensitive, \
                             amount numeric(12,2))",
    )
    .await;
    let err = install_definition(
        &db.pool,
        "TRANSFORM region_totals FROM sales GROUP BY region WITHOUT SOURCE INDEX SELECT sum(amount) AS total",
        &columns(&[
            ("id", ValueType::Numeric),
            ("region", ValueType::Text),
            ("amount", ValueType::Numeric),
        ]),
        "public",
    )
    .await
    .expect_err("a nondeterministic GROUP BY column must be refused");
    assert_refused(err, KeyColumnRole::GroupBy, "trellis.sales", "region");
    assert_no_target(&db.pool, "region_totals").await;
}

/// Every key class above passes under a deterministic non-default collation
/// (`"C"`): a `"C"` source key, a `"C"` `GROUP BY` column, and a `"C"`
/// `GROUP BY` key read from a `JOIN`'s joined table. Needs no ICU, so it runs
/// on every cluster.
#[tokio::test]
async fn deterministic_non_default_key_collations_pass() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    batch(
        &db.pool,
        r#"create table posts (id bigint primary key, author text collate "C");
           create table post_tags
               (id text collate "C" primary key, post bigint, tag text collate "C");"#,
    )
    .await;
    install_definition(
        &db.pool,
        "TRANSFORM tag_counts FROM post_tags GROUP BY tag \
        WITHOUT SOURCE INDEX SELECT count(*) AS n",
        &columns(&[
            ("id", ValueType::Text),
            ("post", ValueType::Numeric),
            ("tag", ValueType::Text),
        ]),
        "public",
    )
    .await
    .expect("\"C\" source key and GROUP BY key are deterministic");
    install_definition(
        &db.pool,
        "TRANSFORM author_tag_counts FROM post_tags JOIN posts ON post_tags.post = posts.id \
         GROUP BY post_tags.tag, posts.author WITHOUT SOURCE INDEX SELECT count(*) AS n",
        &HashMap::new(),
        "public",
    )
    .await
    .expect("\"C\" GROUP BY keys on either side of a JOIN are deterministic");
}

/// Unwraps `err` as [`ValidationError::NondeterministicCollationTextFunction`]
/// and checks it names `field`, `function`, `table.column` and the
/// `case_insensitive` collation.
fn assert_text_function_refused(
    err: CatalogError,
    field: &str,
    function: &str,
    table: &str,
    column: &str,
) {
    let message = err.to_string();
    match err {
        CatalogError::Validate(ValidationError::NondeterministicCollationTextFunction(refused)) => {
            assert_eq!(
                *refused,
                NondeterministicCollationTextFunction {
                    field: field.to_string(),
                    function: function.to_string(),
                    table: table.to_string(),
                    column: column.to_string(),
                    collation: "case_insensitive".to_string(),
                }
            );
        }
        other => panic!("expected NondeterministicCollationTextFunction, got {other:?}"),
    }
    assert!(
        message.contains(&format!("{function} reads {table}.{column}"))
            && message.contains(r#"collation "case_insensitive" is nondeterministic"#),
        "{message}"
    );
}

/// Postgres refuses `strpos` and `regexp_count` under
/// a nondeterministic collation (`0A000`), while the engine would compute
/// them from the exact text, so a field that calls either on such a column
/// is refused: directly, and through `COALESCE` and another field.
/// `char_length`/`octet_length` don't depend on collation and install.
#[tokio::test]
async fn substring_and_regex_functions_on_a_nondeterministic_column_are_refused() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    create_case_insensitive_collation(&db.pool).await;
    batch(
        &db.pool,
        "create table notes (id bigint primary key, title text collate case_insensitive, \
                             body text, post bigint)",
    )
    .await;
    let note_columns = columns(&[
        ("id", ValueType::Numeric),
        ("title", ValueType::Text),
        ("body", ValueType::Text),
        ("post", ValueType::Numeric),
    ]);

    let err = install_definition(
        &db.pool,
        "TRANSFORM note_positions FROM notes SELECT strpos(title, 'x') AS pos",
        &note_columns,
        "public",
    )
    .await
    .expect_err("strpos on a nondeterministic column must be refused");
    assert_text_function_refused(err, "pos", "STRPOS", "trellis.notes", "title");
    assert_no_target(&db.pool, "note_positions").await;

    let err = install_definition(
        &db.pool,
        "TRANSFORM note_counts FROM notes \
         SELECT coalesce(body, title) AS text, regexp_count(text, 'a') AS n",
        &note_columns,
        "public",
    )
    .await
    .expect_err("regexp_count reaching a nondeterministic column must be refused");
    assert_text_function_refused(err, "n", "REGEXP_COUNT", "trellis.notes", "title");
    assert_no_target(&db.pool, "note_counts").await;

    install_definition(
        &db.pool,
        "TRANSFORM note_lengths FROM notes \
         SELECT char_length(title) AS chars, octet_length(title) AS bytes, \
                strpos(body, 'x') AS pos",
        &note_columns,
        "public",
    )
    .await
    .expect("length functions and a deterministic column's strpos install");
}

/// `ALTER TRANSFORM ... ADD` runs the same check on the fields it adds, and a
/// refused edit adds no column.
#[tokio::test]
async fn altering_in_a_substring_search_on_a_nondeterministic_column_is_refused() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    create_case_insensitive_collation(&db.pool).await;
    batch(
        &db.pool,
        "create table notes (id bigint primary key, title text collate case_insensitive)",
    )
    .await;
    install_definition(
        &db.pool,
        "TRANSFORM note_calc FROM notes SELECT char_length(title) AS chars",
        &columns(&[("id", ValueType::Numeric), ("title", ValueType::Text)]),
        "public",
    )
    .await
    .expect("char_length doesn't depend on collation");
    // `ALTER TRANSFORM` needs a live transform; no pipeline runs here, so mark
    // it live directly rather than wait for a build.
    batch(
        &db.pool,
        "update transform_definitions set status = 'live' where target_table = 'public.note_calc'",
    )
    .await;
    let Statement::AlterTransform(alter) =
        parse_statement("ALTER TRANSFORM note_calc ADD strpos(title, 'x') AS pos")
            .expect("parse the ALTER")
    else {
        panic!("expected an ALTER TRANSFORM statement");
    };
    let err = alter_transform(&db.pool, &alter)
        .await
        .expect_err("an added strpos on a nondeterministic column must be refused");
    assert_text_function_refused(err, "pos", "STRPOS", "trellis.notes", "title");
    let added: bool = db
        .pool
        .get()
        .await
        .expect("get connection")
        .query_one(
            "select exists (select 1 from information_schema.columns \
             where table_schema = 'public' and table_name = 'note_calc' \
               and column_name = 'pos')",
            &[],
        )
        .await
        .expect("probe the target's columns")
        .get(0);
    assert!(!added, "a refused ALTER must not add its column");
}

/// A nondeterministic collation's `=` calls `'A'` and `'a'` equal, so a text
/// comparison (`=`, `<>`, `IS [NOT] DISTINCT FROM`, `NULLIF`) that reads such
/// a column is refused like the text functions: directly, through `COALESCE`
/// or a `CASE` result, and through another field. A comparison of a
/// deterministic column, or of a value computed from the column that is not
/// text (`char_length(title) = 3`), installs.
#[tokio::test]
async fn text_comparisons_on_a_nondeterministic_column_are_refused() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    create_case_insensitive_collation(&db.pool).await;
    batch(
        &db.pool,
        "create table notes (id bigint primary key, title text collate case_insensitive, \
                             body text)",
    )
    .await;
    let note_columns = columns(&[
        ("id", ValueType::Numeric),
        ("title", ValueType::Text),
        ("body", ValueType::Text),
    ]);
    for (n, (formula, function)) in [
        ("title = 'a'", "="),
        ("title <> body", "<>"),
        ("'a' != title", "<>"),
        ("title IS DISTINCT FROM body", "IS DISTINCT FROM"),
        ("body IS NOT DISTINCT FROM title", "IS NOT DISTINCT FROM"),
        ("COALESCE(body, title) = 'a'", "="),
        ("CASE WHEN body = 'x' THEN title ELSE body END = 'a'", "="),
        ("NULLIF(title, 'a') IS NULL", "NULLIF"),
    ]
    .into_iter()
    .enumerate()
    {
        let err = install_definition(
            &db.pool,
            &format!("TRANSFORM refused_{n} FROM notes SELECT {formula} AS same"),
            &note_columns,
            "public",
        )
        .await
        .expect_err(formula);
        assert_text_function_refused(err, "same", function, "trellis.notes", "title");
        assert_no_target(&db.pool, &format!("refused_{n}")).await;
    }
    // Through another field.
    let err = install_definition(
        &db.pool,
        "TRANSFORM via_field FROM notes SELECT title AS t, t = body AS same",
        &note_columns,
        "public",
    )
    .await
    .expect_err("a comparison through another field");
    assert_text_function_refused(err, "same", "=", "trellis.notes", "title");

    install_definition(
        &db.pool,
        "TRANSFORM fine FROM notes SELECT char_length(title) = 3 AS short, body = 'a' AS is_a, \
                title IS NULL AS missing, CASE WHEN title IS NULL THEN body END AS b",
        &note_columns,
        "public",
    )
    .await
    .expect("non-text values, a deterministic column and IS NULL need no deterministic collation");
}

// ---------------------------------------------------------------------------
// JOIN definitions: a read resolves against the side its table qualifier
// names.
// ---------------------------------------------------------------------------

/// The 1-1 JOIN knob is process-wide and these tests share a process: each
/// 1-1 test takes this for its whole body.
static KNOB: Mutex<()> = Mutex::const_new(());

async fn with_knob(on: bool) -> MutexGuard<'static, ()> {
    let guard = KNOB.lock().await;
    set_enabled(on);
    guard
}

/// Every text function and comparison the check covers, over `column`: the
/// formula and the function name the refusal reports.
fn text_reads(column: &str) -> Vec<(String, &'static str)> {
    vec![
        (format!("STRPOS({column}, 'a') > 0"), "STRPOS"),
        (format!("REGEXP_COUNT({column}, 'a') > 0"), "REGEXP_COUNT"),
        (format!("{column} = 'a'"), "="),
        (format!("{column} <> 'a'"), "<>"),
        (format!("'a' != {column}"), "<>"),
        (format!("{column} IS DISTINCT FROM 'a'"), "IS DISTINCT FROM"),
        (
            format!("{column} IS NOT DISTINCT FROM 'a'"),
            "IS NOT DISTINCT FROM",
        ),
        (format!("NULLIF({column}, 'a') IS NULL"), "NULLIF"),
    ]
}

/// `post_tags` (the `FROM` table, `a`) and `posts` (joined, `b`). `author` is
/// on both, `note` only on `post_tags`, `title` only on `posts`; the
/// nondeterministic collation is on `posts.author` and `post_tags.note`, so
/// `post_tags.author` and `posts.title` are the deterministic controls.
const JOIN_TABLES: &str = "create table posts (id bigint primary key, \
                                author text collate case_insensitive, title text);
     create table post_tags (id bigint primary key, post bigint, tag text, \
                             author text, note text collate case_insensitive);
     create index on post_tags (post);";

/// The aggregate JOIN counting rows where `formula` holds.
fn aggregate_join(name: &str, formula: &str) -> String {
    format!(
        "TRANSFORM {name} FROM post_tags JOIN posts ON post_tags.post = posts.id \
         GROUP BY post_tags.tag WITHOUT SOURCE INDEX \
         SELECT post_tags.tag AS tag, COUNT(CASE WHEN {formula} THEN 1 END) AS n"
    )
}

/// The 1-1 JOIN selecting `formula`.
fn one_to_one_join(name: &str, formula: &str) -> String {
    format!(
        "TRANSFORM {name} FROM post_tags JOIN posts ON post_tags.post = posts.id \
         SELECT {formula} AS n"
    )
}

/// What each test installs against, per key space.
type BuildJoin = fn(&str, &str) -> String;
const KEY_SPACES: [(&str, BuildJoin); 2] =
    [("aggregate", aggregate_join), ("1-1", one_to_one_join)];

/// Every covered read of a nondeterministic column of either side is refused
/// at define in both key spaces, naming that side's table and column, and
/// registers nothing. The same reads of the deterministic columns install,
/// including `post_tags.author` while `posts.author` is nondeterministic (the
/// same column name on the other side) and the `FROM` table's reads.
#[tokio::test]
async fn join_text_reads_of_a_nondeterministic_column_are_refused_on_either_side() {
    let _knob = with_knob(true).await;
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    create_case_insensitive_collation(&db.pool).await;
    batch(&db.pool, JOIN_TABLES).await;
    let none = HashMap::new();

    for (space, build) in KEY_SPACES {
        let mut n = 0;
        for (table, column) in [("posts", "author"), ("post_tags", "note")] {
            for (formula, function) in text_reads(&format!("{table}.{column}")) {
                n += 1;
                let name = format!("refused_{}_{n}", space.replace('-', ""));
                let err = install_definition(&db.pool, &build(&name, &formula), &none, "public")
                    .await
                    .expect_err(&format!("{space}: {formula}"));
                assert_text_function_refused(err, "n", function, table, column);
                assert_no_target(&db.pool, &name).await;
            }
        }
        // Through COALESCE and a CASE result, and in the second operand.
        for (formula, function) in [
            ("COALESCE(posts.title, posts.author) = 'a'", "="),
            (
                "CASE WHEN posts.title = 'x' THEN posts.author ELSE posts.title END = 'a'",
                "=",
            ),
            ("posts.title = posts.author", "="),
        ] {
            let name = format!("nested_{}", space.replace('-', ""));
            let err = install_definition(&db.pool, &build(&name, formula), &none, "public")
                .await
                .expect_err(formula);
            assert_text_function_refused(err, "n", function, "posts", "author");
            assert_no_target(&db.pool, &name).await;
        }
        // Deterministic columns, the same-named column of the other side, and
        // reads that need no collation install.
        for (n, formula) in [
            "STRPOS(post_tags.author, 'a') > 0",
            "posts.title = 'a'",
            "post_tags.author <> post_tags.tag",
            "CHAR_LENGTH(posts.author) = 3",
            "posts.author IS NULL",
        ]
        .into_iter()
        .enumerate()
        {
            install_definition(
                &db.pool,
                &build(&format!("fine_{}_{n}", space.replace('-', "")), formula),
                &none,
                "public",
            )
            .await
            .unwrap_or_else(|err| panic!("{space}: {formula}: {err}"));
        }
    }
    set_enabled(false);
}

/// A joined column the `FROM` table has no column of is checked (it was
/// skipped as "doesn't exist" against the wrong table), and the `FROM`
/// table's own column of a shared name is not mistaken for it: the refusal
/// names the joined table.
#[tokio::test]
async fn join_text_reads_of_a_column_only_the_joined_table_has_are_refused() {
    let _knob = with_knob(true).await;
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    create_case_insensitive_collation(&db.pool).await;
    batch(
        &db.pool,
        "create table posts (id bigint primary key, slug text collate case_insensitive);
         create table post_tags (id bigint primary key, post bigint, tag text);
         create index on post_tags (post);",
    )
    .await;
    for (space, build) in KEY_SPACES {
        let err = install_definition(
            &db.pool,
            &build(
                &format!("only_b_{}", space.replace('-', "")),
                "posts.slug = 'a'",
            ),
            &HashMap::new(),
            "public",
        )
        .await
        .expect_err(space);
        assert_text_function_refused(err, "n", "=", "posts", "slug");
    }
    set_enabled(false);
}

async fn define_only(dsn: &str) -> Trellis {
    Trellis::connect(
        Config::from_dsn(dsn.to_string()).expect("valid dsn"),
        TrellisOptions::default(),
    )
    .await
    .expect("connect a define-only Trellis")
}

/// A resume re-validates a JOIN against the live columns: re-collating a
/// read column to a nondeterministic collation while the transform is paused
/// refuses the resume, naming that side's table and column, and re-collating
/// it back lets the resume through.
#[tokio::test]
async fn resuming_a_join_whose_read_column_became_nondeterministic_is_refused() {
    let _knob = with_knob(true).await;
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    create_case_insensitive_collation(&db.pool).await;
    batch(
        &db.pool,
        "create table posts (id bigint primary key, author text, title text);
         create table post_tags (id bigint primary key, post bigint, tag text, \
                                 author text, note text);
         create index on post_tags (post);",
    )
    .await;
    let trellis = define_only(db.dsn()).await;
    let mut n = 0;
    for (space, build) in KEY_SPACES {
        for (table, column) in [("posts", "author"), ("post_tags", "note")] {
            for (formula, function) in text_reads(&format!("{table}.{column}")) {
                n += 1;
                let name = format!("resumed_{n}");
                trellis
                    .apply(&build(&name, &formula))
                    .await
                    .unwrap_or_else(|err| panic!("{space}: {formula}: {err}"));
                trellis
                    .apply(&format!("PAUSE TRANSFORM {name}"))
                    .await
                    .expect("pause");
                batch(
                    &db.pool,
                    &format!(
                        "alter table {table} alter column {column} type text \
                         collate case_insensitive"
                    ),
                )
                .await;
                let err = trellis
                    .apply(&format!("RESUME TRANSFORM {name}"))
                    .await
                    .expect_err(&format!("{space}: {formula}"));
                let message = err.to_string();
                assert!(
                    message.contains("can't resume")
                        && message.contains(&format!("{function} reads {table}.{column}")),
                    "{space}: {formula}: {message}"
                );
                assert_eq!(
                    trellis
                        .status(&name)
                        .await
                        .expect("status")
                        .map(|status| status.status),
                    Some(TransformStatus::Paused),
                    "{space}: {formula}"
                );
                batch(
                    &db.pool,
                    &format!(
                        "alter table {table} alter column {column} type text collate \"default\""
                    ),
                )
                .await;
                trellis
                    .apply(&format!("RESUME TRANSFORM {name}"))
                    .await
                    .unwrap_or_else(|err| panic!("{space}: {formula}: {err}"));
                trellis
                    .apply(&format!("PAUSE TRANSFORM {name}"))
                    .await
                    .expect("pause again");
                trellis
                    .apply(&format!("DROP TRANSFORM {name}"))
                    .await
                    .expect("drop");
            }
        }
    }
    set_enabled(false);
}

/// An aggregate JOIN's text read is refused wherever the aggregate puts it:
/// under `SUM`, `AVG` and `COUNT`'s arguments directly, in a field composed
/// of two aggregates, and written with the table's schema.
#[tokio::test]
async fn aggregate_join_text_reads_are_refused_in_every_aggregate_position() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    create_case_insensitive_collation(&db.pool).await;
    batch(&db.pool, JOIN_TABLES).await;
    for (n, (select, function)) in [
        ("SUM(STRPOS(posts.author, 'a')) AS n", "STRPOS"),
        ("AVG(CASE WHEN posts.author = 'a' THEN 1 END) AS n", "="),
        ("COUNT(NULLIF(posts.author, 'a')) AS n", "NULLIF"),
        (
            "COUNT(*) + SUM(REGEXP_COUNT(posts.author, 'a')) AS n",
            "REGEXP_COUNT",
        ),
        (
            "COUNT(CASE WHEN public.posts.author <> 'a' THEN 1 END) AS n",
            "<>",
        ),
    ]
    .into_iter()
    .enumerate()
    {
        let name = format!("positions_{n}");
        let err = install_definition(
            &db.pool,
            &format!(
                "TRANSFORM {name} FROM post_tags JOIN posts ON post_tags.post = posts.id \
                 GROUP BY post_tags.tag WITHOUT SOURCE INDEX \
                 SELECT post_tags.tag AS tag, {select}"
            ),
            &HashMap::new(),
            "public",
        )
        .await
        .expect_err(select);
        assert_text_function_refused(err, "n", function, "posts", "author");
        assert_no_target(&db.pool, &name).await;
    }
}
