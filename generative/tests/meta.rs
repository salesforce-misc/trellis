//! Meta-tests for the harness itself (issue #4, design doc §6): a green
//! generative run must prove it ran. These are deliberately not about any
//! generated program or oracle comparison — they check that the harness's
//! own plumbing (standing up a cluster, classifying a backend that can't
//! stand up) behaves, independent of any classifier logic under test.

use generative::backend::ManualBackend;
use generative::run::{Outcome, classify_stand_up};
use testkit::TestCluster;

/// Fails fast if the harness cannot stand up a `testkit` cluster it should
/// have been able to — a sanity check on the harness, not on any property.
/// Deliberately independent of [`Outcome`]/`classify_stand_up`: this only
/// proves the cluster itself comes up and accepts a trivial query.
#[tokio::test(flavor = "multi_thread")]
async fn the_harness_can_stand_up_a_testkit_cluster() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;

    let row = db
        .pool
        .get()
        .await
        .expect("pool connection")
        .query_one("select 1", &[])
        .await
        .expect("trivial query against a freshly-provisioned cluster");
    let value: i32 = row.get(0);
    assert_eq!(value, 1);
}

/// The other half of design doc §6: a backend that is provisioned (a real,
/// healthy `testkit` cluster) but cannot stand up must be reported as
/// `BackendUnusable`, never silently skipped or treated as green. Forces the
/// failure by connecting to a database name the cluster never created,
/// which fails at the raw `tokio_postgres::connect` step inside
/// `ManualBackend::connect`.
#[tokio::test(flavor = "multi_thread")]
async fn a_backend_that_cannot_stand_up_is_reported_unusable() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;

    let broken_dsn = db.dsn().replace(db.name(), "does_not_exist_at_all");
    assert_ne!(
        broken_dsn,
        db.dsn(),
        "test setup bug: db.dsn() must contain db.name() for this corruption to take effect"
    );

    let outcome = match classify_stand_up(ManualBackend::connect(broken_dsn).await) {
        Ok(_) => panic!("a backend pointed at a nonexistent database must not stand up"),
        Err(outcome) => outcome,
    };

    match &outcome {
        Outcome::BackendUnusable { error } => {
            assert!(
                !error.is_empty(),
                "BackendUnusable must carry the engine's error text"
            );
        }
        other => panic!("expected BackendUnusable, got {other}"),
    }
    assert!(
        !outcome.as_pass(),
        "BackendUnusable must never be a pass: {outcome}"
    );
}

/// Self-enforcing half of the fast/deep split (issue #172): every proptest property is `#[ignore]`d, so a plain `cargo test
/// --workspace` — the command every contributor and CI lane reaches for first —
/// never deep-runs them by accident. Deep runs opt in with `-- --ignored` (just
/// the properties) or `-- --include-ignored` (everything). Properties are also
/// named with a `property_` prefix so they stay easy to spot and to filter
/// (`cargo test -p generative -- --ignored property_convergence`).
///
/// This scans every `generative/tests/*.rs` source file, finds each
/// `proptest! { ... }` block, and asserts every `fn` declared inside one both
/// starts with `property_` and carries an `#[ignore ...]` attribute — so a
/// future property added without either fails the build here instead of
/// silently running on every push at PR-time case counts.
///
/// This is a line-based scan, not a real parser: it tracks brace depth to
/// find the extent of each `proptest! { ... }` block, and within that span
/// looks for lines whose *trimmed* text starts with `fn ` (or `pub fn `) to
/// find declarations, and lines starting with `#[ignore` to find the
/// attribute on the next declaration — matching how every `proptest!` block
/// in this crate is actually formatted today (macro invocation alone on its
/// own line, attributes and `fn` declarations flush against the block's
/// indentation, never inlined after other code). It does not understand
/// string/char literals containing brace characters, so a `proptest!` block
/// containing a brace inside a string literal could throw off depth tracking;
/// none currently do. It also would not catch a property function declared
/// with unusual formatting this scan doesn't recognize (e.g. `fn` and the name
/// split across a line break, or a macro-generated fn) — see this test's own
/// source if extending the property list ever needs either.
#[test]
fn every_fn_declared_inside_proptest_blocks_is_prefixed_and_ignored_by_default() {
    let tests_dir = concat!(env!("CARGO_MANIFEST_DIR"), "/tests");
    let mut violations = Vec::new();
    let mut saw_any_proptest_block = false;

    let mut entries: Vec<_> = std::fs::read_dir(tests_dir)
        .expect("generative/tests directory must be readable")
        .filter_map(|entry| entry.ok())
        .map(|entry| entry.path())
        .filter(|path| path.extension().is_some_and(|ext| ext == "rs"))
        .collect();
    entries.sort();

    for path in entries {
        let source = std::fs::read_to_string(&path)
            .unwrap_or_else(|err| panic!("reading {}: {err}", path.display()));
        let fns = functions_declared_in_proptest_blocks(&source);
        if !fns.is_empty() {
            saw_any_proptest_block = true;
        }
        for ProptestFn { name, ignored } in fns {
            if !name.starts_with("property_") {
                violations.push(format!(
                    "{}: fn {name} is missing the `property_` prefix",
                    path.display()
                ));
            }
            if !ignored {
                violations.push(format!(
                    "{}: fn {name} is missing an `#[ignore = \"...\"]` attribute",
                    path.display()
                ));
            }
        }
    }

    assert!(
        saw_any_proptest_block,
        "found no `proptest! {{ ... }}` blocks at all under {tests_dir} -- the scanner itself is \
         broken (it should have found at least the crate's known properties), not that the crate \
         suddenly has none"
    );
    assert!(
        violations.is_empty(),
        "every fn inside a `proptest! {{ ... }}` block must be named with a `property_` prefix \
         and marked `#[ignore]` so plain `cargo test` never deep-runs it (see \
         generative/README.md), but found:\n{}",
        violations.join("\n")
    );
}

/// One `fn` declared directly inside a `proptest! { ... }` block.
#[derive(Debug, PartialEq, Eq)]
struct ProptestFn {
    name: String,
    /// Whether an `#[ignore ...]` attribute appeared between the previous
    /// declaration (or the block's opening) and this one.
    ignored: bool,
}

/// Scans `source` for every `proptest! { ... }` block (tracking brace depth
/// from the line containing the macro invocation to the line where that
/// depth returns to zero) and returns every `fn` declared directly inside
/// one. See
/// [`every_fn_declared_inside_proptest_blocks_is_prefixed_and_ignored_by_default`]'s
/// doc comment for this scan's known shape assumptions and blind spots.
fn functions_declared_in_proptest_blocks(source: &str) -> Vec<ProptestFn> {
    let mut fns = Vec::new();
    let mut in_block = false;
    let mut depth: i32 = 0;
    let mut pending_ignore = false;

    for line in source.lines() {
        if !in_block {
            let Some(bang_idx) = line.find("proptest!") else {
                continue;
            };
            if !line[bang_idx..].contains('{') {
                continue;
            }
            in_block = true;
            pending_ignore = false;
            depth = 0;
            depth += brace_delta(&line[bang_idx..]);
            if depth <= 0 {
                in_block = false;
            }
            continue;
        }

        if line.trim_start().starts_with("#[ignore") {
            pending_ignore = true;
        }
        if let Some(name) = fn_name_declared_on_line(line) {
            fns.push(ProptestFn {
                name,
                ignored: pending_ignore,
            });
            pending_ignore = false;
        }

        depth += brace_delta(line);
        if depth <= 0 {
            in_block = false;
        }
    }

    fns
}

/// Net change in brace depth across `text` (ignores string/char literals --
/// see the caller's doc comment).
fn brace_delta(text: &str) -> i32 {
    text.chars().fold(0, |depth, ch| match ch {
        '{' => depth + 1,
        '}' => depth - 1,
        _ => depth,
    })
}

/// If `line`'s trimmed text starts with a (possibly `pub`) `fn ` declaration,
/// returns the declared name. Deliberately anchored on the *start* of the
/// trimmed line, not a mid-line search, so it doesn't false-positive on `fn`
/// mentioned in prose inside a doc comment (this crate's doc comments
/// reference other functions in backticks/prose, never at the start of a
/// line).
fn fn_name_declared_on_line(line: &str) -> Option<String> {
    let trimmed = line.trim_start();
    let trimmed = trimmed.strip_prefix("pub ").unwrap_or(trimmed);
    let rest = trimmed.strip_prefix("fn ")?;
    let end = rest
        .find(|c: char| !(c.is_alphanumeric() || c == '_'))
        .unwrap_or(rest.len());
    if end == 0 {
        return None;
    }
    Some(rest[..end].to_string())
}

#[cfg(test)]
mod proptest_scanner_tests {
    use super::{ProptestFn, functions_declared_in_proptest_blocks};

    fn names(source: &str) -> Vec<String> {
        functions_declared_in_proptest_blocks(source)
            .into_iter()
            .map(|f| f.name)
            .collect()
    }

    #[test]
    fn finds_a_single_prefixed_fn_in_a_simple_block() {
        let source = "proptest! {\n    #[test]\n    fn property_foo(x in 0..1i32) {\n        assert!(x >= 0);\n    }\n}\n";
        assert_eq!(names(source), vec!["property_foo".to_string()]);
    }

    #[test]
    fn finds_multiple_properties_in_one_block() {
        let source = "proptest! {\n    #![proptest_config(cfg())]\n\n    #[test]\n    fn property_a(x in 0..1i32) {\n        assert!(x >= 0);\n    }\n\n    #[test]\n    fn property_b(\n        x in 0..1i32,\n    ) {\n        assert!(x >= 0);\n    }\n}\n";
        assert_eq!(
            names(source),
            vec!["property_a".to_string(), "property_b".to_string()]
        );
    }

    #[test]
    fn catches_a_fn_missing_the_prefix() {
        let source = "proptest! {\n    #[test]\n    fn not_prefixed_correctly(x in 0..1i32) {\n        assert!(x >= 0);\n    }\n}\n";
        let names = names(source);
        assert_eq!(names, vec!["not_prefixed_correctly".to_string()]);
        assert!(!names[0].starts_with("property_"));
    }

    #[test]
    fn ignores_fns_outside_any_proptest_block() {
        let source = "fn helper() {}\n\nproptest! {\n    #[test]\n    fn property_only_one(x in 0..1i32) {\n        assert!(x >= 0);\n    }\n}\n\nfn another_helper() {}\n";
        assert_eq!(names(source), vec!["property_only_one".to_string()]);
    }

    #[test]
    fn records_which_fns_carry_an_ignore_attribute() {
        let source = "proptest! {\n    /// Doc comment.\n    #[test]\n    #[ignore = \"deep lane\"]\n    fn property_a(x in 0..1i32) {\n        assert!(x >= 0);\n    }\n\n    #[test]\n    fn property_b(x in 0..1i32) {\n        assert!(x >= 0);\n    }\n}\n";
        assert_eq!(
            functions_declared_in_proptest_blocks(source),
            vec![
                ProptestFn {
                    name: "property_a".to_string(),
                    ignored: true,
                },
                ProptestFn {
                    name: "property_b".to_string(),
                    ignored: false,
                },
            ]
        );
    }

    #[test]
    fn an_ignore_outside_the_block_does_not_leak_into_it() {
        let source = "#[ignore]\n#[test]\nfn unrelated() {}\n\nproptest! {\n    #[test]\n    fn property_a(x in 0..1i32) {\n        assert!(x >= 0);\n    }\n}\n";
        assert_eq!(
            functions_declared_in_proptest_blocks(source),
            vec![ProptestFn {
                name: "property_a".to_string(),
                ignored: false,
            }]
        );
    }
}
