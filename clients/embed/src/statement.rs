//! Which statement form a binding's `define` was handed, decided before
//! anything is applied.
//!
//! `trellis` has one public entry point for every statement form,
//! `apply`, and nothing public that names a statement's form without also
//! running it. A binding's `define` that simply called `apply` would carry
//! out a `DROP TRANSFORM` or `PAUSE TRANSFORM` handed to it and only then
//! report that no transform was defined. [`require_transform_statement`]
//! refuses such a statement first, so `define` can only ever define.

use trellis::ErrorCode;

use crate::PlainError;

/// `Ok` if `text` is a `TRANSFORM` statement by the rule `apply` itself
/// dispatches on, and a `validation` error, before anything has been
/// applied, if it is any other form.
///
/// That rule is the statement's first token. `trellis`'s lexer skips
/// whitespace (`char::is_whitespace`, the same set `str::trim_start` trims)
/// and reads a word as an ASCII letter or `_` followed by ASCII letters,
/// digits and `_`. Its parser then picks the form from that first word alone,
/// case-insensitively. So when this returns `Ok`, `apply` either defines a
/// transform or fails to parse, and when it returns an error, `apply` would
/// have done something other than define a transform (or failed to parse).
/// The `apply_dispatch` tests check this against the parser itself.
///
/// This belongs in `trellis` as a statement-kind query or a
/// `define_transform` of its own. Until one exists, both bindings share this
/// copy of the rule rather than each writing one.
pub fn require_transform_statement(text: &str) -> Result<(), PlainError> {
    let rest = text.trim_start();
    let word_len = rest
        .find(|c: char| !(c.is_ascii_alphanumeric() || c == '_'))
        .unwrap_or(rest.len());
    if rest[..word_len].eq_ignore_ascii_case("TRANSFORM") {
        return Ok(());
    }
    Err(PlainError::new(
        ErrorCode::Validation,
        "define takes only a TRANSFORM statement; this statement is another form (or \
         not a statement), so nothing was applied",
    ))
}

#[cfg(test)]
mod tests {
    use trellis::dev::defs::{ast::Statement, parse_statement};

    use super::require_transform_statement;

    /// What `apply` would do with `text`: `Some(true)` if it defines a
    /// transform, `Some(false)` if it applies some other form, `None` if it
    /// fails to parse (and so applies nothing).
    fn applies_a_transform(text: &str) -> Option<bool> {
        parse_statement(text)
            .ok()
            .map(|statement| matches!(statement, Statement::DefineTransform(_)))
    }

    /// The guard's contract: it lets through every statement `apply` would
    /// define a transform from, and refuses every statement `apply` would
    /// carry out as another form. On text that doesn't parse either answer
    /// is harmless, since `apply` then applies nothing.
    fn assert_guard_matches_apply(text: &str) {
        let guard = require_transform_statement(text);
        match applies_a_transform(text) {
            Some(true) => assert!(guard.is_ok(), "refused a TRANSFORM statement: {text:?}"),
            Some(false) => assert!(
                guard.is_err(),
                "let through a statement apply would carry out as another form: {text:?}"
            ),
            None => {}
        }
    }

    /// One statement of every form `apply` takes, each of which parses.
    const EVERY_FORM: [&str; 10] = [
        "TRANSFORM widget_prices FROM widgets SELECT price AS price",
        "RELATIONSHIP owner FROM widgets.owner_id TO users.id",
        "PAUSE TRANSFORM widget_prices",
        "PAUSE TRANSFORM widget_prices.price",
        "RESUME TRANSFORM widget_prices",
        "DROP TRANSFORM widget_prices",
        "DROP RELATIONSHIP widgets.owner",
        "ALTER TRANSFORM widget_prices ADD price + price AS doubled",
        "ALTER TRANSFORM widget_prices DROP doubled",
        "ALTER TRANSFORM widget_prices ALTER doubled AS price + price + price",
    ];

    #[test]
    fn apply_dispatch_every_form_is_let_through_only_if_it_is_a_transform() {
        for text in EVERY_FORM {
            assert!(
                applies_a_transform(text).is_some(),
                "{text:?} must parse, or this test checks nothing"
            );
            assert_guard_matches_apply(text);
        }
        assert!(require_transform_statement(EVERY_FORM[0]).is_ok());
        for text in &EVERY_FORM[1..] {
            let err = require_transform_statement(text).unwrap_err();
            assert_eq!(err.code, "validation");
            assert!(err.message.contains("nothing was applied"));
        }
    }

    #[test]
    fn apply_dispatch_case_and_leading_whitespace_are_the_lexers() {
        for text in [
            "transform t FROM widgets SELECT price AS price",
            "\n\t  TrAnSfOrM t FROM widgets SELECT price AS price",
            "\u{00a0}\u{2003}TRANSFORM t FROM widgets SELECT price AS price",
            "  drop TRANSFORM t",
            "\u{3000}Pause TRANSFORM t",
            "TRANSFORMS t FROM widgets SELECT price AS price",
            "TRANSFORM_x t FROM widgets SELECT price AS price",
            "_TRANSFORM t",
            "",
            "   ",
        ] {
            assert_guard_matches_apply(text);
        }
        assert!(require_transform_statement("  \u{2003}transform t").is_ok());
        assert!(require_transform_statement("\u{3000}Pause TRANSFORM t").is_err());
        assert!(require_transform_statement("TRANSFORMS t").is_err());
        assert!(require_transform_statement("").is_err());
    }

    proptest::proptest! {
        /// Every form, behind arbitrary leading whitespace and with each
        /// ASCII letter's case flipped at random, is still judged the way
        /// `apply` judges it. The whitespace set includes non-ASCII spaces
        /// and the case flips include the leading keyword.
        #[test]
        fn apply_dispatch_holds_under_whitespace_and_case(
            form in 0..EVERY_FORM.len(),
            leading in proptest::collection::vec(
                proptest::sample::select(vec![" ", "\t", "\n", "\r", "\u{00a0}", "\u{2003}", "\u{3000}"]),
                0..4,
            ),
            flips in proptest::collection::vec(proptest::bool::ANY, 64),
        ) {
            let body: String = EVERY_FORM[form]
                .chars()
                .zip(flips.iter().cycle())
                .map(|(c, &flip)| if flip { c.to_ascii_lowercase() } else { c })
                .collect();
            let text = format!("{}{body}", leading.concat());
            assert_guard_matches_apply(&text);
        }

        /// Arbitrary text never gets through the guard as something `apply`
        /// would carry out as another form, and never panics it.
        #[test]
        fn apply_dispatch_holds_for_arbitrary_text(text in "\\PC{0,40}") {
            assert_guard_matches_apply(&text);
        }
    }
}
