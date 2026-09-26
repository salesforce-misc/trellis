//! Which statement form a binding's `define` was handed, decided before
//! anything is applied.
//!
//! `trellis` has one entry point for every statement form, `apply`. A
//! binding's `define` that simply called it would carry out a
//! `DROP TRANSFORM` or `PAUSE TRANSFORM` handed to it and only then report
//! that no transform was defined. [`require_transform_statement`] refuses
//! such a statement first, so `define` can only ever define.

use trellis::{ErrorCode, StatementKind};

use crate::PlainError;

/// `Ok` if `text` is a well-formed `TRANSFORM` statement, and an error,
/// before anything has been applied, if it is not.
///
/// The form comes from [`trellis::statement_kind`], which runs `apply`'s own
/// parser over the whole text, so `Ok` means `apply` will define a
/// transform (or fail doing it, never do something else). Any other
/// well-formed statement is a `validation` error naming its form, and text
/// that doesn't parse is the `parse` error `apply` would have returned.
pub fn require_transform_statement(text: &str) -> Result<(), PlainError> {
    match trellis::statement_kind(text)? {
        StatementKind::DefineTransform => Ok(()),
        other => Err(PlainError::new(
            ErrorCode::Validation,
            format!(
                "define takes only a TRANSFORM statement, and this is a {} statement, so \
                 nothing was applied",
                other.keywords()
            ),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::require_transform_statement;

    #[test]
    fn a_transform_statement_is_let_through() {
        for text in [
            "TRANSFORM widget_prices FROM widgets SELECT price AS price",
            "\n\u{2003} transform widget_prices FROM widgets SELECT price AS price",
        ] {
            assert!(require_transform_statement(text).is_ok(), "{text:?}");
        }
    }

    #[test]
    fn every_other_form_is_refused_as_validation_naming_its_form() {
        for (text, keyword) in [
            (
                "RELATIONSHIP owner FROM widgets.owner_id TO users.id",
                "RELATIONSHIP",
            ),
            ("PAUSE TRANSFORM widget_prices", "PAUSE TRANSFORM"),
            ("RESUME TRANSFORM widget_prices", "RESUME TRANSFORM"),
            ("  drop TRANSFORM widget_prices", "DROP TRANSFORM"),
            ("DROP RELATIONSHIP widgets.owner", "DROP RELATIONSHIP"),
            (
                "ALTER TRANSFORM widget_prices DROP doubled",
                "ALTER TRANSFORM",
            ),
        ] {
            let err = require_transform_statement(text).unwrap_err();
            assert_eq!(err.code, "validation", "{text:?}");
            assert!(err.message.contains("nothing was applied"), "{text:?}");
            assert!(
                err.message.contains(&format!("a {keyword} statement")),
                "{text:?}: {}",
                err.message
            );
        }
    }

    /// Text `apply` couldn't parse is refused with the same `parse` error,
    /// whatever keyword it leads with: a malformed `TRANSFORM` is not let
    /// through, and a malformed `DROP` isn't mistaken for a well-formed one.
    #[test]
    fn text_that_does_not_parse_is_a_parse_error() {
        for text in [
            "TRANSFORM oops",
            "DROP widget_prices",
            "TRANSFORMS t",
            "",
            "   ",
        ] {
            let err = require_transform_statement(text).unwrap_err();
            assert_eq!(err.code, "parse", "{text:?}");
        }
    }
}
