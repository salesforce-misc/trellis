//! `sample_quarantined` pages by keyset: its `after` argument is the
//! `(src_table, key)` of the last row the caller already has. That pair
//! crosses as one opaque string the caller hands back for the next page
//! (ADR-0010 decision 4), so neither host models a tuple whose parts it has
//! no business reading.
//!
//! The encoding is `<byte length of src_table>:<src_table><key>`. The length
//! prefix makes it unambiguous for any two strings, whatever characters
//! either holds. Hosts must treat it as opaque; only this module reads it.

use trellis::{ErrorCode, PoisonSample};

use crate::PlainError;

/// The cursor for the page after `(src_table, key)`.
pub fn encode_cursor(src_table: &str, key: &str) -> String {
    format!("{}:{src_table}{key}", src_table.len())
}

/// The cursor for the page after `page`, or `None` for an empty page, which
/// has no last row to continue from. A page shorter than the limit still gets
/// a cursor: rows quarantined since may sort after it.
pub fn next_cursor(page: &[PoisonSample]) -> Option<String> {
    page.last()
        .map(|last| encode_cursor(&last.src_table, &last.key))
}

/// Reads a cursor back into `sample_quarantined`'s `after` argument. `None`
/// (a host's `nil`) asks for the first page. A string [`encode_cursor`] could
/// not have produced is a `validation` error.
pub fn decode_cursor(cursor: Option<&str>) -> Result<Option<(String, String)>, PlainError> {
    let Some(cursor) = cursor else {
        return Ok(None);
    };
    let malformed = || {
        PlainError::new(
            ErrorCode::Validation,
            format!("{cursor:?} is not a sample_quarantined cursor"),
        )
    };
    let (len, rest) = cursor.split_once(':').ok_or_else(malformed)?;
    // Canonical digits only (no sign, no leading zero), so a cursor that
    // decodes is exactly the one `encode_cursor` would produce.
    let canonical = !len.is_empty()
        && len.bytes().all(|b| b.is_ascii_digit())
        && (len == "0" || !len.starts_with('0'));
    if !canonical {
        return Err(malformed());
    }
    let len: usize = len.parse().map_err(|_| malformed())?;
    if !rest.is_char_boundary(len) {
        return Err(malformed());
    }
    let (src_table, key) = rest.split_at(len);
    Ok(Some((src_table.to_string(), key.to_string())))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample(src_table: &str, key: &str) -> PoisonSample {
        PoisonSample {
            src_table: src_table.to_string(),
            key: key.to_string(),
            error_message: "division by zero".to_string(),
        }
    }

    #[test]
    fn no_cursor_is_the_first_page() {
        assert_eq!(decode_cursor(None).unwrap(), None);
    }

    #[test]
    fn a_cursor_round_trips_whatever_its_parts_contain() {
        for (src_table, key) in [
            ("public.orders", "42"),
            ("public.orders", ""),
            ("", "42"),
            ("", ""),
            // Separator and prefix look-alikes in either part.
            ("a:b", "c:d"),
            ("12:x", "3:"),
            // Multi-byte text, and a composite key's rendering.
            ("public.bestellungen", "(\"müller\",7)"),
            ("naïve", "日本"),
        ] {
            let cursor = encode_cursor(src_table, key);
            assert_eq!(
                decode_cursor(Some(&cursor)).unwrap(),
                Some((src_table.to_string(), key.to_string())),
                "{cursor:?}",
            );
        }
    }

    #[test]
    fn the_next_cursor_continues_after_the_last_row() {
        let page = [sample("public.orders", "1"), sample("public.orders", "2")];

        let cursor = next_cursor(&page).unwrap();

        assert_eq!(
            decode_cursor(Some(&cursor)).unwrap(),
            Some(("public.orders".to_string(), "2".to_string())),
        );
    }

    #[test]
    fn an_empty_page_has_no_next_cursor() {
        assert_eq!(next_cursor(&[]), None);
    }

    proptest::proptest! {
        /// Any two strings, whatever bytes they hold, come back unchanged.
        #[test]
        fn any_pair_round_trips(src_table in ".*", key in ".*") {
            let cursor = encode_cursor(&src_table, &key);
            proptest::prop_assert_eq!(
                decode_cursor(Some(&cursor)).unwrap(),
                Some((src_table, key)),
            );
        }

        /// Any string decodes without panicking, to either a validation error
        /// or exactly the pair whose cursor it is.
        #[test]
        fn any_string_decodes_canonically_or_is_rejected(cursor in "[0-9]{0,3}:?.*|.*") {
            match decode_cursor(Some(&cursor)) {
                Ok(Some((src_table, key))) => {
                    proptest::prop_assert_eq!(encode_cursor(&src_table, &key), cursor);
                }
                Ok(None) => proptest::prop_assert!(false, "Some decoded to None"),
                Err(err) => proptest::prop_assert_eq!(err.code, "validation"),
            }
        }
    }

    #[test]
    fn a_malformed_cursor_is_a_validation_error() {
        for cursor in [
            "",
            "orders",
            ":orders",
            "x:orders",
            "+6:orders",
            "-1:orders",
            "06:orders1",
            "99:orders",
            // Splits inside the two-byte `ï`.
            "3:naïve",
            "99999999999999999999999:x",
        ] {
            let err = decode_cursor(Some(cursor)).unwrap_err();
            assert_eq!(err.code, "validation", "{cursor:?}");
        }
    }
}
