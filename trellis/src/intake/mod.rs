//! What is left of intake after trigger capture replaced the
//! logical-replication stream (ADR-0002): the table-name helper
//! and the rebuild of a table's readers.
//!
//! - [`catch_up`] rebuilds the readers of a table that may be missing
//!   changes to it.
//! - `harness` stands in for the staging worker's build start in tests.

pub(crate) mod catch_up;
pub mod error;
#[cfg(any(test, feature = "internals"))]
pub mod harness;

pub use error::IntakeError;

/// Joins `schema` and `table` into the `"schema.table"` shape a ring row's
/// `src_table` uses throughout this crate: the *only* place that string is
/// built. A schema or table literally named `a.b` is legal Postgres (quoted),
/// but the joined-string representation has no way to tell that dot apart
/// from the separator, so this turns it into a loud error at the point the
/// ambiguous string would otherwise be built.
pub(crate) fn qualify(schema: &str, table: &str) -> Result<String, IntakeError> {
    if schema.contains('.') {
        return Err(IntakeError::DottedIdentifierComponent {
            component: schema.to_string(),
        });
    }
    if table.contains('.') {
        return Err(IntakeError::DottedIdentifierComponent {
            component: table.to_string(),
        });
    }
    Ok(format!("{schema}.{table}"))
}

/// A minimal, dependency-free JSON string-literal encoder (quote, backslash,
/// control characters) — just enough for column names and text-format
/// column values, not a general serializer.
pub(crate) fn json_string(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if (c as u32) < 0x20 => out.push_str(&format!("\\u{:04x}", c as u32)),
            c => out.push(c),
        }
    }
    out.push('"');
    out
}

#[cfg(test)]
mod tests {
    use super::json_string;

    #[test]
    fn json_string_escapes_quotes_backslashes_and_control_characters() {
        assert_eq!(json_string("plain"), r#""plain""#);
        assert_eq!(json_string(r#"a"b\c"#), r#""a\"b\\c""#);
        assert_eq!(json_string("t\tn\nr\r"), r#""t\tn\nr\r""#);
        assert_eq!(json_string("\u{1}\u{1f}"), r#""\u0001\u001f""#);
        assert_eq!(json_string("héllo"), r#""héllo""#);
    }
}
