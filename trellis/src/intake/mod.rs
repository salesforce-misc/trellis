//! What is left of intake (issue #7) after trigger capture replaced the
//! logical-replication stream (issue #622, ADR-0002): the backfill markers,
//! their discharge and the orphan sweep. F (#625) deletes these; the module
//! keeps its name until then (issue #622 plan, Q6).
//!
//! - [`markers`] parks `pending_backfill` markers and discharges them:
//!   the transaction fence, enumeration into the ring, dispatch of waiting
//!   definitions, and go-live catch-ups.
//! - `resume_orphans` is the discharge's sweep of target rows no source row
//!   backs any more (issue #330).

pub mod error;
pub mod markers;
mod resume_orphans;

pub use error::IntakeError;

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
