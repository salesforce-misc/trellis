//! Errors cross as `(code, message)`: exactly [`ErrorCode::as_str`] plus the
//! error's `Display` text, with no error chain, no `source()` walking and no
//! internal variant names (ADR-0010 decision 4).

use std::fmt;

use trellis::{
    ApplyError, CatalogError, ClientError, DdlError, ErrorCode, IntakeError, ParseError,
    SelfCheckError, StagingError, TrellisError, ValidationError,
};

/// Every error code the bindings map explicitly: one Elixir atom and one Ruby
/// `Trellis::Error` subclass each, allocated from this list at load time.
///
/// Written out by hand rather than derived from [`ErrorCode::ALL`] on purpose.
/// [`ErrorCode`] is `#[non_exhaustive]`, so each host also needs a fallback
/// for a code missing from this list; the test below fails when `trellis`
/// grows a code this list lacks, so adding one is a deliberate binding change
/// (a new atom, a new exception subclass) rather than a silent downgrade to
/// the fallback.
pub const ERROR_CODES: [&str; 6] = [
    "parse",
    "validation",
    "connectivity",
    "conflict",
    "not_found",
    "internal",
];

/// An error flattened for an FFI boundary: the stable code and the message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlainError {
    /// [`ErrorCode::as_str`]. A host maps each of [`ERROR_CODES`] to its own
    /// name and falls back for anything else.
    pub code: &'static str,
    /// The error's `Display` text.
    pub message: String,
}

impl PlainError {
    /// A plain error with `code`'s stable name and `message`.
    pub fn new(code: ErrorCode, message: impl Into<String>) -> Self {
        PlainError {
            code: code.as_str(),
            message: message.into(),
        }
    }
}

impl fmt::Display for PlainError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for PlainError {}

/// A `trellis` error that reports an [`ErrorCode`]. Every public error type
/// has an inherent `code()`; this trait lets [`PlainError::from`] take any of
/// them.
pub trait CodedError: fmt::Display {
    /// The error's stable category, as its inherent `code()` reports it.
    fn code(&self) -> ErrorCode;
}

/// Implements [`CodedError`] by delegating to each type's inherent `code()`.
macro_rules! coded_error {
    ($($ty:ty),* $(,)?) => {
        $(
            impl CodedError for $ty {
                fn code(&self) -> ErrorCode {
                    <$ty>::code(self)
                }
            }
        )*
    };
}

// Every error type `trellis` exports at its root. A binding sees only
// `TrellisError` (from `BlockingTrellis`) and `trellis::Error` (from `Config`)
// today; the rest are the payloads those wrap, and cost nothing to cover.
coded_error!(
    TrellisError,
    trellis::Error,
    ClientError,
    CatalogError,
    DdlError,
    ParseError,
    ValidationError,
    IntakeError,
    ApplyError,
    StagingError,
    SelfCheckError,
);

impl<E: CodedError> From<E> for PlainError {
    fn from(err: E) -> Self {
        PlainError::new(err.code(), err.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// ADR-0010 decision 4's required test: every current [`ErrorCode`] has
    /// an explicit binding mapping. A code `trellis` adds fails here until it
    /// is added to [`ERROR_CODES`], and with it to both hosts' mappings.
    #[test]
    fn every_error_code_has_an_explicit_mapping() {
        for code in ErrorCode::ALL {
            assert!(
                ERROR_CODES.contains(&code.as_str()),
                "ErrorCode::{code:?} (\"{code}\") has no binding mapping: add it to \
                 ERROR_CODES, then give it an Elixir atom and a Ruby Trellis::Error subclass",
            );
        }
    }

    /// The other direction: no mapping for a code `trellis` no longer has,
    /// and no code listed twice.
    #[test]
    fn every_mapping_names_a_current_error_code() {
        for name in ERROR_CODES {
            assert!(
                ErrorCode::ALL.iter().any(|code| code.as_str() == name),
                "ERROR_CODES lists \"{name}\", which no ErrorCode reports",
            );
        }
        let mut sorted = ERROR_CODES;
        sorted.sort_unstable();
        let before = sorted.len();
        let mut deduped = sorted.to_vec();
        deduped.dedup();
        assert_eq!(deduped.len(), before, "ERROR_CODES lists a code twice");
    }

    #[test]
    fn a_trellis_error_crosses_as_its_code_and_display() {
        let err = TrellisError::SourceTableNotFound("widgets".to_string());
        let message = err.to_string();

        let plain = PlainError::from(err);

        assert_eq!(plain.code, "not_found");
        assert_eq!(plain.message, message);
        assert_eq!(plain.to_string(), message);
    }

    /// A wrapped error reports the wrapped error's code, and the message is
    /// the outer `Display` only.
    #[test]
    fn a_nested_error_crosses_as_the_outer_display_with_the_inner_code() {
        let inner = CatalogError::SourceTableNotFound("orders".to_string());
        let inner_code = inner.code();
        let outer = TrellisError::Catalog(inner);
        let message = outer.to_string();

        let plain = PlainError::from(outer);

        assert_eq!(plain.code, inner_code.as_str());
        assert_eq!(plain.message, message);
    }

    /// `trellis::Error`, the type `Config`'s constructors fail with, crosses
    /// the same way.
    #[test]
    fn a_config_error_crosses_as_validation() {
        let err = trellis::Config::from_dsn("host=localhost")
            .unwrap()
            .with_pool_max_size(0)
            .unwrap_err();
        let message = err.to_string();

        let plain = PlainError::from(err);

        assert_eq!(plain.code, "validation");
        assert_eq!(plain.message, message);
    }

    #[test]
    fn new_uses_the_code_s_stable_name() {
        let plain = PlainError::new(ErrorCode::Conflict, "taken");
        assert_eq!(
            plain,
            PlainError {
                code: "conflict",
                message: "taken".to_string(),
            }
        );
    }
}
