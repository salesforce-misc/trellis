//! `watermark_token`'s read-your-writes token crosses as an opaque string the
//! caller hands back to `await_converged`, like `sample_quarantined`'s cursor
//! (ADR-0010 decision 4): the host has no business doing arithmetic on it.
//!
//! The string is the WAL position in Postgres's own `X/X` spelling
//! (`pg_current_wal_lsn()`'s), which makes it readable in a log line. Hosts
//! must still treat it as opaque; only this module reads it.

use tokio_postgres::types::PgLsn;
use trellis::ErrorCode;

use crate::PlainError;

/// `token` as the opaque string a host holds.
pub fn encode_watermark(token: PgLsn) -> String {
    token.to_string()
}

/// Reads a token [`encode_watermark`] produced back into the `PgLsn`
/// `await_converged` takes. Any other string, including a non-canonical
/// spelling of a real position, is a `validation` error.
pub fn decode_watermark(token: &str) -> Result<PgLsn, PlainError> {
    token
        .parse::<PgLsn>()
        .ok()
        .filter(|lsn| encode_watermark(*lsn) == token)
        .ok_or_else(|| {
            PlainError::new(
                ErrorCode::Validation,
                format!("{token:?} is not a watermark token"),
            )
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_token_round_trips() {
        for lsn in [0, 1, 0x16B_3748, 0xFFFF_FFFF, 0x1_0000_0000, u64::MAX] {
            let token = encode_watermark(PgLsn::from(lsn));
            assert_eq!(u64::from(decode_watermark(&token).unwrap()), lsn, "{token}");
        }
        assert_eq!(encode_watermark(PgLsn::from(0x1_016B_3748)), "1/16B3748");
    }

    #[test]
    fn anything_else_is_a_validation_error() {
        for token in [
            "",
            "16B3748",
            "0/",
            "/0",
            "0/16b3748",
            "00/16B3748",
            "+0/16B3748",
            "1_0000_0000/0",
            "100000000/0",
            "0/100000000",
            "x/y",
        ] {
            let err = decode_watermark(token).unwrap_err();
            assert_eq!(err.code, "validation", "{token:?}");
        }
    }
}
