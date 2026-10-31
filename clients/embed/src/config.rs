//! A handle's configuration crosses as [`Config`]'s fields and its call pool's
//! limits, with the pool's wait timeout as whole milliseconds, the unit every
//! duration a binding takes or returns uses.
//!
//! Every field but the DSN. The DSN can carry a password, and a host-language
//! config value gets printed whole far too easily: Elixir's `inspect` in a
//! Logger line or crash report, Ruby's `p`, a `to_h` serialized to JSON. A
//! redacting `inspect` would cover only some of those paths, and the host
//! already has the connection string it passed to connect. `Config`'s own
//! `Display` leaves the DSN out for the same reason.

use std::time::Duration;

use trellis::{BlockingTrellis, Config};

/// A handle's configuration flattened to plain data.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlainConfig {
    /// The schema Trellis keeps its own tables in.
    pub schema: String,
    /// The schema a bare target table name is created in.
    pub target_schema: String,
    /// The most connections the handle's call pool opens at once.
    pub max_call_pool_size: u64,
    /// How long a call waits for a free call-pool connection before failing,
    /// in milliseconds, saturating at `u64::MAX`.
    pub call_pool_wait_timeout_ms: u64,
}

impl PlainConfig {
    /// `config`'s fields, and a call pool of `max_call_pool_size` connections
    /// waiting `call_pool_wait_timeout`.
    pub fn new(
        config: &Config,
        max_call_pool_size: usize,
        call_pool_wait_timeout: Duration,
    ) -> Self {
        PlainConfig {
            schema: config.schema().to_string(),
            target_schema: config.target_schema().to_string(),
            max_call_pool_size: u64::try_from(max_call_pool_size).unwrap_or(u64::MAX),
            call_pool_wait_timeout_ms: u64::try_from(call_pool_wait_timeout.as_millis())
                .unwrap_or(u64::MAX),
        }
    }
}

impl From<&BlockingTrellis> for PlainConfig {
    /// The configuration `trellis` connected with, and its call pool as built.
    fn from(trellis: &BlockingTrellis) -> Self {
        PlainConfig::new(
            trellis.config(),
            trellis.max_call_pool_size(),
            trellis.call_pool_wait_timeout(),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_config_flattens_to_its_fields() {
        let config = Config::with_schema("host=/tmp dbname=app password=s3cret", "trellis_app")
            .unwrap()
            .with_target_schema("reporting")
            .unwrap();
        assert_eq!(
            PlainConfig::new(&config, 7, Duration::from_micros(2_500_999)),
            PlainConfig {
                schema: "trellis_app".to_string(),
                target_schema: "reporting".to_string(),
                max_call_pool_size: 7,
                // Truncated to whole milliseconds.
                call_pool_wait_timeout_ms: 2_500,
            }
        );
    }

    #[test]
    fn a_wait_past_u64_milliseconds_saturates() {
        let config = Config::with_schema("host=/tmp dbname=app", "trellis").unwrap();
        assert_eq!(
            PlainConfig::new(&config, 1, Duration::MAX).call_pool_wait_timeout_ms,
            u64::MAX
        );
    }

    #[test]
    fn the_dsn_and_its_password_never_cross() {
        let config =
            Config::with_schema("postgresql://alice:s3cret@db.example.com/app", "trellis").unwrap();
        let plain = format!(
            "{:?}",
            PlainConfig::new(&config, 20, Duration::from_secs(30))
        );
        assert!(!plain.contains("s3cret"), "{plain}");
        assert!(!plain.contains("db.example.com"), "{plain}");
    }
}
