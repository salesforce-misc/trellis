//! [`Config`] crosses as its fields, with the pool's wait timeout as whole
//! milliseconds, the unit every duration a binding takes or returns uses.
//!
//! Every field but the DSN. The DSN can carry a password, and a host-language
//! config value gets printed whole far too easily: Elixir's `inspect` in a
//! Logger line or crash report, Ruby's `p`, a `to_h` serialized to JSON. A
//! redacting `inspect` would cover only some of those paths, and the host
//! already has the connection string it passed to connect. `Config`'s own
//! `Display` leaves the DSN out for the same reason.

use trellis::Config;

/// A [`Config`] flattened to plain data.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlainConfig {
    /// The schema Trellis keeps its own tables in.
    pub schema: String,
    /// The schema a bare target table name is created in.
    pub target_schema: String,
    /// The cap on the connection pool's physical connections.
    pub pool_max_size: u64,
    /// How long a call waits for a free pooled connection before failing,
    /// in milliseconds, saturating at `u64::MAX`.
    pub pool_wait_timeout_ms: u64,
}

impl From<&Config> for PlainConfig {
    fn from(config: &Config) -> Self {
        PlainConfig {
            schema: config.schema().to_string(),
            target_schema: config.target_schema().to_string(),
            pool_max_size: u64::try_from(config.pool_max_size()).unwrap_or(u64::MAX),
            pool_wait_timeout_ms: u64::try_from(config.pool_wait_timeout().as_millis())
                .unwrap_or(u64::MAX),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    #[test]
    fn a_config_flattens_to_its_fields() {
        let config = Config::with_schema("host=/tmp dbname=app password=s3cret", "trellis_app")
            .unwrap()
            .with_target_schema("reporting")
            .unwrap()
            .with_pool_max_size(7)
            .unwrap()
            .with_pool_wait_timeout(Duration::from_micros(2_500_999));
        assert_eq!(
            PlainConfig::from(&config),
            PlainConfig {
                schema: "trellis_app".to_string(),
                target_schema: "reporting".to_string(),
                pool_max_size: 7,
                // Truncated to whole milliseconds.
                pool_wait_timeout_ms: 2_500,
            }
        );
    }

    #[test]
    fn the_dsn_and_its_password_never_cross() {
        let config =
            Config::with_schema("postgresql://alice:s3cret@db.example.com/app", "trellis").unwrap();
        let plain = format!("{:?}", PlainConfig::from(&config));
        assert!(!plain.contains("s3cret"), "{plain}");
        assert!(!plain.contains("db.example.com"), "{plain}");
    }

    #[test]
    fn a_timeout_past_u64_milliseconds_saturates() {
        let config = Config::with_schema("host=/tmp dbname=app", "trellis")
            .unwrap()
            .with_pool_wait_timeout(Duration::MAX);
        assert_eq!(PlainConfig::from(&config).pool_wait_timeout_ms, u64::MAX);
    }
}
