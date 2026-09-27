//! [`Config`] crosses as its fields, with the pool's wait timeout as whole
//! milliseconds, the unit every duration a binding takes or returns uses.
//!
//! The DSN is named `url` here, after the connect option both bindings take
//! it as, so a host's config value reads back the options it connected with.

use trellis::Config;

/// A [`Config`] flattened to plain data.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlainConfig {
    /// The Postgres connection string, exactly as the host passed it.
    pub url: String,
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
            url: config.dsn().to_string(),
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
        let config = Config::with_schema("host=/tmp dbname=app", "trellis_app")
            .unwrap()
            .with_target_schema("reporting")
            .unwrap()
            .with_pool_max_size(7)
            .unwrap()
            .with_pool_wait_timeout(Duration::from_micros(2_500_999));
        assert_eq!(
            PlainConfig::from(&config),
            PlainConfig {
                url: "host=/tmp dbname=app".to_string(),
                schema: "trellis_app".to_string(),
                target_schema: "reporting".to_string(),
                pool_max_size: 7,
                // Truncated to whole milliseconds.
                pool_wait_timeout_ms: 2_500,
            }
        );
    }

    #[test]
    fn a_timeout_past_u64_milliseconds_saturates() {
        let config = Config::with_schema("host=/tmp dbname=app", "trellis")
            .unwrap()
            .with_pool_wait_timeout(Duration::MAX);
        assert_eq!(PlainConfig::from(&config).pool_wait_timeout_ms, u64::MAX);
    }
}
