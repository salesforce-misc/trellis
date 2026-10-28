//! [`Config`] crosses as its fields.
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
}

impl From<&Config> for PlainConfig {
    fn from(config: &Config) -> Self {
        PlainConfig {
            schema: config.schema().to_string(),
            target_schema: config.target_schema().to_string(),
        }
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
            PlainConfig::from(&config),
            PlainConfig {
                schema: "trellis_app".to_string(),
                target_schema: "reporting".to_string(),
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
}
