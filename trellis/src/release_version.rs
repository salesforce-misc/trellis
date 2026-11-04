//! Trellis release versions, compared as semver (ADR-0024, rule 1): the
//! compatibility floors and a binary's own version are compared with
//! [`ReleaseVersion`]'s ordering, which is semver precedence, pre-release
//! tags included (`0.2.0-rc.1` < `0.2.0` < `0.2.1-alpha` < `0.2.1`).
//!
//! Version handover (ADR-0023) compares exact version strings and doesn't use
//! this.

use std::cmp::Ordering;
use std::fmt;

/// A parsed `MAJOR.MINOR.PATCH[-PRE][+BUILD]` version. Build metadata is
/// dropped as it parses: semver gives it no precedence.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ReleaseVersion {
    major: u64,
    minor: u64,
    patch: u64,
    /// Empty for a release.
    pre: Vec<Identifier>,
}

/// One dot-separated pre-release identifier.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Identifier {
    Numeric(u64),
    Alphanumeric(String),
}

impl ReleaseVersion {
    /// Parses `text` as semver 2.0.0, or `None` when it isn't one: three
    /// numeric parts without leading zeros, then optional pre-release
    /// identifiers and build metadata of `[0-9A-Za-z-]`.
    pub(crate) fn parse(text: &str) -> Option<Self> {
        let (text, build) = match text.split_once('+') {
            Some((version, build)) => (version, Some(build)),
            None => (text, None),
        };
        if let Some(build) = build
            && !build.split('.').all(is_identifier)
        {
            return None;
        }
        let (core, pre) = match text.split_once('-') {
            Some((core, pre)) => (core, Some(pre)),
            None => (text, None),
        };
        let mut parts = core.split('.');
        let major = numeric(parts.next()?)?;
        let minor = numeric(parts.next()?)?;
        let patch = numeric(parts.next()?)?;
        if parts.next().is_some() {
            return None;
        }
        let pre = match pre {
            None => Vec::new(),
            Some(pre) => pre
                .split('.')
                .map(|part| {
                    if !is_identifier(part) {
                        None
                    } else if part.bytes().all(|b| b.is_ascii_digit()) {
                        numeric(part).map(Identifier::Numeric)
                    } else {
                        Some(Identifier::Alphanumeric(part.to_string()))
                    }
                })
                .collect::<Option<Vec<_>>>()?,
        };
        Some(ReleaseVersion {
            major,
            minor,
            patch,
            pre,
        })
    }
}

/// A non-empty identifier of `[0-9A-Za-z-]`.
fn is_identifier(part: &str) -> bool {
    !part.is_empty() && part.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-')
}

/// A numeric part: digits, no leading zero unless it is `0`.
fn numeric(part: &str) -> Option<u64> {
    if part.is_empty()
        || !part.bytes().all(|b| b.is_ascii_digit())
        || (part.len() > 1 && part.starts_with('0'))
    {
        return None;
    }
    part.parse().ok()
}

impl Ord for ReleaseVersion {
    fn cmp(&self, other: &Self) -> Ordering {
        (self.major, self.minor, self.patch)
            .cmp(&(other.major, other.minor, other.patch))
            .then_with(|| match (self.pre.is_empty(), other.pre.is_empty()) {
                // A pre-release precedes its release.
                (true, true) => Ordering::Equal,
                (true, false) => Ordering::Greater,
                (false, true) => Ordering::Less,
                // Identifier by identifier, then the shorter set first.
                (false, false) => self.pre.cmp(&other.pre),
            })
    }
}

impl PartialOrd for ReleaseVersion {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Identifier {
    fn cmp(&self, other: &Self) -> Ordering {
        match (self, other) {
            (Identifier::Numeric(a), Identifier::Numeric(b)) => a.cmp(b),
            // Numeric identifiers precede alphanumeric ones.
            (Identifier::Numeric(_), Identifier::Alphanumeric(_)) => Ordering::Less,
            (Identifier::Alphanumeric(_), Identifier::Numeric(_)) => Ordering::Greater,
            (Identifier::Alphanumeric(a), Identifier::Alphanumeric(b)) => a.cmp(b),
        }
    }
}

impl PartialOrd for Identifier {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl fmt::Display for ReleaseVersion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}.{}.{}", self.major, self.minor, self.patch)?;
        for (i, part) in self.pre.iter().enumerate() {
            f.write_str(if i == 0 { "-" } else { "." })?;
            match part {
                Identifier::Numeric(n) => write!(f, "{n}")?,
                Identifier::Alphanumeric(s) => f.write_str(s)?,
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn v(text: &str) -> ReleaseVersion {
        ReleaseVersion::parse(text).unwrap_or_else(|| panic!("{text:?} parses"))
    }

    /// Semver 2.0.0's own precedence example, plus the release boundaries a
    /// floor meets: a pre-release precedes its release, and numbers compare
    /// as numbers.
    #[test]
    fn orders_by_semver_precedence() {
        let ordered = [
            "0.1.0",
            "0.2.0-0",
            "0.2.0-alpha",
            "0.2.0-alpha.1",
            "0.2.0-alpha.beta",
            "0.2.0-beta",
            "0.2.0-beta.2",
            "0.2.0-beta.11",
            "0.2.0-rc.1",
            "0.2.0",
            "0.2.1-alpha",
            "0.2.1",
            "0.10.0",
            "1.0.0",
        ];
        for pair in ordered.windows(2) {
            assert!(v(pair[0]) < v(pair[1]), "{} < {}", pair[0], pair[1]);
        }
    }

    #[test]
    fn build_metadata_has_no_precedence() {
        assert_eq!(v("0.2.0+abc.1"), v("0.2.0"));
        assert_eq!(v("0.2.0-rc.1+build"), v("0.2.0-rc.1"));
    }

    #[test]
    fn refuses_text_that_isnt_semver() {
        for text in [
            "",
            "0.2",
            "0.2.0.1",
            "v0.2.0",
            "00.2.0",
            "0.02.0",
            "0.2.0-",
            "0.2.0-rc..1",
            "0.2.0-01",
            "0.2.0+",
            "0.2.0-rc_1",
            " 0.2.0",
            "0.2.x",
        ] {
            assert_eq!(ReleaseVersion::parse(text), None, "{text:?}");
        }
    }

    #[test]
    fn displays_without_build_metadata() {
        assert_eq!(v("0.2.0-rc.1+abc").to_string(), "0.2.0-rc.1");
        assert_eq!(v("10.0.3").to_string(), "10.0.3");
    }
}
