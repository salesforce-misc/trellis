//! [`TransformStatus`] and [`QuarantineState`] cross as their `as_str()`
//! words, which a host turns into atoms (Elixir) or symbols (Ruby) from a
//! closed set allocated at load time, never from a database value
//! (ADR-0010 decision 4).
//!
//! The sets come from `trellis`'s own `ALL` lists rather than being written
//! out here, so a binding never hard-codes which statuses exist: a status
//! `trellis` adds or renames reaches the host with no binding change.

use trellis::{QuarantineState, TransformStatus};

/// `status`'s stable word, as [`TransformStatus::as_str`] spells it.
pub fn transform_status(status: TransformStatus) -> &'static str {
    status.as_str()
}

/// `state`'s stable word, as [`QuarantineState::as_str`] spells it.
pub fn quarantine_state(state: QuarantineState) -> &'static str {
    state.as_str()
}

/// Every word [`transform_status`] can return: the set a host allocates its
/// status names from at load time.
pub fn transform_status_names() -> Vec<&'static str> {
    TransformStatus::ALL.iter().map(|s| s.as_str()).collect()
}

/// Every word [`quarantine_state`] can return: the set a host allocates its
/// state names from at load time.
pub fn quarantine_state_names() -> Vec<&'static str> {
    QuarantineState::ALL.iter().map(|s| s.as_str()).collect()
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::*;

    #[test]
    fn a_status_crosses_as_its_persisted_word_and_parses_back() {
        for status in TransformStatus::ALL {
            let word = transform_status(status);
            assert_eq!(TransformStatus::from_persisted(word), Some(status));
        }
        assert_eq!(
            transform_status(TransformStatus::WaitingToBackfill),
            "waiting_to_backfill"
        );
    }

    #[test]
    fn every_status_word_is_in_the_load_time_set_once() {
        let names = transform_status_names();
        let unique: HashSet<_> = names.iter().copied().collect();
        assert_eq!(unique.len(), names.len());
        for status in TransformStatus::ALL {
            assert!(unique.contains(transform_status(status)));
        }
    }

    #[test]
    fn every_state_word_is_in_the_load_time_set_once() {
        let names = quarantine_state_names();
        let unique: HashSet<_> = names.iter().copied().collect();
        assert_eq!(unique.len(), names.len());
        for state in QuarantineState::ALL {
            assert!(unique.contains(quarantine_state(state)));
        }
    }

    /// A state that mirrors a status crosses as the same word, so a host can
    /// share one atom/symbol table between the two reads.
    #[test]
    fn a_mirrored_state_crosses_as_its_status_word() {
        for status in TransformStatus::ALL {
            assert_eq!(quarantine_state(status.into()), transform_status(status));
        }
        assert_eq!(quarantine_state(QuarantineState::Paused), "paused");
    }
}
