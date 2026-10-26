//! The segment lifecycle's legal-transition graph (issue #9, stage 03) —
//! see the `stateDiagram-v2` in
//! docs/staging-and-claiming/03-sealing-and-the-fence.md.
//!
//! [`SegmentState::can_transition_to`] is the *one* function that owns this
//! graph. Stage 03 only ever drives `Active -> Sealed`; `Draining`/`Drained`
//! are here so the graph is complete from the start and a later stage
//! (claiming, apply) extends this function instead of re-deriving the edges
//! at a new call site.

use tokio_postgres::Client;

use super::error::StagingError;

/// One `segments.state` value. All four are valid `segments.state` CHECK values.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SegmentState {
    Active,
    Sealed,
    Draining,
    Drained,
}

impl SegmentState {
    pub fn as_sql(self) -> &'static str {
        match self {
            SegmentState::Active => "active",
            SegmentState::Sealed => "sealed",
            SegmentState::Draining => "draining",
            SegmentState::Drained => "drained",
        }
    }

    pub fn from_sql(value: &str) -> Option<Self> {
        match value {
            "active" => Some(SegmentState::Active),
            "sealed" => Some(SegmentState::Sealed),
            "draining" => Some(SegmentState::Draining),
            "drained" => Some(SegmentState::Drained),
            _ => None,
        }
    }

    /// Whether `self -> next` is a legal edge. Self-loops (`active ->
    /// active` on every append, `draining -> draining` on every heartbeat)
    /// aren't registry-state transitions at all — they don't touch
    /// `segments.state` — so they're deliberately not edges here.
    pub fn can_transition_to(self, next: SegmentState) -> bool {
        use SegmentState::*;
        matches!(
            (self, next),
            (Active, Sealed)     // seal: the two-phase flip
                | (Sealed, Draining) // claim (stage 04)
                | (Draining, Sealed) // reclaim: heartbeat went stale
                | (Draining, Drained) // apply ∪ mark, one txn (stage 05)
        )
    }
}

/// Counts of `segments` rows by state, `Active` first through `Drained`
/// last — the reading side of ADR-0009 decision 5's `staging_segments{state}`
/// gauge (`docs/decisions/0009-observability-decisions.md`). Always returns
/// all four states, defaulting an unrepresented one to `0`, so a state that
/// just drained to empty (e.g. no more `active` segments) reports `0` rather
/// than leaving a stale prior value in whatever's rendering the gauge.
pub async fn segment_state_counts(
    client: &Client,
) -> Result<[(SegmentState, i64); 4], StagingError> {
    let rows = client
        .query_typed("select state, count(*) from segments group by state", &[])
        .await?;
    let mut counts = [
        (SegmentState::Active, 0i64),
        (SegmentState::Sealed, 0i64),
        (SegmentState::Draining, 0i64),
        (SegmentState::Drained, 0i64),
    ];
    for row in rows {
        let state_sql: String = row.get(0);
        let count: i64 = row.get(1);
        if let Some(state) = SegmentState::from_sql(&state_sql)
            && let Some(slot) = counts.iter_mut().find(|(s, _)| *s == state)
        {
            slot.1 = count;
        }
    }
    Ok(counts)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn seal_is_the_only_edge_out_of_active() {
        assert!(SegmentState::Active.can_transition_to(SegmentState::Sealed));
        assert!(!SegmentState::Active.can_transition_to(SegmentState::Draining));
        assert!(!SegmentState::Active.can_transition_to(SegmentState::Drained));
        assert!(!SegmentState::Active.can_transition_to(SegmentState::Active));
    }

    #[test]
    fn draining_can_reclaim_back_to_sealed_or_advance_to_drained() {
        assert!(SegmentState::Draining.can_transition_to(SegmentState::Sealed));
        assert!(SegmentState::Draining.can_transition_to(SegmentState::Drained));
        assert!(!SegmentState::Draining.can_transition_to(SegmentState::Active));
    }

    #[test]
    fn sql_round_trips() {
        for state in [
            SegmentState::Active,
            SegmentState::Sealed,
            SegmentState::Draining,
            SegmentState::Drained,
        ] {
            assert_eq!(SegmentState::from_sql(state.as_sql()), Some(state));
        }
        assert_eq!(SegmentState::from_sql("bogus"), None);
    }
}
