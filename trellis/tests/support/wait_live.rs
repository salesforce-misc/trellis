//! Polling a definition to `live` through the public facade, for tests that
//! run a real background pipeline and take a watermark token.
//!
//! `Trellis::await_converged` waits for the ring only, never a definition's
//! status, so a token is only a read-your-writes guarantee for a `live`
//! target (ADR-0002, "What `live` promises"). A target still building can
//! hold changes in its group deltas that the token doesn't wait for (#728).
//! This poll is that contract's first half, the way an embedder writes it
//! (`docs/embedding.md`, "Poll to `live`, don't wait").

use std::time::{Duration, Instant};

use trellis::{TransformStatus, Trellis};

/// Polls `Trellis::status` until `target` reports `live`, failing after 60s.
pub async fn wait_for_live(trellis: &Trellis, target: &str) {
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let status = trellis
            .status(target)
            .await
            .expect("read status")
            .expect("the definition is registered")
            .status;
        if status == TransformStatus::Live {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "{target} never reported live (last: {status:?})"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}
