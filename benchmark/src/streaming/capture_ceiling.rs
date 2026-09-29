//! The capture ceiling (#565's E2, rebuilt for #622 C4; it replaces
//! `intake-ceiling`): how many rows/s reach the ring as writers are added,
//! at 1,000 rows per commit, with nothing draining.
//!
//! Each cell is a [`write_tax`] cell of shape `<rows>x<writers>`, so the
//! setup, the measurements and the JSON line are the same, labelled
//! `capture-ceiling`. The number this scenario is about is
//! `capture_rows_per_sec`:
//!
//! - `trigger`: the writers' own rate, since each ring row commits with its
//!   source row. The cell checks the ring holds exactly the rows written.
//! - `slot`: every row over the writers' window plus intake's catch-up, the
//!   rate the ring actually received (E1's "staged rows/s"). The client
//!   stopped running intake in #622 C5, so this no longer works; C7 deletes
//!   it. C4's baseline holds its numbers.
//! - `none`: `null`; its `rows_per_sec` is the writers' ceiling with no
//!   capture at all.
//!
//! Next to it: `wal_mb_per_sec` over the writers' window (the disk tier's
//! question: does the ring's WAL reach the device's limit first), the top
//! waits (E2 found `BufferContent` on the ring's right edge at 16 writers),
//! and commit latency.

use crate::streaming::write_tax::{self, CellOptions, CellResult, Shape, Variant};

pub const DEFAULT_WRITERS: &[usize] = &[1, 4, 8, 16, 32];
pub const DEFAULT_ROWS_PER_COMMIT: usize = 1000;
/// `slot` isn't among them: the client stopped running intake in #622 C5, so
/// it stages nothing, and C7 deletes it.
pub const DEFAULT_VARIANTS: &[Variant] = &[Variant::None, Variant::Trigger];

/// One shape per writer count.
pub fn shapes(rows_per_commit: usize, writers: &[usize]) -> Vec<Shape> {
    writers
        .iter()
        .map(|&w| Shape::batch(rows_per_commit, w))
        .collect()
}

pub async fn run(
    variants: &[Variant],
    rows_per_commit: usize,
    writers: &[usize],
    reps: usize,
    opts: &CellOptions,
) -> Vec<CellResult> {
    write_tax::run_matrix(
        "capture-ceiling",
        variants,
        &shapes(rows_per_commit, writers),
        reps,
        opts,
    )
    .await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn one_batch_shape_per_writer_count() {
        let shapes = shapes(1000, DEFAULT_WRITERS);
        let labels: Vec<String> = shapes.iter().map(Shape::label).collect();
        assert_eq!(labels, ["1000x1", "1000x4", "1000x8", "1000x16", "1000x32"]);
    }
}
