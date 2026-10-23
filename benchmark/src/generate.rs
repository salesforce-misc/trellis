//! Deterministic `posts` row generation and bulk loading (issue #63,
//! milestone 0).
//!
//! Every derived value is a pure function of `id` — no RNG, no seed — so a
//! given `(n, g)` always produces byte-identical source data across runs,
//! machines, and milestones. That's what lets M0's numbers be compared
//! apples-to-apples against M2/M3/M4 later.

use std::time::{Duration, Instant};
use trellis::Pool;

/// `author` is a pure function of `id` and the group count `g`: evenly
/// distributes `n` rows across `g` groups (`id=1..n`, so `author` ranges
/// over `0..g`).
pub fn author_for(id: i64, g: i64) -> i64 {
    id % g
}

/// `word_count` is a pure function of `id` alone, bounded to a plausible
/// small range so downstream sums stay easy to sanity-check by eye.
pub fn word_count_for(id: i64) -> i64 {
    100 + (id % 900)
}

/// `byte_size` derives from `word_count` (itself a pure function of `id`)
/// plus a small `id`-dependent wobble, so it's neither a constant multiple
/// of `word_count` nor independent of it.
pub fn byte_size_for(id: i64) -> i64 {
    word_count_for(id) * 6 + (id % 13)
}

const LOAD_BATCH: i64 = 50_000;

/// Creates the `posts` source table: `id` is the primary key (`1..=n`),
/// `author`/`word_count`/`byte_size` are the deterministic columns above.
pub async fn create_posts_table(pool: &Pool) {
    let client = pool.get().await.expect("acquire connection");
    client
        .batch_execute(
            "create table posts ( \
                 id bigint primary key, \
                 author bigint not null, \
                 word_count bigint not null, \
                 byte_size bigint not null \
             )",
        )
        .await
        .expect("create posts table");
}

/// Bulk-loads `n` deterministic rows (`id = 1..=n`) into `posts`, batched
/// via `unnest`-bound array parameters (one round trip per
/// [`LOAD_BATCH`]-sized chunk) rather than one `INSERT` per row. Returns how
/// long the load took — reported for context, but excluded from the
/// benchmark's timed backfill span, which starts only once this data is
/// already sitting in the source table (matching the poc's from-scratch
/// backfill shape).
pub async fn load_posts(pool: &Pool, n: i64, g: i64) -> Duration {
    let start = Instant::now();
    let client = pool.get().await.expect("acquire connection");

    let mut lo = 1i64;
    while lo <= n {
        let hi = (lo + LOAD_BATCH - 1).min(n);
        let batch_len = (hi - lo + 1) as usize;
        let mut ids = Vec::with_capacity(batch_len);
        let mut authors = Vec::with_capacity(batch_len);
        let mut word_counts = Vec::with_capacity(batch_len);
        let mut byte_sizes = Vec::with_capacity(batch_len);
        for id in lo..=hi {
            ids.push(id);
            authors.push(author_for(id, g));
            word_counts.push(word_count_for(id));
            byte_sizes.push(byte_size_for(id));
        }

        client
            .execute(
                "insert into posts (id, author, word_count, byte_size) \
                 select * from unnest($1::bigint[], $2::bigint[], $3::bigint[], $4::bigint[])",
                &[&ids, &authors, &word_counts, &byte_sizes],
            )
            .await
            .unwrap_or_else(|e| panic!("bulk-load posts rows {lo}..={hi} failed: {e}"));

        lo = hi + 1;
    }

    start.elapsed()
}
