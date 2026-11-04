//! The staging ring's gauges: its rows and bytes, how long its oldest
//! undrained row has waited, and the undrained bytes of each source table
//! (`docs/observability.md`). One statement reads them, together with the
//! segment counts of `trellis_staging_segments`, on the staging worker's
//! housekeeping pass.
//!
//! The statement is cheap on a ring of any size, and adds no transaction to a
//! pass that already counted the segments:
//!
//! - **Bytes** are `pg_total_relation_size` of each slot, a catalog-level
//!   read.
//! - **Rows and the split of bytes by source table** come from
//!   `TABLESAMPLE SYSTEM` over each slot, sized to read at most
//!   [`SAMPLE_PAGES`] pages of it. A slot that small is read whole, and its
//!   counts are exact; a larger one is scaled up from the sample, so its
//!   rows are an estimate and its split follows the sizes of the rows the
//!   sampled pages hold. A row's size is [`ROW_SIZE`], its columns' stored
//!   sizes: a TOASTed image counts what it holds in the TOAST table without
//!   the read fetching it, so the read stays within the sampled pages.
//! - **Undrained** is decided per slot, by the drain's own condition
//!   ([`ring_slot_applied_sql`]): the slot's segment has not drained, or a
//!   late row of it has not. A slot is counted whole while any of its rows
//!   is still to drain, so a segment draining its last buckets still counts
//!   all its bytes.
//! - **The oldest undrained row's age** is read from the first
//!   [`AGE_PAGES`] pages of each undrained slot, not from every row: the
//!   ring is append-only and truncated whole, so the heap runs in append
//!   order, and `appended_at` is a writer's transaction start, which
//!   concurrent transactions only reorder slightly. The pages are named by
//!   a TID range, not a `LIMIT` on a plain scan: a large slot's sequential
//!   scan starts wherever another scan of it reported reaching
//!   (`synchronize_seqscans`), so its first rows would be a later stretch of
//!   the slot. Only when those pages hold no visible row (a long writer's
//!   uncommitted rows, or rows deleted from the slot) is the age read from
//!   the first [`AGE_ROWS`] rows a plain scan returns. The age is that of the
//!   oldest row of the oldest segment not fully drained, which can include a
//!   bucket of it that has drained already.

use std::collections::BTreeMap;
use std::time::Duration;

use tokio_postgres::Client;

use super::converge::per_ring_table;
use super::error::StagingError;
use super::page::ring_slot_applied_sql;
use super::state::SegmentState;
use crate::metrics::RingGauges;

/// The most pages of one ring slot a read samples, 4 MiB at the default page
/// size: about 22,000 captured rows.
pub(crate) const SAMPLE_PAGES: u32 = 512;

/// A ring row's size on disk, over ring row `t`: its variable-width columns
/// as stored (a TOASTed one's stored size, without fetching it), plus the
/// tuple header with its null bitmap (32 bytes) and the fixed-width columns
/// (64). Not `pg_column_size(t)`: a
/// whole-row value inlines every TOASTed column, so the sample would read
/// each sampled row's images out of the TOAST table.
const ROW_SIZE: &str = "(96 + pg_column_size(t.src_table) + pg_column_size(t.key) \
     + pg_column_size(t.op) + coalesce(pg_column_size(t.old_image), 0) \
     + coalesce(pg_column_size(t.new_image), 0) \
     + coalesce(pg_column_size(t.transition_images), 0))";

/// The pages at the start of an undrained slot that its age is read from,
/// 256 KiB at the default page size.
const AGE_PAGES: u32 = 32;

/// The rows the age is read from when the first [`AGE_PAGES`] pages hold no
/// visible row.
const AGE_ROWS: u32 = 1000;

/// What one read of the ring found: the segment counts and the ring's gauges.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct RingReading {
    /// Counts of `segments` rows by state, `Active` first through `Drained`
    /// last. All four states are always present, a state with no segment at
    /// `0`, so a state that just emptied reports `0` rather than leaving a
    /// stale value in whatever renders the gauge.
    pub(crate) segments: [(SegmentState, i64); 4],
    pub(crate) ring: RingGauges,
}

/// The statement for a read that samples at most `sample_pages` pages of
/// each slot. One row per slot and source table in the slot's sample, or one
/// with a null `src_table` for a slot with no row in it.
fn sql(sample_pages: u32) -> String {
    let slots = per_ring_table("\n union all\n", |slot, table| {
        // The share of the slot's pages to sample, 100 when it fits.
        let pct = format!(
            "least(100.0, 100.0 * {sample_pages} \
                          / greatest(1, pg_relation_size('{table}') \
                                        / current_setting('block_size')::bigint))"
        );
        format!(
            "select {slot}::int as slot, \
                    pg_total_relation_size('{table}'::regclass)::bigint as bytes, \
                    u.undrained, \
                    (100.0 / {pct})::float8 as scale, \
                    case when u.undrained then \
                        extract(epoch from now() - coalesce( \
                            (select min(appended_at) from {table} \
                             where ctid < '({AGE_PAGES},0)'::tid), \
                            (select min(a.appended_at) \
                             from (select appended_at from {table} limit {AGE_ROWS}) a)))::float8 \
                    end as age, \
                    g.src_table, g.n, g.b \
             from (select not {applied} as undrained) u \
             left join (select src_table, count(*) as n, \
                               sum({ROW_SIZE})::float8 as b \
                        from {table} t tablesample system ({pct}) \
                        group by src_table) g on true",
            applied = ring_slot_applied_sql(slot),
        )
    });
    format!(
        "select r.*, \
                c.states, c.counts \
         from ({slots}) r \
         cross join (select coalesce(array_agg(state), '{{}}') as states, \
                            coalesce(array_agg(n), '{{}}') as counts \
                     from (select state, count(*) as n from segments group by state) q) c"
    )
}

/// Reads the ring's gauges and segment counts in one statement.
pub(crate) async fn read(client: &Client) -> Result<RingReading, StagingError> {
    read_sampling(client, SAMPLE_PAGES).await
}

/// [`read`], sampling at most `sample_pages` pages of each slot.
pub(crate) async fn read_sampling(
    client: &Client,
    sample_pages: u32,
) -> Result<RingReading, StagingError> {
    let rows = client.query_typed(&sql(sample_pages), &[]).await?;

    let mut segments = [
        (SegmentState::Active, 0i64),
        (SegmentState::Sealed, 0i64),
        (SegmentState::Draining, 0i64),
        (SegmentState::Drained, 0i64),
    ];
    // Per slot: its bytes, whether it is undrained, and its sample.
    #[derive(Default)]
    struct Slot {
        bytes: i64,
        undrained: bool,
        age: Option<f64>,
        /// (source table, scaled rows, sampled bytes)
        tables: Vec<(String, f64, f64)>,
    }
    let mut slots: BTreeMap<i32, Slot> = BTreeMap::new();
    for row in &rows {
        let (states, counts): (Vec<String>, Vec<i64>) = (row.get(8), row.get(9));
        for (state, count) in states.iter().zip(counts) {
            if let Some(state) = SegmentState::from_sql(state)
                && let Some(slot) = segments.iter_mut().find(|(s, _)| *s == state)
            {
                slot.1 = count;
            }
        }
        let slot = slots.entry(row.get(0)).or_default();
        slot.bytes = row.get(1);
        slot.undrained = row.get(2);
        slot.age = row.get(4);
        if let Some(table) = row.get::<_, Option<String>>(5) {
            let scale: f64 = row.get(3);
            let rows: i64 = row.get(6);
            let bytes: f64 = row.get(7);
            slot.tables.push((table, rows as f64 * scale, bytes));
        }
    }

    let mut ring = RingGauges::default();
    let mut oldest = 0f64;
    for slot in slots.values() {
        ring.bytes += slot.bytes.max(0) as u64;
        ring.rows += slot
            .tables
            .iter()
            .map(|(_, rows, _)| rows)
            .sum::<f64>()
            .round() as u64;
        if !slot.undrained {
            continue;
        }
        oldest = oldest.max(slot.age.unwrap_or(0.0));
        let sampled: f64 = slot.tables.iter().map(|(_, _, bytes)| bytes).sum();
        if sampled <= 0.0 {
            continue;
        }
        for (table, _, bytes) in &slot.tables {
            *ring.undrained_bytes.entry(table.clone()).or_default() +=
                (slot.bytes.max(0) as f64 * bytes / sampled).round() as u64;
        }
    }
    ring.oldest_undrained_age = Duration::from_secs_f64(oldest.max(0.0));
    Ok(RingReading { segments, ring })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A ring row of `src_table`, its key padded so every row of a test
    /// weighs the same.
    const SEED_ROW: &str = "insert into seg_{slot} (src_table, key, op, new_image, appended_at) \
         select '{table}', lpad(i::text, 12, '0'), 'insert', \
                jsonb_build_object('id', i, 'pad', repeat('x', {pad})), \
                now() - interval '{age}'
         from generate_series(1, {rows}) i";

    async fn seed(
        db: &testkit::TestDatabase,
        slot: u8,
        table: &str,
        rows: u32,
        pad: u32,
        age: &str,
    ) {
        let sql = SEED_ROW
            .replace("{slot}", &slot.to_string())
            .replace("{table}", table)
            .replace("{rows}", &rows.to_string())
            .replace("{pad}", &pad.to_string())
            .replace("{age}", age);
        let conn = db.pool.get().await.expect("connection");
        conn.batch_execute(&sql).await.expect("seed the slot");
    }

    async fn total_size(db: &testkit::TestDatabase, slot: u8) -> u64 {
        let conn = db.pool.get().await.expect("connection");
        let size: i64 = conn
            .query_one(&format!("select pg_total_relation_size('seg_{slot}')"), &[])
            .await
            .expect("size")
            .get(0);
        size as u64
    }

    /// An empty ring reads zero everywhere but the empty slots' index pages,
    /// and its one active segment.
    #[tokio::test]
    async fn an_idle_ring_reads_no_rows_no_age_and_no_tables() {
        let cluster = testkit::TestCluster::start();
        let db = cluster.create_isolated_database().await;
        let conn = db.pool.get().await.expect("connection");
        let reading = read(&conn).await.expect("read");

        assert_eq!(reading.ring.rows, 0);
        assert_eq!(reading.ring.oldest_undrained_age, Duration::ZERO);
        assert!(reading.ring.undrained_bytes.is_empty());
        let mut size = 0;
        for slot in 0..4 {
            size += total_size(&db, slot).await;
        }
        assert_eq!(reading.ring.bytes, size);
        assert_eq!(
            reading.segments.map(|(_, n)| n),
            [1, 0, 0, 0],
            "the one active segment a migrated ring starts with"
        );
    }

    /// Rows and bytes cover every slot; the undrained bytes cover only the
    /// slots whose segment has not drained, split by table in proportion to
    /// what each table's rows weigh; and the age is that of the oldest row
    /// of those slots, a drained slot's older rows not counting.
    #[tokio::test]
    async fn a_ring_reads_its_rows_bytes_age_and_undrained_bytes_per_table() {
        let cluster = testkit::TestCluster::start();
        let db = cluster.create_isolated_database().await;
        let conn = db.pool.get().await.expect("connection");
        // Slot 0 is active (the migration's segment), slot 1 sealed, slot 2
        // drained, slot 3 holds no segment.
        conn.batch_execute(
            "insert into segments (seg_seq, ring_slot, state) values (2, 1, 'sealed'), \
                                                                      (3, 2, 'drained')",
        )
        .await
        .expect("segments");
        seed(&db, 0, "public.small", 100, 10, "5 minutes").await;
        seed(&db, 1, "public.small", 200, 10, "2 hours").await;
        seed(&db, 1, "public.big", 200, 300, "1 hour").await;
        seed(&db, 2, "public.small", 50, 10, "10 hours").await;

        let reading = read(&conn).await.expect("read");

        assert_eq!(reading.ring.rows, 100 + 200 + 200 + 50);
        let mut sizes = Vec::new();
        for slot in 0..4 {
            sizes.push(total_size(&db, slot).await);
        }
        assert_eq!(reading.ring.bytes, sizes.iter().sum::<u64>());

        // Slots 0 and 1 are undrained, whole; slot 2 (drained) is not.
        let undrained = &reading.ring.undrained_bytes;
        assert_eq!(undrained.len(), 2, "{undrained:?}");
        let (small, big) = (undrained["public.small"], undrained["public.big"]);
        let slots_01 = sizes[0] + sizes[1];
        assert!(
            small + big <= slots_01 + 2 && small + big + 2 >= slots_01,
            "{small} + {big} is the undrained slots' {slots_01}"
        );
        // Slot 1 holds 200 small and 200 big rows, the big ones ~15x heavier.
        assert!(big > small, "{big} should outweigh {small}");
        assert!(
            big > sizes[1] / 2,
            "the big table is most of slot 1: {big} of {}",
            sizes[1]
        );

        // The oldest undrained row is slot 1's 2 hours; the drained slot's
        // 10 hours do not count.
        let age = reading.ring.oldest_undrained_age;
        assert!(
            age >= Duration::from_secs(2 * 3600) && age < Duration::from_secs(2 * 3600 + 120),
            "{age:?}"
        );
        assert_eq!(reading.segments.map(|(_, n)| n), [1, 1, 0, 1]);
    }

    /// The age is the slot's first rows', even while another scan of a slot
    /// large enough for synchronized scans has stopped midway through it: a
    /// plain scan of the slot would start where that one stopped, and read
    /// rows half the slot younger.
    #[tokio::test]
    async fn the_age_reads_the_start_of_a_slot_another_scan_stopped_midway() {
        let cluster = testkit::TestCluster::start();
        let db = cluster.create_isolated_database().await;
        let conn = db.pool.get().await.expect("connection");
        // Synchronized scans start on a table of more than a quarter of
        // `shared_buffers`; outgrow that by a fifth.
        let buffers: i64 = conn
            .query_one(
                "select setting::bigint from pg_settings where name = 'shared_buffers'",
                &[],
            )
            .await
            .expect("shared_buffers")
            .get(0);
        // About 25 rows of this size to a page; a row a second, the first the
        // oldest.
        let rows = buffers / 4 * 25 * 6 / 5;
        conn.batch_execute(&format!(
            "insert into seg_0 (src_table, key, op, new_image, appended_at) \
             select 'public.t', lpad(i::text, 12, '0'), 'insert', \
                    jsonb_build_object('id', i, 'pad', repeat('x', 200)), \
                    now() - ({rows} - i) * interval '1 second' \
             from generate_series(1, {rows}) i"
        ))
        .await
        .expect("seed");
        let pages = total_size(&db, 0).await as i64 / 8192;
        assert!(
            pages > buffers / 4,
            "{pages} pages should exceed {buffers} / 4"
        );

        // Another scan stops halfway, its cursor open.
        let scanner = db.pool.get().await.expect("connection");
        scanner
            .batch_execute(&format!(
                "begin; declare c cursor for select * from seg_0; \
                 move forward {} in c",
                rows / 2
            ))
            .await
            .expect("scan halfway");

        let reading = read(&conn).await.expect("read");
        let age = reading.ring.oldest_undrained_age.as_secs() as i64;
        assert!(
            age >= rows - 60,
            "the oldest row is {rows} s old, read as {age} s"
        );
        scanner
            .batch_execute("rollback")
            .await
            .expect("end the scan");
    }

    /// A slot of rows whose images are TOASTed is read without fetching the
    /// images: the read's buffers stay within the sampled and age pages, and
    /// the split weighs a row by what it stores, TOAST included.
    #[tokio::test]
    async fn a_read_does_not_fetch_toasted_images() {
        let cluster = testkit::TestCluster::start();
        let db = cluster.create_isolated_database().await;
        let conn = db.pool.get().await.expect("connection");
        // 300 rows of ~19 KB of md5 text each (too random to compress under
        // the TOAST threshold), next to 300 small rows.
        conn.batch_execute(
            "insert into seg_0 (src_table, key, op, new_image) \
             select 'public.wide', lpad(i::text, 12, '0'), 'insert', \
                    jsonb_build_object('pad', (select string_agg(md5(i::text || '.' || k), '') \
                                               from generate_series(1, 600) k)) \
             from generate_series(1, 300) i; \
             insert into seg_0 (src_table, key, op, new_image) \
             select 'public.narrow', lpad(i::text, 12, '0'), 'insert', jsonb_build_object('id', i) \
             from generate_series(1, 300) i",
        )
        .await
        .expect("seed");
        let toast_pages: i64 = conn
            .query_one(
                "select pg_relation_size(reltoastrelid) / 8192 from pg_class \
                 where oid = 'seg_0'::regclass",
                &[],
            )
            .await
            .expect("toast size")
            .get(0);
        assert!(toast_pages > 500, "{toast_pages} TOAST pages");

        let plan: Vec<String> = conn
            .query(
                &format!(
                    "explain (analyze, buffers, format text) {}",
                    sql(SAMPLE_PAGES)
                ),
                &[],
            )
            .await
            .expect("explain")
            .iter()
            .map(|row| row.get(0))
            .collect();
        // The plan's first `Buffers:` line is its root's, the whole read's.
        let buffers: i64 = plan
            .iter()
            .find_map(|line| line.trim().strip_prefix("Buffers: shared "))
            .expect("the root's buffers")
            .split_whitespace()
            .filter_map(|count| count.split_once('=')?.1.parse::<i64>().ok())
            .sum();
        assert!(
            buffers < 100,
            "the read touched {buffers} buffers of a slot of {toast_pages} TOAST pages:\n{}",
            plan.join("\n")
        );

        let reading = read(&conn).await.expect("read");
        let undrained = &reading.ring.undrained_bytes;
        let (wide, narrow) = (undrained["public.wide"], undrained["public.narrow"]);
        assert!(
            wide > 50 * narrow,
            "{wide} is mostly TOAST, {narrow} is not"
        );
    }

    /// A slot bigger than the sample is scaled up from it: its rows are an
    /// estimate, near the true count, and the split by table follows the
    /// sampled rows.
    #[tokio::test]
    async fn a_slot_larger_than_the_sample_is_estimated_from_it() {
        let cluster = testkit::TestCluster::start();
        let db = cluster.create_isolated_database().await;
        let conn = db.pool.get().await.expect("connection");
        // Three interleaved tables, so every page holds all of them.
        conn.batch_execute(
            "insert into seg_0 (src_table, key, op, new_image) \
             select 't' || (i % 3), lpad(i::text, 12, '0'), 'insert', \
                    jsonb_build_object('id', i, 'pad', repeat('x', 100)) \
             from generate_series(1, 60000) i",
        )
        .await
        .expect("seed");
        let pages = total_size(&db, 0).await / 8192;
        assert!(pages > 4 * 100, "{pages} pages should dwarf the sample");

        let reading = read_sampling(&conn, 100).await.expect("read");

        let rows = reading.ring.rows as f64;
        assert!(
            (rows - 60000.0).abs() < 60000.0 * 0.35,
            "{rows} should be near 60000"
        );
        let undrained = &reading.ring.undrained_bytes;
        assert_eq!(undrained.len(), 3, "{undrained:?}");
        let total: u64 = undrained.values().sum();
        let slot0 = total_size(&db, 0).await;
        assert!(total.abs_diff(slot0) <= 3, "{total} of {slot0}");
        for (table, bytes) in undrained {
            let share = *bytes as f64 / total as f64;
            assert!((share - 1.0 / 3.0).abs() < 0.02, "{table}: {share}");
        }
    }
}
