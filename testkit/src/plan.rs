//! Reads what an `explain (analyze)` plan says a statement did, so a plan
//! test can assert on the work done rather than on the planner's estimates
//! (#791).

/// How many rows of `relation` the scans in `plan` read, from an `explain
/// (analyze)` of the statement in text format, with costs on or off: every
/// scan node on `relation` contributes its actual rows times its loops, plus
/// the rows its filter and index recheck removed (also times its loops).
/// Postgres prints a node's rows and removed rows per loop, so a parallel
/// scan's workers are counted this way too.
///
/// A full scan of the relation counts every row it holds, whatever the
/// estimate said; a probe per key counts the rows it found. A hash join over
/// a sequential scan of a 1M-row table reads 1M, and the same join through
/// the key's index reads the batch. A merge join stops reading the index at
/// its last key, so a test's keys should be spread over the table.
///
/// `relation` is the name the plan prints (unqualified without `verbose`).
/// Panics if `plan` has no analyzed scan of it, so a misnamed relation, a
/// partitioned one (whose scans name its partitions), or a plain `explain`
/// fails the test instead of reading as zero rows.
pub fn rows_read(plan: &str, relation: &str) -> u64 {
    let on = format!(" on {relation} ");
    let mut scanned = false;
    let mut total = 0.0_f64;
    // The loops of the scan node whose detail lines are being read.
    let mut scan_loops: Option<f64> = None;
    for line in plan.lines() {
        // A node's own line carries its actual figures; detail lines don't.
        let node = line.contains("(actual ") || line.contains("(never executed)");
        if node {
            scan_loops = None;
            // `on <relation> <alias>`, or `on <relation> (` without an alias.
            let is_scan = line.contains("Scan") && line.contains(&on);
            if !is_scan {
                continue;
            }
            scanned = true;
            let actual = line.split("(actual ").nth(1).unwrap_or("");
            let rows = number_after(actual, "rows=").unwrap_or(0.0);
            let loops = number_after(actual, "loops=").unwrap_or(1.0);
            total += rows * loops;
            scan_loops = Some(loops);
        } else if let Some(loops) = scan_loops
            && let Some(removed) = [
                "Rows Removed by Filter: ",
                "Rows Removed by Index Recheck: ",
            ]
            .iter()
            .find_map(|label| number_after(line, label))
        {
            total += removed * loops;
        }
    }
    assert!(
        scanned,
        "no analyzed scan of {relation} in the plan (is it `explain (analyze)`?):\n{plan}"
    );
    total.round() as u64
}

fn number_after(text: &str, label: &str) -> Option<f64> {
    let rest = text.split(label).nth(1)?;
    let end = rest
        .find(|c: char| !(c.is_ascii_digit() || c == '.'))
        .unwrap_or(rest.len());
    rest[..end].parse().ok()
}

#[cfg(test)]
mod tests {
    use super::rows_read;

    #[test]
    fn a_full_scan_counts_every_row_it_read() {
        let plan = "\
Hash Join  (cost=1.00..2.00 rows=100 width=8) (actual rows=5000 loops=1)
  Hash Cond: (t.id = u.c0)
  ->  Seq Scan on pair t  (cost=0.00..1.00 rows=100 width=8) (actual rows=1000000 loops=1)
        Filter: (id > 0)
        Rows Removed by Filter: 5
  ->  Hash  (cost=1.00..1.00 rows=10 width=8) (actual rows=5000 loops=1)";
        assert_eq!(rows_read(plan, "pair"), 1_000_005);
    }

    #[test]
    fn a_probe_per_key_counts_the_rows_it_found_across_its_loops() {
        let plan = "\
Nested Loop  (cost=1.00..2.00 rows=100 width=8) (actual rows=5000 loops=1)
  ->  Function Scan on u  (cost=0.00..1.00 rows=5000 width=8) (actual rows=5000 loops=1)
  ->  Index Scan using pair_pkey on pair t  (cost=0.42..1.00 rows=1 width=8) (actual rows=1.00 loops=5000)
        Index Cond: (id = u.c0)
        Rows Removed by Filter: 0
        Index Searches: 5000";
        assert_eq!(rows_read(plan, "pair"), 5000);
    }

    /// PostgreSQL 17's plan of a merge join over a scan of a stale table's
    /// whole key index (the shape `enable_nestloop = off` gives a keyset join):
    /// the estimate is close to the table, and the scan stops at the last key.
    #[test]
    fn a_whole_index_scan_counts_what_it_read_before_the_last_key() {
        let plan = "\
LockRows  (cost=357.62..39102.22 rows=1 width=105) (actual rows=5000 loops=1)
  ->  Merge Join  (cost=357.62..39102.21 rows=1 width=105) (actual rows=5000 loops=1)
        Merge Cond: ((t.a = k.c0) AND (t.b = k.c1))
        ->  Index Scan using stale_pkey on stale t  (cost=0.42..36004.49 rows=540600 width=13) (actual rows=426996 loops=1)
        ->  Sort  (cost=357.20..369.70 rows=5000 width=96) (actual rows=5000 loops=1)
              Sort Key: k.c0, k.c1
              Sort Method: quicksort  Memory: 544kB
              ->  Function Scan on k  (cost=0.01..50.01 rows=5000 width=96) (actual rows=5000 loops=1)";
        assert_eq!(rows_read(plan, "stale"), 426_996);
    }

    /// PostgreSQL 18's parallel scan, which prints its rows and removed rows
    /// per loop (the leader and each worker), and no alias.
    #[test]
    fn a_parallel_scan_counts_every_worker() {
        let plan = "\
Gather  (cost=1000.00..13797.67 rows=1000 width=15) (actual rows=66666.00 loops=1)
  Workers Planned: 2
  Workers Launched: 2
  ->  Parallel Seq Scan on pair  (cost=0.00..12697.67 rows=417 width=15) (actual rows=22222.00 loops=3)
        Filter: ((g < 200000) AND ((total % 3) = 0))
        Rows Removed by Filter: 311111
        Buffers: shared hit=5406";
        assert_eq!(rows_read(plan, "pair"), 999_999);
    }

    /// A bitmap scan's rows come from its heap scan (the index scan names
    /// the index), plus what its filter and recheck removed; costs off, and
    /// a scan that never ran reads nothing.
    #[test]
    fn a_bitmap_scan_counts_its_heap_scan_with_costs_off() {
        let plan = "\
Nested Loop (actual rows=0 loops=1)
  ->  Bitmap Heap Scan on pair a (actual rows=10 loops=1)
        Recheck Cond: (g < 200000)
        Rows Removed by Index Recheck: 7
        Filter: ((total % 3) = 0)
        Rows Removed by Filter: 20
        Heap Blocks: exact=1 lossy=1
        ->  Bitmap Index Scan on pair_pkey (actual rows=37 loops=1)
              Index Cond: (g < 200000)
  ->  Index Scan using pair_pkey on pair b (never executed)
        Index Cond: (g = a.g)";
        assert_eq!(rows_read(plan, "pair"), 37);
    }

    #[test]
    #[should_panic(expected = "no analyzed scan of other")]
    fn a_plan_without_a_scan_of_the_relation_fails() {
        rows_read(
            "Seq Scan on pair t  (cost=0.00..1.00 rows=100 width=8) (actual rows=10 loops=1)",
            "other",
        );
    }

    #[test]
    #[should_panic(expected = "no analyzed scan of pair")]
    fn a_plan_that_was_not_analyzed_fails() {
        rows_read(
            "Seq Scan on pair t  (cost=0.00..1.00 rows=100 width=8)",
            "pair",
        );
    }
}
