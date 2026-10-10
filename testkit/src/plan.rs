//! Reads what an `explain (analyze)` plan says a statement did, so a plan
//! test can assert on the work done rather than on the planner's estimates,
//! which lag with the table's statistics (#791).

/// How many rows of `relation` the scans in `plan` read, from an `explain
/// (analyze)` of the statement: every scan node on `relation` contributes
/// its actual rows times its loops, plus the rows its filter and index
/// recheck removed (also times its loops).
///
/// A full scan of the relation counts every row it holds, whatever the
/// estimate said; a probe per key counts the rows it found. A hash join over
/// a sequential scan of a 1M-row table reads 1M, and the same join through
/// the key's index reads the batch.
pub fn rows_read(plan: &str, relation: &str) -> u64 {
    let mut total = 0.0_f64;
    // The loops of the scan node whose detail lines are being read.
    let mut scan_loops: Option<f64> = None;
    for line in plan.lines() {
        if line.contains("(cost=") {
            scan_loops = None;
            let is_scan = line.contains("Scan") && line.contains(&format!(" on {relation} "));
            if !is_scan {
                continue;
            }
            let actual = line.split("(actual").nth(1).unwrap_or("");
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
  ->  Function Scan on unnest u  (cost=0.00..1.00 rows=5000 width=8) (actual rows=5000 loops=1)
  ->  Index Scan using pair_pkey on pair t  (cost=0.42..1.00 rows=1 width=8) (actual rows=1.00 loops=5000)
        Index Cond: (id = u.c0)
        Rows Removed by Filter: 0
        Index Searches: 5000";
        assert_eq!(rows_read(plan, "pair"), 5000);
        assert_eq!(rows_read(plan, "other"), 0);
    }
}
