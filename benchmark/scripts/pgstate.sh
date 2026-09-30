#!/usr/bin/env bash
# Samples a running benchmark cluster's Postgres-side state every INTERVAL seconds until the
# cluster goes away, for diagnosing a long run (#617, #629) without touching the harness:
# pg_wal size, the definitions' status and build chunks, the
# ring's segments, and the oldest client transactions with what they wait on.
#
# Usage: benchmark/scripts/pgstate.sh <out-file> [interval-secs, default 300]
# The cluster is found under $DIR (default: $TRELLIS_BENCH_DISK_DIR, else target/bench-disk in
# this worktree, which is where `bench --disk` puts it; use DIR=/tmp for a tmpfs run).
# Peak RSS is not here: build-under-load reports it itself (benchmark/src/streaming/process_memory.rs).
# Each sample runs a few catalog queries, so keep the interval long on a measured run.
set -uo pipefail
out=${1:?usage: pgstate.sh <out-file> [interval-secs]}
iv=${2:-300}
root="$(git rev-parse --show-toplevel 2>/dev/null || pwd)"
DIR=${DIR:-${TRELLIS_BENCH_DISK_DIR:-$root/target/bench-disk}}
until sock=$(ls -d "$DIR"/trellis-testkit-*/sock 2>/dev/null | head -1) && [ -n "$sock" ]; do sleep 1; done
while [ -d "$sock" ]; do
  port=$(ls -a "$sock" | sed -n 's/^\.s\.PGSQL\.\([0-9]*\)$/\1/p' | head -1)
  [ -n "$port" ] || break
  db=$(psql -U postgres -h "$sock" -p "$port" -d postgres -XAtc \
    "select datname from pg_database where datname not in ('postgres','template0','template1') order by oid desc limit 1" 2>/dev/null) || break
  [ -n "$db" ] || break
  {
    echo "=== $(date '+%F %T')  pg_wal $(du -sh "$(dirname "$sock")/data/pg_wal" 2>/dev/null | cut -f1)"
    timeout 60 psql -U postgres -h "$sock" -p "$port" -d "$db" -X \
      -c "select d.target_table, d.status, count(c.id) filter (where c.done) chunks_done, count(c.id) chunks from trellis.transform_definitions d left join trellis.backfill_chunks c on c.definition_id = d.id group by 1, 2" \
      -c "select seg_seq, ring_slot, state from trellis.segments order by seg_seq" \
      -c "select pid, wait_event_type, wait_event, now()-xact_start xact_age, pg_blocking_pids(pid) blockers, left(regexp_replace(query,'\s+',' ','g'),70) q from pg_stat_activity where backend_type='client backend' and xact_start is not null order by xact_start limit 4" 2>&1
  } >> "$out"
  sleep "$iv"
done
echo "=== $(date '+%F %T') cluster gone" >> "$out"
