#!/usr/bin/env bash
# The release upgrade battery (docs/releasing.md, "The upgrade battery"): the
# released v0.1.0 CLI defines and builds databases of every definition shape,
# the release candidate takes them over, and each check prints PASS, FAIL, INFO,
# MANUAL or SKIP. Exit 0 when nothing FAILed, 1 otherwise, 2 for a usage or
# setup error. A step it prints MANUAL for is not run; the runbook has its
# commands.
#
#   upgrade-battery.sh [options] [step ...]
#
# Steps (default: all, in this order):
#   migrations    1. the tag's files and history rows unchanged; refuses a tampered checksum (control)
#   definitions   2. every shape resumes, converges, self_check is clean
#   grammar       3. definitions the candidate refuses pause with a remedy, and it works
#   inflight      4. mid-build, mid-rebuild, paused, held key, waiting for capture
#   structure     5a. a structure.sql dumped by v0.1.0 loads and builds
#   pgrestore     5b. a pg_dump/pg_restore of a v0.1.0 database (documented pauses, then RESUME)
#   basebackup    5c. a pg_basebackup restore of a running v0.1.0 database
#   clients       6. Ruby gem and Elixir package (manual)
#   mixed         7. the candidate stands by beside a live v0.1.0, then takes over
#   downgrade     8. rollback: the old release on a database the candidate wrote (ADR-0024)
#
# Options:
#   --candidate REV|DIR   the release candidate: a directory or a git revision.
#                         Default: the checkout this script is in.
#   --candidate-version V the version string the candidate is built as (default
#                         0.2.0, the release version: a candidate must report
#                         exactly it, since version handover compares the exact
#                         string). It is set in a scratch checkout, never in the
#                         tree under test, and nothing is committed. `none`
#                         builds the tree with the version it carries.
#   --candidate-bin PATH  use this CLI binary; the candidate's source is still
#                         needed (--candidate) for the library probe
#   --old-tag TAG         the released version to start from (default v0.1.0)
#   --old-bin PATH        use this CLI binary instead of the tag's release asset
#   --build-old           build the tag's CLI instead of downloading the asset
#                         (also the fallback when the download fails)
#   --pg-bin DIR          PostgreSQL 15+ bin directory (default PGBIN, then
#                         ~/.asdf/installs/postgres/17.11/bin, then PATH)
#   --port N              cluster port (default 54920; the basebackup clone uses N+1)
#   --work DIR            scratch directory (default <repo>/target/upgrade-battery)
#   --keep                keep clusters running and files in place on exit
#   -h, --help            this text
#
# A revision is built in a scratch worktree. A directory is built in place only
# with --candidate-version none; otherwise it is rebuilt in a scratch worktree
# of its HEAD with the directory's uncommitted and untracked changes copied in.
# --candidate-bin is used as it is: its own version is the candidate's.
#
# Needs: git, cargo, curl or gh (to download the release), PostgreSQL 15+
# server binaries (initdb, pg_ctl, psql, pg_dump, pg_restore, pg_basebackup).
# Starts throwaway clusters of its own on 127.0.0.1, trust auth, and touches no
# other database. The probe crate (upgrade-battery/check) is built against the
# tag and against the candidate for the two library calls the CLI lacks.
# Uses SF_GH_TOKEN (or GH_TOKEN) for `gh` when set.
set -euo pipefail

repo=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
upstream=salesforce-misc/trellis

old_tag=v0.1.0
old_bin=""
build_old=0
candidate="$repo"
candidate_bin=""
cand_ver_req=0.2.0
pg_bin="${PGBIN:-}"
port=54920
work="$repo/target/upgrade-battery"
keep=0
steps=()

usage() { sed -n '2,/^set -e/p' "$0" | sed '$d; s/^# \{0,1\}//'; }
die() { echo "upgrade-battery: $*" >&2; exit 2; }

while [ $# -gt 0 ]; do
  case "$1" in
    --candidate) candidate="${2:?--candidate needs a revision or directory}"; shift 2 ;;
    --candidate-bin) candidate_bin="${2:?}"; shift 2 ;;
    --candidate-version) cand_ver_req="${2:?--candidate-version needs a version or none}"; shift 2 ;;
    --old-tag) old_tag="${2:?}"; shift 2 ;;
    --old-bin) old_bin="${2:?}"; shift 2 ;;
    --build-old) build_old=1; shift ;;
    --pg-bin) pg_bin="${2:?}"; shift 2 ;;
    --port) port="${2:?}"; shift 2 ;;
    --work) work="${2:?}"; shift 2 ;;
    --keep) keep=1; shift ;;
    -h|--help) usage; exit 0 ;;
    -*) die "unknown option $1 (--help lists them)" ;;
    *) steps+=("$1"); shift ;;
  esac
done
[ "$cand_ver_req" = none ] || [[ "$cand_ver_req" =~ ^[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z.-]+)?$ ]] ||
  die "--candidate-version $cand_ver_req is not a version (want e.g. 0.2.0, or none)"
all_steps="migrations definitions grammar inflight structure pgrestore basebackup clients mixed downgrade"
[ ${#steps[@]} -gt 0 ] || read -r -a steps <<< "$all_steps"
for s in "${steps[@]}"; do
  case " $all_steps " in *" $s "*) ;; *) die "unknown step $s (steps: $all_steps)" ;; esac
done
want() { local s; for s in "${steps[@]}"; do [ "$s" = "$1" ] && return 0; done; return 1; }

if [ -z "$pg_bin" ]; then
  if [ -x "$HOME/.asdf/installs/postgres/17.11/bin/initdb" ]; then pg_bin="$HOME/.asdf/installs/postgres/17.11/bin"
  elif command -v initdb >/dev/null 2>&1; then pg_bin=$(dirname "$(command -v initdb)")
  else die "no PostgreSQL server binaries: pass --pg-bin"; fi
fi
for b in initdb pg_ctl psql pg_dump pg_restore pg_basebackup; do
  [ -x "$pg_bin/$b" ] || die "$pg_bin/$b not found"
done
for b in git cargo; do command -v "$b" >/dev/null 2>&1 || die "$b not found"; done

mkdir -p "$work/logs"
work=$(cd "$work" && pwd)
logs="$work/logs"
port_b=$((port + 1))

# ---------------------------------------------------------------- reporting
results=()
fails=0
record() { # PASS|FAIL|INFO|MANUAL|SKIP <step> <message>
  printf '%-6s %-12s %s\n' "$1" "$2" "$3"
  results+=("$1|$2|$3")
  [ "$1" != FAIL ] || fails=$((fails + 1))
}
say() { printf '== %s\n' "$*"; }

# ---------------------------------------------------------------- processes
bg_pids=()
clusters=()
extra_worktrees=()
cleanup() {
  local p
  for p in ${bg_pids[@]+"${bg_pids[@]}"}; do kill -KILL "$p" 2>/dev/null || true; done
  if [ "$keep" -eq 0 ]; then
    for p in ${clusters[@]+"${clusters[@]}"}; do "$pg_bin/pg_ctl" -D "$p" -m immediate stop >/dev/null 2>&1 || true; done
    for p in ${extra_worktrees[@]+"${extra_worktrees[@]}"}; do git -C "$repo" worktree remove --force "$p" >/dev/null 2>&1 || true; done
  fi
}
trap cleanup EXIT

url() { echo "postgres://postgres@127.0.0.1:$1/$2"; } # port db
sql() { # port db psql-args...
  local p=$1 d=$2; shift 2
  "$pg_bin/psql" -X -q -h 127.0.0.1 -p "$p" -U postgres -d "$d" -v ON_ERROR_STOP=1 "$@"
}
scalar() { sql "$1" "$2" -At -c "$3"; } # port db query
trellis() { # port db bin args...  (the CLI against a database)
  local p=$1 d=$2 b=$3; shift 3
  TRELLIS_DATABASE_URL=$(url "$p" "$d") "$b" "$@"
}

start_cluster() { # dir port
  local dir=$1 p=$2
  mkdir -p "$dir/sock"
  "$pg_bin/pg_ctl" -D "$dir/data" -o "-p $p -k $dir/sock -c listen_addresses=127.0.0.1 -c fsync=off -c max_connections=300" \
    -l "$logs/pg-$p.log" -w start >/dev/null
  clusters+=("$dir/data")
}

# run_bg <name> <port> <db> <bin> [run args]: `trellis run` in the background.
run_bg() {
  local name=$1 p=$2 d=$3 b=$4; shift 4
  TRELLIS_DATABASE_URL=$(url "$p" "$d") "$b" run "$@" >"$logs/run-$name.log" 2>&1 &
  echo $! >"$work/run-$name.pid"
  bg_pids+=("$!")
}
stop_run() { # name: graceful (Ctrl-C), as an operator stops it
  local pid i
  pid=$(cat "$work/run-$1.pid" 2>/dev/null) || return 0
  kill -INT "$pid" 2>/dev/null || return 0
  for i in $(seq 1 60); do kill -0 "$pid" 2>/dev/null || return 0; sleep 0.5; done
  kill -KILL "$pid" 2>/dev/null || true
}
run_alive() { local pid; pid=$(cat "$work/run-$1.pid" 2>/dev/null) && kill -0 "$pid" 2>/dev/null; }

# status_lines <port> <db> <bin>: "name=status" per definition, from the CLI.
status_lines() {
  trellis "$1" "$2" "$3" status 2>/dev/null | sed -n 's/^  target=[^.]*\.\([^ ]*\) status=\([^ ]*\).*/\1=\2/p'
}
status_of() { status_lines "$1" "$2" "$3" | sed -n "s/^$4=//p"; } # port db bin name
# stored_status <port> <db> <name>: the catalog's word (no CLI, so no `catching_up`).
stored_status() { scalar "$1" "$2" "select status from trellis.transform_definitions where target_table = 'public.$3'"; }

# wait_for <seconds> <description> <command...>: poll until the command succeeds.
wait_for() {
  local limit=$1 what=$2 i; shift 2
  for i in $(seq 1 $((limit * 2))); do "$@" && return 0; sleep 0.5; done
  echo "upgrade-battery: timed out after ${limit}s waiting for $what" >&2
  return 1
}
all_status() { # port db bin status name...
  local p=$1 d=$2 b=$3 want=$4 n got; shift 4
  got=$(status_lines "$p" "$d" "$b")
  for n in "$@"; do grep -qx "$n=$want" <<<"$got" || return 1; done
}
wait_all() { wait_for "${WAIT:-180}" "$5... $4" all_status "$@"; } # port db bin status name...

failure_reason() { # port db name -> capture_failures.error
  scalar "$1" "$2" "select cf.error from trellis.capture_failures cf join trellis.transform_definitions d on d.id = cf.transform_id where d.target_table = 'public.$3'"
}
history() { scalar "$1" "$2" "select version, name, checksum, applied_on from trellis.refinery_schema_history order by version"; }

# ---------------------------------------------------------------- writers
writer_sql="$work/writer.sql"
cat >"$writer_sql" <<'EOF'
insert into orders select 100000 + (random()*900000)::int, 1+(random()*49)::int, (random()*1000)::numeric(12,2), 'w' from generate_series(1,20) on conflict do nothing;
update orders set amount = amount + 1 where id in (select 1+(random()*2000)::int from generate_series(1,20));
update orders set customer_id = 1+(random()*49)::int where id in (select 1+(random()*2000)::int from generate_series(1,10));
delete from orders where id in (select 100000+(random()*900000)::int from generate_series(1,10));
update customers set region = case when random() < 0.5 then 'r1' else 'r2' end where id = 1+(random()*49)::int;
update ledger set n = (random()*6)::int where id <> 5 and id in (select 1+(random()*99)::int from generate_series(1,5));
insert into events select (random()*1000000)::bigint, (random()*20)::bigint, 1 from generate_series(1,10) on conflict do nothing;
update events set v = v + 1 where tenant = (random()*20)::bigint;
delete from events where (id, tenant) in (select id, tenant from events limit 5);
EOF
writer_iters=0
start_writers() { # port db
  : >"$work/writers.on"; : >"$work/writer-iters.log"
  (
    while [ -e "$work/writers.on" ]; do
      sql "$1" "$2" -f "$writer_sql" >>"$logs/writers.log" 2>&1 || true
      echo >>"$work/writer-iters.log"
      sleep 0.05
    done
  ) &
  bg_pids+=("$!")
}
stop_writers() {
  rm -f "$work/writers.on"
  sleep 1
  writer_iters=$(wc -l <"$work/writer-iters.log" 2>/dev/null || echo 0)
}

held_key() { [ "$(scalar "$1" "$2" "select count(*) from trellis.poison")" -ge 1 ]; } # port db
builds_in_flight() { [ "$(scalar "$1" "$2" "select count(*) from trellis.transform_definitions where status = 'backfilling'")" -ge "$3" ]; }
paused_columns() { scalar "$1" "$2" "select count(*) from trellis.column_status where transform_table ~ '(^|[.])orders_wide$' and column_name = 'plus3'"; }

ring_rows() { # port db: rows in the ring's segment tables
  scalar "$1" "$2" "select coalesce(sum((xpath('/row/c/text()', query_to_xml(format('select count(*) c from trellis.%I', relname), false, true, '')))[1]::text::bigint), 0) from pg_class where relnamespace = 'trellis'::regnamespace and relname ~ '^seg_[0-9]+\$'"
}

# ---------------------------------------------------------------- the old build
say "obtaining $old_tag"
if [ -z "$old_bin" ]; then
  case "$(uname -s)-$(uname -m)" in
    Linux-x86_64) triple=x86_64-unknown-linux-gnu ;;
    Linux-aarch64|Linux-arm64) triple=aarch64-unknown-linux-gnu ;;
    Darwin-arm64) triple=aarch64-apple-darwin ;;
    Darwin-x86_64) triple=x86_64-apple-darwin ;;
    *) triple="" ;;
  esac
  asset="trellis-$old_tag-$triple.tar.gz"
  dl="$work/dl-$old_tag"
  if [ "$build_old" -eq 0 ] && [ -n "$triple" ]; then
    mkdir -p "$dl"
    ok=0
    if command -v gh >/dev/null 2>&1 &&
      GH_TOKEN="${SF_GH_TOKEN:-${GH_TOKEN:-}}" gh release download "$old_tag" --repo "$upstream" \
        -p "$asset" -p SHA256SUMS --dir "$dl" --clobber >/dev/null 2>&1; then ok=1
    elif curl -fsSL -o "$dl/$asset" "https://github.com/$upstream/releases/download/$old_tag/$asset" &&
      curl -fsSL -o "$dl/SHA256SUMS" "https://github.com/$upstream/releases/download/$old_tag/SHA256SUMS"; then ok=1; fi
    if [ "$ok" -eq 1 ]; then
      want_sum=$(awk -v a="$asset" '$2 == a { print $1 }' "$dl/SHA256SUMS")
      got_sum=$( (sha256sum "$dl/$asset" 2>/dev/null || shasum -a 256 "$dl/$asset") | awk '{ print $1 }')
      [ -n "$want_sum" ] && [ "$want_sum" = "$got_sum" ] || die "$asset does not match SHA256SUMS"
      tar -xzf "$dl/$asset" -C "$dl"
      old_bin=$(find "$dl" -name trellis -type f -perm -u+x | head -1)
      [ -n "$old_bin" ] || die "no trellis binary in $asset"
      echo "published $asset, checksum verified"
    else
      echo "download of $asset failed; building the tag instead" >&2
      build_old=1
    fi
  else
    build_old=1
  fi
fi
# checkout_at <dir> <rev>: a scratch worktree at that revision. One a kept or
# killed run left behind is moved to <rev>, so a re-run never tests a stale tree.
checkout_at() {
  if [ -f "$1/.git" ]; then
    git -C "$1" checkout -q --force --detach "$2" && git -C "$1" clean -fdq
  else
    rm -rf "$1"; git -C "$repo" worktree prune
    git -C "$repo" worktree add -q --detach "$1" "$2"
  fi
}
old_src=""
ensure_old_src() {
  [ -n "$old_src" ] && return 0
  git -C "$repo" rev-parse --verify -q "$old_tag^{commit}" >/dev/null ||
    git -C "$repo" fetch -q upstream "refs/tags/$old_tag:refs/tags/$old_tag" ||
    { echo "tag $old_tag is not in this repository and could not be fetched" >&2; return 1; }
  checkout_at "$work/old-src" "$old_tag" || return 1
  old_src="$work/old-src"
  extra_worktrees+=("$old_src")
}
if [ "$build_old" -eq 1 ] && [ -z "$old_bin" ]; then
  ensure_old_src || die "cannot build $old_tag"
  (cd "$old_src" && CARGO_TARGET_DIR="$work/old-target" cargo build --release -p trellis-cli --locked) >"$logs/build-old.log" 2>&1 ||
    die "building $old_tag failed (see $logs/build-old.log)"
  old_bin="$work/old-target/release/trellis"
fi
old_version=$("$old_bin" --version)
echo "old: $old_bin ($old_version)"

# ---------------------------------------------------------------- the candidate
say "building the candidate"
# The version handover compares the exact version string, so a candidate is
# built reporting the release version, set in a scratch checkout: the tree under
# test is never touched and nothing is committed.
override_version=""
if [ "$cand_ver_req" != none ] && [ -z "$candidate_bin" ]; then override_version=$cand_ver_req; fi
cand_dirty=""
if [ -d "$candidate" ] && [ -z "$override_version" ]; then
  cand_src=$(cd "$candidate" && pwd)
  cand_target="$cand_src/target"
elif [ -d "$candidate" ]; then
  # The checkout's top level, so a subdirectory candidate copies every change.
  dir_src=$(git -C "$candidate" rev-parse --show-toplevel) || die "$candidate is not a git checkout"
  cand_src="$work/cand-src"
  cand_head=$(git -C "$dir_src" rev-parse HEAD) || die "$dir_src is not a git checkout"
  checkout_at "$cand_src" "$cand_head" || die "cannot check out $cand_head of $dir_src (the candidate directory must be a worktree of $repo)"
  extra_worktrees+=("$cand_src")
  # A plain patch for git apply, whatever the user's diff settings.
  git -C "$dir_src" diff --binary --no-color --no-ext-diff --no-textconv --no-relative \
    --src-prefix=a/ --dst-prefix=b/ HEAD >"$work/cand-uncommitted.patch"
  [ ! -s "$work/cand-uncommitted.patch" ] || git -C "$cand_src" apply --whitespace=nowarn "$work/cand-uncommitted.patch" ||
    die "cannot copy the uncommitted changes of $dir_src"
  git -C "$dir_src" ls-files -z --others --exclude-standard >"$work/cand-untracked.list"
  if [ -s "$work/cand-untracked.list" ]; then
    (cd "$dir_src" && tar -cf - --null -T "$work/cand-untracked.list") | tar -xf - -C "$cand_src" ||
      die "cannot copy the untracked files of $dir_src"
  fi
  [ -z "$(git -C "$dir_src" status --porcelain 2>/dev/null)" ] || cand_dirty="+uncommitted changes"
  cand_target="$work/cand-target"
else
  cand_src="$work/cand-src"
  checkout_at "$cand_src" "$candidate" || die "cannot check out the candidate $candidate"
  extra_worktrees+=("$cand_src")
  cand_target="$work/cand-target"
fi
cand_rev=$(git -C "${dir_src:-$cand_src}" rev-parse --short HEAD 2>/dev/null || echo unknown)
[ -n "${dir_src:-}" ] || [ -z "$(git -C "$cand_src" status --porcelain 2>/dev/null)" ] || cand_dirty="+uncommitted changes"
if [ -n "$override_version" ]; then
  # The crates the CLI and the library read their version from, and the
  # lockfile entries for them (the build is --locked).
  for f in cli/Cargo.toml trellis/Cargo.toml; do
    V="$override_version" perl -0pi -e 's/^version = "[^"]*"$/version = "$ENV{V}"/m' "$cand_src/$f"
  done
  (cd "$cand_src" && cargo update --workspace --offline --quiet) >"$logs/set-version.log" 2>&1 ||
    die "setting the candidate's version in the scratch checkout failed (see $logs/set-version.log)"
  for c in trellis-cli trellis; do
    got=$(cd "$cand_src" && cargo pkgid -p "$c" --offline | sed 's/.*[#@]//')
    [ "$got" = "$override_version" ] || die "the scratch checkout's $c is version $got, not $override_version"
  done
fi
if [ -z "$candidate_bin" ]; then
  (cd "$cand_src" && CARGO_TARGET_DIR="$cand_target" cargo build --release -p trellis-cli --locked) >"$logs/build-candidate.log" 2>&1 ||
    die "building the candidate failed (see $logs/build-candidate.log)"
  candidate_bin="$cand_target/release/trellis"
fi
cand_version=$("$candidate_bin" --version)
cand_ver=${cand_version#trellis }
[ -z "$override_version" ] || [ "$cand_ver" = "$override_version" ] ||
  die "the candidate binary reports version '$cand_ver', not $override_version"
if [ -n "$override_version" ]; then cand_built="built as version $override_version, set in a scratch checkout, never committed"
elif [ "$cand_ver_req" != none ]; then cand_built="a prebuilt binary: version $cand_ver as it is (--candidate-version applies to builds)"
else cand_built="built with the version the tree carries"; fi
echo "candidate: $candidate_bin ($cand_version) at $cand_rev$cand_dirty, $cand_built"

# build_probe <name> <source tree>: the library probe against that tree.
build_probe() {
  local name=$1 src=$2 d="$work/probe-$1"
  rm -rf "$d"; mkdir -p "$d"
  cp -r "$repo/.github/scripts/upgrade-battery/check/." "$d/"
  sed -i.bak "s#TRELLIS_PATH#$src/trellis#" "$d/Cargo.toml" && rm -f "$d/Cargo.toml.bak"
  cp "$src/Cargo.lock" "$d/Cargo.lock"
  if (cd "$d" && CARGO_TARGET_DIR="$d/target" cargo build --release) >"$logs/build-probe-$name.log" 2>&1; then
    echo "$d/target/release/upgrade-battery-check"
  else
    return 1
  fi
}
probe_old=""
probe_new=""
if probe_new=$(build_probe candidate "$cand_src"); then :; else
  probe_new=""; echo "the probe does not build against the candidate (see $logs/build-probe-candidate.log)" >&2
fi
if ensure_old_src 2>/dev/null && probe_old=$(build_probe old "$old_src"); then :; else
  probe_old=""; echo "the probe does not build against $old_tag (see $logs/build-probe-old.log)" >&2
fi
probe() { # <probe-bin> port db args...
  local b=$1 p=$2 d=$3; shift 3
  TRELLIS_DATABASE_URL=$(url "$p" "$d") "$b" "$@"
}

# ---------------------------------------------------------------- the cluster
say "starting a cluster on port $port"
# A cluster a kept or killed run left running is stopped before its directory goes.
for d in "$work/cluster-a/data" "$work/cluster-b/data"; do
  [ ! -f "$d/postmaster.pid" ] || "$pg_bin/pg_ctl" -D "$d" -m immediate stop >/dev/null 2>&1 || true
done
rm -rf "$work/cluster-a" "$work/cluster-b" "$work/cluster-b-data" "$work/backup"
mkdir -p "$work/cluster-a"
"$pg_bin/initdb" -D "$work/cluster-a/data" -U postgres --no-sync -A trust >/dev/null
start_cluster "$work/cluster-a" "$port"
pg_major=$("$pg_bin/psql" -X -At -h 127.0.0.1 -p "$port" -U postgres -d postgres -c "show server_version_num")
[ "$pg_major" -ge 150000 ] || die "PostgreSQL 15 or later is required (partitioned sources); this is $pg_major"
for d in shapes inflight ctl mixed clients; do sql "$port" postgres -c "create database $d"; done

# ---------------------------------------------------------------- prep: shapes
say "building the $old_tag databases"
shapes_expected_live="orders_copy chain1 orders_wide cust_totals cust_join cust_join_left tenant_totals tenant_join ledger_ovf"
shapes_grammar_paused="ledger_copy deep deep2"
shapes_paused="$shapes_grammar_paused ord_paused"
# Step 8: definitions only the candidate can run. Each uses a feature,
# overload, argument value or type-rule admission whose release in the
# candidate's .github/feature-floors.txt is after the old release's (a
# candidate-only type admission among them, when the candidate adds one). One
# entry each: the manifest lines it covers (each without its release, an
# admission also without its case, `;` between them), `|`, the target's name,
# `|`, the definition over the shapes tables. The step fails a candidate-only
# manifest line no entry covers. Empty for 0.2.0: floors arrive in it, and
# v0.1.0, which carries none, refuses its database.
rollback_new_defs=(
)
# What a release before 0.2.0 says when a later release has migrated the
# database. Its `migrate`, which its `run` and `apply` call first, refuses the
# marker's instance_format_version (2 from the floor migration on, above the 1
# it understands) before its migration runner starts; refinery, which refuses
# an applied migration the binary lacks, would refuse it next.
pre_floor_refusal='written by a newer Trellis|refinery|missing from the filesystem'

shapes_schema=$(cat <<'EOF'
create table customers(id bigint primary key, name text not null, region text);
create table orders(id bigint primary key, customer_id bigint not null, amount numeric(12,2) not null, note text);
create index on orders(customer_id);
create table ledger(id bigint primary key, "order" bigint not null, n bigint not null);
create table events(id bigint not null, tenant bigint not null, v bigint not null, primary key (id, tenant)) partition by hash (tenant);
create table events_0 partition of events for values with (modulus 4, remainder 0);
create table events_1 partition of events for values with (modulus 4, remainder 1);
create table events_2 partition of events for values with (modulus 4, remainder 2);
create table events_3 partition of events for values with (modulus 4, remainder 3);
create index on events(tenant);
insert into customers select g, 'c'||g, case when g % 3 = 0 then null else 'r'||(g % 3) end from generate_series(1,50) g;
insert into orders select g, 1 + g % 50, g, 'n' from generate_series(1,2000) g;
insert into ledger select g, g, g % 7 from generate_series(1,100) g;
insert into events select g, g % 20, 1 from generate_series(1,1000) g;
EOF
)
sql "$port" shapes <<<"$shapes_schema"
seed_rows=$((50 + 2000 + 100 + 1000))

deep_chain="n"; for _ in $(seq 1 130); do deep_chain="$deep_chain + 1"; done
deep_nested="n"; for _ in $(seq 1 130); do deep_nested="($deep_nested) + 1"; done
apply_old() { trellis "$port" "$1" "$old_bin" apply "$2" >>"$logs/apply-old.log" 2>&1 || { echo "upgrade-battery: $old_tag refused: $2" >&2; tail -2 "$logs/apply-old.log" >&2; return 1; }; }

# Shapes: plain 1-1, aggregate, JOIN aggregates (inner and LEFT), aggregate and
# JOIN over a hash-partitioned source, a 1-1 with a held key, and the three
# definitions v0.1.0 accepts that the candidate refuses (a bare reserved word;
# an expression nested over 100 levels, as a chain and as parentheses).
apply_old shapes 'TRANSFORM orders_copy FROM orders SELECT amount AS amount, amount + 1 AS plus'
apply_old shapes 'TRANSFORM cust_totals FROM orders GROUP BY customer_id SELECT customer_id AS customer_id, COUNT(*) AS n, SUM(amount) AS total'
apply_old shapes 'TRANSFORM cust_join FROM orders JOIN customers ON orders.customer_id = customers.id GROUP BY customers.region SELECT customers.region AS region, COUNT(*) AS n, SUM(orders.amount) AS total'
apply_old shapes 'TRANSFORM cust_join_left FROM orders LEFT JOIN customers ON orders.customer_id = customers.id GROUP BY customers.region SELECT customers.region AS region, COUNT(*) AS n'
apply_old shapes 'TRANSFORM tenant_totals FROM events GROUP BY tenant SELECT tenant AS tenant, COUNT(*) AS n, SUM(v) AS s'
apply_old shapes 'TRANSFORM tenant_join FROM events JOIN customers ON events.tenant = customers.id GROUP BY events.tenant SELECT events.tenant AS tenant, COUNT(*) AS n'
apply_old shapes 'TRANSFORM orders_wide FROM orders SELECT amount AS amount, amount + 2 AS plus2, amount + 3 AS plus3'
apply_old shapes 'TRANSFORM ord_paused FROM orders SELECT note AS note'
apply_old shapes 'TRANSFORM ledger_ovf FROM ledger SELECT n + 9223372036854775000 AS big'
apply_old shapes 'TRANSFORM ledger_copy FROM ledger SELECT order AS o'
apply_old shapes "TRANSFORM deep FROM ledger SELECT $deep_chain AS d"
apply_old shapes "TRANSFORM deep2 FROM ledger SELECT $deep_nested AS d"

run_bg old-shapes "$port" shapes "$old_bin"
WAIT=240 wait_all "$port" shapes "$old_bin" live orders_copy orders_wide cust_totals cust_join cust_join_left tenant_totals tenant_join ledger_ovf ord_paused ||
  die "the $old_tag definitions did not go live (see $logs/run-old-shapes.log)"
# A chained target needs its upstream live; a column pause and a whole-transform pause.
apply_old shapes 'TRANSFORM chain1 FROM orders_copy SELECT plus AS plus_again'
WAIT=120 wait_all "$port" shapes "$old_bin" live chain1 || die "the chained target did not go live"
apply_old shapes 'PAUSE TRANSFORM orders_wide.plus3'
apply_old shapes 'PAUSE TRANSFORM ord_paused'
pre_col_paused=$(paused_columns "$port" shapes)
[ "$pre_col_paused" = 1 ] || die "the column pause was not recorded"

start_writers "$port" shapes
sleep 4
sql "$port" shapes -c "update ledger set n = 1000 where id = 5"
wait_for 60 "the held key" held_key "$port" shapes || die "the overflowing key was not quarantined"

# Undrained ring segments: stop the draining instance, keep the staging one, keep writing.
stop_run old-shapes
run_bg old-shapes "$port" shapes "$old_bin" --drain-threads 0
sleep 8
ring_before=$(ring_rows "$port" shapes)
pre_shapes_status=$(status_lines "$port" shapes "$old_bin" | tr '\n' ' ')
pre_history=$(history "$port" shapes)
pre_poison=$(scalar "$port" shapes "select count(*) from trellis.poison")
say "taking the backups of the running $old_tag database"
"$pg_bin/pg_basebackup" -h 127.0.0.1 -p "$port" -U postgres -D "$work/backup" -X stream -c fast --no-sync >"$logs/basebackup.log" 2>&1 ||
  die "pg_basebackup failed (see $logs/basebackup.log)"
"$pg_bin/pg_dump" -h 127.0.0.1 -p "$port" -U postgres -Fc -f "$work/shapes.dump" shapes
"$pg_bin/pg_dump" -h 127.0.0.1 -p "$port" -U postgres --schema-only -n public -n trellis -f "$work/structure-schema.sql" shapes
structure_ok=0
if [ -n "$probe_old" ] && probe "$probe_old" "$port" shapes dump-definitions >"$work/structure-definitions.sql" 2>"$logs/dump-definitions.log"; then
  structure_ok=1
fi

# ---------------------------------------------------------------- prep: inflight
sql "$port" inflight <<'EOF'
create table src(id bigint primary key, g bigint not null, v bigint not null);
create index on src(g);
create table big(id bigint primary key, g bigint not null, v bigint not null);
create index on big(g);
insert into src select g, g % 50, g % 100 from generate_series(1,5000) g;
insert into big select g, g % 1000, g from generate_series(1,100000) g;
EOF
seed_rows=$((seed_rows + 5000 + 100000))
apply_old inflight 'TRANSFORM live_ok FROM src GROUP BY g SELECT g AS g, COUNT(*) AS n, SUM(v) AS s'
apply_old inflight 'TRANSFORM rebuild_me FROM src SELECT v + 1 AS v1'
apply_old inflight 'TRANSFORM held_t FROM src SELECT v + 9223372036854775000 AS big'
apply_old inflight 'TRANSFORM paused_t FROM src SELECT v + 2 AS v2'
run_bg old-inflight "$port" inflight "$old_bin"
WAIT=180 wait_all "$port" inflight "$old_bin" live live_ok rebuild_me held_t paused_t || die "the inflight definitions did not go live"
sql "$port" inflight -c "update src set v = 1000 where id = 5"
wait_for 60 "the held key" held_key "$port" inflight || die "the inflight overflowing key was not quarantined"
apply_old inflight 'PAUSE TRANSFORM paused_t'
apply_old inflight 'PAUSE TRANSFORM rebuild_me'
stop_run old-inflight
# Without a drain worker a build starts and never finishes: it stays `backfilling`.
run_bg old-inflight "$port" inflight "$old_bin" --drain-threads 0
apply_old inflight 'TRANSFORM big_agg FROM big GROUP BY g SELECT g AS g, COUNT(*) AS n, SUM(v) AS s'
apply_old inflight 'RESUME TRANSFORM rebuild_me'
wait_for 90 "builds in flight" builds_in_flight "$port" inflight 2 || die "the builds did not start (is a drain worker running?)"
stop_run old-inflight
# Defined with no staging worker running: waiting for capture.
apply_old inflight 'TRANSFORM wait_agg FROM src GROUP BY g SELECT g AS g, COUNT(*) AS n'
pre_inflight_status=$(status_lines "$port" inflight "$old_bin" | tr '\n' ' ')
pre_inflight_poison=$(scalar "$port" inflight "select count(*) from trellis.poison")

# ctl: a database the old CLI created, for the migration control.
sql "$port" ctl -c "create table t(id bigint primary key, v bigint not null)"
apply_old ctl 'TRANSFORM tc FROM t SELECT v AS v2'
pre_ctl_history=$(history "$port" ctl)

# clients: built and run by the old CLI only, for the manual client step; the
# candidate never opens it, so the old client opens it first.
if want clients; then
  sql "$port" clients -c "create table c(id bigint primary key, g bigint not null, v bigint not null)" -c "create index on c(g)" \
    -c "insert into c select g, g % 10, g from generate_series(1,1000) g"
  seed_rows=$((seed_rows + 1000))
  apply_old clients 'TRANSFORM c_copy FROM c SELECT v + 1 AS v1'
  apply_old clients 'TRANSFORM c_agg FROM c GROUP BY g SELECT g AS g, COUNT(*) AS n, SUM(v) AS s'
  run_bg old-clients "$port" clients "$old_bin"
  WAIT=120 wait_all "$port" clients "$old_bin" live c_copy c_agg || die "the clients definitions did not go live"
  stop_run old-clients
fi

# ---------------------------------------------------------------- the upgrade
say "upgrading: stop $old_tag, start the candidate (writers keep running)"
stop_run old-shapes
run_bg new-shapes "$port" shapes "$candidate_bin"
run_bg new-inflight "$port" inflight "$candidate_bin"
sleep 8
stop_writers
live_shapes=$shapes_expected_live
WAIT=240 wait_all "$port" shapes "$candidate_bin" live $live_shapes || true
WAIT=240 wait_all "$port" inflight "$candidate_bin" live live_ok rebuild_me held_t big_agg wait_agg || true
post_shapes_status=$(status_lines "$port" shapes "$candidate_bin" | tr '\n' ' ')
post_inflight_status=$(status_lines "$port" inflight "$candidate_bin" | tr '\n' ' ')
echo "ring rows in flight when $old_tag stopped: $ring_before; writer iterations: $writer_iters"
echo "shapes   before: $pre_shapes_status"
echo "shapes   after:  $post_shapes_status"
echo "inflight before: $pre_inflight_status"
echo "inflight after:  $post_inflight_status"

# ---------------------------------------------------------------- the steps
check_self() { # port db label targets...: self_check each with the candidate's probe (PROBE= another)
  local p=$1 d=$2 label=$3 out; shift 3
  local b=${PROBE-$probe_new}
  if [ -z "$b" ]; then record SKIP "$label" "the probe does not build against ${PROBE+$old_tag}${PROBE-the candidate}"; return 0; fi
  if out=$(probe "$b" "$p" "$d" self-check "$@" 2>&1); then
    if grep -q 'held_keys=[1-9]' <<<"$out"; then
      record FAIL "$label" "self_check converged but a key is held: $(grep 'held_keys=[1-9]' <<<"$out" | head -1)"
    else
      record PASS "$label" "self_check clean on $* ($(grep -o 'rows_compared=[0-9]*' <<<"$out" | tr '\n' ' '))"
    fi
  else
    record FAIL "$label" "self_check: $(echo "$out" | tr '\n' ';')"
  fi
}
missing_status() { # port db bin status names...: the names not at that status
  local p=$1 d=$2 b=$3 want=$4 n got out=""; shift 4
  got=$(status_lines "$p" "$d" "$b")
  for n in "$@"; do grep -qx "$n=$want" <<<"$got" || out="$out $n($(sed -n "s/^$n=//p" <<<"$got"))"; done
  echo "$out"
}

if want migrations; then
  say "1. migrations"
  if git -C "$cand_src" rev-parse -q --verify "$old_tag^{commit}" >/dev/null 2>&1; then
    # The candidate's files as they are on disk (a directory candidate may hold
    # uncommitted or untracked migrations), against the tag's.
    old_mig_n=$(git -C "$cand_src" ls-tree --name-only "$old_tag" trellis/migrations/ | wc -l | tr -d ' ')
    # Every file the tag shipped must be unchanged; files the candidate adds
    # are new migrations (for 0.2.0, V9), reported for the release notes.
    mig_changed=$(git -C "$cand_src" diff --name-status --diff-filter=a "$old_tag" -- trellis/migrations | tr '\t' ' ' | tr '\n' ';')
    mig_added=$( (git -C "$cand_src" diff --name-only --diff-filter=A "$old_tag" -- trellis/migrations; git -C "$cand_src" ls-files --others --exclude-standard -- trellis/migrations) | grep . | xargs -r -n1 basename | tr '\n' ' ')
    if [ "$old_mig_n" -eq 0 ]; then
      record FAIL migrations "$old_tag has no files in trellis/migrations: the script's migrations path is out of date, so nothing was compared"
    elif [ -n "$mig_changed" ]; then
      record FAIL migrations "the candidate changes or removes migrations $old_tag shipped: $mig_changed"
    else
      record PASS migrations "the $old_mig_n migration files of $old_tag are byte-identical at the candidate"
      [ -z "$mig_added" ] ||
        record INFO migrations "the candidate adds migrations: ${mig_added% } (the upgrade path runs them; $old_tag can't open a database they ran on, so downgrade is ruled out)"
    fi
  else
    record SKIP migrations "the candidate checkout does not have tag $old_tag, so its migration files were not compared"
  fi
  post_history=$(history "$port" shapes)
  pre_n=$(wc -l <<<"$pre_history" | tr -d ' ')
  if [ "$(head -n "$pre_n" <<<"$post_history")" = "$pre_history" ]; then
    new_n=$(( $(wc -l <<<"$post_history") - pre_n ))
    record PASS migrations "history rows unchanged after the candidate opened the database ($pre_n rows, $new_n added)"
  else
    record FAIL migrations "history rows changed: before=[$(tr '\n' ' ' <<<"$pre_history")] after=[$(tr '\n' ' ' <<<"$post_history")]"
  fi
  # The control: the same open, after one stored checksum is altered, must refuse.
  stop_run new-shapes; stop_run new-inflight
  sql "$port" postgres -c "create database ctl_bad template ctl"
  sql "$port" ctl_bad -c "update trellis.refinery_schema_history set checksum = (checksum::numeric + 1)::text where version = 3"
  ctl_out=$(trellis "$port" ctl_bad "$candidate_bin" apply 'PAUSE TRANSFORM tc' 2>&1) && ctl_rc=0 || ctl_rc=$?
  if [ "$ctl_rc" -ne 0 ] && grep -qi 'different than\|checksum\|divergent' <<<"$ctl_out"; then
    record PASS migrations "control: the candidate refuses a database whose V3 checksum was altered ($(head -c 120 <<<"$ctl_out"))"
  else
    record FAIL migrations "control: the candidate did not refuse an altered checksum (rc=$ctl_rc): $ctl_out"
  fi
  ctl_ok=$(trellis "$port" ctl "$candidate_bin" apply 'PAUSE TRANSFORM tc' 2>&1) && ctl_ok_rc=0 || ctl_ok_rc=$?
  ctl_history=$(history "$port" ctl)
  if [ "$ctl_ok_rc" -eq 0 ] && [ "$(head -n "$(wc -l <<<"$pre_ctl_history")" <<<"$ctl_history")" = "$pre_ctl_history" ]; then
    record PASS migrations "the candidate opens an unaltered $old_tag-created database through the CLI, leaves its history rows alone and adds $(( $(wc -l <<<"$ctl_history") - $(wc -l <<<"$pre_ctl_history") )) ($ctl_ok)"
  else
    record FAIL migrations "the candidate refused an unaltered database: $ctl_ok"
  fi
  run_bg new-shapes "$port" shapes "$candidate_bin"
  run_bg new-inflight "$port" inflight "$candidate_bin"
  sleep 3
else
  record SKIP migrations "not selected"
fi

if want definitions; then
  say "2. live definitions of every shape"
  [ "${ring_before:-0}" -gt 0 ] && [ "$writer_iters" -gt 0 ] &&
    record PASS definitions "the candidate took over $ring_before ring rows $old_tag had not drained, with writers running ($writer_iters writer iterations)" ||
    record FAIL definitions "precondition: no undrained ring rows ($ring_before) or no writer iterations ($writer_iters) at the upgrade, so the takeover was not exercised"
  bad=$(missing_status "$port" shapes "$candidate_bin" live $live_shapes)
  if [ -z "$bad" ]; then record PASS definitions "live after the upgrade: $live_shapes"
  else record FAIL definitions "not live after the upgrade:$bad"; fi
  unexpected=$(status_lines "$port" shapes "$candidate_bin" | grep -E '=(paused|quarantined)$' | sed 's/=.*//' | sort | tr '\n' ' ')
  expected_paused=$(tr ' ' '\n' <<<"$shapes_paused" | sort | tr '\n' ' ')
  if [ "$unexpected" = "$expected_paused" ]; then
    record PASS definitions "paused after the upgrade: exactly the expected set ($expected_paused)"
  else
    record FAIL definitions "paused after the upgrade: [$unexpected], expected [$expected_paused]"
  fi
  [ "$(scalar "$port" shapes "select count(*) from trellis.poison")" = "$pre_poison" ] &&
    record PASS definitions "the held key is still held ($pre_poison)" ||
    record FAIL definitions "held keys: $(scalar "$port" shapes "select count(*) from trellis.poison") after, $pre_poison before"
  [ "$(paused_columns "$port" shapes)" = "$pre_col_paused" ] && [ "$pre_col_paused" = 1 ] &&
    record PASS definitions "the paused column (orders_wide.plus3) is still paused" ||
    record FAIL definitions "the paused column: $(paused_columns "$port" shapes) after the upgrade, $pre_col_paused before"
  # An oracle that does not use self_check: recompute two targets from the sources.
  diff_rows=$(scalar "$port" shapes "select count(*) from ((select customer_id, count(*)::bigint, sum(amount) from orders group by 1 except select customer_id, n, total from cust_totals) union all (select customer_id, n, total from cust_totals except select customer_id, count(*)::bigint, sum(amount) from orders group by 1)) d")
  diff_copy=$(scalar "$port" shapes "select count(*) from ((select id, amount, amount + 1 from orders except select id, amount, plus from orders_copy) union all (select id, amount, plus from orders_copy except select id, amount, amount + 1 from orders)) d")
  [ "$diff_rows" = 0 ] && [ "$diff_copy" = 0 ] &&
    record PASS definitions "independent recompute: cust_totals and orders_copy equal their sources" ||
    record FAIL definitions "independent recompute differs: cust_totals $diff_rows rows, orders_copy $diff_copy rows"
  check_self "$port" shapes definitions orders_copy chain1 orders_wide cust_totals cust_join cust_join_left tenant_totals tenant_join
  held_out=$(probe "${probe_new:-/nonexistent}" "$port" shapes self-check ledger_ovf 2>&1 || true)
  grep -q 'held_keys=1' <<<"$held_out" &&
    record PASS definitions "ledger_ovf's self_check reports its held key ($(head -c 110 <<<"$held_out"))" ||
    record FAIL definitions "ledger_ovf's self_check does not report the held key: $held_out"
else
  record SKIP definitions "not selected"
fi

if want grammar; then
  say "3. definitions the candidate refuses that $old_tag accepted"
  for n in ledger_copy deep deep2; do
    st=$(stored_status "$port" shapes "$n")
    reason=$(failure_reason "$port" shapes "$n")
    if [ "$st" = paused ] && grep -qi 'drop the transform and define it again' <<<"$reason"; then
      record PASS grammar "$n pauses with a remedy: $(echo "$reason" | sed 's/.*refused by this release: //' | cut -c1-110)..."
    else
      record FAIL grammar "$n is '$st' (reason: ${reason:-none})"
    fi
  done
  # The remedy: drop, define again in the current grammar.
  if rem=$(trellis "$port" shapes "$candidate_bin" apply 'DROP TRANSFORM ledger_copy' 2>&1 &&
    trellis "$port" shapes "$candidate_bin" apply 'TRANSFORM ledger_copy FROM ledger SELECT ledger.order AS o' 2>&1 &&
    trellis "$port" shapes "$candidate_bin" apply 'DROP TRANSFORM deep' 2>&1 &&
    trellis "$port" shapes "$candidate_bin" apply 'TRANSFORM deep FROM ledger SELECT n + 1 + 1 AS d' 2>&1 &&
    trellis "$port" shapes "$candidate_bin" apply 'DROP TRANSFORM deep2' 2>&1 &&
    trellis "$port" shapes "$candidate_bin" apply 'TRANSFORM deep2 FROM ledger SELECT (n + 1) + 1 AS d' 2>&1); then
    if WAIT=120 wait_all "$port" shapes "$candidate_bin" live ledger_copy deep deep2; then
      record PASS grammar "the remedy (drop, define again with ledger.order / a shorter expression) goes live"
      check_self "$port" shapes grammar ledger_copy deep deep2
    else
      record FAIL grammar "the redefined transforms did not go live: $(status_lines "$port" shapes "$candidate_bin" | grep -E 'ledger_copy|deep' | tr '\n' ' ')"
    fi
  else
    record FAIL grammar "the remedy did not apply: $rem"
  fi
  record MANUAL grammar "not exercised: an index over more than 32 key columns, a table over 1600 columns (v0.1.0 cannot build either here), and any refusal added after this script; see the runbook"
else
  record SKIP grammar "not selected"
fi

if want inflight; then
  say "4. work in flight at the upgrade"
  echo "before: $pre_inflight_status"
  pre_bad=""
  for kv in big_agg=backfilling rebuild_me=backfilling wait_agg=waiting_to_backfill paused_t=paused; do
    case " $pre_inflight_status " in *" $kv "*) ;; *) pre_bad="$pre_bad $kv" ;; esac
  done
  [ -z "$pre_bad" ] &&
    record PASS inflight "before the upgrade: big_agg and rebuild_me mid-build, wait_agg waiting for capture, paused_t paused" ||
    record FAIL inflight "precondition: before the upgrade, expected$pre_bad; was $pre_inflight_status"
  bad=$(missing_status "$port" inflight "$candidate_bin" live live_ok rebuild_me held_t big_agg wait_agg)
  [ -z "$bad" ] && record PASS inflight "continued to live: big_agg (was mid-build), rebuild_me (was mid-rebuild), wait_agg (waiting for capture), live_ok, held_t" ||
    record FAIL inflight "did not continue to live:$bad"
  [ "$(status_of "$port" inflight "$candidate_bin" paused_t)" = paused ] &&
    record PASS inflight "paused_t stays paused" || record FAIL inflight "paused_t is $(status_of "$port" inflight "$candidate_bin" paused_t)"
  [ "$(scalar "$port" inflight "select count(*) from trellis.poison")" = "$pre_inflight_poison" ] &&
    record PASS inflight "the held key is still held ($pre_inflight_poison)" || record FAIL inflight "held keys changed from $pre_inflight_poison"
  check_self "$port" inflight inflight live_ok rebuild_me big_agg wait_agg
  record INFO inflight "the fuse-tripped status 'quarantined' is not produced by this script; the held key stands for quarantine"
else
  record SKIP inflight "not selected"
fi

# Writers are stopped; the candidate stays up on shapes and inflight until the end.

if want structure; then
  say "5a. structure.sql from $old_tag"
  if [ "$structure_ok" -ne 1 ]; then
    record SKIP structure "the $old_tag probe (dump_definitions_sql) did not build or run; see $logs/dump-definitions.log"
  else
    sql "$port" postgres -c "create database loaded"
    sed 's/^CREATE SCHEMA public;/CREATE SCHEMA IF NOT EXISTS public;/' "$work/structure-schema.sql" | sql "$port" loaded >"$logs/structure-load.log" 2>&1 &&
      sql "$port" loaded -f "$work/structure-definitions.sql" >>"$logs/structure-load.log" 2>&1 &&
      record PASS structure "the schema-only dump plus $old_tag's dump_definitions_sql loads into an empty database" ||
      record FAIL structure "loading failed: $(tail -3 "$logs/structure-load.log" | tr '\n' ';')"
    run_bg new-loaded "$port" loaded "$candidate_bin"
    sql "$port" loaded <<'EOF' >/dev/null 2>&1 || true
insert into customers select g, 'c'||g, case when g % 3 = 0 then null else 'r'||(g % 3) end from generate_series(1,50) g;
insert into orders select g, 1 + g % 50, g, 'n' from generate_series(1,500) g;
insert into ledger select g, g, g % 7 from generate_series(1,100) g;
insert into events select g, g % 20, 1 from generate_series(1,300) g;
EOF
    load_live="orders_copy chain1 orders_wide cust_totals cust_join cust_join_left tenant_totals tenant_join ord_paused ledger_ovf"
    WAIT=240 wait_all "$port" loaded "$candidate_bin" live $load_live || true
    bad=$(missing_status "$port" loaded "$candidate_bin" live $load_live)
    [ -z "$bad" ] && record PASS structure "every loaded definition built and is live (including both JOIN aggregates and the one the dump held paused)" ||
      record FAIL structure "not live after the load:$bad"
    p3=$(status_lines "$port" loaded "$candidate_bin" | grep -E '=paused$' | sed 's/=.*//' | sort | tr '\n' ' ')
    [ "$p3" = "deep deep2 ledger_copy " ] &&
      record PASS structure "paused: only the three definitions the candidate refuses ($p3)" ||
      record FAIL structure "paused after the load: [$p3]"
    check_self "$port" loaded structure orders_copy chain1 cust_totals cust_join cust_join_left tenant_totals tenant_join
    stop_run new-loaded
  fi
else
  record SKIP structure "not selected"
fi

if want pgrestore; then
  say "5b. pg_dump and pg_restore"
  sql "$port" postgres -c "create database restored"
  if "$pg_bin/pg_restore" -h 127.0.0.1 -p "$port" -U postgres -d restored --no-owner "$work/shapes.dump" >"$logs/pg_restore.log" 2>&1; then
    record PASS pgrestore "pg_restore of the $old_tag database loads ($(grep -c . "$logs/pg_restore.log") log lines)"
  else
    record INFO pgrestore "pg_restore reported errors (often harmless, e.g. CREATE SCHEMA public): $(head -c 200 "$logs/pg_restore.log" | tr '\n' ';')"
  fi
  run_bg new-restored "$port" restored "$candidate_bin"
  restored_set="orders_copy chain1 orders_wide cust_totals cust_join cust_join_left tenant_totals tenant_join ord_paused ledger_ovf"
  WAIT=60 wait_all "$port" restored "$candidate_bin" paused $restored_set || true
  not_paused=$(missing_status "$port" restored "$candidate_bin" paused $restored_set)
  # The documented reason, on each one that was not already paused before the dump.
  wrong_reason=""
  for n in $restored_set; do
    [ "$n" != ord_paused ] || continue
    grep -q 'restored from a logical dump' <<<"$(failure_reason "$port" restored "$n")" || wrong_reason="$wrong_reason $n"
  done
  if [ -z "$not_paused" ] && [ -z "$wrong_reason" ]; then
    reason=$(failure_reason "$port" restored orders_copy)
    record PASS pgrestore "every definition paused on the restored database, as documented; orders_copy: $(echo "$reason" | cut -c1-120)"
  elif [ -z "$not_paused" ]; then
    record FAIL pgrestore "paused, but not for the documented logical-restore reason:$wrong_reason"
  else
    record FAIL pgrestore "documented pause after a logical restore did not happen for:$not_paused"
  fi
  for n in $restored_set; do trellis "$port" restored "$candidate_bin" apply "RESUME TRANSFORM $n" >>"$logs/pgrestore-resume.log" 2>&1 || true; done
  WAIT=240 wait_all "$port" restored "$candidate_bin" live $restored_set || true
  bad=$(missing_status "$port" restored "$candidate_bin" live $restored_set)
  [ -z "$bad" ] && record PASS pgrestore "after RESUME every definition is live again" || record FAIL pgrestore "not live after RESUME:$bad"
  check_self "$port" restored pgrestore orders_copy chain1 cust_totals cust_join cust_join_left tenant_totals tenant_join
  stop_run new-restored
else
  record SKIP pgrestore "not selected"
fi

if want basebackup; then
  say "5c. pg_basebackup restore"
  mv "$work/backup" "$work/cluster-b-data" 2>/dev/null || true
  mkdir -p "$work/cluster-b"
  rm -rf "$work/cluster-b/data"; mv "$work/cluster-b-data" "$work/cluster-b/data"; chmod 700 "$work/cluster-b/data"
  if start_cluster "$work/cluster-b" "$port_b"; then
    run_bg new-backup "$port_b" shapes "$candidate_bin"
    WAIT=240 wait_all "$port_b" shapes "$candidate_bin" live $shapes_expected_live || true
    bad=$(missing_status "$port_b" shapes "$candidate_bin" live $shapes_expected_live)
    [ -z "$bad" ] && record PASS basebackup "the restored cluster opens under the candidate and every expected definition is live (no pause, no RESUME)" ||
      record FAIL basebackup "not live on the restored cluster:$bad"
    p5=$(status_lines "$port_b" shapes "$candidate_bin" | grep -E '=paused$' | sed 's/=.*//' | sort | tr '\n' ' ')
    [ "$p5" = "deep deep2 ledger_copy ord_paused " ] &&
      record PASS basebackup "paused: only the expected set ($p5)" || record FAIL basebackup "paused on the restored cluster: [$p5]"
    check_self "$port_b" shapes basebackup orders_copy chain1 orders_wide cust_totals cust_join cust_join_left tenant_totals tenant_join
    stop_run new-backup
  else
    record FAIL basebackup "the base backup did not start as a cluster (see $logs/pg-$port_b.log)"
  fi
else
  record SKIP basebackup "not selected"
fi

if want clients; then
  record MANUAL clients "Ruby gem and Elixir package: not automated; run the runbook's commands (docs/releasing.md, The upgrade battery, step 6) against $(url "$port" clients), with --keep"
else
  record SKIP clients "not selected"
fi

if want mixed; then
  say "7. two instances of different versions"
  sql "$port" mixed -c "create table m(id bigint primary key, g bigint not null)" -c "create index on m(g)" -c "insert into m select g, g % 5 from generate_series(1,200) g"
  apply_old mixed 'TRANSFORM m_agg FROM m GROUP BY g SELECT g AS g, COUNT(*) AS n'
  run_bg old-mixed "$port" mixed "$old_bin"
  WAIT=120 wait_all "$port" mixed "$old_bin" live m_agg || true
  pre_mixed_history=$(history "$port" mixed)
  run_bg new-mixed "$port" mixed "$candidate_bin"
  sleep 15
  workers=$(scalar "$port" mixed "select count(*) from trellis.worker_registry where last_seen > now() - interval '30 seconds'")
  paused_n=$(scalar "$port" mixed "select count(*) from trellis.capture_failures")
  # ADR-0023: the candidate's `trellis run` stands by, with this message, and
  # runs no migration while the old instance is live.
  standby_msg='trellis standing by: migrations are pending and Trellis processes are live on this instance'
  if run_alive new-mixed && grep -qF "$standby_msg" "$logs/run-new-mixed.log" &&
     [ "$(history "$port" mixed)" = "$pre_mixed_history" ] && [ "$paused_n" -eq 0 ]; then
    record PASS mixed "the candidate stands by beside a live $old_tag instance: no migration ran, nothing paused ($(grep -F "$standby_msg" "$logs/run-new-mixed.log" | head -1 | cut -c1-200))"
  else
    record FAIL mixed "the candidate did not stand by beside a live $old_tag instance ($workers workers registered, $paused_n paused, candidate process $(run_alive new-mixed && echo running || echo exited), history $( [ "$(history "$port" mixed)" = "$pre_mixed_history" ] && echo unchanged || echo changed), log: $(tr '\n' ' ' <"$logs/run-new-mixed.log" | cut -c1-300))"
  fi
  # The message must name the candidate's exact version and the old build as a
  # release with no registered version. With migrations pending the candidate
  # stands by beside a live process of any version, its own included, so the
  # standby alone does not show a handover between two versions: a candidate
  # that reports the old build's version string fails here.
  case "$old_tag" in
    v0.1.*)
      if [ "$cand_ver" = "${old_version#trellis }" ]; then
        record FAIL mixed "the candidate reports version $cand_ver, the same string as $old_tag, so this is no handover between two versions: build it as the release version (--candidate-version, default 0.2.0)"
      elif grep -qF "this binary is version $cand_ver;" "$logs/run-new-mixed.log" &&
         grep -qF 'at a release before 0.2.0' "$logs/run-new-mixed.log"; then
        record PASS mixed "the standby message names this binary's version $cand_ver and $old_tag as a release before 0.2.0 (it registers no version)"
      else
        record FAIL mixed "the standby message does not name 'this binary is version $cand_ver;' and 'at a release before 0.2.0': $(head -c 300 "$logs/run-new-mixed.log" | tr '\n' ' ')"
      fi ;;
    *) record INFO mixed "$old_tag may register a version, so the standby message's version names were not asserted" ;;
  esac
  # The old instance keeps the pipeline: the writes made now converge under it.
  sql "$port" mixed -c "insert into m select g, g % 5 from generate_series(201,400) g"
  # Stopping the old instance hands over: the candidate migrates and takes over.
  stop_run old-mixed
  taken=0
  for _ in $(seq 1 120); do
    if [ "$(history "$port" mixed | wc -l)" -gt "$(wc -l <<<"$pre_mixed_history")" ] &&
       grep -q 'migrated, taking over' "$logs/run-new-mixed.log"; then taken=1; break; fi
    sleep 0.5
  done
  if [ "$taken" -eq 1 ] && run_alive new-mixed; then
    record PASS mixed "once $old_tag stopped, the candidate migrated and took over"
    reg=$(scalar "$port" mixed "select count(*) from trellis.worker_registry where version = '$cand_ver' and last_seen > now() - interval '30 seconds'")
    [ "$reg" -ge 1 ] &&
      record PASS mixed "the candidate's live worker_registry row carries version $cand_ver" ||
      record FAIL mixed "no live worker_registry row carries version $cand_ver (versions: $(scalar "$port" mixed "select coalesce(string_agg(distinct coalesce(version, 'none'), ','), '') from trellis.worker_registry where last_seen > now() - interval '30 seconds'"))"
  else
    record FAIL mixed "the candidate did not take over within 60 s of $old_tag stopping (process $(run_alive new-mixed && echo running || echo exited), log: $(tail -c 300 "$logs/run-new-mixed.log" | tr '\n' ' '))"
  fi
  sql "$port" mixed -c "insert into m select g, g % 5 from generate_series(401,600) g"
  WAIT=120 wait_all "$port" mixed "$candidate_bin" live m_agg || true
  check_self "$port" mixed mixed m_agg
  stop_run new-mixed
else
  record SKIP mixed "not selected"
fi

# ver_cmp <a> <b>: -1, 0 or 1, comparing two major.minor.patch releases.
ver_cmp() {
  awk -v a="$1" -v b="$2" 'BEGIN {
    split(a, x, "."); split(b, y, ".")
    for (i = 1; i <= 3; i++) {
      if (x[i] + 0 < y[i] + 0) { print -1; exit }
      if (x[i] + 0 > y[i] + 0) { print 1; exit }
    }
    print 0 }'
}
# release_of <version>: its major.minor.patch (`trellis 0.3.0-rc.1` is 0.3.0).
release_of() { local v=${1#trellis }; echo "${v%%[-+]*}"; }
# manifest_ids <manifest> <release>: the identity of each line of a
# feature-floor manifest whose release is after <release> (check-floors.py's:
# the line without its release, and an admission without its case).
manifest_ids() {
  local line rel id
  while IFS= read -r line; do
    case "$line" in ''|'#'*) continue ;; esac
    rel=${line##* }; id=${line% *}
    case "$id" in "admit "*) id=${id%% by *} ;; esac
    [ "$(ver_cmp "$rel" "$2")" -le 0 ] || echo "$id"
  done <"$1"
}
not_alive() { ! run_alive "$1"; }
paused_with() { # port db name pattern: stored paused, and capture_failures.error matches
  [ "$(stored_status "$1" "$2" "$3")" = paused ] && failure_reason "$1" "$2" "$3" | grep -qE "$4"
}
# leftovers <port> <db> <id>: rows any trellis table keeps for that transform id,
# plus trellis relations named with it.
leftovers() {
  scalar "$1" "$2" "select coalesce(sum((xpath('/row/c/text()', query_to_xml(format('select count(*) c from trellis.%I where transform_id = %s', c.relname, $3), false, true, '')))[1]::text::bigint), 0)
      + (select count(*) from pg_class where relnamespace = 'trellis'::regnamespace and relname ~ ('_' || $3 || '(_|\$)'))
    from pg_class c join pg_attribute a on a.attrelid = c.oid
   where c.relnamespace = 'trellis'::regnamespace and c.relkind in ('r', 'p') and a.attname = 'transform_id' and not a.attisdropped"
}

# A release before 0.2.0 carries no floor check (ADR-0024 rule 8). It refuses
# the database in `migrate`, which its `run` and `apply` both call first
# (pre_floor_refusal).
rollback_before_floors() {
  local out first
  run_bg down-shapes "$port" shapes "$old_bin"
  if wait_for 60 "$old_tag to exit" not_alive down-shapes; then
    first=$(grep -m1 -iE "$pre_floor_refusal" "$logs/run-down-shapes.log" || true)
    if [ -n "$first" ]; then
      record PASS downgrade "$old_tag run refuses the database the candidate migrated: ${first:0:240}"
    else
      record FAIL downgrade "$old_tag run exited without the migration refusal: $(head -c 240 "$logs/run-down-shapes.log")"
    fi
  else
    record FAIL downgrade "$old_tag run is up on a database the candidate migrated: $(status_lines "$port" shapes "$old_bin" | tr '\n' ' ')"
    stop_run down-shapes
  fi
  if out=$(trellis "$port" shapes "$old_bin" apply 'TRANSFORM rb_probe FROM orders SELECT amount AS amount' 2>&1); then
    record FAIL downgrade "$old_tag apply defined a transform on a database the candidate migrated"
  elif first=$(grep -m1 -iE "$pre_floor_refusal" <<<"$out"); then
    record PASS downgrade "$old_tag apply refuses it too: ${first:0:240}"
  else
    record FAIL downgrade "$old_tag apply failed without the migration refusal: ${out:0:240}"
  fi
  record INFO downgrade "rolling back to $old_tag means restoring a backup taken before the upgrade, which discards the application's writes since: $old_tag carries no floors (ADR-0024 rule 8, docs/upgrading.md); the release notes say so"
}

# From 0.2.0 on: the candidate defines every shape and the candidate-only
# definitions, with writes; the old release then runs on that database.
rollback_with_floors() {
  local db=rollback n e rest name text floor id out first missing covered stmt
  local cand_only="" shared="orders_copy chain1 orders_wide cust_totals cust_join cust_join_left tenant_totals tenant_join"
  sql "$port" postgres -c "create database $db"
  sql "$port" "$db" <<<"$shapes_schema"
  seed_rows=$((seed_rows + 3150))
  # Every manifest line after the old release is covered by a definition.
  if [ -f "$cand_src/.github/feature-floors.txt" ]; then
    covered=$(for e in ${rollback_new_defs[@]+"${rollback_new_defs[@]}"}; do echo "${e%%|*}" | tr ';' '\n'; done)
    missing=$(manifest_ids "$cand_src/.github/feature-floors.txt" "$old_rel" | grep -vxF -f <(echo "${covered:-<none>}") || true)
    if [ -n "$missing" ]; then
      record FAIL downgrade "candidate-only features no rollback definition uses (add each to rollback_new_defs): $(echo "$missing" | head -5 | tr '\n' ';')"
    else
      record PASS downgrade "every feature after $old_rel in the candidate's feature-floor manifest has a rollback definition"
    fi
  else
    record FAIL downgrade "the candidate has no .github/feature-floors.txt"
  fi
  apply_new() { trellis "$port" "$db" "$candidate_bin" apply "$1" >>"$logs/apply-rollback.log" 2>&1 || { record FAIL downgrade "the candidate refused: $1"; return 1; }; }
  apply_new 'TRANSFORM orders_copy FROM orders SELECT amount AS amount, amount + 1 AS plus' || return 0
  apply_new 'TRANSFORM cust_totals FROM orders GROUP BY customer_id SELECT customer_id AS customer_id, COUNT(*) AS n, SUM(amount) AS total' || return 0
  apply_new 'TRANSFORM cust_join FROM orders JOIN customers ON orders.customer_id = customers.id GROUP BY customers.region SELECT customers.region AS region, COUNT(*) AS n, SUM(orders.amount) AS total' || return 0
  apply_new 'TRANSFORM cust_join_left FROM orders LEFT JOIN customers ON orders.customer_id = customers.id GROUP BY customers.region SELECT customers.region AS region, COUNT(*) AS n' || return 0
  apply_new 'TRANSFORM tenant_totals FROM events GROUP BY tenant SELECT tenant AS tenant, COUNT(*) AS n, SUM(v) AS s' || return 0
  apply_new 'TRANSFORM tenant_join FROM events JOIN customers ON events.tenant = customers.id GROUP BY events.tenant SELECT events.tenant AS tenant, COUNT(*) AS n' || return 0
  apply_new 'TRANSFORM orders_wide FROM orders SELECT amount AS amount, amount + 2 AS plus2, amount + 3 AS plus3' || return 0
  for e in ${rollback_new_defs[@]+"${rollback_new_defs[@]}"}; do
    rest=${e#*|}; name=${rest%%|*}; text=${rest#*|}
    apply_new "$text" || return 0
    cand_only="$cand_only $name"
  done
  run_bg cand-rb "$port" "$db" "$candidate_bin"
  WAIT=240 wait_all "$port" "$db" "$candidate_bin" live orders_copy || { record FAIL downgrade "orders_copy did not go live under the candidate"; return 0; }
  apply_new 'TRANSFORM chain1 FROM orders_copy SELECT plus AS plus_again' || return 0
  # shellcheck disable=SC2086
  if ! WAIT=240 wait_all "$port" "$db" "$candidate_bin" live $shared $cand_only; then
    record FAIL downgrade "under the candidate, not live:$(missing_status "$port" "$db" "$candidate_bin" live $shared $cand_only)"
    return 0
  fi
  start_writers "$port" "$db"
  sleep 5
  for n in $cand_only; do
    floor=$(scalar "$port" "$db" "select floor_version from trellis.transform_definitions where target_table = 'public.$n'")
    if [ "$(ver_cmp "$floor" "$old_rel")" -gt 0 ]; then
      record PASS downgrade "$n has floor $floor under the candidate, after $old_rel"
    else
      record FAIL downgrade "$n has floor $floor, which $old_tag meets: its rollback_new_defs entry uses no candidate-only feature"
    fi
  done
  floor=$(scalar "$port" "$db" "select version from trellis.catalog_floor")
  stop_run cand-rb

  if [ "$(ver_cmp "$floor" "$old_rel")" -gt 0 ]; then
    # The candidate raised the catalog floor: every handle of the old release
    # refuses with the floor error (ADR-0024 rule 2); `run` stands by.
    run_bg old-rb "$port" "$db" "$old_bin"
    sleep 15
    if run_alive old-rb && grep -q "the catalog floor is $floor" "$logs/run-old-rb.log"; then
      record PASS downgrade "$old_tag run stands by below the catalog floor $floor, naming it"
    else
      record FAIL downgrade "$old_tag run below the catalog floor $floor: $(head -c 240 "$logs/run-old-rb.log")"
    fi
    stop_run old-rb
    if out=$(trellis "$port" "$db" "$old_bin" apply 'PAUSE TRANSFORM orders_copy' 2>&1); then
      record FAIL downgrade "$old_tag apply changed a catalog above its floor"
    elif grep -q "the catalog floor is $floor" <<<"$out"; then
      record PASS downgrade "$old_tag apply refuses with the floor error"
    else
      record FAIL downgrade "$old_tag apply failed without the floor error: ${out:0:240}"
    fi
    out=$(trellis "$port" "$db" "$old_bin" status 2>&1 || true)
    if grep -q "$floor" <<<"$out"; then
      record PASS downgrade "$old_tag status reads the catalog and names the floor"
    else
      record FAIL downgrade "$old_tag status does not name the catalog floor $floor: ${out:0:240}"
    fi
    stop_writers
    record INFO downgrade "the candidate raised the catalog floor to $floor, above $old_rel: rolling back to $old_tag means restoring a backup taken before the upgrade; the release notes say so"
    return 0
  fi

  # The old release on the candidate's database, the writers still running.
  run_bg old-rb "$port" "$db" "$old_bin"
  for n in $cand_only; do
    floor=$(scalar "$port" "$db" "select floor_version from trellis.transform_definitions where target_table = 'public.$n'")
    if wait_for 120 "$n paused" paused_with "$port" "$db" "$n" "this release can't run transform [^:]*$n: its floor, the oldest Trellis release that can run it, is $floor, and this binary runs release $old_rel"; then
      record PASS downgrade "$old_tag pauses $n with the documented reason"
    else
      record FAIL downgrade "$old_tag: $n is $(stored_status "$port" "$db" "$n"), reason: $(failure_reason "$port" "$db" "$n" | head -c 200)"
    fi
  done
  # shellcheck disable=SC2086
  WAIT=240 wait_all "$port" "$db" "$old_bin" live $shared || true
  stop_writers
  sleep 5
  # shellcheck disable=SC2086
  missing=$(missing_status "$port" "$db" "$old_bin" live $shared)
  if [ -z "$missing" ]; then
    record PASS downgrade "$old_tag runs every other definition: all live"
    # shellcheck disable=SC2086
    PROBE=$probe_old check_self "$port" "$db" downgrade $shared
  else
    record FAIL downgrade "under $old_tag, not live:$missing"
  fi
  for n in $cand_only; do
    floor=$(scalar "$port" "$db" "select floor_version from trellis.transform_definitions where target_table = 'public.$n'")
    for stmt in "RESUME TRANSFORM $n" "DROP TRANSFORM $n"; do
      if out=$(trellis "$port" "$db" "$old_bin" apply "$stmt" 2>&1); then
        record FAIL downgrade "$old_tag accepted $stmt above its floor"
      elif grep -q "its floor is $floor" <<<"$out" && [ "$(stored_status "$port" "$db" "$n")" = paused ] &&
        [ -n "$(scalar "$port" "$db" "select to_regclass('public.$n')")" ]; then
        record PASS downgrade "$old_tag refuses $stmt, naming the floor, and changes nothing"
      else
        record FAIL downgrade "$old_tag $stmt: ${out:0:200}"
      fi
    done
  done
  if run_alive old-rb; then record PASS downgrade "$old_tag stayed up throughout"
  else record FAIL downgrade "$old_tag exited: $(tail -c 240 "$logs/run-old-rb.log")"; fi
  stop_run old-rb

  # Roll forward: the paused definitions stay paused until RESUME.
  run_bg cand-rb2 "$port" "$db" "$candidate_bin"
  sleep 15
  # shellcheck disable=SC2086
  WAIT=180 wait_all "$port" "$db" "$candidate_bin" live $shared || true
  for n in $cand_only; do
    if [ "$(stored_status "$port" "$db" "$n")" = paused ]; then
      record PASS downgrade "rolled forward, $n stays paused until RESUME"
    else
      record FAIL downgrade "rolled forward, $n is $(stored_status "$port" "$db" "$n") without a RESUME"
    fi
  done
  # The first candidate-only definition is dropped there, the rest resumed.
  first=""; local resumed=""
  for n in $cand_only; do
    if [ -z "$first" ]; then
      first=$n
      id=$(scalar "$port" "$db" "select id from trellis.transform_definitions where target_table = 'public.$n'")
      if trellis "$port" "$db" "$candidate_bin" apply "DROP TRANSFORM $n" >>"$logs/apply-rollback.log" 2>&1 &&
        [ -z "$(scalar "$port" "$db" "select to_regclass('public.$n')")" ] && [ "$(leftovers "$port" "$db" "$id")" = 0 ]; then
        record PASS downgrade "rolled forward, DROP TRANSFORM $n removes its target, catalog rows and hidden tables"
      else
        record FAIL downgrade "rolled forward, DROP TRANSFORM $n left state behind (see $logs/apply-rollback.log; leftovers $(leftovers "$port" "$db" "$id"))"
      fi
    else
      trellis "$port" "$db" "$candidate_bin" apply "RESUME TRANSFORM $n" >>"$logs/apply-rollback.log" 2>&1 || record FAIL downgrade "RESUME TRANSFORM $n refused"
      resumed="$resumed $n"
    fi
  done
  # shellcheck disable=SC2086
  if WAIT=240 wait_all "$port" "$db" "$candidate_bin" live $shared $resumed; then
    record PASS downgrade "rolled forward, every definition converges${resumed:+ (resumed:$resumed)}"
    # shellcheck disable=SC2086
    check_self "$port" "$db" downgrade $shared $resumed
  else
    record FAIL downgrade "rolled forward, not live:$(missing_status "$port" "$db" "$candidate_bin" live $shared $resumed)"
  fi
  [ -n "$cand_only" ] || record INFO downgrade "the candidate adds no feature after $old_rel: $old_tag runs every definition it can define"
  stop_run cand-rb2
}

if want downgrade; then
  say "8. rollback: $old_tag against a database the candidate wrote"
  stop_run new-shapes; stop_run new-inflight
  sleep 1
  old_rel=$(release_of "$old_version")
  if [ "$(ver_cmp "$old_rel" 0.2.0)" -lt 0 ]; then
    rollback_before_floors
  else
    rollback_with_floors
  fi
else
  record SKIP downgrade "not selected"
fi

# ---------------------------------------------------------------- summary
stop_run new-shapes 2>/dev/null || true; stop_run new-inflight 2>/dev/null || true
say "summary"
echo "old:        $old_version ($old_bin)"
echo "candidate:  $cand_version at $cand_rev$cand_dirty ($cand_built)"
echo "postgres:   $("$pg_bin/postgres" --version 2>/dev/null || echo "server_version_num $pg_major")"
est_rows=$((seed_rows + writer_iters * 80))
echo "rows written (estimate, for the logbook): $est_rows ($seed_rows seeded, $writer_iters writer iterations of about 80)"
npass=0; nfail=0; ninfo=0; nman=0; nskip=0
for r in "${results[@]}"; do
  case "${r%%|*}" in PASS) npass=$((npass+1));; FAIL) nfail=$((nfail+1));; INFO) ninfo=$((ninfo+1));; MANUAL) nman=$((nman+1));; SKIP) nskip=$((nskip+1));; esac
done
echo "PASS=$npass FAIL=$nfail INFO=$ninfo MANUAL=$nman SKIP=$nskip"
if [ "$keep" -eq 1 ]; then
  echo "kept: the cluster on port $port ($work/cluster-a/data; databases shapes, inflight, ctl, mixed, clients, loaded, restored) and, if the basebackup step ran, the clone on port '$port_b' stay running; stop with $pg_bin/pg_ctl -D $work/cluster-a/data stop"
fi
[ "$nfail" -eq 0 ] || exit 1
