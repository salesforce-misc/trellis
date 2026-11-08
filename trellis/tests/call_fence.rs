//! A state-changing call's fence (issue #1637; ADR-0023 rule 6, ADR-0024
//! rule 2): each write transaction of a call takes the handover lock shared
//! as its first lock and checks the catalog floor and for another live
//! version again under it, so a newer release's `migrate` or takeover can't
//! land between the call's pre-check and its writes.
//!
//! The windows are forced with `staging::interleave`'s
//! `BeforeCallFence`/`AfterCallFence` gates, whose target `call-fence-<n>`
//! names the call's `n`th fenced transaction. A `migrate` that raises the
//! floor is stood in for by what it does under the handover lock: a raw
//! session takes the lock exclusive, raises `catalog_floor`, and lets go.
//! A takeover is a hand-stepped `handover_pass_as` of another version. No
//! test sleeps or polls for convergence.

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use testkit::{TestCluster, TestDatabase};
use tokio::task::JoinHandle;
use tokio_postgres::{Client, NoTls};
use trellis::client::handover_pass_as;
use trellis::staging::interleave::{PausePoint, PauseScope, with_scope};
use trellis::{Config, ErrorCode, SelfCheckMode, Trellis, TrellisError, TrellisOptions};

const SCHEMA: &str = trellis::config::DEFAULT_SCHEMA;

/// Another release's version, for a takeover.
const OTHER: &str = "9.9.9-call-fence-test";

/// The floor a newer release's migration raises the catalog to.
const RAISED_FLOOR: &str = "9.9.9";

/// The advisory lock a gated call blocks on.
const GATE: i64 = 1637;

fn config(db: &TestDatabase) -> Config {
    Config::from_dsn(db.dsn().to_string()).expect("config")
}

async fn raw(dsn: &str) -> Client {
    let (client, connection) = tokio_postgres::connect(dsn, NoTls).await.expect("connect");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
        .batch_execute(&format!("set search_path to {SCHEMA}, public"))
        .await
        .expect("search_path");
    client
}

async fn define_only(config: Config) -> Arc<Trellis> {
    Arc::new(
        Trellis::connect(config, TrellisOptions::default())
            .await
            .expect("connect a define-only handle"),
    )
}

fn handover_key() -> i64 {
    trellis::handover::handover_lock_key(SCHEMA)
}

/// A migration that raises the catalog floor, as `migrate` runs it: under
/// the handover lock, taken exclusive.
async fn raise_the_floor(raw: &Client) {
    raw.execute("select pg_advisory_lock($1)", &[&handover_key()])
        .await
        .expect("migrate's handover lock");
    raw.execute("update catalog_floor set version = $1", &[&RAISED_FLOOR])
        .await
        .expect("raise the floor");
    raw.execute("select pg_advisory_unlock($1)", &[&handover_key()])
        .await
        .expect("release the handover lock");
}

/// Whether a session could take the handover lock exclusive now, as
/// `migrate` and every handover pass do: false while a call's fence holds it
/// shared. Lets go of it again at once.
async fn handover_lock_free(raw: &Client) -> bool {
    let free: bool = raw
        .query_one("select pg_try_advisory_lock($1)", &[&handover_key()])
        .await
        .expect("try the handover lock")
        .get(0);
    if free {
        raw.execute("select pg_advisory_unlock($1)", &[&handover_key()])
            .await
            .expect("let go");
    }
    free
}

/// A call parked at a gate inside one of its fenced transactions.
struct Parked<T> {
    gate: Client,
    call: JoinHandle<T>,
}

impl<T> Parked<T> {
    async fn release(self) -> T {
        self.gate
            .execute("select pg_advisory_unlock($1)", &[&GATE])
            .await
            .expect("release the gate");
        self.call.await.expect("the call's task")
    }
}

/// Runs `call` until its `n`th fenced transaction reaches `point`, and
/// leaves it parked there.
async fn park<T: Send + 'static>(
    db: &TestDatabase,
    point: PausePoint,
    n: usize,
    call: impl Future<Output = T> + Send + 'static,
) -> Parked<T> {
    let gate = raw(db.dsn()).await;
    gate.execute("select pg_advisory_lock($1)", &[&GATE])
        .await
        .expect("take the gate");
    let scope = PauseScope::new();
    let reached = scope.arm(point, &format!("call-fence-{n}"), GATE);
    let mut call = tokio::spawn(with_scope(scope, call));
    tokio::select! {
        reached = reached => { reached.expect("pause scope dropped"); }
        _ = &mut call => panic!("the call finished without reaching {point:?} of fence {n}"),
    }
    Parked { gate, call }
}

fn refused_with<T: std::fmt::Debug>(result: Result<T, TrellisError>, code: ErrorCode) {
    let err = result.expect_err("refused");
    assert_eq!(err.code(), code, "{err}");
}

async fn definitions(raw: &Client) -> Vec<String> {
    raw.query(
        "select split_part(target_table, '.', 2) from transform_definitions order by 1",
        &[],
    )
    .await
    .expect("read the definitions")
    .iter()
    .map(|row| row.get(0))
    .collect()
}

async fn status_of(raw: &Client, target: &str) -> String {
    raw.query_one(
        "select status from transform_definitions where split_part(target_table, '.', 2) = $1",
        &[&target],
    )
    .await
    .expect("read the status")
    .get(0)
}

async fn source(raw: &Client) {
    raw.batch_execute(
        "create table public.m (id bigint primary key, g bigint not null, v bigint); \
         create index on public.m (g); \
         insert into public.m select i, i % 3, i from generate_series(1, 20) i",
    )
    .await
    .expect("source");
}

/// Window A: a floor-raising `migrate` commits after a define's pre-check
/// passed and before its transaction's fence. The fence refuses with
/// `version_floor`, and nothing is defined.
#[tokio::test]
async fn a_floor_raised_after_the_pre_check_refuses_the_write() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let raw = raw(db.dsn()).await;
    source(&raw).await;
    let caller = define_only(config(&db)).await;

    let call = {
        let caller = caller.clone();
        async move { caller.apply("TRANSFORM m_copy FROM m SELECT g AS g").await }
    };
    let parked = park(&db, PausePoint::BeforeCallFence, 1, call).await;
    raise_the_floor(&raw).await;
    refused_with(parked.release().await, ErrorCode::VersionFloor);
    assert_eq!(definitions(&raw).await, Vec::<String>::new());
    let target: Option<String> = raw
        .query_one("select to_regclass('public.m_copy')::text", &[])
        .await
        .expect("read")
        .get(0);
    assert_eq!(target, None, "the target's DDL rolled back with the rest");
}

/// Rule 1's other half: a call parked after its fence holds a floor-raising
/// `migrate` back. Released, the call commits; then the migration runs, and
/// the next call refuses with `version_floor` and writes nothing.
#[tokio::test]
async fn a_call_holding_its_fence_holds_migrate_back_until_it_commits() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let raw = raw(db.dsn()).await;
    source(&raw).await;
    let caller = define_only(config(&db)).await;

    let call = {
        let caller = caller.clone();
        async move { caller.apply("TRANSFORM m_copy FROM m SELECT g AS g").await }
    };
    let parked = park(&db, PausePoint::AfterCallFence, 1, call).await;
    assert!(
        !handover_lock_free(&raw).await,
        "migrate waits for the call's transaction"
    );
    parked.release().await.expect("the call commits first");
    raise_the_floor(&raw).await;
    refused_with(
        caller.apply("TRANSFORM m_other FROM m SELECT g AS g").await,
        ErrorCode::VersionFloor,
    );
    assert_eq!(definitions(&raw).await, ["m_copy"]);
}

/// Window B: another version takes over after a `PAUSE`'s pre-check passed
/// and before its transaction's fence. The fence refuses with
/// `version_handover`, and the transform stays as it was.
#[tokio::test]
async fn another_version_taking_over_after_the_pre_check_refuses_the_write() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let raw = raw(db.dsn()).await;
    source(&raw).await;
    let caller = define_only(config(&db)).await;
    caller
        .apply("TRANSFORM m_copy FROM m SELECT g AS g")
        .await
        .expect("define");
    let before = status_of(&raw, "m_copy").await;

    let call = {
        let caller = caller.clone();
        async move { caller.apply("PAUSE TRANSFORM m_copy").await }
    };
    let parked = park(&db, PausePoint::BeforeCallFence, 1, call).await;
    let pool = trellis::pool::Pool::new(&config(&db)).expect("pool");
    assert!(
        handover_pass_as(&pool, &config(&db).with_binary_version(OTHER), "other")
            .await
            .expect("pass"),
        "the other version takes over: no process of this one is live"
    );
    refused_with(parked.release().await, ErrorCode::VersionHandover);
    assert_eq!(status_of(&raw, "m_copy").await, before);
}

/// Rule 6's takeover side: a takeover pass of another version waits behind a
/// call parked after its fence; the call commits, the pass then takes over,
/// and a call started after it refuses with `version_handover`.
#[tokio::test]
async fn a_takeover_pass_waits_for_a_call_holding_its_fence() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let raw = raw(db.dsn()).await;
    source(&raw).await;
    let caller = define_only(config(&db)).await;
    caller
        .apply("TRANSFORM m_copy FROM m SELECT g AS g")
        .await
        .expect("define");

    let call = {
        let caller = caller.clone();
        async move { caller.apply("PAUSE TRANSFORM m_copy").await }
    };
    let parked = park(&db, PausePoint::AfterCallFence, 1, call).await;
    assert!(
        !handover_lock_free(&raw).await,
        "a pass waits for the call's transaction"
    );
    let pass = {
        let db_config = config(&db);
        tokio::spawn(async move {
            let pool = trellis::pool::Pool::new(&db_config).expect("pool");
            handover_pass_as(&pool, &db_config.with_binary_version(OTHER), "other").await
        })
    };
    parked.release().await.expect("the call commits first");
    assert!(
        pass.await.expect("pass task").expect("pass"),
        "then the other version takes over"
    );
    assert_eq!(status_of(&raw, "m_copy").await, "paused");
    refused_with(
        caller.apply("RESUME TRANSFORM m_copy").await,
        ErrorCode::VersionHandover,
    );
    assert_eq!(status_of(&raw, "m_copy").await, "paused");
}

/// Rule 6, a call of several transactions: a column `PAUSE` overtaken by a
/// floor-raising `migrate` between its own pause and its cascade's first
/// pair refuses on that pair. The pause's own row stays committed, marked
/// as owing its cascade, as after a cascade that failed part way; the
/// dependent isn't paused. Retried under the new release, the call
/// finishes the cascade.
#[tokio::test]
async fn a_column_pause_overtaken_between_its_transactions_refuses_on_the_next() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut raw = raw(db.dsn()).await;
    source(&raw).await;
    let caller = define_only(config(&db)).await;
    caller
        .apply("TRANSFORM m_copy FROM m SELECT g AS g, v AS v")
        .await
        .expect("define the upstream");
    bring_live(&mut raw, &db.pool).await;
    caller
        .apply("TRANSFORM m_sum FROM m_copy SELECT v + v AS total")
        .await
        .expect("define its reader");

    let call = {
        let caller = caller.clone();
        async move { caller.apply("PAUSE TRANSFORM m_copy.v").await }
    };
    // Fence 1 is the pause's own transaction; 2 is the cascade's first pair.
    let parked = park(&db, PausePoint::BeforeCallFence, 2, call).await;
    raise_the_floor(&raw).await;
    refused_with(parked.release().await, ErrorCode::VersionFloor);
    assert_eq!(
        column_pause(&raw, "m_copy", "v").await,
        Some(true),
        "the pause's own row committed, still owing its cascade"
    );
    assert_eq!(column_pause(&raw, "m_sum", "total").await, None);

    let newer = define_only(config(&db).with_binary_version(RAISED_FLOOR)).await;
    newer
        .apply("PAUSE TRANSFORM m_copy.v")
        .await
        .expect("a retry under the new release finishes the call");
    assert_eq!(column_pause(&raw, "m_copy", "v").await, Some(false));
    assert_eq!(column_pause(&raw, "m_sum", "total").await, Some(false));
}

/// `cascade_pending` of the column's pause row, `None` when it isn't paused.
async fn column_pause(raw: &Client, transform: &str, column: &str) -> Option<bool> {
    raw.query_opt(
        "select cascade_pending from column_status \
         where transform_table = $1 and column_name = $2",
        &[&transform, &column],
    )
    .await
    .expect("read column_status")
    .map(|row| row.get(0))
}

/// Rule 4: work outside a state-changing call takes no fence. Here a
/// session holds the handover lock exclusive, as `migrate` does for the
/// whole attach (a queued schema dump's restore included), and the engine's
/// define (the restore's) and a background cascade completion run under it
/// without waiting for it.
#[tokio::test]
async fn work_outside_a_call_is_not_fenced() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut raw = raw(db.dsn()).await;
    source(&raw).await;
    let caller = define_only(config(&db)).await;
    caller
        .apply("TRANSFORM m_copy FROM m SELECT g AS g, v AS v")
        .await
        .expect("define the upstream");
    bring_live(&mut raw, &db.pool).await;
    caller
        .apply("TRANSFORM m_sum FROM m_copy SELECT v + v AS total")
        .await
        .expect("define its reader");
    raw.execute(
        "insert into column_status \
             (transform_table, column_name, paused_at, local_fuse, cascade_pending) \
         values ('m_copy', 'v', now(), true, true)",
        &[],
    )
    .await
    .expect("a pause that owes its cascade");

    raw.execute("select pg_advisory_lock($1)", &[&handover_key()])
        .await
        .expect("migrate's handover lock");
    // A fence here would wait out this lock timeout, and fail.
    let impatient =
        Config::from_dsn(format!("{} options='-c lock_timeout=2s'", db.dsn())).expect("config");
    let pool = trellis::pool::Pool::new(&impatient).expect("pool");
    trellis::defs::install_definition(
        &pool,
        "TRANSFORM m_restored FROM m SELECT g AS g",
        &["id", "g", "v"]
            .into_iter()
            .map(|c| {
                (
                    c.to_string(),
                    trellis::defs::ValueType::Integer(trellis::integer::IntWidth::Int8),
                )
            })
            .collect(),
        "public",
    )
    .await
    .expect("a define outside a call takes no fence");
    assert_eq!(
        trellis::staging::quarantine::complete_pause_cascades(&pool)
            .await
            .expect("a background cascade takes no fence"),
        1
    );
    raw.execute("select pg_advisory_unlock($1)", &[&handover_key()])
        .await
        .expect("let go");
    assert_eq!(column_pause(&raw, "m_sum", "total").await, Some(false));
    assert!(definitions(&raw).await.contains(&"m_restored".to_string()));
}

/// The SQL of the probe [`every_write_of_every_state_changing_call_is_fenced`]
/// installs: a statement trigger that refuses a write to the instance's
/// catalog from a session marked `trellis.fence_probe = on` unless its
/// transaction holds the handover lock.
fn probe_function_sql() -> String {
    format!(
        "create function public.fence_probe() returns trigger language plpgsql as $$ \
         begin \
           if current_setting('trellis.fence_probe', true) = 'on' and not exists ( \
               select 1 from pg_catalog.pg_locks \
               where pid = pg_catalog.pg_backend_pid() and locktype = 'advisory' \
                 and granted and objsubid = 1 \
                 and ((classid::int8 << 32) | objid::int8) = {key}) then \
             raise exception 'unfenced write to %.% by a state-changing call', \
               tg_table_schema, tg_table_name; \
           end if; \
           return null; \
         end $$",
        key = handover_key()
    )
}

/// Puts the probe on every table of the instance's catalog that lacks it
/// (a define adds hidden tables).
async fn probe_every_table(raw: &Client) {
    raw.batch_execute(&format!(
        "do $$ declare t regclass; begin \
           for t in select c.oid from pg_catalog.pg_class c \
                    join pg_catalog.pg_namespace n on n.oid = c.relnamespace \
                    where n.nspname = '{SCHEMA}' and c.relkind in ('r', 'p') \
                      and not exists (select 1 from pg_catalog.pg_trigger g \
                                      where g.tgrelid = c.oid and g.tgname = 'fence_probe') \
           loop \
             execute format('create trigger fence_probe after insert or update or delete \
                             or truncate on %s for each statement \
                             execute function public.fence_probe()', t); \
           end loop; \
         end $$"
    ))
    .await
    .expect("install the probe");
}

/// Rules 2 and 3, and the Decision's test (e): every write a state-changing
/// call commits to the instance's catalog, through each entry point and the
/// transactions after its first (a cascade, a resume's pairs and the pause
/// it holds for a frozen reader, an edit's follow-ups, a disruption's pause,
/// a capture's record, a restore's response), is made under the handover
/// lock, which only the call's fence takes in it. The probe refuses any
/// other; and every fence checks that it is its transaction's first lock (in
/// test builds). It sees only the paths the test walks. Setup and the
/// engine's own passes run on unmarked sessions.
#[tokio::test]
async fn every_write_of_every_state_changing_call_is_fenced() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut raw = raw(db.dsn()).await;
    source(&raw).await;
    raw.batch_execute(&probe_function_sql())
        .await
        .expect("probe function");
    let probed = Config::from_dsn(format!("{} options='-c trellis.fence_probe=on'", db.dsn()))
        .expect("config");
    let caller = define_only(probed).await;

    macro_rules! fenced {
        ($what:expr, $call:expr) => {{
            probe_every_table(&raw).await;
            match $call.await {
                Ok(outcome) => outcome,
                Err(err) => panic!("{}: {err}", $what),
            }
        }};
    }

    fenced!(
        "define a 1-1",
        caller.apply("TRANSFORM m_copy FROM m SELECT g AS g, v AS v")
    );
    fenced!(
        "define an aggregate",
        caller.apply("TRANSFORM m_agg FROM m GROUP BY g SELECT g AS g, COUNT(*) AS n")
    );
    bring_live(&mut raw, &db.pool).await;
    fenced!(
        "define a chained reader",
        caller.apply("TRANSFORM m_sum FROM m_copy SELECT v + v AS total")
    );
    bring_live(&mut raw, &db.pool).await;
    fenced!(
        "a column pause and its cascade",
        caller.apply("PAUSE TRANSFORM m_copy.v")
    );
    assert_eq!(column_pause(&raw, "m_sum", "total").await, Some(false));
    fenced!(
        "a whole pause of the cascade's reader",
        caller.apply("PAUSE TRANSFORM m_sum")
    );
    fenced!(
        "a column resume and its pairs, one held as an orphaned pause",
        caller.apply("RESUME TRANSFORM m_copy.v")
    );
    assert_eq!(
        column_pause(&raw, "m_sum", "total").await,
        Some(false),
        "the paused reader's pair is held as a pause of its own"
    );
    fenced!(
        "a whole resume of the reader",
        caller.apply("RESUME TRANSFORM m_sum")
    );
    bring_live(&mut raw, &db.pool).await;
    fenced!(
        "a column resume of the held pair",
        caller.apply("RESUME TRANSFORM m_sum.total")
    );
    assert_eq!(column_pause(&raw, "m_sum", "total").await, None);
    bring_live(&mut raw, &db.pool).await;
    fenced!(
        "an edit that adds a field",
        caller.apply("ALTER TRANSFORM m_copy ADD v + v AS twice")
    );
    bring_live(&mut raw, &db.pool).await;
    fenced!(
        "an edit that drops it",
        caller.apply("ALTER TRANSFORM m_copy DROP twice")
    );
    fenced!("a whole pause", caller.apply("PAUSE TRANSFORM m_agg"));
    fenced!("a whole resume", caller.apply("RESUME TRANSFORM m_agg"));
    bring_live(&mut raw, &db.pool).await;
    fenced!("request_backfill", caller.request_backfill("public.m"));
    bring_live(&mut raw, &db.pool).await;
    fenced!(
        "self_check",
        caller.self_check("m_agg", SelfCheckMode::Standard, Duration::from_secs(1))
    );
    raw.execute(
        "insert into poison (transform_id, src_table, key, last_error) \
         select id, 'public.m', '1', 'test' from transform_definitions \
         where target_table = 'public.m_agg'",
        &[],
    )
    .await
    .expect("hold a key");
    fenced!("release_key", caller.release_key("m_agg", "public.m", "1"));
    // A capture with no record, recorded as it stands.
    raw.execute("delete from capture_triggers", &[])
        .await
        .expect("no records");
    let recorded = fenced!(
        "detect_capture_disruptions recording a capture",
        caller.detect_capture_disruptions()
    );
    assert_eq!(recorded, Vec::<String>::new());
    let records: i64 = raw
        .query_one("select count(*) from capture_triggers", &[])
        .await
        .expect("count the records")
        .get(0);
    assert!(records > 0, "the capture is recorded again");
    raw.batch_execute(
        "alter table public.m disable trigger all; alter table public.m enable trigger all",
    )
    .await
    .expect("a fixture load's disruption");
    let disrupted = fenced!(
        "detect_capture_disruptions",
        caller.detect_capture_disruptions()
    );
    assert_eq!(disrupted, ["public.m"]);
    assert_eq!(status_of(&raw, "m_agg").await, "paused");
    fenced!(
        "a pause before a drop",
        caller.apply("PAUSE TRANSFORM m_sum")
    );
    fenced!("drop", caller.apply("DROP TRANSFORM m_sum"));
    assert_eq!(definitions(&raw).await, ["m_agg", "m_copy"]);
    // A logical restore's response, which pauses every definition.
    raw.execute(
        "update capture_triggers set catalog_oid = (catalog_oid::bigint + 1000)::oid",
        &[],
    )
    .await
    .expect("records from another database");
    let restored = fenced!(
        "detect_capture_disruptions after a restore",
        caller.detect_capture_disruptions()
    );
    assert_eq!(restored, ["public.m"]);

    // The probe catches what it is for: the same write, unfenced.
    let marked = raw_marked(&db).await;
    let err = marked
        .execute("delete from column_status", &[])
        .await
        .expect_err("an unfenced write from a marked session");
    let message = err.as_db_error().map(|db| db.message().to_string());
    assert!(
        message
            .as_deref()
            .is_some_and(|m| m.contains("unfenced write")),
        "{err:?}"
    );
}

async fn raw_marked(db: &TestDatabase) -> Client {
    raw(&format!("{} options='-c trellis.fence_probe=on'", db.dsn())).await
}

/// Capture, builds and drains by hand until every definition is live.
async fn bring_live(raw: &mut Client, pool: &trellis::pool::Pool) {
    use trellis::staging::{apply, has_pending, retire_drained_segments, seal};
    for _ in 0..10 {
        trellis::client::reconcile_pass(raw, pool, SCHEMA)
            .await
            .expect("reconcile pass");
        trellis::staging::build::settle_builds(pool).await;
        for _ in 0..16 {
            let outcome = seal::seal_phase1_waiting(raw).await;
            seal::seal_phase2(raw, outcome.sealed_seg_seq, "call_fence_wake")
                .await
                .expect("seal phase 2");
            while apply::drain_once(
                pool,
                outcome.sealed_seg_seq,
                "call_fence_test",
                1,
                "call_fence_test_wake",
            )
            .await
            .expect("drain_once")
            .is_some()
            {}
            retire_drained_segments(raw)
                .await
                .expect("retire drained segments");
            if !has_pending(raw).await.expect("has_pending") {
                break;
            }
        }
        let all_live: bool = raw
            .query_one(
                "select bool_and(status = 'live') from transform_definitions",
                &[],
            )
            .await
            .expect("read statuses")
            .get(0);
        if all_live {
            return;
        }
    }
    panic!(
        "not every definition went live: {:?}",
        raw.query(
            "select target_table, status from transform_definitions",
            &[]
        )
        .await
        .expect("read")
        .iter()
        .map(|row| (row.get::<_, String>(0), row.get::<_, String>(1)))
        .collect::<Vec<_>>()
    );
}
