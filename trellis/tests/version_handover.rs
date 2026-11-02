//! Version handover (`docs/decisions/0023-version-handover.md`), with the
//! binary version injected (`Config::with_binary_version`) so two versions
//! share one database in one test. Each check is driven directly: a
//! background process's first handover pass runs inside `Trellis::connect`,
//! later passes are stepped with `client::handover_pass_as`, and calls are
//! made after the state they test is set up. Nothing waits for a poll.

use testkit::TestCluster;
use tokio_postgres::Client;
use trellis::client::handover_pass_as;
use trellis::staging::ProducerSession;
use trellis::{Config, ErrorCode, Trellis, TrellisError, TrellisOptions};

const OTHER: &str = "9.9.9-handover-test";

fn config(db: &testkit::TestDatabase) -> Config {
    Config::from_dsn(db.dsn().to_string()).expect("config")
}

async fn raw(db: &testkit::TestDatabase) -> Client {
    let (client, connection) = tokio_postgres::connect(db.dsn(), tokio_postgres::NoTls)
        .await
        .expect("connect");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
        .batch_execute(&format!(
            "set search_path to {}, public",
            trellis::config::DEFAULT_SCHEMA
        ))
        .await
        .expect("search_path");
    client
}

/// `(worker_id, version, standby)` of every registered row, by worker id.
async fn registry(raw: &Client) -> Vec<(String, Option<String>, bool)> {
    raw.query(
        "select worker_id, version, standby from worker_registry order by worker_id",
        &[],
    )
    .await
    .expect("read the registry")
    .iter()
    .map(|row| (row.get(0), row.get(1), row.get(2)))
    .collect()
}

fn refused(result: Result<impl std::fmt::Debug, TrellisError>) -> TrellisError {
    let err = result.expect_err("refused while another version is live");
    assert_eq!(err.code(), ErrorCode::VersionHandover, "{err}");
    err
}

/// Rules 2, 4, 6 and 7 across a cross-version deploy: the new version's
/// background process stands by beside the old one, its calls refuse (from a
/// define-only handle too) while read-only calls and `status` work and say
/// why, the old version's calls go on, and once the old version shuts down
/// the new one takes over and its calls go through.
#[tokio::test]
async fn a_second_version_stands_by_then_takes_over_once_the_first_deregisters() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let raw = raw(&db).await;
    raw.batch_execute("create table public.m (id bigint primary key, g bigint not null); create index on public.m (g)")
        .await
        .expect("source");

    let old = Trellis::connect(
        config(&db),
        TrellisOptions {
            staging: true,
            drain_threads: 1,
            ..Default::default()
        },
    )
    .await
    .expect("connect the old version");
    old.apply("TRANSFORM m_agg FROM m GROUP BY g SELECT g AS g, COUNT(*) AS n")
        .await
        .expect("the old version defines");
    let old_rows = registry(&raw).await;
    assert_eq!(old_rows.len(), 1, "{old_rows:?}");
    let old_worker = old_rows[0].0.clone();

    // The new version's worker: its first pass, in `connect`, stands it by.
    let new_worker = Trellis::connect(
        config(&db).with_binary_version(OTHER),
        TrellisOptions {
            staging: true,
            drain_threads: 1,
            ..Default::default()
        },
    )
    .await
    .expect("a standby connects without error");
    let rows = registry(&raw).await;
    let new_row = rows
        .iter()
        .find(|(worker, _, _)| *worker != old_worker)
        .expect("the new worker registered");
    assert_eq!(new_row.1.as_deref(), Some(OTHER));
    assert!(new_row.2, "the new worker stands by: {rows:?}");
    assert!(
        old.has_live_staging_worker().await.expect("read"),
        "the old version keeps the staging lock"
    );

    // Rule 6: every state-changing call refuses from a handle of the new
    // version, a define-only one included; read-only calls keep working.
    let new_definer = Trellis::connect(
        config(&db).with_binary_version(OTHER),
        TrellisOptions::default(),
    )
    .await
    .expect("connect a define-only handle");
    for handle in [&new_definer, &new_worker] {
        let err = refused(handle.apply("TRANSFORM m_copy FROM m SELECT g AS g").await);
        let text = err.to_string();
        assert!(text.contains(OTHER), "{text}");
        assert!(text.contains(env!("CARGO_PKG_VERSION")), "{text}");
        assert!(text.contains(&old_worker), "{text}");
        assert!(text.contains("30 seconds"), "{text}");
        refused(handle.apply("PAUSE TRANSFORM m_agg").await);
        refused(handle.request_backfill("public.m").await);
        refused(handle.release_key("m_agg", "public.m", "1").await);
        refused(handle.detect_capture_disruptions().await);
        refused(
            handle
                .self_check(
                    "m_agg",
                    trellis::SelfCheckMode::Standard,
                    std::time::Duration::from_secs(1),
                )
                .await,
        );
        assert_eq!(handle.definitions().await.expect("listing").len(), 1);
        let status = handle
            .status("m_agg")
            .await
            .expect("status")
            .expect("defined");
        let seen = status.version_handover.expect("status says why");
        assert_eq!(seen.this_version, OTHER);
        assert_eq!(seen.other_versions(), [env!("CARGO_PKG_VERSION")]);
        assert!(
            seen.processes.iter().any(|p| p.worker_id == old_worker),
            "{seen:?}"
        );
    }
    // The old version's own calls go on: a standby holds nothing back.
    old.apply("PAUSE TRANSFORM m_agg")
        .await
        .expect("the old version still changes state");
    assert_eq!(
        old.status("m_agg")
            .await
            .expect("status")
            .expect("defined")
            .version_handover,
        None
    );

    // The old version stops cleanly: its row goes, and the new version's
    // next pass takes over.
    old.shutdown().await.expect("shutdown the old version");
    let pool = trellis::pool::Pool::new(&config(&db)).expect("pool");
    assert!(
        handover_pass_as(&pool, &config(&db).with_binary_version(OTHER), "next-pass")
            .await
            .expect("pass"),
        "nothing of another version is live any more"
    );
    new_definer
        .apply("RESUME TRANSFORM m_agg")
        .await
        .expect("the new version changes state once it has taken over");
    assert_eq!(
        new_definer
            .status("m_agg")
            .await
            .expect("status")
            .expect("defined")
            .version_handover,
        None
    );

    new_definer.shutdown().await.expect("shutdown");
    new_worker.shutdown().await.expect("shutdown");
}

/// Rule 2's pre-0.2.0 shapes, through the calls: a live registry row with no
/// version (a drain worker of a release before 0.2.0), and the staging lock
/// held by a session no row names (its staging-only leader). Each refuses a
/// state-changing call of this build and stands a background pass by; with
/// both gone, the pass takes over.
#[tokio::test]
async fn the_pre_0_2_0_shapes_hold_this_version_back() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let raw = raw(&db).await;
    let trellis = Trellis::connect(config(&db), TrellisOptions::default())
        .await
        .expect("connect");
    let pool = trellis::pool::Pool::new(&config(&db)).expect("pool");

    raw.execute(
        "insert into worker_registry (worker_id) values ('trellis-old-1-00')",
        &[],
    )
    .await
    .expect("a pre-0.2.0 drain worker's row");
    let err = refused(trellis.request_backfill("public.nothing").await);
    assert!(err.to_string().contains("a release before 0.2.0"), "{err}");
    assert!(
        !handover_pass_as(&pool, &config(&db), "probe")
            .await
            .expect("pass"),
        "a background process stands by"
    );
    raw.execute("delete from worker_registry", &[])
        .await
        .expect("the old drain worker stops");

    let leader = ProducerSession::connect(db.dsn(), trellis::config::DEFAULT_SCHEMA)
        .await
        .expect("a staging leader that registers nothing");
    let err = refused(trellis.apply("PAUSE TRANSFORM nothing").await);
    assert!(err.to_string().contains("registers no version"), "{err}");
    assert!(
        !handover_pass_as(&pool, &config(&db), "probe")
            .await
            .expect("pass")
    );
    leader.release().await.expect("the old leader stops");

    assert!(
        handover_pass_as(&pool, &config(&db), "probe")
            .await
            .expect("pass"),
        "nothing else is live"
    );
    trellis.shutdown().await.expect("shutdown");
}

/// Rule 3: a migration this binary carries is never run while a Trellis
/// process is live; `migrate` refuses instead and runs it once nothing is.
/// The database is put back to before migration V9, with a live row of the
/// release that wrote it.
#[tokio::test]
async fn a_pending_migration_is_not_run_while_a_process_is_live() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let raw = raw(&db).await;
    raw.batch_execute(
        "alter table worker_registry drop column version, drop column staging, \
           drop column drain, drop column standby, drop column staging_pid; \
         delete from refinery_schema_history where version = 9; \
         insert into worker_registry (worker_id) values ('trellis-old-1-00')",
    )
    .await
    .expect("a database at V8 with a live v0.1.0 drain worker");
    let history = async || -> i64 {
        raw.query_one(
            "select count(*) from refinery_schema_history where version = 9",
            &[],
        )
        .await
        .expect("history")
        .get(0)
    };

    let trellis = Trellis::connect(config(&db), TrellisOptions::default())
        .await
        .expect("connect");
    let err = refused(trellis.migrate().await);
    assert!(err.to_string().contains("migrations are pending"), "{err}");
    assert_eq!(history().await, 0, "V9 did not run");
    // A background process waits too, and registers nothing meanwhile.
    let pool = trellis::pool::Pool::new(&config(&db)).expect("pool");
    assert!(
        !handover_pass_as(&pool, &config(&db), "probe")
            .await
            .expect("pass")
    );

    raw.execute("delete from worker_registry", &[])
        .await
        .expect("the old worker stops");
    // Nothing is live, but the migration hasn't run: a background process
    // still waits for it.
    assert!(
        !handover_pass_as(&pool, &config(&db), "probe")
            .await
            .expect("pass")
    );
    trellis
        .migrate()
        .await
        .expect("migrates once nothing is live");
    assert_eq!(history().await, 1, "V9 ran");
    assert!(
        handover_pass_as(&pool, &config(&db), "probe")
            .await
            .expect("pass")
    );
    trellis.shutdown().await.expect("shutdown");
}

/// While another version is live and nothing is pending, `migrate` changes
/// nothing and succeeds, so a deploy's migration step that has nothing to
/// migrate doesn't fail.
#[tokio::test]
async fn migrate_with_nothing_pending_beside_another_version_changes_nothing() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let raw = raw(&db).await;
    raw.execute(
        "insert into worker_registry (worker_id, version) values ('trellis-new-1-00', $1)",
        &[&OTHER],
    )
    .await
    .expect("a live process of another version");
    let trellis = Trellis::connect(config(&db), TrellisOptions::default())
        .await
        .expect("connect");
    trellis
        .migrate()
        .await
        .expect("nothing to migrate and nothing to change");
    refused(trellis.apply("PAUSE TRANSFORM nothing").await);
    trellis.shutdown().await.expect("shutdown");
}
