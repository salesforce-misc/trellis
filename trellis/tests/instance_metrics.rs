//! Issue #873 (epic #806): `trellis_instance_up` follows each handle's life,
//! per instance, in the one registry the process renders.
//!
//! Two instances share a process and a database, each with its own catalog
//! schema, and each runs a staging worker. The label values are the
//! instances' `<database>/<schema>` names, as in their log lines. Nothing
//! here waits: `connect` returns with the worker started and `shutdown` with
//! it joined, so the series are read at those points.

use testkit::TestCluster;
use trellis::config::DEFAULT_SCHEMA;
use trellis::{Config, Trellis, TrellisOptions};

const INSTANCE_B: &str = "instance_b";

/// `trellis_instance_up` of the instance named `instance`, in the process's
/// rendered body.
fn up(instance: &str) -> Option<u8> {
    let line = format!("trellis_instance_up{{trellis_instance=\"{instance}\"}} ");
    trellis::Metrics::new()
        .render_prometheus()
        .lines()
        .find_map(|rendered| {
            rendered
                .strip_prefix(&line)
                .map(|value| value.parse().unwrap())
        })
}

async fn migrated(config: &Config) {
    let definer = Trellis::connect(config.clone(), TrellisOptions::default())
        .await
        .expect("connect a define-only handle");
    definer.migrate().await.expect("migrate");
    definer
        .shutdown()
        .await
        .expect("shutdown the define-only handle");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn each_instance_reports_up_while_its_handle_runs_and_zero_after() {
    let cluster = TestCluster::start();
    let db = cluster.create_empty_database().await;
    let config_a = Config::with_schema(db.dsn().to_string(), DEFAULT_SCHEMA).expect("schema A");
    let config_b = Config::with_schema(db.dsn().to_string(), INSTANCE_B).expect("schema B");
    let raw = db.pool.get().await.expect("connection");
    let database: String = raw
        .query_one("select current_database()::text", &[])
        .await
        .expect("database name")
        .get(0);
    let name_a = format!("{database}/{DEFAULT_SCHEMA}");
    let name_b = format!("{database}/{INSTANCE_B}");

    migrated(&config_a).await;
    migrated(&config_b).await;
    // The define-only handles are gone: both instances are listed, at 0.
    assert_eq!(up(&name_a), Some(0));
    assert_eq!(up(&name_b), Some(0));

    let staging = TrellisOptions {
        staging: true,
        ..TrellisOptions::default()
    };
    let a = Trellis::connect(config_a, staging.clone())
        .await
        .expect("connect A");
    let b = Trellis::connect(config_b, staging)
        .await
        .expect("connect B");
    assert_eq!(up(&name_a), Some(1));
    assert_eq!(up(&name_b), Some(1));

    b.shutdown().await.expect("shutdown B");
    assert_eq!(up(&name_a), Some(1), "A still runs");
    assert_eq!(up(&name_b), Some(0), "B's series stays, at 0");

    drop(a);
    assert_eq!(up(&name_a), Some(0), "a dropped handle is stopped too");
}

/// The series a running instance's workers record carry its name, not
/// `unknown`: the drain workers run on the client's runtime, whose threads
/// carry the instance, and the staging worker's tick sets its gauge there.
///
/// Waits through `await_converged`, the facade's read-your-writes call, whose
/// timeout is only the bound on a failure.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_running_instances_workers_record_under_its_name() {
    let cluster = TestCluster::start();
    let db = cluster.create_empty_database().await;
    let config = Config::with_schema(db.dsn().to_string(), DEFAULT_SCHEMA).expect("schema");
    let raw = db.pool.get().await.expect("connection");
    raw.batch_execute("create table metric_src (id bigint primary key, price numeric)")
        .await
        .expect("create the source");
    let database: String = raw
        .query_one("select current_database()::text", &[])
        .await
        .expect("database name")
        .get(0);
    let name = format!("{database}/{DEFAULT_SCHEMA}");

    let definer = Trellis::connect(config.clone(), TrellisOptions::default())
        .await
        .expect("connect a define-only handle");
    definer.migrate().await.expect("migrate");
    definer
        .apply("TRANSFORM metric_doubled FROM metric_src SELECT price + price AS total")
        .await
        .expect("define");
    definer.shutdown().await.expect("shutdown the definer");

    let trellis = Trellis::connect(
        config,
        TrellisOptions {
            staging: true,
            drain_threads: 1,
            ..TrellisOptions::default()
        },
    )
    .await
    .expect("connect");
    raw.batch_execute("insert into metric_src values (1, 10)")
        .await
        .expect("insert");
    let token = trellis.watermark_token().await.expect("token");
    trellis
        .await_converged(token, std::time::Duration::from_secs(60))
        .await
        .expect("converge");

    let rendered = trellis.metrics().render_prometheus();
    let applied = rendered
        .lines()
        .filter(|line| line.starts_with("trellis_changes_applied_total{"))
        .filter(|line| line.contains("transform=\"metric_doubled\""))
        .collect::<Vec<_>>();
    assert_eq!(applied.len(), 1, "{rendered}");
    assert!(
        applied[0].contains(&format!("trellis_instance=\"{name}\"")),
        "recorded under another instance: {}",
        applied[0]
    );
    trellis.shutdown().await.expect("shutdown");
}
