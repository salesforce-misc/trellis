//! Live proof for issue #246: the walsender now renders CDC text under the
//! same output GUCs the pool does, on a server whose GUCs have been
//! customized away from stock defaults.
//!
//! # The hazard this closes
//!
//! `pool::DETERMINISTIC_TEXT_OUTPUT_GUCS` pins `DateStyle`/`bytea_output`/
//! `extra_float_digits`/`IntervalStyle`/`TimeZone` on every pooled
//! connection, via `pool::session_bootstrap`. Before issue #246,
//! `pgwire_replication::ReplicationConfig` (v0.4) had no way to send a
//! startup `options` parameter at all, so the **walsender** — the backend
//! that actually renders a changed row's text during logical decoding —
//! kept whatever GUCs the server, database or role had configured, with no
//! way for Trellis to override them. On a stock, unconfigured server the two
//! renderers happened to agree (see `pool::DETERMINISTIC_TEXT_OUTPUT_GUCS`'s
//! doc comment for exactly why), but a real operator running
//! `ALTER DATABASE ... SET datestyle = ...` (the issue's own repro) would
//! silently diverge them: the same source row would render one way when
//! backfilled/read through the pool and a *different* way when decoded off
//! the replication stream, corrupting anything using raw-text key matching
//! or the generative suite's byte-exact cross-check (ADR-0013) with no
//! error, ever.
//!
//! This test reproduces exactly that hostile configuration — one GUC per
//! pin in `DETERMINISTIC_TEXT_OUTPUT_GUCS`, each set away from the value
//! Trellis pins — at the **database** level (`ALTER DATABASE ... SET`,
//! which is not scoped to a session, and therefore applies just as much to
//! the walsender's own backend as to the pool's), then proves the walsender
//! decodes a changed row's text **identically** to how a pool connection
//! reads the same row. `TimeZone` is the headline new pin: it is the GUC
//! issue #113 investigated and declined to pin specifically *because* the
//! walsender couldn't be pinned at the time (see `trellis::temporal`'s
//! module doc, and `docs/type-support.md`) — this test's `tstz` column is
//! the direct, live closure of that finding.
//!
//! # Why the comparison is meaningful
//!
//! The staged `new_image` (queried straight out of `seg_0`, a `jsonb`
//! column) holds exactly what `intake::tuple_to_json` built from the
//! walsender's own decoded `pgoutput` bytes, with no re-rendering on the way
//! in: `new_image ->> 'col'` reads a `jsonb` object's string value back
//! verbatim, so it does not introduce a *third* renderer into the
//! comparison. The pool side reads the same physical row's columns with a
//! plain `::text` cast on a freshly built [`Pool`] (not `db.pool`, which
//! deadpool may have already connected before the hostile `ALTER DATABASE`
//! took effect — the same caution `defs_typed_literals.rs`'s
//! `a_hostile_database_level_output_guc_does_not_change_what_the_engine_reads`
//! takes, for the same reason).

use std::time::{Duration, Instant};

use testkit::TestCluster;
use tokio_postgres::{Client, NoTls};
use trellis::Pool;
use trellis::config::{Config, DEFAULT_SCHEMA};
use trellis::intake;
use trellis::staging::StagedWatermark;

/// Connects directly to `dsn` and pins only `search_path` — deliberately
/// **not** the output GUCs `DETERMINISTIC_TEXT_OUTPUT_GUCS` pins, since this
/// helper is used to apply the hostile `ALTER DATABASE` itself, seed
/// `replication_progress`, and observe the staged rows, none of which
/// should be rendered through Trellis's own pinning.
async fn connect_raw(dsn: &str) -> Client {
    let (client, connection) = tokio_postgres::connect(dsn, NoTls).await.expect("connect");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
        .batch_execute(&format!("set search_path to {DEFAULT_SCHEMA}, public"))
        .await
        .expect("set search_path");
    client
}

/// Seeds `slot`'s `replication_progress` row at the slot's own starting
/// position, as `create_slot_and_park_markers` does. A row behind the slot
/// would read as a slot recreated past this instance's confirmed position
/// (issue #406) and fail `Intake::connect`.
async fn seed_progress_at_slot(client: &Client, slot: &str) {
    let seeded = client
        .execute(
            "insert into replication_progress (slot_name, confirmed_lsn) \
             select slot_name, confirmed_flush_lsn from pg_replication_slots \
             where slot_name = $1 and database = current_database()",
            &[&slot],
        )
        .await
        .expect("seed replication_progress at the slot's start");
    assert_eq!(
        seeded, 1,
        "slot {slot} must exist before seeding its progress"
    );
}

/// The exact GUCs `pool::DETERMINISTIC_TEXT_OUTPUT_GUCS` pins, each set here
/// to a value it does *not* pin, one `ALTER DATABASE ... SET <clause>` per
/// entry — chosen so every one of them changes at least one probe column's
/// rendering, which the unpinned-connection control below verifies directly
/// (matching
/// `a_hostile_database_level_output_guc_does_not_change_what_the_engine_reads`'s
/// own "prove the scenario is real" step).
const HOSTILE_DATABASE_GUC_CLAUSES: &[&str] = &[
    "datestyle to 'SQL, MDY'",
    "bytea_output to 'escape'",
    "extra_float_digits to 0",
    "intervalstyle to 'sql_standard'",
    "timezone to 'America/New_York'",
];

#[tokio::test]
async fn walsender_decoded_text_matches_pool_rendered_text_under_hostile_database_gucs() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;

    // Applied at the **database** level and never reverted — `ALTER
    // DATABASE` only affects sessions started afterwards, which is exactly
    // the production hazard: an operator reconfigures the database once,
    // and every connection opened from then on (pool *and* walsender) sees
    // it unless something pins it back. `ALTER DATABASE` takes one `SET`
    // clause per statement, unlike a session's `;`-joined `SET`s.
    {
        let setup = connect_raw(db.dsn()).await;
        for clause in HOSTILE_DATABASE_GUC_CLAUSES {
            setup
                .batch_execute(&format!("alter database \"{}\" set {clause}", db.name()))
                .await
                .unwrap_or_else(|e| panic!("apply hostile database GUC ({clause}): {e}"));
        }
    }

    // Control: an unpinned connection must actually observe the hostile
    // settings, or this test proves nothing.
    {
        let (unpinned, connection) = tokio_postgres::connect(db.dsn(), NoTls)
            .await
            .expect("connect unpinned");
        tokio::spawn(async move {
            let _ = connection.await;
        });
        let row = unpinned
            .query_one(
                "select ('2024-01-02'::date)::text, \
                        ('\\xdeadbeef'::bytea)::text, \
                        (0.1::double precision + 0.2::double precision)::text, \
                        ('1 year 2 mons 3 days 04:05:06'::interval)::text, \
                        ('2024-06-15 12:00:00+00'::timestamptz)::text",
                &[],
            )
            .await
            .expect("render on an unpinned session");
        let (d, b, f, iv, tstz): (String, String, String, String, String) =
            (row.get(0), row.get(1), row.get(2), row.get(3), row.get(4));
        assert_eq!(d, "01/02/2024", "hostile datestyle must be in effect");
        assert!(
            !b.starts_with("\\x"),
            "hostile bytea_output must be in effect, got {b}"
        );
        assert_eq!(f, "0.3", "hostile extra_float_digits must be in effect");
        assert!(
            !iv.contains("mons"),
            "hostile intervalstyle (sql_standard) must be in effect, got {iv}"
        );
        assert_ne!(
            tstz, "2024-06-15 12:00:00+00",
            "hostile timezone must be in effect, got {tstz}"
        );
    }

    let setup = connect_raw(db.dsn()).await;
    setup
        .batch_execute(
            "create table widgets ( \
                 id bigint primary key, \
                 d date, \
                 b bytea, \
                 f double precision, \
                 iv interval, \
                 tstz timestamptz \
             ); \
             create publication intake_pub for table widgets;",
        )
        .await
        .expect("create source table and publication");
    setup
        .query_one(
            "select slot_name from pg_create_logical_replication_slot('intake_slot', 'pgoutput')",
            &[],
        )
        .await
        .expect("create replication slot");
    seed_progress_at_slot(&setup, "intake_slot").await;

    // The change intake should pick up, made *after* the slot exists so it
    // is guaranteed to be in the stream. The GUCs above are all pure
    // *output* settings (`pool::DETERMINISTIC_TEXT_OUTPUT_GUCS`'s doc
    // comment makes the same point), so writing this on the same hostile
    // database does not change what value gets stored — every literal here
    // is unambiguous regardless.
    setup
        .execute(
            "insert into widgets (id, d, b, f, iv, tstz) values \
             (1, '2024-01-02', '\\xdeadbeef', \
              0.1::double precision + 0.2::double precision, \
              '1 year 2 mons 3 days 04:05:06', '2024-06-15 12:00:00+00')",
            &[],
        )
        .await
        .expect("insert source row");

    let config = intake::IntakeConfig {
        dsn: db.dsn().to_string(),
        schema: DEFAULT_SCHEMA.to_string(),
        host: db.socket_dir().display().to_string(),
        port: db.port(),
        user: "postgres".to_string(),
        password: String::new(),
        database: db.name().to_string(),
        slot: "intake_slot".to_string(),
        publication: "intake_pub".to_string(),
        wake_channel: "wake".to_string(),
        spill_threshold: intake::spill::DEFAULT_SPILL_THRESHOLD,
        hard_cap: intake::spill::DEFAULT_HARD_CAP,
        group_commit: None,
    };
    // `Intake::connect` builds a brand-new replication connection every
    // time — see `IntakeConfig::replication_config` — so, unlike the pool
    // below, there is no "already connected before the ALTER" staleness
    // concern to guard against here.
    let mut consumer = intake::Intake::connect(&config, StagedWatermark::new(), db.pool.clone())
        .await
        .expect("connect intake");

    tokio::spawn(async move {
        let _ = consumer.run().await;
    });

    let observer = connect_raw(db.dsn()).await;
    let deadline = Instant::now() + Duration::from_secs(15);
    let staged = loop {
        let row = observer
            .query_opt(
                "select new_image ->> 'd', new_image ->> 'b', new_image ->> 'f', \
                        new_image ->> 'iv', new_image ->> 'tstz' \
                 from seg_0 where src_table = $1",
                &[&format!("{DEFAULT_SCHEMA}.widgets")],
            )
            .await
            .expect("query staged row");
        if let Some(row) = row {
            break row;
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for the change to be staged"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    };
    let (walsender_d, walsender_b, walsender_f, walsender_iv, walsender_tstz): (
        String,
        String,
        String,
        String,
        String,
    ) = (
        staged.get(0),
        staged.get(1),
        staged.get(2),
        staged.get(3),
        staged.get(4),
    );

    // Build a *fresh* pool rather than reusing `db.pool` — `db.pool` was
    // built (and may have already opened connections) before the hostile
    // `ALTER DATABASE` above took effect, and deadpool's
    // `RecyclingMethod::Fast` would let a pre-existing connection keep its
    // old session state indefinitely. See this module's doc comment.
    let config = Config::from_dsn(db.dsn().to_string()).expect("valid dsn");
    let pool = Pool::new(&config).expect("build a pool against the hostile database");
    let pooled = pool.get().await.expect("pooled connection");
    let pool_row = pooled
        .query_one(
            "select d::text, b::text, f::text, iv::text, tstz::text from widgets where id = 1",
            &[],
        )
        .await
        .expect("read the same row through the pool");
    let (pool_d, pool_b, pool_f, pool_iv, pool_tstz): (String, String, String, String, String) = (
        pool_row.get(0),
        pool_row.get(1),
        pool_row.get(2),
        pool_row.get(3),
        pool_row.get(4),
    );

    // The headline assertion: the walsender's own decoded text (staged via
    // `intake::tuple_to_json` from raw `pgoutput` column values, with no
    // re-rendering — see this module's doc comment) agrees byte-for-byte
    // with the pool's `::text` rendering of the identical physical row,
    // despite the whole database being configured hostile to Trellis's own
    // pins.
    assert_eq!(walsender_d, pool_d, "date");
    assert_eq!(walsender_b, pool_b, "bytea");
    assert_eq!(walsender_f, pool_f, "double precision");
    assert_eq!(walsender_iv, pool_iv, "interval");
    assert_eq!(
        walsender_tstz, pool_tstz,
        "timestamptz — the walsender/pool TimeZone gap issue #246 closes"
    );

    // And, since the point of pinning is to match the *canonical* Trellis
    // spelling (not merely "whatever the pool happens to render"), pin the
    // absolute values too — a bug that pinned pool and walsender to the
    // same *wrong* value would pass the comparisons above but not these.
    assert_eq!(pool_d, "2024-01-02");
    assert_eq!(pool_b, "\\xdeadbeef");
    assert_eq!(pool_f, "0.30000000000000004");
    assert_eq!(pool_iv, "1 year 2 mons 3 days 04:05:06");
    assert_eq!(pool_tstz, "2024-06-15 12:00:00+00");
}
