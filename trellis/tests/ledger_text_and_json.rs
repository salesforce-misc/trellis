//! Text `MIN`/`MAX` and `json`/`jsonb` arguments on the ledger (#623 D5).
//!
//! A text entry column takes its source column's collation, so a group's
//! `MIN`/`MAX` recomputed from its entries orders as the build's does over
//! the source. An Apply parses a `json`/`jsonb` column from its image's text
//! rather than populating it, which would read a JSON `null` as SQL `NULL`.
//! Every test ends by checking the target against an oracle over the source.

#[path = "support/drain_driver.rs"]
mod drain_driver;

use drain_driver::Driver;
use trellis::defs::ValueType;
use trellis::defs::pg_type::PgType;

const SRC: &str = "public.src";

const DEFINITION: &str = "TRANSFORM agg FROM public.src GROUP BY g \
     SELECT MIN(c_name) AS c_lo, MAX(c_name) AS c_hi, MIN(label) AS lo, MAX(label) AS hi, \
            COUNT(doc) AS docs, COUNT(raw) AS raws, COUNT(COALESCE(doc, extra)) AS either, \
            COUNT(*) AS n";

const ACTUAL: &str =
    "select g, c_lo, c_hi, lo, hi, docs, raws, either, n from public.agg order by g";

const EXPECTED: &str = "select g, min(c_name), max(c_name), min(label), max(label), \
            count(doc), count(raw), count(coalesce(doc, extra)), count(*) \
     from public.src group by g order by g";

async fn start() -> Driver {
    Driver::start(
        "create table public.src (id integer primary key, g integer, \
             c_name text collate \"C\", label text, doc jsonb, raw json, extra jsonb); \
         insert into public.src values \
             (1, 1, 'a', 'a', '{\"k\": 1}', '{\"k\":  1}', null), \
             (2, 1, 'B', 'B', 'null', 'null', null), \
             (3, 2, 'z', 'z', null, null, '[]');",
        &[
            ("id", ValueType::Numeric),
            ("g", ValueType::Numeric),
            ("c_name", ValueType::Text),
            ("label", ValueType::Text),
            ("doc", ValueType::Other(PgType::Jsonb)),
            ("raw", ValueType::Other(PgType::Json)),
            ("extra", ValueType::Other(PgType::Jsonb)),
        ],
        &[DEFINITION],
        &[SRC],
    )
    .await
}

/// The target equals the oracle, and each live entry holds the JSON its row
/// does, spelled as the source spells it (a `json` column keeps its text).
async fn assert_oracle(driver: &Driver) {
    assert_eq!(driver.rows(ACTUAL).await, driver.rows(EXPECTED).await);
    assert_eq!(
        driver
            .rows(
                "select count(*) from public.agg__ledger l \
                 join public.src s on l.__from_key = s.id::text \
                 where l.__member and (l.__arg2::text is distinct from s.doc::text \
                     or l.__arg3::text is distinct from s.raw::text \
                     or l.__arg4::text is distinct from coalesce(s.doc, s.extra)::text)"
            )
            .await,
        vec!["(0)".to_string()],
        "an entry's JSON differs from its row's"
    );
}

#[tokio::test]
async fn text_min_max_and_json_counts_apply_on_the_ledger_like_the_build() {
    let mut driver = start().await;
    // The target is on the ledger: one entry per source row.
    assert_eq!(
        driver.rows("select count(*) from public.agg__ledger").await,
        vec!["(3)".to_string()]
    );
    assert_oracle(&driver).await;

    let user = driver.user().await;
    user.batch_execute(
        "insert into public.src values \
             (4, 1, 'Z', 'Z', 'null', 'null', null), \
             (5, 2, 'b', 'b', null, '[1]', 'null'); \
         update public.src set doc = null, extra = 'null' where id = 1; \
         update public.src set c_name = 'A', label = 'A' where id = 3;",
    )
    .await
    .expect("apply writes");
    driver.settle().await;
    assert_oracle(&driver).await;

    // Deleting each group's extremum makes the recompute read the next one,
    // in the column's own collation.
    user.batch_execute("delete from public.src where id in (2, 3)")
        .await
        .expect("delete the extrema");
    driver.settle().await;
    assert_oracle(&driver).await;
}

#[tokio::test]
async fn a_rederive_reads_text_and_json_like_the_build() {
    let mut driver = start().await;
    driver.stage_recomputes(SRC, &["1", "2", "3"]).await;
    driver.settle().await;
    assert_oracle(&driver).await;
}
