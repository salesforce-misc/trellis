//! Whole-key attribution's width (#822): which definitions isolation charges
//! a record that fails alone to, when the failure is in shared relationship
//! work or appears only when several definitions apply the record together.
//!
//! Each test stages one change, claims and folds its page, and calls
//! `isolate_and_evict` (or one `drain_once`) once. Faults are injected into
//! one definition's or one relationship's writes: a check constraint, or a
//! trigger pair that refuses two tables written in one transaction. Nothing
//! polls for convergence (#297).

#[path = "support/drain_driver.rs"]
mod drain_driver;

use std::sync::{Arc, LazyLock, Mutex};

use drain_driver::{Driver, WAKE};
use tracing::field::{Field, Visit};
use tracing_subscriber::layer::{Context, Layer, SubscriberExt};
use trellis::config::DEFAULT_SCHEMA;
use trellis::defs::ValueType;
use trellis::staging::quarantine::{DEFAULT_DEATH_THRESHOLD, IsolationOutcome, isolate_and_evict};
use trellis::staging::{BucketFilter, StagedWatermark, apply, claim, fold};

const WORKER: &str = "attribution_test";

/// `pair_guard(mine, other)`: an after-row trigger that marks `mine` written
/// in this transaction and refuses the write if `other` was too, as a user
/// trigger spanning two tables would. A probe's transaction rolls back, and
/// the marks with it.
const PAIR_GUARD: &str = "create function public.pair_guard() returns trigger \
     language plpgsql as $$ \
     begin \
       perform set_config('pair.' || tg_argv[0], 'on', true); \
       if current_setting('pair.' || tg_argv[1], true) = 'on' then \
         raise exception '% and % written together', tg_argv[0], tg_argv[1] \
           using errcode = 'check_violation'; \
       end if; \
       return null; \
     end $$;";

/// Refuses `a` and `b` (qualified tables) written in one transaction.
async fn guard_pair(d: &Driver, a: &str, b: &str) {
    for (table, mine, other) in [(a, a, b), (b, b, a)] {
        d.ctl
            .batch_execute(&format!(
                "create trigger pair_guard after insert or update on {table} \
                 for each row execute function public.pair_guard('{mine}', '{other}')"
            ))
            .await
            .expect("guard a pair of tables");
    }
}

/// The settled projection table of relationship `name`.
async fn projection(d: &Driver, name: &str) -> String {
    let id: i64 = d
        .ctl
        .query_one(
            "select id from relationship_definitions where name = $1",
            &[&name],
        )
        .await
        .expect("read the relationship's id")
        .get(0);
    format!("{DEFAULT_SCHEMA}._trellis_rel_projection_{id}")
}

/// Seals what the test wrote, claims the whole segment as [`WORKER`] and folds
/// it: the page a drain would hold.
async fn claimed_page(d: &mut Driver) -> (i64, Vec<trellis::staging::FoldedChange>) {
    let seg = d.seal().await;
    let mut conn = d.pool().get().await.expect("pool");
    let txn = conn.transaction().await.expect("txn");
    let buckets = claim::claim(&txn, seg, WORKER, 1).await.expect("claim");
    assert!(!buckets.is_empty(), "the segment has buckets to claim");
    let folded = fold(&txn, seg, BucketFilter::all()).await.expect("fold");
    txn.commit().await.expect("commit the claim");
    assert_eq!(folded.len(), 1, "the page holds the one staged change");
    (seg, folded)
}

/// One `isolate_and_evict` call over the page, and the probes it ran (from its
/// end-of-call log line).
async fn isolate(d: &mut Driver) -> (IsolationOutcome, usize) {
    let (seg, folded) = claimed_page(d).await;
    let (_guard, finished) = capture_isolation_finished();
    let outcome = isolate_and_evict(
        d.pool(),
        seg,
        WORKER,
        WAKE,
        &folded,
        DEFAULT_DEATH_THRESHOLD,
        false,
    )
    .await
    .expect("isolation");
    let probes = finished
        .lock()
        .unwrap()
        .expect("isolation logs its end with its probe count");
    (outcome, probes)
}

/// The bare targets `outcome` charged, sorted.
fn charged(outcome: &IsolationOutcome) -> Vec<String> {
    let mut targets: Vec<String> = match outcome {
        IsolationOutcome::ChargedBelowThreshold { charged } => {
            charged.iter().map(|key| key.transform.clone()).collect()
        }
        IsolationOutcome::NothingReproduced => Vec::new(),
        other => panic!("expected a charge below the threshold, or none, got {other:?}"),
    };
    targets.sort();
    targets
}

/// The bare targets with a `key_deaths` row, sorted.
async fn deaths_for(d: &Driver) -> Vec<String> {
    d.ctl
        .query(
            "select split_part(t.target_table, '.', 2) from key_deaths k \
             join transform_definitions t on t.id = k.transform_id order by 1",
            &[],
        )
        .await
        .expect("read key_deaths")
        .iter()
        .map(|row| row.get(0))
        .collect()
}

// ---------------------------------------------------------------------
// A failure in a relationship's reverse work (#822 question 1)
// ---------------------------------------------------------------------

/// `public.par`, read directly by `par_w`, and through two relationships to
/// it: `ra` from `public.sa` (read by `sa_w`) and `rb` from `public.sb` (read
/// by `sb_w`). Row 1 of each, `sa` and `sb` naming `par` 1.
async fn two_relationships() -> Driver {
    Driver::start_with_relationships(
        "create table public.par (id integer primary key, w numeric); \
         create table public.sa (id integer primary key, p integer); \
         create table public.sb (id integer primary key, p integer); \
         insert into public.par values (1, 10); \
         insert into public.sa values (1, 1); \
         insert into public.sb values (1, 1);",
        &[
            ("id", ValueType::Numeric),
            ("w", ValueType::Numeric),
            ("p", ValueType::Numeric),
        ],
        &[
            "RELATIONSHIP ra FROM sa.p TO par.id",
            "RELATIONSHIP rb FROM sb.p TO par.id",
        ],
        &[
            "TRANSFORM sa_w FROM public.sa SELECT ra.w AS pw",
            "TRANSFORM sb_w FROM public.sb SELECT rb.w AS pw",
            "TRANSFORM par_w FROM public.par SELECT w AS w",
        ],
        &["public.par", "public.sa", "public.sb"],
    )
    .await
}

/// `ra`'s settled projection refuses a `w` of 100 or more, so a change to
/// `par` 1's `w` fails in `ra`'s reverse work alone. Only `ra`'s reader is
/// charged: `rb`'s work applies the change, so `sb_w` isn't, and neither is
/// `par_w`, which reads `par` directly.
#[tokio::test]
async fn a_failure_in_one_relationships_work_charges_that_relationships_readers_only() {
    let mut d = two_relationships().await;
    let ra = projection(&d, "ra").await;
    d.ctl
        .batch_execute(&format!(
            "alter table {ra} add constraint cheap check (w < 100); \
             update public.par set w = 500 where id = 1"
        ))
        .await
        .expect("constrain ra's projection and change par 1");

    let (outcome, probes) = isolate(&mut d).await;
    assert_eq!(charged(&outcome), vec!["sa_w".to_string()], "{outcome:?}");
    assert_eq!(deaths_for(&d).await, vec!["sa_w".to_string()]);
    // The record alone, the relationships' share, then `ra`'s and `rb`'s.
    assert_eq!(probes, 4);
}

/// A failure only `ra`'s and `rb`'s work reproduce together (a trigger pair
/// on their projections): neither relationship fails alone, so every reader
/// of both is charged, as before relationships were probed one at a time.
/// `par_w` isn't.
#[tokio::test]
async fn a_failure_only_two_relationships_work_reproduces_together_charges_every_reader() {
    let mut d = two_relationships().await;
    let (ra, rb) = (projection(&d, "ra").await, projection(&d, "rb").await);
    d.ctl.batch_execute(PAIR_GUARD).await.expect("pair_guard");
    guard_pair(&d, &ra, &rb).await;
    d.ctl
        .batch_execute("update public.par set w = 500 where id = 1")
        .await
        .expect("change par 1");

    let (outcome, probes) = isolate(&mut d).await;
    assert_eq!(
        charged(&outcome),
        vec!["sa_w".to_string(), "sb_w".to_string()],
        "{outcome:?}"
    );
    assert_eq!(probes, 4);
}

/// A failure in the re-derive of the children of a `to_col` value the fold
/// erased, which is `ra`'s work alone. `ra` joins `par.code`, which isn't
/// `par`'s key, so the fold carries `code`'s values (`to_col_values`); `rb`
/// joins `par.id`. `par` 1's `code` goes 101 → 102 → 103 in one page, erasing
/// 102, and the ring refuses the recompute staged for `sa` 3, the child of
/// 102. Probing `rb` alone leaves the key out of `ra`'s work whole, the erased
/// value's re-derive with it, so `rb`'s reader isn't charged.
#[tokio::test]
async fn a_failure_in_an_erased_values_rederive_charges_only_that_relationships_readers() {
    let mut d = Driver::start_with_relationships(
        "create table public.par (id integer primary key, code integer unique, w numeric); \
         create table public.sa (id integer primary key, p integer); \
         create table public.sb (id integer primary key, p integer); \
         insert into public.par values (1, 101, 10); \
         insert into public.sa values (1, 101), (3, 102); \
         insert into public.sb values (1, 1);",
        &[
            ("id", ValueType::Numeric),
            ("w", ValueType::Numeric),
            ("p", ValueType::Numeric),
        ],
        &[
            "RELATIONSHIP ra FROM sa.p TO par.code",
            "RELATIONSHIP rb FROM sb.p TO par.id",
        ],
        &[
            "TRANSFORM sa_w FROM public.sa SELECT ra.w AS pw",
            "TRANSFORM sb_w FROM public.sb SELECT rb.w AS pw",
        ],
        &["public.par", "public.sa", "public.sb"],
    )
    .await;
    d.ctl
        .batch_execute(
            "update public.par set code = 102 where id = 1; \
             update public.par set code = 103 where id = 1;",
        )
        .await
        .expect("move par 1's code through 102");
    d.ctl
        .batch_execute(
            "create function public.refuse_sa_3() returns trigger language plpgsql as $$ \
             begin \
               if new.src_table = 'public.sa' and new.key = '3' then \
                 raise exception 'sa 3 refused' using errcode = 'check_violation'; \
               end if; \
               return new; \
             end $$;",
        )
        .await
        .expect("refuse_sa_3");
    for seg in 0..4 {
        d.ctl
            .batch_execute(&format!(
                "create trigger refuse_sa_3 before insert on {DEFAULT_SCHEMA}.seg_{seg} \
                 for each row execute function public.refuse_sa_3()"
            ))
            .await
            .expect("make the ring refuse a recompute of sa 3");
    }

    let (outcome, probes) = isolate(&mut d).await;
    assert_eq!(charged(&outcome), vec!["sa_w".to_string()], "{outcome:?}");
    // The record alone, the relationships' share, then `ra`'s and `rb`'s.
    assert_eq!(probes, 4);
}

// ---------------------------------------------------------------------
// A failure only several direct readers reproduce together (#822 question 2)
// ---------------------------------------------------------------------

/// `public.t`, read directly by one definition per name in `readers`, each
/// copying `v` into a target of that name.
async fn direct_readers(readers: &[&str]) -> Driver {
    let definitions: Vec<String> = readers
        .iter()
        .map(|name| format!("TRANSFORM {name} FROM public.t SELECT v AS v"))
        .collect();
    let definitions: Vec<&str> = definitions.iter().map(String::as_str).collect();
    let d = Driver::start(
        "create table public.t (id integer primary key, v numeric); \
         insert into public.t values (1, 1);",
        &[("id", ValueType::Numeric), ("v", ValueType::Numeric)],
        &definitions,
        &["public.t"],
    )
    .await;
    d.ctl.batch_execute(PAIR_GUARD).await.expect("pair_guard");
    d
}

async fn change_t_1(d: &Driver) {
    d.ctl
        .batch_execute("update public.t set v = 2 where id = 1")
        .await
        .expect("change t 1");
}

/// Three direct readers, and a failure only `a_t`'s and `b_t`'s writes
/// reproduce together. Each applies the record alone, so leave-one-out
/// finds the ones in the failing pair: leaving `a_t` or `b_t` out applies,
/// leaving `c_t` out still fails.
#[tokio::test]
async fn a_failure_two_of_three_readers_reproduce_together_charges_that_pair() {
    let mut d = direct_readers(&["a_t", "b_t", "c_t"]).await;
    guard_pair(&d, "public.a_t", "public.b_t").await;
    change_t_1(&d).await;

    let (outcome, probes) = isolate(&mut d).await;
    assert_eq!(
        charged(&outcome),
        vec!["a_t".to_string(), "b_t".to_string()],
        "{outcome:?}"
    );
    assert_eq!(
        deaths_for(&d).await,
        vec!["a_t".to_string(), "b_t".to_string()]
    );
    // The record alone, each reader alone, then each left out.
    assert_eq!(probes, 1 + 3 + 3);
}

/// Two direct readers in one failing pair: leaving either out is the other's
/// probe alone, which already applied, so both are charged with no probe
/// past those.
#[tokio::test]
async fn a_failure_two_readers_reproduce_together_charges_both_without_more_probes() {
    let mut d = direct_readers(&["a_t", "b_t"]).await;
    guard_pair(&d, "public.a_t", "public.b_t").await;
    change_t_1(&d).await;

    let (outcome, probes) = isolate(&mut d).await;
    assert_eq!(
        charged(&outcome),
        vec!["a_t".to_string(), "b_t".to_string()],
        "{outcome:?}"
    );
    // The record alone, then each reader alone.
    assert_eq!(probes, 1 + 2);
}

/// Two separate failing pairs, `a_t` with `b_t` and `c_t` with `d_t`: leaving
/// any one reader out leaves the other pair failing, so leave-one-out pins
/// nobody. Nobody is charged, and the drain records the page as a holdup.
#[tokio::test]
async fn two_separate_failing_pairs_charge_nobody_and_record_a_drain_holdup() {
    let mut d = direct_readers(&["a_t", "b_t", "c_t", "d_t"]).await;
    guard_pair(&d, "public.a_t", "public.b_t").await;
    guard_pair(&d, "public.c_t", "public.d_t").await;
    change_t_1(&d).await;
    let seg = d.seal().await;

    let err = apply::drain_once(
        d.pool(),
        seg,
        WORKER,
        1,
        WAKE,
        &StagedWatermark::saturated(),
    )
    .await
    .expect_err("the page fails while both pairs apply it");
    assert!(err.to_string().contains("written together"), "{err}");

    assert_eq!(deaths_for(&d).await, Vec::<String>::new(), "nobody charged");
    let held: i64 = d
        .ctl
        .query_one("select count(*) from poison", &[])
        .await
        .expect("count poison")
        .get(0);
    assert_eq!(held, 0);
    let holdups: Vec<i64> = d
        .ctl
        .query("select seg_seq from drain_holdups", &[])
        .await
        .expect("read drain_holdups")
        .iter()
        .map(|row| row.get(0))
        .collect();
    assert_eq!(holdups, vec![seg], "the page is a drain holdup");
}

// ---------------------------------------------------------------------
// Reading isolation's probe count off its end-of-call log line
// ---------------------------------------------------------------------

type Finished = Arc<Mutex<Option<usize>>>;

struct ProbesVisitor(Option<String>, Option<usize>);

impl Visit for ProbesVisitor {
    fn record_u64(&mut self, field: &Field, value: u64) {
        if field.name() == "probes" {
            self.1 = Some(value as usize);
        }
    }

    fn record_i64(&mut self, field: &Field, value: i64) {
        if field.name() == "probes" {
            self.1 = Some(value as usize);
        }
    }

    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        match field.name() {
            "message" => self.0 = Some(format!("{value:?}")),
            "probes" => self.1 = format!("{value:?}").parse().ok(),
            _ => {}
        }
    }
}

struct FinishedLayer(Finished);

impl<S: tracing::Subscriber> Layer<S> for FinishedLayer {
    fn on_event(&self, event: &tracing::Event<'_>, _ctx: Context<'_, S>) {
        let mut visitor = ProbesVisitor(None, None);
        event.record(&mut visitor);
        if visitor
            .0
            .is_some_and(|message| message.starts_with("isolation finished"))
        {
            *self.0.lock().unwrap() = visitor.1;
        }
    }
}

/// Installs a thread-local subscriber that keeps the probe count of the last
/// "isolation finished" line. A permanently live no-op dispatcher keeps
/// `tracing`'s callsite interest computed over every live dispatcher (see
/// `tracing_instrumentation.rs`'s `install_capture`).
fn capture_isolation_finished() -> (tracing::subscriber::DefaultGuard, Finished) {
    static KEEP_INTEREST_GLOBAL: LazyLock<tracing::Dispatch> =
        LazyLock::new(|| tracing::Dispatch::new(tracing::subscriber::NoSubscriber::default()));
    LazyLock::force(&KEEP_INTEREST_GLOBAL);
    let finished = Finished::default();
    let subscriber = tracing_subscriber::registry().with(FinishedLayer(finished.clone()));
    (tracing::subscriber::set_default(subscriber), finished)
}
