//! The opt-in automatic retry of held keys: `retry_held_keys`, one
//! pass at a time, on the real engine, driven by hand through
//! `support/drain_driver.rs`. The pass takes the time it judges due at, so
//! no test sleeps or polls for convergence.
//!
//! Every test holds key 1 of `public.nums` for `doubles`, as
//! `release_held_key.rs` does: a check constraint on its target refuses the
//! doubled value until the test drops it.

#[path = "support/drain_driver.rs"]
mod drain_driver;

use std::time::{Duration, SystemTime};

use drain_driver::Driver;
use trellis::config::DEFAULT_SCHEMA;
use trellis::defs::ValueType;
use trellis::staging::quarantine::{self, HeldKeyRetry};
use trellis::staging::{DEFAULT_DEATH_THRESHOLD, apply};
use trellis::{Config, Trellis, TrellisOptions};

const NUMS: &str = "public.nums";
const POLICY: HeldKeyRetry = HeldKeyRetry {
    initial_backoff: Duration::from_secs(60),
    max_backoff: Duration::from_secs(300),
    batch: 100,
};
const CAP: usize = 100_000;

async fn start() -> Driver {
    let d = Driver::start(
        "create table public.nums (id integer primary key, x integer); \
         insert into public.nums select g, g from generate_series(1, 3) g;",
        &[("id", ValueType::Numeric), ("x", ValueType::Numeric)],
        &[
            "TRANSFORM doubles FROM public.nums SELECT x + x AS doubled",
            "TRANSFORM copies FROM public.nums SELECT x AS x",
        ],
        &[NUMS],
    )
    .await;
    d.ctl
        .batch_execute("alter table public.doubles add constraint small check (doubled < 100)")
        .await
        .expect("constrain the target");
    d
}

/// Drains `batch` until a page commits, returning how many attempts failed
/// first.
async fn drain_through_evictions(d: &Driver, batch: i64) -> usize {
    let mut failures = 0;
    loop {
        match apply::drain_once(d.pool(), batch, "worker", 1, drain_driver::WAKE).await {
            Ok(Some(_)) => continue,
            Ok(None) => return failures,
            Err(err) => {
                failures += 1;
                assert!(failures <= 20, "the batch never drained: {err}");
            }
        }
    }
}

/// Writes key 1 a value `doubles` refuses and drains it until the key is
/// held for `doubles`.
async fn hold_key_1(d: &mut Driver) {
    d.user()
        .await
        .execute("update public.nums set x = 60 where id = 1", &[])
        .await
        .expect("write nums 1");
    let batch = d.seal().await;
    assert_eq!(
        drain_through_evictions(d, batch).await,
        DEFAULT_DEATH_THRESHOLD as usize - 1
    );
    assert_eq!(held(d).await.len(), 1);
}

async fn held(d: &Driver) -> Vec<String> {
    d.rows("select key from poison order by key").await
}

/// `(poisoned_at, retry_attempts)` of the one held key.
async fn poison_row(d: &Driver) -> (SystemTime, i32) {
    let row = d
        .ctl
        .query_one("select poisoned_at, retry_attempts from poison", &[])
        .await
        .expect("read the held key");
    (row.get(0), row.get(1))
}

async fn retry_at(d: &Driver, now: SystemTime) -> quarantine::RetryPass {
    quarantine::retry_held_keys(d.pool(), &POLICY, CAP, Some(now))
        .await
        .expect("retry pass")
}

#[tokio::test]
async fn a_held_key_whose_cause_was_fixed_is_released_when_due_and_the_target_matches() {
    let mut d = start().await;
    hold_key_1(&mut d).await;
    let (poisoned_at, attempts) = poison_row(&d).await;
    assert_eq!(attempts, 0, "a first poisoning has not been retried");

    d.ctl
        .batch_execute("alter table public.doubles drop constraint small")
        .await
        .expect("fix the cause");
    let pass = retry_at(&d, poisoned_at + POLICY.initial_backoff).await;
    assert_eq!(pass.released, 1);
    assert!(!pass.yielded);
    assert_eq!(held(&d).await, Vec::<String>::new());

    let batch = d.seal().await;
    d.drain(batch, "worker").await;
    assert_eq!(
        d.rows("select id, doubled::numeric from public.doubles order by id")
            .await,
        d.rows("select id, (x * 2)::numeric from public.nums order by id")
            .await,
    );
    assert_eq!(
        d.rows("select count(*) from key_deaths").await,
        vec!["(0)"],
        "a clean apply ends the retry's state"
    );
}

#[tokio::test]
async fn a_key_that_is_still_bad_is_poisoned_on_its_first_failure_and_its_backoff_doubles() {
    let mut d = start().await;
    hold_key_1(&mut d).await;
    let (poisoned_at, _) = poison_row(&d).await;

    assert_eq!(
        retry_at(&d, poisoned_at + POLICY.initial_backoff)
            .await
            .released,
        1
    );
    let batch = d.seal().await;
    assert_eq!(
        drain_through_evictions(&d, batch).await,
        0,
        "the first failing page poisons a retried key and the rest of the batch commits, \
         where a fresh key takes a threshold's worth of failed attempts"
    );
    let (poisoned_again_at, attempts) = poison_row(&d).await;
    assert_eq!(attempts, 1);
    assert!(poisoned_again_at > poisoned_at);

    // Not due after the initial backoff again, due after twice it.
    let one_backoff = poisoned_again_at + POLICY.initial_backoff;
    assert_eq!(retry_at(&d, one_backoff).await.released, 0);
    assert_eq!(held(&d).await.len(), 1);
    assert_eq!(
        retry_at(&d, poisoned_again_at + 2 * POLICY.initial_backoff)
            .await
            .released,
        1
    );

    // 60 s doubled twice is 240 s, not yet due at 239 s; the cap (300 s)
    // bounds the next doubling.
    let batch = d.seal().await;
    assert_eq!(drain_through_evictions(&d, batch).await, 0);
    let (third_at, attempts) = poison_row(&d).await;
    assert_eq!(attempts, 2);
    assert_eq!(
        retry_at(
            &d,
            third_at + 4 * POLICY.initial_backoff - Duration::from_secs(1)
        )
        .await
        .released,
        0
    );
    assert_eq!(
        retry_at(&d, third_at + 4 * POLICY.initial_backoff)
            .await
            .released,
        1
    );
    assert_eq!(POLICY.backoff(2), Duration::from_secs(240), "under the cap");
    assert_eq!(POLICY.backoff(3), POLICY.max_backoff, "capped");
    assert_eq!(POLICY.backoff(200), POLICY.max_backoff, "no overflow");
}

#[tokio::test]
async fn a_key_that_is_not_due_is_not_touched() {
    let mut d = start().await;
    hold_key_1(&mut d).await;
    let (poisoned_at, _) = poison_row(&d).await;

    let pass = retry_at(
        &d,
        poisoned_at + POLICY.initial_backoff - Duration::from_secs(1),
    )
    .await;
    assert_eq!(pass.released, 0);
    assert_eq!(held(&d).await.len(), 1);
    assert_eq!(
        d.rows("select count(*) from key_deaths where retry_attempts > 0")
            .await,
        vec!["(0)"]
    );
    assert_eq!(poison_row(&d).await, (poisoned_at, 0));
}

#[tokio::test]
async fn an_operators_release_clears_the_retry_state() {
    let mut d = start().await;
    hold_key_1(&mut d).await;
    let (poisoned_at, _) = poison_row(&d).await;
    retry_at(&d, poisoned_at + POLICY.initial_backoff).await;
    assert_eq!(
        d.rows("select retry_attempts from key_deaths").await,
        vec!["(1)"],
        "the retry carries its count through the release"
    );
    let batch = d.seal().await;
    assert_eq!(drain_through_evictions(&d, batch).await, 0);
    assert_eq!(poison_row(&d).await.1, 1);

    quarantine::release_key(d.pool(), "doubles", NUMS, "1")
        .await
        .expect("operator release");
    assert_eq!(
        d.rows("select count(*) from key_deaths").await,
        vec!["(0)"],
        "an operator's release forgets the retries"
    );
    // Held again by the full threshold of failures, with a fresh count.
    let batch = d.seal().await;
    assert_eq!(
        drain_through_evictions(&d, batch).await,
        DEFAULT_DEATH_THRESHOLD as usize - 1
    );
    assert_eq!(poison_row(&d).await.1, 0);
}

#[tokio::test]
async fn a_pass_yields_to_a_drain_with_a_backlog() {
    let mut d = start().await;
    hold_key_1(&mut d).await;
    let (poisoned_at, _) = poison_row(&d).await;

    // Three sealed, undrained rows are past twice a batch cap of 1.
    d.user()
        .await
        .batch_execute("update public.nums set x = x + 1")
        .await
        .expect("write");
    d.seal().await;
    let due = poisoned_at + POLICY.initial_backoff;
    let pass = quarantine::retry_held_keys(d.pool(), &POLICY, 1, Some(due))
        .await
        .expect("pass");
    assert!(pass.yielded);
    assert_eq!(pass.released, 0);
    assert_eq!(held(&d).await.len(), 1, "the key stays held");

    let pass = quarantine::retry_held_keys(d.pool(), &POLICY, CAP, Some(due))
        .await
        .expect("pass");
    assert!(!pass.yielded);
    assert_eq!(pass.released, 1);
}

#[tokio::test]
async fn the_option_is_off_by_default_and_status_reports_the_schedule_when_on() {
    assert_eq!(TrellisOptions::default().held_key_retry, None);
    assert_eq!(trellis::ClientOptions::default().held_key_retry, None);

    let mut d = start().await;
    hold_key_1(&mut d).await;
    let (poisoned_at, _) = poison_row(&d).await;
    let config = Config::with_schema(d.db.dsn().to_string(), DEFAULT_SCHEMA).expect("config");

    let off = Trellis::connect(config.clone(), TrellisOptions::default())
        .await
        .expect("connect");
    let held_keys = off
        .status("doubles")
        .await
        .expect("status")
        .unwrap()
        .held_keys
        .expect("held");
    assert_eq!(held_keys.next_retry_at, None);
    assert_eq!(held_keys.max_retry_attempts, 0);
    let sample = off.sample_quarantined("doubles", None, 10).await.unwrap();
    assert_eq!(sample[0].next_retry_at, None);

    let on = Trellis::connect(
        config,
        TrellisOptions {
            held_key_retry: Some(POLICY),
            ..Default::default()
        },
    )
    .await
    .expect("connect");
    let held_keys = on
        .status("doubles")
        .await
        .expect("status")
        .unwrap()
        .held_keys
        .expect("held");
    assert_eq!(
        held_keys.next_retry_at,
        Some(poisoned_at + POLICY.initial_backoff)
    );
    let sample = on.sample_quarantined("doubles", None, 10).await.unwrap();
    assert_eq!(sample[0].retry_attempts, 0);
    assert_eq!(
        sample[0].next_retry_at,
        Some(poisoned_at + POLICY.initial_backoff)
    );
}

#[tokio::test]
async fn a_key_poisoned_again_since_the_pass_read_it_is_not_retried_ahead_of_its_backoff() {
    let mut d = start().await;
    hold_key_1(&mut d).await;
    let (first_at, _) = poison_row(&d).await;

    // An operator releases the key and it fails back in: a later
    // `poisoned_at`, so a pass that read the first one (another staging
    // worker's, say) must leave it held.
    quarantine::release_key(d.pool(), "doubles", NUMS, "1")
        .await
        .expect("operator release");
    let batch = d.seal().await;
    drain_through_evictions(&d, batch).await;
    let (again_at, _) = poison_row(&d).await;
    assert!(again_at > first_at);

    let stale = quarantine::retry_held_key(d.pool(), "doubles", NUMS, "1", first_at).await;
    assert!(
        matches!(stale, Err(trellis::staging::ApplyError::KeyNotHeld { .. })),
        "{stale:?}"
    );
    assert_eq!(poison_row(&d).await, (again_at, 0), "still held, untouched");
    assert_eq!(
        d.rows("select count(*) from key_deaths where retry_attempts > 0")
            .await,
        vec!["(0)"]
    );

    quarantine::retry_held_key(d.pool(), "doubles", NUMS, "1", again_at)
        .await
        .expect("the current poisoning is retried");
    assert_eq!(held(&d).await, Vec::<String>::new());
}

#[tokio::test]
async fn a_paused_definitions_keys_stay_held() {
    let mut d = start().await;
    hold_key_1(&mut d).await;
    let (poisoned_at, _) = poison_row(&d).await;
    d.ctl
        .batch_execute(
            "update transform_definitions set status = 'paused' \
             where split_part(target_table, '.', 2) = 'doubles'",
        )
        .await
        .expect("pause");

    let pass = retry_at(&d, poisoned_at + POLICY.max_backoff).await;
    assert_eq!(pass.released, 0);
    assert_eq!(held(&d).await.len(), 1);
}

#[tokio::test]
async fn a_cap_past_what_a_timestamp_holds_does_not_overflow() {
    let policy = HeldKeyRetry {
        initial_backoff: Duration::MAX,
        max_backoff: Duration::MAX,
        batch: 100,
    };
    assert!(
        SystemTime::now()
            .checked_add(policy.backoff(u32::MAX))
            .is_some()
    );

    let mut d = start().await;
    hold_key_1(&mut d).await;
    let pass = quarantine::retry_held_keys(d.pool(), &policy, CAP, None)
        .await
        .expect("the due time stays in range");
    assert_eq!(pass.released, 0);

    let config = Config::with_schema(d.db.dsn().to_string(), DEFAULT_SCHEMA).expect("config");
    let on = Trellis::connect(
        config,
        TrellisOptions {
            held_key_retry: Some(policy),
            ..Default::default()
        },
    )
    .await
    .expect("connect");
    let held_keys = on
        .status("doubles")
        .await
        .expect("status")
        .unwrap()
        .held_keys
        .expect("held");
    assert!(held_keys.next_retry_at.is_some());
    let sample = on.sample_quarantined("doubles", None, 10).await.unwrap();
    assert!(sample[0].next_retry_at.is_some());
}
