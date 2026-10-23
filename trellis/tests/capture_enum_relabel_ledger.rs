//! `capture_enum_relabel.rs`'s `GROUP BY` shape on the ledger path, which
//! takes the group-basis knob off for the whole process (#1241).

#[path = "support/relabel.rs"]
mod relabel;

use relabel::{H, RELABEL};

/// `GROUP BY tone` on the ledger path, with a change staged across the
/// relabel and the resume made before it drains. Its ledger's `GROUP BY`
/// column is typed as the enum and follows the relabel, so it rebuilds in
/// place.
#[tokio::test]
async fn a_ledger_group_by_the_enum_with_a_change_pending_resumes_exactly() {
    trellis::dev::staging::set_group_basis(false);
    let mut h = H::new(
        "create table public.s (id int primary key, tone public.tone, n int); \
         create index on public.s (tone); \
         insert into public.s values (1, 'brisk', 1), (2, 'calm', 2), (3, 'brisk', 3);",
        &["TRANSFORM per_tone FROM public.s GROUP BY tone \
           SELECT tone AS tone, COUNT(*) AS c, SUM(n) AS sn"],
    )
    .await;
    h.write(
        "insert into public.s values (4, 'brisk', 4); \
         update public.s set n = 30 where id = 3; delete from public.s where id = 1",
    )
    .await;
    h.write(RELABEL).await;
    h.capture_pass().await;
    h.assert_paused_by_the_relabel(&["per_tone"]).await;
    h.resume(&["per_tone"]).await;
    h.converge("after resume").await;
    // `self_check` audits no ledger aggregate.
    h.assert_exact_unaudited(
        "per_tone",
        "select tone::text, c, sn from public.per_tone",
        "select tone::text, count(*), sum(n) from public.s group by tone",
    )
    .await;
}
