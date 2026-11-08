//! Meta-tests of the 1-1 JOIN tier's star tier (#1781).

use generative::generate::join::JoinCase;
use generative::generate::join_chained::chained_case_strategy;
use generative::generate::join_one_to_one::one_to_one_case_strategy;
use proptest::strategy::Strategy;

/// FNV-1a over `bytes`: a hash that never changes across Rust releases.
fn fnv(hash: u64, bytes: &[u8]) -> u64 {
    bytes.iter().fold(hash, |h, b| {
        (h ^ u64::from(*b)).wrapping_mul(0x0100_0000_01b3)
    })
}

/// A digest of what a case runs: its ops, its definitions' text, its plan,
/// and its engine settings.
fn digest(cases: &[JoinCase]) -> u64 {
    let mut hash = 0xcbf2_9ce4_8422_2325;
    for case in cases {
        for def in &case.program.defs {
            hash = fnv(
                hash,
                trellis::dev::defs::render_definition_text(def).as_bytes(),
            );
        }
        let rest = format!(
            "{:?}|{:?}|{:?}|{:?}|{}|{}|{}|{}",
            case.program.ops,
            case.program.def_install_after_op,
            case.plan,
            case.shape,
            case.burst_size,
            case.workers,
            case.seal_interval_ms,
            case.program.tables.len()
        );
        hash = fnv(hash, rest.as_bytes());
    }
    hash
}

fn digests(strategy: &proptest::strategy::BoxedStrategy<JoinCase>) -> Vec<u64> {
    (1..=4)
        .map(|seed| digest(&generative::plant_sweep::seeded_cases(strategy, seed, 12)))
        .collect()
}

#[test]
fn print_digests() {
    eprintln!(
        "one_to_one {:?}",
        digests(&one_to_one_case_strategy().boxed())
    );
    eprintln!("chained {:?}", digests(&chained_case_strategy().boxed()));
}
