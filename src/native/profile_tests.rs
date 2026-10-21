use super::*;
#[test]
fn precision_is_the_largest_binary_budget_inside_the_native_reservation() {
    assert_eq!(
        payout_precision(512, consensus::MAX_BLOCK_WEIGHT / 2).unwrap(),
        1024
    );
    assert_eq!(
        payout_precision(8, global::TESTING_MAX_BLOCK_WEIGHT).unwrap(),
        8
    );
    assert!(payout_precision(8, 26).is_err());
    for units in [1u16, 2, 4, 8, 16, 128, 256, 512, 1024, 2048, 4096] {
        let weight = 24 * crate::consensus::output_bound(units, 512) as u64 + 27;
        assert_eq!(payout_precision(512, weight).unwrap(), units);
    }
}

#[test]
fn payout_work_limit_is_twice_the_locally_derived_native_target() {
    global::set_local_chain_type(global::ChainTypes::AutomatedTesting);
    let profile = Profile::new(
        [0; 32],
        global::ChainTypes::AutomatedTesting,
        &crate::pool::Pool::alpha(),
    );
    assert_eq!(profile.payout_work_limit(64).unwrap(), 128);
}

#[test]
fn share_workload_adapts_only_from_authenticated_native_difficulty() {
    global::set_local_chain_type(global::ChainTypes::Mainnet);
    let genesis = hash_bytes(grin_core::genesis::genesis_main().hash());
    let profile = Profile::new(
        genesis,
        global::ChainTypes::Mainnet,
        &crate::pool::Pool::alpha(),
    );
    assert_eq!(profile.assigned(512, 100).unwrap(), 16_384);
    assert_eq!(profile.assigned(8_388_609, 100).unwrap(), 16_385);
    assert_eq!(profile.assigned(16_777_216, 100).unwrap(), 32_768);
    assert!(profile.assigned(0, 100).is_err());
}
#[test]
fn retarget_trims_outliers_and_adapts_to_arrival_rate() {
    global::set_local_chain_type(global::ChainTypes::Mainnet);
    let profile = Profile::new(
        [0; 32],
        global::ChainTypes::Mainnet,
        &crate::pool::Pool::alpha(),
    );
    for (seconds, expected) in [(100, 16_384), (20, 50_000), (2, 500_000)] {
        let mut history: Vec<_> = (0..20)
            .map(|i| (i * seconds, (i as u128 + 1) * 100_000))
            .collect();
        history[0].1 = u128::MAX;
        history[19] = (u64::MAX, u128::MAX);
        assert_eq!(
            profile.retargeted(1_000_000, 100, &history, 10).unwrap(),
            expected
        );
    }
}

#[test]
fn retarget_accepts_nonmonotonic_branch_samples() {
    global::set_local_chain_type(global::ChainTypes::Mainnet);
    let profile = Profile::new(
        [0; 32],
        global::ChainTypes::Mainnet,
        &crate::pool::Pool::alpha(),
    );
    let history = [(1, 100_000u128), (2, 90_000u128), (3, 120_000u128)];
    assert_eq!(
        profile.retargeted(1_000, 100, &history, 10).unwrap(),
        150_000
    );
}

#[test]
fn retarget_checks_multiplication_overflow() {
    global::set_local_chain_type(global::ChainTypes::Mainnet);
    let profile = Profile::new(
        [0; 32],
        global::ChainTypes::Mainnet,
        &crate::pool::Pool::alpha(),
    );
    let history = [(1, 0u128), (2, u128::MAX)];
    assert!(profile.retargeted(1_000, 100, &history, u64::MAX).is_err());
}

#[test]
fn retarget_uses_one_second_floor_for_identical_timestamps() {
    global::set_local_chain_type(global::ChainTypes::Mainnet);
    let profile = Profile::new(
        [0; 32],
        global::ChainTypes::Mainnet,
        &crate::pool::Pool::alpha(),
    );
    let history = [(42, 0u128), (42, 100_000u128)];
    assert_eq!(
        profile.retargeted(1_000, 100, &history, 10).unwrap(),
        1_000_000
    );
}

#[test]
fn fee_units_are_maximal_under_amount_catalogue_and_native_weight() {
    global::set_local_chain_type(global::ChainTypes::Mainnet);
    let profile = Profile::new(
        [1; 32],
        global::ChainTypes::Mainnet,
        &crate::pool::Pool::alpha(),
    );
    let face = 5 * profile.q();
    for receipts in [1, 2, 16, 128, 512] {
        for fees in [
            0,
            face - 1,
            face,
            1_000_000_000,
            60_000_000_000,
            600_000_000_000,
        ] {
            let units = profile.allocation_budget(fees, receipts).unwrap();
            assert!((1024..=2047).contains(&units));
            assert!(u64::from(units) * face <= profile.subsidy + fees - profile.finder_floor);
            assert!(24 * crate::consensus::output_bound(units, receipts) as u64 + 27 <= 20000);
            let next = units + 1;
            assert!(
                next > 2047
                    || u64::from(next) * face > profile.subsidy + fees - profile.finder_floor
                    || 24 * crate::consensus::output_bound(next, receipts) as u64 + 27 > 20000
            );
        }
    }
    assert_eq!(profile.allocation_budget(0, 512).unwrap(), 1024);
    assert!(profile.allocation_budget(face, 512).unwrap() > 1024);
    assert!(profile.allocation_budget(u64::MAX, 512).is_err());
}
