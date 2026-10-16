use super::{check_payment_weight, MAX_NATIVE_FEE};
use grin_core::{
    core::{FeeFields, Transaction},
    global,
};

#[test]
fn payment_weight_counts_receiver_at_native_boundary() {
    for chain in [
        global::ChainTypes::AutomatedTesting,
        global::ChainTypes::Mainnet,
    ] {
        global::set_local_chain_type(chain);
        let limit = global::max_block_weight() / 2;
        let base = Transaction::weight_by_iok(0, 2, 1);
        let input_weight = Transaction::weight_by_iok(1, 2, 1) - base;
        let inputs = (limit - base) / input_weight;
        assert!(Transaction::weight_by_iok(inputs, 2, 1) <= limit);
        assert!(check_payment_weight(inputs, 1).is_ok());
        assert!(Transaction::weight_by_iok(inputs + 1, 2, 1) > limit);
        assert!(check_payment_weight(inputs + 1, 1).is_err());
        assert!(check_payment_weight(u64::MAX, 1).is_err());
        assert!(check_payment_weight(1, u64::MAX).is_err());
    }
}

#[test]
fn reserved_fee_headroom_matches_native_fee_limit() {
    assert_eq!(
        FeeFields::new(0, MAX_NATIVE_FEE).unwrap().fee(),
        MAX_NATIVE_FEE
    );
    assert!(FeeFields::new(0, MAX_NATIVE_FEE + 1).is_err());
}
