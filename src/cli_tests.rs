use super::*;
#[test]
fn global_options_work_on_either_side_of_subcommands() {
    for args in [
        vec![
            "g2pool",
            "--network",
            "testnet",
            "--data-dir",
            "/tmp/g2pool-cli-test",
            "withdraw",
        ],
        vec![
            "g2pool",
            "withdraw",
            "--network",
            "testnet",
            "--data-dir",
            "/tmp/g2pool-cli-test",
        ],
    ] {
        let matches = command().try_get_matches_from(args).unwrap();
        assert_eq!(network(&matches), grin_core::global::ChainTypes::Testnet);
        assert_eq!(base(&matches), PathBuf::from("/tmp/g2pool-cli-test"));
        assert_eq!(matches.subcommand_name(), Some("withdraw"));
    }
    assert!(command()
        .try_get_matches_from(["g2pool", "--scan-wallet-internal"])
        .is_err());
}
