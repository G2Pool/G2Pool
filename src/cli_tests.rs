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

#[test]
fn explicit_pool_path_is_a_startup_input() {
    let dir = tempfile::tempdir().unwrap();
    let matches = command()
        .try_get_matches_from([
            "g2pool",
            "--data-dir",
            dir.path().to_str().unwrap(),
            "--pool",
            "private.json",
        ])
        .unwrap();
    let config = config(&matches).unwrap();
    assert_eq!(
        config.pool_file.as_deref(),
        Some(std::path::Path::new("private.json"))
    );
    assert!(!dir.path().join("pool.json").exists());
}
