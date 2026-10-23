use super::*;
use crate::wallet::payments::tests::Offline;

#[test]
fn locked_startup_creates_one_grin_seed_and_never_replaces_a_missing_seed() {
    global::set_local_chain_type(ChainTypes::AutomatedTesting);
    let dir = tempfile::tempdir().unwrap();
    let lock = open_node_lock(dir.path()).unwrap();
    fs2::FileExt::try_lock_exclusive(&lock).unwrap();
    fs::write(dir.path().join("private.json"), b"unrelated configuration").unwrap();
    let wallet = initialize_wallet(dir.path(), &lock, false, Offline).unwrap();
    let account = wallet.signer().unwrap().account();
    let path = crate::wallet::seed_path(dir.path());
    let seed = fs::read(&path).unwrap();
    assert!(!dir.path().join("wallet.seed").exists());
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        assert_eq!(
            fs::metadata(&path).unwrap().permissions().mode() & 0o777,
            0o600
        );
    }
    drop(wallet);
    let wallet = initialize_wallet(dir.path(), &lock, true, Offline).unwrap();
    assert_eq!(wallet.signer().unwrap().account(), account);
    assert_eq!(fs::read(&path).unwrap(), seed);
    drop(wallet);
    fs::remove_file(&path).unwrap();
    assert!(initialize_wallet(dir.path(), &lock, true, Offline).is_err());
    assert!(!path.exists());
}
#[test]
fn startup_rechecks_seed_presence_and_existing_wallet_state() {
    global::set_local_chain_type(ChainTypes::AutomatedTesting);
    let dir = tempfile::tempdir().unwrap();
    let lock = open_node_lock(dir.path()).unwrap();
    fs2::FileExt::try_lock_exclusive(&lock).unwrap();
    fs::create_dir(dir.path().join("payments")).unwrap();
    fs::write(dir.path().join("payments/intent.json"), b"existing payment").unwrap();
    assert!(initialize_wallet(dir.path(), &lock, false, Offline).is_err());
    assert!(!crate::wallet::exists(dir.path()).unwrap());
}
#[cfg(unix)]
#[test]
fn locked_startup_rejects_unlinked_lock_inode_and_nonregular_paths() {
    use std::os::unix::fs::symlink;
    let dir = tempfile::tempdir().unwrap();
    let lock_path = dir.path().join("node.lock");
    let lock = open_node_lock(dir.path()).unwrap();
    fs2::FileExt::try_lock_exclusive(&lock).unwrap();
    fs::remove_file(&lock_path).unwrap();
    fs::write(&lock_path, b"").unwrap();
    assert!(matches!(
        initialize_wallet(dir.path(), &lock, false, Offline),
        Err(Error::Invalid("lock changed during startup"))
    ));
    fs::remove_file(&lock_path).unwrap();
    symlink(dir.path().join("missing"), &lock_path).unwrap();
    assert!(open_node_lock(dir.path()).is_err());
    fs::remove_file(&lock_path).unwrap();
    assert!(std::process::Command::new("mkfifo")
        .arg(&lock_path)
        .status()
        .unwrap()
        .success());
    let fifo = open_node_lock(dir.path()).unwrap();
    assert!(matches!(
        initialize_wallet(dir.path(), &fifo, false, Offline),
        Err(Error::Invalid("lock must be a regular file"))
    ));
}
fn config(arguments: &[&str]) -> Config {
    let matches = crate::cli::command()
        .try_get_matches_from(std::iter::once("g2pool").chain(arguments.iter().copied()))
        .unwrap();
    crate::cli::config(&matches).unwrap()
}
#[test]
fn listener_settings_are_startup_inputs() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().to_str().unwrap();
    let selected = config(&[
        "--data-dir",
        data,
        "--network",
        "testnet",
        "--grin-listen",
        "0.0.0.0:18000",
        "--stratum",
        "127.0.0.1:18001",
    ]);
    fs::create_dir(dir.path().join("testnet")).unwrap();
    fs::write(dir.path().join("testnet/instance.json"), b"unused settings").unwrap();
    let restarted = config(&["--data-dir", data, "--network", "testnet"]);
    assert_eq!(restarted.network, ChainTypes::Testnet);
    assert_eq!(selected.grin_port, 18000);
    assert_eq!(selected.stratum.port(), 18001);
    assert_eq!(restarted.grin_port, 13418);
    assert_eq!(restarted.stratum.port(), 13420);
    assert_eq!(
        config(&[
            "--data-dir",
            data,
            "--network",
            "testnet",
            "--grin-listen",
            "0.0.0.0:18002"
        ])
        .grin_port,
        18002
    );
    let mainnet = config(&["--data-dir", data, "--network", "mainnet"]);
    assert_eq!(mainnet.network, ChainTypes::Mainnet);
    assert_ne!(mainnet.data, selected.data);
}
#[test]
fn network_names_and_embedded_defaults_are_unambiguous() {
    for name in ["testnet", "mainnet"] {
        let config = config(&["--network", name]);
        assert_eq!(
            config.stratum.port(),
            if name == "testnet" { 13420 } else { 3420 }
        );
        assert_eq!(
            config.peer.port(),
            if name == "testnet" { 13417 } else { 3417 }
        );
        assert_eq!(
            config.grin_port,
            if name == "testnet" { 13418 } else { 3418 }
        );
    }
    assert!(crate::cli::command()
        .try_get_matches_from(["g2pool", "--network", "floonet"])
        .is_err());
    assert!(crate::cli::command()
        .try_get_matches_from(["g2pool", "--unknown", "value"])
        .is_err());
}
#[test]
fn default_state_lives_under_the_user_home() {
    let selected = config(&[]);
    assert_eq!(selected.data, crate::default_data_dir().join("mainnet"));
    assert!(selected.data.is_absolute() || selected.data == std::path::Path::new(".g2pool"));
}
#[test]
fn network_specific_defaults_never_overwrite_explicit_ports() {
    for args in [
        vec![
            "--network",
            "testnet",
            "--stratum",
            "127.0.0.1:5000",
            "--grin-listen",
            "0.0.0.0:5001",
            "--pool-listen",
            "0.0.0.0:5003",
        ],
        vec![
            "--stratum",
            "127.0.0.1:5000",
            "--grin-listen",
            "0.0.0.0:5001",
            "--pool-listen",
            "0.0.0.0:5003",
            "--network",
            "testnet",
        ],
    ] {
        let config = config(&args);
        assert_eq!(config.stratum.port(), 5000);
        assert_eq!(config.grin_port, 5001);
        assert_eq!(config.peer.port(), 5003);
    }
}
