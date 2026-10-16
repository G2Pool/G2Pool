use super::*;
#[test]
fn blank_directory_creates_private_seed_under_lock_and_reuses_it() {
    let dir = tempfile::tempdir().unwrap();
    require_blank_wallet_directory(dir.path()).unwrap();
    let lock = open_node_lock(dir.path()).unwrap();
    fs2::FileExt::try_lock_exclusive(&lock).unwrap();
    let (wallet, created) = initialize_locked_wallet(dir.path(), &lock, None, false).unwrap();
    assert!(created);
    let seed_path = dir.path().join("wallet.seed");
    let seed = fs::read(&seed_path).unwrap();
    assert_eq!(seed.len(), 32);
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        assert_eq!(
            fs::metadata(&seed_path).unwrap().permissions().mode() & 0o777,
            0o600
        );
    }
    fs::write(dir.path().join("pool"), b"persisted accounting").unwrap();
    let (reopened, created) =
        initialize_locked_wallet(dir.path(), &lock, Some(wallet.account()), false).unwrap();
    assert!(!created);
    assert_eq!(reopened.account(), wallet.account());
    assert_eq!(fs::read(seed_path).unwrap(), seed);
    assert_eq!(
        fs::read(dir.path().join("pool")).unwrap(),
        b"persisted accounting"
    );
    fs::remove_file(dir.path().join("wallet.seed")).unwrap();
    assert!(initialize_locked_wallet(dir.path(), &lock, None, false).is_err());
    assert!(!dir.path().join("wallet.seed").exists());
}
#[test]
fn blank_directory_check_rejects_state_and_rechecks_under_lock() {
    let dir = tempfile::tempdir().unwrap();
    require_blank_wallet_directory(dir.path()).unwrap();
    let lock = open_node_lock(dir.path()).unwrap();
    fs2::FileExt::try_lock_exclusive(&lock).unwrap();
    fs::create_dir(dir.path().join("payments")).unwrap();
    assert!(initialize_locked_wallet(dir.path(), &lock, None, false).is_err());
    assert!(!dir.path().join("wallet.seed").exists());
    assert!(dir.path().join("node.lock").is_file());
}
#[test]
fn locked_startup_rejects_seed_removal_or_replacement_after_preflight() {
    let dir = tempfile::tempdir().unwrap();
    let seed = dir.path().join("wallet.seed");
    let account = Wallet::open(&seed).unwrap().account();
    let lock = open_node_lock(dir.path()).unwrap();
    fs2::FileExt::try_lock_exclusive(&lock).unwrap();
    assert_eq!(
        initialize_locked_wallet(dir.path(), &lock, Some(account), false)
            .unwrap()
            .0
            .account(),
        account
    );
    fs::remove_file(&seed).unwrap();
    assert!(
        matches!(initialize_locked_wallet(dir.path(), &lock, Some(account), false), Err(Error::Invalid(message)) if message.contains("wallet seed missing"))
    );
    fs::write(&seed, [91; 32]).unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(&seed, fs::Permissions::from_mode(0o600)).unwrap();
    }
    assert!(matches!(
        initialize_locked_wallet(dir.path(), &lock, Some(account), false),
        Err(Error::Invalid("wallet seed changed during startup"))
    ));
}
#[cfg(unix)]
#[test]
fn locked_startup_rejects_unlinked_lock_inode_and_nonregular_paths() {
    use std::os::unix::fs::symlink;
    let dir = tempfile::tempdir().unwrap();
    let account = Wallet::open(&dir.path().join("wallet.seed"))
        .unwrap()
        .account();
    let lock_path = dir.path().join("node.lock");
    let lock = open_node_lock(dir.path()).unwrap();
    fs2::FileExt::try_lock_exclusive(&lock).unwrap();
    fs::remove_file(&lock_path).unwrap();
    fs::write(&lock_path, b"").unwrap();
    assert!(matches!(
        initialize_locked_wallet(dir.path(), &lock, Some(account), false),
        Err(Error::Invalid("node lock changed during startup"))
    ));
    fs::remove_file(&lock_path).unwrap();
    symlink(dir.path().join("wallet.seed"), &lock_path).unwrap();
    assert!(open_node_lock(dir.path()).is_err());
    fs::remove_file(&lock_path).unwrap();
    assert!(std::process::Command::new("mkfifo")
        .arg(&lock_path)
        .status()
        .unwrap()
        .success());
    let fifo = open_node_lock(dir.path()).unwrap();
    assert!(matches!(
        initialize_locked_wallet(dir.path(), &fifo, Some(account), false),
        Err(Error::Invalid("node lock must be a regular file"))
    ));
}
fn config(arguments: &[&str]) -> Config {
    args_from(arguments.iter().map(|arg| (*arg).to_owned())).unwrap()
}
#[test]
fn source_bootstrap_defaults_use_the_remote_network_port() {
    for (network, port) in [("mainnet", 3417), ("testnet", 13417)] {
        let dir = tempfile::tempdir().unwrap();
        let data = dir.path().to_str().unwrap();
        let selected = config(&[
            "--data-dir",
            data,
            "--network",
            network,
            "--pool-listen",
            "0.0.0.0:5550",
        ]);
        let defaults = bootstrap_peers(&selected);
        assert_eq!(
            defaults,
            vec![
                SocketAddr::new("188.119.24.7".parse().unwrap(), port),
                SocketAddr::new("51.83.197.245".parse().unwrap(), port),
            ]
        );
        assert_eq!(fs::read_dir(dir.path()).unwrap().count(), 0);
    }
    assert_eq!(config(&[]).network, ChainTypes::Mainnet);
    assert_eq!(bootstrap_peers(&config(&["--network", "mainnet"])).len(), 2);
}
#[test]
fn instance_settings_are_selected_by_directory_and_explicit_ports_override() {
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
    publish_config(
        &dir.path().join("testnet/instance.json"),
        &serde_json::to_vec(&Instance::from_config(&selected)).unwrap(),
    )
    .unwrap();
    let restarted = config(&["--data-dir", data, "--network", "testnet"]);
    assert_eq!(restarted.network, ChainTypes::Testnet);
    assert_eq!(restarted.grin_port, 18000);
    assert_eq!(restarted.stratum.port(), 18001);
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
    assert!(args_from(["--network", "floonet"].into_iter().map(str::to_owned)).is_err());
    assert!(args_from(["--unknown", "value"].into_iter().map(str::to_owned)).is_err());
}
#[test]
fn default_state_lives_under_the_user_home() {
    let selected = config(&[]);
    assert_eq!(selected.data, g2pool::default_data_dir().join("mainnet"));
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
