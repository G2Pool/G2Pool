use super::*;

#[test]
fn default_creation_regeneration_and_explicit_selection() {
    let dir = tempfile::tempdir().unwrap();
    let alpha = Pool::load(dir.path(), None).unwrap();
    let original = fs::read(dir.path().join("pool.json")).unwrap();
    assert!(serde_json::from_slice::<serde_json::Value>(&original)
        .unwrap()
        .get("bootstrap_peers")
        .is_none());
    let mut custom = alpha.clone();
    custom.name = "custom".into();
    custom.key = [42; 32];
    custom.bootstrap_peers = Some(BootstrapPeers {
        mainnet: vec![],
        testnet: vec![],
    });
    let path = dir.path().join("custom.json");
    secure_file::publish_atomic(&path, &serde_json::to_vec(&custom).unwrap(), false).unwrap();
    assert_eq!(
        Pool::load(dir.path(), Some(&path)).unwrap().id([1; 32]),
        custom.id([1; 32])
    );
    assert_eq!(
        Pool::load(dir.path(), None).unwrap().id([1; 32]),
        alpha.id([1; 32])
    );
    fs::remove_file(dir.path().join("pool.json")).unwrap();
    Pool::load(dir.path(), None).unwrap();
    assert_eq!(fs::read(dir.path().join("pool.json")).unwrap(), original);
    assert!(Pool::load(dir.path(), Some(&dir.path().join("missing"))).is_err());
    fs::write(dir.path().join("pool.json"), b"invalid").unwrap();
    assert!(Pool::load(dir.path(), None).is_err());
    assert_eq!(fs::read(dir.path().join("pool.json")).unwrap(), b"invalid");
}

#[test]
fn identity_uses_key_then_native_genesis_and_excludes_discovery_metadata() {
    let mut pool = Pool::alpha();
    pool.key = [1; 32];
    let mut input = [1; 64];
    input[32..].fill(2);
    assert_eq!(
        pool.id([2; 32]).as_slice(),
        blake2::blake2b::blake2b(32, &[], &input).as_bytes()
    );
    let id = pool.id([2; 32]);
    pool.name = "renamed".into();
    pool.bootstrap_peers = Some(BootstrapPeers {
        mainnet: vec![],
        testnet: vec!["9.9.9.9:13417".parse().unwrap()],
    });
    assert_eq!(pool.id([2; 32]), id);
    assert_ne!(pool.id([3; 32]), id);
    pool.key = [2; 32];
    assert_ne!(pool.id([1; 32]), id);
    assert!(pool.peers(ChainTypes::Mainnet).unwrap().is_empty());
    assert!(pool
        .peers(ChainTypes::Testnet)
        .unwrap()
        .iter()
        .all(|address| address.port() == 13417));
}

#[test]
fn configuration_rejects_malformed_keys_fields_and_endpoints() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("custom.json");
    let original = serde_json::to_value(Pool::alpha()).unwrap();
    for (field, value) in [
        ("key", serde_json::json!("00")),
        ("key", serde_json::json!("zz".repeat(32))),
        (
            "bootstrap_peers",
            serde_json::json!({"mainnet": ["1.2.3.4:0"], "testnet": []}),
        ),
        (
            "bootstrap_peers",
            serde_json::json!({"mainnet": [], "testnet": [], "other": []}),
        ),
        ("unknown", serde_json::json!(true)),
    ] {
        let mut value_json = original.clone();
        value_json[field] = value;
        secure_file::publish_atomic(&path, &serde_json::to_vec(&value_json).unwrap(), true)
            .unwrap();
        assert!(Pool::load(dir.path(), Some(&path)).is_err(), "{field}");
    }
    let mut value = original;
    value.as_object_mut().unwrap().remove("name");
    assert!(serde_json::from_value::<Pool>(value).is_err());
}

#[test]
fn bootstrap_overrides_replace_defaults_and_custom_pools_cannot_inherit_alpha_peers() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("pool.json");
    let mut config = serde_json::to_value(Pool::alpha()).unwrap();
    config["name"] = serde_json::json!("renamed Alpha");
    secure_file::publish_atomic(&path, &serde_json::to_vec(&config).unwrap(), false).unwrap();
    let alpha = Pool::load(dir.path(), None).unwrap();
    for network in [ChainTypes::Mainnet, ChainTypes::Testnet] {
        assert_eq!(
            alpha.peers(network).unwrap(),
            Pool::alpha().peers(network).unwrap()
        );
    }
    config["bootstrap_peers"] = serde_json::json!({"mainnet":["9.9.9.9:4500"],"testnet":[]});
    secure_file::publish_atomic(&path, &serde_json::to_vec(&config).unwrap(), true).unwrap();
    let overridden = Pool::load(dir.path(), None).unwrap();
    assert_eq!(overridden.id([1; 32]), alpha.id([1; 32]));
    assert_eq!(
        overridden.peers(ChainTypes::Mainnet).unwrap(),
        vec!["9.9.9.9:4500".parse::<SocketAddr>().unwrap()]
    );
    assert!(overridden.peers(ChainTypes::Testnet).unwrap().is_empty());
    config["key"] = serde_json::json!("42".repeat(32));
    secure_file::publish_atomic(&path, &serde_json::to_vec(&config).unwrap(), true).unwrap();
    assert_eq!(
        Pool::load(dir.path(), None)
            .unwrap()
            .peers(ChainTypes::Mainnet)
            .unwrap(),
        overridden.peers(ChainTypes::Mainnet).unwrap()
    );
    config.as_object_mut().unwrap().remove("bootstrap_peers");
    secure_file::publish_atomic(&path, &serde_json::to_vec(&config).unwrap(), true).unwrap();
    assert!(matches!(
        Pool::load(dir.path(), None),
        Err(Error::Invalid("custom pool requires bootstrap_peers"))
    ));
}
