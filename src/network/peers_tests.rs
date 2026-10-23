use super::*;
use crate::network::OUTBOUND_PEERS;

#[test]
fn bans_filter_seed_targets_and_gossip_without_persisting() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("pool");
    let seed: SocketAddr = "8.8.8.8:3417".parse().unwrap();
    let other: SocketAddr = "9.9.9.9:3417".parse().unwrap();
    let mut book = Book::open(path.clone(), [1; 32], false, &[seed, other]).unwrap();
    let now = chrono::Utc::now().timestamp();
    book.outcome(seed, true, now).unwrap();
    book.bans.ban(seed.ip(), super::super::bans::DEFAULT_BAN);
    assert!(!book.targets(now, &[], OUTBOUND_PEERS).contains(&seed));
    assert!(book.targets(now, &[], OUTBOUND_PEERS).contains(&other));
    assert!(!book.addresses().contains(&seed));
    book.flush().unwrap();
    drop(book);
    let reopened = Book::open(path, [1; 32], false, &[seed, other]).unwrap();
    assert!(reopened.targets(now, &[], OUTBOUND_PEERS).contains(&seed));
}

#[test]
fn public_discovery_excludes_special_routes_but_allows_explicit_seeds() {
    for ip in [
        "100.64.0.1",
        "100.127.255.254",
        "198.18.0.1",
        "198.19.255.254",
        "192.0.0.8",
        "192.88.99.1",
        "::ffff:100.64.0.1",
        "::127.0.0.1",
        "64:ff9b::7f00:1",
        "64:ff9b:1::a00:1",
        "100::1",
        "2001::a00:1",
        "2001:2::1",
        "2001:db8::1",
        "2002:7f00:1::1",
        "3fff::1",
        "3fff:fff::1",
        "fc00::1",
        "fe80::1",
        "fec0::1",
    ] {
        let address = SocketAddr::new(ip.parse().unwrap(), 3417);
        let mut book = Book::memory(false);
        assert!(!book.learn(address).unwrap(), "learned {ip}");
        book.seed(address).unwrap();
        assert_eq!(
            book.targets(0, &[], OUTBOUND_PEERS),
            vec![crate::network::normalize(address)]
        );
    }
    for ip in [
        "100.63.255.254",
        "100.128.0.1",
        "198.17.255.254",
        "198.20.0.1",
        "192.0.0.9",
        "192.0.0.10",
        "8.8.8.8",
        "::ffff:8.8.8.8",
        "2001:4860:4860::8888",
        "2606:4700:4700::1111",
    ] {
        assert!(routable(SocketAddr::new(ip.parse().unwrap(), 3417)), "{ip}");
    }
}

#[test]
fn startup_overrides_preserve_routable_peers_without_preserving_seed_privileges() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("pool");
    let default = "188.119.24.7:3417".parse().unwrap();
    let custom = "51.83.197.245:5555".parse().unwrap();
    let private = "127.0.0.1:3417".parse().unwrap();
    Book::open(path.clone(), [1; 32], false, &[custom, private])
        .unwrap()
        .flush()
        .unwrap();
    let book = Book::open(path, [1; 32], false, &[default]).unwrap();
    assert!(book.known().contains(&custom));
    assert!(book.known().contains(&default));
    assert!(!book.known().contains(&private));
    assert!(!book.peers[&custom].seed);
    assert!(book.peers[&default].seed);
}
#[test]
fn reopening_rechecks_learned_routes_without_removing_configured_seeds() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("pool");
    let learned = "100.64.0.1:3417".parse().unwrap();
    let seed = "[2001:db8::1]:3417".parse().unwrap();
    let mut book = Book::open(path.clone(), [1; 32], true, &[seed]).unwrap();
    assert!(book.learn(learned).unwrap());
    book.flush().unwrap();
    drop(book);
    let book = Book::open(path, [1; 32], false, &[seed]).unwrap();
    assert_eq!(book.known(), vec![seed]);
}
#[test]
fn hostile_rumours_cannot_gossip_private_routes_or_fence_other_sources() {
    let mut book = Book::memory(false);
    let origin = "8.8.8.8:3417".parse().unwrap();
    for subnet in 0..128 {
        let _ = book
            .learn_from(format!("11.{subnet}.1.1:3417").parse().unwrap(), origin)
            .unwrap();
    }
    assert_eq!(book.known().len(), 16);
    assert!(book.addresses().is_empty());
    let honest = "51.83.197.245:3417".parse().unwrap();
    assert!(book
        .learn_from(honest, "9.9.9.9:3417".parse().unwrap())
        .unwrap());
    book.outcome(honest, true, chrono::Utc::now().timestamp())
        .unwrap();
    let private = "127.0.0.1:3417".parse().unwrap();
    book.seed(private).unwrap();
    book.outcome(private, true, chrono::Utc::now().timestamp())
        .unwrap();
    assert_eq!(book.addresses(), vec![honest]);
}
#[test]
fn expired_hints_leave_the_routing_set_without_a_restart() {
    let mut book = Book::memory(false);
    let hint = "8.8.8.8:3417".parse().unwrap();
    let seed = "51.83.197.245:3417".parse().unwrap();
    book.learn(hint).unwrap();
    book.seed(seed).unwrap();
    let heard = book.peers[&hint].heard;
    assert!(book.targets(heard, &[], OUTBOUND_PEERS).contains(&hint));
    assert!(!book
        .targets(heard + 86401, &[], OUTBOUND_PEERS)
        .contains(&hint));
    assert!(book
        .targets(heard + 86401, &[], OUTBOUND_PEERS)
        .contains(&seed));
    assert!(book.addresses().is_empty());
}
#[test]
fn malformed_routing_rows_do_not_recreate_wallet_state() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("pool");
    let mut store =
        crate::storage::Store::open(&path, [1; 32], crate::storage::StorageLimits::default())
            .unwrap();
    store.bind_wallet([7; 33]).unwrap();
    drop(store);
    let sharechain = crate::storage::sharechain::SharechainDb::open(
        &path.with_extension("sharechain"),
        crate::storage::sharechain::MAP_BYTES,
        [1; 32],
    )
    .unwrap();
    let runtime = &sharechain;
    runtime
        .replace_peers(&[
            (b"peers/8.8.8.8:3417".to_vec(), b"broken".to_vec()),
            (b"peers/9.9.9.9:3417".to_vec(), vec![7]),
        ])
        .unwrap();
    drop(sharechain);
    let book = Book::open(path.clone(), [1; 32], false, &[]).unwrap();
    assert!(book.known().is_empty());
    drop(book);
    let store =
        crate::storage::Store::open(&path, [1; 32], crate::storage::StorageLimits::default())
            .unwrap();
    assert_eq!(store.wallet_account().unwrap(), Some([7; 33]));
}
#[test]
fn reachability_does_not_make_a_sparse_peer_a_productive_gossip_source() {
    let mut book = Book::memory(false);
    let peer = "8.8.8.8:3417".parse().unwrap();
    book.learn(peer).unwrap();
    book.reached(peer, 100);
    assert!(book.targets(100, &[], OUTBOUND_PEERS).contains(&peer));
    assert!(book.addresses().is_empty());
    let _ = book.outcome(peer, true, 100);
    assert!(book.addresses().contains(&peer));
}

impl Book {
    fn open(
        path: std::path::PathBuf,
        pool_id: Id,
        local: bool,
        seeds: &[SocketAddr],
    ) -> Result<Self> {
        let sharechain = crate::storage::sharechain::SharechainDb::open(
            &path.with_extension("sharechain"),
            crate::storage::sharechain::MAP_BYTES,
            pool_id,
        )
        .map_err(|_| Error::Invalid("peer database identity"))?;
        Self::from_database(sharechain, local, seeds)
    }

    fn learn(&mut self, addr: SocketAddr) -> Result<bool> {
        self.learn_from(addr, addr)
    }

    fn known(&self) -> Vec<SocketAddr> {
        self.peers.keys().copied().collect()
    }

    fn flush(&mut self) -> Result<()> {
        if let Some(snapshot) = self.persistence() {
            let generation = snapshot.write()?;
            self.persisted(generation);
        }
        Ok(())
    }
}

#[test]
fn failed_connections_back_off_but_successes_have_no_probe_cooldown() {
    let peer = "8.8.8.8:3417".parse().unwrap();
    let mut book = Book::memory(false);
    book.seed(peer).unwrap();
    book.outcome(peer, false, 100).unwrap();
    assert!(!book.targets(101, &[], OUTBOUND_PEERS).contains(&peer));
    assert!(book.targets(102, &[], OUTBOUND_PEERS).contains(&peer));
    book.outcome(peer, true, 102).unwrap();
    assert_eq!(book.peers[&peer].failures, 0);
    assert!(book.targets(102, &[], OUTBOUND_PEERS).contains(&peer));
}

#[test]
fn outbound_candidates_prefer_unused_networks_and_exclude_connected_ips() {
    let mut book = Book::memory(false);
    let connected: Vec<SocketAddr> = (0..OUTBOUND_PEERS)
        .map(|index| format!("11.{index}.1.1:3417").parse().unwrap())
        .collect();
    for address in &connected {
        book.seed(*address).unwrap();
    }
    let diverse = "12.0.1.1:3417".parse().unwrap();
    let same_network = "11.0.1.2:3417".parse().unwrap();
    let same_ip = "11.0.1.1:3418".parse().unwrap();
    for address in [diverse, same_network, same_ip] {
        book.seed(address).unwrap();
    }
    assert_eq!(book.targets(0, &connected, 1), vec![diverse]);
    let connected = &connected[..connected.len() - 1];
    let targets = book.targets(0, connected, 1);
    assert_eq!(targets.len(), 1);
    assert!(!connected.contains(&targets[0]));
    assert!(![same_network, same_ip].contains(&targets[0]));
    book.peers.remove(&diverse);
    book.peers.retain(|address, _| {
        connected.contains(address) || *address == same_network || *address == same_ip
    });
    assert_eq!(book.targets(0, connected, 1), vec![same_network]);
    book.peers.remove(&same_network);
    assert!(book.targets(0, connected, 1).is_empty());
}
