use super::*;

struct Servers {
    stop: Arc<AtomicBool>,
    workers: Vec<thread::JoinHandle<Result<()>>>,
}
impl Servers {
    fn new() -> Self {
        Self {
            stop: Arc::new(AtomicBool::new(false)),
            workers: Vec::new(),
        }
    }
    fn start(&mut self, node: Arc<Mutex<Node>>, ip: [u8; 4]) -> SocketAddr {
        let listener = std::net::TcpListener::bind((std::net::Ipv4Addr::from(ip), 0)).unwrap();
        let address = listener.local_addr().unwrap();
        drop(listener);
        self.workers
            .push(super::super::start(address, node, self.stop.clone(), 4).unwrap());
        address
    }
}
impl Drop for Servers {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        for worker in self.workers.drain(..) {
            worker.join().unwrap().unwrap();
        }
    }
}
fn wait_until(mut condition: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(15);
    while !condition() {
        assert!(Instant::now() < deadline, "network condition timed out");
        thread::sleep(Duration::from_millis(25));
    }
}

#[test]
fn full_outbound_set_survives_busy_candidates_then_rotates_after_valid_state_exchange() {
    let (mut left, _left_signer, _left_wallet, _left_dir) = crate::node::tests::setup();
    let (right, _right_signer, _right_wallet, _right_dir) = crate::node::tests::setup();
    let (mut honest, honest_signer, honest_wallet, _honest_dir) = crate::node::tests::setup();
    let (tip, _) = crate::node::tests::share(&mut honest, &honest_signer, &honest_wallet);
    left.peers = super::super::peers::Book::memory(true);
    let left = Arc::new(Mutex::new(left));
    let right = Arc::new(Mutex::new(right));
    let mut servers = Servers::new();
    servers.start(left.clone(), [127, 0, 0, 1]);
    let addresses: Vec<_> = (1..=OUTBOUND_PEERS + OUTBOUND_PENDING)
        .map(|index| servers.start(right.clone(), [127, 42, 0, index as u8]))
        .collect();
    let honest = servers.start(Arc::new(Mutex::new(honest)), [127, 43, 0, 1]);
    let mut connections = Connections::new(left.clone(), Instant::now()).unwrap();
    connections.next_probe = Instant::now();
    connections.tick(Instant::now()).unwrap();
    assert!(connections.peers.is_empty());
    assert!(connections.next_probe > Instant::now());
    lock(&left).unwrap().peers.seed(addresses[0]).unwrap();
    wait_until(|| {
        connections.tick(Instant::now()).unwrap();
        connections
            .peers
            .get(&addresses[0])
            .is_some_and(|peer| peer.generation.is_some())
    });
    let first = connections.peers[&addresses[0]].generation;
    connections.next_probe = Instant::now();
    connections.tick(Instant::now()).unwrap();
    assert_eq!(connections.peers.len(), 1);
    assert_eq!(connections.peers[&addresses[0]].generation, first);
    for address in &addresses[1..OUTBOUND_PEERS] {
        lock(&left).unwrap().peers.seed(*address).unwrap();
    }
    wait_until(|| {
        connections.tick(Instant::now()).unwrap();
        assert!(
            connections
                .peers
                .values()
                .filter(|peer| peer.generation.is_none())
                .count()
                <= OUTBOUND_PENDING
        );
        connections
            .peers
            .values()
            .filter(|peer| peer.generation.is_some())
            .count()
            == OUTBOUND_PEERS
    });
    let original: Vec<_> = connections.peers.keys().copied().collect();
    connections.next_probe = Instant::now();
    connections.tick(Instant::now()).unwrap();
    assert_eq!(
        connections.peers.keys().copied().collect::<Vec<_>>(),
        original
    );
    assert_eq!(connections.peers[&addresses[0]].generation, first);
    let busy = right.lock().unwrap();
    for address in &addresses[OUTBOUND_PEERS..] {
        lock(&left).unwrap().peers.seed(*address).unwrap();
    }
    for _ in 0..OUTBOUND_PENDING + 1 {
        let now = Instant::now();
        connections.next_probe = now;
        connections.tick(now).unwrap();
    }
    wait_until(|| {
        connections.transport.connected.load(Ordering::Acquire) == OUTBOUND_PEERS + OUTBOUND_PENDING
    });
    assert_eq!(connections.peers.len(), OUTBOUND_PEERS + OUTBOUND_PENDING);
    assert_eq!(
        connections
            .peers
            .values()
            .filter(|peer| peer.generation.is_some())
            .count(),
        OUTBOUND_PEERS
    );
    for address in &original {
        assert!(!connections.peers[address].stop.load(Ordering::Acquire));
    }
    let expired = Instant::now() + super::super::codec::DEADLINE;
    connections.next_probe = expired + peer_interval();
    connections.tick(expired).unwrap();
    assert_eq!(
        connections.peers.keys().copied().collect::<Vec<_>>(),
        original
    );
    drop(busy);

    let eligible = lock(&left).unwrap().peers.targets(
        chrono::Utc::now().timestamp(),
        &[],
        OUTBOUND_PEERS + OUTBOUND_PENDING,
    );
    assert!(addresses.iter().all(|address| eligible.contains(address)));
    let mut book = super::super::peers::Book::memory(true);
    for address in original.iter().copied().chain([honest]) {
        book.seed(address).unwrap();
    }
    lock(&left).unwrap().peers = book;

    connections.next_probe = Instant::now();
    wait_until(|| {
        connections.tick(Instant::now()).unwrap();
        assert_eq!(
            connections
                .peers
                .values()
                .filter(|peer| peer.generation.is_some())
                .count(),
            OUTBOUND_PEERS
        );
        assert!(connections.peers.len() <= OUTBOUND_PEERS + OUTBOUND_PENDING);
        connections
            .peers
            .get(&honest)
            .is_some_and(|peer| peer.generation.is_some())
    });
    assert_eq!(connections.peers.len(), OUTBOUND_PEERS);
    assert_eq!(
        original
            .iter()
            .filter(|address| !connections.peers.contains_key(address))
            .count(),
        1
    );
    wait_until(|| connections.transport.connected.load(Ordering::Acquire) == OUTBOUND_PEERS);
    assert!(lock(&left)
        .unwrap()
        .advertised_heads()
        .unwrap()
        .contains(&tip));
    let eligible = lock(&left).unwrap().peers.targets(
        chrono::Utc::now().timestamp(),
        &[],
        OUTBOUND_PEERS + OUTBOUND_PENDING,
    );
    assert!(original.iter().all(|address| eligible.contains(address)));
}

#[test]
fn transport_rejects_stale_candidate_generations_before_retiring_the_old_connection() {
    let (left, _left_signer, _left_wallet, _left_dir) = crate::node::tests::setup();
    let (mut right, right_signer, right_wallet, _right_dir) = crate::node::tests::setup();
    crate::node::tests::share(&mut right, &right_signer, &right_wallet);
    let protocol = left.profile.protocol;
    let left = Arc::new(Mutex::new(left));
    let right = Arc::new(Mutex::new(right));
    let mut servers = Servers::new();
    servers.start(left.clone(), [127, 0, 0, 1]);
    let previous = servers.start(right.clone(), [127, 42, 1, 1]);
    let candidate = servers.start(right, [127, 42, 1, 2]);
    let mut old = Client::new(previous, protocol);
    let mut new = Client::new(candidate, protocol);
    old.synchronize_turns(&left, &servers.stop, 0).unwrap();
    assert!(old.verified_connection(&left).unwrap().is_none());
    old.last_heads = Instant::now() - Duration::from_secs(30);
    old.synchronize(&left, &servers.stop).unwrap();
    new.synchronize_turns(&left, &servers.stop, 0).unwrap();
    let old_generation = old.verified_connection(&left).unwrap().unwrap();
    let generation = new.verified_connection(&left).unwrap().unwrap();
    let transport = lock(&left).unwrap().transport.clone().unwrap();
    let retired = Arc::new(AtomicBool::new(false));
    let previous = || Some((previous, old_generation, retired.clone()));
    assert!(!transport.promote(
        (candidate, ConnectionId::new_unchecked(usize::MAX)),
        previous()
    ));
    assert!(!retired.load(Ordering::Acquire));
    old.synchronize_turns(&left, &servers.stop, 0).unwrap();
    assert_eq!(old.generation, Some(old_generation));
    new.disconnect();
    assert!(!transport.promote((candidate, generation), previous()));
    assert!(!retired.load(Ordering::Acquire));
    wait_until(|| transport.connected.load(Ordering::Acquire) == 1);
    new.initialized = false;
    new.synchronize_turns(&left, &servers.stop, 0).unwrap();
    let generation = new.verified_connection(&left).unwrap().unwrap();
    assert!(transport.promote((candidate, generation), previous()));
    assert!(retired.load(Ordering::Acquire));
    wait_until(|| transport.connected.load(Ordering::Acquire) == 1);
}

#[test]
fn cancelled_first_request_does_not_leave_an_unowned_connection() {
    let (left, _left_signer, _left_wallet, _left_dir) = crate::node::tests::setup();
    let (mut right, _right_signer, _right_wallet, _right_dir) = crate::node::tests::setup();
    right.peers = super::super::peers::Book::memory(true);
    let protocol = left.profile.protocol;
    let left = Arc::new(Mutex::new(left));
    let right = Arc::new(Mutex::new(right));
    let mut servers = Servers::new();
    servers.start(left.clone(), [127, 0, 0, 1]);
    let address = servers.start(right.clone(), [127, 42, 2, 1]);
    let transport = lock(&left).unwrap().transport.clone().unwrap();
    let mut client = Client::new(address, protocol);
    client.handle = Some(transport.clone());
    assert!(matches!(
        client.request(
            super::super::Message::Announce(34567),
            &AtomicBool::new(true)
        ),
        Err(Error::Cancelled)
    ));
    assert!(client.generation.is_none());
    drop(client);
    wait_until(|| {
        lock(&right)
            .unwrap()
            .peers
            .targets(chrono::Utc::now().timestamp(), &[], OUTBOUND_PEERS)
            .iter()
            .any(|address| address.port() == 34567)
    });
    wait_until(|| transport.connected.load(Ordering::Acquire) == 0);
}

#[test]
fn valid_ancestor_and_empty_state_qualify_but_unknown_history_does_not() {
    let (left, _left_signer, _left_wallet, _left_dir) = crate::node::tests::setup();
    let (mut right, right_signer, right_wallet, _right_dir) = crate::node::tests::setup();
    let (ancestor, _) = crate::node::tests::share(&mut right, &right_signer, &right_wallet);
    let protocol = left.profile.protocol;
    let left = Arc::new(Mutex::new(left));
    let right = Arc::new(Mutex::new(right));
    let mut servers = Servers::new();
    servers.start(left.clone(), [127, 0, 0, 1]);
    let address = servers.start(right.clone(), [127, 44, 0, 1]);
    let mut client = Client::new(address, protocol);
    client.synchronize(&left, &servers.stop).unwrap();
    assert!(client.verified_connection(&left).unwrap().is_some());
    let observed = client.observed_heads.clone();
    let (descendant, _) =
        crate::node::tests::share(&mut lock(&right).unwrap(), &right_signer, &right_wallet);
    client.last_heads = Instant::now() - Duration::from_secs(30);
    client.synchronize(&left, &servers.stop).unwrap();
    client.observed_heads = observed;
    assert_ne!(ancestor, descendant);
    assert!(lock(&left).unwrap().sharechain.indexed(ancestor).unwrap());
    assert_eq!(
        lock(&left).unwrap().advertised_heads().unwrap(),
        vec![descendant]
    );
    assert!(client.verified_connection(&left).unwrap().is_some());
    let connection = client.generation.unwrap();
    client.observed_heads = Some((connection, vec![[99; 32]]));
    assert!(client.verified_connection(&left).unwrap().is_none());
    client.observed_heads = Some((connection, vec![]));
    assert!(client.verified_connection(&left).unwrap().is_some());
}

#[test]
fn inbound_and_outbound_sessions_exchange_shares_over_one_connection() {
    let (mut left, left_signer, left_wallet, _left_dir) = crate::node::tests::setup();
    let (mut right, right_signer, right_wallet, _right_dir) = crate::node::tests::setup();
    left.peers = super::super::peers::Book::memory(true);
    right.peers = super::super::peers::Book::memory(true);
    let left = Arc::new(Mutex::new(left));
    let right = Arc::new(Mutex::new(right));
    let mut servers = Servers::new();
    let left_address = servers.start(left.clone(), [127, 0, 0, 1]);
    let right_address = servers.start(right.clone(), [127, 46, 0, 1]);
    lock(&left).unwrap().listening_port = left_address.port();
    lock(&right).unwrap().listening_port = right_address.port();
    lock(&left).unwrap().peers.seed(right_address).unwrap();
    let mut outgoing = Connections::new(left.clone(), Instant::now()).unwrap();
    let mut incoming = Connections::new(right.clone(), Instant::now()).unwrap();
    wait_until(|| {
        outgoing.tick(Instant::now()).unwrap();
        incoming.tick(Instant::now()).unwrap();
        outgoing
            .peers
            .values()
            .any(|worker| worker.generation.is_some())
            && incoming.inbound.len() == 1
    });
    let tip = crate::node::tests::share(&mut lock(&left).unwrap(), &left_signer, &left_wallet).0;
    wait_until(|| {
        outgoing.tick(Instant::now()).unwrap();
        incoming.tick(Instant::now()).unwrap();
        lock(&right).unwrap().sharechain.indexed(tip).unwrap()
    });
    let child =
        crate::node::tests::share(&mut lock(&right).unwrap(), &right_signer, &right_wallet).0;
    wait_until(|| {
        outgoing.tick(Instant::now()).unwrap();
        incoming.tick(Instant::now()).unwrap();
        lock(&left).unwrap().sharechain.indexed(child).unwrap()
    });
    assert!(incoming.peers.is_empty());
    assert_eq!(incoming.transport.connected.load(Ordering::Acquire), 1);
    assert_eq!(outgoing.transport.connected.load(Ordering::Acquire), 1);
    let connection = *incoming
        .transport
        .inbound
        .lock()
        .unwrap()
        .values()
        .next()
        .unwrap();
    let address = *incoming.inbound.keys().next().unwrap();
    incoming.transport.disconnect(address, connection);
    wait_until(|| {
        incoming.tick(Instant::now()).unwrap();
        incoming.inbound.is_empty()
    });
    assert!(incoming.transport.inbound.lock().unwrap().is_empty());
}
