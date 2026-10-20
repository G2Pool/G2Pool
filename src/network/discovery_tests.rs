use super::*;

#[test]
fn retained_connection_refreshes_its_peer_list_without_reconnecting() {
    let (left, _left_keys, _left_wallet, _left_dir) = crate::node::tests::setup();
    let (right, _right_keys, _right_wallet, _right_dir) = crate::node::tests::setup();
    assert_eq!(left.profile.protocol, right.profile.protocol);
    let protocol = left.profile.protocol;
    let left = Arc::new(Mutex::new(left));
    let right = Arc::new(Mutex::new(right));
    let stop = Arc::new(AtomicBool::new(false));
    struct Servers(Arc<AtomicBool>, Vec<thread::JoinHandle<Result<()>>>);
    impl Drop for Servers {
        fn drop(&mut self) {
            self.0.store(true, std::sync::atomic::Ordering::Release);
            for handle in self.1.drain(..) {
                handle.join().unwrap().unwrap();
            }
        }
    }
    let mut servers = Servers(stop.clone(), Vec::new());
    let port = || {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        listener.local_addr().unwrap()
    };
    let left_address = port();
    let right_address = port();
    left.lock().unwrap().listening_port = left_address.port();
    right.lock().unwrap().listening_port = right_address.port();
    servers
        .1
        .push(start(left_address, left.clone(), stop.clone(), 4).unwrap());
    servers
        .1
        .push(start(right_address, right.clone(), stop.clone(), 4).unwrap());
    let first = "8.8.8.8:3417".parse().unwrap();
    let later = "9.9.9.9:3417".parse().unwrap();
    let now = chrono::Utc::now().timestamp();
    {
        let mut right = right.lock().unwrap();
        right.peers.seed(first).unwrap();
        right.peers.outcome(first, true, now).unwrap();
    }
    let mut client = Client::new(right_address, protocol);
    client.synchronize_turns(&left, &stop, 0).unwrap();
    assert!(left
        .lock()
        .unwrap()
        .peers
        .targets(now, &[], OUTBOUND_PEERS)
        .contains(&first));
    let connection = client.generation;
    assert!(connection.is_some());
    let due = client.next_peers;
    {
        let mut right = right.lock().unwrap();
        right.peers.seed(later).unwrap();
        right.peers.outcome(later, true, now).unwrap();
    }
    client
        .refresh_peers(&left, &stop, due - Duration::from_nanos(1))
        .unwrap();
    assert!(!left
        .lock()
        .unwrap()
        .peers
        .targets(now, &[], OUTBOUND_PEERS)
        .contains(&later));
    client.refresh_peers(&left, &stop, due).unwrap();
    assert!(left
        .lock()
        .unwrap()
        .peers
        .targets(now, &[], OUTBOUND_PEERS)
        .contains(&later));
    assert_eq!(client.generation, connection);
    let interval = client.next_peers.duration_since(due);
    assert!((Duration::from_secs(60)..=Duration::from_secs(120)).contains(&interval));
}
