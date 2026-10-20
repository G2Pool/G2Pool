use super::*;

fn ip(last: u8) -> IpAddr {
    IpAddr::from([192, 0, 2, last])
}

#[test]
fn route_capacity_counts_ip_sessions_without_a_peer_identity_quota() {
    use libp2p::swarm::{ConnectionId, NetworkBehaviour};
    let mut gate = Gate::new(16, libp2p::PeerId::random());
    let local = "/ip4/0.0.0.0/tcp/3417".parse().unwrap();
    for n in 1..=MAX_SESSIONS_PER_ROUTE {
        let id = ConnectionId::new_unchecked(n);
        let remote = format!("/ip4/192.0.2.{n}/tcp/9000").parse().unwrap();
        let peer = libp2p::PeerId::random();
        gate.handle_pending_inbound_connection(id, &local, &remote)
            .unwrap();
        gate.handle_established_inbound_connection(id, peer, &local, &remote)
            .unwrap();
        gate.peers.insert(id, peer);
    }
    let remote = "/ip4/192.0.2.9/tcp/9000".parse().unwrap();
    assert!(gate
        .handle_pending_inbound_connection(ConnectionId::new_unchecked(9), &local, &remote)
        .is_err());
    let remote = "/ip4/192.0.3.1/tcp/9000".parse().unwrap();
    gate.handle_pending_inbound_connection(ConnectionId::new_unchecked(10), &local, &remote)
        .unwrap();
    let admission = gate.admission.clone();
    drop(gate);
    assert!(admission.active.lock().unwrap().by_route.is_empty());
}

#[test]
fn bans_reject_both_directions_before_handshake_and_close_live_prefix_connections() {
    use libp2p::swarm::{ConnectionId, NetworkBehaviour, ToSwarm};
    let bans = super::super::bans::IpBans::default();
    let mut gate = Gate::with_bans(16, libp2p::PeerId::random(), bans.clone());
    let local = "/ip4/0.0.0.0/tcp/3417".parse().unwrap();
    let remote: libp2p::Multiaddr = "/ip6/2001:db8:1:2::1/tcp/9000".parse().unwrap();
    let other: libp2p::Multiaddr = "/ip6/2001:db8:1:2::2/tcp/9001".parse().unwrap();
    let unaffected: libp2p::Multiaddr = "/ip6/2001:db8:1:3::1/tcp/9002".parse().unwrap();
    for (n, addr) in [(1, &remote), (2, &other), (3, &unaffected)] {
        let id = ConnectionId::new_unchecked(n);
        let peer = libp2p::PeerId::random();
        gate.handle_pending_inbound_connection(id, &local, addr)
            .unwrap();
        gate.handle_established_inbound_connection(id, peer, &local, addr)
            .unwrap();
        gate.peers.insert(id, peer);
    }
    gate.ban_connection(ConnectionId::new_unchecked(1));
    assert!(bans.contains("2001:db8:1:2::abcd".parse().unwrap()));
    let denied = ConnectionId::new_unchecked(4);
    assert!(gate
        .handle_pending_inbound_connection(denied, &local, &other)
        .is_err());
    assert!(!gate.permits.contains_key(&denied));
    assert!(gate
        .handle_pending_outbound_connection(
            denied,
            None,
            std::slice::from_ref(&other),
            libp2p::core::Endpoint::Dialer
        )
        .is_err());
    assert!(!gate.outbound.contains(&denied));
    assert!(gate
        .handle_established_outbound_connection(
            denied,
            libp2p::PeerId::random(),
            &other,
            libp2p::core::Endpoint::Dialer,
            libp2p::core::transport::PortUse::New
        )
        .is_err());
    let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
    for expected in [1, 2] {
        assert!(
            matches!(gate.poll(&mut cx), std::task::Poll::Ready(ToSwarm::CloseConnection {
            connection: libp2p::swarm::CloseConnection::One(id), ..
        }) if id == ConnectionId::new_unchecked(expected))
        );
    }
    assert!(gate.poll(&mut cx).is_pending());
    assert!(gate.remotes.contains_key(&ConnectionId::new_unchecked(3)));
}

#[test]
fn total_and_per_ip_bounds_are_independent() {
    let admission = Admission::new(3);
    let first = admission.acquire(ip(1)).unwrap();
    let second = admission.acquire(ip(1)).unwrap();
    assert!(admission.acquire(ip(1)).is_none());
    let other = admission.acquire(ip(2)).unwrap();
    assert!(admission.acquire(ip(3)).is_none());
    drop(first);
    let third = admission.acquire(ip(3)).unwrap();
    assert!(admission.acquire(ip(1)).is_none());
    drop((second, other, third));
    assert_eq!(admission.active.lock().unwrap().total, 0);
    assert!(admission.active.lock().unwrap().by_ip.is_empty());
    assert!(Admission::new(0).acquire(ip(1)).is_none());
}

#[test]
fn mapped_ipv6_shares_ipv4_slots() {
    let admission = Admission::new(8);
    let first = admission.acquire(ip(1)).unwrap();
    let second = admission
        .acquire("::ffff:192.0.2.1".parse().unwrap())
        .unwrap();
    assert!(admission.acquire(ip(1)).is_none());
    assert!(admission
        .acquire("::ffff:192.0.2.1".parse().unwrap())
        .is_none());
    let native = admission.acquire("2001:db8::1".parse().unwrap()).unwrap();
    drop((first, second, native));
    assert!(admission.active.lock().unwrap().by_ip.is_empty());
}

#[test]
fn rejected_and_departed_ips_leave_no_retained_entries() {
    let admission = Admission::new(1);
    let permit = admission.acquire(ip(1)).unwrap();
    for last in 2..=255 {
        assert!(admission.acquire(ip(last)).is_none());
    }
    assert_eq!(admission.active.lock().unwrap().by_ip.len(), 1);
    drop(permit);
    for last in 1..=255 {
        drop(admission.acquire(ip(last)).unwrap());
    }
    assert!(admission.active.lock().unwrap().by_ip.is_empty());
}

#[test]
fn concurrent_admission_cannot_exceed_ip_limit() {
    let admission = Admission::new(8);
    let barrier = Arc::new(std::sync::Barrier::new(33));
    let workers: Vec<_> = (0..32)
        .map(|_| {
            let admission = admission.clone();
            let barrier = barrier.clone();
            std::thread::spawn(move || {
                barrier.wait();
                let permit = admission.acquire(ip(1));
                barrier.wait();
                barrier.wait();
                drop(permit);
            })
        })
        .collect();
    barrier.wait();
    barrier.wait();
    assert_eq!(admission.active.lock().unwrap().total, MAX_SESSIONS_PER_IP);
    barrier.wait();
    for worker in workers {
        worker.join().unwrap();
    }
    assert!(admission.active.lock().unwrap().by_ip.is_empty());
}

impl Gate {
    fn new(max: usize, local: libp2p::PeerId) -> Self {
        Self::with_bans(max, local, super::super::bans::IpBans::default())
    }
}

#[test]
fn outbound_limits_allow_sixteen_connections_and_four_concurrent_candidates() {
    use libp2p::swarm::{ConnectionId, NetworkBehaviour};
    let mut gate = Gate::new(64, libp2p::PeerId::random());
    let remote = "/ip4/8.8.8.8/tcp/3417".parse().unwrap();
    let dial = |gate: &mut Gate, index| {
        gate.handle_pending_outbound_connection(
            ConnectionId::new_unchecked(index),
            None,
            std::slice::from_ref(&remote),
            libp2p::core::Endpoint::Dialer,
        )
    };
    for index in 0..super::super::OUTBOUND_PEERS {
        dial(&mut gate, index).unwrap();
        gate.peers
            .insert(ConnectionId::new_unchecked(index), libp2p::PeerId::random());
    }
    for index in
        super::super::OUTBOUND_PEERS..super::super::OUTBOUND_PEERS + super::super::OUTBOUND_PENDING
    {
        dial(&mut gate, index).unwrap();
    }
    assert!(dial(&mut gate, 100).is_err());
    for index in
        super::super::OUTBOUND_PEERS..super::super::OUTBOUND_PEERS + super::super::OUTBOUND_PENDING
    {
        gate.peers
            .insert(ConnectionId::new_unchecked(index), libp2p::PeerId::random());
    }
    assert!(dial(&mut gate, 100).is_err());
    gate.outbound.remove(&ConnectionId::new_unchecked(0));
    gate.peers.remove(&ConnectionId::new_unchecked(0));
    dial(&mut gate, 100).unwrap();
}
