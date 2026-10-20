use std::{
    collections::BTreeMap,
    net::IpAddr,
    sync::{Arc, Mutex},
};

const MAX_SESSIONS_PER_IP: usize = 2;
const MAX_SESSIONS_PER_ROUTE: usize = 8;

#[derive(Default)]
struct Active {
    total: usize,
    by_ip: BTreeMap<IpAddr, usize>,
    by_route: BTreeMap<[u8; 9], usize>,
}

fn route(ip: IpAddr) -> [u8; 9] {
    match ip {
        IpAddr::V4(ip) => {
            let [a, b, c, ..] = ip.octets();
            [0, a, b, c, 0, 0, 0, 0, 0]
        }
        IpAddr::V6(ip) => {
            let bytes = ip.octets();
            [
                1, bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
            ]
        }
    }
}

fn address_ip(address: &libp2p::Multiaddr) -> Option<IpAddr> {
    use libp2p::multiaddr::Protocol;
    match address.iter().next()? {
        Protocol::Ip4(ip) => Some(ip.into()),
        Protocol::Ip6(ip) => Some(ip.into()),
        _ => None,
    }
}

fn denied() -> libp2p::swarm::ConnectionDenied {
    libp2p::swarm::ConnectionDenied::new(std::io::Error::other("pool connection admission"))
}

pub(super) struct Admission {
    max_sessions: usize,
    active: Mutex<Active>,
}

impl Admission {
    pub(super) fn new(max_sessions: usize) -> Arc<Self> {
        Arc::new(Self {
            max_sessions,
            active: Mutex::new(Active::default()),
        })
    }

    pub(super) fn acquire(self: &Arc<Self>, ip: IpAddr) -> Option<Permit> {
        let ip = match ip {
            IpAddr::V6(ip) => ip
                .to_ipv4_mapped()
                .map(IpAddr::V4)
                .unwrap_or(IpAddr::V6(ip)),
            ip => ip,
        };
        let route = route(ip);
        let mut active = self.active.lock().unwrap_or_else(|e| e.into_inner());
        if active.total >= self.max_sessions
            || active.by_ip.get(&ip).copied().unwrap_or(0) >= MAX_SESSIONS_PER_IP
            || active.by_route.get(&route).copied().unwrap_or(0) >= MAX_SESSIONS_PER_ROUTE
        {
            return None;
        }
        active.total += 1;
        *active.by_ip.entry(ip).or_default() += 1;
        *active.by_route.entry(route).or_default() += 1;
        Some(Permit {
            admission: self.clone(),
            ip,
            route,
        })
    }
}

pub(super) struct Permit {
    admission: Arc<Admission>,
    ip: IpAddr,
    route: [u8; 9],
}

impl Drop for Permit {
    fn drop(&mut self) {
        let mut active = self
            .admission
            .active
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        active.total -= 1;
        let count = active.by_ip.get_mut(&self.ip).expect("admitted peer IP");
        *count -= 1;
        if *count == 0 {
            active.by_ip.remove(&self.ip);
        }
        let count = active
            .by_route
            .get_mut(&self.route)
            .expect("admitted peer route");
        *count -= 1;
        if *count == 0 {
            active.by_route.remove(&self.route);
        }
    }
}

pub(super) struct Gate {
    admission: Arc<Admission>,
    bans: super::bans::IpBans,
    remotes: BTreeMap<libp2p::swarm::ConnectionId, IpAddr>,
    permits: BTreeMap<libp2p::swarm::ConnectionId, Permit>,
    rates: BTreeMap<[u8; 9], Window>,
    global: Window,
    outbound: std::collections::BTreeSet<libp2p::swarm::ConnectionId>,
    peers: BTreeMap<libp2p::swarm::ConnectionId, libp2p::PeerId>,
    retiring: std::collections::BTreeSet<libp2p::swarm::ConnectionId>,
    local: libp2p::PeerId,
    closing: std::collections::VecDeque<(libp2p::PeerId, libp2p::swarm::ConnectionId)>,
}
struct Window {
    start: std::time::Instant,
    attempts: usize,
}
impl Window {
    fn new() -> Self {
        Self {
            start: std::time::Instant::now(),
            attempts: 0,
        }
    }
    fn allow(&mut self, limit: usize) -> bool {
        if self.start.elapsed() >= std::time::Duration::from_secs(10) {
            *self = Self::new();
        }
        if self.attempts >= limit {
            return false;
        }
        self.attempts += 1;
        true
    }
}
impl Gate {
    pub(super) fn with_bans(max: usize, local: libp2p::PeerId, bans: super::bans::IpBans) -> Self {
        Self {
            bans,
            remotes: BTreeMap::new(),
            admission: Admission::new(max),
            permits: BTreeMap::new(),
            rates: BTreeMap::new(),
            global: Window::new(),
            outbound: Default::default(),
            peers: Default::default(),
            retiring: Default::default(),
            local,
            closing: Default::default(),
        }
    }
    pub(super) fn ip_banned(&self, ip: IpAddr) -> bool {
        self.bans.contains(ip)
    }
    pub(super) fn retire(&mut self, peer: libp2p::PeerId) {
        self.retiring.extend(
            self.peers
                .iter()
                .filter_map(|(id, connected)| (*connected == peer).then_some(*id)),
        );
    }
    pub(super) fn is_retiring(&self, id: libp2p::swarm::ConnectionId) -> bool {
        self.retiring.contains(&id)
    }
    pub(super) fn ban_connection(&self, id: libp2p::swarm::ConnectionId) {
        if let Some(ip) = self.remotes.get(&id) {
            self.bans.ban(*ip, super::bans::DEFAULT_BAN);
            eprintln!("peer {ip}: malformed RPC data; IP banned for 600 seconds (loopback exempt)");
        }
    }
    fn duplicate(
        &self,
        peer: libp2p::PeerId,
        outbound: bool,
    ) -> Result<(), libp2p::swarm::ConnectionDenied> {
        let preferred = self.local < peer;
        let existing: Vec<_> = self
            .peers
            .iter()
            .filter(|(_, p)| **p == peer)
            .map(|(id, _)| *id)
            .collect();
        if existing.is_empty() {
            return Ok(());
        }
        if outbound == preferred
            && existing
                .iter()
                .all(|id| self.outbound.contains(id) != preferred)
        {
            return Ok(());
        }
        Err(libp2p::swarm::ConnectionDenied::new(std::io::Error::other(
            "duplicate pool peer",
        )))
    }
    fn admit(
        &mut self,
        id: libp2p::swarm::ConnectionId,
        address: &libp2p::Multiaddr,
    ) -> Result<(), libp2p::swarm::ConnectionDenied> {
        use libp2p::multiaddr::Protocol;
        let ip = match address.iter().next() {
            Some(Protocol::Ip4(ip)) => IpAddr::V4(ip),
            Some(Protocol::Ip6(ip)) => IpAddr::V6(ip),
            _ => return Err(denied()),
        };
        let ip = crate::network::normalize(std::net::SocketAddr::new(ip, 1)).ip();
        if self.bans.contains(ip) {
            return Err(denied());
        }
        let key = match ip {
            IpAddr::V4(ip) => {
                let b = ip.octets();
                [0, b[0], b[1], b[2], b[3], 0, 0, 0, 0]
            }
            IpAddr::V6(ip) => {
                let b = ip.octets();
                [1, b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7]]
            }
        };
        self.rates
            .retain(|_, window| window.start.elapsed() < std::time::Duration::from_secs(30));
        if !self.global.allow(16)
            || (!self.rates.contains_key(&key) && self.rates.len() >= 256)
            || !self.rates.entry(key).or_insert_with(Window::new).allow(4)
        {
            return Err(denied());
        }
        let permit = self.admission.acquire(ip).ok_or_else(denied)?;
        self.remotes.insert(id, ip);
        self.permits.insert(id, permit);
        Ok(())
    }
}
impl libp2p::swarm::NetworkBehaviour for Gate {
    type ConnectionHandler = libp2p::swarm::dummy::ConnectionHandler;
    type ToSwarm = std::convert::Infallible;
    fn handle_pending_inbound_connection(
        &mut self,
        id: libp2p::swarm::ConnectionId,
        _: &libp2p::Multiaddr,
        remote: &libp2p::Multiaddr,
    ) -> Result<(), libp2p::swarm::ConnectionDenied> {
        self.admit(id, remote)
    }
    fn handle_established_inbound_connection(
        &mut self,
        id: libp2p::swarm::ConnectionId,
        peer: libp2p::PeerId,
        _: &libp2p::Multiaddr,
        remote: &libp2p::Multiaddr,
    ) -> Result<Self::ConnectionHandler, libp2p::swarm::ConnectionDenied> {
        let Some(ip) = address_ip(remote) else {
            return Err(denied());
        };
        if self.bans.contains(ip) {
            self.permits.remove(&id);
            self.remotes.remove(&id);
            self.outbound.remove(&id);
            return Err(denied());
        }
        self.remotes.insert(id, ip);
        if self.duplicate(peer, false).is_err() {
            self.remotes.remove(&id);
            self.permits.remove(&id);
            return Err(denied());
        }
        Ok(libp2p::swarm::dummy::ConnectionHandler)
    }
    fn handle_established_outbound_connection(
        &mut self,
        id: libp2p::swarm::ConnectionId,
        peer: libp2p::PeerId,
        remote: &libp2p::Multiaddr,
        _: libp2p::core::Endpoint,
        _: libp2p::core::transport::PortUse,
    ) -> Result<Self::ConnectionHandler, libp2p::swarm::ConnectionDenied> {
        let Some(ip) = address_ip(remote) else {
            return Err(denied());
        };
        if self.bans.contains(ip) {
            self.permits.remove(&id);
            self.remotes.remove(&id);
            self.outbound.remove(&id);
            return Err(denied());
        }
        self.remotes.insert(id, ip);
        if self.duplicate(peer, true).is_err() {
            self.remotes.remove(&id);
            self.permits.remove(&id);
            self.outbound.remove(&id);
            return Err(denied());
        }
        Ok(libp2p::swarm::dummy::ConnectionHandler)
    }
    fn handle_pending_outbound_connection(
        &mut self,
        id: libp2p::swarm::ConnectionId,
        _: Option<libp2p::PeerId>,
        addresses: &[libp2p::Multiaddr],
        _: libp2p::core::Endpoint,
    ) -> Result<Vec<libp2p::Multiaddr>, libp2p::swarm::ConnectionDenied> {
        if addresses
            .iter()
            .any(|address| address_ip(address).is_some_and(|ip| self.bans.contains(ip)))
        {
            return Err(denied());
        }
        if self.outbound.len() >= crate::network::OUTBOUND_PEERS + crate::network::OUTBOUND_PENDING
            || self
                .outbound
                .iter()
                .filter(|id| !self.peers.contains_key(id))
                .count()
                >= crate::network::OUTBOUND_PENDING
        {
            return Err(libp2p::swarm::ConnectionDenied::new(std::io::Error::other(
                "outbound pool slots",
            )));
        }
        self.outbound.insert(id);
        Ok(vec![])
    }
    fn on_swarm_event(&mut self, event: libp2p::swarm::FromSwarm) {
        use libp2p::swarm::FromSwarm;
        match event {
            FromSwarm::ConnectionEstablished(e) => {
                for (id, peer) in &self.peers {
                    if *peer == e.peer_id {
                        self.closing.push_back((*peer, *id));
                    }
                }
                self.peers.insert(e.connection_id, e.peer_id);
            }
            FromSwarm::ConnectionClosed(e) => {
                self.retiring.remove(&e.connection_id);
                self.closing.retain(|(_, id)| *id != e.connection_id);
                self.remotes.remove(&e.connection_id);
                self.permits.remove(&e.connection_id);
                self.outbound.remove(&e.connection_id);
                self.peers.remove(&e.connection_id);
            }
            FromSwarm::ListenFailure(e) => {
                self.remotes.remove(&e.connection_id);
                self.permits.remove(&e.connection_id);
            }
            FromSwarm::DialFailure(e) => {
                self.remotes.remove(&e.connection_id);
                self.outbound.remove(&e.connection_id);
            }
            _ => {}
        }
    }
    fn on_connection_handler_event(
        &mut self,
        _: libp2p::PeerId,
        _: libp2p::swarm::ConnectionId,
        event: std::convert::Infallible,
    ) {
        match event {}
    }
    fn poll(
        &mut self,
        _: &mut std::task::Context<'_>,
    ) -> std::task::Poll<libp2p::swarm::ToSwarm<Self::ToSwarm, std::convert::Infallible>> {
        if let Some((id, peer_id)) = self
            .peers
            .iter()
            .find(|(id, _)| {
                self.remotes
                    .get(id)
                    .is_some_and(|ip| self.bans.contains(*ip))
            })
            .map(|(id, peer)| (*id, *peer))
        {
            self.remotes.remove(&id);
            self.retiring.insert(id);
            return std::task::Poll::Ready(libp2p::swarm::ToSwarm::CloseConnection {
                peer_id,
                connection: libp2p::swarm::CloseConnection::One(id),
            });
        }
        if let Some((peer_id, id)) = self.closing.pop_front() {
            self.retiring.insert(id);
            return std::task::Poll::Ready(libp2p::swarm::ToSwarm::CloseConnection {
                peer_id,
                connection: libp2p::swarm::CloseConnection::One(id),
            });
        }
        std::task::Poll::Pending
    }
}

#[cfg(test)]
#[path = "admission_tests.rs"]
mod tests;
