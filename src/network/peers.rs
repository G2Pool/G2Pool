use crate::{codec::Id, Error, Result};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet},
    net::{IpAddr, SocketAddr},
};
pub(crate) const MAX_SEEDS: usize = 64;

pub(crate) fn validate_seed(addr: SocketAddr) -> Result<SocketAddr> {
    let addr = crate::network::normalize(addr);
    if addr.port() == 0 || addr.ip().is_unspecified() || addr.ip().is_multicast() {
        return Err(Error::Invalid("seed address"));
    }
    Ok(addr)
}
#[derive(Debug, Clone, Serialize, Deserialize)]
struct Peer {
    heard: i64,
    source: [u8; 7],
    progress: i64,
    seed: bool,
    last_success: i64,
    last_reached: i64,
    retry: i64,
    failures: u8,
}

impl Peer {
    fn fresh(&self, now: i64) -> bool {
        self.seed
            || now.saturating_sub(self.heard.max(self.last_success).max(self.last_reached))
                <= if self.last_success == 0 {
                    86400
                } else {
                    30 * 86400
                }
    }
}
pub struct Book {
    generation: u64,
    saved: u64,
    local: bool,
    database: Option<crate::storage::sharechain::SharechainDb>,
    peers: BTreeMap<SocketAddr, Peer>,
    pub(crate) bans: super::bans::IpBans,
}
pub(crate) fn route(addr: SocketAddr) -> [u8; 7] {
    let addr = crate::network::normalize(addr);
    match addr.ip() {
        IpAddr::V4(ip) => {
            let b = ip.octets();
            [0, b[0], b[1], 0, 0, 0, 0]
        }
        IpAddr::V6(ip) => {
            let b = ip.octets();
            [1, b[0], b[1], b[2], b[3], b[4], b[5]]
        }
    }
}

fn routable(addr: SocketAddr) -> bool {
    match addr.ip() {
        IpAddr::V4(ip) => {
            let [a, b, c, d] = ip.octets();
            !ip.is_private()
                && !ip.is_loopback()
                && !ip.is_link_local()
                && !ip.is_unspecified()
                && !ip.is_multicast()
                && !ip.is_broadcast()
                && !ip.is_documentation()
                && a != 0
                && a < 240
                && !(a == 100 && (64..=127).contains(&b))
                && !(a == 198 && (18..=19).contains(&b))
                && !(a == 192 && b == 0 && c == 0 && ![9, 10].contains(&d))
                && !(a == 192 && b == 88 && c == 99)
        }
        IpAddr::V6(ip) => {
            if let Some(mapped) = ip.to_ipv4_mapped() {
                return routable(SocketAddr::new(IpAddr::V4(mapped), addr.port()));
            }
            let segments = ip.segments();
            segments[0] & 0xe000 == 0x2000
                && !(segments[0] == 0x2001 && segments[1] < 0x0200)
                && !(segments[0] == 0x2001 && segments[1] == 0x0db8)
                && segments[0] != 0x2002
                && !(segments[0] == 0x3fff && segments[1] & 0xf000 == 0)
        }
    }
}
impl Book {
    pub fn memory(local: bool) -> Self {
        Self {
            generation: 1,
            saved: 0,
            local,
            database: None,
            peers: BTreeMap::new(),
            bans: super::bans::IpBans::default(),
        }
    }

    fn from_database(
        sharechain: crate::storage::sharechain::SharechainDb,
        local: bool,
        seeds: &[SocketAddr],
    ) -> Result<Self> {
        let runtime = &sharechain;
        let rows = runtime
            .read_peers()
            .map_err(|_| Error::Invalid("peer database"))?;
        let mut peers = BTreeMap::<SocketAddr, Peer>::new();
        let now = chrono::Utc::now().timestamp();
        for (key, bytes) in rows {
            if peers.len() >= 448 {
                break;
            }
            let Ok(text) = std::str::from_utf8(key.strip_prefix(b"peers/").unwrap_or_default())
            else {
                continue;
            };
            let Ok(address) = text.parse::<SocketAddr>() else {
                continue;
            };
            let address = crate::network::normalize(address);
            let Ok(peer) = serde_json::from_slice::<Peer>(&bytes) else {
                continue;
            };
            if peer.heard > now
                || peer.last_success > now
                || peer.last_reached > now
                || peer.progress > now
                || peer.retry > now.saturating_add(86400)
            {
                continue;
            }
            if !peer.seed
                && now.saturating_sub(peer.heard.max(peer.last_success).max(peer.last_reached))
                    > if peer.last_success == 0 {
                        86400
                    } else {
                        30 * 86400
                    }
            {
                continue;
            }
            if !seeds
                .iter()
                .any(|seed| crate::network::normalize(*seed) == address)
            {
                if !learnable(address, local) {
                    continue;
                }
                if peers.values().filter(|p| p.last_success == 0).count() >= 256
                    && peer.last_success == 0
                {
                    continue;
                }
                if peers.values().filter(|p| p.last_success > 0).count() >= 128
                    && peer.last_success > 0
                {
                    continue;
                }
                if peers
                    .iter()
                    .filter(|(a, _)| route(**a) == route(address))
                    .count()
                    >= 2
                {
                    continue;
                }
                if peer.last_success == 0
                    && peers
                        .values()
                        .filter(|p| p.source == peer.source && p.last_success == 0)
                        .count()
                        >= 16
                {
                    continue;
                }
            }
            peers.insert(address, peer);
        }
        let seeds: Vec<_> = seeds
            .iter()
            .copied()
            .map(crate::network::normalize)
            .collect();
        peers.retain(|address, peer| {
            peer.seed = seeds.contains(address);
            peer.seed || learnable(*address, local)
        });
        let mut book = Self {
            generation: 1,
            saved: 0,
            local,
            database: Some(sharechain),
            peers,
            bans: super::bans::IpBans::default(),
        };
        for seed in seeds {
            book.seed(seed)?;
        }

        Ok(book)
    }
    pub fn open_store(
        store: &crate::storage::Store,
        pool_id: Id,
        local: bool,
        seeds: &[SocketAddr],
    ) -> Result<Self> {
        if store.pool_id() != pool_id {
            return Err(Error::Invalid("peer database identity"));
        }
        Self::from_database(store.database(), local, seeds)
    }
    pub fn seed(&mut self, addr: SocketAddr) -> Result<()> {
        let addr = validate_seed(addr)?;
        if !self.peers.get(&addr).is_some_and(|p| p.seed)
            && self.peers.values().filter(|p| p.seed).count() >= MAX_SEEDS
        {
            return Err(Error::Capacity("peer seeds"));
        }
        if !self.peers.contains_key(&addr) && self.peers.len() >= 448 {
            let victim = self
                .peers
                .iter()
                .filter(|(_, peer)| !peer.seed)
                .min_by_key(|(address, peer)| {
                    (
                        peer.last_success,
                        std::cmp::Reverse(peer.failures),
                        **address,
                    )
                })
                .map(|(address, _)| *address)
                .ok_or(Error::Capacity("peer seeds"))?;
            self.peers.remove(&victim);
        }
        self.peers
            .entry(addr)
            .and_modify(|p| p.seed = true)
            .or_insert(Peer {
                heard: chrono::Utc::now().timestamp(),
                source: route(addr),
                progress: 0,
                seed: true,
                last_success: 0,
                last_reached: 0,
                retry: 0,
                failures: 0,
            });
        self.changed();
        Ok(())
    }
    pub fn learn_from(&mut self, addr: SocketAddr, from: SocketAddr) -> Result<bool> {
        let addr = crate::network::normalize(addr);
        let origin = route(from);
        self.expire(chrono::Utc::now().timestamp());
        if !learnable(addr, self.local) {
            return Ok(false);
        }
        if self.peers.contains_key(&addr) {
            return Ok(false);
        }
        if self
            .peers
            .values()
            .filter(|p| p.source == origin && p.last_success == 0 && !p.seed)
            .count()
            >= 16
            || self
                .peers
                .keys()
                .filter(|p| route(**p) == route(addr))
                .count()
                >= 2
        {
            return Ok(false);
        }
        if self
            .peers
            .values()
            .filter(|p| p.last_success == 0 && !p.seed)
            .count()
            >= 256
        {
            if let Some(victim) = self
                .peers
                .iter()
                .filter(|(_, p)| !p.seed && p.last_success == 0)
                .min_by_key(|(a, p)| (std::cmp::Reverse(p.failures), p.heard, **a))
                .map(|(a, _)| *a)
            {
                self.peers.remove(&victim);
            }
        }
        self.peers.insert(
            addr,
            Peer {
                heard: chrono::Utc::now().timestamp(),
                source: origin,
                progress: 0,
                seed: false,
                last_success: 0,
                last_reached: 0,
                retry: 0,
                failures: 0,
            },
        );
        self.changed();
        Ok(true)
    }
    pub fn addresses(&self) -> Vec<SocketAddr> {
        let mut peers: Vec<_> = self
            .peers
            .iter()
            .filter(|(addr, p)| {
                p.last_success > 0
                    && p.fresh(chrono::Utc::now().timestamp())
                    && routable(**addr)
                    && !self.bans.contains(addr.ip())
            })
            .collect();
        use rand::seq::SliceRandom;
        peers.shuffle(&mut rand::rng());
        peers.into_iter().take(16).map(|(addr, _)| *addr).collect()
    }
    pub fn targets(&self, now: i64, connected: &[SocketAddr], available: usize) -> Vec<SocketAddr> {
        if available == 0 {
            return Vec::new();
        }
        let mut ips: BTreeSet<_> = connected
            .iter()
            .map(|address| crate::network::normalize(*address).ip())
            .collect();
        let mut routes: BTreeSet<_> = connected.iter().map(|address| route(*address)).collect();
        let mut peers: Vec<_> = self
            .peers
            .iter()
            .filter(|(addr, p)| {
                p.retry <= now
                    && p.fresh(now)
                    && !ips.contains(&addr.ip())
                    && !self.bans.contains(addr.ip())
            })
            .map(|(address, _)| *address)
            .collect();
        use rand::seq::SliceRandom;
        peers.shuffle(&mut rand::rng());
        let mut targets = Vec::new();
        for diverse in [true, false] {
            for address in &peers {
                if ips.contains(&address.ip()) || (diverse && routes.contains(&route(*address))) {
                    continue;
                }
                targets.push(*address);
                ips.insert(address.ip());
                routes.insert(route(*address));
                if targets.len() == available {
                    return targets;
                }
            }
        }
        targets
    }
    pub fn reached(&mut self, addr: SocketAddr, now: i64) {
        if let Some(peer) = self.peers.get_mut(&crate::network::normalize(addr)) {
            peer.last_reached = now;
            peer.retry = now;
            self.changed();
        }
    }
    pub fn outcome(&mut self, addr: SocketAddr, success: bool, now: i64) -> Result<()> {
        let addr = crate::network::normalize(addr);
        let Some(peer) = self.peers.get_mut(&addr) else {
            return Ok(());
        };
        if success {
            let persist = peer.failures != 0 || now.saturating_sub(peer.last_success) >= 30;
            peer.failures = 0;
            peer.retry = now;
            if !persist {
                return Ok(());
            }
            peer.last_success = now;
        } else {
            peer.failures = peer.failures.saturating_add(1);
            peer.retry = now.saturating_add(1i64 << peer.failures.min(8));
        }
        if self
            .peers
            .values()
            .filter(|p| !p.seed && p.last_success > 0)
            .count()
            > 128
        {
            if let Some(victim) = self
                .peers
                .iter()
                .filter(|(a, p)| **a != addr && !p.seed && p.last_success > 0)
                .min_by_key(|(a, p)| (p.progress, p.last_success, **a))
                .map(|(a, _)| *a)
            {
                self.peers.remove(&victim);
            }
        }
        self.changed();
        Ok(())
    }
    fn changed(&mut self) {
        self.generation = self.generation.saturating_add(1);
    }
    pub fn progress(&mut self, addr: SocketAddr, now: i64) {
        if let Some(peer) = self.peers.get_mut(&crate::network::normalize(addr)) {
            peer.progress = now;
            self.changed();
        }
    }
    pub fn expire(&mut self, now: i64) {
        let previous = self.peers.len();
        self.peers.retain(|_, peer| peer.fresh(now));
        if self.peers.len() != previous {
            self.changed();
        }
    }
    pub fn persistence(&self) -> Option<Persistence> {
        if self.generation == self.saved {
            return None;
        }
        Some(Persistence {
            database: self.database.clone()?,
            generation: self.generation,
            peers: self.peers.clone(),
        })
    }
    pub fn persisted(&mut self, generation: u64) {
        self.saved = self.saved.max(generation);
    }
}

pub struct Persistence {
    database: crate::storage::sharechain::SharechainDb,
    generation: u64,
    peers: BTreeMap<SocketAddr, Peer>,
}
impl Persistence {
    pub fn write(self) -> Result<u64> {
        let runtime = &self.database;
        let mut desired = Vec::with_capacity(self.peers.len());
        for (address, peer) in self.peers {
            let endpoint = address.to_string();
            let record = serde_json::to_vec(&peer)?;
            let mut key = b"peers/".to_vec();
            key.extend_from_slice(endpoint.as_bytes());
            desired.push((key, record));
        }
        runtime
            .replace_peers(&desired)
            .map_err(|_| Error::Capacity("peer records"))?;
        Ok(self.generation)
    }
}

fn learnable(addr: SocketAddr, local: bool) -> bool {
    addr.port() != 0
        && (local || routable(addr))
        && !addr.ip().is_unspecified()
        && !addr.ip().is_multicast()
}

#[cfg(test)]
#[path = "peers_tests.rs"]
mod tests;
