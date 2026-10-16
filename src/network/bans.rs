use std::{
    collections::BTreeMap,
    net::{IpAddr, SocketAddr},
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

pub(super) const DEFAULT_BAN: Duration = Duration::from_secs(600);
const MAX_BANS: usize = 448;

fn normalize(ip: IpAddr) -> IpAddr {
    super::normalize(SocketAddr::new(ip, 0)).ip()
}

fn key(ip: IpAddr) -> IpAddr {
    match normalize(ip) {
        IpAddr::V4(ip) => IpAddr::V4(ip),
        IpAddr::V6(ip) => {
            let mut octets = ip.octets();
            octets[8..].fill(0);
            IpAddr::V6(octets.into())
        }
    }
}

#[derive(Clone, Default)]
pub(crate) struct IpBans(Arc<Mutex<BTreeMap<IpAddr, Instant>>>);

impl IpBans {
    pub(crate) fn contains(&self, ip: IpAddr) -> bool {
        self.contains_at(ip, Instant::now())
    }

    fn contains_at(&self, ip: IpAddr, now: Instant) -> bool {
        let ip = normalize(ip);
        if ip.is_loopback() {
            return false;
        }
        let mut entries = self.0.lock().unwrap_or_else(|e| e.into_inner());
        entries.retain(|_, deadline| *deadline > now);
        entries.contains_key(&key(ip))
    }

    pub(crate) fn ban(&self, ip: IpAddr, duration: Duration) {
        self.ban_at(ip, duration, Instant::now());
    }

    fn ban_at(&self, ip: IpAddr, duration: Duration, now: Instant) {
        let ip = normalize(ip);
        if ip.is_loopback() || duration.is_zero() {
            return;
        }
        let Some(deadline) = now.checked_add(duration) else {
            return;
        };
        let mut entries = self.0.lock().unwrap_or_else(|e| e.into_inner());
        entries.retain(|_, deadline| *deadline > now);
        let ip = key(ip);
        if entries.len() >= MAX_BANS && !entries.contains_key(&ip) {
            if let Some(oldest) = entries
                .iter()
                .min_by_key(|(ip, end)| (**end, **ip))
                .map(|(ip, _)| *ip)
            {
                entries.remove(&oldest);
            }
        }
        entries
            .entry(ip)
            .and_modify(|end| *end = (*end).max(deadline))
            .or_insert(deadline);
    }
}

#[cfg(test)]
#[path = "bans_tests.rs"]
mod tests;
