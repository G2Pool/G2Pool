use super::{chain_type, lock, peer_interval, transport, Client, OUTBOUND_PEERS, OUTBOUND_PENDING};
use crate::{node::Node, Error, Result};
use libp2p::swarm::ConnectionId;
use rand::seq::IteratorRandom;
use std::{
    collections::BTreeMap,
    net::SocketAddr,
    sync::{
        atomic::{AtomicBool, Ordering},
        mpsc, Arc, Mutex,
    },
    thread,
    time::{Duration, Instant},
};

struct Connection {
    stop: Arc<AtomicBool>,
    worker: Option<thread::JoinHandle<()>>,
    generation: Option<ConnectionId>,
    started: Instant,
}
impl Drop for Connection {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        if self
            .worker
            .take()
            .is_some_and(|worker| worker.join().is_err())
        {
            eprintln!("outbound peer worker panicked");
        }
    }
}

struct Connections {
    node: Arc<Mutex<Node>>,
    transport: transport::Handle,
    peers: BTreeMap<SocketAddr, Connection>,
    inbound: BTreeMap<SocketAddr, Connection>,
    send: mpsc::SyncSender<(SocketAddr, ConnectionId)>,
    ready: mpsc::Receiver<(SocketAddr, ConnectionId)>,
    next_probe: Instant,
}
impl Connections {
    fn new(node: Arc<Mutex<Node>>, now: Instant) -> Result<Self> {
        let transport = lock(&node)?
            .transport
            .clone()
            .ok_or(Error::Invalid("outbound transport missing"))?;
        let (send, ready) = mpsc::sync_channel(OUTBOUND_PENDING);
        Ok(Self {
            node,
            transport,
            peers: BTreeMap::new(),
            inbound: BTreeMap::new(),
            send,
            ready,
            next_probe: now + peer_interval(),
        })
    }

    fn tick(&mut self, now: Instant) -> Result<()> {
        let inbound = self
            .transport
            .inbound
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone();
        self.inbound
            .retain(|address, worker| inbound.get(address) == worker.generation.as_ref());
        for (address, connection) in &inbound {
            if !self.inbound.contains_key(address) {
                self.connect(*address, now, Some(*connection))?;
            }
        }
        let finished: Vec<_> = self
            .peers
            .iter()
            .filter(|(_, peer)| {
                peer.worker
                    .as_ref()
                    .is_none_or(|worker| worker.is_finished())
                    || (peer.generation.is_none()
                        && now.saturating_duration_since(peer.started) >= super::codec::DEADLINE)
            })
            .map(|(address, _)| *address)
            .collect();
        for address in finished {
            self.peers.remove(&address);
        }
        while let Ok((address, generation)) = self.ready.try_recv() {
            if !self.peers.contains_key(&address) {
                continue;
            }
            let retained = self
                .peers
                .values()
                .filter(|peer| peer.generation.is_some())
                .count();
            let previous = if retained >= OUTBOUND_PEERS {
                self.peers
                    .iter()
                    .filter_map(|(address, peer)| {
                        peer.generation
                            .map(|generation| (*address, generation, peer.stop.clone()))
                    })
                    .choose(&mut rand::rng())
            } else {
                None
            };
            let old = previous.as_ref().map(|(address, _, _)| *address);
            if self.transport.promote((address, generation), previous) {
                if let Some(old) = old {
                    self.peers.remove(&old);
                }
                self.peers.get_mut(&address).unwrap().generation = Some(generation);
            } else {
                self.peers.remove(&address);
            }
        }

        let pending = self
            .peers
            .values()
            .filter(|peer| peer.generation.is_none())
            .count();
        let mut attempts = OUTBOUND_PEERS.saturating_sub(self.peers.len());
        if now >= self.next_probe {
            self.next_probe = now + peer_interval();
            attempts = attempts.max(1);
        }
        attempts = attempts.min(OUTBOUND_PENDING.saturating_sub(pending));
        let connected: Vec<_> = self
            .peers
            .keys()
            .chain(self.inbound.keys())
            .copied()
            .collect();
        let targets =
            lock(&self.node)?
                .peers
                .targets(chrono::Utc::now().timestamp(), &connected, attempts);
        for address in targets {
            self.connect(address, now, None)?;
        }
        Ok(())
    }

    fn connect(
        &mut self,
        address: SocketAddr,
        now: Instant,
        inbound: Option<ConnectionId>,
    ) -> Result<()> {
        let kind = chain_type(&self.node)?;
        let protocol = lock(&self.node)?.profile.protocol;
        let node = self.node.clone();
        let ready = self.send.clone();
        let stop = Arc::new(AtomicBool::new(false));
        let signal = stop.clone();
        let worker = thread::spawn(move || {
            grin_core::global::set_local_chain_type(kind);
            let mut client = Client::new(address, protocol);
            client.inbound = inbound;
            client.generation = inbound;
            let mut announced = false;
            while !signal.load(Ordering::Acquire) {
                let result = client.synchronize(&node, &signal);
                if signal.load(Ordering::Acquire) {
                    break;
                }
                if inbound.is_none()
                    && !announced
                    && matches!(result, Ok(_) | Err(Error::Capacity(_)))
                {
                    if let Ok(Some(connection)) = client.verified_connection(&node) {
                        if ready.try_send((address, connection)).is_err() {
                            break;
                        }
                        announced = true;
                    }
                }
                if inbound.is_none() && !matches!(result, Err(ref error) if retryable(error)) {
                    if let Ok(mut node) = node.lock() {
                        let now = chrono::Utc::now().timestamp();
                        if client.reached_endpoint() {
                            node.peers.reached(address, now);
                        }
                        if result.as_ref().is_ok_and(|count| *count > 0) {
                            let _ = node.peers.outcome(address, true, now);
                            node.peers.progress(address, now);
                        } else if result.is_err() {
                            let _ = node.peers.outcome(address, false, now);
                        }
                    }
                }
                if let Err(error) = result {
                    if !retryable(&error) {
                        if !matches!(error, Error::Cancelled) {
                            eprintln!("peer {address}: {error}");
                        }
                        break;
                    }
                }
                for _ in 0..20 {
                    if signal.load(Ordering::Acquire) {
                        break;
                    }
                    thread::sleep(Duration::from_millis(25));
                }
            }
        });
        let peers = if inbound.is_some() {
            &mut self.inbound
        } else {
            &mut self.peers
        };
        peers.insert(
            address,
            Connection {
                stop,
                worker: Some(worker),
                generation: inbound,
                started: now,
            },
        );
        Ok(())
    }
}
impl Drop for Connections {
    fn drop(&mut self) {
        for peer in self.peers.values().chain(self.inbound.values()) {
            peer.stop.store(true, Ordering::Release);
        }
        self.peers.clear();
        self.inbound.clear();
    }
}

fn retryable(error: &Error) -> bool {
    matches!(
        error,
        Error::Capacity(_)
            | Error::View
            | Error::Future
            | Error::Unavailable(_)
            | Error::NativeUnavailable(_)
            | Error::Cancelled
    )
}

pub(crate) fn maintain(node: Arc<Mutex<Node>>, stop: Arc<AtomicBool>) -> Result<()> {
    grin_core::global::set_local_chain_type(chain_type(&node)?);
    let mut connections = Connections::new(node, Instant::now())?;
    while !stop.load(Ordering::Acquire) {
        connections.tick(Instant::now())?;
        thread::sleep(Duration::from_millis(100));
    }
    Ok(())
}

#[cfg(test)]
#[path = "outbound_tests.rs"]
mod tests;
