mod admission;
mod bans;
mod budget;
mod codec;
pub(crate) mod download;
pub(crate) mod outbound;
pub mod peers;
pub(crate) mod transport;
mod wire;
use crate::{
    codec::{decode, Id, Inventory},
    consensus::EvaluationProgress,
    node::Node,
    objects::{Key, Kind, Source},
    storage::Object,
    Error, Result,
};
use std::{
    collections::{BTreeSet, VecDeque},
    net::SocketAddr,
    sync::{atomic::AtomicBool, Arc, Mutex},
    thread,
    time::{Duration, Instant},
};
pub use wire::Message;
pub(crate) use wire::{MAX_TIPS, REQUEST_KEYS};
pub const OUTBOUND_PEERS: usize = 16;
pub const OUTBOUND_PENDING: usize = 4;
pub const INBOUND_PEERS: usize = 64;

fn peer_interval() -> Duration {
    Duration::from_secs(rand::random_range(60..=120))
}

fn lock(node: &Arc<Mutex<Node>>) -> Result<std::sync::MutexGuard<'_, Node>> {
    node.lock()
        .map_err(|_| Error::Invalid("node sequencer poisoned"))
}
fn chain_type(node: &Arc<Mutex<Node>>) -> Result<grin_core::global::ChainTypes> {
    Ok(lock(node)?.profile.network)
}
pub(crate) fn normalize(address: SocketAddr) -> SocketAddr {
    let ip = match address.ip() {
        std::net::IpAddr::V6(ip) => ip
            .to_ipv4_mapped()
            .map(std::net::IpAddr::V4)
            .unwrap_or(std::net::IpAddr::V6(ip)),
        ip => ip,
    };
    SocketAddr::new(ip, address.port())
}

pub fn start(
    address: SocketAddr,
    node: Arc<Mutex<Node>>,
    stop: Arc<AtomicBool>,
    max_sessions: usize,
    pool_key: [u8; 32],
) -> Result<thread::JoinHandle<Result<()>>> {
    let (handle, worker) = transport::start(address, node.clone(), stop, max_sessions, pool_key)?;
    lock(&node)?.transport = Some(handle);
    Ok(worker)
}
pub struct Client {
    address: SocketAddr,
    pool_id: Id,
    handle: Option<transport::Handle>,
    initialized: bool,
    last_heads: Instant,
    next_peers: Instant,
    generation: Option<libp2p::swarm::ConnectionId>,
    inbound: Option<libp2p::swarm::ConnectionId>,
    observed_heads: Option<(libp2p::swarm::ConnectionId, Vec<Id>)>,
    pending: VecDeque<Id>,
}
impl Drop for Client {
    fn drop(&mut self) {
        self.disconnect();
    }
}
impl Client {
    pub fn new(address: SocketAddr, pool_id: Id) -> Self {
        Self {
            address: normalize(address),
            pool_id,
            handle: None,
            initialized: false,
            generation: None,
            inbound: None,
            observed_heads: None,
            pending: VecDeque::new(),
            last_heads: Instant::now() - Duration::from_secs(30),
            next_peers: Instant::now(),
        }
    }
    pub fn synchronize(&mut self, node: &Arc<Mutex<Node>>, stop: &AtomicBool) -> Result<usize> {
        self.synchronize_turns(node, stop, 256)
    }
    pub fn synchronize_turns(
        &mut self,
        node: &Arc<Mutex<Node>>,
        stop: &AtomicBool,
        turns: usize,
    ) -> Result<usize> {
        if lock(node)?.profile.pool_id != self.pool_id {
            return Err(Error::Invalid("client pool identity"));
        }
        if self.handle.is_none() {
            self.handle = lock(node)?.transport.clone();
            if self.handle.is_none() {
                return Err(Error::Capacity("pool transport is not running"));
            }
        }
        let result = (|| {
            if !self.initialized {
                let port = lock(node)?.listening_port;
                let reply = self.request(Message::Announce(port), stop)?;
                if matches!(reply.message(), Message::Busy) {
                    return Err(Error::Capacity("peer busy"));
                }
                self.initialized = true;
                self.next_peers = Instant::now();
            }
            self.refresh_peers(node, stop, Instant::now())?;
            acquire_turns(self, node, stop, turns)
        })();
        if matches!(result, Err(Error::Invalid(_) | Error::Witness(_))) {
            if let Some(handle) = &self.handle {
                if let Some(generation) = self.generation {
                    handle.disconnect(self.address, generation);
                }
            }
            self.initialized = false;
        }
        result
    }
    fn refresh_peers(
        &mut self,
        node: &Arc<Mutex<Node>>,
        stop: &AtomicBool,
        now: Instant,
    ) -> Result<()> {
        if now < self.next_peers {
            return Ok(());
        }
        self.next_peers = now + peer_interval();
        match self.request(Message::Peers, stop)?.message() {
            Message::Addresses(addresses) => {
                let mut guard = lock(node)?;
                for address in addresses {
                    guard.peers.learn_from(address, self.address);
                }
            }
            Message::Busy => {}
            _ => return Err(Error::Invalid("peer discovery reply")),
        }
        Ok(())
    }
    pub fn reached_endpoint(&self) -> bool {
        self.handle
            .as_ref()
            .is_some_and(|h| h.reached(self.address))
    }
    fn verified_connection(
        &self,
        node: &Arc<Mutex<Node>>,
    ) -> Result<Option<libp2p::swarm::ConnectionId>> {
        let Some((connection, tips)) = &self.observed_heads else {
            return Ok(None);
        };
        if !self.initialized || self.generation != Some(*connection) {
            return Ok(None);
        }
        let guard = lock(node)?;
        if !tips.iter().all(|tip| guard.knows_head(*tip)) {
            return Ok(None);
        }
        Ok(Some(*connection))
    }
    fn disconnect(&self) {
        if let (Some(handle), Some(connection)) = (&self.handle, self.generation) {
            handle.disconnect(self.address, connection);
        }
    }
    fn request(&mut self, message: Message, stop: &AtomicBool) -> Result<codec::Packet> {
        let result = self.handle.as_ref().ok_or(Error::Cancelled)?.request(
            self.address,
            self.inbound,
            message,
            stop,
        );
        if let Ok(packet) = &result {
            if self.generation != packet.origin {
                self.initialized = false;
                self.observed_heads = None;
                self.pending.clear();
                self.last_heads = Instant::now() - Duration::from_secs(30);
            }
            self.generation = packet.origin;
        }
        if result.is_err() {
            self.initialized = false;
            self.observed_heads = None;
            self.pending.clear();
        }
        result
    }
}
fn stage(node: &Arc<Mutex<Node>>, objects: Vec<Object>) -> Result<()> {
    let path = { lock(node)?.store.path().to_owned() };
    if path.parent().is_some_and(|p| {
        fs2::available_space(p).is_ok_and(|free| free < crate::status::runtime::RESERVE)
    }) {
        return Err(Error::Capacity("disk reserve"));
    }
    let task = lock(node)?.prepare_stage(objects)?;
    let checked = task.run()?;
    lock(node)?.finish_stage(checked)
}
pub(crate) fn stage_partial(
    node: &Arc<Mutex<Node>>,
    objects: Vec<Object>,
    address: SocketAddr,
) -> Result<bool> {
    let (source, shared) = {
        let guard = lock(node)?;
        (guard.store.read_source()?, guard.downloads.clone())
    };
    let source = source.open()?;
    let mut stored = BTreeSet::new();
    for object in &objects {
        let kind = match object.key.kind {
            Kind::Core => Some(Kind::Authorization),
            Kind::Authorization => Some(Kind::Core),
            _ => None,
        };
        if let Some(kind) = kind {
            let key = Key::new(kind, object.key.id);
            match source.byte_length(key) {
                Ok(_) => {
                    stored.insert(key);
                }
                Err(Error::Unavailable(_)) => {}
                Err(error) => return Err(error),
            }
        }
    }
    drop(source);
    let assembled = download::Downloads::assemble(&shared, objects, address, &stored)?;
    if assembled.objects.is_empty() {
        return Ok(false);
    }
    let first = assembled.objects[0].key;
    let result = stage(node, assembled.objects.clone());
    match result {
        Err(Error::Witness(_) | Error::Invalid(_)) if assembled.mixed => {
            Err(Error::Unavailable(first))
        }
        other => other.map(|_| true),
    }
}
fn batches(keys: &[Key]) -> Result<Vec<Vec<Key>>> {
    let mut batches = Vec::new();
    let mut batch = Vec::new();
    let mut size = 4usize;
    for key in keys {
        let bytes = 37 + key.kind.ceiling();
        if size + bytes > wire::FRAME || batch.len() == REQUEST_KEYS {
            batches.push(std::mem::take(&mut batch));
            size = 4;
        }
        if bytes + 4 > wire::FRAME {
            return Err(Error::Capacity("object envelope"));
        }
        batch.push(*key);
        size += bytes;
    }
    if !batch.is_empty() {
        batches.push(batch);
    }
    Ok(batches)
}
fn serve_objects(source: crate::storage::ReadSource, keys: Vec<Key>) -> Result<Message> {
    let source = source.open()?;
    let mut objects = Vec::new();
    let mut missing = Vec::new();
    let mut deferred = Vec::new();
    for key in keys {
        match source.read(key) {
            Ok(bytes) => objects.push(Object { key, bytes }),
            Err(Error::Unavailable(_)) => missing.push(key),
            Err(Error::Capacity(_)) => deferred.push(key),
            Err(error) => return Err(error),
        }
    }
    Ok(Message::Reply {
        objects,
        missing,
        deferred,
    })
}
fn acquire_turns(
    session: &mut Client,
    node: &Arc<Mutex<Node>>,
    stop: &AtomicBool,
    max_turns: usize,
) -> Result<usize> {
    grin_core::global::set_local_chain_type(chain_type(node)?);
    let tips = if session.last_heads.elapsed() >= Duration::from_secs(30) {
        let heads = session.request(Message::Heads, stop)?;
        match heads.message() {
            Message::Tips(tips) => {
                session.last_heads = Instant::now();
                session.observed_heads = session.generation.map(|id| (id, tips.clone()));
                tips
            }
            Message::Busy => return Ok(0),
            _ => return Err(Error::Invalid("peer heads response")),
        }
    } else {
        vec![]
    };
    for tip in tips
        .into_iter()
        .chain(session.handle.as_ref().unwrap().notices(session.address))
    {
        session.pending.retain(|old| *old != tip);
        session.pending.push_front(tip);
        session.pending.truncate(MAX_TIPS);
    }
    let mut queue: VecDeque<_> = lock(node)?
        .acquisition_heads()?
        .into_iter()
        .chain(session.pending.iter().copied())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();
    {
        let guard = lock(node)?;
        queue.retain(|tip| !guard.knows_head(*tip));
        session.pending.retain(|tip| !guard.knows_head(*tip));
    }
    if let Some(cursor) = lock(node)?.sync_cursor {
        let pivot = queue.iter().take_while(|tip| **tip <= cursor).count();
        queue.rotate_left(pivot);
    }

    let mut admitted = 0;
    let started = Instant::now();
    let mut turns = 0;
    while let Some(tip) = queue.pop_front() {
        if stop.load(std::sync::atomic::Ordering::Acquire) {
            return Err(Error::Cancelled);
        }
        if turns >= max_turns.min(256) || started.elapsed() > Duration::from_secs(3) {
            break;
        }
        turns += 1;
        let missing = {
            let mut guard = lock(node)?;
            guard.sync_cursor = Some(tip);
            let mut missing = guard.waiting_for(tip)?;
            if missing.is_none() {
                for kind in [Kind::Core, Kind::Authorization] {
                    let key = Key::new(kind, tip);
                    match guard.store.byte_length(key) {
                        Ok(_) => {}
                        Err(Error::Unavailable(_)) => {
                            missing = Some(key);
                            break;
                        }
                        Err(error) => return Err(error),
                    }
                }
            }
            missing
        };
        let missing = if let Some(key) = missing {
            key
        } else {
            let turn = lock(node)?.take_turn(tip);
            let turn = match turn {
                Ok(Some(turn)) => turn,
                Ok(None) | Err(Error::Capacity(_)) => continue,
                Err(error) => return Err(error),
            };
            let (turn, outcome) = turn.run();
            let outcome = lock(node)?.finish_turn(turn, outcome);
            match outcome {
                Ok(EvaluationProgress::Known) => continue,
                Ok(EvaluationProgress::Ready(_)) => {
                    admitted += 1;
                    continue;
                }
                Ok(EvaluationProgress::Pending) => {
                    queue.push_back(tip);
                    thread::yield_now();
                    continue;
                }
                Ok(EvaluationProgress::Missing(key)) => key,
                Err(Error::Cancelled) => return Err(Error::Cancelled),
                Err(Error::View) => return Err(Error::View),
                Err(Error::Capacity(_) | Error::EvaluationTime) => {
                    continue;
                }
                Err(Error::Expired) => {
                    session.pending.retain(|pending| *pending != tip);
                    continue;
                }
                Err(error) => return Err(error),
            }
        };
        let keys = if missing.kind == Kind::Core {
            vec![missing, Key::new(Kind::Authorization, missing.id)]
        } else {
            vec![missing]
        };
        let lease = lock(node)?.reserve_download(tip, session.address, keys.clone(), None);
        let lease = match lease {
            Ok(Some(lease)) => lease,
            Ok(None) | Err(Error::Cancelled) => continue,
            Err(error) => return Err(error),
        };
        let packet = session.request(Message::Get(keys.clone()), stop)?;
        let (objects, absent, deferred) = match packet.message() {
            Message::Reply {
                objects,
                missing: absent,
                deferred,
            } => (objects, absent, deferred),
            Message::Busy => {
                lease.deferred(session.address, &keys);
                continue;
            }
            _ => return Err(Error::Invalid("object reply")),
        };
        lease.absent(session.address, &absent);
        lease.deferred(session.address, &deferred);
        let progress = stage_partial(node, objects, session.address)?;
        drop(packet);
        if missing.kind == Kind::Inventory {
            let source = lock(node)?.store.read_source()?;
            let source = source.open()?;
            let inventory: Inventory = match source.read(missing) {
                Ok(bytes) => decode(&bytes)?,
                Err(Error::Unavailable(_)) => continue,
                Err(error) => return Err(error),
            };
            let needed: Vec<_> = inventory
                .rows
                .0
                .iter()
                .flat_map(|row| row.coupon_ids.0.iter())
                .map(|id| Key::new(Kind::Coupon, *id))
                .filter(|key| matches!(source.byte_length(*key), Err(Error::Unavailable(_))))
                .collect::<BTreeSet<_>>()
                .into_iter()
                .collect();
            drop(source);
            for keys in batches(&needed)? {
                let request =
                    lock(node)?.reserve_download(tip, session.address, keys.clone(), Some(&lease));
                let request = match request {
                    Ok(Some(request)) => request,
                    Ok(None) => continue,
                    Err(Error::Cancelled) => break,
                    Err(error) => return Err(error),
                };
                let packet = session.request(Message::Get(keys.clone()), stop)?;
                let (objects, absent, deferred) = match packet.message() {
                    Message::Reply {
                        objects,
                        missing: absent,
                        deferred,
                    } => (objects, absent, deferred),
                    Message::Busy => {
                        request.deferred(session.address, &keys);
                        break;
                    }
                    _ => return Err(Error::Invalid("object reply")),
                };
                request.absent(session.address, &absent);
                request.deferred(session.address, &deferred);
                let progress = !objects.is_empty();
                if progress {
                    stage(node, objects)?;
                }
                if !progress || !deferred.is_empty() {
                    break;
                }
            }
        }
        drop(lease);
        if progress {
            queue.push_back(tip);
        }
    }
    Ok(admitted)
}

#[cfg(test)]
#[path = "discovery_tests.rs"]
mod discovery_tests;
