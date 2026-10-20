mod admission;
mod bans;
mod budget;
pub mod chain_sync;
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
    match lock(node)?.profile.manifest.network {
        0 => Ok(grin_core::global::ChainTypes::Mainnet),
        1 => Ok(grin_core::global::ChainTypes::Testnet),
        2 => Ok(grin_core::global::ChainTypes::AutomatedTesting),
        3 => Ok(grin_core::global::ChainTypes::UserTesting),
        _ => Err(Error::Invalid("network")),
    }
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

fn encode_compact_hint(target: Id, header: chain_sync::Header) -> Vec<u8> {
    let mut out = Vec::with_capacity(162);
    out.push(1);
    out.extend(target);
    out.extend(header.id);
    out.push(u8::from(header.parent.is_some()));
    if let Some(parent) = header.parent {
        out.extend(parent);
    }
    out.extend(header.height.to_be_bytes());
    out.extend(header.credited_work.to_be_bytes());
    out.extend(header.cumulative_work.to_be_bytes());
    out.extend(header.parent_state_root);
    out.extend(header.state_root);
    out
}

fn decode_compact_hint(bytes: &[u8]) -> Result<(Id, chain_sync::Header)> {
    if bytes.len() < 1 + 32 + 32 + 1 + 8 + 16 + 16 + 32 + 32 {
        return Err(Error::Invalid("compact cursor encoding"));
    }
    let mut offset = 0;
    fn take<'a>(bytes: &'a [u8], offset: &mut usize, n: usize) -> Result<&'a [u8]> {
        let end = offset
            .checked_add(n)
            .ok_or(Error::Invalid("compact cursor encoding"))?;
        let value = bytes
            .get(*offset..end)
            .ok_or(Error::Invalid("compact cursor encoding"))?;
        *offset = end;
        Ok(value)
    }
    if take(bytes, &mut offset, 1)?[0] != 1 {
        return Err(Error::Invalid("compact cursor version"));
    }
    let target: Id = take(bytes, &mut offset, 32)?.try_into().unwrap();
    let id: Id = take(bytes, &mut offset, 32)?.try_into().unwrap();
    let parent = match take(bytes, &mut offset, 1)?[0] {
        0 => None,
        1 => Some(take(bytes, &mut offset, 32)?.try_into().unwrap()),
        _ => return Err(Error::Invalid("compact cursor parent")),
    };
    let u64v = |bytes: &[u8], offset: &mut usize| -> Result<u64> {
        Ok(u64::from_be_bytes(
            take(bytes, offset, 8)?.try_into().unwrap(),
        ))
    };
    let u128v = |bytes: &[u8], offset: &mut usize| -> Result<u128> {
        Ok(u128::from_be_bytes(
            take(bytes, offset, 16)?.try_into().unwrap(),
        ))
    };
    let header = chain_sync::Header {
        id,
        parent,
        height: u64v(bytes, &mut offset)?,
        credited_work: u128v(bytes, &mut offset)?,
        cumulative_work: u128v(bytes, &mut offset)?,
        parent_state_root: take(bytes, &mut offset, 32)?.try_into().unwrap(),
        state_root: take(bytes, &mut offset, 32)?.try_into().unwrap(),
    };
    if offset != bytes.len()
        || header.credited_work == 0
        || header.cumulative_work < header.credited_work
        || (header.height == 0) != header.parent.is_none()
    {
        return Err(Error::Invalid("compact cursor header"));
    }
    Ok((target, header))
}
pub fn start(
    address: SocketAddr,
    node: Arc<Mutex<Node>>,
    stop: Arc<AtomicBool>,
    max_sessions: usize,
) -> Result<thread::JoinHandle<Result<()>>> {
    let (handle, worker) = transport::start(address, node.clone(), stop, max_sessions)?;
    lock(&node)?.transport = Some(handle);
    Ok(worker)
}
pub struct Client {
    address: SocketAddr,
    protocol: Id,
    handle: Option<transport::Handle>,
    initialized: bool,
    last_heads: Instant,
    next_peers: Instant,
    compact_cursor: Option<(Id, chain_sync::Header)>,
    compact_pending: Option<(Id, chain_sync::Header)>,
    generation: Option<libp2p::swarm::ConnectionId>,
    inbound: Option<libp2p::swarm::ConnectionId>,
    observed_heads: Option<(libp2p::swarm::ConnectionId, Vec<Id>)>,
}
impl Drop for Client {
    fn drop(&mut self) {
        self.disconnect();
    }
}
impl Client {
    pub fn new(address: SocketAddr, protocol: Id) -> Self {
        Self {
            address: normalize(address),
            protocol,
            handle: None,
            initialized: false,
            generation: None,
            inbound: None,
            observed_heads: None,
            last_heads: Instant::now() - Duration::from_secs(30),
            next_peers: Instant::now(),
            compact_cursor: None,
            compact_pending: None,
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
        if lock(node)?.profile.protocol != self.protocol {
            return Err(Error::Invalid("client protocol"));
        }
        self.restore_compact_cursor(node)?;
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
            self.compact_cursor = None;
            self.compact_pending = None;
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
                    guard.peers.learn_from(address, self.address)?;
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
        let known = guard.advertised_heads()?;
        let snapshot = guard.sharechain.db().read()?;
        for tip in tips {
            let mut verified = false;
            for head in &known {
                if chain_sync::common_ancestor(&snapshot, *head, &[*tip])?.is_some() {
                    verified = true;
                    break;
                }
            }
            if !verified {
                return Ok(None);
            }
        }
        Ok(Some(*connection))
    }
    fn disconnect(&self) {
        if let (Some(handle), Some(connection)) = (&self.handle, self.generation) {
            handle.disconnect(self.address, connection);
        }
    }
    pub fn compact_headers(
        &mut self,
        locator: Vec<Id>,
        target: Id,
        start: Id,
        stop: &AtomicBool,
    ) -> Result<Vec<chain_sync::Header>> {
        let packet = self.request(
            Message::Locator {
                target,
                start,
                locator,
            },
            stop,
        )?;
        match packet.message() {
            Message::Headers { headers, .. } => Ok(headers),
            Message::Busy => Err(Error::Capacity("peer sharechain sync")),
            _ => Err(Error::Invalid("sharechain header reply")),
        }
    }

    pub fn compact_headers_bounded(
        &mut self,
        locator: Vec<Id>,
        target: Id,
        stop: &AtomicBool,
    ) -> Result<Vec<chain_sync::Header>> {
        let (target, previous) = self
            .compact_cursor
            .map_or((target, None), |(target, header)| (target, Some(header)));
        let start = previous.map_or([0; 32], |header| header.id);
        let request_locator = previous.map_or(locator, |header| vec![header.id]);
        let page = self.compact_headers(request_locator, target, start, stop)?;
        if let (Some(previous), Some(first)) = (previous, page.first()) {
            if first.parent != Some(previous.id)
                || previous.height.checked_add(1) != Some(first.height)
                || previous.cumulative_work.checked_add(first.credited_work)
                    != Some(first.cumulative_work)
                || previous.state_root != first.parent_state_root
            {
                return Err(Error::Invalid("sharechain continuation ancestry"));
            }
        }
        let pending = page.last().copied().map(|header| (target, header));
        self.compact_cursor = pending.filter(|(_, header)| header.id != target);
        self.compact_pending = pending;
        Ok(page)
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
                self.last_heads = Instant::now() - Duration::from_secs(30);
            }
            self.generation = packet.origin;
        }
        if result.is_err() {
            self.initialized = false;
            self.observed_heads = None;
        }
        result
    }

    fn restore_compact_cursor(&mut self, node: &Arc<Mutex<Node>>) -> Result<()> {
        if self.inbound.is_some() || self.compact_cursor.is_some() {
            return Ok(());
        }
        let value = lock(node)?
            .sharechain
            .db()
            .read_compact_cursor(self.protocol, &self.address.to_string())
            .map_err(|_| Error::Invalid("compact cursor read"))?;
        let Some(value) = value else {
            return Ok(());
        };
        let (target, cursor) = decode_compact_hint(&value)?;
        let durable = lock(node)?
            .sharechain
            .db()
            .read()
            .map_err(|_| Error::Invalid("compact cursor snapshot"))?
            .header(&cursor.id)
            .map_err(|_| Error::Invalid("compact cursor header"))?
            .is_some_and(|header| {
                header.parent == cursor.parent
                    && header.height == cursor.height
                    && header.work == cursor.credited_work
                    && header.cumulative_work == cursor.cumulative_work
                    && header.parent_state_root == cursor.parent_state_root
                    && header.state_root == cursor.state_root
            });
        if durable {
            self.compact_cursor = Some((target, cursor));
        } else {
            lock(node)?
                .sharechain
                .db()
                .write_compact_cursor(self.protocol, &self.address.to_string(), None)
                .map_err(|_| Error::Invalid("compact cursor discard"))?;
        }
        Ok(())
    }

    fn persist_compact_cursor(
        &self,
        node: &Arc<Mutex<Node>>,
        target: Id,
        cursor: Option<chain_sync::Header>,
    ) -> Result<()> {
        if self.inbound.is_some() {
            return Ok(());
        }
        let guard = lock(node)?;
        let db = guard.sharechain.db();
        let address = self.address.to_string();
        match cursor {
            Some(cursor) => db.write_compact_cursor(
                self.protocol,
                &address,
                Some(&encode_compact_hint(target, cursor)),
            ),
            None => db.write_compact_cursor(self.protocol, &address, None),
        }
        .map_err(|_| Error::Invalid("compact cursor persist"))
    }

    fn commit_compact_tip(&mut self, node: &Arc<Mutex<Node>>, tip: Id) -> Result<()> {
        let Some((target, boundary)) = self.compact_pending else {
            return Ok(());
        };
        if boundary.id != tip {
            return Ok(());
        }
        let previous = self.compact_cursor;
        let next = if boundary.id == target {
            None
        } else {
            Some((target, boundary))
        };
        if let Err(error) =
            self.persist_compact_cursor(node, target, next.map(|(_, header)| header))
        {
            self.compact_cursor = previous;
            return Err(error);
        }
        self.compact_pending = None;
        self.compact_cursor = next;
        Ok(())
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
    let (tips, compact) = if session.last_heads.elapsed() >= Duration::from_secs(30) {
        let heads = session.request(Message::Heads, stop)?;
        let tips = match heads.message() {
            Message::Tips(tips) => {
                session.last_heads = Instant::now();
                session.observed_heads = session.generation.map(|id| (id, tips.clone()));
                tips
            }
            Message::Busy => return Ok(0),
            _ => return Err(Error::Invalid("peer heads response")),
        };
        let locator = { lock(node)?.sharechain_locator()? };
        let compact = match (locator, tips.first().copied()) {
            (Some(locator), Some(target)) => {
                match session.compact_headers_bounded(locator, target, stop) {
                    Ok(page) => page,
                    Err(Error::Capacity(_)) => vec![],
                    Err(error) => return Err(error),
                }
            }
            _ => vec![],
        };
        (tips, compact)
    } else if let Some((target, header)) = session.compact_cursor {
        let compact = match session.compact_headers_bounded(vec![header.id], target, stop) {
            Ok(page) => page,
            Err(Error::Capacity(_)) => vec![],
            Err(error) => return Err(error),
        };
        (vec![], compact)
    } else {
        (vec![], vec![])
    };
    let mut queue: VecDeque<_> = lock(node)?
        .acquisition_heads()?
        .into_iter()
        .chain(tips)
        .chain(compact.into_iter().map(|header| header.id))
        .chain(session.handle.as_ref().unwrap().notices(session.address))
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();
    let known: BTreeSet<_> = lock(node)?.advertised_heads()?.into_iter().collect();
    queue.retain(|tip| !known.contains(tip));
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
        let turn = {
            let mut guard = lock(node)?;
            guard.sync_cursor = Some(tip);
            guard.take_turn(tip)?
        };
        let Some(turn) = turn else {
            break;
        };
        let (turn, outcome) = turn.run();
        let outcome = lock(node)?.finish_turn(turn, outcome);
        let missing = match outcome {
            Ok(EvaluationProgress::Ready(_)) => {
                admitted += 1;
                session.commit_compact_tip(node, tip)?;
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
            Err(Error::Capacity(_)) => {
                queue.push_back(tip);
                continue;
            }
            Err(error) => return Err(error),
        };
        let keys = if missing.kind == Kind::Core {
            vec![missing, Key::new(Kind::Authorization, missing.id)]
        } else {
            vec![missing]
        };
        lock(node)?.protect_download(tip, &keys)?;
        let shared = lock(node)?.downloads.clone();
        let Some(lease) = download::Downloads::reserve(&shared, session.address, keys.clone())
        else {
            continue;
        };
        let packet = session.request(Message::Get(keys.clone()), stop)?;
        let (objects, absent, deferred) = match packet.message() {
            Message::Reply {
                objects,
                missing: absent,
                deferred,
            } => (objects, absent, deferred),
            Message::Busy => {
                queue.push_back(tip);
                continue;
            }
            _ => return Err(Error::Invalid("object reply")),
        };
        lease.absent(session.address, &absent);
        lease.deferred(session.address, &deferred);
        stage_partial(node, objects, session.address)?;
        drop(packet);
        drop(lease);
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
                lock(node)?.protect_download(tip, &keys)?;
                let Some(lease) =
                    download::Downloads::reserve(&shared, session.address, keys.clone())
                else {
                    continue;
                };
                let packet = session.request(Message::Get(keys), stop)?;
                let (objects, absent, deferred) = match packet.message() {
                    Message::Reply {
                        objects,
                        missing: absent,
                        deferred,
                    } => (objects, absent, deferred),
                    Message::Busy => {
                        break;
                    }
                    _ => return Err(Error::Invalid("object reply")),
                };
                lease.absent(session.address, &absent);
                lease.deferred(session.address, &deferred);
                let progress = !objects.is_empty();
                if progress {
                    stage(node, objects)?;
                }
                if !progress || !deferred.is_empty() {
                    break;
                }
            }
        }
        queue.push_back(tip);
    }
    Ok(admitted)
}

#[cfg(test)]
#[path = "discovery_tests.rs"]
mod discovery_tests;
