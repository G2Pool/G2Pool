use super::{
    budget::Budgets,
    codec::{Codec, Packet, Request, CONTROL, DEADLINE},
    lock,
    wire::Message,
};
use crate::{
    codec::{hash, Id},
    node::Node,
    Error, Result,
};
use libp2p::{
    futures::StreamExt,
    noise, ping, request_response as rpc,
    swarm::{NetworkBehaviour, SwarmEvent},
    tcp, yamux, Multiaddr, PeerId, StreamProtocol, Swarm, SwarmBuilder,
};
use std::{
    collections::{BTreeMap, BTreeSet},
    io,
    net::SocketAddr,
    sync::{
        atomic::{AtomicBool, Ordering},
        mpsc, Arc, Mutex,
    },
    thread,
    time::{Duration, Instant},
};

#[derive(NetworkBehaviour)]
struct Behaviour {
    admission: super::admission::Gate,
    control: rpc::Behaviour<Codec>,
    bulk: rpc::Behaviour<Codec>,
    ping: ping::Behaviour,
}
const MAX_YAMUX_STREAMS: usize = 12;
fn swarm(
    protocol: Id,
    budgets: Budgets,
    max_inbound: u32,
    bans: super::bans::IpBans,
) -> Result<Swarm<Behaviour>> {
    let schema = hash(
        "POOL_WIRE_SCHEMA",
        b"heads;hello-port;peers;notice;ack;busy;get;partitioned-reply;typed-lengths;locator;target-pinned-compact-pages-v2;body-hydration",
    );
    let mut prologue = b"G2Pool transport\0".to_vec();
    prologue.extend(protocol);
    prologue.extend(schema);
    let suffix: String = protocol
        .iter()
        .chain(schema.iter())
        .map(|b| format!("{b:02x}"))
        .collect();
    let control = StreamProtocol::try_from_owned(format!("/g2pool/{suffix}/control"))
        .map_err(|_| Error::Invalid("stream protocol"))?;
    let bulk = StreamProtocol::try_from_owned(format!("/g2pool/{suffix}/objects"))
        .map_err(|_| Error::Invalid("stream protocol"))?;
    Ok(SwarmBuilder::with_new_identity()
        .with_tokio()
        .with_tcp(
            tcp::Config::default().nodelay(true),
            move |key: &libp2p::identity::Keypair| {
                noise::Config::new(key).map(|config| config.with_prologue(prologue.clone()))
            },
            || {
                let mut config = yamux::Config::default();
                config.set_max_num_streams(MAX_YAMUX_STREAMS);
                config.set_max_connection_receive_window(MAX_YAMUX_STREAMS * yamux::DEFAULT_CREDIT);
                config
            },
        )
        .map_err(|_| Error::Invalid("Noise transport"))?
        .with_behaviour(|key| Behaviour {
            control: rpc::Behaviour::with_codec(
                Codec {
                    budgets: budgets.clone(),
                    bulk: false,
                    expected: None,
                },
                [(control, rpc::ProtocolSupport::Full)],
                rpc::Config::default()
                    .with_max_concurrent_streams(8)
                    .with_request_timeout(Duration::from_secs(5)),
            ),
            bulk: rpc::Behaviour::with_codec(
                Codec {
                    budgets,
                    bulk: true,
                    expected: None,
                },
                [(bulk, rpc::ProtocolSupport::Full)],
                rpc::Config::default()
                    .with_max_concurrent_streams(2)
                    .with_request_timeout(DEADLINE),
            ),
            ping: ping::Behaviour::new(
                ping::Config::new()
                    .with_interval(Duration::from_secs(15))
                    .with_timeout(Duration::from_secs(10)),
            ),
            admission: super::admission::Gate::with_bans(
                max_inbound.min(crate::network::INBOUND_PEERS as u32) as usize,
                key.public().to_peer_id(),
                bans,
            ),
        })
        .map_err(|_| Error::Invalid("swarm behaviour"))?
        .with_swarm_config(|config| {
            config
                .with_max_negotiating_inbound_streams(MAX_YAMUX_STREAMS)
                .with_per_connection_event_buffer_size(16)
                .with_idle_connection_timeout(Duration::from_secs(90))
        })
        .build())
}
fn multiaddr(addr: SocketAddr) -> Multiaddr {
    match addr {
        SocketAddr::V4(a) => format!("/ip4/{}/tcp/{}", a.ip(), a.port()),
        SocketAddr::V6(a) => format!("/ip6/{}/tcp/{}", a.ip(), a.port()),
    }
    .parse()
    .expect("numeric endpoint")
}
fn endpoint(addr: &Multiaddr) -> Option<SocketAddr> {
    use libp2p::multiaddr::Protocol;
    let mut iter = addr.iter();
    let ip = match iter.next()? {
        Protocol::Ip4(ip) => ip.into(),
        Protocol::Ip6(ip) => ip.into(),
        _ => return None,
    };
    let Protocol::Tcp(port) = iter.next()? else {
        return None;
    };
    Some(super::normalize(SocketAddr::new(ip, port)))
}
struct Close {
    address: SocketAddr,
    connection: libp2p::swarm::ConnectionId,
}
struct Command {
    address: SocketAddr,
    request: Request,
    response: mpsc::SyncSender<Result<Packet>>,
    expires: Instant,
}
#[derive(Clone)]
pub(crate) struct Handle {
    send: tokio::sync::mpsc::Sender<Command>,
    close: tokio::sync::mpsc::Sender<Close>,
    dialed: Arc<Mutex<BTreeMap<PeerId, SocketAddr>>>,
    bans: super::bans::IpBans,
    budgets: Budgets,
    notices: Arc<Mutex<BTreeMap<SocketAddr, BTreeSet<Id>>>>,
    pub(crate) connected: Arc<std::sync::atomic::AtomicUsize>,
}
impl Handle {
    pub(super) fn request(
        &self,
        address: SocketAddr,
        message: Message,
        stop: &AtomicBool,
    ) -> Result<Packet> {
        if self.bans.contains(address.ip()) {
            return Err(Error::Capacity("peer IP banned"));
        }
        let ceiling = message.response_ceiling()?;
        let pool = if message.is_bulk() {
            &self.budgets.receive
        } else {
            &self.budgets.control
        };
        let ticket = pool
            .reserve(
                ceiling
                    .checked_mul(6)
                    .ok_or(Error::Capacity("network receive bytes"))?,
            )
            .ok_or(Error::Capacity("network receive bytes"))?;
        let (response, receive) = mpsc::sync_channel(1);
        let duration = if message.is_bulk() {
            DEADLINE
        } else {
            Duration::from_secs(5)
        };
        self.send
            .try_send(Command {
                address: super::normalize(address),
                request: Request { message, ticket },
                response,
                expires: Instant::now() + duration,
            })
            .map_err(|_| Error::Capacity("network command queue"))?;
        let expires = Instant::now() + duration;
        loop {
            if stop.load(Ordering::Acquire) {
                return Err(Error::Cancelled);
            }
            match receive.recv_timeout(Duration::from_millis(25)) {
                Ok(result) => return result,
                Err(mpsc::RecvTimeoutError::Disconnected) => return Err(Error::Cancelled),
                Err(_) if Instant::now() >= expires => {
                    return Err(io::Error::from(io::ErrorKind::TimedOut).into())
                }
                Err(_) => {}
            }
        }
    }
    pub(super) fn disconnect(&self, address: SocketAddr, connection: libp2p::swarm::ConnectionId) {
        let _ = self.close.try_send(Close {
            address: super::normalize(address),
            connection,
        });
    }
    pub(super) fn reached(&self, address: SocketAddr) -> bool {
        self.dialed
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .values()
            .any(|a| *a == super::normalize(address))
    }
    pub(super) fn notices(&self, addr: SocketAddr) -> Vec<Id> {
        self.notices
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&super::normalize(addr))
            .unwrap_or_default()
            .into_iter()
            .collect()
    }
}
struct Flight {
    response: Option<mpsc::SyncSender<Result<Packet>>>,
    peer: PeerId,
    connection: libp2p::swarm::ConnectionId,
    expires: Instant,
}
struct Completed {
    peer: PeerId,
    connection: libp2p::swarm::ConnectionId,
    channel: rpc::ResponseChannel<Packet>,
    response: Result<Packet>,
}

pub(super) fn start(
    address: SocketAddr,
    node: Arc<Mutex<Node>>,
    stop: Arc<AtomicBool>,
    max_inbound: usize,
) -> Result<(Handle, thread::JoinHandle<Result<()>>)> {
    let (protocol, bans) = {
        let guard = lock(&node)?;
        (guard.profile.protocol, guard.peers.bans.clone())
    };
    let kind = super::chain_type(&node)?;
    let budgets = Budgets::default();
    let (send, receive) = tokio::sync::mpsc::channel(8);
    let (close, closing) = tokio::sync::mpsc::channel(8);
    let handle = Handle {
        send,
        close,
        bans: bans.clone(),
        dialed: Arc::new(Mutex::new(BTreeMap::new())),
        budgets: budgets.clone(),
        notices: Arc::new(Mutex::new(BTreeMap::new())),
        connected: lock(&node)?.connected.clone(),
    };
    let shared = handle.clone();
    let startup_stop = stop.clone();
    let (ready, startup) = mpsc::sync_channel(1);
    let worker = thread::spawn(move || {
        grin_core::global::set_local_chain_type(kind);
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .max_blocking_threads(2)
            .build()?;
        runtime.block_on(async move {
            let mut swarm = swarm(
                protocol,
                budgets,
                max_inbound.min(crate::network::INBOUND_PEERS) as u32,
                bans,
            )?;
            swarm
                .listen_on(multiaddr(address))
                .map_err(|_| Error::Invalid("pool listener"))?;
            run(&mut swarm, node, stop, shared, (receive, closing), ready).await
        })
    });
    match startup.recv_timeout(Duration::from_secs(5)) {
        Ok(Ok(())) => Ok((handle, worker)),
        Ok(Err(error)) => {
            startup_stop.store(true, Ordering::Release);
            Err(error)
        }
        Err(_) => {
            startup_stop.store(true, Ordering::Release);
            Err(Error::Invalid("pool transport startup"))
        }
    }
}
async fn run(
    swarm: &mut Swarm<Behaviour>,
    node: Arc<Mutex<Node>>,
    stop: Arc<AtomicBool>,
    handle: Handle,
    channels: (
        tokio::sync::mpsc::Receiver<Command>,
        tokio::sync::mpsc::Receiver<Close>,
    ),
    ready: mpsc::SyncSender<Result<()>>,
) -> Result<()> {
    let (mut commands, mut closing) = channels;
    let (completed, mut results) = tokio::sync::mpsc::channel(2);
    let mut state = Manager {
        connected: BTreeMap::new(),
        live: BTreeMap::new(),
        pending: BTreeMap::new(),
        control: BTreeMap::new(),
        bulk: BTreeMap::new(),
        traffic: BTreeMap::new(),
        last_heads: BTreeSet::new(),
        last_repair: Instant::now(),
        completed,
        replacing: BTreeMap::new(),
    };
    let published = lock(&node)?.published.clone();
    let mut tick = tokio::time::interval(Duration::from_millis(25));
    while !stop.load(Ordering::Acquire) {
        tokio::select! {
            _=tick.tick()=>state.tick(swarm,&handle,&published),
            Some(close)=closing.recv()=>state.close(swarm,close),
            Some(command)=commands.recv()=>state.command(swarm,command),
            Some(done)=results.recv()=>state.complete(swarm,done),
            event=swarm.select_next_some()=>state.event(swarm,event,&node,&handle,&ready)?,
        }
    }
    handle.connected.store(0, Ordering::Release);
    Ok(())
}
struct Manager {
    connected: BTreeMap<SocketAddr, PeerId>,
    live: BTreeMap<PeerId, libp2p::swarm::ConnectionId>,
    pending: BTreeMap<libp2p::swarm::ConnectionId, Command>,
    control: BTreeMap<rpc::OutboundRequestId, Flight>,
    bulk: BTreeMap<rpc::OutboundRequestId, Flight>,
    traffic: BTreeMap<PeerId, Traffic>,
    last_heads: BTreeSet<Id>,
    last_repair: Instant,
    completed: tokio::sync::mpsc::Sender<Completed>,
    replacing: BTreeMap<PeerId, libp2p::swarm::ConnectionId>,
}
impl Manager {
    fn tick(
        &mut self,
        swarm: &mut Swarm<Behaviour>,
        handle: &Handle,
        published: &Arc<std::sync::RwLock<crate::node::Published>>,
    ) {
        let now = Instant::now();
        self.pending.retain(|_, command| {
            if command.expires <= now {
                let _ = command
                    .response
                    .try_send(Err(io::Error::from(io::ErrorKind::TimedOut).into()));
                false
            } else {
                true
            }
        });
        for flights in [&mut self.control, &mut self.bulk] {
            flights.retain(|_, flight| {
                if flight.expires <= now {
                    if let Some(send) = &flight.response {
                        let _ = send.try_send(Err(io::Error::from(io::ErrorKind::TimedOut).into()));
                    }
                    false
                } else {
                    true
                }
            });
        }
        let heads = published.read().map(|snapshot| snapshot.heads.clone());
        if let Ok(heads) = heads {
            let heads: BTreeSet<_> = heads.into_iter().collect();
            let ids: Vec<_> = heads.difference(&self.last_heads).copied().collect();
            if !ids.is_empty() {
                for peer in self.connected.values().copied().collect::<BTreeSet<_>>() {
                    if self.replacing.contains_key(&peer) {
                        continue;
                    }
                    if self.control.values().filter(|f| f.peer == peer).count() >= 4 {
                        continue;
                    }
                    if let Some(ticket) = handle.budgets.control.reserve(CONTROL * 6) {
                        let id = swarm.behaviour_mut().control.send_request(
                            &peer,
                            Request {
                                message: Message::Notice(ids.clone()),
                                ticket,
                            },
                        );
                        self.control.insert(
                            id,
                            Flight {
                                response: None,
                                peer,
                                connection: self.live[&peer],
                                expires: now + Duration::from_secs(5),
                            },
                        );
                    }
                }
            }
            self.last_heads = heads;
        }
        if self.last_repair.elapsed() >= Duration::from_secs(30) {
            self.last_heads.clear();
            self.last_repair = now;
        }
    }
    fn close(&self, swarm: &mut Swarm<Behaviour>, close: Close) {
        if let Some(peer) = self.connected.get(&close.address) {
            if self.live.get(peer) == Some(&close.connection) {
                let _ = swarm.disconnect_peer_id(*peer);
            }
        }
    }
    fn command(&mut self, swarm: &mut Swarm<Behaviour>, command: Command) {
        if swarm.behaviour().admission.ip_banned(command.address.ip()) {
            let _ = command
                .response
                .try_send(Err(Error::Capacity("peer IP banned")));
            return;
        }
        if let Some(&peer) = self.connected.get(&command.address) {
            if self.replacing.contains_key(&peer) {
                let _ = command
                    .response
                    .try_send(Err(Error::Capacity("connection settling")));
                return;
            }
            send(
                swarm,
                command,
                peer,
                &mut self.control,
                &mut self.bulk,
                &self.live,
            );
        } else if self.pending.len() >= crate::network::OUTBOUND_PENDING
            || self
                .pending
                .values()
                .any(|pending| pending.address == command.address)
        {
            let _ = command
                .response
                .try_send(Err(Error::Capacity("pending pool dials")));
        } else {
            let options = libp2p::swarm::dial_opts::DialOpts::unknown_peer_id()
                .address(multiaddr(command.address))
                .allocate_new_port()
                .build();
            let connection = options.connection_id();
            match swarm.dial(options) {
                Ok(()) => {
                    self.pending.insert(connection, command);
                }
                Err(_) => {
                    let _ = command.response.try_send(Err(Error::Invalid("pool dial")));
                }
            }
        }
    }
    fn complete(&self, swarm: &mut Swarm<Behaviour>, done: Completed) {
        if self.live.get(&done.peer) == Some(&done.connection) {
            match done.response {
                Ok(response) => {
                    let _ = swarm
                        .behaviour_mut()
                        .bulk
                        .send_response(done.channel, response);
                }
                Err(_error) => {
                    let _ = swarm.disconnect_peer_id(done.peer);
                }
            }
        }
    }
    fn event(
        &mut self,
        swarm: &mut Swarm<Behaviour>,
        event: SwarmEvent<BehaviourEvent>,
        node: &Arc<Mutex<Node>>,
        handle: &Handle,
        ready: &mpsc::SyncSender<Result<()>>,
    ) -> Result<()> {
        match event {
            SwarmEvent::NewListenAddr { .. } => {
                let _ = ready.try_send(Ok(()));
            }
            SwarmEvent::ListenerError { error, .. } => {
                let _ = ready.try_send(Err(Error::Io(error)));
                return Err(Error::Invalid("pool listener failed"));
            }

            SwarmEvent::ConnectionEstablished {
                peer_id,
                connection_id,
                endpoint: point,
                ..
            } => {
                let remote = endpoint(point.get_remote_address());
                if let Some(old) = self.live.insert(peer_id, connection_id) {
                    self.replacing.insert(peer_id, old);
                    for flights in [&mut self.control, &mut self.bulk] {
                        flights.retain(|_, flight| {
                            if flight.peer == peer_id && flight.connection == old {
                                if let Some(send) = &flight.response {
                                    let _ =
                                        send.try_send(Err(Error::Capacity("connection replaced")));
                                }
                                false
                            } else {
                                true
                            }
                        });
                    }
                }
                if let Some(addr) = remote {
                    if point.is_dialer() {
                        handle
                            .dialed
                            .lock()
                            .unwrap_or_else(|e| e.into_inner())
                            .insert(peer_id, addr);
                    }
                    self.connected.insert(addr, peer_id);
                    if let Some(command) = self.pending.remove(&connection_id) {
                        if self.replacing.contains_key(&peer_id) {
                            self.pending.insert(connection_id, command);
                        } else {
                            send(
                                swarm,
                                command,
                                peer_id,
                                &mut self.control,
                                &mut self.bulk,
                                &self.live,
                            );
                        }
                    }
                }
                handle.connected.store(self.live.len(), Ordering::Release);
            }
            SwarmEvent::ConnectionClosed {
                peer_id,
                connection_id,
                ..
            } => {
                if self.live.get(&peer_id) != Some(&connection_id) {
                    if self.replacing.get(&peer_id) == Some(&connection_id) {
                        self.replacing.remove(&peer_id);
                        if let Some(current) = self.live.get(&peer_id) {
                            if let Some(command) = self.pending.remove(current) {
                                send(
                                    swarm,
                                    command,
                                    peer_id,
                                    &mut self.control,
                                    &mut self.bulk,
                                    &self.live,
                                );
                            }
                        }
                    }
                    return Ok(());
                }
                self.replacing.remove(&peer_id);
                self.live.remove(&peer_id);
                handle
                    .dialed
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .remove(&peer_id);
                self.connected.retain(|_, p| *p != peer_id);
                self.traffic.remove(&peer_id);
                handle
                    .notices
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .retain(|a, _| self.connected.contains_key(a));
                for flights in [&mut self.control, &mut self.bulk] {
                    flights.retain(|_, f| {
                        if f.peer == peer_id {
                            if let Some(send) = &f.response {
                                let _ = send.try_send(Err(io::Error::from(
                                    io::ErrorKind::ConnectionAborted,
                                )
                                .into()));
                            }
                            false
                        } else {
                            true
                        }
                    });
                }
                handle.connected.store(self.live.len(), Ordering::Release);
            }
            SwarmEvent::OutgoingConnectionError {
                connection_id,
                error,
                ..
            } => {
                if let Some(command) = self.pending.remove(&connection_id) {
                    let error = match error {
                        libp2p::swarm::DialError::Denied { .. } => {
                            Error::Capacity("pool connection slots")
                        }
                        other => Error::Io(io::Error::other(format!("pool dial: {other}"))),
                    };
                    let _ = command.response.try_send(Err(error));
                }
            }
            SwarmEvent::Behaviour(BehaviourEvent::Control(event)) => handle_rpc(
                swarm,
                event,
                false,
                &mut self.control,
                Service {
                    node,
                    handle,
                    connected: &mut self.connected,
                    completed: &self.completed,
                    traffic: &mut self.traffic,
                    live: &self.live,
                },
            ),
            SwarmEvent::Behaviour(BehaviourEvent::Bulk(event)) => handle_rpc(
                swarm,
                event,
                true,
                &mut self.bulk,
                Service {
                    node,
                    handle,
                    connected: &mut self.connected,
                    completed: &self.completed,
                    traffic: &mut self.traffic,
                    live: &self.live,
                },
            ),
            SwarmEvent::Behaviour(BehaviourEvent::Ping(ping::Event {
                peer,
                connection,
                result: Err(_),
                ..
            })) if self.live.get(&peer) == Some(&connection) => {
                let _ = swarm.disconnect_peer_id(peer);
            }
            _ => {}
        }
        Ok(())
    }
}

fn send(
    swarm: &mut Swarm<Behaviour>,
    command: Command,
    peer: PeerId,
    control: &mut BTreeMap<rpc::OutboundRequestId, Flight>,
    bulk: &mut BTreeMap<rpc::OutboundRequestId, Flight>,
    live: &BTreeMap<PeerId, libp2p::swarm::ConnectionId>,
) {
    let is_bulk = command.request.message.is_bulk();
    let flights = if is_bulk { bulk } else { control };
    if flights.values().filter(|f| f.peer == peer).count() >= if is_bulk { 2 } else { 4 } {
        let _ = command
            .response
            .try_send(Err(Error::Capacity("peer requests")));
        return;
    }
    let rpc = if is_bulk {
        &mut swarm.behaviour_mut().bulk
    } else {
        &mut swarm.behaviour_mut().control
    };
    let id = rpc.send_request(&peer, command.request);
    flights.insert(
        id,
        Flight {
            response: Some(command.response),
            peer,
            connection: live[&peer],
            expires: command.expires,
        },
    );
}
struct Traffic {
    start: Instant,
    control: usize,
    bulk: usize,
}
impl Traffic {
    fn new() -> Self {
        Self {
            start: Instant::now(),
            control: 0,
            bulk: 0,
        }
    }
    fn allow(&mut self, bulk: bool) -> bool {
        if self.start.elapsed() >= Duration::from_secs(1) {
            *self = Self::new();
        }
        let (count, limit) = if bulk {
            (&mut self.bulk, 16)
        } else {
            (&mut self.control, 64)
        };
        if *count >= limit {
            return false;
        }
        *count += 1;
        true
    }
}
struct Service<'a> {
    node: &'a Arc<Mutex<Node>>,
    handle: &'a Handle,
    connected: &'a mut BTreeMap<SocketAddr, PeerId>,
    completed: &'a tokio::sync::mpsc::Sender<Completed>,
    traffic: &'a mut BTreeMap<PeerId, Traffic>,
    live: &'a BTreeMap<PeerId, libp2p::swarm::ConnectionId>,
}

fn handle_rpc(
    swarm: &mut Swarm<Behaviour>,
    event: rpc::Event<Request, Packet>,
    bulk: bool,
    flights: &mut BTreeMap<rpc::OutboundRequestId, Flight>,
    service: Service<'_>,
) {
    let Service {
        node,
        handle,
        connected,
        completed,
        traffic,
        live,
    } = service;
    match event {
        rpc::Event::Message {
            peer,
            connection_id,
            message,
        } if live.get(&peer) == Some(&connection_id) => match message {
            rpc::Message::Response {
                request_id,
                mut response,
            } => {
                response.origin = Some(connection_id);
                if let Some(flight) = flights.remove(&request_id) {
                    if flight.peer == peer && flight.connection == connection_id {
                        if let Some(send) = flight.response {
                            let _ = send.try_send(Ok(response));
                        }
                    }
                }
            }
            rpc::Message::Request {
                request, channel, ..
            } => {
                if !traffic.entry(peer).or_insert_with(Traffic::new).allow(bulk) {
                    let response = Packet::new(Message::Busy, request.ticket);
                    let rpc = if bulk {
                        &mut swarm.behaviour_mut().bulk
                    } else {
                        &mut swarm.behaviour_mut().control
                    };
                    let _ = rpc.send_response(channel, response);
                    return;
                }
                if bulk {
                    let prepared: Result<_> = (|| {
                        let guard = node
                            .try_lock()
                            .map_err(|_| Error::Capacity("pool writer busy"))?;
                        let serving = guard
                            .serve_work
                            .acquire()
                            .ok_or(Error::Capacity("peer serving slots"))?;
                        let work = guard
                            .peer_work
                            .acquire()
                            .ok_or(Error::Capacity("peer native work slots"))?;
                        let ticket = handle
                            .budgets
                            .serve
                            .reserve(
                                request
                                    .message
                                    .response_ceiling()?
                                    .checked_mul(6)
                                    .ok_or(Error::Capacity("network serve bytes"))?,
                            )
                            .ok_or(Error::Capacity("network serve bytes"))?;
                        Ok((
                            guard.store.read_source()?,
                            work,
                            serving,
                            ticket,
                            guard.profile.manifest.network,
                        ))
                    })();
                    match prepared {
                        Ok((source, work, serving, ticket, kind)) => {
                            let completed = completed.clone();
                            tokio::task::spawn_blocking(move || {
                                let kind = match kind {
                                    0 => grin_core::global::ChainTypes::Mainnet,
                                    1 => grin_core::global::ChainTypes::Testnet,
                                    2 => grin_core::global::ChainTypes::AutomatedTesting,
                                    _ => grin_core::global::ChainTypes::UserTesting,
                                };
                                grin_core::global::set_local_chain_type(kind);
                                let _work = work;
                                let _serving = serving;
                                let response = match request.message {
                                    Message::Get(keys) => super::serve_objects(source, keys)
                                        .map(|message| Packet::new(message, ticket)),
                                    _ => Err(Error::Invalid("bulk request")),
                                };
                                let _ = completed.blocking_send(Completed {
                                    peer,
                                    connection: connection_id,
                                    channel,
                                    response,
                                });
                            });
                        }
                        Err(Error::Capacity(_)) => {
                            if let Some(ticket) = handle.budgets.control.reserve(1) {
                                let response = Packet::new(Message::Busy, ticket);
                                let _ = swarm.behaviour_mut().bulk.send_response(channel, response);
                            }
                        }
                        Err(_error) => {
                            let _ = swarm.disconnect_peer_id(peer);
                        }
                    }
                } else {
                    let reply = (|| {
                        let mut node = node
                            .try_lock()
                            .map_err(|_| Error::Capacity("pool writer busy"))?;
                        Ok(match request.message {
                            Message::Heads => Message::Tips(
                                node.published
                                    .read()
                                    .map_err(|_| Error::Invalid("publication snapshot poisoned"))?
                                    .heads
                                    .clone(),
                            ),
                            Message::Locator {
                                target,
                                start,
                                locator,
                            } => {
                                let snapshot = node
                                    .sharechain
                                    .db()
                                    .read()
                                    .map_err(|_| Error::Invalid("sharechain snapshot"))?;
                                let common = if start == [0; 32] {
                                    super::chain_sync::common_ancestor(&snapshot, target, &locator)?
                                } else {
                                    super::chain_sync::ChainIndex::header(&snapshot, start)?
                                };
                                let Some(common) = common else {
                                    return Ok(Message::Busy);
                                };
                                let page = super::chain_sync::header_range(
                                    &snapshot,
                                    target,
                                    common.id,
                                    super::chain_sync::MAX_HEADERS,
                                )?;
                                Message::Headers {
                                    target,
                                    start,
                                    headers: page.headers,
                                }
                            }
                            Message::Peers => {
                                let mut addresses = node.peers.addresses();
                                addresses.retain(|address| connected.get(address) != Some(&peer));
                                Message::Addresses(addresses)
                            }
                            Message::Announce(port) => {
                                if port != 0 {
                                    if let Some(addr) = connected
                                        .iter()
                                        .find(|(_, p)| **p == peer)
                                        .map(|(addr, _)| *addr)
                                    {
                                        let address = SocketAddr::new(addr.ip(), port);
                                        node.peers.learn_from(address, addr)?;
                                    }
                                }
                                Message::Addresses(vec![])
                            }
                            Message::Notice(ids) => {
                                let mut notices =
                                    handle.notices.lock().unwrap_or_else(|e| e.into_inner());
                                if let Some((addr, _)) = connected.iter().find(|(_, p)| **p == peer)
                                {
                                    let entries = notices.entry(*addr).or_default();
                                    for id in ids {
                                        if entries.len() < 64 {
                                            entries.insert(id);
                                        }
                                    }
                                }
                                Message::Ack
                            }
                            _ => return Err(Error::Invalid("control request")),
                        })
                    })();
                    match reply {
                        Ok(message) => {
                            let response = Packet::new(message, request.ticket);
                            let _ = swarm
                                .behaviour_mut()
                                .control
                                .send_response(channel, response);
                        }
                        Err(Error::Capacity(_)) => {
                            let response = Packet::new(Message::Busy, request.ticket);
                            let _ = swarm
                                .behaviour_mut()
                                .control
                                .send_response(channel, response);
                        }
                        Err(_error) => {
                            let _ = swarm.disconnect_peer_id(peer);
                        }
                    }
                }
            }
        },
        rpc::Event::OutboundFailure {
            peer,
            request_id,
            error,
            connection_id,
        } if live.get(&peer) == Some(&connection_id) => {
            if matches!(&error, rpc::OutboundFailure::Io(error) if super::codec::is_remote_protocol_error(error))
            {
                swarm
                    .behaviour_mut()
                    .admission
                    .ban_connection(connection_id);
            }
            if let Some(flight) = flights.remove(&request_id) {
                if let Some(send) = flight.response {
                    let _ = send.try_send(Err(Error::Io(io::Error::other(format!(
                        "pool RPC: {error}"
                    )))));
                }
            }
            if flights
                .values()
                .any(|flight| flight.peer == peer && flight.connection == connection_id)
                || swarm.is_connected(&peer)
            {
                let _ = swarm.disconnect_peer_id(peer);
            }
        }
        rpc::Event::InboundFailure {
            peer,
            error: rpc::InboundFailure::Io(error),
            connection_id,
            ..
        } if live.get(&peer) == Some(&connection_id)
            && super::codec::is_remote_protocol_error(&error) =>
        {
            swarm
                .behaviour_mut()
                .admission
                .ban_connection(connection_id);
            let _ = swarm.disconnect_peer_id(peer);
        }
        _ => {}
    }
}

#[cfg(test)]
#[path = "transport_tests.rs"]
mod tests;
