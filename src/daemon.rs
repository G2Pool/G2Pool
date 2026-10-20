use crate::{
    limits::Limits,
    mining::{
        stratum::{self, Mining},
        Template,
    },
    native::{self, Profile},
    node::Node,
    objects::hex,
    storage::{StorageLimits, Store},
    wallet::payments::{DirectNodeClient, Payments},
    Error, Result,
};
use grin_core::{
    core::hash::Hashed,
    global::{self, ChainTypes},
};
use grin_servers::{Server, ServerConfig};
use std::{
    fs,
    net::{IpAddr, SocketAddr, TcpListener},
    path::PathBuf,
    sync::{atomic::Ordering, Arc, Mutex},
    thread,
    time::{Duration, Instant},
};
pub struct Config {
    pub data: PathBuf,
    pub network: ChainTypes,
    pub stratum: SocketAddr,
    pub peer: SocketAddr,
    pub grin_host: IpAddr,
    pub grin_port: u16,
    pub evaluation: Limits,
    pub storage: StorageLimits,
    pub jobs: stratum::Retention,
}
#[derive(serde::Serialize, serde::Deserialize)]
pub struct Instance {
    network: ChainTypes,
    stratum: SocketAddr,
    peer: SocketAddr,
    grin_host: IpAddr,
    grin_port: u16,
}
impl Instance {
    pub fn from_config(c: &Config) -> Self {
        Self {
            network: c.network,
            stratum: c.stratum,
            peer: c.peer,
            grin_host: c.grin_host,
            grin_port: c.grin_port,
        }
    }
}
fn default_grin_host() -> IpAddr {
    IpAddr::V4(std::net::Ipv4Addr::UNSPECIFIED)
}
fn read_config(path: &std::path::Path) -> Result<Vec<u8>> {
    crate::secure_file::read_regular(path, 16384, false)
        .map_err(|e| Error::NativeUnavailable(format!("configuration {}: {e}", path.display())))
}
pub fn publish_config(path: &std::path::Path, bytes: &[u8]) -> Result<()> {
    crate::secure_file::publish_atomic(path, bytes, true)
}
pub fn private_api_secret(path: &std::path::Path) -> Result<String> {
    let secret = hex(&rand::random::<[u8; 32]>());
    crate::secure_file::publish_atomic(path, format!("{secret}\n").as_bytes(), true)?;
    Ok(secret)
}
impl Config {
    pub(crate) fn load(
        base: PathBuf,
        network: ChainTypes,
        stratum: Option<SocketAddr>,
        peer: Option<SocketAddr>,
        grin: Option<SocketAddr>,
    ) -> Result<Self> {
        let testnet = network == ChainTypes::Testnet;
        let data = crate::network_data_dir(base, if testnet { "testnet" } else { "mainnet" });
        let instance_path = data.join("instance.json");
        let saved: Option<Instance> = match fs::symlink_metadata(&instance_path) {
            Ok(_) => Some(serde_json::from_slice(&read_config(&instance_path)?)?),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
            Err(error) => return Err(error.into()),
        };
        if saved.as_ref().is_some_and(|saved| saved.network != network) {
            return Err(Error::Invalid("network differs from this data directory"));
        }
        let config = Self {
            data,
            network,
            stratum: stratum
                .or_else(|| saved.as_ref().map(|s| s.stratum))
                .unwrap_or_else(|| ([127, 0, 0, 1], if testnet { 13420 } else { 3420 }).into()),
            peer: peer
                .or_else(|| saved.as_ref().map(|s| s.peer))
                .unwrap_or_else(|| ([0, 0, 0, 0], if testnet { 13417 } else { 3417 }).into()),
            grin_host: grin
                .map(|a| a.ip())
                .or_else(|| saved.as_ref().map(|s| s.grin_host))
                .unwrap_or_else(default_grin_host),
            grin_port: grin
                .map(|a| a.port())
                .or_else(|| saved.as_ref().map(|s| s.grin_port))
                .unwrap_or(if testnet { 13418 } else { 3418 }),
            evaluation: Limits::default(),
            storage: StorageLimits::default(),
            jobs: stratum::Retention::default(),
        };
        let ports = [config.stratum.port(), config.peer.port(), config.grin_port];
        if ports.contains(&0) {
            return Err(Error::Invalid("listener port must not be zero"));
        }
        for (index, port) in ports.iter().enumerate() {
            if ports[..index].contains(port) {
                return Err(Error::Invalid(
                    "listener ports must be distinct (Stratum, pool peer, and Grin P2P)",
                ));
            }
        }
        Ok(config)
    }
}
const BOOTSTRAP_PEERS: [std::net::Ipv4Addr; 2] = [
    std::net::Ipv4Addr::new(188, 119, 24, 7),
    std::net::Ipv4Addr::new(51, 83, 197, 245),
];
fn default_pool_port(network: ChainTypes) -> u16 {
    if network == ChainTypes::Testnet {
        13417
    } else {
        3417
    }
}
pub fn bootstrap_peers(config: &Config) -> Vec<SocketAddr> {
    BOOTSTRAP_PEERS
        .iter()
        .map(|ip| SocketAddr::new((*ip).into(), default_pool_port(config.network)))
        .collect()
}
pub fn open_node_lock(data: &std::path::Path) -> Result<fs::File> {
    let mut options = fs::OpenOptions::new();
    options.read(true).write(true).create(true).truncate(false);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK);
    }
    Ok(options.open(data.join("node.lock"))?)
}
fn validate_node_lock(data: &std::path::Path, process_lock: &fs::File) -> Result<()> {
    let opened = process_lock.metadata()?;
    let named = fs::symlink_metadata(data.join("node.lock"))?;
    if !opened.is_file() || !named.is_file() {
        return Err(Error::Invalid("node lock must be a regular file"));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        if opened.dev() != named.dev() || opened.ino() != named.ino() {
            return Err(Error::Invalid("node lock changed during startup"));
        }
    }
    Ok(())
}
pub fn require_new_wallet_directory(data: &std::path::Path) -> Result<()> {
    match fs::symlink_metadata(data) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error.into()),
        Ok(metadata) if !metadata.is_dir() => {
            return Err(Error::Invalid("data directory must be a real directory"))
        }
        Ok(_) => {}
    }
    for entry in fs::read_dir(data)? {
        let entry = entry?;
        let metadata = fs::symlink_metadata(entry.path())?;
        let allowed = match entry.file_name().to_str() {
            Some("grin") => metadata.is_dir() && !metadata.file_type().is_symlink(),
            Some(
                "node.lock"
                | "instance.json"
                | "node-status.json"
                | "g2pool.control.lock"
                | ".g2pool-node-api-secret"
                | ".g2pool-node-foreign-api-secret",
            ) => metadata.is_file() && !metadata.file_type().is_symlink(),
            _ => false,
        };
        if !allowed {
            return Err(Error::Invalid("Grin wallet seed missing from established pool data; refusing to generate a replacement"));
        }
    }
    Ok(())
}
fn initialize_wallet<C: grin_wallet_libwallet::NodeClient + 'static>(
    data: &std::path::Path,
    process_lock: &fs::File,
    existed: bool,
    client: C,
) -> Result<Payments<C>> {
    validate_node_lock(data, process_lock)?;
    if crate::wallet::exists(data)? != existed {
        return Err(Error::Invalid("wallet changed during startup"));
    }
    (if existed {
        Payments::open(data, client)
    } else {
        Payments::create(data, client)
    })
    .map_err(|error| Error::NativeUnavailable(format!("wallet service: {error}")))
}

struct Runtime {
    server: Option<Server>,
    stop: Arc<std::sync::atomic::AtomicBool>,
    workers: Vec<thread::JoinHandle<Result<()>>>,
}
impl Runtime {
    fn finish(&mut self) -> Result<()> {
        self.stop.store(true, Ordering::Release);
        let mut failure = None;
        for worker in self.workers.drain(..) {
            let result = worker
                .join()
                .unwrap_or(Err(Error::Invalid("runtime service panicked")));
            if let Err(error) = result {
                if failure.is_none() {
                    failure = Some(error);
                }
            }
        }
        if let Some(server) = self.server.take() {
            server.stop();
        }
        failure.map_or(Ok(()), Err)
    }
}
impl Drop for Runtime {
    fn drop(&mut self) {
        if let Err(error) = self.finish() {
            eprintln!("runtime shutdown: {error}");
        }
    }
}

pub fn run(config: Config) -> Result<()> {
    let mut directories = fs::DirBuilder::new();
    directories.recursive(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;
        directories.mode(0o700);
    }
    directories.create(&config.data)?;
    let process_lock = open_node_lock(&config.data)?;
    fs2::FileExt::try_lock_exclusive(&process_lock)
        .map_err(|_| Error::Capacity("another node owns this data directory"))?;
    validate_node_lock(&config.data, &process_lock)?;
    let wallet_existed = crate::wallet::exists(&config.data)?;
    if !wallet_existed {
        require_new_wallet_directory(&config.data)?;
    }
    let bootstrap_peers = bootstrap_peers(&config);

    global::init_global_chain_type(config.network);
    global::set_local_chain_type(config.network);
    global::init_global_nrd_enabled(config.network != ChainTypes::Mainnet);
    let mut native = ServerConfig {
        chain_type: config.network,
        db_root: config.data.join("grin").to_string_lossy().into_owned(),
        api_http_addr: "127.0.0.1:0".into(),
        api_secret_path: Some(
            config
                .data
                .join(".g2pool-node-api-secret")
                .to_string_lossy()
                .into_owned(),
        ),
        foreign_api_secret_path: Some(
            config
                .data
                .join(".g2pool-node-foreign-api-secret")
                .to_string_lossy()
                .into_owned(),
        ),
        run_tui: Some(false),
        run_test_miner: Some(false),
        stratum_mining_config: None,
        ..Default::default()
    };
    private_api_secret(&config.data.join(".g2pool-node-api-secret"))?;
    private_api_secret(&config.data.join(".g2pool-node-foreign-api-secret"))?;
    native.p2p_config.host = config.grin_host;
    native.p2p_config.port = config.grin_port;
    {
        native.pool_config.mineable_max_weight = grin_core::consensus::MAX_BLOCK_WEIGHT / 2;
    }
    global::init_global_accept_fee_base(native.pool_config.accept_fee_base);
    global::init_global_future_time_limit(native.future_time_limit);
    let channel = tokio::sync::mpsc::channel(1);
    eprintln!("starting embedded Grin node");
    let grin = Server::start(native, None, None, channel)
        .map_err(|e| Error::NativeUnavailable(format!("native server: {e:?}")))?;
    let chain = grin.chain.clone();
    let status_chain = chain.clone();
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let payment_client = DirectNodeClient::new(chain.clone(), grin.tx_pool.clone(), stop.clone());
    let mut runtime = Runtime {
        server: Some(grin),
        stop: stop.clone(),
        workers: Vec::new(),
    };
    let genesis = native::hash_bytes(chain.genesis().hash());
    let manifest = native::manifest(genesis, config.network);
    let profile = Profile::new(manifest)?;
    let mut store = Store::open(
        &config.data.join("pool"),
        profile.protocol,
        config.storage.clone(),
    )?;
    let payment_service = Arc::new(initialize_wallet(
        &config.data,
        &process_lock,
        wallet_existed,
        payment_client,
    )?);
    let wallet = Arc::new(
        payment_service
            .signer()
            .map_err(|error| Error::NativeUnavailable(format!("wallet signer: {error}")))?,
    );
    store.bind_wallet(wallet.account())?;
    if !wallet_existed {
        println!("Created G2Pool wallet.");
    }
    publish_config(
        &config.data.join("instance.json"),
        &serde_json::to_vec_pretty(&Instance::from_config(&config))?,
    )?;
    let mut node = Node::start(chain.clone(), profile, store, config.evaluation.clone())?;
    node.cancel = stop.clone();
    node.peers = crate::network::peers::Book::open_store(
        &node.store,
        node.profile.protocol,
        false,
        &bootstrap_peers,
    )?;
    node.listening_port = config.peer.port();
    node.ensure_view()?;
    println!(
        "protocol={} account={}",
        hex(&node.profile.protocol),
        hex(&wallet.account())
    );
    println!(
        "network={:?} pool-peer={} stratum={} embedded-grin-p2p={} bootstrap-peers={}",
        config.network,
        config.peer,
        config.stratum,
        SocketAddr::new(config.grin_host, config.grin_port),
        bootstrap_peers.len()
    );
    let signal = stop.clone();
    ctrlc::set_handler(move || signal.store(true, Ordering::Release))
        .map_err(|_| Error::Invalid("signal handler"))?;
    let node = Arc::new(Mutex::new(node));
    let (payment_worker, payment_thread) = crate::wallet::payments::PaymentWorker::new(
        payment_service.clone(),
        runtime
            .server
            .as_ref()
            .ok_or(Error::Invalid("native server stopped"))?,
        node.lock()
            .map_err(|_| Error::Invalid("node poisoned"))?
            .profile
            .protocol,
        stop.clone(),
    )
    .map_err(|error| Error::NativeUnavailable(format!("wallet service: {error}")))?;
    runtime.workers.push(payment_thread);

    let control_listener = crate::control::Listener::bind(&config.data)?;
    let control_service = Arc::new(crate::control::Service::new(
        config.data.clone(),
        payment_worker.clone(),
    ));
    let control_stop = stop.clone();
    let control_thread = thread::spawn(move || {
        crate::control::serve(control_listener, control_stop, move |request| {
            control_service.handle(request)
        })
    });
    runtime.workers.push(control_thread);
    let mining = Arc::new(Mining::bounded(node.clone(), wallet, config.jobs.clone())?);

    let miner_listener = TcpListener::bind(config.stratum)?;
    let peer_thread = crate::network::start(
        config.peer,
        node.clone(),
        stop.clone(),
        crate::network::INBOUND_PEERS,
    )?;
    runtime.workers.push(peer_thread);
    let monitor = {
        let guard = node.lock().map_err(|_| Error::Invalid("node poisoned"))?;
        crate::status::runtime::Monitor::new(
            &guard,
            config.data.clone(),
            runtime.server.as_ref().map(|s| s.sync_state.clone()),
        )
    };
    let monitor_stop = stop.clone();
    let monitor_thread = thread::spawn(move || {
        monitor.run(monitor_stop);
        Ok(())
    });
    runtime.workers.push(monitor_thread);
    let mine_service = mining.clone();
    let miner_stop = stop.clone();
    let miner_thread =
        thread::spawn(move || stratum::serve(miner_listener, mine_service, miner_stop, 32));
    runtime.workers.push(miner_thread);
    let mut last = Instant::now() - Duration::from_secs(30);
    let peer_nodes = node.clone();
    let peer_signal = stop.clone();
    let outgoing =
        thread::spawn(move || crate::network::outbound::maintain(peer_nodes, peer_signal));
    runtime.workers.push(outgoing);
    let mut last_gc = Instant::now();
    let mut last_status = Instant::now() - Duration::from_secs(30);
    let mut recovery_retry = Instant::now();
    let mut solution_retry = Instant::now();
    let mut solution_error = None;
    let mut mining_error = None;
    let mut recovery_error = None;
    let mut service_failed = false;
    while !stop.load(Ordering::Acquire) {
        if runtime.workers.iter().any(|worker| worker.is_finished()) {
            service_failed = true;
            stop.store(true, Ordering::Release);
            eprintln!("a runtime service stopped unexpectedly; shutting down");
            break;
        }
        if Instant::now() >= solution_retry {
            match mining.solutions.replay(&node) {
                Ok(()) => solution_error = None,
                Err(
                    error @ (Error::Capacity(_)
                    | Error::Unavailable(_)
                    | Error::NativeUnavailable(_)
                    | Error::View
                    | Error::Future),
                ) => {
                    let message = error.to_string();
                    if solution_error.as_ref() != Some(&message) {
                        eprintln!("solution recovery pending: {message}");
                    }
                    solution_error = Some(message);
                }
                Err(Error::Cancelled) if stop.load(Ordering::Acquire) => break,
                Err(error) => return Err(error),
            }
            solution_retry = Instant::now() + Duration::from_secs(1);
        }
        if Instant::now() >= recovery_retry {
            let result = (|| -> Result<()> {
                let recovery = node
                    .lock()
                    .map_err(|_| Error::Invalid("node poisoned"))?
                    .prepare_recovery()?;
                if let Some(recovery) = recovery {
                    let result = recovery.run();
                    node.lock()
                        .map_err(|_| Error::Invalid("node poisoned"))?
                        .finish_recovery(result)?;
                }
                Ok(())
            })();
            match result {
                Ok(()) => recovery_error = None,
                Err(Error::View) => {}
                Err(
                    error @ (Error::Capacity(_)
                    | Error::Unavailable(_)
                    | Error::NativeUnavailable(_)),
                ) => {
                    let message = error.to_string();
                    if recovery_error.as_ref() != Some(&message) {
                        eprintln!("pool recovery retryable: {error}");
                    }
                    recovery_error = Some(message);
                    recovery_retry = Instant::now() + Duration::from_secs(1);
                }
                Err(Error::Cancelled) if stop.load(Ordering::Acquire) => break,
                Err(error) => return Err(error),
            }
        }
        if last_status.elapsed() >= Duration::from_secs(30) {
            let sync = if runtime
                .server
                .as_ref()
                .is_some_and(|native| native.sync_state.is_syncing())
            {
                "syncing"
            } else {
                "ready"
            };
            let overlay = crate::status::runtime::read(&config.data)
                .map(|s| format!("pool {}; {} peers", s.state, s.connected_peers))
                .unwrap_or_else(|_| "pool status pending".into());
            println!("{overlay}");
            match payment_worker.snapshot() {
                Ok(snapshot) => println!(
                    "Grin {sync}; height {}; wallet {}{}",
                    status_chain
                        .head_header()
                        .map(|header| header.height)
                        .unwrap_or(snapshot.native_height),
                    snapshot.state,
                    snapshot
                        .balance
                        .map(|balance| format!(
                            "; balance {}.{:09} GRIN",
                            balance.total / 1_000_000_000,
                            balance.total % 1_000_000_000
                        ))
                        .unwrap_or_default()
                ),
                Err(_) => println!("Grin {sync}; wallet balance scan pending"),
            }
            let routing = {
                node.lock()
                    .map_err(|_| Error::Invalid("node poisoned"))?
                    .peers
                    .persistence()
            };
            if let Some(routing) = routing {
                match routing.write() {
                    Ok(generation) => node
                        .lock()
                        .map_err(|_| Error::Invalid("node poisoned"))?
                        .peers
                        .persisted(generation),
                    Err(error) => eprintln!("peer persistence retryable: {error}"),
                }
            }
            last_status = Instant::now();
        }
        if runtime
            .server
            .as_ref()
            .is_some_and(|native| native.sync_state.is_syncing())
        {
            thread::sleep(Duration::from_millis(500));
            continue;
        }
        let needs_job = mining.job().is_err() || last.elapsed() > Duration::from_secs(15);
        if needs_job {
            let result = (|| -> Result<_> {
                let server = runtime
                    .server
                    .as_ref()
                    .ok_or(Error::Invalid("native server stopped"))?;
                let transactions = server
                    .tx_pool
                    .read()
                    .prepare_mineable_transactions()
                    .map_err(|error| {
                        Error::NativeUnavailable(format!("mineable transactions: {error}"))
                    })?;
                let transaction = grin_core::core::transaction::aggregate(&transactions)
                    .map_err(|_| Error::Invalid("native transaction aggregation"))?;
                Template::prepare(&node, &mining.wallet, &payment_service, transaction)
            })();
            match result {
                Ok(template) => match mining.publish(template) {
                    Ok(_) => {
                        last = Instant::now();
                        mining_error = None;
                    }
                    Err(Error::Capacity(reason)) => {
                        eprintln!("job publication retryable: {reason}");
                        thread::sleep(Duration::from_secs(1));
                    }
                    Err(error) => return Err(error),
                },
                Err(e) => {
                    let message = e.to_string();
                    if mining_error.as_ref() != Some(&message) {
                        eprintln!("mining retryable: {message}");
                    }
                    mining_error = Some(message);
                    thread::sleep(Duration::from_secs(1));
                }
            }
        }
        if last_gc.elapsed() > Duration::from_secs(60) {
            let prepared = {
                node.lock()
                    .map_err(|_| Error::Invalid("node poisoned"))?
                    .prepare_maintenance()
            };
            let result = match prepared {
                Ok(plan) => match plan.run() {
                    Ok(plan) => {
                        let pins = mining.pins()?;
                        node.lock()
                            .map_err(|_| Error::Invalid("node poisoned"))?
                            .finish_maintenance(plan, chrono::Utc::now().timestamp() - 120, &pins)
                    }
                    Err(error) => Err(error),
                },
                Err(error) => Err(error),
            };
            if let Err(error) = result {
                eprintln!("collection retryable: {error}");
            }
            last_gc = Instant::now();
        }
        thread::sleep(Duration::from_millis(100));
    }
    eprintln!("shutdown requested; stopping runtime services");
    let routing = node
        .lock()
        .map_err(|_| Error::Invalid("node poisoned"))?
        .peers
        .persistence();
    if let Some(routing) = routing {
        routing.write()?;
    }
    runtime.finish()?;
    if service_failed {
        return Err(Error::Invalid("runtime service stopped unexpectedly"));
    }
    Ok(())
}

#[cfg(test)]
#[path = "daemon_tests.rs"]
mod tests;
