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
    io::IsTerminal,
    net::{IpAddr, SocketAddr, TcpListener},
    path::PathBuf,
    sync::{atomic::Ordering, Arc, Mutex},
    thread,
    time::{Duration, Instant},
};
pub struct Config {
    pub data: PathBuf,
    pub network: ChainTypes,
    pub pool_file: Option<PathBuf>,
    pub stratum: SocketAddr,
    pub peer: SocketAddr,
    pub grin_host: IpAddr,
    pub grin_port: u16,
    pub evaluation: Limits,
    pub storage: StorageLimits,
    pub jobs: stratum::Retention,
}
fn default_grin_host() -> IpAddr {
    IpAddr::V4(std::net::Ipv4Addr::UNSPECIFIED)
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
        pool_file: Option<PathBuf>,
        stratum: Option<SocketAddr>,
        peer: Option<SocketAddr>,
        grin: Option<SocketAddr>,
    ) -> Result<Self> {
        let testnet = network == ChainTypes::Testnet;
        let data = crate::network_data_dir(base, if testnet { "testnet" } else { "mainnet" });
        let config = Self {
            data,
            network,
            pool_file,
            stratum: stratum
                .unwrap_or_else(|| ([127, 0, 0, 1], if testnet { 13420 } else { 3420 }).into()),
            peer: peer.unwrap_or_else(|| ([0, 0, 0, 0], if testnet { 13417 } else { 3417 }).into()),
            grin_host: grin.map(|a| a.ip()).unwrap_or_else(default_grin_host),
            grin_port: grin
                .map(|a| a.port())
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
pub fn open_node_lock(data: &std::path::Path) -> Result<fs::File> {
    crate::secure_file::open_lock(&data.join("node.lock"))
}
fn validate_node_lock(data: &std::path::Path, process_lock: &fs::File) -> Result<()> {
    crate::secure_file::validate_lock(&data.join("node.lock"), process_lock)
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
            log::error!("runtime shutdown: {error}");
        }
    }
}

pub fn run(config: Config, interactive: bool) -> Result<()> {
    let interactive =
        interactive && std::io::stdin().is_terminal() && std::io::stdout().is_terminal();
    let reporting = crate::status::reporting::Reporting::new(interactive)?;
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let signal = stop.clone();
    ctrlc::set_handler(move || signal.store(true, Ordering::Release))
        .map_err(|_| Error::Invalid("signal handler"))?;
    global::set_local_chain_type(config.network);
    if !interactive {
        return run_node(&config, &reporting, stop);
    }
    struct Shutdown<'a>(&'a std::sync::atomic::AtomicBool);
    impl Drop for Shutdown<'_> {
        fn drop(&mut self) {
            self.0.store(true, Ordering::Release);
        }
    }
    thread::scope(|scope| {
        let worker = scope.spawn(|| run_node(&config, &reporting, stop.clone()));
        let ui = {
            let _shutdown = Shutdown(&stop);
            crate::tui::run(&config, &reporting, &stop, || worker.is_finished())
        };
        worker
            .join()
            .unwrap_or(Err(Error::Invalid("daemon panicked")))
            .and(ui)
    })
}

fn run_node(
    config: &Config,
    reporting: &crate::status::reporting::Reporting,
    stop: Arc<std::sync::atomic::AtomicBool>,
) -> Result<()> {
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
    let pool = crate::pool::Pool::load(
        config
            .data
            .parent()
            .ok_or(Error::Invalid("data directory"))?,
        config.pool_file.as_deref(),
    )?;
    let bootstrap_peers = pool.peers(config.network)?;

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
    if stop.load(Ordering::Acquire) {
        return Ok(());
    }
    reporting.phase("Starting embedded Grin node");
    let grin = Server::start(native, None, None, channel)
        .map_err(|e| Error::NativeUnavailable(format!("native server: {e:?}")))?;
    let chain = grin.chain.clone();
    let native_sync = grin.sync_state.clone();
    let payment_client = DirectNodeClient::new(chain.clone(), grin.tx_pool.clone(), stop.clone());
    let mut runtime = Runtime {
        server: Some(grin),
        stop: stop.clone(),
        workers: Vec::new(),
    };
    if stop.load(Ordering::Acquire) {
        return Ok(());
    }
    reporting.phase("Opening pool and wallet");
    let genesis = native::hash_bytes(chain.genesis().hash());
    let profile = Profile::new(genesis, config.network, &pool);
    let mut store = Store::open(
        &config.data.join("pool"),
        profile.pool_id,
        config.storage.clone(),
    )?;
    if !wallet_existed && store.wallet_account()?.is_some() {
        return Err(Error::Invalid(
            "Grin wallet seed missing from established pool data; refusing to generate a replacement",
        ));
    }
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
        log::info!("Created G2Pool wallet");
    }
    let mut node = Node::start(
        chain.clone(),
        profile,
        store,
        config.evaluation.clone(),
        native_sync.clone(),
    )?;
    node.cancel = stop.clone();
    node.peers = crate::network::peers::Book::open_store(
        &node.store,
        node.profile.pool_id,
        false,
        &bootstrap_peers,
    )?;
    node.listening_port = config.peer.port();
    log::info!(
        "pool={} pool-id={} account={}",
        pool.name,
        hex(&node.profile.pool_id),
        hex(&wallet.account())
    );
    log::info!(
        "network={:?} pool-peer={} stratum={} embedded-grin-p2p={} bootstrap-peers={}",
        config.network,
        config.peer,
        config.stratum,
        SocketAddr::new(config.grin_host, config.grin_port),
        bootstrap_peers.len()
    );
    let published = node.published.clone();
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
            .pool_id,
        stop.clone(),
    )
    .map_err(|error| Error::NativeUnavailable(format!("wallet service: {error}")))?;
    runtime.workers.push(payment_thread);

    let status = {
        let guard = node.lock().map_err(|_| Error::Invalid("node poisoned"))?;
        crate::status::runtime::Status::new(&guard, pool.name.clone(), config.data.clone())
    };

    let control_listener = crate::control::Listener::bind(&config.data)?;
    let control_service = Arc::new(crate::control::Service::new(
        status.clone(),
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
    *reporting.sources.write() = Some(crate::status::reporting::Sources {
        node: status,
        wallet: payment_worker,
        mining: mining.clone(),
    });

    let miner_listener = TcpListener::bind(config.stratum)?;
    let peer_thread = crate::network::start(
        config.peer,
        node.clone(),
        stop.clone(),
        crate::network::INBOUND_PEERS,
        pool.key(),
    )?;
    runtime.workers.push(peer_thread);
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
    reporting.phase("Running");
    let mut last_gc = Instant::now();
    let mut last_peers = Instant::now() - Duration::from_secs(30);
    let mut recovery_retry = Instant::now();
    let mut solution_retry = Instant::now();
    let mut service_failed = false;
    while !stop.load(Ordering::Acquire) {
        if runtime.workers.iter().any(|worker| worker.is_finished()) {
            service_failed = true;
            stop.store(true, Ordering::Release);
            log::error!("a runtime service stopped unexpectedly; shutting down");
            break;
        }
        if Instant::now() >= solution_retry {
            match mining.solutions.replay(&node) {
                Ok(()) => {
                    reporting.condition("Solution recovery", None);
                }
                Err(
                    error @ (Error::Capacity(_)
                    | Error::EvaluationTime
                    | Error::Unavailable(_)
                    | Error::NativeUnavailable(_)
                    | Error::View
                    | Error::Future),
                ) => {
                    let message = error.to_string();
                    if reporting.condition("Solution recovery", Some(message.clone())) {
                        log::warn!("solution recovery pending: {message}");
                    }
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
                Ok(()) => {
                    reporting.condition("Pool recovery", None);
                }
                Err(Error::View) => {
                    reporting.condition("Pool recovery", None);
                }
                Err(Error::NativeUnavailable(_)) if native_sync.is_syncing() => {
                    reporting.condition("Pool recovery", None);
                    recovery_retry = Instant::now() + Duration::from_secs(1);
                }
                Err(
                    error @ (Error::Capacity(_)
                    | Error::EvaluationTime
                    | Error::Unavailable(_)
                    | Error::NativeUnavailable(_)),
                ) => {
                    let message = error.to_string();
                    if reporting.condition("Pool recovery", Some(message)) {
                        log::info!("pool recovery retryable: {error}");
                    }
                    recovery_retry = Instant::now() + Duration::from_secs(1);
                }
                Err(Error::Cancelled) if stop.load(Ordering::Acquire) => break,
                Err(error) => return Err(error),
            }
        }
        if last_peers.elapsed() >= Duration::from_secs(30) {
            let routing = {
                node.lock()
                    .map_err(|_| Error::Invalid("node poisoned"))?
                    .peers
                    .persistence()
            };
            if let Some(routing) = routing {
                match routing.write() {
                    Ok(generation) => {
                        node.lock()
                            .map_err(|_| Error::Invalid("node poisoned"))?
                            .peers
                            .persisted(generation);
                        reporting.condition("Address book", None);
                    }
                    Err(error) => {
                        if reporting.condition("Address book", Some(error.to_string())) {
                            log::warn!("peer persistence retryable: {error}");
                        }
                    }
                }
            }
            last_peers = Instant::now();
        }
        if last_gc.elapsed() > Duration::from_secs(60) {
            let prepared = node
                .lock()
                .map_err(|_| Error::Invalid("node poisoned"))?
                .prepare_maintenance();
            let compaction = prepared.and_then(crate::node::Maintenance::run);
            let pins = mining.pins()?;
            let result = node
                .lock()
                .map_err(|_| Error::Invalid("node poisoned"))?
                .finish_maintenance(
                    compaction,
                    chrono::Utc::now().timestamp()
                        - crate::storage::sharechain::ACQUISITION_LIFETIME as i64,
                    &pins,
                );
            match result {
                Ok(_) | Err(Error::View) => {
                    reporting.condition("Storage maintenance", None);
                }
                Err(Error::NativeUnavailable(_)) if native_sync.is_syncing() => {}
                Err(error) => {
                    if reporting.condition("Storage maintenance", Some(error.to_string())) {
                        log::warn!("storage maintenance retryable: {error}");
                    }
                }
            }
            last_gc = Instant::now();
        }
        if native_sync.is_syncing() {
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
                        reporting.condition("Mining", None);
                        reporting.condition("Job publication", None);
                    }
                    Err(Error::Capacity(reason)) => {
                        if reporting.condition("Job publication", Some(reason.into())) {
                            log::warn!("job publication retryable: {reason}");
                        }
                        thread::sleep(Duration::from_secs(1));
                    }
                    Err(error) => return Err(error),
                },
                Err(e) => {
                    let expected = matches!(e, Error::View)
                        || native_sync.is_syncing()
                        || published
                            .read()
                            .map_err(|_| Error::Invalid("publication snapshot poisoned"))?
                            .recovering;
                    if expected {
                        reporting.condition("Mining", None);
                    } else if reporting.condition("Mining", Some(e.to_string())) {
                        log::warn!("mining waiting: {e}");
                    }
                    thread::sleep(Duration::from_secs(1));
                }
            }
        }
        thread::sleep(Duration::from_millis(100));
    }
    reporting.phase("Stopping runtime services");
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
