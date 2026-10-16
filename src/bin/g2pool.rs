use g2pool::{
    limits::Limits,
    mining::stratum::{self, Mining},
    mining::Template,
    native::{self, Profile},
    node::Node,
    objects::hex,
    storage::{StorageLimits, Store},
    wallet::payments::{DirectNodeClient, Payments},
    wallet::Wallet,
    Error, Result,
};
use grin_chain::types::Options;
use grin_core::core::hash::Hashed;
use grin_core::{
    core::{Block, Transaction},
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
struct Config {
    data: PathBuf,
    network: ChainTypes,
    stratum: SocketAddr,
    peer: SocketAddr,
    grin_host: IpAddr,
    grin_port: u16,
    scan: bool,
    evaluation: Limits,
    storage: StorageLimits,
    jobs: stratum::Retention,
}
#[derive(serde::Serialize, serde::Deserialize)]
struct Instance {
    network: ChainTypes,
    stratum: SocketAddr,
    peer: SocketAddr,
    #[serde(default = "default_grin_host")]
    grin_host: IpAddr,
    grin_port: u16,
}
impl Instance {
    fn from_config(c: &Config) -> Self {
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
    g2pool::secure_file::read_regular(path, 16384, false)
        .map_err(|e| Error::NativeUnavailable(format!("configuration {}: {e}", path.display())))
}
fn publish_config(path: &std::path::Path, bytes: &[u8]) -> Result<()> {
    g2pool::secure_file::publish_atomic(path, bytes, true)
}
fn private_api_secret(path: &std::path::Path) -> Result<String> {
    let secret = hex(&rand::random::<[u8; 32]>());
    g2pool::secure_file::publish_atomic(path, format!("{secret}\n").as_bytes(), true)?;
    Ok(secret)
}
fn args() -> Result<Config> {
    args_from(std::env::args().skip(1))
}
fn args_from(arguments: impl IntoIterator<Item = String>) -> Result<Config> {
    let arguments: Vec<String> = arguments.into_iter().collect();
    let mut selected_base = g2pool::default_data_dir();
    for pair in arguments.windows(2) {
        if pair[0] == "--data-dir" {
            selected_base = PathBuf::from(&pair[1]);
        }
    }
    let explicit_network = arguments
        .windows(2)
        .find(|pair| pair[0] == "--network")
        .map(|pair| pair[1].as_str());
    let selected_network = explicit_network.unwrap_or("mainnet");
    let selected_network = match selected_network {
        "testnet" | "mainnet" => selected_network,
        _ => return Err(Error::Invalid("network")),
    };
    let selected_data = g2pool::network_data_dir(selected_base, selected_network);
    let instance_path = selected_data.join("instance.json");
    let saved: Option<Instance> = match fs::symlink_metadata(&instance_path) {
        Ok(_) => Some(serde_json::from_slice(&read_config(&instance_path)?)?),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
        Err(error) => return Err(error.into()),
    };
    let mut c = Config {
        data: selected_data.clone(),
        network: ChainTypes::Mainnet,
        stratum: ([127, 0, 0, 1], 3420).into(),
        peer: ([0, 0, 0, 0], 3417).into(),
        grin_host: default_grin_host(),
        grin_port: 3418,
        scan: false,
        evaluation: Limits::default(),
        storage: StorageLimits::default(),
        jobs: stratum::Retention::default(),
    };
    let mut explicit_ports = [false; 3];
    if let Some(saved) = &saved {
        c.network = saved.network;
        c.stratum = saved.stratum;
        c.peer = saved.peer;
        c.grin_host = saved.grin_host;
        c.grin_port = saved.grin_port;
        explicit_ports = [true; 3];
    }
    let mut args = arguments.into_iter().peekable();
    let mut flags = std::collections::BTreeSet::new();
    while let Some(arg) = args.next() {
        if !flags.insert(arg.clone()) {
            return Err(Error::Invalid("repeated argument"));
        }
        match arg.as_str() {
            "--scan-wallet-internal" => c.scan = true,
            key => {
                let value = args
                    .next()
                    .ok_or(Error::Invalid("missing argument value"))?;
                match key {
                    "--data-dir" => c.data = selected_data.clone(),
                    "--network" => {
                        c.network = match value.as_str() {
                            "testnet" => ChainTypes::Testnet,
                            "mainnet" => ChainTypes::Mainnet,
                            _ => return Err(Error::Invalid("network")),
                        }
                    }
                    "--stratum" => {
                        explicit_ports[0] = true;
                        c.stratum = value
                            .parse()
                            .map_err(|_| Error::Invalid("Stratum address"))?
                    }
                    "--pool-listen" => {
                        explicit_ports[1] = true;
                        c.peer = value.parse().map_err(|_| Error::Invalid("pool address"))?
                    }
                    "--grin-listen" => {
                        explicit_ports[2] = true;
                        let address: SocketAddr =
                            value.parse().map_err(|_| Error::Invalid("Grin address"))?;
                        c.grin_host = address.ip();
                        c.grin_port = address.port();
                    }
                    _ => return Err(Error::Invalid("unknown argument")),
                }
            }
        }
    }
    if !matches!(c.network, ChainTypes::Mainnet | ChainTypes::Testnet) {
        return Err(Error::Invalid("unsupported instance network"));
    }
    if c.network == ChainTypes::Testnet {
        if !explicit_ports[0] {
            c.stratum = ([127, 0, 0, 1], 13420).into();
        }
        if !explicit_ports[1] {
            c.peer = ([0, 0, 0, 0], 13417).into();
        }
        if !explicit_ports[2] {
            c.grin_port = 13418;
        }
    }
    if saved
        .as_ref()
        .is_some_and(|saved| saved.network != c.network)
    {
        return Err(Error::Invalid(
            "network differs from this data directory; select a new data directory",
        ));
    }
    c.data = g2pool::network_data_dir(
        selected_data
            .parent()
            .map(PathBuf::from)
            .unwrap_or_else(g2pool::default_data_dir),
        match c.network {
            ChainTypes::Testnet => "testnet",
            ChainTypes::Mainnet => "mainnet",
            _ => return Err(Error::Invalid("unsupported wallet network")),
        },
    );
    let ports = [c.stratum.port(), c.peer.port(), c.grin_port];
    if ports[..3].contains(&0) {
        return Err(Error::Invalid("listener port must not be zero"));
    }
    for (index, port) in ports.iter().enumerate() {
        if *port != 0 && ports[..index].contains(port) {
            return Err(Error::Invalid(
                "listener ports must be distinct (Stratum, pool peer, and Grin P2P)",
            ));
        }
    }
    if c.jobs.duration.is_zero() || c.jobs.bytes == 0 {
        return Err(Error::Invalid("job retention policy must be positive"));
    }
    Ok(c)
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
fn bootstrap_peers(config: &Config) -> Vec<SocketAddr> {
    BOOTSTRAP_PEERS
        .iter()
        .map(|ip| SocketAddr::new((*ip).into(), default_pool_port(config.network)))
        .collect()
}
fn open_node_lock(data: &std::path::Path) -> Result<fs::File> {
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
fn require_blank_wallet_directory(data: &std::path::Path) -> Result<()> {
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
        if !matches!(entry.file_name().to_str(), Some("node.lock"))
            || !fs::symlink_metadata(entry.path())?.is_file()
        {
            return Err(Error::Invalid("wallet seed missing from established data directory; restore the original wallet.seed"));
        }
    }
    Ok(())
}
fn initialize_locked_wallet(
    data: &std::path::Path,
    process_lock: &fs::File,
    preflight_account: Option<g2pool::codec::Point>,
    scan: bool,
) -> Result<(Wallet, bool)> {
    validate_node_lock(data, process_lock)?;
    let seed = data.join("wallet.seed");
    match fs::symlink_metadata(&seed) {
        Ok(_) => {
            let wallet = Wallet::open_existing(&seed)?;
            if preflight_account.is_some_and(|account| wallet.account() != account) {
                return Err(Error::Invalid("wallet seed changed during startup"));
            }
            Ok((wallet, false))
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            if scan || preflight_account.is_some() {
                return Err(Error::Invalid(
                    "wallet seed missing; restore the original wallet.seed",
                ));
            }
            require_blank_wallet_directory(data)?;
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                fs::set_permissions(data, fs::Permissions::from_mode(0o700))?;
            }
            g2pool::secure_file::sync_directory(
                data.parent()
                    .filter(|path| !path.as_os_str().is_empty())
                    .unwrap_or(std::path::Path::new(".")),
            )?;
            Ok((Wallet::open(&seed)?, true))
        }
        Err(error) => Err(error.into()),
    }
}
fn run() -> Result<()> {
    let config = args()?;
    let seed_path = config.data.join("wallet.seed");
    let preflight_account = match fs::symlink_metadata(&seed_path) {
        Ok(_) => Some(Wallet::open_existing(&seed_path)?.account()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            if config.scan {
                Wallet::open_existing(&seed_path)?;
            }
            require_blank_wallet_directory(&config.data)?;
            None
        }
        Err(error) => return Err(error.into()),
    };
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
    let (wallet, created) =
        initialize_locked_wallet(&config.data, &process_lock, preflight_account, config.scan)?;
    let bootstrap_peers = bootstrap_peers(&config);
    let wallet = Arc::new(wallet);
    if created {
        println!("Created G2Pool wallet. Back up your data directory.");
    }

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
    let payment_client = DirectNodeClient::new(chain.clone(), grin.tx_pool.clone());
    let mut server = Some(grin);
    let genesis = native::hash_bytes(chain.genesis().hash());
    let manifest = native::manifest(genesis, config.network);
    let profile = Profile::new(manifest)?;
    let mut store = Store::open(
        &config.data.join("pool"),
        profile.protocol,
        config.storage.clone(),
    )?;
    store.bind_wallet(wallet.account())?;
    let wallet_namespace = store.wallet_namespace()?;
    let payment_service = Arc::new(
        Payments::open_bound(
            &config.data,
            payment_client,
            wallet.account(),
            wallet_namespace,
        )
        .map_err(|error| Error::NativeUnavailable(format!("wallet service: {error}")))?,
    );
    publish_config(
        &config.data.join("instance.json"),
        &serde_json::to_vec_pretty(&Instance::from_config(&config))?,
    )?;
    let mut node = if config.scan {
        Node::new(chain, profile, store, config.evaluation.clone())?
    } else {
        Node::start(chain, profile, store, config.evaluation.clone())?
    };
    let wallet_scan_start = 1;
    g2pool::mining::submission::Solutions::open(&config.data.join("pool"), node.profile.protocol)?
        .replay(&mut node)?;
    for (id, bytes) in node.store.submissions()? {
        let block: Block = native::parse(&bytes)?;
        if node.chain.get_block(&block.hash()).is_ok()
            || node.chain.process_block(block, Options::MINE).is_ok()
        {
            node.store.submission_done(id)?;
        }
    }
    node.peers = g2pool::network::peers::Book::open_store(
        &node.store,
        node.profile.protocol,
        false,
        &bootstrap_peers,
    )?;
    node.listening_port = config.peer.port();
    node.ensure_view()?;
    if config.scan {
        if server
            .as_ref()
            .is_some_and(|native| native.sync_state.is_syncing())
        {
            return Err(Error::NativeUnavailable(
                "recovery requires a synchronized native chain; run the node to finish synchronization first".into(),
            ));
        }
        println!(
            "{}",
            serde_json::to_string_pretty(&wallet.recover(&node.view()?)?)?
        );
        return Ok(());
    }
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
    let stop = node.cancel.clone();
    let signal = stop.clone();
    ctrlc::set_handler(move || signal.store(true, Ordering::Release))
        .map_err(|_| Error::Invalid("signal handler"))?;
    let wallet_chain = node.chain.clone();
    let wallet_profile = node.profile.clone();
    let status_wallet = wallet.clone();
    let status_stop = stop.clone();
    let status_path = config.data.join("wallet-status.json");
    let status_kind = config.network;
    let node = Arc::new(Mutex::new(node));
    let control_listener = g2pool::control::Listener::bind(&config.data)?;
    let control_service = Arc::new(g2pool::control::Service::new(
        config.data.clone(),
        payment_service.clone(),
    ));
    let control_stop = stop.clone();
    let control_thread = thread::spawn(move || {
        g2pool::control::serve(control_listener, control_stop, move |request| {
            control_service.handle(request)
        })
    });
    let mining = Arc::new(Mining::bounded(node.clone(), wallet, config.jobs.clone())?);
    mining.require_peers(true);

    let miner_listener = TcpListener::bind(config.stratum)?;
    let wallet_thread = thread::spawn(move || {
        global::set_local_chain_type(status_kind);
        g2pool::status::wallet::run(
            wallet_chain,
            wallet_profile,
            status_wallet,
            wallet_namespace,
            wallet_scan_start,
            &status_path,
            status_stop,
        );
    });
    let peer_thread = g2pool::network::start(
        config.peer,
        node.clone(),
        stop.clone(),
        g2pool::network::INBOUND_PEERS,
    )?;
    let monitor = {
        let guard = node.lock().map_err(|_| Error::Invalid("node poisoned"))?;
        g2pool::status::runtime::Monitor::new(
            &guard,
            config.data.clone(),
            server.as_ref().map(|s| s.sync_state.clone()),
        )
    };
    let monitor_stop = stop.clone();
    let monitor_thread = thread::spawn(move || monitor.run(monitor_stop));
    let mine_service = mining.clone();
    let miner_stop = stop.clone();
    let miner_thread =
        thread::spawn(move || stratum::serve(miner_listener, mine_service, miner_stop, 32));
    let mut last = Instant::now() - Duration::from_secs(30);
    let peer_nodes = node.clone();
    let peer_signal = stop.clone();
    let kind = config.network;
    let outgoing = thread::spawn(move || {
        global::set_local_chain_type(kind);
        let mut connections =
            std::collections::BTreeMap::<SocketAddr, thread::JoinHandle<()>>::new();
        while !peer_signal.load(Ordering::Acquire) {
            connections.retain(|_, worker| !worker.is_finished());
            let targets = match peer_nodes.lock() {
                Ok(node) => node.peers.targets(chrono::Utc::now().timestamp()),
                Err(_) => break,
            };
            for addr in targets {
                if connections.contains_key(&addr)
                    || connections.len() >= g2pool::network::OUTBOUND_PEERS
                {
                    continue;
                }
                let node = peer_nodes.clone();
                let stop = peer_signal.clone();
                connections.insert(
                    addr,
                    thread::spawn(move || {
                        global::set_local_chain_type(kind);
                        let protocol = match node.lock() {
                            Ok(n) => n.profile.protocol,
                            Err(_) => return,
                        };
                        let mut client = g2pool::network::Client::new(addr, protocol);
                        let route_started = Instant::now();
                        let route_lifetime =
                            Duration::from_secs(120 + u64::from(rand::random::<u8>() % 61));
                        while !stop.load(Ordering::Acquire) {
                            if route_started.elapsed() > route_lifetime {
                                client.retire();
                                break;
                            }
                            let result = client.synchronize(&node, &stop);
                            if !matches!(
                                result,
                                Err(Error::Capacity(_)
                                    | Error::View
                                    | Error::Cancelled
                                    | Error::Future
                                    | Error::Unavailable(_)
                                    | Error::NativeUnavailable(_))
                            ) {
                                if let Ok(mut n) = node.lock() {
                                    let now = chrono::Utc::now().timestamp();
                                    if client.reached_endpoint() {
                                        n.peers.reached(addr, now);
                                    }
                                    if result.as_ref().is_ok_and(|count| *count > 0) {
                                        let _ = n.peers.outcome(addr, true, now);
                                        n.peers.progress(addr, now);
                                    } else if result.is_err() {
                                        let _ = n.peers.outcome(addr, false, now);
                                    }
                                }
                            }
                            if let Err(error) = result {
                                if !matches!(
                                    error,
                                    Error::Capacity(_)
                                        | Error::View
                                        | Error::Future
                                        | Error::Unavailable(_)
                                        | Error::NativeUnavailable(_)
                                        | Error::Cancelled
                                ) {
                                    eprintln!("peer {addr}: {error}");
                                    break;
                                }
                            }
                            for _ in 0..20 {
                                if stop.load(Ordering::Acquire) {
                                    break;
                                }
                                thread::sleep(Duration::from_millis(25));
                            }
                        }
                    }),
                );
            }
            thread::sleep(Duration::from_millis(100));
        }
        for (_, worker) in connections {
            let _ = worker.join();
        }
    });
    let mut last_gc = Instant::now();
    let mut last_status = Instant::now() - Duration::from_secs(30);
    let mut service_failed = false;
    while !stop.load(Ordering::Acquire) {
        if peer_thread.is_finished()
            || miner_thread.is_finished()
            || outgoing.is_finished()
            || wallet_thread.is_finished()
            || monitor_thread.is_finished()
            || control_thread.is_finished()
        {
            service_failed = true;
            stop.store(true, Ordering::Release);
            eprintln!("a runtime service stopped unexpectedly; shutting down");
            break;
        }
        let recovery = match node
            .lock()
            .map_err(|_| Error::Invalid("node poisoned"))?
            .prepare_recovery()
        {
            Ok(recovery) => recovery,
            Err(Error::View) => continue,
            Err(error) => return Err(error),
        };
        if let Some(recovery) = recovery {
            let result = recovery.run();
            match node
                .lock()
                .map_err(|_| Error::Invalid("node poisoned"))?
                .finish_recovery(result)
            {
                Ok(()) => {}
                Err(Error::View) => continue,
                Err(error) => return Err(error),
            }
        }
        if last_status.elapsed() >= Duration::from_secs(30) {
            let sync = if server
                .as_ref()
                .is_some_and(|native| native.sync_state.is_syncing())
            {
                "syncing"
            } else {
                "ready"
            };
            let overlay = g2pool::status::runtime::read(&config.data)
                .map(|s| format!("pool {}; {} peers", s.state, s.connected_peers))
                .unwrap_or_else(|_| "pool status pending".into());
            println!("{overlay}");
            match g2pool::status::wallet::read_current_snapshot(
                &config.data.join("wallet-status.json"),
            ) {
                Ok(snapshot) => println!(
                    "Grin {sync}; height {}; wallet {}{}",
                    status_chain
                        .head_header()
                        .map(|header| header.height)
                        .unwrap_or(snapshot.native_height),
                    snapshot.state,
                    snapshot
                        .mining_balance
                        .map(|balance| format!(
                            "; mining funds {}.{:09} GRIN",
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
        if server
            .as_ref()
            .is_some_and(|native| native.sync_state.is_syncing())
        {
            thread::sleep(Duration::from_millis(500));
            continue;
        }
        let connected = mining.connected_peers() > 0;
        let needs_job =
            connected && (mining.job().is_err() || last.elapsed() > Duration::from_secs(15));
        if needs_job {
            let transaction = server
                .as_ref()
                .and_then(|s| s.tx_pool.read().prepare_mineable_transactions().ok())
                .and_then(|txs| grin_core::core::transaction::aggregate(&txs).ok())
                .unwrap_or_else(Transaction::empty);
            let mut n = node.lock().map_err(|_| Error::Invalid("node poisoned"))?;
            let result = Template::prepare(&mut n, &mining.wallet, transaction);
            let result = match result {
                Err(Error::Invalid(reason)) => {
                    eprintln!(
                        "transaction template rejected ({reason}); retrying an empty aggregate"
                    );
                    Template::prepare(&mut n, &mining.wallet, Transaction::empty())
                }
                other => other,
            };
            drop(n);
            match result {
                Ok(template) => match mining.publish(template) {
                    Ok(_) => last = Instant::now(),
                    Err(Error::Capacity(reason)) => {
                        eprintln!("job publication retryable: {reason}");
                        thread::sleep(Duration::from_secs(1));
                    }
                    Err(error) => return Err(error),
                },
                Err(e) => {
                    eprintln!("mining retryable: {e}");
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
    eprintln!("shutdown requested; stopping embedded Grin node");
    if let Some(server) = server.take() {
        server.stop();
    }
    let routing = {
        node.lock()
            .map_err(|_| Error::Invalid("node poisoned"))?
            .peers
            .persistence()
    };
    if let Some(routing) = routing {
        routing.write()?;
    }
    let monitor_result = monitor_thread.join();
    let wallet_result = wallet_thread.join();
    let outgoing_result = outgoing.join();
    let peer_result = peer_thread.join();
    let miner_result = miner_thread.join();
    monitor_result.map_err(|_| Error::Invalid("node monitoring panicked"))?;
    wallet_result.map_err(|_| Error::Invalid("wallet monitoring panicked"))?;
    outgoing_result.map_err(|_| Error::Invalid("outgoing service panicked"))?;
    peer_result.map_err(|_| Error::Invalid("peer service panicked"))??;
    miner_result.map_err(|_| Error::Invalid("mining service panicked"))??;
    control_thread
        .join()
        .map_err(|_| Error::Invalid("control service panicked"))??;
    if service_failed {
        return Err(Error::Invalid("runtime service stopped unexpectedly"));
    }
    Ok(())
}
fn help_command() -> clap::Command {
    use clap::{Arg, Command};

    fn network_arg() -> Arg {
        Arg::new("network")
            .long("network")
            .value_name("NETWORK")
            .value_parser(["mainnet", "testnet"])
            .help("network to use")
    }
    fn data_dir_arg() -> Arg {
        Arg::new("data-dir")
            .long("data-dir")
            .value_name("PATH")
            .help("data directory base")
    }

    Command::new("g2pool")
        .disable_help_flag(true)
        .disable_help_subcommand(true)
        .disable_version_flag(true)
        .help_template("Commands:\n{subcommands}\nOptions:\n{options}\n")
        .subcommand(
            Command::new("status")
                .about("show daemon and wallet status")
                .arg(network_arg())
                .arg(data_dir_arg()),
        )
        .subcommand(
            Command::new("backup")
                .about("write wallet recovery words")
                .arg(network_arg())
                .arg(data_dir_arg())
                .arg(
                    Arg::new("output")
                        .long("output")
                        .value_name("FILE")
                        .required(true)
                        .help("recovery-word output file"),
                ),
        )
        .subcommand(
            Command::new("withdraw")
                .about("withdraw GRIN through Slatepack")
                .arg(network_arg())
                .arg(data_dir_arg()),
        )
        .arg(network_arg())
        .arg(data_dir_arg())
        .arg(
            Arg::new("stratum")
                .long("stratum")
                .value_name("ADDR")
                .help("Stratum listener address"),
        )
        .arg(
            Arg::new("pool-listen")
                .long("pool-listen")
                .value_name("ADDR")
                .help("G2Pool peer listener address"),
        )
        .arg(
            Arg::new("grin-listen")
                .long("grin-listen")
                .value_name("ADDR")
                .help("embedded Grin P2P listener address"),
        )
}

fn control_data_dir(arguments: &[String]) -> Result<PathBuf> {
    let mut base = g2pool::default_data_dir();
    let mut network = "mainnet";
    let mut data_seen = false;
    let mut network_seen = false;
    let mut index = 0;
    while index < arguments.len() {
        if arguments[index] == "--data-dir" {
            if data_seen {
                return Err(Error::Invalid("repeated argument"));
            }
            data_seen = true;
            index += 1;
            base = arguments
                .get(index)
                .filter(|value| !value.starts_with('-'))
                .ok_or(Error::Invalid("missing data directory"))?
                .into();
        } else if arguments[index] == "--network" {
            if network_seen {
                return Err(Error::Invalid("repeated argument"));
            }
            network_seen = true;
            index += 1;
            network = arguments
                .get(index)
                .filter(|value| matches!(value.as_str(), "mainnet" | "testnet"))
                .ok_or(Error::Invalid("network"))?;
        } else {
            return Err(Error::Invalid("unknown control option"));
        }
        index += 1;
    }
    Ok(g2pool::network_data_dir(base, network))
}

fn run_control(arguments: &[String], command: g2pool::control::Command) -> Result<()> {
    let data = control_data_dir(arguments)?;
    let response = g2pool::control::call(&data, command)?;
    println!("{}", serde_json::to_string_pretty(&response)?);
    if let g2pool::control::ResponseResult::Err { message, .. } = response.result {
        return Err(Error::NativeUnavailable(message));
    }
    Ok(())
}

fn entry(arguments: Vec<String>) -> std::result::Result<(), String> {
    match arguments.first().map(String::as_str) {
        Some("status")
            if arguments[1..]
                .iter()
                .any(|arg| matches!(arg.as_str(), "--help" | "-h")) =>
        {
            println!("g2pool status [--network NETWORK] [--data-dir PATH]");
            println!("Show daemon and wallet status.");
            Ok(())
        }
        Some("status") => run_control(&arguments[1..], g2pool::control::Command::Status)
            .map_err(|error| error.to_string()),
        Some("backup") => g2pool::wallet::cli::run(arguments).map_err(|error| error.to_string()),
        Some("withdraw") => {
            let message = g2pool::wallet::payments::cli::run(arguments.into_iter().skip(1))
                .map_err(|error| error.to_string())?;
            println!("{message}");
            Ok(())
        }
        Some(command) if !command.starts_with('-') => {
            Err("unknown command; use g2pool --help".to_owned())
        }
        _ if arguments
            .iter()
            .any(|arg| matches!(arg.as_str(), "--help" | "-h")) =>
        {
            print!("{}", help_command().render_help());
            Ok(())
        }
        _ => run().map_err(|error| error.to_string()),
    }
}
fn main() {
    if let Err(error) = entry(std::env::args().skip(1).collect()) {
        eprintln!("g2pool: {error}");
        std::process::exit(1)
    }
}

#[cfg(test)]
#[path = "tests/g2pool_tests.rs"]
mod tests;
