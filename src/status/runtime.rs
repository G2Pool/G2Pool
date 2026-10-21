use crate::{
    node::{Node, Published},
    Error, Result,
};
use grin_core::core::hash::Hashed;
use serde::{Deserialize, Serialize};
use std::{
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc, RwLock,
    },
    thread,
    time::Duration,
};
pub const FILE: &str = "node-status.json";
pub(crate) const RESERVE: u64 = 1024 * 1024 * 1024;
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Snapshot {
    pub updated_at: i64,
    pub state: String,
    pub native_height: u64,
    pub native_head: String,
    pub native_pmmr_size: u64,
    pub connected_peers: usize,
    pub native_syncing: bool,
    pub pool_recovering: bool,
    pub free_disk_bytes: Option<u64>,
    pub database_bytes: u64,
    pub database_limit_bytes: u64,
    pub database_allocated_bytes: u64,
}
fn state(
    syncing: bool,
    recovering: bool,
    peers: usize,
    pool_ready: bool,
    pressure: bool,
) -> &'static str {
    if pressure {
        "degraded"
    } else if syncing {
        "syncing"
    } else if recovering {
        "recovering"
    } else if peers == 0 {
        "disconnected"
    } else if !pool_ready {
        "warming"
    } else {
        "connected"
    }
}
pub struct Monitor {
    chain: std::sync::Arc<grin_chain::Chain>,
    published: Arc<RwLock<Published>>,
    peers: Arc<AtomicUsize>,
    sync: Option<Arc<grin_chain::SyncState>>,
    path: PathBuf,
    database: crate::storage::sharechain::SharechainDb,
}
impl Monitor {
    pub fn new(node: &Node, path: PathBuf, sync: Option<Arc<grin_chain::SyncState>>) -> Self {
        Self {
            chain: node.chain.clone(),
            published: node.published.clone(),
            peers: node.connected.clone(),
            sync,
            path,
            database: node.store.database(),
        }
    }
    pub fn run(self, stop: Arc<AtomicBool>) {
        while !stop.load(Ordering::Acquire) {
            let result = (|| -> Result<()> {
                let snapshot = self
                    .published
                    .read()
                    .map_err(|_| Error::Invalid("publication snapshot poisoned"))?
                    .clone();
                let peers = self.peers.load(Ordering::Acquire);
                let syncing = self.sync.as_ref().is_some_and(|s| s.is_syncing());
                let head = self
                    .chain
                    .head_header()
                    .map_err(|error| Error::NativeUnavailable(error.to_string()))?;
                let free = fs2::available_space(&self.path).ok();
                let pressure = free.is_none_or(|bytes| bytes < RESERVE);
                let status = Snapshot {
                    updated_at: chrono::Utc::now().timestamp(),
                    state: state(
                        syncing,
                        snapshot.recovering,
                        peers,
                        snapshot.revision.is_some(),
                        pressure,
                    )
                    .into(),
                    native_height: head.height,
                    native_head: crate::objects::hex(&crate::native::hash_bytes(head.hash())),
                    native_pmmr_size: head.output_mmr_size,
                    connected_peers: peers,
                    native_syncing: syncing,
                    pool_recovering: snapshot.recovering,
                    free_disk_bytes: free,
                    database_bytes: self
                        .database
                        .used_bytes()
                        .map_err(crate::storage::runtime_error)?,
                    database_limit_bytes: self.database.byte_limit(),
                    database_allocated_bytes: std::fs::metadata(
                        self.path.join("pool.sharechain/data.mdb"),
                    )?
                    .len(),
                };
                crate::status::wallet::publish_json(&self.path.join(FILE), &status)
            })();
            if let Err(error) = result {
                eprintln!("node status unavailable: {error}");
            }
            for _ in 0..40 {
                if stop.load(Ordering::Acquire) {
                    break;
                }
                thread::sleep(Duration::from_millis(25));
            }
        }
    }
}
pub fn read(data: &Path) -> Result<Snapshot> {
    let path = data.join(FILE);
    let bytes = crate::secure_file::read_regular(&path, 4096, false)
        .map_err(|error| Error::NativeUnavailable(format!("node status: {error}")))?;
    let mut snapshot: Snapshot = serde_json::from_slice(&bytes)?;
    let age = chrono::Utc::now()
        .timestamp()
        .checked_sub(snapshot.updated_at);
    if !age.is_some_and(|age| (0..=10).contains(&age)) {
        snapshot.state = "unavailable".into();
    }
    Ok(snapshot)
}

#[cfg(test)]
#[path = "runtime_tests.rs"]
mod tests;
