use crate::{
    node::{Node, Published},
    Error, Result,
};
use grin_core::core::hash::Hashed;
use serde::{Deserialize, Serialize};
use std::{
    path::PathBuf,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, RwLock,
    },
};
pub(crate) const RESERVE: u64 = 1024 * 1024 * 1024;
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Snapshot {
    pub pool_name: String,
    pub pool_id: String,
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
#[derive(Clone)]
pub struct Status {
    pool_name: String,
    pool_id: String,
    chain: std::sync::Arc<grin_chain::Chain>,
    published: Arc<RwLock<Published>>,
    peers: Arc<AtomicUsize>,
    sync: Arc<grin_chain::SyncState>,
    path: PathBuf,
    database: crate::storage::sharechain::SharechainDb,
}
impl Status {
    pub fn new(node: &Node, pool_name: String, path: PathBuf) -> Self {
        Self {
            pool_name,
            pool_id: crate::objects::hex(&node.profile.pool_id),
            chain: node.chain.clone(),
            published: node.published.clone(),
            peers: node.connected.clone(),
            sync: node.native_sync.clone(),
            path,
            database: node.store.database(),
        }
    }
    pub fn snapshot(&self) -> Result<Snapshot> {
        let snapshot = self
            .published
            .read()
            .map_err(|_| Error::Invalid("publication snapshot poisoned"))?
            .clone();
        let peers = self.peers.load(Ordering::Acquire);
        let syncing = self.sync.is_syncing();
        let head = self
            .chain
            .head_header()
            .map_err(|error| Error::NativeUnavailable(error.to_string()))?;
        let free = fs2::available_space(&self.path).ok();
        let pressure = free.is_none_or(|bytes| bytes < RESERVE);
        Ok(Snapshot {
            pool_name: self.pool_name.clone(),
            pool_id: self.pool_id.clone(),
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
            database_allocated_bytes: self
                .database
                .allocated_bytes()
                .map_err(crate::storage::runtime_error)?,
        })
    }
}

#[cfg(test)]
#[path = "runtime_tests.rs"]
mod tests;
