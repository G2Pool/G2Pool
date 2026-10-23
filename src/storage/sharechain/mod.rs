#![deny(unsafe_op_in_unsafe_fn)]

use heed::{types::Bytes, Database, Env, EnvOpenOptions, WithoutTls};
mod budget;
pub(crate) use budget::{SOLUTION_RECORDS, WRITE_RESERVE};
mod dependencies;
mod routing;
mod runtime_store;
pub(crate) use dependencies::DependencyManifest;
pub(crate) use runtime_store::{
    ObjectMode, Representation, RuntimeBatch, RuntimeHead, RuntimeLease, RuntimeLimits,
    RuntimeObject, RuntimeReplacement, RuntimeRevision, RuntimeSnapshot, RuntimeStore,
    RuntimeVariant, StoredObject,
};
use std::path::Path;

pub(crate) const ACQUISITION_LIFETIME: u64 = 900;
pub(crate) const STORAGE_FORMAT: u32 = 1;
pub const MAP_BYTES: usize = 1 << 30;
pub type Id = [u8; 32];
pub type Result<T> = std::result::Result<T, Error>;

pub(crate) fn physical_digest(bytes: &[u8]) -> Id {
    crate::codec::hash("PHYSICAL_OBJECT", bytes)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RoutingKey {
    pub kind: u8,
    pub id: Id,
}
impl RoutingKey {
    fn bytes(self) -> [u8; 33] {
        let mut bytes = [0; 33];
        bytes[0] = self.kind;
        bytes[1..].copy_from_slice(&self.id);
        bytes
    }
    fn decode(bytes: &[u8]) -> Result<Self> {
        if bytes.len() != 33 {
            return Err(Error::Invalid("routing key"));
        }
        Ok(Self {
            kind: bytes[0],
            id: bytes[1..].try_into().unwrap(),
        })
    }
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("LMDB: {0}")]
    Database(#[from] heed::Error),
    #[error("I/O: {0}")]
    Io(#[from] std::io::Error),
    #[error("invalid sharechain record: {0}")]
    Invalid(&'static str),
    #[error("publication generation changed")]
    Stale,
    #[error("missing sharechain object")]
    Missing,
    #[error("storage quota exceeded: {0}")]
    Quota(&'static str),
}

#[derive(Clone)]
pub struct SharechainDb {
    env: Env<WithoutTls>,
    budget: budget::DatabaseBudget,
    runtime: Database<Bytes, Bytes>,
}
impl SharechainDb {
    pub fn open(path: &Path, map_bytes: usize, pool_id: Id) -> Result<Self> {
        if map_bytes == 0 {
            return Err(Error::Invalid("zero storage limit"));
        }
        std::fs::create_dir_all(path)?;
        if std::fs::symlink_metadata(path)?.file_type().is_symlink() {
            return Err(Error::Invalid("symlink environment"));
        }
        let path = std::fs::canonicalize(path)?;
        let env = unsafe {
            EnvOpenOptions::new()
                .read_txn_without_tls()
                .map_size(map_bytes)
                .max_dbs(32)
                .open(path)?
        };
        let mut tx = env.write_txn()?;
        let catalog: Database<Bytes, Bytes> = env
            .open_database(&tx, None)?
            .ok_or(Error::Invalid("database catalog missing"))?;
        let initializing = catalog.is_empty(&tx)?;
        let meta: Database<Bytes, Bytes> = env.create_database(&mut tx, Some("meta"))?;
        if initializing {
            meta.put(&mut tx, b"storage_format", &STORAGE_FORMAT.to_be_bytes())?;
            meta.put(&mut tx, b"pool_id", &pool_id)?;
        } else {
            if meta.get(&tx, b"storage_format")? != Some(STORAGE_FORMAT.to_be_bytes().as_slice()) {
                return Err(Error::Invalid("storage format"));
            }
            if meta.get(&tx, b"pool_id")? != Some(pool_id.as_slice()) {
                return Err(Error::Invalid("pool identity mismatch"));
            }
        }
        let runtime = env.create_database(&mut tx, Some("runtime_store"))?;
        tx.commit()?;
        let budget = budget::DatabaseBudget::new(env.clone(), map_bytes as u64);
        Ok(Self {
            env,
            budget,
            runtime,
        })
    }

    pub(crate) fn set_byte_limit(&mut self, bytes: u64) {
        self.budget.limit = bytes;
    }
    pub(crate) fn used_bytes(&self) -> Result<u64> {
        self.budget.used(&self.env.read_txn()?)
    }
    pub(crate) fn allocated_bytes(&self) -> Result<u64> {
        Ok(self.env.real_disk_size()?)
    }
    pub(crate) fn byte_limit(&self) -> u64 {
        self.budget.limit
    }
    pub fn runtime_store(&self, limits: RuntimeLimits) -> Result<RuntimeStore> {
        RuntimeStore::open(self.env.clone(), limits, self.budget.clone())
    }
}

#[cfg(test)]
#[path = "mod_tests.rs"]
mod tests;
