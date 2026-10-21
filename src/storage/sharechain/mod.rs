#![deny(unsafe_op_in_unsafe_fn)]

use heed::{types::Bytes, Database, Env, EnvOpenOptions, WithoutTls};
mod budget;
pub(crate) use budget::{SOLUTION_RECORDS, WRITE_RESERVE};
mod dependencies;
mod routing;
mod runtime_store;
pub(crate) use dependencies::{
    DependencyBatch, DependencyKey, DependencyManifest, DependencyObject,
};
pub(crate) use runtime_store::{
    ObjectMode, RoutingKey, RuntimeBatch, RuntimeHead, RuntimeLease, RuntimeLimits, RuntimeObject,
    RuntimeReplacement, RuntimeRevision, RuntimeSnapshot, RuntimeStore, StoredObject,
};
use std::path::Path;

pub(crate) const ACQUISITION_LIFETIME: u64 = 900;
pub const MAP_BYTES: usize = 1 << 30;
pub type Id = [u8; 32];
pub type Result<T> = std::result::Result<T, Error>;

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
    #[error("object is retained")]
    ProtectedBody,
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
    pub fn open(path: &Path, map_bytes: usize, protocol: Id) -> Result<Self> {
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
        let runtime = env.create_database(&mut tx, Some("runtime_store"))?;
        let meta: Database<Bytes, Bytes> = env.create_database(&mut tx, Some("meta"))?;
        match meta.get(&tx, b"schema")? {
            Some(b"rolling-sharechain") => {}
            Some(_) => return Err(Error::Invalid("schema")),
            None => meta.put(&mut tx, b"schema", b"rolling-sharechain")?,
        }
        match meta.get(&tx, b"protocol")? {
            Some(previous) if previous != protocol.as_slice() => {
                return Err(Error::Invalid("protocol"))
            }
            Some(previous) if previous.len() != 32 => {
                return Err(Error::Invalid("protocol encoding"))
            }
            Some(_) => {}
            None => meta.put(&mut tx, b"protocol", &protocol)?,
        }
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
