#![deny(unsafe_op_in_unsafe_fn)]

use heed::{types::Bytes, Database, Env, EnvOpenOptions, RoTxn, WithoutTls};
mod dependencies;
mod routing;
mod runtime_store;
pub(crate) use dependencies::{
    DependencyBatch, DependencyGcStats, DependencyKey, DependencyManifest, DependencyObject,
};
pub(crate) use runtime_store::{
    ObjectMode, RoutingKey, RuntimeBatch, RuntimeHead, RuntimeLease, RuntimeLimits, RuntimeObject,
    RuntimeReplacement, RuntimeRevision, RuntimeSnapshot, RuntimeStore, StoredObject,
};
use std::collections::HashSet;
use std::path::Path;

pub const MAP_BYTES: usize = 1 << 30;
pub const MAX_RECORD_BYTES: usize = 16 * 1024 * 1024;
pub type Id = [u8; 32];
pub type Result<T> = std::result::Result<T, Error>;
const MAX_WALLET_IDENTITY_BYTES: usize = 64 * 1024 * 1024;

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
    #[error("missing sharechain header")]
    Missing,
    #[error("body is still referenced by retained sharechain state")]
    ProtectedBody,
}

mod schema;
pub(crate) use schema::*;

#[derive(Clone)]
pub struct SharechainDb {
    env: Env<WithoutTls>,
    headers: Database<Bytes, Bytes>,
    ancestors: Database<Bytes, Bytes>,
    bodies: Database<Bytes, Bytes>,
    deltas: Database<Bytes, Bytes>,
    body_hashes: Database<Bytes, Bytes>,
    states: Database<Bytes, Bytes>,
    meta: Database<Bytes, Bytes>,
    evaluations: Database<Bytes, Bytes>,
    evaluation_bindings: Database<Bytes, Bytes>,
    evaluation_binding_refs: Database<Bytes, Bytes>,
    runtime: Database<Bytes, Bytes>,
    max_record_bytes: usize,
    dependencies: dependencies::DependencyDb,
}
pub struct WalletIdentity {
    pub account: Option<[u8; 33]>,
    pub registrations: Vec<Vec<u8>>,
}
impl SharechainDb {
    pub fn existing_wallet_identity(path: &Path) -> Result<WalletIdentity> {
        if !std::fs::symlink_metadata(path)?.is_dir()
            || std::fs::symlink_metadata(path)?.file_type().is_symlink()
            || !path.join("data.mdb").is_file()
        {
            return Err(Error::Invalid("existing wallet environment"));
        }
        let path = std::fs::canonicalize(path)?;
        let map_bytes = std::fs::metadata(path.join("data.mdb"))
            .map(|metadata| metadata.len().saturating_add(1 << 20))
            .ok()
            .and_then(|bytes| usize::try_from(bytes).ok())
            .unwrap_or(MAP_BYTES)
            .max(MAP_BYTES);
        let env = unsafe {
            EnvOpenOptions::new()
                .read_txn_without_tls()
                .map_size(map_bytes)
                .max_dbs(32)
                .open(path)?
        };
        let tx = env.read_txn()?;
        let meta: Database<Bytes, Bytes> = env
            .open_database(&tx, Some("meta"))?
            .ok_or(Error::Invalid("wallet metadata missing"))?;
        if meta.get(&tx, b"schema")? != Some(b"sharechain-index".as_slice()) {
            return Err(Error::Invalid("schema"));
        }
        let db: Database<Bytes, Bytes> = env
            .open_database(&tx, Some("runtime_store"))?
            .ok_or(Error::Invalid("wallet records missing"))?;
        let account = db
            .get(&tx, b"aux/wallet/record/account")?
            .map(|v| v.try_into().map_err(|_| Error::Invalid("wallet account")))
            .transpose()?;
        let mut registrations = Vec::new();
        let mut registration_bytes = 0usize;
        for row in db.prefix_iter(&tx, b"aux/wallet/record/registration:")? {
            let (key, bytes) = row?;
            if registrations.len() >= 100_000 || key.len() > 512 || bytes.len() > 256 * 1024 {
                return Err(Error::Invalid("wallet registration bounds"));
            }
            registration_bytes = registration_bytes
                .checked_add(bytes.len())
                .ok_or(Error::Invalid("wallet identity bounds"))?;
            if registration_bytes > MAX_WALLET_IDENTITY_BYTES {
                return Err(Error::Invalid("wallet identity bounds"));
            }
            registrations.push(bytes.to_vec());
        }
        Ok(WalletIdentity {
            account,
            registrations,
        })
    }

    pub fn open(
        path: &Path,
        map_bytes: usize,
        max_record_bytes: usize,
        protocol: Id,
    ) -> Result<Self> {
        if map_bytes == 0 || max_record_bytes == 0 {
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
        let headers = env.create_database(&mut tx, Some("headers"))?;
        let ancestors = env.create_database(&mut tx, Some("ancestors"))?;
        let bodies = env.create_database(&mut tx, Some("bodies"))?;
        let deltas = env.create_database(&mut tx, Some("deltas"))?;
        let body_hashes = env.create_database(&mut tx, Some("body_hashes"))?;
        let states = env.create_database(&mut tx, Some("states"))?;
        let evaluations = env.create_database(&mut tx, Some("evaluations"))?;
        let evaluation_bindings = env.create_database(&mut tx, Some("evaluation_bindings"))?;
        let evaluation_binding_refs =
            env.create_database(&mut tx, Some("evaluation_binding_refs"))?;
        let runtime = env.create_database(&mut tx, Some("runtime_store"))?;
        let meta: Database<Bytes, Bytes> = env.create_database(&mut tx, Some("meta"))?;
        match meta.get(&tx, b"schema")? {
            Some(b"sharechain-index") => {}
            Some(_) => return Err(Error::Invalid("schema")),
            None => meta.put(&mut tx, b"schema", b"sharechain-index")?,
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
        let dependencies = dependencies::DependencyDb::open(&env, &mut tx)?;
        tx.commit()?;
        Ok(Self {
            env,
            headers,
            ancestors,
            bodies,
            deltas,
            body_hashes,
            states,
            meta,
            evaluations,
            evaluation_bindings,
            evaluation_binding_refs,
            runtime,
            max_record_bytes,
            dependencies,
        })
    }

    pub fn runtime_store(&self, limits: RuntimeLimits) -> Result<RuntimeStore> {
        RuntimeStore::open(self.env.clone(), limits)
    }

    pub fn read(&self) -> Result<Snapshot<'_>> {
        Ok(Snapshot {
            db: self,
            tx: self.env.read_txn()?,
        })
    }
    pub fn publish_validated(
        &self,
        expected_generation: u64,
        records: &[Record<'_>],
        head: Id,
    ) -> Result<Publication> {
        self.publish_inner(expected_generation, records, head, None, None, true)
    }
    pub fn drain_dependency_gc(&self, max_nodes: usize) -> Result<DependencyGcStats> {
        let mut tx = self.env.write_txn()?;
        let stats = self.dependencies.drain_gc(&mut tx, max_nodes)?;
        tx.commit()?;
        Ok(stats)
    }

    pub fn publish_validated_with_dependencies(
        &self,
        expected_generation: u64,
        records: &[Record<'_>],
        head: Id,
        head_manifest: Id,
        dependencies: &DependencyBatch<'_>,
    ) -> Result<Publication> {
        self.publish_inner(
            expected_generation,
            records,
            head,
            Some((head_manifest, dependencies)),
            None,
            true,
        )
    }
    pub fn insert_validated_with_evaluation(
        &self,
        expected_generation: u64,
        records: &[Record<'_>],
        head: Id,
        head_manifest: Id,
        dependencies: &DependencyBatch<'_>,
        evaluation: EvaluationPublication<'_>,
    ) -> Result<Publication> {
        if evaluation.tip != head {
            return Err(Error::Invalid("evaluation head"));
        }
        self.publish_inner(
            expected_generation,
            records,
            head,
            Some((head_manifest, dependencies)),
            Some(evaluation),
            false,
        )
    }
    pub fn insert_validated(&self, expected_generation: u64, records: &[Record<'_>]) -> Result<()> {
        let head = records.last().ok_or(Error::Invalid("empty records"))?.id;
        self.publish_inner(expected_generation, records, head, None, None, false)
            .map(|_| ())
    }
    fn publish_inner(
        &self,
        expected_generation: u64,
        records: &[Record<'_>],
        head: Id,
        dependencies: Option<(Id, &DependencyBatch<'_>)>,
        evaluation: Option<EvaluationPublication<'_>>,
        select: bool,
    ) -> Result<Publication> {
        if records.len() > PUBLICATION_RECORDS {
            return Err(Error::Invalid("sharechain batch limit"));
        }
        let mut bytes = 0usize;
        let mut unique = HashSet::new();
        for record in records {
            if !unique.insert(record.id) {
                return Err(Error::Invalid("duplicate sharechain record"));
            }
            for field in [Some(record.body), Some(record.delta), Some(record.state)]
                .into_iter()
                .flatten()
            {
                if field.len() > self.max_record_bytes {
                    return Err(Error::Invalid("record size"));
                }
                bytes = bytes
                    .checked_add(field.len())
                    .filter(|total| *total <= PUBLICATION_BYTES)
                    .ok_or(Error::Invalid("sharechain batch bytes limit"))?;
            }
        }
        let mut tx = self.env.write_txn()?;
        let generation =
            publication(self.meta.get(&tx, b"publication")?)?.map_or(0, |p| p.generation);
        if generation != expected_generation {
            return Err(Error::Stale);
        }
        let next = if select {
            generation
                .checked_add(1)
                .ok_or(Error::Invalid("generation overflow"))?
        } else {
            generation
        };
        for record in records {
            for bytes in [Some(record.body), Some(record.delta), Some(record.state)]
                .into_iter()
                .flatten()
            {
                if bytes.len() > self.max_record_bytes {
                    return Err(Error::Invalid("record size"));
                }
            }
            if record.header.work == 0 {
                return Err(Error::Invalid("zero work"));
            }
            let encoded = record.header.encode();
            let body_hash = blake2::blake2b::blake2b(32, &[], record.body);
            if let Some(old) = self.headers.get(&tx, &record.id)? {
                let old_delta = self.deltas.get(&tx, &record.id)?;
                let old_state = self.states.get(&tx, &record.id)?;
                if old != encoded
                    || old_delta.map_or(!record.delta.is_empty(), |old| old != record.delta)
                    || old_state.map_or(!record.state.is_empty(), |old| old != record.state)
                {
                    return Err(Error::Invalid("immutable record changed"));
                }
                if self.body_hashes.get(&tx, &record.id)? != Some(body_hash.as_bytes()) {
                    return Err(Error::Invalid("immutable pruned body changed"));
                }
                if let Some(old_body) = self.bodies.get(&tx, &record.id)? {
                    if old_body != record.body {
                        return Err(Error::Invalid("immutable body changed"));
                    }
                }
                self.bodies.put(&mut tx, &record.id, record.body)?;
                continue;
            }
            let mut jumps = Vec::<Id>::new();
            match record.header.parent {
                None => {
                    if record.header.height != 0
                        || record.header.cumulative_work != record.header.work
                    {
                        return Err(Error::Invalid("root arithmetic"));
                    }
                }
                Some(parent_id) => {
                    let parent = self
                        .headers
                        .get(&tx, &parent_id)?
                        .ok_or(Error::Missing)
                        .and_then(Header::decode)?;
                    if parent.height.checked_add(1) != Some(record.header.height)
                        || parent.state_root != record.header.parent_state_root
                        || parent.cumulative_work.checked_add(record.header.work)
                            != Some(record.header.cumulative_work)
                    {
                        return Err(Error::Invalid("parent arithmetic"));
                    }
                    if record.header.height >= ANCESTOR_STRIDE
                        && record.header.height.is_multiple_of(ANCESTOR_STRIDE)
                    {
                        let first = ancestor_tx(
                            &self.headers,
                            &self.ancestors,
                            &tx,
                            parent_id,
                            record.header.height - ANCESTOR_STRIDE,
                        )?;
                        jumps.push(first);
                        let mut level = 0usize;
                        loop {
                            let bytes = self
                                .ancestors
                                .get(&tx, &jumps[level])?
                                .ok_or(Error::Invalid("missing ancestor index"))?;
                            let Some(next) = bytes
                                .get(level * 32..(level + 1) * 32)
                                .and_then(|part| part.try_into().ok())
                            else {
                                break;
                            };
                            jumps.push(next);
                            level += 1;
                        }
                    }
                }
            }
            self.headers.put(&mut tx, &record.id, &encoded)?;
            let jump_bytes: Vec<u8> = jumps.into_iter().flatten().collect();
            if record.header.height.is_multiple_of(ANCESTOR_STRIDE) {
                self.ancestors.put(&mut tx, &record.id, &jump_bytes)?;
            }
            self.bodies.put(&mut tx, &record.id, record.body)?;
            self.body_hashes
                .put(&mut tx, &record.id, body_hash.as_bytes())?;
            if !record.delta.is_empty() {
                self.deltas.put(&mut tx, &record.id, record.delta)?;
            }
            if !record.state.is_empty() {
                self.states.put(&mut tx, &record.id, record.state)?;
            }
        }
        let header = self
            .headers
            .get(&tx, &head)?
            .ok_or(Error::Missing)
            .and_then(Header::decode)?;
        let published = Publication {
            generation: next,
            head,
            state_root: header.state_root,
        };
        let mut bytes = Vec::with_capacity(72);
        bytes.extend_from_slice(&next.to_be_bytes());
        bytes.extend_from_slice(&head);
        bytes.extend_from_slice(&header.state_root);
        if select {
            self.meta.put(&mut tx, b"publication", &bytes)?;
        }
        if let Some((head_manifest, batch)) = dependencies {
            self.protect_evaluation_bindings(&tx, batch)?;
            self.dependencies
                .apply(&mut tx, batch, self.max_record_bytes)?;
            self.dependencies.root(&tx, &head_manifest)?;
            if select {
                self.meta
                    .put(&mut tx, b"publication-manifest", &head_manifest)?;
            }
        } else if select {
            self.meta.delete(&mut tx, b"publication-manifest")?;
        }
        if let Some(evaluation) = evaluation {
            if evaluation.tip != head {
                return Err(Error::Invalid("evaluation head"));
            }
            self.write_evaluation_record(
                &mut tx,
                evaluation.tip,
                evaluation.grin_height,
                evaluation.grin_hash,
                evaluation.payout,
                Some(evaluation.manifest),
            )?;
        }
        tx.commit()?;
        Ok(published)
    }
    pub fn store_evaluation(
        &self,
        tip: Id,
        grin_height: u64,
        grin_hash: Id,
        payout: &[u8],
    ) -> Result<EvaluationSnapshot> {
        self.store_evaluation_inner(tip, grin_height, grin_hash, payout, None)
    }
    pub fn store_evaluation_with_dependencies(
        &self,
        tip: Id,
        grin_height: u64,
        grin_hash: Id,
        payout: &[u8],
        manifest: Id,
        dependencies: &DependencyBatch<'_>,
    ) -> Result<EvaluationSnapshot> {
        self.store_evaluation_inner(
            tip,
            grin_height,
            grin_hash,
            payout,
            Some((manifest, dependencies)),
        )
    }
    pub fn retire_evaluation_binding(
        &self,
        tip: Id,
        grin_height: u64,
        grin_hash: Id,
        manifest: Id,
    ) -> Result<bool> {
        let mut tx = self.env.write_txn()?;
        let binding = evaluation_binding_key(evaluation_key(tip, grin_height, grin_hash), manifest);
        let Some(root) = self.evaluation_bindings.get(&tx, &binding)? else {
            return Ok(false);
        };
        if root.len() != 32 || self.dependencies.root(&tx, &manifest)? != root {
            return Err(Error::Invalid("evaluation binding root"));
        }
        self.evaluation_bindings.delete(&mut tx, &binding)?;
        let remaining = self.change_evaluation_binding_refs(&mut tx, manifest, false)?;
        let selected = self
            .meta
            .get(&tx, b"publication-manifest")?
            .is_some_and(|id| id == manifest.as_slice());
        if remaining == 0 && !selected {
            self.dependencies.apply(
                &mut tx,
                &DependencyBatch {
                    release: &[manifest],
                    ..Default::default()
                },
                self.max_record_bytes,
            )?;
        }
        tx.commit()?;
        Ok(true)
    }

    pub fn retire_obsolete_evaluation_bindings(
        &self,
        finance_low: u64,
        protected_tips: &[Id],
        protected_manifests: &[Id],
        limit: usize,
    ) -> Result<usize> {
        use std::ops::Bound;
        if limit == 0
            || limit > 256
            || protected_tips.len() > 4096
            || protected_manifests.len() > 4096
        {
            return Err(Error::Invalid("evaluation retirement bounds"));
        }
        let mut tx = self.env.write_txn()?;
        let cursor = self
            .meta
            .get(&tx, b"evaluation_retirement_cursor")?
            .map(|bytes| bytes.to_vec());
        if cursor.as_ref().is_some_and(|cursor| cursor.len() != 104) {
            return Err(Error::Invalid("evaluation retirement cursor"));
        }
        let start = cursor.as_deref().map_or(Bound::Unbounded, Bound::Excluded);
        let range = (start, Bound::Unbounded);
        let rows: Vec<_> = self
            .evaluation_bindings
            .range(&tx, &range)?
            .take(limit)
            .map(|row| row.map(|(key, root)| (key.to_vec(), root.to_vec())))
            .collect::<std::result::Result<_, _>>()?;
        let mut retired = 0;
        for (key, root) in &rows {
            if key.len() != 104 || root.len() != 32 {
                return Err(Error::Invalid("evaluation binding encoding"));
            }
            let tip: Id = key[..32].try_into().unwrap();
            let height = u64::from_be_bytes(key[32..40].try_into().unwrap());
            let manifest: Id = key[72..].try_into().unwrap();
            if height >= finance_low
                || protected_tips.contains(&tip)
                || protected_manifests.contains(&manifest)
            {
                continue;
            }
            if self.dependencies.root(&tx, &manifest)? != root.as_slice() {
                return Err(Error::Invalid("evaluation binding root"));
            }
            self.evaluation_bindings.delete(&mut tx, key)?;
            let remaining = self.change_evaluation_binding_refs(&mut tx, manifest, false)?;
            let evaluation_key_bytes = &key[..72];
            let has_other_binding = self
                .evaluation_bindings
                .prefix_iter(&tx, evaluation_key_bytes)?
                .next()
                .transpose()?
                .is_some();
            if !has_other_binding {
                self.evaluations.delete(&mut tx, evaluation_key_bytes)?;
            }
            let selected = self
                .meta
                .get(&tx, b"publication-manifest")?
                .is_some_and(|id| id == manifest.as_slice());
            if remaining == 0 && !selected {
                self.dependencies.apply(
                    &mut tx,
                    &DependencyBatch {
                        release: &[manifest],
                        ..Default::default()
                    },
                    self.max_record_bytes,
                )?;
            }
            retired += 1;
        }
        if rows.len() == limit {
            self.meta.put(
                &mut tx,
                b"evaluation_retirement_cursor",
                &rows.last().unwrap().0,
            )?;
        } else {
            self.meta.delete(&mut tx, b"evaluation_retirement_cursor")?;
        }

        let remaining = limit - retired;
        if remaining != 0 {
            let evaluation_cursor = self
                .meta
                .get(&tx, b"evaluation_retirement_evaluation_cursor")?
                .map(|bytes| bytes.to_vec());
            if evaluation_cursor
                .as_ref()
                .is_some_and(|cursor| cursor.len() != 72)
            {
                return Err(Error::Invalid("evaluation retirement evaluation cursor"));
            }
            let evaluation_start = evaluation_cursor
                .as_deref()
                .map_or(Bound::Unbounded, Bound::Excluded);
            let evaluation_range = (evaluation_start, Bound::Unbounded);
            let evaluation_rows: Vec<_> = self
                .evaluations
                .range(&tx, &evaluation_range)?
                .take(remaining)
                .map(|row| row.map(|(key, value)| (key.to_vec(), value.to_vec())))
                .collect::<std::result::Result<_, _>>()?;
            for (key, value) in &evaluation_rows {
                if key.len() != 72 {
                    return Err(Error::Invalid("evaluation key encoding"));
                }
                decode_evaluation(value, self.max_record_bytes)?;
                let tip: Id = key[..32].try_into().unwrap();
                let height = u64::from_be_bytes(key[32..40].try_into().unwrap());
                if height >= finance_low || protected_tips.contains(&tip) {
                    continue;
                }
                if self
                    .evaluation_bindings
                    .prefix_iter(&tx, key)?
                    .next()
                    .transpose()?
                    .is_some()
                {
                    continue;
                }
                self.evaluations.delete(&mut tx, key)?;
                retired += 1;
            }
            if evaluation_rows.len() == remaining {
                self.meta.put(
                    &mut tx,
                    b"evaluation_retirement_evaluation_cursor",
                    &evaluation_rows.last().unwrap().0,
                )?;
            } else {
                self.meta
                    .delete(&mut tx, b"evaluation_retirement_evaluation_cursor")?;
            }
        }
        tx.commit()?;
        Ok(retired)
    }
    fn protect_evaluation_bindings(
        &self,
        tx: &RoTxn<'_, WithoutTls>,
        batch: &DependencyBatch<'_>,
    ) -> Result<()> {
        for manifest in batch.release {
            if self.evaluation_binding_refs(tx, *manifest)? != 0 {
                return Err(Error::Invalid("evaluation dependency manifest protected"));
            }
        }
        Ok(())
    }
    fn evaluation_binding_refs(&self, tx: &RoTxn<'_, WithoutTls>, manifest: Id) -> Result<u64> {
        match self.evaluation_binding_refs.get(tx, &manifest)? {
            None => Ok(0),
            Some(bytes) if bytes.len() == 8 => {
                let count = u64::from_be_bytes(bytes.try_into().unwrap());
                if count == 0 {
                    return Err(Error::Invalid("evaluation binding refcount encoding"));
                }
                Ok(count)
            }
            Some(_) => Err(Error::Invalid("evaluation binding refcount encoding")),
        }
    }
    fn change_evaluation_binding_refs(
        &self,
        tx: &mut heed::RwTxn<'_>,
        manifest: Id,
        add: bool,
    ) -> Result<u64> {
        let previous = self.evaluation_binding_refs(tx, manifest)?;
        let count = if add {
            previous.checked_add(1)
        } else {
            previous.checked_sub(1)
        }
        .ok_or(Error::Invalid(
            "evaluation binding refcount overflow or underflow",
        ))?;
        if count == 0 {
            self.evaluation_binding_refs.delete(tx, &manifest)?;
        } else {
            self.evaluation_binding_refs
                .put(tx, &manifest, &count.to_be_bytes())?;
        }
        Ok(count)
    }
    fn write_evaluation_record(
        &self,
        tx: &mut heed::RwTxn<'_>,
        tip: Id,
        grin_height: u64,
        grin_hash: Id,
        payout: &[u8],
        manifest: Option<Id>,
    ) -> Result<[u8; 32]> {
        if payout.is_empty() || payout.len() > self.max_record_bytes {
            return Err(Error::Invalid("evaluation size"));
        }
        Header::decode(self.headers.get(tx, &tip)?.ok_or(Error::Missing)?)?;
        let key = evaluation_key(tip, grin_height, grin_hash);
        let digest = evaluation_digest(payout);
        let mut encoded = Vec::with_capacity(32 + payout.len());
        encoded.extend_from_slice(&digest);
        encoded.extend_from_slice(payout);
        match self.evaluations.get(tx, &key)? {
            Some(old) => {
                decode_evaluation(old, self.max_record_bytes)?;
                if old != encoded {
                    return Err(Error::Invalid("immutable evaluation changed"));
                }
            }
            None => self.evaluations.put(tx, &key, &encoded)?,
        }
        if let Some(manifest) = manifest {
            let root = self.dependencies.root(tx, &manifest)?;
            let binding = evaluation_binding_key(key, manifest);
            match self.evaluation_bindings.get(tx, &binding)? {
                Some(old) if old != root => {
                    return Err(Error::Invalid("immutable evaluation binding changed"));
                }
                Some(_) => {
                    if self.evaluation_binding_refs(tx, manifest)? == 0 {
                        return Err(Error::Invalid("evaluation binding missing refcount"));
                    }
                }
                None => {
                    self.change_evaluation_binding_refs(tx, manifest, true)?;
                    self.evaluation_bindings.put(tx, &binding, &root)?;
                }
            }
        }
        Ok(digest)
    }

    fn store_evaluation_inner(
        &self,
        tip: Id,
        grin_height: u64,
        grin_hash: Id,
        payout: &[u8],
        dependencies: Option<(Id, &DependencyBatch<'_>)>,
    ) -> Result<EvaluationSnapshot> {
        let mut tx = self.env.write_txn()?;
        let dependency_manifest = if let Some((manifest, batch)) = dependencies {
            self.protect_evaluation_bindings(&tx, batch)?;
            self.dependencies
                .apply(&mut tx, batch, self.max_record_bytes)?;
            Some(manifest)
        } else {
            None
        };
        let digest = self.write_evaluation_record(
            &mut tx,
            tip,
            grin_height,
            grin_hash,
            payout,
            dependency_manifest,
        )?;
        tx.commit()?;
        Ok(EvaluationSnapshot {
            digest,
            payout: payout.to_vec(),
            dependency_manifest,
        })
    }
    pub fn prune_unreferenced_bodies_bounded(
        &self,
        selected_head: Id,
        retain_from_height: u64,
        limit: usize,
    ) -> Result<usize> {
        use std::ops::Bound;
        if limit == 0 || limit > PUBLICATION_RECORDS {
            return Err(Error::Invalid("body gc limit"));
        }
        let mut tx = self.env.write_txn()?;
        let cursor = self
            .meta
            .get(&tx, b"body_gc_cursor")?
            .map(|bytes| bytes.to_vec());
        if cursor.as_ref().is_some_and(|bytes| bytes.len() != 32) {
            return Err(Error::Invalid("body gc cursor"));
        }
        let start = cursor.as_deref().map_or(Bound::Unbounded, Bound::Excluded);
        let selected = self
            .headers
            .get(&tx, &selected_head)?
            .ok_or(Error::Missing)
            .and_then(Header::decode)?;
        if retain_from_height > selected.height {
            return Err(Error::Invalid("body retention above selected head"));
        }
        let mut rows = Vec::new();
        for row in self
            .bodies
            .range(&tx, &(start, Bound::Unbounded))?
            .take(limit)
        {
            let (key, _) = row?;
            let id: Id = key
                .try_into()
                .map_err(|_| Error::Invalid("body id encoding"))?;
            rows.push(id);
        }
        let mut removed = 0;
        for id in &rows {
            let header = self
                .headers
                .get(&tx, id)?
                .ok_or(Error::Missing)
                .and_then(Header::decode)?;
            let evaluated = self
                .evaluations
                .prefix_iter(&tx, id)?
                .next()
                .transpose()?
                .is_some();
            if header.height < retain_from_height && !evaluated {
                removed += usize::from(self.bodies.delete(&mut tx, id)?);
            }
        }
        if rows.len() == limit {
            self.meta
                .put(&mut tx, b"body_gc_cursor", rows.last().unwrap())?;
        } else {
            self.meta.delete(&mut tx, b"body_gc_cursor")?;
        }
        tx.commit()?;
        Ok(removed)
    }

    pub fn prune_headers_bounded(
        &self,
        retained_heads: &[Id],
        retain_from_height: u64,
        limit: usize,
    ) -> Result<usize> {
        use std::ops::Bound;
        if limit == 0 || limit > PUBLICATION_RECORDS || retained_heads.len() > 4096 {
            return Err(Error::Invalid("header gc bounds"));
        }
        let mut tx = self.env.write_txn()?;
        let mut protected = HashSet::new();
        for tip in retained_heads {
            let mut cursor = Some(*tip);
            while let Some(id) = cursor {
                if !protected.insert(id) {
                    break;
                }
                let Some(header) = self.headers.get(&tx, &id)? else {
                    break;
                };
                let header = Header::decode(header)?;
                if header.height < retain_from_height
                    && (header.height == 0 || header.height.is_multiple_of(ANCESTOR_STRIDE))
                {
                    break;
                }
                cursor = header.parent;
            }
        }
        let cursor = self
            .meta
            .get(&tx, b"header_gc_cursor")?
            .map(ToOwned::to_owned);
        if cursor.as_ref().is_some_and(|v| v.len() != 32) {
            return Err(Error::Invalid("header gc cursor"));
        }
        let start = cursor.as_deref().map_or(Bound::Unbounded, Bound::Excluded);
        let mut candidates = Vec::new();
        let mut examined = 0usize;
        let mut last_examined = None;
        for row in self
            .headers
            .range(&tx, &(start, Bound::Unbounded))?
            .take(limit)
        {
            examined += 1;
            let (key, value) = row?;
            last_examined = Some(key.to_vec());
            let id: Id = key.try_into().map_err(|_| Error::Invalid("header id"))?;
            let header = Header::decode(value)?;
            let checkpoint = header.height == 0 || header.height.is_multiple_of(ANCESTOR_STRIDE);
            if header.height < retain_from_height && !protected.contains(&id) && !checkpoint {
                candidates.push((id, header.height));
            }
        }
        candidates.sort_unstable_by_key(|(_, height)| std::cmp::Reverse(*height));
        let removed = candidates.len();
        for (id, _) in &candidates {
            self.headers.delete(&mut tx, id)?;
            self.ancestors.delete(&mut tx, id)?;
            self.bodies.delete(&mut tx, id)?;
            self.deltas.delete(&mut tx, id)?;
            self.body_hashes.delete(&mut tx, id)?;
            self.states.delete(&mut tx, id)?;
        }
        if examined == limit {
            self.meta.put(
                &mut tx,
                b"header_gc_cursor",
                last_examined.as_deref().unwrap(),
            )?;
        } else {
            self.meta.delete(&mut tx, b"header_gc_cursor")?;
        }
        tx.commit()?;
        Ok(removed)
    }
    pub fn publish_head(&self, expected_generation: u64, head: Id) -> Result<Publication> {
        let mut tx = self.env.write_txn()?;
        let publication = self.publish_head_in(&mut tx, expected_generation, head)?;
        tx.commit()?;
        Ok(publication)
    }
    pub(crate) fn publish_head_in(
        &self,
        tx: &mut heed::RwTxn<'_>,
        expected_generation: u64,
        head: Id,
    ) -> Result<Publication> {
        let generation = publication(self.meta.get(tx, b"publication")?)?
            .map_or(0, |publication| publication.generation);
        if generation != expected_generation {
            return Err(Error::Stale);
        }
        let header = self
            .headers
            .get(tx, &head)?
            .ok_or(Error::Missing)
            .and_then(Header::decode)?;
        let next = generation
            .checked_add(1)
            .ok_or(Error::Invalid("generation overflow"))?;
        let publication = Publication {
            generation: next,
            head,
            state_root: header.state_root,
        };
        let mut bytes = Vec::with_capacity(72);
        bytes.extend_from_slice(&next.to_be_bytes());
        bytes.extend_from_slice(&head);
        bytes.extend_from_slice(&header.state_root);
        self.meta.put(&mut *tx, b"publication", &bytes)?;
        self.meta.delete(&mut *tx, b"publication-manifest")?;
        Ok(publication)
    }
}
fn ancestor_tx(
    headers: &Database<Bytes, Bytes>,
    ancestors: &Database<Bytes, Bytes>,
    tx: &RoTxn<'_, WithoutTls>,
    mut id: Id,
    height: u64,
) -> Result<Id> {
    let current = headers
        .get(tx, &id)?
        .ok_or(Error::Missing)
        .and_then(Header::decode)?;
    if height > current.height {
        return Err(Error::Invalid("ancestor above tip"));
    }
    let mut current_height = current.height;
    while current_height > height {
        let distance = current_height - height;
        if current_height.is_multiple_of(ANCESTOR_STRIDE) && distance >= ANCESTOR_STRIDE {
            let units = distance / ANCESTOR_STRIDE;
            let level = (u64::BITS - 1 - units.leading_zeros()) as usize;
            let bytes = ancestors
                .get(tx, &id)?
                .ok_or(Error::Invalid("missing sparse ancestor index"))?;
            id = bytes
                .get(level * 32..(level + 1) * 32)
                .ok_or(Error::Invalid("sparse ancestor index"))?
                .try_into()
                .unwrap();
            current_height -= ANCESTOR_STRIDE << level;
        } else {
            let bytes = headers.get(tx, &id)?.ok_or(Error::Missing)?;
            let header = Header::decode(bytes)?;
            id = header.parent.ok_or(Error::Invalid("ancestor root"))?;
            current_height -= 1;
        }
    }
    Ok(id)
}

fn publication(bytes: Option<&[u8]>) -> Result<Option<Publication>> {
    bytes
        .map(|b| {
            if b.len() != 72 {
                return Err(Error::Invalid("publication encoding"));
            }
            Ok(Publication {
                generation: u64::from_be_bytes(b[..8].try_into().unwrap()),
                head: b[8..40].try_into().unwrap(),
                state_root: b[40..72].try_into().unwrap(),
            })
        })
        .transpose()
}

pub struct Snapshot<'a> {
    db: &'a SharechainDb,
    tx: RoTxn<'a, WithoutTls>,
}
impl Snapshot<'_> {
    pub fn dependencies(&self, manifest: &Id) -> Result<Vec<DependencyKey>> {
        self.db.dependencies.resolve(&self.tx, manifest)
    }
    pub fn dependency_object(&self, key: DependencyKey) -> Result<Option<&[u8]>> {
        self.db.dependencies.object(&self.tx, key)
    }
    pub fn dependency_root(&self, id: &Id) -> Result<Id> {
        self.db.dependencies.root(&self.tx, id)
    }
    pub fn dependency_retained(&self, id: &Id) -> Result<bool> {
        self.db.dependencies.retained(&self.tx, id)
    }

    pub fn publication(&self) -> Result<Option<Publication>> {
        publication(self.db.meta.get(&self.tx, b"publication")?)
    }
    pub fn header(&self, id: &Id) -> Result<Option<Header>> {
        self.db
            .headers
            .get(&self.tx, id)?
            .map(Header::decode)
            .transpose()
    }
    pub fn body(&self, id: &Id) -> Result<Option<&[u8]>> {
        Ok(self.db.bodies.get(&self.tx, id)?)
    }
    pub fn delta(&self, id: &Id) -> Result<Option<&[u8]>> {
        Ok(self.db.deltas.get(&self.tx, id)?)
    }
    pub fn evaluation(
        &self,
        tip: Id,
        grin_height: u64,
        grin_hash: Id,
    ) -> Result<Option<EvaluationSnapshot>> {
        self.db
            .evaluations
            .get(&self.tx, &evaluation_key(tip, grin_height, grin_hash))?
            .map(|bytes| decode_evaluation(bytes, self.db.max_record_bytes))
            .transpose()
    }
    pub fn evaluation_with_dependencies(
        &self,
        tip: Id,
        grin_height: u64,
        grin_hash: Id,
        manifest: Id,
    ) -> Result<Option<EvaluationSnapshot>> {
        let key = evaluation_key(tip, grin_height, grin_hash);
        let binding = evaluation_binding_key(key, manifest);
        let Some(root) = self.db.evaluation_bindings.get(&self.tx, &binding)? else {
            return Ok(None);
        };
        self.validate_evaluation_binding(manifest, root)?;
        let mut evaluation = self
            .evaluation(tip, grin_height, grin_hash)?
            .ok_or(Error::Invalid("evaluation binding missing payout"))?;
        evaluation.dependency_manifest = Some(manifest);
        Ok(Some(evaluation))
    }
    pub fn evaluation_bindings(
        &self,
        tip: Id,
        grin_height: u64,
        grin_hash: Id,
        limit: usize,
    ) -> Result<Vec<Id>> {
        let key = evaluation_key(tip, grin_height, grin_hash);
        let mut manifests = Vec::new();
        for binding in self
            .db
            .evaluation_bindings
            .prefix_iter(&self.tx, &key)?
            .take(limit)
        {
            let (binding, root) = binding?;
            if binding.len() != 104 {
                return Err(Error::Invalid("evaluation binding encoding"));
            }
            let manifest = binding[72..].try_into().unwrap();
            self.validate_evaluation_binding(manifest, root)?;
            manifests.push(manifest);
        }
        if !manifests.is_empty() && self.evaluation(tip, grin_height, grin_hash)?.is_none() {
            return Err(Error::Invalid("evaluation binding missing payout"));
        }
        Ok(manifests)
    }
    fn validate_evaluation_binding(&self, manifest: Id, root: &[u8]) -> Result<()> {
        if self.db.evaluation_binding_refs(&self.tx, manifest)? == 0 {
            return Err(Error::Invalid("evaluation binding missing refcount"));
        }
        if root.len() != 32 || self.db.dependencies.root(&self.tx, &manifest)? != root {
            return Err(Error::Invalid("evaluation binding root"));
        }
        Ok(())
    }
    pub fn ancestor(&self, id: Id, height: u64) -> Result<Id> {
        ancestor_tx(&self.db.headers, &self.db.ancestors, &self.tx, id, height)
    }
    pub fn common_ancestor(&self, mut a: Id, mut b: Id) -> Result<Id> {
        let ah = self.header(&a)?.ok_or(Error::Missing)?.height;
        let bh = self.header(&b)?.ok_or(Error::Missing)?.height;
        let height = ah.min(bh);
        a = self.ancestor(a, height)?;
        b = self.ancestor(b, height)?;
        if a == b {
            return Ok(a);
        }
        if self.ancestor(a, 0)? != self.ancestor(b, 0)? {
            return Err(Error::Invalid("disconnected roots"));
        }
        let mut low = 0;
        let mut high = height;
        while high - low > 1 {
            let middle = low + (high - low) / 2;
            if self.ancestor(a, middle)? == self.ancestor(b, middle)? {
                low = middle;
            } else {
                high = middle;
            }
        }
        self.ancestor(a, low)
    }
    pub fn compare_cumulative(&self, left: Id, right: Id) -> Result<Option<std::cmp::Ordering>> {
        let a = self.header(&left)?.ok_or(Error::Missing)?;
        let b = self.header(&right)?.ok_or(Error::Missing)?;
        let common = match self.common_ancestor(left, right) {
            Ok(common) => common,
            Err(Error::Invalid("disconnected roots")) => return Ok(None),
            Err(error) => return Err(error),
        };
        let ancestor = self.header(&common)?.ok_or(Error::Missing)?;
        let aw = a
            .cumulative_work
            .checked_sub(ancestor.cumulative_work)
            .ok_or(Error::Invalid("comparison work underflow"))?;
        let bw = b
            .cumulative_work
            .checked_sub(ancestor.cumulative_work)
            .ok_or(Error::Invalid("comparison work underflow"))?;
        Ok(Some(aw.cmp(&bw).then_with(|| right.cmp(&left))))
    }
    pub fn locator(&self, tip: Id, limit: usize) -> Result<Vec<Id>> {
        if limit < 2 {
            return Err(Error::Invalid("locator limit"));
        }
        let mut height = self.header(&tip)?.ok_or(Error::Missing)?.height;
        let mut out = vec![tip];
        let mut step = 1u64;
        while height > 0 && out.len() < limit - 1 {
            height = height.saturating_sub(step);
            out.push(self.ancestor(tip, height)?);
            if out.len() > 10 {
                step = step.saturating_mul(2);
            }
        }
        if height != 0 {
            out.push(self.ancestor(tip, 0)?);
        }
        Ok(out)
    }
}

#[cfg(test)]
#[path = "mod_tests.rs"]
mod tests;
