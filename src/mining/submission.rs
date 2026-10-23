use crate::{
    codec::{self, Id},
    mining::{Submitted, Template},
    native,
    node::Node,
    objects::{Key, Kind, Source},
    storage::{
        sharechain::{
            physical_digest, ObjectMode, RoutingKey, RuntimeBatch, RuntimeObject, RuntimeStore,
            SharechainDb,
        },
        Object, Store,
    },
    wallet::Signer,
    Error, Result,
};
use grin_core::{core::Block, pow::Proof};
use std::sync::{Arc, Mutex};
const PREFIX: &[u8] = b"aux/native/submission/";
const MAX_RECORD: usize = 64 * 1024 * 1024;
const MAX_BYTES: u64 = crate::storage::sharechain::WRITE_RESERVE;
const MAX_RECORDS: usize = crate::storage::sharechain::SOLUTION_RECORDS;
#[derive(Clone)]
pub struct Solutions {
    db: SharechainDb,
    runtime: RuntimeStore,
}
fn key(id: Id) -> Vec<u8> {
    let mut key = PREFIX.to_vec();
    key.extend(id);
    key
}
pub(crate) fn submit_native(
    chain: &grin_chain::Chain,
    block: Block,
) -> std::result::Result<bool, grin_chain::Error> {
    submission_result(
        chain
            .process_block(block, grin_chain::Options::MINE)
            .map(|_| ()),
    )
}

fn submission_result(
    result: std::result::Result<(), grin_chain::Error>,
) -> std::result::Result<bool, grin_chain::Error> {
    use grin_chain::Error as E;
    match result {
        Ok(()) | Err(E::Unfit(_) | E::OldBlock) => Ok(true),
        Err(
            E::DifficultyTooLow
            | E::WrongTotalDifficulty
            | E::LowEdgebits
            | E::InvalidScaling
            | E::InvalidPow
            | E::InvalidBlockProof { .. }
            | E::InvalidBlockTime
            | E::InvalidBlockHeight
            | E::InvalidRoot
            | E::InvalidMMRSize
            | E::AlreadySpent(_)
            | E::DuplicateCommitment(_)
            | E::ImmatureCoinbase
            | E::TxLockHeight
            | E::NRDRelativeHeight
            | E::InvalidBlockVersion(_)
            | E::Transaction { .. }
            | E::Block(_)
            | E::Committed { .. },
        ) => Ok(false),
        Err(error) => Err(error),
    }
}
impl Solutions {
    pub(crate) fn new(store: &Store) -> Self {
        Self {
            db: store.database(),
            runtime: store.runtime(),
        }
    }
    fn save(&self, template: &Template, wallet: &Signer, nonce: u64, proof: Proof) -> Result<Id> {
        let mut block = template.block.clone();
        block.header.pow.nonce = nonce;
        block.header.pow.proof = proof;
        if wallet.account() != template.job.account {
            return Err(Error::Invalid("solution signer context"));
        }
        let id = codec::hash("WORK_ID", &native::native_bytes(&block.header)?);
        let bytes = native::native_bytes(&block)?;
        if bytes.len() > MAX_RECORD {
            return Err(Error::Capacity("solution intent"));
        }
        let objects = material(template, wallet, &block)?;
        let keys: Vec<_> = template
            .pins()
            .into_iter()
            .chain(objects.iter().map(|object| object.key))
            .collect::<std::collections::BTreeSet<_>>()
            .into_iter()
            .map(|key| RoutingKey {
                kind: key.kind as u8,
                id: key.id,
            })
            .collect();
        let staged: Vec<_> = objects
            .iter()
            .map(|object| RuntimeObject {
                key: RoutingKey {
                    kind: object.key.kind as u8,
                    id: object.key.id,
                },
                mode: if matches!(object.key.kind, Kind::Authorization | Kind::Residual) {
                    ObjectMode::WorkVariant
                } else {
                    ObjectMode::Immutable
                },
                logical_digest: physical_digest(&object.bytes),
                representation: crate::storage::sharechain::Representation::Canonical,
                bytes: &object.bytes,
            })
            .collect();
        let seals: Vec<_> = staged
            .iter()
            .map(|object| (object.key, physical_digest(object.bytes)))
            .collect();
        self.db.solution_transaction(|tx, db| {
            let key = key(id);
            if let Some(previous) = db.get(tx, &key)? {
                if previous != bytes || !self.runtime.retained_in(tx, retention(id))? {
                    return Err(Error::Invalid(
                        "solution intent changed or retention missing",
                    ));
                }
                return Ok(id);
            }
            let mut count = 0;
            let mut used = 0u64;
            for row in db.prefix_iter(tx, PREFIX)? {
                let (_, value) = row?;
                count += 1;
                used = used.saturating_add(value.len() as u64);
            }
            if count >= MAX_RECORDS || used.saturating_add(bytes.len() as u64) > MAX_BYTES {
                return Err(Error::Capacity("solution journal"));
            }
            self.runtime.apply_in(
                tx,
                chrono::Utc::now().timestamp().max(0) as u64,
                RuntimeBatch {
                    objects: &staged,
                    seals: &seals,
                    ..Default::default()
                },
                true,
            )?;
            self.runtime.retain_in(tx, retention(id), &keys)?;
            db.put(tx, &key, &bytes)?;
            Ok(id)
        })
    }
    fn done(&self, id: Id) -> Result<()> {
        self.db.solution_transaction(|tx, db| {
            if db.delete(tx, &key(id))? {
                self.runtime.release_in(tx, retention(id))?;
            }
            Ok(())
        })
    }
    fn pending(&self) -> Result<Vec<Id>> {
        let (keys, used) = self
            .db
            .auxiliary_keys(PREFIX, MAX_RECORDS, MAX_RECORD)
            .map_err(crate::storage::runtime_error)?;
        if used > MAX_BYTES {
            return Err(Error::Capacity("solution journal"));
        }
        keys.into_iter()
            .map(|key| {
                key[PREFIX.len()..]
                    .try_into()
                    .map_err(|_| Error::Invalid("solution key"))
            })
            .collect()
    }
    pub(crate) fn submit(
        &self,
        node: &Arc<Mutex<Node>>,
        template: &Template,
        wallet: &Signer,
        nonce: u64,
        proof: Proof,
    ) -> Result<Submitted> {
        let mut block = template.block.clone();
        block.header.pow.nonce = nonce;
        block.header.pow.proof = proof.clone();
        grin_core::pow::verify_size(&block.header)
            .map_err(|_| Error::Invalid("submitted cycle"))?;
        let difficulty = block.header.pow.to_difficulty(block.header.height).to_num();
        if difficulty < template.job.difficulty {
            return Err(Error::Invalid("submitted difficulty"));
        }
        let chain = node
            .lock()
            .map_err(|_| Error::Invalid("node poisoned"))?
            .chain
            .clone();
        let parent = chain
            .get_block_header(&block.header.prev_hash)
            .map_err(|e| Error::NativeUnavailable(e.to_string()))?;
        let target = block
            .header
            .total_difficulty()
            .to_num()
            .checked_sub(parent.total_difficulty().to_num())
            .ok_or(Error::Invalid("native target"))?;
        let id = codec::hash("WORK_ID", &native::native_bytes(&block.header)?);
        let winner = difficulty >= target;
        if winner {
            self.save(template, wallet, nonce, proof.clone())?;
        }
        let native_accepted = if winner {
            let accepted = submit_native(&chain, block.clone())
                .map_err(|error| Error::NativeUnavailable(error.to_string()))?;
            if !accepted {
                self.done(id)?;
                return Ok(Submitted {
                    work_id: id,
                    native_accepted: false,
                    pool_accepted: false,
                });
            }
            true
        } else {
            false
        };
        let objects = if winner {
            self.objects(node, id)?
        } else {
            material(template, wallet, &block)?
        };
        let result = account(node, id, objects);
        match result {
            Ok(()) => {
                if winner {
                    self.done(id)?;
                }
                Ok(Submitted {
                    work_id: id,
                    native_accepted,
                    pool_accepted: true,
                })
            }
            Err(Error::Expired) if native_accepted => {
                self.done(id)?;
                Ok(Submitted {
                    work_id: id,
                    native_accepted,
                    pool_accepted: false,
                })
            }
            Err(error) if native_accepted => {
                eprintln!("native solution accepted; pool accounting pending: {error}");
                Ok(Submitted {
                    work_id: id,
                    native_accepted,
                    pool_accepted: false,
                })
            }
            Err(error) => Err(error),
        }
    }
    pub(crate) fn replay(&self, node: &Arc<Mutex<Node>>) -> Result<()> {
        let chain = node
            .lock()
            .map_err(|_| Error::Invalid("node poisoned"))?
            .chain
            .clone();
        let mut pending_error = None;
        for id in self.pending()? {
            let Some(bytes) = self
                .db
                .auxiliary_read(&key(id))
                .map_err(crate::storage::runtime_error)?
            else {
                continue;
            };
            let block: Block = native::parse(&bytes)?;
            if codec::hash("WORK_ID", &native::native_bytes(&block.header)?) != id {
                return Err(Error::Invalid("solution identity"));
            }
            let parent_height = block.header.height.saturating_sub(1);
            let parent_hash = native::hash_bytes(block.header.prev_hash);
            match submit_native(&chain, block) {
                Ok(false) => {
                    self.done(id)?;
                    continue;
                }
                Err(error) => {
                    if matches!(error, grin_chain::Error::Orphan)
                        && parent_height
                            < chain
                                .tail()
                                .map_err(|error| Error::NativeUnavailable(error.to_string()))?
                                .height
                    {
                        self.done(id)?;
                        continue;
                    }
                    pending_error.get_or_insert(Error::NativeUnavailable(error.to_string()));
                    continue;
                }
                Ok(true) => {}
            }
            let objects = self.objects(node, id);
            match objects.and_then(|objects| account(node, id, objects)) {
                Ok(()) | Err(Error::Expired) => self.done(id)?,
                Err(Error::Cancelled) => return Err(Error::Cancelled),
                Err(Error::View) => {
                    let guard = node.lock().map_err(|_| Error::Invalid("node poisoned"))?;
                    let view = guard.view()?;
                    if parent_height <= view.head.height {
                        let anchor = chain
                            .get_header_by_height(parent_height)
                            .map_err(|error| Error::NativeUnavailable(error.to_string()))?;
                        if native::hash_bytes(grin_core::core::hash::Hashed::hash(&anchor))
                            != parent_hash
                        {
                            self.done(id)?;
                            continue;
                        }
                    }
                    pending_error.get_or_insert(Error::View);
                }
                Err(error) => {
                    pending_error.get_or_insert(error);
                }
            }
        }
        pending_error.map_or(Ok(()), Err)
    }
    fn objects(&self, node: &Arc<Mutex<Node>>, id: Id) -> Result<Vec<Object>> {
        let source = node
            .lock()
            .map_err(|_| Error::Invalid("node poisoned"))?
            .store
            .read_source()?
            .open()?;
        let core = Key::new(Kind::Core, id);
        let bytes = source.read(core)?;
        let decoded: codec::Core = codec::decode(&bytes)?;
        let job: codec::Job = codec::decode(&decoded.job.0)?;
        let mut objects = vec![Object::new(core, bytes)?];
        for key in [
            Key::new(Kind::Authorization, id),
            Key::new(Kind::Residual, id),
            Key::new(Kind::Encoding, job.kernel_encoding_plan),
            Key::new(Kind::Transaction, job.transaction_only),
            Key::new(Kind::Finder, job.finder_output),
        ] {
            objects.push(Object::new(key, source.read(key)?)?);
        }
        Ok(objects)
    }
}
fn retention(id: Id) -> Id {
    codec::hash("SOLUTION_RETENTION", &id)
}
fn material(template: &Template, wallet: &Signer, block: &Block) -> Result<Vec<Object>> {
    let header = native::native_bytes(&block.header)?;
    let id = codec::hash("WORK_ID", &header);
    let core = codec::Core {
        header: codec::Blob(header),
        job: codec::Blob(codec::encode(&template.job)?),
    };
    let mut objects = template.material.clone();
    objects.extend([
        Object::new(Key::new(Kind::Core, id), codec::encode(&core)?)?,
        Object::new(
            Key::new(Kind::Authorization, id),
            codec::encode(&wallet.authorize(id)?)?,
        )?,
        Object::new(
            Key::new(Kind::Residual, id),
            codec::encode(&template.residual)?,
        )?,
    ]);
    Ok(objects)
}

fn account(node: &Arc<Mutex<Node>>, id: Id, objects: Vec<Object>) -> Result<()> {
    let stage = {
        let mut guard = node.lock().map_err(|_| Error::Invalid("node poisoned"))?;
        guard.ensure_view()?;
        guard.prepare_stage(objects)?
    };
    let checked = stage.run()?;
    node.lock()
        .map_err(|_| Error::Invalid("node poisoned"))?
        .finish_stage(checked)?;
    loop {
        let turn = {
            let mut guard = node.lock().map_err(|_| Error::Invalid("node poisoned"))?;
            guard.ensure_view()?;
            guard
                .take_turn(id)?
                .ok_or(Error::Capacity("candidate verification in progress"))?
        };
        let (turn, outcome) = turn.run();
        match node
            .lock()
            .map_err(|_| Error::Invalid("node poisoned"))?
            .finish_turn(turn, outcome)?
        {
            crate::consensus::EvaluationProgress::Ready(_) => return Ok(()),
            crate::consensus::EvaluationProgress::Pending => std::thread::yield_now(),
            crate::consensus::EvaluationProgress::Missing(key) => {
                return Err(Error::Unavailable(key))
            }
        }
    }
}

#[cfg(test)]
#[path = "submission_tests.rs"]
pub(crate) mod tests;
