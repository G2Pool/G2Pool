use crate::codec::{hash, Id};
use crate::{storage::sharechain, Error, Result};

pub struct Coordinator {
    db: sharechain::SharechainDb,
}
pub struct VerifiedShare<'a> {
    pub id: Id,
    pub parent: Option<Id>,
    pub height: u64,
    pub anchor: Id,
    pub work: u128,
    pub state: &'a [u8],
    pub body: &'a [u8],
    pub delta: &'a [u8],
    pub state_root: Id,
}

fn evaluation_payout(evaluation: &super::Evaluation) -> Result<Vec<u8>> {
    let record = &evaluation.financial.record;
    if record.anchor != evaluation.anchor
        || record.parent != crate::codec::Parent::Receipt(evaluation.tip)
    {
        return Err(Error::Invalid("sharechain evaluation context"));
    }
    crate::codec::encode(record)
}

impl Coordinator {
    pub fn from_db(db: sharechain::SharechainDb) -> Result<Self> {
        db.read()
            .map_err(|_| Error::Invalid("sharechain snapshot"))?
            .publication()
            .map_err(|_| Error::Invalid("sharechain publication"))?;
        Ok(Self { db })
    }
    pub fn db(&self) -> &sharechain::SharechainDb {
        &self.db
    }
    pub fn indexed(&self, id: Id) -> Result<bool> {
        let snapshot = self
            .db
            .read()
            .map_err(|_| Error::Invalid("sharechain snapshot"))?;
        snapshot
            .header(&id)
            .map(|header| header.is_some())
            .map_err(|_| Error::Invalid("sharechain header"))
    }
    pub(crate) fn store_evaluation_closure(
        &self,
        evaluation: &super::Evaluation,
        closure: &crate::consensus::VerifiedClosure,
    ) -> Result<()> {
        let payout = evaluation_payout(evaluation)?;
        let encoded = closure.dependency_objects();
        let objects = encoded.objects();
        let manifest = closure.manifest();
        let batch = sharechain::DependencyBatch {
            objects: &objects,
            manifests: std::slice::from_ref(&manifest),
            ..Default::default()
        };
        self.db
            .store_evaluation_with_dependencies(
                evaluation.tip,
                evaluation.anchor.height,
                evaluation.anchor.hash,
                &payout,
                closure.id(),
                &batch,
            )
            .map(|_| ())
            .map_err(crate::storage::runtime_error)
    }

    pub(crate) fn store_verified_batch_with_evaluation(
        &mut self,
        shares: &[VerifiedShare<'_>],
        evaluation: &super::Evaluation,
        closure: &crate::consensus::VerifiedClosure,
    ) -> Result<sharechain::Publication> {
        let payout = evaluation_payout(evaluation)?;
        if shares.is_empty() {
            return Err(Error::Invalid("empty sharechain publication"));
        }
        let generation = self
            .db
            .read()
            .map_err(|_| Error::Invalid("sharechain snapshot"))?
            .publication()
            .map_err(|_| Error::Invalid("sharechain publication"))?
            .map_or(0, |publication| publication.generation);
        let mut records = Vec::with_capacity(shares.len());
        let mut expected_parent = None;
        let mut previous_state = [0; 32];
        let mut previous_header: Option<sharechain::Header> = None;
        for share in shares {
            if share.work == 0 || share.body.is_empty() {
                return Err(Error::Invalid("sharechain verified record"));
            }
            if share.parent != expected_parent && expected_parent.is_some() {
                return Err(Error::Invalid("sharechain batch parent"));
            }
            let (height, cumulative_work, parent_state_root) = if let Some(parent_id) = share.parent
            {
                let parent = if expected_parent == Some(parent_id) {
                    previous_header.clone().ok_or(Error::Missing(parent_id))?
                } else {
                    self.db
                        .read()
                        .map_err(|_| Error::Invalid("sharechain snapshot"))?
                        .header(&parent_id)
                        .map_err(|_| Error::Invalid("sharechain parent"))?
                        .ok_or(Error::Missing(parent_id))?
                };
                (
                    parent
                        .height
                        .checked_add(1)
                        .ok_or(Error::Invalid("sharechain height overflow"))?,
                    parent
                        .cumulative_work
                        .checked_add(share.work)
                        .ok_or(Error::Invalid("sharechain cumulative work overflow"))?,
                    parent.state_root,
                )
            } else {
                if share.height != 0 {
                    return Err(Error::Invalid("sharechain root height"));
                }
                (0, share.work, [0; 32])
            };
            if share.height != 0 && share.height != height {
                return Err(Error::Invalid("sharechain height"));
            }
            if share.parent.is_some() && !records.is_empty() && parent_state_root != previous_state
            {
                return Err(Error::Invalid("sharechain batch state continuity"));
            }
            let state_root = if share.state_root != [0; 32] {
                if share.state.is_empty()
                    || share.state_root != crate::codec::hash("SHARE_STATE", share.state)
                {
                    return Err(Error::Invalid("share state root"));
                }
                share.state_root
            } else if share.state.is_empty() {
                [0; 32]
            } else {
                hash("SHARE_STATE", share.state)
            };
            records.push(sharechain::Record {
                id: share.id,
                header: sharechain::Header {
                    parent: share.parent,
                    height,
                    work: share.work,
                    cumulative_work,
                    anchor: share.anchor,
                    parent_state_root,
                    state_root,
                },
                body: share.body,
                delta: share.delta,
                state: share.state,
            });
            expected_parent = Some(share.id);
            previous_state = state_root;
            previous_header = Some(records.last().unwrap().header.clone());
        }
        let encoded = closure.dependency_objects();
        let objects = encoded.objects();
        let manifest = closure.manifest();
        let batch = sharechain::DependencyBatch {
            objects: &objects,
            manifests: std::slice::from_ref(&manifest),
            ..Default::default()
        };
        self.db
            .insert_validated_with_evaluation(
                generation,
                &records,
                evaluation.tip,
                closure.id(),
                &batch,
                sharechain::EvaluationPublication {
                    tip: evaluation.tip,
                    grin_height: evaluation.anchor.height,
                    grin_hash: evaluation.anchor.hash,
                    payout: &payout,
                    manifest: closure.id(),
                },
            )
            .map_err(crate::storage::runtime_error)
    }
}
