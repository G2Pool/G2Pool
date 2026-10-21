mod profile;
pub(crate) use profile::MAX_ANCHOR_TIMESTAMP_DEVIATION;
pub use profile::{
    default_limits, manifest, payout_precision, Profile, NATIVE_CODEC, NATIVE_VERSION,
};

use crate::{codec::*, Error, Result};
use grin_chain::{pipe, txhashset, Chain};
use grin_core::core::committed::Committed;
use grin_core::core::hash::{Hash, Hashed};
use grin_core::core::pmmr::n_leaves;
use grin_core::core::{Block, BlockHeader, Output, TransactionBody, TxKernel};
use grin_core::ser::{DeserializationMode, ProtocolVersion, Readable, Writeable};
use grin_core::{consensus, global, pow, ser};
use grin_util::{secp, static_secp_instance};
use std::{io::Cursor, sync::Arc};

pub fn hash_bytes(hash: Hash) -> Id {
    hash.as_bytes().try_into().unwrap()
}
pub fn native_bytes<T: Writeable>(value: &T) -> Result<Vec<u8>> {
    ser::ser_vec(value, ProtocolVersion(profile::NATIVE_CODEC))
        .map_err(|_| Error::Invalid("native encoding"))
}
pub fn parse<T: Readable + Writeable>(bytes: &[u8]) -> Result<T> {
    let mut cursor = Cursor::new(bytes);
    let value: T = ser::deserialize(
        &mut cursor,
        ProtocolVersion(profile::NATIVE_CODEC),
        DeserializationMode::Full,
    )
    .map_err(|_| Error::Invalid("native decoding"))?;
    if cursor.position() != bytes.len() as u64 || native_bytes(&value)? != bytes {
        return Err(Error::Invalid("noncanonical native bytes"));
    }
    Ok(value)
}
pub fn public_key(bytes: Point) -> Result<secp::key::PublicKey> {
    let handle = static_secp_instance();
    let secp = handle.lock();
    let key = secp::key::PublicKey::from_slice(&secp, &bytes)
        .map_err(|_| Error::Invalid("account key"))?;
    if key.serialize_vec(&secp, true).as_slice() != bytes {
        return Err(Error::Invalid("account encoding"));
    }
    Ok(key)
}
pub fn commitment(bytes: Point) -> Result<secp::pedersen::Commitment> {
    let c = secp::pedersen::Commitment(bytes);
    c.to_pubkey(&static_secp_instance().lock())
        .map_err(|_| Error::Invalid("commitment encoding"))?;
    Ok(c)
}
pub fn signature_valid(sig: Sig, digest: Id, public: &secp::key::PublicKey) -> bool {
    let handle = static_secp_instance();
    let secp = handle.lock();
    let signature = match secp::Signature::from_raw_data(&sig) {
        Ok(s) => s,
        Err(_) => return false,
    };
    let message = secp::Message::from_slice(&digest).unwrap();
    grin_core::libtx::aggsig::verify_single(
        &secp,
        &signature,
        &message,
        None,
        public,
        Some(public),
        false,
    )
}
pub struct View<'a> {
    pub chain: &'a Chain,
    pub profile: Profile,
    pub head: Anchor,
    pub now: i64,
}
impl<'a> View<'a> {
    pub fn pin(chain: &'a Chain, profile: Profile, now: i64) -> Result<Self> {
        if hash_bytes(chain.genesis().hash()) != profile.manifest.genesis {
            return Err(Error::View);
        }
        let head = chain.head_header().map_err(unavailable)?;
        Ok(Self {
            chain,
            profile,
            head: Anchor {
                height: head.height,
                hash: hash_bytes(head.hash()),
            },
            now,
        })
    }
    pub fn check(&self) -> Result<()> {
        let h = self.chain.head_header().map_err(unavailable)?;
        if h.height != self.head.height || hash_bytes(h.hash()) != self.head.hash {
            return Err(Error::View);
        }
        Ok(())
    }
    pub fn anchor(&self, a: &Anchor) -> Result<BlockHeader> {
        if a.height > self.head.height {
            return Err(Error::View);
        }
        let h = self
            .chain
            .get_header_by_height(a.height)
            .map_err(unavailable)?;
        if hash_bytes(h.hash()) != a.hash {
            return Err(Error::View);
        }
        Ok(h)
    }
    fn history(&self, anchor: &Anchor) -> Result<()> {
        if self
            .chain
            .tail()
            .is_ok_and(|tail| anchor.height < tail.height.saturating_sub(1))
            || self.head.height.saturating_sub(anchor.height) > global::cut_through_horizon() as u64
        {
            return Err(Error::NativeUnavailable("anchor is outside retained native rewind history; recover native history before pool admission".into()));
        }
        Ok(())
    }
    pub fn target(&self, a: &Anchor) -> Result<consensus::HeaderDifficultyInfo> {
        let h = self.anchor(a)?;
        Ok(consensus::next_difficulty(
            h.height
                .checked_add(1)
                .ok_or(Error::Invalid("height overflow"))?,
            grin_chain::store::DifficultyIter::from(h.hash(), self.chain.store()),
        ))
    }
    pub fn unspent(&self, a: &Anchor, c: secp::pedersen::Commitment) -> Result<bool> {
        Ok(self.unspent_many(a, &[c])?[0])
    }
    pub fn unspent_many(
        &self,
        a: &Anchor,
        commitments: &[secp::pedersen::Commitment],
    ) -> Result<Vec<bool>> {
        self.check()?;
        let parent = self.anchor(a)?;
        self.history(a)?;
        let hh = self.chain.header_pmmr();
        let th = self.chain.txhashset();
        let result = {
            let mut headers = hh.write();
            let mut tree = th.write();
            txhashset::extending_readonly(&mut headers, &mut tree, |ext, batch| {
                pipe::rewind_and_apply_fork(&parent, ext, batch, &|_| Ok(()))?;
                commitments
                    .iter()
                    .map(|c| match batch.get_output_pos_height(c)? {
                        None => Ok(false),
                        Some(pos) => {
                            let output = ext
                                .extension
                                .utxo_view(ext.header_extension)
                                .get_unspent_output_at(pos.pos - 1)?;
                            if output.commitment() != *c {
                                return Err(grin_chain::Error::Other(
                                    "historical index mismatch".into(),
                                ));
                            }
                            Ok(true)
                        }
                    })
                    .collect::<std::result::Result<Vec<_>, grin_chain::Error>>()
            })
            .map_err(unavailable)?
        };
        self.check()?;
        Ok(result)
    }
    pub fn candidate(&self, block: &Block) -> Result<()> {
        let anchor = Anchor {
            height: block
                .header
                .height
                .checked_sub(1)
                .ok_or(Error::Invalid("candidate height"))?,
            hash: hash_bytes(block.header.prev_hash),
        };
        let parent = self.anchor(&anchor)?;
        self.history(&anchor)?;
        block
            .validate(&parent.total_kernel_offset)
            .map_err(|_| Error::Invalid("native body"))?;
        let hh = self.chain.header_pmmr();
        let th = self.chain.txhashset();
        let mut headers = hh.write();
        let mut tree = th.write();
        let mut rewound = false;
        let result = txhashset::extending_readonly(&mut headers, &mut tree, |ext, batch| {
            pipe::rewind_and_apply_fork(&parent, ext, batch, &|_| Ok(()))?;
            rewound = true;
            let view = ext.extension.utxo_view(ext.header_extension);
            view.verify_coinbase_maturity(&block.inputs(), block.header.height, batch)?;
            view.validate_block(block, batch)?;
            let sums = batch.get_block_sums(&block.header.prev_hash)?;
            let body: &dyn Committed = block;
            (sums, body)
                .verify_kernel_sums(block.header.overage(), block.header.total_kernel_offset())?;
            ext.extension
                .apply_block(block, ext.header_extension, batch)?;
            ext.extension.validate_roots(&block.header)?;
            ext.extension.validate_sizes(&block.header)?;
            Ok(())
        });
        match result {
            Ok(()) => self.check(),
            Err(e) if !rewound => Err(unavailable(e)),
            Err(grin_chain::Error::StoreErr(e, _)) => Err(Error::NativeUnavailable(e.to_string())),
            Err(_) => Err(Error::Invalid("native context or roots")),
        }
    }
}
fn unavailable(e: grin_chain::Error) -> Error {
    Error::NativeUnavailable(e.to_string())
}
fn leaves(size: u64) -> Result<u64> {
    let n = n_leaves(size);
    let canonical = 2u128 * n as u128 - n.count_ones() as u128;
    if canonical != size as u128 {
        return Err(Error::Invalid("noncanonical PMMR size"));
    }
    Ok(n)
}
#[derive(Debug, Clone)]
pub struct Work {
    pub(crate) id: Id,
    pub(crate) job: Job,
    pub(crate) header: BlockHeader,
}

pub(crate) fn authenticated_history<F>(
    view: &View<'_>,
    parent: &Parent,
    mut load: F,
) -> Result<Vec<(u64, u128)>>
where
    F: FnMut(Id) -> Result<Arc<Work>>,
{
    let mut cursor = match parent {
        Parent::Root(anchor) => {
            view.anchor(anchor)?;
            None
        }
        Parent::Receipt(id) => Some(*id),
    };
    let depth = usize::from(view.profile.manifest.uncle_depth);
    let history_limit = view.profile.work_window_limit();
    let mut spine = Vec::new();
    let mut primary = std::collections::BTreeSet::new();
    for _ in 0..history_limit + depth {
        let Some(id) = cursor else { break };
        if !primary.insert(id) {
            return Err(Error::Invalid("retarget ancestry cycle"));
        }
        let work = load(id)?;
        if spine
            .last()
            .is_some_and(|child: &Arc<Work>| work.job.anchor.height > child.job.anchor.height)
        {
            return Err(Error::Invalid("retarget anchor regression"));
        }
        cursor = match work.job.parent {
            Parent::Root(_) => None,
            Parent::Receipt(id) => Some(id),
        };
        spine.push(work);
    }
    let mut credited = primary;
    let mut loaded = Vec::new();
    for (index, work) in spine.iter().take(history_limit).enumerate() {
        let job = work.job();
        if job.uncles.0.len() > usize::from(view.profile.manifest.max_uncles) {
            return Err(Error::Invalid("too many uncles"));
        }
        let ancestors = &spine[index + 1..(index + 1 + depth).min(spine.len())];
        let mut uncles = Vec::with_capacity(job.uncles.0.len());
        for uncle in job.uncles.0.iter().copied() {
            if !credited.insert(uncle) {
                return Err(Error::Invalid("retarget duplicate uncle"));
            }
            let uw = load(uncle)?;
            let uj = uw.job();
            if !ancestors.iter().any(|ancestor| {
                ancestor.job.anchor == uj.anchor && ancestor.job.parent == uj.parent
            }) || ancestors
                .iter()
                .any(|ancestor| ancestor.job.uncles.0.contains(&uncle))
            {
                return Err(Error::Invalid("retarget uncle relation"));
            }
            uncles.push(uw);
        }
        loaded.push((work.clone(), uncles));
    }
    let mut cumulative = std::collections::BTreeMap::<Id, u128>::new();
    let mut samples = Vec::new();
    for (work, uncles) in loaded.iter().rev() {
        let parent_work = match work.job.parent {
            Parent::Root(_) => 0,
            Parent::Receipt(id) => cumulative.get(&id).copied().unwrap_or(0),
        };
        let uncle_work = uncles.iter().try_fold(0u128, |sum, uncle| {
            sum.checked_add(u128::from(uncle.job.difficulty))
                .ok_or(Error::Invalid("retarget work overflow"))
        })?;
        let primary_work = parent_work
            .checked_add(u128::from(work.job.difficulty))
            .and_then(|value| value.checked_add(uncle_work))
            .ok_or(Error::Invalid("retarget work overflow"))?;
        cumulative.insert(work.id, primary_work);
        samples.push((
            u64::try_from(work.job.timestamp).map_err(|_| Error::Invalid("timestamp"))?,
            primary_work,
        ));
        for uncle in uncles {
            let uncle_parent = match uncle.job.parent {
                Parent::Root(_) => 0,
                Parent::Receipt(id) => cumulative.get(&id).copied().unwrap_or(0),
            };
            let uncle_cumulative = uncle_parent
                .checked_add(u128::from(uncle.job.difficulty))
                .ok_or(Error::Invalid("retarget work overflow"))?;
            samples.push((
                u64::try_from(uncle.job.timestamp).map_err(|_| Error::Invalid("timestamp"))?,
                uncle_cumulative,
            ));
        }
    }
    Ok(samples)
}
impl Work {
    pub(crate) fn retained_bytes(&self) -> usize {
        std::mem::size_of::<Self>()
            + 192
            + self.job.uncles.0.capacity() * std::mem::size_of::<Id>()
            + self.header.pow.proof.nonces.capacity() * std::mem::size_of::<u64>()
    }
    pub fn id(&self) -> Id {
        self.id
    }
    pub fn job(&self) -> &Job {
        &self.job
    }
    pub fn header(&self) -> &BlockHeader {
        &self.header
    }
    pub fn authorize(&self, auth: &WorkAuthorization, protocol: Id) -> Result<()> {
        if self.job.protocol != protocol
            || auth.fields.protocol != protocol
            || auth.fields.work_id != self.id
        {
            return Err(Error::Witness("work authorization relation"));
        }
        let public = public_key(self.job.account)?;
        if !signature_valid(auth.signature, id(&auth.fields)?, &public) {
            return Err(Error::Witness("work authorization signature"));
        }
        Ok(())
    }
    pub fn verify(bytes: &[u8], view: &View<'_>) -> Result<Self> {
        let core: Core = decode(bytes)?;
        let job: Job = decode(&core.job.0)?;
        let header: BlockHeader = parse(&core.header.0)?;
        if job.protocol != view.profile.protocol {
            return Err(Error::Invalid("protocol"));
        }
        if job.timestamp < 0 {
            return Err(Error::Invalid("timestamp"));
        }
        public_key(job.account)?;
        commitment(job.finder_commitment)?;
        if job.finder_value == 0 {
            return Err(Error::Invalid("finder zero"));
        }
        let parent = view.anchor(&job.anchor)?;
        let target = view.target(&job.anchor)?;
        let d = job.difficulty;
        if d < consensus::graph_weight(header.height, view.profile.manifest.edge_bits) {
            return Err(Error::Invalid("assigned graph weight"));
        }
        if header.prev_hash != parent.hash()
            || header.height
                != parent
                    .height
                    .checked_add(1)
                    .ok_or(Error::Invalid("height overflow"))?
        {
            return Err(Error::Invalid("candidate anchor"));
        }
        if !consensus::valid_header_version(header.height, header.version) {
            return Err(Error::Invalid("header version"));
        }
        let total = parent
            .pow
            .total_difficulty
            .to_num()
            .checked_add(target.difficulty.to_num())
            .ok_or(Error::Invalid("total difficulty overflow"))?;
        if header.pow.total_difficulty.to_num() != total
            || header.pow.secondary_scaling != target.secondary_scaling
        {
            return Err(Error::Invalid("native target/scaling"));
        }
        if header.timestamp.timestamp() != job.timestamp || header.timestamp <= parent.timestamp {
            return Err(Error::Invalid("timestamp"));
        }
        if job.timestamp
            > view
                .now
                .checked_add(i64::from(view.profile.manifest.future_seconds))
                .ok_or(Error::Invalid("clock overflow"))?
        {
            return Err(Error::Future);
        }
        if header.pow.edge_bits() != view.profile.manifest.edge_bits
            || header.pow.proof.proof_size() != view.profile.manifest.proof_size as usize
        {
            return Err(Error::Invalid("proof profile"));
        }
        if job.uncles.0.windows(2).any(|pair| pair[0] >= pair[1]) {
            return Err(Error::Invalid("uncle ordering"));
        }
        if let Parent::Root(a) = &job.parent {
            if a != &job.anchor || !job.uncles.0.is_empty() {
                return Err(Error::Invalid("root parent"));
            }
        }
        if header.total_kernel_offset.as_ref() != scalar(job.protocol, id(&job)?)? {
            return Err(Error::Invalid("job offset binding"));
        }
        let outputs = leaves(header.output_mmr_size)?
            .checked_sub(leaves(parent.output_mmr_size)?)
            .ok_or(Error::Invalid("output MMR regression"))?;
        let kernels = leaves(header.kernel_mmr_size)?
            .checked_sub(leaves(parent.kernel_mmr_size)?)
            .ok_or(Error::Invalid("kernel MMR regression"))?;
        if outputs == 0
            || kernels < 2
            || TransactionBody::weight_by_iok(0, outputs, kernels) > global::max_block_weight()
        {
            return Err(Error::Invalid("native count weight"));
        }
        let mut expected = header.clone();
        view.chain
            .set_prev_root_only(&mut expected)
            .map_err(unavailable)?;
        if header.prev_root != expected.prev_root {
            return Err(Error::Invalid("header PMMR root"));
        }
        pow::verify_size(&header).map_err(|_| Error::Invalid("native cycle"))?;
        if header.pow.to_difficulty(header.height).to_num() < d {
            return Err(Error::Invalid("proof difficulty"));
        }
        let prefix = header.pre_pow();
        let nonce_free = &prefix[..prefix.len() - 8];
        let payload = [
            nonce_free,
            &header.pow.nonce.to_be_bytes(),
            &[header.pow.edge_bits()],
            &header.pow.proof.pack_nonces(),
        ]
        .concat();
        if payload != core.header.0 {
            return Err(Error::Invalid("WorkID payload alias"));
        }
        let work = Self {
            id: hash("WORK_ID", &payload),
            job,
            header,
        };
        view.check()?;
        Ok(work)
    }
}
pub fn reward(c: &Coupon) -> Result<(Output, TxKernel)> {
    let output: Output = parse(&c.reward.output.0)?;
    let kernel: TxKernel = parse(&c.reward.kernel.0)?;
    commitment(output.commitment().0)?;
    commitment(kernel.excess.0)?;
    Ok((output, kernel))
}
