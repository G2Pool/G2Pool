use crate::{
    codec::*,
    native::{self, View, Work},
    Error, Result,
};
use grin_core::core::hash::Hashed;
use grin_core::core::BlockHeader;
use grin_core::{consensus, pow::Proof};
use grin_keychain::BlindingFactor;

#[derive(Debug, Clone)]
struct Local {
    format: u8,
    output_root: Id,
    range_root: Id,
    kernel_root: Id,
    outputs: u16,
    kernels: u16,
    nonce: u64,
    proof: Blob<1024>,
    job: Blob<2048>,
}
impl Field for Local {
    fn read(r: &mut Reader<'_>) -> Result<Self> {
        Ok(Self {
            format: u8::read(r)?,
            output_root: Id::read(r)?,
            range_root: Id::read(r)?,
            kernel_root: Id::read(r)?,
            outputs: u16::read(r)?,
            kernels: u16::read(r)?,
            nonce: u64::read(r)?,
            proof: Blob::read(r)?,
            job: Blob::read(r)?,
        })
    }
    fn write(&self, out: &mut Vec<u8>) -> Result<()> {
        self.format.write(out)?;
        self.output_root.write(out)?;
        self.range_root.write(out)?;
        self.kernel_root.write(out)?;
        self.outputs.write(out)?;
        self.kernels.write(out)?;
        self.nonce.write(out)?;
        self.proof.write(out)?;
        self.job.write(out)
    }
}
impl Object for Local {
    const DOMAIN: &'static str = "LOCAL_STORAGE_ONLY";
    const MAX: usize = 16384;
}
fn size(leaves: u64) -> Result<u64> {
    let result = 2u128 * leaves as u128 - leaves.count_ones() as u128;
    u64::try_from(result).map_err(|_| Error::Invalid("local MMR size overflow"))
}
pub fn pack(work: &Work, view: &View<'_>) -> Result<Vec<u8>> {
    let parent = view.anchor(&work.job().anchor)?;
    let h = work.header();
    let nout = h
        .output_mmr_count()
        .checked_sub(parent.output_mmr_count())
        .ok_or(Error::Invalid("local outputs"))?;
    let nkern = h
        .kernel_mmr_count()
        .checked_sub(parent.kernel_mmr_count())
        .ok_or(Error::Invalid("local kernels"))?;
    let local = Local {
        format: 1,
        output_root: native::hash_bytes(h.output_root),
        range_root: native::hash_bytes(h.range_proof_root),
        kernel_root: native::hash_bytes(h.kernel_root),
        outputs: nout
            .try_into()
            .map_err(|_| Error::Invalid("local output count"))?,
        kernels: nkern
            .try_into()
            .map_err(|_| Error::Invalid("local kernel count"))?,
        nonce: h.pow.nonce,
        proof: Blob(native::native_bytes(&h.pow.proof)?),
        job: Blob(encode(work.job())?),
    };
    let packed = encode(&local)?;
    let canonical = unpack(&packed, view)?;
    if canonical
        != encode(&Core {
            header: Blob(native::native_bytes(h)?),
            job: local.job,
        })?
    {
        return Err(Error::Invalid("local compaction changed native bytes"));
    }
    Ok(packed)
}
pub fn parent_hint(bytes: &[u8]) -> Result<Parent> {
    let local: Local = decode(bytes)?;
    if local.format != 1 {
        return Err(Error::Invalid("local format"));
    }
    let job: Job = decode(&local.job.0)?;
    Ok(job.parent)
}
pub fn unpack(bytes: &[u8], view: &View<'_>) -> Result<Vec<u8>> {
    let wire = expand(bytes, view)?;
    Work::verify(&wire, view)?;
    Ok(wire)
}
pub(crate) fn expand(bytes: &[u8], view: &View<'_>) -> Result<Vec<u8>> {
    let local: Local = decode(bytes)?;
    if local.format != 1 {
        return Err(Error::Invalid("local format"));
    }
    let job: Job = decode(&local.job.0)?;
    if job.pool_id != view.profile.pool_id {
        return Err(Error::Invalid("local profile"));
    }
    let parent = view.anchor(&job.anchor)?;
    let target = view.target(&job.anchor)?;
    let height = parent
        .height
        .checked_add(1)
        .ok_or(Error::Invalid("local height"))?;
    let proof: Proof = native::parse(&local.proof.0)?;
    let mut header = BlockHeader {
        version: consensus::header_version(height),
        height,
        timestamp: chrono::DateTime::from_timestamp(job.timestamp, 0)
            .ok_or(Error::Invalid("local timestamp"))?,
        prev_hash: parent.hash(),
        output_root: grin_core::core::hash::Hash::from_vec(&local.output_root),
        range_proof_root: grin_core::core::hash::Hash::from_vec(&local.range_root),
        kernel_root: grin_core::core::hash::Hash::from_vec(&local.kernel_root),
        total_kernel_offset: BlindingFactor::from_slice(&scalar(id(&job)?)?),
        output_mmr_size: size(
            parent
                .output_mmr_count()
                .checked_add(local.outputs as u64)
                .ok_or(Error::Invalid("local output addition"))?,
        )?,
        kernel_mmr_size: size(
            parent
                .kernel_mmr_count()
                .checked_add(local.kernels as u64)
                .ok_or(Error::Invalid("local kernel addition"))?,
        )?,
        ..BlockHeader::default()
    };
    header.pow.total_difficulty = grin_core::pow::Difficulty::from_num(
        parent
            .total_difficulty()
            .to_num()
            .checked_add(target.difficulty.to_num())
            .ok_or(Error::Invalid("local total work"))?,
    );
    header.pow.secondary_scaling = target.secondary_scaling;
    header.pow.nonce = local.nonce;
    header.pow.proof = proof;
    view.chain
        .set_prev_root_only(&mut header)
        .map_err(|e| Error::NativeUnavailable(e.to_string()))?;
    let wire = encode(&Core {
        header: Blob(native::native_bytes(&header)?),
        job: local.job,
    })?;
    Ok(wire)
}
