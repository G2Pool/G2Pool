use super::{Error, Id, Result};

pub(crate) const ANCESTOR_STRIDE: u64 = 64;
pub(crate) const GC_RECORDS: usize = 256;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Header {
    pub parent: Option<Id>,
    pub height: u64,
    pub work: u128,
    pub cumulative_work: u128,
    pub anchor: Id,
    pub parent_state_root: Id,
    pub state_root: Id,
}
impl Header {
    pub(crate) fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(169);
        out.push(u8::from(self.parent.is_some()));
        out.extend_from_slice(&self.parent.unwrap_or([0; 32]));
        out.extend_from_slice(&self.height.to_be_bytes());
        out.extend_from_slice(&self.work.to_be_bytes());
        out.extend_from_slice(&self.cumulative_work.to_be_bytes());
        out.extend_from_slice(&self.anchor);
        out.extend_from_slice(&self.parent_state_root);
        out.extend_from_slice(&self.state_root);
        out
    }
    pub(crate) fn decode(bytes: &[u8]) -> Result<Self> {
        if bytes.len() != 169 || bytes[0] > 1 {
            return Err(Error::Invalid("header encoding"));
        }
        let parent = bytes[1..33].try_into().unwrap();
        if bytes[0] == 0 && parent != [0; 32] {
            return Err(Error::Invalid("root parent encoding"));
        }
        Ok(Self {
            parent: (bytes[0] == 1).then_some(parent),
            height: u64::from_be_bytes(bytes[33..41].try_into().unwrap()),
            work: u128::from_be_bytes(bytes[41..57].try_into().unwrap()),
            cumulative_work: u128::from_be_bytes(bytes[57..73].try_into().unwrap()),
            anchor: bytes[73..105].try_into().unwrap(),
            parent_state_root: bytes[105..137].try_into().unwrap(),
            state_root: bytes[137..169].try_into().unwrap(),
        })
    }
}

pub struct Record<'a> {
    pub id: Id,
    pub header: Header,
    pub body: &'a [u8],
    pub delta: &'a [u8],
    pub state: &'a [u8],
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Publication {
    pub generation: u64,
    pub head: Id,
    pub state_root: Id,
}

pub struct EvaluationPublication<'a> {
    pub tip: Id,
    pub grin_height: u64,
    pub grin_hash: Id,
    pub payout: &'a [u8],
    pub manifest: Id,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EvaluationSnapshot {
    pub digest: Id,
    pub payout: Vec<u8>,
    pub dependency_manifest: Option<Id>,
}
pub(crate) fn evaluation_key(tip: Id, grin_height: u64, grin_hash: Id) -> [u8; 72] {
    let mut key = [0; 72];
    key[..32].copy_from_slice(&tip);
    key[32..40].copy_from_slice(&grin_height.to_be_bytes());
    key[40..].copy_from_slice(&grin_hash);
    key
}
pub(crate) fn evaluation_digest(payout: &[u8]) -> Id {
    blake2::blake2b::blake2b(32, &[], payout)
        .as_bytes()
        .try_into()
        .unwrap()
}

pub(crate) fn evaluation_binding_key(evaluation: [u8; 72], manifest: Id) -> [u8; 104] {
    let mut key = [0; 104];
    key[..72].copy_from_slice(&evaluation);
    key[72..].copy_from_slice(&manifest);
    key
}
pub(crate) fn decode_evaluation(
    bytes: &[u8],
    max_record_bytes: usize,
) -> Result<EvaluationSnapshot> {
    if bytes.len() <= 32 || bytes.len() - 32 > max_record_bytes {
        return Err(Error::Invalid("evaluation encoding"));
    }
    let digest = bytes[..32].try_into().unwrap();
    let payout = &bytes[32..];
    if evaluation_digest(payout) != digest {
        return Err(Error::Invalid("evaluation digest"));
    }
    Ok(EvaluationSnapshot {
        digest,
        payout: payout.to_vec(),
        dependency_manifest: None,
    })
}
