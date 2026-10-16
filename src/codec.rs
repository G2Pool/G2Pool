use crate::{Error, Result};
pub type Id = [u8; 32];
pub type Point = [u8; 33];
pub type Sig = [u8; 64];
pub const VERSION: u16 = 1;
pub const MAX_ENCODING_ENTRIES: usize = 2048;
pub const MAX_PAYOUT_RECORD_BYTES: usize = 2 * 1024 * 1024;

pub struct Reader<'a> {
    bytes: &'a [u8],
    offset: usize,
}
impl<'a> Reader<'a> {
    pub fn take(&mut self, count: usize) -> Result<&'a [u8]> {
        let end = self
            .offset
            .checked_add(count)
            .ok_or(Error::Invalid("length overflow"))?;
        let bytes = self
            .bytes
            .get(self.offset..end)
            .ok_or(Error::Invalid("truncation"))?;
        self.offset = end;
        Ok(bytes)
    }
}
pub trait Field: Sized {
    fn read(r: &mut Reader<'_>) -> Result<Self>;
    fn write(&self, out: &mut Vec<u8>) -> Result<()>;
}
macro_rules! integer {
    ($($t:ty),*) => {$ (impl Field for $t {
        fn read(r: &mut Reader<'_>) -> Result<Self> {
            Ok(<$t>::from_be_bytes(r.take(std::mem::size_of::<$t>())?.try_into().unwrap()))
        }
        fn write(&self, out: &mut Vec<u8>) -> Result<()> { out.extend(self.to_be_bytes()); Ok(()) }
    })*};
}
integer!(u8, u16, u32, u64, i64);
impl<const N: usize> Field for [u8; N] {
    fn read(r: &mut Reader<'_>) -> Result<Self> {
        Ok(r.take(N)?.try_into().unwrap())
    }
    fn write(&self, out: &mut Vec<u8>) -> Result<()> {
        out.extend(self);
        Ok(())
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Blob<const M: usize>(pub Vec<u8>);
impl<const M: usize> Field for Blob<M> {
    fn read(r: &mut Reader<'_>) -> Result<Self> {
        let count = u32::read(r)? as usize;
        if count > M {
            return Err(Error::Invalid("blob ceiling"));
        }
        Ok(Self(r.take(count)?.to_vec()))
    }
    fn write(&self, out: &mut Vec<u8>) -> Result<()> {
        if self.0.len() > M {
            return Err(Error::Invalid("blob ceiling"));
        }
        (self.0.len() as u32).write(out)?;
        out.extend(&self.0);
        Ok(())
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct List<T, const M: usize>(pub Vec<T>);
impl<T: Field, const M: usize> Field for List<T, M> {
    fn read(r: &mut Reader<'_>) -> Result<Self> {
        let count = u16::read(r)? as usize;
        if count > M {
            return Err(Error::Invalid("list ceiling"));
        }
        let mut items = Vec::with_capacity(count);
        for _ in 0..count {
            items.push(T::read(r)?);
        }
        Ok(Self(items))
    }
    fn write(&self, out: &mut Vec<u8>) -> Result<()> {
        if self.0.len() > M {
            return Err(Error::Invalid("list ceiling"));
        }
        (self.0.len() as u16).write(out)?;
        for item in &self.0 {
            item.write(out)?;
        }
        Ok(())
    }
}
macro_rules! record {
    ($name:ident { $($field:ident: $t:ty),* $(,)? }) => {
        #[derive(Debug, Clone, PartialEq, Eq)]
        pub struct $name { $(pub $field: $t),* }
        impl Field for $name {
            fn read(r: &mut Reader<'_>) -> Result<Self> { Ok(Self { $($field: <$t>::read(r)?),* }) }
            fn write(&self, out: &mut Vec<u8>) -> Result<()> { $(self.$field.write(out)?;)* Ok(()) }
        }
    }
}
record!(Anchor {
    height: u64,
    hash: Id
});
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Parent {
    Root(Anchor),
    Receipt(Id),
}
impl Field for Parent {
    fn read(r: &mut Reader<'_>) -> Result<Self> {
        match u8::read(r)? {
            0 => Ok(Self::Root(Anchor::read(r)?)),
            1 => Ok(Self::Receipt(Id::read(r)?)),
            _ => Err(Error::Invalid("parent tag")),
        }
    }
    fn write(&self, o: &mut Vec<u8>) -> Result<()> {
        match self {
            Self::Root(a) => {
                0u8.write(o)?;
                a.write(o)
            }
            Self::Receipt(id) => {
                1u8.write(o)?;
                id.write(o)
            }
        }
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Encoding {
    Individual,
    Aggregate(Id),
}
impl Field for Encoding {
    fn read(r: &mut Reader<'_>) -> Result<Self> {
        match u8::read(r)? {
            0 => Ok(Self::Individual),
            1 => Ok(Self::Aggregate(Id::read(r)?)),
            _ => Err(Error::Invalid("encoding tag")),
        }
    }
    fn write(&self, o: &mut Vec<u8>) -> Result<()> {
        match self {
            Self::Individual => 0u8.write(o),
            Self::Aggregate(id) => {
                1u8.write(o)?;
                id.write(o)
            }
        }
    }
}
impl Field for Option<Id> {
    fn read(r: &mut Reader<'_>) -> Result<Self> {
        match u8::read(r)? {
            0 => Ok(None),
            1 => Ok(Some(Id::read(r)?)),
            _ => Err(Error::Invalid("option tag")),
        }
    }
    fn write(&self, o: &mut Vec<u8>) -> Result<()> {
        match self {
            None => 0u8.write(o),
            Some(id) => {
                1u8.write(o)?;
                id.write(o)
            }
        }
    }
}
pub const COUPON_LEAVES_PER_ROW: usize = 12;
record!(Limits {
    work_core: u32,
    coupon: u32,
    inventory: u32,
    registration: u32,
    finder_output: u32,
    payout_record: u32,
    kernel_encoding_plan: u32,
    transaction_only: u32,
    residual_body: u32
});
record!(Manifest {
    network: u8,
    genesis: Id,
    grin_revision: [u8; 20],
    subsidy: u64,
    finder_floor: u64,
    unit_budget: u16,
    share_divisor: u64,
    share_target_seconds: u16,
    lifetime: u64,
    leaves_per_row: u8,
    uncle_depth: u8,
    max_uncles: u8,
    future_seconds: u32,
    edge_bits: u8,
    proof_size: u16,
    limits: Limits
});
record!(Reward { output:Blob<2048>, kernel:Blob<256> });
record!(Coupon {
    reward: Reward,
    owner_auth: Sig
});
record!(InventoryRow { family:u8, row:u8, coupon_ids:List<Id,COUPON_LEAVES_PER_ROW> });
record!(Inventory { protocol:Id, owner:Point, rows:List<InventoryRow,14> });
record!(RegistrationAuth {
    protocol: Id,
    owner: Point,
    inventory: Id,
    birth: Anchor,
    revision: u64
});
record!(Registration {
    fields: RegistrationAuth,
    signature: Sig
});
record!(CouponAuth {
    protocol: Id,
    owner: Point,
    reward_id: Id
});
record!(WorkAuthMessage {
    protocol: Id,
    work_id: Id
});
record!(WorkAuthorization {
    fields: WorkAuthMessage,
    signature: Sig
});
record!(ReceiptRow {
    work_id: Id,
    account: Point,
    class: u8,
    difficulty: u64,
    units: u16
});
record!(Outcome { family:u8, row:u8, leaf:u8, coupon:Option<Id>, code:u8, value:u64 });
record!(AccountRow { account:Point, registration:Id, canonical_units:u16, uncle_units:u16,
    base_units:u16, residual_units:u8, outcomes:List<Outcome,14> });
record!(PayoutRecord { protocol:Id, anchor:Anchor, parent:Parent, receipts:List<ReceiptRow,MAX_ENCODING_ENTRIES>,
    accounts:List<AccountRow,MAX_ENCODING_ENTRIES>, paid:u64, finder:u64 });
record!(EncodingEntry {
    account: Point,
    encoding: Encoding
});
record!(EncodingPlan { protocol:Id, entries:List<EncodingEntry,MAX_ENCODING_ENTRIES> });
record!(TxOnly { protocol:Id, native:Blob<8388608> });
record!(Kernel { protocol:Id, native:Blob<256> });
record!(FinderOutput { protocol:Id, output:Blob<2048> });
record!(Job { protocol:Id, anchor:Anchor, timestamp:i64, parent:Parent, account:Point,
    registration:Id, difficulty:u64, uncles:List<Id,8>, transaction_only:Id,
    kernel_encoding_plan:Id, finder_value:u64, finder_commitment:Point, finder_output:Id });
record!(Core { header:Blob<4096>, job:Blob<2048> });
record!(Residual { protocol:Id, finder_kernel:Blob<256>, binding_kernel:Blob<256>,
    aggregate_kernels:List<Blob<256>,256> });

pub trait Object: Field {
    const DOMAIN: &'static str;
    const MAX: usize;
}
macro_rules! object {
    ($($t:ty => ($d:literal, $m:expr)),* $(,)?) => {$ (impl Object for $t {
        const DOMAIN: &'static str = $d; const MAX: usize = $m;
    })*};
}
object!(Manifest=> ("PROTOCOL",512),Reward=> ("REWARD",4096),Coupon=> ("COUPON",2048),
    Inventory=> ("INVENTORY",262144),RegistrationAuth=> ("REGISTRATION_AUTH",192),
    Registration=> ("REGISTRATION",256),WorkAuthMessage=> ("WORK_AUTH",128),
    WorkAuthorization=> ("WORK_AUTHORIZATION",192),CouponAuth=> ("COUPON_AUTH",128),
    PayoutRecord=> ("PAYOUT_RECORD",MAX_PAYOUT_RECORD_BYTES),EncodingPlan=> ("KERNEL_ENCODING_PLAN",262144),
    TxOnly=> ("TX_ONLY",8388608),Kernel=> ("KERNEL",512),FinderOutput=> ("FINDER_OUTPUT",2048),
    Job=> ("INDEPENDENT_JOB",2048),Core=> ("WORK_CORE",16384),Residual=> ("RESIDUAL_BODY",8388608));
pub fn encode<T: Object>(value: &T) -> Result<Vec<u8>> {
    let mut out = Vec::new();
    VERSION.write(&mut out)?;
    value.write(&mut out)?;
    if out.len() > T::MAX {
        return Err(Error::Invalid("object ceiling"));
    }
    Ok(out)
}
pub fn decode<T: Object>(bytes: &[u8]) -> Result<T> {
    if bytes.len() > T::MAX {
        return Err(Error::Invalid("object ceiling"));
    }
    let mut r = Reader { bytes, offset: 0 };
    if u16::read(&mut r)? != VERSION {
        return Err(Error::Invalid("schema version"));
    }
    let value = T::read(&mut r)?;
    if r.offset != bytes.len() {
        return Err(Error::Invalid("trailing bytes"));
    }
    Ok(value)
}
pub fn hash(domain: &str, bytes: &[u8]) -> Id {
    let mut frame = format!("G2POOL_{domain}\0").into_bytes();
    frame.extend(VERSION.to_be_bytes());
    frame.extend((bytes.len() as u32).to_be_bytes());
    frame.extend(bytes);
    blake2::blake2b::blake2b(32, &[], &frame)
        .as_bytes()
        .try_into()
        .unwrap()
}
pub fn id<T: Object>(value: &T) -> Result<Id> {
    Ok(hash(T::DOMAIN, &encode(value)?))
}
pub fn scalar(protocol: Id, job: Id) -> Result<[u8; 32]> {
    let order = [
        0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
        0xfe, 0xba, 0xae, 0xdc, 0xe6, 0xaf, 0x48, 0xa0, 0x3b, 0xbf, 0xd2, 0x5e, 0x8c, 0xd0, 0x36,
        0x41, 0x41,
    ];
    for counter in 0u32..256 {
        let payload = [protocol.as_slice(), job.as_slice(), &counter.to_be_bytes()].concat();
        let digest = hash("JOB_SCALAR", &payload);
        if digest != [0; 32] && digest < order {
            return Ok(digest);
        }
    }
    Err(Error::Invalid("scalar rejection bound"))
}

pub fn roundtrip(domain: &str, bytes: &[u8]) -> Result<Vec<u8>> {
    macro_rules! go {
        ($t:ty) => {
            encode(&decode::<$t>(bytes)?)
        };
    }
    match domain {
        "PROTOCOL" => go!(Manifest),
        "REWARD" => go!(Reward),
        "COUPON" => go!(Coupon),
        "INVENTORY" => go!(Inventory),
        "REGISTRATION_AUTH" => go!(RegistrationAuth),
        "REGISTRATION" => go!(Registration),
        "COUPON_AUTH" => go!(CouponAuth),
        "WORK_AUTH" => go!(WorkAuthMessage),
        "WORK_AUTHORIZATION" => go!(WorkAuthorization),
        "PAYOUT_RECORD" => go!(PayoutRecord),
        "KERNEL_ENCODING_PLAN" => go!(EncodingPlan),
        "TX_ONLY" => go!(TxOnly),
        "KERNEL" => go!(Kernel),
        "FINDER_OUTPUT" => go!(FinderOutput),
        "INDEPENDENT_JOB" => go!(Job),
        "WORK_CORE" => go!(Core),
        "RESIDUAL_BODY" => go!(Residual),
        _ => Err(Error::Invalid("unknown object domain")),
    }
}

#[cfg(test)]
#[path = "codec_tests.rs"]
mod tests;
