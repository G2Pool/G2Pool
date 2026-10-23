use crate::{codec::*, Error, Result};
use std::{fmt, sync::Arc};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
#[repr(u8)]
pub enum Kind {
    Coupon = 2,
    Inventory = 3,
    Registration = 4,
    Encoding = 6,
    Transaction = 7,
    Finder = 9,
    Core = 11,
    Authorization = 12,
    Residual = 13,
}

impl Kind {
    pub fn from_byte(byte: u8) -> Result<Self> {
        match byte {
            2 => Ok(Self::Coupon),
            3 => Ok(Self::Inventory),
            4 => Ok(Self::Registration),
            6 => Ok(Self::Encoding),
            7 => Ok(Self::Transaction),
            9 => Ok(Self::Finder),
            11 => Ok(Self::Core),
            12 => Ok(Self::Authorization),
            13 => Ok(Self::Residual),
            _ => Err(Error::Invalid("object kind")),
        }
    }
    pub fn domain(self) -> &'static str {
        match self {
            Self::Coupon => "COUPON",
            Self::Inventory => "INVENTORY",
            Self::Registration => "REGISTRATION",
            Self::Encoding => "KERNEL_ENCODING_PLAN",
            Self::Transaction => "TX_ONLY",
            Self::Finder => "FINDER_OUTPUT",
            Self::Core => "WORK_CORE",
            Self::Authorization => "WORK_AUTHORIZATION",
            Self::Residual => "RESIDUAL_BODY",
        }
    }
    pub fn ceiling(self) -> usize {
        match self {
            Self::Coupon => <Coupon as Object>::MAX,
            Self::Inventory => <Inventory as Object>::MAX,
            Self::Registration => <Registration as Object>::MAX,
            Self::Encoding => <EncodingPlan as Object>::MAX,
            Self::Transaction => <TxOnly as Object>::MAX,
            Self::Finder => <FinderOutput as Object>::MAX,
            Self::Core => <Core as Object>::MAX,
            Self::Authorization => <WorkAuthorization as Object>::MAX,
            Self::Residual => <Residual as Object>::MAX,
        }
    }
    pub fn addressed_by_work(self) -> bool {
        matches!(self, Self::Core | Self::Authorization | Self::Residual)
    }
}
pub trait StoredObject: Object {
    const KIND: Kind;
}
macro_rules! stored {
    ($($record:ty => $kind:ident),* $(,)?) => {$ (
        impl StoredObject for $record { const KIND: Kind=Kind::$kind; }
    )*};
}
stored!(Coupon=>Coupon,Inventory=>Inventory,
    Registration=>Registration,EncodingPlan=>Encoding,
    TxOnly=>Transaction,FinderOutput=>Finder);

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct Key {
    pub kind: Kind,
    pub id: Id,
}
impl Key {
    pub const fn new(kind: Kind, id: Id) -> Self {
        Self { kind, id }
    }
    pub fn checked(kind: Kind, id: Id, bytes: &[u8]) -> Result<Self> {
        if bytes.len() > kind.ceiling() {
            return Err(Error::Invalid("object ceiling"));
        }
        validate(kind.domain(), bytes).map_err(|error| {
            if kind == Kind::Authorization {
                Error::Witness("work authorization encoding")
            } else {
                error
            }
        })?;
        if !kind.addressed_by_work() && hash(kind.domain(), bytes) != id {
            return Err(Error::Invalid("object identity"));
        }
        Ok(Self { kind, id })
    }
}
impl fmt::Display for Key {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.kind.domain(), hex(&self.id))
    }
}
pub fn hex(bytes: &[u8]) -> String {
    grin_util::ToHex::to_hex(&bytes)
}
pub fn unhex(input: &str) -> Result<Vec<u8>> {
    if !input.len().is_multiple_of(2)
        || !input
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(Error::Invalid("hex"));
    }
    grin_util::from_hex(input).map_err(|_| Error::Invalid("hex"))
}

pub trait Source {
    fn read(&self, key: Key) -> Result<Arc<[u8]>>;
}
