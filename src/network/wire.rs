use crate::{
    codec::Id,
    objects::{Key, Kind},
    storage::Object,
    Error, Result,
};
use std::{collections::BTreeSet, net::SocketAddr};
pub(super) const FRAME: usize = 16 * 1024 * 1024;
pub(crate) const REQUEST_KEYS: usize = 32;
pub(crate) const MAX_TIPS: usize = 64;

#[derive(Debug, Clone)]
pub enum Message {
    Heads,
    Announce(u16),
    Peers,
    Addresses(Vec<SocketAddr>),
    Tips(Vec<Id>),
    Get(Vec<Key>),
    Reply {
        objects: Vec<Object>,
        missing: Vec<Key>,
        deferred: Vec<Key>,
    },
    Notice(Vec<Id>),
    Ack,
    Busy,
}
fn push_key(out: &mut Vec<u8>, key: Key) {
    out.push(key.kind as u8);
    out.extend(key.id);
}
struct Reader<'a> {
    bytes: &'a [u8],
    offset: usize,
}
impl<'a> Reader<'a> {
    fn take(&mut self, n: usize) -> Result<&'a [u8]> {
        let end = self
            .offset
            .checked_add(n)
            .ok_or(Error::Invalid("frame overflow"))?;
        let bytes = self
            .bytes
            .get(self.offset..end)
            .ok_or(Error::Invalid("frame truncation"))?;
        self.offset = end;
        Ok(bytes)
    }
    fn byte(&mut self) -> Result<u8> {
        Ok(self.take(1)?[0])
    }
    fn id(&mut self) -> Result<Id> {
        Ok(self.take(32)?.try_into().unwrap())
    }
    fn key(&mut self) -> Result<Key> {
        Ok(Key::new(Kind::from_byte(self.byte()?)?, self.id()?))
    }
}
impl Message {
    pub fn encode(&self) -> Result<Vec<u8>> {
        if let Self::Reply {
            objects,
            missing,
            deferred,
        } = self
        {
            if missing.len() + deferred.len() + objects.len() > REQUEST_KEYS {
                return Err(Error::Capacity("reply partition"));
            }
            if objects.len() > REQUEST_KEYS {
                return Err(Error::Capacity("object list"));
            }
            let size = objects.iter().try_fold(4usize, |size, object| {
                if object.bytes.len() > object.key.kind.ceiling() {
                    return Err(Error::Invalid("object frame ceiling"));
                }
                size.checked_add(37)
                    .and_then(|size| size.checked_add(object.bytes.len()))
                    .ok_or(Error::Capacity("wire frame"))
            })?;
            let size = size
                .checked_add((missing.len() + deferred.len()) * 33)
                .ok_or(Error::Capacity("wire frame"))?;
            if size > FRAME {
                return Err(Error::Capacity("wire frame"));
            }
        }
        let mut out = Vec::new();
        match self {
            Self::Heads => out.push(0),
            Self::Announce(port) => {
                out.push(5);
                out.extend(port.to_be_bytes());
            }
            Self::Peers => out.push(6),
            Self::Addresses(addresses) => {
                if addresses.len() > 16 {
                    return Err(Error::Capacity("peer addresses"));
                }
                out.extend([7, addresses.len() as u8]);
                for address in addresses {
                    let text = address.to_string();
                    if text.len() > 64 {
                        return Err(Error::Invalid("peer address"));
                    }
                    out.push(text.len() as u8);
                    out.extend(text.as_bytes());
                }
            }
            Self::Tips(ids) | Self::Notice(ids) => {
                if ids.len() > MAX_TIPS {
                    return Err(Error::Capacity("tip list"));
                }
                out.extend([
                    if matches!(self, Self::Tips(_)) { 1 } else { 8 },
                    ids.len() as u8,
                ]);
                for id in ids {
                    out.extend(id);
                }
            }
            Self::Get(keys) => {
                if keys.iter().copied().collect::<BTreeSet<_>>().len() != keys.len() {
                    return Err(Error::Invalid("duplicate request key"));
                }
                if keys.len() > REQUEST_KEYS {
                    return Err(Error::Capacity("key list"));
                }
                out.extend([2, keys.len() as u8]);
                for key in keys {
                    push_key(&mut out, *key);
                }
            }
            Self::Reply {
                objects,
                missing,
                deferred,
            } => {
                if objects.len() > REQUEST_KEYS {
                    return Err(Error::Capacity("object list"));
                }
                out.extend([3, objects.len() as u8]);
                for object in objects {
                    push_key(&mut out, object.key);
                    out.extend((object.bytes.len() as u32).to_be_bytes());
                    out.extend(object.bytes.as_ref());
                }
                for keys in [missing, deferred] {
                    if keys.len() > REQUEST_KEYS {
                        return Err(Error::Capacity("reply key count"));
                    }
                    out.push(keys.len() as u8);
                    for key in keys {
                        push_key(&mut out, *key);
                    }
                }
            }
            Self::Ack => out.push(9),
            Self::Busy => out.push(10),
        }
        if out.len() > FRAME {
            return Err(Error::Capacity("wire frame"));
        }
        Ok(out)
    }
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        if bytes.len() > FRAME {
            return Err(Error::Capacity("wire frame"));
        }
        let mut r = Reader { bytes, offset: 0 };
        let tag = r.byte()?;
        let result = match tag {
            0 => Self::Heads,
            5 => Self::Announce(u16::from_be_bytes(r.take(2)?.try_into().unwrap())),
            6 => Self::Peers,
            7 => {
                let count = r.byte()?;
                if count > 16 {
                    return Err(Error::Invalid("peer address count"));
                }
                let mut addresses = Vec::new();
                for _ in 0..count {
                    let len = r.byte()? as usize;
                    if len > 64 {
                        return Err(Error::Invalid("peer address length"));
                    }
                    let text = std::str::from_utf8(r.take(len)?)
                        .map_err(|_| Error::Invalid("peer address text"))?;
                    let address: SocketAddr =
                        text.parse().map_err(|_| Error::Invalid("peer address"))?;
                    if address.to_string() != text {
                        return Err(Error::Invalid("peer address alias"));
                    }
                    addresses.push(address);
                }
                Self::Addresses(addresses)
            }
            1 | 8 => {
                let n = r.byte()?;
                if usize::from(n) > MAX_TIPS {
                    return Err(Error::Invalid("tip count"));
                }
                let mut tips = Vec::new();
                for _ in 0..n {
                    tips.push(r.id()?);
                }
                if tag == 1 {
                    Self::Tips(tips)
                } else {
                    Self::Notice(tips)
                }
            }
            2 => {
                let n = r.byte()?;
                if n as usize > REQUEST_KEYS {
                    return Err(Error::Invalid("key count"));
                }
                let mut keys = Vec::new();
                for _ in 0..n {
                    keys.push(r.key()?);
                }
                if keys.iter().copied().collect::<BTreeSet<_>>().len() != keys.len() {
                    return Err(Error::Invalid("duplicate request key"));
                }
                Self::Get(keys)
            }
            3 => {
                let n = r.byte()?;
                if n as usize > REQUEST_KEYS {
                    return Err(Error::Invalid("object count"));
                }
                let mut objects = Vec::new();
                for _ in 0..n {
                    let key = r.key()?;
                    let size = u32::from_be_bytes(r.take(4)?.try_into().unwrap()) as usize;
                    if size > key.kind.ceiling() {
                        return Err(Error::Invalid("object frame ceiling"));
                    }
                    objects.push(Object {
                        key,
                        bytes: std::sync::Arc::from(r.take(size)?),
                    });
                }
                let mut lists = [Vec::new(), Vec::new()];
                for keys in &mut lists {
                    let n = r.byte()? as usize;
                    if n > REQUEST_KEYS {
                        return Err(Error::Invalid("reply key count"));
                    }
                    for _ in 0..n {
                        keys.push(r.key()?);
                    }
                }
                let [missing, deferred] = lists;
                Self::Reply {
                    objects,
                    missing,
                    deferred,
                }
            }
            9 => Self::Ack,
            10 => Self::Busy,
            _ => return Err(Error::Invalid("message tag")),
        };
        if r.offset != bytes.len() {
            return Err(Error::Invalid("frame trailing bytes"));
        }
        Ok(result)
    }
}

impl Message {
    pub(super) fn validate_reply(&self, request: &Message) -> Result<()> {
        match (request, self) {
            (_, Self::Busy) => Ok(()),
            (Self::Heads, Self::Tips(_))
            | (Self::Peers | Self::Announce(_), Self::Addresses(_))
            | (Self::Notice(_), Self::Ack) => Ok(()),
            (
                Self::Get(keys),
                Self::Reply {
                    objects,
                    missing,
                    deferred,
                },
            ) => {
                let mut covered = BTreeSet::new();
                for key in objects
                    .iter()
                    .map(|o| o.key)
                    .chain(missing.iter().copied())
                    .chain(deferred.iter().copied())
                {
                    if !covered.insert(key) {
                        return Err(Error::Invalid("duplicate reply key"));
                    }
                }
                if covered != keys.iter().copied().collect() {
                    return Err(Error::Invalid("reply key coverage"));
                }
                Ok(())
            }
            _ => Err(Error::Invalid("unexpected message role")),
        }
    }
    pub(super) fn response_ceiling(&self) -> Result<usize> {
        Ok(match self {
            Self::Heads => 2 + MAX_TIPS * 32,
            Self::Peers | Self::Announce(_) => 2 + 16 * 65,
            Self::Notice(_) => 1,
            Self::Get(keys) => {
                if keys.len() > REQUEST_KEYS {
                    return Err(Error::Capacity("key list"));
                }
                let size = keys.iter().try_fold(4usize, |size, key| {
                    size.checked_add(37 + key.kind.ceiling())
                        .ok_or(Error::Capacity("response frame"))
                })?;
                if size > FRAME {
                    return Err(Error::Capacity("request reply envelope"));
                }
                size
            }
            _ => return Err(Error::Invalid("request role")),
        })
    }
    pub(super) fn accepts_tag(&self, tag: u8) -> bool {
        tag == 10
            || matches!(
                (self, tag),
                (Self::Heads, 1)
                    | (Self::Peers | Self::Announce(_), 7)
                    | (Self::Get(_), 3)
                    | (Self::Notice(_), 9)
            )
    }
    pub(super) fn is_bulk(&self) -> bool {
        matches!(self, Self::Get(_))
    }
}

#[cfg(test)]
#[path = "wire_tests.rs"]
mod tests;
