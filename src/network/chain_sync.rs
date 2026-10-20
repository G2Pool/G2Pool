use crate::{codec::Id, Error, Result};
use std::collections::BTreeSet;

pub const MAX_LOCATOR: usize = 80;
pub const MAX_HEADERS: usize = 128;
pub const LOCATOR_FRAME: usize = 66 + MAX_LOCATOR * 32;
pub const HEADERS_FRAME: usize = 66 + MAX_HEADERS * 169;
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Header {
    pub id: Id,
    pub parent: Option<Id>,
    pub height: u64,
    pub credited_work: u128,
    pub cumulative_work: u128,
    pub parent_state_root: Id,
    pub state_root: Id,
}

pub trait ChainIndex {
    fn header(&self, id: Id) -> Result<Option<Header>>;
    fn ancestor(&self, tip: Id, height: u64) -> Result<Option<Header>>;
}

impl<'a> ChainIndex for crate::storage::sharechain::Snapshot<'a> {
    fn header(&self, id: Id) -> Result<Option<Header>> {
        let Some(stored) = self
            .header(&id)
            .map_err(|_| Error::Invalid("sharechain storage header"))?
        else {
            return Ok(None);
        };
        Ok(Some(Header {
            id,
            parent: stored.parent,
            height: stored.height,
            credited_work: stored.work,
            cumulative_work: stored.cumulative_work,
            parent_state_root: stored.parent_state_root,
            state_root: stored.state_root,
        }))
    }

    fn ancestor(&self, tip: Id, height: u64) -> Result<Option<Header>> {
        let Some(tip_header) = <Self as ChainIndex>::header(self, tip)? else {
            return Ok(None);
        };
        if height > tip_header.height {
            return Ok(None);
        }
        let id = crate::storage::sharechain::Snapshot::ancestor(self, tip, height)
            .map_err(|_| Error::Invalid("sharechain storage ancestor"))?;
        <Self as ChainIndex>::header(self, id)
    }
}

fn lookup(index: &impl ChainIndex, id: Id) -> Result<Header> {
    let header = index.header(id)?.ok_or(Error::Missing(id))?;
    if header.id != id || (header.height == 0) != header.parent.is_none() {
        return Err(Error::Invalid("sharechain index header"));
    }
    if header.credited_work == 0 {
        return Err(Error::Invalid("sharechain zero work"));
    }
    Ok(header)
}
fn ancestor(index: &impl ChainIndex, tip: Id, height: u64) -> Result<Header> {
    let header = index.ancestor(tip, height)?.ok_or(Error::Missing(tip))?;
    if header.height != height || (height == 0) != header.parent.is_none() {
        return Err(Error::Invalid("sharechain index ancestry"));
    }
    Ok(header)
}

pub fn common_ancestor(
    index: &impl ChainIndex,
    target: Id,
    locator: &[Id],
) -> Result<Option<Header>> {
    if locator.is_empty() || locator.len() > MAX_LOCATOR {
        return Err(Error::Invalid("sharechain locator length"));
    }
    let mut distinct = BTreeSet::new();
    if locator.iter().any(|id| !distinct.insert(*id)) {
        return Err(Error::Invalid("sharechain duplicate locator"));
    }
    let target_header = lookup(index, target)?;
    for id in locator {
        let Some(header) = index.header(*id)? else {
            continue;
        };
        if header.id != *id {
            return Err(Error::Invalid("sharechain index identity"));
        }
        if header.height <= target_header.height
            && ancestor(index, target, header.height)?.id == *id
        {
            return Ok(Some(header));
        }
    }
    Ok(None)
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HeaderPage {
    pub target: Header,
    pub start: Header,
    pub headers: Vec<Header>,
}

pub fn header_range(
    index: &impl ChainIndex,
    target: Id,
    start: Id,
    limit: usize,
) -> Result<HeaderPage> {
    if limit == 0 || limit > MAX_HEADERS {
        return Err(Error::Invalid("sharechain header range length"));
    }
    let target = lookup(index, target)?;
    let start = lookup(index, start)?;
    if start.height > target.height || ancestor(index, target.id, start.height)?.id != start.id {
        return Err(Error::Invalid("sharechain range start off branch"));
    }
    let count = (target.height - start.height).min(limit as u64) as usize;
    let mut headers = Vec::with_capacity(count);
    let mut previous = start;
    for offset in 1..=count {
        let header = ancestor(index, target.id, start.height + offset as u64)?;
        check_link(previous, header)?;
        headers.push(header);
        previous = header;
    }
    Ok(HeaderPage {
        target,
        start,
        headers,
    })
}

fn check_link(parent: Header, child: Header) -> Result<()> {
    if parent.height.checked_add(1) != Some(child.height)
        || child.parent != Some(parent.id)
        || child.id == parent.id
        || child.credited_work == 0
        || parent.cumulative_work.checked_add(child.credited_work) != Some(child.cumulative_work)
        || child.parent_state_root != parent.state_root
    {
        return Err(Error::Invalid("sharechain header link"));
    }
    Ok(())
}

#[cfg(test)]
#[path = "chain_sync_tests.rs"]
mod tests;
