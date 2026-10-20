use crate::{codec::Id, Error, Result};
use std::collections::BTreeSet;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct WindowLimits {
    pub(super) primary_limit: usize,
    pub(super) receipt_limit: usize,
    pub(super) work_limit: Option<u128>,
}

pub(super) fn bounded_ids(
    contributions: &[(Id, u64, u8)],
    limits: WindowLimits,
) -> Result<BTreeSet<Id>> {
    if limits.primary_limit == 0 || limits.receipt_limit == 0 || limits.work_limit == Some(0) {
        return Err(Error::Invalid("window bounds"));
    }
    let mut seen = BTreeSet::new();
    let mut selected = BTreeSet::new();
    let mut total = 0u128;
    let mut primaries = 0usize;
    for (id, difficulty, class) in contributions {
        if *difficulty == 0 || *class > 1 || !seen.insert(*id) {
            return Err(Error::Invalid("window contribution"));
        }
        if *class == 0 {
            if primaries == limits.primary_limit {
                break;
            }
            primaries += 1;
        } else if limits
            .work_limit
            .is_some_and(|limit| total.saturating_add(u128::from(*difficulty)) > limit)
        {
            continue;
        }
        total = total
            .checked_add(u128::from(*difficulty))
            .ok_or(Error::Invalid("window work overflow"))?;
        selected.insert(*id);
        if selected.len() == limits.receipt_limit
            || limits.work_limit.is_some_and(|limit| total >= limit)
        {
            break;
        }
    }
    Ok(selected)
}

#[cfg(test)]
#[path = "window_tests.rs"]
mod tests;
