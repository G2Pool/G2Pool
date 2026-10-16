use crate::{Error, Result};
use std::{
    collections::BTreeMap,
    time::{Duration, Instant},
};

#[derive(Debug, Clone)]
pub struct Retention {
    pub duration: Duration,
    pub bytes: usize,
}
impl Default for Retention {
    fn default() -> Self {
        Self {
            duration: Duration::from_secs(120),
            bytes: 256 * 1024 * 1024,
        }
    }
}
struct Entry<T> {
    value: T,
    expires: Instant,
    bytes: usize,
}
pub(super) struct Leases<T> {
    entries: BTreeMap<u64, Entry<T>>,
    used: usize,
    policy: Retention,
}
impl<T> Leases<T> {
    pub(super) fn new(policy: Retention) -> Self {
        Self {
            entries: BTreeMap::new(),
            used: 0,
            policy,
        }
    }
    pub(super) fn expire(&mut self, now: Instant) {
        while self
            .entries
            .first_key_value()
            .is_some_and(|(_, entry)| entry.expires <= now)
        {
            let (_, entry) = self.entries.pop_first().unwrap();
            self.used -= entry.bytes;
        }
    }
    pub(super) fn insert(&mut self, id: u64, value: T, bytes: usize, now: Instant) -> Result<()> {
        self.expire(now);
        if self.entries.contains_key(&id) {
            return Err(Error::Invalid("duplicate mining job ID"));
        }
        let used = self
            .used
            .checked_add(bytes)
            .filter(|used| *used <= self.policy.bytes)
            .ok_or(Error::Capacity("mining job retention"))?;
        let expires = now
            .checked_add(self.policy.duration)
            .filter(|expires| *expires > now)
            .ok_or(Error::Capacity("mining job lifetime"))?;
        if self
            .entries
            .last_key_value()
            .is_some_and(|(last, entry)| id <= *last || expires < entry.expires)
        {
            return Err(Error::Invalid("mining job lease order"));
        }
        self.entries.insert(
            id,
            Entry {
                value,
                expires,
                bytes,
            },
        );
        self.used = used;
        Ok(())
    }
    pub(super) fn get(&self, id: u64) -> Option<&T> {
        self.entries.get(&id).map(|entry| &entry.value)
    }
    pub(super) fn values(&self) -> impl Iterator<Item = &T> {
        self.entries.values().map(|entry| &entry.value)
    }
}

#[cfg(test)]
#[path = "retention_tests.rs"]
mod tests;
