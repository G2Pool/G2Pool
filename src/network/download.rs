use super::budget::{Bytes, Ticket};
use crate::{
    objects::{Key, Kind},
    storage::Object,
    Error, Result,
};
use std::{
    collections::{BTreeMap, BTreeSet},
    net::SocketAddr,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
pub(crate) struct Downloads {
    active: BTreeSet<Key>,
    absent: BTreeMap<(SocketAddr, Key), Instant>,
    fragments: BTreeMap<Key, Fragment>,
    fragment_bytes: Arc<Bytes>,
}
struct Fragment {
    object: Object,
    source: SocketAddr,
    expires: Instant,
    _ticket: Ticket,
}
pub(super) struct Assembled {
    pub objects: Vec<Object>,
    pub mixed: bool,
    _fragments: Vec<Fragment>,
}
impl Default for Downloads {
    fn default() -> Self {
        Self {
            active: Default::default(),
            absent: Default::default(),
            fragments: Default::default(),
            fragment_bytes: Bytes::new(1 << 20),
        }
    }
}
pub(super) struct Lease {
    shared: Arc<Mutex<Downloads>>,
    keys: Vec<Key>,
}
impl Downloads {
    pub(super) fn assemble(
        shared: &Arc<Mutex<Self>>,
        objects: Vec<Object>,
        address: SocketAddr,
        stored: &BTreeSet<Key>,
    ) -> Result<Assembled> {
        let mut state = shared.lock().unwrap_or_else(|e| e.into_inner());
        state
            .fragments
            .retain(|_, fragment| fragment.expires > Instant::now());
        let received: BTreeSet<_> = objects.iter().map(|o| o.key).collect();
        let mut assembled = Assembled {
            objects: vec![],
            mixed: false,
            _fragments: vec![],
        };
        for object in objects {
            let counterpart = match object.key.kind {
                Kind::Core => Some(Kind::Authorization),
                Kind::Authorization => Some(Kind::Core),
                _ => None,
            };
            let Some(kind) = counterpart else {
                assembled.objects.push(object);
                continue;
            };
            let key = Key::new(kind, object.key.id);
            if received.contains(&key) || stored.contains(&key) {
                assembled.mixed |= stored.contains(&key) && !received.contains(&key);
                state.fragments.remove(&object.key);
                assembled.objects.push(object);
                continue;
            }
            let ticket = state
                .fragment_bytes
                .reserve((object.bytes.len() + 128) * 6)
                .ok_or(Error::Capacity("partial witness bytes"))?;
            Key::checked(object.key.kind, object.key.id, &object.bytes)?;
            if state.fragments.len() >= 32 && !state.fragments.contains_key(&object.key) {
                return Err(Error::Capacity("partial witnesses"));
            }
            state.fragments.insert(
                object.key,
                Fragment {
                    object,
                    source: address,
                    expires: Instant::now() + Duration::from_secs(30),
                    _ticket: ticket,
                },
            );
        }
        let pairs: Vec<_> = state
            .fragments
            .keys()
            .filter(|key| {
                key.kind == Kind::Core
                    && state
                        .fragments
                        .contains_key(&Key::new(Kind::Authorization, key.id))
            })
            .copied()
            .collect();
        for core in pairs {
            if assembled.objects.len() + 2 > super::REQUEST_KEYS {
                break;
            }
            for key in [core, Key::new(Kind::Authorization, core.id)] {
                let fragment = state.fragments.remove(&key).expect("checked fragment pair");
                assembled.mixed |= fragment.source != address;
                assembled.objects.push(fragment.object.clone());
                assembled._fragments.push(fragment);
            }
        }
        Ok(assembled)
    }
    pub(super) fn reserve(
        shared: &Arc<Mutex<Self>>,
        address: SocketAddr,
        keys: Vec<Key>,
    ) -> Option<Lease> {
        let mut state = shared.lock().unwrap_or_else(|e| e.into_inner());
        let now = Instant::now();
        state.absent.retain(|_, until| *until > now);
        if keys
            .iter()
            .any(|key| state.active.contains(key) || state.absent.contains_key(&(address, *key)))
        {
            return None;
        }
        if state.active.len() + keys.len() > 128 {
            return None;
        }
        state.active.extend(keys.iter().copied());
        Some(Lease {
            shared: shared.clone(),
            keys,
        })
    }
}
impl Lease {
    pub(super) fn absent(&self, address: SocketAddr, keys: &[Key]) {
        self.cooldown(address, keys, Duration::from_secs(10));
    }
    pub(super) fn deferred(&self, address: SocketAddr, keys: &[Key]) {
        self.cooldown(address, keys, Duration::from_secs(1));
    }
    fn cooldown(&self, address: SocketAddr, keys: &[Key], duration: Duration) {
        let mut state = self.shared.lock().unwrap_or_else(|e| e.into_inner());
        for key in keys {
            if state.absent.len() < 512 {
                state
                    .absent
                    .insert((address, *key), Instant::now() + duration);
            }
        }
    }
}
impl Drop for Lease {
    fn drop(&mut self) {
        let mut state = self.shared.lock().unwrap_or_else(|e| e.into_inner());
        for key in &self.keys {
            state.active.remove(key);
        }
    }
}

#[cfg(test)]
#[path = "download_tests.rs"]
mod tests;
