use super::{Evaluation, Facts};
use crate::{
    codec::{encode, hash, Anchor, Field, Id, Object, PayoutRecord},
    objects::{Key, Kind},
    storage::sharechain::{DependencyKey, DependencyManifest, DependencyObject},
    Error, Result,
};
use im::OrdMap;
use std::{collections::BTreeSet, sync::Arc};

const PAYLOAD_MAGIC: &[u8; 4] = b"G2DP";
const PAYLOAD_HEADER: usize = 4 + 1 + 32 + 4;

#[derive(Debug, Clone)]
struct SealedFact {
    route: Key,
    bytes: Arc<[u8]>,
}

#[derive(Debug, Clone)]
pub struct VerifiedClosure {
    tip: Id,
    id: Id,
    logical_id: Id,
    keys: Vec<DependencyKey>,
    facts: OrdMap<DependencyKey, SealedFact>,
    routes: OrdMap<Key, DependencyKey>,
    parent: Option<Id>,
    added: Vec<DependencyKey>,
    removed: Vec<DependencyKey>,
    fact_bytes: usize,
    retained_bytes: usize,
}

pub struct ClosureObjects {
    payloads: Vec<(DependencyKey, Vec<u8>)>,
}
impl ClosureObjects {
    pub fn objects(&self) -> Vec<DependencyObject<'_>> {
        self.payloads
            .iter()
            .map(|(key, bytes)| DependencyObject { key: *key, bytes })
            .collect()
    }
}

fn bound_key(route: Key, bytes: &[u8]) -> DependencyKey {
    let mut binding = Vec::with_capacity(65);
    binding.push(route.kind as u8);
    binding.extend(route.id);
    binding.extend(hash("SEALED_DEPENDENCY_BYTES", bytes));
    DependencyKey {
        kind: route.kind as u8,
        id: hash("SEALED_DEPENDENCY_KEY", &binding),
    }
}

fn identity(tip: Id, anchor: &Anchor, payout: &PayoutRecord, keys: &[DependencyKey]) -> Result<Id> {
    let mut binding = Vec::with_capacity(108 + keys.len() * 33);
    binding.extend(tip);
    anchor.write(&mut binding)?;
    binding.extend(hash(PayoutRecord::DOMAIN, &encode(payout)?));
    binding.extend((keys.len() as u64).to_be_bytes());
    for key in keys {
        binding.push(key.kind);
        binding.extend(key.id);
    }
    Ok(hash("VERIFIED_CLOSURE", &binding))
}

impl VerifiedClosure {
    pub(super) fn from_facts(
        evaluation: &Evaluation,
        facts: &Facts,
        parent: Option<&Self>,
    ) -> Result<Self> {
        let mut sealed = parent.map_or_else(OrdMap::new, |parent| parent.facts.clone());
        let mut routes = parent.map_or_else(OrdMap::new, |parent| parent.routes.clone());
        let mut fact_bytes = parent.map_or(0, |parent| parent.fact_bytes);
        let mut selected = BTreeSet::new();
        for route in &evaluation.required {
            let bytes = facts.loaded.get(route).ok_or(Error::Unavailable(*route))?;
            let cached = parent.and_then(|parent| {
                let key = *parent.routes.get(route)?;
                Arc::ptr_eq(bytes, &parent.facts[&key].bytes).then_some(key)
            });
            let key = match cached {
                Some(key) => key,
                None => {
                    Key::checked(route.kind, route.id, bytes)?;
                    bound_key(*route, bytes)
                }
            };
            if !selected.insert(key) {
                return Err(Error::Invalid("sealed dependency collision"));
            }
            if cached.is_none() {
                if sealed.get(&key).is_some_and(|fact| fact.route != *route) {
                    return Err(Error::Invalid("sealed dependency collision"));
                }
                if let Some(previous) = sealed.insert(
                    key,
                    SealedFact {
                        route: *route,
                        bytes: bytes.clone(),
                    },
                ) {
                    fact_bytes -= previous.bytes.len() + 640;
                }
                fact_bytes = fact_bytes
                    .checked_add(bytes.len())
                    .and_then(|sum| sum.checked_add(640))
                    .ok_or(Error::Capacity("sealed dependency retained bytes"))?;
                routes.insert(*route, key);
            }
        }
        let keys: Vec<_> = selected.iter().copied().collect();
        let logical_id = identity(
            evaluation.tip,
            &evaluation.anchor,
            &evaluation.financial.record,
            &keys,
        )?;
        if let Some(previous) = parent.filter(|parent| parent.logical_id == logical_id) {
            return Ok(previous.clone());
        }
        let added: Vec<_> = keys
            .iter()
            .copied()
            .filter(|key| parent.is_none_or(|parent| !parent.facts.contains_key(key)))
            .collect();
        let removed = parent.map_or_else(Vec::new, |parent| {
            parent
                .keys
                .iter()
                .copied()
                .filter(|key| !selected.contains(key))
                .collect()
        });
        for key in &removed {
            let removed = sealed.remove(key).expect("previous sealed fact");
            fact_bytes -= removed.bytes.len() + 640;
            if !evaluation.required.contains(&removed.route) {
                routes.remove(&removed.route);
            }
        }
        let mut derivation = logical_id.to_vec();
        derivation.push(u8::from(parent.is_some()));
        derivation.extend(parent.map_or([0; 32], |parent| parent.id));
        for delta in [&added, &removed] {
            derivation.extend((delta.len() as u64).to_be_bytes());
            for key in delta {
                derivation.push(key.kind);
                derivation.extend(key.id);
            }
        }
        let id = hash("VERIFIED_CLOSURE_MANIFEST", &derivation);
        let retained_bytes = std::mem::size_of::<Self>()
            .saturating_add(fact_bytes)
            .saturating_add(
                keys.capacity()
                    .saturating_add(added.capacity())
                    .saturating_add(removed.capacity())
                    .saturating_mul(std::mem::size_of::<DependencyKey>()),
            );
        Ok(Self {
            tip: evaluation.tip,
            id,
            logical_id,
            keys,
            facts: sealed,
            routes,
            parent: parent.map(|parent| parent.id),
            added,
            removed,
            fact_bytes,
            retained_bytes,
        })
    }

    pub fn id(&self) -> Id {
        self.id
    }
    pub fn tip(&self) -> Id {
        self.tip
    }
    pub fn keys(&self) -> &[DependencyKey] {
        &self.keys
    }
    pub fn added(&self) -> &[DependencyKey] {
        &self.added
    }
    pub fn removed(&self) -> &[DependencyKey] {
        &self.removed
    }
    pub fn retained_bytes(&self) -> usize {
        self.retained_bytes
    }
    pub fn encoded_delta_bytes(&self) -> usize {
        self.added.iter().fold(0usize, |sum, key| {
            sum.saturating_add(PAYLOAD_HEADER)
                .saturating_add(self.facts[key].bytes.len())
                .saturating_add(std::mem::size_of::<(DependencyKey, Vec<u8>)>())
                .saturating_add(std::mem::size_of::<DependencyObject<'_>>())
        })
    }
    pub fn manifest(&self) -> DependencyManifest<'_> {
        DependencyManifest {
            id: self.id,
            parent: self.parent,
            added: &self.added,
            removed: &self.removed,
        }
    }
    pub fn dependency_objects(&self) -> ClosureObjects {
        let payloads = self
            .added
            .iter()
            .map(|key| {
                let fact = &self.facts[key];
                let mut bytes = Vec::with_capacity(PAYLOAD_HEADER + fact.bytes.len());
                bytes.extend(PAYLOAD_MAGIC);
                bytes.push(fact.route.kind as u8);
                bytes.extend(fact.route.id);
                bytes.extend((fact.bytes.len() as u32).to_be_bytes());
                bytes.extend(fact.bytes.as_ref());
                (*key, bytes)
            })
            .collect();
        ClosureObjects { payloads }
    }
}

pub fn decode_dependency(key: DependencyKey, payload: &[u8]) -> Result<(Key, &[u8])> {
    if payload.len() < PAYLOAD_HEADER || payload[..4] != PAYLOAD_MAGIC[..] {
        return Err(Error::Invalid("sealed dependency encoding"));
    }
    let kind = Kind::from_byte(payload[4])?;
    if payload.len() > PAYLOAD_HEADER + kind.ceiling() {
        return Err(Error::Invalid("sealed dependency ceiling"));
    }
    let route = Key::new(kind, payload[5..37].try_into().unwrap());
    let length = u32::from_be_bytes(payload[37..41].try_into().unwrap()) as usize;
    let bytes = &payload[PAYLOAD_HEADER..];
    if length != bytes.len() {
        return Err(Error::Invalid("sealed dependency length"));
    }
    Key::checked(route.kind, route.id, bytes)?;
    if bound_key(route, bytes) != key {
        return Err(Error::Invalid("sealed dependency binding"));
    }
    Ok((route, bytes))
}

#[cfg(test)]
#[path = "closure_tests.rs"]
mod tests;
