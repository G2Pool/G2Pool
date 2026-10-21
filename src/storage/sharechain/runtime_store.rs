use crate::storage::sharechain::dependencies::DependencyDb;
use crate::storage::sharechain::{
    DependencyBatch, DependencyKey, DependencyManifest, DependencyObject, Error, Id, Result,
};
use heed::{types::Bytes, Database, Env, RoTxn, RwTxn, WithoutTls};
use std::collections::BTreeSet;

const OBJECT_HEADER_BYTES: usize = 1 + 4 + 32;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RoutingKey {
    pub kind: u8,
    pub id: Id,
}
impl RoutingKey {
    fn bytes(self) -> Vec<u8> {
        let mut v = vec![self.kind];
        v.extend(self.id);
        v
    }
    fn decode(v: &[u8]) -> Result<Self> {
        if v.len() != 33 {
            return Err(Error::Invalid("routing key"));
        }
        Ok(Self {
            kind: v[0],
            id: v[1..].try_into().unwrap(),
        })
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ObjectMode {
    Immutable,
    WorkVariant,
}
pub struct RuntimeObject<'a> {
    pub key: RoutingKey,
    pub mode: ObjectMode,
    pub logical_digest: Id,
    pub format: u32,
    pub bytes: &'a [u8],
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StoredObject {
    pub digest: Id,
    pub logical_digest: Id,
    pub format: u32,
    pub seen: u64,
    pub bytes: Vec<u8>,
}
pub struct RuntimeReplacement<'a> {
    pub key: RoutingKey,
    pub logical_digest: Id,
    pub expected_digest: Id,
    pub expected_format: u32,
    pub format: u32,
    pub bytes: &'a [u8],
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RuntimeVariant {
    pub digest: Id,
    pub format: u32,
    pub seen: u64,
    pub byte_len: u64,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RuntimeLimits {
    pub max_objects: u64,
    pub max_bytes: u64,
    pub max_object_bytes: u64,
    pub max_lease_keys: u64,
    pub max_head_keys: u64,
    pub max_heads: u64,
}
impl RuntimeLimits {
    fn encode(self) -> Vec<u8> {
        [
            self.max_objects,
            self.max_bytes,
            self.max_object_bytes,
            self.max_lease_keys,
            self.max_head_keys,
            self.max_heads,
        ]
        .into_iter()
        .flat_map(u64::to_be_bytes)
        .collect()
    }

    fn decode(bytes: &[u8]) -> Result<Self> {
        if bytes.len() != 6 * 8 {
            return Err(Error::Invalid("runtime limits"));
        }
        let mut values = [0u64; 6];
        for (index, value) in values.iter_mut().enumerate() {
            let start = index * 8;
            *value = u64::from_be_bytes(
                bytes[start..start + 8]
                    .try_into()
                    .map_err(|_| Error::Invalid("runtime limits"))?,
            );
            if *value == 0 {
                return Err(Error::Invalid("runtime zero limit"));
            }
        }
        Ok(Self {
            max_objects: values[0],
            max_bytes: values[1],
            max_object_bytes: values[2],
            max_lease_keys: values[3],
            max_head_keys: values[4],
            max_heads: values[5],
        })
    }

    fn can_grow_from(self, previous: Self) -> bool {
        self.max_objects >= previous.max_objects
            && self.max_bytes >= previous.max_bytes
            && self.max_object_bytes >= previous.max_object_bytes
            && self.max_lease_keys >= previous.max_lease_keys
            && self.max_head_keys >= previous.max_head_keys
            && self.max_heads >= previous.max_heads
    }
}
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct RuntimeRevision {
    pub generation: u64,
    pub anchor_height: u64,
    pub anchor_hash: Id,
    pub tip: Option<Id>,
}
impl RuntimeRevision {
    fn encode(self) -> Vec<u8> {
        let mut v = Vec::new();
        v.extend(self.generation.to_be_bytes());
        v.extend(self.anchor_height.to_be_bytes());
        v.extend(self.anchor_hash);
        v.push(self.tip.is_some() as u8);
        v.extend(self.tip.unwrap_or([0; 32]));
        v
    }
    fn decode(v: &[u8]) -> Result<Self> {
        if v.len() != 81 || v[48] > 1 || (v[48] == 0 && v[49..] != [0; 32]) {
            return Err(Error::Invalid("runtime revision"));
        }
        Ok(Self {
            generation: u64::from_be_bytes(v[..8].try_into().unwrap()),
            anchor_height: u64::from_be_bytes(v[8..16].try_into().unwrap()),
            anchor_hash: v[16..48].try_into().unwrap(),
            tip: (v[48] == 1).then(|| v[49..].try_into().unwrap()),
        })
    }
}
pub struct RuntimeHead<'a> {
    pub id: Id,
    pub binding: Id,
    pub metadata: &'a [u8],
    pub parent: Option<Id>,
    pub added: &'a [RoutingKey],
    pub removed: &'a [RoutingKey],
}
pub struct RuntimeLease<'a> {
    pub id: Id,
    pub keys: &'a [RoutingKey],
}
pub type LeaseInfo = (Id, u64, Vec<RoutingKey>);
#[derive(Default)]
pub struct RuntimeBatch<'a> {
    pub objects: &'a [RuntimeObject<'a>],
    pub replacements: &'a [RuntimeReplacement<'a>],
    pub leases: &'a [RuntimeLease<'a>],
    pub release_leases: &'a [Id],
    pub heads: &'a [RuntimeHead<'a>],
    pub release_heads: &'a [Id],
    pub seals: &'a [(RoutingKey, Id)],
    pub selection: Option<(RuntimeRevision, RuntimeRevision)>,
    pub expect_revision: Option<RuntimeRevision>,
    pub lease_key_limit: Option<u64>,
}
#[derive(Clone)]
pub struct RuntimeStore {
    env: Env<WithoutTls>,
    budget: super::budget::DatabaseBudget,
    db: Database<Bytes, Bytes>,
    limits: RuntimeLimits,
    trie: DependencyDb,
}
#[derive(Clone)]
pub struct RuntimeSnapshot {
    store: RuntimeStore,
    txn: std::sync::Arc<std::sync::Mutex<RoTxn<'static, WithoutTls>>>,
}
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct RuntimeGc {
    pub examined: usize,
    pub removed: usize,
}

fn key(prefix: u8, tail: &[u8]) -> Vec<u8> {
    let mut v = vec![prefix];
    v.extend(tail);
    v
}
fn digest(v: &[u8]) -> Id {
    blake2::blake2b::blake2b(32, &[], v)
        .as_bytes()
        .try_into()
        .unwrap()
}
fn num(v: Option<&[u8]>) -> Result<u64> {
    match v {
        None => Ok(0),
        Some(v) if v.len() == 8 => Ok(u64::from_be_bytes(v.try_into().unwrap())),
        _ => Err(Error::Invalid("runtime counter")),
    }
}
impl RuntimeStore {
    pub(crate) fn open(
        env: Env<WithoutTls>,
        limits: RuntimeLimits,
        budget: super::budget::DatabaseBudget,
    ) -> Result<Self> {
        if limits.encode().as_chunks::<8>().0.contains(&[0; 8]) {
            return Err(Error::Invalid("runtime zero limit"));
        }
        let mut tx = env.write_txn()?;
        let previous_bytes = budget.used(&tx)?;
        let db: Database<Bytes, Bytes> = env.create_database(&mut tx, Some("runtime_atomic_v1"))?;
        match db.get(&tx, b"SCHEMA")? {
            None => db.put(&mut tx, b"SCHEMA", b"runtime-atomic")?,
            Some(b"runtime-atomic") => {}
            _ => return Err(Error::Invalid("runtime schema")),
        }
        match db.get(&tx, b"LIMITS")? {
            None => db.put(&mut tx, b"LIMITS", &limits.encode())?,
            Some(v) => {
                let previous = RuntimeLimits::decode(v)?;
                if previous != limits {
                    if !limits.can_grow_from(previous) {
                        return Err(Error::Invalid("runtime limits can only grow"));
                    }
                    db.put(&mut tx, b"LIMITS", &limits.encode())?;
                }
            }
        }
        let trie = DependencyDb::open_runtime(&env, &mut tx)?;
        budget.check(&tx, previous_bytes, 0)?;
        tx.commit()?;
        Ok(Self {
            env,
            budget,
            db,
            limits,
            trie,
        })
    }
    pub fn snapshot(&self) -> Result<RuntimeSnapshot> {
        Ok(RuntimeSnapshot {
            store: self.clone(),
            txn: std::sync::Arc::new(std::sync::Mutex::new(self.env.clone().static_read_txn()?)),
        })
    }
    fn revision(&self, tx: &RoTxn<'_, WithoutTls>) -> Result<RuntimeRevision> {
        self.db
            .get(tx, b"REVISION")?
            .map(RuntimeRevision::decode)
            .transpose()
            .map(|v| v.unwrap_or_default())
    }
    fn seen(&self, tx: &RoTxn<'_, WithoutTls>, physical: &[u8]) -> Result<u64> {
        num(Some(
            self.db
                .get(tx, &key(b't', physical))?
                .ok_or(Error::Missing)?,
        ))
    }
    fn expire(&self, tx: &mut RwTxn<'_>, now: u64) -> Result<()> {
        let mut expired = Vec::new();
        for row in self.db.prefix_iter(tx, b"l")? {
            let (k, v) = row?;
            if v.len() < 8 {
                return Err(Error::Invalid("runtime lease"));
            }
            if num(Some(&v[..8]))? <= now {
                expired.push(k.to_vec());
            }
        }
        for k in expired {
            self.db.delete(tx, &k)?;
        }
        Ok(())
    }
    fn protected(
        &self,
        tx: &RoTxn<'_, WithoutTls>,
        routing: RoutingKey,
        now: u64,
        variant: Id,
    ) -> Result<bool> {
        if self.trie.protected_key(
            tx,
            DependencyKey {
                kind: routing.kind,
                id: routing.id,
            },
        )? && self
            .db
            .get(tx, &key(b's', &routing.bytes()))?
            .is_some_and(|seal| seal == variant)
        {
            return Ok(true);
        }
        for row in self.db.prefix_iter(tx, b"l")? {
            let (_, v) = row?;
            if v.len() < 8 || (v.len() - 8) % 33 != 0 {
                return Err(Error::Invalid("runtime lease"));
            }
            if num(Some(&v[..8]))? > now
                && v[8..]
                    .as_chunks::<33>()
                    .0
                    .iter()
                    .any(|k| k.as_slice() == routing.bytes())
            {
                return Ok(true);
            }
        }
        Ok(false)
    }
    pub fn apply(&self, now: u64, batch: RuntimeBatch<'_>) -> Result<RuntimeRevision> {
        self.apply_with(now, batch, |_| Ok(()))
    }

    pub fn retire_head(&self, now: u64, expected: RuntimeRevision, id: Id) -> Result<()> {
        let mut tx = self.env.write_txn()?;
        self.apply_in(
            &mut tx,
            now,
            RuntimeBatch {
                release_heads: &[id],
                expect_revision: Some(expected),
                ..Default::default()
            },
            false,
        )?;
        tx.commit()?;
        Ok(())
    }

    pub(crate) fn apply_with(
        &self,
        now: u64,
        batch: RuntimeBatch<'_>,
        finish: impl FnOnce(&mut RwTxn<'_>) -> Result<()>,
    ) -> Result<RuntimeRevision> {
        let mut tx = self.env.write_txn()?;
        let previous_bytes = self.budget.used(&tx)?;
        let next = self.apply_in(&mut tx, now, batch, false)?;
        finish(&mut tx)?;
        self.budget.check(&tx, previous_bytes, 0)?;
        tx.commit()?;
        Ok(next)
    }
    pub(crate) fn apply_in(
        &self,
        tx: &mut RwTxn<'_>,
        now: u64,
        batch: RuntimeBatch<'_>,
        solution: bool,
    ) -> Result<RuntimeRevision> {
        let max_objects = self.limits.max_objects.saturating_add(if solution {
            super::budget::SOLUTION_OBJECTS
        } else {
            0
        });
        let max_bytes =
            self.limits
                .max_bytes
                .saturating_add(if solution { super::WRITE_RESERVE } else { 0 });
        if batch.objects.len() as u64 > max_objects
            || batch.replacements.len() as u64 > self.limits.max_objects
            || batch.seals.len() as u64 > max_objects
            || batch.leases.len() > 8
            || batch.release_leases.len() > 8
            || batch.heads.len() as u64 > self.limits.max_heads
            || batch.release_heads.len() as u64 > self.limits.max_heads
            || batch
                .leases
                .iter()
                .any(|l| l.keys.len() as u64 > self.limits.max_lease_keys)
            || batch.heads.iter().any(|h| {
                h.added.len() as u64 > self.limits.max_head_keys
                    || h.removed.len() as u64 > self.limits.max_head_keys
            })
        {
            return Err(Error::Quota("runtime batch quota"));
        }
        let previous = self.revision(tx)?;
        if batch
            .expect_revision
            .is_some_and(|expected| expected != previous)
        {
            return Err(Error::Stale);
        }
        let lease_limit = batch.lease_key_limit.unwrap_or(self.limits.max_lease_keys);
        if lease_limit > self.limits.max_lease_keys {
            return Err(Error::Quota("runtime lease limit"));
        }
        if let Some((expected, next)) = batch.selection {
            if expected != previous {
                return Err(Error::Stale);
            }
            if next.generation
                != previous
                    .generation
                    .checked_add(1)
                    .ok_or(Error::Invalid("runtime generation overflow"))?
            {
                return Err(Error::Invalid("runtime generation"));
            }
        }
        self.expire(tx, now)?;
        let mut count = num(self.db.get(tx, b"COUNT")?)?;
        let mut bytes = num(self.db.get(tx, b"BYTES")?)?;
        let previous_object_bytes = bytes;
        for object in batch.objects {
            if object.bytes.len() as u64 > self.limits.max_object_bytes {
                return Err(Error::Quota("runtime object quota"));
            }
            let d = digest(object.bytes);
            let rk = object.key.bytes();
            let mk = key(b'm', &rk);
            let mode = match object.mode {
                ObjectMode::Immutable => 0,
                ObjectMode::WorkVariant => 1,
            };
            if let Some(old) = self.db.get(tx, &mk)? {
                if old != [mode] {
                    return Err(Error::Invalid("runtime object mode"));
                }
            }
            let mut physical = rk.clone();
            physical.extend(d);
            let pk = key(b'o', &physical);
            let mut value = vec![1];
            value.extend(object.format.to_be_bytes());
            value.extend(d);
            value.extend(object.bytes);
            if let Some(old) = self.db.get(tx, &pk)? {
                if old.len() < OBJECT_HEADER_BYTES
                    || old[1..5] != value[1..5]
                    || old[OBJECT_HEADER_BYTES..] != value[OBJECT_HEADER_BYTES..]
                {
                    return Err(Error::Invalid("runtime physical collision"));
                }
                if mode == 1 {
                    self.db.put(tx, &key(b'p', &rk), &d)?;
                }
                continue;
            }
            if mode == 0 {
                if object.logical_digest != d {
                    return Err(Error::Invalid("runtime canonical digest"));
                }
                let identity = key(b'i', &rk);
                if self
                    .db
                    .get(tx, &identity)?
                    .is_some_and(|old| old != object.logical_digest)
                {
                    return Err(Error::Invalid("runtime immutable collision"));
                }
                self.db.put(tx, &identity, &object.logical_digest)?;
                let sk = key(b's', &rk);
                if let Some(old) = self.db.get(tx, &sk)? {
                    if old != d {
                        continue;
                    }
                }
                self.db.put(tx, &sk, &d)?;
            }
            count = count
                .checked_add(1)
                .ok_or(Error::Invalid("runtime count overflow"))?;
            bytes = bytes
                .checked_add(value.len() as u64)
                .ok_or(Error::Invalid("runtime bytes overflow"))?;
            if count > max_objects || bytes > max_bytes {
                return Err(Error::Quota("runtime object quota"));
            }
            self.db.put(tx, &mk, &[mode])?;
            self.db.put(tx, &pk, &value)?;
            self.db.put(tx, &key(b't', &physical), &now.to_be_bytes())?;
            self.db.put(tx, &key(b'p', &rk), &d)?;
        }
        for replacement in batch.replacements {
            let route = replacement.key.bytes();
            let seal = key(b's', &route);
            if self.db.get(tx, &key(b'm', &route))? != Some(&[0][..]) {
                return Err(Error::Invalid("runtime replacement mode"));
            }
            if self.db.get(tx, &key(b'i', &route))? != Some(replacement.logical_digest.as_slice()) {
                return Err(Error::Invalid("runtime replacement canonical digest"));
            }
            if self.db.get(tx, &seal)? != Some(replacement.expected_digest.as_slice()) {
                return Err(Error::Stale);
            }
            let mut old_key = key(b'o', &route);
            old_key.extend(replacement.expected_digest);
            let old = self.db.get(tx, &old_key)?.ok_or(Error::Missing)?;
            if old.len() < OBJECT_HEADER_BYTES
                || old[0] != 1
                || old[5..OBJECT_HEADER_BYTES] != replacement.expected_digest
            {
                return Err(Error::Invalid("runtime replacement encoding"));
            }
            if u32::from_be_bytes(old[1..5].try_into().unwrap()) != replacement.expected_format {
                return Err(Error::Stale);
            }
            if replacement.bytes.len() as u64 > self.limits.max_object_bytes {
                return Err(Error::Quota("runtime object quota"));
            }
            let seen = self.seen(tx, &old_key[1..])?;
            let old_len = old.len() as u64;
            let physical = digest(replacement.bytes);
            let mut value = vec![1];
            value.extend(replacement.format.to_be_bytes());
            value.extend(physical);
            value.extend(replacement.bytes);
            let mut new_key = key(b'o', &route);
            new_key.extend(physical);
            if new_key != old_key && self.db.get(tx, &new_key)?.is_some() {
                return Err(Error::Invalid("runtime replacement already staged"));
            }
            bytes = bytes
                .checked_sub(old_len)
                .and_then(|b| b.checked_add(value.len() as u64))
                .ok_or(Error::Invalid("runtime replacement bytes overflow"))?;
            if bytes > max_bytes && bytes > previous_object_bytes {
                return Err(Error::Quota("runtime object quota"));
            }
            self.db.delete(tx, &old_key)?;
            self.db.delete(tx, &key(b't', &old_key[1..]))?;
            self.db.put(tx, &new_key, &value)?;
            self.db
                .put(tx, &key(b't', &new_key[1..]), &seen.to_be_bytes())?;
            self.db.put(tx, &seal, &physical)?;
            self.db.put(tx, &key(b'p', &route), &physical)?;
        }
        for id in batch.release_leases {
            self.db.delete(tx, &key(b'l', id))?;
        }
        for lease in batch.leases {
            let mut keys: BTreeSet<_> = lease.keys.iter().copied().collect();
            if keys.len() != lease.keys.len() {
                return Err(Error::Invalid("duplicate lease key"));
            }
            let lk = key(b'l', &lease.id);
            let deadline = if let Some(old) = self.db.get(tx, &lk)? {
                if old.len() < 8 || (old.len() - 8) % 33 != 0 {
                    return Err(Error::Invalid("runtime lease"));
                }
                for k in old[8..].as_chunks::<33>().0 {
                    keys.insert(RoutingKey::decode(k)?);
                }
                num(Some(&old[..8]))?
            } else {
                now.checked_add(super::ACQUISITION_LIFETIME)
                    .ok_or(Error::Invalid("runtime deadline overflow"))?
            };
            if keys.len() as u64 > lease_limit {
                return Err(Error::Quota("runtime lease quota"));
            }
            let mut value = deadline.to_be_bytes().to_vec();
            for k in keys {
                value.extend(k.bytes());
            }
            self.db.put(tx, &lk, &value)?;
        }
        let mut lease_count = 0u64;
        let mut leased_memberships = 0u64;
        for row in self.db.prefix_iter(tx, b"l")? {
            let (_, v) = row?;
            lease_count = lease_count
                .checked_add(1)
                .ok_or(Error::Invalid("runtime lease count overflow"))?;
            if v.len() < 8 || (v.len() - 8) % 33 != 0 {
                return Err(Error::Invalid("runtime lease"));
            }
            leased_memberships = leased_memberships
                .checked_add(((v.len() - 8) / 33) as u64)
                .ok_or(Error::Invalid("runtime lease membership overflow"))?;
        }
        if lease_count > 8 || leased_memberships > lease_limit {
            return Err(Error::Quota("runtime lease quota"));
        }
        for (routing, d) in batch.seals {
            let mut p = routing.bytes();
            p.extend(d);
            if self.db.get(tx, &key(b'o', &p))?.is_none() {
                return Err(Error::Missing);
            }
            let sk = key(b's', &routing.bytes());
            if let Some(old) = self.db.get(tx, &sk)? {
                if old != d {
                    if old.len() != 32 {
                        return Err(Error::Invalid("runtime seal"));
                    }
                    let mut old_physical = routing.bytes();
                    old_physical.extend(old);
                    if self.db.get(tx, &key(b'o', &old_physical))?.is_none() {
                        return Err(Error::Missing);
                    }
                    if self.db.get(tx, &key(b'm', &routing.bytes()))? != Some(&[1][..])
                        || self.trie.protected_key(
                            tx,
                            DependencyKey {
                                kind: routing.kind,
                                id: routing.id,
                            },
                        )?
                    {
                        return Err(Error::Invalid("runtime sealed variant"));
                    }
                }
            }
            self.db.put(tx, &sk, d)?;
        }
        let mut released: BTreeSet<RoutingKey> = batch
            .heads
            .iter()
            .flat_map(|head| head.removed.iter().copied())
            .collect();
        for head in batch.heads {
            if head.metadata.len() > 4096 {
                return Err(Error::Invalid("runtime head metadata"));
            }
            let old_binding = self
                .db
                .get(tx, &key(b'h', &head.id))?
                .map(|v| {
                    if v.len() != 40 {
                        return Err(Error::Invalid("runtime head"));
                    }
                    Ok::<Id, Error>(v[8..].try_into().unwrap())
                })
                .transpose()?;
            for row in self.db.prefix_iter(tx, b"h")? {
                let (id, v) = row?;
                if v.len() != 40 {
                    return Err(Error::Invalid("runtime head"));
                }
                if id[1..] != head.id && v[8..] == head.binding {
                    return Err(Error::Invalid("runtime shared head binding"));
                }
            }
            let (parent_count, parent_binding) = match head.parent {
                Some(parent) => {
                    let value = self
                        .db
                        .get(tx, &key(b'h', &parent))?
                        .ok_or(Error::Missing)?;
                    if value.len() != 40 {
                        return Err(Error::Invalid("runtime head"));
                    }
                    (
                        num(Some(&value[..8]))?,
                        Some(value[8..].try_into().unwrap()),
                    )
                }
                None => (0, None),
            };
            let size = parent_count
                .checked_sub(head.removed.len() as u64)
                .and_then(|n| n.checked_add(head.added.len() as u64))
                .ok_or(Error::Invalid("runtime membership size"))?;
            if size > self.limits.max_head_keys {
                return Err(Error::Quota("runtime head quota"));
            }
            let added: Vec<_> = head
                .added
                .iter()
                .map(|k| DependencyKey {
                    kind: k.kind,
                    id: k.id,
                })
                .collect();
            let removed: Vec<_> = head
                .removed
                .iter()
                .map(|k| DependencyKey {
                    kind: k.kind,
                    id: k.id,
                })
                .collect();
            for k in head.added {
                if self.db.get(tx, &key(b's', &k.bytes()))?.is_none() {
                    return Err(Error::Missing);
                }
            }
            let objects: Vec<_> = added
                .iter()
                .map(|k| DependencyObject {
                    key: *k,
                    bytes: b"runtime-membership",
                })
                .collect();
            self.trie.apply(
                tx,
                &DependencyBatch {
                    objects: &objects,
                    manifests: &[DependencyManifest {
                        id: head.binding,
                        parent: parent_binding,
                        added: &added,
                        removed: &removed,
                    }],
                    ..Default::default()
                },
                usize::MAX / 64,
            )?;
            if old_binding.is_some_and(|old| old != head.binding) {
                released.extend(
                    self.trie
                        .resolve(tx, &old_binding.unwrap())?
                        .into_iter()
                        .map(|k| RoutingKey {
                            kind: k.kind,
                            id: k.id,
                        }),
                );
                self.trie.apply(
                    tx,
                    &DependencyBatch {
                        release: &[old_binding.unwrap()],
                        ..Default::default()
                    },
                    usize::MAX / 64,
                )?;
            }
            let mut value = size.to_be_bytes().to_vec();
            value.extend(head.binding);
            self.db.put(tx, &key(b'h', &head.id), &value)?;
            self.db.put(tx, &key(b'j', &head.id), head.metadata)?;
        }
        for id in batch.release_heads {
            if let Some(value) = self.db.get(tx, &key(b'h', id))? {
                if value.len() != 40 {
                    return Err(Error::Invalid("runtime head"));
                }
                let binding: Id = value[8..].try_into().unwrap();
                released.extend(
                    self.trie
                        .resolve(tx, &binding)?
                        .into_iter()
                        .map(|k| RoutingKey {
                            kind: k.kind,
                            id: k.id,
                        }),
                );
                self.trie.apply(
                    tx,
                    &DependencyBatch {
                        release: &[binding],
                        ..Default::default()
                    },
                    usize::MAX / 64,
                )?;
                self.db.delete(tx, &key(b'h', id))?;
                self.db.delete(tx, &key(b'j', id))?;
            }
        }
        let mut kept = BTreeSet::new();
        for head in batch.heads {
            kept.extend(
                self.trie
                    .resolve(tx, &head.binding)?
                    .into_iter()
                    .map(|k| RoutingKey {
                        kind: k.kind,
                        id: k.id,
                    }),
            );
        }
        for routing in released.difference(&kept).copied() {
            let Some(seal) = self.db.get(tx, &key(b's', &routing.bytes()))? else {
                continue;
            };
            let mut physical = routing.bytes();
            physical.extend(seal);
            let Some(value) = self.db.get(tx, &key(b'o', &physical))? else {
                return Err(Error::Missing);
            };
            if value.len() < OBJECT_HEADER_BYTES {
                return Err(Error::Invalid("runtime physical encoding"));
            }
            let released_at = now.max(self.seen(tx, &physical)?);
            self.db
                .put(tx, &key(b't', &physical), &released_at.to_be_bytes())?;
        }
        let mut heads = 0u64;
        for row in self.db.prefix_iter(tx, b"h")? {
            row?;
            heads = heads
                .checked_add(1)
                .ok_or(Error::Invalid("runtime heads overflow"))?;
        }
        if heads > self.limits.max_heads {
            return Err(Error::Quota("runtime head quota"));
        }
        let next = batch.selection.map(|(_, n)| n).unwrap_or(previous);
        if let Some(tip) = next.tip {
            if self.db.get(tx, &key(b'h', &tip))?.is_none() {
                return Err(Error::Missing);
            }
        }
        self.db.put(tx, b"COUNT", &count.to_be_bytes())?;
        self.db.put(tx, b"BYTES", &bytes.to_be_bytes())?;
        self.db.put(tx, b"REVISION", &next.encode())?;
        Ok(next)
    }
    pub(crate) fn retain_in(&self, tx: &mut RwTxn<'_>, id: Id, keys: &[RoutingKey]) -> Result<()> {
        if keys.len() as u64 > self.limits.max_lease_keys {
            return Err(Error::Quota("retained dependency keys"));
        }
        let parent = self
            .revision(tx)?
            .tip
            .map(|tip| {
                let head = self.db.get(tx, &key(b'h', &tip))?.ok_or(Error::Missing)?;
                if head.len() != 40 {
                    return Err(Error::Invalid("runtime head"));
                }
                Ok::<Id, Error>(head[8..].try_into().unwrap())
            })
            .transpose()?;
        let previous: BTreeSet<_> = parent
            .map(|parent| self.trie.resolve(tx, &parent))
            .transpose()?
            .unwrap_or_default()
            .into_iter()
            .collect();
        let required: BTreeSet<_> = keys
            .iter()
            .map(|key| DependencyKey {
                kind: key.kind,
                id: key.id,
            })
            .collect();
        let added: Vec<_> = required.difference(&previous).copied().collect();
        let removed: Vec<_> = previous.difference(&required).copied().collect();
        for key in keys {
            let seal = self
                .db
                .get(tx, &self::key(b's', &key.bytes()))?
                .ok_or(Error::Missing)?;
            let mut physical = key.bytes();
            physical.extend(seal);
            if self.db.get(tx, &self::key(b'o', &physical))?.is_none() {
                return Err(Error::Missing);
            }
        }
        let objects: Vec<_> = added
            .iter()
            .map(|key| DependencyObject {
                key: *key,
                bytes: b"runtime-membership",
            })
            .collect();
        self.trie.apply(
            tx,
            &DependencyBatch {
                objects: &objects,
                manifests: &[DependencyManifest {
                    id,
                    parent,
                    added: &added,
                    removed: &removed,
                }],
                ..Default::default()
            },
            usize::MAX / 64,
        )
    }
    pub(crate) fn release_in(&self, tx: &mut RwTxn<'_>, id: Id) -> Result<()> {
        self.trie.apply(
            tx,
            &DependencyBatch {
                release: &[id],
                ..Default::default()
            },
            usize::MAX / 64,
        )
    }
    pub(crate) fn retained_in(&self, tx: &RoTxn<'_, WithoutTls>, id: Id) -> Result<bool> {
        self.trie.retained(tx, &id)
    }

    pub fn collect_keep(
        &self,
        now: u64,
        min_age: u64,
        limit: usize,
        keep: &[RoutingKey],
    ) -> Result<RuntimeGc> {
        if keep.len() as u64 > self.limits.max_head_keys {
            return Err(Error::Quota("runtime GC keep quota"));
        }
        let keep: BTreeSet<_> = keep.iter().copied().collect();
        use std::ops::Bound;
        if limit == 0 {
            return Ok(RuntimeGc::default());
        }
        let mut tx = self.env.write_txn()?;
        self.expire(&mut tx, now)?;
        self.trie.drain_gc(&mut tx, usize::MAX)?;
        let cursor = self.db.get(&tx, b"GC-CURSOR")?.map(ToOwned::to_owned);
        let start = cursor
            .as_deref()
            .map_or(Bound::Included(b"o".as_slice()), Bound::Excluded);
        let rows = self
            .db
            .range(&tx, &(start, Bound::Excluded(b"p".as_slice())))?
            .take(limit)
            .map(|r| {
                r.map(|(k, v)| {
                    (
                        k.to_vec(),
                        v.len(),
                        v[..v.len().min(OBJECT_HEADER_BYTES)].to_vec(),
                    )
                })
            })
            .collect::<std::result::Result<Vec<_>, _>>()?;
        let mut stats = RuntimeGc::default();
        let mut count = num(self.db.get(&tx, b"COUNT")?)?;
        let mut bytes = num(self.db.get(&tx, b"BYTES")?)?;
        for (k, physical_len, v) in &rows {
            stats.examined += 1;
            if k.len() != 66
                || v.len() < OBJECT_HEADER_BYTES
                || v[0] != 1
                || v[5..OBJECT_HEADER_BYTES] != k[34..]
            {
                return Err(Error::Invalid("runtime physical encoding"));
            }
            let routing = RoutingKey::decode(&k[1..34])?;
            let seen = self.seen(&tx, &k[1..])?;
            if now.checked_sub(seen).is_some_and(|age| age >= min_age)
                && !keep.contains(&routing)
                && !self.protected(&tx, routing, now, k[34..].try_into().unwrap())?
            {
                self.db.delete(&mut tx, k)?;
                self.db.delete(&mut tx, &key(b't', &k[1..]))?;
                count = count
                    .checked_sub(1)
                    .ok_or(Error::Invalid("runtime count underflow"))?;
                bytes = bytes
                    .checked_sub(*physical_len as u64)
                    .ok_or(Error::Invalid("runtime bytes underflow"))?;
                stats.removed += 1;
                let preferred = key(b'p', &routing.bytes());
                if self.db.get(&tx, &preferred)?.is_some_and(|d| d == &k[34..]) {
                    self.db.delete(&mut tx, &preferred)?;
                }
                let sk = key(b's', &routing.bytes());
                if self.db.get(&tx, &sk)?.is_some_and(|d| d == &k[34..]) {
                    self.db.delete(&mut tx, &sk)?;
                }
                let object_prefix = key(b'o', &routing.bytes());
                if self
                    .db
                    .prefix_iter(&tx, &object_prefix)?
                    .next()
                    .transpose()?
                    .is_none()
                {
                    self.db.delete(&mut tx, &key(b'm', &routing.bytes()))?;
                    self.db.delete(&mut tx, &key(b'i', &routing.bytes()))?;
                }
            }
        }
        if rows.len() < limit || rows.is_empty() {
            self.db.delete(&mut tx, b"GC-CURSOR")?;
        } else {
            self.db
                .put(&mut tx, b"GC-CURSOR", &rows.last().unwrap().0)?;
        }
        self.db.put(&mut tx, b"COUNT", &count.to_be_bytes())?;
        self.db.put(&mut tx, b"BYTES", &bytes.to_be_bytes())?;
        tx.commit()?;
        Ok(stats)
    }
}
impl RuntimeSnapshot {
    fn read<T>(&self, f: impl FnOnce(RuntimeReader<'_>) -> Result<T>) -> Result<T> {
        let txn = self
            .txn
            .lock()
            .map_err(|_| Error::Invalid("runtime snapshot mutex poisoned"))?;
        f(RuntimeReader {
            store: &self.store,
            txn: &txn,
        })
    }
    pub fn revision(&self) -> Result<RuntimeRevision> {
        self.read(|reader| reader.revision())
    }
    pub fn usage(&self) -> Result<(u64, u64)> {
        self.read(|reader| reader.usage())
    }
    pub fn heads(&self, limit: usize) -> Result<Vec<(Id, Vec<u8>)>> {
        self.read(|reader| reader.heads(limit))
    }
    pub fn leases(&self, limit: usize) -> Result<Vec<LeaseInfo>> {
        self.read(|reader| reader.leases(limit))
    }
    pub fn objects(&self, kind: u8, limit: usize) -> Result<Vec<(RoutingKey, RuntimeVariant)>> {
        self.read(|reader| reader.objects(kind, limit))
    }
    pub fn objects_after(
        &self,
        kind: u8,
        after: Option<(RoutingKey, Id)>,
        limit: usize,
        max_metadata_bytes: usize,
    ) -> Result<Vec<(RoutingKey, RuntimeVariant)>> {
        self.read(|reader| reader.objects_after(kind, after, limit, max_metadata_bytes))
    }
    pub fn object_preferred(&self, routing: RoutingKey) -> Result<Option<StoredObject>> {
        self.read(|reader| reader.object_preferred(routing))
    }
    pub fn variants(&self, routing: RoutingKey, limit: usize) -> Result<Vec<RuntimeVariant>> {
        self.read(|reader| reader.variants(routing, limit))
    }
    pub fn object(&self, routing: RoutingKey, variant: Option<Id>) -> Result<Option<StoredObject>> {
        self.read(|reader| reader.object(routing, variant))
    }
    pub fn head_keys(&self, id: Id) -> Result<Option<Vec<RoutingKey>>> {
        self.read(|reader| reader.head_keys(id))
    }
    pub fn head_binding(&self, id: Id) -> Result<Option<Id>> {
        self.read(|reader| reader.head_binding(id))
    }

    pub fn lease(&self, id: Id) -> Result<Option<(u64, Vec<RoutingKey>)>> {
        self.read(|reader| reader.lease(id))
    }
}
struct RuntimeReader<'a> {
    store: &'a RuntimeStore,
    txn: &'a RoTxn<'static, WithoutTls>,
}
impl RuntimeReader<'_> {
    pub fn revision(&self) -> Result<RuntimeRevision> {
        self.store.revision(self.txn)
    }
    pub fn usage(&self) -> Result<(u64, u64)> {
        Ok((
            num(self.store.db.get(self.txn, b"COUNT")?)?,
            num(self.store.db.get(self.txn, b"BYTES")?)?,
        ))
    }
    pub fn heads(&self, limit: usize) -> Result<Vec<(Id, Vec<u8>)>> {
        self.store
            .db
            .prefix_iter(self.txn, b"h")?
            .take(limit.min(usize::try_from(self.store.limits.max_heads).unwrap_or(usize::MAX)))
            .map(|row| {
                let (k, v) = row?;
                if k.len() != 33 || v.len() != 40 {
                    return Err(Error::Invalid("runtime head"));
                }
                let id: Id = k[1..].try_into().unwrap();
                let metadata = self
                    .store
                    .db
                    .get(self.txn, &key(b'j', &id))?
                    .ok_or(Error::Missing)?;
                if metadata.len() > 4096 {
                    return Err(Error::Invalid("runtime head metadata"));
                }
                Ok((id, metadata.to_vec()))
            })
            .collect()
    }
    pub fn leases(&self, limit: usize) -> Result<Vec<LeaseInfo>> {
        self.store
            .db
            .prefix_iter(self.txn, b"l")?
            .take(limit.min(8))
            .map(|row| {
                let (k, v) = row?;
                if k.len() != 33 || v.len() < 8 || (v.len() - 8) % 33 != 0 {
                    return Err(Error::Invalid("runtime lease"));
                }
                let id: Id = k[1..].try_into().unwrap();
                Ok((
                    id,
                    num(Some(&v[..8]))?,
                    v[8..]
                        .as_chunks::<33>()
                        .0
                        .iter()
                        .map(|k| RoutingKey::decode(k))
                        .collect::<Result<Vec<_>>>()?,
                ))
            })
            .collect()
    }
    pub fn objects(&self, kind: u8, limit: usize) -> Result<Vec<(RoutingKey, RuntimeVariant)>> {
        self.objects_after(kind, None, limit, usize::MAX)
    }
    pub fn objects_after(
        &self,
        kind: u8,
        after: Option<(RoutingKey, Id)>,
        limit: usize,
        max_metadata_bytes: usize,
    ) -> Result<Vec<(RoutingKey, RuntimeVariant)>> {
        use std::ops::Bound;
        let start = match after {
            Some((routing, digest)) => {
                if routing.kind != kind {
                    return Err(Error::Invalid("runtime object cursor kind"));
                }
                let mut cursor = key(b'o', &routing.bytes());
                cursor.extend(digest);
                (cursor, true)
            }
            None => (vec![b'o', kind], false),
        };
        let end = if kind == u8::MAX {
            vec![b'p']
        } else {
            vec![b'o', kind + 1]
        };
        let lower = if start.1 {
            Bound::Excluded(start.0.as_slice())
        } else {
            Bound::Included(start.0.as_slice())
        };
        let bound = limit
            .min(max_metadata_bytes / 85)
            .min(usize::try_from(self.store.limits.max_objects).unwrap_or(usize::MAX));
        self.store
            .db
            .range(self.txn, &(lower, Bound::Excluded(end.as_slice())))?
            .take(bound)
            .map(|row| {
                let (k, v) = row?;
                if k.len() != 66
                    || v.len() < OBJECT_HEADER_BYTES
                    || v[0] != 1
                    || v[5..OBJECT_HEADER_BYTES] != k[34..]
                {
                    return Err(Error::Invalid("runtime physical encoding"));
                }
                Ok((
                    RoutingKey::decode(&k[1..34])?,
                    RuntimeVariant {
                        digest: k[34..].try_into().unwrap(),
                        format: u32::from_be_bytes(v[1..5].try_into().unwrap()),
                        seen: self.store.seen(self.txn, &k[1..])?,
                        byte_len: (v.len() - OBJECT_HEADER_BYTES) as u64,
                    },
                ))
            })
            .collect()
    }
    pub fn object_preferred(&self, routing: RoutingKey) -> Result<Option<StoredObject>> {
        if let Some(object) = self.object(routing, None)? {
            return Ok(Some(object));
        }
        if let Some(value) = self.store.db.get(self.txn, &key(b'p', &routing.bytes()))? {
            if value.len() != 32 {
                return Err(Error::Invalid("runtime preferred variant"));
            }
            return self.object(routing, Some(value.try_into().unwrap()));
        }
        let candidate = self
            .variants(
                routing,
                usize::try_from(self.store.limits.max_objects).unwrap_or(usize::MAX),
            )?
            .into_iter()
            .max_by_key(|v| (v.seen, v.digest));
        match candidate {
            Some(v) => self.object(routing, Some(v.digest)),
            None => Ok(None),
        }
    }
    pub fn variants(&self, routing: RoutingKey, limit: usize) -> Result<Vec<RuntimeVariant>> {
        let bound = limit.min(usize::try_from(self.store.limits.max_objects).unwrap_or(usize::MAX));
        self.store
            .db
            .prefix_iter(self.txn, &key(b'o', &routing.bytes()))?
            .take(bound)
            .map(|row| {
                let (k, v) = row?;
                if k.len() != 66
                    || v.len() < OBJECT_HEADER_BYTES
                    || v[0] != 1
                    || v[5..OBJECT_HEADER_BYTES] != k[34..]
                {
                    return Err(Error::Invalid("runtime physical encoding"));
                }
                Ok(RuntimeVariant {
                    digest: k[34..].try_into().unwrap(),
                    format: u32::from_be_bytes(v[1..5].try_into().unwrap()),
                    seen: self.store.seen(self.txn, &k[1..])?,
                    byte_len: (v.len() - OBJECT_HEADER_BYTES) as u64,
                })
            })
            .collect()
    }
    pub fn object(&self, routing: RoutingKey, variant: Option<Id>) -> Result<Option<StoredObject>> {
        let d = match variant {
            Some(d) => d,
            None => match self.store.db.get(self.txn, &key(b's', &routing.bytes()))? {
                None => return Ok(None),
                Some(d) if d.len() == 32 => d.try_into().unwrap(),
                _ => return Err(Error::Invalid("runtime seal")),
            },
        };
        let logical_digest = match self.store.db.get(self.txn, &key(b'i', &routing.bytes()))? {
            Some(logical) if logical.len() == 32 => logical.try_into().unwrap(),
            Some(_) => return Err(Error::Invalid("runtime canonical digest")),
            None => d,
        };
        let mut p = routing.bytes();
        p.extend(d);
        self.store
            .db
            .get(self.txn, &key(b'o', &p))?
            .map(|v| {
                if v.len() < OBJECT_HEADER_BYTES || v[0] != 1 || v[5..OBJECT_HEADER_BYTES] != d {
                    return Err(Error::Invalid("runtime object"));
                }
                Ok(StoredObject {
                    digest: d,
                    logical_digest,
                    format: u32::from_be_bytes(v[1..5].try_into().unwrap()),
                    seen: self.store.seen(self.txn, &p)?,
                    bytes: v[OBJECT_HEADER_BYTES..].to_vec(),
                })
            })
            .transpose()
    }
    pub fn head_binding(&self, id: Id) -> Result<Option<Id>> {
        self.store
            .db
            .get(self.txn, &key(b'h', &id))?
            .map(|value| {
                if value.len() != 40 {
                    return Err(Error::Invalid("runtime head"));
                }
                Ok(value[8..].try_into().unwrap())
            })
            .transpose()
    }
    pub fn head_keys(&self, id: Id) -> Result<Option<Vec<RoutingKey>>> {
        if self.store.db.get(self.txn, &key(b'h', &id))?.is_none() {
            return Ok(None);
        }
        let value = self
            .store
            .db
            .get(self.txn, &key(b'h', &id))?
            .ok_or(Error::Missing)?;
        if value.len() != 40 {
            return Err(Error::Invalid("runtime head"));
        }
        let binding: Id = value[8..].try_into().unwrap();
        self.store.trie.resolve(self.txn, &binding).map(|keys| {
            Some(
                keys.into_iter()
                    .map(|k| RoutingKey {
                        kind: k.kind,
                        id: k.id,
                    })
                    .collect(),
            )
        })
    }

    pub fn lease(&self, id: Id) -> Result<Option<(u64, Vec<RoutingKey>)>> {
        self.store
            .db
            .get(self.txn, &key(b'l', &id))?
            .map(|v| {
                if v.len() < 8 || (v.len() - 8) % 33 != 0 {
                    return Err(Error::Invalid("runtime lease"));
                }
                Ok((
                    num(Some(&v[..8]))?,
                    v[8..]
                        .as_chunks::<33>()
                        .0
                        .iter()
                        .map(|k| RoutingKey::decode(k))
                        .collect::<Result<Vec<_>>>()?,
                ))
            })
            .transpose()
    }
}

#[cfg(test)]
#[path = "runtime_store_tests.rs"]
mod tests;
