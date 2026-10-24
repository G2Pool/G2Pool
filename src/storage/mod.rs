pub mod compact;
pub mod sharechain;
pub(crate) use crate::storage::sharechain::Representation;
use crate::storage::sharechain::{
    physical_digest, ObjectMode, RoutingKey, RuntimeBatch, RuntimeHead, RuntimeLease,
    RuntimeLimits, RuntimeObject, RuntimeReplacement, RuntimeRevision, RuntimeSnapshot,
    RuntimeStore, StoredObject as PhysicalObject,
};
use crate::{
    codec::*,
    consensus::Evaluation,
    objects::{Key, Kind, Source, StoredObject},
    Error, Result,
};
use std::{
    collections::BTreeSet,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};
mod auxiliary;
const MAX_RUNTIME_LEASE_KEYS: u64 = 1_000_000;
const MAX_RUNTIME_HEAD_KEYS: u64 = 100_000;
#[derive(Debug, Clone)]
pub struct StorageLimits {
    pub bytes: u64,
    pub objects: u64,
    pub heads: usize,
}
impl Default for StorageLimits {
    fn default() -> Self {
        Self {
            bytes: 4 * 1024 * 1024 * 1024,
            objects: 2_000_000,
            heads: 64,
        }
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Revision {
    pub generation: u64,
    pub anchor: Anchor,
    pub tip: Option<Id>,
}
#[derive(Debug, Clone)]
pub struct Head {
    pub tip: Id,
    pub anchor: Anchor,
}
#[derive(Debug, Clone)]
pub struct Object {
    pub key: Key,
    pub bytes: Arc<[u8]>,
}
impl Object {
    pub fn content<T: StoredObject>(value: &T) -> Result<Self> {
        let bytes = encode(value)?;
        Self::new(Key::new(T::KIND, hash(T::DOMAIN, &bytes)), bytes)
    }
    pub fn new(key: Key, bytes: impl Into<Arc<[u8]>>) -> Result<Self> {
        let bytes = bytes.into();
        Key::checked(key.kind, key.id, &bytes)?;
        Ok(Self { key, bytes })
    }
}

pub struct Store {
    database: crate::storage::sharechain::SharechainDb,
    native: Option<(Arc<grin_chain::Chain>, crate::native::Profile)>,
    reader_deadline: Option<std::time::Instant>,
    path: PathBuf,
    runtime: RuntimeStore,
    snapshot: Option<Arc<RuntimeSnapshot>>,
    compaction_cursor: Arc<std::sync::Mutex<Option<(RoutingKey, Id)>>>,
    pool_id: Id,
    limits: StorageLimits,
}
#[derive(Clone)]
pub(crate) struct ReadSource {
    database: crate::storage::sharechain::SharechainDb,
    deadline: std::time::Instant,
    path: PathBuf,
    native: Option<(Arc<grin_chain::Chain>, crate::native::Profile)>,
    pool_id: Id,
    limits: StorageLimits,
    runtime: RuntimeStore,
    snapshot: Arc<RuntimeSnapshot>,
    compaction_cursor: Arc<std::sync::Mutex<Option<(RoutingKey, Id)>>>,
}
impl ReadSource {
    pub(crate) fn open(&self) -> Result<Store> {
        if std::time::Instant::now() >= self.deadline {
            return Err(Error::Capacity("reader lifetime"));
        }
        Ok(Store {
            database: self.database.clone(),
            native: self.native.clone(),
            reader_deadline: Some(self.deadline),
            path: self.path.clone(),
            runtime: self.runtime.clone(),
            snapshot: Some(self.snapshot.clone()),
            compaction_cursor: self.compaction_cursor.clone(),
            pool_id: self.pool_id,
            limits: self.limits.clone(),
        })
    }
}
pub(crate) struct Compaction {
    pool_id: Id,
    anchor: Anchor,
    entries: Vec<(Key, Id, Vec<u8>)>,
    next_cursor: Option<(RoutingKey, Id)>,
}
fn route(key: Key) -> RoutingKey {
    RoutingKey {
        kind: key.kind as u8,
        id: key.id,
    }
}
pub(crate) fn runtime_error(error: crate::storage::sharechain::Error) -> Error {
    use crate::storage::sharechain::Error as E;
    match error {
        E::Stale => Error::View,
        E::Io(error) => Error::Io(error),
        E::Quota(message) => Error::Capacity(message),
        E::Database(heed::Error::Mdb(heed::MdbError::MapFull)) => Error::Capacity("LMDB map full"),
        E::Database(error) => Error::Storage(error),
        E::Invalid(message) => Error::Invalid(message),
        E::Missing => Error::Invalid("missing sharechain object"),
    }
}
fn runtime_revision(revision: &Revision) -> RuntimeRevision {
    RuntimeRevision {
        generation: revision.generation,
        anchor_height: revision.anchor.height,
        anchor_hash: revision.anchor.hash,
        tip: revision.tip,
    }
}
fn revision(value: RuntimeRevision) -> Revision {
    Revision {
        generation: value.generation,
        anchor: Anchor {
            height: value.anchor_height,
            hash: value.anchor_hash,
        },
        tip: value.tip,
    }
}
fn now() -> u64 {
    chrono::Utc::now().timestamp().max(0) as u64
}
impl Store {
    pub fn open(path: &Path, pool_id: Id, limits: StorageLimits) -> Result<Self> {
        let map_bytes = usize::try_from(
            limits
                .bytes
                .checked_add(2 * sharechain::SOLUTION_RESERVE_BYTES)
                .ok_or(Error::Capacity("storage map size"))?,
        )
        .map_err(|_| Error::Capacity("storage map size"))?
        .max(sharechain::MIN_MAP_BYTES);
        let mut index = crate::storage::sharechain::SharechainDb::open(
            &path.with_extension("sharechain"),
            map_bytes,
            pool_id,
        )
        .map_err(runtime_error)?;
        index.set_byte_limit(limits.bytes);
        let runtime = index
            .runtime_store(RuntimeLimits {
                max_objects: limits.objects,
                max_object_bytes: 8 * 1024 * 1024,
                max_lease_keys: limits.objects.min(MAX_RUNTIME_LEASE_KEYS),
                max_head_keys: limits.objects.min(MAX_RUNTIME_HEAD_KEYS),
                max_heads: limits.heads as u64,
            })
            .map_err(runtime_error)?;
        Ok(Self {
            database: index,
            native: None,
            reader_deadline: None,
            path: path.to_owned(),
            runtime,
            snapshot: None,
            compaction_cursor: Arc::new(std::sync::Mutex::new(None)),
            pool_id,
            limits,
        })
    }
    pub(crate) fn runtime(&self) -> RuntimeStore {
        self.runtime.clone()
    }
    pub(crate) fn database(&self) -> crate::storage::sharechain::SharechainDb {
        self.database.clone()
    }
    fn check_reader(&self) -> Result<()> {
        if self
            .reader_deadline
            .is_some_and(|deadline| std::time::Instant::now() >= deadline)
        {
            return Err(Error::Capacity("reader lifetime"));
        }
        Ok(())
    }
    fn check_writer(&self) -> Result<()> {
        self.check_reader()?;
        if self.snapshot.is_some() {
            return Err(Error::Invalid("immutable reader"));
        }
        Ok(())
    }
    pub(crate) fn read_source(&self) -> Result<ReadSource> {
        self.check_reader()?;
        let snapshot = match &self.snapshot {
            Some(snapshot) => snapshot.clone(),
            None => Arc::new(self.runtime.snapshot().map_err(runtime_error)?),
        };
        Ok(ReadSource {
            database: self.database.clone(),
            deadline: self
                .reader_deadline
                .unwrap_or_else(|| std::time::Instant::now() + Duration::from_secs(30)),
            path: self.path.clone(),
            native: self.native.clone(),
            pool_id: self.pool_id,
            limits: self.limits.clone(),
            runtime: self.runtime.clone(),
            snapshot,
            compaction_cursor: self.compaction_cursor.clone(),
        })
    }
    fn snapshot(&self) -> Result<Arc<RuntimeSnapshot>> {
        self.check_reader()?;
        match &self.snapshot {
            Some(snapshot) => Ok(snapshot.clone()),
            None => Ok(Arc::new(self.runtime.snapshot().map_err(runtime_error)?)),
        }
    }
    fn physical(&self, key: Key) -> Result<PhysicalObject> {
        let physical = self
            .snapshot()?
            .object_preferred(route(key))
            .map_err(runtime_error)?
            .ok_or(Error::Unavailable(key))?;
        if physical.bytes.len() > key.kind.ceiling() {
            return Err(Error::Invalid("stored object ceiling"));
        }
        Ok(physical)
    }
    fn variant(&self, key: Key) -> Result<sharechain::RuntimeVariant> {
        let variant = self
            .snapshot()?
            .preferred_variant(route(key))
            .map_err(runtime_error)?
            .ok_or(Error::Unavailable(key))?;
        if variant.byte_len > key.kind.ceiling() as u64 {
            return Err(Error::Invalid("stored object ceiling"));
        }
        Ok(variant)
    }
    pub(crate) fn binding(&self, key: Key) -> Result<(Representation, Id)> {
        let variant = self.variant(key)?;
        Ok((variant.representation, variant.digest))
    }
    pub fn path(&self) -> &Path {
        &self.path
    }
    pub fn pool_id(&self) -> Id {
        self.pool_id
    }
    pub fn attach_native(
        &mut self,
        chain: Arc<grin_chain::Chain>,
        profile: crate::native::Profile,
    ) -> Result<()> {
        if profile.pool_id != self.pool_id {
            return Err(Error::Invalid("native store pool identity"));
        }
        self.native = Some((chain, profile));
        Ok(())
    }
    fn check_native(&self, anchor: &Anchor) -> Result<()> {
        if let Some((chain, profile)) = &self.native {
            let view =
                crate::native::View::pin(chain, profile.clone(), chrono::Utc::now().timestamp())?;
            if &view.head != anchor {
                return Err(Error::View);
            }
        }
        Ok(())
    }
    pub fn revision(&self) -> Result<Option<Revision>> {
        let value = self.snapshot()?.revision().map_err(runtime_error)?;
        Ok((value.generation != 0).then(|| revision(value)))
    }
    pub fn fence(&mut self, anchor: &Anchor) -> Result<Revision> {
        self.check_writer()?;
        let before = self.snapshot()?.revision().map_err(runtime_error)?;
        if before.generation != 0
            && before.anchor_height == anchor.height
            && before.anchor_hash == anchor.hash
        {
            return Ok(revision(before));
        }
        let next = RuntimeRevision {
            generation: before
                .generation
                .checked_add(1)
                .ok_or(Error::Capacity("generation"))?,
            anchor_height: anchor.height,
            anchor_hash: anchor.hash,
            tip: None,
        };
        self.runtime
            .apply(
                now(),
                RuntimeBatch {
                    selection: Some((before, next)),
                    ..Default::default()
                },
            )
            .map(revision)
            .map_err(runtime_error)
    }
    pub fn clear_selection(&mut self) -> Result<()> {
        self.check_writer()?;
        let before = self.snapshot()?.revision().map_err(runtime_error)?;
        if before.generation == 0 {
            return Err(Error::Invalid("selection missing"));
        }
        let next = RuntimeRevision {
            generation: before
                .generation
                .checked_add(1)
                .ok_or(Error::Capacity("generation"))?,
            tip: None,
            ..before
        };
        self.runtime
            .apply(
                now(),
                RuntimeBatch {
                    selection: Some((before, next)),
                    ..Default::default()
                },
            )
            .map_err(runtime_error)?;
        Ok(())
    }
    pub(crate) fn stage_checked(&mut self, objects: &[Object]) -> Result<()> {
        self.stage_with(objects, |_| Ok(()))
    }
    fn stage_with(
        &mut self,
        objects: &[Object],
        finish: impl FnOnce(&mut heed::RwTxn<'_>) -> sharechain::Result<()>,
    ) -> Result<()> {
        self.check_writer()?;
        let snapshot = self.snapshot()?;
        let mut staged = Vec::with_capacity(objects.len());
        for object in objects {
            if object.key.kind == Kind::Core {
                if let Some(old) = snapshot
                    .object(route(object.key), None)
                    .map_err(runtime_error)?
                {
                    if old.representation == Representation::Compact {
                        if physical_digest(&object.bytes) != old.logical_digest {
                            return Err(Error::Invalid("conflicting content"));
                        }
                        continue;
                    }
                }
            }
            staged.push(RuntimeObject {
                key: route(object.key),
                mode: if matches!(object.key.kind, Kind::Authorization | Kind::Residual) {
                    ObjectMode::WorkVariant
                } else {
                    ObjectMode::Immutable
                },
                representation: Representation::Canonical,
                logical_digest: physical_digest(&object.bytes),
                bytes: &object.bytes,
            });
        }
        let objects = staged;
        self.runtime
            .apply_with(
                now(),
                RuntimeBatch {
                    objects: &objects,
                    ..Default::default()
                },
                finish,
            )
            .map_err(runtime_error)?;
        Ok(())
    }
    pub fn heads(&self) -> Result<Vec<Head>> {
        self.snapshot()?
            .heads(self.limits.heads + 1)
            .map_err(runtime_error)?
            .into_iter()
            .map(|(tip, bytes)| {
                if bytes.len() != 40 {
                    return Err(Error::Invalid("stored head metadata"));
                }
                Ok(Head {
                    tip,
                    anchor: Anchor {
                        height: u64::from_be_bytes(bytes[..8].try_into().unwrap()),
                        hash: bytes[8..40].try_into().unwrap(),
                    },
                })
            })
            .collect()
    }

    pub(crate) fn promote_replacing(
        &mut self,
        expected: &Revision,
        result: &Evaluation,
        choose: bool,
        witnesses: &[Object],
        replaced: Option<Id>,
    ) -> Result<Revision> {
        self.check_writer()?;
        for witness in witnesses {
            if !matches!(witness.key.kind, Kind::Authorization | Kind::Residual)
                || !result.required.contains(&witness.key)
            {
                return Err(Error::Invalid("publication witness route"));
            }
            if witness.bytes.len() > witness.key.kind.ceiling() {
                return Err(Error::Invalid("publication witness ceiling"));
            }
        }
        self.check_native(&expected.anchor)?;
        let snap = self.snapshot()?;
        if result.anchor != expected.anchor
            || snap.revision().map_err(runtime_error)? != runtime_revision(expected)
        {
            return Err(Error::View);
        }
        let heads = snap.heads(self.limits.heads + 1).map_err(runtime_error)?;
        let mut covered: Vec<Id> = heads
            .iter()
            .filter(|(id, _)| {
                *id != result.tip
                    && ((choose && result.primary.contains(id)) || Some(*id) == replaced)
            })
            .map(|(id, _)| *id)
            .collect();
        if !choose && expected.tip.is_some_and(|tip| covered.contains(&tip)) {
            return Err(Error::Invalid("retire incumbent"));
        }
        if !heads.iter().any(|(id, _)| *id == result.tip)
            && heads.len().saturating_sub(covered.len()) >= self.limits.heads
        {
            if let Some(incumbent) = expected.tip.filter(|id| {
                choose && heads.iter().any(|(head, _)| head == id) && !covered.contains(id)
            }) {
                covered.push(incumbent);
            } else {
                return Err(Error::Capacity("materialized heads"));
            }
        }
        let parent = heads
            .iter()
            .rev()
            .find(|(id, _)| *id != result.tip && result.primary.contains(id))
            .or_else(|| {
                heads
                    .iter()
                    .find(|(id, _)| *id != result.tip && Some(*id) == expected.tip)
            })
            .or_else(|| heads.iter().find(|(id, _)| *id != result.tip))
            .map(|(id, _)| *id);
        let old: BTreeSet<_> = match parent {
            Some(id) => snap
                .head_keys(id)
                .map_err(runtime_error)?
                .ok_or(Error::Invalid("missing retained closure"))?
                .into_iter()
                .collect(),
            None => BTreeSet::new(),
        };
        let required: BTreeSet<_> = result.required.iter().copied().map(route).collect();
        let added: Vec<_> = required.difference(&old).copied().collect();
        let removed: Vec<_> = old.difference(&required).copied().collect();
        let mut metadata = result.anchor.height.to_be_bytes().to_vec();
        metadata.extend(result.anchor.hash);
        let mut binding_bytes = metadata.clone();
        binding_bytes.extend(result.tip);
        let parent_binding = parent
            .map(|id| snap.head_binding(id).map_err(runtime_error))
            .transpose()?
            .flatten();
        binding_bytes.push(u8::from(parent_binding.is_some()));
        binding_bytes.extend(parent_binding.unwrap_or([0; 32]));
        for keys in [&added, &removed] {
            binding_bytes.extend((keys.len() as u64).to_be_bytes());
            for key in keys {
                binding_bytes.push(key.kind);
                binding_bytes.extend(key.id);
            }
        }
        let binding = hash("RUNTIME_HEAD_BINDING", &binding_bytes);
        let objects: Vec<_> = witnesses
            .iter()
            .map(|object| RuntimeObject {
                key: route(object.key),
                mode: ObjectMode::WorkVariant,
                representation: Representation::Canonical,
                logical_digest: physical_digest(&object.bytes),
                bytes: &object.bytes,
            })
            .collect();
        let mut seals = Vec::new();
        for key in &result.required {
            if matches!(key.kind, Kind::Authorization | Kind::Residual) {
                let digest =
                    if let Some(witness) = witnesses.iter().find(|witness| witness.key == *key) {
                        physical_digest(&witness.bytes)
                    } else {
                        snap.object_preferred(route(*key))
                            .map_err(runtime_error)?
                            .ok_or(Error::Unavailable(*key))?
                            .digest
                    };
                seals.push((route(*key), digest));
            } else if snap
                .object(route(*key), None)
                .map_err(runtime_error)?
                .is_none()
            {
                return Err(Error::Unavailable(*key));
            }
        }
        let mut next = expected.clone();
        if choose {
            next.generation = next
                .generation
                .checked_add(1)
                .ok_or(Error::Capacity("generation"))?;
            next.tip = Some(result.tip);
        }
        self.check_native(&expected.anchor)?;
        self.runtime
            .apply(
                now(),
                RuntimeBatch {
                    objects: &objects,
                    seals: &seals,
                    heads: &[RuntimeHead {
                        id: result.tip,
                        binding,
                        parent,
                        added: &added,
                        removed: &removed,
                        metadata: &metadata,
                    }],
                    release_heads: &covered,
                    expect_revision: Some(runtime_revision(expected)),
                    selection: choose
                        .then(|| (runtime_revision(expected), runtime_revision(&next))),
                    ..Default::default()
                },
            )
            .map_err(runtime_error)?;
        Ok(next)
    }
    pub fn retire(&mut self, tip: Id) -> Result<()> {
        self.check_writer()?;
        let expected = self.snapshot()?.revision().map_err(runtime_error)?;
        if expected.tip == Some(tip) {
            return Err(Error::Invalid("retire incumbent"));
        }
        self.runtime
            .retire_head(now(), expected, tip)
            .map_err(runtime_error)?;
        Ok(())
    }
    pub fn acquisitions(&self) -> Result<Vec<Id>> {
        Ok(self
            .snapshot()?
            .leases(9)
            .map_err(runtime_error)?
            .into_iter()
            .filter(|(_, expires, _)| *expires > now())
            .map(|(tip, _, _)| tip)
            .collect())
    }
    pub fn acquire(&mut self, tip: Id, keys: &BTreeSet<Key>, max_keys: usize) -> Result<u64> {
        self.check_writer()?;
        let now = now();
        let leases = self.snapshot()?.leases(9).map_err(runtime_error)?;
        let release: Vec<_> = leases
            .iter()
            .filter(|(_, expires, _)| *expires <= now)
            .map(|(id, _, _)| *id)
            .collect();
        let retained: Vec<_> = leases
            .iter()
            .filter(|(id, _, _)| !release.contains(id))
            .collect();
        if !retained.iter().any(|(id, _, _)| *id == tip) && retained.len() >= 8 {
            return Err(Error::Capacity("acquisition tasks"));
        }
        let deadline = match retained.iter().find(|(id, _, _)| *id == tip) {
            Some((_, expires, _)) => *expires,
            None => now
                .checked_add(sharechain::ACQUISITION_LIFETIME)
                .ok_or(Error::Capacity("acquisition deadline"))?,
        };
        let keys: Vec<_> = keys.iter().copied().map(route).collect();
        self.runtime
            .apply(
                now,
                RuntimeBatch {
                    leases: &[RuntimeLease {
                        id: tip,
                        keys: &keys,
                    }],
                    release_leases: &release,
                    lease_key_limit: Some(max_keys as u64),
                    ..Default::default()
                },
            )
            .map_err(runtime_error)?;
        Ok(deadline)
    }
    pub fn release_acquisition(&mut self, tip: Id) -> Result<()> {
        self.check_writer()?;
        self.runtime
            .apply(
                now(),
                RuntimeBatch {
                    release_leases: &[tip],
                    ..Default::default()
                },
            )
            .map_err(runtime_error)?;
        Ok(())
    }
    pub fn collect(&mut self, keep: &BTreeSet<Key>, older_than: i64) -> Result<usize> {
        self.check_writer()?;
        let keep: Vec<_> = keep.iter().copied().map(route).collect();
        let now = now();
        let age = now.saturating_sub(older_than.max(0) as u64);
        self.runtime
            .collect_keep(now, age, 256, &keep)
            .map(|gc| gc.removed)
            .map_err(runtime_error)
    }
    pub fn byte_length(&self, key: Key) -> Result<usize> {
        let variant = self.variant(key)?;
        Ok(if variant.representation == Representation::Compact {
            key.kind.ceiling()
        } else {
            variant.byte_len as usize
        })
    }
    pub fn parent_hint(&self, work: Id) -> Result<Parent> {
        let physical = self.physical(Key::new(Kind::Core, work))?;
        match physical.representation {
            Representation::Canonical => {
                let core: Core = decode(&physical.bytes)?;
                if hash("WORK_ID", &core.header.0) != work {
                    return Err(Error::Invalid("stored core identity"));
                }
                Ok(decode::<Job>(&core.job.0)?.parent)
            }
            Representation::Compact => crate::storage::compact::parent_hint(&physical.bytes),
        }
    }
    pub(crate) fn prepare_compaction(
        &self,
        view: &crate::native::View<'_>,
        cancel: &std::sync::atomic::AtomicBool,
    ) -> Result<Compaction> {
        if view.profile.pool_id != self.pool_id {
            return Err(Error::View);
        }
        let mut entries = Vec::new();
        let started = std::time::Instant::now();
        let snapshot = self.snapshot()?;
        let after = *self
            .compaction_cursor
            .lock()
            .map_err(|_| Error::Invalid("compaction cursor lock"))?;
        let rows = snapshot
            .objects_after(Kind::Core as u8, after, 256, 256 * 85)
            .map_err(runtime_error)?;
        let row_count = rows.len();
        let mut processed = 0;
        let mut next_cursor = after;
        for (routing, variant) in rows {
            self.check_reader()?;
            if started.elapsed() > Duration::from_millis(200) {
                break;
            }
            if cancel.load(std::sync::atomic::Ordering::Acquire) {
                return Err(Error::Cancelled);
            }
            next_cursor = Some((routing, variant.digest));
            processed += 1;
            if variant.representation != Representation::Canonical {
                continue;
            }
            let physical = snapshot
                .object(routing, Some(variant.digest))
                .map_err(runtime_error)?
                .ok_or(Error::Invalid("compaction physical source"))?;
            if physical.bytes.len() > Kind::Core.ceiling() {
                return Err(Error::Invalid("stored core ceiling"));
            }
            match crate::native::Work::verify(&physical.bytes, view) {
                Ok(work) => {
                    if work.id() != routing.id {
                        return Err(Error::Invalid("compaction WorkID"));
                    }
                    let packed = crate::storage::compact::pack(&work, view)?;
                    if packed.len() < physical.bytes.len() {
                        let expanded = crate::storage::compact::expand(&packed, view)?;
                        if expanded != physical.bytes {
                            return Err(Error::Invalid("compaction canonical representation"));
                        }
                        entries.push((Key::new(Kind::Core, routing.id), physical.digest, packed));
                    }
                }
                Err(Error::View | Error::NativeUnavailable(_)) => {}
                Err(error) => return Err(error),
            }
        }
        view.check()?;
        if processed == row_count && row_count < 256 {
            next_cursor = None;
        }
        Ok(Compaction {
            pool_id: self.pool_id,
            anchor: view.head.clone(),
            entries,
            next_cursor,
        })
    }
    pub(crate) fn apply_compaction(&mut self, plan: Compaction) -> Result<usize> {
        self.check_writer()?;
        if plan.pool_id != self.pool_id {
            return Err(Error::View);
        }
        self.check_native(&plan.anchor)?;
        let snapshot = self.snapshot()?;
        let mut replacements = Vec::new();
        for (key, digest, packed) in &plan.entries {
            if snapshot
                .object(route(*key), None)
                .map_err(runtime_error)?
                .is_some_and(|old| {
                    old.digest == *digest && old.representation == Representation::Canonical
                })
            {
                replacements.push(RuntimeReplacement {
                    key: route(*key),
                    logical_digest: *digest,
                    expected_digest: *digest,
                    expected_representation: Representation::Canonical,
                    representation: Representation::Compact,
                    bytes: packed,
                });
            }
        }
        self.check_native(&plan.anchor)?;
        let mut cursor = self
            .compaction_cursor
            .lock()
            .map_err(|_| Error::Invalid("compaction cursor lock"))?;
        self.runtime
            .apply(
                now(),
                RuntimeBatch {
                    replacements: &replacements,
                    ..Default::default()
                },
            )
            .map_err(runtime_error)?;
        *cursor = plan.next_cursor;
        Ok(replacements.len())
    }
}
impl Source for Store {
    fn read(&self, key: Key) -> Result<Arc<[u8]>> {
        let physical = self.physical(key)?;
        let bytes = match physical.representation {
            Representation::Canonical => physical.bytes,
            Representation::Compact if key.kind == Kind::Core => {
                let (chain, profile) = self.native.as_ref().ok_or(Error::NativeUnavailable(
                    "compact store needs native view".into(),
                ))?;
                let view = crate::native::View::pin(
                    chain,
                    profile.clone(),
                    chrono::Utc::now().timestamp(),
                )?;
                let bytes = crate::storage::compact::expand(&physical.bytes, &view)?;
                if physical_digest(&bytes) != physical.logical_digest {
                    return Err(Error::Invalid("compact canonical identity"));
                }
                bytes
            }
            Representation::Compact => return Err(Error::Invalid("compact object kind")),
        };
        Key::checked(key.kind, key.id, &bytes)?;
        self.check_reader()?;
        Ok(bytes.into())
    }
}

#[cfg(test)]
#[path = "mod_tests.rs"]
mod tests;
