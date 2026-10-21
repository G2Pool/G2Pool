use super::*;
use crate::{limits::Budget, storage::ReadSource};
use std::cell::RefCell;

pub(crate) struct Stage {
    source: ReadSource,
    _permit: crate::limits::WorkPermit,
    chain: Arc<Chain>,
    profile: Profile,
    anchor: Anchor,
    limits: Limits,
    cancel: Arc<AtomicBool>,
    objects: Vec<Object>,
}

#[derive(Debug)]
pub(crate) struct VerifiedStage {
    pool_id: Id,
    anchor: Anchor,
    objects: Vec<Object>,
    checked_at: i64,
    dependencies: BTreeMap<Key, (u8, Id)>,
}

struct TrackedSource {
    store: Store,
    dependencies: RefCell<BTreeMap<Key, (u8, Id)>>,
}
impl Source for TrackedSource {
    fn read(&self, key: Key) -> Result<Arc<[u8]>> {
        let binding = self.store.binding(key)?;
        let bytes = self.store.read(key)?;
        self.dependencies.borrow_mut().insert(key, binding);
        Ok(bytes)
    }
}

fn package_bytes(objects: &[Object]) -> Result<usize> {
    if objects.len() > 65536 {
        return Err(Error::Capacity("incoming package"));
    }
    if objects
        .iter()
        .map(|object| object.key)
        .collect::<BTreeSet<_>>()
        .len()
        != objects.len()
    {
        return Err(Error::Invalid("duplicate package key"));
    }
    objects.iter().try_fold(0usize, |bytes, object| {
        bytes
            .checked_add(object.bytes.len())
            .filter(|bytes| *bytes <= 64 * 1024 * 1024)
            .ok_or(Error::Capacity("incoming package"))
    })
}

pub(super) fn verify<S: Source>(
    source: &S,
    objects: &[Object],
    view: &View<'_>,
    limits: Limits,
    cancel: Arc<AtomicBool>,
) -> Result<()> {
    package_bytes(objects)?;
    let mut budget = Budget::new(limits, cancel);
    for object in objects {
        budget.object(object.bytes.len())?;
        Key::checked(object.key.kind, object.key.id, &object.bytes)?;
    }
    let overlay = Overlay {
        base: source,
        entries: objects
            .iter()
            .map(|object| (object.key, object.bytes.clone()))
            .collect(),
    };
    let work_ids: BTreeSet<_> = objects
        .iter()
        .filter(|object| matches!(object.key.kind, Kind::Core | Kind::Authorization))
        .map(|object| object.key.id)
        .collect();
    for id in work_ids {
        budget.work()?;
        let work = Work::verify(&overlay.read(Key::new(Kind::Core, id))?, view)?;
        if work.id() != id {
            return Err(Error::Invalid("staged WorkID"));
        }
        let auth: WorkAuthorization = decode(&overlay.read(Key::new(Kind::Authorization, id))?)?;
        work.authorize(&auth, view.profile.pool_id)?;
    }
    view.check()?;
    budget.check()
}

impl Stage {
    pub(crate) fn run(self) -> Result<VerifiedStage> {
        let source = TrackedSource {
            store: self.source.open()?,
            dependencies: RefCell::new(BTreeMap::new()),
        };
        let view = View::pin(&self.chain, self.profile, chrono::Utc::now().timestamp())?;
        if view.head != self.anchor {
            return Err(Error::View);
        }
        verify(&source, &self.objects, &view, self.limits, self.cancel)?;
        Ok(VerifiedStage {
            pool_id: view.profile.pool_id,
            anchor: view.head.clone(),
            objects: self.objects,
            checked_at: view.now,
            dependencies: source.dependencies.into_inner(),
        })
    }
}

impl Node {
    pub(crate) fn prepare_stage(&self, objects: Vec<Object>) -> Result<Stage> {
        if objects.len() > crate::network::REQUEST_KEYS {
            return Err(Error::Capacity("peer staging package"));
        }
        let permit = self
            .peer_work
            .acquire()
            .ok_or(Error::Capacity("peer native work slots"))?;

        package_bytes(&objects)?;
        Ok(Stage {
            _permit: permit,
            source: self.store.read_source()?,
            chain: self.chain.clone(),
            profile: self.profile.clone(),
            anchor: self.view()?.head,
            limits: self.limits.clone(),
            cancel: self.cancel.clone(),
            objects,
        })
    }
    pub(crate) fn finish_stage(&mut self, verified: VerifiedStage) -> Result<()> {
        let view = self.view()?;
        if self.cancel.load(std::sync::atomic::Ordering::Acquire) {
            return Err(Error::Cancelled);
        }
        if verified.pool_id != self.profile.pool_id
            || view.head != verified.anchor
            || view.now < verified.checked_at
        {
            return Err(Error::View);
        }
        for (key, binding) in verified.dependencies {
            match self.store.binding(key) {
                Ok(current) if current == binding => {}
                Ok(_) | Err(Error::Unavailable(_)) => return Err(Error::View),
                Err(error) => return Err(error),
            }
        }
        self.store.stage_checked(&verified.objects)
    }
}
