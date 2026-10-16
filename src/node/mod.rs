mod acquisition;
mod maintenance;
mod staging;
use acquisition::{Acquisition, Running};
pub use acquisition::{Recovery, RecoveryResult};
pub use maintenance::{Maintenance, MaintenanceResult};

use crate::{
    codec::*,
    consensus::{Evaluation, EvaluationProgress, Facts, Validator},
    limits::Limits,
    native::{Profile, View, Work},
    objects::{Key, Kind, Source},
    storage::{Object, Revision, Store},
    Error, Result,
};
use grin_chain::Chain;
use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    sync::{atomic::AtomicBool, Arc},
};

#[derive(Clone, Default)]
pub(crate) struct Published {
    pub revision: Option<Revision>,
    pub heads: Vec<Id>,
    pub recovering: bool,
}
pub struct Node {
    pub(crate) connected: Arc<std::sync::atomic::AtomicUsize>,
    pub(crate) published: Arc<std::sync::RwLock<Published>>,
    pub chain: Arc<Chain>,
    pub peers: crate::network::peers::Book,
    pub listening_port: u16,
    pub(crate) transport: Option<crate::network::transport::Handle>,
    pub(crate) downloads: Arc<std::sync::Mutex<crate::network::download::Downloads>>,
    pub profile: Profile,
    pub store: Store,
    pub(crate) sharechain: crate::consensus::coordinator::Coordinator,
    pub limits: Limits,
    pub cancel: Arc<AtomicBool>,
    pub(crate) peer_work: Arc<crate::limits::WorkSlots>,
    pub(crate) serve_work: Arc<crate::limits::WorkSlots>,
    best: Option<Id>,
    validated: BTreeSet<Id>,
    tasks: BTreeMap<Id, Acquisition>,
    running: BTreeMap<Id, Running>,
    warm: Option<(Anchor, Facts)>,
    dependency_closure: Option<crate::consensus::VerifiedClosure>,
    cooperative: bool,
    recovery: VecDeque<(Id, Id)>,
    recovery_seen: BTreeSet<Id>,
    pub(crate) sync_cursor: Option<Id>,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ForkComparison {
    PreferLeft,
    PreferRight,
    Equal,
    Deferred,
}

fn compare_suffix_work(
    left: &[u128],
    right: &[u128],
    left_tip: Id,
    right_tip: Id,
) -> ForkComparison {
    let left_work = left
        .iter()
        .try_fold(0u128, |sum, work| sum.checked_add(*work));
    let right_work = right
        .iter()
        .try_fold(0u128, |sum, work| sum.checked_add(*work));
    match (left_work, right_work) {
        (Some(left_work), Some(right_work)) => match left_work.cmp(&right_work) {
            std::cmp::Ordering::Greater => ForkComparison::PreferLeft,
            std::cmp::Ordering::Less => ForkComparison::PreferRight,
            std::cmp::Ordering::Equal => {
                if left_tip == right_tip {
                    ForkComparison::Equal
                } else if left_tip < right_tip {
                    ForkComparison::PreferLeft
                } else {
                    ForkComparison::PreferRight
                }
            }
        },
        _ => ForkComparison::Deferred,
    }
}
struct Overlay<'a, S: Source> {
    base: &'a S,
    entries: BTreeMap<Key, Arc<[u8]>>,
}
impl<S: Source> Source for Overlay<'_, S> {
    fn read(&self, key: Key) -> Result<Arc<[u8]>> {
        self.entries
            .get(&key)
            .cloned()
            .map(Ok)
            .unwrap_or_else(|| self.base.read(key))
    }
}
impl Node {
    pub fn new(chain: Arc<Chain>, profile: Profile, store: Store, limits: Limits) -> Result<Self> {
        Self::initialize(chain, profile, store, limits, false)
    }
    pub fn start(
        chain: Arc<Chain>,
        profile: Profile,
        store: Store,
        limits: Limits,
    ) -> Result<Self> {
        Self::initialize(chain, profile, store, limits, true)
    }
    fn initialize(
        chain: Arc<Chain>,
        profile: Profile,
        mut store: Store,
        limits: Limits,
        cooperative: bool,
    ) -> Result<Self> {
        if u64::from(grin_core::global::cut_through_horizon()) < 2 * profile.manifest.lifetime - 2 {
            return Err(Error::NativeUnavailable(
                "native rewind horizon is shorter than pool finance horizon".into(),
            ));
        }
        if profile.protocol != store.protocol() {
            return Err(Error::Invalid("node protocol"));
        }
        store.attach_native(chain.clone(), profile.clone())?;
        let sharechain = crate::consensus::coordinator::Coordinator::from_db(store.archive())?;
        let peers = crate::network::peers::Book::memory(profile.manifest.network >= 2);
        let mut node = Self {
            connected: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            published: Arc::new(std::sync::RwLock::new(Published::default())),
            peers,
            listening_port: 0,
            transport: None,
            downloads: Arc::new(std::sync::Mutex::new(
                crate::network::download::Downloads::default(),
            )),
            chain,
            profile,
            store,
            sharechain,
            limits,
            cancel: Arc::new(AtomicBool::new(false)),
            peer_work: crate::limits::WorkSlots::new(2),
            serve_work: crate::limits::WorkSlots::new(1),
            best: None,
            validated: BTreeSet::new(),
            tasks: BTreeMap::new(),
            running: BTreeMap::new(),
            warm: None,
            dependency_closure: None,
            cooperative,
            recovery: VecDeque::new(),
            recovery_seen: BTreeSet::new(),
            sync_cursor: None,
        };
        if cooperative {
            node.begin_refresh()?;
        } else {
            node.refresh()?;
        }
        node.publish_snapshot()?;
        Ok(node)
    }
    pub(crate) fn publish_snapshot(&mut self) -> Result<()> {
        let retained: BTreeSet<_> = self
            .store
            .heads()?
            .into_iter()
            .map(|head| head.tip)
            .collect();
        self.validated.retain(|tip| retained.contains(tip));
        let revision = self.store.revision()?;
        let heads = match &revision {
            Some(revision) => {
                let mut heads: Vec<_> = self
                    .store
                    .heads()?
                    .into_iter()
                    .filter(|h| h.anchor == revision.anchor && self.validated.contains(&h.tip))
                    .map(|h| h.tip)
                    .collect();
                heads.sort_by_key(|tip| (self.best.is_none_or(|best| best != *tip), *tip));
                heads.truncate(crate::network::MAX_TIPS);
                heads
            }
            None => vec![],
        };
        let recovering =
            !self.recovery.is_empty() || (self.best.is_none() && !self.store.heads()?.is_empty());
        *self
            .published
            .write()
            .map_err(|_| Error::Invalid("publication snapshot poisoned"))? = Published {
            revision,
            heads,
            recovering,
        };
        Ok(())
    }
    pub fn view(&self) -> Result<View<'_>> {
        View::pin(
            &self.chain,
            self.profile.clone(),
            chrono::Utc::now().timestamp(),
        )
    }
    pub fn revision(&self) -> Result<Revision> {
        self.store.revision()?.ok_or(Error::View)
    }
    pub fn sharechain_publication(
        &self,
    ) -> Result<Option<crate::storage::sharechain::Publication>> {
        self.sharechain
            .db()
            .read()
            .map_err(|_| Error::Invalid("sharechain snapshot"))?
            .publication()
            .map_err(|_| Error::Invalid("sharechain publication"))
    }
    pub(crate) fn sharechain_locator(&self) -> Result<Option<Vec<Id>>> {
        let snapshot = self
            .sharechain
            .db()
            .read()
            .map_err(|_| Error::Invalid("sharechain snapshot"))?;
        let Some(publication) = snapshot
            .publication()
            .map_err(|_| Error::Invalid("sharechain publication"))?
        else {
            return Ok(None);
        };
        snapshot
            .locator(publication.head, crate::network::chain_sync::MAX_LOCATOR)
            .map(Some)
            .map_err(|_| Error::Invalid("sharechain locator"))
    }
    pub(crate) fn compare_sharechain(
        &self,
        left: Id,
        right: Id,
    ) -> Result<Option<std::cmp::Ordering>> {
        let snapshot = self
            .sharechain
            .db()
            .read()
            .map_err(|_| Error::Invalid("sharechain snapshot"))?;
        snapshot
            .compare_cumulative(left, right)
            .map_err(|_| Error::Invalid("sharechain comparison"))
    }
    fn prefer_candidate(&self, tip: Id, facts: &Facts, incumbent: Id) -> Result<bool> {
        if self.sharechain.indexed(tip)? && self.sharechain.indexed(incumbent)? {
            if let Some(ordering) = self.compare_sharechain(tip, incumbent)? {
                return Ok(ordering.is_gt());
            }
        }
        let (result, incumbent_facts) = self.evaluate_cached(incumbent)?;
        Ok(matches!(
            self.compare_verified_branches(tip, facts, result.tip, &incumbent_facts),
            ForkComparison::PreferLeft
        ))
    }
    pub fn parent(&self) -> Result<Parent> {
        let revision = self.revision()?;
        let view = self.view()?;
        if revision.anchor != view.head {
            return Err(Error::View);
        }
        if self.best.is_none() && !self.store.heads()?.is_empty() {
            return Err(Error::Capacity("pool recovery is incomplete"));
        }
        Ok(self
            .best
            .map(Parent::Receipt)
            .unwrap_or(Parent::Root(view.head)))
    }
    pub(crate) fn share_history(&self, parent: Option<Id>) -> Result<Vec<(u64, u128)>> {
        self.store.share_history(parent)
    }
    pub fn advertised_heads(&self) -> Result<Vec<Id>> {
        let head = self.view()?.head;
        let mut heads: Vec<_> = self
            .store
            .heads()?
            .into_iter()
            .filter(|candidate| candidate.anchor == head && self.validated.contains(&candidate.tip))
            .map(|candidate| candidate.tip)
            .collect();
        let best = self.best;
        heads.sort_by_key(|tip| (Some(*tip) != best, *tip));
        heads.truncate(crate::network::MAX_TIPS);
        Ok(heads)
    }
    pub fn evaluate(&self, tip: Id) -> Result<Evaluation> {
        let view = self.view()?;
        Validator::bounded(&self.store, &view, self.limits.clone(), self.cancel.clone())
            .evaluate(tip)
    }
    pub(crate) fn compare_verified_branches(
        &self,
        left: Id,
        left_facts: &Facts,
        right: Id,
        right_facts: &Facts,
    ) -> ForkComparison {
        fn path(id: Id, facts: &Facts) -> Option<(Vec<Id>, Option<Anchor>)> {
            let mut ids = Vec::new();
            let mut root = None;
            let mut cursor = Some(id);
            let mut seen = BTreeSet::new();
            while let Some(current) = cursor {
                if !seen.insert(current) {
                    return None;
                }
                let Some(work) = facts.work(current) else {
                    break;
                };
                let job = work.job();
                ids.push(current);
                cursor = match &job.parent {
                    Parent::Root(anchor) => {
                        root = Some(anchor.clone());
                        None
                    }
                    Parent::Receipt(parent) => Some(*parent),
                };
            }
            Some((ids, root))
        }
        let Some((left_path, left_root)) = path(left, left_facts) else {
            return ForkComparison::Deferred;
        };
        let Some((right_path, right_root)) = path(right, right_facts) else {
            return ForkComparison::Deferred;
        };
        let right_positions: BTreeMap<Id, usize> = right_path
            .iter()
            .enumerate()
            .map(|(index, id)| (*id, index))
            .collect();
        let common = left_path.iter().enumerate().find_map(|(index, id)| {
            right_positions
                .get(id)
                .map(|right_index| (index, *right_index))
        });
        let credited = |ids: &[Id], facts: &Facts| -> Option<u128> {
            ids.iter().try_fold(0u128, |sum, id| {
                let work = facts.work(*id)?;
                let with_uncles = work.job().uncles.0.iter().try_fold(
                    u128::from(work.job().difficulty),
                    |total, uncle| {
                        total.checked_add(u128::from(facts.work(*uncle)?.job().difficulty))
                    },
                )?;
                sum.checked_add(with_uncles)
            })
        };
        let (common_left, common_right) = match common {
            Some(indices) => indices,
            None if left_root.is_some() && left_root == right_root => {
                (left_path.len(), right_path.len())
            }
            None => {
                let (Some(left_root), Some(right_root)) = (left_root, right_root) else {
                    return ForkComparison::Deferred;
                };
                let Ok(view) = self.view() else {
                    return ForkComparison::Deferred;
                };
                let left_valid = view.anchor(&left_root).is_ok();
                let right_valid = view.anchor(&right_root).is_ok();
                match (left_valid, right_valid) {
                    (true, false) => return ForkComparison::PreferLeft,
                    (false, true) => return ForkComparison::PreferRight,
                    (false, false) => return ForkComparison::Deferred,
                    (true, true) => {
                        let Some(left_work) = credited(&left_path, left_facts) else {
                            return ForkComparison::Deferred;
                        };
                        let Some(right_work) = credited(&right_path, right_facts) else {
                            return ForkComparison::Deferred;
                        };
                        return match left_work.cmp(&right_work) {
                            std::cmp::Ordering::Greater => ForkComparison::PreferLeft,
                            std::cmp::Ordering::Less => ForkComparison::PreferRight,
                            std::cmp::Ordering::Equal if left_root.height != right_root.height => {
                                if left_root.height > right_root.height {
                                    ForkComparison::PreferLeft
                                } else {
                                    ForkComparison::PreferRight
                                }
                            }
                            std::cmp::Ordering::Equal => ForkComparison::Deferred,
                        };
                    }
                }
            }
        };
        let Some(left_work) = credited(&left_path[..common_left], left_facts) else {
            return ForkComparison::Deferred;
        };
        let Some(right_work) = credited(&right_path[..common_right], right_facts) else {
            return ForkComparison::Deferred;
        };
        compare_suffix_work(&[left_work], &[right_work], left, right)
    }
    pub(crate) fn publish_evaluation_share(
        &mut self,
        result: &Evaluation,
        facts: &Facts,
    ) -> Result<()> {
        let suffix = facts.compact_chain(
            result.tip,
            self.profile
                .validation_limit()
                .saturating_add(usize::from(self.profile.manifest.uncle_depth)),
            |id| self.sharechain.indexed(id),
        )?;
        let parent_tip = facts
            .work(result.tip)
            .and_then(|work| match work.job().parent {
                Parent::Receipt(parent) => Some(parent),
                Parent::Root(_) => None,
            });
        let parent_closure = self
            .dependency_closure
            .as_ref()
            .filter(|closure| parent_tip == Some(closure.tip()));
        let closure = facts.verified_closure(result, parent_closure)?;
        let retained = facts
            .bytes()
            .saturating_add(closure.retained_bytes())
            .saturating_add(closure.encoded_delta_bytes());
        if retained > self.limits.bytes {
            return Err(Error::Capacity("evaluation dependency closure"));
        }
        let suffix_len = suffix.len();
        let mut shares = Vec::with_capacity(suffix_len);
        let snapshot = self
            .sharechain
            .db()
            .read()
            .map_err(|_| Error::Invalid("sharechain snapshot"))?;
        let mut state_bytes = Vec::with_capacity(suffix_len);
        let mut state_roots = Vec::with_capacity(suffix_len);
        let mut credited_work = Vec::with_capacity(suffix_len);
        let mut previous_id = None;
        let mut previous_state_root = [0; 32];
        for (work, _) in &suffix {
            let mut credited = u128::from(work.job().difficulty);
            for uncle_id in &work.job().uncles.0 {
                let uncle = facts.uncle_work(*uncle_id)?;
                credited = credited
                    .checked_add(u128::from(uncle.job().difficulty))
                    .ok_or(Error::Invalid("sharechain uncle work overflow"))?;
            }
            let parent = match work.job().parent {
                Parent::Root(_) => None,
                Parent::Receipt(id) => Some(id),
            };
            let parent_state_root = if parent == previous_id {
                previous_state_root
            } else if let Some(parent) = parent {
                snapshot
                    .header(&parent)
                    .map_err(|_| Error::Invalid("sharechain parent"))?
                    .ok_or(Error::Missing(parent))?
                    .state_root
            } else {
                [0; 32]
            };
            let mut transition = Vec::with_capacity(192 + work.job().uncles.0.len() * 40);
            transition.extend(b"g2pool.branch-transition\0");
            transition.extend(1u16.to_be_bytes());
            transition.extend(parent_state_root);
            transition.extend(work.id());
            transition.push(u8::from(parent.is_some()));
            transition.extend(parent.unwrap_or([0; 32]));
            transition.extend(work.job().anchor.height.to_be_bytes());
            transition.extend(work.job().anchor.hash);
            transition.extend(credited.to_be_bytes());
            transition.push(work.job().uncles.0.len() as u8);
            for uncle_id in &work.job().uncles.0 {
                let uncle = facts.uncle_work(*uncle_id)?;
                transition.extend(*uncle_id);
                transition.extend(uncle.job().difficulty.to_be_bytes());
            }
            let state_root = crate::codec::hash("SHARE_STATE", &transition);
            credited_work.push(credited);
            state_bytes.push(transition);
            state_roots.push(state_root);
            previous_id = Some(work.id());
            previous_state_root = state_root;
        }
        for (index, (work, body)) in suffix.iter().enumerate() {
            let parent = match work.job().parent {
                Parent::Root(_) => None,
                Parent::Receipt(id) => Some(id),
            };
            let share = crate::consensus::coordinator::VerifiedShare {
                id: work.id(),
                parent,
                height: 0,
                anchor: work.job().anchor.hash,
                work: credited_work[index],
                state: &state_bytes[index],
                body,
                delta: &[],
                state_root: state_roots[index],
            };
            shares.push(share);
        }
        drop(snapshot);
        if !shares.is_empty() {
            self.sharechain
                .store_verified_batch_with_evaluation(&shares, result, &closure)?;
        } else if self
            .dependency_closure
            .as_ref()
            .is_none_or(|previous| previous.id() != closure.id())
        {
            self.sharechain.store_evaluation_closure(result, &closure)?;
        }
        self.dependency_closure = Some(closure);
        Ok(())
    }
    pub(crate) fn verified_facts(&self, anchor: &Anchor) -> Option<&Facts> {
        self.warm
            .as_ref()
            .filter(|(verified, _)| verified == anchor)
            .map(|(_, facts)| facts)
    }
    fn evaluate_cached(&self, tip: Id) -> Result<(Evaluation, Facts)> {
        let view = self.view()?;
        let mut validator =
            Validator::bounded(&self.store, &view, self.limits.clone(), self.cancel.clone());
        if let Some((anchor, facts)) = &self.warm {
            if *anchor == view.head {
                validator.seed(facts);
            }
        }
        validator.evaluate_with_facts(tip)
    }
    pub fn stage(&mut self, objects: &[Object]) -> Result<()> {
        let view = self.view()?;
        staging::verify(
            &self.store,
            objects,
            &view,
            self.limits.clone(),
            self.cancel.clone(),
        )?;
        self.store.stage_checked(objects)
    }
    pub fn consider(&mut self, tip: Id) -> Result<bool> {
        self.ensure_view()?;
        if self.best.is_some_and(|incumbent| incumbent == tip) {
            return Ok(false);
        }
        self.release_abandoned()?;
        if self.running.contains_key(&tip) {
            return Err(Error::Capacity("candidate verification in progress"));
        }
        loop {
            match self.poll(tip)? {
                EvaluationProgress::Ready(_) => {
                    return Ok(self.best.is_some_and(|best| best == tip))
                }
                EvaluationProgress::Pending => std::thread::yield_now(),
                EvaluationProgress::Missing(key) => return Err(Error::Unavailable(key)),
            }
        }
    }
    pub fn ensure_view(&mut self) -> Result<()> {
        let head = self.view()?.head;
        if self.revision()?.anchor != head {
            if self.cooperative {
                self.begin_refresh()?;
            } else {
                self.refresh()?;
            }
        }
        Ok(())
    }
    fn rebase_facts(&mut self, head: &Anchor) -> Result<()> {
        let Some((old, facts)) = self.warm.take() else {
            return Ok(());
        };
        let view = self.view()?;
        if old.height <= head.height
            && view.anchor(&old).is_ok()
            && facts.has_active_tip(view.profile.finance_low(head.height))
        {
            view.check()?;
            self.warm = Some((head.clone(), facts));
        }
        Ok(())
    }
    fn begin_refresh(&mut self) -> Result<()> {
        let head = self.view()?.head;
        self.tasks.clear();
        self.rebase_facts(&head)?;
        self.best = None;
        self.validated.clear();
        self.recovery_seen.clear();
        self.store.fence(&head)?;
        self.store.clear_selection()?;
        self.recovery = self
            .store
            .heads()?
            .into_iter()
            .map(|head| (head.tip, head.tip))
            .collect();
        if self.recovery.len() > self.limits.work {
            return Err(Error::Capacity("recovery roots"));
        }
        self.publish_snapshot()
    }
    pub fn refresh(&mut self) -> Result<()> {
        let head = self.view()?.head;
        self.tasks.clear();
        self.rebase_facts(&head)?;
        self.store.fence(&head)?;
        self.store.clear_selection()?;
        self.best = None;
        self.validated.clear();
        let heads = self.store.heads()?;
        let mut visited = BTreeSet::new();
        for candidate in heads {
            let mut tip = candidate.tip;
            loop {
                if !visited.insert(tip) {
                    break;
                }
                if visited.len() > self.limits.work {
                    return Err(Error::Capacity("recovery candidates"));
                }
                match self.evaluate_cached(tip) {
                    Ok((result, facts)) => {
                        let expected = self.revision()?;
                        self.publish_evaluation_share(&result, &facts)?;
                        let choose = match self.best {
                            None => true,
                            Some(old) => self.prefer_candidate(result.tip, &facts, old)?,
                        };
                        self.store.promote(&expected, &result, choose)?;
                        self.validated.insert(result.tip);
                        if choose {
                            self.best = Some(result.tip);
                            self.warm = Some((result.anchor.clone(), facts));
                        }
                        break;
                    }
                    Err(Error::Expired) => {
                        self.store.retire(candidate.tip)?;
                        break;
                    }
                    Err(Error::View) => {
                        if self.view()?.head != head {
                            return Err(Error::View);
                        }
                        match self.store.parent_hint(tip)? {
                            Parent::Receipt(parent) => tip = parent,
                            Parent::Root(_) => break,
                        }
                    }
                    Err(error) => return Err(error),
                }
            }
        }
        self.publish_snapshot()
    }
    pub fn uncles(&self, parent: &Parent) -> Result<Vec<Id>> {
        let Parent::Receipt(tip) = parent else {
            return Ok(vec![]);
        };
        let view = self.view()?;
        let mut cursor = Some(*tip);
        let mut ancestors = Vec::new();
        let mut forbidden = BTreeSet::new();
        let depth = usize::from(view.profile.manifest.uncle_depth);
        for _ in 0..depth {
            let Some(id) = cursor else { break };
            let bytes = match self.store.read(Key::new(Kind::Core, id)) {
                Ok(bytes) => bytes,
                Err(Error::Unavailable(_)) => break,
                Err(error) => return Err(error),
            };
            let work = Work::verify(&bytes, &view)?;
            if work.job().anchor.height < view.profile.finance_low(view.head.height) {
                break;
            }
            forbidden.insert(id);
            forbidden.extend(work.job().uncles.0.iter().copied());
            cursor = match work.job().parent {
                Parent::Receipt(id) => Some(id),
                Parent::Root(_) => None,
            };
            ancestors.push(work);
        }
        let mut uncles = Vec::new();
        for head in self.store.heads()? {
            if forbidden.contains(&head.tip) {
                continue;
            }
            if head.anchor != view.head {
                continue;
            }
            let work = match self
                .store
                .read(Key::new(Kind::Core, head.tip))
                .and_then(|bytes| Work::verify(&bytes, &view))
            {
                Ok(work) => work,
                Err(
                    Error::View
                    | Error::Expired
                    | Error::Unavailable(_)
                    | Error::NativeUnavailable(_),
                ) => continue,
                Err(error) => return Err(error),
            };
            if work.job().uncles.0.len() > usize::from(view.profile.manifest.max_uncles) {
                continue;
            }
            if ancestors
                .iter()
                .any(|a| a.job().parent == work.job().parent && a.job().anchor == work.job().anchor)
            {
                self.evaluate_cached(head.tip)?;
                uncles.push(head.tip);
            }
        }
        uncles.sort();
        uncles.truncate(usize::from(view.profile.manifest.max_uncles));
        Ok(uncles)
    }
    fn collection_keep(&self, extra: &BTreeSet<Key>) -> Result<BTreeSet<Key>> {
        let mut keep = extra.clone();
        for task in self.tasks.values() {
            keep.extend(task.leased.iter().copied());
        }
        for bytes in self.store.wallet_registrations()? {
            let registration: Registration = decode(&bytes)?;
            keep.insert(Key::new(Kind::Registration, id(&registration)?));
            let key = Key::new(Kind::Inventory, registration.fields.inventory);
            let inventory: Inventory = decode(&self.store.read(key)?)?;
            keep.insert(key);
            for row in inventory.rows.0 {
                for cid in row.coupon_ids.0 {
                    keep.insert(Key::new(Kind::Coupon, cid));
                }
            }
        }
        if let Some((_, facts)) = &self.warm {
            keep.extend(facts.pins());
        }
        Ok(keep)
    }
}
