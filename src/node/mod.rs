mod acquisition;
mod maintenance;
mod staging;
use acquisition::{Acquisition, Running};
pub use acquisition::{Recovery, RecoveryResult};
pub use maintenance::{Maintenance, MaintenanceResult};

use crate::{
    codec::*,
    consensus::{EvaluationProgress, Facts},
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

#[cfg(test)]
#[path = "mod_tests.rs"]
pub(crate) mod tests;
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
    pub limits: Limits,
    pub cancel: Arc<AtomicBool>,
    pub(crate) peer_work: Arc<crate::limits::WorkSlots>,
    pub(crate) serve_work: Arc<crate::limits::WorkSlots>,
    best: Option<Id>,
    validated: BTreeSet<Id>,
    tasks: BTreeMap<Id, Acquisition>,
    running: BTreeMap<Id, Running>,
    warm: Option<(Anchor, Facts)>,
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
    pub fn start(
        chain: Arc<Chain>,
        profile: Profile,
        mut store: Store,
        limits: Limits,
    ) -> Result<Self> {
        if u64::from(grin_core::global::cut_through_horizon()) < 2 * profile.lifetime - 2 {
            return Err(Error::NativeUnavailable(
                "native rewind horizon is shorter than pool finance horizon".into(),
            ));
        }
        if profile.pool_id != store.pool_id() {
            return Err(Error::Invalid("node pool identity"));
        }
        store.attach_native(chain.clone(), profile.clone())?;
        let peers = crate::network::peers::Book::memory(profile.network >= 2);
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
            limits,
            cancel: Arc::new(AtomicBool::new(false)),
            peer_work: crate::limits::WorkSlots::new(2),
            serve_work: crate::limits::WorkSlots::new(1),
            best: None,
            validated: BTreeSet::new(),
            tasks: BTreeMap::new(),
            running: BTreeMap::new(),
            warm: None,
            recovery: VecDeque::new(),
            recovery_seen: BTreeSet::new(),
            sync_cursor: None,
        };
        node.begin_refresh()?;
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
        let recovering = self.best.is_none() && !self.store.heads()?.is_empty();
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
    fn prefer_candidate(&self, tip: Id, facts: &Facts, incumbent: Id) -> Result<bool> {
        let (anchor, incumbent_facts) = self.warm.as_ref().ok_or(Error::View)?;
        if *anchor != self.revision()?.anchor {
            return Err(Error::View);
        }
        Ok(matches!(
            self.compare_verified_branches(tip, facts, incumbent, incumbent_facts),
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

    pub(crate) fn knows_head(&self, tip: Id) -> bool {
        self.validated.contains(&tip)
            || self.warm.as_ref().is_some_and(|(_, facts)| {
                facts.branch().is_some_and(|branch| branch.contains(&tip))
            })
    }
    pub(crate) fn compare_verified_branches(
        &self,
        left: Id,
        left_facts: &Facts,
        right: Id,
        right_facts: &Facts,
    ) -> ForkComparison {
        let (Some(a), Some(b)) = (left_facts.branch(), right_facts.branch()) else {
            return ForkComparison::Deferred;
        };
        if a.frames.front().map(|f| f.id()) != Some(left)
            || b.frames.front().map(|f| f.id()) != Some(right)
        {
            return ForkComparison::Deferred;
        }
        let Ok(view) = self.view() else {
            return ForkComparison::Deferred;
        };
        let window = self.profile.work_window_limit();
        let low = self.profile.work_low(view.head.height);
        let positions: BTreeMap<_, _> = b
            .frames
            .iter()
            .enumerate()
            .map(|(i, f)| (f.id(), i))
            .collect();
        let common = a
            .frames
            .iter()
            .enumerate()
            .find_map(|(i, f)| positions.get(&f.id()).map(|j| (i, *j)));
        let suffix = |branch: &crate::consensus::Ledger, n: usize| -> Option<Vec<u128>> {
            branch
                .frames
                .iter()
                .take(n)
                .enumerate()
                .map(|(index, frame)| {
                    frame
                        .credited(index, window, low)
                        .try_fold(0u128, |sum, c| sum.checked_add(u128::from(c.difficulty)))
                })
                .collect()
        };
        if let Some((i, j)) = common {
            let (Some(a), Some(b)) = (suffix(a, i), suffix(b, j)) else {
                return ForkComparison::Deferred;
            };
            return if view.check().is_ok() {
                compare_suffix_work(&a, &b, left, right)
            } else {
                ForkComparison::Deferred
            };
        }
        let (Ok(a_work), Ok(b_work)) = (a.window_score(window, low), b.window_score(window, low))
        else {
            return ForkComparison::Deferred;
        };
        if a_work <= b_work {
            return ForkComparison::PreferRight;
        }
        match self.fresh_alternative(a, left_facts, b, right_facts) {
            Ok(true) => ForkComparison::PreferLeft,
            Ok(false) => ForkComparison::PreferRight,
            Err(_) => ForkComparison::Deferred,
        }
    }
    fn fresh_alternative(
        &self,
        candidate: &crate::consensus::Ledger,
        candidate_facts: &Facts,
        incumbent: &crate::consensus::Ledger,
        incumbent_facts: &Facts,
    ) -> Result<bool> {
        let view = self.view()?;
        let window = self.profile.work_window_limit();
        let low = self.profile.work_low(view.head.height);
        let mut anchors = BTreeSet::new();
        let mut timestamps = Vec::new();
        let mut oldest = u64::MAX;
        let mut newest = 0;
        for contribution in candidate.recent(window, low) {
            let work = candidate_facts.work(contribution.id).ok_or(Error::View)?;
            let header = view.anchor(&work.job().anchor)?;
            if work.job().timestamp.abs_diff(header.timestamp.timestamp())
                > crate::native::MAX_ANCHOR_TIMESTAMP_DEVIATION
            {
                return Ok(false);
            }
            anchors.insert(work.job().anchor.hash);
            oldest = oldest.min(header.height);
            newest = newest.max(header.height);
            timestamps.push(contribution.timestamp);
        }
        let incumbent_anchors: BTreeSet<_> = incumbent
            .recent(window, low)
            .map(|c| {
                incumbent_facts
                    .work(c.id)
                    .map(|w| w.job().anchor.hash)
                    .ok_or(Error::View)
            })
            .collect::<Result<_>>()?;
        if view.head.height.saturating_sub(newest) > self.profile.alternative_age()
            || view.head.height.saturating_sub(oldest) > self.profile.anchor_window()
            || anchors.len() * 2 < incumbent_anchors.len()
        {
            return Ok(false);
        }
        let primary_count = candidate
            .recent(window, low)
            .filter(|c| c.class == 0)
            .count() as u64;
        if (candidate.boundary.is_none() && incumbent.boundary.is_none())
            || primary_count * u64::from(self.profile.share_target_seconds)
                < grin_core::consensus::BLOCK_TIME_SEC
        {
            view.check()?;
            return Ok(true);
        }
        timestamps.sort_unstable();
        let cut = timestamps.len().div_ceil(10);
        if cut == 0 || cut > timestamps.len() - cut {
            return Ok(false);
        }
        let span = u128::from(timestamps[timestamps.len() - cut] - timestamps[cut - 1]);
        let native_span =
            u128::from(newest - oldest + 1) * u128::from(grin_core::consensus::BLOCK_TIME_SEC) * 8
                / 10;
        view.check()?;
        Ok(span * 3 >= native_span * 2 && span * 3 <= native_span * 4)
    }
    pub(crate) fn verified_facts(&self, anchor: &Anchor) -> Option<&Facts> {
        self.warm
            .as_ref()
            .filter(|(verified, _)| verified == anchor)
            .map(|(_, facts)| facts)
    }
    pub fn ensure_view(&mut self) -> Result<()> {
        let head = self.view()?.head;
        if self.revision()?.anchor != head {
            self.begin_refresh()?;
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
        self.release_abandoned()?;
        let incumbent = self
            .best
            .or(self.store.revision()?.and_then(|revision| revision.tip))
            .or_else(|| self.recovery.front().map(|(origin, _)| *origin));
        let head = self.view()?.head;
        let view = View::pin(
            &self.chain,
            self.profile.clone(),
            chrono::Utc::now().timestamp(),
        )?;
        for task in self.tasks.values_mut() {
            task.session.rebase(&view)?;
        }
        view.check()?;
        self.rebase_facts(&head)?;
        self.best = None;
        self.validated.clear();
        self.recovery_seen.clear();
        self.store.fence(&head)?;
        self.store.clear_selection()?;
        let mut heads = self.store.heads()?;
        heads.sort_by_key(|head| Some(head.tip) != incumbent);
        self.recovery = heads.into_iter().map(|head| (head.tip, head.tip)).collect();
        if self.recovery.len() > self.limits.work {
            return Err(Error::Capacity("recovery roots"));
        }
        self.publish_snapshot()
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
