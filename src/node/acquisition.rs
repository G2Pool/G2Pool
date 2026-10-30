use super::*;
use crate::consensus::Session;
use crate::storage::sharechain::MAX_ACQUISITIONS;
use std::time::Instant;

pub(super) struct Acquisition {
    pub(super) session: Session,
    pub(super) leased: BTreeSet<Key>,
    pub(super) deadline: u64,
    checked_at: i64,
    checked_tip: bool,
    recovery_origin: Option<Id>,
    pub(super) waiting: Option<Key>,
    last_progress: Instant,
    pub(super) owner: Arc<()>,
    pub(super) critical: bool,
}
pub(super) struct Running {
    bytes: usize,
    live: std::sync::Weak<()>,
    recovery_origin: Option<Id>,
    owner: std::sync::Weak<()>,
    critical: bool,
}
pub(crate) struct Turn {
    tip: Id,
    _permit: Option<crate::limits::WorkPermit>,
    _live: Arc<()>,
    expected: Revision,
    task: Acquisition,
    source: crate::storage::ReadSource,
    chain: Arc<Chain>,
    profile: Profile,
    limits: Limits,
    cancel: Arc<AtomicBool>,
}
impl Turn {
    pub(crate) fn run(mut self) -> (Self, Result<EvaluationProgress>) {
        let progress = self.task.session.progress();
        let outcome = self.source.open().and_then(|source| {
            View::pin(
                &self.chain,
                self.profile.clone(),
                chrono::Utc::now().timestamp(),
            )
            .and_then(|view| {
                self.task.checked_at = view.now;
                self.task
                    .session
                    .poll(&source, &view, self.limits.clone(), self.cancel.clone(), 64)
            })
        });
        if self.task.session.progress() != progress {
            self.task.last_progress = Instant::now();
        }
        (self, outcome)
    }
}
pub struct Recovery {
    origin: Id,
    tip: Id,
    turn: Turn,
}
pub struct RecoveryResult {
    recovery: Recovery,
    outcome: Result<EvaluationProgress>,
}
fn queue_recovery(node: &mut Node, item: (Id, Id)) -> Result<()> {
    if node.recovery.contains(&item) {
        return Ok(());
    }
    if node.recovery.len() >= node.limits.work {
        return Err(Error::Capacity("recovery queue"));
    }
    node.recovery.push_back(item);
    Ok(())
}
impl Recovery {
    pub fn run(mut self) -> RecoveryResult {
        let (turn, outcome) = self.turn.run();
        self.turn = turn;
        RecoveryResult {
            recovery: self,
            outcome,
        }
    }
}
impl Node {
    fn acquisition_satisfied(&self, tip: Id, task: &Acquisition, view: &View<'_>) -> Result<bool> {
        if self.best.is_some() && self.revision()?.anchor == view.head && self.knows_head(tip) {
            return Ok(true);
        }
        if let Some(anchor) = task.session.tip_anchor() {
            if anchor.height < self.profile.finance_low(view.head.height) {
                return Err(Error::Expired);
            }
            if anchor.height <= view.head.height {
                view.anchor(anchor)?;
            }
        }
        Ok(false)
    }
    pub(super) fn release_obsolete(&mut self) -> Result<()> {
        let view = self.view()?;
        let mut obsolete = Vec::new();
        for (tip, task) in &self.tasks {
            match self.acquisition_satisfied(*tip, task, &view) {
                Ok(true) | Err(Error::View | Error::Expired) => {
                    obsolete.push((*tip, task.recovery_origin))
                }
                Ok(false) | Err(Error::NativeUnavailable(_)) => {}
                Err(error) => return Err(error),
            }
        }
        view.check()?;
        for (tip, origin) in obsolete {
            self.store.release_acquisition(tip)?;
            self.tasks.remove(&tip);
            if let Some(origin) = origin {
                queue_recovery(self, (origin, tip))?;
            }
        }
        Ok(())
    }
    pub(super) fn release_abandoned(&mut self) -> Result<()> {
        let now = chrono::Utc::now().timestamp().max(0) as u64;
        let expired: Vec<_> = self
            .tasks
            .iter()
            .filter(|(_, task)| task.deadline <= now)
            .map(|(id, _)| *id)
            .collect();
        for id in expired {
            self.store.release_acquisition(id)?;
            self.tasks.remove(&id);
        }
        let abandoned: Vec<_> = self
            .running
            .iter()
            .filter(|(_, task)| task.live.strong_count() == 0)
            .map(|(tip, task)| (*tip, task.recovery_origin))
            .collect();
        for (tip, origin) in abandoned {
            self.store.release_acquisition(tip)?;
            self.running.remove(&tip);
            if let Some(origin) = origin {
                queue_recovery(self, (origin, tip))?;
            }
        }
        Ok(())
    }
    fn acquisition_bytes(&self) -> usize {
        self.tasks.values().fold(
            self.warm
                .as_ref()
                .map_or(0, |(_, facts)| facts.bytes())
                .saturating_add(
                    self.running
                        .values()
                        .filter(|task| !task.critical)
                        .map(|task| task.bytes)
                        .sum::<usize>(),
                ),
            |sum, task| {
                if task.critical {
                    sum
                } else {
                    sum.saturating_add(task.session.bytes() + task.leased.len() * 128)
                }
            },
        )
    }
    pub(super) fn protect_with_room(
        &mut self,
        tip: Id,
        keys: &BTreeSet<Key>,
        bytes: usize,
        critical: bool,
    ) -> Result<u64> {
        if bytes > self.limits.bytes {
            return Err(Error::Capacity("evaluation cache"));
        }
        let leases = self.store.acquisition_leases()?;
        let now = chrono::Utc::now().timestamp().max(0) as u64;
        let mut count = leases
            .iter()
            .filter(|(id, expires)| {
                *id != tip
                    && *expires > now
                    && (critical
                        || !self.tasks.get(id).is_some_and(|task| task.critical)
                            && !self.running.get(id).is_some_and(|task| task.critical))
            })
            .count();
        let mut memory = self.acquisition_bytes().saturating_add(bytes);
        let limit = MAX_ACQUISITIONS - usize::from(!critical);
        let mut victims: Vec<_> = leases
            .iter()
            .filter(|(id, _)| {
                *id != tip
                    && !self.running.contains_key(id)
                    && self
                        .tasks
                        .get(id)
                        .is_none_or(|task| !task.critical && Arc::strong_count(&task.owner) == 1)
            })
            .map(|(id, _)| *id)
            .collect();
        victims.sort_by_key(|id| {
            self.tasks
                .get(id)
                .map(|task| (task.waiting.is_none(), task.last_progress))
        });
        let mut replaced = Vec::new();
        for victim in victims {
            if count < limit && (critical || memory <= self.limits.bytes) {
                break;
            }
            if let Some(task) = self.tasks.get(&victim) {
                memory = memory.saturating_sub(task.session.bytes() + task.leased.len() * 128);
            }
            if leases
                .iter()
                .any(|(id, expires)| *id == victim && *expires > now)
            {
                count = count.saturating_sub(1);
            }
            replaced.push(victim);
        }
        if count >= limit || !critical && memory > self.limits.bytes {
            return Err(Error::Capacity("acquisition capacity in use"));
        }
        let deadline = if replaced.is_empty() && keys.is_empty() {
            match leases
                .iter()
                .find(|(id, expires)| *id == tip && *expires > now)
            {
                Some((_, deadline)) => *deadline,
                None => self
                    .store
                    .replace_acquisition(tip, keys, self.limits.facts, &replaced)?,
            }
        } else {
            self.store
                .replace_acquisition(tip, keys, self.limits.facts, &replaced)?
        };
        for id in replaced {
            if let Some(task) = self.tasks.remove(&id) {
                if let Some(origin) = task.recovery_origin {
                    queue_recovery(self, (origin, id))?;
                }
            }
        }
        Ok(deadline)
    }
    fn retain_acquisition(
        &mut self,
        tip: Id,
        task: Acquisition,
        fresh: &BTreeSet<Key>,
    ) -> Result<()> {
        self.protect_with_room(
            tip,
            fresh,
            task.session.bytes() + task.leased.len() * 128,
            task.critical,
        )?;
        self.tasks.insert(tip, task);
        Ok(())
    }
    fn prepare_acquisition(&mut self, tip: Id, critical: bool) -> Result<Revision> {
        let expected = self.revision()?;
        if !self.tasks.contains_key(&tip) {
            let mut session = Session::new(tip, expected.anchor.clone(), self.profile.pool_id);
            if let Some((anchor, facts)) = &self.warm {
                if *anchor == expected.anchor {
                    session.seed(facts);
                }
            }
            let deadline =
                self.protect_with_room(tip, &BTreeSet::new(), session.bytes(), critical)?;
            self.tasks.insert(
                tip,
                Acquisition {
                    session,
                    leased: BTreeSet::new(),
                    deadline,
                    checked_at: i64::MIN,
                    checked_tip: false,
                    recovery_origin: None,
                    waiting: None,
                    last_progress: Instant::now(),
                    owner: Arc::new(()),
                    critical,
                },
            );
        }
        self.tasks.get_mut(&tip).unwrap().critical = critical;
        Ok(expected)
    }

    pub(crate) fn take_turn(&mut self, tip: Id) -> Result<Option<Turn>> {
        self.take_turn_inner(tip, false)
    }
    pub(crate) fn take_local_turn(&mut self, tip: Id) -> Result<Option<Turn>> {
        self.ensure_view()?;
        if self.best.is_none() && !self.recovery.is_empty() {
            return Err(Error::View);
        }
        self.take_turn_inner(tip, true)
    }
    fn take_turn_inner(&mut self, tip: Id, critical: bool) -> Result<Option<Turn>> {
        self.release_abandoned()?;
        self.ensure_view()?;
        if self.best.is_none()
            && (self.recovery.front().is_some_and(|(_, next)| *next != tip)
                || self
                    .running
                    .values()
                    .any(|task| task.recovery_origin.is_some()))
        {
            return Ok(None);
        }
        if self.running.contains_key(&tip) {
            return Ok(None);
        }
        if self
            .tasks
            .get(&tip)
            .is_some_and(|task| task.critical && !critical)
        {
            return Ok(None);
        }
        let permit = if critical {
            None
        } else {
            let Some(permit) = self.peer_work.acquire() else {
                return Ok(None);
            };
            Some(permit)
        };
        let expected = self.prepare_acquisition(tip, critical)?;
        let source = self.store.read_source()?;
        let task = self.tasks.remove(&tip).unwrap();
        let live = Arc::new(());
        self.running.insert(
            tip,
            Running {
                bytes: task.session.bytes() + task.leased.len() * 128,
                live: Arc::downgrade(&live),
                recovery_origin: task.recovery_origin,
                owner: Arc::downgrade(&task.owner),
                critical,
            },
        );
        Ok(Some(Turn {
            tip,
            _permit: permit,
            _live: live,
            expected,
            task,
            source,
            chain: self.chain.clone(),
            profile: self.profile.clone(),
            limits: self.limits.clone(),
            cancel: self.cancel.clone(),
        }))
    }
    pub(crate) fn finish_turn(
        &mut self,
        turn: Turn,
        outcome: Result<EvaluationProgress>,
    ) -> Result<EvaluationProgress> {
        self.ensure_view()?;
        let tip = turn.tip;
        self.running.remove(&tip);
        let result = self.finish_acquisition(tip, turn.expected, turn.task, outcome);
        if !self.tasks.contains_key(&tip) {
            self.store.release_acquisition(tip)?;
        }
        result
    }
    fn finish_acquisition(
        &mut self,
        tip: Id,
        previous: Revision,
        mut task: Acquisition,
        outcome: Result<EvaluationProgress>,
    ) -> Result<EvaluationProgress> {
        let view = self.view()?;
        let satisfied = self.acquisition_satisfied(tip, &task, &view);
        view.check()?;
        if satisfied? {
            return Ok(EvaluationProgress::Known);
        }
        if task.deadline <= chrono::Utc::now().timestamp().max(0) as u64 {
            return Err(Error::Capacity("acquisition lifetime"));
        }
        let mut pins = task.session.acquired();
        if let Ok(EvaluationProgress::Missing(key)) = &outcome {
            pins.insert(*key);
            if key.kind == Kind::Core {
                pins.insert(Key::new(Kind::Authorization, key.id));
            }
        }
        let fresh: BTreeSet<_> = pins.difference(&task.leased).copied().collect();
        task.leased.extend(pins);
        let view = self.view()?;
        if view.head != previous.anchor || view.now < task.checked_at {
            task.session.rebase(&view)?;
            self.retain_acquisition(tip, task, &fresh)?;
            self.ensure_view()?;
            return Ok(EvaluationProgress::Pending);
        }
        let expected = self.revision()?;
        if expected.anchor != previous.anchor {
            self.retain_acquisition(tip, task, &fresh)?;
            return Ok(EvaluationProgress::Pending);
        }
        let progress = outcome?;
        task.waiting = match &progress {
            EvaluationProgress::Missing(key) => Some(*key),
            _ => None,
        };
        if !task.checked_tip {
            let facts = task.session.facts();
            if let Some(work) = facts.work(tip) {
                if self.best.is_none()
                    && task.recovery_origin.is_none()
                    && expected
                        .anchor
                        .height
                        .saturating_sub(work.job().anchor.height)
                        > self.profile.bootstrap_age()
                {
                    return Err(Error::Expired);
                }
                task.checked_tip = true;
            }
        }
        if let EvaluationProgress::Ready(result) = &progress {
            let witness_metadata = Session::witness_count(&result.required)
                .checked_mul(std::mem::size_of::<Object>())
                .ok_or(Error::Capacity("publication witnesses"))?;
            self.protect_with_room(
                tip,
                &fresh,
                task.session.bytes() + task.leased.len() * 128 + witness_metadata,
                task.critical,
            )?;
            let incumbent = self.best;
            let facts = task.session.facts();
            let choose = match incumbent {
                None => true,
                Some(old) => self.prefer_candidate(result.tip, &facts, old)?,
            };
            self.store.promote_replacing(
                &expected,
                result,
                choose,
                &task.session.witnesses(&result.required)?,
                task.recovery_origin,
            )?;
            self.validated.insert(tip);
            if choose {
                self.best = Some(tip);
                self.warm = Some((result.anchor.clone(), facts));
            }
        } else {
            self.retain_acquisition(tip, task, &fresh)?;
        }
        self.publish_snapshot()?;
        Ok(progress)
    }
    pub fn prepare_recovery(&mut self) -> Result<Option<Recovery>> {
        self.release_abandoned()?;
        self.ensure_view()?;
        let Some(&(origin, tip)) = self.recovery.front() else {
            return Ok(None);
        };
        if self.waiting_for(tip)?.is_some() {
            return Ok(None);
        }
        let mut turn = match self.take_turn_inner(tip, true) {
            Ok(Some(turn)) => turn,
            Ok(None) | Err(Error::Capacity(_)) => return Ok(None),
            Err(error) => return Err(error),
        };
        if let Some(task) = self.running.get_mut(&tip) {
            task.recovery_origin = Some(origin);
        }
        turn.task.recovery_origin = Some(origin);
        Ok(Some(Recovery { origin, tip, turn }))
    }
    pub fn finish_recovery(&mut self, result: RecoveryResult) -> Result<()> {
        let tip = result.recovery.tip;
        let outcome = self.finish_recovery_inner(result);
        self.release_critical(tip)?;
        outcome
    }
    fn finish_recovery_inner(&mut self, result: RecoveryResult) -> Result<()> {
        let RecoveryResult {
            recovery: Recovery { origin, tip, turn },
            outcome,
        } = result;
        let anchor = turn.expected.anchor.clone();
        match self.finish_turn(turn, outcome) {
            Ok(EvaluationProgress::Known) => {
                if origin != tip && self.revision()?.tip != Some(origin) {
                    self.store.retire(origin)?;
                }
                self.recovery.retain(|item| *item != (origin, tip));
            }
            Ok(EvaluationProgress::Missing(_)) if tip != origin => {
                self.tasks.remove(&tip);
                self.store.release_acquisition(tip)?;
                self.store.retire(origin)?;
                self.recovery.retain(|item| *item != (origin, tip));
            }
            Ok(EvaluationProgress::Pending | EvaluationProgress::Missing(_)) => {
                queue_recovery(self, (origin, tip))?;
            }
            Ok(EvaluationProgress::Ready(_)) => {
                self.recovery.retain(|item| *item != (origin, tip));
            }
            Err(Error::Expired) => {
                self.store.retire(origin)?;
                self.recovery.retain(|item| *item != (origin, tip));
            }
            Err(Error::View) => {
                if self.native_sync.is_syncing() {
                    return Err(Error::NativeUnavailable(
                        "Grin node is synchronizing".into(),
                    ));
                }
                if self.view()?.head != anchor {
                    return Err(Error::View);
                }
                self.recovery_seen.insert(tip);
                if self.recovery_seen.len() > self.limits.work {
                    queue_recovery(self, (origin, tip))?;
                    return Err(Error::Capacity("recovery candidates"));
                }
                if let Parent::Receipt(parent) = self.store.parent_hint(tip)? {
                    queue_recovery(self, (origin, parent))?;
                } else {
                    self.store.retire(origin)?;
                }
                self.recovery.retain(|item| *item != (origin, tip));
            }
            Err(error) => {
                queue_recovery(self, (origin, tip))?;
                return Err(error);
            }
        }
        self.publish_snapshot()
    }
    pub fn acquisition_heads(&self) -> Result<Vec<Id>> {
        Ok(self
            .store
            .acquisitions()?
            .into_iter()
            .chain(self.recovery.iter().map(|(_, tip)| *tip))
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect())
    }
    pub(crate) fn waiting_for(&mut self, tip: Id) -> Result<Option<Key>> {
        self.release_abandoned()?;
        self.ensure_view()?;
        if let Some(task) = self.tasks.get_mut(&tip) {
            if let Some(key) = task.waiting {
                match self.store.byte_length(key) {
                    Ok(_) => task.waiting = None,
                    Err(Error::Unavailable(_)) => return Ok(Some(key)),
                    Err(error) => return Err(error),
                }
            }
        }
        Ok(None)
    }
    pub(crate) fn release_critical(&mut self, tip: Id) -> Result<()> {
        self.release_abandoned()?;
        if !self.tasks.get(&tip).is_some_and(|task| task.critical) {
            return Ok(());
        }
        if let Some(mut task) = self.tasks.remove(&tip) {
            task.critical = false;
            if let Err(error) = self.retain_acquisition(tip, task, &BTreeSet::new()) {
                self.store.release_acquisition(tip)?;
                return Err(error);
            }
        }
        Ok(())
    }
    pub(crate) fn reserve_download(
        &mut self,
        tip: Id,
        address: std::net::SocketAddr,
        keys: Vec<Key>,
        previous: Option<&crate::network::download::Lease>,
    ) -> Result<Option<crate::network::download::Lease>> {
        self.release_abandoned()?;
        let owner = self
            .tasks
            .get(&tip)
            .map(|task| task.owner.clone())
            .or_else(|| self.running.get(&tip).and_then(|task| task.owner.upgrade()));
        if let Some(previous) = previous {
            if !previous
                .owner
                .as_ref()
                .zip(owner.as_ref())
                .is_some_and(|(old, current)| Arc::ptr_eq(old, current))
            {
                return Err(Error::Cancelled);
            }
        }
        if owner.is_none()
            && keys
                .iter()
                .any(|key| key.id != tip || !matches!(key.kind, Kind::Core | Kind::Authorization))
        {
            return Err(Error::Cancelled);
        }
        let Some(mut lease) =
            crate::network::download::Downloads::reserve(&self.downloads, address, keys.clone())
        else {
            return Ok(None);
        };
        if owner.is_some() {
            self.store
                .acquire(tip, &keys.iter().copied().collect(), self.limits.facts)?;
            if let Some(task) = self.tasks.get_mut(&tip) {
                task.leased.extend(keys);
            }
        }
        lease.owner = owner;
        Ok(Some(lease))
    }
}
