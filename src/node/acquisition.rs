use super::*;
use crate::consensus::Session;

pub(super) struct Acquisition {
    pub(super) session: Session,
    pub(super) leased: BTreeSet<Key>,
    deadline: u64,
    checked_at: i64,
    checked_tip: bool,
    recovery_origin: Option<Id>,
}
pub(super) struct Running {
    bytes: usize,
    live: std::sync::Weak<()>,
    recovery_origin: Option<Id>,
}
pub(crate) struct Turn {
    tip: Id,
    _permit: crate::limits::WorkPermit,
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
    pub(super) fn release_abandoned(&mut self) -> Result<()> {
        let now = chrono::Utc::now().timestamp().max(0) as u64;
        let expired: Vec<_> = self
            .tasks
            .iter()
            .filter(|(_, task)| task.deadline <= now)
            .map(|(id, _)| *id)
            .collect();
        for id in expired {
            self.tasks.remove(&id);
            self.store.release_acquisition(id)?;
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
                .saturating_add(self.running.values().map(|task| task.bytes).sum::<usize>()),
            |sum, task| sum.saturating_add(task.session.bytes() + task.leased.len() * 128),
        )
    }
    fn retain_acquisition(&mut self, tip: Id, task: Acquisition) -> Result<()> {
        if self
            .acquisition_bytes()
            .saturating_add(task.session.bytes() + task.leased.len() * 128)
            > self.limits.bytes
        {
            self.store.release_acquisition(tip)?;
            return Err(Error::Capacity("acquisition memory"));
        }
        self.tasks.insert(tip, task);
        Ok(())
    }
    fn prepare_acquisition(&mut self, tip: Id) -> Result<Revision> {
        let expected = self.revision()?;
        if !self.tasks.contains_key(&tip) {
            if self.tasks.len() + self.running.len() >= 8 {
                let victim = self
                    .tasks
                    .iter()
                    .filter(|(_, task)| task.session.cost().work == 0)
                    .min_by_key(|(_, task)| task.deadline)
                    .map(|(id, _)| *id)
                    .ok_or(Error::Capacity("acquisition tasks"))?;
                self.tasks.remove(&victim);
                self.store.release_acquisition(victim)?;
            }
            let deadline = self
                .store
                .acquire(tip, &BTreeSet::new(), self.limits.facts)?;
            let mut session = Session::new(tip, expected.anchor.clone(), self.profile.protocol);
            if let Some((anchor, facts)) = &self.warm {
                if *anchor == expected.anchor {
                    session.seed(facts);
                }
            }
            self.tasks.insert(
                tip,
                Acquisition {
                    session,
                    leased: BTreeSet::new(),
                    deadline,
                    checked_at: i64::MIN,
                    checked_tip: false,
                    recovery_origin: None,
                },
            );
        }
        Ok(expected)
    }

    pub(crate) fn take_turn(&mut self, tip: Id) -> Result<Option<Turn>> {
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
        let Some(permit) = self.peer_work.acquire() else {
            return Ok(None);
        };
        let expected = self.prepare_acquisition(tip)?;
        let source = self.store.read_source()?;
        let task = self.tasks.remove(&tip).unwrap();
        let live = Arc::new(());
        self.running.insert(
            tip,
            Running {
                bytes: task.session.bytes() + task.leased.len() * 128,
                live: Arc::downgrade(&live),
                recovery_origin: task.recovery_origin,
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
        self.running.remove(&turn.tip);
        self.finish_acquisition(turn.tip, turn.expected, turn.task, outcome)
    }
    fn finish_acquisition(
        &mut self,
        tip: Id,
        previous: Revision,
        mut task: Acquisition,
        outcome: Result<EvaluationProgress>,
    ) -> Result<EvaluationProgress> {
        if task.deadline <= chrono::Utc::now().timestamp().max(0) as u64 {
            self.store.release_acquisition(tip)?;
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
        if !fresh.is_empty() {
            if let Err(error) = self.store.acquire(tip, &fresh, self.limits.facts) {
                self.store.release_acquisition(tip)?;
                return Err(error);
            }
        }
        task.leased.extend(pins);
        let view = self.view()?;
        if view.head != previous.anchor || view.now < task.checked_at {
            task.session.rebase(&view)?;
            self.retain_acquisition(tip, task)?;
            self.ensure_view()?;
            return Ok(EvaluationProgress::Pending);
        }
        let expected = self.revision()?;
        if expected.anchor != previous.anchor {
            self.retain_acquisition(tip, task)?;
            return Ok(EvaluationProgress::Pending);
        }
        if let Err(error) = outcome {
            self.store.release_acquisition(tip)?;
            return Err(error);
        }
        let progress = outcome.unwrap();
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
                    self.store.release_acquisition(tip)?;
                    return Err(Error::Expired);
                }
                task.checked_tip = true;
            }
        }
        let memory = self.acquisition_bytes();
        if memory.saturating_add(task.session.bytes() + task.leased.len() * 128) > self.limits.bytes
        {
            self.store.release_acquisition(tip)?;
            return Err(Error::Capacity("acquisition memory"));
        }
        if let EvaluationProgress::Ready(result) = &progress {
            let witness_metadata = Session::witness_count(&result.required)
                .checked_mul(std::mem::size_of::<Object>())
                .ok_or(Error::Capacity("publication witnesses"))?;
            if memory
                .saturating_add(task.session.bytes() + task.leased.len() * 128)
                .saturating_add(witness_metadata)
                > self.limits.bytes
            {
                self.store.release_acquisition(tip)?;
                return Err(Error::Capacity("publication witnesses"));
            }
            let incumbent = self.best;
            let facts = task.session.facts();
            let choose = match incumbent {
                None => true,
                Some(old) => self.prefer_candidate(result.tip, &facts, old)?,
            };
            if let Err(error) = self.store.promote_replacing(
                &expected,
                result,
                choose,
                &task.session.witnesses(&result.required)?,
                task.recovery_origin,
            ) {
                self.store.release_acquisition(tip)?;
                return Err(error);
            }
            self.validated.insert(tip);
            if choose {
                self.best = Some(tip);
                self.warm = Some((result.anchor.clone(), facts));
            }
            self.store.release_acquisition(tip)?;
        } else {
            self.retain_acquisition(tip, task)?;
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
        let mut turn = match self.take_turn(tip) {
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
        let RecoveryResult {
            recovery: Recovery { origin, tip, turn },
            outcome,
        } = result;
        let anchor = turn.expected.anchor.clone();
        match self.finish_turn(turn, outcome) {
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
        self.store.acquisitions()
    }
    pub fn protect_download(&mut self, tip: Id, keys: &[Key]) -> Result<()> {
        self.store
            .acquire(tip, &keys.iter().copied().collect(), self.limits.facts)?;
        if let Some(task) = self.tasks.get_mut(&tip) {
            task.leased.extend(keys.iter().copied());
        }
        Ok(())
    }
}
