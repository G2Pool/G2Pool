use super::*;
use crate::consensus::Session;

pub(super) struct Acquisition {
    pub(super) session: Session,
    pub(super) leased: BTreeSet<Key>,
    started: std::time::Instant,
    seeded: bool,
    checked_at: i64,
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
                    .poll(&source, &view, self.limits.clone(), self.cancel.clone(), 1)
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
    if node.recovery.len() >= node.limits.work {
        return Err(Error::Capacity("recovery queue"));
    }
    if !node.recovery.contains(&item) {
        node.recovery.push_back(item);
    }
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
    fn prepare_acquisition(&mut self, tip: Id) -> Result<Revision> {
        self.ensure_view()?;
        let expected = self.revision()?;
        let now = std::time::Instant::now();
        let expired: Vec<_> = self
            .tasks
            .iter()
            .filter(|(_, task)| {
                now.duration_since(task.started) >= std::time::Duration::from_secs(900)
            })
            .map(|(id, _)| *id)
            .collect();
        for id in expired {
            self.tasks.remove(&id);
            self.store.release_acquisition(id)?;
        }
        if !self.tasks.contains_key(&tip) {
            if self.tasks.len() + self.running.len() >= 8 {
                let victim = self
                    .tasks
                    .iter()
                    .filter(|(_, task)| task.session.cost().work == 0)
                    .min_by_key(|(_, task)| task.started)
                    .map(|(id, _)| *id)
                    .ok_or(Error::Capacity("acquisition tasks"))?;
                self.tasks.remove(&victim);
                self.store.release_acquisition(victim)?;
            }
            self.store
                .acquire(tip, &expected.anchor, &BTreeSet::new(), self.limits.facts)?;
            let session = Session::new(tip, expected.anchor.clone(), self.profile.protocol);
            self.tasks.insert(
                tip,
                Acquisition {
                    session,
                    leased: BTreeSet::new(),
                    started: now,
                    seeded: false,
                    checked_at: i64::MIN,
                },
            );
        }
        Ok(expected)
    }
    pub fn poll(&mut self, tip: Id) -> Result<EvaluationProgress> {
        self.release_abandoned()?;
        if self.running.contains_key(&tip) {
            return Ok(EvaluationProgress::Pending);
        }
        let expected = self.prepare_acquisition(tip)?;
        let chain = self.chain.clone();
        let view = View::pin(&chain, self.profile.clone(), chrono::Utc::now().timestamp())?;
        let task = self.tasks.get_mut(&tip).unwrap();
        task.checked_at = view.now;
        let outcome = task.session.poll(
            &self.store,
            &view,
            self.limits.clone(),
            self.cancel.clone(),
            1,
        );
        let task = self.tasks.remove(&tip).unwrap();
        self.finish_acquisition(tip, expected, task, outcome)
    }
    pub(crate) fn take_turn(&mut self, tip: Id) -> Result<Option<Turn>> {
        self.release_abandoned()?;
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
                recovery_origin: None,
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
        if task.started.elapsed() >= std::time::Duration::from_secs(900) {
            self.store.release_acquisition(tip)?;
            return Err(Error::Capacity("acquisition lifetime"));
        }
        let view = self.view()?;
        if view.head != previous.anchor || view.now < task.checked_at {
            self.store.release_acquisition(tip)?;
            return Err(Error::View);
        }
        let expected = self.revision()?;
        if expected.anchor != previous.anchor {
            self.store.release_acquisition(tip)?;
            return Err(Error::View);
        }
        if let Err(error) = outcome {
            self.store.release_acquisition(tip)?;
            return Err(error);
        }
        let progress = outcome.unwrap();
        if !task.seeded && task.session.cost().work > 0 {
            if let Some((anchor, facts)) = &self.warm {
                if *anchor == expected.anchor {
                    task.session.seed(facts);
                }
            }
            task.seeded = true;
        }
        let mut pins = task.session.acquired();
        if let EvaluationProgress::Missing(key) = &progress {
            pins.insert(*key);
            if key.kind == Kind::Core {
                pins.insert(Key::new(Kind::Authorization, key.id));
            }
        }
        let fresh: BTreeSet<_> = pins.difference(&task.leased).copied().collect();
        task.leased.extend(pins);
        if !fresh.is_empty() {
            if let Err(error) = self
                .store
                .acquire(tip, &expected.anchor, &fresh, self.limits.facts)
            {
                self.store.release_acquisition(tip)?;
                return Err(error);
            }
        }
        let memory = self.tasks.values().fold(
            self.warm
                .as_ref()
                .map_or(0, |(_, facts)| facts.bytes())
                .saturating_add(
                    self.dependency_closure
                        .as_ref()
                        .map_or(0, |closure| closure.retained_bytes()),
                )
                .saturating_add(self.running.values().map(|task| task.bytes).sum::<usize>()),
            |sum, task| sum.saturating_add(task.session.bytes() + task.leased.len() * 128),
        );
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
            if let Err(error) = self.publish_evaluation_share(result, &facts) {
                self.store.release_acquisition(tip)?;
                return Err(error);
            }
            if let Err(error) = self.store.promote_bound(
                &expected,
                result,
                choose,
                &task.session.witnesses(&result.required)?,
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
            self.tasks.insert(tip, task);
        }
        self.publish_snapshot()?;
        Ok(progress)
    }
    pub fn prepare_recovery(&mut self) -> Result<Option<Recovery>> {
        self.release_abandoned()?;
        self.ensure_view()?;
        let Some((origin, tip)) = self.recovery.pop_front() else {
            return Ok(None);
        };
        let turn = match self.take_turn(tip) {
            Ok(Some(turn)) => turn,
            Ok(None) | Err(Error::Capacity(_)) => {
                queue_recovery(self, (origin, tip))?;
                return Ok(None);
            }
            Err(error) => {
                self.recovery.push_back((origin, tip));
                return Err(error);
            }
        };
        if let Some(task) = self.running.get_mut(&tip) {
            task.recovery_origin = Some(origin);
        }
        Ok(Some(Recovery { origin, tip, turn }))
    }
    pub fn finish_recovery(&mut self, result: RecoveryResult) -> Result<()> {
        let RecoveryResult {
            recovery: Recovery { origin, tip, turn },
            outcome,
        } = result;
        let anchor = turn.expected.anchor.clone();
        match self.finish_turn(turn, outcome) {
            Ok(EvaluationProgress::Pending) => queue_recovery(self, (origin, tip))?,
            Ok(EvaluationProgress::Ready(_) | EvaluationProgress::Missing(_)) => {}
            Err(Error::Expired) => self.store.retire(origin)?,
            Err(Error::View) => {
                if self.view()?.head != anchor {
                    return Err(Error::View);
                }
                if self.recovery_seen.insert(tip) {
                    if self.recovery_seen.len() > self.limits.work {
                        return Err(Error::Capacity("recovery candidates"));
                    }
                    if let Parent::Receipt(parent) = self.store.parent_hint(tip)? {
                        queue_recovery(self, (origin, parent))?;
                    }
                }
            }
            Err(error) => {
                queue_recovery(self, (origin, tip))?;
                return Err(error);
            }
        }
        self.publish_snapshot()
    }
    pub fn acquisition_heads(&self) -> Result<Vec<Id>> {
        self.store.acquisitions(&self.revision()?.anchor)
    }
    pub fn protect_download(&mut self, tip: Id, keys: &[Key]) -> Result<()> {
        let anchor = self.revision()?.anchor;
        self.store.acquire(
            tip,
            &anchor,
            &keys.iter().copied().collect(),
            self.limits.facts,
        )?;
        if let Some(task) = self.tasks.get_mut(&tip) {
            task.leased.extend(keys.iter().copied());
        }
        Ok(())
    }
}
