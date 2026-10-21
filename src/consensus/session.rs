use super::*;

pub enum EvaluationProgress {
    Pending,
    Missing(Key),
    Ready(Box<Evaluation>),
}

pub struct Session {
    tip: Id,
    anchor: Anchor,
    pool_id: Id,
    cursor: Option<Id>,
    previous: u64,
    ranked: bool,
    ledger: Ledger,
    reuse: Option<Ledger>,
    reuse_index: Option<usize>,
    incremental: bool,
    full: BTreeSet<Id>,
    primary: BTreeSet<Id>,
    pending: std::collections::VecDeque<Id>,
    facts: Facts,
    required: BTreeSet<Key>,
    budget: Option<Budget>,
    acquired: BTreeSet<Key>,
    required_traces: BTreeMap<usize, Arc<Trace>>,
    history: BTreeMap<Id, (usize, usize)>,
}
impl Session {
    pub fn new(tip: Id, anchor: Anchor, pool_id: Id) -> Self {
        Self {
            tip,
            previous: anchor.height,
            ledger: Ledger::new(anchor.height),
            anchor,
            pool_id,
            cursor: Some(tip),
            ranked: false,
            reuse: None,
            reuse_index: None,
            incremental: false,
            full: BTreeSet::new(),
            primary: BTreeSet::new(),
            pending: std::collections::VecDeque::new(),
            facts: Facts::default(),
            required: BTreeSet::new(),
            budget: None,
            acquired: BTreeSet::new(),
            required_traces: BTreeMap::new(),
            history: BTreeMap::new(),
        }
    }
    pub(crate) fn seed(&mut self, facts: &Facts) {
        self.acquired.extend(facts.pins());
        self.facts = self.facts.warmed(facts);
        self.inherit_topology(self.facts.topology.clone());
    }
    pub(super) fn inherit_topology(&mut self, topology: Option<Arc<Ledger>>) {
        if !self.ranked {
            self.reuse = topology.as_ref().map(|ledger| ledger.as_ref().clone());
            self.incremental = self.reuse.is_some();
        }
    }
    pub(crate) fn rebase(&mut self, view: &View<'_>) -> Result<()> {
        if self.anchor == view.head {
            return Ok(());
        }
        let extension = self.anchor.height <= view.head.height && view.anchor(&self.anchor).is_ok();
        if extension {
            self.ledger.rebase(view.head.height)?;
            if let Some(reuse) = &mut self.reuse {
                reuse.rebase(view.head.height)?;
            }
            if self.ranked {
                self.full = self.ledger.full(view.profile.finance_low(view.head.height));
                self.pending.retain(|id| self.full.contains(id));
            }
        } else {
            let loaded = std::mem::take(&mut self.facts.loaded);
            let leaves = std::mem::take(&mut self.facts.leaves);
            let payload_bytes = self.facts.payload_bytes;
            self.facts = Facts {
                loaded,
                leaves,
                payload_bytes,
                ..Facts::default()
            };
            self.cursor = Some(self.tip);
            self.previous = view.head.height;
            self.ranked = false;
            self.ledger = Ledger::new(view.head.height);
            self.reuse = None;
            self.reuse_index = None;
            self.incremental = false;
            self.full.clear();
            self.primary.clear();
            self.pending.clear();
            self.required.clear();
            self.required_traces.clear();
            self.history.clear();
        }
        view.check()?;
        self.anchor = view.head.clone();
        Ok(())
    }
    pub(crate) fn facts(&self) -> Facts {
        self.facts.clone()
    }
    pub(crate) fn acquired(&mut self) -> BTreeSet<Key> {
        std::mem::take(&mut self.acquired)
    }
    pub fn bytes(&self) -> usize {
        self.facts
            .bytes()
            .saturating_add(self.ledger.bytes())
            .saturating_add(self.reuse.as_ref().map_or(0, Ledger::bytes))
            .saturating_add(
                self.required_traces.len() * 128
                    + self.history.len() * 128
                    + self.acquired.len() * 128,
            )
            .saturating_add((self.full.len() + self.pending.len()) * 96 + self.required.len() * 128)
    }
    pub(crate) fn witness_count(required: &BTreeSet<Key>) -> usize {
        required
            .iter()
            .filter(|key| matches!(key.kind, Kind::Authorization | Kind::Residual))
            .count()
    }
    pub(crate) fn witnesses(
        &self,
        required: &BTreeSet<Key>,
    ) -> Result<Vec<crate::storage::Object>> {
        let mut witnesses = Vec::with_capacity(Self::witness_count(required));
        for key in required
            .iter()
            .filter(|key| matches!(key.kind, Kind::Authorization | Kind::Residual))
        {
            let bytes = self
                .facts
                .loaded
                .get(key)
                .cloned()
                .ok_or(Error::Unavailable(*key))?;
            witnesses.push(crate::storage::Object { key: *key, bytes });
        }
        Ok(witnesses)
    }
    pub fn cost(&self) -> Cost {
        self.budget.as_ref().map_or(Cost::default(), |b| b.cost)
    }
    pub fn poll<S: Source>(
        &mut self,
        source: &S,
        view: &View<'_>,
        limits: Limits,
        cancel: Arc<AtomicBool>,
        quantum: usize,
    ) -> Result<EvaluationProgress> {
        if view.profile.pool_id != self.pool_id {
            return Err(Error::View);
        }
        self.rebase(view)?;
        let ceiling = limits.bytes;
        let budget = self
            .budget
            .take()
            .unwrap_or_else(|| Budget::new(limits, cancel));
        let mut validator = Validator {
            source,
            view,
            budget,
            required: std::mem::take(&mut self.required),
            facts: std::mem::take(&mut self.facts),
            reuse_plans: self.incremental,
            traces: vec![],
            acquired: BTreeSet::new(),
            required_traces: std::mem::take(&mut self.required_traces),
            history: std::mem::take(&mut self.history),
        };
        validator.budget.resume();
        let outcome = self.advance(&mut validator, quantum.max(1));
        self.budget = Some(validator.budget);
        self.required = validator.required;
        self.facts = validator.facts;
        self.acquired.extend(validator.acquired);
        self.required_traces = validator.required_traces;
        self.history = validator.history;
        if self.bytes() > ceiling {
            return Err(Error::Capacity("evaluation cache"));
        }
        match outcome {
            Ok(Some(result)) => Ok(EvaluationProgress::Ready(Box::new(result))),
            Ok(None) => Ok(EvaluationProgress::Pending),
            Err(Error::Unavailable(key)) => Ok(EvaluationProgress::Missing(key)),
            Err(Error::Capacity("evaluation time")) => Ok(EvaluationProgress::Pending),
            Err(error) => Err(error),
        }
    }
    fn finish_ranking(&mut self, low: u64) {
        self.ranked = true;
        self.full = self.ledger.full(low);
        self.primary = self.ledger.primary();
        self.pending = self.full.iter().copied().collect();
    }
    pub(super) fn advance<S: Source>(
        &mut self,
        v: &mut Validator<'_, '_, S>,
        quantum: usize,
    ) -> Result<Option<Evaluation>> {
        v.view.check()?;
        let tip = v.work(self.tip)?;
        if tip.job().anchor.height < v.view.profile.finance_low(self.anchor.height) {
            return Err(Error::Expired);
        }
        for _ in 0..quantum {
            v.traces.clear();
            v.budget.check()?;
            v.budget
                .retained(v.retained_bytes().saturating_add(self.bytes()))?;
            if !self.ranked {
                let Some(id) = self.cursor else {
                    self.finish_ranking(v.view.profile.finance_low(self.anchor.height));
                    continue;
                };
                let core = v.work(id)?;
                let height = core.job().anchor.height;
                if self.ledger.contains(&id) || height > self.previous {
                    return Err(Error::Invalid("spine cycle/regression"));
                }
                if self.ledger.frames.len() == v.view.profile.work_window_limit() {
                    self.ledger.boundary = Some(id);
                    self.finish_ranking(v.view.profile.finance_low(self.anchor.height));
                    continue;
                }
                if let Some(reuse) = &mut self.reuse {
                    if self.reuse_index.is_none() {
                        reuse.rebase(self.anchor.height)?;
                        if reuse.contains(&id) {
                            reuse.rewind_to(id);
                            reuse.trim(
                                v.view.profile.work_window_limit() - self.ledger.frames.len(),
                            );
                            self.reuse_index = Some(0);
                        }
                    }
                    if let Some(index) = self.reuse_index {
                        if let Some(frame) = reuse.frames.get(index) {
                            v.require(&frame.trace)?;
                            v.budget.cost.topology_reused += 1;
                            self.reuse_index = Some(index + 1);
                            continue;
                        }
                        if let Some(boundary) = reuse.boundary {
                            v.work(boundary)?;
                        }
                        let mut joined = self.reuse.take().expect("reuse ledger");
                        for frame in self.ledger.frames.iter().rev() {
                            joined.prepend(frame.clone())?;
                        }
                        self.cursor = joined.boundary;
                        self.previous = joined
                            .frames
                            .back()
                            .map_or(self.anchor.height, |frame| frame.height());
                        self.ledger = joined;
                        if self.ledger.frames.len() == v.view.profile.work_window_limit()
                            || self.cursor.is_none()
                        {
                            self.finish_ranking(v.view.profile.finance_low(self.anchor.height));
                        } else {
                            self.ledger.boundary = None;
                            self.reuse_index = None;
                        }
                        continue;
                    }
                }
                self.ledger.rebase(self.anchor.height)?;
                let frame = v.frame(core)?;
                let parent = frame.parent;
                self.ledger.append(frame)?;
                v.budget.cost.topology_frames += 1;
                self.previous = height;
                self.cursor = parent;
                continue;
            }
            if let Some(id) = self.pending.front().copied() {
                if let Some(trace) = v.facts.completed.get(&id).cloned() {
                    v.require(&trace)?;
                } else {
                    v.full_check(id)?;
                    let trace = Trace::join(v.traces.iter().cloned());
                    v.facts.certificate_bytes = v
                        .facts
                        .certificate_bytes
                        .saturating_add(trace.own_metadata());
                    v.facts.completed.insert(id, trace);
                }
                self.pending.pop_front();
                continue;
            }
            let topology = Arc::new(self.ledger.clone());
            if self.incremental {
                v.facts.topology = Some(topology.clone());
            }
            let financial = v.plan(&Parent::Receipt(self.tip), &self.anchor, 0)?;
            v.facts.topology = Some(topology);
            v.view.check()?;
            v.budget.check()?;
            v.facts.retain(&v.required, &self.full);
            v.budget
                .retained(v.retained_bytes().saturating_add(self.bytes()))?;
            return Ok(Some(Evaluation {
                tip: self.tip,
                anchor: self.anchor.clone(),
                financial,
                full: self.full.clone(),
                primary: self.primary.clone(),
                required: v.required.clone(),
                cost: v.budget.cost,
            }));
        }
        Ok(None)
    }
}
