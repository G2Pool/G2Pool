use super::*;
use im::{OrdMap, OrdSet, Vector};

#[derive(Clone, Copy)]
pub(super) struct Contribution {
    pub id: Id,
    pub height: u64,
    pub timestamp: u64,
    pub difficulty: u64,
    pub class: u8,
}

pub(super) struct Frame {
    primary: Contribution,
    uncles: Vec<Contribution>,
    pub parent: Option<Id>,
    pub trace: Arc<Trace>,
    pub financial_trace: Arc<Trace>,
}
impl Frame {
    pub fn id(&self) -> Id {
        self.primary.id
    }
    pub fn height(&self) -> u64 {
        self.primary.height
    }
    pub fn contributions(&self) -> impl Iterator<Item = &Contribution> {
        std::iter::once(&self.primary).chain(self.uncles.iter())
    }
    fn bytes(&self) -> usize {
        std::mem::size_of::<Self>()
            + 2 * std::mem::size_of::<usize>()
            + self.uncles.capacity() * std::mem::size_of::<Contribution>()
            + self.trace.own_metadata()
            + self.financial_trace.own_metadata()
    }
}

#[derive(Default, Clone)]
pub(super) struct Ledger {
    pub height: u64,
    low: u64,
    pub frames: Vector<Arc<Frame>>,
    spine: OrdSet<Id>,
    seen: OrdSet<Id>,
    active: OrdMap<(u64, Id), Contribution>,
    pub boundary: Option<Id>,
    pub score: u128,
    frame_bytes: usize,
    window_order: Vector<Contribution>,
    window_cache: Option<WindowCache>,
}

#[derive(Clone)]
struct WindowCache {
    low: u64,
    limits: window::WindowLimits,
    ids: Vec<Id>,
}
impl Ledger {
    pub fn new(height: u64, low: u64) -> Self {
        Self {
            height,
            low,
            ..Self::default()
        }
    }
    pub fn contains(&self, id: &Id) -> bool {
        self.spine.contains(id)
    }
    pub fn full(&self) -> BTreeSet<Id> {
        self.active.values().map(|entry| entry.id).collect()
    }
    pub fn validation(&self, limit: usize) -> BTreeSet<Id> {
        if limit == usize::MAX {
            return self.full();
        }
        let mut selected = BTreeSet::new();
        for frame in &self.frames {
            for entry in frame.contributions() {
                if entry.height >= self.low && selected.len() < limit {
                    selected.insert(entry.id);
                }
            }
            if selected.len() >= limit {
                break;
            }
        }
        selected
    }
    pub fn primary(&self) -> BTreeSet<Id> {
        self.spine.iter().copied().collect()
    }
    pub fn bytes(&self) -> usize {
        std::mem::size_of::<Self>()
            + self.frames.len() * std::mem::size_of::<Arc<Frame>>()
            + self.frame_bytes
            + (self.spine.len() + self.seen.len()) * 96
            + self.active.len() * 192
            + self.window_order.len() * std::mem::size_of::<Contribution>()
    }

    pub fn retarget_history(&self, limit: usize) -> Result<Vec<(u64, u128)>> {
        let mut total = 0u128;
        let mut samples = Vec::new();
        for frame in self.frames.iter().take(limit).rev() {
            let parent_total = total;
            let uncle_work = frame
                .uncles
                .iter()
                .try_fold(0u128, |sum, uncle| {
                    sum.checked_add(u128::from(uncle.difficulty))
                })
                .ok_or(Error::Invalid("retarget work overflow"))?;
            total = parent_total
                .checked_add(u128::from(frame.primary.difficulty))
                .and_then(|value| value.checked_add(uncle_work))
                .ok_or(Error::Invalid("retarget work overflow"))?;
            samples.push((frame.primary.timestamp, total));
            for uncle in &frame.uncles {
                let uncle_total = parent_total
                    .checked_add(u128::from(uncle.difficulty))
                    .ok_or(Error::Invalid("retarget work overflow"))?;
                samples.push((uncle.timestamp, uncle_total));
            }
        }
        Ok(samples)
    }

    pub fn can_retarget(&self, limit: usize) -> bool {
        (self.frames.len() >= limit || self.boundary.is_none())
            && self
                .frames
                .iter()
                .take(limit)
                .all(|frame| frame.uncles.is_empty())
    }

    fn insert(&mut self, frame: &Frame) -> Result<()> {
        let mut unique = BTreeSet::new();
        let mut score = self.score;
        for entry in frame.contributions() {
            if self.seen.contains(&entry.id) || !unique.insert(entry.id) {
                return Err(Error::Invalid("duplicate contribution"));
            }
            if entry.height >= self.low {
                score = score
                    .checked_add(entry.difficulty as u128)
                    .ok_or(Error::Invalid("score overflow"))?;
            }
        }
        self.seen.extend(unique);
        self.spine.insert(frame.id());
        self.score = score;
        for entry in frame.contributions() {
            if entry.height >= self.low {
                self.active.insert((entry.height, entry.id), *entry);
            }
        }
        self.window_cache = None;
        Ok(())
    }
    fn add_order_back(&mut self, frame: &Frame) {
        self.window_order.extend(frame.contributions().copied());
    }
    fn add_order_front(&mut self, frame: &Frame) {
        let entries: Vec<_> = frame.contributions().copied().collect();
        for entry in entries.into_iter().rev() {
            self.window_order.push_front(entry);
        }
    }
    fn remove_order_front(&mut self, frame: &Frame) {
        for expected in frame.contributions() {
            let actual = self.window_order.pop_front().expect("window order");
            debug_assert_eq!(actual.id, expected.id);
        }
    }
    fn remove_order_back(&mut self, frame: &Frame) {
        let expected: Vec<_> = frame.contributions().collect();
        for expected in expected.into_iter().rev() {
            let actual = self.window_order.pop_back().expect("window order");
            debug_assert_eq!(actual.id, expected.id);
        }
    }
    pub(super) fn window_ids(&mut self, low: u64, limits: window::WindowLimits) -> Result<Vec<Id>> {
        if let Some(cache) = &self.window_cache {
            if cache.low == low && cache.limits == limits {
                return Ok(cache.ids.clone());
            }
        }
        if limits.primary_limit == 0 || limits.receipt_limit == 0 || limits.work_limit == Some(0) {
            return Err(Error::Invalid("window bounds"));
        }
        let mut ids = Vec::new();
        let mut total = 0u128;
        let mut primaries = 0usize;
        for entry in self.window_order.iter().filter(|entry| entry.height >= low) {
            if entry.class == 0 {
                if primaries == limits.primary_limit {
                    break;
                }
                primaries += 1;
            } else if limits
                .work_limit
                .is_some_and(|limit| total.saturating_add(u128::from(entry.difficulty)) > limit)
            {
                continue;
            }
            ids.push(entry.id);
            total = total
                .checked_add(u128::from(entry.difficulty))
                .ok_or(Error::Invalid("window work overflow"))?;
            if ids.len() == limits.receipt_limit
                || limits.work_limit.is_some_and(|limit| total >= limit)
            {
                break;
            }
        }
        self.window_cache = Some(WindowCache {
            low,
            limits,
            ids: ids.clone(),
        });
        Ok(ids)
    }
    pub fn append(&mut self, frame: Arc<Frame>) -> Result<()> {
        if self
            .frames
            .back()
            .is_some_and(|last| last.parent != Some(frame.id()) || frame.height() > last.height())
        {
            return Err(Error::Invalid("spine cycle/regression"));
        }
        self.insert(&frame)?;
        self.add_order_back(&frame);
        self.frame_bytes += frame.bytes();
        self.frames.push_back(frame);
        Ok(())
    }
    pub fn prepend(&mut self, frame: Arc<Frame>) -> Result<()> {
        let parent = self
            .frames
            .front()
            .map(|first| first.id())
            .or(self.boundary);
        if frame.parent != parent
            || self
                .frames
                .front()
                .is_some_and(|first| first.height() > frame.height())
        {
            return Err(Error::Invalid("spine cycle/regression"));
        }
        self.insert(&frame)?;
        self.add_order_front(&frame);
        self.frame_bytes += frame.bytes();
        self.frames.push_front(frame);
        Ok(())
    }
    fn deactivate(&mut self, contribution: &Contribution) {
        if let Some(entry) = self.active.remove(&(contribution.height, contribution.id)) {
            self.score -= entry.difficulty as u128;
        }
    }
    fn undo(&mut self, frame: &Frame) {
        self.frame_bytes -= frame.bytes();
        self.spine.remove(&frame.id());
        for entry in frame.contributions() {
            self.seen.remove(&entry.id);
            self.deactivate(entry);
        }
        self.window_cache = None;
    }
    pub fn rewind_to(&mut self, id: Id) {
        while self.frames.front().is_some_and(|frame| frame.id() != id) {
            let frame = self.frames.pop_front().expect("nonempty ledger");
            self.remove_order_front(&frame);
            self.undo(&frame);
        }
    }
    pub fn advance(&mut self, height: u64, low: u64) -> Result<()> {
        if height < self.height || low < self.low {
            return Err(Error::View);
        }
        self.height = height;
        self.low = low;
        while self.frames.back().is_some_and(|frame| frame.height() < low) {
            let frame = self.frames.pop_back().expect("nonempty ledger");
            self.boundary = Some(frame.id());
            self.remove_order_back(&frame);
            self.undo(&frame);
        }
        while let Some(entry) = self.active.get_min().map(|(_, entry)| *entry) {
            if entry.height >= low {
                break;
            }
            self.deactivate(&entry);
        }
        Ok(())
    }
}

impl<S: Source> Validator<'_, '_, S> {
    pub(super) fn frame(&mut self, core: Arc<Work>) -> Result<Arc<Frame>> {
        let uncles = self.uncles(&core)?;
        let financial_trace = Trace::join(self.traces.iter().cloned());
        let parent = self.parent(&core)?.map(|work| work.id());
        let contribution = |work: &Work, class| -> Result<Contribution> {
            Ok(Contribution {
                id: work.id(),
                height: work.job().anchor.height,
                timestamp: u64::try_from(work.job().timestamp)
                    .map_err(|_| Error::Invalid("share timestamp"))?,
                difficulty: work.job().difficulty,
                class,
            })
        };
        Ok(Arc::new(Frame {
            primary: contribution(&core, 0)?,
            uncles: uncles
                .iter()
                .map(|work| contribution(work, 1))
                .collect::<Result<Vec<_>>>()?,
            parent,
            trace: Trace::join(self.traces.iter().cloned()),
            financial_trace,
        }))
    }
}

#[cfg(test)]
#[path = "ledger_tests.rs"]
mod tests;
