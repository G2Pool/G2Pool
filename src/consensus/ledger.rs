use super::*;
use im::{OrdSet, Vector};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct WindowLimits {
    pub(super) primary_limit: usize,
    pub(super) receipt_limit: usize,
    pub(super) work_limit: Option<u128>,
}

#[derive(Clone, Copy)]
pub(crate) struct Contribution {
    pub id: Id,
    pub height: u64,
    pub timestamp: u64,
    pub difficulty: u64,
    pub class: u8,
    pub depth: usize,
}

pub(crate) struct Frame {
    primary: Contribution,
    uncles: Vec<Contribution>,
    pub parent: Option<Id>,
    pub(super) trace: Arc<Trace>,
    pub(super) financial_trace: Arc<Trace>,
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
    pub fn credited(
        &self,
        index: usize,
        limit: usize,
        low: u64,
    ) -> impl Iterator<Item = &Contribution> {
        self.contributions()
            .filter(move |entry| index + entry.depth < limit && entry.height >= low)
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
pub(crate) struct Ledger {
    pub height: u64,
    pub frames: Vector<Arc<Frame>>,
    spine: OrdSet<Id>,
    seen: OrdSet<Id>,
    pub boundary: Option<Id>,
    frame_bytes: usize,
    window_order: Vector<Contribution>,
    window_cache: Option<WindowCache>,
}

#[derive(Clone)]
struct WindowCache {
    low: u64,
    limits: WindowLimits,
    ids: Vec<Id>,
}
impl Ledger {
    pub fn new(height: u64) -> Self {
        Self {
            height,
            ..Self::default()
        }
    }
    pub fn contains(&self, id: &Id) -> bool {
        self.spine.contains(id)
    }
    pub fn full(&self, low: u64) -> BTreeSet<Id> {
        self.window_order
            .iter()
            .filter(|entry| entry.height >= low)
            .map(|entry| entry.id)
            .collect()
    }
    pub fn trim(&mut self, limit: usize) {
        while self.frames.len() > limit {
            let frame = self.frames.pop_back().expect("nonempty ledger");
            self.boundary = Some(frame.id());
            self.remove_order_back(&frame);
            self.undo(&frame);
        }
    }
    pub fn recent(&self, limit: usize, low: u64) -> impl Iterator<Item = &Contribution> {
        self.frames
            .iter()
            .take(limit)
            .enumerate()
            .flat_map(move |(index, frame)| frame.credited(index, limit, low))
    }
    pub fn window_score(&self, limit: usize, low: u64) -> Result<u128> {
        self.recent(limit, low).try_fold(0u128, |sum, entry| {
            sum.checked_add(u128::from(entry.difficulty))
                .ok_or(Error::Invalid("score overflow"))
        })
    }
    pub fn primary(&self) -> BTreeSet<Id> {
        self.spine.iter().copied().collect()
    }
    pub fn bytes(&self) -> usize {
        std::mem::size_of::<Self>()
            + self.frames.len() * std::mem::size_of::<Arc<Frame>>()
            + self.frame_bytes
            + (self.spine.len() + self.seen.len()) * 96
            + self.window_order.len() * std::mem::size_of::<Contribution>()
    }

    fn insert(&mut self, frame: &Frame) -> Result<()> {
        let mut unique = BTreeSet::new();
        for entry in frame.contributions() {
            if self.seen.contains(&entry.id) || !unique.insert(entry.id) {
                return Err(Error::Invalid("duplicate contribution"));
            }
        }
        self.seen.extend(unique);
        self.spine.insert(frame.id());
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
    pub(super) fn window_ids(&mut self, low: u64, limits: WindowLimits) -> Result<Vec<Id>> {
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
    fn undo(&mut self, frame: &Frame) {
        self.frame_bytes -= frame.bytes();
        self.spine.remove(&frame.id());
        for entry in frame.contributions() {
            self.seen.remove(&entry.id);
        }
        self.window_cache = None;
    }
    pub fn rebase(&mut self, height: u64) -> Result<()> {
        if height < self.height {
            return Err(Error::View);
        }
        self.height = height;
        Ok(())
    }
    pub fn rewind_to(&mut self, id: Id) {
        while self.frames.front().is_some_and(|frame| frame.id() != id) {
            let frame = self.frames.pop_front().expect("nonempty ledger");
            self.remove_order_front(&frame);
            self.undo(&frame);
        }
    }
}

impl<S: Source> Validator<'_, '_, S> {
    pub(super) fn frame(&mut self, core: Arc<Work>) -> Result<Arc<Frame>> {
        let uncles = self.uncles(&core)?;
        let financial_trace = Trace::join(self.traces.iter().cloned());
        let parent = self.parent(&core)?.map(|work| work.id());
        let contribution = |work: &Work, class, depth| -> Result<Contribution> {
            Ok(Contribution {
                id: work.id(),
                height: work.job().anchor.height,
                timestamp: u64::try_from(work.job().timestamp)
                    .map_err(|_| Error::Invalid("share timestamp"))?,
                difficulty: work.job().difficulty,
                class,
                depth,
            })
        };
        Ok(Arc::new(Frame {
            primary: contribution(&core, 0, 0)?,
            uncles: uncles
                .iter()
                .map(|work| {
                    let mut cursor = core.job().parent.clone();
                    for depth in 1..=usize::from(self.view.profile.manifest.uncle_depth) {
                        let Parent::Receipt(id) = cursor else { break };
                        let ancestor = self
                            .facts
                            .work(id)
                            .ok_or(Error::Unavailable(Key::new(Kind::Core, id)))?;
                        if ancestor.job().parent == work.job().parent
                            && ancestor.job().anchor == work.job().anchor
                        {
                            return contribution(work, 1, depth);
                        }
                        cursor = ancestor.job().parent.clone();
                    }
                    Err(Error::Invalid("uncle depth"))
                })
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
