use crate::{Error, Result};
use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

pub(crate) struct WorkSlots {
    active: std::sync::atomic::AtomicUsize,
    limit: usize,
}
pub(crate) struct WorkPermit(Arc<WorkSlots>);
impl WorkSlots {
    pub(crate) fn new(limit: usize) -> Arc<Self> {
        Arc::new(Self {
            active: std::sync::atomic::AtomicUsize::new(0),
            limit,
        })
    }
    pub(crate) fn acquire(self: &Arc<Self>) -> Option<WorkPermit> {
        self.active
            .try_update(Ordering::AcqRel, Ordering::Acquire, |active| {
                (active < self.limit).then(|| active + 1)
            })
            .ok()
            .map(|_| WorkPermit(self.clone()))
    }
}
impl Drop for WorkPermit {
    fn drop(&mut self) {
        self.0.active.fetch_sub(1, Ordering::AcqRel);
    }
}

#[derive(Debug, Clone)]
pub struct Limits {
    pub bytes: usize,
    pub facts: usize,
    pub work: usize,
    pub full_jobs: usize,
    pub native_queries: usize,
    pub duration: Duration,
}
impl Default for Limits {
    fn default() -> Self {
        Self {
            bytes: 256 * 1024 * 1024,
            facts: 1_000_000,
            work: 100_000,
            full_jobs: 100_000,
            native_queries: 1_000_000,
            duration: Duration::from_secs(30),
        }
    }
}
#[derive(Debug, Default, Clone, Copy)]
pub struct Cost {
    pub topology_frames: usize,
    pub topology_reused: usize,
    pub window_reused: usize,
    pub payout_reused: usize,
    pub bytes: usize,
    pub facts: usize,
    pub work: usize,
    pub full_jobs: usize,
    pub native_queries: usize,
}
fn charge(counter: &mut usize, limit: usize, reason: &'static str) -> Result<()> {
    let next = counter
        .checked_add(1)
        .filter(|next| *next <= limit)
        .ok_or(Error::Capacity(reason))?;
    *counter = next;
    Ok(())
}
pub struct Budget {
    limits: Limits,
    deadline: Instant,
    cancel: Arc<AtomicBool>,
    remaining: Duration,
    pub cost: Cost,
}
impl Budget {
    pub fn new(limits: Limits, cancel: Arc<AtomicBool>) -> Self {
        Self {
            remaining: limits.duration,
            deadline: Instant::now()
                .checked_add(limits.duration)
                .unwrap_or_else(Instant::now),
            limits,
            cancel,
            cost: Cost::default(),
        }
    }
    pub(crate) fn resume(&mut self) {
        self.deadline = Instant::now()
            .checked_add(self.remaining)
            .unwrap_or_else(Instant::now);
    }
    pub(crate) fn suspend(&mut self) {
        self.remaining = self.deadline.saturating_duration_since(Instant::now());
    }
    pub fn check(&self) -> Result<()> {
        if self.cancel.load(Ordering::Acquire) {
            return Err(Error::Cancelled);
        }
        if Instant::now() >= self.deadline {
            return Err(Error::Capacity("evaluation time"));
        }
        Ok(())
    }
    pub fn object(&mut self, bytes: usize) -> Result<()> {
        self.check()?;
        self.cost.bytes = self
            .cost
            .bytes
            .checked_add(bytes)
            .ok_or(Error::Capacity("evaluation bytes"))?;
        charge(&mut self.cost.facts, self.limits.facts, "evaluation facts")?;
        if self.cost.bytes > self.limits.bytes {
            return Err(Error::Capacity("evaluation material"));
        }
        Ok(())
    }
    pub(crate) fn retained(&self, bytes: usize) -> Result<()> {
        self.check()?;
        if bytes > self.limits.bytes {
            return Err(Error::Capacity("evaluation cache"));
        }
        Ok(())
    }
    pub fn work(&mut self) -> Result<()> {
        self.check()?;
        charge(&mut self.cost.work, self.limits.work, "work checks")
    }
    pub fn full(&mut self) -> Result<()> {
        self.check()?;
        charge(&mut self.cost.full_jobs, self.limits.full_jobs, "full jobs")
    }
    pub fn query(&mut self) -> Result<()> {
        self.check()?;
        charge(
            &mut self.cost.native_queries,
            self.limits.native_queries,
            "native queries",
        )
    }
}
