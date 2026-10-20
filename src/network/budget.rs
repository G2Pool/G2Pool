use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

#[derive(Debug)]
pub(super) struct Bytes {
    used: AtomicUsize,
    limit: usize,
}
#[derive(Debug)]
pub(super) struct Ticket {
    pool: Arc<Bytes>,
    amount: usize,
}
impl Bytes {
    pub(super) fn new(limit: usize) -> Arc<Self> {
        Arc::new(Self {
            used: AtomicUsize::new(0),
            limit,
        })
    }
    pub(super) fn reserve(self: &Arc<Self>, amount: usize) -> Option<Ticket> {
        self.used
            .try_update(Ordering::AcqRel, Ordering::Acquire, |used| {
                used.checked_add(amount).filter(|next| *next <= self.limit)
            })
            .ok()
            .map(|_| Ticket {
                pool: self.clone(),
                amount,
            })
    }
}
impl Drop for Ticket {
    fn drop(&mut self) {
        self.pool.used.fetch_sub(self.amount, Ordering::AcqRel);
    }
}

#[derive(Clone)]
pub(super) struct Budgets {
    pub receive: Arc<Bytes>,
    pub serve: Arc<Bytes>,
    pub control: Arc<Bytes>,
}
impl Default for Budgets {
    fn default() -> Self {
        Self {
            receive: Bytes::new(64 << 20),
            serve: Bytes::new(64 << 20),
            control: Bytes::new(1 << 20),
        }
    }
}

#[cfg(test)]
#[path = "budget_tests.rs"]
mod tests;
