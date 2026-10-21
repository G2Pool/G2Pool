use super::*;
use crate::storage::{Compaction, ReadSource};

pub struct Maintenance {
    source: ReadSource,
    _permit: crate::limits::WorkPermit,
    chain: Arc<Chain>,
    profile: Profile,
    anchor: Anchor,
    cancel: Arc<AtomicBool>,
}
pub struct MaintenanceResult {
    compaction: Compaction,
}
impl Maintenance {
    pub fn run(self) -> Result<MaintenanceResult> {
        let source = self.source.open()?;
        let view = View::pin(&self.chain, self.profile, chrono::Utc::now().timestamp())?;
        if view.head != self.anchor {
            return Err(Error::View);
        }
        Ok(MaintenanceResult {
            compaction: source.prepare_compaction(&view, &self.cancel)?,
        })
    }
}
impl Node {
    pub fn prepare_maintenance(&mut self) -> Result<Maintenance> {
        self.ensure_view()?;
        let permit = self
            .peer_work
            .acquire()
            .ok_or(Error::Capacity("peer native work slots"))?;
        Ok(Maintenance {
            _permit: permit,
            source: self.store.read_source()?,
            chain: self.chain.clone(),
            profile: self.profile.clone(),
            anchor: self.view()?.head,
            cancel: self.cancel.clone(),
        })
    }
    pub fn finish_maintenance(
        &mut self,
        result: MaintenanceResult,
        older_than: i64,
        extra: &BTreeSet<Key>,
    ) -> Result<usize> {
        if self.cancel.load(std::sync::atomic::Ordering::Acquire) {
            return Err(Error::Cancelled);
        }
        self.ensure_view()?;
        let keep = self.collection_keep(extra)?;
        self.store.apply_compaction(result.compaction)?;
        let collected = self.store.collect(&keep, older_than)?;
        Ok(collected)
    }
}
