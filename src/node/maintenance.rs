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
        let view = self.view()?;
        let low = view.profile.finance_low(view.head.height);
        let mut tips: BTreeSet<Id> = self
            .store
            .heads()?
            .into_iter()
            .map(|head| head.tip)
            .collect();
        tips.extend(self.tasks.keys().copied());
        tips.extend(self.running.keys().copied());
        tips.extend(
            self.recovery
                .iter()
                .flat_map(|(origin, tip)| [*origin, *tip]),
        );
        tips.extend(self.store.acquisitions(&view.head)?);
        let manifests: Vec<_> = self
            .dependency_closure
            .as_ref()
            .map(|closure| closure.id())
            .into_iter()
            .collect();
        let retained_tips: Vec<_> = tips.iter().copied().collect();
        self.sharechain
            .db()
            .retire_obsolete_evaluation_bindings(low, &retained_tips, &manifests, 64)
            .map_err(|_| Error::Invalid("sharechain evaluation retirement"))?;
        self.sharechain
            .db()
            .drain_dependency_gc(256)
            .map_err(|_| Error::Invalid("sharechain dependency gc"))?;
        if let Some(publication) = self.sharechain_publication()? {
            let snapshot = self
                .sharechain
                .db()
                .read()
                .map_err(|_| Error::Invalid("sharechain snapshot"))?;
            let head_height = snapshot
                .header(&publication.head)
                .map_err(|_| Error::Invalid("sharechain head"))?
                .ok_or(Error::Missing(publication.head))?
                .height;
            let retain_from = head_height.saturating_sub(self.profile.validation_limit() as u64);
            self.sharechain
                .db()
                .prune_unreferenced_bodies_bounded(publication.head, retain_from, 256)
                .map_err(|_| Error::Invalid("sharechain body gc"))?;
            self.sharechain
                .db()
                .prune_headers_bounded(&retained_tips, retain_from, 256)
                .map_err(|_| Error::Invalid("sharechain header gc"))?;
        }
        Ok(collected)
    }
}
