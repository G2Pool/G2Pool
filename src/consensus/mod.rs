mod candidate;
mod closure;
pub use closure::{decode_dependency, ClosureObjects, VerifiedClosure};
pub mod coordinator;
mod ledger;
use ledger::Ledger;
mod payout;
pub use payout::output_bound;
mod session;
mod topology;
mod trace;
mod window;
pub use session::{EvaluationProgress, Session};
use trace::Trace;

use crate::{
    codec::*,
    limits::{Budget, Cost, Limits},
    native::{self, View, Work},
    objects::{Key, Kind, Source, StoredObject},
    Error, Result,
};
use grin_core::core::{Output, TxKernel};
use im::OrdMap;
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{atomic::AtomicBool, Arc},
};

#[derive(Debug, Clone)]
pub struct Financial {
    pub record: PayoutRecord,
    pub coupons: Vec<(Point, Output, TxKernel)>,
}
struct PayoutState {
    fees: u64,
    financial: Financial,
    trace: Arc<Trace>,
    bytes: usize,
}
impl PayoutState {
    fn new(fees: u64, financial: Financial, trace: Arc<Trace>) -> Self {
        let bytes = std::mem::size_of::<Self>()
            + 2 * std::mem::size_of::<usize>()
            + financial.record.receipts.0.capacity() * std::mem::size_of::<ReceiptRow>()
            + financial.record.accounts.0.capacity() * std::mem::size_of::<AccountRow>()
            + financial
                .record
                .accounts
                .0
                .iter()
                .map(|row| row.outcomes.0.capacity() * std::mem::size_of::<Outcome>())
                .sum::<usize>()
            + financial.coupons.capacity() * std::mem::size_of::<(Point, Output, TxKernel)>()
            + trace.own_metadata();
        Self {
            fees,
            financial,
            trace,
            bytes,
        }
    }
}
#[derive(Debug)]
pub struct Evaluation {
    pub tip: Id,
    pub anchor: Anchor,
    pub score: u128,
    pub financial: Financial,
    pub full: BTreeSet<Id>,
    pub closure: BTreeSet<Id>,
    pub(crate) primary: BTreeSet<Id>,
    pub required: BTreeSet<Key>,
    pub cost: Cost,
}
struct AccountAllocation {
    canonical: u16,
    uncle: u16,
    position: usize,
    registration: Arc<Registration>,
    inventory: Arc<Inventory>,
}
impl AccountAllocation {
    fn new(position: usize, registration: Arc<Registration>, inventory: Arc<Inventory>) -> Self {
        Self {
            canonical: 0,
            uncle: 0,
            position,
            registration,
            inventory,
        }
    }
    fn renew(
        &mut self,
        position: usize,
        registration: Arc<Registration>,
        inventory: Arc<Inventory>,
    ) {
        let next = (
            registration.fields.birth.height,
            registration.fields.revision,
        );
        let current = (
            self.registration.fields.birth.height,
            self.registration.fields.revision,
        );
        if next > current || (next == current && position < self.position) {
            self.position = position;
            self.registration = registration;
            self.inventory = inventory;
        }
    }
}

struct CouponMaterial {
    coupon: Coupon,
    output: Output,
    kernel: TxKernel,
}
impl CouponMaterial {
    fn bytes(&self) -> usize {
        std::mem::size_of::<Self>()
            + 192
            + self.coupon.reward.output.0.capacity()
            + self.coupon.reward.kernel.0.capacity()
    }
}
#[derive(Clone)]
struct RegistrationFacts {
    registration: Arc<Registration>,
    inventory: Arc<Inventory>,
    trace: Arc<Trace>,
}
impl RegistrationFacts {
    fn bytes(&self) -> usize {
        512 + self.inventory.rows.0.capacity() * std::mem::size_of::<InventoryRow>()
            + self
                .inventory
                .rows
                .0
                .iter()
                .map(|row| row.coupon_ids.0.capacity() * std::mem::size_of::<Id>())
                .sum::<usize>()
            + self.trace.own_metadata()
    }
}
#[derive(Default, Clone)]
pub(crate) struct Facts {
    topology: Option<Arc<Ledger>>,
    payout: Option<Arc<PayoutState>>,
    loaded: OrdMap<Key, Arc<[u8]>>,
    leaves: OrdMap<Key, Arc<Trace>>,
    coupons: OrdMap<(Id, Point, u64), u8>,
    material: OrdMap<Id, Arc<CouponMaterial>>,
    work: OrdMap<Id, Arc<Work>>,
    registrations: OrdMap<Id, RegistrationFacts>,
    historical: OrdMap<(u64, Id, Point), bool>,
    completed: OrdMap<Id, Arc<Trace>>,
    payload_bytes: usize,
    work_bytes: usize,
    material_bytes: usize,
    registration_bytes: usize,
    certificate_bytes: usize,
}

fn retain_ord_map<K, V, F>(
    map: &mut OrdMap<K, V>,
    mut keep: F,
    bytes: impl Fn(&V) -> usize,
) -> usize
where
    K: Clone + Ord,
    V: Clone,
    F: FnMut(&K, &V) -> bool,
{
    let removed: Vec<K> = map
        .iter()
        .filter(|(key, value)| !keep(key, value))
        .map(|(key, _)| key.clone())
        .collect();
    removed
        .into_iter()
        .map(|key| bytes(&map.remove(&key).expect("retained map entry disappeared")))
        .sum()
}

fn extend_missing<K: Clone + Ord, V: Clone>(
    map: &mut OrdMap<K, V>,
    other: &OrdMap<K, V>,
    bytes: impl Fn(&V) -> usize,
) -> usize {
    if map.ptr_eq(other) {
        return 0;
    }
    let mut added_bytes = 0;
    for (key, value) in other {
        if !map.contains_key(key) {
            added_bytes += bytes(value);
            map.insert(key.clone(), value.clone());
        }
    }
    added_bytes
}

impl Facts {
    pub(crate) fn verified_closure(
        &self,
        evaluation: &Evaluation,
        parent: Option<&VerifiedClosure>,
    ) -> Result<VerifiedClosure> {
        VerifiedClosure::from_facts(evaluation, self, parent)
    }
    pub(crate) fn work(&self, id: Id) -> Option<&Work> {
        self.work.get(&id).map(Arc::as_ref)
    }

    pub(crate) fn compact_share(&self, tip: Id) -> Result<(Arc<Work>, Vec<u8>)> {
        let work = self
            .work
            .get(&tip)
            .cloned()
            .ok_or(Error::Unavailable(Key::new(Kind::Core, tip)))?;
        let body = self
            .loaded
            .get(&Key::new(Kind::Core, tip))
            .cloned()
            .ok_or(Error::Unavailable(Key::new(Kind::Core, tip)))?
            .to_vec();
        Ok((work, body))
    }
    pub(crate) fn uncle_work(&self, id: Id) -> Result<Arc<Work>> {
        self.work
            .get(&id)
            .cloned()
            .ok_or(Error::Unavailable(Key::new(Kind::Core, id)))
    }
    pub(crate) fn compact_chain(
        &self,
        tip: Id,
        limit: usize,
        mut indexed: impl FnMut(Id) -> Result<bool>,
    ) -> Result<Vec<(Arc<Work>, Vec<u8>)>> {
        if limit == 0 {
            return Err(Error::Invalid("compact ancestry limit"));
        }
        let mut chain = Vec::new();
        let mut cursor = Some(tip);
        let mut seen = BTreeSet::new();
        while let Some(id) = cursor {
            if indexed(id)? {
                break;
            }
            if !seen.insert(id) {
                return Err(Error::Invalid("compact ancestry cycle"));
            }
            if seen.len() > limit {
                return Err(Error::Expired);
            }
            let (work, body) = self.compact_share(id)?;
            cursor = match work.job().parent {
                Parent::Root(_) => None,
                Parent::Receipt(parent) => Some(parent),
            };
            chain.push((work, body));
        }
        chain.reverse();
        Ok(chain)
    }
    pub(crate) fn has_active_tip(&self, low: u64) -> bool {
        self.topology.as_ref().is_some_and(|ledger| {
            ledger
                .frames
                .front()
                .is_some_and(|frame| frame.height() >= low)
        })
    }
    pub(crate) fn warmed(&self, warm: &Self) -> Self {
        let mut facts = warm.clone();
        facts.payload_bytes += extend_missing(&mut facts.loaded, &self.loaded, |bytes| bytes.len());
        extend_missing(&mut facts.leaves, &self.leaves, |_| 0);
        facts.work_bytes +=
            extend_missing(&mut facts.work, &self.work, |work| work.retained_bytes());
        facts.registration_bytes += extend_missing(
            &mut facts.registrations,
            &self.registrations,
            RegistrationFacts::bytes,
        );
        facts.material_bytes += extend_missing(&mut facts.material, &self.material, |material| {
            material.bytes()
        });
        extend_missing(&mut facts.coupons, &self.coupons, |_| 0);
        extend_missing(&mut facts.historical, &self.historical, |_| 0);
        facts.certificate_bytes += extend_missing(&mut facts.completed, &self.completed, |trace| {
            trace.own_metadata()
        });
        facts
    }
    pub(crate) fn bytes(&self) -> usize {
        self.payload_bytes
            .saturating_add(self.topology.as_ref().map_or(0, |ledger| ledger.bytes()))
            .saturating_add(self.payout.as_ref().map_or(0, |payout| payout.bytes))
            .saturating_add(self.loaded.len() * 288)
            .saturating_add(self.coupons.len() * 192)
            .saturating_add(self.material_bytes)
            .saturating_add(self.work_bytes)
            .saturating_add(self.historical.len() * 128)
            .saturating_add(self.registration_bytes)
            .saturating_add(self.certificate_bytes)
    }
    pub(crate) fn pins(&self) -> BTreeSet<Key> {
        self.loaded.keys().copied().collect()
    }
    pub(crate) fn retain(&mut self, required: &BTreeSet<Key>, full: &BTreeSet<Id>) {
        self.payload_bytes -= retain_ord_map(
            &mut self.loaded,
            |key, _| required.contains(key),
            |bytes| bytes.len(),
        );
        retain_ord_map(&mut self.leaves, |key, _| required.contains(key), |_| 0);
        self.work_bytes -= retain_ord_map(
            &mut self.work,
            |id, _| required.contains(&Key::new(Kind::Core, *id)),
            |work| work.retained_bytes(),
        );
        self.certificate_bytes -= retain_ord_map(
            &mut self.completed,
            |id, _| full.contains(id),
            |trace| trace.own_metadata(),
        );
        self.registration_bytes -= retain_ord_map(
            &mut self.registrations,
            |id, _| required.contains(&Key::new(Kind::Registration, *id)),
            RegistrationFacts::bytes,
        );
        self.material_bytes -= retain_ord_map(
            &mut self.material,
            |id, _| required.contains(&Key::new(Kind::Coupon, *id)),
            |material| material.bytes(),
        );
        retain_ord_map(
            &mut self.coupons,
            |(id, _, _), _| required.contains(&Key::new(Kind::Coupon, *id)),
            |_| 0,
        );
        let anchors: BTreeSet<_> = self
            .work
            .values()
            .map(|work| (work.job().anchor.height, work.job().anchor.hash))
            .collect();
        retain_ord_map(
            &mut self.historical,
            |(height, hash, _), _| anchors.contains(&(*height, *hash)),
            |_| 0,
        );
    }
}

pub struct Validator<'a, 'v, S: Source> {
    source: &'a S,
    view: &'a View<'v>,
    budget: Budget,
    required: BTreeSet<Key>,
    closure: BTreeSet<Id>,
    facts: Facts,
    reuse_plans: bool,
    traces: Vec<Arc<Trace>>,
    acquired: BTreeSet<Key>,
    required_traces: BTreeMap<usize, Arc<Trace>>,
}
impl<'a, 'v, S: Source> Validator<'a, 'v, S> {
    pub fn bounded(
        source: &'a S,
        view: &'a View<'v>,
        limits: Limits,
        cancel: Arc<AtomicBool>,
    ) -> Self {
        Self {
            source,
            view,
            budget: Budget::new(limits, cancel),
            required: BTreeSet::new(),
            closure: BTreeSet::new(),
            facts: Facts::default(),
            reuse_plans: false,
            traces: vec![],
            acquired: BTreeSet::new(),
            required_traces: BTreeMap::new(),
        }
    }
    pub(crate) fn seed(&mut self, facts: &Facts) {
        self.facts = facts.clone();
        self.reuse_plans = true;
    }
    pub(crate) fn dependencies(self) -> Result<BTreeSet<Key>> {
        self.view.check()?;
        self.budget.check()?;
        self.budget.retained(self.retained_bytes())?;
        Ok(self.required)
    }
    fn retained_bytes(&self) -> usize {
        self.facts
            .bytes()
            .saturating_add(self.required.len() * 128)
            .saturating_add(self.closure.len() * 96)
            .saturating_add(self.required_traces.len() * 128)
            .saturating_add(self.traces.capacity() * std::mem::size_of::<Arc<Trace>>())
    }
    fn read(&mut self, key: Key) -> Result<Arc<[u8]>> {
        self.budget.check()?;
        if let Some(bytes) = self.facts.loaded.get(&key).cloned() {
            if self.required.insert(key) {
                self.budget.object(bytes.len())?;
            }
            self.traces.push(self.facts.leaves[&key].clone());
            return Ok(bytes);
        }
        let bytes = self.source.read(key)?;
        Key::checked(key.kind, key.id, &bytes)?;
        if self.required.insert(key) {
            self.budget.object(bytes.len())?;
        }
        let trace = Trace::leaf(key);
        self.traces.push(trace.clone());
        self.facts.leaves.insert(key, trace);
        self.facts.payload_bytes = self.facts.payload_bytes.saturating_add(bytes.len());
        self.acquired.insert(key);
        self.facts.loaded.insert(key, bytes.clone());
        Ok(bytes)
    }
    fn object<T: StoredObject>(&mut self, id: Id) -> Result<T> {
        decode(&self.read(Key::new(T::KIND, id))?)
    }
    fn retarget_history(&mut self, parent: &Parent) -> Result<Vec<(u64, u128)>> {
        if let Parent::Receipt(tip) = parent {
            if let Some(ledger) = self.facts.topology.as_ref() {
                if ledger
                    .frames
                    .front()
                    .is_some_and(|frame| frame.id() == *tip)
                    && ledger.can_retarget(self.view.profile.work_window_limit())
                {
                    let history = ledger.retarget_history(self.view.profile.work_window_limit())?;
                    if history.len() >= 2 {
                        return Ok(history);
                    }
                }
            }
        }
        native::authenticated_history(self.view, parent, |key| self.read(key))
    }
    fn coupon(&mut self, id: Id) -> Result<Arc<CouponMaterial>> {
        if let Some(material) = self.facts.material.get(&id).cloned() {
            self.read(Key::new(Kind::Coupon, id))?;
            return Ok(material);
        }
        let coupon: Coupon = self.object(id)?;
        let (output, kernel) = native::reward(&coupon)?;
        let material = Arc::new(CouponMaterial {
            coupon,
            output,
            kernel,
        });
        self.facts.material_bytes = self.facts.material_bytes.saturating_add(material.bytes());
        self.facts.material.insert(id, material.clone());
        Ok(material)
    }
    fn work(&mut self, id: Id) -> Result<Arc<Work>> {
        self.budget.check()?;
        self.closure.insert(id);
        if let Some(core) = self.facts.work.get(&id).cloned() {
            self.view.anchor(&core.job().anchor)?;
            if core.job().timestamp
                > self
                    .view
                    .now
                    .checked_add(i64::from(self.view.profile.manifest.future_seconds))
                    .ok_or(Error::Invalid("clock overflow"))?
            {
                return Err(Error::Future);
            }
            self.read(Key::new(Kind::Core, id))?;
            self.read(Key::new(Kind::Authorization, id))?;
            return Ok(core);
        }
        let bytes = self.read(Key::new(Kind::Core, id))?;
        let auth: WorkAuthorization = decode(&self.read(Key::new(Kind::Authorization, id))?)
            .map_err(|_| Error::Witness("work authorization encoding"))?;
        self.budget.work()?;
        let decoded: Core = decode(&bytes)?;
        let decoded_job: Job = decode(&decoded.job.0)?;
        let target = self.view.target(&decoded_job.anchor)?.difficulty.to_num();
        let history = self.retarget_history(&decoded_job.parent)?;
        let assigned = self.view.profile.retargeted(
            target,
            native::parse::<grin_core::core::BlockHeader>(&decoded.header.0)?.height,
            &history,
            u64::from(self.view.profile.manifest.share_target_seconds),
        )?;
        let core = Work::verify_with_target(&bytes, self.view, Some(assigned))?;
        if core.job().difficulty != assigned {
            return Err(Error::Invalid("assigned target"));
        }
        if core.id() != id {
            return Err(Error::Invalid("core lookup identity"));
        }
        core.authorize(&auth, self.view.profile.protocol)?;
        let core = Arc::new(core);
        self.facts.work_bytes = self.facts.work_bytes.saturating_add(core.retained_bytes());
        self.facts.work.insert(id, core.clone());
        Ok(core)
    }
    fn require(&mut self, trace: &Arc<Trace>) -> Result<()> {
        let mark = self.traces.len();
        let mut stack = vec![(trace.clone(), false)];
        while let Some((node, expanded)) = stack.pop() {
            self.budget.check()?;
            let pointer = Arc::as_ptr(&node) as usize;
            if self.required_traces.contains_key(&pointer) {
                continue;
            }
            if !expanded {
                stack.push((node.clone(), true));
                stack.extend(node.children.iter().cloned().map(|child| (child, false)));
                continue;
            }
            for key in &node.keys {
                if key.kind == Kind::Core {
                    self.work(key.id)?;
                } else {
                    self.read(*key)?;
                }
            }
            self.required_traces.insert(pointer, node);
        }
        self.traces.truncate(mark);
        self.traces.push(trace.clone());
        Ok(())
    }
    pub fn evaluate(self, tip: Id) -> Result<Evaluation> {
        self.evaluate_with_facts(tip)
            .map(|(evaluation, _)| evaluation)
    }
    pub(crate) fn evaluate_with_facts(self, tip: Id) -> Result<(Evaluation, Facts)> {
        let mut session = Session::new(tip, self.view.head.clone(), self.view.profile.protocol);
        session.inherit_topology(self.facts.topology.clone());
        let mut validator = self;
        loop {
            if let Some(result) = session.advance(&mut validator, usize::MAX)? {
                return Ok((result, validator.facts));
            }
        }
    }
}

#[cfg(test)]
#[path = "mod_tests.rs"]
mod tests;
