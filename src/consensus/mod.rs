mod candidate;
mod ledger;
pub(crate) use ledger::Ledger;
mod payout;
pub use payout::output_bound;
mod session;
mod topology;
mod trace;
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
    pub financial: Financial,
    pub full: BTreeSet<Id>,
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
    targets: OrdMap<Id, Arc<Trace>>,
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
    pub(crate) fn work(&self, id: Id) -> Option<&Work> {
        self.work.get(&id).map(Arc::as_ref)
    }
    pub(crate) fn branch(&self) -> Option<&Ledger> {
        self.topology.as_deref()
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
        facts.certificate_bytes += extend_missing(&mut facts.targets, &self.targets, |trace| {
            trace.own_metadata()
        });
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
            &mut self.targets,
            |id, _| required.contains(&Key::new(Kind::Core, *id)),
            |trace| trace.own_metadata(),
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
    facts: Facts,
    reuse_plans: bool,
    traces: Vec<Arc<Trace>>,
    acquired: BTreeSet<Key>,
    required_traces: BTreeMap<usize, Arc<Trace>>,
    history: BTreeMap<Id, (usize, usize)>,
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
            facts: Facts::default(),
            reuse_plans: false,
            traces: vec![],
            acquired: BTreeSet::new(),
            required_traces: BTreeMap::new(),
            history: BTreeMap::new(),
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
            .saturating_add(self.required_traces.len() * 128 + self.history.len() * 128)
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
        let view = self.view;
        native::authenticated_history(view, parent, |id| self.authenticated(id))
    }
    pub(crate) fn share_difficulty(&mut self, parent: &Parent, anchor: &Anchor) -> Result<u64> {
        let target = self.view.target(anchor)?.difficulty.to_num();
        let history = self.retarget_history(parent)?;
        self.view.profile.retargeted(
            target,
            anchor
                .height
                .checked_add(1)
                .ok_or(Error::Invalid("height overflow"))?,
            &history,
            u64::from(self.view.profile.share_target_seconds),
        )
    }
    fn require_history(&mut self, parent: &Parent) -> Result<()> {
        let window = self.view.profile.work_window_limit();
        let mut primary = window + usize::from(self.view.profile.uncle_depth);
        let mut uncles = window;
        let mut cursor = parent.clone();
        let mut visited = Vec::new();
        while let Parent::Receipt(id) = cursor {
            if primary == 0
                || self
                    .history
                    .get(&id)
                    .is_some_and(|&(p, u)| p >= primary && u >= uncles)
            {
                break;
            }
            let work = self.authenticated(id)?;
            if uncles > 0 {
                for uncle in &work.job().uncles.0 {
                    self.authenticated(*uncle)?;
                }
            }
            visited.push((id, primary, uncles));
            primary -= 1;
            uncles = uncles.saturating_sub(1);
            cursor = work.job().parent.clone();
        }
        for (id, primary, uncles) in visited {
            let entry = self.history.entry(id).or_default();
            entry.0 = entry.0.max(primary);
            entry.1 = entry.1.max(uncles);
        }
        Ok(())
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
    fn authenticated(&mut self, id: Id) -> Result<Arc<Work>> {
        self.budget.check()?;
        let bytes = self.read(Key::new(Kind::Core, id))?;
        let authorization = self.read(Key::new(Kind::Authorization, id))?;
        if let Some(core) = self.facts.work.get(&id).cloned() {
            if core.job().timestamp
                > self
                    .view
                    .now
                    .saturating_add(i64::from(self.view.profile.future_seconds))
            {
                return Err(Error::Future);
            }
            return Ok(core);
        }
        self.budget.work()?;
        let core = Work::verify(&bytes, self.view)?;
        if core.id() != id {
            return Err(Error::Invalid("core lookup identity"));
        }
        let auth: WorkAuthorization =
            decode(&authorization).map_err(|_| Error::Witness("work authorization encoding"))?;
        core.authorize(&auth, self.view.profile.pool_id)?;
        let core = Arc::new(core);
        self.facts.work_bytes += core.retained_bytes();
        self.facts.work.insert(id, core.clone());
        Ok(core)
    }
    fn work(&mut self, id: Id) -> Result<Arc<Work>> {
        let mark = self.traces.len();
        let core = self.authenticated(id)?;
        let trace = self.facts.targets.get(&id).cloned();
        if trace.as_ref().is_none_or(|trace| {
            !self
                .required_traces
                .contains_key(&(Arc::as_ptr(trace) as usize))
        }) {
            if trace.is_none() {
                let assigned = self.share_difficulty(&core.job().parent, &core.job().anchor)?;
                if core.job().difficulty != assigned {
                    return Err(Error::Invalid("assigned target"));
                }
            } else {
                self.require_history(&core.job().parent)?;
            }
        }
        let trace = trace.unwrap_or_else(|| {
            let trace = Trace::target(id);
            self.facts.certificate_bytes += trace.own_metadata();
            self.facts.targets.insert(id, trace.clone());
            trace
        });
        self.required_traces
            .insert(Arc::as_ptr(&trace) as usize, trace.clone());
        self.traces.truncate(mark);
        self.traces.push(trace);
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
            if let Some(id) = node.target {
                self.work(id)?;
            }
            for key in &node.keys {
                if key.kind == Kind::Core {
                    self.authenticated(key.id)?;
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
        let mut session = Session::new(tip, self.view.head.clone(), self.view.profile.pool_id);
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
