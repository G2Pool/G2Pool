use super::*;
use grin_util::secp;

impl<S: Source> Validator<'_, '_, S> {
    pub(super) fn parent(&mut self, core: &Work) -> Result<Option<Arc<Work>>> {
        match &core.job.parent {
            Parent::Root(a) => {
                if a != &core.job.anchor {
                    return Err(Error::Invalid("root anchor"));
                }
                Ok(None)
            }
            Parent::Receipt(id) => {
                let p = self.work(*id)?;
                if p.job.anchor.height > core.job.anchor.height || p.id == core.id {
                    return Err(Error::Invalid("primary edge"));
                }
                Ok(Some(p))
            }
        }
    }
    pub(super) fn uncles(&mut self, core: &Work) -> Result<Vec<Arc<Work>>> {
        if core.job.uncles.0.is_empty() {
            return Ok(vec![]);
        }
        if core.job.uncles.0.len() > usize::from(self.view.profile.max_uncles) {
            return Err(Error::Invalid("too many uncles"));
        }
        let mut ancestors = Vec::new();
        let mut cursor = self.parent(core)?;
        let mut ids = BTreeSet::new();
        let depth = usize::from(self.view.profile.uncle_depth);
        for _ in 0..depth {
            let ancestor = match cursor {
                Some(a) => a,
                None => break,
            };
            if !ids.insert(ancestor.id) {
                return Err(Error::Invalid("support cycle"));
            }
            cursor = if ancestors.len() + 1 < depth {
                self.parent(&ancestor)?
            } else {
                None
            };
            ancestors.push(ancestor);
        }
        let mut uncles = Vec::new();
        for id in &core.job.uncles.0 {
            let u = self.work(*id)?;
            if u.id == core.id
                || ancestors
                    .iter()
                    .any(|a| a.id == u.id || a.job.uncles.0.contains(id))
            {
                return Err(Error::Invalid("recent duplicate uncle"));
            }
            if !ancestors
                .iter()
                .any(|a| a.job.parent == u.job.parent && a.job.anchor == u.job.anchor)
            {
                return Err(Error::Invalid("uncle sibling relation"));
            }
            uncles.push(u);
        }
        Ok(uncles)
    }
    pub(super) fn contributions_bounded(
        &mut self,
        parent: &Parent,
        anchor: u64,
        limit: Option<usize>,
        work_limit: Option<u128>,
        primary_limit: Option<usize>,
    ) -> Result<Vec<(Arc<Work>, u8)>> {
        if let Parent::Receipt(tip) = parent {
            let reusable = self.facts.topology.as_ref().is_some_and(|ledger| {
                ledger.height == anchor
                    && ledger
                        .frames
                        .front()
                        .is_some_and(|frame| frame.id() == *tip)
            });
            if reusable {
                let low = self.view.profile.finance_low(anchor);
                let limits = ledger::WindowLimits {
                    primary_limit: primary_limit.unwrap_or(usize::MAX),
                    receipt_limit: limit.unwrap_or(usize::MAX),
                    work_limit,
                };
                let selected =
                    Arc::make_mut(self.facts.topology.as_mut().unwrap()).window_ids(low, limits)?;
                let selected: BTreeSet<_> = selected.into_iter().collect();
                let ledger = self.facts.topology.as_ref().unwrap().clone();
                let mut out = Vec::new();
                for frame in &ledger.frames {
                    self.require(&frame.financial_trace)?;
                    for entry in frame.contributions() {
                        if selected.contains(&entry.id) {
                            out.push((self.work(entry.id)?, entry.class));
                            if out.len() == selected.len() {
                                self.budget.cost.window_reused += 1;
                                return Ok(out);
                            }
                        }
                    }
                }
                if let Some(boundary) = ledger.boundary {
                    self.work(boundary)?;
                }
                self.budget.cost.window_reused += 1;
                return Ok(out);
            }
        }
        let mut out = Vec::new();
        let mut seen = BTreeSet::new();
        let mut primary = BTreeSet::new();
        let mut total_work = 0u128;
        let mut primary_count = 0usize;
        let mut cursor = match parent {
            Parent::Root(a) => {
                self.view.anchor(a)?;
                return Ok(out);
            }
            Parent::Receipt(id) => Some(self.work(*id)?),
        };
        let low = self.view.profile.finance_low(anchor);
        let mut previous = anchor;
        while let Some(core) = cursor {
            self.budget.check()?;
            if !primary.insert(core.id) || core.job.anchor.height > previous {
                return Err(Error::Invalid("spine cycle/regression"));
            }
            if primary_limit.is_some_and(|limit| primary_count == limit) {
                break;
            }
            primary_count += 1;
            previous = core.job.anchor.height;
            if previous < low {
                break;
            }
            let mut candidates = vec![(core.clone(), 0)];
            candidates.extend(self.uncles(&core)?.into_iter().map(|u| (u, 1)));
            for (item, class) in candidates {
                if !seen.insert(item.id) {
                    return Err(Error::Invalid("duplicate contribution"));
                }
                if item.job.anchor.height < low {
                    continue;
                }
                if class == 1
                    && work_limit.is_some_and(|limit| {
                        total_work.saturating_add(u128::from(item.job.difficulty)) > limit
                    })
                {
                    continue;
                }
                total_work = total_work
                    .checked_add(u128::from(item.job.difficulty))
                    .ok_or(Error::Invalid("window work overflow"))?;
                out.push((item, class));
                if limit == Some(out.len()) || work_limit.is_some_and(|limit| total_work >= limit) {
                    return Ok(out);
                }
            }
            cursor = self.parent(&core)?;
        }
        Ok(out)
    }
    pub(super) fn historical_unspent(
        &mut self,
        anchor: &Anchor,
        c: secp::pedersen::Commitment,
    ) -> Result<bool> {
        let key = (anchor.height, anchor.hash, c.0);
        if let Some(value) = self.facts.historical.get(&key) {
            return Ok(*value);
        }
        self.budget.query()?;
        let value = self.view.unspent(anchor, c)?;
        self.facts.historical.insert(key, value);
        Ok(value)
    }

    pub(super) fn historical_unspent_many(
        &mut self,
        anchor: &Anchor,
        commitments: &[secp::pedersen::Commitment],
    ) -> Result<()> {
        const BATCH: usize = 256;
        let missing: Vec<_> = commitments
            .iter()
            .copied()
            .filter(|commitment| {
                !self
                    .facts
                    .historical
                    .contains_key(&(anchor.height, anchor.hash, commitment.0))
            })
            .collect();
        for chunk in missing.chunks(BATCH) {
            for _ in chunk {
                self.budget.query()?;
            }
            let values = self.view.unspent_many(anchor, chunk)?;
            if values.len() != chunk.len() {
                return Err(Error::Invalid("historical output result shape"));
            }
            for (commitment, value) in chunk.iter().zip(values) {
                self.facts
                    .historical
                    .insert((anchor.height, anchor.hash, commitment.0), value);
            }
        }
        Ok(())
    }
    pub(super) fn registration(
        &mut self,
        work: &Work,
    ) -> Result<(Arc<Registration>, Arc<Inventory>)> {
        if let Some(facts) = self
            .facts
            .registrations
            .get(&work.job.registration)
            .cloned()
        {
            let RegistrationFacts {
                registration: r,
                inventory: inv,
                trace,
            } = facts;
            if r.fields.owner != work.job.account || r.fields.birth != work.job.anchor {
                return Err(Error::Invalid("registration relation"));
            }
            self.require(&trace)?;
            return Ok((r, inv));
        }
        let mark = self.traces.len();
        let r: Registration = self.object(work.job.registration)?;
        if r.fields.pool_id != self.view.profile.pool_id
            || r.fields.owner != work.job.account
            || r.fields.birth != work.job.anchor
        {
            return Err(Error::Invalid("registration relation"));
        }
        self.view.anchor(&r.fields.birth)?;
        let public = native::public_key(r.fields.owner)?;
        if !native::signature_valid(r.signature, id(&r.fields)?, &public) {
            return Err(Error::Invalid("registration signature"));
        }
        let inv: Inventory = self.object(r.fields.inventory)?;
        if inv.owner != r.fields.owner {
            return Err(Error::Invalid("inventory context"));
        }
        let base = (self.view.profile.unit_budget.ilog2() + 1) as usize;
        if inv.rows.0.len() != base + 3 {
            return Err(Error::Invalid("inventory row count"));
        }
        let mut cs = BTreeSet::new();
        let mut es = BTreeSet::new();
        for (index, row) in inv.rows.0.iter().enumerate() {
            self.budget.check()?;
            let (family, level) = if index < base {
                (0, index)
            } else {
                (1, index - base)
            };
            if row.family != family
                || row.row as usize != level
                || row.coupon_ids.0.len() != crate::codec::COUPON_LEAVES_PER_ROW
            {
                return Err(Error::Invalid("inventory matrix"));
            }
            for id in &row.coupon_ids.0 {
                let material = self.coupon(*id)?;
                if !cs.insert(material.output.commitment().0)
                    || !es.insert(material.kernel.excess.0)
                {
                    return Err(Error::Invalid("inventory C/E uniqueness"));
                }
            }
        }

        let facts = (Arc::new(r), Arc::new(inv));
        let trace = Trace::join(self.traces[mark..].iter().cloned());
        self.traces.truncate(mark);
        self.traces.push(trace.clone());
        let registration = RegistrationFacts {
            registration: facts.0.clone(),
            inventory: facts.1.clone(),
            trace,
        };
        self.facts.registration_bytes = self
            .facts
            .registration_bytes
            .saturating_add(registration.bytes());
        self.facts
            .registrations
            .insert(work.job.registration, registration);
        Ok(facts)
    }
}
