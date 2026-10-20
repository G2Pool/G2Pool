use super::*;
use grin_util::static_secp_instance;

pub fn output_bound(units: u16, receipts: usize) -> usize {
    let slots = receipts.min(usize::from(units));
    if slots == 0 {
        return 0;
    }
    let mut remaining = usize::from(units);
    let (mut outputs, mut cost) = (0, 1);
    while remaining >= slots * cost {
        remaining -= slots * cost;
        outputs += slots;
        cost *= 2;
    }
    outputs + remaining / cost
}

fn prefetch_coupon_presence(
    anchor: &Anchor,
    rows: &[(Anchor, Vec<grin_util::secp::pedersen::Commitment>)],
    mut query: impl FnMut(&Anchor, &[grin_util::secp::pedersen::Commitment]) -> Result<Vec<bool>>,
) -> Result<()> {
    let mut unresolved = vec![true; rows.len()];
    let mut births = BTreeMap::<(u64, Id), (Anchor, BTreeSet<[u8; 33]>)>::new();
    for leaf in 0..crate::codec::COUPON_LEAVES_PER_ROW {
        let needed: BTreeSet<_> = rows
            .iter()
            .zip(&unresolved)
            .filter(|(_, pending)| **pending)
            .filter_map(|((_, commitments), _)| commitments.get(leaf).map(|c| c.0))
            .collect();
        if needed.is_empty() {
            break;
        }
        let commitments: Vec<_> = needed
            .into_iter()
            .map(grin_util::secp::pedersen::Commitment)
            .collect();
        let values = query(anchor, &commitments)?;
        if values.len() != commitments.len() {
            return Err(Error::Invalid("historical output result shape"));
        }
        let present: BTreeMap<_, _> = commitments.iter().map(|c| c.0).zip(values).collect();
        for ((birth, commitments), pending) in rows.iter().zip(&mut unresolved) {
            if !*pending {
                continue;
            }
            if let Some(commitment) = commitments.get(leaf) {
                if !present[&commitment.0] {
                    *pending = false;
                    births
                        .entry((birth.height, birth.hash))
                        .or_insert_with(|| (birth.clone(), BTreeSet::new()))
                        .1
                        .insert(commitment.0);
                }
            }
        }
    }
    for (_, (birth, commitments)) in births {
        let commitments: Vec<_> = commitments
            .into_iter()
            .map(grin_util::secp::pedersen::Commitment)
            .collect();
        if query(&birth, &commitments)?.len() != commitments.len() {
            return Err(Error::Invalid("historical output result shape"));
        }
    }
    Ok(())
}

impl<S: Source> Validator<'_, '_, S> {
    fn coupon_check(
        &mut self,
        cid: Id,
        coupon: &Coupon,
        owner: Point,
        value: u64,
        output: &grin_core::core::Output,
        kernel: &grin_core::core::TxKernel,
    ) -> Result<u8> {
        let key = (cid, owner, value);
        if let Some(code) = self.facts.coupons.get(&key) {
            return Ok(*code);
        }
        self.budget.check()?;
        let handle = static_secp_instance();
        let secp = handle.lock();
        let expected = secp.commit_sum(
            vec![output.commitment()],
            vec![secp
                .commit_value(value)
                .map_err(|_| Error::Invalid("amount commitment"))?],
        );
        let amount_valid = expected.ok() == Some(kernel.excess);
        drop(secp);
        let code = if !amount_valid {
            3
        } else if !output.is_coinbase() || !kernel.is_coinbase() {
            4
        } else if output.verify_proof().is_err() {
            5
        } else if kernel.verify().is_err() {
            6
        } else {
            let auth = CouponAuth {
                protocol: self.view.profile.protocol,
                owner,
                reward_id: id(&coupon.reward)?,
            };
            let public = kernel
                .excess
                .to_pubkey(&static_secp_instance().lock())
                .map_err(|_| Error::Invalid("coupon public"))?;
            if native::signature_valid(coupon.owner_auth, id(&auth)?, &public) {
                0
            } else {
                7
            }
        };
        self.facts.coupons.insert(key, code);
        Ok(code)
    }

    pub fn plan(&mut self, parent: &Parent, anchor: &Anchor, fees: u64) -> Result<Financial> {
        self.view.anchor(anchor)?;
        if let Parent::Root(a) = parent {
            if a != anchor {
                return Err(Error::Invalid("plan root anchor"));
            }
        }
        if let Some(state) = self.facts.payout.clone().filter(|state| {
            self.reuse_plans
                && state.fees == fees
                && state.financial.record.parent == *parent
                && state.financial.record.anchor == *anchor
        }) {
            self.require(&state.trace)?;
            self.budget.cost.payout_reused += 1;
            return Ok(state.financial.clone());
        }
        let trace_start = self.traces.len();
        let target = self.view.target(anchor)?.difficulty.to_num();
        let work_limit = self.view.profile.payout_work_limit(target)?;
        let entries = self.contributions_bounded(
            parent,
            anchor.height,
            Some(crate::codec::MAX_ENCODING_ENTRIES),
            Some(work_limit),
            Some(self.view.profile.work_window_limit()),
        )?;
        let ids: Vec<_> = entries
            .iter()
            .map(|(work, class)| (work.id(), work.job().difficulty, *class))
            .collect();
        let selected = crate::consensus::window::bounded_ids(
            &ids,
            crate::consensus::window::WindowLimits {
                primary_limit: self.view.profile.work_window_limit(),
                receipt_limit: crate::codec::MAX_ENCODING_ENTRIES,
                work_limit: Some(work_limit),
            },
        )?;
        let entries: Vec<_> = entries
            .into_iter()
            .filter(|(work, _)| selected.contains(&work.id()))
            .collect();
        let output_slots = entries
            .iter()
            .map(|(work, _)| work.job.account)
            .collect::<BTreeSet<_>>()
            .len();
        let units = self.view.profile.allocation_budget(fees, output_slots)?;
        let n = usize::from(units);
        let total: u128 = entries.iter().map(|(w, _)| w.job.difficulty as u128).sum();
        let mut quotas: Vec<u16> = entries
            .iter()
            .map(|(w, _)| (n as u128 * w.job.difficulty as u128 / total) as u16)
            .collect();
        let mut priority: Vec<usize> = (0..entries.len()).collect();
        priority.sort_by_key(|i| {
            (
                std::cmp::Reverse(n as u128 * entries[*i].0.job.difficulty as u128 % total),
                *i,
            )
        });
        for i in priority
            .into_iter()
            .take(n - quotas.iter().map(|q| *q as usize).sum::<usize>())
        {
            quotas[i] += 1;
        }
        let mut accounts: BTreeMap<Point, AccountAllocation> = BTreeMap::new();
        let mut receipts = Vec::new();
        let mut primary_bonus = vec![0u16; entries.len()];
        let mut primary_index = None;
        for (index, (_, class)) in entries.iter().enumerate() {
            if *class == 0 {
                primary_index = Some(index);
            } else if let Some(primary) = primary_index {
                primary_bonus[primary] = primary_bonus[primary]
                    .checked_add(quotas[index] / 5)
                    .ok_or(Error::Invalid("uncle penalty overflow"))?;
            }
        }
        for (index, ((w, class), units)) in entries.iter().zip(quotas).enumerate() {
            self.budget.check()?;
            let (r, inv) = self.registration(w)?;
            let expires = r
                .fields
                .birth
                .height
                .checked_add(self.view.profile.manifest.lifetime)
                .ok_or(Error::Invalid("expiry overflow"))?;
            if !(r.fields.birth.height <= anchor.height && anchor.height < expires) {
                return Err(Error::Invalid("registration expiry"));
            }
            receipts.push(ReceiptRow {
                work_id: w.id,
                account: w.job.account,
                class: *class,
                difficulty: w.job.difficulty,
                units,
            });
            let a = accounts
                .entry(w.job.account)
                .or_insert_with(|| AccountAllocation::new(index, r.clone(), inv.clone()));
            if *class == 0 {
                a.canonical = a
                    .canonical
                    .checked_add(units)
                    .and_then(|value| value.checked_add(primary_bonus[index]))
                    .ok_or(Error::Invalid("canonical allocation overflow"))?;
            } else {
                a.uncle += units;
            }
            a.renew(index, r, inv);
        }
        let mut rows = Vec::new();
        for allocation in accounts.values() {
            let base = allocation.canonical + 4 * (allocation.uncle / 5);
            let residual = allocation.uncle % 5;
            for row in &allocation.inventory.rows.0 {
                let units = if row.family == 0 { base } else { residual };
                if units & (1 << row.row) == 0 {
                    continue;
                }
                let commitments = row
                    .coupon_ids
                    .0
                    .iter()
                    .map(|id| {
                        self.facts
                            .material
                            .get(id)
                            .map(|material| material.output.commitment())
                            .ok_or(Error::Unavailable(Key::new(Kind::Coupon, *id)))
                    })
                    .collect::<Result<Vec<_>>>()?;
                rows.push((allocation.registration.fields.birth.clone(), commitments));
            }
        }
        prefetch_coupon_presence(anchor, &rows, |historical_anchor, commitments| {
            self.historical_unspent_many(historical_anchor, commitments)?;
            commitments
                .iter()
                .map(|commitment| self.historical_unspent(historical_anchor, *commitment))
                .collect()
        })?;
        let mut account_rows = Vec::new();
        let mut inserted = Vec::new();
        let mut cs = BTreeSet::new();
        let mut es = BTreeSet::new();
        let mut paid = 0u64;
        for (owner, allocation) in accounts {
            let AccountAllocation {
                canonical: c,
                uncle: t,
                registration: r,
                inventory: inv,
                ..
            } = allocation;
            let base = c + 4 * (t / 5);
            let residual = (t % 5) as u8;
            let mut outcomes = Vec::new();
            for row in &inv.rows.0 {
                let units = if row.family == 0 {
                    base
                } else {
                    residual as u16
                };
                if units & (1 << row.row) == 0 {
                    continue;
                }
                let value = self.view.profile.face(row.family, row.row)?;
                let mut outcome = Outcome {
                    family: row.family,
                    row: row.row,
                    leaf: 255,
                    coupon: None,
                    code: 1,
                    value,
                };
                for (leaf, cid) in row.coupon_ids.0.iter().enumerate() {
                    self.budget.check()?;
                    let material = self.coupon(*cid)?;
                    let (output, kernel) = (&material.output, &material.kernel);
                    if self.historical_unspent(anchor, output.commitment())? {
                        continue;
                    }
                    outcome.leaf = leaf as u8;
                    outcome.coupon = Some(*cid);
                    outcome.code =
                        if self.historical_unspent(&r.fields.birth, output.commitment())? {
                            2
                        } else {
                            let intrinsic = self.coupon_check(
                                *cid,
                                &material.coupon,
                                owner,
                                value,
                                output,
                                kernel,
                            )?;
                            if intrinsic != 0 {
                                intrinsic
                            } else if cs.contains(&output.commitment().0) {
                                8
                            } else if es.contains(&kernel.excess.0) {
                                9
                            } else {
                                0
                            }
                        };
                    if outcome.code == 0 {
                        cs.insert(output.commitment().0);
                        es.insert(kernel.excess.0);
                        paid = paid
                            .checked_add(value)
                            .ok_or(Error::Invalid("paid overflow"))?;
                        inserted.push((owner, *output, *kernel));
                    }
                    break;
                }
                outcomes.push(outcome);
            }
            account_rows.push(AccountRow {
                account: owner,
                registration: id(r.as_ref())?,
                canonical_units: c,
                uncle_units: t,
                base_units: base,
                residual_units: residual,
                outcomes: List(outcomes),
            });
        }
        if inserted.len() > output_bound(units, output_slots.saturating_add(1)) {
            return Err(Error::Invalid("payout output envelope"));
        }
        let reward = self
            .view
            .profile
            .manifest
            .subsidy
            .checked_add(fees)
            .ok_or(Error::Invalid("reward overflow"))?;
        let finder = reward
            .checked_sub(paid)
            .ok_or(Error::Invalid("overpayment"))?;
        let financial = Financial {
            record: PayoutRecord {
                protocol: self.view.profile.protocol,
                anchor: anchor.clone(),
                parent: parent.clone(),
                receipts: List(receipts),
                accounts: List(account_rows),
                paid,
                finder,
            },
            coupons: inserted,
        };
        let trace = Trace::join(self.traces[trace_start..].iter().cloned());
        self.facts.payout = Some(Arc::new(PayoutState::new(fees, financial.clone(), trace)));
        Ok(financial)
    }
}

#[cfg(test)]
#[path = "payout_tests.rs"]
mod tests;
