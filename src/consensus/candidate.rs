use super::*;
use grin_core::core::{Block, KernelFeatures, Transaction, TxKernel, Weighting};
use grin_util::{secp, static_secp_instance};

impl<S: Source> Validator<'_, '_, S> {
    pub fn full_check(&mut self, id: Id) -> Result<Block> {
        let work = self.work(id)?;
        self.parent(&work)?;
        if work.job.uncles.0.len() > usize::from(self.view.profile.max_uncles) {
            return Err(Error::Invalid("too many uncles"));
        }
        self.uncles(&work)?;
        self.registration(&work)?;
        let tx_object: TxOnly = self.object(work.job.transaction_only)?;
        let tx: Transaction = native::parse(&tx_object.native.0)?;
        if tx.outputs().iter().any(|o| o.is_coinbase())
            || tx.kernels().iter().any(|k| k.is_coinbase())
        {
            return Err(Error::Invalid("transaction-only coinbase"));
        }
        let output: FinderOutput = self.object(work.job.finder_output)?;
        let residual: Residual = decode(&self.read(Key::new(Kind::Residual, id))?)?;
        let encoding: EncodingPlan = self.object(work.job.kernel_encoding_plan)?;
        if tx != Transaction::empty() {
            tx.validate(Weighting::AsBlock)
                .map_err(|_| Error::Invalid("transaction validation"))?;
        }
        let fees = tx.kernels().iter().try_fold(0u64, |sum, k| {
            let fee = match k.features {
                KernelFeatures::Plain { fee }
                | KernelFeatures::HeightLocked { fee, .. }
                | KernelFeatures::NoRecentDuplicate { fee, .. } => fee.fee(),
                KernelFeatures::Coinbase => 0,
            };
            sum.checked_add(fee).ok_or(Error::Invalid("fees overflow"))
        })?;
        let financial = self.plan(&work.job.parent, &work.job.anchor, fees)?;
        if financial.record.finder != work.job.finder_value {
            return Err(Error::Invalid("finder allocation"));
        }
        let finder: grin_core::core::Output = native::parse(&output.output.0)?;
        if finder.commitment().0 != work.job.finder_commitment
            || !finder.is_coinbase()
            || financial
                .coupons
                .iter()
                .any(|(_, o, _)| o.commitment() == finder.commitment())
        {
            return Err(Error::Invalid("finder output"));
        }
        let finder_kernel: TxKernel = native::parse(&residual.finder_kernel.0)?;
        if !finder_kernel.is_coinbase() {
            return Err(Error::Invalid("finder kernel features"));
        }
        let handle = static_secp_instance();
        let secp = handle.lock();
        let expected_finder = secp
            .commit_sum(
                vec![finder.commitment()],
                vec![secp
                    .commit_value(work.job.finder_value)
                    .map_err(|_| Error::Invalid("finder value"))?],
            )
            .map_err(|_| Error::Invalid("finder sum"))?;
        if finder_kernel.excess != expected_finder {
            return Err(Error::Invalid("finder kernel excess"));
        }
        drop(secp);
        if encoding
            .entries
            .0
            .windows(2)
            .any(|w| w[0].account >= w[1].account)
        {
            return Err(Error::Invalid("encoding order"));
        }
        let mut groups: BTreeMap<Point, Vec<TxKernel>> = BTreeMap::new();
        for (owner, _, k) in &financial.coupons {
            groups.entry(*owner).or_default().push(*k);
        }
        if encoding.entries.0.len() != groups.len() {
            return Err(Error::Invalid("encoding coverage"));
        }
        let mut kernels = Vec::new();
        let mut aggregate_index = 0;
        for entry in &encoding.entries.0 {
            let group = groups
                .remove(&entry.account)
                .ok_or(Error::Invalid("encoding owner"))?;
            match &entry.encoding {
                Encoding::Individual => kernels.extend(group),
                Encoding::Aggregate(cid) => {
                    if group.len() < 2 {
                        return Err(Error::Invalid("single-output aggregation"));
                    }
                    let bytes = residual
                        .aggregate_kernels
                        .0
                        .get(aggregate_index)
                        .ok_or(Error::Invalid("missing aggregate"))?;
                    aggregate_index += 1;
                    let object = Kernel {
                        native: bytes.clone(),
                    };
                    if crate::codec::id(&object)? != *cid {
                        return Err(Error::Invalid("aggregate CID"));
                    }
                    let k: TxKernel = native::parse(&bytes.0)?;
                    let excess = static_secp_instance()
                        .lock()
                        .commit_sum(group.iter().map(|k| k.excess).collect(), vec![])
                        .map_err(|_| Error::Invalid("aggregate zero"))?;
                    if !k.is_coinbase() || k.excess != excess || k.verify().is_err() {
                        return Err(Error::Invalid("aggregate signature/excess"));
                    }
                    kernels.push(k);
                }
            }
        }
        if aggregate_index != residual.aggregate_kernels.0.len() {
            return Err(Error::Invalid("extra aggregate"));
        }
        let parent = self.view.anchor(&work.job.anchor)?;
        let binding: TxKernel = native::parse(&residual.binding_kernel.0)?;
        let m = scalar(crate::codec::id(&work.job)?)?;
        let handle = static_secp_instance();
        let secp = handle.lock();
        let m = secp::key::SecretKey::from_slice(&secp, &m).unwrap();
        let mut subtract = Vec::new();
        for factor in [&parent.total_kernel_offset, &tx.offset] {
            if !factor.is_zero() {
                subtract.push(
                    factor
                        .secret_key(&secp)
                        .map_err(|_| Error::Invalid("offset scalar"))?,
                );
            }
        }
        let beta = secp
            .blind_sum(vec![m], subtract)
            .map_err(|_| Error::Invalid("zero beta"))?;
        if beta.as_ref() == [0; 32] {
            return Err(Error::Invalid("zero beta"));
        }
        let negative = secp
            .blind_sum(vec![], vec![beta])
            .map_err(|_| Error::Invalid("binding negation"))?;
        let expected = secp
            .commit(0, negative)
            .map_err(|_| Error::Invalid("binding commitment"))?;
        drop(secp);
        if binding.features
            != (KernelFeatures::Plain {
                fee: grin_core::core::FeeFields::zero(),
            })
            || binding.excess != expected
            || binding.verify().is_err()
        {
            return Err(Error::Invalid("binding kernel"));
        }
        let mut excesses = BTreeSet::new();
        for k in kernels
            .iter()
            .chain([&finder_kernel, &binding])
            .chain(tx.kernels())
        {
            if !excesses.insert(k.excess.0) {
                return Err(Error::Invalid("job kernel collision"));
            }
        }
        let mut block = Block {
            header: work.header.clone(),
            body: tx
                .body
                .clone()
                .with_output(finder)
                .with_kernel(finder_kernel)
                .with_kernel(binding),
        };
        for (_, output, _) in financial.coupons {
            block.body = block.body.with_output(output);
        }
        for kernel in kernels {
            block.body = block.body.with_kernel(kernel);
        }
        self.budget.full()?;
        self.view.candidate(&block)?;

        Ok(block)
    }
}
