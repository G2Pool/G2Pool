pub mod stratum;
pub mod submission;
use crate::{
    codec::*,
    consensus::Validator,
    native::{self, Work},
    node::Node,
    objects::{Key, Kind, Source},
    storage::{Object, Revision},
    wallet::{kernel, Wallet},
    Error, Result,
};
use grin_chain::Options;
use grin_core::{
    core::{Block, FeeFields, KernelFeatures, Transaction, Weighting},
    pow::{self, Proof},
};
use grin_keychain::BlindingFactor;
use grin_util::{secp, static_secp_instance};

pub struct Template {
    pub revision: Revision,
    pub job: Job,
    pub block: Block,
    pub residual: Residual,
    pub material: Vec<Object>,
    dependencies: std::collections::BTreeSet<Key>,
}
impl Template {
    pub fn prepare(node: &mut Node, wallet: &Wallet, transaction: Transaction) -> Result<Self> {
        node.ensure_view()?;
        let parent = node.parent()?;
        let uncles = node.uncles(&parent)?;
        let profile = node.profile.clone();
        let chain = node.chain.clone();
        let view = native::View::pin(&chain, profile.clone(), chrono::Utc::now().timestamp())?;
        let transaction_inputs: Vec<_> = match transaction.inputs() {
            grin_core::core::Inputs::CommitOnly(inputs) => {
                inputs.iter().map(|input| input.commitment()).collect()
            }
            grin_core::core::Inputs::FeaturesAndCommit(inputs) => {
                inputs.iter().map(|input| input.commitment()).collect()
            }
        };
        let registration =
            wallet.registration_for_transaction(&mut node.store, &view, &transaction_inputs)?;
        let revision = node.revision()?;
        if transaction != Transaction::empty() {
            transaction
                .validate(Weighting::AsBlock)
                .map_err(|_| Error::Invalid("template transaction"))?;
        }
        let transaction_weight = Transaction::weight_by_iok(
            transaction.inputs().len() as u64,
            transaction.outputs().len() as u64,
            transaction.kernels().len() as u64,
        );
        if transaction_weight > profile.payout_weight_reservation() {
            return Err(Error::Invalid("template transaction weight"));
        }
        if transaction.outputs().iter().any(|o| o.is_coinbase())
            || transaction.kernels().iter().any(|k| k.is_coinbase())
        {
            return Err(Error::Invalid("template coinbase transaction"));
        }
        let fees = transaction.kernels().iter().try_fold(0u64, |sum, k| {
            sum.checked_add(match k.features {
                KernelFeatures::Plain { fee }
                | KernelFeatures::HeightLocked { fee, .. }
                | KernelFeatures::NoRecentDuplicate { fee, .. } => fee.fee(),
                KernelFeatures::Coinbase => 0,
            })
            .ok_or(Error::Invalid("template fee sum"))
        })?;
        let mut validator =
            Validator::bounded(&node.store, &view, node.limits.clone(), node.cancel.clone());
        if let Some(facts) = node.verified_facts(&view.head) {
            validator.seed(facts);
        }
        let financial = validator.plan(&parent, &view.head, fees)?;
        let mut dependencies = validator.dependencies()?;
        let own_registration: Registration = decode(
            &node
                .store
                .read(Key::new(Kind::Registration, registration))?,
        )?;
        dependencies.insert(Key::new(Kind::Registration, registration));
        let inventory = Key::new(Kind::Inventory, own_registration.fields.inventory);
        let own_inventory: Inventory = decode(&node.store.read(inventory)?)?;
        dependencies.insert(inventory);
        for row in own_inventory.rows.0 {
            for coupon in row.coupon_ids.0 {
                dependencies.insert(Key::new(Kind::Coupon, coupon));
            }
        }
        for uncle in &uncles {
            dependencies.extend(node.evaluate(*uncle)?.required);
        }
        let mut groups = std::collections::BTreeMap::<
            Point,
            Vec<(grin_core::core::Output, grin_core::core::TxKernel)>,
        >::new();
        for (owner, output, kernel) in &financial.coupons {
            groups.entry(*owner).or_default().push((*output, *kernel));
        }
        let mut kernels = Vec::new();
        let mut aggregate_kernels = Vec::new();
        let mut entries = Vec::new();
        for (owner, group) in groups {
            let aggregate = if owner == wallet.account() && group.len() > 1 {
                wallet
                    .aggregate(&group.iter().map(|(o, _)| *o).collect::<Vec<_>>())
                    .ok()
            } else {
                None
            };
            let encoding = if let Some(kernel) = aggregate {
                let native = Blob(native::native_bytes(&kernel)?);
                let object = Kernel {
                    protocol: profile.protocol,
                    native: native.clone(),
                };
                aggregate_kernels.push(native);
                kernels.push(kernel);
                Encoding::Aggregate(id(&object)?)
            } else {
                kernels.extend(group.iter().map(|(_, kernel)| *kernel));
                Encoding::Individual
            };
            entries.push(EncodingEntry {
                account: owner,
                encoding,
            });
        }
        let encoding = EncodingPlan {
            protocol: profile.protocol,
            entries: List(entries),
        };
        let tx = TxOnly {
            protocol: profile.protocol,
            native: Blob(native::native_bytes(&transaction)?),
        };
        let reward = wallet.reward(&mut node.store, financial.record.finder, true)?;
        let finder = FinderOutput {
            protocol: profile.protocol,
            output: Blob(native::native_bytes(&reward.output)?),
        };
        let native_parent = view.anchor(&view.head)?;
        let target = view.target(&view.head)?;
        let history = node.share_history(match &parent {
            Parent::Root(_) => None,
            Parent::Receipt(id) => Some(*id),
        })?;
        let timestamp = view.now.max(
            native_parent
                .timestamp
                .timestamp()
                .checked_add(1)
                .ok_or(Error::Invalid("timestamp overflow"))?,
        );
        let job = Job {
            protocol: profile.protocol,
            anchor: view.head.clone(),
            timestamp,
            parent,
            account: wallet.account(),
            registration,
            difficulty: profile.retargeted(
                target.difficulty.to_num(),
                native_parent
                    .height
                    .checked_add(1)
                    .ok_or(Error::Invalid("candidate height overflow"))?,
                &history,
                u64::from(profile.manifest.share_target_seconds),
            )?,
            uncles: List(uncles),
            transaction_only: id(&tx)?,
            kernel_encoding_plan: id(&encoding)?,
            finder_value: financial.record.finder,
            finder_commitment: reward.output.commitment().0,
            finder_output: id(&finder)?,
        };
        let scalar = scalar(profile.protocol, id(&job)?)?;
        let negative = {
            let handle = static_secp_instance();
            let secp = handle.lock();
            let secret = secp::key::SecretKey::from_slice(&secp, &scalar)
                .map_err(|_| Error::Invalid("job scalar"))?;
            let mut sub = Vec::new();
            for factor in [&native_parent.total_kernel_offset, &transaction.offset] {
                if !factor.is_zero() {
                    sub.push(
                        factor
                            .secret_key(&secp)
                            .map_err(|_| Error::Invalid("transaction offset"))?,
                    );
                }
            }
            let beta = secp
                .blind_sum(vec![secret], sub)
                .map_err(|_| Error::Invalid("zero beta"))?;
            if beta.as_ref() == [0; 32] {
                return Err(Error::Invalid("zero beta"));
            }
            secp.blind_sum(vec![], vec![beta])
                .map_err(|_| Error::Invalid("binding negation"))?
        };
        let binding = kernel(
            negative,
            KernelFeatures::Plain {
                fee: FeeFields::zero(),
            },
        )?;
        let transactions = if transaction == Transaction::empty() {
            vec![]
        } else {
            vec![transaction]
        };
        let mut block = Block::from_reward(
            &native_parent,
            &transactions,
            reward.output,
            reward.kernel,
            target.difficulty,
        )
        .map_err(|_| Error::Invalid("native block builder"))?;
        block.header.timestamp =
            chrono::DateTime::from_timestamp(timestamp, 0).ok_or(Error::Invalid("timestamp"))?;
        block.header.pow.secondary_scaling = target.secondary_scaling;
        block.header.pow.proof = Proof {
            edge_bits: profile.manifest.edge_bits,
            nonces: vec![0; profile.manifest.proof_size as usize],
        };
        block.header.total_kernel_offset = BlindingFactor::from_slice(&scalar);
        block.body = block.body.with_kernel(binding);
        for (_, output, _) in financial.coupons {
            block.body = block.body.with_output(output);
        }
        for kernel in kernels {
            block.body = block.body.with_kernel(kernel);
        }
        chain
            .set_txhashset_roots(&mut block)
            .map_err(|e| Error::NativeUnavailable(e.to_string()))?;
        view.candidate(&block)?;
        let residual = Residual {
            protocol: profile.protocol,
            finder_kernel: Blob(native::native_bytes(&reward.kernel)?),
            binding_kernel: Blob(native::native_bytes(&binding)?),
            aggregate_kernels: List(aggregate_kernels),
        };
        let material = vec![
            Object::content(&encoding)?,
            Object::content(&tx)?,
            Object::content(&finder)?,
        ];
        node.store.stage(&material)?;
        view.check()?;
        if node.revision()? != revision {
            return Err(Error::View);
        }
        Ok(Self {
            revision,
            job,
            block,
            residual,
            material,
            dependencies,
        })
    }
    pub fn pre_pow(&self) -> String {
        let bytes = self.block.header.pre_pow();
        crate::objects::hex(&bytes[..bytes.len() - 8])
    }
    pub fn submit(
        &self,
        node: &mut Node,
        wallet: &Wallet,
        nonce: u64,
        proof: Proof,
    ) -> Result<Submitted> {
        let mut block = self.block.clone();
        block.header.pow.nonce = nonce;
        block.header.pow.proof = proof;
        pow::verify_size(&block.header).map_err(|_| Error::Invalid("submitted cycle"))?;
        let difficulty = block.header.pow.to_difficulty(block.header.height);
        if difficulty.to_num() < self.job.difficulty {
            return Err(Error::Invalid("submitted difficulty"));
        }
        let core = Core {
            header: Blob(native::native_bytes(&block.header)?),
            job: Blob(encode(&self.job)?),
        };
        let work_id = hash("WORK_ID", &core.header.0);
        let network_target = block
            .header
            .pow
            .total_difficulty
            .to_num()
            .checked_sub(
                node.chain
                    .get_block_header(&block.header.prev_hash)
                    .map_err(|e| Error::NativeUnavailable(e.to_string()))?
                    .total_difficulty()
                    .to_num(),
            )
            .ok_or(Error::Invalid("native target"))?;
        let native_winner = difficulty.to_num() >= network_target;
        let mut native_accepted = false;
        if native_winner {
            let bytes = native::native_bytes(&block)?;
            node.store.submission_intent(work_id, &bytes)?;
            use grin_core::core::hash::Hashed;
            let native_result = if node.chain.get_block(&block.hash()).is_ok() {
                Ok(())
            } else {
                node.chain
                    .process_block(block.clone(), Options::MINE)
                    .map(|_| ())
            };
            match native_result {
                Ok(_) => {
                    native_accepted = true;
                    if let Err(error) = node.store.submission_done(work_id) {
                        eprintln!("native solution accepted; intent cleanup retryable: {error}");
                    }
                }
                Err(error) => {
                    if !matches!(error, grin_chain::Error::Unfit(_)) {
                        return Err(Error::NativeUnavailable(error.to_string()));
                    }
                    node.store.submission_done(work_id)?;
                }
            }
        }
        let pool = (|| -> Result<()> {
            node.ensure_view()?;
            let view = node.view()?;
            let history = node.share_history(match &self.job.parent {
                Parent::Root(_) => None,
                Parent::Receipt(id) => Some(*id),
            })?;
            let assigned = view.profile.retargeted(
                view.target(&self.job.anchor)?.difficulty.to_num(),
                self.block.header.height,
                &history,
                u64::from(view.profile.manifest.share_target_seconds),
            )?;
            let work = Work::verify_with_target(&encode(&core)?, &view, Some(assigned))?;
            let auth = wallet.authorize(self.job.protocol, work.id())?;
            let mut material = self.material.clone();
            material.extend([
                Object::new(Key::new(Kind::Core, work.id()), encode(&core)?)?,
                Object::new(Key::new(Kind::Authorization, work.id()), encode(&auth)?)?,
                Object::new(Key::new(Kind::Residual, work.id()), encode(&self.residual)?)?,
            ]);
            node.stage(&material)?;
            node.consider(work.id())?;
            Ok(())
        })();
        let pool_accepted = match pool {
            Ok(()) => true,
            Err(error) if native_accepted => {
                eprintln!("native solution accepted; pool accounting retryable: {error}");
                false
            }
            Err(error) => return Err(error),
        };
        Ok(Submitted {
            work_id,
            native_accepted,
            pool_accepted,
        })
    }
    pub(crate) fn retained_bytes(&self) -> Result<usize> {
        let block_bytes = native::native_bytes(&self.block)?.len();
        let job_bytes = encode(&self.job)?.len();
        let residual_bytes = encode(&self.residual)?.len();
        let payload = block_bytes
            .checked_add(job_bytes)
            .and_then(|bytes| bytes.checked_add(residual_bytes))
            .ok_or(Error::Capacity("mining job size"))?;
        self.material.iter().try_fold(
            payload
                .saturating_mul(2)
                .saturating_add(self.dependencies.len() * 128)
                .saturating_add(4096),
            |bytes, object| {
                bytes
                    .checked_add(object.bytes.len())
                    .ok_or(Error::Capacity("mining job size"))
            },
        )
    }
    pub fn pins(&self) -> std::collections::BTreeSet<Key> {
        self.dependencies
            .iter()
            .copied()
            .chain(self.material.iter().map(|object| object.key))
            .collect()
    }
}
#[derive(Debug)]
pub struct Submitted {
    pub work_id: Id,
    pub native_accepted: bool,
    pub pool_accepted: bool,
}
