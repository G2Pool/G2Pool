pub mod stratum;
pub mod submission;
use crate::{
    codec::*,
    consensus::Validator,
    native::{self, Work},
    node::Node,
    objects::{Key, Kind, Source},
    storage::{Object, Representation, Revision},
    wallet::{kernel, Signer},
    Error, Result,
};
use grin_core::{
    core::{Block, FeeFields, KernelFeatures, Transaction, Weighting},
    pow::Proof,
};
use grin_keychain::BlindingFactor;
use grin_util::{secp, static_secp_instance};
use std::collections::BTreeSet;

pub struct Template {
    pub revision: Revision,
    pub job: Job,
    pub block: Block,
    pub residual: Residual,
    pub material: Vec<Object>,
    dependencies: std::collections::BTreeSet<Key>,
}
pub(crate) struct Preparation {
    source: crate::storage::ReadSource,
    chain: std::sync::Arc<grin_chain::Chain>,
    profile: native::Profile,
    revision: Revision,
    parent: Parent,
    facts: Option<crate::consensus::Facts>,
    limits: crate::limits::Limits,
    cancel: std::sync::Arc<std::sync::atomic::AtomicBool>,
}
pub(crate) struct Prepared {
    template: Template,
    registration: crate::wallet::RegistrationPlan,
    bindings: std::collections::BTreeMap<Key, (Representation, Id)>,
    checked_at: i64,
}
impl Prepared {
    pub(crate) fn finish(self, node: &mut Node) -> Result<Template> {
        node.ensure_view()?;
        let view = node.view()?;
        if node.revision()? != self.template.revision
            || view.head != self.template.job.anchor
            || view.now < self.checked_at
        {
            return Err(Error::View);
        }
        if node.cancel.load(std::sync::atomic::Ordering::Acquire) {
            return Err(Error::Cancelled);
        }
        for (key, binding) in self.bindings {
            if node.store.binding(key)? != binding {
                return Err(Error::View);
            }
        }
        let mut objects = self.registration.objects.clone();
        objects.extend(self.template.material.iter().cloned());
        node.store
            .publish_registration(&objects, &self.registration)?;
        Ok(self.template)
    }
}
impl Preparation {
    pub(crate) fn new(node: &mut Node) -> Result<Self> {
        node.ensure_view()?;
        let revision = node.revision()?;
        Ok(Self {
            source: node.store.read_source()?,
            chain: node.chain.clone(),
            profile: node.profile.clone(),
            parent: node.parent()?,
            facts: node.verified_facts(&revision.anchor).cloned(),
            revision,
            limits: node.limits.clone(),
            cancel: node.cancel.clone(),
        })
    }
    pub(crate) fn run<C: grin_wallet_libwallet::NodeClient + 'static>(
        self,
        wallet: &Signer,
        payments: &crate::wallet::payments::Payments<C>,
        transaction: Transaction,
    ) -> Result<Prepared> {
        let store = self.source.open()?;
        let chain = self.chain;
        let profile = self.profile;
        let revision = self.revision;
        let parent = self.parent;
        let view = native::View::pin(&chain, profile.clone(), chrono::Utc::now().timestamp())?;
        if view.head != revision.anchor {
            return Err(Error::View);
        }
        let uncles = uncles(
            &store,
            &view,
            &parent,
            &self.limits,
            &self.cancel,
            self.facts.as_ref(),
        )?;
        let transaction_inputs: Vec<_> = match transaction.inputs() {
            grin_core::core::Inputs::CommitOnly(inputs) => {
                inputs.iter().map(|input| input.commitment()).collect()
            }
            grin_core::core::Inputs::FeaturesAndCommit(inputs) => {
                inputs.iter().map(|input| input.commitment()).collect()
            }
        };
        let registration =
            wallet.registration_for_transaction(&store, &view, payments, &transaction_inputs)?;
        let registration_id = registration.id;
        let own_registration: Registration = decode(&registration.bytes)?;
        let own_inventory: Inventory = if let Some(object) =
            registration.objects.iter().find(|object| {
                object.key == Key::new(Kind::Inventory, own_registration.fields.inventory)
            }) {
            decode(&object.bytes)?
        } else {
            decode(&store.read(Key::new(Kind::Inventory, own_registration.fields.inventory))?)?
        };
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
        let fees = transaction.fee();
        let mut validator =
            Validator::bounded(&store, &view, self.limits.clone(), self.cancel.clone());
        if let Some(facts) = self.facts.as_ref() {
            validator.seed(facts);
        }
        let financial = validator.plan(&parent, &view.head, fees)?;
        let difficulty = validator.share_difficulty(&parent, &view.head)?;
        let mut dependencies = validator.dependencies()?;
        dependencies.insert(Key::new(Kind::Registration, registration_id));
        let inventory = Key::new(Kind::Inventory, own_registration.fields.inventory);
        dependencies.insert(inventory);
        for row in own_inventory.rows.0 {
            for coupon in row.coupon_ids.0 {
                dependencies.insert(Key::new(Kind::Coupon, coupon));
            }
        }
        for uncle in &uncles {
            let mut validator =
                Validator::bounded(&store, &view, self.limits.clone(), self.cancel.clone());
            if let Some(facts) = self.facts.as_ref() {
                validator.seed(facts);
            }
            dependencies.extend(validator.evaluate(*uncle)?.required);
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
                Some(wallet.aggregate(&group.iter().map(|(o, _)| *o).collect::<Vec<_>>())?)
            } else {
                None
            };
            let encoding = if let Some(kernel) = aggregate {
                let native = Blob(native::native_bytes(&kernel)?);
                let object = Kernel {
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
            entries: List(entries),
        };
        let tx = TxOnly {
            native: Blob(native::native_bytes(&transaction)?),
        };
        let reward = payments.reward(wallet, financial.record.finder)?;
        let finder = FinderOutput {
            output: Blob(native::native_bytes(&reward.output)?),
        };
        let native_parent = view.anchor(&view.head)?;
        let target = view.target(&view.head)?;
        let timestamp = view.now.max(
            native_parent
                .timestamp
                .timestamp()
                .checked_add(1)
                .ok_or(Error::Invalid("timestamp overflow"))?,
        );
        let job = Job {
            pool_id: profile.pool_id,
            anchor: view.head.clone(),
            timestamp,
            parent,
            account: wallet.account(),
            registration: registration_id,
            difficulty,
            uncles: List(uncles),
            transaction_only: id(&tx)?,
            kernel_encoding_plan: id(&encoding)?,
            finder_value: financial.record.finder,
            finder_commitment: reward.output.commitment().0,
            finder_output: id(&finder)?,
        };
        let scalar = scalar(id(&job)?)?;
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
            edge_bits: profile.edge_bits,
            nonces: vec![0; profile.proof_size as usize],
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
            finder_kernel: Blob(native::native_bytes(&reward.kernel)?),
            binding_kernel: Blob(native::native_bytes(&binding)?),
            aggregate_kernels: List(aggregate_kernels),
        };
        let material = vec![
            Object::content(&encoding)?,
            Object::content(&tx)?,
            Object::content(&finder)?,
        ];
        view.check()?;
        let new_keys: std::collections::BTreeSet<_> = registration
            .objects
            .iter()
            .chain(&material)
            .map(|object| object.key)
            .collect();
        let mut bindings = std::collections::BTreeMap::new();
        for key in dependencies.difference(&new_keys) {
            bindings.insert(*key, store.binding(*key)?);
        }
        let template = Template {
            revision,
            job,
            block,
            residual,
            material,
            dependencies,
        };
        Ok(Prepared {
            template,
            registration,
            bindings,
            checked_at: view.now,
        })
    }
}
impl Template {
    pub(crate) fn prepare<C: grin_wallet_libwallet::NodeClient + 'static>(
        node: &std::sync::Arc<std::sync::Mutex<Node>>,
        wallet: &Signer,
        payments: &crate::wallet::payments::Payments<C>,
        transaction: Transaction,
    ) -> Result<Self> {
        let preparation =
            Preparation::new(&mut *node.lock().map_err(|_| Error::Invalid("node poisoned"))?)?;
        let prepared = preparation.run(wallet, payments, transaction)?;
        prepared.finish(&mut *node.lock().map_err(|_| Error::Invalid("node poisoned"))?)
    }
    pub fn pre_pow(&self) -> String {
        let bytes = self.block.header.pre_pow();
        crate::objects::hex(&bytes[..bytes.len() - 8])
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

fn uncles(
    store: &crate::storage::Store,
    view: &native::View<'_>,
    parent: &Parent,
    limits: &crate::limits::Limits,
    cancel: &std::sync::Arc<std::sync::atomic::AtomicBool>,
    facts: Option<&crate::consensus::Facts>,
) -> Result<Vec<Id>> {
    let Parent::Receipt(tip) = parent else {
        return Ok(vec![]);
    };
    let mut cursor = Some(*tip);
    let mut ancestors = Vec::new();
    let mut forbidden = BTreeSet::new();
    let depth = usize::from(view.profile.uncle_depth);
    for _ in 0..depth {
        let Some(id) = cursor else { break };
        let bytes = match store.read(Key::new(Kind::Core, id)) {
            Ok(bytes) => bytes,
            Err(Error::Unavailable(_)) => break,
            Err(error) => return Err(error),
        };
        let work = Work::verify(&bytes, view)?;
        if work.job().anchor.height < view.profile.finance_low(view.head.height) {
            break;
        }
        forbidden.insert(id);
        forbidden.extend(work.job().uncles.0.iter().copied());
        cursor = match work.job().parent {
            Parent::Receipt(id) => Some(id),
            Parent::Root(_) => None,
        };
        ancestors.push(work);
    }
    let mut uncles = Vec::new();
    for head in store.heads()? {
        if forbidden.contains(&head.tip) {
            continue;
        }
        if head.anchor != view.head {
            continue;
        }
        let work = match store
            .read(Key::new(Kind::Core, head.tip))
            .and_then(|bytes| Work::verify(&bytes, view))
        {
            Ok(work) => work,
            Err(
                Error::View | Error::Expired | Error::Unavailable(_) | Error::NativeUnavailable(_),
            ) => continue,
            Err(error) => return Err(error),
        };
        if work.job().uncles.0.len() > usize::from(view.profile.max_uncles) {
            continue;
        }
        if ancestors
            .iter()
            .any(|a| a.job().parent == work.job().parent && a.job().anchor == work.job().anchor)
        {
            let mut validator = Validator::bounded(store, view, limits.clone(), cancel.clone());
            if let Some(facts) = facts {
                validator.seed(facts);
            }
            validator.evaluate(head.tip)?;
            uncles.push(head.tip);
        }
    }
    uncles.sort();
    uncles.truncate(usize::from(view.profile.max_uncles));
    Ok(uncles)
}
