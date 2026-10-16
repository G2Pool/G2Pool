use anyhow::{anyhow, bail, Context, Result};
use grin_core::core::hash::Hashed;
use grin_keychain::{ExtKeychain, ExtKeychainPath, Keychain, SwitchCommitmentType};
use grin_util::ToHex;
use grin_util::{secp::key::SecretKey, Mutex, ZeroingString};
use grin_wallet_api::Owner;
use grin_wallet_impls::{DefaultLCProvider, DefaultWalletImpl};
use grin_wallet_libwallet::{
    AcctPathMapping, InitTxArgs, NodeClient, OutputStatus, SlateState, WalletInst,
};
use serde::{Deserialize, Serialize};
use std::{
    fs::{self, File, OpenOptions},
    path::{Path, PathBuf},
    sync::Arc,
};
use zeroize::Zeroizing;

pub mod cli;
mod direct;
mod scan_guard;
pub use direct::DirectNodeClient;
pub use scan_guard::GuardedNodeClient;

pub type Wallet<C> =
    Arc<Mutex<Box<dyn WalletInst<'static, DefaultLCProvider<C, ExtKeychain>, C, ExtKeychain>>>>;
pub type DaemonPayments = Payments<crate::wallet::payments::DirectNodeClient>;
pub struct Payments<C: NodeClient + 'static> {
    pub wallet: Wallet<GuardedNodeClient<C>>,
    pub owner: Owner<
        DefaultLCProvider<GuardedNodeClient<C>, ExtKeychain>,
        GuardedNodeClient<C>,
        ExtKeychain,
    >,
    pub mask: Option<SecretKey>,
    directory: PathBuf,
    namespace: [u32; 2],
    _lock: File,
}
#[derive(Serialize, Deserialize)]
struct Intent {
    source: String,
    id: String,
    amount: u64,
    stage: String,
    outgoing: String,
    finalized: Option<String>,
    response: Option<String>,
}
fn private_read(path: &Path, max: u64) -> Result<Vec<u8>> {
    regular_read(path, max, true)
}
fn regular_read(path: &Path, max: u64, private: bool) -> Result<Vec<u8>> {
    crate::secure_file::read_regular(path, max, private).map_err(|e| anyhow!(e.to_string()))
}
fn write_private(path: &Path, bytes: &[u8], replace: bool) -> Result<()> {
    crate::secure_file::publish_atomic(path, bytes, replace).map_err(|e| anyhow!(e.to_string()))
}
impl<C: NodeClient + 'static> Payments<C> {
    pub fn open_bound(
        data: &Path,
        client: C,
        account: [u8; 33],
        namespace: [u32; 2],
    ) -> Result<Self> {
        let directory = data.join("payments");
        fs::create_dir_all(&directory)?;
        let meta = fs::symlink_metadata(&directory)?;
        if !meta.is_dir() || meta.file_type().is_symlink() {
            bail!("invalid payments directory");
        }
        #[cfg(unix)]
        fs::set_permissions(&directory, {
            use std::os::unix::fs::PermissionsExt;
            fs::Permissions::from_mode(0o700)
        })?;
        let mut lock_options = OpenOptions::new();
        lock_options.read(true).write(true).create(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            lock_options.mode(0o600).custom_flags(libc::O_NOFOLLOW);
        }
        let lock = lock_options.open(directory.join("payments.lock"))?;
        fs2::FileExt::try_lock_exclusive(&lock).context("payments backend already in use")?;
        let seed = Zeroizing::new(private_read(&data.join("wallet.seed"), 32)?);
        if ![16, 20, 24, 28, 32].contains(&seed.len()) {
            bail!("invalid seed length");
        }
        let keychain = ExtKeychain::from_seed(&seed, false)?;
        let account_secret = keychain.derive_key(
            0,
            &ExtKeychainPath::new(2, 1, 0, 0, 0).to_identifier(),
            SwitchCommitmentType::Regular,
        )?;
        let derived_account =
            grin_util::secp::key::PublicKey::from_secret_key(keychain.secp(), &account_secret)?
                .serialize_vec(keychain.secp(), true);
        if account.as_slice() != derived_account.as_slice() {
            bail!("wallet seed does not match pool store");
        }
        let mut wallet = Box::new(DefaultWalletImpl::<GuardedNodeClient<C>>::new(
            GuardedNodeClient::new(client, keychain),
        )?)
            as Box<
                dyn WalletInst<
                    'static,
                    DefaultLCProvider<GuardedNodeClient<C>, ExtKeychain>,
                    GuardedNodeClient<C>,
                    ExtKeychain,
                >,
            >;
        let lc = wallet.lc_provider()?;
        lc.set_top_level_directory(directory.to_str().context("non-UTF8 data directory")?)?;
        if !lc.wallet_exists(None)? {
            lc.create_wallet(
                None,
                Some(ZeroingString::from(
                    grin_keychain::mnemonic::from_entropy(&seed)
                        .map_err(|_| anyhow!("invalid seed entropy"))?,
                )),
                seed.len(),
                ZeroingString::from(""),
                false,
            )?;
        }
        let mask = lc.open_wallet(None, ZeroingString::from(""), true, false)?;
        let actual = lc.wallet_inst()?.keychain(mask.as_ref())?.derive_key(
            0,
            &ExtKeychainPath::new(2, 1, 0, 0, 0).to_identifier(),
            SwitchCommitmentType::Regular,
        )?;
        if actual != account_secret {
            bail!("payments backend seed mismatch");
        }
        let wallet = Arc::new(Mutex::new(wallet));
        let owner = Owner::new(wallet.clone(), None, directory.join("grin-wallet.toml"));
        let result = Self {
            wallet,
            owner,
            mask,
            directory,
            namespace,
            _lock: lock,
        };
        result.ensure_accounts()?;
        Ok(result)
    }
    fn ensure_accounts(&self) -> Result<()> {
        let existing = self.owner.accounts(self.mask.as_ref())?;
        let mut lock = self.wallet.lock();
        let backend = lock.lc_provider()?.wallet_inst()?;
        let mut batch = backend.batch(self.mask.as_ref())?;
        for role in [2, 3, 6] {
            let path = ExtKeychainPath::new(3, role, self.namespace[0], self.namespace[1], 0)
                .to_identifier();
            let label = format!("g2pool-role-{role}");
            if let Some(a) = existing.iter().find(|a| a.label == label) {
                if a.path != path {
                    bail!("reserved account path mismatch");
                }
            } else {
                batch.save_acct_path(AcctPathMapping { label, path })?;
            }
        }
        batch.commit()?;
        drop(lock);
        self.owner
            .set_active_account(self.mask.as_ref(), "g2pool-role-6")?;
        Ok(())
    }
    fn check_stored_encoding(&self, id: &str) -> Result<()> {
        let path = self
            .directory
            .join("wallet_data/saved_txs")
            .join(format!("{id}.grintx"));
        let hex = String::from_utf8(regular_read(&path, 2_000_000, false)?)?;
        let bytes = grin_util::from_hex(&hex)
            .map_err(|_| anyhow!("invalid stored native transaction encoding"))?;
        let tx: grin_core::core::Transaction = grin_core::ser::deserialize(
            &mut bytes.as_slice(),
            grin_core::ser::ProtocolVersion(1),
            grin_core::ser::DeserializationMode::Full,
        )?;
        if tx.kernels().len() != 1 || tx.aggregate_fee_fields().is_err() {
            bail!("invalid stored native transaction structure");
        }
        Ok(())
    }
    fn ensure_change_capacity(&self) -> Result<()> {
        let parent =
            ExtKeychainPath::new(3, 6, self.namespace[0], self.namespace[1], 0).to_identifier();
        let mut lock = self.wallet.lock();
        let backend = lock.lc_provider()?.wallet_inst()?;
        if backend.current_child_index(&parent)? == u32::MAX {
            bail!("payment change derivation capacity exhausted");
        }
        Ok(())
    }
    fn group_mining_outputs(&self) -> Result<()> {
        let parent =
            ExtKeychainPath::new(3, 6, self.namespace[0], self.namespace[1], 0).to_identifier();
        let mut lock = self.wallet.lock();
        let backend = lock.lc_provider()?.wallet_inst()?;
        let outputs: Vec<_> = backend
            .iter()?
            .filter(|o| {
                let p = o.key_id.to_path();
                p.depth == 4
                    && [2, 3].contains(&u32::from(p.path[0]))
                    && u32::from(p.path[1]) == self.namespace[0]
                    && u32::from(p.path[2]) == self.namespace[1]
                    && o.status == OutputStatus::Unspent
            })
            .collect();
        let mut batch = backend.batch(self.mask.as_ref())?;
        for mut output in outputs {
            if output.root_key_id != parent {
                output.root_key_id = parent.clone();
                output.tx_log_entry = None;
                batch.save(output)?;
            }
        }
        batch.commit()?;
        Ok(())
    }
    fn save(&self, i: &Intent) -> Result<()> {
        write_private(
            &self.directory.join("intent.json"),
            &serde_json::to_vec(i)?,
            true,
        )
    }
    fn load(&self) -> Result<Intent> {
        self.load_path(&self.directory.join("intent.json"))
    }
    fn load_path(&self, path: &Path) -> Result<Intent> {
        let i: Intent = serde_json::from_slice(&private_read(path, 2_000_000)?)?;
        let original = self.owner.slate_from_slatepack_message(
            self.mask.as_ref(),
            i.outgoing.clone(),
            vec![],
        )?;
        if i.source != "g2pool-role-6"
            || original.id.to_string() != i.id
            || original.amount != i.amount
            || original.state != SlateState::Standard1
            || ![
                "locking",
                "awaiting-response",
                "finalizing",
                "ready-to-post",
                "posted",
            ]
            .contains(&i.stage.as_str())
        {
            bail!("invalid payment intent");
        }
        Ok(i)
    }
    pub fn offer(&self, amount: u64) -> Result<String> {
        check_amount(amount)?;
        if self.directory.join("intent.json").exists() {
            let mut previous = self.load()?;
            if previous.stage == "posted" {
                self.archive(&previous)?;
            } else if previous.amount == amount
                && ["locking", "awaiting-response"].contains(&previous.stage.as_str())
            {
                self.owner
                    .set_active_account(self.mask.as_ref(), "g2pool-role-6")?;
                if previous.stage == "locking" {
                    let original = self.owner.slate_from_slatepack_message(
                        self.mask.as_ref(),
                        previous.outgoing.clone(),
                        vec![],
                    )?;
                    let (_, txs) = self.owner.retrieve_txs(
                        self.mask.as_ref(),
                        false,
                        None,
                        Some(original.id),
                        None,
                    )?;
                    if txs.is_empty() {
                        self.owner.tx_lock_outputs(self.mask.as_ref(), &original)?;
                    }
                    previous.stage = "awaiting-response".into();
                    self.save(&previous)?;
                }
                return Ok(previous.outgoing);
            } else {
                bail!(
                    "pending payment {} exists; finalize its response before a different send",
                    previous.id
                );
            }
        }
        self.ensure_change_capacity()?;
        self.owner.scan(self.mask.as_ref(), Some(1), false)?;
        self.owner
            .set_active_account(self.mask.as_ref(), "g2pool-role-6")?;
        self.group_mining_outputs()?;
        self.ensure_change_capacity()?;
        let source = "g2pool-role-6".to_owned();
        let args = InitTxArgs {
            src_acct_name: Some(source.clone()),
            amount,
            minimum_confirmations: 1,
            max_outputs: 500,
            num_change_outputs: 1,
            selection_strategy_is_use_all: false,
            ..Default::default()
        };
        let slate = self.owner.init_send_tx(self.mask.as_ref(), args)?;
        {
            let mut lock = self.wallet.lock();
            let backend = lock.lc_provider()?.wallet_inst()?;
            let context = backend.get_private_context(self.mask.as_ref(), slate.id.as_bytes())?;
            check_payment_weight(
                context.input_ids.len() as u64,
                context.output_ids.len() as u64,
            )?;
        }
        let outgoing =
            self.owner
                .create_slatepack_message(self.mask.as_ref(), &slate, None, vec![])?;
        let mut i = Intent {
            source,
            id: slate.id.to_string(),
            amount,
            stage: "locking".into(),
            outgoing,
            finalized: None,
            response: None,
        };
        self.save(&i)?;
        self.owner.tx_lock_outputs(self.mask.as_ref(), &slate)?;
        i.stage = "awaiting-response".into();
        self.save(&i)?;
        Ok(i.outgoing)
    }
    pub fn balance(&self) -> Result<grin_wallet_libwallet::WalletInfo> {
        self.owner.scan(self.mask.as_ref(), Some(1), false)?;
        self.owner
            .set_active_account(self.mask.as_ref(), "g2pool-role-6")?;
        self.group_mining_outputs()?;
        Ok(self
            .owner
            .retrieve_summary_info(self.mask.as_ref(), true, 1)?
            .1)
    }
    fn archive(&self, i: &Intent) -> Result<()> {
        let archived = self.directory.join(format!("{}.posted.json", i.id));
        fs::rename(self.directory.join("intent.json"), archived)?;
        crate::secure_file::sync_directory(&self.directory)?;
        Ok(())
    }
    fn preview_response(
        &self,
        response: &grin_wallet_libwallet::Slate,
        original: &grin_wallet_libwallet::Slate,
    ) -> Result<()> {
        use grin_core::{
            core::{committed::Committed, KernelFeatures, Output, OutputFeatures},
            libtx::{aggsig, build, proof::ProofBuilder, tx_fee},
        };
        if original.kernel_features != 0
            || original.kernel_features_args.is_some()
            || original.payment_proof.is_some()
            || response.kernel_features != 0
            || response.kernel_features_args.is_some()
            || response.payment_proof.is_some()
            || response.participant_data.len() != 1
            || response.participant_data[0].part_sig.is_none()
        {
            bail!("unsupported or incomplete recipient response");
        }
        let body = response
            .tx
            .as_ref()
            .context("recipient transaction missing")?;
        if !body.inputs().is_empty()
            || body.outputs().len() != 1
            || body.outputs()[0].features() != OutputFeatures::Plain
            || body.kernels().len() != 1
            || body.kernels()[0].features
                != (KernelFeatures::Plain {
                    fee: response.fee_fields,
                })
        {
            bail!("recipient response must contain one plain output and no inputs");
        }
        let mut lock = self.wallet.lock();
        let backend = lock.lc_provider()?.wallet_inst()?;
        let context = backend.get_private_context(self.mask.as_ref(), original.id.as_bytes())?;
        if context.late_lock_args.is_some()
            || context.amount != original.amount
            || context.fee != Some(original.fee_fields)
        {
            bail!("payment context does not match original offer");
        }
        let keychain = backend.keychain(self.mask.as_ref())?;
        let mut candidate = response.clone();
        candidate.amount = context.amount;
        candidate.fee_fields = context.fee.context("payment fee missing")?;
        candidate.adjust_offset(&keychain, &context)?;
        candidate.add_participant_info(&keychain, &context, None)?;
        if candidate.participant_data.len() != 2 || candidate.participant_data[1].part_sig.is_some()
        {
            bail!("recipient response contains sender signature data");
        }
        let receiver = &candidate.participant_data[0];
        aggsig::verify_partial_sig(
            keychain.secp(),
            receiver
                .part_sig
                .as_ref()
                .context("recipient signature missing")?,
            &candidate.pub_nonce_sum(keychain.secp())?,
            &receiver.public_blind_excess,
            Some(&candidate.pub_blind_sum(keychain.secp())?),
            &candidate.msg_to_sign()?,
        )?;
        let mut parts = vec![];
        for (id, mmr_index, value) in context.get_inputs() {
            let input = backend
                .get(&id, &mmr_index)
                .ok()
                .or_else(|| backend.iter().ok()?.find(|out| out.key_id == id))
                .context("payment input missing")?;
            if input.is_coinbase {
                parts.push(build::coinbase_input(value, input.key_id));
            } else {
                parts.push(build::input(value, input.key_id));
            }
        }
        for (id, _, value) in context.get_outputs() {
            let output = backend
                .iter()?
                .find(|out| out.key_id == id)
                .context("payment change output missing")?;
            parts.push(build::output(value, output.key_id));
        }
        candidate.add_transaction_elements(&keychain, &ProofBuilder::new(&keychain), parts)?;
        let excess = candidate.calc_excess(keychain.secp())?;
        let mut transaction = candidate
            .tx
            .as_ref()
            .context("candidate transaction missing")?
            .clone();
        transaction.offset = candidate.offset.clone();
        let mut kernel = transaction.kernels()[0];
        kernel.excess = excess;
        transaction = transaction.replace_kernel(kernel);
        transaction.validate_read()?;
        let commits: Vec<_> = transaction
            .outputs()
            .iter()
            .map(|out| out.commitment())
            .collect();
        let proofs: Vec<_> = transaction.outputs().iter().map(|out| out.proof).collect();
        Output::batch_verify_proofs(&commits, &proofs)?;
        transaction.verify_kernel_sums(transaction.overage(), transaction.offset.clone())?;
        if transaction.fee() < tx_fee(transaction.inputs().len(), transaction.outputs().len(), 1) {
            bail!("payment fee is below native minimum");
        }
        Ok(())
    }
    pub fn finalize(
        &self,
        response: &str,
        cancelled: &std::sync::atomic::AtomicBool,
    ) -> Result<()> {
        if response.len() > 2_000_000 {
            bail!("response Slatepack is too large");
        }
        if !self.directory.join("intent.json").exists() {
            let packed = response.to_owned();
            if self
                .owner
                .decode_slatepack_message(self.mask.as_ref(), packed.clone(), vec![])?
                .mode
                != 0
            {
                bail!("only plaintext manual Slatepacks are supported");
            }
            let slate = self.owner.slate_from_slatepack_message(
                self.mask.as_ref(),
                packed.clone(),
                vec![],
            )?;
            let completed =
                self.load_path(&self.directory.join(format!("{}.posted.json", slate.id)))?;
            if completed.stage == "posted" && completed.response.as_ref() == Some(&packed) {
                return Ok(());
            }
            bail!("response does not match a completed payment");
        }
        let mut i = self.load()?;
        if let Some(saved) = i.response.as_ref() {
            if response != saved {
                bail!("supplied response differs from the saved payment response");
            }
        }
        if i.stage == "posted" {
            let packed = response.to_owned();
            if i.response.as_ref() != Some(&packed) {
                bail!("response does not match completed payment");
            }
            self.archive(&i)?;
            return Ok(());
        }
        self.owner
            .set_active_account(self.mask.as_ref(), &i.source)?;
        let result = (|| {
            let slate = if i.finalized.is_some() {
                let original = self.owner.slate_from_slatepack_message(
                    self.mask.as_ref(),
                    i.outgoing.clone(),
                    vec![],
                )?;
                self.check_stored_encoding(&i.id)?;
                self.owner
                    .get_stored_tx(self.mask.as_ref(), None, Some(&original.id))?
                    .context("finalized SDK transaction is missing")?
            } else {
                let packed = if let Some(ref saved) = i.response {
                    saved.clone()
                } else {
                    response.to_owned()
                };
                if self
                    .owner
                    .decode_slatepack_message(self.mask.as_ref(), packed.clone(), vec![])?
                    .mode
                    != 0
                {
                    bail!("only plaintext manual Slatepacks are supported");
                }
                let slate = self.owner.slate_from_slatepack_message(
                    self.mask.as_ref(),
                    packed.clone(),
                    vec![],
                )?;
                let original = self.owner.slate_from_slatepack_message(
                    self.mask.as_ref(),
                    i.outgoing.clone(),
                    vec![],
                )?;
                if slate.id.to_string() != i.id
                    || slate.state != SlateState::Standard2
                    || (slate.amount != 0 && slate.amount != i.amount)
                    || (slate.fee_fields.fee() != 0 && slate.fee_fields != original.fee_fields)
                    || slate.num_participants != original.num_participants
                    || slate.ttl_cutoff_height != original.ttl_cutoff_height
                {
                    bail!("response does not match pending payment");
                }
                if i.stage == "locking" {
                    let (_, txs) = self.owner.retrieve_txs(
                        self.mask.as_ref(),
                        false,
                        None,
                        Some(original.id),
                        None,
                    )?;
                    if txs.is_empty() {
                        self.owner.tx_lock_outputs(self.mask.as_ref(), &original)?;
                    }
                }
                if i.response.is_none()
                    && ["locking", "awaiting-response"].contains(&i.stage.as_str())
                {
                    self.preview_response(&slate, &original)?;
                }
                let stored = if i.stage == "finalizing" {
                    self.check_stored_encoding(&i.id)?;
                    self.owner
                        .get_stored_tx(self.mask.as_ref(), None, Some(&original.id))?
                } else {
                    None
                };
                let recovered = stored.filter(|s| {
                    s.tx.as_ref()
                        .map(|tx| {
                            tx.validate(grin_core::core::Weighting::AsTransaction)
                                .is_ok()
                        })
                        .unwrap_or(false)
                });
                if i.response.is_none() && cancelled.load(std::sync::atomic::Ordering::Acquire) {
                    bail!("withdrawal cancelled before signing");
                }
                i.response = Some(packed);
                i.stage = "finalizing".into();
                self.save(&i)?;
                let finalized = if let Some(s) = recovered {
                    s
                } else {
                    self.owner.finalize_tx(self.mask.as_ref(), &slate)?
                };
                i.finalized = Some(
                    finalized
                        .tx
                        .as_ref()
                        .context("finalized SDK transaction missing")?
                        .hash()
                        .to_hex(),
                );
                i.stage = "ready-to-post".into();
                self.save(&i)?;
                finalized
            };
            if slate.id.to_string() != i.id || slate.state != SlateState::Standard3 {
                bail!("invalid finalized payment");
            }
            let tx = slate
                .tx
                .as_ref()
                .context("finalized payment has no transaction")?;
            tx.validate(grin_core::core::Weighting::AsTransaction)?;
            if i.finalized.as_ref() != Some(&tx.hash().to_hex()) {
                bail!("stored finalized transaction does not match payment intent");
            }
            if tx.kernels().len() != 1 {
                bail!("payment must have exactly one kernel");
            }
            let already_on_chain = {
                let mut lock = self.wallet.lock();
                let backend = lock.lc_provider()?.wallet_inst()?;
                backend
                    .w2n_client()
                    .get_kernel(&tx.kernels()[0].excess, None, None)?
                    .is_some()
            };
            if !already_on_chain {
                self.owner.post_tx(self.mask.as_ref(), &slate, false)?;
            }
            i.stage = "posted".into();
            self.save(&i)?;
            self.archive(&i)?;
            Ok(())
        })();
        self.owner
            .set_active_account(self.mask.as_ref(), "g2pool-role-6")?;
        result
    }
    pub fn cancel(&self) -> Result<()> {
        if !self.directory.join("intent.json").exists() {
            return Ok(());
        }
        let i = self.load()?;
        if !["locking", "awaiting-response"].contains(&i.stage.as_str())
            || i.response.is_some()
            || i.finalized.is_some()
        {
            bail!("cannot cancel a payment after finalization began");
        }
        self.owner
            .set_active_account(self.mask.as_ref(), "g2pool-role-6")?;
        let original = self.owner.slate_from_slatepack_message(
            self.mask.as_ref(),
            i.outgoing.clone(),
            vec![],
        )?;
        let (_, txs) =
            self.owner
                .retrieve_txs(self.mask.as_ref(), false, None, Some(original.id), None)?;
        if !txs.is_empty() {
            self.check_stored_encoding(&i.id)?;
            if let Some(stored) =
                self.owner
                    .get_stored_tx(self.mask.as_ref(), None, Some(&original.id))?
            {
                if stored
                    .tx
                    .as_ref()
                    .map(|tx| {
                        tx.validate(grin_core::core::Weighting::AsTransaction)
                            .is_ok()
                    })
                    .unwrap_or(false)
                {
                    bail!("cannot cancel a signed payment");
                }
            }
        }
        if txs
            .iter()
            .any(|t| t.tx_type != grin_wallet_libwallet::TxLogEntryType::TxSentCancelled)
        {
            self.owner
                .cancel_tx(self.mask.as_ref(), None, Some(original.id))?;
        }
        {
            let mut lock = self.wallet.lock();
            let backend = lock.lc_provider()?.wallet_inst()?;
            let mut batch = backend.batch(self.mask.as_ref())?;
            batch.delete_private_context(original.id.as_bytes())?;
            batch.commit()?;
        }
        let archived = self.directory.join(format!("{}.cancelled.json", i.id));
        if archived.exists() {
            if private_read(&archived, 2_000_000)?
                != private_read(&self.directory.join("intent.json"), 2_000_000)?
            {
                bail!("cancelled archive mismatch");
            }
        } else {
            fs::hard_link(self.directory.join("intent.json"), &archived)?;
        }
        crate::secure_file::sync_directory(&self.directory)?;
        fs::remove_file(self.directory.join("intent.json"))?;
        crate::secure_file::sync_directory(&self.directory)?;
        Ok(())
    }
}
fn check_payment_weight(inputs: u64, change_outputs: u64) -> Result<()> {
    let outputs = change_outputs
        .checked_add(1)
        .context("payment output count overflow")?;
    if grin_core::core::Transaction::weight_by_iok(inputs, outputs, 1)
        > grin_core::global::max_block_weight() / 2
    {
        bail!("payment exceeds native transaction weight limit");
    }
    Ok(())
}

const MAX_NATIVE_FEE: u64 = (1u64 << 40) - 1;

fn check_amount(amount: u64) -> Result<()> {
    if amount == 0 {
        bail!("amount must be positive");
    }
    amount
        .checked_add(MAX_NATIVE_FEE)
        .context("amount leaves insufficient headroom for a native fee")?;
    Ok(())
}

pub fn parse_amount(s: &str) -> Result<u64> {
    let mut parts = s.split('.');
    let whole = parts.next().unwrap_or("");
    let fraction = parts.next().unwrap_or("");
    if whole.is_empty()
        || !whole.bytes().all(|b| b.is_ascii_digit())
        || !fraction.bytes().all(|b| b.is_ascii_digit())
        || fraction.len() > 9
        || parts.next().is_some()
    {
        bail!("amount must be a positive decimal with at most nine fractional digits");
    }
    let amount = whole
        .parse::<u64>()?
        .checked_mul(1_000_000_000)
        .and_then(|v| {
            v.checked_add(if fraction.is_empty() {
                0
            } else {
                fraction.parse::<u64>().unwrap() * 10u64.pow(9 - fraction.len() as u32)
            })
        })
        .context("amount overflow")?;
    check_amount(amount)?;
    Ok(amount)
}

#[cfg(test)]
#[path = "mod_tests.rs"]
mod tests;
