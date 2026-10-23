use crate::status::wallet::Snapshot;
use anyhow::{anyhow, bail, Context, Result};
use grin_core::core::hash::Hashed;
use grin_keychain::ExtKeychain;
use grin_util::ToHex;
use grin_util::{secp::key::SecretKey, Mutex, ZeroingString};
use grin_wallet_api::Owner;
use grin_wallet_impls::{DefaultLCProvider, DefaultWalletImpl};
use grin_wallet_libwallet::{InitTxArgs, NodeClient, SlateState, WalletInst};
use serde::{Deserialize, Serialize};
use std::{
    fs::{self, OpenOptions},
    path::{Path, PathBuf},
    sync::Arc,
};

pub mod cli;
mod direct;
mod worker;
pub use direct::DirectNodeClient;
pub use worker::{PaymentWorker, WorkerAction};

pub(crate) const MAX_SLATEPACK_BYTES: usize = 2_000_000;
const MAX_INTENT_BYTES: u64 = (2 * 6 * MAX_SLATEPACK_BYTES + 1024) as u64;
const MAX_STORED_TX_BYTES: u64 = 2_000_000;

pub type Wallet<C> =
    Arc<Mutex<Box<dyn WalletInst<'static, DefaultLCProvider<C, ExtKeychain>, C, ExtKeychain>>>>;
pub type DaemonPayments = Payments<crate::wallet::payments::DirectNodeClient>;
pub struct Payments<C: NodeClient + 'static> {
    pub wallet: Wallet<C>,
    pub owner: Owner<DefaultLCProvider<C, ExtKeychain>, C, ExtKeychain>,
    pub mask: Option<SecretKey>,
    directory: PathBuf,
    account: [u8; 33],
}
#[derive(Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
enum Phase {
    Locking,
    AwaitingResponse,
    Finalizing,
    ReadyToPost,
}
#[derive(Serialize, Deserialize)]
struct Intent {
    id: String,
    amount: u64,
    stage: Phase,
    outgoing: String,
    finalized: Option<String>,
    response: Option<String>,
}
impl Intent {
    fn check_size(&self) -> Result<()> {
        if self.outgoing.len() > MAX_SLATEPACK_BYTES
            || self
                .response
                .as_ref()
                .is_some_and(|response| response.len() > MAX_SLATEPACK_BYTES)
        {
            bail!("payment Slatepack is too large");
        }
        Ok(())
    }
}
fn private_read(path: &Path, max: u64) -> Result<Vec<u8>> {
    crate::secure_file::read_regular(path, max, true).map_err(|e| anyhow!(e.to_string()))
}
fn write_private(path: &Path, bytes: &[u8], replace: bool) -> Result<()> {
    crate::secure_file::publish_atomic(path, bytes, replace).map_err(|e| anyhow!(e.to_string()))
}
impl<C: NodeClient + 'static> Payments<C> {
    pub fn create(data: &Path, client: C) -> Result<Self> {
        Self::initialize(data, client, true)
    }
    pub fn open(data: &Path, client: C) -> Result<Self> {
        Self::initialize(data, client, false)
    }
    fn initialize(data: &Path, client: C, create: bool) -> Result<Self> {
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
        let mut wallet = Box::new(DefaultWalletImpl::<C>::new(client)?)
            as Box<dyn WalletInst<'static, DefaultLCProvider<C, ExtKeychain>, C, ExtKeychain>>;
        let lc = wallet.lc_provider()?;
        lc.set_top_level_directory(directory.to_str().context("non-UTF8 data directory")?)?;
        if create {
            if lc.wallet_exists(None)? {
                bail!("Grin wallet already exists");
            }
            if fs::read_dir(&directory)?.next().is_some() {
                bail!("refusing to create a new seed in an existing wallet directory");
            }
            lc.create_wallet(None, None, 32, ZeroingString::from(""), false)?;
            let seed = super::seed_path(data);
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                fs::set_permissions(&seed, fs::Permissions::from_mode(0o600))?;
            }
            OpenOptions::new().write(true).open(&seed)?.sync_all()?;
            crate::secure_file::sync_directory(seed.parent().context("wallet seed parent")?)?;
        } else if !lc.wallet_exists(None)? {
            bail!("Grin wallet seed missing; refusing to generate a replacement");
        }
        let mask = lc.open_wallet(None, ZeroingString::from(""), true, false)?;
        let signer = super::Signer::from_keychain(lc.wallet_inst()?.keychain(mask.as_ref())?)?;
        let account = signer.account();
        let wallet = Arc::new(Mutex::new(wallet));
        let (status_send, _status_receive) = std::sync::mpsc::channel();
        let owner = Owner::new(
            wallet.clone(),
            Some(status_send),
            directory.join("grin-wallet.toml"),
        );
        Ok(Self {
            wallet,
            owner,
            mask,
            directory,
            account,
        })
    }
    pub fn signer(&self) -> Result<super::Signer> {
        let mut lock = self.wallet.lock();
        let backend = lock.lc_provider()?.wallet_inst()?;
        Ok(super::Signer::from_keychain(
            backend.keychain(self.mask.as_ref())?,
        )?)
    }
    fn require_allocation_ready(
        backend: &mut grin_wallet_libwallet::WalletBackend<C, ExtKeychain>,
    ) -> Result<()> {
        use grin_wallet_libwallet::WalletInitStatus;
        match backend.init_status()? {
            WalletInitStatus::InitNeedsScanning => {
                bail!("Grin wallet initialization scan is incomplete")
            }
            WalletInitStatus::InitComplete if backend.last_scanned_block()?.hash.is_empty() => {
                bail!("Grin wallet initialization scan is incomplete")
            }
            _ => {}
        }
        let parent = backend.parent_key_id();
        if backend.current_child_index(&parent)? == u32::MAX {
            bail!("wallet derivation capacity exhausted");
        }
        Ok(())
    }
    pub(super) fn refresh(&self) -> Result<()> {
        let (updated, _) = self
            .owner
            .retrieve_summary_info(self.mask.as_ref(), true, 1)?;
        if !updated {
            bail!("wallet refresh could not validate the native chain");
        }
        Ok(())
    }
    pub(super) fn rebroadcast(&self) -> Result<()> {
        let (_, pending) = self.owner.retrieve_txs(
            self.mask.as_ref(),
            false,
            None,
            None,
            Some(grin_wallet_libwallet::RetrieveTxQueryArgs {
                include_outstanding_only: Some(true),
                exclude_cancelled: Some(true),
                ..Default::default()
            }),
        )?;
        let mut first_error = None;
        for entry in pending {
            if !matches!(
                entry.tx_type,
                grin_wallet_libwallet::TxLogEntryType::TxSent
                    | grin_wallet_libwallet::TxLogEntryType::TxReverted
            ) {
                continue;
            }
            if let Err(error) = self.rebroadcast_entry(entry) {
                first_error.get_or_insert(error);
            }
        }
        match first_error {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }
    fn rebroadcast_entry(&self, mut entry: grin_wallet_libwallet::TxLogEntry) -> Result<()> {
        let id = entry
            .tx_slate_id
            .context("signed Grin transaction has no slate ID")?;
        let Some(slate) = self.stored_transaction(&id.to_string())? else {
            if entry.tx_slate_state == Some(SlateState::Standard3) {
                bail!("signed Grin transaction is missing");
            }
            return Ok(());
        };
        if is_signed(&slate) {
            let excess = slate.tx_or_err()?.kernels()[0].excess;
            if entry.kernel_excess != Some(excess)
                || entry.tx_slate_state != Some(SlateState::Standard3)
            {
                entry.kernel_excess = Some(excess);
                entry.tx_slate_state = Some(SlateState::Standard3);
                let parent = entry.parent_key_id.clone();
                let mut wallet = self.wallet.lock();
                let backend = wallet.lc_provider()?.wallet_inst()?;
                let mut batch = backend.batch(self.mask.as_ref())?;
                batch.save_tx_log_entry(entry, &parent)?;
                batch.delete_private_context(id.as_bytes())?;
                batch.commit()?;
            }
            self.owner.post_tx(self.mask.as_ref(), &slate, false)?;
        }
        Ok(())
    }
    pub fn reward(&self, signer: &super::Signer, value: u64) -> crate::Result<super::Reward> {
        let path = (|| -> Result<_> {
            let mut lock = self.wallet.lock();
            let backend = lock.lc_provider()?.wallet_inst()?;
            Self::require_allocation_ready(backend)?;
            Ok(backend.next_child(self.mask.as_ref())?)
        })()
        .map_err(|error| crate::Error::NativeUnavailable(format!("reward key: {error}")))?;
        signer.reward(value, path)
    }
    pub(super) fn status_snapshot(
        &self,
        profile: [u8; 32],
        state: &str,
        error: Option<String>,
    ) -> Result<Snapshot> {
        let scanned = {
            let mut lock = self.wallet.lock();
            lock.lc_provider()?.wallet_inst()?.last_scanned_block()?
        };
        let head = if scanned.hash.is_empty() {
            None
        } else {
            let hash = grin_core::core::hash::Hash::from_hex(&scanned.hash)?;
            Some((scanned.height, crate::native::hash_bytes(hash)))
        };
        if state != "ready" {
            return Ok(crate::status::wallet::snapshot(
                &self.account,
                &profile,
                head,
                scanned.last_pmmr_index,
                state,
                None,
                error,
            ));
        }
        let (_, info) = self
            .owner
            .retrieve_summary_info(self.mask.as_ref(), false, 1)?;
        let (_, outputs) = self
            .owner
            .retrieve_outputs(self.mask.as_ref(), false, false, None)?;
        let output_count = outputs.len();
        Ok(crate::status::wallet::snapshot(
            &self.account,
            &profile,
            head,
            scanned.last_pmmr_index,
            state,
            Some((info, output_count)),
            error,
        ))
    }
    fn stored_transaction(&self, id: &str) -> Result<Option<grin_wallet_libwallet::Slate>> {
        let id = id.parse()?;
        let path = self
            .directory
            .join("wallet_data/saved_txs")
            .join(format!("{id}.grintx"));
        let bytes = match crate::secure_file::read_regular(&path, MAX_STORED_TX_BYTES, false) {
            Ok(bytes) => bytes,
            Err(crate::Error::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => {
                return Ok(None)
            }
            Err(error) => return Err(error.into()),
        };
        let hex = String::from_utf8(bytes)?;
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
        Ok(self
            .owner
            .get_stored_tx(self.mask.as_ref(), None, Some(&id))?)
    }
    fn require_signing_context(&self, slate: &grin_wallet_libwallet::Slate) -> Result<()> {
        let mut wallet = self.wallet.lock();
        wallet
            .lc_provider()?
            .wallet_inst()?
            .get_private_context(self.mask.as_ref(), slate.id.as_bytes())
            .context("SDK signing context unavailable; withdrawal preserved")?;
        Ok(())
    }
    fn save(&self, i: &Intent) -> Result<()> {
        i.check_size()?;
        write_private(
            &self.directory.join("intent.json"),
            &serde_json::to_vec(i)?,
            true,
        )
    }
    fn load(&self) -> Result<Intent> {
        let i: Intent = serde_json::from_slice(&private_read(
            &self.directory.join("intent.json"),
            MAX_INTENT_BYTES,
        )?)?;
        i.check_size()?;
        let original = self.owner.slate_from_slatepack_message(
            self.mask.as_ref(),
            i.outgoing.clone(),
            vec![],
        )?;
        if original.id.to_string() != i.id
            || original.amount != i.amount
            || original.state != SlateState::Standard1
        {
            bail!("invalid payment intent");
        }
        Ok(i)
    }
    pub(super) fn resume(
        &self,
        cancelled: &std::sync::atomic::AtomicBool,
    ) -> Result<serde_json::Value> {
        if !self.directory.join("intent.json").exists() {
            return Ok(serde_json::json!({"state": "idle"}));
        }
        let intent = self.load()?;
        if matches!(intent.stage, Phase::Locking | Phase::AwaitingResponse) {
            let slatepack = self.offer(intent.amount)?;
            return Ok(
                serde_json::json!({"state": "awaiting_response", "amount": intent.amount, "slatepack": slatepack}),
            );
        }
        let response = intent
            .response
            .context("pending signed payment has no saved response")?;
        self.finalize(&response, cancelled)?;
        Ok(serde_json::json!({"state": "completed"}))
    }
    pub(super) fn offer(&self, amount: u64) -> Result<String> {
        check_amount(amount)?;
        if self.directory.join("intent.json").exists() {
            let mut previous = self.load()?;
            if previous.amount == amount
                && matches!(previous.stage, Phase::Locking | Phase::AwaitingResponse)
            {
                if previous.stage == Phase::Locking {
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
                    previous.stage = Phase::AwaitingResponse;
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
        let args = InitTxArgs {
            amount,
            minimum_confirmations: 1,
            max_outputs: 500,
            num_change_outputs: 1,
            selection_strategy_is_use_all: false,
            refresh_outputs_from_node: false,
            ..Default::default()
        };
        let slate = {
            let mut lock = self.wallet.lock();
            let backend = lock.lc_provider()?.wallet_inst()?;
            Self::require_allocation_ready(backend)?;
            let slate = grin_wallet_libwallet::api_impl::owner::init_send_tx(
                backend,
                self.mask.as_ref(),
                args,
                false,
            )?;
            let context = backend.get_private_context(self.mask.as_ref(), slate.id.as_bytes())?;
            check_payment_weight(
                context.input_ids.len() as u64,
                context.output_ids.len() as u64,
            )?;
            slate
        };
        let outgoing =
            self.owner
                .create_slatepack_message(self.mask.as_ref(), &slate, None, vec![])?;
        let mut i = Intent {
            id: slate.id.to_string(),
            amount,
            stage: Phase::Locking,
            outgoing,
            finalized: None,
            response: None,
        };
        self.save(&i)?;
        self.owner.tx_lock_outputs(self.mask.as_ref(), &slate)?;
        i.stage = Phase::AwaitingResponse;
        self.save(&i)?;
        Ok(i.outgoing)
    }
    fn clear_intent(&self) -> Result<()> {
        fs::remove_file(self.directory.join("intent.json"))?;
        crate::secure_file::sync_directory(&self.directory)?;
        Ok(())
    }
    fn check_response(
        response: &grin_wallet_libwallet::Slate,
        original: &grin_wallet_libwallet::Slate,
    ) -> Result<()> {
        use grin_core::core::{KernelFeatures, OutputFeatures};
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

        Ok(())
    }
    pub fn finalize(
        &self,
        response: &str,
        cancelled: &std::sync::atomic::AtomicBool,
    ) -> Result<()> {
        if response.len() > MAX_SLATEPACK_BYTES {
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
            let (_, txs) =
                self.owner
                    .retrieve_txs(self.mask.as_ref(), false, None, Some(slate.id), None)?;
            if slate.state == SlateState::Standard2
                && txs.iter().any(|tx| {
                    matches!(
                        tx.tx_type,
                        grin_wallet_libwallet::TxLogEntryType::TxSent
                            | grin_wallet_libwallet::TxLogEntryType::TxReverted
                    )
                })
                && self
                    .stored_transaction(&slate.id.to_string())?
                    .as_ref()
                    .is_some_and(is_signed)
            {
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
        let slate = if i.finalized.is_some() {
            self.stored_transaction(&i.id)?
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
            if i.stage == Phase::Locking {
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
            if i.response.is_none() && matches!(i.stage, Phase::Locking | Phase::AwaitingResponse) {
                Self::check_response(&slate, &original)?;
            }
            let recovered = if i.stage == Phase::Finalizing {
                match self.stored_transaction(&i.id) {
                    Ok(Some(stored)) if is_signed(&stored) => Some(stored),
                    Err(error) if error.downcast_ref::<crate::Error>().is_some() => {
                        return Err(error)
                    }
                    _ => {
                        self.require_signing_context(&slate)?;
                        None
                    }
                }
            } else {
                None
            };
            if i.response.is_none() && cancelled.load(std::sync::atomic::Ordering::Acquire) {
                bail!("withdrawal cancelled before signing");
            }
            i.response = Some(packed);
            i.stage = Phase::Finalizing;
            self.save(&i)?;
            let finalized = if let Some(s) = recovered {
                s
            } else {
                match self.owner.finalize_tx(self.mask.as_ref(), &slate) {
                    Ok(signed) => signed,
                    Err(error) => match self.cancel() {
                        Ok(_) => bail!("Grin rejected the reply; unsigned withdrawal cancelled: {error}"),
                        Err(cancel_error) => bail!("finalization failed; withdrawal preserved for retry: {error}; {cancel_error}"),
                    },
                }
            };
            i.finalized = Some(
                finalized
                    .tx
                    .as_ref()
                    .context("finalized SDK transaction missing")?
                    .hash()
                    .to_hex(),
            );
            i.stage = Phase::ReadyToPost;
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
        self.clear_intent()?;
        Ok(())
    }
    pub fn cancel(&self) -> Result<bool> {
        if !self.directory.join("intent.json").exists() {
            return Ok(false);
        }
        let i = self.load()?;
        if i.stage == Phase::ReadyToPost || i.finalized.is_some() {
            bail!("cannot cancel a payment after finalization began");
        }
        let original = self.owner.slate_from_slatepack_message(
            self.mask.as_ref(),
            i.outgoing.clone(),
            vec![],
        )?;
        let (_, txs) =
            self.owner
                .retrieve_txs(self.mask.as_ref(), false, None, Some(original.id), None)?;
        if i.response.is_some()
            && (txs.is_empty()
                || !txs
                    .iter()
                    .all(|tx| tx.tx_type == grin_wallet_libwallet::TxLogEntryType::TxSentCancelled))
        {
            match self.stored_transaction(&i.id) {
                Ok(Some(stored)) if is_signed(&stored) => bail!("cannot cancel a signed payment"),
                Ok(Some(_)) => {}
                Err(error) if error.downcast_ref::<crate::Error>().is_some() => return Err(error),
                _ => self.require_signing_context(&original)?,
            }
        }
        if txs
            .iter()
            .any(|tx| tx.tx_type != grin_wallet_libwallet::TxLogEntryType::TxSentCancelled)
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
        self.clear_intent()?;
        Ok(true)
    }
}
fn is_signed(slate: &grin_wallet_libwallet::Slate) -> bool {
    slate.tx.as_ref().is_some_and(|tx| {
        tx.validate(grin_core::core::Weighting::AsTransaction)
            .is_ok()
    })
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
pub(crate) mod tests;
