pub mod cli;
pub mod payments;
use crate::{
    codec::*,
    native::{self, View},
    objects::{Key, Kind, Source},
    storage::{Object, Store},
    Error, Result,
};
use grin_core::{
    core::{KernelFeatures, Output, OutputFeatures, TxKernel},
    libtx::{aggsig, proof},
};
use grin_keychain::{ExtKeychain, ExtKeychainPath, Identifier, Keychain, SwitchCommitmentType};
use grin_util::{secp, static_secp_instance};
use std::{fs, path::Path};

pub struct Wallet {
    keychain: ExtKeychain,
    account_secret: secp::key::SecretKey,
    account: Point,
}
pub struct Reward {
    pub output: Output,
    pub kernel: TxKernel,
    pub path: Identifier,
}
fn path(role: u32, index: u32) -> Identifier {
    ExtKeychainPath::new(2, role, index, 0, 0).to_identifier()
}
fn reserve_path(store: &mut Store, role: u32, name: &str) -> Result<Identifier> {
    let index = store.reserve(name)?;
    let namespace = store.wallet_namespace()?;
    Ok(ExtKeychainPath::new(4, role, namespace[0], namespace[1], index).to_identifier())
}
pub fn sign(secret: &secp::key::SecretKey, message: Id) -> Result<Sig> {
    let handle = static_secp_instance();
    let secp = handle.lock();
    let public = secp::key::PublicKey::from_secret_key(&secp, secret)
        .map_err(|_| Error::Invalid("signing key"))?;
    let msg = secp::Message::from_slice(&message).map_err(|_| Error::Invalid("signing message"))?;
    let signature = aggsig::sign_single(&secp, &msg, secret, None, Some(&public))
        .map_err(|_| Error::Invalid("native signature"))?;
    native::native_bytes(&signature)?
        .try_into()
        .map_err(|_| Error::Invalid("signature width"))
}
pub fn kernel(secret: secp::key::SecretKey, features: KernelFeatures) -> Result<TxKernel> {
    let handle = static_secp_instance();
    let secp = handle.lock();
    let excess = secp
        .commit(0, secret.clone())
        .map_err(|_| Error::Invalid("kernel excess"))?;
    let public = excess
        .to_pubkey(&secp)
        .map_err(|_| Error::Invalid("kernel public"))?;
    let signature = aggsig::sign_single(
        &secp,
        &features
            .kernel_sig_msg()
            .map_err(|_| Error::Invalid("kernel message"))?,
        &secret,
        None,
        Some(&public),
    )
    .map_err(|_| Error::Invalid("kernel signing"))?;
    Ok(TxKernel {
        features,
        excess,
        excess_sig: signature,
    })
}
fn valid_entropy_length(length: usize) -> bool {
    matches!(length, 16 | 20 | 24 | 28 | 32)
}
impl Wallet {
    pub fn open(file: &Path) -> Result<Self> {
        let missing = match fs::symlink_metadata(file) {
            Ok(_) => false,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => true,
            Err(error) => return Err(error.into()),
        };
        if missing {
            let seed = zeroize::Zeroizing::new(rand::random::<[u8; 32]>());
            match crate::secure_file::publish_atomic(file, seed.as_ref(), false) {
                Ok(()) => {}
                Err(Error::Io(error)) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
                Err(error) => return Err(error),
            }
        }
        Self::open_existing(file)
    }
    pub fn open_existing(file: &Path) -> Result<Self> {
        let seed = zeroize::Zeroizing::new(
            crate::secure_file::read_regular(file, 32, true).map_err(|error| match error {
                Error::Io(error) if error.kind() == std::io::ErrorKind::NotFound => Error::Invalid(
                    "wallet seed missing; restore the original wallet.seed before recovery",
                ),
                error => error,
            })?,
        );
        if !valid_entropy_length(seed.len()) {
            return Err(Error::Invalid("wallet seed file"));
        }
        Self::from_seed(&seed)
    }
    pub fn from_seed(seed: &[u8]) -> Result<Self> {
        Self::from_keychain(
            ExtKeychain::from_seed(seed, false).map_err(|_| Error::Invalid("wallet seed"))?,
        )
    }
    fn from_keychain(keychain: ExtKeychain) -> Result<Self> {
        let account_secret = keychain
            .derive_key(0, &path(1, 0), SwitchCommitmentType::Regular)
            .map_err(|_| Error::Invalid("account derivation"))?;
        let account = secp::key::PublicKey::from_secret_key(keychain.secp(), &account_secret)
            .map_err(|_| Error::Invalid("account public"))?
            .serialize_vec(keychain.secp(), true)
            .as_slice()
            .try_into()
            .map_err(|_| Error::Invalid("account width"))?;
        Ok(Self {
            keychain,
            account_secret,
            account,
        })
    }
    pub fn account(&self) -> Point {
        self.account
    }
    pub fn authorize(&self, protocol: Id, work_id: Id) -> Result<WorkAuthorization> {
        let fields = WorkAuthMessage { protocol, work_id };
        let signature = sign(&self.account_secret, id(&fields)?)?;
        Ok(WorkAuthorization { fields, signature })
    }
    pub fn reward(&self, store: &mut Store, value: u64, finder: bool) -> Result<Reward> {
        let role = if finder { 3 } else { 2 };
        let path = reserve_path(store, role, if finder { "finder" } else { "coupon" })?;
        let switch = SwitchCommitmentType::Regular;
        let commitment = self
            .keychain
            .commit(value, &path, switch)
            .map_err(|_| Error::Invalid("reward commitment"))?;
        let builder = proof::ProofBuilder::new(&self.keychain);
        let proof = proof::create(
            &self.keychain,
            &builder,
            value,
            &path,
            switch,
            commitment,
            None,
        )
        .map_err(|_| Error::Invalid("rangeproof creation"))?;
        let secret = self
            .keychain
            .derive_key(value, &path, switch)
            .map_err(|_| Error::Invalid("reward derivation"))?;
        let reward = Reward {
            output: Output::new(OutputFeatures::Coinbase, commitment, proof),
            kernel: kernel(secret, KernelFeatures::Coinbase)?,
            path,
        };
        Ok(reward)
    }
    fn coupon(&self, store: &mut Store, protocol: Id, value: u64) -> Result<Id> {
        let reward = self.reward(store, value, false)?;
        let secret = self
            .keychain
            .derive_key(value, &reward.path, SwitchCommitmentType::Regular)
            .map_err(|_| Error::Invalid("coupon key"))?;
        let reward = crate::codec::Reward {
            output: Blob(native::native_bytes(&reward.output)?),
            kernel: Blob(native::native_bytes(&reward.kernel)?),
        };
        let fields = CouponAuth {
            protocol,
            owner: self.account,
            reward_id: id(&reward)?,
        };
        let coupon = Coupon {
            reward,
            owner_auth: sign(&secret, id(&fields)?)?,
        };
        let bytes = encode(&coupon)?;
        let cid = id(&coupon)?;
        store.stage(&[Object::new(Key::new(Kind::Coupon, cid), bytes)?])?;
        Ok(cid)
    }
    pub fn registration_for_transaction(
        &self,
        store: &mut Store,
        view: &View<'_>,
        transaction_inputs: &[secp::pedersen::Commitment],
    ) -> Result<Id> {
        let record_name = format!("registration:{}", crate::objects::hex(&self.account));
        let previous = store
            .wallet_record(&record_name)?
            .map(|b| decode::<Registration>(&b))
            .transpose()?;
        let mut revision = 0;
        let mut inventory = None;
        if let Some(previous) = &previous {
            if previous.fields.protocol == view.profile.protocol
                && previous.fields.owner == self.account
                && view.anchor(&previous.fields.birth).is_ok()
                && view.head.height
                    < previous
                        .fields
                        .birth
                        .height
                        .saturating_add(view.profile.manifest.lifetime)
            {
                let bytes = store.read(Key::new(Kind::Inventory, previous.fields.inventory))?;
                inventory = Some(decode::<Inventory>(&bytes)?);
                if previous.fields.birth == view.head {
                    revision = previous.fields.revision;
                }
            }
        }
        let mut changed = false;
        let mut inv = inventory.unwrap_or_else(|| Inventory {
            protocol: view.profile.protocol,
            owner: self.account,
            rows: List(vec![]),
        });
        let base = view.profile.manifest.unit_budget.ilog2() + 1;
        if inv.rows.0.is_empty() {
            for (family, count) in [(0, base), (1, 3)] {
                for row in 0..count {
                    let value = view.profile.face(family, row as u8)?;
                    let mut ids = Vec::new();
                    for _ in 0..crate::codec::COUPON_LEAVES_PER_ROW {
                        ids.push(self.coupon(store, view.profile.protocol, value)?);
                    }
                    inv.rows.0.push(InventoryRow {
                        family,
                        row: row as u8,
                        coupon_ids: List(ids),
                    });
                }
            }
            changed = true;
        } else {
            let same_head = previous
                .as_ref()
                .is_some_and(|registration| registration.fields.birth == view.head);
            if same_head && transaction_inputs.is_empty() {
                return id(&previous.ok_or(Error::View)?);
            }
            let mut commitments = Vec::new();
            for row in &inv.rows.0 {
                for cid in &row.coupon_ids.0 {
                    let coupon: Coupon = decode(&store.read(Key::new(Kind::Coupon, *cid))?)?;
                    commitments.push(native::reward(&coupon)?.0.commitment());
                }
            }
            if same_head
                && transaction_inputs
                    .iter()
                    .all(|input| !commitments.contains(input))
            {
                return id(&previous.ok_or(Error::View)?);
            }
            let presence = view.unspent_many(&view.head, &commitments)?;
            let mut presence = presence.into_iter();
            for row in &mut inv.rows.0 {
                for cid in &mut row.coupon_ids.0 {
                    if presence
                        .next()
                        .ok_or(Error::Invalid("wallet presence shape"))?
                    {
                        *cid = self.coupon(
                            store,
                            view.profile.protocol,
                            view.profile.face(row.family, row.row)?,
                        )?;
                        changed = true;
                    }
                }
            }
        }
        if changed
            && previous
                .as_ref()
                .is_some_and(|r| r.fields.birth == view.head)
        {
            revision = revision
                .checked_add(1)
                .ok_or(Error::Capacity("registration revision"))?;
        }
        view.check()?;
        let iid = id(&inv)?;
        let fields = RegistrationAuth {
            protocol: view.profile.protocol,
            owner: self.account,
            inventory: iid,
            birth: view.head.clone(),
            revision,
        };
        let registration = if let Some(previous) = previous.filter(|r| r.fields == fields) {
            previous
        } else {
            Registration {
                signature: sign(&self.account_secret, id(&fields)?)?,
                fields,
            }
        };
        let bytes = encode(&registration)?;
        let rid = id(&registration)?;
        store.stage(&[
            Object::new(Key::new(Kind::Inventory, iid), encode(&inv)?)?,
            Object::new(Key::new(Kind::Registration, rid), bytes.clone())?,
        ])?;
        store.save_wallet_record(&record_name, &bytes)?;
        Ok(rid)
    }
    pub(crate) fn rewind_owned_details(
        &self,
        output: &Output,
    ) -> Result<Option<(u64, Identifier)>> {
        let modern = proof::rewind(
            self.keychain.secp(),
            &proof::ProofBuilder::new(&self.keychain),
            output.commitment(),
            None,
            output.proof,
        )
        .map_err(|_| Error::Invalid("wallet output rewind"))?;
        let recovered = match modern {
            Some(owned) => Some(owned),
            None => proof::rewind(
                self.keychain.secp(),
                &proof::LegacyProofBuilder::new(&self.keychain),
                output.commitment(),
                None,
                output.proof,
            )
            .map_err(|_| Error::Invalid("wallet output rewind"))?,
        };
        match recovered {
            Some((value, path, switch)) => {
                let commitment = self
                    .keychain
                    .commit(value, &path, switch)
                    .map_err(|_| Error::Invalid("wallet output commitment"))?;
                if commitment != output.commitment() {
                    return Err(Error::Invalid("wallet output ownership"));
                }
                Ok(Some((value, path)))
            }
            None => Ok(None),
        }
    }
    pub fn aggregate(&self, outputs: &[Output]) -> Result<TxKernel> {
        if outputs.len() < 2 {
            return Err(Error::Invalid("aggregate arity"));
        }
        let builder = proof::ProofBuilder::new(&self.keychain);
        let mut secrets = Vec::new();
        for output in outputs {
            let (value, path, switch) = proof::rewind(
                self.keychain.secp(),
                &builder,
                output.commitment(),
                None,
                output.proof,
            )
            .map_err(|_| Error::Invalid("aggregate rewind"))?
            .ok_or(Error::Invalid("aggregate owner"))?;
            if self
                .keychain
                .commit(value, &path, switch)
                .map_err(|_| Error::Invalid("aggregate commitment"))?
                != output.commitment()
            {
                return Err(Error::Invalid("aggregate ownership"));
            }
            secrets.push(
                self.keychain
                    .derive_key(value, &path, switch)
                    .map_err(|_| Error::Invalid("aggregate key"))?,
            );
        }
        let summed = static_secp_instance()
            .lock()
            .blind_sum(secrets, vec![])
            .map_err(|_| Error::Invalid("aggregate zero"))?;
        kernel(summed, KernelFeatures::Coinbase)
    }
    pub fn recover(&self, view: &View<'_>) -> Result<Vec<Recovered>> {
        let mut cursor = 1;
        let mut found = Vec::new();
        let maximum = view.anchor(&view.head)?.output_mmr_size;
        loop {
            let (last, highest, outputs) = view
                .chain
                .unspent_outputs_by_pmmr_index(cursor, 256, Some(maximum))
                .map_err(|e| Error::NativeUnavailable(e.to_string()))?;
            for output in outputs {
                if let Some((value, path)) = self.rewind_owned_details(&output)? {
                    let header = view
                        .chain
                        .get_header_for_output(output.commitment())
                        .map_err(|e| Error::NativeUnavailable(e.to_string()))?;
                    found.push(Recovered {
                        commitment: crate::objects::hex(&output.commitment().0),
                        value,
                        height: header.height,
                        mature: !output.is_coinbase()
                            || view.head.height
                                >= header
                                    .height
                                    .checked_add(view.profile.manifest.lifetime)
                                    .ok_or(Error::Invalid("maturity overflow"))?,
                        path: crate::objects::hex(&path.to_bytes()),
                    });
                }
            }
            view.check()?;
            if last >= highest {
                break;
            }
            cursor = last
                .checked_add(1)
                .ok_or(Error::Invalid("recovery cursor"))?;
        }
        Ok(found)
    }
}
#[derive(Debug, serde::Serialize)]
pub struct Recovered {
    pub commitment: String,
    pub value: u64,
    pub height: u64,
    pub mature: bool,
    pub path: String,
}
