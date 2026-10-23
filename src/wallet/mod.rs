pub mod payments;

pub(crate) fn seed_path(data: &std::path::Path) -> std::path::PathBuf {
    data.join("payments/wallet_data/wallet.seed")
}
pub(crate) fn exists(data: &std::path::Path) -> Result<bool> {
    for directory in [data.join("payments"), data.join("payments/wallet_data")] {
        match std::fs::symlink_metadata(&directory) {
            Ok(metadata) if !metadata.is_dir() || metadata.file_type().is_symlink() => {
                return Err(Error::Invalid("wallet directory must be a real directory"))
            }
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
    }
    match std::fs::symlink_metadata(seed_path(data)) {
        Ok(metadata) if metadata.is_file() && !metadata.file_type().is_symlink() => Ok(true),
        Ok(_) => Err(Error::Invalid("Grin wallet seed must be a regular file")),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(error.into()),
    }
}

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
use grin_keychain::{ExtKeychain, Identifier, Keychain, SwitchCommitmentType};
use grin_util::{secp, static_secp_instance};

pub struct Signer {
    keychain: ExtKeychain,
    account_secret: secp::key::SecretKey,
    account: Point,
}
pub struct Reward {
    pub output: Output,
    pub kernel: TxKernel,
    pub path: Identifier,
}
pub(crate) struct RegistrationPlan {
    pub id: Id,
    pub previous: Option<Vec<u8>>,
    pub bytes: Vec<u8>,
    pub objects: Vec<Object>,
}
impl RegistrationPlan {
    fn existing(bytes: Vec<u8>) -> Result<Self> {
        Ok(Self {
            id: id(&decode::<Registration>(&bytes)?)?,
            previous: Some(bytes.clone()),
            bytes,
            objects: Vec::new(),
        })
    }
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
impl Signer {
    pub(crate) fn from_keychain(keychain: ExtKeychain) -> Result<Self> {
        let account_secret = keychain
            .derive_key(
                0,
                &ExtKeychain::derive_key_id(2, 1, 0, 0, 0),
                SwitchCommitmentType::Regular,
            )
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
    pub fn authorize(&self, work_id: Id) -> Result<WorkAuthorization> {
        let fields = WorkAuthMessage { work_id };
        let signature = sign(&self.account_secret, id(&fields)?)?;
        Ok(WorkAuthorization { fields, signature })
    }
    pub(crate) fn reward(&self, value: u64, path: Identifier) -> Result<Reward> {
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
    fn coupon<C: grin_wallet_libwallet::NodeClient + 'static>(
        &self,
        payments: &payments::Payments<C>,
        pool_id: Id,
        value: u64,
    ) -> Result<Object> {
        let reward = payments.reward(self, value)?;
        let secret = self
            .keychain
            .derive_key(value, &reward.path, SwitchCommitmentType::Regular)
            .map_err(|_| Error::Invalid("coupon key"))?;
        let reward = crate::codec::Reward {
            output: Blob(native::native_bytes(&reward.output)?),
            kernel: Blob(native::native_bytes(&reward.kernel)?),
        };
        let fields = CouponAuth {
            pool_id,
            owner: self.account,
            reward_id: id(&reward)?,
        };
        let coupon = Coupon {
            reward,
            owner_auth: sign(&secret, id(&fields)?)?,
        };
        let bytes = encode(&coupon)?;
        let cid = id(&coupon)?;
        Object::new(Key::new(Kind::Coupon, cid), bytes)
    }
    pub(crate) fn registration_for_transaction<C: grin_wallet_libwallet::NodeClient + 'static>(
        &self,
        store: &Store,
        view: &View<'_>,
        payments: &payments::Payments<C>,
        transaction_inputs: &[secp::pedersen::Commitment],
    ) -> Result<RegistrationPlan> {
        let previous_bytes = store.wallet_registration()?;
        let previous = previous_bytes
            .as_deref()
            .map(decode::<Registration>)
            .transpose()?;
        let mut objects = Vec::new();
        let mut coupon = |value| -> Result<Id> {
            let object = self.coupon(payments, view.profile.pool_id, value)?;
            let id = object.key.id;
            objects.push(object);
            Ok(id)
        };
        let mut revision = 0;
        let mut inventory = None;
        if let Some(previous) = &previous {
            if previous.fields.pool_id == view.profile.pool_id
                && previous.fields.owner == self.account
                && view.anchor(&previous.fields.birth).is_ok()
                && view.head.height
                    < previous
                        .fields
                        .birth
                        .height
                        .saturating_add(view.profile.lifetime)
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
            owner: self.account,
            rows: List(vec![]),
        });
        let base = view.profile.unit_budget.ilog2() + 1;
        if inv.rows.0.is_empty() {
            for (family, count) in [(0, base), (1, 3)] {
                for row in 0..count {
                    let value = view.profile.face(family, row as u8)?;
                    let mut ids = Vec::new();
                    for _ in 0..crate::codec::COUPON_LEAVES_PER_ROW {
                        ids.push(coupon(value)?);
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
                return RegistrationPlan::existing(previous_bytes.ok_or(Error::View)?);
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
                return RegistrationPlan::existing(previous_bytes.ok_or(Error::View)?);
            }
            let presence = view.unspent_many(&view.head, &commitments)?;
            let mut presence = presence.into_iter();
            for row in &mut inv.rows.0 {
                for cid in &mut row.coupon_ids.0 {
                    if presence
                        .next()
                        .ok_or(Error::Invalid("wallet presence shape"))?
                    {
                        *cid = coupon(view.profile.face(row.family, row.row)?)?;
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
            pool_id: view.profile.pool_id,
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
        objects.extend([
            Object::new(Key::new(Kind::Inventory, iid), encode(&inv)?)?,
            Object::new(Key::new(Kind::Registration, rid), bytes.clone())?,
        ]);
        Ok(RegistrationPlan {
            id: rid,
            previous: previous_bytes,
            bytes,
            objects,
        })
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
}
