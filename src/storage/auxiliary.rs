use super::*;

const ACCOUNT: &[u8] = b"aux/wallet/record/account";
const REGISTRATION: &[u8] = b"aux/wallet/record/registration";

impl Store {
    pub fn wallet_account(&self) -> Result<Option<Point>> {
        self.check_reader()?;
        self.database
            .auxiliary_read(ACCOUNT)
            .map_err(runtime_error)?
            .map(|bytes| {
                bytes
                    .try_into()
                    .map_err(|_| Error::Invalid("wallet account encoding"))
            })
            .transpose()
    }
    pub fn wallet_registration(&self) -> Result<Option<Vec<u8>>> {
        self.check_reader()?;
        self.database
            .auxiliary_read(REGISTRATION)
            .map_err(runtime_error)
    }
    pub fn bind_wallet(&mut self, account: Point) -> Result<()> {
        self.check_writer()?;
        self.database.auxiliary_transaction(|tx, db| {
            if let Some(previous) = db.get(tx, ACCOUNT)? {
                if previous != account {
                    return Err(Error::Invalid("wallet seed does not match this store"));
                }
            } else {
                db.put(tx, ACCOUNT, &account)?;
            }
            Ok(())
        })
    }
    pub(crate) fn publish_registration(
        &mut self,
        objects: &[Object],
        registration: &crate::wallet::RegistrationPlan,
    ) -> Result<()> {
        let database = self.database.clone();
        self.stage_with(objects, |tx| {
            database.auxiliary_compare_exchange(
                tx,
                REGISTRATION,
                registration.previous.as_deref(),
                &registration.bytes,
            )
        })
    }
}
