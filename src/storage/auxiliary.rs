use super::*;

const RECORDS: &[u8] = b"aux/wallet/record/";

fn key(prefix: &[u8], name: &str) -> Vec<u8> {
    let mut key = prefix.to_vec();
    key.extend(name.as_bytes());
    key
}
fn read(store: &Store, key: &[u8]) -> Result<Option<Vec<u8>>> {
    store.check_reader()?;
    store.database.auxiliary_read(key).map_err(runtime_error)
}
impl Store {
    pub fn wallet_registrations(&self) -> Result<Vec<Vec<u8>>> {
        self.check_reader()?;
        self.database
            .auxiliary_read_prefix(b"aux/wallet/record/registration:", 100_000, 512, 256 * 1024)
            .map_err(runtime_error)
            .map(|rows| {
                rows.into_iter()
                    .filter(|(k, _)| k.starts_with(b"aux/wallet/record/registration:"))
                    .map(|(_, v)| v)
                    .collect()
            })
    }
    pub fn bind_wallet(&mut self, account: Point) -> Result<()> {
        self.check_writer()?;
        self.database.auxiliary_transaction(|tx, db| {
            let account_key = key(RECORDS, "account");
            if let Some(previous) = db.get(tx, &account_key)? {
                if previous != account {
                    return Err(Error::Invalid("wallet seed does not match this store"));
                }
            } else {
                for (index, row) in db
                    .prefix_iter(tx, b"aux/wallet/record/registration:")?
                    .enumerate()
                {
                    let (key, bytes) = row?;
                    if index >= 100_000 || key.len() > 512 || bytes.len() > 256 * 1024 {
                        return Err(Error::Invalid("wallet registration bounds"));
                    }
                    let registration: Registration = decode(bytes)?;
                    if registration.fields.owner != account {
                        return Err(Error::Invalid("wallet seed does not match this store"));
                    }
                }
                db.put(tx, &account_key, &account)?;
            }
            Ok(())
        })
    }
    pub fn wallet_record(&self, name: &str) -> Result<Option<Vec<u8>>> {
        read(self, &key(RECORDS, name))
    }
    pub(crate) fn publish_registration(
        &mut self,
        objects: &[Object],
        registration: &crate::wallet::RegistrationPlan,
    ) -> Result<()> {
        let database = self.database.clone();
        let key = key(RECORDS, &registration.record);
        self.stage_with(objects, |tx| {
            database.auxiliary_compare_exchange(
                tx,
                &key,
                registration.previous.as_deref(),
                &registration.bytes,
            )
        })
    }
}
