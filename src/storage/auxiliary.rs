use super::*;

const COUNTERS: &[u8] = b"aux/wallet/counter/";
const RECORDS: &[u8] = b"aux/wallet/record/";
const SUBMISSIONS: &[u8] = b"aux/native/submission/";

fn key(prefix: &[u8], name: &str) -> Vec<u8> {
    let mut key = prefix.to_vec();
    key.extend(name.as_bytes());
    key
}
fn storage(error: crate::storage::sharechain::Error) -> Error {
    match error {
        crate::storage::sharechain::Error::Io(error) => Error::Io(error),
        _ => Error::Invalid("auxiliary storage"),
    }
}
fn read(store: &Store, key: &[u8]) -> Result<Option<Vec<u8>>> {
    store.check_reader()?;
    store.archive.auxiliary_read(key).map_err(storage)
}
fn write(store: &Store, key: &[u8], value: &[u8]) -> Result<()> {
    store.check_writer()?;
    store.archive.auxiliary_write(key, value).map_err(storage)
}
impl Store {
    pub fn reserve(&mut self, name: &str) -> Result<u32> {
        self.check_writer()?;
        let key = key(COUNTERS, name);
        self.archive.auxiliary_transaction(|tx, db| {
            let previous = match db.get(tx, &key)? {
                Some(v) => {
                    u64::from_be_bytes(v.try_into().map_err(|_| Error::Invalid("wallet counter"))?)
                }
                None => 0,
            };
            let next = previous
                .checked_add(1)
                .ok_or(Error::Capacity("wallet key space"))?;
            if next >= u64::from(u32::MAX) {
                return Err(Error::Capacity("wallet key space"));
            }
            db.put(tx, &key, &next.to_be_bytes())?;
            Ok(next as u32)
        })
    }
    pub fn wallet_namespace(&mut self) -> Result<[u32; 2]> {
        self.check_writer()?;
        let key = key(RECORDS, "namespace");
        self.archive.auxiliary_transaction(|tx, db| {
            let bytes: [u8; 8] = match db.get(tx, &key)? {
                Some(bytes) => bytes
                    .try_into()
                    .map_err(|_| Error::Invalid("wallet namespace"))?,
                None => {
                    let bytes = rand::random::<[u8; 8]>();
                    db.put(tx, &key, &bytes)?;
                    bytes
                }
            };
            Ok([
                u32::from_be_bytes(bytes[..4].try_into().unwrap()),
                u32::from_be_bytes(bytes[4..].try_into().unwrap()),
            ])
        })
    }
    pub fn wallet_registrations(&self) -> Result<Vec<Vec<u8>>> {
        self.check_reader()?;
        self.archive
            .auxiliary_read_prefix(b"aux/wallet/record/registration:", 100_000, 512, 256 * 1024)
            .map_err(storage)
            .map(|rows| {
                rows.into_iter()
                    .filter(|(k, _)| k.starts_with(b"aux/wallet/record/registration:"))
                    .map(|(_, v)| v)
                    .collect()
            })
    }
    pub fn bind_wallet(&mut self, account: Point) -> Result<()> {
        self.check_writer()?;
        self.archive.auxiliary_transaction(|tx, db| {
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
    pub fn save_wallet_record(&mut self, name: &str, bytes: &[u8]) -> Result<()> {
        if bytes.len() > 256 * 1024 {
            return Err(Error::Capacity("wallet record"));
        }
        write(self, &key(RECORDS, name), bytes)
    }
    pub fn submission_intent(&mut self, id: Id, block: &[u8]) -> Result<()> {
        self.check_writer()?;
        if block.len() > 8 * 1024 * 1024 {
            return Err(Error::Capacity("native submission"));
        }
        let intent_key = key(SUBMISSIONS, &crate::objects::hex(&id));
        self.archive.auxiliary_transaction(|tx, db| {
            if db.get(tx, &intent_key)?.is_some() {
                return Ok(());
            }
            let count = db
                .prefix_iter(tx, SUBMISSIONS)?
                .take(64)
                .try_fold(0, |n, row| row.map(|_| n + 1))?;
            if count >= 64 {
                return Err(Error::Capacity("native submissions"));
            }
            db.put(tx, &intent_key, block)?;
            Ok(())
        })
    }
    pub fn submission_done(&mut self, id: Id) -> Result<()> {
        self.check_writer()?;
        let text = crate::objects::hex(&id);
        self.archive
            .auxiliary_delete(&key(SUBMISSIONS, &text))
            .map_err(storage)?;
        Ok(())
    }
    pub fn submissions(&self) -> Result<Vec<(Id, Vec<u8>)>> {
        self.check_reader()?;
        self.archive
            .auxiliary_read_prefix(SUBMISSIONS, 64, 512, 8 * 1024 * 1024)
            .map_err(storage)?
            .into_iter()
            .map(|(key, bytes)| {
                let suffix = key
                    .strip_prefix(SUBMISSIONS)
                    .ok_or(Error::Invalid("submission key"))?;
                if suffix.len() != 64 || !suffix.iter().all(u8::is_ascii_hexdigit) {
                    return Err(Error::Invalid("submission key"));
                }
                let text =
                    std::str::from_utf8(suffix).map_err(|_| Error::Invalid("submission key"))?;
                let mut id = [0u8; 32];
                for i in 0..32 {
                    id[i] = u8::from_str_radix(&text[i * 2..i * 2 + 2], 16)
                        .map_err(|_| Error::Invalid("submission key"))?;
                }
                Ok((id, bytes))
            })
            .collect()
    }
}
