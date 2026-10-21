use super::*;

impl SharechainDb {
    const PEER_PREFIX: &'static [u8] = b"peers/";
    const PEER_LIMIT: usize = 448;
    const PEER_KEY_BYTES: usize = 70;
    const PEER_VALUE_BYTES: usize = 1024;

    pub fn read_peers(&self) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        self.auxiliary_read_prefix(
            Self::PEER_PREFIX,
            Self::PEER_LIMIT + 1,
            Self::PEER_KEY_BYTES,
            Self::PEER_VALUE_BYTES,
        )
    }

    pub fn replace_peers(&self, records: &[(Vec<u8>, Vec<u8>)]) -> Result<()> {
        if records.iter().any(|(key, value)| {
            key.len() > Self::PEER_KEY_BYTES || value.len() > Self::PEER_VALUE_BYTES
        }) {
            return Err(Error::Invalid("peer record bounds"));
        }
        if records.len() > Self::PEER_LIMIT
            || records
                .iter()
                .any(|(key, _)| !key.starts_with(Self::PEER_PREFIX))
        {
            return Err(Error::Invalid("runtime namespace bounds"));
        }
        let desired: std::collections::BTreeMap<_, _> = records
            .iter()
            .map(|(key, value)| (key.as_slice(), value.as_slice()))
            .collect();
        if desired.len() != records.len() {
            return Err(Error::Invalid("duplicate runtime namespace key"));
        }
        let mut txn = self.env.write_txn()?;
        let previous_bytes = self.budget.used(&txn)?;
        let mut removed = Vec::new();
        let mut changed = std::collections::BTreeSet::new();
        for (index, entry) in self
            .runtime
            .prefix_iter(&txn, Self::PEER_PREFIX)?
            .enumerate()
        {
            if index >= Self::PEER_LIMIT {
                return Err(Error::Invalid("runtime namespace bounds"));
            }
            let (key, value) = entry?;
            match desired.get(key) {
                None => removed.push(key.to_vec()),
                Some(wanted) if *wanted != value => {
                    changed.insert(key.to_vec());
                }
                Some(_) => {}
            }
        }
        for key in removed {
            self.runtime.delete(&mut txn, &key)?;
        }
        for (key, value) in records {
            if changed.contains(key) || self.runtime.get(&txn, key)?.is_none() {
                self.runtime.put(&mut txn, key, value)?;
            }
        }
        self.budget.check(&txn, previous_bytes, 0)?;
        txn.commit()?;
        Ok(())
    }

    pub(crate) fn auxiliary_read_prefix(
        &self,
        prefix: &[u8],
        limit: usize,
        max_key_bytes: usize,
        max_value_bytes: usize,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let txn = self.env.read_txn()?;
        let mut records = Vec::new();
        for entry in self.runtime.prefix_iter(&txn, prefix)?.take(limit) {
            let (key, value) = entry?;
            if key.len() > max_key_bytes || value.len() > max_value_bytes {
                return Err(Error::Invalid("runtime namespace record bounds"));
            }
            records.push((key.to_vec(), value.to_vec()));
        }
        Ok(records)
    }

    pub(crate) fn auxiliary_keys(
        &self,
        prefix: &[u8],
        limit: usize,
        max_value: usize,
    ) -> Result<(Vec<Vec<u8>>, u64)> {
        let tx = self.env.read_txn()?;
        let mut keys = Vec::new();
        let mut bytes = 0u64;
        for row in self.runtime.prefix_iter(&tx, prefix)? {
            let (key, value) = row?;
            if keys.len() >= limit || value.len() > max_value {
                return Err(Error::Quota("auxiliary records"));
            }
            keys.push(key.to_vec());
            bytes = bytes
                .checked_add(value.len() as u64)
                .ok_or(Error::Invalid("auxiliary bytes overflow"))?;
        }
        Ok((keys, bytes))
    }
    pub(crate) fn auxiliary_read(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let txn = self.env.read_txn()?;
        Ok(self.runtime.get(&txn, key)?.map(ToOwned::to_owned))
    }

    pub(crate) fn auxiliary_compare_exchange(
        &self,
        tx: &mut heed::RwTxn<'_>,
        key: &[u8],
        previous: Option<&[u8]>,
        value: &[u8],
    ) -> Result<()> {
        if self.runtime.get(tx, key)? != previous {
            return Err(Error::Stale);
        }
        self.runtime.put(tx, key, value)?;
        Ok(())
    }
    #[cfg(test)]
    pub(crate) fn auxiliary_write(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.auxiliary_transaction(|tx, db| {
            db.put(tx, key, value)?;
            Ok(())
        })
    }
    #[cfg(test)]
    pub(crate) fn auxiliary_delete(&self, key: &[u8]) -> Result<()> {
        self.auxiliary_transaction(|tx, db| {
            db.delete(tx, key)?;
            Ok(())
        })
    }
    pub(crate) fn solution_transaction<T, E>(
        &self,
        operation: impl FnOnce(
            &mut heed::RwTxn<'_>,
            Database<Bytes, Bytes>,
        ) -> std::result::Result<T, E>,
    ) -> std::result::Result<T, E>
    where
        E: From<heed::Error> + From<Error>,
    {
        self.transaction(super::WRITE_RESERVE, operation)
    }
    pub(crate) fn auxiliary_transaction<T, E>(
        &self,
        operation: impl FnOnce(
            &mut heed::RwTxn<'_>,
            Database<Bytes, Bytes>,
        ) -> std::result::Result<T, E>,
    ) -> std::result::Result<T, E>
    where
        E: From<heed::Error> + From<Error>,
    {
        self.transaction(0, operation)
    }
    fn transaction<T, E>(
        &self,
        reserve: u64,
        operation: impl FnOnce(
            &mut heed::RwTxn<'_>,
            Database<Bytes, Bytes>,
        ) -> std::result::Result<T, E>,
    ) -> std::result::Result<T, E>
    where
        E: From<heed::Error> + From<Error>,
    {
        let mut tx = self.env.write_txn()?;
        let previous_bytes = self.budget.used(&tx)?;
        let result = operation(&mut tx, self.runtime)?;
        self.budget.check(&tx, previous_bytes, reserve)?;
        tx.commit()?;
        Ok(result)
    }
}
