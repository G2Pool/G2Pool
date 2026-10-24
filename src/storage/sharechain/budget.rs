use super::{Error, Result};
use heed::{types::Bytes, Database, Env, RoTxn, WithoutTls};

pub(crate) const SOLUTION_RECORDS: usize = 1024;
pub(crate) const SOLUTION_OBJECTS: u64 = (SOLUTION_RECORDS * 6) as u64;
pub(crate) const SOLUTION_RESERVE_BYTES: u64 = 512 * 1024 * 1024;

#[derive(Clone)]
pub(crate) struct DatabaseBudget {
    env: Env<WithoutTls>,
    pub limit: u64,
}
impl DatabaseBudget {
    pub fn new(env: Env<WithoutTls>, limit: u64) -> Self {
        Self { env, limit }
    }
    pub fn used(&self, tx: &RoTxn<'_, WithoutTls>) -> Result<u64> {
        let main: Database<Bytes, Bytes> = self
            .env
            .open_database(tx, None)?
            .ok_or(Error::Invalid("database catalog missing"))?;
        let names = main
            .iter(tx)?
            .map(|row| {
                let (key, _) = row?;
                std::str::from_utf8(key)
                    .map(str::to_owned)
                    .map_err(|_| Error::Invalid("database name"))
            })
            .collect::<Result<Vec<_>>>()?;
        let mut databases = vec![main];
        for name in names {
            let database = self
                .env
                .open_database(tx, Some(&name))?
                .ok_or(Error::Invalid("database missing"))?;
            databases.push(database);
        }
        databases.into_iter().try_fold(0u64, |total, database| {
            let stat = database.stat(tx)?;
            let bytes =
                (stat.branch_pages as u64 + stat.leaf_pages as u64 + stat.overflow_pages as u64)
                    .checked_mul(stat.page_size as u64)
                    .ok_or(Error::Invalid("database size overflow"))?;
            total
                .checked_add(bytes)
                .ok_or(Error::Invalid("database size overflow"))
        })
    }
    pub fn check(&self, tx: &RoTxn<'_, WithoutTls>, previous: u64, reserve: u64) -> Result<()> {
        let used = self.used(tx)?;
        if used > self.limit.saturating_add(reserve) && used > previous {
            return Err(Error::Quota("pool database bytes"));
        }
        Ok(())
    }
}

#[cfg(test)]
#[path = "budget_tests.rs"]
mod tests;
