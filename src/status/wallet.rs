use crate::{objects::hex, Error, Result};
use serde::{Deserialize, Serialize};
use std::path::Path;

pub(super) const MAX_SNAPSHOT_BYTES: u64 = 64 * 1024;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Snapshot {
    pub account: String,
    pub pool_id: String,
    pub native_height: u64,
    pub native_head: String,
    pub updated_at: i64,
    pub state: String,
    pub scanned_pmmr_position: u64,
    pub balance: Option<grin_wallet_libwallet::WalletInfo>,
    pub output_count: usize,
    pub error: Option<String>,
}

pub fn snapshot(
    account: &[u8; 33],
    pool_id: &[u8; 32],
    head: Option<(u64, [u8; 32])>,
    scanned_pmmr_position: u64,
    state: &str,
    balance: Option<(grin_wallet_libwallet::WalletInfo, usize)>,
    error: Option<String>,
) -> Snapshot {
    let (native_height, native_head) = head
        .map(|(height, hash)| (height, hex(&hash)))
        .unwrap_or_default();
    let (balance, output_count) =
        balance.map_or((None, 0), |(balance, count)| (Some(balance), count));
    Snapshot {
        account: hex(account),
        pool_id: hex(pool_id),
        native_height,
        native_head,
        updated_at: chrono::Utc::now().timestamp(),
        state: state.into(),
        scanned_pmmr_position,
        balance,
        output_count,
        error,
    }
}

pub(crate) fn publish_json(path: &Path, value: &impl Serialize) -> Result<()> {
    let bytes = serde_json::to_vec_pretty(value)?;
    if bytes.len() as u64 > MAX_SNAPSHOT_BYTES {
        return Err(Error::Invalid("wallet snapshot size"));
    }
    crate::secure_file::publish_atomic(path, &bytes, true)
}
