use crate::{
    codec::Anchor,
    native::{Profile, View},
    objects::hex,
    wallet::Wallet,
    Error, Result,
};
use grin_chain::Chain;
use grin_util::secp::pedersen::Commitment;
use serde::{Deserialize, Serialize};
use std::{
    path::Path,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    thread,
    time::Duration,
};

const BATCH: u64 = 1_000;
pub const MAX_OWNED_OUTPUTS: usize = 100_000;
const MAX_SNAPSHOT_BYTES: u64 = 64 * 1024;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct Balance {
    pub output_count: usize,
    pub total: u64,
    pub mature: u64,
    pub immature: u64,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Snapshot {
    pub account: String,
    pub profile: String,
    pub native_height: u64,
    pub native_head: String,
    pub updated_at: i64,
    pub state: String,
    pub scanned_pmmr_position: u64,
    pub native_pmmr_size: u64,
    pub balance: Option<Balance>,
    pub mining_balance: Option<Balance>,
    pub error: Option<String>,
}
struct Owned {
    commitment: Commitment,
    value: u64,
    height: u64,
    coinbase: bool,
    mining: bool,
}
#[derive(serde::Serialize, serde::Deserialize)]
struct ScanState {
    account: String,
    profile: String,
    head_height: u64,
    head_hash: String,
    cursor: u64,
    owned: Vec<PersistedOwned>,
}
#[derive(serde::Serialize, serde::Deserialize)]
struct PersistedOwned {
    commitment: String,
    value: u64,
    height: u64,
    coinbase: bool,
    mining: bool,
}
const MAX_SCAN_STATE_BYTES: u64 = 32 * 1024 * 1024;
impl Balance {
    fn add(&mut self, owned: &Owned, view: &View<'_>) -> Result<()> {
        self.output_count += 1;
        self.total = self
            .total
            .checked_add(owned.value)
            .ok_or(Error::Invalid("wallet balance overflow"))?;
        let mature = !owned.coinbase
            || view.head.height
                >= owned
                    .height
                    .checked_add(view.profile.manifest.lifetime)
                    .ok_or(Error::Invalid("wallet maturity overflow"))?;
        let subtotal = if mature {
            &mut self.mature
        } else {
            &mut self.immature
        };
        *subtotal = subtotal
            .checked_add(owned.value)
            .ok_or(Error::Invalid("wallet balance overflow"))?;
        Ok(())
    }
}

pub struct Monitor {
    namespace: [u32; 2],
    start_position: u64,
    head: Option<Anchor>,
    cursor: u64,
    owned: Vec<Owned>,
    recheck: usize,
    rechecking: bool,
    pending_balance: Balance,
    pending_mining_balance: Balance,
    snapshot: Option<Snapshot>,
    saved_checkpoint: Option<(Anchor, u64, usize)>,
}
impl Monitor {
    pub fn new(namespace: [u32; 2], start_position: u64) -> Self {
        Self {
            namespace,
            start_position: start_position.max(1),
            head: None,
            cursor: start_position.max(1),
            owned: Vec::new(),
            recheck: 0,
            rechecking: false,
            pending_balance: Balance::default(),
            pending_mining_balance: Balance::default(),
            snapshot: None,
            saved_checkpoint: None,
        }
    }
}
impl Monitor {
    pub fn snapshot(&self) -> Option<&Snapshot> {
        self.snapshot.as_ref()
    }
    fn restore(
        &mut self,
        path: &Path,
        wallet: &Wallet,
        profile: &Profile,
        start_position: u64,
    ) -> Result<bool> {
        let bytes = match crate::secure_file::read_regular(path, MAX_SCAN_STATE_BYTES, true) {
            Ok(bytes) => bytes,
            Err(Error::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => {
                return Ok(false)
            }
            Err(error) => return Err(error),
        };
        let state: ScanState = serde_json::from_slice(&bytes)?;
        if state.account != hex(&wallet.account()) || state.profile != hex(&profile.protocol) {
            return Ok(false);
        }
        if state.head_hash.len() != 64
            || !state
                .head_hash
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
            || state.owned.len() > MAX_OWNED_OUTPUTS
        {
            return Err(Error::Invalid("wallet scan state"));
        }
        let head_hash: [u8; 32] = crate::objects::unhex(&state.head_hash)?
            .try_into()
            .map_err(|_| Error::Invalid("wallet scan state head"))?;
        let owned = state
            .owned
            .into_iter()
            .map(|owned| {
                let commitment: [u8; 33] = crate::objects::unhex(&owned.commitment)?
                    .try_into()
                    .map_err(|_| Error::Invalid("wallet scan state commitment"))?;
                crate::native::commitment(commitment)?;
                Ok(Owned {
                    commitment: Commitment(commitment),
                    value: owned.value,
                    height: owned.height,
                    coinbase: owned.coinbase,
                    mining: owned.mining,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        self.head = Some(Anchor {
            height: state.head_height,
            hash: head_hash,
        });
        self.cursor = state.cursor.max(start_position).max(1);
        self.owned = owned;
        Ok(true)
    }
    fn save_state(&mut self, path: &Path, wallet: &Wallet, profile: &Profile) -> Result<()> {
        let head = self
            .head
            .as_ref()
            .ok_or(Error::Invalid("wallet scan state head"))?;
        let checkpoint = (head.clone(), self.cursor, self.owned.len());
        if self.saved_checkpoint.as_ref() == Some(&checkpoint) {
            return Ok(());
        }
        let state = ScanState {
            account: hex(&wallet.account()),
            profile: hex(&profile.protocol),
            head_height: head.height,
            head_hash: hex(&head.hash),
            cursor: self.cursor,
            owned: self
                .owned
                .iter()
                .map(|owned| PersistedOwned {
                    commitment: hex(&owned.commitment.0),
                    value: owned.value,
                    height: owned.height,
                    coinbase: owned.coinbase,
                    mining: owned.mining,
                })
                .collect(),
        };
        let bytes = serde_json::to_vec(&state)?;
        if bytes.len() as u64 > MAX_SCAN_STATE_BYTES {
            return Err(Error::Capacity("wallet scan state"));
        }
        crate::secure_file::publish_atomic(path, &bytes, true)?;
        self.saved_checkpoint = Some(checkpoint);
        Ok(())
    }
    pub fn step(&mut self, wallet: &Wallet, view: &View<'_>) -> Result<&Snapshot> {
        if let Err(error) = self.advance(wallet, view) {
            self.unavailable(&error);
            return Err(error);
        }
        Ok(self.snapshot.as_ref().unwrap())
    }
    fn advance(&mut self, wallet: &Wallet, view: &View<'_>) -> Result<()> {
        let maximum = view.anchor(&view.head)?.output_mmr_size;
        if self.head.as_ref() != Some(&view.head) {
            let extension = self
                .head
                .as_ref()
                .is_some_and(|old| view.anchor(old).is_ok());
            if !extension {
                self.owned.clear();
                self.cursor = self.start_position;
            }
            self.head = Some(view.head.clone());
            self.recheck = 0;
            self.rechecking = false;
            self.snapshot = Some(Snapshot {
                account: hex(&wallet.account()),
                profile: hex(&view.profile.protocol),
                native_height: view.head.height,
                native_head: hex(&view.head.hash),
                updated_at: view.now,
                state: "scanning".into(),
                scanned_pmmr_position: self.cursor - 1,
                native_pmmr_size: maximum,
                balance: None,
                mining_balance: None,
                error: None,
            });
        }
        if self.snapshot.as_ref().is_some_and(|s| s.state == "ready") {
            view.check()?;
            self.snapshot.as_mut().unwrap().updated_at = view.now;
            return Ok(());
        }
        if self.cursor <= maximum {
            let (last, _, outputs) = view
                .chain
                .unspent_outputs_by_pmmr_index(self.cursor, BATCH, Some(maximum))
                .map_err(|e| Error::NativeUnavailable(e.to_string()))?;
            let mut discovered = Vec::new();
            for output in outputs {
                if let Some((value, path)) = wallet.rewind_owned_details(&output)? {
                    let path = path.to_path();
                    let mining = path.depth == 4
                        && matches!(u32::from(path.path[0]), 2 | 3 | 6)
                        && u32::from(path.path[1]) == self.namespace[0]
                        && u32::from(path.path[2]) == self.namespace[1];
                    let header = view
                        .chain
                        .get_header_for_output(output.commitment())
                        .map_err(|e| Error::NativeUnavailable(e.to_string()))?;
                    discovered.push(Owned {
                        commitment: output.commitment(),
                        value,
                        height: header.height,
                        coinbase: output.is_coinbase(),
                        mining,
                    });
                }
            }
            view.check()?;
            if self.owned.len().saturating_add(discovered.len()) > MAX_OWNED_OUTPUTS {
                self.snapshot.as_mut().unwrap().state = "capacity".into();
                return Err(Error::Invalid("wallet monitor output capacity exceeded"));
            }
            self.owned.extend(discovered);
            self.cursor = if last < self.cursor {
                maximum.saturating_add(1)
            } else {
                last + 1
            };
            self.snapshot.as_mut().unwrap().scanned_pmmr_position = self.cursor - 1;
        } else {
            if !self.rechecking {
                self.recheck = 0;
                self.rechecking = true;
                self.pending_balance = Balance::default();
                self.pending_mining_balance = Balance::default();
            }
            let end = (self.recheck + BATCH as usize).min(self.owned.len());
            let mut spent = Vec::new();
            let mut balance = self.pending_balance.clone();
            let mut mining_balance = self.pending_mining_balance.clone();
            for index in self.recheck..end {
                let owned = &self.owned[index];
                match view
                    .chain
                    .get_unspent(owned.commitment)
                    .map_err(|e| Error::NativeUnavailable(e.to_string()))?
                {
                    Some((identifier, position)) => {
                        if position.height != owned.height
                            || identifier.is_coinbase() != owned.coinbase
                        {
                            return Err(Error::View);
                        }
                        balance.add(owned, view)?;
                        if owned.mining {
                            mining_balance.add(owned, view)?;
                        }
                    }
                    None => spent.push(index),
                }
            }
            view.check()?;
            for index in spent.iter().rev() {
                self.owned.remove(*index);
            }
            self.recheck = end - spent.len();
            self.pending_balance = balance;
            self.pending_mining_balance = mining_balance;
            if self.recheck == self.owned.len() {
                let snapshot = self.snapshot.as_mut().unwrap();
                snapshot.state = "ready".into();
                snapshot.balance = Some(self.pending_balance.clone());
                snapshot.mining_balance = Some(self.pending_mining_balance.clone());
                snapshot.error = None;
            }
        }
        self.snapshot.as_mut().unwrap().updated_at = view.now;
        Ok(())
    }
    fn unavailable(&mut self, error: &Error) {
        if let Some(snapshot) = self.snapshot.as_mut() {
            if snapshot.state != "capacity" {
                snapshot.state = "unavailable".into();
            }
            snapshot.balance = None;
            snapshot.mining_balance = None;
            snapshot.error = Some(error.to_string().chars().take(1024).collect());
        }
    }
    fn retry_after_view_change(&mut self) {
        if let Some(snapshot) = self.snapshot.as_mut() {
            snapshot.state = "scanning".into();
            snapshot.balance = None;
            snapshot.mining_balance = None;
            snapshot.error = None;
        }
    }
}
pub fn read_snapshot(path: &Path) -> Result<Snapshot> {
    let data = crate::secure_file::read_regular(path, MAX_SNAPSHOT_BYTES, false)?;
    let snapshot: Snapshot = serde_json::from_slice(&data)?;
    validate_snapshot(&snapshot)?;
    Ok(snapshot)
}
pub fn read_current_snapshot(path: &Path) -> Result<Snapshot> {
    let mut snapshot = read_snapshot(path)?;
    let fresh = chrono::Utc::now()
        .timestamp()
        .checked_sub(snapshot.updated_at)
        .is_some_and(|age| (0..=10).contains(&age));
    if !fresh {
        snapshot.state = "unavailable".into();
        snapshot.balance = None;
        snapshot.mining_balance = None;
        snapshot.error = Some("wallet status is stale; start or check this node".into());
    }
    Ok(snapshot)
}
fn validate_snapshot(snapshot: &Snapshot) -> Result<()> {
    fn hex_width(value: &str, width: usize) -> bool {
        value.len() == width
            && value
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    }
    fn valid_balance(balance: &Balance) -> bool {
        balance.output_count <= MAX_OWNED_OUTPUTS
            && balance.mature.checked_add(balance.immature) == Some(balance.total)
            && (balance.output_count != 0 || balance.total == 0)
    }
    if !hex_width(&snapshot.account, 66)
        || !hex_width(&snapshot.profile, 64)
        || !matches!(
            snapshot.state.as_str(),
            "scanning" | "ready" | "unavailable" | "capacity"
        )
        || snapshot.updated_at <= 0
        || snapshot.scanned_pmmr_position > snapshot.native_pmmr_size
        || snapshot
            .error
            .as_ref()
            .is_some_and(|s| s.chars().count() > 1024)
    {
        return Err(Error::Invalid("wallet snapshot metadata"));
    }
    let account: [u8; 33] = crate::objects::unhex(&snapshot.account)?
        .try_into()
        .map_err(|_| Error::Invalid("wallet snapshot account"))?;
    crate::native::public_key(account).map_err(|_| Error::Invalid("wallet snapshot account"))?;
    if !hex_width(&snapshot.native_head, 64)
        && (!snapshot.native_head.is_empty()
            || snapshot.native_height != 0
            || snapshot.native_pmmr_size != 0
            || snapshot.state == "ready")
    {
        return Err(Error::Invalid("wallet snapshot native head"));
    }
    match (&snapshot.balance, &snapshot.mining_balance) {
        (Some(all), Some(mining))
            if snapshot.state == "ready"
                && snapshot.error.is_none()
                && snapshot.scanned_pmmr_position == snapshot.native_pmmr_size
                && valid_balance(all)
                && valid_balance(mining)
                && mining.output_count <= all.output_count
                && mining.total <= all.total
                && mining.mature <= all.mature
                && mining.immature <= all.immature =>
        {
            Ok(())
        }
        (None, None) if snapshot.state != "ready" => Ok(()),
        _ => Err(Error::Invalid("wallet snapshot balance")),
    }
}
pub fn publish(path: &Path, snapshot: &Snapshot) -> Result<()> {
    publish_json(path, snapshot)
}
pub(crate) fn publish_json(path: &Path, value: &impl Serialize) -> Result<()> {
    let bytes = serde_json::to_vec_pretty(value)?;
    if bytes.len() as u64 > MAX_SNAPSHOT_BYTES {
        return Err(Error::Invalid("wallet snapshot size"));
    }
    crate::secure_file::publish_atomic(path, &bytes, true)
}
pub fn run(
    chain: Arc<Chain>,
    profile: Profile,
    wallet: Arc<Wallet>,
    namespace: [u32; 2],
    start_position: u64,
    path: &Path,
    stop: Arc<AtomicBool>,
) {
    let mut monitor = Monitor::new(namespace, start_position);
    let state_path = path.with_file_name("wallet-scan-state.json");
    match monitor.restore(&state_path, &wallet, &profile, start_position) {
        Ok(true) => eprintln!(
            "wallet scan resumed at PMMR position {} with {} owned outputs",
            monitor.cursor,
            monitor.owned.len()
        ),
        Ok(false) => {}
        Err(error) => {
            eprintln!("wallet scan checkpoint unavailable; starting a fresh scan: {error}")
        }
    }
    monitor.snapshot = Some(Snapshot {
        account: hex(&wallet.account()),
        profile: hex(&profile.protocol),
        native_height: 0,
        native_head: String::new(),
        updated_at: chrono::Utc::now().timestamp(),
        state: "scanning".into(),
        scanned_pmmr_position: 0,
        native_pmmr_size: 0,
        balance: None,
        mining_balance: None,
        error: None,
    });
    if let Err(error) = publish(path, monitor.snapshot().unwrap()) {
        eprintln!("wallet status publication retryable: {error}");
    }
    let mut previous = None;
    let mut published = None;
    let mut last_progress_log = std::time::Instant::now() - Duration::from_secs(10);
    let mut last_progress_position = monitor.cursor.saturating_sub(1);
    let mut last_state_save = std::time::Instant::now() - Duration::from_secs(5);
    while !stop.load(Ordering::Acquire) {
        let result = View::pin(&chain, profile.clone(), chrono::Utc::now().timestamp())
            .and_then(|view| monitor.step(&wallet, &view).map(|_| ()));
        match result {
            Ok(()) => previous = None,
            Err(Error::View) => {
                monitor.retry_after_view_change();
                if previous.as_deref() != Some("view") {
                    eprintln!(
                        "wallet scan retrying: native head advanced while checking PMMR position {}",
                        monitor.snapshot().map_or(0, |snapshot| snapshot.scanned_pmmr_position)
                    );
                }
                previous = Some("view".into());
            }
            Err(error) => {
                monitor.unavailable(&error);
                if previous.as_ref() != Some(&error.to_string()) {
                    eprintln!("wallet monitoring retryable: {error}");
                }
                previous = Some(error.to_string());
            }
        }
        if let Some(snapshot) = monitor.snapshot() {
            if published.as_ref() != Some(snapshot) {
                match publish(path, snapshot) {
                    Ok(()) => published = Some(snapshot.clone()),
                    Err(error) => eprintln!("wallet status publication retryable: {error}"),
                }
            }
            if snapshot.state == "scanning"
                && last_progress_log.elapsed() >= Duration::from_secs(10)
            {
                let percent = if snapshot.native_pmmr_size == 0 {
                    0
                } else {
                    snapshot
                        .scanned_pmmr_position
                        .saturating_mul(100)
                        .checked_div(snapshot.native_pmmr_size)
                        .unwrap_or(100)
                        .min(100)
                };
                let elapsed = last_progress_log.elapsed().as_secs_f64().max(0.001);
                let rate = snapshot
                    .scanned_pmmr_position
                    .saturating_sub(last_progress_position) as f64
                    / elapsed;
                let eta =
                    if rate > 0.0 && snapshot.native_pmmr_size > snapshot.scanned_pmmr_position {
                        format!(
                            "; ETA {}s",
                            ((snapshot.native_pmmr_size - snapshot.scanned_pmmr_position) as f64
                                / rate) as u64
                        )
                    } else {
                        String::new()
                    };
                eprintln!(
                    "wallet scanning: PMMR {}/{} ({}%), owned outputs {}, rate {:.0} PMMR/s{}",
                    snapshot.scanned_pmmr_position,
                    snapshot.native_pmmr_size,
                    percent,
                    monitor.owned.len(),
                    rate,
                    eta
                );
                last_progress_position = snapshot.scanned_pmmr_position;
                last_progress_log = std::time::Instant::now();
            }
            let scanning = snapshot.state == "scanning";
            if last_state_save.elapsed() >= Duration::from_secs(5) || !scanning {
                match monitor.save_state(&state_path, &wallet, &profile) {
                    Ok(()) => last_state_save = std::time::Instant::now(),
                    Err(error) => eprintln!("wallet scan checkpoint retryable: {error}"),
                }
            }
            if !scanning {
                thread::sleep(Duration::from_millis(250));
            } else {
                thread::yield_now();
            }
        } else {
            thread::sleep(Duration::from_millis(250));
        }
    }
    monitor.unavailable(&Error::Invalid("daemon stopped"));
    if let Err(error) = monitor.save_state(&state_path, &wallet, &profile) {
        eprintln!("wallet scan checkpoint retryable: {error}");
    }
    if let Some(snapshot) = monitor.snapshot() {
        if let Err(error) = publish(path, snapshot) {
            eprintln!("wallet status publication retryable: {error}");
        }
    }
}

#[cfg(test)]
#[path = "wallet_tests.rs"]
mod tests;
