use super::*;

pub const NATIVE_VERSION: &[u8; 10] = b"grin-5.5.2";
pub const NATIVE_CODEC: u32 = 1000;
const BOOTSTRAP_MAX_AGE_SECONDS: u64 = 4 * 60;
const ALTERNATIVE_MAX_AGE_SECONDS: u64 = 20 * 60;
pub(crate) const MAX_ANCHOR_TIMESTAMP_DEVIATION: u64 = 3 * 60 * 60;

pub fn default_limits() -> Limits {
    Limits {
        work_core: 16384,
        coupon: 2048,
        inventory: 262144,
        registration: 256,
        finder_output: 2048,
        payout_record: crate::codec::MAX_PAYOUT_RECORD_BYTES as u32,
        kernel_encoding_plan: 262144,
        transaction_only: 8388608,
        residual_body: 8388608,
    }
}
fn base_manifest(genesis: Id, kind: global::ChainTypes) -> Manifest {
    let (network, unit_budget, share_window, lifetime, edge, proof) = match kind {
        global::ChainTypes::Mainnet => (0, 512, 2160, 2160, 32, 42),
        global::ChainTypes::Testnet => (1, 512, 2160, 2160, 32, 42),
        global::ChainTypes::AutomatedTesting => (2, 8, 3, 3, 10, 8),
        global::ChainTypes::UserTesting => (3, 8, 3, 3, 15, 42),
    };
    Manifest {
        network,
        genesis,
        grin_version: *NATIVE_VERSION,
        subsidy: 60_000_000_000,
        finder_floor: 1,
        unit_budget,
        share_divisor: 512,
        share_target_seconds: 10,
        share_window,
        lifetime,
        leaves_per_row: crate::codec::COUPON_LEAVES_PER_ROW as u8,
        uncle_depth: 8,
        max_uncles: 8,
        future_seconds: 300,
        edge_bits: edge,
        proof_size: proof,
        limits: default_limits(),
    }
}
pub fn manifest(genesis: Id, kind: global::ChainTypes) -> Manifest {
    let mut profile = base_manifest(genesis, kind);
    let (receipts, weight) = match kind {
        global::ChainTypes::Mainnet | global::ChainTypes::Testnet => {
            (512, consensus::MAX_BLOCK_WEIGHT / 2)
        }
        _ => (8, global::TESTING_MAX_BLOCK_WEIGHT),
    };
    profile.unit_budget =
        payout_precision(receipts, weight).expect("supported native payout reservation");
    profile
}
pub fn payout_precision(receipts: usize, reserved_weight: u64) -> Result<u16> {
    let mut best = None;
    for bits in 0..=15 {
        let units = 1u16 << bits;
        let outputs = crate::consensus::output_bound(units, receipts) as u64;
        let weight = outputs * (consensus::OUTPUT_WEIGHT + consensus::KERNEL_WEIGHT)
            + consensus::OUTPUT_WEIGHT
            + 2 * consensus::KERNEL_WEIGHT;
        if weight <= reserved_weight {
            best = Some(units);
        } else {
            break;
        }
    }
    best.ok_or(Error::Invalid("native payout reservation too small"))
}
#[derive(Debug, Clone)]
pub struct Profile {
    pub manifest: Manifest,
    pub protocol: Id,
}
impl Profile {
    pub fn new(m: Manifest) -> Result<Self> {
        let kind = global::get_chain_type();
        if m != manifest(m.genesis, kind) {
            return Err(Error::Invalid("unsupported profile"));
        }
        let protocol = id(&m)?;
        Ok(Self {
            manifest: m,
            protocol,
        })
    }
    pub fn work_window_limit(&self) -> usize {
        self.manifest.share_window as usize
    }
    pub fn bootstrap_age(&self) -> u64 {
        BOOTSTRAP_MAX_AGE_SECONDS.div_ceil(consensus::BLOCK_TIME_SEC)
    }
    pub fn alternative_age(&self) -> u64 {
        ALTERNATIVE_MAX_AGE_SECONDS.div_ceil(consensus::BLOCK_TIME_SEC)
    }
    pub fn window_seconds(&self) -> u64 {
        u64::from(self.manifest.share_window) * u64::from(self.manifest.share_target_seconds)
    }
    pub fn anchor_window(&self) -> u64 {
        (4 * self.window_seconds()).div_ceil(consensus::BLOCK_TIME_SEC)
    }
    pub fn payout_work_limit(&self, network_difficulty: u64) -> Result<u128> {
        u128::from(network_difficulty)
            .checked_mul(2)
            .ok_or(Error::Invalid("payout work window overflow"))
    }
    pub fn payout_weight_reservation(&self) -> u64 {
        if self.manifest.network < 2 {
            consensus::MAX_BLOCK_WEIGHT / 2
        } else {
            global::max_block_weight()
        }
    }
    pub fn finance_low(&self, anchor: u64) -> u64 {
        anchor.saturating_sub(self.manifest.lifetime - 1)
    }
    pub fn work_low(&self, anchor: u64) -> u64 {
        self.finance_low(anchor)
            .max(anchor.saturating_sub(self.anchor_window()))
    }
    pub fn assigned(&self, network: u64, height: u64) -> Result<u64> {
        if network == 0 {
            return Err(Error::Invalid("zero network target"));
        }
        let divisor = self.manifest.share_divisor;
        let ceil = network / divisor + u64::from(!network.is_multiple_of(divisor));
        Ok(ceil.max(consensus::graph_weight(height, self.manifest.edge_bits)))
    }

    pub fn retargeted(
        &self,
        network: u64,
        height: u64,
        history: &[(u64, u128)],
        target_seconds: u64,
    ) -> Result<u64> {
        if target_seconds == 0 || history.len() < 2 {
            return self.assigned(network, height);
        }
        let cut = history.len().div_ceil(10);
        let low_index = cut - 1;
        let high_index = history.len() - cut;
        let mut timestamps: Vec<u64> = history.iter().map(|(timestamp, _)| *timestamp).collect();
        timestamps.sort_unstable();
        let first = timestamps[low_index];
        let last = timestamps[high_index];
        let delta_index = high_index.saturating_sub(low_index).max(1) as u64;
        let delta_t = last.saturating_sub(first).max(delta_index);
        let (low_work, high_work) = history
            .iter()
            .filter(|(timestamp, _)| *timestamp >= first && *timestamp <= last)
            .fold((u128::MAX, 0u128), |(low, high), (_, work)| {
                (low.min(*work), high.max(*work))
            });
        if low_work == u128::MAX {
            return Err(Error::Invalid("share timestamp window"));
        }
        let work = high_work
            .checked_sub(low_work)
            .ok_or(Error::Invalid("share work regression"))?;
        let expected = u128::from(target_seconds)
            .checked_mul(work)
            .and_then(|value| value.checked_div(u128::from(delta_t)))
            .ok_or(Error::Invalid("share retarget overflow"))?;
        let graph = u128::from(consensus::graph_weight(height, self.manifest.edge_bits));
        Ok(expected.max(graph).min(u128::from(u64::MAX)) as u64)
    }
    pub fn allocation_budget(&self, fees: u64, output_slots: usize) -> Result<u16> {
        let precision = self.manifest.unit_budget;
        let reward = self
            .manifest
            .subsidy
            .checked_add(fees)
            .ok_or(Error::Invalid("reward overflow"))?;
        let face = 5 * self.q();
        let value_budget = (reward - self.manifest.finder_floor) / face;
        let max_catalogue = u32::from(precision) * 2 - 1;
        let mut high = value_budget.min(u64::from(max_catalogue)) as u16;
        if high == 0 {
            return Ok(0);
        }
        let fits = |units: u16| {
            let outputs = crate::consensus::output_bound(units, output_slots) as u64;
            outputs * (consensus::OUTPUT_WEIGHT + consensus::KERNEL_WEIGHT)
                + consensus::OUTPUT_WEIGHT
                + 2 * consensus::KERNEL_WEIGHT
                <= self.payout_weight_reservation()
        };
        let mut low = precision.min(high);
        if !fits(low) {
            low = 1;
        }
        while low < high {
            let middle = low + (high - low).div_ceil(2);
            if fits(middle) {
                low = middle;
            } else {
                high = middle - 1;
            }
        }
        Ok(low)
    }
    pub fn q(&self) -> u64 {
        (self.manifest.subsidy - self.manifest.finder_floor)
            / (5 * self.manifest.unit_budget as u64)
    }
    pub fn face(&self, family: u8, row: u8) -> Result<u64> {
        let max_base = self.manifest.unit_budget.ilog2() as u8;
        if (family == 0 && row > max_base) || (family == 1 && row > 2) || family > 1 {
            return Err(Error::Invalid("denomination row"));
        }
        Ok(self.q() * (if family == 0 { 5 } else { 4 }) * (1u64 << row))
    }
}

#[cfg(test)]
#[path = "profile_tests.rs"]
mod tests;
