use super::*;

pub const NATIVE_CODEC: u32 = 1000;
const BOOTSTRAP_MAX_AGE_SECONDS: u64 = 4 * 60;
const ALTERNATIVE_MAX_AGE_SECONDS: u64 = 20 * 60;
pub(crate) const MAX_ANCHOR_TIMESTAMP_DEVIATION: u64 = 3 * 60 * 60;

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
    pub pool_id: Id,
    pub network: global::ChainTypes,
    pub genesis: Id,
    pub subsidy: u64,
    pub finder_floor: u64,
    pub unit_budget: u16,
    pub share_divisor: u64,
    pub share_target_seconds: u16,
    pub share_window: u32,
    pub lifetime: u64,
    pub uncle_depth: u8,
    pub max_uncles: u8,
    pub future_seconds: u32,
    pub edge_bits: u8,
    pub proof_size: u16,
}
impl Profile {
    pub fn new(genesis: Id, kind: global::ChainTypes, pool: &crate::pool::Pool) -> Self {
        let (receipts, share_window, lifetime, edge_bits, proof_size) = match kind {
            global::ChainTypes::Mainnet | global::ChainTypes::Testnet => (512, 2160, 2160, 32, 42),
            global::ChainTypes::AutomatedTesting => (8, 3, 3, 10, 8),
            global::ChainTypes::UserTesting => (8, 3, 3, 15, 42),
        };
        let weight = if matches!(
            kind,
            global::ChainTypes::Mainnet | global::ChainTypes::Testnet
        ) {
            consensus::MAX_BLOCK_WEIGHT / 2
        } else {
            global::TESTING_MAX_BLOCK_WEIGHT
        };
        Self {
            pool_id: pool.id(genesis),
            network: kind,
            genesis,
            subsidy: 60_000_000_000,
            finder_floor: 1,
            unit_budget: payout_precision(receipts, weight)
                .expect("supported native payout reservation"),
            share_divisor: 512,
            share_target_seconds: 10,
            share_window,
            lifetime,
            uncle_depth: 8,
            max_uncles: 8,
            future_seconds: 300,
            edge_bits,
            proof_size,
        }
    }
    pub fn work_window_limit(&self) -> usize {
        self.share_window as usize
    }
    pub fn bootstrap_age(&self) -> u64 {
        BOOTSTRAP_MAX_AGE_SECONDS.div_ceil(consensus::BLOCK_TIME_SEC)
    }
    pub fn alternative_age(&self) -> u64 {
        ALTERNATIVE_MAX_AGE_SECONDS.div_ceil(consensus::BLOCK_TIME_SEC)
    }
    pub fn window_seconds(&self) -> u64 {
        u64::from(self.share_window) * u64::from(self.share_target_seconds)
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
        if matches!(
            self.network,
            global::ChainTypes::Mainnet | global::ChainTypes::Testnet
        ) {
            consensus::MAX_BLOCK_WEIGHT / 2
        } else {
            global::max_block_weight()
        }
    }
    pub fn finance_low(&self, anchor: u64) -> u64 {
        anchor.saturating_sub(self.lifetime - 1)
    }
    pub fn work_low(&self, anchor: u64) -> u64 {
        self.finance_low(anchor)
            .max(anchor.saturating_sub(self.anchor_window()))
    }
    pub fn assigned(&self, network: u64, height: u64) -> Result<u64> {
        if network == 0 {
            return Err(Error::Invalid("zero network target"));
        }
        let divisor = self.share_divisor;
        let ceil = network / divisor + u64::from(!network.is_multiple_of(divisor));
        Ok(ceil.max(consensus::graph_weight(height, self.edge_bits)))
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
        let graph = u128::from(consensus::graph_weight(height, self.edge_bits));
        Ok(expected.max(graph).min(u128::from(u64::MAX)) as u64)
    }
    pub fn allocation_budget(&self, fees: u64, output_slots: usize) -> Result<u16> {
        let precision = self.unit_budget;
        let reward = self
            .subsidy
            .checked_add(fees)
            .ok_or(Error::Invalid("reward overflow"))?;
        let face = 5 * self.q();
        let value_budget = (reward - self.finder_floor) / face;
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
        (self.subsidy - self.finder_floor) / (5 * self.unit_budget as u64)
    }
    pub fn face(&self, family: u8, row: u8) -> Result<u64> {
        let max_base = self.unit_budget.ilog2() as u8;
        if (family == 0 && row > max_base) || (family == 1 && row > 2) || family > 1 {
            return Err(Error::Invalid("denomination row"));
        }
        Ok(self.q() * (if family == 0 { 5 } else { 4 }) * (1u64 << row))
    }
}

#[cfg(test)]
#[path = "profile_tests.rs"]
mod tests;
