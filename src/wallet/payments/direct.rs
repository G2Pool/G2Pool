use grin_chain::Chain;
use grin_core::core::{Transaction, TxKernel};
use grin_pool::TxSource;
use grin_util::{secp::pedersen::Commitment, ToHex};
use grin_wallet_libwallet::{NodeClient, NodeVersionInfo};
use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};

#[derive(Clone)]
pub struct DirectNodeClient {
    chain: Arc<Chain>,
    tx_pool: grin_servers::ServerTxPool,
    version: Option<NodeVersionInfo>,
    stop: Arc<AtomicBool>,
}

impl DirectNodeClient {
    pub fn new(
        chain: Arc<Chain>,
        tx_pool: grin_servers::ServerTxPool,
        stop: Arc<AtomicBool>,
    ) -> Self {
        Self {
            chain,
            tx_pool,
            version: None,
            stop,
        }
    }

    fn check_running(&self) -> Result<(), grin_wallet_libwallet::Error> {
        if self.stop.load(Ordering::Acquire) {
            return Err(Self::error("G2Pool is stopping"));
        }
        Ok(())
    }
    fn error(message: impl Into<String>) -> grin_wallet_libwallet::Error {
        grin_wallet_libwallet::Error::ClientCallback(message.into())
    }
}

impl NodeClient for DirectNodeClient {
    fn node_url(&self) -> &str {
        "embedded://grin"
    }

    fn set_node_url(&mut self, _node_url: &str) {}

    fn node_api_secret(&self) -> Option<String> {
        None
    }

    fn set_node_api_secret(&mut self, _node_api_secret: Option<String>) {}

    fn post_tx(&self, tx: &Transaction, fluff: bool) -> Result<(), grin_wallet_libwallet::Error> {
        self.check_running()?;
        let header = self
            .chain
            .head_header()
            .map_err(|e| Self::error(e.to_string()))?;
        self.tx_pool
            .write()
            .add_to_pool(TxSource::PushApi, tx.clone(), !fluff, &header)
            .or_else(|error| match error {
                grin_pool::PoolError::DuplicateTx => Ok(()),
                error => Err(error),
            })
            .map_err(|e| Self::error(e.to_string()))
    }

    fn get_version_info(&mut self) -> Option<NodeVersionInfo> {
        self.check_running().ok()?;
        if let Some(version) = &self.version {
            return Some(version.clone());
        }
        let version = NodeVersionInfo {
            node_version: "embedded".into(),
            block_header_version: self.chain.head_header().ok()?.version.0,
            verified: Some(true),
        };
        self.version = Some(version.clone());
        Some(version)
    }

    fn get_chain_tip(&self) -> Result<(u64, String), grin_wallet_libwallet::Error> {
        self.check_running()?;
        let tip = self.chain.head().map_err(|e| Self::error(e.to_string()))?;
        Ok((tip.height, tip.last_block_h.to_hex()))
    }

    fn get_kernel(
        &mut self,
        excess: &Commitment,
        min_height: Option<u64>,
        max_height: Option<u64>,
    ) -> Result<Option<(TxKernel, u64, u64)>, grin_wallet_libwallet::Error> {
        self.check_running()?;
        self.chain
            .get_kernel_height(excess, min_height, max_height)
            .map_err(|e| Self::error(e.to_string()))
    }

    fn get_outputs_from_node(
        &self,
        wallet_outputs: Vec<Commitment>,
    ) -> Result<HashMap<Commitment, (String, u64, u64)>, grin_wallet_libwallet::Error> {
        let mut result = HashMap::new();
        self.check_running()?;
        for commit in wallet_outputs {
            self.check_running()?;
            if let Some((_, output)) = self
                .chain
                .get_unspent(commit)
                .map_err(|e| Self::error(e.to_string()))?
            {
                result.insert(commit, (commit.0.to_hex(), output.height, output.pos));
            }
        }
        Ok(result)
    }

    fn get_outputs_by_pmmr_index(
        &self,
        start: u64,
        end: Option<u64>,
        max: u64,
    ) -> Result<
        (
            u64,
            u64,
            Vec<(
                Commitment,
                grin_util::secp::pedersen::RangeProof,
                bool,
                u64,
                u64,
            )>,
        ),
        grin_wallet_libwallet::Error,
    > {
        self.check_running()?;
        let (last, highest, outputs) = self
            .chain
            .unspent_outputs_by_pmmr_index(start.max(1), max, end)
            .map_err(|e| Self::error(e.to_string()))?;
        let mut result = Vec::with_capacity(outputs.len());
        for output in outputs {
            self.check_running()?;
            let commit = output.commitment();
            let header = self
                .chain
                .get_header_for_output(commit)
                .map_err(|e| Self::error(e.to_string()))?;
            let position = self
                .chain
                .get_output_pos(&commit)
                .map_err(|e| Self::error(e.to_string()))?;
            result.push((
                commit,
                output.proof,
                output.features().is_coinbase(),
                header.height,
                position,
            ));
        }
        Ok((last, highest, result))
    }

    fn height_range_to_pmmr_indices(
        &self,
        start: u64,
        end: Option<u64>,
    ) -> Result<(u64, u64), grin_wallet_libwallet::Error> {
        self.check_running()?;
        self.chain
            .block_height_range_to_pmmr_indices(start, end)
            .map_err(|e| Self::error(e.to_string()))
    }
}
