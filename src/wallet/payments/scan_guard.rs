use grin_core::{
    core::{Transaction, TxKernel},
    libtx::proof,
};
use grin_keychain::{ExtKeychain, Keychain};
use grin_util::secp::pedersen::{Commitment, RangeProof};
use grin_wallet_libwallet::{Error, NodeClient, NodeVersionInfo};
use std::{collections::HashMap, sync::Arc};

type OutputPage = (u64, u64, Vec<(Commitment, RangeProof, bool, u64, u64)>);

#[derive(Clone)]
pub struct GuardedNodeClient<C> {
    inner: C,
    keychain: Arc<ExtKeychain>,
}
impl<C> GuardedNodeClient<C> {
    pub fn new(inner: C, keychain: ExtKeychain) -> Self {
        Self {
            inner,
            keychain: Arc::new(keychain),
        }
    }
}
impl<C: NodeClient> NodeClient for GuardedNodeClient<C> {
    fn node_url(&self) -> &str {
        self.inner.node_url()
    }
    fn set_node_url(&mut self, url: &str) {
        self.inner.set_node_url(url)
    }
    fn node_api_secret(&self) -> Option<String> {
        self.inner.node_api_secret()
    }
    fn set_node_api_secret(&mut self, secret: Option<String>) {
        self.inner.set_node_api_secret(secret)
    }
    fn post_tx(&self, tx: &Transaction, fluff: bool) -> Result<(), Error> {
        self.inner.post_tx(tx, fluff)
    }
    fn get_version_info(&mut self) -> Option<NodeVersionInfo> {
        self.inner.get_version_info()
    }
    fn get_chain_tip(&self) -> Result<(u64, String), Error> {
        self.inner.get_chain_tip()
    }
    fn get_kernel(
        &mut self,
        excess: &Commitment,
        min: Option<u64>,
        max: Option<u64>,
    ) -> Result<Option<(TxKernel, u64, u64)>, Error> {
        self.inner.get_kernel(excess, min, max)
    }
    fn get_outputs_from_node(
        &self,
        outputs: Vec<Commitment>,
    ) -> Result<HashMap<Commitment, (String, u64, u64)>, Error> {
        self.inner.get_outputs_from_node(outputs)
    }
    fn height_range_to_pmmr_indices(
        &self,
        start: u64,
        end: Option<u64>,
    ) -> Result<(u64, u64), Error> {
        self.inner.height_range_to_pmmr_indices(start, end)
    }
    fn get_outputs_by_pmmr_index(
        &self,
        start: u64,
        end: Option<u64>,
        max: u64,
    ) -> Result<OutputPage, Error> {
        let page = self.inner.get_outputs_by_pmmr_index(start, end, max)?;
        let keychain = self.keychain.as_ref();
        let builder = proof::ProofBuilder::new(keychain);
        let legacy = proof::LegacyProofBuilder::new(keychain);
        for (commit, rangeproof, _, _, _) in &page.2 {
            let modern = proof::rewind(keychain.secp(), &builder, *commit, None, *rangeproof)?;
            let old = proof::rewind(keychain.secp(), &legacy, *commit, None, *rangeproof)?;
            for (amount, id, switch) in modern.into_iter().chain(old) {
                if keychain.commit(amount, &id, switch)? == *commit
                    && id.to_path().last_path_index() == u32::MAX
                {
                    return Err(Error::ClientCallback(
                        "owned output derivation capacity exhausted".into(),
                    ));
                }
            }
        }
        Ok(page)
    }
}
