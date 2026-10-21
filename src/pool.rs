use crate::{
    codec::Id,
    objects::{hex, unhex},
    secure_file, Error, Result,
};
use grin_core::global::ChainTypes;
use serde::{Deserialize, Serialize};
use std::{fs, net::SocketAddr, path::Path};

const ALPHA_KEY: [u8; 32] = [
    0xeb, 0x18, 0x3d, 0x59, 0xb6, 0xd5, 0x2d, 0xa9, 0x42, 0x7d, 0x71, 0x5c, 0x63, 0xe1, 0x22, 0xe7,
    0x03, 0x90, 0xfa, 0x8a, 0x58, 0xe7, 0xe4, 0x05, 0xe7, 0x24, 0x06, 0xbc, 0x47, 0xb6, 0x92, 0x7b,
];

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Pool {
    pub name: String,
    #[serde(with = "key_encoding")]
    key: [u8; 32],
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bootstrap_peers: Option<BootstrapPeers>,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BootstrapPeers {
    pub mainnet: Vec<SocketAddr>,
    pub testnet: Vec<SocketAddr>,
}

impl Pool {
    pub fn alpha() -> Self {
        Self {
            name: "Alpha".into(),
            key: ALPHA_KEY,
            bootstrap_peers: None,
        }
    }
    pub fn id(&self, genesis: Id) -> Id {
        let mut input = [0; 64];
        input[..32].copy_from_slice(&self.key);
        input[32..].copy_from_slice(&genesis);
        blake2::blake2b::blake2b(32, &[], &input)
            .as_bytes()
            .try_into()
            .unwrap()
    }
    pub(crate) fn key(&self) -> [u8; 32] {
        self.key
    }
    pub fn peers(&self, network: ChainTypes) -> Result<Vec<SocketAddr>> {
        if let Some(peers) = &self.bootstrap_peers {
            return Ok(match network {
                ChainTypes::Mainnet => peers.mainnet.clone(),
                ChainTypes::Testnet => peers.testnet.clone(),
                _ => Vec::new(),
            });
        }
        if self.key != ALPHA_KEY {
            return Err(Error::Invalid("custom pool requires bootstrap_peers"));
        }
        let addresses: &[&str] = match network {
            ChainTypes::Mainnet => &["188.119.24.7:3417", "51.83.197.245:3417"],
            ChainTypes::Testnet => &["188.119.24.7:13417", "51.83.197.245:13417"],
            _ => &[],
        };
        Ok(addresses
            .iter()
            .map(|address| address.parse().unwrap())
            .collect())
    }
    pub fn load(base: &Path, explicit: Option<&Path>) -> Result<Self> {
        let default = base.join("pool.json");
        let path = explicit.unwrap_or(&default);
        if explicit.is_none() {
            match fs::symlink_metadata(path) {
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                    fs::create_dir_all(base)?;
                    match secure_file::publish_atomic(
                        path,
                        &serde_json::to_vec_pretty(&Self::alpha())?,
                        false,
                    ) {
                        Ok(()) => {}
                        Err(Error::Io(error))
                            if error.kind() == std::io::ErrorKind::AlreadyExists => {}
                        Err(error) => return Err(error),
                    }
                }
                Ok(_) => {}
                Err(error) => return Err(error.into()),
            }
        }
        let pool: Self = serde_json::from_slice(&secure_file::read_regular(path, 16384, true)?)?;
        for network in [ChainTypes::Mainnet, ChainTypes::Testnet] {
            let mut book = crate::network::peers::Book::memory(false);
            for peer in pool.peers(network)? {
                book.seed(peer)?;
            }
        }
        Ok(pool)
    }
}

mod key_encoding {
    use super::*;
    pub fn serialize<S: serde::Serializer>(
        key: &[u8; 32],
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        serializer.serialize_str(&hex(key))
    }
    pub fn deserialize<'de, D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> std::result::Result<[u8; 32], D::Error> {
        let text = String::deserialize(deserializer)?;
        unhex(&text)
            .map_err(serde::de::Error::custom)?
            .try_into()
            .map_err(|_| serde::de::Error::custom("pool key must contain 32 bytes"))
    }
}

#[cfg(test)]
#[path = "pool_tests.rs"]
mod tests;
