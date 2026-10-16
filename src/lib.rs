#![deny(unsafe_op_in_unsafe_fn)]

pub mod codec;
pub mod consensus;
pub mod control;
pub mod limits;
#[doc(hidden)]
pub mod mining;
pub mod native;
#[doc(hidden)]
pub mod network;
#[doc(hidden)]
pub mod node;
pub mod objects;
pub mod secure_file;
#[doc(hidden)]
pub mod status;
#[doc(hidden)]
pub mod storage;
#[doc(hidden)]
pub mod wallet;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("invalid: {0}")]
    Invalid(&'static str),
    #[error("bad replaceable witness: {0}")]
    Witness(&'static str),
    #[error("required object missing: {0:?}")]
    Missing(codec::Id),
    #[error("required object unavailable: {0}")]
    Unavailable(objects::Key),
    #[error("native data unavailable: {0}")]
    NativeUnavailable(String),
    #[error("native view incompatible or changed")]
    View,
    #[error("candidate timestamp is in the future")]
    Future,
    #[error("tip expired")]
    Expired,
    #[error("local resource limit: {0}")]
    Capacity(&'static str),
    #[error("evaluation cancelled")]
    Cancelled,
    #[error("storage: {0}")]
    Storage(#[from] heed::Error),
    #[error("I/O: {0}")]
    Io(#[from] std::io::Error),
    #[error("JSON: {0}")]
    Json(#[from] serde_json::Error),
}
pub type Result<T> = std::result::Result<T, Error>;

pub fn default_data_dir() -> std::path::PathBuf {
    directories::ProjectDirs::from("org", "G2Pool", "G2Pool")
        .map(|dirs| dirs.data_local_dir().to_owned())
        .unwrap_or_else(|| std::path::PathBuf::from(".g2pool"))
}

pub fn network_data_dir(base: std::path::PathBuf, network: &str) -> std::path::PathBuf {
    base.join(network)
}
