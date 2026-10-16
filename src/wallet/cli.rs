use crate::{codec::Registration, wallet::Wallet, Error, Result};
use grin_keychain::mnemonic;
use std::{
    fs,
    path::{Path, PathBuf},
};

fn publish(path: &Path, bytes: &[u8]) -> Result<()> {
    crate::secure_file::publish_atomic(path, bytes, false)
}
fn validate_store(data: &Path, account: &[u8]) -> Result<()> {
    let identity = crate::storage::sharechain::SharechainDb::existing_wallet_identity(
        &data.join("pool.sharechain"),
    )
    .map_err(|_| Error::Invalid("wallet store"))?;
    if let Some(previous) = identity.account {
        if previous.as_slice() != account {
            return Err(Error::Invalid("wallet seed does not match this store"));
        }
    }
    for bytes in identity.registrations {
        let registration: Registration = crate::codec::decode(&bytes)?;
        if registration.fields.owner != account {
            return Err(Error::Invalid("wallet seed does not match this store"));
        }
    }
    Ok(())
}

pub fn run(arguments: Vec<String>) -> Result<()> {
    let help = arguments
        .iter()
        .any(|arg| matches!(arg.as_str(), "--help" | "-h"));
    let mut args = arguments.into_iter();
    let command = args.next();
    if help {
        match command.as_deref() {
            Some("backup") => println!("g2pool backup [--network NETWORK] [--data-dir PATH] --output NEWFILE\nSave the wallet recovery words privately outside the data directory."),
            _ => return Err(Error::Invalid("use g2pool backup")),
        }
        return Ok(());
    }
    if command.as_deref() != Some("backup") {
        return Err(Error::Invalid("use g2pool backup (use --help)"));
    }
    let (mut base, mut output) = (None, None);
    let mut network = "mainnet".to_owned();
    let mut network_seen = false;
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--data-dir" if base.is_none() => {
                let value = args
                    .next()
                    .filter(|value| !value.starts_with('-'))
                    .ok_or(Error::Invalid("missing data directory"))?;
                base = Some(PathBuf::from(value));
            }
            "--network" if !network_seen => {
                network_seen = true;
                network = args
                    .next()
                    .filter(|value| matches!(value.as_str(), "mainnet" | "testnet"))
                    .ok_or(Error::Invalid("network"))?;
            }
            "--output" if output.is_none() => {
                let value = args
                    .next()
                    .filter(|value| !value.starts_with('-'))
                    .ok_or(Error::Invalid("missing output file"))?;
                output = Some(PathBuf::from(value));
            }
            _ => return Err(Error::Invalid("unknown or repeated argument")),
        }
    }
    let data = fs::canonicalize(crate::network_data_dir(
        base.unwrap_or_else(crate::default_data_dir),
        &network,
    ))?;
    let requested = output.ok_or(Error::Invalid("missing output file"))?;
    let name = requested
        .file_name()
        .ok_or(Error::Invalid("output filename"))?;
    let parent = fs::canonicalize(
        requested
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or(Path::new(".")),
    )?;
    if parent.starts_with(&data) {
        return Err(Error::Invalid(
            "backup output must be outside the pool data directory",
        ));
    }
    let output = parent.join(name);
    match fs::symlink_metadata(&output) {
        Ok(_) => return Err(Error::Invalid("backup output already exists")),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => (),
        Err(e) => return Err(e.into()),
    }
    let seed_path = data.join("wallet.seed");
    let wallet = Wallet::open_existing(&seed_path)?;
    let account = wallet.account();
    validate_store(&data, &account)?;
    let mut seed = crate::secure_file::read_regular(&seed_path, 33, true)?;
    if !matches!(seed.len(), 16 | 20 | 24 | 28 | 32)
        || Wallet::from_seed(&seed)?.account() != account
    {
        seed.fill(0);
        return Err(Error::Invalid("wallet seed changed"));
    }
    let mut words =
        mnemonic::from_entropy(&seed).map_err(|_| Error::Invalid("recovery encoding"))?;
    seed.fill(0);
    words.push('\n');
    let result = publish(&output, words.as_bytes());
    clear_recovery(&mut words);
    result?;
    println!("Seed backup written to {:?}.", output);
    Ok(())
}
fn clear_recovery(words: &mut String) {
    words.replace_range(.., &" ".repeat(words.len()));
    words.clear();
}
