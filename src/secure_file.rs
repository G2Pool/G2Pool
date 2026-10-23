use crate::{Error, Result};
#[cfg(unix)]
use std::os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt};
use std::{
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    path::Path,
};

pub(crate) fn open_lock(path: &Path) -> Result<File> {
    let mut options = OpenOptions::new();
    options.read(true).write(true).create(true).truncate(false);
    #[cfg(unix)]
    options
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK);
    Ok(options.open(path)?)
}

pub(crate) fn validate_lock(path: &Path, file: &File) -> Result<()> {
    let opened = file.metadata()?;
    let named = fs::symlink_metadata(path)?;
    if !opened.is_file() || !named.is_file() {
        return Err(Error::Invalid("lock must be a regular file"));
    }
    #[cfg(unix)]
    if opened.dev() != named.dev() || opened.ino() != named.ino() {
        return Err(Error::Invalid("lock changed during startup"));
    }
    Ok(())
}

pub fn sync_directory(path: &Path) -> Result<()> {
    #[cfg(unix)]
    File::open(path)?.sync_all()?;
    #[cfg(not(unix))]
    let _ = path;
    Ok(())
}

pub fn read_regular(path: &Path, max: u64, private: bool) -> Result<Vec<u8>> {
    let before = fs::symlink_metadata(path)?;
    if !before.is_file() {
        return Err(Error::Invalid("expected regular file"));
    }
    #[cfg(unix)]
    if private && before.permissions().mode() & 0o077 != 0 {
        return Err(Error::Invalid("private file permissions"));
    }
    let mut opts = OpenOptions::new();
    opts.read(true);
    #[cfg(unix)]
    opts.custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK);
    let file = opts.open(path)?;
    let after = file.metadata()?;
    #[cfg(unix)]
    if before.dev() != after.dev() || before.ino() != after.ino() {
        return Err(Error::Invalid("file changed while opening"));
    }
    if !after.is_file() || after.len() > max {
        return Err(Error::Invalid("expected regular file"));
    }
    #[cfg(unix)]
    if private && after.permissions().mode() & 0o077 != 0 {
        return Err(Error::Invalid("private file permissions"));
    }
    let mut out = Vec::new();
    file.take(max + 1).read_to_end(&mut out)?;
    if out.len() as u64 > max {
        return Err(Error::Invalid("private file too large"));
    }
    Ok(out)
}

pub fn publish_atomic(path: &Path, bytes: &[u8], replace: bool) -> Result<()> {
    #[cfg(windows)]
    if replace {
        let mut file = atomic_write_file::AtomicWriteFile::open(path)?;
        file.write_all(bytes)?;
        file.commit()?;
        return Ok(());
    }
    let parent = path
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    let temp = parent.join(format!(
        ".g2pool-secure-{}-{}",
        std::process::id(),
        rand::random::<u64>()
    ));
    let mut opts = OpenOptions::new();
    opts.write(true).create_new(true);
    #[cfg(unix)]
    opts.mode(0o600).custom_flags(libc::O_NOFOLLOW);
    let result = (|| {
        let mut f = opts.open(&temp)?;
        f.write_all(bytes)?;
        f.sync_all()?;
        if replace {
            fs::rename(&temp, path)?;
        } else {
            fs::hard_link(&temp, path)?;
            fs::remove_file(&temp)?;
        }
        sync_directory(parent)?;
        Ok(())
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temp);
    }
    result
}
