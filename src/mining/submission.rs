use crate::{
    codec::{self, Id},
    mining::Template,
    native,
    node::Node,
    objects::{hex, Key, Kind},
    storage::Object,
    wallet::Wallet,
    Error, Result,
};
use grin_core::{
    core::{hash::Hashed, Block},
    pow::Proof,
};
#[cfg(unix)]
use std::os::unix::fs::OpenOptionsExt;
use std::{
    fs::{self, OpenOptions},
    io::{Read, Write},
    path::{Path, PathBuf},
    sync::Mutex,
};
const MAGIC: &[u8; 4] = b"G2SP";
const MAX_RECORD: usize = 64 * 1024 * 1024;
const MAX_BYTES: u64 = 512 * 1024 * 1024;
const MAX_RECORDS: usize = 1024;

pub struct Solutions {
    directory: PathBuf,
    protocol: Id,
    writes: Mutex<()>,
    _lease: fs::File,
}
struct Intent {
    id: Id,
    block: Block,
    objects: Vec<Object>,
}
impl Solutions {
    pub fn open(pool: &Path, protocol: Id) -> Result<Self> {
        let directory = pool.with_extension("solutions");
        fs::create_dir_all(&directory)?;
        let mut options = OpenOptions::new();
        options.read(true).write(true).create(true).truncate(false);
        #[cfg(unix)]
        options.mode(0o600);
        let lease = options.open(directory.join(".lock"))?;
        fs2::FileExt::try_lock_exclusive(&lease)?;
        for entry in fs::read_dir(&directory)? {
            let entry = entry?;
            if entry.path().extension().is_some_and(|ext| ext == "tmp") {
                fs::remove_file(entry.path())?;
            }
        }
        crate::secure_file::sync_directory(&directory)?;
        Ok(Self {
            directory,
            protocol,
            writes: Mutex::new(()),
            _lease: lease,
        })
    }
    pub fn save(
        &self,
        template: &Template,
        wallet: &Wallet,
        nonce: u64,
        proof: Proof,
    ) -> Result<Id> {
        let mut header = template.block.header.clone();
        header.pow.nonce = nonce;
        header.pow.proof = proof;
        grin_core::pow::verify_size(&header).map_err(|_| Error::Invalid("submitted cycle"))?;
        if template.job.protocol != self.protocol || wallet.account() != template.job.account {
            return Err(Error::Invalid("solution signer context"));
        }
        let header_bytes = native::native_bytes(&header)?;
        let id = codec::hash("WORK_ID", &header_bytes);
        let _lease = self
            .writes
            .lock()
            .map_err(|_| Error::Invalid("solution journal poisoned"))?;
        let final_path = self.directory.join(format!("{}.intent", hex(&id)));
        if final_path.exists() {
            return Ok(id);
        }
        let mut block = template.block.clone();
        block.header = header;
        let core = codec::Core {
            header: codec::Blob(header_bytes),
            job: codec::Blob(codec::encode(&template.job)?),
        };
        let mut objects = template.material.clone();
        objects.extend([
            Object::new(Key::new(Kind::Core, id), codec::encode(&core)?)?,
            Object::new(
                Key::new(Kind::Authorization, id),
                codec::encode(&wallet.authorize(self.protocol, id)?)?,
            )?,
            Object::new(
                Key::new(Kind::Residual, id),
                codec::encode(&template.residual)?,
            )?,
        ]);
        let mut bytes = Vec::new();
        bytes.extend(MAGIC);
        bytes.extend(self.protocol);
        bytes.extend(id);
        bytes.extend(block.header.height.to_be_bytes());
        let block_bytes = native::native_bytes(&block)?;
        append(&mut bytes, &block_bytes)?;
        bytes.extend((objects.len() as u32).to_be_bytes());
        for object in objects {
            bytes.push(object.key.kind as u8);
            bytes.extend(object.key.id);
            append(&mut bytes, &object.bytes)?;
        }
        let digest = codec::hash("NATIVE_INTENT", &bytes);
        bytes.extend(digest);
        if bytes.len() > MAX_RECORD {
            return Err(Error::Capacity("solution intent"));
        }
        let mut count = 0;
        let mut used = 0u64;
        for entry in fs::read_dir(&self.directory)? {
            let entry = entry?;
            if entry.path().extension().is_some_and(|ext| ext == "intent") {
                count += 1;
                used = used.saturating_add(entry.metadata()?.len());
            }
        }
        if count >= MAX_RECORDS || used.saturating_add(bytes.len() as u64) > MAX_BYTES {
            return Err(Error::Capacity("solution journal"));
        }
        let temporary = self
            .directory
            .join(format!("{}.{}.tmp", hex(&id), rand::random::<u64>()));
        let mut options = OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        options.mode(0o600);
        let mut file = options.open(&temporary)?;
        file.write_all(&bytes)?;
        file.sync_all()?;
        let publish = fs::hard_link(&temporary, &final_path);
        fs::remove_file(&temporary)?;
        if let Err(error) = publish {
            if error.kind() != std::io::ErrorKind::AlreadyExists {
                return Err(error.into());
            }
        }
        crate::secure_file::sync_directory(&self.directory)?;
        Ok(id)
    }
    pub fn done(&self, id: Id) -> Result<()> {
        match fs::remove_file(self.directory.join(format!("{}.intent", hex(&id)))) {
            Ok(()) => crate::secure_file::sync_directory(&self.directory),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(error.into()),
        }
    }
    fn pending(&self) -> Result<Vec<(u64, PathBuf)>> {
        let mut pending = Vec::new();
        let mut used = 0u64;
        for entry in fs::read_dir(&self.directory)? {
            let entry = entry?;
            let path = entry.path();
            if path.extension().is_none_or(|ext| ext != "intent") {
                continue;
            }
            if !entry.file_type()?.is_file() {
                return Err(Error::Invalid("solution intent file"));
            }
            let length = entry.metadata()?.len();
            if length > MAX_RECORD as u64 {
                return Err(Error::Capacity("solution intent"));
            }
            used = used.saturating_add(length);
            if pending.len() >= MAX_RECORDS || used > MAX_BYTES {
                return Err(Error::Capacity("solution journal"));
            }
            let mut prefix = [0; 76];
            fs::File::open(&path)?.read_exact(&mut prefix)?;
            pending.push((u64::from_be_bytes(prefix[68..76].try_into().unwrap()), path));
        }
        pending.sort();
        Ok(pending)
    }
    pub fn replay(&self, node: &mut Node) -> Result<usize> {
        let mut recovered = 0;
        for (_, path) in self.pending()? {
            let intent = self.decode(&fs::read(&path)?)?;
            if path.file_stem().and_then(|stem| stem.to_str()) != Some(hex(&intent.id).as_str()) {
                return Err(Error::Invalid("solution filename"));
            }
            let known = node.chain.get_block(&intent.block.hash()).is_ok()
                || node
                    .chain
                    .get_header_by_height(intent.block.header.height)
                    .is_ok_and(|header| header.hash() == intent.block.hash());
            if !known {
                if let Err(error) = node
                    .chain
                    .process_block(intent.block, grin_chain::Options::MINE)
                {
                    eprintln!("native solution replay retryable: {error}");
                    continue;
                }
            }
            node.ensure_view()?;
            let outcome = node
                .stage(&intent.objects)
                .and_then(|_| node.consider(intent.id));
            match outcome {
                Ok(_) | Err(Error::Expired) => {
                    self.done(intent.id)?;
                    recovered += 1;
                }
                Err(error) => {
                    eprintln!("native solution preserved; pool recovery retryable: {error}")
                }
            }
        }
        Ok(recovered)
    }
    fn decode(&self, bytes: &[u8]) -> Result<Intent> {
        if bytes.len() < 112 || bytes.len() > MAX_RECORD {
            return Err(Error::Invalid("solution framing"));
        }
        let (payload, digest) = bytes.split_at(bytes.len() - 32);
        if codec::hash("NATIVE_INTENT", payload).as_slice() != digest {
            return Err(Error::Invalid("solution checksum"));
        }
        let mut input = payload;
        if take(&mut input, 4)? != MAGIC || take(&mut input, 32)? != self.protocol {
            return Err(Error::Invalid("solution protocol"));
        }
        let id = take(&mut input, 32)?.try_into().unwrap();
        let height = u64::from_be_bytes(take(&mut input, 8)?.try_into().unwrap());
        let block: Block = native::parse(blob(&mut input)?)?;
        if block.header.height != height {
            return Err(Error::Invalid("solution height"));
        }
        if codec::hash("WORK_ID", &native::native_bytes(&block.header)?) != id {
            return Err(Error::Invalid("solution identity"));
        }
        let count = u32::from_be_bytes(take(&mut input, 4)?.try_into().unwrap());
        if count > 32 {
            return Err(Error::Invalid("solution objects"));
        }
        let mut objects = Vec::new();
        for _ in 0..count {
            let kind = Kind::from_byte(take(&mut input, 1)?[0])?;
            let key = Key::new(kind, take(&mut input, 32)?.try_into().unwrap());
            objects.push(Object::new(key, blob(&mut input)?.to_vec())?);
        }
        if !input.is_empty() {
            return Err(Error::Invalid("solution trailing bytes"));
        }
        Ok(Intent { id, block, objects })
    }
}
fn append(out: &mut Vec<u8>, bytes: &[u8]) -> Result<()> {
    if out.len().saturating_add(bytes.len()).saturating_add(4) > MAX_RECORD {
        return Err(Error::Capacity("solution intent"));
    }
    out.extend((bytes.len() as u32).to_be_bytes());
    out.extend(bytes);
    Ok(())
}
fn take<'a>(input: &mut &'a [u8], len: usize) -> Result<&'a [u8]> {
    if input.len() < len {
        return Err(Error::Invalid("solution truncation"));
    }
    let (value, rest) = input.split_at(len);
    *input = rest;
    Ok(value)
}
fn blob<'a>(input: &mut &'a [u8]) -> Result<&'a [u8]> {
    let len = u32::from_be_bytes(take(input, 4)?.try_into().unwrap()) as usize;
    take(input, len)
}

pub(crate) struct Journal {
    send: Option<std::sync::mpsc::SyncSender<JournalCommand>>,
    worker: Option<std::thread::JoinHandle<()>>,
}
enum JournalCommand {
    Save {
        template: std::sync::Arc<Template>,
        wallet: std::sync::Arc<Wallet>,
        nonce: u64,
        proof: Proof,
        result: std::sync::mpsc::SyncSender<Result<Id>>,
    },
    Done(Id),
}
impl Journal {
    pub(crate) fn new(
        solutions: std::sync::Arc<Solutions>,
        kind: grin_core::global::ChainTypes,
    ) -> Self {
        let (send, receive) = std::sync::mpsc::sync_channel(1);
        let worker = std::thread::spawn(move || {
            grin_core::global::set_local_chain_type(kind);
            while let Ok(command) = receive.recv() {
                match command {
                    JournalCommand::Save {
                        template,
                        wallet,
                        nonce,
                        proof,
                        result,
                    } => {
                        let saved = solutions.save(&template, &wallet, nonce, proof);
                        let _ = result.try_send(saved);
                    }
                    JournalCommand::Done(id) => {
                        if let Err(error) = solutions.done(id) {
                            eprintln!("solution cleanup retryable: {error}");
                        }
                    }
                }
            }
        });
        Self {
            send: Some(send),
            worker: Some(worker),
        }
    }
    pub(crate) fn prepare(
        &self,
        template: std::sync::Arc<Template>,
        wallet: std::sync::Arc<Wallet>,
        nonce: u64,
        proof: Proof,
    ) -> Option<Id> {
        let (result, receive) = std::sync::mpsc::sync_channel(1);
        let command = JournalCommand::Save {
            template,
            wallet,
            nonce,
            proof,
            result,
        };
        if self.send.as_ref()?.try_send(command).is_err() {
            eprintln!("solution journal busy; native relay withheld");
            return None;
        }
        match receive.recv_timeout(std::time::Duration::from_millis(100)) {
            Ok(Ok(id)) => Some(id),
            Ok(Err(error)) => {
                eprintln!("solution intent unavailable; native relay withheld: {error}");
                None
            }
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                eprintln!("solution journal stalled; native relay withheld while intent completes");
                None
            }
            Err(_) => {
                eprintln!("solution journal stopped; native relay withheld");
                None
            }
        }
    }
    pub(crate) fn finish(&self, id: Id) {
        if let Some(send) = &self.send {
            let _ = send.try_send(JournalCommand::Done(id));
        }
    }
}
impl Drop for Journal {
    fn drop(&mut self) {
        self.send.take();
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}

#[cfg(test)]
#[path = "submission_tests.rs"]
mod tests;
