mod retention;
use retention::Leases;
pub use retention::Retention;

use crate::{mining::Template, node::Node, wallet::Signer, Error, Result};
use grin_core::pow::Proof;
use serde::Deserialize;
use serde_json::{json, Value};
use std::{
    net::TcpListener,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    time::{Duration, Instant},
};
use tokio::{
    io::{AsyncBufRead, AsyncBufReadExt, AsyncWrite, AsyncWriteExt, BufReader},
    net::TcpStream,
    sync::watch,
    task::JoinSet,
};
const LINE: usize = 16 * 1024;
#[derive(Deserialize)]
struct Submission {
    height: u64,
    job_id: u64,
    nonce: u64,
    edge_bits: u8,
    pow: Vec<u64>,
}
impl Submission {
    fn proof(&self, template: &Template) -> Result<Proof> {
        let expected = &template.block.header;
        if self.height != expected.height || self.edge_bits != expected.pow.edge_bits() {
            return Err(Error::Invalid("submit profile"));
        }
        if self.pow.len() != expected.pow.proof.proof_size() {
            return Err(Error::Invalid("proof length"));
        }
        if self.pow.windows(2).any(|pair| pair[0] >= pair[1]) {
            return Err(Error::Invalid("proof order"));
        }
        let limit = 1u64
            .checked_shl(u32::from(self.edge_bits))
            .ok_or(Error::Invalid("proof edge bits"))?;
        if self.pow.iter().any(|nonce| *nonce >= limit) {
            return Err(Error::Invalid("proof nonce range"));
        }
        Ok(Proof {
            edge_bits: self.edge_bits,
            nonces: self.pow.clone(),
        })
    }
}
fn miner_difficulty(assigned: u64, weight: u64) -> Result<u64> {
    if weight == 0 || assigned < weight {
        return Err(Error::Invalid("miner difficulty scale"));
    }
    Ok(assigned / weight)
}
struct Jobs {
    next: u64,
    current: Option<u64>,
    active: std::collections::BTreeMap<u64, std::sync::Weak<Template>>,
    templates: Leases<Arc<Template>>,
}
pub struct Mining {
    native: Arc<grin_chain::Chain>,
    native_sync: Arc<grin_chain::SyncState>,
    network: grin_core::global::ChainTypes,
    published: Arc<std::sync::RwLock<crate::node::Published>>,
    pub node: Arc<Mutex<Node>>,
    pub wallet: Arc<Signer>,
    jobs: Mutex<Jobs>,
    updates: watch::Sender<()>,
    pub(crate) solutions: crate::mining::submission::Solutions,
}
impl Mining {
    pub fn bounded(
        node: Arc<Mutex<Node>>,
        wallet: Arc<Signer>,
        retention: Retention,
    ) -> Result<Self> {
        if retention.duration.is_zero() || retention.bytes == 0 {
            return Err(Error::Invalid("job retention policy must be positive"));
        }
        let guard = node
            .lock()
            .map_err(|_| Error::Invalid("node sequencer poisoned"))?;
        let native = guard.chain.clone();
        let native_sync = guard.native_sync.clone();
        let network = guard.profile.network;
        let published = guard.published.clone();
        let solutions = crate::mining::submission::Solutions::new(&guard.store);
        drop(guard);
        Ok(Self {
            native,
            native_sync,
            network,
            published,
            solutions,
            node,
            wallet,
            jobs: Mutex::new(Jobs {
                next: 0,
                current: None,
                active: Default::default(),
                templates: Leases::new(retention),
            }),
            updates: watch::channel(()).0,
        })
    }
    pub fn publish(&self, template: Template) -> Result<u64> {
        let bytes = template.retained_bytes()?;
        let mut jobs = self
            .jobs
            .lock()
            .map_err(|_| Error::Invalid("job registry poisoned"))?;
        let id = jobs.next;
        let next = id.checked_add(1).ok_or(Error::Capacity("job IDs"))?;
        jobs.templates
            .insert(id, Arc::new(template), bytes, Instant::now())?;
        jobs.next = next;
        jobs.current = Some(id);
        self.updates.send_replace(());
        Ok(id)
    }
    pub fn job(&self) -> Result<Value> {
        if self.native_sync.is_syncing() {
            return Err(Error::NativeUnavailable(
                "Grin node is synchronizing".into(),
            ));
        }
        let (id, template) = {
            let mut jobs = self
                .jobs
                .lock()
                .map_err(|_| Error::Invalid("job registry poisoned"))?;
            jobs.templates.expire(Instant::now());
            let id = jobs
                .current
                .ok_or(Error::Capacity("no prepared mining job"))?;
            (
                id,
                jobs.templates
                    .get(id)
                    .cloned()
                    .ok_or(Error::Capacity("current mining job expired"))?,
            )
        };
        let snapshot = self
            .published
            .read()
            .map_err(|_| Error::Invalid("publication snapshot poisoned"))?
            .clone();
        if snapshot.recovering || snapshot.revision.as_ref() != Some(&template.revision) {
            return Err(Error::View);
        }
        let head = self
            .native
            .head_header()
            .map_err(|e| Error::NativeUnavailable(e.to_string()))?;
        if head.height != template.job.anchor.height
            || crate::native::hash_bytes(grin_core::core::hash::Hashed::hash(&head))
                != template.job.anchor.hash
        {
            return Err(Error::View);
        }
        let weight = grin_core::consensus::graph_weight(
            template.block.header.height,
            template.block.header.pow.proof.edge_bits,
        );
        let difficulty = miner_difficulty(template.job.difficulty, weight)?;
        Ok(
            json!({"height":template.block.header.height,"job_id":id,"difficulty":difficulty,
            "edge_bits":template.block.header.pow.proof.edge_bits,
            "pool_difficulty":template.job.difficulty,"pre_pow":template.pre_pow()}),
        )
    }

    pub fn pins(&self) -> Result<std::collections::BTreeSet<crate::objects::Key>> {
        let mut jobs = self
            .jobs
            .lock()
            .map_err(|_| Error::Invalid("job registry poisoned"))?;
        jobs.templates.expire(Instant::now());
        jobs.active.retain(|_, job| job.strong_count() > 0);
        let mut pins: std::collections::BTreeSet<_> =
            jobs.templates.values().flat_map(|job| job.pins()).collect();
        for job in jobs.active.values().filter_map(std::sync::Weak::upgrade) {
            pins.extend(job.pins());
        }
        Ok(pins)
    }
    pub fn handle(&self, request: &Value) -> Result<Value> {
        match request
            .get("method")
            .and_then(Value::as_str)
            .ok_or(Error::Invalid("Stratum method"))?
        {
            "login" => Ok(json!("ok")),
            "keepalive" => Ok(json!("ok")),
            "getjobtemplate" => self.job(),
            "status" => {
                let job = self.job()?;
                Ok(
                    json!({"id":env!("CARGO_PKG_NAME"),"height":job["height"],"difficulty":job["difficulty"]}),
                )
            }
            "submit" => {
                let params = request
                    .get("params")
                    .ok_or(Error::Invalid("submit params"))?;
                let submission: Submission = serde_json::from_value(params.clone())
                    .map_err(|_| Error::Invalid("submit params"))?;
                let template = {
                    let mut jobs = self
                        .jobs
                        .lock()
                        .map_err(|_| Error::Invalid("job registry poisoned"))?;
                    jobs.templates.expire(Instant::now());
                    let template = jobs
                        .templates
                        .get(submission.job_id)
                        .cloned()
                        .ok_or(Error::Invalid("unknown job"))?;
                    jobs.active.retain(|_, job| job.strong_count() > 0);
                    jobs.active
                        .insert(submission.job_id, Arc::downgrade(&template));
                    template
                };
                let proof = submission.proof(&template)?;
                let mut header = template.block.header.clone();
                header.pow.nonce = submission.nonce;
                header.pow.proof = proof.clone();
                let result = self.solutions.submit(
                    &self.node,
                    &template,
                    &self.wallet,
                    submission.nonce,
                    proof,
                )?;
                if !result.pool_accepted && !result.native_accepted {
                    return Err(Error::Invalid("native solution rejected"));
                }
                Ok(
                    json!({"status":"ok","work_id":crate::objects::hex(&result.work_id),"block":result.native_accepted,"pool":result.pool_accepted,
                        "block_hash":crate::objects::hex(&crate::native::hash_bytes(grin_core::core::hash::Hashed::hash(&header)))}),
                )
            }
            _ => Err(Error::Invalid("unknown Stratum method")),
        }
    }
}
pub fn serve(
    listener: TcpListener,
    mining: Arc<Mining>,
    stop: Arc<AtomicBool>,
    max_connections: usize,
) -> Result<()> {
    listener.set_nonblocking(true)?;
    grin_core::global::set_local_chain_type(mining.network);
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?
        .block_on(async {
            let listener = tokio::net::TcpListener::from_std(listener)?;
            let mut workers = JoinSet::new();
            let mut tick = tokio::time::interval(Duration::from_millis(100));
            let mut result = async {
                while !stop.load(Ordering::Acquire) {
                    tokio::select! {
                        _ = tick.tick() => {},
                        Some(worker) = workers.join_next(), if !workers.is_empty() => {
                            worker.map_err(|_| Error::Invalid("Stratum worker panicked"))?;
                        },
                        accepted = listener.accept() => match accepted {
                            Ok((stream, _)) => {
                                if workers.len() < max_connections {
                                    let mining = mining.clone();
                                    workers.spawn(async move {
                                        let _ = connection(stream, &mining).await;
                                    });
                                }
                            },
                            Err(error) if error.kind() == std::io::ErrorKind::Interrupted => {},
                            Err(error) => return Err(error.into()),
                        },
                    }
                }
                Ok(())
            }
            .await;
            stop.store(true, Ordering::Release);
            workers.abort_all();
            while let Some(worker) = workers.join_next().await {
                if worker.is_err_and(|error| error.is_panic()) && result.is_ok() {
                    result = Err(Error::Invalid("Stratum worker panicked"));
                }
            }
            result
        })
}
async fn write_message(writer: &mut (impl AsyncWrite + Unpin), message: &Value) -> Result<()> {
    let mut bytes = serde_json::to_vec(message)?;
    bytes.push(b'\n');
    tokio::time::timeout(Duration::from_secs(5), writer.write_all(&bytes))
        .await
        .map_err(|_| std::io::Error::from(std::io::ErrorKind::TimedOut))??;
    Ok(())
}
async fn send_job(
    writer: &mut (impl AsyncWrite + Unpin),
    mining: &Mining,
    updates: &mut watch::Receiver<()>,
    last_job: &mut Option<u64>,
) -> Result<()> {
    updates.borrow_and_update();
    if let Ok(job) = mining.job() {
        let id = job["job_id"].as_u64();
        if id != *last_job {
            write_message(
                writer,
                &json!({"id":"Stratum","jsonrpc":"2.0","method":"job","params":job}),
            )
            .await?;
            *last_job = id;
        }
    }
    Ok(())
}
async fn connection(stream: TcpStream, mining: &Arc<Mining>) -> Result<()> {
    stream.set_nodelay(true)?;
    let mut updates = mining.updates.subscribe();
    let (reader, mut writer) = stream.into_split();
    let mut reader = BufReader::new(reader);
    let mut last_job = None;
    loop {
        let read = read_line(
            &mut reader,
            tokio::time::Instant::now() + Duration::from_secs(5),
        );
        tokio::pin!(read);
        let line = loop {
            send_job(&mut writer, mining, &mut updates, &mut last_job).await?;
            tokio::select! {
                line = &mut read => break line?,
                _ = updates.changed() => {},
            }
        };
        let Some(line) = line else {
            return Ok(());
        };
        if line.is_empty() {
            continue;
        }
        let request: Value = serde_json::from_slice(&line)?;
        let id = request.get("id").cloned().unwrap_or(Value::Null);
        let method = request.get("method").cloned().unwrap_or(Value::Null);
        let result = if method == "submit" {
            let service = mining.clone();
            let mut submission = tokio::task::spawn_blocking(move || {
                grin_core::global::set_local_chain_type(service.network);
                service.handle(&request)
            });
            loop {
                tokio::select! {
                    result = &mut submission => break result
                        .unwrap_or_else(|error| std::panic::resume_unwind(error.into_panic())),
                    _ = updates.changed() => {
                        send_job(&mut writer, mining, &mut updates, &mut last_job).await?;
                    },
                }
            }
        } else {
            mining.handle(&request)
        };
        let response = match result {
            Ok(result) => {
                if method == "submit" {
                    let wire_result = if result["block"] == true {
                        format!(
                            "blockfound - {}",
                            result["block_hash"].as_str().unwrap_or_default()
                        )
                    } else {
                        "ok".to_owned()
                    };
                    json!({"jsonrpc":"2.0","id":id,"method":method,"result":wire_result,"error":null,"g2pool":result})
                } else {
                    json!({"jsonrpc":"2.0","id":id,"method":method,"result":result,"error":null})
                }
            }
            Err(error) => {
                json!({"jsonrpc":"2.0","id":id,"method":method,"result":null,"error":{"code":-32000,"message":error.to_string()}})
            }
        };
        write_message(&mut writer, &response).await?;
    }
}

async fn read_line(
    reader: &mut (impl AsyncBufRead + Unpin),
    deadline: tokio::time::Instant,
) -> Result<Option<Vec<u8>>> {
    let mut line = Vec::new();
    loop {
        if tokio::time::Instant::now() >= deadline {
            break;
        }
        match tokio::time::timeout_at(deadline, reader.fill_buf()).await {
            Ok(Ok([])) => return Ok(None),
            Ok(Ok(bytes)) => {
                let take = bytes
                    .iter()
                    .position(|byte| *byte == b'\n')
                    .map_or(bytes.len(), |position| position + 1);
                if line.len() + take > LINE {
                    return Err(Error::Capacity("Stratum line"));
                }
                let done = bytes[take - 1] == b'\n';
                line.extend_from_slice(&bytes[..take]);
                reader.consume(take);
                if done {
                    return Ok(Some(line));
                }
            }
            Err(_) => break,
            Ok(Err(error)) => return Err(error.into()),
        }
    }
    if line.is_empty() {
        Ok(Some(line))
    } else {
        Err(Error::Capacity("partial Stratum line"))
    }
}

#[cfg(test)]
#[path = "mod_tests.rs"]
mod tests;
