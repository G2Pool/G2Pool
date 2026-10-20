mod retention;
use retention::Leases;
pub use retention::Retention;

use crate::{mining::Template, node::Node, wallet::Signer, Error, Result};
use grin_core::pow::Proof;
use serde::Deserialize;
use serde_json::{json, Value};
use std::{
    io::{BufRead, BufReader, Write},
    net::{TcpListener, TcpStream},
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc, Mutex,
    },
    thread,
    time::{Duration, Instant},
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
    network: u8,
    published: Arc<std::sync::RwLock<crate::node::Published>>,
    pub node: Arc<Mutex<Node>>,
    pub wallet: Arc<Signer>,
    jobs: Mutex<Jobs>,
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
        let network = guard.profile.manifest.network;
        let published = guard.published.clone();
        let solutions = crate::mining::submission::Solutions::new(&guard.store);
        drop(guard);
        Ok(Self {
            native,
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
        Ok(id)
    }
    pub fn job(&self) -> Result<Value> {
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
    let active = Arc::new(AtomicUsize::new(0));
    let mut workers = Vec::new();
    let network = mining.network;
    while !stop.load(Ordering::Acquire) {
        workers.retain(|h: &thread::JoinHandle<()>| !h.is_finished());
        match listener.accept() {
            Ok((stream, _)) => {
                if active.fetch_add(1, Ordering::AcqRel) >= max_connections {
                    active.fetch_sub(1, Ordering::AcqRel);
                    continue;
                }
                let mining = mining.clone();
                let stop = stop.clone();
                let active = active.clone();
                workers.push(thread::spawn(move || {
                    struct Release(Arc<AtomicUsize>);
                    impl Drop for Release {
                        fn drop(&mut self) {
                            self.0.fetch_sub(1, Ordering::AcqRel);
                        }
                    }
                    let _release = Release(active);
                    grin_core::global::set_local_chain_type(match network {
                        0 => grin_core::global::ChainTypes::Mainnet,
                        1 => grin_core::global::ChainTypes::Testnet,
                        2 => grin_core::global::ChainTypes::AutomatedTesting,
                        _ => grin_core::global::ChainTypes::UserTesting,
                    });
                    let _ = connection(stream, &mining, &stop);
                }));
            }
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                thread::sleep(Duration::from_millis(25))
            }
            Err(e) => return Err(e.into()),
        }
    }
    for worker in workers {
        let _ = worker.join();
    }
    Ok(())
}
fn connection(mut stream: TcpStream, mining: &Mining, stop: &AtomicBool) -> Result<()> {
    stream.set_read_timeout(Some(Duration::from_secs(5)))?;
    stream.set_write_timeout(Some(Duration::from_secs(5)))?;
    let mut reader = BufReader::new(stream.try_clone()?);
    let mut accepted = 0;
    let mut rejected = 0;
    let mut last_job = None;
    while !stop.load(Ordering::Acquire) {
        if let Ok(job) = mining.job() {
            let id = job["job_id"].as_u64();
            if id != last_job {
                serde_json::to_writer(
                    &mut stream,
                    &json!({"id":"Stratum","jsonrpc":"2.0","method":"job","params":job}),
                )?;
                stream.write_all(b"\n")?;
                last_job = id;
            }
        }
        let Some(line) = read_line(&mut reader, stop, Instant::now() + Duration::from_secs(5))?
        else {
            return Ok(());
        };
        if line.is_empty() {
            continue;
        }
        let request: Value = serde_json::from_slice(&line)?;
        let id = request.get("id").cloned().unwrap_or(Value::Null);
        let method = request.get("method").cloned().unwrap_or(Value::Null);
        let response = match mining.handle(&request) {
            Ok(result) => {
                accepted += 1;
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
                rejected += 1;
                json!({"jsonrpc":"2.0","id":id,"method":method,"result":null,"error":{"code":-32000,"message":error.to_string()}})
            }
        };
        if rejected > 128 || accepted > 10000 {
            return Err(Error::Capacity("Stratum requests"));
        }
        serde_json::to_writer(&mut stream, &response)?;
        stream.write_all(b"\n")?;
        stream.flush()?;
    }
    Ok(())
}

fn read_line(
    reader: &mut BufReader<TcpStream>,
    stop: &AtomicBool,
    deadline: Instant,
) -> Result<Option<Vec<u8>>> {
    let mut line = Vec::new();
    loop {
        if stop.load(Ordering::Acquire) {
            return Ok(None);
        }
        let remaining = deadline
            .checked_duration_since(Instant::now())
            .filter(|remaining| !remaining.is_zero());
        let Some(remaining) = remaining else {
            return if line.is_empty() {
                Ok(Some(line))
            } else {
                Err(Error::Capacity("partial Stratum line"))
            };
        };
        reader.get_mut().set_read_timeout(Some(remaining))?;
        match reader.fill_buf() {
            Ok([]) => return Ok(None),
            Ok(bytes) => {
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
            Err(error)
                if matches!(
                    error.kind(),
                    std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
                ) =>
            {
                return if line.is_empty() {
                    Ok(Some(line))
                } else {
                    Err(Error::Capacity("partial Stratum line"))
                };
            }
            Err(error) => return Err(error.into()),
        }
    }
}

#[cfg(test)]
#[path = "mod_tests.rs"]
mod tests;
