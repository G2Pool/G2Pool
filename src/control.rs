use crate::Result;
use serde::{Deserialize, Serialize};
use std::{
    fs,
    io::{BufRead, BufReader, Write},
    net::{TcpListener, TcpStream},
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};
pub const SOCKET_FILE: &str = "g2pool.control";
const MAX_FRAME: usize = 6 * crate::wallet::payments::MAX_SLATEPACK_BYTES + 1024;
const MAX_CONNECTIONS: usize = 64;
#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Request {
    pub token: String,
    pub command: Command,
}
#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "command", rename_all = "snake_case", deny_unknown_fields)]
pub enum Command {
    Status,
    Withdraw { amount: Option<u64> },
    WithdrawalReply { slatepack: String },
    CancelWithdrawal,
}
#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Response {
    pub result: ResponseResult,
}
#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case", deny_unknown_fields)]
pub enum ResponseResult {
    Ok { value: serde_json::Value },
    Err { code: String, message: String },
}
#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Endpoint {
    address: String,
    token: String,
}
pub fn socket_path(data: &Path) -> PathBuf {
    data.join(SOCKET_FILE)
}
pub struct Listener {
    listener: TcpListener,
    path: PathBuf,
    token: String,
}
impl Listener {
    pub fn bind(data: &Path) -> Result<Self> {
        let path = socket_path(data);
        let listener = TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0))?;
        listener.set_nonblocking(true)?;
        let token = crate::objects::hex(&rand::random::<[u8; 32]>());
        let endpoint = Endpoint {
            address: listener.local_addr()?.to_string(),
            token: token.clone(),
        };
        let bytes = serde_json::to_vec(&endpoint)?;
        crate::secure_file::publish_atomic(&path, &bytes, true)?;
        Ok(Self {
            listener,
            path,
            token,
        })
    }
    fn accept(&self) -> std::io::Result<TcpStream> {
        self.listener.accept().map(|(s, _)| s)
    }
}
impl Drop for Listener {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
    }
}
fn write_response(stream: &mut TcpStream, response: Response) -> Result<()> {
    let bytes = serde_json::to_vec(&response)?;
    if bytes.len() > MAX_FRAME {
        return Err(crate::Error::Capacity("control response"));
    }
    stream.write_all(&bytes)?;
    stream.write_all(b"\n")?;
    stream.flush()?;
    Ok(())
}
fn read_frame(reader: &mut impl BufRead, deadline: Option<Instant>) -> Result<Vec<u8>> {
    let mut line = Vec::new();
    loop {
        let buffer = reader.fill_buf()?;
        if deadline.is_some_and(|deadline| Instant::now() >= deadline) {
            return Err(std::io::Error::from(std::io::ErrorKind::TimedOut).into());
        }
        if buffer.is_empty() {
            break;
        }
        let newline = buffer.iter().position(|byte| *byte == b'\n');
        let count = newline.unwrap_or(buffer.len());
        if line.len().saturating_add(count) > MAX_FRAME {
            return Err(crate::Error::Invalid("control frame"));
        }
        line.extend_from_slice(&buffer[..count]);
        reader.consume(count + usize::from(newline.is_some()));
        if newline.is_some() {
            break;
        }
    }
    Ok(line)
}
pub fn serve_one(
    mut stream: TcpStream,
    token: &str,
    handler: impl Fn(Request) -> Response,
) -> Result<()> {
    stream.set_read_timeout(Some(std::time::Duration::from_secs(5)))?;
    stream.set_write_timeout(Some(std::time::Duration::from_secs(5)))?;
    let mut reader = BufReader::new(stream.try_clone()?);
    let line = read_frame(&mut reader, Some(Instant::now() + Duration::from_secs(5)))?;
    let request: Request = serde_json::from_slice(&line)?;
    if request.token != token {
        return write_response(
            &mut stream,
            Response {
                result: ResponseResult::Err {
                    code: "auth".into(),
                    message: "invalid control token".into(),
                },
            },
        );
    }
    write_response(&mut stream, handler(request))
}
pub fn serve(
    listener: Listener,
    stop: Arc<AtomicBool>,
    handler: impl Fn(Request) -> Response + Send + Sync + 'static,
) -> Result<()> {
    let handler = Arc::new(handler);
    struct Clients {
        stop: Arc<AtomicBool>,
        workers: Vec<(TcpStream, std::thread::JoinHandle<()>)>,
    }
    impl Drop for Clients {
        fn drop(&mut self) {
            self.stop.store(true, Ordering::Release);
            for (stream, _) in &self.workers {
                let _ = stream.shutdown(std::net::Shutdown::Both);
            }
            for (_, worker) in self.workers.drain(..) {
                let _ = worker.join();
            }
        }
    }
    let mut clients = Clients {
        stop: stop.clone(),
        workers: Vec::new(),
    };
    while !stop.load(Ordering::Acquire) {
        match listener.accept() {
            Ok(stream) => {
                let mut index = 0;
                while index < clients.workers.len() {
                    if clients.workers[index].1.is_finished() {
                        let (_, worker) = clients.workers.swap_remove(index);
                        worker
                            .join()
                            .map_err(|_| crate::Error::Invalid("control handler panicked"))?;
                    } else {
                        index += 1;
                    }
                }
                if clients.workers.len() >= MAX_CONNECTIONS {
                    continue;
                }
                let shutdown = stream.try_clone()?;
                let h = handler.clone();
                let token = listener.token.clone();
                let worker = std::thread::spawn(move || {
                    let _ = serve_one(stream, &token, |r| h(r));
                });
                clients.workers.push((shutdown, worker));
            }
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                std::thread::sleep(std::time::Duration::from_millis(25))
            }
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(error) => return Err(error.into()),
        }
    }
    Ok(())
}
fn read_endpoint(path: &Path) -> Result<Endpoint> {
    let bytes = crate::secure_file::read_regular(path, 4096, true)?;
    Ok(serde_json::from_slice(&bytes)?)
}
pub fn call(data: &Path, command: Command) -> Result<Response> {
    let endpoint = read_endpoint(&socket_path(data))?;
    let mut stream = TcpStream::connect(&endpoint.address)?;
    let timeout = if matches!(command, Command::Status) {
        Some(std::time::Duration::from_secs(5))
    } else {
        None
    };
    stream.set_read_timeout(timeout)?;
    stream.set_write_timeout(Some(std::time::Duration::from_secs(5)))?;
    let request = Request {
        token: endpoint.token,
        command,
    };
    let bytes = serde_json::to_vec(&request)?;
    if bytes.len() > MAX_FRAME {
        return Err(crate::Error::Capacity("control request"));
    }
    stream.write_all(&bytes)?;
    stream.write_all(b"\n")?;
    stream.flush()?;
    let line = read_frame(
        &mut BufReader::new(stream),
        timeout.map(|timeout| Instant::now() + timeout),
    )?;
    let response: Response = serde_json::from_slice(&line)?;
    Ok(response)
}

pub struct Service {
    status: crate::status::runtime::Status,
    payments: Arc<crate::wallet::payments::PaymentWorker>,
}
impl Service {
    pub fn new(
        status: crate::status::runtime::Status,
        payments: Arc<crate::wallet::payments::PaymentWorker>,
    ) -> Self {
        Self { status, payments }
    }
    pub fn handle(&self, request: Request) -> Response {
        let result = match request.command {
            Command::Status => {
                let node = self.status.snapshot();
                let wallet = self.payments.snapshot();
                match (node, wallet) {
                    (Ok(node), Ok(wallet)) => ResponseResult::Ok {
                        value: serde_json::json!({"node": node, "wallet": wallet}),
                    },
                    (Err(error), _) => Self::error("status_unavailable", error),
                    (_, Err(error)) => Self::error("status_unavailable", error),
                }
            }
            command => {
                use crate::wallet::payments::WorkerAction;
                let action = match command {
                    Command::Withdraw { amount: None } => WorkerAction::Resume,
                    Command::Withdraw {
                        amount: Some(amount),
                    } => WorkerAction::Offer(amount),
                    Command::WithdrawalReply { slatepack } => WorkerAction::Finalize(slatepack),
                    Command::CancelWithdrawal => WorkerAction::Cancel,
                    Command::Status => unreachable!(),
                };
                match self.payments.call(action) {
                    Ok(value) => ResponseResult::Ok { value },
                    Err(error) => Self::error("withdrawal", error),
                }
            }
        };
        Response { result }
    }
    fn error(code: &str, error: impl std::fmt::Display) -> ResponseResult {
        ResponseResult::Err {
            code: code.into(),
            message: error.to_string(),
        }
    }
}

#[cfg(test)]
#[path = "control_tests.rs"]
mod tests;
