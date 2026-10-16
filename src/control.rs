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
};
pub const VERSION: u16 = 1;
pub const SOCKET_FILE: &str = "g2pool.control";
const MAX_FRAME: usize = 1 << 20;
const MAX_CONNECTIONS: usize = 64;
#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Request {
    pub version: u16,
    pub id: u64,
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
    pub version: u16,
    pub id: u64,
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
    _lock: fs::File,
}
impl Listener {
    pub fn bind(data: &Path) -> Result<Self> {
        let lock = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(data.join("g2pool.control.lock"))?;
        fs2::FileExt::try_lock_exclusive(&lock)
            .map_err(|_| crate::Error::Capacity("control service already running"))?;
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
            _lock: lock,
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
fn read_frame(reader: &mut impl BufRead) -> Result<Vec<u8>> {
    let mut line = Vec::new();
    loop {
        let buffer = reader.fill_buf()?;
        if buffer.is_empty() {
            break;
        }
        let newline = buffer.iter().position(|byte| *byte == b'\n');
        let count = newline.map_or(buffer.len(), |index| index + 1);
        if line.len().saturating_add(count) > MAX_FRAME {
            return Err(crate::Error::Invalid("control frame"));
        }
        line.extend_from_slice(&buffer[..count]);
        reader.consume(count);
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
    let line = read_frame(&mut reader)?;
    let request: Request = serde_json::from_slice(&line)?;
    if request.version != VERSION {
        return write_response(
            &mut stream,
            Response {
                version: VERSION,
                id: request.id,
                result: ResponseResult::Err {
                    code: "version".into(),
                    message: "unsupported control protocol version".into(),
                },
            },
        );
    }
    if request.token != token {
        return write_response(
            &mut stream,
            Response {
                version: VERSION,
                id: request.id,
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
    let active = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    while !stop.load(Ordering::Acquire) {
        match listener.accept() {
            Ok(stream) => {
                if active.fetch_add(1, Ordering::AcqRel) >= MAX_CONNECTIONS {
                    active.fetch_sub(1, Ordering::AcqRel);
                    continue;
                }
                let h = handler.clone();
                let token = listener.token.clone();
                let active = active.clone();
                let _ = std::thread::spawn(move || {
                    struct Release(Arc<std::sync::atomic::AtomicUsize>);
                    impl Drop for Release {
                        fn drop(&mut self) {
                            self.0.fetch_sub(1, Ordering::AcqRel);
                        }
                    }
                    let _release = Release(active);
                    let _ = serve_one(stream, &token, |r| h(r));
                });
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
    static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
    let endpoint = read_endpoint(&socket_path(data))?;
    let mut stream = TcpStream::connect(&endpoint.address)?;
    stream.set_read_timeout(Some(std::time::Duration::from_secs(5)))?;
    stream.set_write_timeout(Some(std::time::Duration::from_secs(5)))?;
    let request = Request {
        version: VERSION,
        id: NEXT.fetch_add(1, Ordering::Relaxed),
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
    let line = read_frame(&mut BufReader::new(stream))?;
    let response: Response = serde_json::from_slice(&line)?;
    if response.version != VERSION || response.id != request.id {
        return Err(crate::Error::Invalid("control response identity"));
    }
    Ok(response)
}

pub struct Service {
    data: PathBuf,
    payments: std::sync::Mutex<Arc<crate::wallet::payments::DaemonPayments>>,
}
impl Service {
    pub fn new(data: PathBuf, payments: Arc<crate::wallet::payments::DaemonPayments>) -> Self {
        Self {
            data,
            payments: std::sync::Mutex::new(payments),
        }
    }
    pub fn handle(&self, request: Request) -> Response {
        let result = match request.command {
            Command::Status => {
                let node = crate::status::runtime::read(&self.data);
                let wallet = crate::status::wallet::read_current_snapshot(
                    &self.data.join("wallet-status.json"),
                );
                match (node, wallet) {
                    (Ok(node), Ok(mut wallet)) => {
                        wallet.native_height = node.native_height;
                        wallet.native_head = node.native_head.clone();
                        wallet.native_pmmr_size = node.native_pmmr_size;
                        ResponseResult::Ok {
                            value: serde_json::json!({"node": node, "wallet": wallet}),
                        }
                    }
                    (Err(error), _) | (_, Err(error)) => Self::error("status_unavailable", error),
                }
            }
            Command::Withdraw { amount: None } => {
                self.payment(|p| Ok(serde_json::to_value(p.balance()?)?))
            }
            Command::Withdraw {
                amount: Some(amount),
            } => self.payment(|p| Ok(serde_json::json!({"slatepack": p.offer(amount)?}))),
            Command::WithdrawalReply { slatepack } => self.payment(|p| {
                p.finalize(&slatepack, &AtomicBool::new(false))?;
                Ok(serde_json::json!({"completed":true}))
            }),
            Command::CancelWithdrawal => self.payment(|p| {
                p.cancel()?;
                Ok(serde_json::json!({"cancelled":true}))
            }),
        };
        Response {
            version: VERSION,
            id: request.id,
            result,
        }
    }
    fn payment(
        &self,
        operation: impl FnOnce(
            &crate::wallet::payments::DaemonPayments,
        ) -> anyhow::Result<serde_json::Value>,
    ) -> ResponseResult {
        match self.payments.lock() {
            Ok(payments) => operation(&payments)
                .map(|value| ResponseResult::Ok { value })
                .unwrap_or_else(|error| Self::error("wallet_error", error)),
            Err(_) => Self::error("wallet_error", "wallet service lock poisoned"),
        }
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
