use super::DaemonPayments;
use anyhow::{anyhow, bail, Result};
use grin_core::core::hash::Hashed;
use serde_json::Value;
use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        mpsc, Arc, Mutex, RwLock,
    },
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};

pub enum WorkerAction {
    Resume,
    Offer(u64),
    Finalize(String),
    Cancel,
}
struct Request {
    action: WorkerAction,
    reply: mpsc::SyncSender<Result<Value, String>>,
}
pub struct PaymentWorker {
    send: mpsc::SyncSender<Request>,
    active: Mutex<()>,
    cancelled: Arc<AtomicBool>,
    snapshot: Arc<RwLock<crate::status::wallet::Snapshot>>,
}
impl PaymentWorker {
    pub fn new(
        payments: Arc<DaemonPayments>,
        native: &grin_servers::Server,
        profile: [u8; 32],
        stop: Arc<AtomicBool>,
    ) -> Result<(Arc<Self>, JoinHandle<crate::Result<()>>)> {
        let chain = native.chain.clone();
        let sync = native.sync_state.clone();
        let kind = native.config.chain_type;
        let (send, receive) = mpsc::sync_channel(1);
        let cancelled = Arc::new(AtomicBool::new(false));
        let snapshot = Arc::new(RwLock::new(payments.status_snapshot(
            profile,
            "refreshing",
            None,
        )?));
        let worker = Arc::new(Self {
            send,
            active: Mutex::new(()),
            cancelled: cancelled.clone(),
            snapshot: snapshot.clone(),
        });
        let handle = thread::spawn(move || {
            grin_core::global::set_local_chain_type(kind);
            run(
                payments,
                (chain, sync),
                profile,
                receive,
                cancelled,
                snapshot,
                stop,
            )
            .map_err(|error| crate::Error::NativeUnavailable(format!("payment service: {error}")))
        });
        Ok((worker, handle))
    }
    pub fn call(&self, action: WorkerAction) -> Result<Value> {
        if matches!(action, WorkerAction::Cancel) {
            return self.cancel();
        }
        if self.cancelled.load(Ordering::Acquire) {
            bail!("withdrawal cancellation is in progress");
        }
        let _active = self
            .active
            .try_lock()
            .map_err(|_| anyhow!("withdrawal service is busy"))?;
        if self.cancelled.load(Ordering::Acquire) {
            bail!("withdrawal cancellation is in progress");
        }
        self.exchange(action)
    }
    fn cancel(&self) -> Result<Value> {
        if self.cancelled.swap(true, Ordering::AcqRel) {
            bail!("withdrawal cancellation is already in progress");
        }
        let result = (|| {
            let _active = self
                .active
                .lock()
                .map_err(|_| anyhow!("withdrawal service poisoned"))?;
            self.exchange(WorkerAction::Cancel)
        })();
        self.cancelled.store(false, Ordering::Release);
        result
    }
    fn exchange(&self, action: WorkerAction) -> Result<Value> {
        let (reply, result) = mpsc::sync_channel(1);
        self.send
            .send(Request { action, reply })
            .map_err(|_| anyhow!("payment worker stopped"))?;
        result
            .recv()
            .map_err(|_| anyhow!("payment worker stopped"))?
            .map_err(anyhow::Error::msg)
    }
    pub fn snapshot(&self) -> Result<crate::status::wallet::Snapshot> {
        Ok(self
            .snapshot
            .read()
            .map_err(|_| anyhow!("wallet status poisoned"))?
            .clone())
    }
}
fn run(
    payments: Arc<DaemonPayments>,
    (chain, sync): (Arc<grin_chain::Chain>, Arc<grin_chain::SyncState>),
    profile: [u8; 32],
    receive: mpsc::Receiver<Request>,
    cancelled: Arc<AtomicBool>,
    snapshot: Arc<RwLock<crate::status::wallet::Snapshot>>,
    stop: Arc<AtomicBool>,
) -> Result<()> {
    let mut last_head = None;
    let mut refresh_retry = Instant::now();
    let publish = |state, error| -> Result<()> {
        *snapshot
            .write()
            .map_err(|_| anyhow!("wallet status poisoned"))? =
            payments.status_snapshot(profile, state, error)?;
        Ok(())
    };
    while !stop.load(Ordering::Acquire) {
        let head = chain.head_header().ok().map(|header| header.hash());
        if sync.is_syncing() {
            if last_head.is_some() {
                publish("refreshing", None)?;
            }
            last_head = None;
        } else if head != last_head && Instant::now() >= refresh_retry {
            publish("refreshing", None)?;
            match payments.refresh() {
                Ok(()) if !sync.is_syncing() => {
                    let error = payments
                        .rebroadcast()
                        .err()
                        .map(|error| format!("transaction rebroadcast: {error}"));
                    if !sync.is_syncing() {
                        publish("ready", error)?;
                        last_head = head;
                    }
                }
                Ok(()) => {}
                Err(error) => {
                    publish("unavailable", Some(error.to_string()))?;
                    refresh_retry = Instant::now() + Duration::from_secs(1);
                }
            }
        }
        match receive.recv_timeout(Duration::from_millis(250)) {
            Ok(request) => {
                let result = if sync.is_syncing() && !matches!(request.action, WorkerAction::Cancel)
                {
                    Err(anyhow!("Grin node is synchronizing"))
                } else {
                    match request.action {
                    WorkerAction::Resume => payments.resume(&cancelled),
                    WorkerAction::Offer(amount) => payments.offer(amount).map(|slatepack| {
                        serde_json::json!({"state": "awaiting_response", "amount": amount, "slatepack": slatepack})
                    }),
                    WorkerAction::Finalize(slatepack) => payments.finalize(&slatepack, &cancelled)
                        .map(|_| serde_json::json!({"state": "completed"})),
                    WorkerAction::Cancel => payments.cancel()
                        .map(|cancelled| serde_json::json!({"state": if cancelled { "cancelled" } else { "no_pending_withdrawal" }})),
                }
                };
                publish("refreshing", None)?;
                let _ = request
                    .reply
                    .send(result.map_err(|error| error.to_string()));
                last_head = None;
            }
            Err(mpsc::RecvTimeoutError::Disconnected) => break,
            Err(mpsc::RecvTimeoutError::Timeout) => {}
        }
    }
    Ok(())
}

#[cfg(test)]
#[path = "worker_tests.rs"]
mod tests;
