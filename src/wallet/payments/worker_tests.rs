use super::*;

#[test]
fn busy_requests_are_rejected_and_cancellation_waits_for_its_own_result() {
    let (send, receive) = mpsc::sync_channel::<Request>(1);
    let snapshot =
        crate::status::wallet::snapshot(&[0; 33], &[0; 32], None, 0, "refreshing", None, None);
    let worker = Arc::new(PaymentWorker {
        send,
        active: Mutex::new(()),
        cancelling: AtomicBool::new(false),
        cancelled: Arc::new(AtomicBool::new(false)),
        snapshot: Arc::new(RwLock::new(snapshot)),
    });
    let (started, running) = mpsc::sync_channel(1);
    let (release, released) = mpsc::sync_channel(1);
    let cancelled = worker.cancelled.clone();
    let actor = thread::spawn(move || {
        let first = receive.recv().unwrap();
        assert!(matches!(first.action, WorkerAction::Offer(10)));
        started.send(()).unwrap();
        released.recv().unwrap();
        assert!(cancelled.load(Ordering::Acquire));
        first
            .reply
            .send(Ok(serde_json::json!({"amount": 10})))
            .unwrap();
        let cancel = receive.recv().unwrap();
        assert!(matches!(cancel.action, WorkerAction::Cancel));
        cancel
            .reply
            .send(Ok(serde_json::json!({"state": "cancelled"})))
            .unwrap();
    });
    let first = worker.clone();
    let caller = thread::spawn(move || first.call(WorkerAction::Offer(10)));
    running.recv_timeout(Duration::from_secs(5)).unwrap();
    assert!(worker
        .call(WorkerAction::Offer(20))
        .unwrap_err()
        .to_string()
        .contains("busy"));
    let cancelling = worker.clone();
    let cancel = thread::spawn(move || cancelling.call(WorkerAction::Cancel));
    let deadline = Instant::now() + Duration::from_secs(5);
    while !worker.cancelled.load(Ordering::Acquire) {
        assert!(Instant::now() < deadline);
        thread::yield_now();
    }
    assert!(worker.call(WorkerAction::Offer(30)).is_err());
    release.send(()).unwrap();
    assert_eq!(caller.join().unwrap().unwrap()["amount"], 10);
    assert_eq!(cancel.join().unwrap().unwrap()["state"], "cancelled");
    actor.join().unwrap();
}
