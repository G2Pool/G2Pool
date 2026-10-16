use super::*;
#[test]
fn reservations_survive_queue_handoff_and_release_on_cancellation() {
    let pool = Bytes::new(100);
    let (send, receive) = std::sync::mpsc::sync_channel(1);
    send.send(pool.reserve(80).unwrap()).unwrap();
    assert!(pool.reserve(21).is_none());
    let held = receive.recv().unwrap();
    assert_eq!(pool.used(), 80);
    drop(held);
    assert!(pool.reserve(100).is_some());
    assert_eq!(pool.used(), 0);
    assert!(pool.reserve(usize::MAX).is_none());
}

impl Bytes {
    pub(in crate::network) fn used(&self) -> usize {
        self.used.load(Ordering::Acquire)
    }
}
