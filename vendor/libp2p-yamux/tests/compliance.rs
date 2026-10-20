mod harness;
use libp2p_yamux::Config;

#[tokio::test(flavor = "current_thread")]
async fn close_implies_flush() {
    let (alice, bob) = harness::connected_muxers_on_memory_ring_buffer::<Config, _, _>().await;

    harness::close_implies_flush(alice, bob).await;
}

#[tokio::test(flavor = "current_thread")]
async fn read_after_close() {
    let (alice, bob) = harness::connected_muxers_on_memory_ring_buffer::<Config, _, _>().await;

    harness::read_after_close(alice, bob).await;
}
