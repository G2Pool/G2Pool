use super::*;
#[test]
fn connectivity_is_never_a_mainnet_readiness_certificate() {
    assert_eq!(state(false, false, 0, false, false), "disconnected");
    assert_eq!(state(false, false, 1, true, false), "connected");
    assert_eq!(state(false, false, 1, false, false), "warming");
    assert_eq!(state(true, false, 1, false, false), "syncing");
    assert_eq!(state(false, true, 1, false, false), "recovering");
    assert_eq!(state(false, false, 1, true, true), "degraded");
}
