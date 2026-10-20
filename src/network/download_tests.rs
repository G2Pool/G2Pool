use super::*;
use crate::objects::Kind;
#[test]
fn duplicate_requests_are_shared_and_missing_is_source_specific() {
    let state = Arc::new(Mutex::new(Downloads::default()));
    let key = Key::new(Kind::Coupon, [7; 32]);
    let a = "8.8.8.8:3417".parse().unwrap();
    let b = "9.9.9.9:3417".parse().unwrap();
    let lease = Downloads::reserve(&state, a, vec![key]).unwrap();
    assert!(Downloads::reserve(&state, b, vec![key]).is_none());
    lease.absent(a, &[key]);
    drop(lease);
    assert!(Downloads::reserve(&state, a, vec![key]).is_none());
    assert!(Downloads::reserve(&state, b, vec![key]).is_some());
    assert!(state.lock().unwrap().active.is_empty());
}
