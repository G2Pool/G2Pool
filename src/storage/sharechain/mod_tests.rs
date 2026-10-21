use super::*;
use tempfile::tempdir;

#[test]
fn routing_namespace_updates_are_atomic_and_isolated() {
    let dir = tempdir().unwrap();
    let db = SharechainDb::open(dir.path(), 1 << 24, [42; 32]).unwrap();
    let kv = &db;
    kv.auxiliary_write(b"wallet/sentinel", b"preserved")
        .unwrap();
    let original = vec![
        (b"peers/a".to_vec(), b"one".to_vec()),
        (b"peers/b".to_vec(), b"two".to_vec()),
    ];
    kv.replace_peers(&original).unwrap();
    kv.replace_peers(&original).unwrap();
    let desired = vec![(b"peers/b".to_vec(), b"new".to_vec())];
    kv.replace_peers(&desired).unwrap();
    assert_eq!(kv.read_peers().unwrap(), desired);
    assert_eq!(
        kv.auxiliary_read(b"wallet/sentinel").unwrap().as_deref(),
        Some(b"preserved".as_slice())
    );
    let duplicates = vec![
        (b"peers/b".to_vec(), b"x".to_vec()),
        (b"peers/b".to_vec(), b"y".to_vec()),
    ];
    assert!(kv.replace_peers(&duplicates).is_err());
    assert!(kv
        .replace_peers(&[(b"wallet/other".to_vec(), vec![])])
        .is_err());
    assert!(kv
        .replace_peers(&[(b"peers/a".to_vec(), vec![0; 1025])])
        .is_err());
    assert_eq!(kv.read_peers().unwrap(), desired);
}
