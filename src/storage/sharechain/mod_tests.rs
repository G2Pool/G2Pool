use super::*;
use tempfile::tempdir;

#[test]
fn storage_format_is_the_only_persistent_layout_marker() {
    for (key, value, expected) in [
        ("storage_format", Some(2u32.to_be_bytes()), "storage format"),
        ("storage_format", None, "storage format"),
        ("pool_id", None, "pool identity mismatch"),
    ] {
        let dir = tempdir().unwrap();
        let db = SharechainDb::open(dir.path(), 1 << 24, [42; 32]).unwrap();
        let mut tx = db.env.write_txn().unwrap();
        let meta: Database<Bytes, Bytes> =
            db.env.open_database(&tx, Some("meta")).unwrap().unwrap();
        match value {
            Some(value) => meta.put(&mut tx, key.as_bytes(), &value).unwrap(),
            None => {
                meta.delete(&mut tx, key.as_bytes()).unwrap();
            }
        }
        tx.commit().unwrap();
        drop(db);
        match SharechainDb::open(dir.path(), 1 << 24, [77; 32]) {
            Err(Error::Invalid(message)) => assert_eq!(message, expected),
            Err(error) => panic!("unexpected open error: {error}"),
            Ok(_) => panic!("accepted invalid {key}"),
        }
    }
}

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
