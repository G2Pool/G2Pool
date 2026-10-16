use super::*;
fn inventory() -> Object {
    Object::content(&Inventory {
        protocol: [1; 32],
        owner: [2; 33],
        rows: List(Vec::new()),
    })
    .unwrap()
}
fn auth(key: Key, signature: u8) -> Object {
    Object::new(
        key,
        encode(&WorkAuthorization {
            fields: WorkAuthMessage {
                protocol: [1; 32],
                work_id: key.id,
            },
            signature: [signature; 64],
        })
        .unwrap(),
    )
    .unwrap()
}
#[test]
fn native_snapshot_provisional_variants_and_restart_leases() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("pool");
    let limits = StorageLimits::default();
    let mut store = Store::open(&path, [1; 32], limits.clone()).unwrap();
    let key = Key::new(Kind::Authorization, [3; 32]);
    let first = auth(key, 1);
    let second = auth(key, 2);
    store.stage(&[first.clone(), inventory()]).unwrap();
    let source = store.read_source().unwrap();
    store.stage(std::slice::from_ref(&second)).unwrap();
    assert_eq!(source.open().unwrap().read(key).unwrap(), first.bytes);
    assert_eq!(store.read(key).unwrap(), second.bytes);
    let anchor = Anchor {
        height: 7,
        hash: [7; 32],
    };
    store.fence(&anchor).unwrap();
    store
        .acquire([8; 32], &anchor, &[key].into_iter().collect(), 2)
        .unwrap();
    store
        .collect(&BTreeSet::new(), chrono::Utc::now().timestamp() + 1)
        .unwrap();
    assert_eq!(store.read(key).unwrap(), second.bytes);
    drop(source);
    drop(store);
    let mut store = Store::open(&path, [1; 32], limits).unwrap();
    assert_eq!(store.acquisitions(&anchor).unwrap(), vec![[8; 32]]);
    store.release_acquisition([8; 32]).unwrap();
    store
        .collect(&BTreeSet::new(), chrono::Utc::now().timestamp() + 1)
        .unwrap();
    assert!(matches!(store.read(key), Err(Error::Unavailable(_))));
}
