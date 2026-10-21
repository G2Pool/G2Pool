use super::*;

#[test]
fn quota_errors_are_retryable_without_hiding_corrupt_metadata_or_database_errors() {
    use crate::storage::sharechain::Error as E;
    assert!(matches!(
        runtime_error(E::Quota("runtime head quota")),
        Error::Capacity(_)
    ));
    assert!(matches!(
        runtime_error(E::Invalid("runtime limits")),
        Error::Invalid("runtime limits")
    ));
    assert!(matches!(
        runtime_error(E::Database(heed::Error::Mdb(heed::MdbError::Corrupted))),
        Error::Storage(_)
    ));
    assert!(matches!(
        runtime_error(E::Database(heed::Error::Mdb(heed::MdbError::MapFull))),
        Error::Capacity(_)
    ));
}
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
    let deadline = store
        .acquire([8; 32], &[key].into_iter().collect(), 2)
        .unwrap();
    store
        .collect(&BTreeSet::new(), chrono::Utc::now().timestamp() + 1)
        .unwrap();
    assert_eq!(store.read(key).unwrap(), second.bytes);
    drop(source);
    drop(store);
    let mut store = Store::open(&path, [1; 32], limits).unwrap();
    assert_eq!(store.acquisitions().unwrap(), vec![[8; 32]]);
    store
        .fence(&Anchor {
            height: 8,
            hash: [9; 32],
        })
        .unwrap();
    assert_eq!(
        store.acquire([8; 32], &BTreeSet::new(), 2).unwrap(),
        deadline
    );
    assert_eq!(store.acquisitions().unwrap(), vec![[8; 32]]);
    for id in 9..16 {
        store.acquire([id; 32], &BTreeSet::new(), 2).unwrap();
    }
    assert!(matches!(
        store.acquire([16; 32], &BTreeSet::new(), 2),
        Err(Error::Capacity("acquisition tasks"))
    ));
    assert!(store.acquisitions().unwrap().contains(&[8; 32]));
    store.release_acquisition([8; 32]).unwrap();
    store
        .collect(&BTreeSet::new(), chrono::Utc::now().timestamp() + 1)
        .unwrap();
    assert!(matches!(store.read(key), Err(Error::Unavailable(_))));
}

#[test]
fn winning_disconnected_head_replaces_only_the_incumbent_at_capacity() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("pool");
    let limits = StorageLimits::default();
    let mut store = Store::open(&path, [1; 32], limits.clone()).unwrap();
    let object = inventory();
    store.stage(std::slice::from_ref(&object)).unwrap();
    let anchor = Anchor {
        height: 7,
        hash: [7; 32],
    };
    let evaluation = |tip| Evaluation {
        tip,
        anchor: anchor.clone(),
        financial: crate::consensus::Financial {
            record: PayoutRecord {
                protocol: [1; 32],
                anchor: anchor.clone(),
                parent: Parent::Root(anchor.clone()),
                receipts: List(vec![]),
                accounts: List(vec![]),
                paid: 0,
                finder: 0,
            },
            coupons: vec![],
        },
        full: [tip].into_iter().collect(),
        primary: [tip].into_iter().collect(),
        required: [object.key].into_iter().collect(),
        cost: Default::default(),
    };
    let expected = store.fence(&anchor).unwrap();
    let incumbent = [1; 32];
    let expected = store
        .promote_replacing(&expected, &evaluation(incumbent), true, &[], None)
        .unwrap();
    for n in 2..=limits.heads {
        store
            .promote_replacing(&expected, &evaluation([n as u8; 32]), false, &[], None)
            .unwrap();
    }
    let before: BTreeSet<_> = store.heads().unwrap().iter().map(|h| h.tip).collect();
    assert_eq!(before.len(), limits.heads);
    let mut candidate = evaluation([255; 32]);
    assert!(matches!(
        store.promote_replacing(&expected, &candidate, false, &[], None),
        Err(Error::Capacity("materialized heads"))
    ));
    let absent = Key::new(Kind::Inventory, [254; 32]);
    candidate.required.insert(absent);
    assert!(matches!(
        store.promote_replacing(&expected, &candidate, true, &[], None),
        Err(Error::Unavailable(key)) if key == absent
    ));
    assert_eq!(store.revision().unwrap().unwrap(), expected);
    assert_eq!(
        store
            .heads()
            .unwrap()
            .iter()
            .map(|h| h.tip)
            .collect::<BTreeSet<_>>(),
        before
    );
    candidate.required.remove(&absent);
    let selected = store
        .promote_replacing(&expected, &candidate, true, &[], None)
        .unwrap();
    let mut after = before;
    after.remove(&incumbent);
    after.insert(candidate.tip);
    drop(store);
    let store = Store::open(&path, [1; 32], limits).unwrap();
    assert_eq!(store.revision().unwrap().unwrap(), selected);
    assert_eq!(selected.tip, Some(candidate.tip));
    assert_eq!(
        store
            .heads()
            .unwrap()
            .iter()
            .map(|h| h.tip)
            .collect::<BTreeSet<_>>(),
        after
    );
    assert_eq!(store.read(object.key).unwrap(), object.bytes);
}

impl Store {
    pub fn save_wallet_record(&mut self, name: &str, bytes: &[u8]) -> Result<()> {
        if bytes.len() > 256 * 1024 {
            return Err(Error::Capacity("wallet record"));
        }
        self.database
            .auxiliary_write(format!("aux/wallet/record/{name}").as_bytes(), bytes)
            .map_err(runtime_error)
    }
}

impl Store {
    pub(crate) fn stage(&mut self, objects: &[Object]) -> Result<()> {
        for object in objects {
            Key::checked(object.key.kind, object.key.id, &object.bytes)?;
        }
        self.stage_checked(objects)
    }
}
