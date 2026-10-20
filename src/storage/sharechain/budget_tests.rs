use super::*;
use crate::storage::sharechain::{runtime_store::*, Header, Record, SharechainDb};

#[test]
fn one_budget_bounds_archive_runtime_and_auxiliary_writes_but_preserves_solution_space() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = SharechainDb::open(dir.path(), 8 * 1024 * 1024, 1024 * 1024, [7; 32]).unwrap();
    let limits = RuntimeLimits {
        max_objects: 100,
        max_bytes: 1024 * 1024,
        max_object_bytes: 1024 * 1024,
        max_lease_keys: 100,
        max_head_keys: 100,
        max_heads: 10,
    };
    db.runtime_store(limits).unwrap();
    let initial = db.used_bytes().unwrap();
    assert_eq!(initial, db.env.non_free_pages_size().unwrap());
    db.set_byte_limit(initial + 8192);
    let runtime = db.runtime_store(limits).unwrap();
    let bytes = vec![1; 128 * 1024];
    assert!(matches!(
        db.auxiliary_write(b"test", &bytes),
        Err(Error::Quota(_))
    ));
    assert!(db.auxiliary_read(b"test").unwrap().is_none());
    let record = Record {
        id: [1; 32],
        header: Header {
            parent: None,
            height: 0,
            work: 1,
            cumulative_work: 1,
            anchor: [9; 32],
            parent_state_root: [0; 32],
            state_root: [0; 32],
        },
        body: &bytes,
        delta: &[],
        state: &[],
    };
    assert!(matches!(
        db.publish_validated(0, &[record], [1; 32]),
        Err(Error::Quota(_))
    ));
    assert!(db.read().unwrap().header(&[1; 32]).unwrap().is_none());
    assert!(matches!(
        runtime.apply(
            0,
            RuntimeBatch {
                objects: &[RuntimeObject {
                    key: RoutingKey {
                        kind: 1,
                        id: [1; 32]
                    },
                    mode: ObjectMode::Immutable,
                    format: 0,
                    logical_digest: blake2::blake2b::blake2b(32, &[], &bytes)
                        .as_bytes()
                        .try_into()
                        .unwrap(),
                    bytes: &bytes
                }],
                ..Default::default()
            }
        ),
        Err(Error::Quota(_))
    ));
    assert_eq!(runtime.snapshot().unwrap().usage().unwrap(), (0, 0));
    db.solution_transaction::<_, Error>(|tx, table| {
        table.put(tx, b"aux/native/submission/test", &bytes)?;
        Ok(())
    })
    .unwrap();
    assert!(db.used_bytes().unwrap() > db.byte_limit());
    assert!(matches!(
        db.auxiliary_write(b"test", &bytes),
        Err(Error::Quota(_))
    ));
    db.auxiliary_delete(b"aux/native/submission/test").unwrap();
    assert!(db.used_bytes().unwrap() <= db.byte_limit());
}

#[test]
fn bounded_cleanup_can_run_at_the_live_budget_with_an_old_reader() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = SharechainDb::open(dir.path(), 5 * 1024 * 1024, 64 * 1024, [7; 32]).unwrap();
    db.set_byte_limit(4 * 1024 * 1024);
    let value = vec![1; 4096];
    let mut keys = Vec::new();
    for index in 0u32..2000 {
        let key = index.to_be_bytes().to_vec();
        match db.auxiliary_write(&key, &value) {
            Ok(()) => keys.push(key),
            Err(Error::Quota(_)) => break,
            Err(error) => panic!("ran out of map space before the live budget: {error}"),
        }
    }
    assert!(keys.len() > 256 && keys.len() < 2000);
    let previous = db.used_bytes().unwrap();
    let old = db.read().unwrap();
    db.auxiliary_transaction::<_, Error>(|tx, table| {
        for key in keys.iter().take(256) {
            table.delete(tx, key)?;
        }
        Ok(())
    })
    .unwrap();
    assert!(db.used_bytes().unwrap() < previous);
    assert!(db.runtime.get(&old.tx, &keys[0]).unwrap().is_some());
    assert!(db.auxiliary_read(&keys[0]).unwrap().is_none());
    let retained_key = &keys[256];
    let mut full = false;
    for index in 0..4096u32 {
        let mut changed = value.clone();
        changed[..4].copy_from_slice(&index.to_be_bytes());
        match db.auxiliary_write(retained_key, &changed) {
            Ok(()) => {}
            Err(Error::Database(heed::Error::Mdb(heed::MdbError::MapFull))) => {
                full = true;
                break;
            }
            Err(error) => panic!("unexpected error under reader pressure: {error}"),
        }
    }
    assert!(full);
    assert_eq!(
        db.runtime.get(&old.tx, retained_key).unwrap(),
        Some(value.as_slice())
    );
    drop(old);
    db.auxiliary_delete(retained_key).unwrap();
    assert!(db.auxiliary_read(retained_key).unwrap().is_none());
}
