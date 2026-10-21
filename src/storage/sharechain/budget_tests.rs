use super::*;
use crate::storage::sharechain::{runtime_store::*, SharechainDb};

#[test]
fn one_budget_bounds_runtime_and_auxiliary_writes_but_preserves_solution_space() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = SharechainDb::open(dir.path(), 8 * 1024 * 1024, [7; 32]).unwrap();
    let limits = RuntimeLimits {
        max_objects: 100,
        max_bytes: 1024 * 1024,
        max_object_bytes: 1024 * 1024,
        max_lease_keys: 100,
        max_head_keys: 100,
        max_heads: 1,
    };
    let selected = RuntimeRevision {
        generation: 1,
        tip: Some([10; 32]),
        ..Default::default()
    };
    db.runtime_store(limits)
        .unwrap()
        .apply(
            0,
            RuntimeBatch {
                heads: &[RuntimeHead {
                    id: [10; 32],
                    binding: [10; 32],
                    parent: None,
                    added: &[],
                    removed: &[],
                    metadata: &[],
                }],
                selection: Some((RuntimeRevision::default(), selected)),
                ..Default::default()
            },
        )
        .unwrap();
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
                heads: &[RuntimeHead {
                    id: [11; 32],
                    binding: [11; 32],
                    parent: Some([10; 32]),
                    added: &[RoutingKey {
                        kind: 1,
                        id: [1; 32]
                    }],
                    removed: &[],
                    metadata: &[],
                }],
                release_heads: &[[10; 32]],
                selection: Some((
                    selected,
                    RuntimeRevision {
                        generation: 2,
                        tip: Some([11; 32]),
                        ..selected
                    }
                )),
                ..Default::default()
            }
        ),
        Err(Error::Quota(_))
    ));
    assert_eq!(runtime.snapshot().unwrap().usage().unwrap(), (0, 0));
    assert_eq!(runtime.snapshot().unwrap().revision().unwrap(), selected);
    assert_eq!(
        runtime.snapshot().unwrap().heads(2).unwrap(),
        vec![([10; 32], vec![])]
    );
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
    let mut db = SharechainDb::open(dir.path(), 5 * 1024 * 1024, [7; 32]).unwrap();
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
    let old = db.env.read_txn().unwrap();
    db.auxiliary_transaction::<_, Error>(|tx, table| {
        for key in keys.iter().take(256) {
            table.delete(tx, key)?;
        }
        Ok(())
    })
    .unwrap();
    assert!(db.used_bytes().unwrap() < previous);
    assert!(db.runtime.get(&old, &keys[0]).unwrap().is_some());
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
        db.runtime.get(&old, retained_key).unwrap(),
        Some(value.as_slice())
    );
    drop(old);
    db.auxiliary_delete(retained_key).unwrap();
    assert!(db.auxiliary_read(retained_key).unwrap().is_none());
}

#[test]
fn retirement_and_collection_progress_at_quota_without_copying_large_bodies() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = SharechainDb::open(dir.path(), 8 * 1024 * 1024, [7; 32]).unwrap();
    let limits = RuntimeLimits {
        max_objects: 10,
        max_bytes: 8 * 1024 * 1024,
        max_object_bytes: 4 * 1024 * 1024,
        max_lease_keys: 10,
        max_head_keys: 10,
        max_heads: 2,
    };
    let runtime = db.runtime_store(limits).unwrap();
    let payload = vec![1; 3 * 1024 * 1024];
    let digest = blake2::blake2b::blake2b(32, &[], &payload)
        .as_bytes()
        .try_into()
        .unwrap();
    let keys = [
        RoutingKey {
            kind: 1,
            id: [1; 32],
        },
        RoutingKey {
            kind: 1,
            id: [2; 32],
        },
    ];
    let objects: Vec<_> = keys
        .iter()
        .map(|key| RuntimeObject {
            key: *key,
            mode: ObjectMode::Immutable,
            format: 0,
            bytes: &payload,
            logical_digest: digest,
        })
        .collect();
    let heads: Vec<_> = keys
        .iter()
        .map(|key| RuntimeHead {
            id: key.id,
            binding: key.id,
            parent: None,
            added: std::slice::from_ref(key),
            removed: &[],
            metadata: &[],
        })
        .collect();
    runtime
        .apply(
            0,
            RuntimeBatch {
                objects: &objects,
                heads: &heads,
                ..Default::default()
            },
        )
        .unwrap();
    db.set_byte_limit(db.used_bytes().unwrap());
    db.solution_transaction::<_, Error>(|tx, table| {
        table.put(tx, b"aux/native/submission/pressure", &vec![3; 512 * 1024])?;
        Ok(())
    })
    .unwrap();
    assert!(db.used_bytes().unwrap() > db.byte_limit());
    let runtime = db.runtime_store(limits).unwrap();
    let old = runtime.snapshot().unwrap();
    runtime
        .retire_head(1, RuntimeRevision::default(), keys[0].id)
        .unwrap();
    assert_eq!(
        runtime.snapshot().unwrap().heads(2).unwrap(),
        vec![(keys[1].id, vec![])]
    );
    assert_eq!(old.object(keys[0], None).unwrap().unwrap().seen, 0);
    assert_eq!(
        runtime
            .snapshot()
            .unwrap()
            .object(keys[0], None)
            .unwrap()
            .unwrap()
            .seen,
        1
    );
    assert!(matches!(
        db.auxiliary_write(b"overflow", &vec![2; 128 * 1024]),
        Err(Error::Quota(_))
    ));
    assert!(db.auxiliary_read(b"overflow").unwrap().is_none());
    assert_eq!(
        runtime.collect_keep(1000, 900, 256, &[]).unwrap().removed,
        1
    );
    assert!(runtime
        .snapshot()
        .unwrap()
        .object(keys[0], None)
        .unwrap()
        .is_none());
    assert_eq!(
        runtime
            .snapshot()
            .unwrap()
            .object(keys[1], None)
            .unwrap()
            .unwrap()
            .bytes,
        payload
    );
    assert_eq!(old.object(keys[0], None).unwrap().unwrap().bytes, payload);
    assert!(db.used_bytes().unwrap() < db.byte_limit());
    assert!(db
        .auxiliary_read(b"aux/native/submission/pressure")
        .unwrap()
        .is_some());
}
