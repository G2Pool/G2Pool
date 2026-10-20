use super::*;
use crate::storage::sharechain::SharechainDb;
fn limits() -> RuntimeLimits {
    RuntimeLimits {
        max_objects: 10,
        max_bytes: 10000,
        max_object_bytes: 1000,
        max_lease_keys: 10,
        max_head_keys: 10,
        max_heads: 10,
    }
}
fn open(path: &std::path::Path) -> SharechainDb {
    SharechainDb::open(path, 16 * 1024 * 1024, 1000, [7; 32]).unwrap()
}

#[test]
fn runtime_limits_can_grow_but_cannot_shrink_in_place() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(dir.path());
    let original = limits();
    db.runtime_store(original).unwrap();

    let mut grown = original;
    grown.max_objects += 1;
    grown.max_bytes += 1;
    grown.max_object_bytes += 1;
    grown.max_lease_keys += 1;
    grown.max_head_keys += 1;
    grown.max_heads += 1;
    db.runtime_store(grown).unwrap();

    let mut shrunk = grown;
    shrunk.max_heads -= 1;
    assert!(matches!(
        db.runtime_store(shrunk),
        Err(Error::Invalid("runtime limits can only grow"))
    ));
}
fn rk(n: u8) -> RoutingKey {
    RoutingKey {
        kind: 1,
        id: [n; 32],
    }
}
fn object(k: RoutingKey) -> RuntimeObject<'static> {
    RuntimeObject {
        key: k,
        mode: ObjectMode::Immutable,
        format: 1,
        bytes: b"payload",
        logical_digest: digest(b"payload"),
    }
}
#[test]
fn quota_failure_rolls_back_every_namespace() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(dir.path());
    let mut l = limits();
    l.max_lease_keys = 1;
    let s = db.runtime_store(l).unwrap();
    s.apply(
        0,
        RuntimeBatch {
            leases: &[RuntimeLease {
                metadata: &[],
                id: [2; 32],
                keys: &[rk(2)],
            }],
            ..Default::default()
        },
    )
    .unwrap();
    let lease = RuntimeLease {
        metadata: &[],
        id: [1; 32],
        keys: &[rk(1)],
    };
    assert!(s
        .apply(
            5,
            RuntimeBatch {
                objects: &[object(rk(1))],
                leases: &[lease],
                ..Default::default()
            }
        )
        .is_err());
    let snap = s.snapshot().unwrap();
    assert_eq!(snap.usage().unwrap(), (0, 0));
    assert!(snap.lease([1; 32]).unwrap().is_none());
    assert!(snap.lease([2; 32]).unwrap().is_some());
    assert!(snap.object(rk(1), None).unwrap().is_none());
}
#[test]
fn forks_seals_cas_restart_and_snapshot() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(dir.path());
    let s = db.runtime_store(limits()).unwrap();
    let k = rk(1);
    let w = rk(2);
    let a = RuntimeObject {
        key: w,
        mode: ObjectMode::WorkVariant,
        format: 2,
        bytes: b"work A",
        logical_digest: digest(b"work A"),
    };
    let b = RuntimeObject {
        key: w,
        mode: ObjectMode::WorkVariant,
        format: 2,
        bytes: b"work B",
        logical_digest: digest(b"work B"),
    };
    s.apply(
        0,
        RuntimeBatch {
            objects: &[object(k), a, b],
            ..Default::default()
        },
    )
    .unwrap();
    assert!(s.snapshot().unwrap().object(w, None).unwrap().is_none());
    let parent = RuntimeHead {
        id: [10; 32],
        binding: [10; 32],
        metadata: &[],
        parent: None,
        added: &[k, w],
        removed: &[],
    };
    let selected = RuntimeRevision {
        generation: 1,
        anchor_height: 9,
        anchor_hash: [9; 32],
        tip: Some([10; 32]),
    };
    s.apply(
        1,
        RuntimeBatch {
            heads: &[parent],
            seals: &[(w, digest(b"work A"))],
            selection: Some((RuntimeRevision::default(), selected)),
            ..Default::default()
        },
    )
    .unwrap();
    let old = s.snapshot().unwrap();
    let fork = RuntimeHead {
        id: [11; 32],
        binding: [11; 32],
        metadata: &[],
        parent: Some([10; 32]),
        added: &[],
        removed: &[w],
    };
    s.apply(
        2,
        RuntimeBatch {
            heads: &[fork],
            ..Default::default()
        },
    )
    .unwrap();
    assert_eq!(s.snapshot().unwrap().ref_count(k).unwrap(), 2);
    assert_eq!(old.ref_count(k).unwrap(), 1);
    drop(old);
    assert!(s
        .apply(
            3,
            RuntimeBatch {
                seals: &[(w, digest(b"work B"))],
                ..Default::default()
            }
        )
        .is_err());
    assert!(matches!(
        s.apply(
            3,
            RuntimeBatch {
                release_heads: &[[11; 32]],
                selection: Some((RuntimeRevision::default(), selected)),
                ..Default::default()
            }
        ),
        Err(Error::Stale)
    ));
    assert_eq!(s.collect(1000, 10, 10).unwrap().removed, 1);
    drop(s);
    drop(db);
    let db = open(dir.path());
    let s = db.runtime_store(limits()).unwrap();
    assert_eq!(s.snapshot().unwrap().revision().unwrap(), selected);
    assert_eq!(
        s.snapshot()
            .unwrap()
            .object(w, None)
            .unwrap()
            .unwrap()
            .bytes,
        b"work A"
    );
    let next = RuntimeRevision {
        generation: 2,
        tip: None,
        ..selected
    };
    s.apply(
        1001,
        RuntimeBatch {
            release_heads: &[[10; 32], [11; 32]],
            selection: Some((selected, next)),
            ..Default::default()
        },
    )
    .unwrap();
    assert_eq!(s.collect(1001, 10, 10).unwrap().removed, 2);
}
#[test]
fn leases_protect_future_arrivals_and_do_not_extend() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(dir.path());
    let s = db.runtime_store(limits()).unwrap();
    let lease = RuntimeLease {
        metadata: &[],
        id: [1; 32],
        keys: &[rk(1)],
    };
    s.apply(
        0,
        RuntimeBatch {
            leases: &[lease],
            ..Default::default()
        },
    )
    .unwrap();
    s.apply(
        800,
        RuntimeBatch {
            objects: &[object(rk(1))],
            leases: &[RuntimeLease {
                metadata: &[],
                id: [1; 32],
                keys: &[rk(1)],
            }],
            ..Default::default()
        },
    )
    .unwrap();
    assert_eq!(
        s.snapshot().unwrap().lease([1; 32]).unwrap().unwrap().0,
        900
    );
    assert_eq!(s.collect(899, 0, 1).unwrap().removed, 0);
    assert_eq!(s.collect(900, 0, 1).unwrap().removed, 0);
    assert_eq!(s.collect(900, 0, 1).unwrap().removed, 1);
}
#[test]
fn owned_snapshot_is_send_sync_and_shared_across_workers() {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<RuntimeSnapshot>();
    let dir = tempfile::tempdir().unwrap();
    let db = open(dir.path());
    let s = db.runtime_store(limits()).unwrap();
    s.apply(
        0,
        RuntimeBatch {
            objects: &[object(rk(1))],
            ..Default::default()
        },
    )
    .unwrap();
    let snap = s.snapshot().unwrap();
    let worker = snap.clone();
    let join = std::thread::spawn(move || worker.object_preferred(rk(1)).unwrap().unwrap().bytes);
    assert_eq!(snap.object(rk(1), None).unwrap().unwrap().bytes, b"payload");
    assert_eq!(join.join().unwrap(), b"payload");
}
#[test]
fn metadata_pages_seek_after_exact_variants_and_cover_filtered_rows() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(dir.path());
    let s = db.runtime_store(limits()).unwrap();
    let k = rk(1);
    let other = RoutingKey {
        kind: 2,
        id: [1; 32],
    };
    let last = RoutingKey {
        kind: 255,
        id: [1; 32],
    };
    s.apply(
        0,
        RuntimeBatch {
            objects: &[
                RuntimeObject {
                    key: k,
                    mode: ObjectMode::WorkVariant,
                    logical_digest: [0; 32],
                    format: 1,
                    bytes: b"A",
                },
                RuntimeObject {
                    key: k,
                    mode: ObjectMode::WorkVariant,
                    logical_digest: [0; 32],
                    format: 2,
                    bytes: b"B",
                },
                object(rk(2)),
                object(other),
                object(last),
            ],
            ..Default::default()
        },
    )
    .unwrap();
    let snap = s.snapshot().unwrap();
    let all = snap.objects(1, 10).unwrap();
    let mut cursor = None;
    let mut paged = Vec::new();
    let mut filtered = Vec::new();
    loop {
        let page = snap.objects_after(1, cursor, 10, 85).unwrap();
        if page.is_empty() {
            break;
        }
        assert_eq!(page.len(), 1);
        let (route, info) = page[0];
        cursor = Some((route, info.digest));
        if info.format == 1 {
            filtered.push((route, info));
        }
        paged.extend(page);
    }
    assert_eq!(paged, all);
    assert_eq!(
        filtered,
        all.iter()
            .copied()
            .filter(|(_, info)| info.format == 1)
            .collect::<Vec<_>>()
    );
    assert_eq!(snap.objects_after(1, None, 10, 84).unwrap().len(), 0);
    assert!(snap
        .objects_after(1, Some((other, [0; 32])), 1, 85)
        .is_err());
    assert_eq!(snap.objects_after(255, None, 1, 85).unwrap()[0].0, last);
    s.apply(
        1,
        RuntimeBatch {
            objects: &[object(rk(3))],
            ..Default::default()
        },
    )
    .unwrap();
    assert_eq!(snap.objects(1, 10).unwrap(), all);
    assert_eq!(
        s.snapshot().unwrap().objects(1, 10).unwrap().len(),
        all.len() + 1
    );
}
#[test]
fn immutable_equivalent_representation_cas_preserves_identity_and_snapshots() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(dir.path());
    let s = db.runtime_store(limits()).unwrap();
    let k = rk(1);
    s.apply(
        0,
        RuntimeBatch {
            objects: &[object(k)],
            heads: &[RuntimeHead {
                id: [10; 32],
                binding: [20; 32],
                metadata: &[],
                parent: None,
                added: &[k],
                removed: &[],
            }],
            ..Default::default()
        },
    )
    .unwrap();
    assert!(s
        .apply(
            2,
            RuntimeBatch {
                objects: &[RuntimeObject {
                    key: k,
                    mode: ObjectMode::Immutable,
                    format: 1,
                    bytes: b"changed",
                    logical_digest: digest(b"changed"),
                }],
                ..Default::default()
            }
        )
        .is_err());
    let old = s.snapshot().unwrap();
    let logical = digest(b"payload");
    let replacement = RuntimeReplacement {
        key: k,
        logical_digest: logical,
        expected_digest: logical,
        expected_format: 1,
        format: 2,
        bytes: b"packed",
    };
    s.apply(
        1,
        RuntimeBatch {
            replacements: &[replacement],
            ..Default::default()
        },
    )
    .unwrap();
    assert_eq!(old.object(k, None).unwrap().unwrap().bytes, b"payload");
    assert_eq!(
        s.snapshot()
            .unwrap()
            .object(k, None)
            .unwrap()
            .unwrap()
            .bytes,
        b"packed"
    );
    assert_eq!(s.snapshot().unwrap().ref_count(k).unwrap(), 1);
    assert!(matches!(
        s.apply(
            2,
            RuntimeBatch {
                replacements: &[RuntimeReplacement {
                    key: k,
                    logical_digest: logical,
                    expected_digest: logical,
                    expected_format: 1,
                    format: 3,
                    bytes: b"stale overwrite"
                }],
                ..Default::default()
            }
        ),
        Err(Error::Stale)
    ));
    s.apply(
        3,
        RuntimeBatch {
            objects: &[object(k)],
            ..Default::default()
        },
    )
    .unwrap();
    assert_eq!(
        s.snapshot()
            .unwrap()
            .object(k, None)
            .unwrap()
            .unwrap()
            .bytes,
        b"packed"
    );
    s.apply(
        4,
        RuntimeBatch {
            release_heads: &[[10; 32]],
            ..Default::default()
        },
    )
    .unwrap();
    assert_eq!(s.collect(5, 0, 10).unwrap().removed, 1);
    s.apply(
        7,
        RuntimeBatch {
            objects: &[object(k)],
            ..Default::default()
        },
    )
    .unwrap();
    assert_eq!(
        s.snapshot()
            .unwrap()
            .object(k, None)
            .unwrap()
            .unwrap()
            .bytes,
        b"payload"
    );
}
#[test]
fn owned_snapshot_head_binding_replacement_metadata_and_gc_keep() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(dir.path());
    let s = db.runtime_store(limits()).unwrap();
    let k = rk(1);
    let extra = rk(2);
    s.apply(
        0,
        RuntimeBatch {
            objects: &[object(k), object(extra)],
            heads: &[RuntimeHead {
                id: [10; 32],
                binding: [20; 32],
                metadata: b"first",
                parent: None,
                added: &[k],
                removed: &[],
            }],
            leases: &[RuntimeLease {
                id: [1; 32],
                keys: &[extra],
                metadata: b"anchor",
            }],
            ..Default::default()
        },
    )
    .unwrap();
    let old = s.snapshot().unwrap();
    let cloned = old.clone();
    s.apply(
        1,
        RuntimeBatch {
            heads: &[RuntimeHead {
                id: [10; 32],
                binding: [21; 32],
                metadata: b"second",
                parent: Some([10; 32]),
                added: &[extra],
                removed: &[k],
            }],
            expect_revision: Some(RuntimeRevision::default()),
            ..Default::default()
        },
    )
    .unwrap();
    let current = s.snapshot().unwrap();
    assert_eq!(current.head_keys([10; 32]).unwrap(), Some(vec![extra]));
    assert_eq!(
        current.heads(10).unwrap(),
        vec![([10; 32], b"second".to_vec())]
    );
    assert_eq!(cloned.head_keys([10; 32]).unwrap(), Some(vec![k]));
    assert_eq!(current.leases(10).unwrap()[0].3, b"anchor");
    assert_eq!(current.objects(1, 1).unwrap().len(), 1);
    drop(current);
    s.apply(
        2,
        RuntimeBatch {
            release_heads: &[[10; 32]],
            release_leases: &[[1; 32]],
            ..Default::default()
        },
    )
    .unwrap();
    assert_eq!(s.collect_keep(3, 0, 10, &[extra]).unwrap().removed, 1);
    assert!(s.snapshot().unwrap().object(extra, None).unwrap().is_some());
    drop(s);
    drop(db);
    assert!(old.object(k, None).unwrap().is_some());
}
#[test]
fn provisional_variant_uses_insertion_order_without_sealing() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(dir.path());
    let s = db.runtime_store(limits()).unwrap();
    let k = rk(1);
    s.apply(
        0,
        RuntimeBatch {
            objects: &[
                RuntimeObject {
                    key: k,
                    mode: ObjectMode::WorkVariant,
                    format: 1,
                    bytes: b"second by digest",
                    logical_digest: digest(b"second by digest"),
                },
                RuntimeObject {
                    key: k,
                    mode: ObjectMode::WorkVariant,
                    format: 1,
                    bytes: b"last inserted",
                    logical_digest: digest(b"last inserted"),
                },
            ],
            ..Default::default()
        },
    )
    .unwrap();
    let snap = s.snapshot().unwrap();
    assert!(snap.object(k, None).unwrap().is_none());
    assert_eq!(
        snap.object_preferred(k).unwrap().unwrap().bytes,
        b"last inserted"
    );
}
#[test]
fn released_work_variant_can_be_resealed_after_bounded_reclaim() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(dir.path());
    let s = db.runtime_store(limits()).unwrap();
    let w = rk(1);
    let objects = [
        RuntimeObject {
            key: w,
            mode: ObjectMode::WorkVariant,
            format: 2,
            bytes: b"work A",
            logical_digest: digest(b"work A"),
        },
        RuntimeObject {
            key: w,
            mode: ObjectMode::WorkVariant,
            format: 2,
            bytes: b"work B",
            logical_digest: digest(b"work B"),
        },
    ];
    s.apply(
        0,
        RuntimeBatch {
            objects: &objects,
            seals: &[(w, digest(b"work A"))],
            heads: &[RuntimeHead {
                id: [10; 32],
                binding: [10; 32],
                metadata: &[],
                parent: None,
                added: &[w],
                removed: &[],
            }],
            ..Default::default()
        },
    )
    .unwrap();
    let old = s.snapshot().unwrap();
    assert_eq!(old.variants(w, 1).unwrap().len(), 1);
    assert_eq!(old.variants(w, 10).unwrap().len(), 2);
    assert!(s
        .apply(
            1,
            RuntimeBatch {
                seals: &[(w, digest(b"work B"))],
                heads: &[RuntimeHead {
                    id: [11; 32],
                    binding: [11; 32],
                    metadata: &[],
                    parent: None,
                    added: &[w],
                    removed: &[]
                }],
                ..Default::default()
            }
        )
        .is_err());
    assert!(s.snapshot().unwrap().head_keys([11; 32]).unwrap().is_none());
    s.apply(
        2,
        RuntimeBatch {
            release_heads: &[[10; 32]],
            ..Default::default()
        },
    )
    .unwrap();
    assert!(s
        .apply(
            3,
            RuntimeBatch {
                seals: &[(w, digest(b"work B"))],
                ..Default::default()
            }
        )
        .is_err());
    s.apply(
        3,
        RuntimeBatch {
            leases: &[RuntimeLease {
                metadata: &[],
                id: [1; 32],
                keys: &[w],
            }],
            ..Default::default()
        },
    )
    .unwrap();
    s.collect(4, u64::MAX, 10).unwrap();
    s.apply(
        5,
        RuntimeBatch {
            seals: &[(w, digest(b"work B"))],
            heads: &[RuntimeHead {
                id: [11; 32],
                binding: [11; 32],
                metadata: &[],
                parent: None,
                added: &[w],
                removed: &[],
            }],
            ..Default::default()
        },
    )
    .unwrap();
    assert_eq!(
        s.snapshot()
            .unwrap()
            .object(w, None)
            .unwrap()
            .unwrap()
            .bytes,
        b"work B"
    );
    assert_eq!(old.object(w, None).unwrap().unwrap().bytes, b"work A");
    assert!(s
        .apply(
            6,
            RuntimeBatch {
                seals: &[(w, digest(b"work A"))],
                ..Default::default()
            }
        )
        .is_err());
}
#[test]
fn incremental_lease_union_preserves_deadline_and_atomic_quota() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(dir.path());
    let mut l = limits();
    l.max_lease_keys = 3;
    let s = db.runtime_store(l).unwrap();
    s.apply(
        0,
        RuntimeBatch {
            leases: &[RuntimeLease {
                metadata: &[],
                id: [1; 32],
                keys: &[rk(1)],
            }],
            ..Default::default()
        },
    )
    .unwrap();
    s.apply(
        100,
        RuntimeBatch {
            leases: &[RuntimeLease {
                metadata: &[],
                id: [1; 32],
                keys: &[rk(2)],
            }],
            ..Default::default()
        },
    )
    .unwrap();
    assert_eq!(
        s.snapshot().unwrap().lease([1; 32]).unwrap(),
        Some((900, vec![rk(1), rk(2)]))
    );
    s.apply(
        200,
        RuntimeBatch {
            leases: &[RuntimeLease {
                metadata: &[],
                id: [2; 32],
                keys: &[rk(1)],
            }],
            ..Default::default()
        },
    )
    .unwrap();
    assert!(s
        .apply(
            300,
            RuntimeBatch {
                objects: &[object(rk(3))],
                leases: &[RuntimeLease {
                    metadata: &[],
                    id: [1; 32],
                    keys: &[rk(3)]
                }],
                ..Default::default()
            }
        )
        .is_err());
    let snap = s.snapshot().unwrap();
    assert_eq!(
        snap.lease([1; 32]).unwrap(),
        Some((900, vec![rk(1), rk(2)]))
    );
    assert_eq!(snap.lease([2; 32]).unwrap(), Some((1100, vec![rk(1)])));
    assert_eq!(snap.usage().unwrap(), (0, 0));
}
#[test]
fn persistent_head_delta_has_bounded_path_copy() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(dir.path());
    let mut l = limits();
    l.max_objects = 1100;
    l.max_bytes = 1000000;
    l.max_head_keys = 1100;
    let s = db.runtime_store(l).unwrap();
    let keys: Vec<_> = (0u64..1000)
        .map(|n| {
            let mut id = [0; 32];
            id[24..].copy_from_slice(&n.to_be_bytes());
            RoutingKey { kind: 1, id }
        })
        .collect();
    let objects: Vec<_> = keys.iter().copied().map(object).collect();
    s.apply(
        0,
        RuntimeBatch {
            objects: &objects,
            heads: &[RuntimeHead {
                id: [10; 32],
                binding: [10; 32],
                metadata: &[],
                parent: None,
                added: &keys,
                removed: &[],
            }],
            ..Default::default()
        },
    )
    .unwrap();
    let mut tx = s.env.write_txn().unwrap();
    while s.trie.drain_gc(&mut tx, 100000).unwrap().pending_nodes != 0 {}
    tx.commit().unwrap();
    assert_eq!(s.trie.node_count(&s.env.read_txn().unwrap()).unwrap(), 1999);
    let extra = rk(200);
    s.apply(
        1,
        RuntimeBatch {
            objects: &[object(extra)],
            heads: &[RuntimeHead {
                id: [11; 32],
                binding: [11; 32],
                metadata: &[],
                parent: Some([10; 32]),
                added: &[extra],
                removed: &[],
            }],
            ..Default::default()
        },
    )
    .unwrap();
    assert!(s.trie.node_count(&s.env.read_txn().unwrap()).unwrap() <= 1999 + 265);
    assert_eq!(
        s.snapshot()
            .unwrap()
            .head_keys([11; 32])
            .unwrap()
            .unwrap()
            .len(),
        1001
    );
    s.apply(
        2,
        RuntimeBatch {
            release_heads: &[[10; 32]],
            ..Default::default()
        },
    )
    .unwrap();
    let mut tx = s.env.write_txn().unwrap();
    while s.trie.drain_gc(&mut tx, 100000).unwrap().pending_nodes != 0 {}
    tx.commit().unwrap();
    assert_eq!(s.trie.node_count(&s.env.read_txn().unwrap()).unwrap(), 2001);
}
#[test]
fn two_handles_serialize_lease_and_collection() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(dir.path());
    let a = db.runtime_store(limits()).unwrap();
    let b = db.runtime_store(limits()).unwrap();
    a.apply(
        0,
        RuntimeBatch {
            objects: &[object(rk(1))],
            ..Default::default()
        },
    )
    .unwrap();
    let barrier = std::sync::Arc::new(std::sync::Barrier::new(2));
    let gate = barrier.clone();
    let t = std::thread::spawn(move || {
        gate.wait();
        a.apply(
            10,
            RuntimeBatch {
                leases: &[RuntimeLease {
                    metadata: &[],
                    id: [1; 32],
                    keys: &[rk(1)],
                }],
                ..Default::default()
            },
        )
        .unwrap();
    });
    barrier.wait();
    let gc = b.collect(10, 0, 10).unwrap();
    t.join().unwrap();
    let snap = b.snapshot().unwrap();
    assert!(snap.lease([1; 32]).unwrap().is_some());
    assert_eq!(snap.object(rk(1), None).unwrap().is_some(), gc.removed == 0);
    drop(snap);
    b.apply(
        11,
        RuntimeBatch {
            objects: &[object(rk(1))],
            ..Default::default()
        },
    )
    .unwrap();
    assert_eq!(b.collect(11, 0, 10).unwrap().removed, 0);
}

impl RuntimeStore {
    pub fn collect(&self, now: u64, min_age: u64, limit: usize) -> Result<RuntimeGc> {
        self.collect_keep(now, min_age, limit, &[])
    }
}

impl RuntimeSnapshot {
    pub fn ref_count(&self, key: RoutingKey) -> Result<u64> {
        self.read(|reader| reader.ref_count(key))
    }
}
impl RuntimeReader<'_> {
    pub fn ref_count(&self, k: RoutingKey) -> Result<u64> {
        let mut count = 0;
        for row in self.store.db.prefix_iter(self.txn, b"h")? {
            let (_, value) = row?;
            if value.len() != 40 {
                return Err(Error::Invalid("runtime head"));
            }
            if self.store.trie.contains(
                self.txn,
                &value[8..].try_into().unwrap(),
                DependencyKey {
                    kind: k.kind,
                    id: k.id,
                },
            )? {
                count += 1;
            }
        }
        Ok(count)
    }
}

#[test]
fn collecting_immutable_objects_also_reclaims_their_metadata() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(dir.path());
    let store = db.runtime_store(limits()).unwrap();
    for round in 0..20 {
        let object = object(rk(round));
        store
            .apply(
                0,
                RuntimeBatch {
                    objects: &[object],
                    ..Default::default()
                },
            )
            .unwrap();
        assert_eq!(store.collect(1000, 0, 10).unwrap().removed, 1);
    }
    assert_eq!(store.snapshot().unwrap().usage().unwrap(), (0, 0));
    let tx = store.env.read_txn().unwrap();
    for prefix in [b"i", b"m", b"p", b"s", b"o"] {
        assert_eq!(store.db.prefix_iter(&tx, prefix).unwrap().count(), 0);
    }
}

#[test]
fn durable_retention_shares_head_dependencies_and_releases_atomically() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(dir.path());
    let store = db.runtime_store(limits()).unwrap();
    let keys = [rk(1), rk(2), rk(3)];
    let selected = RuntimeRevision {
        generation: 1,
        tip: Some([10; 32]),
        ..Default::default()
    };
    store
        .apply(
            0,
            RuntimeBatch {
                objects: &[object(keys[0]), object(keys[1]), object(keys[2])],
                heads: &[RuntimeHead {
                    id: [10; 32],
                    binding: [20; 32],
                    metadata: &[],
                    parent: None,
                    added: &keys[..2],
                    removed: &[],
                }],
                selection: Some((RuntimeRevision::default(), selected)),
                ..Default::default()
            },
        )
        .unwrap();
    let retain = [30; 32];
    let result: Result<()> = db.solution_transaction(|tx, table| {
        store.retain_in(tx, retain, &[keys[0], keys[2]])?;
        table.put(tx, b"test-intent", b"block")?;
        Err(Error::Quota("injected failure"))
    });
    assert!(result.is_err());
    assert!(db.auxiliary_read(b"test-intent").unwrap().is_none());
    assert!(!store
        .retained_in(&db.env.read_txn().unwrap(), retain)
        .unwrap());
    db.solution_transaction::<_, Error>(|tx, table| {
        store.retain_in(tx, retain, &[keys[0], keys[2]])?;
        table.put(tx, b"test-intent", b"block")?;
        Ok(())
    })
    .unwrap();
    store
        .apply(
            1,
            RuntimeBatch {
                release_heads: &[[10; 32]],
                selection: Some((
                    selected,
                    RuntimeRevision {
                        generation: 2,
                        tip: None,
                        ..selected
                    },
                )),
                ..Default::default()
            },
        )
        .unwrap();
    for _ in 0..20 {
        store.collect(3600, 0, 10).unwrap();
    }
    let snapshot = store.snapshot().unwrap();
    assert!(snapshot.object(keys[0], None).unwrap().is_some());
    assert!(snapshot.object(keys[1], None).unwrap().is_none());
    assert!(snapshot.object(keys[2], None).unwrap().is_some());
    drop(snapshot);
    db.solution_transaction::<_, Error>(|tx, table| {
        store.release_in(tx, retain)?;
        table.delete(tx, b"test-intent")?;
        Ok(())
    })
    .unwrap();
    for _ in 0..20 {
        store.collect(7200, 0, 10).unwrap();
    }
    assert_eq!(store.snapshot().unwrap().usage().unwrap(), (0, 0));
}

#[test]
fn winning_solution_objects_use_reserved_capacity_without_admitting_peer_growth() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(dir.path());
    let mut policy = limits();
    policy.max_objects = 1;
    let store = db.runtime_store(policy).unwrap();
    store
        .apply(
            0,
            RuntimeBatch {
                objects: &[object(rk(1))],
                ..Default::default()
            },
        )
        .unwrap();
    assert!(matches!(
        store.apply(
            0,
            RuntimeBatch {
                objects: &[object(rk(2))],
                ..Default::default()
            }
        ),
        Err(Error::Quota(_))
    ));
    db.solution_transaction::<_, Error>(|tx, table| {
        store.apply_in(
            tx,
            0,
            RuntimeBatch {
                objects: &[object(rk(2))],
                ..Default::default()
            },
            true,
        )?;
        store.retain_in(tx, [50; 32], &[rk(1), rk(2)])?;
        table.put(tx, b"test-solution", b"block")?;
        Ok(())
    })
    .unwrap();
    assert_eq!(store.snapshot().unwrap().usage().unwrap().0, 2);
    assert!(matches!(
        store.apply(
            0,
            RuntimeBatch {
                objects: &[object(rk(3))],
                ..Default::default()
            }
        ),
        Err(Error::Quota(_))
    ));
}

#[test]
fn collection_keeps_up_with_sustained_head_updates() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(dir.path());
    let mut bounds = limits();
    bounds.max_objects = 10000;
    bounds.max_bytes = 4 * 1024 * 1024;
    bounds.max_head_keys = 10000;
    let store = db.runtime_store(bounds).unwrap();
    let route = |n: u64| RoutingKey {
        kind: 1,
        id: digest(&n.to_be_bytes()),
    };
    let initial: Vec<_> = (0..2000u64).map(route).collect();
    let initial_objects: Vec<_> = initial.iter().copied().map(object).collect();
    let mut previous = digest(b"initial");
    store
        .apply(
            0,
            RuntimeBatch {
                objects: &initial_objects,
                heads: &[RuntimeHead {
                    id: previous,
                    binding: previous,
                    metadata: &[],
                    parent: None,
                    added: &initial,
                    removed: &[],
                }],
                ..Default::default()
            },
        )
        .unwrap();
    let mut tx = store.env.write_txn().unwrap();
    while store
        .trie
        .drain_gc(&mut tx, usize::MAX)
        .unwrap()
        .pending_nodes
        != 0
    {}
    tx.commit().unwrap();
    let mut garbage = Vec::new();
    for minute in 0..20u64 {
        for share in 0..6u64 {
            let start = 2000 + (minute * 6 + share) * 6;
            let added: Vec<_> = (start..start + 6).map(route).collect();
            let objects: Vec<_> = added.iter().copied().map(object).collect();
            let next = digest(&(start + 1000000).to_be_bytes());
            store
                .apply(
                    minute * 60,
                    RuntimeBatch {
                        objects: &objects,
                        heads: &[RuntimeHead {
                            id: next,
                            binding: next,
                            metadata: &[],
                            parent: Some(previous),
                            added: &added,
                            removed: &[],
                        }],
                        release_heads: &[previous],
                        ..Default::default()
                    },
                )
                .unwrap();
            previous = next;
        }
        store.collect_keep(minute * 60, 0, 256, &[]).unwrap();
        let live = 2 * (2000 + (minute + 1) * 36) - 1;
        let nodes = store
            .trie
            .node_count(&store.env.read_txn().unwrap())
            .unwrap();
        garbage.push(nodes - live);
    }
    assert!(
        garbage.iter().all(|nodes| *nodes < 256),
        "garbage backlog: {garbage:?}"
    );
}
