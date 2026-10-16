use super::*;
use crate::storage::sharechain::{Header, Record, SharechainDb};
fn id(n: u64) -> Id {
    let mut id = [0; 32];
    id[..8].copy_from_slice(&n.to_be_bytes());
    id
}
fn key(n: u64) -> DependencyKey {
    DependencyKey { kind: 7, id: id(n) }
}
fn apply(db: &SharechainDb, batch: &DependencyBatch<'_>) -> Result<()> {
    let mut tx = db.env.write_txn()?;
    let staged = DependencyBatch {
        objects: batch.objects,
        manifests: batch.manifests,
        retain: batch.retain,
        release: batch.release,
        prune: &[],
    };
    db.dependencies
        .apply(&mut tx, &staged, db.max_record_bytes)?;
    db.dependencies.drain_gc(&mut tx, usize::MAX)?;
    db.dependencies.apply(
        &mut tx,
        &DependencyBatch {
            prune: batch.prune,
            ..Default::default()
        },
        db.max_record_bytes,
    )?;
    tx.commit()?;
    Ok(())
}
fn apply_pending(db: &SharechainDb, batch: &DependencyBatch<'_>) -> Result<()> {
    let mut tx = db.env.write_txn()?;
    db.dependencies.apply(&mut tx, batch, db.max_record_bytes)?;
    tx.commit()?;
    Ok(())
}
fn seed_pending(db: &SharechainDb, manifest: u64, keys: &[DependencyKey]) {
    let objects: Vec<_> = keys
        .iter()
        .map(|key| DependencyObject {
            key: *key,
            bytes: b"body",
        })
        .collect();
    apply_pending(
        db,
        &DependencyBatch {
            objects: &objects,
            manifests: &[DependencyManifest {
                id: id(manifest),
                parent: None,
                added: keys,
                removed: &[],
            }],
            ..Default::default()
        },
    )
    .unwrap();
}
fn finish_gc(db: &SharechainDb, budget: usize) {
    loop {
        let stats = db.drain_dependency_gc(budget).unwrap();
        assert!(stats.processed_nodes <= budget);
        assert!(stats.reclaimed_nodes <= stats.processed_nodes);
        if stats.pending_nodes == 0 {
            break;
        }
    }
}
#[test]
fn durable_gc_is_bounded_and_resumes_after_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let db = SharechainDb::open(dir.path(), 1 << 26, 4096, id(999)).unwrap();
    let keys: Vec<_> = (1..=96).map(key).collect();
    seed_pending(&db, 1000, &keys);
    finish_gc(&db, 7);
    let before = db
        .dependencies
        .nodes
        .len(&db.env.read_txn().unwrap())
        .unwrap();
    apply_pending(
        &db,
        &DependencyBatch {
            release: &[id(1000)],
            ..Default::default()
        },
    )
    .unwrap();
    assert_eq!(
        db.dependencies
            .nodes
            .len(&db.env.read_txn().unwrap())
            .unwrap(),
        before
    );
    assert_eq!(db.drain_dependency_gc(0).unwrap().pending_nodes, 1);
    let stats = db.drain_dependency_gc(3).unwrap();
    assert_eq!(stats.processed_nodes, 3);
    assert_eq!(stats.reclaimed_nodes, 3);
    assert!(stats.pending_nodes > 0);
    drop(db);
    let db = SharechainDb::open(dir.path(), 1 << 26, 4096, id(999)).unwrap();
    assert!(db.drain_dependency_gc(0).unwrap().pending_nodes > 0);
    finish_gc(&db, 5);
    assert_refs(&db);
    let tx = db.env.read_txn().unwrap();
    assert_eq!(db.dependencies.nodes.len(&tx).unwrap(), 0);
    assert_eq!(db.dependencies.objects.len(&tx).unwrap(), 0);
}
#[test]
fn pending_root_reattachment_and_repeated_release_do_not_double_drop_edges() {
    let dir = tempfile::tempdir().unwrap();
    let db = SharechainDb::open(dir.path(), 1 << 24, 1024, id(999)).unwrap();
    let keys = [key(1), key(2)];
    seed_pending(&db, 1, &keys);
    finish_gc(&db, 4);
    let root = db.read().unwrap().dependency_root(&id(1)).unwrap();
    apply_pending(
        &db,
        &DependencyBatch {
            release: &[id(1)],
            ..Default::default()
        },
    )
    .unwrap();
    seed_pending(&db, 1, &keys);
    assert_eq!(db.read().unwrap().dependency_root(&id(1)).unwrap(), root);
    finish_gc(&db, 1);
    assert_eq!(db.read().unwrap().dependencies(&id(1)).unwrap(), keys);
    assert_refs(&db);
    apply_pending(
        &db,
        &DependencyBatch {
            release: &[id(1)],
            ..Default::default()
        },
    )
    .unwrap();
    seed_pending(&db, 1, &keys);
    apply_pending(
        &db,
        &DependencyBatch {
            release: &[id(1)],
            ..Default::default()
        },
    )
    .unwrap();
    finish_gc(&db, 1);
    assert_refs(&db);
    assert_eq!(
        db.dependencies
            .nodes
            .len(&db.env.read_txn().unwrap())
            .unwrap(),
        0
    );
}
#[test]
fn queued_shared_forks_keep_live_objects_and_prune_protection() {
    let dir = tempfile::tempdir().unwrap();
    let db = SharechainDb::open(dir.path(), 1 << 24, 1024, id(999)).unwrap();
    seed_pending(&db, 1, &[key(1), key(2)]);
    finish_gc(&db, 4);
    apply_pending(
        &db,
        &DependencyBatch {
            manifests: &[DependencyManifest {
                id: id(2),
                parent: Some(id(1)),
                added: &[],
                removed: &[key(2)],
            }],
            release: &[id(1)],
            ..Default::default()
        },
    )
    .unwrap();
    assert!(matches!(
        apply_pending(
            &db,
            &DependencyBatch {
                prune: &[key(2)],
                ..Default::default()
            }
        ),
        Err(Error::ProtectedBody)
    ));
    assert!(db
        .read()
        .unwrap()
        .dependency_object(key(2))
        .unwrap()
        .is_some());
    finish_gc(&db, 1);
    assert_eq!(
        db.read().unwrap().dependencies(&id(2)).unwrap(),
        vec![key(1)]
    );
    assert!(db
        .read()
        .unwrap()
        .dependency_object(key(1))
        .unwrap()
        .is_some());
    assert!(db
        .read()
        .unwrap()
        .dependency_object(key(2))
        .unwrap()
        .is_none());
    assert_refs(&db);
}
#[test]
fn corrupt_gc_children_roll_back_whole_drain() {
    let dir = tempfile::tempdir().unwrap();
    let db = SharechainDb::open(dir.path(), 1 << 24, 1024, id(999)).unwrap();
    seed_pending(&db, 1, &[key(1), key(2)]);
    finish_gc(&db, 8);
    let root = db.read().unwrap().dependency_root(&id(1)).unwrap();
    apply_pending(
        &db,
        &DependencyBatch {
            release: &[id(1)],
            ..Default::default()
        },
    )
    .unwrap();
    let mut tx = db.env.write_txn().unwrap();
    let child = db.dependencies.node(&tx, &root).unwrap().children[0];
    db.dependencies.refs.delete(&mut tx, &child).unwrap();
    tx.commit().unwrap();
    let before = db.drain_dependency_gc(0).unwrap();
    assert!(db.drain_dependency_gc(8).is_err());
    assert_eq!(db.drain_dependency_gc(0).unwrap(), before);
    let tx = db.env.read_txn().unwrap();
    assert!(db.dependencies.nodes.get(&tx, &root).unwrap().is_some());
    assert_eq!(db.dependencies.nodes.len(&tx).unwrap(), 3);
    assert_eq!(
        count(db.dependencies.object_refs, &tx, &key(1).encode()).unwrap(),
        1
    );
}
#[test]
fn fork_retention_and_failed_pruning_are_atomic() {
    let dir = tempfile::tempdir().unwrap();
    let db = SharechainDb::open(dir.path(), 1 << 24, 1024, id(999)).unwrap();
    apply(
        &db,
        &DependencyBatch {
            objects: &[
                DependencyObject {
                    key: key(1),
                    bytes: b"one",
                },
                DependencyObject {
                    key: key(2),
                    bytes: b"two",
                },
            ],
            manifests: &[
                DependencyManifest {
                    id: id(1),
                    parent: None,
                    added: &[key(1)],
                    removed: &[],
                },
                DependencyManifest {
                    id: id(2),
                    parent: Some(id(1)),
                    added: &[key(2)],
                    removed: &[key(1)],
                },
                DependencyManifest {
                    id: id(3),
                    parent: Some(id(1)),
                    added: &[],
                    removed: &[],
                },
            ],
            ..Default::default()
        },
    )
    .unwrap();
    let old = db.read().unwrap();
    assert_eq!(old.dependencies(&id(2)).unwrap(), vec![key(2)]);
    assert_eq!(old.dependencies(&id(3)).unwrap(), vec![key(1)]);
    assert!(apply(
        &db,
        &DependencyBatch {
            release: &[id(1)],
            prune: &[key(1)],
            ..Default::default()
        }
    )
    .is_err());
    assert!(db.read().unwrap().dependency_retained(&id(1)).unwrap());
    apply(
        &db,
        &DependencyBatch {
            release: &[id(1), id(3)],
            ..Default::default()
        },
    )
    .unwrap();
    apply(
        &db,
        &DependencyBatch {
            prune: &[key(1)],
            ..Default::default()
        },
    )
    .unwrap();
    apply(
        &db,
        &DependencyBatch {
            release: &[id(2)],
            prune: &[key(1), key(2)],
            ..Default::default()
        },
    )
    .unwrap();
    assert!(db
        .read()
        .unwrap()
        .dependency_object(key(1))
        .unwrap()
        .is_none());
    assert_eq!(
        old.dependency_object(key(1)).unwrap(),
        Some(b"one".as_slice())
    );
    assert!(apply(
        &db,
        &DependencyBatch {
            objects: &[DependencyObject {
                key: key(1),
                bytes: b"changed"
            }],
            ..Default::default()
        }
    )
    .is_err());
    assert!(apply(
        &db,
        &DependencyBatch {
            manifests: &[DependencyManifest {
                id: id(1),
                parent: None,
                added: &[],
                removed: &[]
            }],
            ..Default::default()
        }
    )
    .is_err());
    assert!(apply(
        &db,
        &DependencyBatch {
            release: &[id(2)],
            ..Default::default()
        }
    )
    .is_err());
}
#[test]
fn roots_require_no_replay_and_reclaim_removed_ancestor_objects() {
    let dir = tempfile::tempdir().unwrap();
    let db = SharechainDb::open(dir.path(), 1 << 24, 1024, id(999)).unwrap();
    apply(
        &db,
        &DependencyBatch {
            objects: &[DependencyObject {
                key: key(1),
                bytes: b"one",
            }],
            manifests: &[DependencyManifest {
                id: id(1),
                parent: None,
                added: &[key(1)],
                removed: &[],
            }],
            ..Default::default()
        },
    )
    .unwrap();
    for n in 2..=130 {
        let removed = if n == 2 { vec![key(1)] } else { Vec::new() };
        apply(
            &db,
            &DependencyBatch {
                manifests: &[DependencyManifest {
                    id: id(n),
                    parent: Some(id(n - 1)),
                    added: &[],
                    removed: &removed,
                }],
                release: &[id(n - 1)],
                ..Default::default()
            },
        )
        .unwrap();
        let snapshot = db.read().unwrap();
        assert_eq!(snapshot.dependency_root(&id(n)).unwrap(), [0; 32]);
        assert!(snapshot.dependencies(&id(n)).unwrap().is_empty());
    }
    assert!(db.read().unwrap().dependencies(&id(1)).is_err());
    apply(
        &db,
        &DependencyBatch {
            prune: &[key(1)],
            ..Default::default()
        },
    )
    .unwrap();
    assert!(db
        .read()
        .unwrap()
        .dependency_object(key(1))
        .unwrap()
        .is_none());
}
#[test]
fn corrupt_shape_prefix_and_edges_fail_before_traversal() {
    let dir = tempfile::tempdir().unwrap();
    let db = SharechainDb::open(dir.path(), 1 << 24, 1024, id(999)).unwrap();
    apply(
        &db,
        &DependencyBatch {
            objects: &[
                DependencyObject {
                    key: key(1),
                    bytes: b"1",
                },
                DependencyObject {
                    key: key(2),
                    bytes: b"2",
                },
            ],
            manifests: &[DependencyManifest {
                id: id(1),
                parent: None,
                added: &[key(1), key(2)],
                removed: &[],
            }],
            ..Default::default()
        },
    )
    .unwrap();
    let tx = db.env.read_txn().unwrap();
    let root = db.dependencies.root(&tx, &id(1)).unwrap();
    let original = db.dependencies.node(&tx, &root).unwrap();
    drop(tx);
    let mut reversed = original.clone();
    reversed.children.reverse();
    let mut cycle = original.clone();
    cycle.children[0] = root;
    let mut bad_prefix = original.clone();
    bad_prefix.key = Some(key(1));
    let mut bad_depth = original.clone();
    bad_depth.depth = KEY_BITS as u16;
    for malformed in [reversed, cycle, bad_prefix, bad_depth] {
        let mut tx = db.env.write_txn().unwrap();
        db.dependencies
            .nodes
            .put(&mut tx, &root, &malformed.encode())
            .unwrap();
        assert!(db.dependencies.resolve(&tx, &id(1)).is_err());
        assert!(db.dependencies.has(&tx, &root, key(1)).is_err());
        db.dependencies
            .nodes
            .put(&mut tx, &root, &original.encode())
            .unwrap();
        let malformed_id = hash(&malformed.encode());
        db.dependencies
            .nodes
            .put(&mut tx, &malformed_id, &malformed.encode())
            .unwrap();
        db.dependencies
            .roots
            .put(&mut tx, &id(1), &malformed_id)
            .unwrap();
        assert!(db.dependencies.resolve(&tx, &id(1)).is_err());
        assert!(db.dependencies.has(&tx, &malformed_id, key(1)).is_err());
    }
    assert_refs(&db);
}
#[test]
fn incompatible_checkpoint_schema_is_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let db = SharechainDb::open(dir.path(), 1 << 24, 1024, id(999)).unwrap();
    let mut tx = db.env.write_txn().unwrap();
    db.meta
        .put(&mut tx, b"schema", b"sharechain-index-3")
        .unwrap();
    tx.commit().unwrap();
    drop(db);
    assert!(matches!(
        SharechainDb::open(dir.path(), 1 << 24, 1024, id(999)),
        Err(Error::Invalid("schema"))
    ));
}
fn assert_refs(db: &SharechainDb) {
    let tx = db.env.read_txn().unwrap();
    let dep = &db.dependencies;
    let mut expected = std::collections::BTreeMap::<Id, u64>::new();
    let mut objects = std::collections::BTreeMap::<DependencyKey, u64>::new();
    for row in dep.roots.iter(&tx).unwrap() {
        let (_, value) = row.unwrap();
        assert_eq!(value.len(), 32);
        let root: Id = value.try_into().unwrap();
        if root != [0; 32] {
            *expected.entry(root).or_default() += 1;
        }
    }
    for row in dep.nodes.iter(&tx).unwrap() {
        let (id, value) = row.unwrap();
        assert!(value.len() <= 101);
        assert_eq!(id, hash(value));
        let node = TrieNode::decode(value).unwrap();
        for child in node.children {
            if child != [0; 32] {
                *expected.entry(child).or_default() += 1;
            }
        }
        if !node.branch {
            let key = node.key.unwrap();
            *objects.entry(key).or_default() += 1;
        }
    }
    assert_eq!(dep.refs.len(&tx).unwrap(), expected.len() as u64);
    assert_eq!(dep.nodes.len(&tx).unwrap(), expected.len() as u64);
    for (id, n) in expected {
        assert_eq!(count(dep.refs, &tx, &id).unwrap(), n);
    }
    assert_eq!(dep.object_refs.len(&tx).unwrap(), objects.len() as u64);
    for (key, n) in objects {
        assert_eq!(count(dep.object_refs, &tx, &key.encode()).unwrap(), n);
    }
}
#[test]
fn path_copy_growth_and_structural_counts_are_bounded() {
    let dir = tempfile::tempdir().unwrap();
    let db = SharechainDb::open(dir.path(), 1 << 26, 4096, id(999)).unwrap();
    let keys: Vec<_> = (1..=96).map(key).collect();
    let objects: Vec<_> = keys
        .iter()
        .map(|key| DependencyObject {
            key: *key,
            bytes: b"body",
        })
        .collect();
    apply(
        &db,
        &DependencyBatch {
            objects: &objects,
            manifests: &[DependencyManifest {
                id: id(1000),
                parent: None,
                added: &keys,
                removed: &[],
            }],
            ..Default::default()
        },
    )
    .unwrap();
    assert_refs(&db);
    let before = db
        .dependencies
        .nodes
        .len(&db.env.read_txn().unwrap())
        .unwrap();
    apply(
        &db,
        &DependencyBatch {
            objects: &[DependencyObject {
                key: key(97),
                bytes: b"next",
            }],
            manifests: &[DependencyManifest {
                id: id(1001),
                parent: Some(id(1000)),
                added: &[key(97)],
                removed: &[],
            }],
            retain: &[id(1000), id(1000)],
            ..Default::default()
        },
    )
    .unwrap();
    let after = db
        .dependencies
        .nodes
        .len(&db.env.read_txn().unwrap())
        .unwrap();
    assert_eq!(before, (2 * keys.len() - 1) as u64);
    assert!(after <= before + KEY_BITS as u64 + 2);
    assert_refs(&db);
    apply(
        &db,
        &DependencyBatch {
            manifests: &[DependencyManifest {
                id: id(1001),
                parent: Some(id(1000)),
                added: &[key(97)],
                removed: &[],
            }],
            ..Default::default()
        },
    )
    .unwrap();
    assert_refs(&db);
    apply(
        &db,
        &DependencyBatch {
            release: &[id(1000), id(1001)],
            ..Default::default()
        },
    )
    .unwrap();
    assert_refs(&db);
    assert_eq!(
        db.dependencies
            .nodes
            .len(&db.env.read_txn().unwrap())
            .unwrap(),
        0
    );
    drop(db);
    let db = SharechainDb::open(dir.path(), 1 << 26, 4096, id(999)).unwrap();
    assert_refs(&db);
}
#[test]
fn patricia_roots_are_canonical_and_deletions_collapse_paths() {
    let dir = tempfile::tempdir().unwrap();
    let db = SharechainDb::open(dir.path(), 1 << 26, 4096, id(999)).unwrap();
    let keys: Vec<_> = (0u64..40)
        .map(|n| DependencyKey {
            kind: (n % 3) as u8,
            id: hash(&n.to_be_bytes()),
        })
        .collect();
    let objects: Vec<_> = keys
        .iter()
        .map(|key| DependencyObject {
            key: *key,
            bytes: b"x",
        })
        .collect();
    apply(
        &db,
        &DependencyBatch {
            objects: &objects,
            manifests: &[DependencyManifest {
                id: id(1000),
                parent: None,
                added: &keys,
                removed: &[],
            }],
            ..Default::default()
        },
    )
    .unwrap();
    let manifests: Vec<_> = keys
        .iter()
        .rev()
        .enumerate()
        .map(|(n, key)| DependencyManifest {
            id: id(2000 + n as u64),
            parent: if n == 0 {
                None
            } else {
                Some(id(1999 + n as u64))
            },
            added: std::slice::from_ref(key),
            removed: &[],
        })
        .collect();
    let releases: Vec<_> = (2000..2039).map(id).collect();
    apply(
        &db,
        &DependencyBatch {
            manifests: &manifests,
            release: &releases,
            ..Default::default()
        },
    )
    .unwrap();
    let tx = db.env.read_txn().unwrap();
    assert_eq!(
        db.dependencies.root(&tx, &id(1000)).unwrap(),
        db.dependencies.root(&tx, &id(2039)).unwrap()
    );
    assert_eq!(db.dependencies.nodes.len(&tx).unwrap(), 79);
    drop(tx);
    assert_refs(&db);
    let mut parent = id(2039);
    apply(
        &db,
        &DependencyBatch {
            release: &[id(1000)],
            ..Default::default()
        },
    )
    .unwrap();
    for (n, key) in keys.iter().enumerate() {
        let next = id(3000 + n as u64);
        apply(
            &db,
            &DependencyBatch {
                manifests: &[DependencyManifest {
                    id: next,
                    parent: Some(parent),
                    added: &[],
                    removed: std::slice::from_ref(key),
                }],
                release: &[parent],
                ..Default::default()
            },
        )
        .unwrap();
        parent = next;
        let tx = db.env.read_txn().unwrap();
        let remaining = keys.len() - n - 1;
        assert_eq!(
            db.dependencies.nodes.len(&tx).unwrap(),
            if remaining == 0 {
                0
            } else {
                (2 * remaining - 1) as u64
            }
        );
        drop(tx);
        assert_refs(&db);
    }
}
#[test]
fn object_restore_and_manifest_digest_separate_delta_boundaries() {
    let dir = tempfile::tempdir().unwrap();
    let db = SharechainDb::open(dir.path(), 1 << 24, 1024, id(999)).unwrap();
    apply(
        &db,
        &DependencyBatch {
            objects: &[DependencyObject {
                key: key(1),
                bytes: b"one",
            }],
            manifests: &[DependencyManifest {
                id: id(1),
                parent: None,
                added: &[key(1)],
                removed: &[],
            }],
            ..Default::default()
        },
    )
    .unwrap();
    apply(
        &db,
        &DependencyBatch {
            release: &[id(1)],
            prune: &[key(1)],
            ..Default::default()
        },
    )
    .unwrap();
    assert!(apply(
        &db,
        &DependencyBatch {
            manifests: &[DependencyManifest {
                id: id(1),
                parent: None,
                added: &[],
                removed: &[key(1)]
            }],
            ..Default::default()
        }
    )
    .is_err());
    apply(
        &db,
        &DependencyBatch {
            objects: &[DependencyObject {
                key: key(1),
                bytes: b"one",
            }],
            manifests: &[DependencyManifest {
                id: id(1),
                parent: None,
                added: &[key(1)],
                removed: &[],
            }],
            ..Default::default()
        },
    )
    .unwrap();
    assert_eq!(
        db.read().unwrap().dependencies(&id(1)).unwrap(),
        vec![key(1)]
    );
    assert_refs(&db);
}
#[test]
fn invalid_delta_cycles_and_missing_objects_leave_publication_unchanged() {
    let dir = tempfile::tempdir().unwrap();
    let db = SharechainDb::open(dir.path(), 1 << 24, 1024, id(999)).unwrap();
    let record = Record {
        id: id(1),
        header: Header {
            parent: None,
            height: 0,
            work: 1,
            cumulative_work: 1,
            anchor: id(9),
            parent_state_root: [0; 32],
            state_root: [0; 32],
        },
        body: b"body",
        delta: &[],
        state: &[],
    };
    let batch = DependencyBatch {
        manifests: &[DependencyManifest {
            id: id(1),
            parent: None,
            added: &[key(1)],
            removed: &[],
        }],
        ..Default::default()
    };
    assert!(db
        .publish_validated_with_dependencies(0, &[record], id(1), id(1), &batch)
        .is_err());
    let snapshot = db.read().unwrap();
    assert!(snapshot.publication().unwrap().is_none());
    assert!(snapshot.header(&id(1)).unwrap().is_none());
    drop(snapshot);
    assert!(apply(
        &db,
        &DependencyBatch {
            manifests: &[DependencyManifest {
                id: id(1),
                parent: Some(id(1)),
                added: &[],
                removed: &[]
            }],
            ..Default::default()
        }
    )
    .is_err());
    assert!(apply(
        &db,
        &DependencyBatch {
            manifests: &[
                DependencyManifest {
                    id: id(1),
                    parent: Some(id(2)),
                    added: &[],
                    removed: &[]
                },
                DependencyManifest {
                    id: id(2),
                    parent: Some(id(1)),
                    added: &[],
                    removed: &[]
                }
            ],
            ..Default::default()
        }
    )
    .is_err());
    apply(
        &db,
        &DependencyBatch {
            objects: &[DependencyObject {
                key: key(1),
                bytes: b"one",
            }],
            manifests: &[DependencyManifest {
                id: id(1),
                parent: None,
                added: &[key(1)],
                removed: &[],
            }],
            ..Default::default()
        },
    )
    .unwrap();
    for (added, removed) in [
        (vec![key(1)], vec![]),
        (vec![], vec![key(2)]),
        (vec![key(2), key(2)], vec![]),
        (vec![key(1)], vec![key(1)]),
    ] {
        assert!(apply(
            &db,
            &DependencyBatch {
                manifests: &[DependencyManifest {
                    id: id(2),
                    parent: Some(id(1)),
                    added: &added,
                    removed: &removed
                }],
                ..Default::default()
            }
        )
        .is_err());
        assert!(db.read().unwrap().dependencies(&id(2)).is_err());
    }
}

impl DependencyDb {
    pub(crate) fn node_count(&self, tx: &RoTxn<'_, WithoutTls>) -> Result<u64> {
        Ok(self.nodes.len(tx)?)
    }
}
