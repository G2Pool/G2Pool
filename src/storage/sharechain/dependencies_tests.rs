use super::*;
use crate::storage::sharechain::SharechainDb as Environment;
use std::path::Path;
#[derive(Default)]
struct DependencyBatch<'a> {
    manifests: &'a [DependencyManifest<'a>],
    release: &'a [Id],
}
impl DependencyDb {
    fn apply(&self, tx: &mut RwTxn<'_>, batch: &DependencyBatch<'_>) -> Result<()> {
        for manifest in batch.manifests {
            self.publish(tx, manifest)?;
        }
        for id in batch.release {
            self.release(tx, id)?;
        }
        Ok(())
    }
}
struct Fixture {
    env: Env<WithoutTls>,
    dependencies: DependencyDb,
}
impl Fixture {
    fn open(path: &Path, map_bytes: usize, pool_id: Id) -> Result<Self> {
        let env = Environment::open(path, map_bytes, pool_id)?.env;
        let mut tx = env.write_txn()?;
        let dependencies = DependencyDb::open_runtime(&env, &mut tx)?;
        tx.commit()?;
        Ok(Self { env, dependencies })
    }
    fn read(&self) -> Result<Snapshot<'_>> {
        Ok(Snapshot {
            db: self,
            tx: self.env.read_txn()?,
        })
    }
    fn drain_dependency_gc(&self, max_nodes: usize) -> Result<DependencyGcStats> {
        let mut tx = self.env.write_txn()?;
        let result = self.dependencies.drain_gc(&mut tx, max_nodes)?;
        tx.commit()?;
        Ok(result)
    }
}
struct Snapshot<'a> {
    db: &'a Fixture,
    tx: RoTxn<'a, WithoutTls>,
}
impl Snapshot<'_> {
    fn dependencies(&self, manifest: &Id) -> Result<Vec<RoutingKey>> {
        self.db.dependencies.resolve(&self.tx, manifest)
    }
    fn protected(&self, key: RoutingKey) -> Result<bool> {
        self.db.dependencies.protected_key(&self.tx, key)
    }
    fn dependency_root(&self, id: &Id) -> Result<Id> {
        self.db.dependencies.root(&self.tx, id)
    }
    fn dependency_retained(&self, id: &Id) -> Result<bool> {
        self.db.dependencies.retained(&self.tx, id)
    }
}

fn id(n: u64) -> Id {
    let mut id = [0; 32];
    id[..8].copy_from_slice(&n.to_be_bytes());
    id
}
fn key(n: u64) -> RoutingKey {
    RoutingKey { kind: 7, id: id(n) }
}
fn apply(db: &Fixture, batch: &DependencyBatch<'_>) -> Result<()> {
    let mut tx = db.env.write_txn()?;
    db.dependencies.apply(&mut tx, batch)?;
    db.dependencies.drain_gc(&mut tx, usize::MAX)?;
    tx.commit()?;
    Ok(())
}
fn apply_pending(db: &Fixture, batch: &DependencyBatch<'_>) -> Result<()> {
    let mut tx = db.env.write_txn()?;
    db.dependencies.apply(&mut tx, batch)?;
    tx.commit()?;
    Ok(())
}
fn seed_pending(db: &Fixture, manifest: u64, keys: &[RoutingKey]) {
    apply_pending(
        db,
        &DependencyBatch {
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
fn finish_gc(db: &Fixture, budget: usize) {
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
    let db = Fixture::open(dir.path(), 1 << 26, id(999)).unwrap();
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
    let db = Fixture::open(dir.path(), 1 << 26, id(999)).unwrap();
    assert!(db.drain_dependency_gc(0).unwrap().pending_nodes > 0);
    finish_gc(&db, 5);
    assert_refs(&db);
    let tx = db.env.read_txn().unwrap();
    assert_eq!(db.dependencies.nodes.len(&tx).unwrap(), 0);
}
#[test]
fn pending_root_reattachment_and_repeated_release_do_not_double_drop_edges() {
    let dir = tempfile::tempdir().unwrap();
    let db = Fixture::open(dir.path(), 1 << 24, id(999)).unwrap();
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
fn queued_shared_forks_protect_keys_until_their_leaf_is_reclaimed() {
    let dir = tempfile::tempdir().unwrap();
    let db = Fixture::open(dir.path(), 1 << 24, id(999)).unwrap();
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
        },
    )
    .unwrap();
    assert!(db.read().unwrap().protected(key(2)).unwrap());
    finish_gc(&db, 1);
    assert_eq!(
        db.read().unwrap().dependencies(&id(2)).unwrap(),
        vec![key(1)]
    );
    assert!(db.read().unwrap().protected(key(1)).unwrap());
    assert!(!db.read().unwrap().protected(key(2)).unwrap());
    assert_refs(&db);
}
#[test]
fn corrupt_gc_children_roll_back_whole_drain() {
    let dir = tempfile::tempdir().unwrap();
    let db = Fixture::open(dir.path(), 1 << 24, id(999)).unwrap();
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
    assert!(db.dependencies.protected_key(&tx, key(1)).unwrap());
}
#[test]
fn fork_retention_and_invalid_deltas_are_atomic_across_snapshots() {
    let dir = tempfile::tempdir().unwrap();
    let db = Fixture::open(dir.path(), 1 << 24, id(999)).unwrap();
    apply(
        &db,
        &DependencyBatch {
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
            manifests: &[DependencyManifest {
                id: id(4),
                parent: Some(id(1)),
                added: &[],
                removed: &[key(2)]
            }],
            release: &[id(1)],
        }
    )
    .is_err());
    assert!(db.read().unwrap().dependency_retained(&id(1)).unwrap());
    assert!(db.read().unwrap().dependencies(&id(4)).is_err());
    apply(
        &db,
        &DependencyBatch {
            release: &[id(1), id(3)],
            ..Default::default()
        },
    )
    .unwrap();
    assert!(!db.read().unwrap().protected(key(1)).unwrap());
    assert!(db.read().unwrap().protected(key(2)).unwrap());
    assert_eq!(old.dependencies(&id(3)).unwrap(), vec![key(1)]);
    assert!(old.protected(key(1)).unwrap());
    apply(
        &db,
        &DependencyBatch {
            release: &[id(2)],
            ..Default::default()
        },
    )
    .unwrap();
    assert!(!db.read().unwrap().protected(key(2)).unwrap());
    assert_refs(&db);
    assert_eq!(
        db.dependencies
            .manifest_digests
            .len(&db.env.read_txn().unwrap())
            .unwrap(),
        0
    );
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
fn roots_require_no_replay_and_reclaim_removed_ancestor_keys() {
    let dir = tempfile::tempdir().unwrap();
    let db = Fixture::open(dir.path(), 1 << 24, id(999)).unwrap();
    apply(
        &db,
        &DependencyBatch {
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
            },
        )
        .unwrap();
        let snapshot = db.read().unwrap();
        assert_eq!(snapshot.dependency_root(&id(n)).unwrap(), [0; 32]);
        assert!(snapshot.dependencies(&id(n)).unwrap().is_empty());
    }
    assert!(db.read().unwrap().dependencies(&id(1)).is_err());
    assert!(!db.read().unwrap().protected(key(1)).unwrap());
}
#[test]
fn corrupt_shape_prefix_and_edges_fail_before_traversal() {
    let dir = tempfile::tempdir().unwrap();
    let db = Fixture::open(dir.path(), 1 << 24, id(999)).unwrap();
    apply(
        &db,
        &DependencyBatch {
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
    bad_prefix.key = key(1);
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
        let malformed_id = node_hash(&malformed.encode());
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
fn assert_refs(db: &Fixture) {
    let tx = db.env.read_txn().unwrap();
    let dep = &db.dependencies;
    let mut expected = std::collections::BTreeMap::<Id, u64>::new();
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
        assert_eq!(id, node_hash(value));
        let node = TrieNode::decode(value).unwrap();
        for child in node.children {
            if child != [0; 32] {
                *expected.entry(child).or_default() += 1;
            }
        }
    }
    assert_eq!(dep.refs.len(&tx).unwrap(), expected.len() as u64);
    assert_eq!(dep.nodes.len(&tx).unwrap(), expected.len() as u64);
    for (id, n) in expected {
        assert_eq!(count(dep.refs, &tx, &id).unwrap(), n);
    }
}
#[test]
fn path_copy_growth_and_structural_counts_are_bounded() {
    let dir = tempfile::tempdir().unwrap();
    let db = Fixture::open(dir.path(), 1 << 26, id(999)).unwrap();
    let keys: Vec<_> = (1..=96).map(key).collect();
    apply(
        &db,
        &DependencyBatch {
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
    let db = Fixture::open(dir.path(), 1 << 26, id(999)).unwrap();
    assert_refs(&db);
}
#[test]
fn patricia_roots_are_canonical_and_deletions_collapse_paths() {
    let dir = tempfile::tempdir().unwrap();
    let db = Fixture::open(dir.path(), 1 << 26, id(999)).unwrap();
    let keys: Vec<_> = (0u64..40)
        .map(|n| RoutingKey {
            kind: (n % 3) as u8,
            id: node_hash(&n.to_be_bytes()),
        })
        .collect();
    apply(
        &db,
        &DependencyBatch {
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
impl DependencyDb {
    pub(crate) fn node_count(&self, tx: &RoTxn<'_, WithoutTls>) -> Result<u64> {
        Ok(self.nodes.len(tx)?)
    }
}

impl DependencyDb {
    pub(crate) fn contains(
        &self,
        tx: &RoTxn<'_, WithoutTls>,
        id: &Id,
        key: RoutingKey,
    ) -> Result<bool> {
        self.has(tx, &self.root(tx, id)?, key)
    }
}
