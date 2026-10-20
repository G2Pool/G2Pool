use super::*;
use tempfile::tempdir;

#[test]
fn routing_namespace_updates_are_atomic_and_isolated() {
    let dir = tempdir().unwrap();
    let db = SharechainDb::open(dir.path(), 1 << 24, 1024, [42; 32]).unwrap();
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

    kv.write_compact_cursor([1; 32], "8.8.8.8:3417", Some(b"cursor"))
        .unwrap();
    assert_eq!(
        kv.read_compact_cursor([1; 32], "8.8.8.8:3417").unwrap(),
        Some(b"cursor".to_vec())
    );
    assert!(kv
        .write_compact_cursor([1; 32], "8.8.8.8:3417", Some(&[0; 513]))
        .is_err());
    assert_eq!(
        kv.read_compact_cursor([2; 32], "8.8.8.8:3417").unwrap(),
        None
    );
    kv.write_compact_cursor([1; 32], "8.8.8.8:3417", None)
        .unwrap();
    assert_eq!(kv.read_peers().unwrap(), desired);
}

#[test]
fn peer_replacement_discards_only_orphaned_sync_cursors() {
    let dir = tempdir().unwrap();
    let db = SharechainDb::open(dir.path(), 1 << 24, 1024, [42; 32]).unwrap();
    let protocol = [42; 32];
    db.write_compact_cursor(protocol, "8.8.8.8:3417", Some(b"kept"))
        .unwrap();
    db.write_compact_cursor(protocol, "9.9.9.9:3417", Some(b"discarded"))
        .unwrap();
    db.auxiliary_write(b"wallet/sentinel", b"preserved")
        .unwrap();
    db.replace_peers(&[(b"peers/8.8.8.8:3417".to_vec(), b"peer".to_vec())])
        .unwrap();
    assert_eq!(
        db.read_compact_cursor(protocol, "8.8.8.8:3417").unwrap(),
        Some(b"kept".to_vec())
    );
    assert_eq!(
        db.read_compact_cursor(protocol, "9.9.9.9:3417").unwrap(),
        None
    );
    assert_eq!(
        db.auxiliary_read(b"wallet/sentinel").unwrap(),
        Some(b"preserved".to_vec())
    );
    db.replace_peers(&[]).unwrap();
    assert_eq!(
        db.read_compact_cursor(protocol, "8.8.8.8:3417").unwrap(),
        None
    );
}

fn id(n: u8) -> Id {
    [n; 32]
}
fn rec(n: u8, parent: Option<(u8, u64, u128)>) -> (Id, Header, Vec<u8>, Vec<u8>) {
    let (height, cumulative, p) = parent
        .map(|(p, h, c)| (h + 1, c + 10, Some(id(p))))
        .unwrap_or((0, 10, None));
    (
        id(n),
        Header {
            parent: p,
            height,
            work: 10,
            cumulative_work: cumulative,
            anchor: id(90),
            parent_state_root: parent.map(|(p, _, _)| id(p + 100)).unwrap_or([0; 32]),
            state_root: id(n + 100),
        },
        vec![n],
        vec![n + 1],
    )
}

#[test]
fn durable_chain_locates_fork_and_prunes_body() {
    let dir = tempdir().unwrap();
    let db = SharechainDb::open(dir.path(), 1 << 24, 1024, id(88)).unwrap();
    let (r, rh, rb, rd) = rec(1, None);
    let (a, ah, ab, ad) = rec(2, Some((1, 0, 10)));
    let (b, bh, bb, bd) = rec(3, Some((2, 1, 20)));
    let (c, ch, cb, cd) = rec(4, Some((1, 0, 10)));
    let records = [
        Record {
            id: r,
            header: rh,
            body: &rb,
            delta: &rd,
            state: &[],
        },
        Record {
            id: a,
            header: ah,
            body: &ab,
            delta: &ad,
            state: &[],
        },
        Record {
            id: b,
            header: bh,
            body: &bb,
            delta: &bd,
            state: &[],
        },
    ];
    assert_eq!(db.publish_validated(0, &records, b).unwrap().generation, 1);
    let fork = [Record {
        id: c,
        header: ch,
        body: &cb,
        delta: &cd,
        state: &[],
    }];
    assert_eq!(db.publish_validated(1, &fork, c).unwrap().head, c);
    let snap = db.read().unwrap();
    assert!(snap.compare_cumulative(b, c).unwrap().is_some());
    assert_eq!(snap.header(&b).unwrap().unwrap().cumulative_work, 30);
    assert_eq!(snap.locator(b, 8).unwrap().first(), Some(&b));
    drop(snap);
    assert_eq!(db.prune_bodies(&[r, a]).unwrap(), 2);
    let snap = db.read().unwrap();
    assert!(snap.body(&r).unwrap().is_none());
    assert_eq!(snap.delta(&r).unwrap(), Some(&rd[..]));
}

#[test]
fn stale_generation_and_failed_batches_leave_the_prior_snapshot_intact() {
    let dir = tempdir().unwrap();
    let db = SharechainDb::open(dir.path(), 1 << 24, 1024, id(88)).unwrap();
    let (root, header, body, delta) = rec(1, None);
    db.publish_validated(
        0,
        &[Record {
            id: root,
            header,
            body: &body,
            delta: &delta,
            state: &[],
        }],
        root,
    )
    .unwrap();
    let (child, mut header, body, delta) = rec(2, Some((1, 0, 10)));
    header.cumulative_work += 1;
    let bad = Record {
        id: child,
        header,
        body: &body,
        delta: &delta,
        state: &[],
    };
    assert!(matches!(
        db.publish_validated(0, &[bad], child),
        Err(Error::Stale)
    ));
    let (_, mut header, _, _) = rec(2, Some((1, 0, 10)));
    header.cumulative_work += 1;
    assert!(db
        .publish_validated(
            1,
            &[Record {
                id: child,
                header,
                body: &body,
                delta: &delta,
                state: &[]
            }],
            child
        )
        .is_err());
    let snapshot = db.read().unwrap();
    assert!(snapshot.header(&child).unwrap().is_none());
    assert_eq!(snapshot.publication().unwrap().unwrap().head, root);
    assert_eq!(snapshot.publication().unwrap().unwrap().generation, 1);
}

#[test]
fn pruning_retains_immutable_body_identity_and_coherent_reader_state() {
    let dir = tempdir().unwrap();
    let db = SharechainDb::open(dir.path(), 1 << 24, 1024, id(88)).unwrap();
    let (root, header, body, delta) = rec(1, None);
    db.publish_validated(
        0,
        &[Record {
            id: root,
            header: header.clone(),
            body: &body,
            delta: &delta,
            state: &[],
        }],
        root,
    )
    .unwrap();
    let reader = db.read().unwrap();
    db.prune_bodies(&[root]).unwrap();
    assert_eq!(reader.body(&root).unwrap(), Some(body.as_slice()));
    drop(reader);
    assert!(db.read().unwrap().body(&root).unwrap().is_none());
    assert!(db
        .publish_validated(
            1,
            &[Record {
                id: root,
                header: header.clone(),
                body: b"different",
                delta: &delta,
                state: &[]
            }],
            root
        )
        .is_err());
    db.publish_validated(
        1,
        &[Record {
            id: root,
            header,
            body: &body,
            delta: &delta,
            state: &[],
        }],
        root,
    )
    .unwrap();
    assert_eq!(
        db.read().unwrap().body(&root).unwrap(),
        Some(body.as_slice())
    );
}

#[test]
fn bounded_pruning_reclaims_old_canonical_bodies() {
    let dir = tempdir().unwrap();
    let db = SharechainDb::open(dir.path(), 1 << 24, 1024, id(88)).unwrap();
    let (root, rh, rb, rd) = rec(1, None);
    let (head, hh, hb, hd) = rec(2, Some((1, 0, 10)));
    db.publish_validated(
        0,
        &[
            Record {
                id: root,
                header: rh,
                body: &rb,
                delta: &rd,
                state: &[],
            },
            Record {
                id: head,
                header: hh,
                body: &hb,
                delta: &hd,
                state: &[],
            },
        ],
        head,
    )
    .unwrap();
    assert_eq!(
        db.prune_unreferenced_bodies_bounded(head, 1, 256).unwrap(),
        1
    );
    let snapshot = db.read().unwrap();
    assert!(snapshot.body(&root).unwrap().is_none());
    assert!(snapshot.body(&head).unwrap().is_some());
}

#[test]
fn fork_comparison_uses_credited_work() {
    let dir = tempdir().unwrap();
    let db = SharechainDb::open(dir.path(), 1 << 24, 1024, id(88)).unwrap();
    let (root, rh, rb, rd) = rec(1, None);
    let (a, ah, ab, ad) = rec(2, Some((1, 0, 10)));
    let (b, mut bh, bb, bd) = rec(3, Some((1, 0, 10)));
    bh.work = 11;
    bh.cumulative_work = 21;
    db.publish_validated(
        0,
        &[
            Record {
                id: root,
                header: rh,
                body: &rb,
                delta: &rd,
                state: &[],
            },
            Record {
                id: a,
                header: ah,
                body: &ab,
                delta: &ad,
                state: &[],
            },
            Record {
                id: b,
                header: bh,
                body: &bb,
                delta: &bd,
                state: &[],
            },
        ],
        b,
    )
    .unwrap();
    let snapshot = db.read().unwrap();
    assert_eq!(
        snapshot.compare_cumulative(a, b).unwrap(),
        Some(std::cmp::Ordering::Less)
    );
}

#[test]
fn sparse_ancestor_rows_cover_deep_headers() {
    let dir = tempdir().unwrap();
    let db = SharechainDb::open(dir.path(), 1 << 26, 1024, id(88)).unwrap();
    let mut owned = Vec::new();
    let mut parent = None;
    for height in 0..=200u64 {
        let share = id((height + 1) as u8);
        owned.push((
            share,
            Header {
                parent,
                height,
                work: 1,
                cumulative_work: u128::from(height + 1),
                anchor: id(90),
                parent_state_root: [0; 32],
                state_root: [0; 32],
            },
            vec![0u8],
        ));
        parent = Some(share);
    }
    let records: Vec<_> = owned
        .iter()
        .map(|(share, header, body)| Record {
            id: *share,
            header: header.clone(),
            body,
            delta: &[],
            state: &[],
        })
        .collect();
    db.publish_validated(0, &records, parent.unwrap()).unwrap();
    let snapshot = db.read().unwrap();
    let tip = parent.unwrap();
    assert_eq!(snapshot.ancestor(tip, 0).unwrap(), id(1));
    assert_eq!(snapshot.ancestor(tip, 64).unwrap(), id(65));
    assert_eq!(snapshot.ancestor(tip, 123).unwrap(), id(124));
    assert_eq!(snapshot.ancestor(tip, 200).unwrap(), tip);
    assert_eq!(
        snapshot.compare_cumulative(tip, tip).unwrap(),
        Some(std::cmp::Ordering::Equal)
    );
    drop(snapshot);
    let fork = id(240);
    db.publish_validated(
        1,
        &[Record {
            id: fork,
            header: Header {
                parent: Some(id(101)),
                height: 101,
                work: 3,
                cumulative_work: 104,
                anchor: id(90),
                parent_state_root: [0; 32],
                state_root: [0; 32],
            },
            body: &[1],
            delta: &[],
            state: &[],
        }],
        fork,
    )
    .unwrap();
    let snapshot = db.read().unwrap();
    assert_eq!(
        snapshot.compare_cumulative(tip, fork).unwrap(),
        Some(std::cmp::Ordering::Greater)
    );
    assert_eq!(snapshot.ancestor(fork, 64).unwrap(), id(65));
}

#[test]
fn parent_state_root_is_part_of_the_atomic_transition_boundary() {
    let dir = tempdir().unwrap();
    let db = SharechainDb::open(dir.path(), 1 << 24, 1024, id(88)).unwrap();
    let (root, mut root_header, body, delta) = rec(1, None);
    root_header.state_root = id(140);
    db.publish_validated(
        0,
        &[Record {
            id: root,
            header: root_header,
            body: &body,
            delta: &delta,
            state: &[],
        }],
        root,
    )
    .unwrap();
    let (child, mut child_header, child_body, child_delta) = rec(2, Some((1, 0, 10)));
    child_header.parent_state_root = id(141);
    assert!(db
        .publish_validated(
            1,
            &[Record {
                id: child,
                header: child_header,
                body: &child_body,
                delta: &child_delta,
                state: &[]
            }],
            child
        )
        .is_err());
    let snapshot = db.read().unwrap();
    assert!(snapshot.header(&child).unwrap().is_none());
    assert_eq!(snapshot.publication().unwrap().unwrap().head, root);
}

#[test]
fn evaluations_are_view_keyed_and_immutable() {
    let dir = tempdir().unwrap();
    let db = SharechainDb::open(dir.path(), 1 << 24, 1024, id(88)).unwrap();
    let (root, header, body, delta) = rec(1, None);
    let original_root = header.state_root;
    db.publish_validated(
        0,
        &[Record {
            id: root,
            header,
            body: &body,
            delta: &delta,
            state: &[],
        }],
        root,
    )
    .unwrap();
    let view_a = id(9);
    let view_b = id(10);
    db.store_evaluation(root, 7, view_a, b"payout-a").unwrap();
    db.store_evaluation(root, 7, view_b, b"payout-b").unwrap();
    assert_eq!(
        db.read()
            .unwrap()
            .evaluation(root, 7, view_a)
            .unwrap()
            .unwrap()
            .payout,
        b"payout-a"
    );
    assert!(db.store_evaluation(root, 7, view_a, b"different").is_err());
    assert_eq!(
        db.read()
            .unwrap()
            .header(&root)
            .unwrap()
            .unwrap()
            .state_root,
        original_root
    );
}
fn evaluation_fixture() -> (tempfile::TempDir, SharechainDb, Id) {
    let dir = tempdir().unwrap();
    let db = SharechainDb::open(dir.path(), 1 << 24, 1024, id(88)).unwrap();
    let (tip, header, body, delta) = rec(1, None);
    db.publish_validated(
        0,
        &[Record {
            id: tip,
            header,
            body: &body,
            delta: &delta,
            state: &[],
        }],
        tip,
    )
    .unwrap();
    (dir, db, tip)
}

#[test]
fn atomic_verified_archival_rolls_back_headers_and_dependencies_on_evaluation_failure() {
    let (_dir, db, selected) = evaluation_fixture();
    let selected_manifest = id(29);
    let selected_batch = DependencyBatch {
        manifests: &[DependencyManifest {
            id: selected_manifest,
            parent: None,
            added: &[],
            removed: &[],
        }],
        ..Default::default()
    };
    db.publish_validated_with_dependencies(1, &[], selected, selected_manifest, &selected_batch)
        .unwrap();
    let (tip, header, body, delta) = rec(2, Some((1, 0, 10)));
    let records = [Record {
        id: tip,
        header,
        body: &body,
        delta: &delta,
        state: &[],
    }];
    let witness = DependencyKey {
        kind: 1,
        id: id(20),
    };
    let manifest = id(30);
    let batch = DependencyBatch {
        objects: &[DependencyObject {
            key: witness,
            bytes: b"witness",
        }],
        manifests: &[DependencyManifest {
            id: manifest,
            parent: None,
            added: &[witness],
            removed: &[],
        }],
        ..Default::default()
    };
    assert!(matches!(
        db.insert_validated_with_evaluation(
            2,
            &records,
            tip,
            manifest,
            &batch,
            EvaluationPublication {
                tip,
                grin_height: 7,
                grin_hash: id(9),
                payout: &[],
                manifest
            },
        ),
        Err(Error::Invalid("evaluation size"))
    ));
    let snapshot = db.read().unwrap();
    assert!(snapshot.header(&tip).unwrap().is_none());
    assert!(snapshot.body(&tip).unwrap().is_none());
    assert!(snapshot.dependency_object(witness).unwrap().is_none());
    assert!(!snapshot.dependency_retained(&manifest).unwrap());
    assert!(snapshot.evaluation(tip, 7, id(9)).unwrap().is_none());
    assert_eq!(snapshot.publication().unwrap().unwrap().head, selected);
    assert_eq!(
        db.meta.get(&snapshot.tx, b"publication-manifest").unwrap(),
        Some(selected_manifest.as_slice())
    );
    drop(snapshot);
    let archived = db
        .insert_validated_with_evaluation(
            2,
            &records,
            tip,
            manifest,
            &batch,
            EvaluationPublication {
                tip,
                grin_height: 7,
                grin_hash: id(9),
                payout: b"payout",
                manifest,
            },
        )
        .unwrap();
    assert_eq!(archived.generation, 2);
    let snapshot = db.read().unwrap();
    assert!(snapshot.header(&tip).unwrap().is_some());
    assert_eq!(
        snapshot
            .evaluation_bindings(tip, 7, id(9), 10)
            .unwrap()
            .len(),
        1
    );
    assert_eq!(snapshot.publication().unwrap().unwrap().head, selected);
    assert_eq!(
        db.meta.get(&snapshot.tx, b"publication-manifest").unwrap(),
        Some(selected_manifest.as_slice())
    );
}

#[test]
fn evaluation_dependency_failures_roll_back_payout_objects_and_manifests() {
    let (_dir, db, tip) = evaluation_fixture();
    let witness = DependencyKey {
        kind: 1,
        id: id(20),
    };
    let manifest = id(30);
    let missing_object = DependencyBatch {
        manifests: &[DependencyManifest {
            id: manifest,
            parent: None,
            added: &[witness],
            removed: &[],
        }],
        ..Default::default()
    };
    assert!(db
        .store_evaluation_with_dependencies(tip, 7, id(9), b"payout", manifest, &missing_object,)
        .is_err());
    let objects = [DependencyObject {
        key: witness,
        bytes: b"witness",
    }];
    let manifests = [DependencyManifest {
        id: manifest,
        parent: None,
        added: &[witness],
        removed: &[],
    }];
    let batch = DependencyBatch {
        objects: &objects,
        manifests: &manifests,
        ..Default::default()
    };
    assert!(db
        .store_evaluation_with_dependencies(tip, 7, id(9), b"payout", id(31), &batch,)
        .is_err());
    let snapshot = db.read().unwrap();
    assert!(snapshot.evaluation(tip, 7, id(9)).unwrap().is_none());
    assert!(snapshot
        .evaluation_bindings(tip, 7, id(9), 10)
        .unwrap()
        .is_empty());
    assert!(!snapshot.dependency_retained(&manifest).unwrap());
    assert!(snapshot.dependency_object(witness).unwrap().is_none());
    drop(snapshot);
    db.store_evaluation(tip, 7, id(9), b"payout").unwrap();
    assert!(db
        .store_evaluation_with_dependencies(tip, 7, id(9), b"changed-payout", manifest, &batch,)
        .is_err());
    let snapshot = db.read().unwrap();
    assert_eq!(
        snapshot.evaluation(tip, 7, id(9)).unwrap().unwrap().payout,
        b"payout"
    );
    assert!(!snapshot.dependency_retained(&manifest).unwrap());
    assert!(snapshot.dependency_object(witness).unwrap().is_none());
    assert!(snapshot
        .evaluation_bindings(tip, 7, id(9), 10)
        .unwrap()
        .is_empty());
}

#[test]
fn evaluation_witness_variants_coexist_and_retire_independently() {
    let (_dir, db, tip) = evaluation_fixture();
    let mut witness_keys = Vec::new();
    for n in [20, 21] {
        let witness = DependencyKey { kind: 1, id: id(n) };
        witness_keys.push(witness);
        let manifest = id(n + 10);
        let result = db
            .store_evaluation_with_dependencies(
                tip,
                7,
                id(9),
                b"payout",
                manifest,
                &DependencyBatch {
                    objects: &[DependencyObject {
                        key: witness,
                        bytes: &[n],
                    }],
                    manifests: &[DependencyManifest {
                        id: manifest,
                        parent: None,
                        added: &[witness],
                        removed: &[],
                    }],
                    ..Default::default()
                },
            )
            .unwrap();
        assert_eq!(result.dependency_manifest, Some(manifest));
    }
    let snapshot = db.read().unwrap();
    assert_eq!(
        snapshot.evaluation_bindings(tip, 7, id(9), 0).unwrap(),
        Vec::<Id>::new()
    );
    assert_eq!(
        snapshot.evaluation_bindings(tip, 7, id(9), 1).unwrap(),
        vec![id(30)]
    );
    assert_eq!(
        snapshot.evaluation_bindings(tip, 7, id(9), 10).unwrap(),
        vec![id(30), id(31)]
    );
    for (manifest, witness) in [id(30), id(31)].into_iter().zip(witness_keys) {
        assert_eq!(snapshot.dependencies(&manifest).unwrap(), vec![witness]);
        let evaluated = snapshot
            .evaluation_with_dependencies(tip, 7, id(9), manifest)
            .unwrap()
            .unwrap();
        assert_eq!(evaluated.payout, b"payout");
        assert_eq!(evaluated.dependency_manifest, Some(manifest));
    }
    assert!(snapshot
        .evaluation_with_dependencies(tip, 7, id(9), id(32))
        .unwrap()
        .is_none());
    assert_eq!(
        snapshot
            .evaluation(tip, 7, id(9))
            .unwrap()
            .unwrap()
            .dependency_manifest,
        None
    );
    assert!(db.retire_evaluation_binding(tip, 7, id(9), id(30)).unwrap());
    assert!(snapshot.dependency_retained(&id(30)).unwrap());
    let current = db.read().unwrap();
    assert!(!current.dependency_retained(&id(30)).unwrap());
    assert!(current.dependency_retained(&id(31)).unwrap());
    assert_eq!(
        current.evaluation_bindings(tip, 7, id(9), 10).unwrap(),
        vec![id(31)]
    );
    assert!(!db.retire_evaluation_binding(tip, 7, id(9), id(30)).unwrap());
}

#[test]
fn bounded_evaluation_retirement_advances_cursor_and_reclaims() {
    let (_dir, db, tip) = evaluation_fixture();
    let witness = DependencyKey {
        kind: 1,
        id: id(70),
    };
    let manifest = id(71);
    db.store_evaluation_with_dependencies(
        tip,
        3,
        id(9),
        b"payout",
        manifest,
        &DependencyBatch {
            objects: &[DependencyObject {
                key: witness,
                bytes: b"w",
            }],
            manifests: &[DependencyManifest {
                id: manifest,
                parent: None,
                added: &[witness],
                removed: &[],
            }],
            ..Default::default()
        },
    )
    .unwrap();
    assert_eq!(
        db.retire_obsolete_evaluation_bindings(4, &[], &[], 1)
            .unwrap(),
        1
    );
    assert!(!db.read().unwrap().dependency_retained(&manifest).unwrap());
    let stats = db.drain_dependency_gc(256).unwrap();
    assert!(stats.processed_nodes > 0);
    assert!(db
        .read()
        .unwrap()
        .dependency_object(witness)
        .unwrap()
        .is_none());
}

#[test]
fn bounded_retirement_reclaims_unbound_archived_evaluation_tip() {
    let dir = tempdir().unwrap();
    let db = SharechainDb::open(dir.path(), 1 << 24, 1024, id(88)).unwrap();
    let (root, rh, rb, rd) = rec(1, None);
    let (branch, bh, bb, bd) = rec(2, Some((1, 0, 10)));
    let (head, hh, hb, hd) = rec(4, Some((2, 1, 20)));
    let (archived, ah, ab, ad) = rec(3, Some((1, 0, 10)));
    db.publish_validated(
        0,
        &[
            Record {
                id: root,
                header: rh,
                body: &rb,
                delta: &rd,
                state: &[],
            },
            Record {
                id: branch,
                header: bh,
                body: &bb,
                delta: &bd,
                state: &[],
            },
            Record {
                id: head,
                header: hh,
                body: &hb,
                delta: &hd,
                state: &[],
            },
            Record {
                id: archived,
                header: ah,
                body: &ab,
                delta: &ad,
                state: &[],
            },
        ],
        head,
    )
    .unwrap();
    db.store_evaluation(archived, 3, id(9), b"payout").unwrap();
    assert_eq!(
        db.retire_obsolete_evaluation_bindings(4, &[], &[], 1)
            .unwrap(),
        1
    );
    assert!(db
        .read()
        .unwrap()
        .evaluation(archived, 3, id(9))
        .unwrap()
        .is_none());
    assert_eq!(
        db.prune_unreferenced_bodies_bounded(head, 2, 256).unwrap(),
        3
    );
    assert!(db.read().unwrap().body(&archived).unwrap().is_none());
}

#[test]
fn shared_evaluation_manifest_is_protected_until_last_binding_retires() {
    let (_dir, db, tip) = evaluation_fixture();
    let manifest = id(30);
    db.store_evaluation_with_dependencies(
        tip,
        7,
        id(9),
        b"payout",
        manifest,
        &DependencyBatch {
            manifests: &[DependencyManifest {
                id: manifest,
                parent: None,
                added: &[],
                removed: &[],
            }],
            ..Default::default()
        },
    )
    .unwrap();
    assert_eq!(
        db.evaluation_binding_refs(&db.read().unwrap().tx, manifest)
            .unwrap(),
        1
    );
    db.store_evaluation_with_dependencies(
        tip,
        7,
        id(9),
        b"payout",
        manifest,
        &DependencyBatch::default(),
    )
    .unwrap();
    assert_eq!(
        db.evaluation_binding_refs(&db.read().unwrap().tx, manifest)
            .unwrap(),
        1
    );
    db.store_evaluation_with_dependencies(
        tip,
        8,
        id(10),
        b"payout",
        manifest,
        &DependencyBatch::default(),
    )
    .unwrap();
    assert_eq!(
        db.evaluation_binding_refs(&db.read().unwrap().tx, manifest)
            .unwrap(),
        2
    );
    let head_manifest = [DependencyManifest {
        id: tip,
        parent: None,
        added: &[],
        removed: &[],
    }];
    assert!(db
        .publish_validated_with_dependencies(
            1,
            &[],
            tip,
            tip,
            &DependencyBatch {
                manifests: &head_manifest,
                release: &[manifest],
                ..Default::default()
            }
        )
        .is_err());
    assert_eq!(
        db.read()
            .unwrap()
            .publication()
            .unwrap()
            .unwrap()
            .generation,
        1
    );
    assert!(db
        .store_evaluation_with_dependencies(
            tip,
            9,
            id(11),
            b"payout",
            manifest,
            &DependencyBatch {
                release: &[manifest],
                ..Default::default()
            }
        )
        .is_err());
    assert!(db
        .read()
        .unwrap()
        .evaluation(tip, 9, id(11))
        .unwrap()
        .is_none());
    db.retire_evaluation_binding(tip, 7, id(9), manifest)
        .unwrap();
    assert!(db.read().unwrap().dependency_retained(&manifest).unwrap());
    assert_eq!(
        db.evaluation_binding_refs(&db.read().unwrap().tx, manifest)
            .unwrap(),
        1
    );
    db.retire_evaluation_binding(tip, 8, id(10), manifest)
        .unwrap();
    assert!(!db.read().unwrap().dependency_retained(&manifest).unwrap());
    assert_eq!(
        db.evaluation_binding_refs(&db.read().unwrap().tx, manifest)
            .unwrap(),
        0
    );
    assert!(db
        .read()
        .unwrap()
        .evaluation(tip, 8, id(10))
        .unwrap()
        .is_some());
    db.store_evaluation_with_dependencies(
        tip,
        10,
        id(12),
        b"payout",
        tip,
        &DependencyBatch {
            manifests: &head_manifest,
            ..Default::default()
        },
    )
    .unwrap();
    db.publish_validated_with_dependencies(
        1,
        &[],
        tip,
        tip,
        &DependencyBatch {
            manifests: &head_manifest,
            ..Default::default()
        },
    )
    .unwrap();
    db.retire_evaluation_binding(tip, 10, id(12), tip).unwrap();
    assert!(db.read().unwrap().dependency_retained(&tip).unwrap());
}

#[test]
fn selected_head_and_dependency_manifest_have_independent_identity() {
    let (_dir, db, tip) = evaluation_fixture();
    let manifest = id(91);
    let batch = DependencyBatch {
        manifests: &[DependencyManifest {
            id: manifest,
            parent: None,
            added: &[],
            removed: &[],
        }],
        ..Default::default()
    };
    db.store_evaluation_with_dependencies(tip, 7, id(9), b"payout", manifest, &batch)
        .unwrap();
    db.publish_validated_with_dependencies(1, &[], tip, manifest, &batch)
        .unwrap();
    db.retire_evaluation_binding(tip, 7, id(9), manifest)
        .unwrap();
    assert!(db.read().unwrap().dependency_retained(&manifest).unwrap());
}

#[test]
fn evaluation_decode_and_binding_corruption_fail_consistently() {
    let (_dir, db, tip) = evaluation_fixture();
    let view = id(9);
    let manifest = id(30);
    db.store_evaluation_with_dependencies(
        tip,
        7,
        view,
        b"payout",
        manifest,
        &DependencyBatch {
            manifests: &[DependencyManifest {
                id: manifest,
                parent: None,
                added: &[],
                removed: &[],
            }],
            ..Default::default()
        },
    )
    .unwrap();
    let mut tx = db.env.write_txn().unwrap();
    db.evaluation_bindings
        .put(
            &mut tx,
            &evaluation_binding_key(evaluation_key(tip, 7, view), manifest),
            &id(99),
        )
        .unwrap();
    tx.commit().unwrap();
    assert!(db
        .read()
        .unwrap()
        .evaluation_with_dependencies(tip, 7, view, manifest)
        .is_err());
    assert!(db
        .read()
        .unwrap()
        .evaluation_bindings(tip, 7, view, 10)
        .is_err());
    assert!(db
        .store_evaluation_with_dependencies(
            tip,
            7,
            view,
            b"payout",
            manifest,
            &DependencyBatch::default()
        )
        .is_err());
    let mut tx = db.env.write_txn().unwrap();
    db.evaluations
        .put(&mut tx, &evaluation_key(tip, 7, view), &[1; 33])
        .unwrap();
    tx.commit().unwrap();
    assert!(db.read().unwrap().evaluation(tip, 7, view).is_err());
    assert!(matches!(
        db.store_evaluation(tip, 7, view, b"payout"),
        Err(Error::Invalid("evaluation digest"))
    ));
}
#[test]
fn evaluation_binding_refcount_failures_roll_back_new_and_retired_bindings() {
    let (_dir, db, tip) = evaluation_fixture();
    let manifest = id(30);
    db.store_evaluation_with_dependencies(
        tip,
        7,
        id(9),
        b"payout",
        manifest,
        &DependencyBatch {
            manifests: &[DependencyManifest {
                id: manifest,
                parent: None,
                added: &[],
                removed: &[],
            }],
            ..Default::default()
        },
    )
    .unwrap();
    let mut tx = db.env.write_txn().unwrap();
    db.evaluation_binding_refs
        .put(&mut tx, &manifest, &u64::MAX.to_be_bytes())
        .unwrap();
    tx.commit().unwrap();
    assert!(matches!(
        db.store_evaluation_with_dependencies(
            tip,
            8,
            id(10),
            b"payout",
            manifest,
            &DependencyBatch::default()
        ),
        Err(Error::Invalid(
            "evaluation binding refcount overflow or underflow"
        ))
    ));
    assert!(db
        .read()
        .unwrap()
        .evaluation(tip, 8, id(10))
        .unwrap()
        .is_none());
    assert!(db
        .read()
        .unwrap()
        .evaluation_with_dependencies(tip, 8, id(10), manifest)
        .unwrap()
        .is_none());
    let mut tx = db.env.write_txn().unwrap();
    db.evaluation_binding_refs
        .delete(&mut tx, &manifest)
        .unwrap();
    tx.commit().unwrap();
    assert!(matches!(
        db.retire_evaluation_binding(tip, 7, id(9), manifest),
        Err(Error::Invalid(
            "evaluation binding refcount overflow or underflow"
        ))
    ));
    assert!(db.read().unwrap().dependency_retained(&manifest).unwrap());
    let snapshot = db.read().unwrap();
    assert!(db
        .evaluation_bindings
        .get(
            &snapshot.tx,
            &evaluation_binding_key(evaluation_key(tip, 7, id(9)), manifest)
        )
        .unwrap()
        .is_some());
}

#[test]
fn header_pruning_preserves_deep_sparse_ancestry() {
    const SHARES: u64 = 4096;
    const PAGE: usize = 256;
    let directory = tempdir().unwrap();
    let db = SharechainDb::open(directory.path(), 1 << 24, 4096, [7; 32]).unwrap();
    let id64 = |value: u64| {
        let mut id = [0; 32];
        id[..8].copy_from_slice(&value.to_be_bytes());
        id
    };
    db.publish_validated(
        0,
        &[Record {
            id: id64(0),
            header: Header {
                parent: None,
                height: 0,
                work: 1,
                cumulative_work: 1,
                anchor: [9; 32],
                parent_state_root: [0; 32],
                state_root: [0; 32],
            },
            body: b"root",
            delta: &[],
            state: &[],
        }],
        id64(0),
    )
    .unwrap();
    for start in (1..SHARES).step_by(PAGE) {
        let records: Vec<_> = (start..(start + PAGE as u64).min(SHARES))
            .map(|height| Record {
                id: id64(height),
                header: Header {
                    parent: Some(id64(height - 1)),
                    height,
                    work: 1,
                    cumulative_work: u128::from(height + 1),
                    anchor: [9; 32],
                    parent_state_root: [0; 32],
                    state_root: [0; 32],
                },
                body: b"body",
                delta: &[],
                state: &[],
            })
            .collect();
        db.insert_validated(1, &records).unwrap();
    }
    let fork = id64(SHARES);
    db.insert_validated(
        1,
        &[Record {
            id: fork,
            header: Header {
                parent: Some(id64(1)),
                height: 2,
                work: 1,
                cumulative_work: 3,
                anchor: [9; 32],
                parent_state_root: [0; 32],
                state_root: [0; 32],
            },
            body: b"fork",
            delta: &[],
            state: &[],
        }],
    )
    .unwrap();
    let tip = id64(SHARES - 1);
    db.publish_head(1, tip).unwrap();
    let retain_from = SHARES - PAGE as u64;
    let mut reclaimed = 0;
    for _ in 0..(SHARES + 1).div_ceil(PAGE as u64) {
        reclaimed += db.prune_headers_bounded(&[tip], retain_from, PAGE).unwrap();
    }
    let snapshot = db.read().unwrap();
    assert!(reclaimed > 0);
    assert!(snapshot.header(&id64(1)).unwrap().is_none());
    assert!(snapshot.header(&fork).unwrap().is_none());
    for height in retain_from..SHARES {
        assert!(snapshot.header(&id64(height)).unwrap().is_some());
    }
    for height in (0..SHARES).step_by(64) {
        assert_eq!(snapshot.ancestor(tip, height).unwrap(), id64(height));
    }
    let locator = snapshot.locator(tip, 80).unwrap();
    assert_eq!(locator.first(), Some(&tip));
    assert!(locator
        .iter()
        .all(|id| snapshot.header(id).unwrap().is_some()));
    assert_eq!(locator.last(), Some(&id64(0)));
    assert_eq!(
        snapshot.compare_cumulative(tip, id64(64)).unwrap(),
        Some(std::cmp::Ordering::Greater)
    );
}

impl SharechainDb {
    fn prune_bodies(&self, ids: &[Id]) -> Result<usize> {
        let mut tx = self.env.write_txn()?;
        let mut count = 0;
        for id in ids {
            count += usize::from(self.bodies.delete(&mut tx, id)?);
        }
        tx.commit()?;
        Ok(count)
    }
}

impl SharechainDb {
    pub fn publish_validated(
        &self,
        expected_generation: u64,
        records: &[Record<'_>],
        head: Id,
    ) -> Result<Publication> {
        self.publish_inner(expected_generation, records, head, None, None, true)
    }
    pub fn publish_validated_with_dependencies(
        &self,
        expected_generation: u64,
        records: &[Record<'_>],
        head: Id,
        head_manifest: Id,
        dependencies: &DependencyBatch<'_>,
    ) -> Result<Publication> {
        self.publish_inner(
            expected_generation,
            records,
            head,
            Some((head_manifest, dependencies)),
            None,
            true,
        )
    }
    pub fn insert_validated(&self, expected_generation: u64, records: &[Record<'_>]) -> Result<()> {
        let head = records.last().ok_or(Error::Invalid("empty records"))?.id;
        self.publish_inner(expected_generation, records, head, None, None, false)
            .map(|_| ())
    }
    pub fn store_evaluation(
        &self,
        tip: Id,
        grin_height: u64,
        grin_hash: Id,
        payout: &[u8],
    ) -> Result<EvaluationSnapshot> {
        self.store_evaluation_inner(tip, grin_height, grin_hash, payout, None)
    }
    pub fn retire_evaluation_binding(
        &self,
        tip: Id,
        grin_height: u64,
        grin_hash: Id,
        manifest: Id,
    ) -> Result<bool> {
        let mut tx = self.env.write_txn()?;
        let binding = evaluation_binding_key(evaluation_key(tip, grin_height, grin_hash), manifest);
        let Some(root) = self.evaluation_bindings.get(&tx, &binding)? else {
            return Ok(false);
        };
        if root.len() != 32 || self.dependencies.root(&tx, &manifest)? != root {
            return Err(Error::Invalid("evaluation binding root"));
        }
        self.evaluation_bindings.delete(&mut tx, &binding)?;
        let remaining = self.change_evaluation_binding_refs(&mut tx, manifest, false)?;
        let selected = self
            .meta
            .get(&tx, b"publication-manifest")?
            .is_some_and(|id| id == manifest.as_slice());
        if remaining == 0 && !selected {
            self.dependencies.apply(
                &mut tx,
                &DependencyBatch {
                    release: &[manifest],
                    ..Default::default()
                },
                self.max_record_bytes,
            )?;
        }
        tx.commit()?;
        Ok(true)
    }
}

#[test]
fn long_suffix_and_evaluation_roll_back_together() {
    let dir = tempdir().unwrap();
    let db = SharechainDb::open(dir.path(), 1 << 24, 1024, [7; 32]).unwrap();
    let id64 = |value: u64| {
        let mut id = [0; 32];
        id[..8].copy_from_slice(&value.to_be_bytes());
        id
    };
    let records: Vec<_> = (0..300)
        .map(|height| Record {
            id: id64(height),
            header: Header {
                parent: height.checked_sub(1).map(id64),
                height,
                work: 1,
                cumulative_work: u128::from(height + 1),
                anchor: [9; 32],
                parent_state_root: [0; 32],
                state_root: [0; 32],
            },
            body: b"body",
            delta: &[],
            state: &[],
        })
        .collect();
    let manifest = DependencyManifest {
        id: id(77),
        parent: None,
        added: &[],
        removed: &[],
    };
    let batch = DependencyBatch {
        manifests: &[manifest],
        ..Default::default()
    };
    let oversized = vec![0; 1025];
    assert!(db
        .insert_validated_with_evaluation(
            0,
            &records,
            id64(299),
            id(77),
            &batch,
            EvaluationPublication {
                tip: id64(299),
                grin_height: 1,
                grin_hash: id(9),
                payout: &oversized,
                manifest: id(77)
            }
        )
        .is_err());
    assert!(db.read().unwrap().header(&id64(0)).unwrap().is_none());
    assert!(db.read().unwrap().dependency_root(&id(77)).is_err());
    db.insert_validated_with_evaluation(
        0,
        &records,
        id64(299),
        id(77),
        &batch,
        EvaluationPublication {
            tip: id64(299),
            grin_height: 1,
            grin_hash: id(9),
            payout: b"payout",
            manifest: id(77),
        },
    )
    .unwrap();
    let snapshot = db.read().unwrap();
    assert!(snapshot.header(&id64(0)).unwrap().is_some());
    assert!(snapshot.header(&id64(299)).unwrap().is_some());
    assert!(snapshot.evaluation(id64(299), 1, id(9)).unwrap().is_some());
}
