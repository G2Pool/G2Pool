use super::*;

#[test]
fn traversal_and_ledger_payout_windows_stop_when_an_uncle_fills_the_budget() {
    let (node, _signer, _payments, _dir) = crate::node::tests::setup();
    let view = node.view().unwrap();
    let mut facts = Facts::default();
    for (n, difficulty) in [(1, 7), (2, 4), (3, 6)] {
        let id = [n; 32];
        let work = Work {
            id,
            job: Job {
                protocol: view.profile.protocol,
                anchor: view.head.clone(),
                timestamp: 0,
                parent: if n == 3 {
                    Parent::Receipt([1; 32])
                } else {
                    Parent::Root(view.head.clone())
                },
                account: [0; 33],
                registration: [0; 32],
                difficulty,
                uncles: List(if n == 3 { vec![[2; 32]] } else { vec![] }),
                transaction_only: [0; 32],
                kernel_encoding_plan: [0; 32],
                finder_value: 1,
                finder_commitment: [0; 33],
                finder_output: [0; 32],
            },
            header: Default::default(),
        };
        facts.work.insert(id, Arc::new(work));
        facts.targets.insert(id, Trace::target(id));
        for kind in [Kind::Core, Kind::Authorization] {
            let key = Key::new(kind, id);
            facts.loaded.insert(key, Arc::from([n]));
            facts.leaves.insert(key, Trace::leaf(key));
        }
    }
    facts.recount();
    let mut builder =
        Validator::bounded(&node.store, &view, node.limits.clone(), node.cancel.clone());
    builder.seed(&facts);
    let mut ledger = Ledger::new(view.head.height);
    for id in [[3; 32], [1; 32]] {
        ledger
            .append(builder.frame(facts.work[&id].clone()).unwrap())
            .unwrap();
    }
    for cached in [false, true] {
        let mut validator =
            Validator::bounded(&node.store, &view, node.limits.clone(), node.cancel.clone());
        validator.seed(&facts);
        if cached {
            validator.facts.topology = Some(Arc::new(ledger.clone()));
        }
        let entries = validator
            .contributions_bounded(
                &Parent::Receipt([3; 32]),
                view.head.height,
                Some(8),
                Some(10),
                Some(8),
            )
            .unwrap();
        assert_eq!(
            entries
                .iter()
                .map(|(work, class)| (work.id(), *class))
                .collect::<Vec<_>>(),
            vec![([3; 32], 0), ([2; 32], 1)]
        );
    }
}

#[test]
fn facts_clone_shares_maps_and_retention_is_snapshot_independent() {
    let kept = Key::new(Kind::Core, [1; 32]);
    let removed = Key::new(Kind::Core, [2; 32]);
    let mut original = Facts::default();
    for key in [removed, kept] {
        original.loaded.insert(key, Arc::from([key.id[0]]));
        original.leaves.insert(key, Trace::leaf(key));
        original.completed.insert(key.id, Trace::leaf(key));
    }
    original.recount();
    let mut child = original.clone();
    assert!(child.loaded.ptr_eq(&original.loaded));
    assert!(child.leaves.ptr_eq(&original.leaves));
    assert!(child.completed.ptr_eq(&original.completed));
    assert!(child.work.ptr_eq(&original.work));
    assert!(child.registrations.ptr_eq(&original.registrations));
    assert!(child.material.ptr_eq(&original.material));
    assert!(child.coupons.ptr_eq(&original.coupons));
    assert!(child.historical.ptr_eq(&original.historical));

    child.retain(&BTreeSet::from([kept]), &BTreeSet::from([kept.id]));
    assert_eq!(child.pins(), BTreeSet::from([kept]));
    assert_eq!(child.payload_bytes, 1);
    assert_eq!(child.completed.len(), 1);
    assert_eq!(original.pins(), BTreeSet::from([kept, removed]));
    assert_eq!(original.payload_bytes, 2);
    assert_eq!(original.completed.len(), 2);
    assert!(Arc::ptr_eq(&child.loaded[&kept], &original.loaded[&kept]));
}

#[test]
fn warmed_facts_preserve_certified_witnesses_on_collisions() {
    let key = Key::new(Kind::Core, [1; 32]);
    let extra = Key::new(Kind::Core, [2; 32]);
    let mut warm = Facts::default();
    warm.loaded.insert(key, Arc::from([1]));
    warm.leaves.insert(key, Trace::leaf(key));
    warm.completed.insert(key.id, Trace::leaf(key));
    warm.recount();
    let mut challenger = Facts::default();
    challenger.loaded.insert(key, Arc::from([9]));
    challenger.leaves.insert(key, Trace::leaf(extra));
    challenger.completed.insert(key.id, Trace::leaf(extra));
    challenger.loaded.insert(extra, Arc::from([2]));
    challenger.leaves.insert(extra, Trace::leaf(extra));
    challenger.completed.insert(extra.id, Trace::leaf(extra));

    let facts = challenger.warmed(&warm);
    assert!(Arc::ptr_eq(&facts.loaded[&key], &warm.loaded[&key]));
    assert!(Arc::ptr_eq(&facts.leaves[&key], &warm.leaves[&key]));
    assert!(Arc::ptr_eq(
        &facts.completed[&key.id],
        &warm.completed[&key.id]
    ));
    assert!(Arc::ptr_eq(
        &facts.loaded[&extra],
        &challenger.loaded[&extra]
    ));
    assert_eq!(
        facts.loaded.keys().copied().collect::<Vec<_>>(),
        [key, extra]
    );
    assert_eq!(facts.payload_bytes, 2);
    let mut recounted = facts.clone();
    recounted.recount();
    assert_eq!(facts.bytes(), recounted.bytes());
    let same = facts.warmed(&facts);
    assert!(same.loaded.ptr_eq(&facts.loaded));
    assert!(same.completed.ptr_eq(&facts.completed));
    assert_eq!(same.bytes(), facts.bytes());
}

#[test]
fn retention_without_removals_preserves_shared_map_nodes() {
    let original: OrdMap<i32, i32> = [(3, 30), (1, 10), (2, 20)].into_iter().collect();
    let mut retained = original.clone();
    assert_eq!(retain_ord_map(&mut retained, |_, _| true, |_| 1), 0);
    assert!(retained.ptr_eq(&original));
    assert_eq!(extend_missing(&mut retained, &original, |_| 1), 0);
    assert!(retained.ptr_eq(&original));
    assert_eq!(retained.keys().copied().collect::<Vec<_>>(), [1, 2, 3]);
    let additions = [(2, 200), (4, 40)].into_iter().collect();
    assert_eq!(
        extend_missing(&mut retained, &additions, |value| *value as usize),
        40
    );
    assert_eq!(retained[&2], 20);
}

#[test]
fn mixed_retention_byte_deltas_match_full_recount() {
    grin_core::global::set_local_chain_type(grin_core::global::ChainTypes::AutomatedTesting);
    let mut facts = Facts::default();
    for value in 1..=3 {
        let id = [value; 32];
        let anchor = Anchor {
            height: u64::from(value),
            hash: id,
        };
        let job = Job {
            protocol: [0; 32],
            anchor: anchor.clone(),
            timestamp: 0,
            parent: Parent::Root(anchor.clone()),
            account: [0; 33],
            registration: id,
            difficulty: 1,
            uncles: List(vec![id; usize::from(value)]),
            transaction_only: [0; 32],
            kernel_encoding_plan: [0; 32],
            finder_value: 1,
            finder_commitment: [0; 33],
            finder_output: [0; 32],
        };
        let mut work = Work {
            id,
            job: decode(&encode(&job).unwrap()).unwrap(),
            header: Default::default(),
        };
        work.header.pow.proof.nonces = vec![u64::from(value); usize::from(value)];
        facts.work.insert(id, Arc::new(work));
        for kind in [Kind::Core, Kind::Registration, Kind::Coupon] {
            let key = Key::new(kind, id);
            facts
                .loaded
                .insert(key, Arc::from(vec![value; usize::from(value)]));
            facts.leaves.insert(key, Trace::leaf(key));
        }
        facts
            .completed
            .insert(id, Trace::leaf(Key::new(Kind::Core, id)));
        facts.registrations.insert(
            id,
            RegistrationFacts {
                registration: Arc::new(Registration {
                    fields: RegistrationAuth {
                        protocol: [0; 32],
                        owner: [0; 33],
                        inventory: id,
                        birth: anchor,
                        revision: 0,
                    },
                    signature: [0; 64],
                }),
                inventory: Arc::new(Inventory {
                    protocol: [0; 32],
                    owner: [0; 33],
                    rows: List(vec![InventoryRow {
                        family: 0,
                        row: 0,
                        coupon_ids: List(vec![id; usize::from(value)]),
                    }]),
                }),
                trace: Trace::leaf(Key::new(Kind::Registration, id)),
            },
        );
        facts.material.insert(
            id,
            Arc::new(CouponMaterial {
                coupon: Coupon {
                    reward: Reward {
                        output: Blob(vec![value; usize::from(value)]),
                        kernel: Blob(vec![value; usize::from(value) * 2]),
                    },
                    owner_auth: [0; 64],
                },
                output: Output::new(
                    grin_core::core::OutputFeatures::Coinbase,
                    grin_util::secp::pedersen::Commitment([0; 33]),
                    grin_util::secp::pedersen::RangeProof::zero(),
                ),
                kernel: TxKernel::empty(),
            }),
        );
        facts.coupons.insert((id, [value; 33], 0), value);
        facts
            .historical
            .insert((u64::from(value), id, [value; 33]), true);
    }
    facts.recount();
    let original = facts.clone();
    facts.retain(
        &BTreeSet::from([
            Key::new(Kind::Core, [1; 32]),
            Key::new(Kind::Registration, [2; 32]),
            Key::new(Kind::Coupon, [3; 32]),
        ]),
        &BTreeSet::from([[2; 32]]),
    );
    let mut recounted = facts.clone();
    recounted.recount();
    assert_eq!(facts.payload_bytes, recounted.payload_bytes);
    assert_eq!(facts.work_bytes, recounted.work_bytes);
    assert_eq!(facts.registration_bytes, recounted.registration_bytes);
    assert_eq!(facts.material_bytes, recounted.material_bytes);
    assert_eq!(facts.certificate_bytes, recounted.certificate_bytes);
    assert_eq!(facts.bytes(), recounted.bytes());
    assert_eq!(facts.historical.len(), 1);
    assert!(facts.historical.contains_key(&(1, [1; 32], [1; 33])));
    assert!(facts.bytes() < original.bytes());
    facts.retain(&BTreeSet::new(), &BTreeSet::new());
    assert_eq!(facts.bytes(), Facts::default().bytes());
    assert_eq!(original.work.len(), 3);
}

impl Facts {
    fn recount(&mut self) {
        self.work_bytes = self.work.values().map(|work| work.retained_bytes()).sum();
        self.material_bytes = self
            .material
            .values()
            .map(|material| material.bytes())
            .sum();
        self.payload_bytes = self.loaded.values().map(|bytes| bytes.len()).sum();
        self.registration_bytes = self
            .registrations
            .values()
            .map(RegistrationFacts::bytes)
            .sum();
        self.certificate_bytes = self
            .completed
            .values()
            .chain(self.targets.values())
            .map(|trace| trace.own_metadata())
            .sum();
    }
}
