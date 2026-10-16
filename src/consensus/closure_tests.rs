use super::*;
use crate::{
    codec::{Blob, List, Parent, Residual, Reward, WorkAuthMessage, WorkAuthorization},
    consensus::Financial,
    limits::Cost,
};
use std::collections::BTreeSet;

fn evaluation(required: impl IntoIterator<Item = Key>) -> Evaluation {
    let anchor = Anchor {
        height: 7,
        hash: [7; 32],
    };
    Evaluation {
        tip: [9; 32],
        anchor: anchor.clone(),
        score: 1,
        financial: Financial {
            record: PayoutRecord {
                protocol: [1; 32],
                anchor: anchor.clone(),
                parent: Parent::Root(anchor),
                receipts: List(vec![]),
                accounts: List(vec![]),
                paid: 0,
                finder: 1,
            },
            coupons: vec![],
        },
        full: BTreeSet::new(),
        closure: BTreeSet::new(),
        primary: BTreeSet::new(),
        required: required.into_iter().collect(),
        cost: Cost::default(),
    }
}
fn reward(value: u8) -> (Key, Arc<[u8]>) {
    let bytes = encode(&Reward {
        output: Blob(vec![value]),
        kernel: Blob(vec![]),
    })
    .unwrap();
    (
        Key::new(Kind::Reward, hash(Reward::DOMAIN, &bytes)),
        bytes.into(),
    )
}
fn facts(entries: impl IntoIterator<Item = (Key, Arc<[u8]>)>) -> Facts {
    Facts {
        loaded: entries.into_iter().collect(),
        ..Default::default()
    }
}
fn authorization(work: Id, signature: u8) -> (Key, Arc<[u8]>) {
    let bytes = encode(&WorkAuthorization {
        fields: WorkAuthMessage {
            protocol: [1; 32],
            work_id: work,
        },
        signature: [signature; 64],
    })
    .unwrap();
    (Key::new(Kind::Authorization, work), bytes.into())
}

#[test]
fn content_and_detached_witness_keys_bind_exact_bytes_and_routes() {
    let (route, bytes) = reward(1);
    assert_ne!(bound_key(route, &bytes).id, route.id);
    let first = authorization([3; 32], 1);
    let second = authorization([3; 32], 2);
    let a = facts([first.clone()])
        .verified_closure(&evaluation([first.0]), None)
        .unwrap();
    let b = facts([second.clone()])
        .verified_closure(&evaluation([second.0]), None)
        .unwrap();
    assert_ne!(a.keys(), b.keys());
    let changed_route = Key::new(Kind::Authorization, [4; 32]);
    assert_ne!(
        bound_key(first.0, &first.1),
        bound_key(changed_route, &first.1)
    );
    assert_ne!(
        bound_key(first.0, &first.1),
        bound_key(Key::new(Kind::Residual, first.0.id), &first.1)
    );
    let residual = |value| {
        encode(&Residual {
            protocol: [1; 32],
            finder_kernel: Blob(vec![value]),
            binding_kernel: Blob(vec![]),
            aggregate_kernels: List(vec![]),
        })
        .unwrap()
    };
    let route = Key::new(Kind::Residual, [3; 32]);
    assert_ne!(
        bound_key(route, &residual(1)),
        bound_key(route, &residual(2))
    );
    let payloads = a.dependency_objects();
    let objects = payloads.objects();
    assert_eq!(
        decode_dependency(objects[0].key, objects[0].bytes).unwrap(),
        (first.0, first.1.as_ref())
    );
    assert!(decode_dependency(b.keys()[0], objects[0].bytes).is_err());
}

#[test]
fn deterministic_ordering_and_identity_bind_native_context() {
    let a = reward(1);
    let b = reward(2);
    let e = evaluation([b.0, a.0]);
    let first = facts([a.clone(), b.clone()])
        .verified_closure(&e, None)
        .unwrap();
    let second = facts([b.clone(), a.clone()])
        .verified_closure(&e, None)
        .unwrap();
    assert_eq!(first.id(), second.id());
    assert!(first.keys().windows(2).all(|keys| keys[0] < keys[1]));
    let mut changed = evaluation([a.0, b.0]);
    changed.tip[0] ^= 1;
    assert_ne!(
        first.id(),
        facts([a.clone(), b.clone()])
            .verified_closure(&changed, None)
            .unwrap()
            .id()
    );
    changed = evaluation([a.0, b.0]);
    changed.anchor.height += 1;
    assert_ne!(
        first.id(),
        facts([a.clone(), b.clone()])
            .verified_closure(&changed, None)
            .unwrap()
            .id()
    );
    changed = evaluation([a.0, b.0]);
    changed.financial.record.finder += 1;
    assert_ne!(
        first.id(),
        facts([a, b]).verified_closure(&changed, None).unwrap().id()
    );
}

#[test]
fn missing_required_fact_fails_and_extra_loaded_facts_are_excluded() {
    let a = reward(1);
    let extra = reward(2);
    let f = facts([extra.clone()]);
    assert!(
        matches!(f.verified_closure(&evaluation([a.0]), None), Err(Error::Unavailable(key)) if key == a.0)
    );
    let closure = facts([a.clone(), extra])
        .verified_closure(&evaluation([a.0]), None)
        .unwrap();
    assert_eq!(closure.keys(), &[bound_key(a.0, &a.1)]);
    assert!(Arc::ptr_eq(&closure.facts[&closure.keys()[0]].bytes, &a.1));
}

#[test]
fn branch_delta_reconstructs_exact_set_and_encodes_only_additions() {
    let a = reward(1);
    let b = reward(2);
    let c = reward(3);
    let base = facts([a.clone(), b.clone()])
        .verified_closure(&evaluation([a.0, b.0]), None)
        .unwrap();
    let next = facts([b.clone(), c.clone()])
        .verified_closure(&evaluation([b.0, c.0]), Some(&base))
        .unwrap();
    assert_eq!(next.added(), &[bound_key(c.0, &c.1)]);
    assert_eq!(next.removed(), &[bound_key(a.0, &a.1)]);
    let mut reconstructed: BTreeSet<_> = base.keys().iter().copied().collect();
    for key in next.removed() {
        assert!(reconstructed.remove(key));
    }
    for key in next.added() {
        assert!(reconstructed.insert(*key));
    }
    assert_eq!(reconstructed.into_iter().collect::<Vec<_>>(), next.keys());
    let payloads = next.dependency_objects();
    let objects = payloads.objects();
    assert_eq!(objects.len(), 1);
    assert_eq!(
        decode_dependency(objects[0].key, objects[0].bytes).unwrap(),
        (c.0, c.1.as_ref())
    );
    assert!(Arc::ptr_eq(&next.facts[&bound_key(b.0, &b.1)].bytes, &b.1));
    let repeat = facts([b.clone(), c.clone()])
        .verified_closure(&evaluation([b.0, c.0]), Some(&next))
        .unwrap();
    assert_eq!(repeat.id(), next.id());
    assert_eq!(repeat.manifest().parent, next.manifest().parent);
    let fresh = facts([b, c])
        .verified_closure(
            &evaluation(next.facts.values().map(|fact| fact.route)),
            None,
        )
        .unwrap();
    assert_ne!(fresh.id(), next.id());
}

#[test]
fn malformed_and_oversized_replay_payloads_fail_closed() {
    let (route, bytes) = reward(1);
    let closure = facts([(route, bytes)])
        .verified_closure(&evaluation([route]), None)
        .unwrap();
    let payloads = closure.dependency_objects();
    let objects = payloads.objects();
    for length in [0, 1, PAYLOAD_HEADER - 1, objects[0].bytes.len() - 1] {
        assert!(decode_dependency(objects[0].key, &objects[0].bytes[..length]).is_err());
    }
    let mut oversized = objects[0].bytes[..PAYLOAD_HEADER].to_vec();
    oversized.resize(PAYLOAD_HEADER + route.kind.ceiling() + 1, 0);
    assert!(matches!(
        decode_dependency(objects[0].key, &oversized),
        Err(Error::Invalid("sealed dependency ceiling"))
    ));
    let oversized_facts = facts([(route, Arc::from(vec![0; route.kind.ceiling() + 1]))]);
    assert!(matches!(
        oversized_facts.verified_closure(&evaluation([route]), None),
        Err(Error::Invalid("object ceiling"))
    ));
    let mut trailing = objects[0].bytes.to_vec();
    trailing.push(0);
    assert!(decode_dependency(objects[0].key, &trailing).is_err());
    let mut unknown_kind = objects[0].bytes.to_vec();
    unknown_kind[4] = 255;
    assert!(decode_dependency(objects[0].key, &unknown_kind).is_err());
}

#[test]
fn unchanged_arcs_share_persistent_maps_and_witness_replacement_is_a_delta() {
    let original = authorization([3; 32], 1);
    let f = facts([original.clone()]);
    let base = f.verified_closure(&evaluation([original.0]), None).unwrap();
    let mut changed_tip = evaluation([original.0]);
    changed_tip.tip[0] ^= 1;
    let shifted = f.verified_closure(&changed_tip, Some(&base)).unwrap();
    assert!(shifted.added().is_empty());
    assert!(shifted.removed().is_empty());
    assert!(shifted.facts.ptr_eq(&base.facts));
    assert!(shifted.routes.ptr_eq(&base.routes));
    assert_eq!(shifted.encoded_delta_bytes(), 0);
    let replacement = authorization(original.0.id, 2);
    let replaced = facts([replacement.clone()])
        .verified_closure(&changed_tip, Some(&shifted))
        .unwrap();
    assert_eq!(
        replaced.added(),
        &[bound_key(replacement.0, &replacement.1)]
    );
    assert_eq!(replaced.removed(), &[bound_key(original.0, &original.1)]);
    assert_eq!(replaced.facts.len(), 1);
    assert_eq!(replaced.routes.len(), 1);
    assert_eq!(replaced.routes[&replacement.0], replaced.keys()[0]);
    assert!(replaced.retained_bytes() >= replacement.1.len());
    assert!(replaced.encoded_delta_bytes() >= replacement.1.len() + PAYLOAD_HEADER);
}

#[test]
fn cached_retained_bytes_match_full_oracle_after_delta_changes() {
    fn assert_bytes(closure: &VerifiedClosure) {
        let fact_bytes: usize = closure
            .facts
            .values()
            .map(|fact| fact.bytes.len() + 640)
            .sum();
        let oracle = std::mem::size_of::<VerifiedClosure>()
            + fact_bytes
            + (closure.keys.capacity() + closure.added.capacity() + closure.removed.capacity())
                * std::mem::size_of::<DependencyKey>();
        assert_eq!(closure.fact_bytes, fact_bytes);
        assert_eq!(closure.retained_bytes(), oracle);
    }
    let a = reward(1);
    let b = reward(2);
    let auth = authorization([3; 32], 1);
    let base = facts([a.clone(), b.clone(), auth.clone()])
        .verified_closure(&evaluation([a.0, b.0, auth.0]), None)
        .unwrap();
    assert_bytes(&base);
    let replacement = authorization(auth.0.id, 2);
    let next = facts([b.clone(), replacement.clone()])
        .verified_closure(&evaluation([b.0, replacement.0]), Some(&base))
        .unwrap();
    assert_bytes(&next);
    let mut metadata_only = evaluation([b.0, replacement.0]);
    metadata_only.tip[0] ^= 1;
    let shifted = facts([b.clone(), replacement])
        .verified_closure(&metadata_only, Some(&next))
        .unwrap();
    assert_bytes(&shifted);
    let emptied = facts([])
        .verified_closure(&evaluation([]), Some(&shifted))
        .unwrap();
    assert_bytes(&emptied);
    assert_eq!(emptied.fact_bytes, 0);
    let rebuilt = facts([a.clone()])
        .verified_closure(&evaluation([a.0]), Some(&emptied))
        .unwrap();
    assert_bytes(&rebuilt);
    let copy: Arc<[u8]> = Arc::from(a.1.as_ref());
    let mut changed = evaluation([a.0]);
    changed.tip[0] ^= 1;
    let copied = facts([(a.0, copy)])
        .verified_closure(&changed, Some(&rebuilt))
        .unwrap();
    assert_bytes(&copied);
    assert!(copied.added().is_empty());
    assert_eq!(copied.fact_bytes, rebuilt.fact_bytes);
}
