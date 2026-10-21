use super::*;

#[test]
fn native_rejections_preserve_duplicates_orphans_and_shutdown_correctly() {
    use grin_chain::Error as E;
    assert!(submission_result(Err(E::Unfit("duplicate block".into()))).unwrap());
    assert!(submission_result(Err(E::OldBlock)).unwrap());
    assert!(!submission_result(Err(E::InvalidRoot)).unwrap());
    assert!(!submission_result(Err(E::InvalidPow)).unwrap());
    for error in [
        E::Orphan,
        E::Stopped,
        E::TxHashSetErr("disk unavailable".into()),
        E::Other("unknown failure".into()),
    ] {
        assert_eq!(submission_result(Err(error.clone())).unwrap_err(), error);
    }
}

pub(crate) fn winning_template(
    node: &mut Node,
    wallet: &Signer,
    payments: &crate::wallet::payments::Payments<crate::wallet::payments::tests::Offline>,
) -> Template {
    use grin_core::{
        global,
        pow::{self, Difficulty},
    };
    let mut template = crate::node::tests::prepare_template(
        node,
        wallet,
        payments,
        grin_core::core::Transaction::empty(),
    )
    .unwrap();
    let parent = node
        .chain
        .get_block_header(&template.block.header.prev_hash)
        .unwrap();
    let target =
        template.block.header.total_difficulty().to_num() - parent.total_difficulty().to_num();
    pow::pow_size(
        &mut template.block.header,
        Difficulty::from_num(target),
        global::proofsize(),
        global::min_edge_bits(),
    )
    .unwrap();
    template
}
#[test]
fn winning_block_survives_native_acceptance_and_pool_failure() {
    use grin_core::core::hash::Hashed;
    let (mut node, wallet, payments, _dir) = crate::node::tests::setup();
    let template = winning_template(&mut node, &wallet, &payments);
    let solutions = Solutions::new(&node.store);
    let hash = template.block.hash();
    let node = Arc::new(Mutex::new(node));
    node.lock().unwrap().limits.bytes = 1;
    let result = solutions
        .submit(
            &node,
            &template,
            &wallet,
            template.block.header.pow.nonce,
            template.block.header.pow.proof.clone(),
        )
        .unwrap();
    assert!(result.native_accepted);
    assert!(!result.pool_accepted);
    assert!(node.lock().unwrap().chain.get_block(&hash).is_ok());
    assert_eq!(solutions.pending().unwrap(), vec![result.work_id]);
    node.lock().unwrap().limits = crate::limits::Limits::default();
    let reopened = Solutions::new(&node.lock().unwrap().store);
    reopened.replay(&node).unwrap();
    assert!(reopened.pending().unwrap().is_empty());
    assert!(node
        .lock()
        .unwrap()
        .store
        .heads()
        .unwrap()
        .iter()
        .any(|head| head.tip == result.work_id));
}
#[test]
fn saved_winning_block_is_relayed_by_runtime_replay() {
    use grin_core::core::hash::Hashed;
    let (mut node, wallet, payments, _dir) = crate::node::tests::setup();
    let template = winning_template(&mut node, &wallet, &payments);
    let solutions = Solutions::new(&node.store);
    let id = solutions
        .save(
            &template,
            &wallet,
            template.block.header.pow.nonce,
            template.block.header.pow.proof.clone(),
        )
        .unwrap();
    let hash = template.block.hash();
    assert!(node.chain.get_block(&hash).is_err());
    drop(solutions);
    let node = Arc::new(Mutex::new(node));
    let reopened = Solutions::new(&node.lock().unwrap().store);
    reopened.replay(&node).unwrap();
    assert!(node.lock().unwrap().chain.get_block(&hash).is_ok());
    assert!(reopened.pending().unwrap().is_empty());
    assert!(node
        .lock()
        .unwrap()
        .store
        .heads()
        .unwrap()
        .iter()
        .any(|head| head.tip == id));
}

#[test]
fn pending_solution_retains_dependencies_through_collection_and_restart() {
    use crate::objects::Source;
    let (mut node, signer, payments, _dir) = crate::node::tests::setup();
    let template = winning_template(&mut node, &signer, &payments);
    let solutions = Solutions::new(&node.store);
    let id = solutions
        .save(
            &template,
            &signer,
            template.block.header.pow.nonce,
            template.block.header.pow.proof.clone(),
        )
        .unwrap();
    assert!(submit_native(&node.chain, template.block.clone()).unwrap());
    crate::node::tests::prepare_template(
        &mut node,
        &signer,
        &payments,
        grin_core::core::Transaction::empty(),
    )
    .unwrap();
    let chain = node.chain.clone();
    let profile = node.profile.clone();
    let path = node.store.path().to_owned();
    drop(node);
    drop(solutions);
    let store = Store::open(
        &path,
        profile.pool_id,
        crate::storage::StorageLimits::default(),
    )
    .unwrap();
    let mut node = Node::start(chain, profile, store, crate::limits::Limits::default()).unwrap();
    for _ in 0..4 {
        let plan = node.prepare_maintenance().unwrap().run().unwrap();
        node.finish_maintenance(
            plan,
            chrono::Utc::now().timestamp() + 3600,
            &Default::default(),
        )
        .unwrap();
    }
    let registration = Key::new(Kind::Registration, template.job.registration);
    assert!(node.store.read(registration).is_ok());
    let solutions = Solutions::new(&node.store);
    assert_eq!(solutions.pending().unwrap(), vec![id]);
    let node = Arc::new(Mutex::new(node));
    solutions.replay(&node).unwrap();
    assert!(solutions.pending().unwrap().is_empty());
    solutions
        .db
        .solution_transaction::<_, Error>(|tx, _| {
            assert!(!solutions.runtime.retained_in(tx, retention(id))?);
            Ok(())
        })
        .unwrap();
    assert!(node
        .lock()
        .unwrap()
        .store
        .heads()
        .unwrap()
        .iter()
        .any(|head| head.tip == id));
}

#[test]
fn transient_native_orphan_is_retained_but_pruned_parent_releases_the_intent() {
    use grin_core::core::hash::Hashed;
    let (mut node, signer, payments, _dir) = crate::node::tests::setup();
    let first = winning_template(&mut node, &signer, &payments);
    assert!(submit_native(&node.chain, first.block.clone()).unwrap());
    let stale = winning_template(&mut node, &signer, &payments);
    let solutions = Solutions::new(&node.store);
    let id = solutions
        .save(
            &stale,
            &signer,
            stale.block.header.pow.nonce,
            stale.block.header.pow.proof.clone(),
        )
        .unwrap();
    for _ in 0..5 {
        let next = winning_template(&mut node, &signer, &payments);
        assert!(submit_native(&node.chain, next.block).unwrap());
    }
    assert!(node.profile.finance_low(node.chain.head().unwrap().height) > stale.job.anchor.height);
    let store = node.chain.store();
    let mut batch = store.batch().unwrap();
    batch.delete_block(&first.block.hash()).unwrap();
    batch.commit().unwrap();
    let node = Arc::new(Mutex::new(node));
    let error = solutions.replay(&node).unwrap_err();
    assert!(matches!(error, Error::NativeUnavailable(_)));
    assert_eq!(solutions.pending().unwrap(), vec![id]);
    let chain = node.lock().unwrap().chain.clone();
    let head = chain.head().unwrap();
    let mut batch = store.batch().unwrap();
    batch.save_body_tail(&head).unwrap();
    batch.commit().unwrap();
    solutions.replay(&node).unwrap();
    assert!(solutions.pending().unwrap().is_empty());
    solutions
        .db
        .solution_transaction::<_, Error>(|tx, _| {
            assert!(!solutions.runtime.retained_in(tx, retention(id))?);
            Ok(())
        })
        .unwrap();
}
