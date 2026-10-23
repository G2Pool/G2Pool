use super::*;
use crate::{
    consensus::{Evaluation, Validator},
    mining::Template,
    storage::StorageLimits,
    wallet::Signer,
};
use grin_core::{
    core::{hash::Hashed, Block, BlockHeader, Transaction},
    global::{self, ChainTypes},
    libtx::{reward, ProofBuilder},
    pow::{self, Difficulty},
};
use grin_keychain::{ExtKeychain, ExtKeychainPath, Keychain};

pub(crate) fn setup() -> (
    Node,
    Signer,
    crate::wallet::payments::Payments<crate::wallet::payments::tests::Offline>,
    tempfile::TempDir,
) {
    global::set_local_chain_type(ChainTypes::AutomatedTesting);
    let dir = tempfile::tempdir().unwrap();
    let chain = Arc::new(
        Chain::init(
            dir.path().join("grin").to_str().unwrap().to_owned(),
            Arc::new(grin_chain::types::NoopAdapter {}),
            pow::mine_genesis_block().unwrap(),
            pow::verify_size,
            false,
            None,
        )
        .unwrap(),
    );
    let profile = Profile::new(
        crate::native::hash_bytes(chain.genesis().hash()),
        ChainTypes::AutomatedTesting,
        &crate::pool::Pool::alpha(),
    );
    let payments = crate::wallet::payments::Payments::create(
        dir.path(),
        crate::wallet::payments::tests::Offline,
    )
    .unwrap();
    let wallet = payments.signer().unwrap();
    let mut store = Store::open(
        &dir.path().join("pool"),
        profile.pool_id,
        StorageLimits {
            heads: 1,
            ..Default::default()
        },
    )
    .unwrap();
    store.bind_wallet(wallet.account()).unwrap();
    (
        Node::start(chain, profile, store, Limits::default()).unwrap(),
        wallet,
        payments,
        dir,
    )
}

pub(crate) fn prepare_template(
    node: &mut Node,
    wallet: &Signer,
    payments: &crate::wallet::payments::Payments<crate::wallet::payments::tests::Offline>,
    transaction: Transaction,
) -> Result<Template> {
    recover(node);
    crate::mining::Preparation::new(node)?
        .run(wallet, payments, transaction)?
        .finish(node)
}

fn native_block(node: &Node, parent: &BlockHeader, child: u32) -> Block {
    let keychain = ExtKeychain::from_seed(&[17; 32], false).unwrap();
    let path = ExtKeychainPath::new(1, child, 0, 0, 0).to_identifier();
    let reward = reward::output(&keychain, &ProofBuilder::new(&keychain), &path, 0, false).unwrap();
    let target = grin_core::consensus::next_difficulty(
        parent.height + 1,
        grin_chain::store::DifficultyIter::from(parent.hash(), node.chain.store()),
    );
    let mut block = Block::new(parent, &[], target.difficulty, reward).unwrap();
    block.header.timestamp = parent.timestamp + chrono::Duration::seconds(60);
    block.header.pow.secondary_scaling = target.secondary_scaling;
    node.chain.set_txhashset_roots(&mut block).unwrap();
    pow::pow_size(
        &mut block.header,
        target.difficulty,
        global::proofsize(),
        global::min_edge_bits(),
    )
    .unwrap();
    block
}

pub(crate) fn share(
    node: &mut Node,
    wallet: &Signer,
    payments: &crate::wallet::payments::Payments<crate::wallet::payments::tests::Offline>,
) -> (Id, Template) {
    share_at(node, wallet, payments, None)
}

fn share_at(
    node: &mut Node,
    wallet: &Signer,
    payments: &crate::wallet::payments::Payments<crate::wallet::payments::tests::Offline>,
    timestamp: Option<i64>,
) -> (Id, Template) {
    let mut template = prepare_template(node, wallet, payments, Transaction::empty()).unwrap();
    if let Some(timestamp) = timestamp {
        template.job.timestamp = timestamp;
        template.block.header.timestamp = chrono::DateTime::from_timestamp(timestamp, 0).unwrap();
        let scalar = scalar(id(&template.job).unwrap()).unwrap();
        let parent = node.view().unwrap().anchor(&template.job.anchor).unwrap();
        let negative = {
            let handle = grin_util::static_secp_instance();
            let secp = handle.lock();
            let secret = grin_util::secp::key::SecretKey::from_slice(&secp, &scalar).unwrap();
            let subtract = if parent.total_kernel_offset.is_zero() {
                vec![]
            } else {
                vec![parent.total_kernel_offset.secret_key(&secp).unwrap()]
            };
            let beta = secp.blind_sum(vec![secret], subtract).unwrap();
            secp.blind_sum(vec![], vec![beta]).unwrap()
        };
        let binding = crate::wallet::kernel(
            negative,
            grin_core::core::KernelFeatures::Plain {
                fee: grin_core::core::FeeFields::zero(),
            },
        )
        .unwrap();
        let old: grin_core::core::TxKernel =
            crate::native::parse(&template.residual.binding_kernel.0).unwrap();
        let mut kernels: Vec<_> = template
            .block
            .kernels()
            .iter()
            .copied()
            .filter(|kernel| *kernel != old)
            .collect();
        kernels.push(binding);
        template.block.body = grin_core::core::TransactionBody::init(
            template.block.inputs(),
            template.block.outputs(),
            &kernels,
            false,
        )
        .unwrap();
        template.block.header.total_kernel_offset =
            grin_keychain::BlindingFactor::from_slice(&scalar);
        template.residual.binding_kernel = Blob(crate::native::native_bytes(&binding).unwrap());
        node.chain.set_txhashset_roots(&mut template.block).unwrap();
    }
    pow::pow_size(
        &mut template.block.header,
        Difficulty::from_num(template.job.difficulty),
        global::proofsize(),
        global::min_edge_bits(),
    )
    .unwrap();
    let core = Core {
        header: Blob(crate::native::native_bytes(&template.block.header).unwrap()),
        job: Blob(encode(&template.job).unwrap()),
    };
    let id = hash("WORK_ID", &core.header.0);
    let mut objects = template.material.clone();
    objects.extend([
        Object::new(Key::new(Kind::Core, id), encode(&core).unwrap()).unwrap(),
        Object::new(
            Key::new(Kind::Authorization, id),
            encode(&wallet.authorize(id).unwrap()).unwrap(),
        )
        .unwrap(),
        Object::new(
            Key::new(Kind::Residual, id),
            encode(&template.residual).unwrap(),
        )
        .unwrap(),
    ]);
    let verified = node.prepare_stage(objects).unwrap().run().unwrap();
    node.finish_stage(verified).unwrap();
    loop {
        let turn = node.take_turn(id).unwrap().unwrap();
        let (turn, outcome) = turn.run();
        match node.finish_turn(turn, outcome).unwrap() {
            EvaluationProgress::Ready(_) => break,
            EvaluationProgress::Pending => {}
            EvaluationProgress::Missing(key) => panic!("missing {key:?}"),
        }
    }
    (id, template)
}

fn reorg(node: &Node, parent: &BlockHeader) {
    let fork = native_block(node, parent, 100);
    node.chain
        .process_block(fork.clone(), grin_chain::Options::MINE)
        .unwrap();
    let next = native_block(node, &fork.header, 101);
    node.chain
        .process_block(next, grin_chain::Options::MINE)
        .unwrap();
}

fn recover(node: &mut Node) {
    for _ in 0..200 {
        let Some(recovery) = node.prepare_recovery().unwrap() else {
            return;
        };
        node.finish_recovery(recovery.run()).unwrap();
    }
    panic!("recovery did not finish");
}

fn fetch_missing(source: &Node, target: &mut Node, tip: Id, key: Key) {
    let keys = if key.kind == Kind::Core {
        vec![key, Key::new(Kind::Authorization, key.id)]
    } else {
        vec![key]
    };
    target.protect_download(tip, &keys).unwrap();
    let objects = keys
        .into_iter()
        .map(|key| Object::new(key, source.store.read(key).unwrap()).unwrap())
        .collect();
    let checked = target.prepare_stage(objects).unwrap().run().unwrap();
    target.finish_stage(checked).unwrap();
}

fn acquire(source: &Node, target: &mut Node, tip: Id) -> Evaluation {
    for _ in 0..1000 {
        let turn = target.take_turn(tip).unwrap().unwrap();
        let (turn, outcome) = turn.run();
        match target.finish_turn(turn, outcome).unwrap() {
            EvaluationProgress::Ready(result) => return *result,
            EvaluationProgress::Pending => {}
            EvaluationProgress::Missing(key) => fetch_missing(source, target, tip, key),
        }
    }
    panic!("acquisition did not finish");
}

#[test]
fn reorg_retires_invalid_root_after_restart_and_allows_new_work() {
    let (mut node, wallet, payments, _dir) = setup();
    let parent = node.chain.head_header().unwrap();
    let block = native_block(&node, &parent, 1);
    node.chain
        .process_block(block, grin_chain::Options::MINE)
        .unwrap();
    share(&mut node, &wallet, &payments);
    reorg(&node, &parent);
    let chain = node.chain.clone();
    let profile = node.profile.clone();
    let path = node.store.path().to_owned();
    drop(node);
    let store = Store::open(&path, profile.pool_id, StorageLimits::default()).unwrap();
    let mut node = Node::start(chain, profile, store, Limits::default()).unwrap();
    recover(&mut node);
    assert!(node.store.heads().unwrap().is_empty());
    assert!(matches!(node.parent().unwrap(), Parent::Root(_)));
    prepare_template(&mut node, &wallet, &payments, Transaction::empty()).unwrap();
}

#[test]
fn reorg_replaces_invalid_head_with_valid_ancestor_at_head_limit() {
    let (mut node, wallet, payments, _dir) = setup();
    let (ancestor, _) = share(&mut node, &wallet, &payments);
    let parent = node.chain.head_header().unwrap();
    let block = native_block(&node, &parent, 1);
    node.chain
        .process_block(block, grin_chain::Options::MINE)
        .unwrap();
    let (invalid, _) = share(&mut node, &wallet, &payments);
    assert_eq!(node.store.heads().unwrap().len(), 1);
    reorg(&node, &parent);
    recover(&mut node);
    assert_eq!(node.parent().unwrap(), Parent::Receipt(ancestor));
    let heads = node.store.heads().unwrap();
    assert_eq!(heads.len(), 1);
    assert_eq!(heads[0].tip, ancestor);
    assert_ne!(heads[0].tip, invalid);
}

#[test]
fn prepared_template_does_not_publish_after_native_head_changes() {
    let (mut node, wallet, payments, _dir) = setup();
    let prepared = crate::mining::Preparation::new(&mut node)
        .unwrap()
        .run(&wallet, &payments, Transaction::empty())
        .unwrap();
    assert!(node.store.wallet_registration().unwrap().is_none());
    let head = node.chain.head_header().unwrap();
    let block = native_block(&node, &head, 200);
    node.chain
        .process_block(block, grin_chain::Options::MINE)
        .unwrap();
    assert!(matches!(prepared.finish(&mut node), Err(Error::View)));
    assert!(node.store.wallet_registration().unwrap().is_none());
    prepare_template(&mut node, &wallet, &payments, Transaction::empty()).unwrap();
    assert!(node.store.wallet_registration().unwrap().is_some());
}

#[test]
fn valid_witness_retransmission_recovers_an_unsealed_candidate() {
    use crate::objects::{Key, Kind, Source};
    let (mut source, signer, wallet, _source_dir) = crate::node::tests::setup();
    let (tip, template) = crate::node::tests::share(&mut source, &signer, &wallet);
    let (mut target, _signer, _wallet, _target_dir) = crate::node::tests::setup();
    let mut keys = template.pins();
    keys.extend([
        Key::new(Kind::Core, tip),
        Key::new(Kind::Authorization, tip),
        Key::new(Kind::Residual, tip),
    ]);
    let objects: Vec<_> = keys
        .into_iter()
        .map(|key| crate::storage::Object::new(key, source.store.read(key).unwrap()).unwrap())
        .collect();
    for chunk in objects.chunks(32) {
        let checked = target.prepare_stage(chunk.to_vec()).unwrap().run().unwrap();
        target.finish_stage(checked).unwrap();
    }
    let view = target.view().unwrap();
    crate::consensus::Validator::bounded(
        &target.store,
        &view,
        target.limits.clone(),
        target.cancel.clone(),
    )
    .full_check(tip)
    .unwrap();
    let good = objects
        .iter()
        .find(|object| object.key == Key::new(Kind::Residual, tip))
        .unwrap()
        .clone();
    let mut changed: crate::codec::Residual = crate::codec::decode(&good.bytes).unwrap();
    changed.finder_kernel = crate::codec::Blob(vec![1]);
    let bad =
        crate::storage::Object::new(good.key, crate::codec::encode(&changed).unwrap()).unwrap();
    let checked = target
        .prepare_stage(vec![bad.clone()])
        .unwrap()
        .run()
        .unwrap();
    target.finish_stage(checked).unwrap();
    let checked = target.prepare_stage(vec![good]).unwrap().run().unwrap();
    target.finish_stage(checked).unwrap();
    assert_ne!(target.store.read(bad.key).unwrap(), bad.bytes);
    let view = target.view().unwrap();
    assert!(crate::consensus::Validator::bounded(
        &target.store,
        &view,
        target.limits.clone(),
        target.cancel.clone()
    )
    .full_check(tip)
    .is_ok());
}

#[test]
fn pruned_branch_bootstraps_across_native_extension_and_restart() {
    let (source, signer, payments, source_dir) = setup();
    let mut profile = source.profile.clone();
    profile.lifetime = 8;

    let chain = source.chain.clone();
    drop(source);
    let mut store = Store::open(
        &source_dir.path().join("rolling"),
        profile.pool_id,
        StorageLimits {
            heads: 1,
            ..StorageLimits::default()
        },
    )
    .unwrap();
    store.bind_wallet(signer.account()).unwrap();
    let mut source = Node::start(chain, profile, store, Limits::default()).unwrap();
    let mut origin = None;
    let mut tip = [0; 32];
    let shares = 4 * (source.profile.work_window_limit() + usize::from(source.profile.uncle_depth));
    let start = chrono::Utc::now().timestamp() - shares as i64 * 10;
    for i in 0..shares {
        tip = share_at(&mut source, &signer, &payments, Some(start + i as i64 * 10)).0;
        origin.get_or_insert(tip);
    }
    let keep = source.collection_keep(&BTreeSet::new()).unwrap();
    for _ in 0..32 {
        source
            .store
            .collect(&keep, chrono::Utc::now().timestamp() + 1)
            .unwrap();
    }
    assert!(matches!(
        source.store.read(Key::new(Kind::Core, origin.unwrap())),
        Err(Error::Unavailable(_))
    ));
    let cold = tempfile::tempdir().unwrap();
    let path = cold.path().join("pool");
    let store = Store::open(&path, source.profile.pool_id, StorageLimits::default()).unwrap();
    let mut target = Node::start(
        source.chain.clone(),
        source.profile.clone(),
        store,
        Limits::default(),
    )
    .unwrap();
    let mut advanced = false;
    let mut fetched = 0;
    let mut grown = 0;
    loop {
        let turn = target.take_turn(tip).unwrap().unwrap();
        let (turn, outcome) = turn.run();
        match target.finish_turn(turn, outcome).unwrap() {
            EvaluationProgress::Ready(result) => {
                assert_eq!(result.primary.len(), source.profile.work_window_limit());
                break;
            }
            EvaluationProgress::Pending => {}
            EvaluationProgress::Missing(key) => {
                fetch_missing(&source, &mut target, tip, key);
                fetched += 1;
                assert!(fetched < 1000);
                if !advanced && target.tasks[&tip].session.facts().work(tip).is_some() {
                    for child in 300..305 {
                        let parent = source.chain.head_header().unwrap();
                        let block = native_block(&source, &parent, child);
                        source
                            .chain
                            .process_block(block, grin_chain::Options::MINE)
                            .unwrap();
                    }
                    target.ensure_view().unwrap();
                    assert!(target.tasks.contains_key(&tip));
                    advanced = true;
                }
                if fetched % 8 == 0 && grown < 16 {
                    share_at(
                        &mut source,
                        &signer,
                        &payments,
                        Some(start + (shares + grown) as i64 * 10),
                    );
                    grown += 1;
                    let keep = source.collection_keep(&BTreeSet::new()).unwrap();
                    source
                        .store
                        .collect(
                            &keep,
                            chrono::Utc::now().timestamp()
                                - crate::storage::sharechain::ACQUISITION_LIFETIME as i64,
                        )
                        .unwrap();
                }
            }
        }
    }
    assert!(advanced);
    assert!(grown > source.profile.work_window_limit());
    assert_ne!(source.parent().unwrap(), Parent::Receipt(tip));
    assert_eq!(target.parent().unwrap(), Parent::Receipt(tip));
    let warm = target.evaluate_cached(tip).unwrap();
    let window = source.profile.work_window_limit();
    let low = source
        .profile
        .work_low(source.chain.head_header().unwrap().height);
    assert_eq!(
        warm.1.branch().unwrap().window_score(window, low).unwrap(),
        source
            .evaluate_cached(tip)
            .unwrap()
            .1
            .branch()
            .unwrap()
            .window_score(window, low)
            .unwrap()
    );
    drop(target);
    let store = Store::open(&path, source.profile.pool_id, StorageLimits::default()).unwrap();
    let mut target = Node::start(
        source.chain.clone(),
        source.profile.clone(),
        store,
        Limits::default(),
    )
    .unwrap();
    recover(&mut target);
    assert_eq!(target.parent().unwrap(), Parent::Receipt(tip));
    let Parent::Receipt(parent_tip) = source.parent().unwrap() else {
        panic!("source has no selected head");
    };
    let child = share(&mut source, &signer, &payments).0;
    assert_ne!(child, tip);
    let (child_result, child_facts) = source.evaluate_cached(child).unwrap();
    let (parent_result, parent_facts) = source.evaluate_cached(parent_tip).unwrap();
    let view = source.view().unwrap();
    let cold_parent = Validator::bounded(
        &source.store,
        &view,
        source.limits.clone(),
        source.cancel.clone(),
    )
    .evaluate_with_facts(parent_tip)
    .unwrap();
    assert_eq!(
        parent_result.primary.len(),
        source.profile.work_window_limit()
    );
    assert_eq!(
        parent_facts
            .branch()
            .unwrap()
            .window_score(window, low)
            .unwrap(),
        cold_parent
            .1
            .branch()
            .unwrap()
            .window_score(window, low)
            .unwrap()
    );
    assert_eq!(
        source.compare_verified_branches(
            child_result.tip,
            &child_facts,
            parent_result.tip,
            &parent_facts
        ),
        ForkComparison::PreferLeft
    );
    let inflight = loop {
        let turn = target.take_turn(child).unwrap().unwrap();
        let (turn, outcome) = turn.run();
        match target.finish_turn(turn, outcome).unwrap() {
            EvaluationProgress::Missing(key) => {
                fetch_missing(&source, &mut target, child, key);
            }
            EvaluationProgress::Pending => {}
            EvaluationProgress::Ready(_) => panic!("child was published before the reorg"),
        }
        if target.tasks[&child].session.facts().work(child).is_some() {
            break target.take_turn(child).unwrap().unwrap().run();
        }
    };
    let keep = source.collection_keep(&BTreeSet::new()).unwrap();
    for _ in 0..32 {
        source
            .store
            .collect(&keep, chrono::Utc::now().timestamp() + 1)
            .unwrap();
    }
    let parent = source
        .chain
        .get_header_by_height(source.chain.head_header().unwrap().height - 1)
        .unwrap();
    reorg(&source, &parent);
    assert!(matches!(
        target.finish_turn(inflight.0, inflight.1).unwrap(),
        EvaluationProgress::Pending
    ));
    recover(&mut target);
    assert_eq!(target.parent().unwrap(), Parent::Receipt(tip));
    let turn = target.take_turn(child).unwrap().unwrap();
    let (turn, outcome) = turn.run();
    assert!(matches!(
        target.finish_turn(turn, outcome),
        Err(Error::View)
    ));
    assert!(!target.tasks.contains_key(&child));
    assert!(!target.acquisition_heads().unwrap().contains(&child));
    recover(&mut source);
    assert!(matches!(source.parent().unwrap(), Parent::Root(_)));
    assert!(source.store.heads().unwrap().is_empty());
    assert!(source.tasks.is_empty());
    prepare_template(&mut source, &signer, &payments, Transaction::empty()).unwrap();
}

impl Node {
    fn evaluate_cached(&self, tip: Id) -> Result<(Evaluation, Facts)> {
        let view = self.view()?;
        let mut validator =
            Validator::bounded(&self.store, &view, self.limits.clone(), self.cancel.clone());
        if let Some((anchor, facts)) = &self.warm {
            if *anchor == view.head {
                validator.seed(facts);
            }
        }
        validator.evaluate_with_facts(tip)
    }
}

#[test]
fn short_branches_preserve_incumbent_across_refresh_and_restart_and_enforce_freshness() {
    let (mut left, signer, payments, dir) = setup();
    let peer = |name: &str| {
        let store = Store::open(
            &dir.path().join(name),
            left.profile.pool_id,
            StorageLimits::default(),
        )
        .unwrap();
        Node::start(
            left.chain.clone(),
            left.profile.clone(),
            store,
            Limits::default(),
        )
        .unwrap()
    };
    let mut right = peer("right");
    let mut target = peer("target");
    let timestamp = left.chain.genesis().timestamp.timestamp() + 60;
    let a = share_at(&mut left, &signer, &payments, Some(timestamp)).0;
    let b = share_at(&mut right, &signer, &payments, Some(timestamp + 10)).0;
    let (first, second, incumbent, alternative) = if a > b {
        (&left, &right, a, b)
    } else {
        (&right, &left, b, a)
    };
    acquire(first, &mut target, incumbent);
    acquire(second, &mut target, alternative);
    assert_eq!(target.parent().unwrap(), Parent::Receipt(incumbent));
    assert_eq!(target.store.heads().unwrap().len(), 2);
    target.recovery.push_back((alternative, alternative));
    let background = target.prepare_recovery().unwrap().unwrap().run();
    let block = native_block(&left, &left.chain.head_header().unwrap(), 600);
    left.chain
        .process_block(block, grin_chain::Options::MINE)
        .unwrap();
    target.ensure_view().unwrap();
    assert!(target.take_turn(alternative).unwrap().is_none());
    target.finish_recovery(background).unwrap();
    assert_eq!(target.recovery.front().unwrap().0, incumbent);
    let initial = target.prepare_recovery().unwrap().unwrap();
    let initial = initial.run();
    let block = native_block(&left, &left.chain.head_header().unwrap(), 601);
    left.chain
        .process_block(block, grin_chain::Options::MINE)
        .unwrap();
    assert!(target.take_turn(alternative).unwrap().is_none());
    target.finish_recovery(initial).unwrap();
    recover(&mut target);
    assert_eq!(target.parent().unwrap(), Parent::Receipt(incumbent));
    let path = target.store.path().to_owned();
    drop(target);
    let store = Store::open(&path, left.profile.pool_id, StorageLimits::default()).unwrap();
    let mut target = Node::start(
        left.chain.clone(),
        left.profile.clone(),
        store,
        Limits::default(),
    )
    .unwrap();
    recover(&mut target);
    assert_eq!(target.parent().unwrap(), Parent::Receipt(incumbent));

    let timestamp = right.chain.head_header().unwrap().timestamp.timestamp() + 10;
    let stronger = share_at(&mut right, &signer, &payments, Some(timestamp)).0;
    let a_facts = &left.warm.as_ref().unwrap().1;
    let b_facts = &right.warm.as_ref().unwrap().1;
    assert!(a_facts.branch().unwrap().boundary.is_none());
    assert!(b_facts.branch().unwrap().boundary.is_none());
    assert_eq!(
        right.compare_verified_branches(stronger, b_facts, a, a_facts),
        ForkComparison::PreferLeft
    );
    for child in 602..623 {
        let block = native_block(&left, &left.chain.head_header().unwrap(), child);
        left.chain
            .process_block(block, grin_chain::Options::MINE)
            .unwrap();
    }
    assert_eq!(
        right.compare_verified_branches(stronger, b_facts, a, a_facts),
        ForkComparison::PreferRight
    );
}

#[test]
fn disconnected_branches_compare_recent_work_with_native_freshness() {
    let (node, signer, payments, dir) = setup();
    let mut profile = node.profile.clone();
    profile.share_window = 12;

    let mut block = native_block(&node, &node.chain.head_header().unwrap(), 400);
    let now = chrono::Utc::now().timestamp();
    block.header.timestamp = chrono::DateTime::from_timestamp(now - 600, 0).unwrap();
    let target = node
        .view()
        .unwrap()
        .target(&Anchor {
            height: 0,
            hash: crate::native::hash_bytes(node.chain.genesis().hash()),
        })
        .unwrap();
    pow::pow_size(
        &mut block.header,
        target.difficulty,
        global::proofsize(),
        global::min_edge_bits(),
    )
    .unwrap();
    node.chain
        .process_block(block, grin_chain::Options::MINE)
        .unwrap();
    let branch = |name: &str, start: i64, interval: i64, window: u32| {
        let mut profile = profile.clone();
        profile.share_window = window;

        let store = Store::open(
            &dir.path().join(name),
            profile.pool_id,
            StorageLimits {
                heads: 1,
                ..StorageLimits::default()
            },
        )
        .unwrap();
        let mut branch = Node::start(
            node.chain.clone(),
            profile.clone(),
            store,
            Limits::default(),
        )
        .unwrap();
        let mut tip = [0; 32];
        for i in 0..17 {
            tip = share_at(&mut branch, &signer, &payments, Some(start + i * interval)).0;
        }
        (branch, tip)
    };
    let (mut incumbent, a) = branch("incumbent", now - 400, 10, 12);
    let (candidate, b) = branch("candidate", now - 200, 5, 12);
    let a_facts = incumbent.warm.as_ref().unwrap().1.clone();
    let b_facts = &candidate.warm.as_ref().unwrap().1;
    let low = candidate
        .profile
        .work_low(candidate.chain.head_header().unwrap().height);
    assert!(
        candidate
            .warm
            .as_ref()
            .unwrap()
            .1
            .branch()
            .unwrap()
            .window_score(12, low)
            .unwrap()
            > a_facts.branch().unwrap().window_score(12, low).unwrap()
    );
    assert_eq!(
        candidate.compare_verified_branches(b, b_facts, a, &a_facts),
        ForkComparison::PreferLeft
    );
    let (compressed, c) = branch("compressed", now - 100, 2, 12);
    assert_eq!(
        compressed.compare_verified_branches(c, &compressed.warm.as_ref().unwrap().1, a, &a_facts),
        ForkComparison::PreferRight
    );
    let (small_incumbent, small_a) = branch("small-incumbent", now - 400, 10, 3);
    let (small_candidate, small_b) = branch("small-candidate", now - 200, 5, 3);
    assert_eq!(
        small_candidate.compare_verified_branches(
            small_b,
            &small_candidate.warm.as_ref().unwrap().1,
            small_a,
            &small_incumbent.warm.as_ref().unwrap().1
        ),
        ForkComparison::PreferLeft
    );
    acquire(&candidate, &mut incumbent, b);
    assert_eq!(incumbent.parent().unwrap(), Parent::Receipt(b));
    assert_eq!(incumbent.store.heads().unwrap().len(), 1);
    for child in 401..422 {
        let block = native_block(&node, &node.chain.head_header().unwrap(), child);
        node.chain
            .process_block(block, grin_chain::Options::MINE)
            .unwrap();
    }
    assert_eq!(
        candidate.compare_verified_branches(b, b_facts, a, &a_facts),
        ForkComparison::PreferRight
    );
}

#[test]
fn slow_rolling_window_matches_cold_recovery_and_reorg() {
    let (original, signer, payments, dir) = setup();
    let mut profile = original.profile.clone();
    profile.share_window = 12;
    profile.lifetime = 8;

    let chain = original.chain.clone();
    drop(original);
    let path = dir.path().join("slow");
    let limits = StorageLimits {
        heads: 1,
        ..Default::default()
    };
    let mut store = Store::open(&path, profile.pool_id, limits.clone()).unwrap();
    store.bind_wallet(signer.account()).unwrap();
    let mut node = Node::start(chain.clone(), profile.clone(), store, Limits::default()).unwrap();
    let mut tip = [0; 32];
    let mut shares = Vec::new();
    for round in 0..5 {
        if round > 0 {
            for child in 0..6 {
                let parent = chain.head_header().unwrap();
                let block = native_block(&node, &parent, 1000 + round * 6 + child);
                chain
                    .process_block(block, grin_chain::Options::MINE)
                    .unwrap();
            }
        }
        let timestamp = chain.head_header().unwrap().timestamp.timestamp() + 1;
        tip = share_at(&mut node, &signer, &payments, Some(timestamp)).0;
        shares.push(tip);
        assert_eq!(node.parent().unwrap(), Parent::Receipt(tip));
    }
    assert_eq!(chain.head_header().unwrap().height, 24);
    let (warm, facts) = node.evaluate_cached(tip).unwrap();
    assert_eq!(warm.full, [shares[3], shares[4]].into_iter().collect());
    let low = profile.work_low(chain.head_header().unwrap().height);
    let expected_score = facts
        .branch()
        .unwrap()
        .window_score(profile.work_window_limit(), low)
        .unwrap();
    let keep = node.collection_keep(&BTreeSet::new()).unwrap();
    for _ in 0..32 {
        node.store
            .collect(&keep, chrono::Utc::now().timestamp() + 1)
            .unwrap();
    }
    assert!(matches!(
        node.store.read(Key::new(Kind::Residual, shares[0])),
        Err(Error::Unavailable(_))
    ));
    drop(node);
    let store = Store::open(&path, profile.pool_id, limits).unwrap();
    let mut cold = Node::start(chain, profile, store, Limits::default()).unwrap();
    let result = cold.prepare_recovery().unwrap().unwrap().run();
    let result = cold.finish_recovery(result);
    assert!(result.is_ok());
    recover(&mut cold);
    assert_eq!(cold.parent().unwrap(), Parent::Receipt(tip));
    let (checked, facts) = cold.evaluate_cached(tip).unwrap();
    assert_eq!(checked.full, warm.full);
    assert_eq!(checked.financial.record, warm.financial.record);
    assert_eq!(
        facts
            .branch()
            .unwrap()
            .window_score(cold.profile.work_window_limit(), low)
            .unwrap(),
        expected_score
    );
    let parent = cold.chain.get_header_by_height(23).unwrap();
    reorg(&cold, &parent);
    recover(&mut cold);
    assert_eq!(cold.parent().unwrap(), Parent::Receipt(shares[3]));
}

#[test]
fn native_refresh_keeps_running_acquisitions_pinned_across_restart() {
    use crate::storage::sharechain::{ObjectMode, RoutingKey, RuntimeBatch, RuntimeObject};
    let (mut source, signer, payments, dir) = setup();
    let tip = share(&mut source, &signer, &payments).0;
    let store = Store::open(
        &dir.path().join("leases"),
        source.profile.pool_id,
        StorageLimits::default(),
    )
    .unwrap();
    let mut target = Node::start(
        source.chain.clone(),
        source.profile.clone(),
        store,
        Limits::default(),
    )
    .unwrap();
    let material: Vec<_> = [Kind::Core, Kind::Authorization]
        .into_iter()
        .map(|kind| {
            let key = Key::new(kind, tip);
            (key, source.store.read(key).unwrap())
        })
        .collect();
    let objects: Vec<_> = material
        .iter()
        .map(|(key, bytes)| RuntimeObject {
            key: RoutingKey {
                kind: key.kind as u8,
                id: key.id,
            },
            mode: if key.kind == Kind::Core {
                ObjectMode::Immutable
            } else {
                ObjectMode::WorkVariant
            },
            representation: crate::storage::sharechain::Representation::Canonical,
            logical_digest: crate::codec::hash("PHYSICAL_OBJECT", bytes),
            bytes,
        })
        .collect();
    target
        .store
        .runtime()
        .apply(
            0,
            RuntimeBatch {
                objects: &objects,
                ..Default::default()
            },
        )
        .unwrap();
    let turn = target.take_turn(tip).unwrap().unwrap();
    let (turn, outcome) = turn.run();
    assert!(matches!(outcome, Ok(EvaluationProgress::Missing(_))));
    let block = native_block(&source, &source.chain.head_header().unwrap(), 1299);
    source
        .chain
        .process_block(block, grin_chain::Options::MINE)
        .unwrap();
    assert!(matches!(
        target.finish_turn(turn, outcome).unwrap(),
        EvaluationProgress::Pending
    ));
    let (_, durable) = target
        .store
        .runtime()
        .snapshot()
        .unwrap()
        .lease(tip)
        .unwrap()
        .unwrap();
    for (key, _) in &material {
        assert!(target.tasks[&tip].leased.contains(key));
        assert!(durable
            .iter()
            .any(|entry| entry.kind == key.kind as u8 && entry.id == key.id));
    }
    let deadline = target
        .store
        .acquire(tip, &BTreeSet::new(), target.limits.facts)
        .unwrap();
    let running = target.take_turn(tip).unwrap().unwrap();
    let other = [123; 32];
    let turn = target.take_turn(other).unwrap().unwrap();
    let (turn, outcome) = turn.run();
    assert!(matches!(
        target.finish_turn(turn, outcome).unwrap(),
        EvaluationProgress::Missing(_)
    ));
    let block = native_block(&source, &source.chain.head_header().unwrap(), 1300);
    source
        .chain
        .process_block(block, grin_chain::Options::MINE)
        .unwrap();
    target.ensure_view().unwrap();
    let keep = target.collection_keep(&BTreeSet::new()).unwrap();
    let removed = target
        .store
        .collect(&keep, chrono::Utc::now().timestamp() - 900)
        .unwrap();
    let result = target.store.read(Key::new(Kind::Core, tip));
    assert!(result.is_ok());
    assert_eq!(removed, 0);
    let (running, outcome) = running.run();
    assert!(matches!(
        target.finish_turn(running, outcome).unwrap(),
        EvaluationProgress::Pending
    ));
    assert_eq!(
        target
            .store
            .acquire(tip, &BTreeSet::new(), target.limits.facts)
            .unwrap(),
        deadline
    );
    let path = target.store.path().to_owned();
    drop(target);
    let store = Store::open(&path, source.profile.pool_id, StorageLimits::default()).unwrap();
    let mut target = Node::start(
        source.chain.clone(),
        source.profile.clone(),
        store,
        Limits::default(),
    )
    .unwrap();
    let turn = target.take_turn(tip).unwrap().unwrap();
    let (turn, outcome) = turn.run();
    assert!(matches!(
        target.finish_turn(turn, outcome).unwrap(),
        EvaluationProgress::Missing(_)
    ));
    assert_eq!(
        target
            .store
            .acquire(tip, &BTreeSet::new(), target.limits.facts)
            .unwrap(),
        deadline
    );
}

#[test]
fn mined_work_and_signed_wallet_statements_cannot_be_relabelled_for_another_pool() {
    let (mut node, wallet, payments, _dir) = setup();
    let (tip, template) = share(&mut node, &wallet, &payments);
    let bytes = node.store.read(Key::new(Kind::Core, tip)).unwrap();
    let view = node.view().unwrap();
    let work = Work::verify(&bytes, &view).unwrap();
    let auth = wallet.authorize(tip).unwrap();
    work.authorize(&auth, view.profile.pool_id).unwrap();
    let mut other_profile = node.profile.clone();
    other_profile.pool_id[0] ^= 1;
    let other = View::pin(&node.chain, other_profile, view.now).unwrap();
    assert!(Work::verify(&bytes, &other).is_err());
    assert!(work.authorize(&auth, other.profile.pool_id).is_err());
    let mut core: Core = decode(&bytes).unwrap();
    let mut job: Job = decode(&core.job.0).unwrap();
    job.pool_id = other.profile.pool_id;
    core.job = Blob(encode(&job).unwrap());
    assert!(matches!(
        Work::verify(&encode(&core).unwrap(), &other),
        Err(Error::Invalid("job offset binding"))
    ));

    let mut registration: Registration = decode(
        &node
            .store
            .read(Key::new(Kind::Registration, template.job.registration))
            .unwrap(),
    )
    .unwrap();
    let public = crate::native::public_key(wallet.account()).unwrap();
    assert!(crate::native::signature_valid(
        registration.signature,
        id(&registration.fields).unwrap(),
        &public
    ));
    let inventory: Inventory = decode(
        &node
            .store
            .read(Key::new(Kind::Inventory, registration.fields.inventory))
            .unwrap(),
    )
    .unwrap();
    registration.fields.pool_id = other.profile.pool_id;
    assert!(!crate::native::signature_valid(
        registration.signature,
        id(&registration.fields).unwrap(),
        &public
    ));
    let coupon: Coupon = decode(
        &node
            .store
            .read(Key::new(Kind::Coupon, inventory.rows.0[0].coupon_ids.0[0]))
            .unwrap(),
    )
    .unwrap();
    let kernel = crate::native::reward(&coupon).unwrap().1;
    let public = kernel
        .excess
        .to_pubkey(&grin_util::static_secp_instance().lock())
        .unwrap();
    let mut fields = CouponAuth {
        pool_id: view.profile.pool_id,
        owner: wallet.account(),
        reward_id: id(&coupon.reward).unwrap(),
    };
    assert!(crate::native::signature_valid(
        coupon.owner_auth,
        id(&fields).unwrap(),
        &public
    ));
    fields.pool_id = other.profile.pool_id;
    assert!(!crate::native::signature_valid(
        coupon.owner_auth,
        id(&fields).unwrap(),
        &public
    ));
}
