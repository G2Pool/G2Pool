use super::*;
use crate::{mining::Template, storage::StorageLimits, wallet::Signer};
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
    let profile = Profile::new(crate::native::manifest(
        crate::native::hash_bytes(chain.genesis().hash()),
        ChainTypes::AutomatedTesting,
    ))
    .unwrap();
    let payments = crate::wallet::payments::Payments::create(
        dir.path(),
        crate::wallet::payments::tests::Offline,
    )
    .unwrap();
    let wallet = payments.signer().unwrap();
    let mut store = Store::open(
        &dir.path().join("pool"),
        profile.protocol,
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
    let mut template = prepare_template(node, wallet, payments, Transaction::empty()).unwrap();
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
            encode(&wallet.authorize(node.profile.protocol, id).unwrap()).unwrap(),
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
    let store = Store::open(&path, profile.protocol, StorageLimits::default()).unwrap();
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
    assert!(node.store.wallet_registrations().unwrap().is_empty());
    let head = node.chain.head_header().unwrap();
    let block = native_block(&node, &head, 200);
    node.chain
        .process_block(block, grin_chain::Options::MINE)
        .unwrap();
    assert!(matches!(prepared.finish(&mut node), Err(Error::View)));
    assert!(node.store.wallet_registrations().unwrap().is_empty());
    prepare_template(&mut node, &wallet, &payments, Transaction::empty()).unwrap();
    assert_eq!(node.store.wallet_registrations().unwrap().len(), 1);
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
