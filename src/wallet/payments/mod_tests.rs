use super::{check_payment_weight, MAX_NATIVE_FEE};
use grin_core::{
    core::{FeeFields, Transaction},
    global,
};

#[test]
fn payment_weight_counts_receiver_at_native_boundary() {
    for chain in [
        global::ChainTypes::AutomatedTesting,
        global::ChainTypes::Mainnet,
    ] {
        global::set_local_chain_type(chain);
        let limit = global::max_block_weight() / 2;
        let base = Transaction::weight_by_iok(0, 2, 1);
        let input_weight = Transaction::weight_by_iok(1, 2, 1) - base;
        let inputs = (limit - base) / input_weight;
        assert!(Transaction::weight_by_iok(inputs, 2, 1) <= limit);
        assert!(check_payment_weight(inputs, 1).is_ok());
        assert!(Transaction::weight_by_iok(inputs + 1, 2, 1) > limit);
        assert!(check_payment_weight(inputs + 1, 1).is_err());
        assert!(check_payment_weight(u64::MAX, 1).is_err());
        assert!(check_payment_weight(1, u64::MAX).is_err());
    }
}

#[test]
fn reserved_fee_headroom_matches_native_fee_limit() {
    assert_eq!(
        FeeFields::new(0, MAX_NATIVE_FEE).unwrap().fee(),
        MAX_NATIVE_FEE
    );
    assert!(FeeFields::new(0, MAX_NATIVE_FEE + 1).is_err());
}

use grin_core::core::{Transaction as NativeTransaction, TxKernel};
use grin_util::secp::pedersen::{Commitment, RangeProof};
use grin_wallet_libwallet::{Error, NodeClient, NodeVersionInfo};
use std::collections::HashMap;
#[derive(Clone)]
pub(crate) struct Offline;
impl NodeClient for Offline {
    fn node_url(&self) -> &str {
        "offline"
    }
    fn set_node_url(&mut self, _: &str) {}
    fn node_api_secret(&self) -> Option<String> {
        None
    }
    fn set_node_api_secret(&mut self, _: Option<String>) {}
    fn post_tx(&self, _: &NativeTransaction, _: bool) -> Result<(), Error> {
        unreachable!()
    }
    fn get_version_info(&mut self) -> Option<NodeVersionInfo> {
        None
    }
    fn get_chain_tip(&self) -> Result<(u64, String), Error> {
        unreachable!()
    }
    fn get_kernel(
        &mut self,
        _: &Commitment,
        _: Option<u64>,
        _: Option<u64>,
    ) -> Result<Option<(TxKernel, u64, u64)>, Error> {
        unreachable!()
    }
    fn get_outputs_from_node(
        &self,
        _: Vec<Commitment>,
    ) -> Result<HashMap<Commitment, (String, u64, u64)>, Error> {
        unreachable!()
    }
    fn get_outputs_by_pmmr_index(
        &self,
        _: u64,
        _: Option<u64>,
        _: u64,
    ) -> Result<(u64, u64, Vec<(Commitment, RangeProof, bool, u64, u64)>), Error> {
        unreachable!()
    }
    fn height_range_to_pmmr_indices(&self, _: u64, _: Option<u64>) -> Result<(u64, u64), Error> {
        unreachable!()
    }
}

#[test]
fn rewards_and_grin_change_share_the_persisted_default_allocator() {
    global::set_local_chain_type(global::ChainTypes::AutomatedTesting);
    let dir = tempfile::tempdir().unwrap();
    let payments = super::Payments::create(dir.path(), Offline).unwrap();
    let signer = payments.signer().unwrap();
    let first = payments.reward(&signer, 123).unwrap();
    let change = {
        let mut lock = payments.wallet.lock();
        lock.lc_provider()
            .unwrap()
            .wallet_inst()
            .unwrap()
            .next_child(payments.mask.as_ref())
            .unwrap()
    };
    let second = payments.reward(&signer, 123).unwrap();
    assert_ne!(first.path, change);
    assert_ne!(change, second.path);
    assert_ne!(first.output.commitment(), second.output.commitment());
    assert_eq!(
        first.path.parent_path(),
        grin_wallet_libwallet::WalletBackend::<Offline, grin_keychain::ExtKeychain>::default_path()
    );
    drop(payments);
    let payments = super::Payments::open(dir.path(), Offline).unwrap();
    let next = payments.reward(&signer, 123).unwrap();
    assert_ne!(next.path, first.path);
    assert_ne!(next.path, change);
    assert_ne!(next.path, second.path);
    let mut lock = payments.wallet.lock();
    let backend = lock.lc_provider().unwrap().wallet_inst().unwrap();
    let parent = backend.parent_key_id();
    let mut batch = backend.batch(payments.mask.as_ref()).unwrap();
    batch.save_child_index(&parent, u32::MAX).unwrap();
    batch.commit().unwrap();
    drop(lock);
    assert!(payments.reward(&signer, 123).is_err());
    assert!(payments.offer(1_000_000_000).is_err());
}

#[test]
fn withdrawal_recovers_signing_and_unconfirmed_broadcasts_without_unlocking_inputs() {
    use grin_keychain::ExtKeychain;
    use grin_wallet_impls::{
        test_framework::{self, LocalWalletClient, WalletProxy},
        DefaultLCProvider,
    };
    use std::sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    };
    global::set_local_chain_type(global::ChainTypes::AutomatedTesting);
    global::set_local_accept_fee_base(1);
    let dir = tempfile::tempdir().unwrap();
    let mut proxy: WalletProxy<
        'static,
        DefaultLCProvider<LocalWalletClient, ExtKeychain>,
        LocalWalletClient,
        ExtKeychain,
    > = WalletProxy::new(dir.path().to_str().unwrap());
    let sender_dir = dir.path().join("sender");
    let receiver_dir = dir.path().join("receiver");
    std::fs::create_dir_all(&sender_dir).unwrap();
    std::fs::create_dir_all(&receiver_dir).unwrap();
    let (relay, incoming) = std::sync::mpsc::channel();
    let sender_client = LocalWalletClient::new("sender", relay);
    let receiver_client = LocalWalletClient::new("receiver", proxy.tx.clone());
    let sender = super::Payments::create(&sender_dir, sender_client.clone()).unwrap();
    let receiver = super::Payments::create(&receiver_dir, receiver_client.clone()).unwrap();
    let signer = sender.signer().unwrap();
    proxy.add_wallet(
        "sender",
        sender_client.get_send_instance(),
        sender.wallet.clone(),
        sender.mask.clone(),
    );
    proxy.add_wallet(
        "receiver",
        receiver_client.get_send_instance(),
        receiver.wallet.clone(),
        receiver.mask.clone(),
    );
    let chain = proxy.chain.clone();
    let running = proxy.running.clone();
    struct ProxyGuard(
        Arc<AtomicBool>,
        Option<std::thread::JoinHandle<()>>,
        Option<std::thread::JoinHandle<()>>,
    );
    impl Drop for ProxyGuard {
        fn drop(&mut self) {
            self.0.store(false, Ordering::Relaxed);
            self.1.take().unwrap().join().unwrap();
            self.2.take().unwrap().join().unwrap();
        }
    }
    let backend = proxy.tx.clone();
    let sender_response = sender_client.get_send_instance();
    let thread = std::thread::spawn(move || proxy.run().unwrap());
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    while !running.load(Ordering::Relaxed) {
        assert!(std::time::Instant::now() < deadline);
        std::thread::yield_now();
    }
    let posts = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let observed = posts.clone();
    let alive = running.clone();
    let broadcast = Arc::new(AtomicBool::new(false));
    let relay_broadcast = broadcast.clone();
    let forwarding = std::thread::spawn(move || {
        while alive.load(Ordering::Relaxed) {
            match incoming.recv_timeout(std::time::Duration::from_millis(10)) {
                Ok(mut message)
                    if message.method == "post_tx" && !relay_broadcast.load(Ordering::Acquire) =>
                {
                    observed.fetch_add(1, Ordering::SeqCst);
                    message.dest = std::mem::replace(&mut message.sender_id, "node".into());
                    message.body.clear();
                    sender_response.send(message).unwrap();
                }
                Ok(message) => backend.send(message).unwrap(),
                Err(_) => {}
            }
        }
    });
    let _proxy = ProxyGuard(running, Some(thread), Some(forwarding));
    let reward = sender
        .reward(&signer, grin_core::consensus::REWARD)
        .unwrap();
    test_framework::add_block_with_reward(&chain, &[], reward.output, reward.kernel);
    test_framework::award_blocks_to_wallet(
        &chain,
        sender.wallet.clone(),
        sender.mask.as_ref(),
        5,
        false,
    )
    .unwrap();
    sender.refresh().unwrap();
    let before = sender.status_snapshot([7; 32], "ready", None).unwrap();
    assert!(before.balance.unwrap().amount_currently_spendable > 0);
    let offer = sender.offer(1_000_000_000).unwrap();
    let offer_slate = sender
        .owner
        .slate_from_slatepack_message(sender.mask.as_ref(), offer, vec![])
        .unwrap();
    let intent_before = std::fs::read(sender.directory.join("intent.json")).unwrap();
    let wallet_before = sender
        .owner
        .retrieve_summary_info(sender.mask.as_ref(), false, 1)
        .unwrap()
        .1;
    for _ in 0..2 {
        let status = sender.status_snapshot([7; 32], "refreshing", None).unwrap();
        let pending = status.withdrawal.unwrap();
        assert_eq!(pending.amount, 1_000_000_000);
        assert_eq!(pending.fee, offer_slate.fee_fields.fee());
        assert_eq!(pending.stage, super::Phase::AwaitingResponse);
        assert_eq!(pending.id, offer_slate.id.to_string());
        assert_eq!(
            std::fs::read(sender.directory.join("intent.json")).unwrap(),
            intent_before
        );
    }
    assert_eq!(
        sender
            .owner
            .retrieve_summary_info(sender.mask.as_ref(), false, 1)
            .unwrap()
            .1,
        wallet_before
    );
    assert_eq!(posts.load(Ordering::SeqCst), 0);
    let mut intent = sender.load().unwrap().0;
    intent.response = Some("\0".repeat(super::MAX_SLATEPACK_BYTES));
    sender.save(&intent).unwrap();
    assert_eq!(
        sender.load().unwrap().0.response.unwrap().len(),
        super::MAX_SLATEPACK_BYTES
    );
    intent.response.as_mut().unwrap().push('\0');
    assert!(sender.save(&intent).is_err());
    intent.response = None;
    sender.save(&intent).unwrap();
    let partial_path = sender
        .directory
        .join("wallet_data/saved_txs")
        .join(format!("{}.grintx", offer_slate.id));
    let partial = std::fs::read(&partial_path).unwrap();
    std::fs::remove_file(&partial_path).unwrap();
    sender.refresh().unwrap();
    sender.rebroadcast().unwrap();
    std::fs::write(&partial_path, b"invalid encoding").unwrap();
    sender.refresh().unwrap();
    assert!(sender.rebroadcast().is_err());
    std::fs::write(&partial_path, partial).unwrap();
    std::fs::remove_file(&partial_path).unwrap();
    assert!(sender.cancel().unwrap());
    assert!(!sender.directory.join("intent.json").exists());
    assert!(sender
        .status_snapshot([7; 32], "refreshing", None)
        .unwrap()
        .withdrawal
        .is_none());
    sender.refresh().unwrap();
    let offer = sender.offer(1_000_000_000).unwrap();
    let offer_slate = sender
        .owner
        .slate_from_slatepack_message(sender.mask.as_ref(), offer, vec![])
        .unwrap();
    let foreign = grin_wallet_api::Foreign::new(
        receiver.wallet.clone(),
        receiver.directory.join("grin-wallet.toml"),
        receiver.mask.clone(),
        None,
        false,
    );
    let reply = foreign.receive_tx(&offer_slate, None, None).unwrap();
    let mut invalid = reply.clone();
    invalid.participant_data[0].public_blind_excess =
        offer_slate.participant_data[0].public_blind_excess;
    let invalid = receiver
        .owner
        .create_slatepack_message(receiver.mask.as_ref(), &invalid, None, vec![])
        .unwrap();
    assert!(sender.finalize(&invalid, &AtomicBool::new(false)).is_err());
    assert!(!sender.directory.join("intent.json").exists());
    sender.refresh().unwrap();
    let offer = sender.offer(1_000_000_000).unwrap();
    let offer_slate = sender
        .owner
        .slate_from_slatepack_message(sender.mask.as_ref(), offer, vec![])
        .unwrap();
    let reply = foreign.receive_tx(&offer_slate, None, None).unwrap();
    let packed = receiver
        .owner
        .create_slatepack_message(receiver.mask.as_ref(), &reply, None, vec![])
        .unwrap();
    let mut intent = sender.load().unwrap().0;
    intent.response = Some(packed.clone());
    intent.stage = super::Phase::Finalizing;
    sender.save(&intent).unwrap();
    let stored_path = sender
        .directory
        .join("wallet_data/saved_txs")
        .join(format!("{}.grintx", reply.id));
    std::fs::write(&stored_path, b"interrupted native transaction write").unwrap();
    let (disconnected, receive) = std::sync::mpsc::channel();
    drop(receive);
    let relay = std::mem::replace(&mut *sender_client.proxy_tx.lock(), disconnected);
    assert!(sender.finalize(&packed, &AtomicBool::new(false)).is_err());
    assert!(sender.load().unwrap().0.stage == super::Phase::ReadyToPost);
    assert!(super::is_signed(
        &sender
            .stored_transaction(&reply.id.to_string())
            .unwrap()
            .unwrap()
    ));
    assert!(sender.cancel().is_err());
    let error = sender.rebroadcast().unwrap_err().to_string();
    *sender_client.proxy_tx.lock() = relay;
    assert!(error.starts_with("withdrawal "));
    assert!(error.contains("post_tx send"));
    sender.refresh().unwrap();
    sender.rebroadcast().unwrap();
    sender.finalize(&packed, &AtomicBool::new(false)).unwrap();
    let first_signed_path = stored_path;
    let (_, entries) = sender
        .owner
        .retrieve_txs(sender.mask.as_ref(), false, None, Some(reply.id), None)
        .unwrap();
    assert_eq!(
        entries[0].tx_slate_state,
        Some(grin_wallet_libwallet::SlateState::Standard3)
    );
    posts.store(0, Ordering::SeqCst);
    let offer = sender.offer(1_000_000_000).unwrap();
    let offer_slate = sender
        .owner
        .slate_from_slatepack_message(sender.mask.as_ref(), offer, vec![])
        .unwrap();
    let reply = foreign.receive_tx(&offer_slate, None, None).unwrap();
    let packed = receiver
        .owner
        .create_slatepack_message(receiver.mask.as_ref(), &reply, None, vec![])
        .unwrap();
    let mut intent = sender.load().unwrap().0;
    intent.response = Some(packed.clone());
    intent.stage = super::Phase::Finalizing;
    sender.save(&intent).unwrap();
    let (_, mut txs) = sender
        .owner
        .retrieve_txs(sender.mask.as_ref(), false, None, Some(reply.id), None)
        .unwrap();
    let unfinished = txs.remove(0);
    let context = {
        let mut wallet = sender.wallet.lock();
        wallet
            .lc_provider()
            .unwrap()
            .wallet_inst()
            .unwrap()
            .get_private_context(sender.mask.as_ref(), reply.id.as_bytes())
            .unwrap()
    };
    sender
        .owner
        .finalize_tx(sender.mask.as_ref(), &reply)
        .unwrap();
    {
        let parent = unfinished.parent_key_id.clone();
        let mut wallet = sender.wallet.lock();
        let backend = wallet.lc_provider().unwrap().wallet_inst().unwrap();
        let mut batch = backend.batch(sender.mask.as_ref()).unwrap();
        batch.save_tx_log_entry(unfinished, &parent).unwrap();
        batch
            .save_private_context(reply.id.as_bytes(), &context)
            .unwrap();
        batch.commit().unwrap();
    }
    assert!(sender.cancel().is_err());
    drop(sender);
    let sender = super::Payments::open(&sender_dir, sender_client.clone()).unwrap();
    assert_eq!(
        sender.resume(&AtomicBool::new(false)).unwrap()["state"],
        "completed"
    );
    assert!(!sender.directory.join("intent.json").exists());
    assert_eq!(posts.load(Ordering::SeqCst), 1);
    sender.finalize(&packed, &AtomicBool::new(false)).unwrap();
    assert_eq!(posts.load(Ordering::SeqCst), 1);
    assert!(std::fs::read_dir(&sender.directory).unwrap().all(|entry| {
        let name = entry.unwrap().file_name();
        let name = name.to_string_lossy();
        !name.ends_with(".posted.json") && !name.ends_with(".cancelled.json")
    }));
    sender.refresh().unwrap();
    let signed_bytes = std::fs::read(&first_signed_path).unwrap();
    std::fs::write(&first_signed_path, b"invalid encoding").unwrap();
    let before_posts = posts.load(Ordering::SeqCst);
    let error = sender.rebroadcast().unwrap_err().to_string();
    assert!(posts.load(Ordering::SeqCst) > before_posts);
    let ready = sender
        .status_snapshot([7; 32], "ready", Some(error.clone()))
        .unwrap();
    assert_eq!(ready.state, "ready");
    assert!(ready.balance.is_some());
    assert_eq!(ready.error, Some(error));
    std::fs::write(&first_signed_path, signed_bytes).unwrap();

    let (disconnected, receive) = std::sync::mpsc::channel();
    drop(receive);
    let relay = std::mem::replace(&mut *sender_client.proxy_tx.lock(), disconnected);
    let error = sender.rebroadcast().unwrap_err();
    *sender_client.proxy_tx.lock() = relay;
    sender.refresh().unwrap();
    let ready = sender
        .status_snapshot([7; 32], "ready", Some(error.to_string()))
        .unwrap();
    assert!(ready.balance.is_some());
    assert!(sender
        .stored_transaction(&reply.id.to_string())
        .unwrap()
        .is_some());
    sender.rebroadcast().unwrap();
    {
        let (_, txs) = sender
            .owner
            .retrieve_txs(sender.mask.as_ref(), false, None, Some(reply.id), None)
            .unwrap();
        assert_eq!(
            txs[0].tx_slate_state,
            Some(grin_wallet_libwallet::SlateState::Standard3)
        );
        let mut wallet = sender.wallet.lock();
        assert!(wallet
            .lc_provider()
            .unwrap()
            .wallet_inst()
            .unwrap()
            .get_private_context(sender.mask.as_ref(), reply.id.as_bytes())
            .is_err());
    }
    let (_, info) = sender
        .owner
        .retrieve_summary_info(sender.mask.as_ref(), false, 1)
        .unwrap();
    assert!(info.amount_locked > 0);
    assert!(!sender.cancel().unwrap());
    drop(sender);
    let sender = super::Payments::open(&sender_dir, sender_client).unwrap();
    assert_eq!(
        sender.resume(&AtomicBool::new(false)).unwrap()["state"],
        "idle"
    );
    sender.refresh().unwrap();
    sender.rebroadcast().unwrap();
    assert!(posts.load(Ordering::SeqCst) >= 3);
    broadcast.store(true, Ordering::Release);
    sender.refresh().unwrap();
    sender.rebroadcast().unwrap();
    assert!(!sender.directory.join("intent.json").exists());
    assert_eq!(
        sender.resume(&AtomicBool::new(false)).unwrap()["state"],
        "idle"
    );
    receiver.refresh().unwrap();
    let received = receiver.status_snapshot([7; 32], "ready", None).unwrap();
    assert_eq!(received.balance.unwrap().total, 2_000_000_000);
    sender.refresh().unwrap();
    let checkpoint = sender.status_snapshot([7; 32], "ready", None).unwrap();
    test_framework::award_block_to_wallet(
        &chain,
        &[],
        receiver.wallet.clone(),
        receiver.mask.as_ref(),
    )
    .unwrap();
    let unchanged = sender.status_snapshot([7; 32], "ready", None).unwrap();
    assert_eq!(unchanged.native_height, checkpoint.native_height);
    assert!(unchanged.native_height < chain.head().unwrap().height);
    {
        let mut lock = sender.wallet.lock();
        let backend = lock.lc_provider().unwrap().wallet_inst().unwrap();
        let parent = backend.parent_key_id();
        let mut batch = backend.batch(sender.mask.as_ref()).unwrap();
        batch.save_child_index(&parent, u32::MAX - 1).unwrap();
        batch.commit().unwrap();
    }
    let start = std::sync::Barrier::new(2);
    let (reward, offer) = std::thread::scope(|scope| {
        let reward = scope.spawn(|| {
            global::set_local_chain_type(global::ChainTypes::AutomatedTesting);
            start.wait();
            sender.reward(&signer, grin_core::consensus::REWARD)
        });
        let offer = scope.spawn(|| {
            global::set_local_chain_type(global::ChainTypes::AutomatedTesting);
            global::set_local_accept_fee_base(1);
            start.wait();
            sender.offer(1_000_000_000)
        });
        (reward.join().unwrap(), offer.join().unwrap())
    });
    assert_ne!(reward.is_ok(), offer.is_ok());
}

#[test]
fn rebuilding_grin_wallet_database_blocks_allocation_until_the_scan_finishes() {
    use grin_keychain::ExtKeychain;
    use grin_wallet_impls::{
        test_framework::{self, LocalWalletClient, WalletProxy},
        DefaultLCProvider,
    };
    use grin_wallet_libwallet::{ScannedBlockInfo, WalletInitStatus};
    use std::sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    };
    global::set_local_chain_type(global::ChainTypes::AutomatedTesting);
    let dir = tempfile::tempdir().unwrap();
    let mut proxy: WalletProxy<
        'static,
        DefaultLCProvider<LocalWalletClient, ExtKeychain>,
        LocalWalletClient,
        ExtKeychain,
    > = WalletProxy::new(dir.path().to_str().unwrap());
    let original_dir = dir.path().join("original");
    let restored_dir = dir.path().join("rebuilt");
    let original_client = LocalWalletClient::new("original", proxy.tx.clone());
    let original = super::Payments::create(&original_dir, original_client).unwrap();
    let signer = original.signer().unwrap();
    let existing = original
        .reward(&signer, grin_core::consensus::REWARD)
        .unwrap();
    test_framework::add_block_with_reward(&proxy.chain, &[], existing.output, existing.kernel);
    std::fs::create_dir_all(restored_dir.join("payments/wallet_data")).unwrap();
    std::fs::copy(
        crate::wallet::seed_path(&original_dir),
        crate::wallet::seed_path(&restored_dir),
    )
    .unwrap();
    drop(original);
    let restored_client = LocalWalletClient::new("rebuilt", proxy.tx.clone());
    let restored = super::Payments::open(&restored_dir, restored_client.clone()).unwrap();
    proxy.add_wallet(
        "rebuilt",
        restored_client.get_send_instance(),
        restored.wallet.clone(),
        restored.mask.clone(),
    );
    let running = proxy.running.clone();
    struct ProxyGuard(Arc<AtomicBool>, Option<std::thread::JoinHandle<()>>);
    impl Drop for ProxyGuard {
        fn drop(&mut self) {
            self.0.store(false, Ordering::Relaxed);
            self.1.take().unwrap().join().unwrap();
        }
    }
    let chain = proxy.chain.clone();
    let thread = std::thread::spawn(move || proxy.run().unwrap());
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    while !running.load(Ordering::Relaxed) {
        assert!(std::time::Instant::now() < deadline);
        std::thread::yield_now();
    }
    let _proxy = ProxyGuard(running, Some(thread));
    assert_eq!(restored.signer().unwrap().account(), signer.account());
    assert!(restored
        .reward(&signer, grin_core::consensus::REWARD)
        .is_err());
    assert!(restored.offer(1_000_000_000).is_err());
    {
        let mut wallet = restored.wallet.lock();
        let backend = wallet.lc_provider().unwrap().wallet_inst().unwrap();
        assert!(matches!(
            backend.init_status().unwrap(),
            WalletInitStatus::InitComplete
        ));
        assert!(backend.last_scanned_block().unwrap().hash.is_empty());
        let mut batch = backend.batch(restored.mask.as_ref()).unwrap();
        batch
            .save_init_status(WalletInitStatus::InitComplete)
            .unwrap();
        batch
            .save_last_scanned_block(ScannedBlockInfo {
                height: 0,
                hash: String::new(),
                start_pmmr_index: 0,
                last_pmmr_index: 0,
            })
            .unwrap();
        batch.commit().unwrap();
    }
    assert!(restored
        .reward(&signer, grin_core::consensus::REWARD)
        .is_err());
    restored.refresh().unwrap();
    let recovered = restored.status_snapshot([7; 32], "ready", None).unwrap();
    assert_eq!(
        recovered.balance.unwrap().total,
        grin_core::consensus::REWARD
    );
    let next = restored
        .reward(&signer, grin_core::consensus::REWARD)
        .unwrap();
    assert_ne!(next.path, existing.path);
    assert_ne!(next.output.commitment(), existing.output.commitment());
    assert!(chain
        .get_unspent(next.output.commitment())
        .unwrap()
        .is_none());
}

#[test]
fn wallet_checkpoint_status_does_not_require_native_history() {
    global::set_local_chain_type(global::ChainTypes::AutomatedTesting);
    let dir = tempfile::tempdir().unwrap();
    let payments = super::Payments::create(dir.path(), Offline).unwrap();
    {
        let mut wallet = payments.wallet.lock();
        let backend = wallet.lc_provider().unwrap().wallet_inst().unwrap();
        let mut batch = backend.batch(payments.mask.as_ref()).unwrap();
        batch
            .save_last_scanned_block(grin_wallet_libwallet::ScannedBlockInfo {
                height: 100,
                hash: "11".repeat(32),
                start_pmmr_index: 1,
                last_pmmr_index: 200,
            })
            .unwrap();
        batch.commit().unwrap();
    }
    let snapshot = payments
        .status_snapshot([7; 32], "refreshing", None)
        .unwrap();
    assert_eq!(snapshot.native_height, 100);
    assert_eq!(snapshot.native_head, "11".repeat(32));
    assert_eq!(snapshot.scanned_pmmr_position, 200);
    assert!(snapshot.balance.is_none());
}
