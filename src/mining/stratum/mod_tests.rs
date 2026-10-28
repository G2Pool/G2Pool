use super::*;
use std::thread;
#[test]
fn unscaled_miner_target_cannot_hide_a_scaled_eligible_proof() {
    let base = 1u128 << 64;
    for weight in [20u64, 16384] {
        for assigned in [
            weight,
            weight + 1,
            2 * weight - 1,
            2 * weight,
            10 * weight + 7,
            u64::MAX,
        ] {
            let target = miner_difficulty(assigned, weight).unwrap();
            let boundary = ((weight as u128) * base) / (assigned as u128);
            for hash in [
                1u128,
                boundary.saturating_sub(1),
                boundary,
                boundary + 1,
                base - 1,
            ] {
                if hash == 0 || hash >= base {
                    continue;
                }
                let scaled = ((weight as u128) * base / hash).min(u64::MAX as u128) as u64;
                let raw = (base / hash).min(u64::MAX as u128) as u64;
                if scaled >= assigned {
                    assert!(raw >= target);
                }
            }
        }
    }
    assert_eq!(miner_difficulty(16385, 16384).unwrap(), 1);
    let hash = base * 3 / 4;
    assert!(16384 * base / hash >= 16385);
    assert_eq!(base / hash, 1);
}
#[tokio::test]
async fn slow_partial_request_cannot_extend_its_deadline() {
    let (reader, mut writer) = tokio::io::duplex(LINE);
    let sender = tokio::spawn(async move {
        for byte in b"{}\n" {
            if writer.write_all(&[*byte]).await.is_err() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(70)).await;
        }
    });
    let mut reader = BufReader::new(reader);
    assert!(matches!(
        read_line(
            &mut reader,
            tokio::time::Instant::now() + Duration::from_millis(100)
        )
        .await,
        Err(Error::Capacity("partial Stratum line"))
    ));
    drop(reader);
    sender.await.unwrap();
}

#[tokio::test]
async fn publications_reach_miners_during_idle_reads_partial_requests_and_submissions() {
    use grin_core::core::Transaction;
    let (mut node, signer, payments, _dir) = crate::node::tests::setup();
    let first = crate::mining::submission::tests::winning_template(&mut node, &signer, &payments);
    let submission = json!({"id":2,"method":"submit","params":{
        "height":first.block.header.height,"job_id":0,"nonce":first.block.header.pow.nonce,
        "edge_bits":first.block.header.pow.proof.edge_bits,"pow":first.block.header.pow.proof.nonces
    }});
    let second =
        crate::node::tests::prepare_template(&mut node, &signer, &payments, Transaction::empty())
            .unwrap();
    let third =
        crate::node::tests::prepare_template(&mut node, &signer, &payments, Transaction::empty())
            .unwrap();
    let mining = Arc::new(
        Mining::bounded(
            Arc::new(Mutex::new(node)),
            Arc::new(signer),
            Retention::default(),
        )
        .unwrap(),
    );
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let service = mining.clone();
    let stop = Arc::new(AtomicBool::new(false));
    let signal = stop.clone();
    struct Server(Arc<AtomicBool>, Option<thread::JoinHandle<Result<()>>>);
    impl Drop for Server {
        fn drop(&mut self) {
            self.0.store(true, Ordering::Release);
            self.1.take().unwrap().join().unwrap().unwrap();
        }
    }
    let server = Server(
        stop,
        Some(thread::spawn(move || serve(listener, service, signal, 2))),
    );
    let client = TcpStream::connect(address).await.unwrap();
    let (reader, mut writer) = client.into_split();
    let mut reader = BufReader::new(reader);
    let request = b"{\"id\":1,\"method\":\"keepalive\"}\n";
    writer.write_all(request).await.unwrap();
    async fn receive(reader: &mut (impl AsyncBufRead + Unpin)) -> Value {
        let line = read_line(reader, tokio::time::Instant::now() + Duration::from_secs(1))
            .await
            .unwrap()
            .unwrap();
        serde_json::from_slice::<Value>(&line).unwrap()
    }
    assert_eq!(receive(&mut reader).await["result"], "ok");
    let first = mining.publish(first).unwrap();
    let job = receive(&mut reader).await;
    assert_eq!(job["method"], "job");
    assert_eq!(job["params"]["job_id"], first);

    writer.write_all(&request[..12]).await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;
    let second = mining.publish(second).unwrap();
    let job = receive(&mut reader).await;
    assert_eq!(job["method"], "job");
    assert_eq!(job["params"]["job_id"], second);
    writer.write_all(&request[12..]).await.unwrap();
    let response = receive(&mut reader).await;
    assert_eq!(response["id"], 1);
    assert_eq!(response["result"], "ok");

    let (release, wait) = std::sync::mpsc::channel::<()>();
    let (locked, ready) = std::sync::mpsc::sync_channel(1);
    let node = mining.node.clone();
    struct BlockedNode(
        Option<std::sync::mpsc::Sender<()>>,
        Option<thread::JoinHandle<()>>,
    );
    impl Drop for BlockedNode {
        fn drop(&mut self) {
            self.0.take();
            self.1.take().unwrap().join().unwrap();
        }
    }
    let blocked = BlockedNode(
        Some(release),
        Some(thread::spawn(move || {
            let _guard = node.lock().unwrap();
            locked.send(()).unwrap();
            let _ = wait.recv();
        })),
    );
    ready.recv_timeout(Duration::from_secs(1)).unwrap();
    write_message(&mut writer, &submission).await.unwrap();
    tokio::time::timeout(Duration::from_secs(1), async {
        while mining.jobs.lock().unwrap().active.is_empty() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    let third = mining.publish(third).unwrap();
    let job = receive(&mut reader).await;
    assert_eq!(job["method"], "job");
    assert_eq!(job["params"]["job_id"], third);
    drop(blocked);
    let line = read_line(
        &mut reader,
        tokio::time::Instant::now() + Duration::from_secs(30),
    )
    .await
    .unwrap()
    .unwrap();
    let response: Value = serde_json::from_slice(&line).unwrap();
    assert_eq!(response["id"], 2);
    assert!(response["error"].is_null());
    assert_eq!(response["g2pool"]["block"], true);
    writer.write_all(b"{").await.unwrap();
    let stopped = Instant::now();
    drop(server);
    assert!(stopped.elapsed() < Duration::from_secs(1));
    let mut line = String::new();
    assert_eq!(reader.read_line(&mut line).await.unwrap(), 0);
}

#[test]
fn submitted_job_keeps_dependencies_after_its_advertised_lease_expires() {
    use crate::storage::sharechain::Error as StorageError;
    let (mut node, signer, payments, _dir) = crate::node::tests::setup();
    let template =
        crate::mining::submission::tests::winning_template(&mut node, &signer, &payments);
    let registration = crate::objects::Key::new(
        crate::objects::Kind::Registration,
        template.job.registration,
    );
    let proof = template.block.header.pow.proof.clone();
    let height = template.block.header.height;
    let nonce = template.block.header.pow.nonce;
    let db = node.store.database();
    let node = Arc::new(Mutex::new(node));
    let mining = Mining::bounded(node, Arc::new(signer), Retention::default()).unwrap();
    let job = mining.publish(template).unwrap();
    let request = json!({"method": "submit", "params": {"height": height, "job_id": job,
        "nonce": nonce, "edge_bits": proof.edge_bits, "pow": proof.nonces}});
    thread::scope(|scope| {
        let submitter = db
            .solution_transaction::<_, StorageError>(|_, _| {
                let submitter = scope.spawn(|| {
                    grin_core::global::set_local_chain_type(
                        grin_core::global::ChainTypes::AutomatedTesting,
                    );
                    mining.handle(&request)
                });
                let deadline = Instant::now() + Duration::from_secs(5);
                loop {
                    let mut jobs = mining.jobs.lock().unwrap();
                    if !jobs.active.is_empty() {
                        jobs.templates
                            .expire(Instant::now() + Duration::from_secs(3600));
                        break;
                    }
                    drop(jobs);
                    assert!(Instant::now() < deadline);
                    thread::yield_now();
                }
                assert!(mining.pins().unwrap().contains(&registration));
                Ok(submitter)
            })
            .unwrap();
        assert_eq!(submitter.join().unwrap().unwrap()["block"], true);
    });
}
