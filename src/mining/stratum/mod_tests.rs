use super::*;
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
#[test]
fn slow_partial_request_cannot_extend_its_deadline() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let sender = thread::spawn(move || {
        let mut stream = TcpStream::connect(address).unwrap();
        for byte in b"{}\n" {
            if stream.write_all(&[*byte]).is_err() {
                break;
            }
            thread::sleep(Duration::from_millis(70));
        }
    });
    let (stream, _) = listener.accept().unwrap();
    let mut reader = BufReader::new(stream);
    assert!(matches!(
        read_line(
            &mut reader,
            &AtomicBool::new(false),
            Instant::now() + Duration::from_millis(100)
        ),
        Err(Error::Capacity("partial Stratum line"))
    ));
    drop(reader);
    sender.join().unwrap();
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
