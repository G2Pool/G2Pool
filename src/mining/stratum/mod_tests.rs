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
