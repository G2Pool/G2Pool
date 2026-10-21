use super::*;
#[test]
fn replies_partition_the_exact_request_including_negative_results() {
    let a = Key::new(Kind::Coupon, [1; 32]);
    let b = Key::new(Kind::Coupon, [2; 32]);
    let request = Message::Get(vec![a, b]);
    for reply in [
        Message::Reply {
            objects: vec![],
            missing: vec![],
            deferred: vec![],
        },
        Message::Reply {
            objects: vec![],
            missing: vec![a, a],
            deferred: vec![b],
        },
        Message::Reply {
            objects: vec![],
            missing: vec![a],
            deferred: vec![Key::new(Kind::Coupon, [3; 32])],
        },
    ] {
        assert!(reply.validate_reply(&request).is_err());
    }
    let reply = Message::Reply {
        objects: vec![],
        missing: vec![a],
        deferred: vec![b],
    };
    reply.validate_reply(&request).unwrap();
    Message::decode(&reply.encode().unwrap())
        .unwrap()
        .validate_reply(&request)
        .unwrap();
    assert!(Message::Ack.validate_reply(&request).is_err());
}
#[test]
fn maximum_object_batches_include_envelope_overhead() {
    let a = Key::new(Kind::Transaction, [1; 32]);
    let b = Key::new(Kind::Residual, [2; 32]);
    assert!(Message::Get(vec![a, b]).response_ceiling().is_err());
    let batches = super::super::batches(&[a, b]).unwrap();
    assert_eq!(batches, vec![vec![a], vec![b]]);
    for batch in batches {
        assert!(Message::Get(batch).response_ceiling().unwrap() <= FRAME);
    }
}
#[test]
fn deterministic_wire_fuzz_never_panics_or_accepts_trailing_bytes() {
    let mut state = 17u64;
    for length in 0..2048 {
        let bytes: Vec<_> = (0..length)
            .map(|_| {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                state as u8
            })
            .collect();
        let _ = Message::decode(&bytes);
    }
    for message in [
        Message::Heads,
        Message::Announce(3417),
        Message::Peers,
        Message::Ack,
        Message::Busy,
        Message::Notice(vec![[1; 32]]),
    ] {
        let mut wire = message.encode().unwrap();
        Message::decode(&wire).unwrap();
        wire.push(0);
        assert!(Message::decode(&wire).is_err());
    }
}
