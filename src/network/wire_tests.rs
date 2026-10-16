use super::*;
fn header(id: u8, parent: u8, height: u64) -> Header {
    Header {
        id: [id; 32],
        parent: Some([parent; 32]),
        height,
        credited_work: 1,
        cumulative_work: u128::from(height) + 1,
        parent_state_root: [parent; 32],
        state_root: [id; 32],
    }
}

#[test]
fn compact_reply_requires_exact_continuation_and_forward_progress() {
    let request = Message::Locator {
        target: [9; 32],
        start: [1; 32],
        locator: vec![[1; 32], [2; 32]],
    };
    let reply = |headers| Message::Headers {
        target: [9; 32],
        start: [1; 32],
        headers,
    };
    reply(vec![header(3, 1, 1)])
        .validate_reply(&request)
        .unwrap();
    for headers in [
        vec![],
        vec![header(3, 2, 1)],
        vec![header(1, 2, 1)],
        vec![header(9, 1, 1), header(4, 9, 2)],
        vec![header(3, 1, 1), header(1, 3, 2)],
        vec![header(3, 1, 1), header(4, 3, 3)],
        vec![header(3, 1, 1), header(4, 2, 2)],
    ] {
        assert!(reply(headers).validate_reply(&request).is_err());
    }
    reply(vec![header(9, 1, 1)])
        .validate_reply(&request)
        .unwrap();
    Message::Busy.validate_reply(&request).unwrap();
}

#[test]
fn compact_empty_reply_requires_a_completed_boundary() {
    for (start, locator, accepted) in [
        ([1; 32], vec![[1; 32]], false),
        ([9; 32], vec![[9; 32]], true),
        ([0; 32], vec![[1; 32]], false),
        ([0; 32], vec![[9; 32], [1; 32]], true),
    ] {
        let request = Message::Locator {
            target: [9; 32],
            start,
            locator,
        };
        let reply = Message::Headers {
            target: [9; 32],
            start,
            headers: vec![],
        };
        assert_eq!(reply.validate_reply(&request).is_ok(), accepted);
    }
    let request = Message::Locator {
        target: [9; 32],
        start: [9; 32],
        locator: vec![[9; 32]],
    };
    assert!(Message::Headers {
        target: [9; 32],
        start: [9; 32],
        headers: vec![header(10, 9, 1)],
    }
    .validate_reply(&request)
    .is_err());
}

#[test]
fn compact_initial_page_rejects_cycle_through_common_ancestor() {
    let request = Message::Locator {
        target: [9; 32],
        start: [0; 32],
        locator: vec![[1; 32]],
    };
    assert!(Message::Headers {
        target: [9; 32],
        start: [0; 32],
        headers: vec![header(2, 1, 1), header(1, 2, 2)],
    }
    .validate_reply(&request)
    .is_err());
}

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
        Message::Locator {
            target: [2; 32],
            start: [0; 32],
            locator: vec![[1; 32], [2; 32]],
        },
        Message::Headers {
            target: [2; 32],
            start: [0; 32],
            headers: vec![Header {
                id: [2; 32],
                parent: Some([1; 32]),
                height: 1,
                credited_work: 7,
                cumulative_work: 12,
                parent_state_root: [0; 32],
                state_root: [3; 32],
            }],
        },
    ] {
        let mut wire = message.encode().unwrap();
        Message::decode(&wire).unwrap();
        wire.push(0);
        assert!(Message::decode(&wire).is_err());
    }
}
