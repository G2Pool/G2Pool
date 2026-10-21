use super::*;

#[tokio::test]
async fn only_malformed_remote_data_is_marked_for_banning() {
    use rpc::Codec as _;
    let budgets = Budgets::default();
    let requested = crate::objects::Key::new(crate::objects::Kind::Coupon, [1; 32]);
    let reply = Message::Reply {
        objects: vec![],
        missing: vec![],
        deferred: vec![],
    }
    .encode()
    .unwrap();
    let mut wire = libp2p::futures::io::Cursor::new(Vec::new());
    write_frame(&mut wire, &reply, crate::VERSION, crate::MIN_PEER_VERSION)
        .await
        .unwrap();
    let wire = wire.into_inner();
    let mut codec = Codec {
        budgets: budgets.clone(),
        bulk: true,
        expected: Some(Request {
            message: Message::Get(vec![requested]),
            ticket: budgets.receive.reserve(4096).unwrap(),
        }),
    };
    let error = codec
        .read_response(
            &StreamProtocol::new("/test"),
            &mut libp2p::futures::io::Cursor::new(wire),
        )
        .await
        .unwrap_err();
    assert!(is_remote_protocol_error(&error));
    assert_eq!(budgets.receive.used(), 0);
    assert!(!is_remote_protocol_error(&io::Error::from(
        io::ErrorKind::InvalidData
    )));
    assert!(!is_remote_protocol_error(&io::Error::from(
        io::ErrorKind::TimedOut
    )));
    let mut codec = Codec {
        budgets: budgets.clone(),
        bulk: true,
        expected: None,
    };
    let request = Request {
        message: Message::Get(vec![requested, requested]),
        ticket: budgets.control.reserve(4096).unwrap(),
    };
    let error = codec
        .write_request(
            &StreamProtocol::new("/test"),
            &mut libp2p::futures::io::Cursor::new(Vec::<u8>::new()),
            request,
        )
        .await
        .unwrap_err();
    assert!(!is_remote_protocol_error(&error));
}

#[tokio::test]
async fn versions_are_checked_before_payloads_without_semver_compatibility_ranges() {
    use libp2p::futures::io::Cursor;
    let current = semver::Version::parse(crate::VERSION).unwrap();
    let minimum = semver::Version::parse(crate::MIN_PEER_VERSION).unwrap();
    assert!(!current.cmp_precedence(&minimum).is_lt());
    for (local, local_min, remote, remote_min) in [
        ("0.1.9", "0.1.9", "0.1.10", "0.1.0"),
        ("0.1.0", "0.1.0", "3.0.0", "0.1.0"),
        ("0.2.0", "0.2.0", "0.2.0+build", "0.2.0"),
    ] {
        let mut wire = Cursor::new(Vec::new());
        write_frame(&mut wire, &[0], remote, remote_min)
            .await
            .unwrap();
        wire.set_position(0);
        assert_eq!(
            frame(&mut wire, CONTROL, local, local_min, |tag| tag == 0)
                .await
                .unwrap(),
            [0]
        );
    }
    for (local, local_min, remote, remote_min) in [
        ("0.2.0", "0.2.0", "0.1.10", "0.1.0"),
        ("0.1.10", "0.1.10", "0.1.9", "0.1.0"),
        ("0.2.0", "0.2.0", "0.2.0-rc.1", "0.1.0"),
        ("0.1.0", "0.1.0", "0.2.0", "0.2.0"),
    ] {
        let mut wire = Cursor::new(Vec::new());
        write_frame(&mut wire, &[255], remote, remote_min)
            .await
            .unwrap();
        wire.set_position(0);
        let error = frame(&mut wire, CONTROL, local, local_min, |_| {
            panic!("incompatible payload inspected")
        })
        .await
        .unwrap_err();
        assert!(is_incompatible_version(&error));
        assert!(!is_remote_protocol_error(&error));
    }
    for remote in ["nonsense", "01.2.3"] {
        let mut wire = Cursor::new(Vec::new());
        write_frame(&mut wire, &[0], remote, "0.1.0").await.unwrap();
        wire.set_position(0);
        let error = frame(&mut wire, CONTROL, "0.1.0", "0.1.0", |_| true)
            .await
            .unwrap_err();
        assert!(is_remote_protocol_error(&error));
    }
}

#[tokio::test]
async fn incompatible_versions_precede_payload_limits_and_release_tickets_in_both_directions() {
    use libp2p::futures::io::Cursor;
    use rpc::Codec as _;
    let budgets = Budgets::default();
    let local = semver::Version::parse(crate::VERSION).unwrap();
    let future = format!("{}.0.0", local.major + 1);
    for response in [false, true] {
        for (version, minimum) in [("0.0.0", "0.0.0"), (future.as_str(), future.as_str())] {
            let mut wire = Cursor::new(Vec::new());
            write_frame(
                &mut wire,
                &vec![255; CONTROL + ENVELOPE + 1],
                version,
                minimum,
            )
            .await
            .unwrap();
            wire.set_position(0);
            let mut codec = Codec {
                budgets: budgets.clone(),
                bulk: false,
                expected: response.then(|| Request {
                    message: Message::Heads,
                    ticket: budgets.control.reserve(CONTROL).unwrap(),
                }),
            };
            let error = if response {
                codec
                    .read_response(&StreamProtocol::new("/test"), &mut wire)
                    .await
                    .unwrap_err()
            } else {
                codec
                    .read_request(&StreamProtocol::new("/test"), &mut wire)
                    .await
                    .unwrap_err()
            };
            assert!(is_incompatible_version(&error));
            assert!(!is_remote_protocol_error(&error));
            assert_eq!(
                wire.position() as usize,
                4 + 2 + version.len() + minimum.len()
            );
            assert_eq!(budgets.control.used(), 0);
        }
    }
}
