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
    let mut wire = (reply.len() as u32).to_be_bytes().to_vec();
    wire.extend(reply);
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
