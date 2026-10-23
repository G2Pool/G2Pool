use super::*;
#[test]
fn control_frames_are_bounded_in_both_directions() {
    let mut input = std::io::Cursor::new(b"{}\nnext");
    assert_eq!(read_frame(&mut input, None).unwrap(), b"{}");
    let mut oversized = std::io::Cursor::new(vec![b' '; MAX_FRAME + 1]);
    assert!(matches!(
        read_frame(&mut oversized, None),
        Err(crate::Error::Invalid("control frame"))
    ));

    let request = Request {
        token: "00".repeat(32),
        command: Command::WithdrawalReply {
            slatepack: "\0".repeat(crate::wallet::payments::MAX_SLATEPACK_BYTES),
        },
    };
    let mut encoded = serde_json::to_vec(&request).unwrap();
    assert!(encoded.len() > 1 << 20);
    encoded.push(b'\n');
    let decoded: Request =
        serde_json::from_slice(&read_frame(&mut std::io::Cursor::new(encoded), None).unwrap())
            .unwrap();
    assert!(
        matches!(decoded.command, Command::WithdrawalReply { slatepack }
        if slatepack.len() == crate::wallet::payments::MAX_SLATEPACK_BYTES)
    );
}

#[test]
fn trickled_bytes_do_not_extend_the_request_frame_deadline() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let (ready, received) = std::sync::mpsc::sync_channel(1);
    let server = std::thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(1)))
            .unwrap();
        let mut reader = BufReader::new(stream);
        assert_eq!(reader.fill_buf().unwrap(), b"{");
        let deadline = Instant::now() + Duration::from_millis(50);
        ready.send(()).unwrap();
        read_frame(&mut reader, Some(deadline))
    });
    let mut client = TcpStream::connect(address).unwrap();
    client.write_all(b"{").unwrap();
    received.recv().unwrap();
    std::thread::sleep(Duration::from_millis(100));
    let _ = client.write_all(b"}\n");
    assert!(
        matches!(server.join().unwrap(), Err(crate::Error::Io(error))
        if error.kind() == std::io::ErrorKind::TimedOut)
    );
}

#[test]
fn control_requests_are_authenticated_and_endpoint_is_removed_on_shutdown() {
    let dir = tempfile::tempdir().unwrap();
    let listener = Listener::bind(dir.path()).unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        assert_eq!(
            fs::metadata(socket_path(dir.path()))
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o600,
        );
    }
    let stop = Arc::new(AtomicBool::new(false));
    let signal = stop.clone();
    let server = std::thread::spawn(move || {
        serve(listener, signal, |_| Response {
            result: ResponseResult::Ok {
                value: serde_json::json!({"running":true}),
            },
        })
    });
    let response = call(dir.path(), Command::Status).unwrap();
    assert!(matches!(response.result, ResponseResult::Ok { value } if value["running"] == true));
    let mut endpoint = read_endpoint(&socket_path(dir.path())).unwrap();
    endpoint.token = "bad token".into();
    crate::secure_file::publish_atomic(
        &socket_path(dir.path()),
        &serde_json::to_vec(&endpoint).unwrap(),
        true,
    )
    .unwrap();
    let response = call(dir.path(), Command::Status).unwrap();
    assert!(matches!(response.result, ResponseResult::Err { code, .. } if code == "auth"));
    stop.store(true, Ordering::Release);
    server.join().unwrap().unwrap();
    assert!(!socket_path(dir.path()).exists());
}
