use super::*;

#[test]
fn checkpoints_skip_idle_rewrites_and_capture_progress_and_spent_outputs() {
    grin_core::global::set_local_chain_type(grin_core::global::ChainTypes::AutomatedTesting);
    let profile = Profile::new(crate::native::manifest(
        [0; 32],
        grin_core::global::ChainTypes::AutomatedTesting,
    ))
    .unwrap();
    let wallet = Wallet::from_seed(&[1; 32]).unwrap();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("scan.json");
    let mut monitor = Monitor::new([1, 2], 1);
    monitor.head = Some(Anchor {
        height: 7,
        hash: [7; 32],
    });
    monitor.cursor = 10;
    monitor.save_state(&path, &wallet, &profile).unwrap();
    #[cfg(unix)]
    let original_inode = {
        use std::os::unix::fs::MetadataExt;
        std::fs::metadata(&path).unwrap().ino()
    };
    monitor.save_state(&path, &wallet, &profile).unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        assert_eq!(std::fs::metadata(&path).unwrap().ino(), original_inode);
    }
    monitor.cursor = 20;
    monitor.save_state(&path, &wallet, &profile).unwrap();
    let read = || serde_json::from_slice::<ScanState>(&std::fs::read(&path).unwrap()).unwrap();
    assert_eq!(read().cursor, 20);
    monitor.owned.push(Owned {
        commitment: Commitment([2; 33]),
        value: 3,
        height: 7,
        coinbase: true,
        mining: true,
    });
    monitor.save_state(&path, &wallet, &profile).unwrap();
    assert_eq!(read().owned.len(), 1);
    monitor.owned.clear();
    monitor.save_state(&path, &wallet, &profile).unwrap();
    assert!(read().owned.is_empty());
    monitor.head = Some(Anchor {
        height: 8,
        hash: [8; 32],
    });
    monitor.save_state(&path, &wallet, &profile).unwrap();
    assert_eq!(read().head_height, 8);
}
