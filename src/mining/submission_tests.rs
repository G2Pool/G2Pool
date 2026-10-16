use super::*;

#[test]
fn restored_journal_obeys_count_and_total_byte_limits() {
    let dir = tempfile::tempdir().unwrap();
    let journal = Solutions::open(&dir.path().join("count"), [1; 32]).unwrap();
    for index in 0..=MAX_RECORDS {
        fs::write(journal.directory.join(format!("{index}.intent")), [0; 76]).unwrap();
    }
    assert!(matches!(
        journal.pending(),
        Err(Error::Capacity("solution journal"))
    ));

    let journal = Solutions::open(&dir.path().join("bytes"), [1; 32]).unwrap();
    for index in 0..=MAX_BYTES / MAX_RECORD as u64 {
        let file = fs::File::create(journal.directory.join(format!("{index}.intent"))).unwrap();
        file.set_len(MAX_RECORD as u64).unwrap();
    }
    assert!(matches!(
        journal.pending(),
        Err(Error::Capacity("solution journal"))
    ));
}

#[cfg(unix)]
#[test]
fn restored_journal_rejects_symlinks() {
    let dir = tempfile::tempdir().unwrap();
    let journal = Solutions::open(&dir.path().join("pool"), [1; 32]).unwrap();
    let outside = dir.path().join("outside");
    fs::write(&outside, [0; 76]).unwrap();
    std::os::unix::fs::symlink(&outside, journal.directory.join("link.intent")).unwrap();
    assert!(matches!(
        journal.pending(),
        Err(Error::Invalid("solution intent file"))
    ));
    assert_eq!(fs::read(outside).unwrap(), [0; 76]);
}
