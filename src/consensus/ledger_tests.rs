use super::*;

fn oracle_ids(ledger: &Ledger, low: u64, limits: window::WindowLimits) -> Vec<Id> {
    let mut out = Vec::new();
    let mut total = 0u128;
    let mut primaries = 0usize;
    for frame in &ledger.frames {
        for entry in frame.contributions().filter(|entry| entry.height >= low) {
            if entry.class == 0 {
                if primaries == limits.primary_limit {
                    return out;
                }
                primaries += 1;
            } else if limits
                .work_limit
                .is_some_and(|limit| total.saturating_add(u128::from(entry.difficulty)) > limit)
            {
                continue;
            }
            out.push(entry.id);
            total += u128::from(entry.difficulty);
            if out.len() == limits.receipt_limit
                || limits.work_limit.is_some_and(|limit| total >= limit)
            {
                return out;
            }
        }
    }
    out
}

#[test]
fn bounded_membership_matches_cold_after_expiry_and_fork() {
    let mut warm = Ledger::new(10, 0);
    for frame in [
        frame(5, 10, Some(6), Some((1, 7))),
        frame(6, 8, Some(0), Some((2, 6))),
        frame(0, 6, None, None),
    ] {
        warm.append(frame).unwrap();
    }
    for low in [0, 7, 9] {
        let mut branch = warm.clone();
        branch.advance(11, low).unwrap();
        for receipts in [1, 2, 8] {
            for work in [1, 6, 10, 100] {
                let limits = window::WindowLimits {
                    primary_limit: 8,
                    receipt_limit: receipts,
                    work_limit: Some(work),
                };
                let expected = oracle_ids(&branch, low, limits);
                assert_eq!(branch.window_ids(low, limits).unwrap(), expected);
                assert_eq!(branch.window_ids(low, limits).unwrap(), expected);
            }
        }
    }
    let limits = window::WindowLimits {
        primary_limit: 8,
        receipt_limit: 8,
        work_limit: Some(10),
    };
    assert_eq!(
        warm.window_ids(0, limits).unwrap(),
        vec![[5; 32], [1; 32], [6; 32]]
    );
    let mut fork = warm.clone();
    fork.rewind_to([6; 32]);
    fork.prepend(frame(8, 10, Some(6), Some((3, 9)))).unwrap();
    assert_eq!(
        fork.window_ids(0, limits).unwrap(),
        oracle_ids(&fork, 0, limits)
    );
    assert_eq!(
        warm.window_ids(0, limits).unwrap(),
        vec![[5; 32], [1; 32], [6; 32]]
    );
}

#[test]
fn branch_state_tracks_retarget_samples_after_expiry() {
    let mut ledger = Ledger::new(10, 0);
    ledger.append(frame(5, 10, Some(6), Some((1, 7)))).unwrap();
    ledger.append(frame(6, 8, Some(0), Some((2, 6)))).unwrap();
    let history = ledger.retarget_history(8).unwrap();
    assert_eq!(history.len(), 4);
    assert!(history.iter().all(|(_, work)| *work > 0));
    assert!(ledger
        .retarget_history(1)
        .unwrap()
        .iter()
        .any(|(time, _)| *time == 10));
    ledger.advance(11, 9).unwrap();
    assert_eq!(ledger.retarget_history(8).unwrap(), vec![(10, 8), (7, 2)]);
}

fn frame(id: u8, height: u64, parent: Option<u8>, uncle: Option<(u8, u64)>) -> Arc<Frame> {
    let primary = Contribution {
        id: [id; 32],
        height,
        timestamp: height,
        difficulty: u64::from(id) + 1,
        class: 0,
    };
    let uncles = uncle
        .into_iter()
        .map(|(id, height)| Contribution {
            id: [id; 32],
            height,
            timestamp: height,
            difficulty: u64::from(id) + 1,
            class: 1,
        })
        .collect();
    Arc::new(Frame {
        primary,
        uncles,
        parent: parent.map(|id| [id; 32]),
        trace: Trace::join([]),
        financial_trace: Trace::join([]),
    })
}

fn assert_cold(ledger: &Ledger, chain: &[Arc<Frame>], low: u64) {
    let frames: Vec<_> = chain.iter().filter(|frame| frame.height() >= low).collect();
    let entries: Vec<_> = frames
        .iter()
        .flat_map(|frame| frame.contributions())
        .collect();
    let active: Vec<_> = entries.iter().filter(|entry| entry.height >= low).collect();
    assert_eq!(ledger.full(), active.iter().map(|entry| entry.id).collect());
    assert_eq!(
        ledger.score,
        active
            .iter()
            .map(|entry| u128::from(entry.difficulty))
            .sum()
    );
    assert_eq!(ledger.seen, entries.iter().map(|entry| entry.id).collect());
    assert_eq!(
        ledger.spine,
        frames.iter().map(|frame| frame.id()).collect()
    );
    assert_eq!(
        ledger
            .frames
            .iter()
            .map(|frame| frame.id())
            .collect::<Vec<_>>(),
        frames.iter().map(|frame| frame.id()).collect::<Vec<_>>()
    );
    assert_eq!(
        ledger.boundary,
        chain
            .iter()
            .find(|frame| frame.height() < low)
            .map(|frame| frame.id())
    );
}

#[test]
fn branch_undo_and_age_deltas_equal_cold_recomputation() {
    for low in 0..=11 {
        for fork in low..=11 {
            let mut chain: Vec<_> = (0..12)
                .rev()
                .map(|id| {
                    frame(
                        id,
                        u64::from(id),
                        id.checked_sub(1),
                        Some((id + 32, u64::from(id.saturating_sub(2)))),
                    )
                })
                .collect();
            let mut ledger = Ledger::new(11, 0);
            for frame in &chain {
                ledger.append(frame.clone()).unwrap();
            }
            ledger.advance(12, u64::from(low)).unwrap();
            assert_cold(&ledger, &chain, u64::from(low));
            ledger.rewind_to([fork; 32]);
            chain.retain(|frame| frame.height() <= u64::from(fork));
            let replacement = frame(80, 12, Some(fork), Some((81, 12)));
            ledger.prepend(replacement.clone()).unwrap();
            chain.insert(0, replacement);
            assert_cold(&ledger, &chain, u64::from(low));
            ledger.advance(14, 12).unwrap();
            assert_cold(&ledger, &chain, 12);
        }
    }
}

#[test]
fn expired_uncle_still_blocks_duplicates_until_its_frame_is_removed() {
    let mut ledger = Ledger::new(5, 0);
    ledger.append(frame(1, 5, None, Some((2, 1)))).unwrap();
    ledger.advance(6, 3).unwrap();
    assert_eq!(ledger.full(), BTreeSet::from([[1; 32]]));
    let before = (ledger.score, ledger.seen.clone(), ledger.full());
    assert!(matches!(
        ledger.prepend(frame(3, 6, Some(1), Some((2, 1)))),
        Err(Error::Invalid("duplicate contribution"))
    ));
    assert_eq!((ledger.score, ledger.seen.clone(), ledger.full()), before);
    ledger.advance(8, 6).unwrap();
    assert!(ledger.frames.is_empty());
    assert!(!ledger.seen.contains(&[2; 32]));
    ledger.prepend(frame(3, 8, Some(1), Some((2, 1)))).unwrap();
    assert_eq!(ledger.full(), BTreeSet::from([[3; 32]]));
    let before = (ledger.height, ledger.score, ledger.full());
    assert!(matches!(ledger.advance(7, 5), Err(Error::View)));
    assert_eq!((ledger.height, ledger.score, ledger.full()), before);
}
