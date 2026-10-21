use super::*;

fn oracle_ids(ledger: &Ledger, low: u64, limits: WindowLimits) -> Vec<Id> {
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
    let mut warm = Ledger::new(10);
    for frame in [
        frame(5, 10, Some(6), Some((1, 7))),
        frame(6, 8, Some(0), Some((2, 6))),
        frame(0, 6, None, None),
    ] {
        warm.append(frame).unwrap();
    }
    for low in [0, 7, 9] {
        let mut branch = warm.clone();
        branch.rebase(11).unwrap();
        for receipts in [1, 2, 8] {
            for work in [1, 6, 10, 100] {
                let limits = WindowLimits {
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
    let limits = WindowLimits {
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

fn frame(id: u8, height: u64, parent: Option<u8>, uncle: Option<(u8, u64)>) -> Arc<Frame> {
    let primary = Contribution {
        id: [id; 32],
        height,
        timestamp: height,
        difficulty: u64::from(id) + 1,
        class: 0,
        depth: 0,
    };
    let uncles = uncle
        .into_iter()
        .map(|(id, height)| Contribution {
            id: [id; 32],
            height,
            timestamp: height,
            difficulty: u64::from(id) + 1,
            class: 1,
            depth: 1,
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

#[test]
fn rolling_pruning_matches_cold_recomputation() {
    for window in 1..=12 {
        let mut warm = Ledger::new(0);
        let mut chain = Vec::new();
        for n in 0..48u8 {
            warm.rebase(u64::from(n)).unwrap();
            let next = frame(
                n,
                u64::from(n),
                n.checked_sub(1),
                Some((n + 128, u64::from(n.saturating_sub(2)))),
            );
            warm.prepend(next.clone()).unwrap();
            chain.insert(0, next);
            warm.trim(window);
            let mut cold = Ledger::new(u64::from(n));
            for frame in chain.iter().take(window) {
                cold.append(frame.clone()).unwrap();
            }
            cold.boundary = chain.get(window).map(|frame| frame.id());
            assert_eq!(warm.spine, cold.spine);
            assert_eq!(warm.seen, cold.seen);
            assert_eq!(warm.boundary, cold.boundary);
            assert_eq!(
                warm.window_score(window, 0).unwrap(),
                cold.window_score(window, 0).unwrap()
            );
            let limits = WindowLimits {
                primary_limit: window,
                receipt_limit: 8,
                work_limit: Some(1000),
            };
            let low = u64::from(n.saturating_sub(2));
            assert_eq!(
                warm.window_ids(low, limits).unwrap(),
                cold.window_ids(low, limits).unwrap()
            );
        }
    }
}

#[test]
fn uncle_still_blocks_duplicates_until_its_frame_is_removed() {
    let mut ledger = Ledger::new(5);
    ledger.append(frame(1, 5, None, Some((2, 1)))).unwrap();
    ledger.rebase(6).unwrap();
    assert_eq!(ledger.full(0), BTreeSet::from([[1; 32], [2; 32]]));
    let before = (ledger.seen.clone(), ledger.full(0));
    assert!(matches!(
        ledger.prepend(frame(3, 6, Some(1), Some((2, 1)))),
        Err(Error::Invalid("duplicate contribution"))
    ));
    assert_eq!((ledger.seen.clone(), ledger.full(0)), before);
    ledger.rebase(8).unwrap();
    ledger.trim(0);
    assert!(ledger.frames.is_empty());
    assert!(!ledger.seen.contains(&[2; 32]));
    ledger.prepend(frame(3, 8, Some(1), Some((2, 1)))).unwrap();
    assert_eq!(ledger.full(0), BTreeSet::from([[3; 32], [2; 32]]));
    let before = (ledger.height, ledger.full(0));
    assert!(matches!(ledger.rebase(7), Err(Error::View)));
    assert_eq!((ledger.height, ledger.full(0)), before);
}

#[test]
fn uncle_credit_uses_share_distance_at_the_window_boundary() {
    let mut ledger = Ledger::new(5);
    for n in (0..=10u8).rev() {
        let mut next = frame(n, 5, n.checked_sub(1), (n == 10).then_some((200, 5)));
        if n == 10 {
            Arc::get_mut(&mut next).unwrap().uncles[0].depth = 8;
        }
        ledger.append(next).unwrap();
    }
    assert!(!ledger.recent(8, 0).any(|entry| entry.id == [200; 32]));
    assert!(ledger.recent(9, 0).any(|entry| entry.id == [200; 32]));
    ledger.trim(8);
    assert!(ledger.full(0).contains(&[200; 32]));
    assert!(!ledger.recent(8, 0).any(|entry| entry.id == [200; 32]));
}

#[test]
fn fresh_work_credit_preserves_older_financial_contributions() {
    let mut ledger = Ledger::new(100);
    ledger
        .append(frame(6, 100, Some(5), Some((200, 94))))
        .unwrap();
    ledger.append(frame(5, 94, Some(4), None)).unwrap();
    ledger.append(frame(4, 90, None, None)).unwrap();
    assert_eq!(ledger.window_score(3, 95).unwrap(), 7);
    assert_eq!(
        ledger.full(90),
        BTreeSet::from([[6; 32], [200; 32], [5; 32], [4; 32]])
    );
    let limits = WindowLimits {
        primary_limit: 3,
        receipt_limit: 8,
        work_limit: None,
    };
    assert_eq!(
        ledger.window_ids(90, limits).unwrap(),
        vec![[6; 32], [200; 32], [5; 32], [4; 32]]
    );
    assert_eq!(ledger.full(95), BTreeSet::from([[6; 32]]));
    assert_eq!(ledger.window_ids(95, limits).unwrap(), vec![[6; 32]]);
}
