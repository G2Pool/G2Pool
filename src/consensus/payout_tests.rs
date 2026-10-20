use super::*;
#[test]
fn batched_presence_visits_exactly_the_scalar_planners_queries() {
    use grin_util::secp::pedersen::Commitment;
    let head = Anchor {
        height: 100,
        hash: [100; 32],
    };
    let birth = Anchor {
        height: 90,
        hash: [90; 32],
    };
    let unused_birth = Anchor {
        height: 1,
        hash: [1; 32],
    };
    let rows = vec![
        (
            birth.clone(),
            (1..=12).map(|n| Commitment([n; 33])).collect::<Vec<_>>(),
        ),
        (
            birth.clone(),
            (20..32).map(|n| Commitment([n; 33])).collect::<Vec<_>>(),
        ),
        (
            unused_birth,
            (40..52).map(|n| Commitment([n; 33])).collect::<Vec<_>>(),
        ),
    ];
    let is_present =
        |anchor: &Anchor, c: Commitment| anchor == &head && c.0[0] != 2 && c.0[0] != 20;
    let mut oracle = BTreeSet::new();
    for (birth, commitments) in &rows {
        for commitment in commitments {
            oracle.insert((head.height, commitment.0));
            if !is_present(&head, *commitment) {
                oracle.insert((birth.height, commitment.0));
                break;
            }
        }
    }
    let mut actual = BTreeSet::new();
    let mut batches = 0;
    prefetch_coupon_presence(&head, &rows, |anchor, commitments| {
        assert_ne!(
            anchor.height, 1,
            "unused birth history must never be queried"
        );
        batches += 1;
        for c in commitments {
            actual.insert((anchor.height, c.0));
        }
        Ok(commitments.iter().map(|c| is_present(anchor, *c)).collect())
    })
    .unwrap();
    assert_eq!(actual, oracle);
    assert!(batches < oracle.len());
}

#[test]
fn settlement_respects_the_precision_and_receipt_envelope() {
    for c in 0..=512u16 {
        for t in 0..=512 - c {
            let outputs = (c + 4 * (t / 5)).count_ones() + (t % 5).count_ones();
            let classes = usize::from(c > 0) + usize::from(t > 0);
            assert!(outputs as usize <= output_bound(c + t, classes));
        }
    }
    assert_eq!(output_bound(512, 512), 512);
    assert_eq!(output_bound(1024, 512), 768);
    assert_eq!(output_bound(2048, 512), 1152);
    assert_eq!(output_bound(4096, 512), 1600);
    assert_eq!(output_bound(8192, 512), 2080);
    assert_eq!(output_bound(u16::MAX, 1), 16);
}
