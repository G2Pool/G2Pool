use super::*;

#[test]
fn encoding_plan_accepts_work_window_accounts_and_rejects_overflow() {
    for count in [513, MAX_ENCODING_ENTRIES] {
        let entries = (0..count)
            .map(|i| EncodingEntry {
                account: {
                    let mut point = [2u8; 33];
                    point[1..3].copy_from_slice(&(i as u16).to_be_bytes());
                    point
                },
                encoding: Encoding::Individual,
            })
            .collect();
        let plan = EncodingPlan {
            entries: List(entries),
        };
        assert!(encode(&plan).is_ok());
    }
    let entries = (0..=MAX_ENCODING_ENTRIES)
        .map(|_| EncodingEntry {
            account: [2; 33],
            encoding: Encoding::Individual,
        })
        .collect();
    let plan = EncodingPlan {
        entries: List(entries),
    };
    assert!(matches!(encode(&plan), Err(Error::Invalid("list ceiling"))));
}
