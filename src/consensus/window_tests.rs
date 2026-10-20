use super::*;
#[test]
fn bounded_ids_keeps_newest_when_work_crosses() {
    let ids = [[1; 32], [2; 32]];
    let got = bounded_ids(
        &[(ids[0], 6, 0), (ids[1], 7, 0), ([3; 32], 1, 0)],
        WindowLimits {
            primary_limit: 8,
            receipt_limit: 8,
            work_limit: Some(10),
        },
    )
    .unwrap();
    assert_eq!(got, BTreeSet::from([ids[0], ids[1]]));
}

#[test]
fn bounded_ids_count_crossing_keeps_newest_prefix() {
    let ids = [[1; 32], [2; 32], [3; 32]];
    let got = bounded_ids(
        &[(ids[0], 1, 0), (ids[1], 1, 0), (ids[2], 1, 0)],
        WindowLimits {
            primary_limit: 8,
            receipt_limit: 2,
            work_limit: None,
        },
    )
    .unwrap();
    assert_eq!(got, BTreeSet::from([ids[0], ids[1]]));
}
