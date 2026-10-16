use super::*;
#[test]
fn rapid_job_turnover_preserves_the_entire_promised_interval() {
    let now = Instant::now();
    let mut leases = Leases::new(Retention {
        duration: Duration::from_secs(60),
        bytes: 100,
    });
    for id in 0..50 {
        leases
            .insert(id, id, 2, now + Duration::from_millis(id))
            .unwrap();
    }
    assert_eq!(leases.get(0), Some(&0));
    assert!(matches!(
        leases.insert(50, 50, 2, now + Duration::from_secs(1)),
        Err(Error::Capacity("mining job retention"))
    ));
    assert_eq!(leases.get(0), Some(&0));
    leases.expire(now + Duration::from_secs(60));
    assert_eq!(leases.get(0), None);
    assert_eq!(leases.get(49), Some(&49));
    leases
        .insert(50, 50, 2, now + Duration::from_secs(60))
        .unwrap();
}
