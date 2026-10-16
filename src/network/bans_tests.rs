use super::*;

#[test]
fn normalized_prefixes_expiry_extension_and_loopback() {
    let bans = IpBans::default();
    let now = Instant::now();
    let duration = Duration::from_secs(600);
    bans.ban_at("::ffff:8.8.8.8".parse().unwrap(), duration, now);
    assert!(bans.contains_at("8.8.8.8".parse().unwrap(), now));
    assert!(!bans.contains_at("8.8.8.9".parse().unwrap(), now));
    bans.ban_at("2001:db8:1:2::1".parse().unwrap(), duration, now);
    assert!(bans.contains_at("2001:db8:1:2::ffff".parse().unwrap(), now));
    assert!(!bans.contains_at("2001:db8:1:3::1".parse().unwrap(), now));
    for ip in ["127.0.0.1", "::1", "::ffff:127.0.0.1"] {
        bans.ban_at(ip.parse().unwrap(), duration, now);
        assert!(!bans.contains_at(ip.parse().unwrap(), now));
    }
    bans.ban_at("8.8.8.8".parse().unwrap(), Duration::from_secs(1), now);
    assert!(bans.contains_at("8.8.8.8".parse().unwrap(), now + Duration::from_secs(599)));
    assert!(!bans.contains_at("8.8.8.8".parse().unwrap(), now + duration));
    assert!(!IpBans::default().contains("8.8.8.8".parse().unwrap()));
}

#[test]
fn full_table_remains_bounded_and_accepts_new_bans() {
    let bans = IpBans::default();
    let now = Instant::now();
    for n in 0..MAX_BANS + 1 {
        bans.ban_at(
            IpAddr::from([10, 0, (n / 256) as u8, n as u8]),
            DEFAULT_BAN,
            now,
        );
    }
    assert_eq!(bans.0.lock().unwrap().len(), MAX_BANS);
    assert!(bans.contains_at(IpAddr::from([10, 0, 1, 192]), now));
}
