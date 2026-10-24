use super::*;
use crate::status::reporting::Event as LogEvent;
use grin_util::{Mutex, RwLock};
use ratatui::{backend::TestBackend, Terminal};
use std::{
    collections::{BTreeMap, VecDeque},
    sync::Arc,
};

fn reporting() -> Reporting {
    Reporting {
        phase: RwLock::new("Starting G2Pool"),
        sources: RwLock::new(None),
        conditions: Mutex::new(BTreeMap::new()),
        events: Arc::new(Mutex::new(VecDeque::new())),
    }
}

fn screen() -> Screen {
    Screen {
        snapshot: Some(Snapshot {
            node: Ok(status::runtime::Snapshot {
                pool_name: "Alpha".into(),
                pool_id: "ab".repeat(32),
                updated_at: chrono::Utc::now().timestamp(),
                state: "disconnected".into(),
                native_height: 10,
                native_head: "cd".repeat(32),
                native_pmmr_size: 30,
                connected_peers: 0,
                native_syncing: false,
                pool_recovering: false,
                pool_tip: None,
                free_disk_bytes: Some(10 * status::runtime::RESERVE),
                database_bytes: 4096,
                database_limit_bytes: 4 * 1024 * 1024 * 1024,
                database_allocated_bytes: 8192,
            }),
            wallet: Ok(status::wallet::snapshot(
                &[0; 33],
                &[0; 32],
                None,
                0,
                "refreshing",
                None,
                None,
            )),
            job: Ok(serde_json::json!({"height":11,"job_id":1,"difficulty":3,"pool_difficulty":3})),
            sync: SyncStatus::NoSync,
        }),
        tab: 0,
        scroll: (0, 0),
        withdrawal: withdrawal::Panel::default(),
    }
}

fn render(screen: &mut Screen, reporting: &Reporting, width: u16, height: u16) -> String {
    let config = Config::load(
        std::path::PathBuf::from("unused"),
        grin_core::global::ChainTypes::Testnet,
        None,
        None,
        None,
        None,
    )
    .unwrap();
    let mut terminal = Terminal::new(TestBackend::new(width, height)).unwrap();
    terminal
        .draw(|frame| screen.draw(frame, &config, reporting, false))
        .unwrap();
    terminal
        .backend()
        .buffer()
        .content
        .chunks(width as usize)
        .map(|row| row.iter().map(|cell| cell.symbol()).collect::<String>())
        .collect::<Vec<_>>()
        .join("\n")
}

#[test]
fn isolated_mining_and_unavailable_wallet_balances_are_displayed_independently() {
    let reporting = reporting();
    let mut screen = screen();
    let pool = render(&mut screen, &reporting, 100, 28);
    assert!(pool.contains("work available"));
    assert!(pool.contains("0 (isolated)"));
    screen.tab = 2;
    let wallet = render(&mut screen, &reporting, 100, 28);
    assert!(wallet.contains("Balance                Not yet available"));
    assert!(wallet.contains("Not yet available"));
    assert!(!wallet.contains("0.000000000 GRIN"));
    assert!(wallet.contains("Withdrawal"));
    assert!(wallet.contains("w: withdraw"));
    let pending = crate::wallet::payments::Withdrawal {
        id: "test".into(),
        amount: 1_250_000_000,
        fee: 7,
        stage: crate::wallet::payments::Phase::AwaitingResponse,
        outgoing: "BEGINSLATEPACK. offer ENDSLATEPACK.".into(),
    };
    screen
        .snapshot
        .as_mut()
        .unwrap()
        .wallet
        .as_mut()
        .unwrap()
        .withdrawal = Some(pending.clone());
    let wallet = render(&mut screen, &reporting, 100, 28);
    assert!(wallet.contains("1.250000000 GRIN"));
    assert!(wallet.contains("0.000000007 GRIN"));
    assert!(wallet.contains("p: paste reply"));
    assert_eq!(
        screen
            .snapshot
            .as_ref()
            .unwrap()
            .wallet
            .as_ref()
            .unwrap()
            .withdrawal,
        Some(pending)
    );
}

#[test]
fn conditions_survive_repeated_retries_and_all_views_render_in_a_small_terminal() {
    let reporting = reporting();
    assert!(reporting.condition("Storage", Some("database capacity".into())));
    let since = reporting.conditions.lock()["Storage"].since;
    assert!(!reporting.condition("Storage", Some("database capacity".into())));
    assert_eq!(reporting.conditions.lock()["Storage"].since, since);
    reporting.events.lock().push_back(LogEvent {
        time: chrono::Local::now(),
        level: log::Level::Warn,
        source: "g2pool::storage".into(),
        message: "database capacity".into(),
    });
    let mut screen = screen();
    for tab in 0..TABS.len() {
        screen.tab = tab;
        let text = render(&mut screen, &reporting, 80, 24);
        assert!(text.contains("Storage: database capacity"));
        render(&mut screen, &reporting, 40, 12);
    }
    assert!(reporting.condition("Storage", None));
    assert!(reporting.conditions.lock().is_empty());
    assert!(!reporting.events.lock().is_empty());
}

#[test]
fn withdrawal_input_requires_explicit_actions_and_preserves_the_saved_offer() {
    use crate::wallet::payments::{Phase, Withdrawal, WorkerAction, MAX_SLATEPACK_BYTES};
    use crossterm::event::KeyEvent;
    let key = |code, modifiers| Event::Key(KeyEvent::new(code, modifiers));
    let plain = |code| key(code, KeyModifiers::NONE);
    let mut panel = withdrawal::Panel::default();
    assert!(panel.event(&plain(KeyCode::Char('w')), None).is_none());
    assert!(panel.event(&Event::Paste("1.25".into()), None).is_none());
    assert!(matches!(
        panel.event(&plain(KeyCode::Enter), None),
        Some(WorkerAction::Offer(1_250_000_000))
    ));
    panel.complete(Ok(serde_json::json!({"state":"awaiting_response"})));
    let mut pending = Withdrawal {
        id: "test".into(),
        amount: 1_250_000_000,
        fee: 7,
        stage: Phase::AwaitingResponse,
        outgoing: "offer".into(),
    };
    assert!(panel
        .event(&plain(KeyCode::Char('p')), Some(&pending))
        .is_none());
    assert!(panel
        .event(
            &Event::Paste("reply\r\nENDSLATEPACK.".into()),
            Some(&pending)
        )
        .is_none());
    assert!(
        matches!(panel.event(&key(KeyCode::Char('s'), KeyModifiers::CONTROL), Some(&pending)), Some(WorkerAction::Finalize(reply)) if reply == "reply\nENDSLATEPACK.")
    );
    assert!(panel.event(&plain(KeyCode::Esc), Some(&pending)).is_none());
    assert!(!panel.editing());
    assert!(matches!(
        panel.event(&plain(KeyCode::Char('x')), Some(&pending)),
        Some(WorkerAction::Cancel)
    ));
    pending.stage = Phase::ReadyToPost;
    assert!(panel
        .event(&plain(KeyCode::Char('x')), Some(&pending))
        .is_none());
    assert!(matches!(
        panel.event(&plain(KeyCode::Char('r')), Some(&pending)),
        Some(WorkerAction::Resume)
    ));
    pending.stage = Phase::AwaitingResponse;
    panel.event(&plain(KeyCode::Char('p')), Some(&pending));
    panel.event(
        &Event::Paste("a".repeat(MAX_SLATEPACK_BYTES + 1)),
        Some(&pending),
    );
    assert!(panel
        .event(
            &key(KeyCode::Char('s'), KeyModifiers::CONTROL),
            Some(&pending)
        )
        .is_none());
    panel.busy = true;
    assert!(!panel.editing());
    assert!(panel
        .event(&plain(KeyCode::Char('r')), Some(&pending))
        .is_none());
}
