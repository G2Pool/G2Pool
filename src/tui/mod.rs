use crate::{
    daemon::Config,
    status::{self, reporting::Reporting},
    Result,
};
use crossterm::{
    event::{
        self, DisableBracketedPaste, EnableBracketedPaste, Event, KeyCode, KeyEventKind,
        KeyModifiers,
    },
    execute,
};
use grin_chain::SyncStatus;
use std::{
    sync::atomic::{AtomicBool, Ordering},
    time::{Duration, Instant},
};

const TABS: [&str; 4] = ["Pool", "Grin", "Wallet", "Events"];
mod render;
mod withdrawal;

struct Snapshot {
    node: std::result::Result<status::runtime::Snapshot, String>,
    wallet: std::result::Result<status::wallet::Snapshot, String>,
    job: std::result::Result<serde_json::Value, String>,
    sync: SyncStatus,
}

struct Screen {
    snapshot: Option<Snapshot>,
    tab: usize,
    scroll: (u16, u16),
    withdrawal: withdrawal::Panel,
}

pub(crate) fn run(
    config: &Config,
    reporting: &Reporting,
    stop: &AtomicBool,
    finished: impl Fn() -> bool,
) -> Result<()> {
    let mut screen = Screen {
        snapshot: None,
        tab: 0,
        scroll: (0, 0),
        withdrawal: withdrawal::Panel::default(),
    };
    let mut next = Instant::now();
    ratatui::run(|terminal| {
        struct Paste;
        impl Drop for Paste {
            fn drop(&mut self) {
                let _ = execute!(std::io::stdout(), DisableBracketedPaste);
            }
        }
        execute!(std::io::stdout(), EnableBracketedPaste)?;
        let _paste = Paste;
        std::thread::scope(|scope| {
            struct Shutdown<'a>(&'a AtomicBool);
            impl Drop for Shutdown<'_> {
                fn drop(&mut self) {
                    self.0.store(true, Ordering::Release);
                }
            }
            let _shutdown = Shutdown(stop);
            let mut pending: Option<
                std::thread::ScopedJoinHandle<'_, anyhow::Result<serde_json::Value>>,
            > = None;
            while !finished() {
                if pending.as_ref().is_some_and(|worker| worker.is_finished()) {
                    let result =
                        pending.take().unwrap().join().unwrap_or_else(|_| {
                            Err(anyhow::anyhow!("withdrawal request panicked"))
                        });
                    screen.withdrawal.complete(result);
                    next = Instant::now();
                }
                if Instant::now() >= next {
                    let sources = reporting.sources.read().clone();
                    screen.snapshot = sources.map(|sources| Snapshot {
                        node: sources.node.snapshot().map_err(|error| error.to_string()),
                        wallet: sources.wallet.snapshot().map_err(|error| error.to_string()),
                        job: sources.mining.job().map_err(|error| error.to_string()),
                        sync: sources.node.sync_status(),
                    });
                    next = Instant::now() + Duration::from_secs(1);
                }
                terminal.draw(|frame| {
                    screen.draw(frame, config, reporting, stop.load(Ordering::Acquire))
                })?;
                if event::poll(Duration::from_millis(200))? {
                    let event = event::read()?;
                    if matches!(&event, Event::Key(key) if key.kind == KeyEventKind::Release) {
                        continue;
                    }
                    if matches!(&event, Event::Key(key) if key.code == KeyCode::Char('c') && key.modifiers.contains(KeyModifiers::CONTROL))
                    {
                        stop.store(true, Ordering::Release);
                        continue;
                    }
                    if stop.load(Ordering::Acquire) {
                        continue;
                    }
                    let editing = screen.tab == 2 && screen.withdrawal.editing();
                    if screen.tab == 2 {
                        let withdrawal = screen
                            .snapshot
                            .as_ref()
                            .and_then(|snapshot| snapshot.wallet.as_ref().ok())
                            .and_then(|wallet| wallet.withdrawal.as_ref());
                        if let Some(action) = screen.withdrawal.event(&event, withdrawal) {
                            if let Some(sources) = reporting.sources.read().clone() {
                                pending = Some(scope.spawn(move || sources.wallet.call(action)));
                                screen.withdrawal.busy = true;
                            } else {
                                screen
                                    .withdrawal
                                    .complete(Err(anyhow::anyhow!("Wallet service is starting")));
                            }
                        }
                    }
                    if editing {
                        continue;
                    }
                    if let Event::Key(key) = event {
                        match key.code {
                            KeyCode::Char('q') => stop.store(true, Ordering::Release),
                            KeyCode::Tab => {
                                screen.tab = (screen.tab + 1) % TABS.len();
                                screen.scroll = (0, 0);
                            }
                            KeyCode::BackTab => {
                                screen.tab = (screen.tab + TABS.len() - 1) % TABS.len();
                                screen.scroll = (0, 0);
                            }
                            KeyCode::Char(c @ '1'..='4') => {
                                screen.tab = c as usize - '1' as usize;
                                screen.scroll = (0, 0);
                            }
                            KeyCode::Up => screen.scroll.0 = screen.scroll.0.saturating_sub(1),
                            KeyCode::Down => screen.scroll.0 = screen.scroll.0.saturating_add(1),
                            KeyCode::Left => screen.scroll.1 = screen.scroll.1.saturating_sub(4),
                            KeyCode::Right => screen.scroll.1 = screen.scroll.1.saturating_add(4),
                            KeyCode::PageUp => screen.scroll.0 = screen.scroll.0.saturating_sub(10),
                            KeyCode::PageDown => {
                                screen.scroll.0 = screen.scroll.0.saturating_add(10)
                            }
                            KeyCode::Home => screen.scroll = (0, 0),
                            _ => {}
                        }
                    }
                }
            }
            Ok(())
        })
    })
}

#[cfg(test)]
mod tests;
