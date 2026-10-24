use super::{Screen, Snapshot, TABS};
use crate::{
    daemon::Config,
    status::{self, reporting::Reporting},
};
use grin_chain::SyncStatus;
use ratatui::{
    layout::{Constraint, Layout, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Padding, Paragraph, Tabs, Wrap},
    Frame,
};

pub(super) fn field(label: &str, value: impl std::fmt::Display) -> Line<'static> {
    Line::from(vec![
        Span::styled(format!("{label:<23}"), Style::default().fg(Color::Cyan)),
        Span::raw(value.to_string()),
    ])
}

pub(super) fn amount(value: u64) -> String {
    format!(
        "{}.{:09} GRIN",
        value / 1_000_000_000,
        value % 1_000_000_000
    )
}

fn sync_label(status: SyncStatus) -> String {
    match status {
        SyncStatus::Initial => "Initializing".into(),
        SyncStatus::NoSync => "Synchronized".into(),
        SyncStatus::AwaitingPeers(_) => "Waiting for Grin peers".into(),
        SyncStatus::HeaderSync {
            sync_head,
            highest_height,
            ..
        } => format!("Headers: {} / {highest_height}", sync_head.height),
        SyncStatus::TxHashsetPibd {
            completed_leaves,
            leaves_required,
            errored,
            aborted,
            ..
        } => format!(
            "State reconstruction: {completed_leaves} / {leaves_required}{}",
            if errored || aborted {
                " (restarting)"
            } else {
                ""
            }
        ),
        SyncStatus::TxHashsetDownload(stats) => format!(
            "State download: {} / {} bytes",
            stats.downloaded_size, stats.total_size
        ),
        SyncStatus::TxHashsetSetup { .. } => "Preparing state validation".into(),
        SyncStatus::TxHashsetKernelsValidation {
            kernels,
            kernels_total,
        } => format!("Validating kernels: {kernels} / {kernels_total}"),
        SyncStatus::TxHashsetRangeProofsValidation {
            rproofs,
            rproofs_total,
        } => format!("Validating range proofs: {rproofs} / {rproofs_total}"),
        SyncStatus::TxHashsetSave => "Saving validated state".into(),
        SyncStatus::TxHashsetDone => "State synchronized".into(),
        SyncStatus::BodySync {
            current_height,
            highest_height,
        } => format!("Blocks: {current_height} / {highest_height}"),
        SyncStatus::Shutdown => "Stopping Grin".into(),
    }
}

fn pool_lines(config: &Config, snapshot: &Snapshot) -> Vec<Line<'static>> {
    let mut lines = Vec::new();
    if let Ok(node) = &snapshot.node {
        lines.push(field("Pool", &node.pool_name));
    }
    lines.extend([
        field("Network", format!("{:?}", config.network)),
        field("Pool P2P", config.peer),
        field("Stratum", config.stratum),
        Line::raw(""),
    ]);
    match &snapshot.job {
        Ok(job) => {
            lines.push(field("Mining", "Work available"));
            for (label, key) in [
                ("Mining height", "height"),
                ("Job", "job_id"),
                ("Miner difficulty", "difficulty"),
                ("Pool difficulty", "pool_difficulty"),
            ] {
                if let Some(value) = job.get(key) {
                    lines.push(field(label, value));
                }
            }
        }
        Err(error) => {
            lines.push(field("Mining", "Waiting for work"));
            lines.push(field("Reason", error));
        }
    }
    if let Ok(node) = &snapshot.node {
        lines.push(Line::raw(""));
        lines.push(field(
            "Pool peers",
            if node.connected_peers == 0 {
                "0 (isolated)".into()
            } else {
                node.connected_peers.to_string()
            },
        ));
        lines.push(field(
            "Sharechain",
            if node.pool_recovering {
                "Recovering"
            } else if node.pool_tip.is_some() {
                "Verified tip"
            } else {
                "New branch"
            },
        ));
        lines.push(field(
            "Selected tip",
            node.pool_tip.as_deref().unwrap_or("None"),
        ));
        lines.push(field("Pool id", &node.pool_id));
        lines.push(Line::raw(""));
        lines.push(field(
            "Database usage",
            format!(
                "{:.2} / {:.0} MiB",
                node.database_bytes as f64 / 1048576.0,
                node.database_limit_bytes as f64 / 1048576.0
            ),
        ));
    }
    lines
}

fn wallet_lines(snapshot: &Snapshot) -> Vec<Line<'static>> {
    let wallet = match &snapshot.wallet {
        Ok(wallet) => wallet,
        Err(error) => return vec![field("Wallet unavailable", error)],
    };
    let mut lines = vec![
        field("State", &wallet.state),
        field(
            "Verified height",
            if wallet.native_head.is_empty() {
                "Not yet available".into()
            } else {
                wallet.native_height.to_string()
            },
        ),
        field("PMMR position", wallet.scanned_pmmr_position),
    ];
    if let Some(balance) = &wallet.balance {
        lines.push(Line::raw(""));
        for (name, value) in [
            ("Spendable", balance.amount_currently_spendable),
            ("Total", balance.total),
            ("Immature", balance.amount_immature),
            ("Locked", balance.amount_locked),
            (
                "Awaiting finalization",
                balance.amount_awaiting_finalization,
            ),
            (
                "Awaiting confirmation",
                balance.amount_awaiting_confirmation,
            ),
            ("Reverted", balance.amount_reverted),
        ] {
            lines.push(field(name, amount(value)));
        }
        lines.push(field("Tracked outputs", wallet.output_count));
    } else {
        lines.push(field("Balance", "Not yet available"));
    }
    if let Some(error) = &wallet.error {
        lines.push(field("Attention", error));
    }
    lines.push(Line::raw(""));
    lines.push(field("Account", &wallet.account));
    lines.push(field(
        "Snapshot age",
        format!(
            "{}s",
            chrono::Utc::now()
                .timestamp()
                .saturating_sub(wallet.updated_at)
                .max(0)
        ),
    ));
    lines
}

impl Screen {
    pub(super) fn draw(
        &mut self,
        frame: &mut Frame<'_>,
        config: &Config,
        reporting: &Reporting,
        stopping: bool,
    ) {
        let snapshot = self.snapshot.as_ref();
        let tab = self.tab;
        let scroll = &mut self.scroll;
        let conditions = reporting.conditions.lock().clone();
        let mut attention: Vec<Line<'static>> = conditions
            .iter()
            .map(|(component, condition)| {
                Line::raw(format!(
                    "{component}: {} ({}s)",
                    condition.message,
                    condition.since.elapsed().as_secs()
                ))
            })
            .collect();
        if let Some(snapshot) = snapshot {
            if let Err(error) = &snapshot.node {
                attention.push(field("Node status", error));
            }
            match &snapshot.wallet {
                Ok(wallet) => {
                    if let Some(error) = &wallet.error {
                        attention.push(field("Wallet", error));
                    }
                }
                Err(error) => attention.push(field("Wallet", error)),
            }
            if let Ok(node) = &snapshot.node {
                match node.free_disk_bytes {
                    Some(bytes) if bytes < status::runtime::RESERVE => {
                        attention.push(Line::raw("Disk space is below the free-space reserve"))
                    }
                    None => attention.push(Line::raw("Free disk space is unavailable")),
                    _ => {}
                }
            }
        }
        let tabs_width = TABS
            .iter()
            .map(|title| Line::from(*title).width())
            .sum::<usize>()
            + 3 * TABS.len()
            + 1;
        let [top, mut body, attention_area, footer] = Layout::vertical([
            Constraint::Length(3),
            Constraint::Min(0),
            Constraint::Length(if attention.is_empty() {
                0
            } else {
                attention.len() as u16 + 2
            }),
            Constraint::Length(1),
        ])
        .areas(frame.area());
        let [header, statuses] = Layout::horizontal([Constraint::Min(0), Constraint::Length(41)])
            .spacing(2)
            .areas(top);
        if usize::from(header.width) >= tabs_width && body.height > 0 {
            body = body.union(Rect {
                x: header.x,
                y: top.bottom().saturating_sub(1),
                width: tabs_width as u16,
                height: 1,
            });
        }
        let [mining_area, wallet_area] =
            Layout::horizontal([Constraint::Length(20), Constraint::Length(20)])
                .spacing(1)
                .areas(statuses);
        frame.render_widget(
            Paragraph::new(Line::styled(
                format!("G2Pool {} | {:?}", crate::VERSION, config.network),
                Style::default().add_modifier(Modifier::BOLD),
            ))
            .wrap(Wrap { trim: true }),
            header,
        );
        let (mining, wallet) = if stopping {
            ("stopping", "stopping")
        } else if let Some(snapshot) = snapshot {
            (
                if snapshot.job.is_ok() {
                    "work available"
                } else {
                    "waiting"
                },
                snapshot
                    .wallet
                    .as_ref()
                    .map(|wallet| wallet.state.as_str())
                    .unwrap_or("unavailable"),
            )
        } else {
            ("starting", "starting")
        };
        for (title, value, area) in [
            ("Mining", mining, mining_area),
            ("Wallet", wallet, wallet_area),
        ] {
            frame.render_widget(
                Paragraph::new(value).block(
                    Block::bordered()
                        .title(title)
                        .padding(Padding::horizontal(1)),
                ),
                area,
            );
        }
        let lines = if tab == 3 {
            reporting
                .events
                .lock()
                .iter()
                .rev()
                .map(|event| {
                    let color = match event.level {
                        log::Level::Error => Color::Red,
                        log::Level::Warn => Color::Yellow,
                        _ => Color::Reset,
                    };
                    Line::styled(
                        format!(
                            "{} {:5} {}: {}",
                            event.time.format("%H:%M:%S"),
                            event.level,
                            event.source,
                            event.message
                        ),
                        Style::default().fg(color),
                    )
                })
                .collect()
        } else if let Some(snapshot) = snapshot {
            match tab {
                0 => pool_lines(config, snapshot),
                1 => {
                    let mut lines = vec![
                        field("Network", format!("{:?}", config.network)),
                        field(
                            "Grin P2P",
                            std::net::SocketAddr::new(config.grin_host, config.grin_port),
                        ),
                        field("Synchronization", sync_label(snapshot.sync)),
                    ];
                    if let Ok(node) = &snapshot.node {
                        lines.extend([
                            field("Block height", node.native_height),
                            field("Block hash", &node.native_head),
                            field("Output PMMR size", node.native_pmmr_size),
                        ]);
                    }
                    lines
                }
                _ => wallet_lines(snapshot),
            }
        } else {
            vec![Line::raw(*reporting.phase.read())]
        };
        let tabs_area = Rect {
            width: (tabs_width as u16).min(body.width),
            height: body.height.min(3),
            ..body
        };
        let [_, body] = Layout::vertical([Constraint::Length(2), Constraint::Min(0)]).areas(body);
        let content = Block::bordered();
        let inner = content.inner(body);
        frame.render_widget(content, body);
        let details = if tab == 2 {
            let [details, withdrawal] =
                Layout::horizontal([Constraint::Percentage(48), Constraint::Percentage(52)])
                    .spacing(1)
                    .areas(inner);
            let pending = snapshot
                .and_then(|snapshot| snapshot.wallet.as_ref().ok())
                .and_then(|wallet| wallet.withdrawal.as_ref());
            self.withdrawal.draw(frame, withdrawal, pending);
            details
        } else {
            inner
        };
        scroll.0 = scroll.0.min(
            lines
                .len()
                .saturating_sub(details.height as usize)
                .min(u16::MAX as usize) as u16,
        );
        scroll.1 = scroll.1.min(
            lines
                .iter()
                .map(Line::width)
                .max()
                .unwrap_or(0)
                .saturating_sub(details.width as usize)
                .min(u16::MAX as usize) as u16,
        );
        frame.render_widget(Paragraph::new(lines).scroll(*scroll), details);
        frame.render_widget(
            Tabs::new(TABS.to_vec())
                .select(tab)
                .divider("│")
                .highlight_style(
                    Style::default()
                        .fg(Color::Cyan)
                        .add_modifier(Modifier::BOLD),
                )
                .block(Block::bordered().border_set(ratatui::symbols::border::Set {
                    bottom_left: "├",
                    bottom_right: if tabs_area.width == body.width {
                        "┤"
                    } else {
                        "┴"
                    },
                    ..ratatui::symbols::border::PLAIN
                })),
            tabs_area,
        );
        if !attention.is_empty() {
            frame.render_widget(
                Paragraph::new(attention)
                    .style(Style::default().fg(Color::Yellow))
                    .block(Block::bordered().title("Current conditions")),
                attention_area,
            );
        }
        frame.render_widget(
            "q/Ctrl+C: stop | Tab/1-4: view | Arrows: scroll | Home: reset",
            footer,
        );
    }
}
