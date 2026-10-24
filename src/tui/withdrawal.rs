use super::render::{amount, field};
use crate::wallet::payments::{parse_amount, Phase, Withdrawal, WorkerAction, MAX_SLATEPACK_BYTES};
use crossterm::{
    clipboard::CopyToClipboard,
    event::{Event, KeyCode, KeyModifiers},
    execute,
};
use ratatui::{
    layout::{Constraint, Layout, Rect},
    style::{Color, Style},
    text::Line,
    widgets::{Block, Padding, Paragraph, Wrap},
    Frame,
};
use ratatui_textarea::TextArea;

enum Editor {
    Amount(TextArea<'static>),
    Reply(TextArea<'static>),
}

#[derive(Default)]
pub(super) struct Panel {
    editor: Option<Editor>,
    pub busy: bool,
    message: Option<Line<'static>>,
    show_offer: bool,
}

impl Panel {
    pub fn editing(&self) -> bool {
        self.editor.is_some() && !self.busy
    }

    pub fn complete(&mut self, result: anyhow::Result<serde_json::Value>) {
        self.busy = false;
        match result {
            Ok(value) => {
                self.editor = None;
                self.message = match value["state"].as_str() {
                    Some("completed") => Some("Withdrawal submitted to Grin. Check the receiving wallet for confirmation."),
                    Some("cancelled") => Some("Unsigned withdrawal cancelled."),
                    Some("no_pending_withdrawal" | "idle") => Some("No pending withdrawal."),
                    _ => None,
                }.map(Line::raw);
            }
            Err(error) => {
                self.message = Some(Line::styled(
                    error.to_string(),
                    Style::default().fg(Color::Red),
                ))
            }
        }
    }

    pub fn event(&mut self, event: &Event, pending: Option<&Withdrawal>) -> Option<WorkerAction> {
        if self.busy {
            return None;
        }
        let key = match event {
            Event::Key(key) => Some(key),
            _ => None,
        };
        if key.is_some_and(|key| key.code == KeyCode::Esc) {
            self.editor = None;
            self.show_offer = false;
            self.message = None;
            return None;
        }
        if let Some(editor) = &mut self.editor {
            let (input, is_amount) = match editor {
                Editor::Amount(input) => (input, true),
                Editor::Reply(input) => (input, false),
            };
            if key.is_some_and(|key| {
                (is_amount && key.code == KeyCode::Enter)
                    || (!is_amount
                        && key.code == KeyCode::Char('s')
                        && key.modifiers.contains(KeyModifiers::CONTROL))
            }) {
                let value = input.lines().join("\n");
                let action = if is_amount {
                    match parse_amount(value.trim()) {
                        Ok(value) => WorkerAction::Offer(value),
                        Err(error) => {
                            self.message = Some(Line::styled(
                                error.to_string(),
                                Style::default().fg(Color::Red),
                            ));
                            return None;
                        }
                    }
                } else if value.trim().is_empty() {
                    self.message = Some(Line::styled(
                        "Paste the recipient's reply before submitting.",
                        Style::default().fg(Color::Red),
                    ));
                    return None;
                } else {
                    WorkerAction::Finalize(value.trim().to_owned())
                };
                self.message = None;
                return Some(action);
            }
            let length = input.lines().iter().map(String::len).sum::<usize>()
                + input.lines().len().saturating_sub(1);
            let extra = match event {
                Event::Paste(text) => text.len(),
                Event::Key(key) => match key.code {
                    KeyCode::Char(c) => c.len_utf8(),
                    KeyCode::Enter => 1,
                    _ => 0,
                },
                _ => 0,
            };
            if length.saturating_add(extra) > MAX_SLATEPACK_BYTES {
                self.message = Some(Line::styled(
                    "Input exceeds the Slatepack size limit.",
                    Style::default().fg(Color::Red),
                ));
                return None;
            }
            match event {
                Event::Paste(text) => {
                    input.insert_str(text.replace("\r\n", "\n").replace('\r', "\n"));
                }
                Event::Key(key) if key.code != KeyCode::Tab => {
                    input.input(*key);
                }
                _ => {}
            }
            return None;
        }
        match key.map(|key| key.code) {
            Some(KeyCode::Char('w')) if pending.is_none() => {
                let mut input = TextArea::default();
                input.set_block(Block::bordered().title("Amount in GRIN"));
                input.set_placeholder_text("Amount, excluding transaction fee");
                input.set_cursor_line_style(Style::default());
                self.editor = Some(Editor::Amount(input));
                self.message = None;
            }
            Some(KeyCode::Char('p'))
                if pending.is_some_and(|pending| pending.stage == Phase::AwaitingResponse) =>
            {
                let mut input = TextArea::default();
                input.set_block(Block::bordered().title("Recipient reply"));
                input.set_placeholder_text("Paste the reply Slatepack here");
                input.set_cursor_line_style(Style::default());
                self.editor = Some(Editor::Reply(input));
                self.show_offer = false;
                self.message = None;
            }
            Some(KeyCode::Char('c')) if pending.is_some() => {
                self.message = Some(
                    match execute!(
                        std::io::stdout(),
                        CopyToClipboard::to_clipboard_from(&pending.unwrap().outgoing)
                    ) {
                        Ok(()) => Line::raw(
                            "Copy requested. Your terminal must support clipboard access (OSC 52).",
                        ),
                        Err(error) => Line::styled(
                            format!("Could not copy offer: {error}"),
                            Style::default().fg(Color::Red),
                        ),
                    },
                );
            }
            Some(KeyCode::Char('v')) if pending.is_some() => self.show_offer = !self.show_offer,
            Some(KeyCode::Char('r')) if pending.is_some() => return Some(WorkerAction::Resume),
            Some(KeyCode::Char('x'))
                if pending.is_some_and(|pending| {
                    matches!(pending.stage, Phase::Locking | Phase::AwaitingResponse)
                }) =>
            {
                return Some(WorkerAction::Cancel)
            }
            _ => {}
        }
        None
    }

    pub fn draw(&self, frame: &mut Frame<'_>, area: Rect, pending: Option<&Withdrawal>) {
        let block = Block::bordered()
            .title("Withdrawal")
            .padding(Padding::horizontal(1));
        let inner = block.inner(area);
        frame.render_widget(block, area);
        let mut lines = Vec::new();
        if let Some(pending) = pending {
            lines.extend([
                field("Amount", amount(pending.amount)),
                field("Fee", amount(pending.fee)),
                field("Total", amount(pending.amount.saturating_add(pending.fee))),
            ]);
            lines.push(Line::raw(match pending.stage {
                Phase::Locking => "Offer preparation was interrupted. Resume to continue.",
                Phase::AwaitingResponse => {
                    "Copy the offer into your receiving wallet, then paste its reply."
                }
                Phase::Finalizing => "Finalization was interrupted. Resume the saved withdrawal.",
                Phase::ReadyToPost => "Signed payment saved. Automatic rebroadcast is active.",
            }));
        } else {
            lines.push(Line::raw("Send GRIN to another wallet using Slatepack."));
        }
        let feedback = if self.busy {
            vec![Line::styled("Working…", Style::default().fg(Color::Cyan))]
        } else {
            self.message.iter().cloned().collect::<Vec<_>>()
        };
        let help = if self.busy {
            "You can change tabs while the wallet finishes."
        } else {
            match &self.editor {
                Some(Editor::Amount(_)) => "Enter: create offer   Esc: close",
                Some(Editor::Reply(_)) => "Ctrl+S: submit reply   Esc: close",
                None if pending.is_some_and(|pending| pending.stage == Phase::AwaitingResponse) => {
                    "c: copy   v: view   p: paste reply   x: cancel"
                }
                None if pending.is_some_and(|pending| pending.stage == Phase::Locking) => {
                    "r: resume   x: cancel"
                }
                None if pending.is_some() => "c: copy offer   v: view   r: resume",
                None => "w: withdraw",
            }
        };
        let height = |lines: &[Line<'_>]| {
            lines
                .iter()
                .map(|line| line.width().div_ceil(usize::from(inner.width.max(1))))
                .sum::<usize>()
                .min(u16::MAX as usize) as u16
        };
        let [summary, editor, feedback_area, _, help_area] = Layout::vertical([
            Constraint::Length(height(&lines)),
            if matches!(self.editor, Some(Editor::Amount(_))) {
                Constraint::Length(3)
            } else {
                Constraint::Min(0)
            },
            Constraint::Length(height(&feedback)),
            if matches!(self.editor, Some(Editor::Amount(_))) {
                Constraint::Min(0)
            } else {
                Constraint::Length(0)
            },
            Constraint::Length(2),
        ])
        .areas(inner);
        frame.render_widget(Paragraph::new(lines).wrap(Wrap { trim: true }), summary);
        match &self.editor {
            Some(Editor::Amount(input) | Editor::Reply(input)) => {
                frame.render_widget(input, editor)
            }
            None if self.show_offer => {
                if let Some(pending) = pending {
                    frame.render_widget(
                        Paragraph::new(pending.outgoing.as_str()).wrap(Wrap { trim: false }),
                        editor,
                    );
                }
            }
            _ => {}
        }
        frame.render_widget(
            Paragraph::new(feedback).wrap(Wrap { trim: true }),
            feedback_area,
        );
        frame.render_widget(
            Paragraph::new(help)
                .wrap(Wrap { trim: true })
                .style(Style::default().fg(Color::Cyan)),
            help_area,
        );
    }
}
