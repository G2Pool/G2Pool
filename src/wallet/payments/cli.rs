use crate::wallet::payments::parse_amount;
use anyhow::{bail, Context, Result};
use crossterm::{
    event::{
        self, DisableBracketedPaste, EnableBracketedPaste, Event, KeyCode, KeyEventKind,
        KeyModifiers,
    },
    execute,
    terminal::{disable_raw_mode, enable_raw_mode},
};
use std::{
    collections::VecDeque,
    io::{self, IsTerminal, Write},
    time::Duration,
};
pub fn run(data: &std::path::Path) -> Result<String> {
    if !crate::control::socket_path(data).is_file() {
        bail!("G2Pool daemon control socket unavailable; start the daemon first");
    }
    let cancelled = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let signal = cancelled.clone();
    ctrlc::set_handler(move || signal.store(true, std::sync::atomic::Ordering::Release))?;
    let mut input = Input::default();
    let mut value = call(
        data,
        crate::control::Command::Withdraw { amount: None },
        &cancelled,
    )?;
    if value["state"] == "completed" {
        return Ok("Withdrawal submitted to Grin. G2Pool will retry it until confirmed.".into());
    }
    if value["state"] == "idle" {
        let amount = loop {
            prompt("Amount to withdraw (GRIN, plus transaction fee; Ctrl+C to cancel): ")?;
            let Some(value) = input.line(&cancelled)? else {
                return Ok("Withdrawal cancelled.".into());
            };
            match parse_amount(value.trim()) {
                Ok(amount) => break amount,
                Err(error) => println!("{error}"),
            }
        };
        value = call(
            data,
            crate::control::Command::Withdraw {
                amount: Some(amount),
            },
            &cancelled,
        )?;
    }
    let slatepack = value["slatepack"]
        .as_str()
        .context("daemon returned an invalid Slatepack offer")?;
    println!("\nCopy this Slatepack into your receiving wallet:\n{slatepack}\n");
    println!("Paste the reply Slatepack, ending with ENDSLATEPACK. (Ctrl+C to cancel):");
    let mut response = String::new();
    loop {
        let Some(value) = input.line(&cancelled)? else {
            let result = response_value(crate::control::call(
                data,
                crate::control::Command::CancelWithdrawal,
            )?)?;
            return Ok(if result["state"] == "cancelled" {
                "Withdrawal cancelled."
            } else {
                "No pending unsigned withdrawal to cancel."
            }
            .into());
        };
        response.push_str(value.trim());
        response.push('\n');
        if response.len() > 2_000_000 {
            bail!("reply Slatepack is too large");
        }
        if value.trim().ends_with("ENDSLATEPACK.") {
            break;
        }
    }
    call(
        data,
        crate::control::Command::WithdrawalReply {
            slatepack: response,
        },
        &cancelled,
    )?;
    Ok("Withdrawal submitted to Grin. G2Pool will retry it until confirmed.".into())
}
fn response_value(response: crate::control::Response) -> Result<serde_json::Value> {
    match response.result {
        crate::control::ResponseResult::Ok { value } => Ok(value),
        crate::control::ResponseResult::Err { message, .. } => bail!(message),
    }
}
fn call(
    data: &std::path::Path,
    command: crate::control::Command,
    cancelled: &std::sync::atomic::AtomicBool,
) -> Result<serde_json::Value> {
    std::thread::scope(|scope| {
        let (send, receive) = std::sync::mpsc::sync_channel(1);
        scope.spawn(move || {
            let _ = send.send(crate::control::call(data, command));
        });
        let mut cancellation_requested = false;
        loop {
            if cancelled.load(std::sync::atomic::Ordering::Acquire) && !cancellation_requested {
                let result = response_value(crate::control::call(
                    data,
                    crate::control::Command::CancelWithdrawal,
                )?)?;
                if result["state"] == "cancelled" {
                    bail!("Withdrawal cancelled.");
                }
                cancellation_requested = true;
            }
            match receive.recv_timeout(Duration::from_millis(100)) {
                Ok(response) => return response_value(response?),
                Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
                    bail!("withdrawal connection stopped")
                }
                Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {}
            }
        }
    })
}

fn prompt(text: &str) -> Result<()> {
    print!("{text}");
    std::io::stdout().flush()?;
    Ok(())
}

#[derive(Default)]
struct Input {
    pending: VecDeque<char>,
}
impl Input {
    fn line(&mut self, cancelled: &std::sync::atomic::AtomicBool) -> Result<Option<String>> {
        if !io::stdin().is_terminal() {
            let mut line = String::new();
            let count = io::stdin().read_line(&mut line)?;
            if count == 0 {
                return Ok(None);
            }
            return Ok(Some(line.trim_end_matches(['\r', '\n']).to_owned()));
        }
        enable_raw_mode()?;
        let mut stdout = io::stdout();
        if let Err(error) = execute!(stdout, EnableBracketedPaste) {
            let _ = disable_raw_mode();
            return Err(error.into());
        }
        let result = self.line_terminal(cancelled, &mut stdout);
        let _ = execute!(stdout, DisableBracketedPaste);
        let _ = disable_raw_mode();
        result
    }
    fn line_terminal(
        &mut self,
        cancelled: &std::sync::atomic::AtomicBool,
        stdout: &mut impl Write,
    ) -> Result<Option<String>> {
        use std::sync::atomic::Ordering;
        let mut line = String::new();
        loop {
            if cancelled.load(Ordering::Acquire) {
                return Ok(None);
            }
            while let Some(character) = self.pending.pop_front() {
                if character == '\n' {
                    stdout.write_all(b"\r\n")?;
                    stdout.flush()?;
                    return Ok(Some(line));
                }
                if character == '\r' {
                    continue;
                }
                line.push(character);
                if line.len() > 2_000_000 {
                    bail!("input is too large");
                }
                write!(stdout, "{character}")?;
            }
            stdout.flush()?;
            if !event::poll(std::time::Duration::from_millis(100))? {
                continue;
            }
            match event::read()? {
                Event::Paste(text) => {
                    self.pending.extend(text.chars());
                }
                Event::Key(key) if key.kind == KeyEventKind::Press => match key.code {
                    KeyCode::Enter => {
                        stdout.write_all(b"\r\n")?;
                        stdout.flush()?;
                        return Ok(Some(line));
                    }
                    KeyCode::Backspace if line.pop().is_some() => {
                        stdout.write_all(b"\x08 \x08")?;
                        stdout.flush()?;
                    }
                    KeyCode::Esc => return Ok(None),
                    KeyCode::Char('c') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                        return Ok(None)
                    }
                    KeyCode::Char('d')
                        if key.modifiers.contains(KeyModifiers::CONTROL) && line.is_empty() =>
                    {
                        return Ok(None)
                    }
                    KeyCode::Char(character) => {
                        line.push(character);
                        write!(stdout, "{character}")?;
                        stdout.flush()?;
                    }
                    _ => {}
                },
                _ => {}
            }
        }
    }
}
