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
    path::PathBuf,
};
pub const HELP: &str = "g2pool withdraw [--network NETWORK] [--data-dir PATH]
  Show balances and ask how much GRIN to withdraw.
  Copy the Slatepack into the receiving wallet, then paste its reply here.
  Ctrl+C or end of input cancels an unsigned offer. Mining continues.
  Run this command again to retry a withdrawal interrupted after signing.";

pub fn run(arguments: impl IntoIterator<Item = String>) -> Result<String> {
    let arguments: Vec<String> = arguments.into_iter().collect();
    if arguments.iter().any(|a| a == "--help" || a == "-h") {
        return Ok(HELP.to_owned());
    }
    let mut args = arguments.into_iter();
    let mut base = None;
    let mut network = "mainnet".to_owned();
    let mut network_seen = false;
    while let Some(flag) = args.next() {
        if flag == "--data-dir" && base.is_none() {
            let value = args
                .next()
                .filter(|value| !value.starts_with('-'))
                .context("missing data directory")?;
            base = Some(PathBuf::from(value));
        } else if flag == "--network" && !network_seen {
            network_seen = true;
            network = args
                .next()
                .filter(|value| matches!(value.as_str(), "mainnet" | "testnet"))
                .context("invalid network")?;
        } else {
            bail!("unknown or repeated option; use g2pool withdraw --help");
        }
    }
    let data = crate::network_data_dir(base.unwrap_or_else(crate::default_data_dir), &network);
    if !crate::control::socket_path(&data).is_file() {
        bail!("G2Pool daemon control socket unavailable; start the daemon first");
    }
    run_daemon_withdrawal(&data)
}

fn run_daemon_withdrawal(data: &std::path::Path) -> Result<String> {
    let cancelled = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let signal = cancelled.clone();
    ctrlc::set_handler(move || signal.store(true, std::sync::atomic::Ordering::Release))?;
    let mut input = Input::default();
    let amount = loop {
        prompt("Amount to withdraw (GRIN, plus transaction fee; Ctrl+C to cancel): ")?;
        let Some(value) = input.line(&cancelled)? else {
            let _ = crate::control::call(data, crate::control::Command::CancelWithdrawal);
            return Ok("Withdrawal cancelled.".into());
        };
        match parse_amount(value.trim()) {
            Ok(amount) => break amount,
            Err(error) => println!("{error}"),
        }
    };
    let offer = crate::control::call(
        data,
        crate::control::Command::Withdraw {
            amount: Some(amount),
        },
    )?;
    let slatepack = match offer.result {
        crate::control::ResponseResult::Ok { value } => value["slatepack"]
            .as_str()
            .context("daemon returned an invalid Slatepack offer")?
            .to_owned(),
        crate::control::ResponseResult::Err { message, .. } => bail!(message),
    };
    println!("\nCopy this Slatepack into your receiving wallet:\n{slatepack}\n");
    println!("Paste the reply Slatepack, ending with ENDSLATEPACK. (Ctrl+C to cancel):");
    let mut response = String::new();
    loop {
        let Some(value) = input.line(&cancelled)? else {
            let _ = crate::control::call(data, crate::control::Command::CancelWithdrawal);
            return Ok("Withdrawal cancelled.".into());
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
    let result = crate::control::call(
        data,
        crate::control::Command::WithdrawalReply {
            slatepack: response,
        },
    )?;
    match result.result {
        crate::control::ResponseResult::Ok { .. } => {
            Ok("Withdrawal completed and submitted to the Grin node.".into())
        }
        crate::control::ResponseResult::Err { message, .. } => bail!(message),
    }
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
