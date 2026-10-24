use crate::{control, daemon, wallet};
use clap::{Arg, ArgMatches, Command};
use std::{net::SocketAddr, path::PathBuf};

pub(crate) fn command() -> Command {
    Command::new("g2pool")
        .about("P2Pool implementation for Grin")
        .arg(Arg::new("network").long("network").global(true).value_parser(["mainnet", "testnet"]).default_value("mainnet"))
        .arg(Arg::new("data-dir").long("data-dir").global(true).value_parser(clap::value_parser!(PathBuf)))
        .arg(Arg::new("pool").long("pool").value_parser(clap::value_parser!(PathBuf)).help("Pool definition file (default: <data-dir>/pool.json)"))
        .arg(Arg::new("stratum").long("stratum").value_parser(clap::value_parser!(SocketAddr)))
        .arg(Arg::new("pool-listen").long("pool-listen").value_parser(clap::value_parser!(SocketAddr)))
        .arg(Arg::new("grin-listen").long("grin-listen").value_parser(clap::value_parser!(SocketAddr)))
        .arg(Arg::new("no-tui").long("no-tui").action(clap::ArgAction::SetTrue).help("Run without the terminal interface"))
        .subcommand(Command::new("status").about("Show daemon and wallet status"))
        .subcommand(Command::new("withdraw").about("Withdraw GRIN through Slatepack")
            .long_about("Withdraw GRIN through Slatepack. Copy the offer into your receiving wallet, then paste its reply. Ctrl+C cancels an unsigned offer. Run again to resume an interrupted withdrawal."))
}
fn base(matches: &ArgMatches) -> PathBuf {
    matches
        .get_one::<PathBuf>("data-dir")
        .cloned()
        .unwrap_or_else(crate::default_data_dir)
}
fn network(matches: &ArgMatches) -> grin_core::global::ChainTypes {
    match matches.get_one::<String>("network").map(String::as_str) {
        Some("testnet") => grin_core::global::ChainTypes::Testnet,
        _ => grin_core::global::ChainTypes::Mainnet,
    }
}
pub(crate) fn config(matches: &ArgMatches) -> crate::Result<daemon::Config> {
    daemon::Config::load(
        base(matches),
        network(matches),
        matches.get_one::<PathBuf>("pool").cloned(),
        matches.get_one::<SocketAddr>("stratum").copied(),
        matches.get_one::<SocketAddr>("pool-listen").copied(),
        matches.get_one::<SocketAddr>("grin-listen").copied(),
    )
}
pub fn run() -> anyhow::Result<()> {
    let matches = command().get_matches();
    if matches.subcommand_name().is_some()
        && (["stratum", "pool-listen", "grin-listen", "pool"]
            .iter()
            .any(|arg| matches.contains_id(arg))
            || matches.get_flag("no-tui"))
    {
        anyhow::bail!("daemon options apply only when starting the daemon");
    }
    let data = crate::network_data_dir(
        base(&matches),
        if network(&matches) == grin_core::global::ChainTypes::Testnet {
            "testnet"
        } else {
            "mainnet"
        },
    );
    match matches.subcommand() {
        Some(("status", _)) => {
            let response = control::call(&data, control::Command::Status)?;
            println!("{}", serde_json::to_string_pretty(&response)?);
            if let control::ResponseResult::Err { message, .. } = response.result {
                anyhow::bail!(message);
            }
        }
        Some(("withdraw", _)) => println!("{}", wallet::payments::cli::run(&data)?),
        None => daemon::run(config(&matches)?, !matches.get_flag("no-tui"))?,
        _ => unreachable!(),
    }
    Ok(())
}

#[cfg(test)]
#[path = "cli_tests.rs"]
mod tests;
