use bytes::Bytes;
use clap::{Parser, Subcommand, ValueEnum};
use loja::{Client, DEFAULT_HOST, DEFAULT_PORT};
use std::{
    io::{BufRead, IsTerminal, Write},
    time::Duration,
};
use tokio::net::TcpStream;
use tracing::debug;

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let cli = LojaCli::parse();
    let addr = std::net::SocketAddr::new(cli.host, cli.port);
    let mut client = Client::connect(&addr).await?;

    match cli.subcommand {
        Some(subcommand) => cli_mode(&mut client, subcommand).await?,
        None => interactive_mode(client).await?,
    }

    Ok(())
}

async fn interactive_mode(mut client: Client<TcpStream>) -> anyhow::Result<()> {
    let stdin = std::io::stdin();
    let is_terminal = stdin.is_terminal();
    print_prompt(is_terminal);

    for line in stdin.lock().lines() {
        let line = line?;

        let mut args = vec![""];
        args.extend(line.split_whitespace());
        let cli = LojaCli::try_parse_from(args);
        if let Ok(cli) = cli {
            debug!(?cli);
            if let Some(subcommand) = cli.subcommand {
                cli_mode(&mut client, subcommand).await?;
            }
        } else {
            let error = cli.unwrap_err().render();
            let styled_str = error.to_string();
            let render = styled_str.lines().next().unwrap();
            eprintln!("{render}");
        }
        print_prompt(is_terminal);
    }

    Ok(())
}

fn print_prompt(is_terminal: bool) {
    if is_terminal {
        print!("loja> ");
        std::io::stdout().flush().unwrap();
    }
}

async fn cli_mode(
    client: &mut Client<TcpStream>,
    subcommand: LojaSubcommand,
) -> anyhow::Result<()> {
    match subcommand {
        LojaSubcommand::Ping { msg } => {
            let response = client.ping(msg.map(|s| s.into())).await?;
            println!("{}", String::from_utf8_lossy(response.as_ref()));
        }
        LojaSubcommand::Get { key } => {
            let response = client.get(&key).await?;
            if let Some(value) = response {
                println!("{}", String::from_utf8_lossy(value.as_ref()));
            } else {
                println!("(nil)");
            }
        }
        LojaSubcommand::Set {
            key,
            value,
            expire_unit,
            expires,
        } => {
            let duration = to_duration(expire_unit, expires);
            if let Some(duration) = duration {
                client
                    .set_expires(&key, Bytes::from(value), duration)
                    .await?;
            } else {
                client.set(&key, Bytes::from(value)).await?;
            }
            println!("OK");
        }
    };

    Ok(())
}

#[derive(Debug, Parser)]
#[command(name = "loja-cli", version, author, disable_help_flag(true))]
/// A simple Redis cli client
///
/// When providing subcommands, the client will execute the command and exit.
/// If no subcommands are provided, the client will start in interactive mode.
struct LojaCli {
    #[clap(long, action = clap::ArgAction::HelpLong)]
    /// Display cli help.
    help: Option<bool>,
    #[clap(subcommand)]
    subcommand: Option<LojaSubcommand>,
    #[arg(short, long, default_value = DEFAULT_HOST)]
    host: std::net::IpAddr,
    #[arg(short, long, default_value_t = DEFAULT_PORT)]
    port: u16,
}

#[derive(Debug, Subcommand)]
/// Subcommand to execute in one-shot command mode.
enum LojaSubcommand {
    /// Ping the server.
    Ping {
        /// Message to ping
        msg: Option<String>,
    },
    /// Get the value of key.
    Get {
        /// Name of key to get.
        key: String,
    },
    /// Set key to hold the string value.
    Set {
        /// Name of the key to set.
        key: String,
        /// Value to set.
        value: String,
        /// Expiration unit, can be either `ex` or `px`.
        #[arg(value_enum, requires = "expires")]
        expire_unit: Option<ExpirationUnit>,
        /// Expire the value after the specified amount of time.
        #[arg(requires = "expire_unit")]
        expires: Option<u64>,
    },
}

#[derive(Debug, Clone, ValueEnum)]
enum ExpirationUnit {
    EX,
    PX,
}

fn to_duration(unit: Option<ExpirationUnit>, expires: Option<u64>) -> Option<Duration> {
    match (unit, expires) {
        (Some(ExpirationUnit::EX), Some(expires)) => Some(Duration::from_secs(expires)),
        (Some(ExpirationUnit::PX), Some(expires)) => Some(Duration::from_millis(expires)),
        _ => None,
    }
}
