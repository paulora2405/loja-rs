use anyhow::Context;
use clap::{command, Parser};
use loja::{server, DEFAULT_HOST, DEFAULT_PORT};
use tokio::net::TcpListener;
use tracing::info;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    setup_logging();

    let cli = LojaServerCli::parse();
    let addr = std::net::SocketAddr::new(cli.host, cli.port);

    let listener = TcpListener::bind(&addr)
        .await
        .context("failed to bind tcp listener")?;
    info!("listening on {addr}");

    server::run(listener, tokio::signal::ctrl_c()).await;

    Ok(())
}

#[derive(Debug, Parser)]
#[command(name = "loja-server", version, author, disable_help_flag(true))]
/// A simple Redis cli client
struct LojaServerCli {
    #[clap(long, action = clap::ArgAction::HelpLong)]
    /// Display cli help.
    help: Option<bool>,
    #[arg(short, long, default_value = DEFAULT_HOST)]
    /// Host to bind to.
    host: std::net::IpAddr,
    #[arg(short, long, default_value_t = DEFAULT_PORT)]
    /// Port to bind to.
    port: u16,
}

fn setup_logging() {
    tracing_subscriber::fmt::fmt()
        .with_thread_ids(true)
        .compact()
        .with_env_filter(EnvFilter::from_default_env())
        .init();
}
