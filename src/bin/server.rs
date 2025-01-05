use anyhow::Context;
use nano_valkey::{server, DEFAULT_HOST, DEFAULT_PORT};
use tokio::net::TcpListener;
use tracing::info;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();
    let addr = format!("{DEFAULT_HOST}:{DEFAULT_PORT}");

    let listener = TcpListener::bind(&addr)
        .await
        .context("failed to bind tcp listener")?;
    info!("Listening on {addr}");

    server::run(listener, tokio::signal::ctrl_c()).await;

    Ok(())
}
