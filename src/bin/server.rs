use anyhow::Context;
use nano_valkey::{CommandVariant, Connection, Db, Shutdown, DEFAULT_HOST, DEFAULT_PORT};
use std::{collections::HashMap, sync::Arc};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::RwLock,
};
use tracing::info;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();
    let addr = format!("{DEFAULT_HOST}:{DEFAULT_PORT}");

    let listener = TcpListener::bind(&addr)
        .await
        .context("failed to bind tcp listener")?;
    info!("Listening on {addr}");

    let db = Arc::new(RwLock::new(HashMap::new()));

    loop {
        let (socket, _) = listener
            .accept()
            .await
            .context("failed to accept connection")?;

        let db = db.clone();

        tokio::spawn(async move { process(socket, db).await });
    }
}

#[tracing::instrument(skip_all)]
async fn process(socket: TcpStream, db: Db) {
    let mut connection = Connection::new(socket);

    while let Some(frame) = connection.read_frame().await.unwrap() {
        CommandVariant::from_frame(frame)
            .expect("frame was not a valid command")
            .apply(&db, &mut connection, &mut Shutdown {})
            .await
            .expect("command failed to be applied");
    }
}
