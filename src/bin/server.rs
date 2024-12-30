use anyhow::Context;
use bytes::Bytes;
use mini_redis::Command::{self, Get, Set};
use nano_valkey::{Connection, Frame};
use std::{collections::HashMap, sync::Arc};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::RwLock,
};
use tracing::{debug, debug_span, info};

type Db = Arc<RwLock<HashMap<String, Bytes>>>;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let listener = TcpListener::bind("127.0.0.1:6379")
        .await
        .context("failed to bind tcp listener")?;
    info!("Listening on port 6379");

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

#[tracing::instrument(skip(db))]
async fn process(socket: TcpStream, db: Db) {
    let mut connection = Connection::new(socket);

    while let Some(frame) = connection.read_frame().await.unwrap() {
        let response = match Command::from_frame(frame).unwrap() {
            Set(cmd) => {
                let span = debug_span!("set");
                let _enter = span.enter();
                debug!("{:?}:{:?}", cmd.key(), cmd.value());
                {
                    let mut db = db.write().await;
                    db.insert(cmd.key().to_string(), cmd.value().clone());
                }
                Frame::SimpleString("OK".to_string())
            }
            Get(cmd) => {
                let span = debug_span!("get");
                let _enter = span.enter();
                let value = {
                    let db = db.read().await;
                    db.get(cmd.key()).cloned()
                };
                debug!("{:?}:{value:?}", cmd.key());
                if let Some(value) = value {
                    Frame::BulkString(value)
                } else {
                    Frame::Null
                }
            }
            cmd => unimplemented!("command {cmd:?}"),
        };

        connection.write_frame(&response).await.unwrap();
    }
}
