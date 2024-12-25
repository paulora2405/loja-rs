use anyhow::Context;
use bytes::Bytes;
use log::info;
use mini_redis::{Connection, Frame};
use std::{
    collections::HashMap,
    io::Read,
    sync::{Arc, Mutex},
};
use tokio::net::{TcpListener, TcpStream};

type Db = Arc<Mutex<HashMap<String, Bytes>>>;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::builder()
        .filter_level(log::LevelFilter::Info)
        .init();

    let listener = TcpListener::bind("127.0.0.1:6379")
        .await
        .context("failed to bind tcp listener")?;
    info!("Listening on port 6379");

    let db = Arc::new(Mutex::new(HashMap::new()));

    loop {
        let (socket, _) = listener
            .accept()
            .await
            .context("failed to accept connection")?;

        let db = db.clone();

        tokio::spawn(async move { process(socket, db).await });
    }
}

async fn process(socket: TcpStream, db: Db) {
    use mini_redis::Command::{self, Get, Set};

    let mut connection = Connection::new(socket);

    while let Some(frame) = connection.read_frame().await.unwrap() {
        let response = match Command::from_frame(frame).unwrap() {
            Set(cmd) => {
                info!(
                    "SET {:?} = {:?}",
                    cmd.key(),
                    String::from_utf8_lossy(cmd.value())
                );
                let mut db = db.lock().unwrap();
                db.insert(cmd.key().to_string(), cmd.value().clone());
                Frame::Simple("OK".to_string())
            }
            Get(cmd) => {
                info!("GET {:?}", cmd.key());
                let db = db.lock().unwrap();
                if let Some(value) = db.get(cmd.key()) {
                    Frame::Bulk(value.clone())
                } else {
                    Frame::Null
                }
            }
            cmd => unimplemented!("command {cmd:?}"),
        };

        connection.write_frame(&response).await.unwrap();
    }
}
