use anyhow::Context;
use nano_valkey::{cmd::Command, CommandVariant, Connection, Db, DEFAULT_HOST, DEFAULT_PORT};
use std::{collections::HashMap, sync::Arc};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::RwLock,
};
use tracing::{debug, debug_span, info};

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

#[tracing::instrument(skip(db))]
async fn process(socket: TcpStream, db: Db) {
    let mut connection = Connection::new(socket);

    while let Some(frame) = connection.read_frame().await.unwrap() {
        use CommandVariant as C;
        let response = match CommandVariant::from_frame(frame).expect("frame to valid command") {
            C::Set => {
                // let span = debug_span!("set");
                // let _enter = span.enter();
                // debug!("{:?}:{:?}", cmd.key(), cmd.value());
                // {
                //     let mut db = db.write().await;
                //     db.insert(cmd.key().to_string(), cmd.value().clone());
                // }
                // Frame::SimpleString("OK".to_string())
                todo!()
            }
            C::Get => {
                // let span = debug_span!("get");
                // let _enter = span.enter();
                // let value = {
                //     let db = db.read().await;
                //     db.get(cmd.key()).cloned()
                // };
                // debug!("{:?}:{value:?}", cmd.key());
                // if let Some(value) = value {
                //     Frame::BulkString(value)
                // } else {
                //     Frame::Null
                // }
                todo!()
            }
            C::Ping(cmd) => {
                let span = debug_span!("ping");
                let _enter = span.enter();
                debug!(?cmd);
                cmd.into_frame().expect("ping command to frame")
            }
        };

        connection.write_frame(&response).await.unwrap();
    }
}
