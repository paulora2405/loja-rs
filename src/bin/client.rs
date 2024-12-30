use bytes::Bytes;
use mini_redis::client;
use tokio::sync::{mpsc, oneshot};
use tracing::info;

type Responder<T> = oneshot::Sender<mini_redis::Result<T>>;
#[derive(Debug)]
enum Command {
    Get {
        key: String,
        res: Responder<Option<Bytes>>,
    },
    Set {
        key: String,
        val: Bytes,
        res: Responder<()>,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();
    info!("Starting client");

    let (tx, mut rx) = mpsc::channel(32);
    let tx2 = tx.clone();

    let t1 = tokio::spawn(async move {
        let (res_tx, res_rx) = oneshot::channel();

        let cmd = Command::Get {
            key: "foo".to_string(),
            res: res_tx,
        };

        tx.send(cmd).await.unwrap();

        let res = res_rx.await.unwrap();
        info!("GET = {:?}", res);
    });

    let t2 = tokio::spawn(async move {
        let (res_tx, res_rx) = oneshot::channel();

        let cmd = Command::Set {
            key: "foo".to_string(),
            val: "bar".into(),
            res: res_tx,
        };

        tx2.send(cmd).await.unwrap();

        let res = res_rx.await.unwrap();
        info!("SET = {:?}", res);
    });

    let manager = tokio::spawn(async move {
        let mut client = client::connect("127.0.0.1:6379").await.unwrap();

        use Command as C;
        while let Some(cmd) = rx.recv().await {
            match cmd {
                C::Get { key, res } => {
                    info!("GET key = {}", key);
                    let client_res = client.get(&key).await;
                    let _ = res.send(client_res);
                }
                C::Set { key, val, res } => {
                    let client_res = client.set(&key, val).await;
                    let _ = res.send(client_res);
                }
            }
        }
    });

    t1.await?;
    t2.await?;
    manager.await?;

    Ok(())
}
