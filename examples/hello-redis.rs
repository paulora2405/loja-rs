use loja::Client;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let mut client = Client::connect("127.0.0.1:6379").await?;

    client.set("hello", "world".into()).await?;

    let result = client.get("hello").await?;

    println!("got this from server: result={result:?}");

    Ok(())
}
