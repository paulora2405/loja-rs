#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let mut client = loja::Client::connect("127.0.0.1:6379").await?;

    client.set("hello", "world".into()).await?;

    let result = client.get("hello").await?;

    println!("got this shit from server: result={result:?}");

    Ok(())
}
