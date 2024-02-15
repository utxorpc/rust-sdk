use utxorpc::{CardanoSyncClient, ClientBuilder, Error};

#[tokio::main]
async fn main() -> Result<(), Error> {
    let mut client = ClientBuilder::new()
        .uri("https://preview.utxorpc-v0.demeter.run")?
        .metadata(
            "dmtr-api-key",
            "dmtr_utxorpc10zrj5dglh53dn8lhgk4p2lffuuu7064j",
        )?
        .build::<CardanoSyncClient>()
        .await;

    let mut start = None;

    loop {
        let page = client.dump_history(start, 20).await?;

        for block in page.items {
            println!("block: {}", block.header.unwrap().slot);
        }

        match page.next {
            Some(next) => {
                start = Some(next);
            }
            None => break,
        }
    }

    Ok(())
}
