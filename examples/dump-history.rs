use utxorpc::{spec::sync::BlockRef, CardanoSyncClient, ClientBuilder, Error};

#[tokio::main]
async fn main() -> Result<(), Error> {
    let mut client = ClientBuilder::new()
        .uri("http://localhost:50051")?
        .build::<CardanoSyncClient>()
        .await;

    let mut start = Some(BlockRef {
        index: 52254715,
        hash: hex::decode("03a5cfb33fd6ffcd7a6b237ba7a612cb8c348fbc37a4ca442e42369fc4c4eb67")
            .unwrap()
            .into(),
    });

    loop {
        let page = client.dump_history(start, 20).await?;

        for block in page.items {
            println!("block: {}", block.parsed.unwrap().header.unwrap().slot);
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
