use utxorpc::{CardanoSyncClient, ClientBuilder, Error, TipEvent};

#[tokio::main]
async fn main() -> Result<(), Error> {
    let mut client = ClientBuilder::new()
        .uri("http://localhost:50051")?
        .build::<CardanoSyncClient>()
        .await;

    let mut tip = client.follow_tip(vec![]).await.unwrap();

    while let Ok(Some(event)) = tip.event().await {
        match event {
            TipEvent::Apply(block) => {
                println!("apply: {}", block.parsed.unwrap().header.unwrap().slot)
            }
            TipEvent::Undo(block) => {
                println!("undo: {}", block.parsed.unwrap().header.unwrap().slot)
            }
            TipEvent::Reset(point) => println!("reset: {}", point.slot),
        }
    }

    Ok(())
}
