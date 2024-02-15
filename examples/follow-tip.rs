use utxorpc::{CardanoSyncClient, ClientBuilder, Error, TipEvent};

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

    let mut tip = client.follow_tip(vec![]).await.unwrap();

    while let Ok(event) = tip.event().await {
        match event {
            TipEvent::Apply(block) => println!("apply: {}", block.header.unwrap().slot),
            TipEvent::Undo(block) => println!("undo: {}", block.header.unwrap().slot),
            TipEvent::Reset(point) => println!("reset: {}", point.index),
        }
    }

    Ok(())
}
