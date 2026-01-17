mod common;

use utxorpc::{spec, *};

#[tokio::test]
async fn client_build() {
    ClientBuilder::new()
        .uri("http://localhost:50051")
        .unwrap()
        .build::<CardanoSyncClient>()
        .await;
}

#[tokio::test]
async fn follow_tip() {
    let mut client = ClientBuilder::new()
        .uri("http://localhost:50051")
        .unwrap()
        .build::<CardanoSyncClient>()
        .await;

    let intersect = vec![spec::sync::BlockRef {
        slot: 85213090,
        hash: hex::decode("e50842b1cc3ac813cb88d1533c3dea0f92e0ea945f53487c1d960c2210d0c3ba")
            .unwrap()
            .into(),
        height: 3399486,
        timestamp: 1768682099000,
    }];

    let mut tip = client.follow_tip(intersect).await.unwrap();

    let first_event = tip.event().await.unwrap().unwrap();
    match first_event {
        TipEvent::Reset(point) => {
            assert_eq!(point.slot, 85213090);
            assert_eq!(
                hex::encode(&point.hash),
                "e50842b1cc3ac813cb88d1533c3dea0f92e0ea945f53487c1d960c2210d0c3ba"
            );
        }
        _ => panic!("Expected Reset event as first event"),
    }

    let second_event = tip.event().await.unwrap().unwrap();
    match second_event {
        TipEvent::Apply(block) => {
            if let Some(parsed) = block.parsed {
                if let Some(header) = parsed.header {
                    assert_eq!(header.slot, 85213091);
                    assert_eq!(header.height, 3399487);
                    assert!(!header.hash.is_empty());
                }
            }
        }
        _ => panic!("Expected Apply event as second event"),
    }
}

#[tokio::test]
async fn read_tip() {
    let mut client = ClientBuilder::new()
        .uri("http://localhost:50051")
        .unwrap()
        .build::<CardanoSyncClient>()
        .await;

    let tip = client.read_tip().await.unwrap().unwrap();

    assert!(tip.slot > 1, "Slot should be greater than 1");
    assert!(!tip.hash.is_empty(), "Hash should not be empty");
}

#[tokio::test]
async fn fetch_block() {
    let mut client = ClientBuilder::new()
        .uri("http://localhost:50051")
        .unwrap()
        .build::<CardanoSyncClient>()
        .await;

    let block_ref = spec::sync::BlockRef {
        slot: 85213090,
        hash: hex::decode("e50842b1cc3ac813cb88d1533c3dea0f92e0ea945f53487c1d960c2210d0c3ba")
            .unwrap()
            .into(),
        height: 3399486,
        timestamp: 1768682099000,
    };

    let blocks = client.fetch_block(vec![block_ref]).await.unwrap();

    assert_eq!(blocks.len(), 1, "Should fetch exactly one block");

    if let Some(parsed) = &blocks[0].parsed {
        if let Some(header) = &parsed.header {
            assert_eq!(header.slot, 85213090);
            assert_eq!(header.height, 3399486);
        }
    }
}

#[tokio::test]
async fn dump_history() {
    let mut client = ClientBuilder::new()
        .uri("http://localhost:50051")
        .unwrap()
        .build::<CardanoSyncClient>()
        .await;

    let start_ref = spec::sync::BlockRef {
        slot: 85213090,
        hash: hex::decode("e50842b1cc3ac813cb88d1533c3dea0f92e0ea945f53487c1d960c2210d0c3ba")
            .unwrap()
            .into(),
        height: 3399486,
        timestamp: 1768682099000,
    };

    let history = client.dump_history(Some(start_ref), 1).await.unwrap();

    assert!(!history.items.is_empty(), "Should have at least one block");

    let first_block = &history.items[0];
    if let Some(parsed) = &first_block.parsed {
        if let Some(header) = &parsed.header {
            assert_eq!(header.slot, 85213090);
            assert_eq!(header.height, 3399486);
        }
    }
}
