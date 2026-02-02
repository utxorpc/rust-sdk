mod common;

use utxorpc::{spec, *};

#[tokio::test]
async fn read_utxo() {
    let mut client = ClientBuilder::new()
        .uri("http://localhost:50051")
        .unwrap()
        .build::<CardanoQueryClient>()
        .await;

    let refs = vec![spec::query::TxoRef {
        hash: hex::decode("9874bdf4ad47b2d30a2146fc4ba1f94859e58e772683e75001aca6e85de7690d")
            .unwrap()
            .into(),
        index: 0,
    }];

    let utxos = client.read_utxos(refs).await.unwrap();

    assert!(!utxos.is_empty());
}

#[tokio::test]
async fn match_utxos_by_address() {
    let mut client = ClientBuilder::new()
        .uri("http://localhost:50051")
        .unwrap()
        .build::<CardanoQueryClient>()
        .await;

    let pattern = spec::cardano::TxOutputPattern {
        address: Some(spec::cardano::AddressPattern {
            exact_address: hex::decode(
                "00729c67d0de8cde3c0afc768fb0fcb1596e8cfcbf781b553efcd228813b7bb577937983e016d4e8429ff48cf386d6818883f9e88b62a804e0",
            )
            .unwrap()
            .into(),
            payment_part: Default::default(),
            delegation_part: Default::default(),
        }),
        asset: None,
    };

    let utxos = client.match_utxos(pattern, None, 10).await.unwrap();

    assert!(!utxos.items.is_empty());
}

#[tokio::test]
async fn match_utxos_by_payment_part() {
    let mut client = ClientBuilder::new()
        .uri("http://localhost:50051")
        .unwrap()
        .build::<CardanoQueryClient>()
        .await;

    let pattern = spec::cardano::TxOutputPattern {
        address: Some(spec::cardano::AddressPattern {
            exact_address: Default::default(),
            payment_part: hex::decode("729c67d0de8cde3c0afc768fb0fcb1596e8cfcbf781b553efcd22881")
                .unwrap()
                .into(),
            delegation_part: Default::default(),
        }),
        asset: None,
    };

    let utxos = client.match_utxos(pattern, None, 10).await.unwrap();

    assert!(!utxos.items.is_empty());
}

#[tokio::test]
async fn match_utxos_by_delegation_part() {
    let mut client = ClientBuilder::new()
        .uri("http://localhost:50051")
        .unwrap()
        .build::<CardanoQueryClient>()
        .await;

    let pattern = spec::cardano::TxOutputPattern {
        address: Some(spec::cardano::AddressPattern {
            exact_address: Default::default(),
            payment_part: Default::default(),
            delegation_part: hex::decode(
                "3b7bb577937983e016d4e8429ff48cf386d6818883f9e88b62a804e0",
            )
            .unwrap()
            .into(),
        }),
        asset: None,
    };

    let utxos = client.match_utxos(pattern, None, 10).await.unwrap();

    assert!(!utxos.items.is_empty());
}

#[tokio::test]
async fn match_utxos_by_policy_id() {
    let mut client = ClientBuilder::new()
        .uri("http://localhost:50051")
        .unwrap()
        .build::<CardanoQueryClient>()
        .await;

    let pattern = spec::cardano::TxOutputPattern {
        address: None,
        asset: Some(spec::cardano::AssetPattern {
            policy_id: hex::decode("047e0f912c4260fe66ae271e5ae494dcd5f79635bbbb1386be195f4e")
                .unwrap()
                .into(),
            asset_name: Default::default(),
        }),
    };

    let utxos = client.match_utxos(pattern, None, 10).await.unwrap();

    assert!(!utxos.items.is_empty());
}

#[tokio::test]
async fn match_utxos_by_asset() {
    let mut client = ClientBuilder::new()
        .uri("http://localhost:50051")
        .unwrap()
        .build::<CardanoQueryClient>()
        .await;

    let full_asset = hex::decode(
        "047e0f912c4260fe66ae271e5ae494dcd5f79635bbbb1386be195f4e414c4c45594b41545a3030303630",
    )
    .unwrap();

    let policy_id = full_asset[..28].to_vec();

    let pattern = spec::cardano::TxOutputPattern {
        address: None,
        asset: Some(spec::cardano::AssetPattern {
            policy_id: policy_id.into(),
            asset_name: Default::default(),
        }),
    };

    let utxos = client.match_utxos(pattern, None, 10).await.unwrap();

    assert!(!utxos.items.is_empty());
}

#[tokio::test]
async fn read_params() {
    let mut client = ClientBuilder::new()
        .uri("http://localhost:50051")
        .unwrap()
        .build::<CardanoQueryClient>()
        .await;

    let params = client.read_params().await.unwrap();

    assert!(matches!(params.params, Some(_)));
}
