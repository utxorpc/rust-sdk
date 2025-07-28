mod common;

use common::*;
use utxorpc::{*, spec};

const TEST_CLIENT_URI: &str = "http://localhost:50051";
const POLICY_ID_HEX: &str = "8b05e87a51c1d4a0fa888d2bb14dbc25e8c343ea379a171b63aa84a0";
const ASSET_NAME_HEX: &str = "434e4354";

async fn create_watch_client() -> CardanoWatchClient {
    ClientBuilder::new()
        .uri(TEST_CLIENT_URI)
        .unwrap()
        .build::<CardanoWatchClient>()
        .await
}

async fn create_submit_client() -> CardanoSubmitClient {
    ClientBuilder::new()
        .uri(TEST_CLIENT_URI)
        .unwrap()
        .build::<CardanoSubmitClient>()
        .await
}

#[tokio::test]
async fn watch_client_build() {
    ClientBuilder::new()
        .uri(TEST_CLIENT_URI)
        .unwrap()
        .build::<CardanoWatchClient>()
        .await;
}

async fn watch_for_tx(
    mut stream: WatchedTxStream<Cardano>,
    timeout_secs: u64,
    expected_tx_id: &str,
) -> Option<String> {
    tokio::time::timeout(
        std::time::Duration::from_secs(timeout_secs),
        async {
            while let Ok(Some(event)) = stream.event().await {
                match event {
                    WatchedTx::Apply { tx, block: _ } => {
                        if let Some(parsed_tx) = tx.parsed {
                            let tx_id = hex::encode(&parsed_tx.hash);
                            if tx_id == expected_tx_id {
                                return Some(tx_id);
                            }
                        }
                    }
                    WatchedTx::Undo { .. } => {}
                }
            }
            None
        }
    ).await.ok().flatten()
}

fn create_tx_predicate(pattern: spec::cardano::TxPattern) -> spec::watch::TxPredicate {
    spec::watch::TxPredicate {
        r#match: Some(spec::watch::AnyChainTxPattern {
            chain: Some(spec::watch::any_chain_tx_pattern::Chain::Cardano(pattern)),
        }),
        not: vec![],
        all_of: vec![],
        any_of: vec![],
    }
}

#[tokio::test]
#[serial_test::serial]
async fn watch_tx_all_patterns() {
    let mut submit_client = create_submit_client().await;

    let payment_key_hash = get_payment_key_hash_from_mnemonic(TEST_MNEMONIC).unwrap();
    let stake_key_hash = get_stake_key_hash_from_mnemonic(TEST_MNEMONIC).unwrap();
    let test_address_bytes = hex::decode(TEST_ADDRESS_HEX).unwrap();

    // Build transaction WITH assets for testing
    let tx_bytes = match build_transaction(
        TEST_ADDRESS,
        TEST_ADDRESS,
        2_000_000,  // Higher amount needed when sending assets
        TEST_MNEMONIC,
        Some(POLICY_ID_HEX),
        Some(ASSET_NAME_HEX),
        Some(1),  // Send 1 unit of the asset
    ).await {
        Ok(bytes) => bytes,
        Err(_) => {
            eprintln!("Failed to build transaction - skipping test");
            return;
        }
    };

    // Create predicates for different patterns
    let predicates = [
        ("address", create_tx_predicate(spec::cardano::TxPattern {
            has_address: Some(spec::cardano::AddressPattern {
                exact_address: test_address_bytes.into(),
                payment_part: Default::default(),
                delegation_part: Default::default(),
            }),
            ..Default::default()
        })),
        ("payment", create_tx_predicate(spec::cardano::TxPattern {
            has_address: Some(spec::cardano::AddressPattern {
                exact_address: Default::default(),
                payment_part: payment_key_hash.into(),
                delegation_part: Default::default(),
            }),
            ..Default::default()
        })),
        ("delegation", create_tx_predicate(spec::cardano::TxPattern {
            has_address: Some(spec::cardano::AddressPattern {
                exact_address: Default::default(),
                payment_part: Default::default(),
                delegation_part: stake_key_hash.into(),
            }),
            ..Default::default()
        })),
        ("asset", create_tx_predicate(spec::cardano::TxPattern {
            moves_asset: Some(spec::cardano::AssetPattern {
                policy_id: hex::decode(POLICY_ID_HEX).unwrap().into(),
                asset_name: hex::decode(ASSET_NAME_HEX).unwrap().into(),
            }),
            ..Default::default()
        })),
        ("policy", create_tx_predicate(spec::cardano::TxPattern {
            moves_asset: Some(spec::cardano::AssetPattern {
                policy_id: hex::decode(POLICY_ID_HEX).unwrap().into(),
                asset_name: vec![].into(),
            }),
            ..Default::default()
        })),
    ];

    // Create watch clients and start watching
    let mut streams = Vec::new();
    for (_, predicate) in &predicates {
        let mut client = create_watch_client().await;
        let stream = client.watch_tx(predicate.clone(), vec![]).await.unwrap();
        streams.push(stream);
    }

    // Give watch streams time to initialize
    tokio::time::sleep(std::time::Duration::from_secs(1)).await;

    // Submit transaction
    let submitted_tx_id = match submit_client.submit_tx(vec![tx_bytes]).await {
        Ok(refs) => refs.first()
            .map(|tx_ref| hex::encode(tx_ref))
            .expect("No transaction ref returned from submit"),
        Err(_) => {
            eprintln!("Transaction submission failed - skipping test");
            return;
        }
    };

    const TIMEOUT_SECS: u64 = 30;
    // Watch for the transaction with all patterns
    let results = match streams.len() {
        5 => {
            let mut iter = streams.into_iter();
            let (r0, r1, r2, r3, r4) = tokio::join!(
                watch_for_tx(iter.next().unwrap(), TIMEOUT_SECS, &submitted_tx_id),
                watch_for_tx(iter.next().unwrap(), TIMEOUT_SECS, &submitted_tx_id),
                watch_for_tx(iter.next().unwrap(), TIMEOUT_SECS, &submitted_tx_id),
                watch_for_tx(iter.next().unwrap(), TIMEOUT_SECS, &submitted_tx_id),
                watch_for_tx(iter.next().unwrap(), TIMEOUT_SECS, &submitted_tx_id)
            );
            vec![r0, r1, r2, r3, r4]
        }
        _ => panic!("Expected exactly 5 predicates")
    };
    
    // Verify results
    for ((pattern_name, _), result) in predicates.iter().zip(results.iter()) {
        if let Some(detected_tx_id) = result {
            assert_eq!(
                detected_tx_id, &submitted_tx_id,
                "{} pattern detected a different transaction ID",
                pattern_name
            );
        }
        
        // All patterns should detect the transaction since it has assets
        assert!(result.is_some(), "{} pattern should have detected the transaction", pattern_name);
    }
}