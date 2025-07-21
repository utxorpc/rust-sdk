mod common;

use common::*;
use utxorpc::{*, spec};

#[tokio::test]
async fn watch_client_build() {
    ClientBuilder::new()
        .uri("http://localhost:50051")
        .unwrap()
        .build::<CardanoWatchClient>()
        .await;
}

async fn create_watch_client() -> CardanoWatchClient {
    ClientBuilder::new()
        .uri("http://localhost:50051")
        .unwrap()
        .build::<CardanoWatchClient>()
        .await
}

async fn watch_for_tx(
    mut stream: WatchedTxStream<Cardano>,
    timeout_secs: u64,
    expected_tx_id: &str,
) -> Option<String> {
    println!("[watch_for_tx] Starting to watch for transaction: {}", expected_tx_id);
    match tokio::time::timeout(
        std::time::Duration::from_secs(timeout_secs),
        async {
            let mut event_count = 0;
            while let Ok(Some(event)) = stream.event().await {
                event_count += 1;
                match event {
                    WatchedTx::Apply { tx, block: _ } => {
                        if let Some(parsed_tx) = tx.parsed {
                            let tx_id = hex::encode(&parsed_tx.hash);
                            println!("[watch_for_tx] Event {}: Found transaction {}", event_count, tx_id);
                            if tx_id == expected_tx_id {
                                println!("[watch_for_tx] MATCH! Found expected transaction");
                                return Some(tx_id);
                            }
                        } else {
                            println!("[watch_for_tx] Event {}: Apply event without parsed tx", event_count);
                        }
                    }
                    WatchedTx::Undo { .. } => {
                        println!("[watch_for_tx] Event {}: Undo event", event_count);
                    }
                }
            }
            println!("[watch_for_tx] Stream ended without finding transaction (processed {} events)", event_count);
            None
        }
    ).await {
        Ok(result) => result,
        Err(_) => {
            println!("[watch_for_tx] Timeout after {} seconds", timeout_secs);
            None
        }
    }
}

#[tokio::test]
#[serial_test::serial]
async fn watch_tx_all_patterns() {
    use whisky_provider::BlockfrostProvider;
    use whisky_common::Fetcher;
    
    let mut submit_client = ClientBuilder::new()
        .uri("http://localhost:50051")
        .unwrap()
        .build::<CardanoSubmitClient>()
        .await;

    let mut wallet = whisky::Wallet::new_mnemonic(TEST_MNEMONIC);
    wallet.payment_account(0, 0);
    
    let provider = BlockfrostProvider::new(
        "previewajMhMPYerz9Pd3GsqjayLwP5mgnNnZCC",
        "preview"
    );
    
    let wallet_address = TEST_ADDRESS;
    
    let utxos = provider.fetch_address_utxos(wallet_address, None).await
        .map_err(|e| panic!("Failed to fetch UTXOs: {:?}", e))
        .unwrap();
    
    if utxos.is_empty() {
        panic!("No UTxOs found for test wallet at address: {}", wallet_address);
    }
    
    let policy_id_hex = "8b05e87a51c1d4a0fa888d2bb14dbc25e8c343ea379a171b63aa84a0";
    let asset_name_hex = "434e4354";
    let asset_unit = format!("{}{}", policy_id_hex, asset_name_hex);
    
    // Calculate required amount: send amount + estimated fees + minimum change
    let send_amount_with_asset = 1_500_000; // Amount when sending with asset
    let send_amount_ada_only = 2_000_000; // Amount when sending ADA only
    let estimated_fees = 200_000; // Conservative estimate for fees
    let min_change = 1_000_000; // Minimum ADA for change output
    let min_for_asset_utxo = send_amount_with_asset + estimated_fees + min_change;
    let min_for_ada_only = send_amount_ada_only + estimated_fees + min_change;
    
    let selected_utxo = utxos.iter().find(|u| {
        u.output.amount.iter().any(|a| a.unit() == asset_unit) &&
        u.output.amount.iter()
            .find(|a| a.unit() == "lovelace")
            .map(|a| a.quantity().parse::<u64>().unwrap_or(0) >= min_for_asset_utxo)
            .unwrap_or(false)
    }).or_else(|| {
        utxos.iter()
            .filter(|u| {
                u.output.amount.iter()
                    .find(|a| a.unit() == "lovelace")
                    .map(|a| a.quantity().parse::<u64>().unwrap_or(0) >= min_for_ada_only)
                    .unwrap_or(false)
            })
            .max_by_key(|u| {
                u.output.amount.iter()
                    .find(|a| a.unit() == "lovelace")
                    .map(|a| a.quantity().parse::<u64>().unwrap_or(0))
                    .unwrap_or(0)
            })
    }).expect(&format!(
        "No UTXO with sufficient balance found. Required: {} lovelace, Available UTXOs: {}",
        min_for_ada_only,
        utxos.iter()
            .filter_map(|u| u.output.amount.iter().find(|a| a.unit() == "lovelace"))
            .filter_map(|a| a.quantity().parse::<u64>().ok())
            .collect::<Vec<_>>()
            .iter()
            .map(|b| b.to_string())
            .collect::<Vec<_>>()
            .join(", ")
    ));
    
    let has_asset = selected_utxo.output.amount.iter()
        .any(|a| a.unit() == asset_unit);
    
    let tx_hash = selected_utxo.input.tx_hash.clone();
    let output_index = selected_utxo.input.output_index;
    let sender_address = selected_utxo.output.address.clone();

    let tx_bytes = build_simple_transaction(
        &tx_hash,
        output_index,
        TEST_ADDRESS,
        if has_asset { 1_500_000 } else { 2_000_000 },
        &wallet,
        &sender_address,
        if has_asset { Some(policy_id_hex) } else { None },
        if has_asset { Some(asset_name_hex) } else { None },
        if has_asset { Some(1) } else { None },
    ).await.expect("Failed to build transaction");
    
    let payment_key_hash = get_payment_key_hash_from_wallet(&wallet).unwrap();
    let delegation_key_hash = get_stake_key_hash_from_wallet(&mut wallet).unwrap();
    let test_address_bytes = hex::decode(TEST_ADDRESS_HEX).unwrap();

    let address_predicate = spec::watch::TxPredicate {
        r#match: Some(spec::watch::AnyChainTxPattern {
            chain: Some(spec::watch::any_chain_tx_pattern::Chain::Cardano(
                spec::cardano::TxPattern {
                    has_address: Some(spec::cardano::AddressPattern {
                        exact_address: test_address_bytes.into(),
                        payment_part: Default::default(),
                        delegation_part: Default::default(),
                    }),
                    ..Default::default()
                },
            )),
        }),
        not: vec![],
        all_of: vec![],
        any_of: vec![],
    };

    let payment_predicate = spec::watch::TxPredicate {
        r#match: Some(spec::watch::AnyChainTxPattern {
            chain: Some(spec::watch::any_chain_tx_pattern::Chain::Cardano(
                spec::cardano::TxPattern {
                    has_address: Some(spec::cardano::AddressPattern {
                        exact_address: Default::default(),
                        payment_part: payment_key_hash.into(),
                        delegation_part: Default::default(),
                    }),
                    ..Default::default()
                },
            )),
        }),
        not: vec![],
        all_of: vec![],
        any_of: vec![],
    };

    let delegation_predicate = spec::watch::TxPredicate {
        r#match: Some(spec::watch::AnyChainTxPattern {
            chain: Some(spec::watch::any_chain_tx_pattern::Chain::Cardano(
                spec::cardano::TxPattern {
                    has_address: Some(spec::cardano::AddressPattern {
                        exact_address: Default::default(),
                        payment_part: Default::default(),
                        delegation_part: delegation_key_hash.into(),
                    }),
                    ..Default::default()
                },
            )),
        }),
        not: vec![],
        all_of: vec![],
        any_of: vec![],
    };

    println!("\nAsset pattern - Policy ID: {}, Asset name: {}", policy_id_hex, asset_name_hex);
    let asset_predicate = spec::watch::TxPredicate {
        r#match: Some(spec::watch::AnyChainTxPattern {
            chain: Some(spec::watch::any_chain_tx_pattern::Chain::Cardano(
                spec::cardano::TxPattern {
                    moves_asset: Some(spec::cardano::AssetPattern {
                        policy_id: hex::decode(policy_id_hex).unwrap().into(),
                        asset_name: hex::decode(asset_name_hex).unwrap().into(),
                    }),
                    ..Default::default()
                },
            )),
        }),
        not: vec![],
        all_of: vec![],
        any_of: vec![],
    };

    println!("Policy pattern - Policy ID: {} (empty asset name)", policy_id_hex);
    let policy_predicate = spec::watch::TxPredicate {
        r#match: Some(spec::watch::AnyChainTxPattern {
            chain: Some(spec::watch::any_chain_tx_pattern::Chain::Cardano(
                spec::cardano::TxPattern {
                    moves_asset: Some(spec::cardano::AssetPattern {
                        policy_id: hex::decode(policy_id_hex).unwrap().into(),
                        asset_name: vec![].into(),
                    }),
                    ..Default::default()
                },
            )),
        }),
        not: vec![],
        all_of: vec![],
        any_of: vec![],
    };

    let mut address_client = create_watch_client().await;
    let mut payment_client = create_watch_client().await;
    let mut delegation_client = create_watch_client().await;
    let mut asset_client = create_watch_client().await;
    let mut policy_client = create_watch_client().await;

    let address_stream = address_client.watch_tx(address_predicate, vec![]).await.unwrap();
    let payment_stream = payment_client.watch_tx(payment_predicate, vec![]).await.unwrap();
    let delegation_stream = delegation_client.watch_tx(delegation_predicate, vec![]).await.unwrap();
    let asset_stream = asset_client.watch_tx(asset_predicate, vec![]).await.unwrap();
    let policy_stream = policy_client.watch_tx(policy_predicate, vec![]).await.unwrap();

    let submitted_tx_id = match submit_client.submit_tx(vec![tx_bytes]).await {
        Ok(tx_refs) => {
            if let Some(tx_ref) = tx_refs.first() {
                let tx_id = hex::encode(tx_ref);
                tx_id
            } else {
                panic!("No transaction ref returned from submit");
            }
        }
        Err(e) => {
            panic!("Failed to submit transaction: {:?}", e);
        }
    };

    
    println!("Submitted transaction ID: {}", submitted_tx_id);
    println!("Has asset in transaction: {}", has_asset);
    
    let timeout_secs = 200;
    
    let (address_result, payment_result, delegation_result, asset_result, policy_result) = tokio::join!(
        watch_for_tx(address_stream, timeout_secs, &submitted_tx_id),
        watch_for_tx(payment_stream, timeout_secs, &submitted_tx_id),
        watch_for_tx(delegation_stream, timeout_secs, &submitted_tx_id),
        watch_for_tx(asset_stream, timeout_secs, &submitted_tx_id),
        watch_for_tx(policy_stream, timeout_secs, &submitted_tx_id)
    );
    
    println!("\nTest Results:");
    println!("- Address pattern: {:?}", address_result.is_some());
    println!("- Payment pattern: {:?}", payment_result.is_some());
    println!("- Delegation pattern: {:?}", delegation_result.is_some());
    println!("- Asset pattern: {:?}", asset_result.is_some());
    println!("- Policy pattern: {:?}", policy_result.is_some());
    
    for (pattern_name, result) in [
        ("address", &address_result),
        ("payment", &payment_result),
        ("delegation", &delegation_result),
        ("asset", &asset_result),
        ("policy", &policy_result),
    ] {
        if let Some(detected_tx_id) = result {
            println!("{} pattern detected transaction: {}", pattern_name, detected_tx_id);
            assert_eq!(
                detected_tx_id, &submitted_tx_id,
                "{} pattern detected a different transaction ID: {} vs submitted {}",
                pattern_name, detected_tx_id, submitted_tx_id
            );
        } else {
            println!("{} pattern did NOT detect the transaction", pattern_name);
        }
    }
    
    assert!(address_result.is_some(), "Address pattern should have detected the transaction");
    assert!(payment_result.is_some(), "Payment pattern should have detected the transaction");
    assert!(delegation_result.is_some(), "Delegation pattern should have detected the transaction");
    
    // Asset and policy patterns should only match if the transaction actually includes the asset
    if has_asset {
        assert!(asset_result.is_some(), "Asset pattern should have detected the transaction (transaction includes asset)");
        assert!(policy_result.is_some(), "Policy pattern should have detected the transaction (transaction includes asset)");
    } else {
        println!("\nTransaction does not include asset, skipping asset/policy pattern assertions");
        assert!(asset_result.is_none(), "Asset pattern should NOT detect transaction without asset");
        assert!(policy_result.is_none(), "Policy pattern should NOT detect transaction without asset");
    }
}