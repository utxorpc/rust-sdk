mod common;

use common::*;
use utxorpc::{*, spec};

#[tokio::test]
#[serial_test::serial]
async fn submit_and_wait_for_tx() {
    use whisky::Wallet;
    use whisky_provider::BlockfrostProvider;
    use whisky_common::Fetcher;
    
    let mut wallet = Wallet::new_mnemonic(TEST_MNEMONIC);
    wallet.payment_account(0, 0);
    
    let provider = BlockfrostProvider::new(
        "previewajMhMPYerz9Pd3GsqjayLwP5mgnNnZCC",
        "preview"
    );
    
    let wallet_address = TEST_ADDRESS;

    let mut submit_client = ClientBuilder::new()
        .uri("http://localhost:50051")
        .unwrap()
        .build::<CardanoSubmitClient>()
        .await;

    let utxos = provider.fetch_address_utxos(wallet_address, None).await
        .expect("Failed to fetch UTXOs");

    if utxos.is_empty() {
        panic!("No UTxOs found for test wallet at address: {}", wallet_address);
    }

    // Calculate required amount: send amount + estimated fees + minimum change
    let send_amount = 2_000_000;
    let estimated_fees = 200_000; // Conservative estimate for fees
    let min_change = 1_000_000; // Minimum ADA for change output
    let min_required = send_amount + estimated_fees + min_change;
    
    // Find the UTXO with the highest balance that meets our requirements
    let selected_utxo = utxos.iter()
        .filter_map(|u| {
            u.output.amount.iter()
                .find(|a| a.unit() == "lovelace")
                .and_then(|a| a.quantity().parse::<u64>().ok())
                .map(|balance| (u, balance))
        })
        .filter(|(_, balance)| *balance >= min_required)
        .max_by_key(|(_, balance)| *balance)
        .map(|(utxo, _)| utxo)
        .expect(&format!(
            "No UTXO with sufficient balance found. Required: {} lovelace, Available UTXOs: {}",
            min_required,
            utxos.iter()
                .filter_map(|u| u.output.amount.iter().find(|a| a.unit() == "lovelace"))
                .filter_map(|a| a.quantity().parse::<u64>().ok())
                .collect::<Vec<_>>()
                .iter()
                .map(|b| b.to_string())
                .collect::<Vec<_>>()
                .join(", ")
        ));
    
    let tx_hash = selected_utxo.input.tx_hash.clone();
    let output_index = selected_utxo.input.output_index;
    let sender_address = selected_utxo.output.address.clone();
    
    let tx_bytes = build_simple_transaction(
        &tx_hash,
        output_index,
        TEST_ADDRESS,
        2_000_000,
        &wallet,
        &sender_address,
        None,
        None,
        None,
    ).await.expect("Failed to build transaction");
    
    match submit_client.submit_tx(vec![tx_bytes]).await {
        Ok(refs) => {
            assert!(!refs.is_empty());
            match submit_client.wait_for_tx(refs).await {
                Ok(mut stream) => {
                    match tokio::time::timeout(
                        std::time::Duration::from_secs(5),
                        stream.event()
                    ).await {
                        Ok(Ok(Some(event))) => {
                            assert!(!event.r#ref.is_empty());
                        }
                        Ok(Ok(None)) => {
                            panic!("Stream ended without transaction event");
                        }
                        Ok(Err(e)) => {
                            panic!("Error while waiting for transaction: {}", e);
                        }
                        Err(_) => {
                        }
                    }
                }
                Err(e) => {
                    panic!("Failed to wait for transaction: {}", e);
                }
            }
        }
        Err(e) => {
            panic!("Failed to submit transaction: {}", e);
        }
    }
}

async fn watch_for_mempool_tx(
    mut stream: MempoolStream,
    timeout_secs: u64,
    expected_tx_id: &str,
) -> Option<String> {
    match tokio::time::timeout(
        std::time::Duration::from_secs(timeout_secs),
        async {
            while let Ok(Some(tx)) = stream.tx().await {
                let tx_id = hex::encode(&tx.r#ref);
                assert!(!tx.native_bytes.is_empty());
                assert!(tx.stage >= 1);
                
                if tx_id == expected_tx_id {
                    return Some(tx_id);
                }
            }
            None
        }
    ).await {
        Ok(result) => result,
        Err(_) => None
    }
}

#[tokio::test]
#[serial_test::serial]
async fn watch_mempool_all_patterns() {
    
    let mut submit_client = ClientBuilder::new()
        .uri("http://localhost:50051")
        .unwrap()
        .build::<CardanoSubmitClient>()
        .await;

    let mut query_client = ClientBuilder::new()
        .uri("http://localhost:50051")
        .unwrap()
        .build::<CardanoQueryClient>()
        .await;

    let mut wallet = whisky::Wallet::new_mnemonic(TEST_MNEMONIC);
    wallet.payment_account(0, 0);
    
    let payment_key_hash = get_payment_key_hash_from_wallet(&wallet).unwrap();
    
    let pattern = spec::cardano::TxOutputPattern {
        address: Some(spec::cardano::AddressPattern {
            exact_address: Default::default(),
            payment_part: payment_key_hash.into(),
            delegation_part: Default::default(),
        }),
        asset: None,
    };

    let utxos = query_client.match_utxos(pattern, None, 10).await.unwrap();
    
    if utxos.items.is_empty() {
        panic!("No UTxOs found for test wallet");
    }
    
    // Calculate required amount: send amount + estimated fees + minimum change
    let send_amount = 2_000_000;
    let estimated_fees = 200_000; // Conservative estimate for fees
    let min_change = 1_000_000; // Minimum ADA for change output
    let min_required = send_amount + estimated_fees + min_change;
    
    // Find the UTXO with the highest balance that meets our requirements
    let utxo = utxos.items.iter()
        .filter(|u| {
            u.parsed.as_ref().map(|p| p.coin >= min_required).unwrap_or(false)
        })
        .max_by_key(|u| {
            u.parsed.as_ref().map(|p| p.coin).unwrap_or(0)
        })
        .expect(&format!(
            "No UTXO with sufficient balance found. Required: {} lovelace, Available UTXOs: {}",
            min_required,
            utxos.items.iter()
                .filter_map(|u| u.parsed.as_ref().map(|p| p.coin))
                .collect::<Vec<_>>()
                .iter()
                .map(|b| b.to_string())
                .collect::<Vec<_>>()
                .join(", ")
        ));
    
    let tx_hash = hex::encode(&utxo.txo_ref.as_ref().unwrap().hash);
    let output_index = utxo.txo_ref.as_ref().unwrap().index;
    let sender_address = if let Some(parsed) = &utxo.parsed {
        bytes_to_bech32_address(&parsed.address, true).unwrap()
    } else {
        TEST_ADDRESS.to_string()
    };
    
    let tx_bytes = match build_simple_transaction(
        &tx_hash,
        output_index,
        TEST_ADDRESS,
        2_000_000,
        &wallet,
        &sender_address,
        None,
        None,
        None,
    ).await {
        Ok(bytes) => bytes,
        Err(e) => {
            panic!("Failed to submit transaction: {:?}", e);
        }
    };
    
    
    let mut wallet = whisky::Wallet::new_mnemonic(TEST_MNEMONIC);
    wallet.payment_account(0, 0);
    let payment_key_hash = get_payment_key_hash_from_wallet(&wallet).unwrap();
    let delegation_key_hash = get_stake_key_hash_from_wallet(&mut wallet).unwrap();
    
    let test_address_bytes = hex::decode(TEST_ADDRESS_HEX).unwrap();
    
    let policy_id_hex = "8b05e87a51c1d4a0fa888d2bb14dbc25e8c343ea379a171b63aa84a0";
    let asset_name_hex = "434e4354";
    let subject = format!("{}{}", policy_id_hex, asset_name_hex);
    let subject_bytes = hex::decode(&subject).unwrap();

    let address_predicate = spec::submit::TxPredicate {
        r#match: Some(spec::submit::AnyChainTxPattern {
            chain: Some(spec::submit::any_chain_tx_pattern::Chain::Cardano(
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

    let payment_predicate = spec::submit::TxPredicate {
        r#match: Some(spec::submit::AnyChainTxPattern {
            chain: Some(spec::submit::any_chain_tx_pattern::Chain::Cardano(
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

    let delegation_predicate = spec::submit::TxPredicate {
        r#match: Some(spec::submit::AnyChainTxPattern {
            chain: Some(spec::submit::any_chain_tx_pattern::Chain::Cardano(
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

    let asset_predicate = spec::submit::TxPredicate {
        r#match: Some(spec::submit::AnyChainTxPattern {
            chain: Some(spec::submit::any_chain_tx_pattern::Chain::Cardano(
                spec::cardano::TxPattern {
                    moves_asset: Some(spec::cardano::AssetPattern {
                        policy_id: Default::default(),
                        asset_name: subject_bytes.into(),
                    }),
                    ..Default::default()
                },
            )),
        }),
        not: vec![],
        all_of: vec![],
        any_of: vec![],
    };

    let policy_predicate = spec::submit::TxPredicate {
        r#match: Some(spec::submit::AnyChainTxPattern {
            chain: Some(spec::submit::any_chain_tx_pattern::Chain::Cardano(
                spec::cardano::TxPattern {
                    moves_asset: Some(spec::cardano::AssetPattern {
                        policy_id: hex::decode(policy_id_hex).unwrap().into(),
                        asset_name: Default::default(),
                    }),
                    ..Default::default()
                },
            )),
        }),
        not: vec![],
        all_of: vec![],
        any_of: vec![],
    };

    let address_stream = submit_client.watch_mempool(Some(address_predicate.clone())).await.unwrap();
    let payment_stream = submit_client.watch_mempool(Some(payment_predicate.clone())).await.unwrap();
    let delegation_stream = submit_client.watch_mempool(Some(delegation_predicate.clone())).await.unwrap();
    let asset_stream = submit_client.watch_mempool(Some(asset_predicate.clone())).await.unwrap();
    let policy_stream = submit_client.watch_mempool(Some(policy_predicate.clone())).await.unwrap();

    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

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

    
    let timeout_secs = 30;
    
    let (address_result, payment_result, delegation_result, asset_result, policy_result) = tokio::join!(
        watch_for_mempool_tx(address_stream, timeout_secs, &submitted_tx_id),
        watch_for_mempool_tx(payment_stream, timeout_secs, &submitted_tx_id),
        watch_for_mempool_tx(delegation_stream, timeout_secs, &submitted_tx_id),
        watch_for_mempool_tx(asset_stream, timeout_secs, &submitted_tx_id),
        watch_for_mempool_tx(policy_stream, timeout_secs, &submitted_tx_id)
    );
    
    for (pattern_name, result) in [
        ("address", &address_result),
        ("payment", &payment_result),
        ("delegation", &delegation_result),
        ("asset", &asset_result),
        ("policy", &policy_result),
    ] {
        if let Some(detected_tx_id) = result {
            assert_eq!(
                detected_tx_id, &submitted_tx_id,
                "{} pattern detected a different transaction ID: {} vs submitted {}",
                pattern_name, detected_tx_id, submitted_tx_id
            );
        }
    }
    
    assert!(address_result.is_some(), "Address pattern should have detected the transaction");
    assert!(payment_result.is_some(), "Payment pattern should have detected the transaction");
    assert!(delegation_result.is_some(), "Delegation pattern should have detected the transaction");
    assert!(asset_result.is_some(), "Asset pattern should have detected the transaction");
    assert!(policy_result.is_some(), "Policy pattern should have detected the transaction");
}