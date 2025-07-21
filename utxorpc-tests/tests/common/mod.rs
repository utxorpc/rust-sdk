use utxorpc::*;

#[allow(dead_code)]
pub const TEST_MNEMONIC: &str = "february next piano since banana hurdle tide soda reward hood luggage bronze polar veteran fold doctor melt usual rose coral mask interest army clump";
#[allow(dead_code)]
pub const TEST_ADDRESS: &str = "addr_test1qpflhll6k7cqz2qezl080uv2szr5zwlqxsqakj4z5ldlpts4j8f56k6tyu5dqj5qlhgyrw6jakenfkkt7fd2y7rhuuuquqeeh5";
#[allow(dead_code)]
pub const TEST_ADDRESS_HEX: &str = "0053fbfffab7b001281917de77f18a8087413be03401db4aa2a7dbf0ae1591d34d5b4b2728d04a80fdd041bb52edb334dacbf25aa27877e738";

#[allow(dead_code)]
pub fn bytes_to_bech32_address(address_bytes: &[u8], is_testnet: bool) -> Result<String> {
    use bech32::{Hrp, Bech32};
    
    let prefix = if is_testnet { "addr_test" } else { "addr" };
    let hrp = Hrp::parse(prefix)
        .map_err(|e| Error::ParseError(format!("HRP parse error: {:?}", e)))?;
    
    let address = bech32::encode::<Bech32>(hrp, address_bytes)
        .map_err(|e| Error::ParseError(format!("Bech32 encoding error: {:?}", e)))?;
    
    Ok(address)
}

#[allow(dead_code)]
pub fn get_payment_key_hash_from_wallet(wallet: &whisky::Wallet) -> Result<Vec<u8>> {
    let account = wallet.get_account()
        .map_err(|e| Error::ParseError(format!("Failed to get account: {:?}", e)))?;
    
    let pubkey_bytes = account.public_key.as_bytes();
    
    use blake2::{Blake2b, digest::{consts::U28, Digest}};
    
    let mut hasher = Blake2b::<U28>::new();
    hasher.update(pubkey_bytes);
    let hash = hasher.finalize();
    
    Ok(hash.to_vec())
}

#[allow(dead_code)]
pub fn get_stake_key_hash_from_wallet(wallet: &mut whisky::Wallet) -> Result<Vec<u8>> {
    wallet.stake_account(0, 0);
    
    let account = wallet.get_account()
        .map_err(|e| Error::ParseError(format!("Failed to get stake account: {:?}", e)))?;
    
    let pubkey_bytes = account.public_key.as_bytes();
    
    use blake2::{Blake2b, digest::{consts::U28, Digest}};
    
    let mut hasher = Blake2b::<U28>::new();
    hasher.update(pubkey_bytes);
    let hash = hasher.finalize();
    
    wallet.payment_account(0, 0);
    
    Ok(hash.to_vec())
}

#[allow(dead_code)]
pub async fn build_simple_transaction(
    tx_hash: &str,
    output_index: u32,
    recipient_address: &str,
    amount: u64,
    wallet: &whisky::Wallet,
    sender_address: &str,
    asset_policy_id: Option<&str>,
    asset_name: Option<&str>,
    asset_amount: Option<u64>,
) -> Result<Vec<u8>> {
    use whisky_provider::BlockfrostProvider;
    use whisky_common::Fetcher;
    use whisky::builder::TxBuilder;
    use whisky::{Asset, UTxO, UtxoInput, UtxoOutput};
    
    let provider = BlockfrostProvider::new(
        "previewajMhMPYerz9Pd3GsqjayLwP5mgnNnZCC",
        "preview"
    );
    
    let utxos = provider.fetch_address_utxos(sender_address, None).await
        .map_err(|e| Error::ParseError(format!("Failed to fetch UTXOs: {:?}", e)))?;
    
    let utxo = utxos.iter()
        .find(|u| u.input.tx_hash == tx_hash && u.input.output_index == output_index)
        .ok_or_else(|| Error::ParseError("UTXO not found".to_string()))?;
    
    let mut input_assets = vec![];
    for asset in &utxo.output.amount {
        input_assets.push(Asset::new_from_str(
            &asset.unit(),
            &asset.quantity()
        ));
    }
    
    let input_utxo = UTxO {
        input: UtxoInput {
            tx_hash: tx_hash.to_string(),
            output_index: output_index,
        },
        output: UtxoOutput {
            address: sender_address.to_string(),
            amount: input_assets,
            data_hash: None,
            plutus_data: None,
            script_ref: None,
            script_hash: None,
        },
    };
    
    let mut tx_builder = TxBuilder::new_core();
    
    let mut output_assets = vec![Asset::new_from_str("lovelace", &amount.to_string())];
    
    if let (Some(policy_id), Some(name), Some(amt)) = (asset_policy_id, asset_name, asset_amount) {
        let asset_id = format!("{}{}", policy_id, name);
        output_assets.push(Asset::new_from_str(&asset_id, &amt.to_string()));
    }
    
    tx_builder.tx_out(recipient_address, &output_assets);
    tx_builder.change_address(sender_address);
    tx_builder.select_utxos_from(&[input_utxo], amount + 1_000_000);
    
    match tx_builder.complete(None).await {
        Ok(tx_builder_completed) => {
            match wallet.sign_tx(&tx_builder_completed.tx_hex()) {
                Ok(signed_tx) => {
                    hex::decode(&signed_tx).map_err(|e| Error::ParseError(format!("Hex decode error: {}", e)))
                }
                Err(_) => {
                    let tx_hex = tx_builder_completed.tx_hex();
                    hex::decode(&tx_hex).map_err(|e| Error::ParseError(format!("Hex decode error: {}", e)))
                }
            }
        }
        Err(e) => {
            Err(Error::ParseError(format!("Transaction completion failed: {:?}", e)))
        }
    }
}