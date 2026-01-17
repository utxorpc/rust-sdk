use utxorpc::*;
use pallas_crypto::{hash::{Hash, Hasher}, key::ed25519};
use pallas_addresses::{Address, PaymentKeyHash, StakeKeyHash, Network, ShelleyAddress};
use pallas_primitives::Fragment;
use pallas_primitives::conway::{
    TransactionBody, TransactionInput, TransactionOutput, Value, 
    AssetName, PolicyId, PostAlonzoTransactionOutput,
    Tx, VKeyWitness, WitnessSet, PositiveCoin, NonEmptySet,
};
use pallas_codec::utils::{NonEmptyKeyValuePairs, Nullable};
use pallas_codec::minicbor::to_vec;
use pallas_wallet::hd::Bip32PrivateKey;

type Result<T> = utxorpc::Result<T>;

// BIP32/BIP44 derivation constants for Cardano
const HARDENED: u32 = 0x80000000;
const PURPOSE: u32 = 1852; // CIP-1852
const COIN_TYPE: u32 = 1815; // ADA
const ACCOUNT: u32 = 0;
const ROLE_EXTERNAL: u32 = 0;
const ROLE_STAKE: u32 = 2;
const INDEX: u32 = 0;

#[allow(dead_code)]
pub const TEST_MNEMONIC: &str = "february next piano since banana hurdle tide soda reward hood luggage bronze polar veteran fold doctor melt usual rose coral mask interest army clump";

#[allow(dead_code)]
pub const TEST_ADDRESS: &str = "addr_test1qztdg2rj2c3y4f56x48fvnxzqt0dd2mawfsmayplt3gn6k4v3cx3x9d83mmpt60cargn7lav9arlg2x6vr8s2yrz9tsq0hfayr";
#[allow(dead_code)]
pub const TEST_ADDRESS_HEX: &str = "0096d4287256224aa69a354e964cc202ded6ab7d7261be903f5c513d5aac8e0d1315a78ef615e9f8e8d13f7fac2f47f428da60cf0510622ae0";

#[allow(dead_code)]
pub fn derive_address_from_mnemonic(mnemonic: &str) -> Result<String> {
    let payment_key_hash = get_payment_key_hash_from_mnemonic(mnemonic)?;
    let stake_key_hash = get_stake_key_hash_from_mnemonic(mnemonic)?;
    
    let mut payment_hash = [0u8; 28];
    payment_hash.copy_from_slice(&payment_key_hash);
    let mut stake_hash = [0u8; 28];
    stake_hash.copy_from_slice(&stake_key_hash);
    
    let address = ShelleyAddress::new(
        Network::Testnet,
        pallas_addresses::ShelleyPaymentPart::Key(PaymentKeyHash::from(payment_hash)),
        pallas_addresses::ShelleyDelegationPart::Key(StakeKeyHash::from(stake_hash))
    );
    
    address.to_bech32().map_err(|e| Error::ParseError(format!("Address encoding error: {:?}", e)))
}

fn derive_key_from_mnemonic(mnemonic: &str, role: u32) -> Result<Bip32PrivateKey> {
    let xprv = Bip32PrivateKey::from_bip39_mnenomic(mnemonic.to_string(), String::new())
        .map_err(|e| Error::ParseError(format!("Invalid mnemonic: {:?}", e)))?;
    
    Ok(xprv
        .derive(PURPOSE | HARDENED)
        .derive(COIN_TYPE | HARDENED)
        .derive(ACCOUNT | HARDENED)
        .derive(role)
        .derive(INDEX))
}

#[allow(dead_code)]
pub fn derive_payment_signing_key_from_mnemonic(mnemonic: &str) -> Result<ed25519::SecretKey> {
    let payment_key = derive_key_from_mnemonic(mnemonic, ROLE_EXTERNAL)?;
    let extended_key_bytes = payment_key.as_bytes();
    let mut key_bytes = [0u8; 32];
    key_bytes.copy_from_slice(&extended_key_bytes[..32]);
    Ok(ed25519::SecretKey::from(key_bytes))
}

#[allow(dead_code)]
pub fn derive_stake_signing_key_from_mnemonic(mnemonic: &str) -> Result<ed25519::SecretKey> {
    let stake_key = derive_key_from_mnemonic(mnemonic, ROLE_STAKE)?;
    let private_key_bytes = stake_key.as_bytes();
    let mut key_bytes = [0u8; 32];
    key_bytes.copy_from_slice(&private_key_bytes[..32]);
    Ok(ed25519::SecretKey::from(key_bytes))
}

#[allow(dead_code)]
pub fn get_payment_key_hash_from_mnemonic(mnemonic: &str) -> Result<Vec<u8>> {
    let secret_key = derive_payment_signing_key_from_mnemonic(mnemonic)?;
    let public_key = secret_key.public_key();
    let key_hash = PaymentKeyHash::from(Hasher::<224>::hash(public_key.as_ref()));
    Ok(key_hash.to_vec())
}

#[allow(dead_code)]
pub fn get_stake_key_hash_from_mnemonic(mnemonic: &str) -> Result<Vec<u8>> {
    let secret_key = derive_stake_signing_key_from_mnemonic(mnemonic)?;
    let public_key = secret_key.public_key();
    let key_hash = StakeKeyHash::from(Hasher::<224>::hash(public_key.as_ref()));
    Ok(key_hash.to_vec())
}

#[allow(dead_code)]
pub async fn fetch_utxos_from_utxorpc(
    address: &str,
) -> Result<Vec<(String, u32, u64, Vec<(String, String, u64)>)>> {
    use utxorpc::spec;
    
    let mut query_client = ClientBuilder::new()
        .uri("http://localhost:50051")
        .unwrap()
        .build::<CardanoQueryClient>()
        .await;
    
    let address_bytes = Address::from_bech32(address)
        .map_err(|e| Error::ParseError(format!("Invalid address: {:?}", e)))?
        .to_vec();
    
    let pattern = spec::cardano::TxOutputPattern {
        address: Some(spec::cardano::AddressPattern {
            exact_address: address_bytes.into(),
            payment_part: Default::default(),
            delegation_part: Default::default(),
        }),
        asset: None,
    };
    
    let response = query_client.match_utxos(pattern, None, 100).await?;
    
    let mut result = Vec::new();
    for item in response.items {
        if let Some(parsed) = item.parsed {
            let tx_ref = item.txo_ref.unwrap();
            let tx_hash = hex::encode(&tx_ref.hash);
            let output_index = tx_ref.index;
            
            let lovelace_amount = parsed.coin.and_then(unwrap_bigint).unwrap_or_default();
            
            let mut assets = Vec::new();
            for asset_group in parsed.assets {
                let policy_id = hex::encode(&asset_group.policy_id);
                for asset in asset_group.assets {
                    let asset_name = hex::encode(&asset.name);
                    let asset_amount = match asset.quantity {
                        Some(spec::cardano::asset::Quantity::OutputCoin(int)) => unwrap_bigint(int).unwrap_or_default(),
                        _ => 0,
                    };
                    assets.push((policy_id.clone(), asset_name, asset_amount));
                }
            }
            
            result.push((tx_hash, output_index, lovelace_amount, assets));
        }
    }
    
    Ok(result)
}


async fn build_transaction_with_utxo(
    tx_hash: &str,
    output_index: u32,
    input_lovelace: u64,
    input_assets: Vec<(String, String, u64)>,
    recipient_address: &str,
    amount: u64,
    mnemonic: &str,
    sender_address: &str,
    asset_policy_id: Option<&str>,
    asset_name: Option<&str>,
    asset_amount: Option<u64>,
) -> Result<Vec<u8>> {
    // Parse addresses
    let recipient_addr = Address::from_bech32(recipient_address)
        .map_err(|e| Error::ParseError(format!("Invalid recipient address: {:?}", e)))?;
    let sender_addr = Address::from_bech32(sender_address)
        .map_err(|e| Error::ParseError(format!("Invalid sender address: {:?}", e)))?;
    
    // Create transaction input
    let tx_bytes = hex::decode(tx_hash)
        .map_err(|e| Error::ParseError(format!("Invalid tx hash: {:?}", e)))?;
    let mut tx_hash_bytes = [0u8; 32];
    tx_hash_bytes.copy_from_slice(&tx_bytes);
    let tx_input = TransactionInput {
        transaction_id: Hash::<32>::from(tx_hash_bytes),
        index: output_index as u64,
    };
    
    // Create transaction output
    let mut output_value = Value::Coin(amount);
    
    // Add asset if specified
    if let (Some(policy_id), Some(name), Some(amt)) = (asset_policy_id, asset_name, asset_amount) {
        let policy_bytes = hex::decode(policy_id)
            .map_err(|e| Error::ParseError(format!("Invalid policy ID: {:?}", e)))?;
        let mut policy_id_bytes = [0u8; 28];
        policy_id_bytes.copy_from_slice(&policy_bytes);
        let policy = PolicyId::from(policy_id_bytes);
        
        let asset_name = AssetName::from(hex::decode(name)
            .map_err(|e| Error::ParseError(format!("Invalid asset name: {:?}", e)))?);
        
        // Create NonEmptyKeyValuePairs for assets
        let asset_map = vec![(asset_name, PositiveCoin::try_from(amt).unwrap())];
        let assets = NonEmptyKeyValuePairs::Def(asset_map);
        
        // Create NonEmptyKeyValuePairs for multiasset
        let multiasset_map = vec![(policy, assets)];
        let multiasset = NonEmptyKeyValuePairs::Def(multiasset_map);
        
        output_value = Value::Multiasset(amount, multiasset);
    }
    
    // Create the output properly wrapped in KeepRaw
    let post_alonzo_output = PostAlonzoTransactionOutput {
        address: recipient_addr.to_vec().into(),
        value: output_value,
        datum_option: None,
        script_ref: None
    };
    
    let tx_output = TransactionOutput::PostAlonzo(post_alonzo_output);
    
    // Create change output
    let fee = 200_000u64;
    let change_amount = input_lovelace.saturating_sub(amount).saturating_sub(fee);
    
    let min_utxo_with_assets = 1_200_000u64;
    let change_has_assets = !input_assets.is_empty() && 
        (asset_policy_id.is_none() || asset_name.is_none() || asset_amount.is_none() ||
         input_assets.iter().any(|(p, n, a)| {
            p != asset_policy_id.unwrap() || n != asset_name.unwrap() || *a > asset_amount.unwrap()
         }));
         
    if change_has_assets && change_amount < min_utxo_with_assets {
        return Err(Error::ParseError(format!(
            "Insufficient funds: change amount {} is below minimum {} required for UTxO with assets",
            change_amount, min_utxo_with_assets
        )));
    }
    
    // Handle change output with remaining assets
    let change_output = if change_amount > 0 || !input_assets.is_empty() {
        // Calculate remaining assets after sending what was requested
        let mut remaining_assets = input_assets.clone();
        
        if let (Some(policy_id), Some(name), Some(amt)) = (asset_policy_id, asset_name, asset_amount) {
            // Remove the sent asset amount from remaining
            for (p, n, a) in &mut remaining_assets {
                if p == policy_id && n == name {
                    *a = a.saturating_sub(amt);
                    break;
                }
            }
        }
        
        // Filter out assets with 0 amount
        remaining_assets.retain(|(_, _, amt)| *amt > 0);
        
        let change_value = if remaining_assets.is_empty() {
            Value::Coin(change_amount)
        } else {
            // Build multiasset for change
            let mut asset_groups: std::collections::HashMap<String, Vec<(AssetName, PositiveCoin)>> = std::collections::HashMap::new();
            
            for (policy, name, amount) in remaining_assets {
                let asset_name = AssetName::from(hex::decode(&name).unwrap_or_default());
                let coin = PositiveCoin::try_from(amount).unwrap();
                asset_groups.entry(policy).or_insert_with(Vec::new).push((asset_name, coin));
            }
            
            let mut multiasset_vec = vec![];
            for (policy_hex, assets) in asset_groups {
                let policy_bytes = hex::decode(&policy_hex).unwrap_or_default();
                if policy_bytes.len() == 28 {
                    let mut policy_id_bytes = [0u8; 28];
                    policy_id_bytes.copy_from_slice(&policy_bytes);
                    let policy = PolicyId::from(policy_id_bytes);
                    let assets_pairs = NonEmptyKeyValuePairs::Def(assets);
                    multiasset_vec.push((policy, assets_pairs));
                }
            }
            
            if multiasset_vec.is_empty() {
                Value::Coin(change_amount)
            } else {
                let multiasset = NonEmptyKeyValuePairs::Def(multiasset_vec);
                Value::Multiasset(change_amount, multiasset)
            }
        };
        
        let change_post_alonzo = PostAlonzoTransactionOutput {
            address: sender_addr.to_vec().into(),
            value: change_value,
            datum_option: None,
            script_ref: None
        };
        Some(TransactionOutput::PostAlonzo(change_post_alonzo))
    } else {
        None
    };
    
    // Build transaction body
    let mut outputs = vec![tx_output];
    if let Some(change) = change_output {
        outputs.push(change);
    }
    
    let tx_body = TransactionBody {
        inputs: vec![tx_input].into(),
        outputs,
        fee,
        ttl: None,
        certificates: None,
        withdrawals: None,
        auxiliary_data_hash: None,
        validity_interval_start: None,
        mint: None,
        script_data_hash: None,
        collateral: None,
        required_signers: None,
        network_id: None,
        collateral_return: None,
        total_collateral: None,
        reference_inputs: None,
        voting_procedures: None,
        proposal_procedures: None,
        treasury_value: None,
        donation: None,
    };
    
    // Sign transaction - create properly wrapped witness
    let tx_body_bytes = to_vec(&tx_body)
        .map_err(|e| Error::ParseError(format!("Failed to encode tx body: {:?}", e)))?;
    let tx_hash = Hasher::<256>::hash(&tx_body_bytes);
    let payment_key = derive_payment_signing_key_from_mnemonic(mnemonic)?;
    let signature = payment_key.sign(&tx_hash.to_vec());
    
    let vkey_witness = VKeyWitness {
        vkey: Vec::from(payment_key.public_key().as_ref()).into(),
        signature: Vec::from(signature.as_ref()).into(),
    };
    
    // Create witness set with properly wrapped vkey witnesses
    let witness_vec = vec![vkey_witness];
    let witness_set = WitnessSet {
        vkeywitness: NonEmptySet::from_vec(witness_vec),
        native_script: None,
        bootstrap_witness: None,
        plutus_v1_script: None,
        plutus_data: None,
        redeemer: None,
        plutus_v2_script: None,
        plutus_v3_script: None,
    };
    
    
    // Create the final transaction
    let tx = Tx {
        transaction_body: tx_body,
        transaction_witness_set: witness_set,
        auxiliary_data: Nullable::Null,
        success: true,
    };
    
    // Encode transaction  
    let tx_bytes = tx.encode_fragment()
        .map_err(|e| Error::ParseError(format!("Failed to encode transaction: {:?}", e)))?;
    
    Ok(tx_bytes)
}

#[allow(dead_code)]
pub async fn build_transaction(
    sender_address: &str,
    recipient_address: &str,
    amount: u64,
    mnemonic: &str,
    asset_policy_id: Option<&str>,
    asset_name: Option<&str>,
    asset_amount: Option<u64>,
) -> Result<Vec<u8>> {
    // Fetch available UTXOs using UTxO RPC
    let utxos = fetch_utxos_from_utxorpc(sender_address).await?;
    
    if utxos.is_empty() {
        return Err(Error::ParseError("No UTXOs available".to_string()));
    }
    
    // Find suitable UTXO based on requirements
    let min_ada = amount + 300_000; // amount + fee buffer
    let suitable_utxo = if asset_policy_id.is_some() && asset_name.is_some() {
        // Need to find UTXO with the specific asset
        utxos.iter()
            .find(|(_, _, lovelace, assets)| {
                *lovelace >= min_ada && 
                assets.iter().any(|(policy, name, _)| {
                    policy == asset_policy_id.unwrap() && 
                    name == asset_name.unwrap()
                })
            })
            .ok_or_else(|| Error::ParseError(format!(
                "No UTXO with at least {} lovelace and asset {}:{}",
                min_ada, asset_policy_id.unwrap(), asset_name.unwrap()
            )))?
    } else {
        // Just need enough ADA
        utxos.iter()
            .find(|(_, _, lovelace, _)| *lovelace >= min_ada)
            .ok_or_else(|| Error::ParseError(format!("No UTXO with at least {} lovelace", min_ada)))?
    };
    
    let (tx_hash, output_index, input_lovelace, input_assets) = suitable_utxo;
    
    build_transaction_with_utxo(
        tx_hash,
        *output_index,
        *input_lovelace,
        input_assets.clone(),
        recipient_address,
        amount,
        mnemonic,
        sender_address,
        asset_policy_id,
        asset_name,
        asset_amount,
    ).await
}

fn unwrap_bigint(value: spec::cardano::BigInt) -> Option<u64> {
    match value.big_int {
        Some(spec::cardano::big_int::BigInt::Int(inner)) => Some(inner as u64),
        _ => None
    }
}