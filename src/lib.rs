use std::{
    marker::PhantomData,
    ops::{Deref, DerefMut},
    str::FromStr,
};

use thiserror::Error;
use tonic::{
    metadata::{Ascii, MetadataKey, MetadataMap, MetadataValue},
    service::{interceptor::InterceptedService, Interceptor},
    transport::{ClientTlsConfig, Endpoint},
    Streaming,
};

pub use spec::submit::Stage;
pub use utxorpc_spec::utxorpc::v1alpha as spec;

use utxorpc_spec::utxorpc::v1alpha::sync::DumpHistoryResponse;

#[derive(Error, Debug)]
pub enum Error {
    #[error("transport error")]
    TransportError(#[from] tonic::transport::Error),

    #[error("grpc error")]
    GrpcError(#[from] tonic::Status),

    #[error("parse error")]
    ParseError(String),
}

pub type Result<T> = core::result::Result<T, Error>;

#[derive(Debug, Clone)]
pub struct MetadataInterceptor {
    map: MetadataMap,
}

impl Interceptor for MetadataInterceptor {
    fn call(
        &mut self,
        mut req: tonic::Request<()>,
    ) -> std::prelude::v1::Result<tonic::Request<()>, tonic::Status> {
        for item in self.map.iter() {
            match item {
                tonic::metadata::KeyAndValueRef::Ascii(key, val) => {
                    req.metadata_mut().insert(key.clone(), val.clone());
                }
                tonic::metadata::KeyAndValueRef::Binary(key, val) => {
                    req.metadata_mut().insert_bin(key.clone(), val.clone());
                }
            }
        }

        Ok(req)
    }
}

pub type InnerService = InterceptedService<tonic::transport::Channel, MetadataInterceptor>;

#[derive(Default)]
pub struct ClientBuilder {
    endpoint: Option<Endpoint>,
    metadata: MetadataMap,
}

impl ClientBuilder {
    pub fn new() -> Self {
        Default::default()
    }

    /// Sets the URL to connect to. Mandatory.
    pub fn uri(self, s: impl ToString) -> Result<Self> {
        let parsed = Endpoint::from_str(s.to_string().as_str())?;

        let value = Self {
            endpoint: Some(parsed),
            ..self
        };

        Ok(value)
    }

    pub fn metadata(self, key: impl ToString, value: impl ToString) -> Result<Self> {
        let key: MetadataKey<Ascii> = key.to_string().parse().unwrap();
        let value: MetadataValue<Ascii> = value.to_string().parse().unwrap();

        let mut metadata = self.metadata;
        metadata.insert(key, value);

        let value = Self { metadata, ..self };

        Ok(value)
    }

    pub async fn build<T>(&self) -> T
    where
        T: From<InnerService>,
    {
        let channel = self
            .endpoint
            .clone()
            .unwrap()
            .tls_config(ClientTlsConfig::new().with_enabled_roots())
            .unwrap()
            .connect_lazy();

        let inner = InterceptedService::new(
            channel,
            MetadataInterceptor {
                map: self.metadata.clone(),
            },
        );

        T::from(inner)
    }
}

pub type NativeBytes = ::bytes::Bytes;

macro_rules! impl_grpc_client {
    (
        $client_name:ident<$chain:ident: Chain>,
        $service_type:ty,
        $client_type:ty
    ) => {
        #[derive(Debug, Clone)]
        pub struct $client_name<$chain: Chain> {
            pub inner: $client_type,
            _phantom: PhantomData<$chain>,
        }

        impl<$chain: Chain> Deref for $client_name<$chain> {
            type Target = $client_type;

            fn deref(&self) -> &Self::Target {
                &self.inner
            }
        }

        impl<$chain: Chain> DerefMut for $client_name<$chain> {
            fn deref_mut(&mut self) -> &mut Self::Target {
                &mut self.inner
            }
        }

        impl<$chain: Chain> From<InnerService> for $client_name<$chain> {
            fn from(value: InnerService) -> Self {
                Self {
                    inner: <$client_type>::new(value)
                        .max_decoding_message_size(usize::MAX),
                    _phantom: Default::default(),
                }
            }
        }
    };
}

pub trait Chain {
    type ParsedBlock;
    type ParsedTx;
    type ParsedUtxo;
    type Intersect;
    type UtxoPattern;

    fn block_from_any_chain(x: spec::sync::AnyChainBlock) -> ChainBlock<Self::ParsedBlock>;
    fn tx_from_any_chain(x: spec::watch::AnyChainTx) -> ChainTx<Self::ParsedTx>;
    fn utxo_from_any_chain(x: spec::query::AnyUtxoData) -> ChainUtxo<Self::ParsedUtxo>;
    fn pattern_into_any_chain(x: Self::UtxoPattern) -> spec::query::AnyUtxoPattern;
}

#[derive(Debug, Clone)]
pub struct ChainBlock<B> {
    pub parsed: Option<B>,
    pub native: NativeBytes,
}

#[derive(Debug, Clone)]
pub struct ChainTx<B> {
    pub parsed: Option<B>,
    pub native: NativeBytes,
}

#[derive(Debug, Clone)]
pub struct ChainUtxo<B> {
    pub parsed: Option<B>,
    pub native: NativeBytes,
    pub txo_ref: Option<spec::query::TxoRef>,
}

#[derive(Debug, Clone)]
pub struct Cardano;

impl Chain for Cardano {
    type ParsedBlock = spec::cardano::Block;
    type ParsedTx = spec::cardano::Tx;
    type ParsedUtxo = spec::cardano::TxOutput;
    type Intersect = Vec<spec::sync::BlockRef>;
    type UtxoPattern = spec::cardano::TxOutputPattern;

    fn pattern_into_any_chain(x: Self::UtxoPattern) -> spec::query::AnyUtxoPattern {
        spec::query::AnyUtxoPattern {
            utxo_pattern: Some(spec::query::any_utxo_pattern::UtxoPattern::Cardano(x)),
        }
    }

    fn block_from_any_chain(x: spec::sync::AnyChainBlock) -> ChainBlock<Self::ParsedBlock> {
        ChainBlock {
            parsed: match x.chain {
                Some(spec::sync::any_chain_block::Chain::Cardano(x)) => Some(x),
                _ => None,
            },
            native: x.native_bytes,
        }
    }

    fn tx_from_any_chain(x: spec::watch::AnyChainTx) -> ChainTx<Self::ParsedTx> {
        ChainTx {
            parsed: match x.chain {
                Some(spec::watch::any_chain_tx::Chain::Cardano(tx)) => Some(tx),
                _ => None,
            },
            native: Default::default(),
        }
    }

    fn utxo_from_any_chain(x: spec::query::AnyUtxoData) -> ChainUtxo<Self::ParsedUtxo> {
        ChainUtxo {
            parsed: match x.parsed_state {
                Some(spec::query::any_utxo_data::ParsedState::Cardano(x)) => Some(x),
                _ => None,
            },
            native: x.native_bytes,
            txo_ref: x.txo_ref,
        }
    }
}

#[derive(Debug, Clone)]
pub enum TipEvent<C>
where
    C: Chain,
{
    Apply(ChainBlock<C::ParsedBlock>),
    Undo(ChainBlock<C::ParsedBlock>),
    Reset(spec::sync::BlockRef),
}

impl<C> TryFrom<spec::sync::FollowTipResponse> for TipEvent<C>
where
    C: Chain,
{
    type Error = ();

    fn try_from(
        value: spec::sync::FollowTipResponse,
    ) -> std::prelude::v1::Result<Self, Self::Error> {
        match value.action.ok_or(())? {
            spec::sync::follow_tip_response::Action::Apply(x) => {
                let block = C::block_from_any_chain(x);
                Ok(Self::Apply(block))
            }
            spec::sync::follow_tip_response::Action::Undo(x) => {
                let block = C::block_from_any_chain(x);
                Ok(Self::Undo(block))
            }
            spec::sync::follow_tip_response::Action::Reset(x) => Ok(Self::Reset(x)),
        }
    }
}

pub struct LiveTip<C: Chain>(Streaming<spec::sync::FollowTipResponse>, PhantomData<C>);

impl<C: Chain> LiveTip<C> {
    pub async fn event(&mut self) -> Result<Option<TipEvent<C>>> {
        let tip_event = match self.0.message().await? {
            Some(event) => Some(
                TipEvent::try_from(event)
                    .map_err(|_| Error::ParseError("error to parse FollowTipResponse".into()))?,
            ),
            None => None,
        };

        Ok(tip_event)
    }
}

#[derive(Debug, Clone)]
pub struct HistoryPage<C: Chain> {
    pub items: Vec<ChainBlock<C::ParsedBlock>>,
    pub next: Option<spec::sync::BlockRef>,
}

impl<C: Chain> From<DumpHistoryResponse> for HistoryPage<C> {
    fn from(value: DumpHistoryResponse) -> Self {
        Self {
            items: value
                .block
                .into_iter()
                .map(C::block_from_any_chain)
                .collect(),
            next: value.next_token,
        }
    }
}

#[derive(Debug, Clone)]
pub struct TxEvent {
    pub r#ref: NativeBytes,
    pub stage: Stage,
}

pub struct TxEventStream(Streaming<spec::submit::WaitForTxResponse>);

impl TxEventStream {
    pub async fn event(&mut self) -> Result<Option<TxEvent>> {
        Ok(self.0.message().await?.map(|event| {
            let stage = event.stage.try_into().ok().unwrap_or_default();
            TxEvent {
                r#ref: event.r#ref,
                stage,
            }
        }))
    }
}

pub struct MempoolStream(Streaming<spec::submit::WatchMempoolResponse>);

impl MempoolStream {
    pub async fn tx(&mut self) -> Result<Option<spec::submit::TxInMempool>> {
        Ok(self.0.message().await?.and_then(|resp| resp.tx))
    }
}

impl_grpc_client!(
    SyncClient<C: Chain>,
    spec::sync::sync_service_client::SyncServiceClient<InnerService>,
    spec::sync::sync_service_client::SyncServiceClient<InnerService>
);

impl<C: Chain> SyncClient<C> {
    pub async fn read_tip(&mut self) -> Result<Option<spec::sync::BlockRef>> {
        let req = spec::sync::ReadTipRequest {};

        let res = self.inner.read_tip(req).await?.into_inner();

        Ok(res.tip)
    }

    pub async fn follow_tip(&mut self, intersect: Vec<spec::sync::BlockRef>) -> Result<LiveTip<C>> {
        let req = spec::sync::FollowTipRequest {
            intersect,
            field_mask: None,
        };

        let stream = self.inner.follow_tip(req).await?;

        Ok(LiveTip(stream.into_inner(), PhantomData::default()))
    }

    pub async fn dump_history(
        &mut self,
        start_token: Option<spec::sync::BlockRef>,
        max_items: u32,
    ) -> Result<HistoryPage<C>> {
        let req = spec::sync::DumpHistoryRequest {
            start_token,
            max_items,
            field_mask: None,
        };

        let res = self.inner.dump_history(req).await?;
        Ok(res.into_inner().into())
    }

    pub async fn fetch_block(
        &mut self,
        r#ref: Vec<spec::sync::BlockRef>,
    ) -> Result<Vec<ChainBlock<C::ParsedBlock>>> {
        let req = spec::sync::FetchBlockRequest {
            r#ref,
            field_mask: None,
        };

        let res = self.inner.fetch_block(req).await?;
        let blocks = res
            .into_inner()
            .block
            .into_iter()
            .map(C::block_from_any_chain)
            .collect();

        Ok(blocks)
    }
}

#[derive(Debug, Clone)]
pub struct UtxoPage<C: Chain> {
    pub items: Vec<ChainUtxo<C::ParsedUtxo>>,
    pub next: Option<String>,
}

impl<C: Chain> From<spec::query::SearchUtxosResponse> for UtxoPage<C> {
    fn from(value: spec::query::SearchUtxosResponse) -> Self {
        Self {
            items: value
                .items
                .into_iter()
                .map(C::utxo_from_any_chain)
                .collect(),
            next: match value.next_token.is_empty() {
                true => None,
                false => Some(value.next_token),
            },
        }
    }
}

impl_grpc_client!(
    QueryClient<C: Chain>,
    spec::query::query_service_client::QueryServiceClient<InnerService>,
    spec::query::query_service_client::QueryServiceClient<InnerService>
);

impl<C: Chain> QueryClient<C> {
    pub async fn read_params(&mut self) -> Result<spec::query::AnyChainParams> {
        let req = spec::query::ReadParamsRequest {
            field_mask: None,
        };

        let res = self.inner.read_params(req).await?;
        let params_response = res.into_inner();

        params_response
            .values
            .ok_or_else(|| Error::ParseError("No parameters in response".to_string()))
    }

    /// Read the genesis configuration of the blockchain.
    /// Returns the raw genesis data as bytes.
    pub async fn read_genesis(&mut self) -> Result<NativeBytes> {
        let req = spec::query::ReadGenesisRequest {
            field_mask: None,
        };

        let res = self.inner.read_genesis(req).await?;
        let genesis_response = res.into_inner();

        Ok(genesis_response.genesis)
    }

    /// Read a summary of the blockchain eras.
    /// Returns information about different eras in the blockchain's history.
    pub async fn read_era_summary(&mut self) -> Result<spec::query::read_era_summary_response::Summary> {
        let req = spec::query::ReadEraSummaryRequest {
            field_mask: None,
        };

        let res = self.inner.read_era_summary(req).await?;
        let era_response = res.into_inner();

        era_response
            .summary
            .ok_or_else(|| Error::ParseError("No era summary data in response".to_string()))
    }

    pub async fn read_utxos(
        &mut self,
        refs: Vec<spec::query::TxoRef>,
    ) -> Result<Vec<ChainUtxo<C::ParsedUtxo>>> {
        let req = spec::query::ReadUtxosRequest {
            keys: refs,
            field_mask: None,
        };

        let res = self.inner.read_utxos(req).await?;

        let utxos = res
            .into_inner()
            .items
            .into_iter()
            .map(C::utxo_from_any_chain)
            .collect();

        Ok(utxos)
    }

    pub async fn search_utxos(
        &mut self,
        predicate: spec::query::UtxoPredicate,
        start_token: Option<String>,
        max_items: u32,
    ) -> Result<UtxoPage<C>> {
        let req = spec::query::SearchUtxosRequest {
            predicate: Some(predicate),
            field_mask: None,
            start_token: start_token.unwrap_or_default(),
            max_items: max_items as i32,
        };

        let res = self.inner.search_utxos(req).await?;

        let utxos = res.into_inner().into();

        Ok(utxos)
    }

    pub async fn match_utxos(
        &mut self,
        pattern: C::UtxoPattern,
        start_token: Option<String>,
        max_items: u32,
    ) -> Result<UtxoPage<C>> {
        let predicate = spec::query::UtxoPredicate {
            r#match: Some(C::pattern_into_any_chain(pattern)),
            not: vec![],
            all_of: vec![],
            any_of: vec![],
        };

        self.search_utxos(predicate, start_token, max_items).await
    }
}

impl_grpc_client!(
    SubmitClient<C: Chain>,
    spec::submit::submit_service_client::SubmitServiceClient<InnerService>,
    spec::submit::submit_service_client::SubmitServiceClient<InnerService>
);

impl<C: Chain> SubmitClient<C> {
    pub async fn submit_tx<B: Into<NativeBytes>>(
        &mut self,
        txs: Vec<B>,
    ) -> Result<Vec<NativeBytes>> {
        let tx = txs
            .into_iter()
            .map(|bytes| spec::submit::AnyChainTx {
                r#type: Some(spec::submit::any_chain_tx::Type::Raw(bytes.into())),
            })
            .collect();

        let req = spec::submit::SubmitTxRequest { tx };

        let res = self.inner.submit_tx(req).await?;
        let refs = res.into_inner().r#ref;
        Ok(refs)
    }

    pub async fn wait_for_tx<B: Into<NativeBytes>>(
        &mut self,
        refs: Vec<B>,
    ) -> Result<TxEventStream> {
        let r#ref = refs.into_iter().map(|b| b.into()).collect();
        let req = spec::submit::WaitForTxRequest { r#ref };

        let res = self.inner.wait_for_tx(req).await?;
        Ok(TxEventStream(res.into_inner()))
    }

    pub async fn watch_mempool(
        &mut self,
        predicate: Option<spec::submit::TxPredicate>,
    ) -> Result<MempoolStream> {
        let req = spec::submit::WatchMempoolRequest { 
            predicate,
            field_mask: None,
        };

        let res = self.inner.watch_mempool(req).await?;
        Ok(MempoolStream(res.into_inner()))
    }
}

impl_grpc_client!(
    WatchClient<C: Chain>,
    spec::watch::watch_service_client::WatchServiceClient<InnerService>,
    spec::watch::watch_service_client::WatchServiceClient<InnerService>
);

#[derive(Debug, Clone)]
pub enum WatchedTx<C>
where
    C: Chain,
{
    Apply {
        tx: ChainTx<C::ParsedTx>,
        block: ChainBlock<C::ParsedBlock>,
    },
    Undo {
        tx: ChainTx<C::ParsedTx>,
        block: ChainBlock<C::ParsedBlock>,
    },
}

pub struct WatchedTxStream<C: Chain>(Streaming<spec::watch::WatchTxResponse>, PhantomData<C>);

impl<C: Chain> WatchedTxStream<C> {
    pub async fn event(&mut self) -> Result<Option<WatchedTx<C>>> {
        match self.0.message().await? {
            Some(res) => match res.action {
                Some(spec::watch::watch_tx_response::Action::Apply(tx)) => {
                    let chain_tx = chain_tx_from_any::<C>(tx);
                    Ok(Some(WatchedTx::Apply {
                        tx: chain_tx.0,
                        block: chain_tx.1,
                    }))
                }
                Some(spec::watch::watch_tx_response::Action::Undo(tx)) => {
                    let chain_tx = chain_tx_from_any::<C>(tx);
                    Ok(Some(WatchedTx::Undo {
                        tx: chain_tx.0,
                        block: chain_tx.1,
                    }))
                }
                None => Ok(None),
            },
            None => Ok(None),
        }
    }
}

fn chain_tx_from_any<C: Chain>(
    any_tx: spec::watch::AnyChainTx,
) -> (ChainTx<C::ParsedTx>, ChainBlock<C::ParsedBlock>) {
    let chain_tx = C::tx_from_any_chain(any_tx);
    let chain_block = ChainBlock {
        parsed: None,
        native: Default::default(),
    };
    (chain_tx, chain_block)
}

impl<C: Chain> WatchClient<C> {
    pub async fn watch_tx(
        &mut self,
        predicate: spec::watch::TxPredicate,
        intersect: Vec<spec::watch::BlockRef>,
    ) -> Result<WatchedTxStream<C>> {
        let req = spec::watch::WatchTxRequest {
            predicate: Some(predicate),
            field_mask: None,
            intersect: intersect,
        };

        let res = self.inner.watch_tx(req).await?;
        Ok(WatchedTxStream(res.into_inner(), PhantomData))
    }
}

pub type CardanoSyncClient = SyncClient<Cardano>;
pub type CardanoQueryClient = QueryClient<Cardano>;
pub type CardanoSubmitClient = SubmitClient<Cardano>;
pub type CardanoWatchClient = WatchClient<Cardano>;

#[cfg(test)]
mod tests {
    use super::*;
    const TEST_MNEMONIC: &str = "february next piano since banana hurdle tide soda reward hood luggage bronze polar veteran fold doctor melt usual rose coral mask interest army clump";
    const TEST_ADDRESS: &str = "addr_test1qpflhll6k7cqz2qezl080uv2szr5zwlqxsqakj4z5ldlpts4j8f56k6tyu5dqj5qlhgyrw6jakenfkkt7fd2y7rhuuuquqeeh5";
    const TEST_ADDRESS_HEX: &str = "0053fbfffab7b001281917de77f18a8087413be03401db4aa2a7dbf0ae1591d34d5b4b2728d04a80fdd041bb52edb334dacbf25aa27877e738";
    
    fn bytes_to_bech32_address(address_bytes: &[u8], is_testnet: bool) -> Result<String> {
        use bech32::{Hrp, Bech32};
        
        let prefix = if is_testnet { "addr_test" } else { "addr" };
        let hrp = Hrp::parse(prefix)
            .map_err(|e| Error::ParseError(format!("HRP parse error: {:?}", e)))?;
        
        let address = bech32::encode::<Bech32>(hrp, address_bytes)
            .map_err(|e| Error::ParseError(format!("Bech32 encoding error: {:?}", e)))?;
        
        Ok(address)
    }
    
    fn get_payment_key_hash_from_wallet(wallet: &whisky::Wallet) -> Result<Vec<u8>> {
        let account = wallet.get_account()
            .map_err(|e| Error::ParseError(format!("Failed to get account: {:?}", e)))?;
        
        let pubkey_bytes = account.public_key.as_bytes();
        
        use blake2::{Blake2b, digest::{consts::U28, Digest}};
        
        let mut hasher = Blake2b::<U28>::new();
        hasher.update(pubkey_bytes);
        let hash = hasher.finalize();
        
        Ok(hash.to_vec())
    }
    
    fn get_stake_key_hash_from_wallet(wallet: &mut whisky::Wallet) -> Result<Vec<u8>> {
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
    
    async fn build_simple_transaction(
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


    #[tokio::test]
    async fn test_client_build() {
        ClientBuilder::new()
            .uri("http://localhost:50051")
            .unwrap()
            .build::<CardanoSyncClient>()
            .await;
    }

    #[tokio::test]
    async fn test_follow_tip() {
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
    async fn test_read_tip() {
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
    async fn test_fetch_block() {
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
    async fn test_dump_history() {
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

    #[tokio::test]
    async fn test_read_utxo() {
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
    async fn test_match_utxos_by_address() {
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
    async fn test_match_utxos_by_payment_part() {
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
    async fn test_match_utxos_by_delegation_part() {
        let mut client = ClientBuilder::new()
            .uri("http://localhost:50051")
            .unwrap()
            .build::<CardanoQueryClient>()
            .await;

        let pattern = spec::cardano::TxOutputPattern {
            address: Some(spec::cardano::AddressPattern {
                exact_address: Default::default(),
                payment_part: Default::default(),
                delegation_part: hex::decode("3b7bb577937983e016d4e8429ff48cf386d6818883f9e88b62a804e0")
                    .unwrap()
                    .into(),
            }),
            asset: None,
        };

        let utxos = client.match_utxos(pattern, None, 10).await.unwrap();

        assert!(!utxos.items.is_empty());
    }

    #[tokio::test]
    async fn test_match_utxos_by_policy_id() {
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
    async fn test_match_utxos_by_asset() {
        let mut client = ClientBuilder::new()
            .uri("http://localhost:50051")
            .unwrap()
            .build::<CardanoQueryClient>()
            .await;

        let full_asset = hex::decode("047e0f912c4260fe66ae271e5ae494dcd5f79635bbbb1386be195f4e414c4c45594b41545a3030303630")
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
    async fn test_read_params() {
        let mut client = ClientBuilder::new()
            .uri("http://localhost:50051")
            .unwrap()
            .build::<CardanoQueryClient>()
            .await;

        let params = client.read_params().await.unwrap();

        assert!(matches!(params.params, Some(_)));
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn test_submit_and_wait_for_tx() {
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
    async fn test_watch_mempool_all_patterns() {
        
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

    #[tokio::test]
    async fn test_watch_client_build() {
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
        match tokio::time::timeout(
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
        ).await {
            Ok(result) => result,
            Err(_) => None
        }
    }


    #[tokio::test]
    #[serial_test::serial]
    async fn test_watch_tx_all_patterns() {
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

        
        let timeout_secs = 200;
        
        let (address_result, payment_result, delegation_result, asset_result, policy_result) = tokio::join!(
            watch_for_tx(address_stream, timeout_secs, &submitted_tx_id),
            watch_for_tx(payment_stream, timeout_secs, &submitted_tx_id),
            watch_for_tx(delegation_stream, timeout_secs, &submitted_tx_id),
            watch_for_tx(asset_stream, timeout_secs, &submitted_tx_id),
            watch_for_tx(policy_stream, timeout_secs, &submitted_tx_id)
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

}
