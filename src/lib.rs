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

use utxorpc_spec::utxorpc::v1alpha::{query::ChainPoint, sync::DumpHistoryResponse};

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
                    inner: <$client_type>::new(value).max_decoding_message_size(usize::MAX),
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
    fn query_tx_from_any_chain(x: spec::query::AnyChainTx) -> ChainTx<Self::ParsedTx>;
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
    pub block_ref: Option<ChainPoint>,
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
            block_ref: None,
            native: Default::default(),
        }
    }

    fn query_tx_from_any_chain(x: spec::query::AnyChainTx) -> ChainTx<Self::ParsedTx> {
        ChainTx {
            parsed: match x.chain {
                Some(spec::query::any_chain_tx::Chain::Cardano(tx)) => Some(tx),
                _ => None,
            },
            block_ref: x.block_ref,
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
        let req = spec::query::ReadParamsRequest { field_mask: None };

        let res = self.inner.read_params(req).await?;
        let params_response = res.into_inner();

        params_response
            .values
            .ok_or_else(|| Error::ParseError("No parameters in response".to_string()))
    }

    /// Read the genesis configuration of the blockchain.
    /// Returns the raw genesis data as bytes.
    pub async fn read_genesis(&mut self) -> Result<NativeBytes> {
        let req = spec::query::ReadGenesisRequest { field_mask: None };

        let res = self.inner.read_genesis(req).await?;
        let genesis_response = res.into_inner();

        Ok(genesis_response.genesis)
    }

    /// Read a summary of the blockchain eras.
    /// Returns information about different eras in the blockchain's history.
    pub async fn read_era_summary(
        &mut self,
    ) -> Result<spec::query::read_era_summary_response::Summary> {
        let req = spec::query::ReadEraSummaryRequest { field_mask: None };

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

    pub async fn read_tx(&mut self, hash: NativeBytes) -> Result<Option<ChainTx<C::ParsedTx>>> {
        let req = spec::query::ReadTxRequest {
            hash: hash,
            field_mask: None,
        };

        let res = self.inner.read_tx(req).await?;
        let tx_response = res.into_inner();

        if let Some(any_tx) = tx_response.tx {
            let tx = C::query_tx_from_any_chain(any_tx);
            Ok(Some(tx))
        } else {
            Ok(None)
        }
    }
}

impl_grpc_client!(
    SubmitClient<C: Chain>,
    spec::submit::submit_service_client::SubmitServiceClient<InnerService>,
    spec::submit::submit_service_client::SubmitServiceClient<InnerService>
);

impl<C: Chain> SubmitClient<C> {
    pub async fn submit_tx<B: Into<NativeBytes>>(&mut self, tx: B) -> Result<NativeBytes> {
        let tx = Some(spec::submit::AnyChainTx {
            r#type: Some(spec::submit::any_chain_tx::Type::Raw(tx.into())),
        });

        let req = spec::submit::SubmitTxRequest { tx };

        let res = self.inner.submit_tx(req).await?;
        let r#ref = res.into_inner().r#ref;
        Ok(r#ref)
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
                Some(spec::watch::watch_tx_response::Action::Idle(_)) => Ok(None),
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
