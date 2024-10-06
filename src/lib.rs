use std::{
    marker::PhantomData,
    ops::{Deref, DerefMut},
    str::FromStr,
};

use thiserror::Error;
use tonic::{
    metadata::{Ascii, MetadataKey, MetadataMap, MetadataValue},
    service::{interceptor::InterceptedService, Interceptor},
    transport::Endpoint,
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
}

pub type Result<T> = core::result::Result<T, Error>;

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
        let channel = self.endpoint.clone().unwrap().connect_lazy();

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

pub trait Chain {
    type ParsedBlock;
    type ParsedUtxo;
    type Intersect;

    fn block_from_any_chain(x: spec::sync::AnyChainBlock) -> ChainBlock<Self::ParsedBlock>;
    fn utxo_from_any_chain(x: spec::query::AnyUtxoData) -> ChainUtxo<Self::ParsedUtxo>;
}

pub struct ChainBlock<B> {
    pub parsed: Option<B>,
    pub native: NativeBytes,
}

pub struct ChainTx<B> {
    pub parsed: Option<B>,
    pub native: NativeBytes,
}

#[derive(Debug)]
pub struct ChainUtxo<B> {
    pub parsed: Option<B>,
    pub native: NativeBytes,
}

pub struct Cardano;

impl Chain for Cardano {
    type ParsedBlock = spec::cardano::Block;
    type ParsedUtxo = spec::cardano::TxOutput;
    type Intersect = Vec<spec::sync::BlockRef>;

    fn block_from_any_chain(x: spec::sync::AnyChainBlock) -> ChainBlock<Self::ParsedBlock> {
        ChainBlock {
            parsed: match x.chain {
                Some(spec::sync::any_chain_block::Chain::Cardano(x)) => Some(x),
                _ => None,
            },
            native: x.native_bytes,
        }
    }

    fn utxo_from_any_chain(x: spec::query::AnyUtxoData) -> ChainUtxo<Self::ParsedUtxo> {
        ChainUtxo {
            parsed: match x.parsed_state {
                Some(spec::query::any_utxo_data::ParsedState::Cardano(x)) => Some(x),
                _ => None,
            },
            native: x.native_bytes,
        }
    }
}

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
    pub async fn event(&mut self) -> Result<TipEvent<C>> {
        loop {
            if let Some(event) = self.0.message().await? {
                match TipEvent::try_from(event) {
                    Ok(evt) => return Ok(evt),
                    Err(_) => continue,
                }
            }
        }
    }
}

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

pub struct QueryClient<C: Chain> {
    inner: spec::query::query_service_client::QueryServiceClient<InnerService>,
    _phantom: PhantomData<C>,
}

impl<C: Chain> QueryClient<C> {
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
    ) -> Result<Vec<ChainUtxo<C::ParsedUtxo>>> {
        let req = spec::query::SearchUtxosRequest {
            predicate: Some(predicate),
            field_mask: None,
        };

        let res = self.inner.search_utxos(req).await?;

        let utxos = res
            .into_inner()
            .items
            .into_iter()
            .map(C::utxo_from_any_chain)
            .collect();

        Ok(utxos)
    }
}

impl<C: Chain> Deref for QueryClient<C> {
    type Target = spec::query::query_service_client::QueryServiceClient<InnerService>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<C: Chain> DerefMut for QueryClient<C> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

impl<C: Chain> From<InnerService> for QueryClient<C> {
    fn from(value: InnerService) -> Self {
        Self {
            inner: spec::query::query_service_client::QueryServiceClient::new(value)
                // we need to relax this limit because there are blocks edge-case blocks that, when including resolved inputs, don't fit in gRPC defaults.
                .max_decoding_message_size(usize::MAX),
            _phantom: Default::default(),
        }
    }
}

pub struct SyncClient<C: Chain> {
    inner: spec::sync::sync_service_client::SyncServiceClient<InnerService>,
    _phantom: PhantomData<C>,
}

impl<C: Chain> SyncClient<C> {
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
}

impl<C: Chain> Deref for SyncClient<C> {
    type Target = spec::sync::sync_service_client::SyncServiceClient<InnerService>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<C: Chain> DerefMut for SyncClient<C> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

impl<C: Chain> From<InnerService> for SyncClient<C> {
    fn from(value: InnerService) -> Self {
        Self {
            inner: spec::sync::sync_service_client::SyncServiceClient::new(value)
                // we need to relax this limit because there are blocks edge-case blocks that, when including resolved inputs, don't fit in gRPC defaults.
                .max_decoding_message_size(usize::MAX),
            _phantom: Default::default(),
        }
    }
}

pub struct SubmitClient<C: Chain> {
    inner: spec::submit::submit_service_client::SubmitServiceClient<InnerService>,
    _phantom: PhantomData<C>,
}

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
}

impl<C: Chain> Deref for SubmitClient<C> {
    type Target = spec::submit::submit_service_client::SubmitServiceClient<InnerService>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<C: Chain> DerefMut for SubmitClient<C> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

impl<C: Chain> From<InnerService> for SubmitClient<C> {
    fn from(value: InnerService) -> Self {
        Self {
            inner: spec::submit::submit_service_client::SubmitServiceClient::new(value)
                // we need to relax this limit because there are blocks edge-case blocks that, when including resolved inputs, don't fit in gRPC defaults.
                .max_decoding_message_size(usize::MAX),
            _phantom: Default::default(),
        }
    }
}

pub type CardanoSyncClient = SyncClient<Cardano>;
pub type CardanoQueryClient = QueryClient<Cardano>;
pub type CardanoSubmitClient = SubmitClient<Cardano>;

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_client_build() {
        ClientBuilder::new()
            .uri("https://preview.utxorpc-v0.demeter.run")
            .unwrap()
            .metadata(
                "dmtr-api-key",
                "dmtr_utxorpc10zrj5dglh53dn8lhgk4p2lffuuu7064j",
            )
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

        let mut tip = client.follow_tip(vec![]).await.unwrap();

        for _ in 0..10 {
            let evt = tip.event().await.unwrap();
            match evt {
                TipEvent::Apply(b) => {
                    dbg!(&b.parsed);
                    dbg!(hex::encode(&b.native));
                }
                _ => println!("other event"),
            }
        }
    }

    #[tokio::test]
    async fn test_read_utxo_by_ref() {
        let mut client = ClientBuilder::new()
            .uri("https://cardano-preview.utxorpc.cloud")
            .unwrap()
            .metadata("dmtr-api-key", "xxxx")
            .unwrap()
            .build::<CardanoQueryClient>()
            .await;

        let refs = vec![spec::query::TxoRef {
            hash: hex::decode("283a0bae03ded3903a9e62d4001849f047ac73fe5ff7291e1cd8753a0017b6dd")
                .unwrap()
                .into(),
            index: 1,
        }];

        let utxos = client.read_utxos(refs).await.unwrap();

        for utxo in utxos {
            dbg!(utxo);
        }
    }
}
