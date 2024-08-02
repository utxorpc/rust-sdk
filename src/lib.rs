use std::{
    marker::PhantomData,
    ops::{Deref, DerefMut},
    str::{Bytes, FromStr},
};

use thiserror::Error;
use tonic::{
    metadata::{Ascii, MetadataKey, MetadataMap, MetadataValue},
    service::{interceptor::InterceptedService, Interceptor},
    transport::Endpoint,
    Streaming,
};

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
    type Intersect;

    fn block_from_any_chain(x: spec::sync::AnyChainBlock) -> ChainBlock<Self::ParsedBlock>;
}

pub struct ChainBlock<B> {
    pub parsed: Option<B>,
    pub native: NativeBytes,
}

pub struct Cardano;

impl Chain for Cardano {
    type ParsedBlock = spec::cardano::Block;
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

pub type CardanoSyncClient = SyncClient<Cardano>;

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
}
