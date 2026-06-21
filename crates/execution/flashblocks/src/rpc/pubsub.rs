//! `eth_` `PubSub` RPC extension for flashblocks and standard subscriptions
//!
//! This module provides an extended `eth_subscribe` implementation that supports both
//! standard Ethereum subscription types (newHeads, logs, newPendingTransactions, syncing)
//! and Base-specific flashblocks subscriptions (newFlashblocks, pendingLogs).

use std::sync::Arc;

use alloy_primitives::B256;
use alloy_rpc_types_eth::{Filter, Log, pubsub::Params};
use base_common_network::Base;
use futures::stream;
use jsonrpsee::{
    PendingSubscriptionSink, SubscriptionSink,
    core::{SubscriptionResult, async_trait},
    proc_macros::rpc,
    server::SubscriptionMessage,
};
use jsonrpsee_types::{ErrorObjectOwned, error::INVALID_PARAMS_CODE};
use reth_rpc::eth::EthPubSub as RethEthPubSub;
use reth_rpc_eth_api::{
    EthApiTypes, RpcBlock, RpcNodeCore, RpcTransaction,
    pubsub::EthPubSubApiServer as RethEthPubSubApiServer,
};
use serde::Serialize;
use tokio::sync::broadcast::{self, error::RecvError};
use tokio_stream::{Stream, StreamExt, wrappers::BroadcastStream};
use tracing::error;

use crate::{
    FastFlashblockFeedEvent, FlashblocksAPI, TransactionWithLogs,
    metrics::Metrics,
    rpc::types::{
        BaseSubscriptionKind, ExtendedSubscriptionKind, FlashblockLogsBatch,
        unsupported_in_hot_only,
    },
};

#[derive(Clone, Copy, Debug)]
enum PubSubMetric {
    FastLogs,
    LogsBatch,
}

/// Eth pub-sub RPC extension for flashblocks and standard subscriptions.
///
/// This trait defines the `eth_subscribe` and `eth_unsubscribe` methods that handle
/// both standard Ethereum subscriptions and Base-specific flashblocks subscriptions.
#[rpc(server, namespace = "eth")]
pub trait EthPubSubApi {
    /// Create an Eth subscription for the given kind.
    ///
    /// Supports standard subscription types (newHeads, logs, newPendingTransactions, syncing)
    /// as well as Base-specific subscriptions (newFlashblocks, pendingLogs).
    #[subscription(
        name = "subscribe" => "subscription",
        unsubscribe = "unsubscribe",
        item = serde_json::Value
    )]
    async fn subscribe(
        &self,
        kind: ExtendedSubscriptionKind,
        params: Option<Params>,
    ) -> SubscriptionResult;
}

/// `Eth` pubsub RPC implementation that extends reth's standard implementation
/// with flashblocks support.
///
/// This handles `eth_subscribe` RPC calls for both standard Ethereum subscriptions
/// and Base-specific flashblocks subscriptions.
#[derive(Clone, Debug)]
pub struct EthPubSub<Eth, FB> {
    /// Reth's standard `EthPubSub` for handling standard subscription types
    inner: RethEthPubSub<Eth>,
    /// Flashblocks state for accessing pending blocks stream
    flashblocks_state: Arc<FB>,
}

impl<Eth, FB> EthPubSub<Eth, FB> {
    /// Creates a new instance with the given eth API and flashblocks state.
    pub fn new(eth_api: Eth, flashblocks_state: Arc<FB>) -> Self {
        Self { inner: RethEthPubSub::new(eth_api), flashblocks_state }
    }

    /// Returns a stream that yields all new flashblocks as RPC blocks
    fn new_flashblocks_stream(flashblocks_state: Arc<FB>) -> impl Stream<Item = RpcBlock<Base>>
    where
        FB: FlashblocksAPI + Send + Sync + 'static,
    {
        BroadcastStream::new(flashblocks_state.subscribe_to_flashblocks()).filter_map(|result| {
            let pending_blocks = match result {
                Ok(blocks) => blocks,
                Err(err) => {
                    error!(
                        message = "Error in flashblocks stream",
                        error = %err
                    );
                    return None;
                }
            };
            Some(base_metrics::time!(Metrics::new_flashblocks_build_duration(), {
                pending_blocks.get_latest_block(true)
            }))
        })
    }

    /// Returns a stream that yields individual logs from only the latest flashblock matching the
    /// filter.
    ///
    /// Each matching log is emitted as a separate stream item (one log per WebSocket message).
    /// Only logs from the most recent flashblock are emitted to avoid duplicates.
    fn pending_logs_stream(flashblocks_state: Arc<FB>, filter: Filter) -> impl Stream<Item = Log>
    where
        FB: FlashblocksAPI + Send + Sync + 'static,
    {
        futures::StreamExt::flat_map(
            StreamExt::filter_map(
                BroadcastStream::new(flashblocks_state.subscribe_to_flashblocks()),
                move |result| {
                    let pending_blocks = match result {
                        Ok(blocks) => blocks,
                        Err(err) => {
                            error!(
                                message = "Error in flashblocks stream for pending logs",
                                error = %err
                            );
                            return None;
                        }
                    };
                    let logs = pending_blocks.get_latest_flashblock_logs(&filter);
                    if logs.is_empty() { None } else { Some(logs) }
                },
            ),
            stream::iter,
        )
    }

    /// Returns a stream that yields individual full transactions with logs from only the latest
    /// flashblock.
    ///
    /// Each transaction (with its associated logs) is emitted as a separate stream item
    /// (one transaction per WebSocket message). Only transactions from the most recent
    /// flashblock are emitted to avoid duplicates.
    fn new_flashblock_transactions_full_stream(
        flashblocks_state: Arc<FB>,
    ) -> impl Stream<Item = TransactionWithLogs>
    where
        FB: FlashblocksAPI + Send + Sync + 'static,
    {
        futures::StreamExt::flat_map(
            StreamExt::filter_map(
                BroadcastStream::new(flashblocks_state.subscribe_to_flashblocks()),
                |result| {
                    let pending_blocks = match result {
                        Ok(blocks) => blocks,
                        Err(err) => {
                            error!(
                                message = "Error in flashblocks stream for transactions",
                                error = %err
                            );
                            return None;
                        }
                    };
                    let txs = pending_blocks.get_latest_flashblock_transactions_with_logs();
                    if txs.is_empty() { None } else { Some(txs) }
                },
            ),
            stream::iter,
        )
    }

    /// Returns a stream that yields full transactions with logs from only the latest flashblock,
    /// filtered to include only transactions where at least one log matches the filter.
    fn new_flashblock_transactions_filtered_stream(
        flashblocks_state: Arc<FB>,
        filter: Filter,
    ) -> impl Stream<Item = TransactionWithLogs>
    where
        FB: FlashblocksAPI + Send + Sync + 'static,
    {
        futures::StreamExt::flat_map(
            StreamExt::filter_map(
                BroadcastStream::new(flashblocks_state.subscribe_to_flashblocks()),
                move |result| {
                    let pending_blocks = match result {
                        Ok(blocks) => blocks,
                        Err(err) => {
                            error!(
                                message = "Error in flashblocks stream for filtered transactions",
                                error = %err
                            );
                            return None;
                        }
                    };
                    let txs = pending_blocks
                        .get_latest_flashblock_transactions_with_logs_filtered(&filter);
                    if txs.is_empty() { None } else { Some(txs) }
                },
            ),
            stream::iter,
        )
    }

    /// Returns a stream that yields individual transaction hashes from only the latest flashblock.
    ///
    /// Each hash is emitted as a separate stream item (one hash per WebSocket message).
    /// Only hashes from the most recent flashblock are emitted to avoid duplicates.
    fn new_flashblock_transactions_hash_stream(
        flashblocks_state: Arc<FB>,
    ) -> impl Stream<Item = B256>
    where
        FB: FlashblocksAPI + Send + Sync + 'static,
    {
        futures::StreamExt::flat_map(
            StreamExt::filter_map(
                BroadcastStream::new(flashblocks_state.subscribe_to_flashblocks()),
                |result| {
                    let pending_blocks = match result {
                        Ok(blocks) => blocks,
                        Err(err) => {
                            error!(
                                message = "Error in flashblocks stream for transaction hashes",
                                error = %err
                            );
                            return None;
                        }
                    };
                    let hashes = pending_blocks.get_latest_flashblock_transaction_hashes();
                    if hashes.is_empty() { None } else { Some(hashes) }
                },
            ),
            stream::iter,
        )
    }
}

fn flashblock_logs_filter_from_params(
    params: Option<Params>,
    subscription_name: &str,
) -> Result<Option<Filter>, ErrorObjectOwned> {
    match params {
        None | Some(Params::None) => Ok(None),
        Some(Params::Logs(filter)) => Ok(Some(*filter)),
        Some(_) => Err(ErrorObjectOwned::owned(
            INVALID_PARAMS_CODE,
            format!(
                "invalid params for {subscription_name}: expected omitted/null params or a logs filter object"
            ),
            None::<()>,
        )),
    }
}

#[async_trait]
impl<Eth, FB> EthPubSubApiServer for EthPubSub<Eth, FB>
where
    Eth: RpcNodeCore + EthApiTypes + Clone + Send + Sync + 'static,
    RethEthPubSub<Eth>: RethEthPubSubApiServer<RpcTransaction<Eth::NetworkTypes>>,
    FB: FlashblocksAPI + Send + Sync + 'static,
{
    /// Handler for `eth_subscribe`
    ///
    /// Routes standard subscription types to reth's implementation and handles
    /// flashblocks subscriptions directly.
    async fn subscribe(
        &self,
        pending: PendingSubscriptionSink,
        kind: ExtendedSubscriptionKind,
        params: Option<Params>,
    ) -> SubscriptionResult {
        // For standard subscription types, delegate to reth's implementation
        if let Some(standard_kind) = kind.as_standard() {
            return RethEthPubSubApiServer::subscribe(&self.inner, pending, standard_kind, params)
                .await;
        }

        // Handle flashblocks-specific subscriptions
        let ExtendedSubscriptionKind::Base(base_kind) = kind else {
            unreachable!("Standard subscription types should be delegated to inner");
        };

        match base_kind {
            BaseSubscriptionKind::NewFastFlashblockLogs => {
                let filter =
                    match flashblock_logs_filter_from_params(params, "newFastFlashblockLogs") {
                        Ok(filter) => filter,
                        Err(err) => {
                            pending.reject(err).await;
                            return Ok(());
                        }
                    };
                let receiver = self.flashblocks_state.subscribe_to_fast_flashblock_logs();
                let sink = pending.accept().await?;

                tokio::spawn(async move {
                    pipe_fast_flashblock_logs_subscription(sink, receiver, filter).await;
                });
            }
            BaseSubscriptionKind::NewFlashblockLogsBatch => {
                let filter =
                    match flashblock_logs_filter_from_params(params, "newFlashblockLogsBatch") {
                        Ok(filter) => filter,
                        Err(err) => {
                            pending.reject(err).await;
                            return Ok(());
                        }
                    };
                if matches!(self.flashblocks_state.mode(), crate::FlashblocksMode::HotOnly) {
                    let receiver = self.flashblocks_state.subscribe_to_fast_flashblock_logs();
                    let sink = pending.accept().await?;

                    tokio::spawn(async move {
                        pipe_flashblock_logs_batch_from_fast_delta_subscription(
                            sink, receiver, filter,
                        )
                        .await;
                    });
                } else {
                    let sink = pending.accept().await?;
                    let flashblocks_state = Arc::clone(&self.flashblocks_state);

                    tokio::spawn(async move {
                        pipe_flashblock_logs_batch_subscription(sink, flashblocks_state, filter)
                            .await;
                    });
                }
            }
            BaseSubscriptionKind::NewFlashblocks => {
                if matches!(self.flashblocks_state.mode(), crate::FlashblocksMode::HotOnly) {
                    pending
                        .reject(unsupported_in_hot_only("eth_subscribe(\"newFlashblocks\")", None))
                        .await;
                    return Ok(());
                }

                let sink = pending.accept().await?;
                let stream = Self::new_flashblocks_stream(Arc::clone(&self.flashblocks_state));

                tokio::spawn(async move {
                    pipe_flashblocks_stream(sink, stream).await;
                });
            }
            BaseSubscriptionKind::PendingLogs => {
                if matches!(self.flashblocks_state.mode(), crate::FlashblocksMode::HotOnly) {
                    pending
                        .reject(unsupported_in_hot_only("eth_subscribe(\"pendingLogs\")", None))
                        .await;
                    return Ok(());
                }

                let sink = pending.accept().await?;
                // Extract filter from params, default to empty filter (match all)
                let filter = match params {
                    Some(Params::Logs(filter)) => *filter,
                    _ => Filter::default(),
                };

                let stream = Self::pending_logs_stream(Arc::clone(&self.flashblocks_state), filter);

                tokio::spawn(async move {
                    pipe_from_stream(sink, stream).await;
                });
            }
            BaseSubscriptionKind::NewFlashblockTransactions => match params {
                _ if matches!(self.flashblocks_state.mode(), crate::FlashblocksMode::HotOnly) => {
                    pending
                        .reject(unsupported_in_hot_only(
                            "eth_subscribe(\"newFlashblockTransactions\")",
                            None,
                        ))
                        .await;
                    return Ok(());
                }
                Some(Params::Logs(filter)) => {
                    let sink = pending.accept().await?;
                    let stream = Self::new_flashblock_transactions_filtered_stream(
                        Arc::clone(&self.flashblocks_state),
                        *filter,
                    );
                    tokio::spawn(async move {
                        pipe_from_stream(sink, stream).await;
                    });
                }
                Some(Params::Bool(true)) => {
                    let sink = pending.accept().await?;
                    let stream = Self::new_flashblock_transactions_full_stream(Arc::clone(
                        &self.flashblocks_state,
                    ));
                    tokio::spawn(async move {
                        pipe_from_stream(sink, stream).await;
                    });
                }
                _ => {
                    let sink = pending.accept().await?;
                    let stream = Self::new_flashblock_transactions_hash_stream(Arc::clone(
                        &self.flashblocks_state,
                    ));
                    tokio::spawn(async move {
                        pipe_from_stream(sink, stream).await;
                    });
                }
            },
        }

        Ok(())
    }
}

async fn pipe_flashblock_logs_batch_from_fast_delta_subscription(
    sink: SubscriptionSink,
    mut receiver: broadcast::Receiver<FastFlashblockFeedEvent>,
    filter: Option<Filter>,
) {
    loop {
        tokio::select! {
            _ = sink.closed() => return,
            result = receiver.recv() => {
                let event = match result {
                    Ok(event) => event,
                    Err(RecvError::Closed) => return,
                    Err(RecvError::Lagged(skipped)) => {
                        error!(
                            target: "flashblocks_rpc::pubsub",
                            skipped,
                            "closing newFlashblockLogsBatch subscription after broadcast lag"
                        );
                        return;
                    }
                };
                let delta = match event {
                    FastFlashblockFeedEvent::Delta(delta) => delta,
                    FastFlashblockFeedEvent::Resync => continue,
                    FastFlashblockFeedEvent::InvalidateSession => return,
                };

                let batch = base_metrics::time!(Metrics::logs_batch_build_duration(), {
                    filter.as_ref().map_or_else(
                        || FlashblockLogsBatch::from_fast_delta(delta.as_ref()),
                        |filter| FlashblockLogsBatch::from_fast_delta(&delta.filtered(filter)),
                    )
                });

                if !send_subscription_item(&sink, &batch, Some(PubSubMetric::LogsBatch)).await {
                    return;
                }
            }
        }
    }
}

async fn pipe_flashblock_logs_batch_subscription<FB>(
    sink: SubscriptionSink,
    flashblocks_state: Arc<FB>,
    filter: Option<Filter>,
) where
    FB: FlashblocksAPI + Send + Sync + 'static,
{
    let mut receiver = flashblocks_state.subscribe_to_flashblocks();

    loop {
        tokio::select! {
            _ = sink.closed() => return,
            result = receiver.recv() => {
                let pending_blocks = match result {
                    Ok(pending_blocks) => pending_blocks,
                    Err(RecvError::Closed) => return,
                    Err(RecvError::Lagged(skipped)) => {
                        error!(
                            target: "flashblocks_rpc::pubsub",
                            skipped,
                            "closing newFlashblockLogsBatch subscription after broadcast lag"
                        );
                        return;
                    }
                };

                let batch = pending_blocks.get_latest_flashblock_logs_batch(filter.as_ref());
                if !send_subscription_item(
                    &sink,
                    &batch,
                    Some(PubSubMetric::LogsBatch),
                )
                .await
                {
                    return;
                }
            }
        }
    }
}

async fn pipe_fast_flashblock_logs_subscription(
    sink: SubscriptionSink,
    mut receiver: broadcast::Receiver<FastFlashblockFeedEvent>,
    filter: Option<Filter>,
) {
    loop {
        tokio::select! {
            _ = sink.closed() => return,
            result = receiver.recv() => {
                let event = match result {
                    Ok(event) => event,
                    Err(RecvError::Closed) => return,
                    Err(RecvError::Lagged(skipped)) => {
                        error!(
                            target: "flashblocks_rpc::pubsub",
                            skipped,
                            "closing newFastFlashblockLogs subscription after broadcast lag"
                        );
                        return;
                    }
                };
                let delta = match event {
                    FastFlashblockFeedEvent::Delta(delta) => delta,
                    FastFlashblockFeedEvent::Resync => continue,
                    FastFlashblockFeedEvent::InvalidateSession => return,
                };

                if let Some(filter) = filter.as_ref() {
                    let filtered_delta = delta.filtered(filter);
                    if !send_subscription_item(&sink, &filtered_delta, Some(PubSubMetric::FastLogs)).await {
                        return;
                    }
                } else if !send_subscription_item(&sink, delta.as_ref(), Some(PubSubMetric::FastLogs)).await {
                    return;
                }
            }
        }
    }
}

async fn pipe_flashblocks_stream<St>(sink: SubscriptionSink, mut stream: St)
where
    St: Stream<Item = RpcBlock<Base>> + Unpin,
{
    loop {
        tokio::select! {
            _ = sink.closed() => return,

            maybe_item = stream.next() => {
                let Some(item) = maybe_item else {
                    return;
                };

                if !send_subscription_item(&sink, &item, None).await {
                    return;
                }
            }
        }
    }
}

async fn send_subscription_item<T>(
    sink: &SubscriptionSink,
    item: &T,
    metric: Option<PubSubMetric>,
) -> bool
where
    T: Serialize,
{
    let msg = match metric {
        Some(PubSubMetric::FastLogs) => {
            match base_metrics::time!(Metrics::fast_pubsub_serialize_duration(), {
                SubscriptionMessage::new(sink.method_name(), sink.subscription_id(), item)
            }) {
                Ok(msg) => msg,
                Err(err) => {
                    error!(
                        target: "flashblocks_rpc::pubsub",
                        %err,
                        "Failed to serialize subscription message"
                    );
                    return false;
                }
            }
        }
        Some(PubSubMetric::LogsBatch) => {
            match base_metrics::time!(Metrics::logs_batch_pubsub_serialize_duration(), {
                SubscriptionMessage::new(sink.method_name(), sink.subscription_id(), item)
            }) {
                Ok(msg) => msg,
                Err(err) => {
                    error!(
                        target: "flashblocks_rpc::pubsub",
                        %err,
                        "failed to serialize newFlashblockLogsBatch subscription message"
                    );
                    return false;
                }
            }
        }
        None => match SubscriptionMessage::new(sink.method_name(), sink.subscription_id(), item) {
            Ok(msg) => msg,
            Err(err) => {
                error!(
                    target: "flashblocks_rpc::pubsub",
                    %err,
                    "Failed to serialize subscription message"
                );
                return false;
            }
        },
    };

    let send_result = match metric {
        Some(PubSubMetric::FastLogs) => {
            base_metrics::time!(Metrics::fast_pubsub_send_duration(), { sink.send(msg).await })
        }
        Some(PubSubMetric::LogsBatch) => {
            base_metrics::time!(Metrics::logs_batch_pubsub_send_duration(), {
                sink.send(msg).await
            })
        }
        None => sink.send(msg).await,
    };

    send_result.is_ok()
}

/// Pipes all stream items to the subscription sink.
///
/// This function runs until the stream ends, the client disconnects, or a serialization error occurs.
/// All exit conditions result in graceful termination.
async fn pipe_from_stream<T, St>(sink: SubscriptionSink, mut stream: St)
where
    St: Stream<Item = T> + Unpin,
    T: Serialize,
{
    loop {
        tokio::select! {
            // dropped by client
            _ = sink.closed() => return,

            maybe_item = stream.next() => {
                // stream ended
                let Some(item) = maybe_item else {
                    return;
                };

                // if it fails, client disconnected
                if !send_subscription_item(&sink, &item, None).await {
                    return;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use alloy_primitives::{Address, B256};
    use alloy_rpc_types_engine::PayloadId;
    use jsonrpsee::{
        RpcModule,
        core::{EmptyServerParams, SubscriptionResult},
    };
    use serde_json::Value;
    use tokio::{sync::broadcast, time::timeout};

    use super::*;
    use crate::{FastFlashblockLogsDelta, FlashblockSnapshotId};

    #[tokio::test]
    async fn fast_flashblock_update_resync_keeps_fast_subscription_open() {
        let (sender, _) = broadcast::channel(4);
        let mut module = RpcModule::new(());
        module
            .register_subscription::<SubscriptionResult, _, _>(
                "fast_flashblock_update_resync",
                "fast_flashblock_update_resync",
                "fast_flashblock_update_resync_unsubscribe",
                {
                    let sender = sender.clone();
                    move |_, pending, _, _| {
                        let sender = sender.clone();
                        async move {
                            let receiver = sender.subscribe();
                            let sink = pending.accept().await?;
                            pipe_fast_flashblock_logs_subscription(sink, receiver, None).await;
                            Ok(())
                        }
                    }
                },
            )
            .unwrap();

        let mut subscription = module
            .subscribe_unbounded("fast_flashblock_update_resync", EmptyServerParams::new())
            .await
            .unwrap();

        sender.send(FastFlashblockFeedEvent::Resync).unwrap();
        sender.send(FastFlashblockFeedEvent::Delta(Arc::new(test_fast_delta()))).unwrap();

        let next = timeout(Duration::from_secs(1), subscription.next::<Value>()).await.unwrap();
        let (value, _) = next.expect("subscription should stay open after resync").unwrap();
        assert_eq!(value["blockNumber"], "0x1");
    }

    #[tokio::test]
    async fn fast_flashblock_update_invalidate_session_closes_fast_subscription() {
        let (sender, _) = broadcast::channel(4);
        let mut module = RpcModule::new(());
        module
            .register_subscription::<SubscriptionResult, _, _>(
                "fast_flashblock_update_invalidate_session",
                "fast_flashblock_update_invalidate_session",
                "fast_flashblock_update_invalidate_session_unsubscribe",
                {
                    let sender = sender.clone();
                    move |_, pending, _, _| {
                        let sender = sender.clone();
                        async move {
                            let receiver = sender.subscribe();
                            let sink = pending.accept().await?;
                            pipe_fast_flashblock_logs_subscription(sink, receiver, None).await;
                            Ok(())
                        }
                    }
                },
            )
            .unwrap();

        let mut subscription = module
            .subscribe_unbounded(
                "fast_flashblock_update_invalidate_session",
                EmptyServerParams::new(),
            )
            .await
            .unwrap();

        sender.send(FastFlashblockFeedEvent::InvalidateSession).unwrap();
        sender.send(FastFlashblockFeedEvent::Delta(Arc::new(test_fast_delta()))).unwrap();

        let next = timeout(Duration::from_secs(1), subscription.next::<Value>()).await.unwrap();
        assert!(next.is_none(), "subscription should close after session invalidation");
    }

    #[tokio::test]
    async fn fast_flashblock_update_resync_keeps_logs_batch_subscription_open() {
        let (sender, _) = broadcast::channel(4);
        let mut module = RpcModule::new(());
        module
            .register_subscription::<SubscriptionResult, _, _>(
                "logs_batch_update_resync",
                "logs_batch_update_resync",
                "logs_batch_update_resync_unsubscribe",
                {
                    let sender = sender.clone();
                    move |_, pending, _, _| {
                        let sender = sender.clone();
                        async move {
                            let receiver = sender.subscribe();
                            let sink = pending.accept().await?;
                            pipe_flashblock_logs_batch_from_fast_delta_subscription(
                                sink, receiver, None,
                            )
                            .await;
                            Ok(())
                        }
                    }
                },
            )
            .unwrap();

        let mut subscription = module
            .subscribe_unbounded("logs_batch_update_resync", EmptyServerParams::new())
            .await
            .unwrap();

        sender.send(FastFlashblockFeedEvent::Resync).unwrap();
        sender.send(FastFlashblockFeedEvent::Delta(Arc::new(test_fast_delta()))).unwrap();

        let next = timeout(Duration::from_secs(1), subscription.next::<Value>()).await.unwrap();
        let (value, _) = next.expect("subscription should stay open after resync").unwrap();
        assert_eq!(value["blockNumber"], "0x1");
    }

    #[tokio::test]
    async fn invalidate_session_closes_logs_batch_subscription() {
        let (sender, _) = broadcast::channel(4);
        let mut module = RpcModule::new(());
        module
            .register_subscription::<SubscriptionResult, _, _>(
                "logs_batch_update_invalidate_session",
                "logs_batch_update_invalidate_session",
                "logs_batch_update_invalidate_session_unsubscribe",
                {
                    let sender = sender.clone();
                    move |_, pending, _, _| {
                        let sender = sender.clone();
                        async move {
                            let receiver = sender.subscribe();
                            let sink = pending.accept().await?;
                            pipe_flashblock_logs_batch_from_fast_delta_subscription(
                                sink, receiver, None,
                            )
                            .await;
                            Ok(())
                        }
                    }
                },
            )
            .unwrap();

        let mut subscription = module
            .subscribe_unbounded("logs_batch_update_invalidate_session", EmptyServerParams::new())
            .await
            .unwrap();

        sender.send(FastFlashblockFeedEvent::InvalidateSession).unwrap();
        sender.send(FastFlashblockFeedEvent::Delta(Arc::new(test_fast_delta()))).unwrap();

        let next = timeout(Duration::from_secs(1), subscription.next::<Value>()).await.unwrap();
        assert!(next.is_none(), "subscription should close after session invalidation");
    }

    fn test_fast_delta() -> FastFlashblockLogsDelta {
        FastFlashblockLogsDelta::new(
            FlashblockSnapshotId::new(7, 1, 0, PayloadId::new([2; 8]), B256::with_last_byte(3)),
            Some(4),
            Vec::new(),
            Vec::new(),
        )
    }

    #[test]
    fn logs_batch_filter_accepts_missing_params() {
        assert!(
            flashblock_logs_filter_from_params(None, "newFlashblockLogsBatch").unwrap().is_none()
        );
    }

    #[test]
    fn logs_batch_filter_accepts_null_params() {
        assert!(
            flashblock_logs_filter_from_params(Some(Params::None), "newFlashblockLogsBatch")
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn logs_batch_filter_accepts_logs_filter() {
        let filter = Filter::new().address(Address::with_last_byte(1));
        let parsed = flashblock_logs_filter_from_params(
            Some(Params::Logs(Box::new(filter.clone()))),
            "newFlashblockLogsBatch",
        )
        .unwrap()
        .unwrap();
        assert_eq!(parsed, filter);
    }

    #[test]
    fn logs_batch_filter_rejects_bool_params() {
        let err =
            flashblock_logs_filter_from_params(Some(Params::Bool(true)), "newFlashblockLogsBatch")
                .unwrap_err();
        assert!(err.to_string().contains("newFlashblockLogsBatch"));
    }

    #[test]
    fn fast_logs_filter_rejects_bool_params() {
        let err =
            flashblock_logs_filter_from_params(Some(Params::Bool(true)), "newFastFlashblockLogs")
                .unwrap_err();
        assert!(err.to_string().contains("newFastFlashblockLogs"));
    }
}
