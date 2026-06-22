//! Traits for the Flashblocks module.

use std::sync::Arc;

use alloy_eips::BlockNumberOrTag;
use alloy_primitives::{Address, TxHash, U256};
use alloy_rpc_types_eth::{Filter, Log, state::StateOverride};
use arc_swap::Guard;
use base_common_flashblocks::Flashblock;
use base_common_network::Base;
use reth_rpc_convert::RpcTransaction;
use reth_rpc_eth_api::{RpcBlock, RpcReceipt};
use tokio::sync::broadcast;

use crate::{
    FastFlashblockFeedEvent, FlashblockSnapshotId, FlashblocksMode, HotDryRunSeed, HotSnapshot,
    PendingBlocks,
};

/// Trait for receiving flashblock updates.
pub trait FlashblocksReceiver {
    /// Called when a new flashblock is received.
    fn on_flashblock_received(&self, flashblock: Flashblock);
}

/// Core API for accessing flashblock state and data.
pub trait FlashblocksAPI {
    /// Retrieves the pending blocks.
    fn get_pending_blocks(&self) -> Guard<Option<Arc<PendingBlocks>>>;

    /// Subscribes to fast flashblock log feed events.
    fn subscribe_to_fast_flashblock_logs(&self) -> broadcast::Receiver<FastFlashblockFeedEvent>;

    /// Returns a cached pending snapshot for a previously emitted fast delta.
    ///
    /// This lookup is best effort. After pending-state reset, cache clear, or cache eviction, a
    /// previously buffered fast update may no longer resolve even if the update itself is still
    /// readable from the broadcast channel. Callers must treat `None` as a resync signal.
    fn get_snapshot(&self, snapshot_id: FlashblockSnapshotId) -> Option<Arc<PendingBlocks>>;

    /// Returns a cached hot snapshot for pinned flashblock RPC.
    fn get_hot_snapshot(&self, snapshot_id: FlashblockSnapshotId) -> Option<Arc<HotSnapshot>>;

    /// Returns the most recently cached hot snapshot for latest dry-run RPC.
    fn get_latest_hot_snapshot(&self) -> Option<Arc<HotSnapshot>>;

    /// Returns the most recently cached hot dry-run seed for latest dry-run RPC.
    fn get_latest_hot_dry_run_seed(&self) -> Option<Arc<HotDryRunSeed>>;

    /// Returns the configured flashblocks runtime mode.
    fn mode(&self) -> FlashblocksMode;

    /// Subscribes to flashblock updates.
    fn subscribe_to_flashblocks(&self) -> broadcast::Receiver<Arc<PendingBlocks>>;
}

/// API for accessing pending blocks data.
pub trait PendingBlocksAPI {
    /// Get the canonical block number on top of which all pending state is built
    fn get_canonical_block_number(&self) -> BlockNumberOrTag;

    /// Get the pending transactions count for an address
    fn get_transaction_count(&self, address: Address) -> U256;

    /// Retrieves the current block. If `full` is true, includes full transaction details.
    fn get_block(&self, full: bool) -> Option<RpcBlock<Base>>;

    /// Gets transaction receipt by hash.
    fn get_transaction_receipt(&self, tx_hash: TxHash) -> Option<RpcReceipt<Base>>;

    /// Gets transaction details by hash.
    fn get_transaction_by_hash(&self, tx_hash: TxHash) -> Option<RpcTransaction<Base>>;

    /// Gets balance for an address. Returns None if address not updated in flashblocks.
    fn get_balance(&self, address: Address) -> Option<U256>;

    /// Gets the state overrides for the pending blocks
    fn get_state_overrides(&self) -> Option<StateOverride>;

    /// Gets logs from pending state matching the provided filter.
    fn get_pending_logs(&self, filter: &Filter) -> Vec<Log>;
}
