//! Flashblocks state management.

use std::{
    sync::{Arc, Mutex as StdMutex},
    time::{Duration, Instant},
};

use alloy_consensus::Header;
use arc_swap::{ArcSwapOption, Guard};
use base_common_chains::Upgrades;
use base_common_consensus::BaseBlock;
use base_common_flashblocks::Flashblock;
use reth_chainspec::{ChainSpecProvider, EthChainSpec};
use reth_primitives::RecoveredBlock;
use reth_provider::{BlockReaderIdExt, StateProviderFactory};
use tokio::sync::{
    Mutex,
    broadcast::{self, Sender},
    mpsc,
};

use crate::{
    FastFlashblockFeedEvent, FlashblockSnapshotId, FlashblocksAPI, FlashblocksMode,
    FlashblocksReceiver, HotDryRunSeed, HotSnapshot, HotSnapshotRing, LatestHotDryRunSeedCache,
    PendingBlocks, SnapshotCache,
    processor::{StateProcessor, StateProcessorHandles, StateUpdate},
    snapshot_cache::{DEFAULT_SNAPSHOT_CACHE_CAPACITY, DEFAULT_SNAPSHOT_CACHE_TTL},
};

const FLASHBLOCK_BROADCAST_BUFFER_CAPACITY: usize = 20;

// Keep snapshot retention at least as deep as the fast broadcast ring.
const SNAPSHOT_CACHE_CAPACITY: usize =
    if DEFAULT_SNAPSHOT_CACHE_CAPACITY < FLASHBLOCK_BROADCAST_BUFFER_CAPACITY {
        FLASHBLOCK_BROADCAST_BUFFER_CAPACITY
    } else {
        DEFAULT_SNAPSHOT_CACHE_CAPACITY
    };

const SNAPSHOT_CACHE_RETENTION_TTL: Duration = DEFAULT_SNAPSHOT_CACHE_TTL;

/// Manages the pending flashblock state and processes incoming updates.
#[derive(Debug)]
pub struct FlashblocksState {
    pending_blocks: Arc<ArcSwapOption<PendingBlocks>>,
    queue: mpsc::UnboundedSender<StateUpdate>,
    rx: Arc<Mutex<mpsc::UnboundedReceiver<StateUpdate>>>,
    fast_flashblock_sender: Sender<FastFlashblockFeedEvent>,
    flashblock_sender: Sender<Arc<PendingBlocks>>,
    snapshot_cache: Arc<StdMutex<SnapshotCache>>,
    hot_snapshot_ring: Arc<StdMutex<HotSnapshotRing>>,
    latest_hot_dry_run_seed: Arc<LatestHotDryRunSeedCache>,
    max_pending_blocks_depth: u64,
    mode: FlashblocksMode,
}

impl FlashblocksState {
    /// Creates a new flashblocks state manager using the legacy runtime mode.
    ///
    /// The state is created without a client. Call [`start`](Self::start) with a client
    /// to spawn the state processor after the node is launched.
    pub fn new(max_pending_blocks_depth: u64) -> Self {
        Self::new_with_mode(max_pending_blocks_depth, FlashblocksMode::Legacy)
    }

    /// Creates a new flashblocks state manager with an explicit runtime mode.
    pub fn new_with_mode(max_pending_blocks_depth: u64, mode: FlashblocksMode) -> Self {
        let (tx, rx) = mpsc::unbounded_channel::<StateUpdate>();
        let pending_blocks: Arc<ArcSwapOption<PendingBlocks>> = Arc::new(ArcSwapOption::new(None));
        let (fast_flashblock_sender, _) = broadcast::channel(FLASHBLOCK_BROADCAST_BUFFER_CAPACITY);
        let (flashblock_sender, _) = broadcast::channel(FLASHBLOCK_BROADCAST_BUFFER_CAPACITY);

        Self {
            pending_blocks,
            queue: tx,
            rx: Arc::new(Mutex::new(rx)),
            fast_flashblock_sender,
            flashblock_sender,
            snapshot_cache: Arc::new(StdMutex::new(SnapshotCache::new(
                SNAPSHOT_CACHE_CAPACITY,
                SNAPSHOT_CACHE_RETENTION_TTL,
            ))),
            hot_snapshot_ring: Arc::new(StdMutex::new(HotSnapshotRing::new(
                SNAPSHOT_CACHE_CAPACITY,
            ))),
            latest_hot_dry_run_seed: Arc::new(LatestHotDryRunSeedCache::default()),
            max_pending_blocks_depth,
            mode,
        }
    }

    /// Returns the configured flashblocks runtime mode.
    pub const fn mode(&self) -> FlashblocksMode {
        self.mode
    }

    /// Returns the shared hot snapshot ring used by the processor and RPC readers.
    pub fn hot_snapshot_ring(&self) -> Arc<StdMutex<HotSnapshotRing>> {
        Arc::clone(&self.hot_snapshot_ring)
    }

    /// Starts the flashblocks state processor with the given client.
    ///
    /// This spawns a background task that processes canonical blocks and flashblocks.
    /// Should be called after the node is launched and the provider is available.
    pub fn start<Client>(&self, client: Client)
    where
        Client: StateProviderFactory
            + ChainSpecProvider<ChainSpec: EthChainSpec<Header = Header> + Upgrades>
            + BlockReaderIdExt<Header = Header>
            + Clone
            + 'static,
    {
        let state_processor = StateProcessor::new_with_mode(
            client,
            Arc::clone(&self.pending_blocks),
            self.max_pending_blocks_depth,
            self.mode,
            StateProcessorHandles::new(
                Arc::clone(&self.rx),
                self.fast_flashblock_sender.clone(),
                self.flashblock_sender.clone(),
                Arc::clone(&self.snapshot_cache),
                Arc::clone(&self.hot_snapshot_ring),
                Arc::clone(&self.latest_hot_dry_run_seed),
            ),
        );

        tokio::spawn(async move {
            state_processor.start().await;
        });
    }

    /// Handles a canonical block being received.
    pub fn on_canonical_block_received(&self, block: RecoveredBlock<BaseBlock>) {
        let block_number = block.number;
        match self.queue.send(StateUpdate::Canonical(block)) {
            Ok(_) => {
                info!(message = "added canonical block to processing queue", block_number)
            }
            Err(e) => {
                error!(message = "could not add canonical block to processing queue", block_number, error = %e);
            }
        }
    }
}

impl FlashblocksReceiver for FlashblocksState {
    fn on_flashblock_received(&self, flashblock: Flashblock) {
        let flashblock_index = flashblock.index;
        let block_number = flashblock.metadata.block_number;
        let enqueued_at = Instant::now();
        match self.queue.send(StateUpdate::Flashblock { flashblock, enqueued_at }) {
            Ok(_) => {
                debug!(
                    message = "added flashblock to processing queue",
                    block_number, flashblock_index,
                );
            }
            Err(e) => {
                error!(message = "could not add flashblock to processing queue", block_number, flashblock_index, error = %e);
            }
        }
    }
}

impl Default for FlashblocksState {
    fn default() -> Self {
        Self::new(10)
    }
}

impl FlashblocksAPI for FlashblocksState {
    fn get_pending_blocks(&self) -> Guard<Option<Arc<PendingBlocks>>> {
        self.pending_blocks.load()
    }

    fn subscribe_to_fast_flashblock_logs(&self) -> broadcast::Receiver<FastFlashblockFeedEvent> {
        self.fast_flashblock_sender.subscribe()
    }

    fn get_snapshot(&self, snapshot_id: FlashblockSnapshotId) -> Option<Arc<PendingBlocks>> {
        self.snapshot_cache.lock().expect("snapshot cache mutex poisoned").get(&snapshot_id)
    }

    fn get_hot_snapshot(&self, snapshot_id: FlashblockSnapshotId) -> Option<Arc<HotSnapshot>> {
        if self.mode != FlashblocksMode::HotOnly {
            return None;
        }

        self.hot_snapshot_ring.lock().expect("hot snapshot ring mutex poisoned").get(snapshot_id)
    }

    fn get_latest_hot_snapshot(&self) -> Option<Arc<HotSnapshot>> {
        if self.mode != FlashblocksMode::HotOnly {
            return None;
        }

        self.hot_snapshot_ring.lock().expect("hot snapshot ring mutex poisoned").latest()
    }

    fn get_latest_hot_dry_run_seed(&self) -> Option<Arc<HotDryRunSeed>> {
        if self.mode != FlashblocksMode::HotOnly {
            return None;
        }

        self.latest_hot_dry_run_seed.latest()
    }

    fn mode(&self) -> FlashblocksMode {
        self.mode
    }

    fn subscribe_to_flashblocks(&self) -> broadcast::Receiver<Arc<PendingBlocks>> {
        self.flashblock_sender.subscribe()
    }
}

impl FlashblocksState {
    /// Sets the pending blocks directly for testing purposes.
    ///
    /// This bypasses the normal flashblock processing pipeline and allows
    /// tests to inject a pre-built `PendingBlocks` state.
    pub fn set_pending_blocks_for_testing(&self, pending_blocks: Option<PendingBlocks>) {
        self.snapshot_cache.lock().expect("snapshot cache mutex poisoned").clear();
        self.pending_blocks.store(pending_blocks.map(Arc::new));
    }

    #[cfg(test)]
    /// Clears the latest hot dry-run seed cache in tests.
    pub fn clear_latest_hot_dry_run_seed_for_test(&self) {
        self.latest_hot_dry_run_seed.clear();
    }

    #[cfg(test)]
    /// Publishes a latest hot dry-run seed in tests.
    pub fn publish_latest_hot_dry_run_seed_for_test(&self, seed: Arc<HotDryRunSeed>) {
        self.latest_hot_dry_run_seed.publish(seed);
    }
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::Duration};

    use alloy_primitives::{Address, B256, Bloom, Bytes, U256, hex_literal::hex};
    use alloy_rpc_types_engine::PayloadId;
    use base_common_consensus::BasePrimitives;
    use base_common_flashblocks::{
        ExecutionPayloadBaseV1, ExecutionPayloadFlashblockDeltaV1, Flashblock, Metadata,
    };
    use base_execution_chainspec::{BaseChainSpec, BaseChainSpecBuilder};
    use reth_chainspec::ChainSpecProvider;
    use reth_provider::test_utils::MockEthProvider;
    use tokio::{sync::broadcast, time::timeout};

    use super::*;
    use crate::{FastFlashblockFeedEvent, FlashblocksAPI, FlashblocksReceiver};

    const RECV_TIMEOUT: Duration = Duration::from_secs(1);

    fn test_client() -> MockEthProvider<BasePrimitives, Arc<BaseChainSpec>> {
        let chain_spec = Arc::new(BaseChainSpecBuilder::base_mainnet().build());
        MockEthProvider::<BasePrimitives>::new().with_chain_spec(chain_spec).with_genesis_block()
    }

    fn encoded_l1_info_tx() -> Bytes {
        Bytes::from_static(&hex!(
            "7ef9015aa044bae9d41b8380d781187b426c6fe43df5fb2fb57bd4466ef6a701e1f01e015694deaddeaddeaddeaddeaddeaddeaddeaddead000194420000000000000000000000000000000000001580808408f0d18001b90104015d8eb900000000000000000000000000000000000000000000000000000000008057650000000000000000000000000000000000000000000000000000000063d96d10000000000000000000000000000000000000000000000000000000000009f35273d89754a1e0387b89520d989d3be9c37c1f32495a88faf1ea05c61121ab0d1900000000000000000000000000000000000000000000000000000000000000010000000000000000000000002d679b567db6187c0c8323fa982cfb88b74dbcc7000000000000000000000000000000000000000000000000000000000000083400000000000000000000000000000000000000000000000000000000000f4240"
        ))
    }

    fn test_flashblock(
        index: u64,
        block_number: u64,
        payload_id: PayloadId,
        parent_hash: B256,
    ) -> Flashblock {
        Flashblock {
            payload_id,
            index,
            base: Some(ExecutionPayloadBaseV1 {
                parent_beacon_block_root: B256::ZERO,
                parent_hash,
                fee_recipient: Address::ZERO,
                prev_randao: B256::ZERO,
                block_number,
                gas_limit: 30_000_000,
                timestamp: 1_700_000_000 + block_number,
                extra_data: Bytes::default(),
                base_fee_per_gas: U256::from(1_000_000_000u64),
            }),
            diff: ExecutionPayloadFlashblockDeltaV1 {
                state_root: B256::ZERO,
                receipts_root: B256::ZERO,
                logs_bloom: Bloom::default(),
                gas_used: 21_000,
                block_hash: fixture_wire_block_hash(block_number, index),
                transactions: vec![encoded_l1_info_tx()],
                withdrawals: vec![],
                withdrawals_root: B256::ZERO,
                blob_gas_used: None,
            },
            metadata: Metadata { block_number },
        }
    }

    fn fixture_wire_block_hash(block_number: u64, index: u64) -> B256 {
        let mut bytes = [0u8; 32];
        bytes[..8].copy_from_slice(&block_number.to_be_bytes());
        bytes[8..16].copy_from_slice(&index.to_be_bytes());
        bytes[31] = 1;
        B256::from(bytes)
    }

    async fn recv_fast_flashblock_event(
        receiver: &mut broadcast::Receiver<FastFlashblockFeedEvent>,
    ) -> FastFlashblockFeedEvent {
        timeout(RECV_TIMEOUT, receiver.recv())
            .await
            .expect("fast flashblock event should arrive")
            .expect("fast flashblock channel should stay open")
    }

    async fn recv_pending_blocks(
        receiver: &mut broadcast::Receiver<Arc<PendingBlocks>>,
    ) -> Arc<PendingBlocks> {
        timeout(RECV_TIMEOUT, receiver.recv())
            .await
            .expect("compat flashblock update should arrive")
            .expect("compat flashblock channel should stay open")
    }

    async fn wait_for_pending_clear(state: &FlashblocksState) {
        timeout(RECV_TIMEOUT, async {
            loop {
                if (*state.get_pending_blocks()).is_none() {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("pending state should clear");
    }

    async fn wait_for_hot_snapshot_clear(
        state: &FlashblocksState,
        snapshot_id: FlashblockSnapshotId,
    ) {
        timeout(RECV_TIMEOUT, async {
            loop {
                if state.get_hot_snapshot(snapshot_id).is_none() {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("hot snapshot should clear");
    }

    async fn wait_for_latest_hot_dry_run_seed_clear(state: &FlashblocksState) {
        timeout(RECV_TIMEOUT, async {
            loop {
                if state.get_latest_hot_dry_run_seed().is_none() {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("latest hot dry-run seed should clear");
    }

    #[test]
    fn flashblocks_state_new_defaults_to_legacy_mode() {
        let state = FlashblocksState::new(5);

        assert_eq!(state.mode(), FlashblocksMode::Legacy);
    }

    #[test]
    fn flashblocks_state_new_with_mode_stores_hot_mode() {
        let state = FlashblocksState::new_with_mode(5, FlashblocksMode::HotOnly);

        assert_eq!(state.mode(), FlashblocksMode::HotOnly);
    }

    #[tokio::test]
    async fn fast_flashblock_update_emits_snapshot_and_preserves_compatibility_broadcast() {
        let state = FlashblocksState::new(10);
        let mut fast_receiver = state.subscribe_to_fast_flashblock_logs();
        let mut compat_receiver = state.subscribe_to_flashblocks();

        state.start(test_client());

        let payload_id = PayloadId::new([0x11; 8]);
        let parent_hash = B256::with_last_byte(0x22);
        let flashblock = test_flashblock(0, 1, payload_id, parent_hash);
        state.on_flashblock_received(flashblock.clone());

        let update = recv_fast_flashblock_event(&mut fast_receiver).await;
        let compat_pending_blocks = recv_pending_blocks(&mut compat_receiver).await;

        let FastFlashblockFeedEvent::Delta(delta) = update else {
            panic!("expected fast flashblock delta event");
        };

        assert_eq!(delta.snapshot_id.block_number(), flashblock.metadata.block_number);
        assert_eq!(delta.snapshot_id.flashblock_index(), flashblock.index);
        assert_eq!(delta.snapshot_id.payload_id(), flashblock.payload_id);
        assert_eq!(delta.snapshot_id.parent_hash(), parent_hash);

        let snapshot = state
            .get_snapshot(delta.snapshot_id)
            .expect("snapshot id should resolve to cached pending blocks");
        assert!(Arc::ptr_eq(&snapshot, &compat_pending_blocks));
    }

    #[tokio::test]
    async fn fast_flashblock_update_duplicate_flashblock_does_not_emit_a_second_fast_update() {
        let state = FlashblocksState::new(10);
        let mut fast_receiver = state.subscribe_to_fast_flashblock_logs();
        let mut compat_receiver = state.subscribe_to_flashblocks();

        state.start(test_client());

        let flashblock =
            test_flashblock(0, 1, PayloadId::new([0x33; 8]), B256::with_last_byte(0x44));
        state.on_flashblock_received(flashblock.clone());

        let _ = recv_fast_flashblock_event(&mut fast_receiver).await;
        let _ = recv_pending_blocks(&mut compat_receiver).await;

        state.on_flashblock_received(flashblock);

        let _ = recv_pending_blocks(&mut compat_receiver).await;
        assert!(matches!(fast_receiver.try_recv(), Err(broadcast::error::TryRecvError::Empty)));
    }

    #[tokio::test]
    async fn fast_flashblock_update_does_not_cache_snapshots_without_fast_subscribers() {
        let state = FlashblocksState::new(10);
        let mut compat_receiver = state.subscribe_to_flashblocks();

        state.start(test_client());

        let payload_id = PayloadId::new([0x88; 8]);
        let parent_hash = B256::with_last_byte(0x99);
        let flashblock = test_flashblock(0, 1, payload_id, parent_hash);
        state.on_flashblock_received(flashblock.clone());

        let pending_blocks = recv_pending_blocks(&mut compat_receiver).await;

        let expected_snapshot_id = FlashblockSnapshotId::new(
            1,
            flashblock.metadata.block_number,
            flashblock.index,
            flashblock.payload_id,
            parent_hash,
        );

        assert_eq!(pending_blocks.latest_block_number(), flashblock.metadata.block_number);
        assert!(state.get_snapshot(expected_snapshot_id).is_none());
    }

    #[tokio::test]
    async fn hot_only_emits_fast_delta_without_populating_pending_blocks() {
        let state = FlashblocksState::new_with_mode(10, FlashblocksMode::HotOnly);
        let mut fast_receiver = state.subscribe_to_fast_flashblock_logs();
        let mut compat_receiver = state.subscribe_to_flashblocks();
        let client = test_client();
        let parent_hash = client.chain_spec().genesis_hash();

        state.start(client);

        let flashblock = test_flashblock(0, 1, PayloadId::new([0x42; 8]), parent_hash);
        state.on_flashblock_received(flashblock);

        let event = recv_fast_flashblock_event(&mut fast_receiver).await;
        let FastFlashblockFeedEvent::Delta(delta) = event else {
            panic!("expected hot-only fast flashblock delta event");
        };

        let latest = state
            .get_latest_hot_snapshot()
            .expect("latest hot snapshot should exist after fast delta");
        assert_eq!(latest.snapshot_id, delta.snapshot_id);
        assert!(state.get_hot_snapshot(delta.snapshot_id).is_some());
        assert!((*state.get_pending_blocks()).is_none());
        assert!(matches!(compat_receiver.try_recv(), Err(broadcast::error::TryRecvError::Empty)));
    }

    #[tokio::test]
    async fn hot_only_emits_resync_and_clears_hot_snapshots_after_non_cacheable_error() {
        let state = FlashblocksState::new_with_mode(10, FlashblocksMode::HotOnly);
        let mut fast_receiver = state.subscribe_to_fast_flashblock_logs();
        let client = test_client();
        let parent_hash = client.chain_spec().genesis_hash();

        state.start(client);

        let first_flashblock = test_flashblock(0, 1, PayloadId::new([0x51; 8]), parent_hash);
        state.on_flashblock_received(first_flashblock);

        let first_event = recv_fast_flashblock_event(&mut fast_receiver).await;
        let FastFlashblockFeedEvent::Delta(first_delta) = first_event else {
            panic!("expected initial hot-only fast flashblock delta event");
        };

        let mut malformed_flashblock =
            test_flashblock(1, 1, PayloadId::new([0x51; 8]), parent_hash);
        malformed_flashblock.diff.transactions = vec![Bytes::from_static(&[0x01])];
        state.on_flashblock_received(malformed_flashblock);

        let resync_event = recv_fast_flashblock_event(&mut fast_receiver).await;
        assert!(matches!(resync_event, FastFlashblockFeedEvent::Resync));

        wait_for_hot_snapshot_clear(&state, first_delta.snapshot_id).await;
    }

    #[tokio::test]
    async fn hot_only_missing_first_cache_reset_clears_stale_hot_snapshot_ring() {
        let state = FlashblocksState::new_with_mode(10, FlashblocksMode::HotOnly);
        let mut fast_receiver = state.subscribe_to_fast_flashblock_logs();
        let client = test_client();
        let parent_hash = client.chain_spec().genesis_hash();

        state.start(client);

        let first_flashblock = test_flashblock(0, 1, PayloadId::new([0x61; 8]), parent_hash);
        state.on_flashblock_received(first_flashblock);

        let first_event = recv_fast_flashblock_event(&mut fast_receiver).await;
        let FastFlashblockFeedEvent::Delta(first_delta) = first_event else {
            panic!("expected initial hot-only fast flashblock delta event");
        };
        let stale_snapshot = state
            .get_hot_snapshot(first_delta.snapshot_id)
            .expect("initial hot snapshot should be retained");

        let mut malformed_flashblock =
            test_flashblock(1, 1, PayloadId::new([0x61; 8]), parent_hash);
        malformed_flashblock.diff.transactions = vec![Bytes::from_static(&[0x01])];
        state.on_flashblock_received(malformed_flashblock);

        let resync_event = recv_fast_flashblock_event(&mut fast_receiver).await;
        assert!(matches!(resync_event, FastFlashblockFeedEvent::Resync));
        wait_for_hot_snapshot_clear(&state, first_delta.snapshot_id).await;

        state
            .hot_snapshot_ring()
            .lock()
            .expect("hot snapshot ring mutex poisoned")
            .insert(Arc::clone(&stale_snapshot));
        assert!(state.get_hot_snapshot(first_delta.snapshot_id).is_some());

        state.on_flashblock_received(test_flashblock(
            0,
            2,
            PayloadId::new([0x63; 8]),
            B256::with_last_byte(0x11),
        ));
        state.on_flashblock_received(test_flashblock(
            1,
            2,
            PayloadId::new([0x63; 8]),
            B256::with_last_byte(0x11),
        ));

        wait_for_hot_snapshot_clear(&state, first_delta.snapshot_id).await;
    }

    #[tokio::test]
    async fn hot_only_missing_first_non_cacheable_reset_emits_resync_and_clears_ring() {
        let state = FlashblocksState::new_with_mode(10, FlashblocksMode::HotOnly);
        let mut fast_receiver = state.subscribe_to_fast_flashblock_logs();
        let client = test_client();
        let parent_hash = client.chain_spec().genesis_hash();

        state.start(client);

        let first_flashblock = test_flashblock(0, 1, PayloadId::new([0x71; 8]), parent_hash);
        state.on_flashblock_received(first_flashblock);

        let first_event = recv_fast_flashblock_event(&mut fast_receiver).await;
        let FastFlashblockFeedEvent::Delta(first_delta) = first_event else {
            panic!("expected initial hot-only fast flashblock delta event");
        };
        let stale_snapshot = state
            .get_hot_snapshot(first_delta.snapshot_id)
            .expect("initial hot snapshot should be retained");
        let stale_seed = state
            .get_latest_hot_dry_run_seed()
            .expect("initial hot dry-run seed should be retained");

        let mut malformed_flashblock =
            test_flashblock(1, 1, PayloadId::new([0x71; 8]), parent_hash);
        malformed_flashblock.diff.transactions = vec![Bytes::from_static(&[0x01])];
        state.on_flashblock_received(malformed_flashblock);

        let resync_event = recv_fast_flashblock_event(&mut fast_receiver).await;
        assert!(matches!(resync_event, FastFlashblockFeedEvent::Resync));
        wait_for_hot_snapshot_clear(&state, first_delta.snapshot_id).await;
        wait_for_latest_hot_dry_run_seed_clear(&state).await;

        state
            .hot_snapshot_ring()
            .lock()
            .expect("hot snapshot ring mutex poisoned")
            .insert(Arc::clone(&stale_snapshot));
        state.publish_latest_hot_dry_run_seed_for_test(Arc::clone(&stale_seed));
        assert!(state.get_hot_snapshot(first_delta.snapshot_id).is_some());
        assert_eq!(
            state.get_latest_hot_dry_run_seed().as_ref().map(|seed| seed.snapshot_id),
            Some(stale_seed.snapshot_id)
        );

        state.on_flashblock_received(test_flashblock(
            1,
            2,
            PayloadId::new([0x73; 8]),
            B256::with_last_byte(0x22),
        ));

        let resync_event = recv_fast_flashblock_event(&mut fast_receiver).await;
        assert!(matches!(resync_event, FastFlashblockFeedEvent::Resync));
        wait_for_hot_snapshot_clear(&state, first_delta.snapshot_id).await;
        wait_for_latest_hot_dry_run_seed_clear(&state).await;
    }

    #[tokio::test]
    async fn snapshot_cache_may_not_resolve_buffered_fast_updates_after_reset() {
        let state = FlashblocksState::new(10);
        let mut fast_receiver = state.subscribe_to_fast_flashblock_logs();

        state.start(test_client());

        let parent_hash = B256::with_last_byte(0x66);
        let flashblock = test_flashblock(0, 1, PayloadId::new([0x55; 8]), parent_hash);
        state.on_flashblock_received(flashblock);

        let gap_flashblock = test_flashblock(2, 1, PayloadId::new([0x77; 8]), parent_hash);
        state.on_flashblock_received(gap_flashblock);

        wait_for_pending_clear(&state).await;

        let buffered_event = recv_fast_flashblock_event(&mut fast_receiver).await;
        let FastFlashblockFeedEvent::Delta(buffered_delta) = buffered_event else {
            panic!("expected buffered fast flashblock delta event");
        };
        assert!(state.get_snapshot(buffered_delta.snapshot_id).is_none());
    }
}
