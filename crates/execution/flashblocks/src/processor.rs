//! Flashblocks state processor.

use std::{
    collections::{BTreeMap, VecDeque},
    sync::{
        Arc, Mutex as StdMutex,
        atomic::{AtomicU64, Ordering},
    },
    time::Instant,
};

use alloy_consensus::{
    Header,
    transaction::{Recovered, SignerRecoverable},
};
use alloy_eips::BlockNumberOrTag;
use alloy_network::TransactionResponse;
use alloy_primitives::{Address, BlockNumber};
use alloy_rpc_types_eth::state::StateOverride;
use arc_swap::ArcSwapOption;
use base_common_chains::Upgrades;
use base_common_consensus::{BaseBlock, BaseTxEnvelope};
use base_common_flashblocks::Flashblock;
use base_execution_evm::{BaseEvmConfig, BaseNextBlockEnvAttributes};
use rayon::prelude::*;
use reth_chainspec::{ChainSpecProvider, EthChainSpec};
use reth_evm::ConfigureEvm;
use reth_primitives::RecoveredBlock;
use reth_provider::{BlockReaderIdExt, StateProviderFactory};
use reth_revm::{State, database::StateProviderDatabase};
use revm_database::states::bundle_state::BundleRetention;
use tokio::sync::{Mutex, broadcast::Sender, mpsc::UnboundedReceiver};

use crate::{
    BlockAssembler, CachedFlashblock, ExecutionError, FastFlashblockFeedEvent,
    FastFlashblockLogsDelta, FlashblockCache, FlashblocksMode, HotApplyOutcome, HotEngine,
    HotInvalidationReason, HotSnapshotRing, PendingBlocks, PendingBlocksBuilder,
    PendingStateBuilder, ProviderError, Result, SnapshotCache, StateProcessorError,
    metrics::Metrics,
    validation::{
        CanonicalBlockReconciler, FlashblockSequenceValidator, ReconciliationStrategy,
        ReorgDetector, SequenceValidationResult,
    },
};

/// Messages consumed by the state processor.
#[derive(Debug, Clone)]
pub enum StateUpdate {
    /// New canonical block to reconcile against pending state.
    Canonical(RecoveredBlock<BaseBlock>),
    /// Incoming flashblock payload to extend pending state.
    Flashblock {
        /// The flashblock to apply to pending state.
        flashblock: Flashblock,
        /// The local time when this update was enqueued.
        enqueued_at: Instant,
    },
}

/// Processes flashblocks and canonical blocks to keep pending state updated.
#[derive(Debug, Clone)]
pub struct StateProcessor<Client> {
    rx: Arc<Mutex<UnboundedReceiver<StateUpdate>>>,
    pending_blocks: Arc<ArcSwapOption<PendingBlocks>>,
    max_depth: u64,
    mode: FlashblocksMode,
    client: Client,
    fast_sender: Sender<FastFlashblockFeedEvent>,
    sender: Sender<Arc<PendingBlocks>>,
    cache: Arc<Mutex<FlashblockCache>>,
    snapshot_cache: Arc<StdMutex<SnapshotCache>>,
    hot_snapshot_ring: Arc<StdMutex<HotSnapshotRing>>,
    next_snapshot_nonce: Arc<AtomicU64>,
    hot_engine: Option<Arc<Mutex<HotEngine<Client>>>>,
}

impl<Client> StateProcessor<Client>
where
    Client: StateProviderFactory
        + ChainSpecProvider<ChainSpec: EthChainSpec<Header = Header> + Upgrades>
        + BlockReaderIdExt<Header = Header>
        + Clone
        + 'static,
{
    /// Creates a new state processor wired to the provided channels and state using the legacy
    /// runtime mode.
    pub fn new(
        client: Client,
        pending_blocks: Arc<ArcSwapOption<PendingBlocks>>,
        max_depth: u64,
        rx: Arc<Mutex<UnboundedReceiver<StateUpdate>>>,
        fast_sender: Sender<FastFlashblockFeedEvent>,
        sender: Sender<Arc<PendingBlocks>>,
        snapshot_cache: Arc<StdMutex<SnapshotCache>>,
        hot_snapshot_ring: Arc<StdMutex<HotSnapshotRing>>,
    ) -> Self {
        Self::new_with_mode(
            client,
            pending_blocks,
            max_depth,
            FlashblocksMode::Legacy,
            rx,
            fast_sender,
            sender,
            snapshot_cache,
            hot_snapshot_ring,
        )
    }

    /// Creates a new state processor wired to the provided channels and state with an explicit
    /// runtime mode.
    pub fn new_with_mode(
        client: Client,
        pending_blocks: Arc<ArcSwapOption<PendingBlocks>>,
        max_depth: u64,
        mode: FlashblocksMode,
        rx: Arc<Mutex<UnboundedReceiver<StateUpdate>>>,
        fast_sender: Sender<FastFlashblockFeedEvent>,
        sender: Sender<Arc<PendingBlocks>>,
        snapshot_cache: Arc<StdMutex<SnapshotCache>>,
        hot_snapshot_ring: Arc<StdMutex<HotSnapshotRing>>,
    ) -> Self {
        let cache = client
            .best_block_number()
            .map_or_else(|_| FlashblockCache::new(0), FlashblockCache::new);
        let hot_engine = (mode == FlashblocksMode::HotOnly)
            .then(|| Arc::new(Mutex::new(HotEngine::new(client.clone(), max_depth))));

        Self {
            pending_blocks,
            client,
            max_depth,
            mode,
            rx,
            fast_sender,
            sender,
            cache: Arc::new(Mutex::new(cache)),
            snapshot_cache,
            hot_snapshot_ring,
            next_snapshot_nonce: Arc::new(AtomicU64::new(0)),
            hot_engine,
        }
    }

    /// Returns the configured flashblocks runtime mode.
    pub fn mode(&self) -> FlashblocksMode {
        self.mode
    }

    /// Processes updates from the queue until the channel closes.
    pub async fn start(&self) {
        while let Some(update) = self.rx.lock().await.recv().await {
            if let StateUpdate::Flashblock { enqueued_at, .. } = &update {
                Metrics::state_queue_delay_duration().record(enqueued_at.elapsed());
            }

            match update {
                StateUpdate::Canonical(block) => {
                    debug!(message = "processing canonical block", block_number = block.number);
                    if self.mode == FlashblocksMode::HotOnly {
                        self.apply_hot_only_canonical(block).await;
                    } else {
                        let prev_pending_blocks = self.pending_blocks.load_full();
                        match self.process_canonical_block(prev_pending_blocks, &block) {
                            Ok(new_pending_blocks) => {
                                if new_pending_blocks.is_none() {
                                    self.clear_snapshot_cache();
                                }
                                self.pending_blocks.swap(new_pending_blocks);

                                let mut cache = self.cache.lock().await;
                                cache.update_canonical(block.number);
                                let cached = cache.drain(block.number + 1);
                                drop(cache);

                                if !cached.is_empty() {
                                    debug!(
                                        message =
                                            "replaying cached flashblocks after canonical block",
                                        canonical_block = block.number,
                                        cached_count = cached.len(),
                                    );
                                    for flashblock in cached {
                                        let fb_prev = self.pending_blocks.load_full();
                                        self.apply_flashblock(fb_prev, flashblock).await;
                                    }
                                }
                            }
                            Err(e) => {
                                error!(message = "could not process canonical block", error = %e);
                            }
                        }
                    }
                }
                StateUpdate::Flashblock { flashblock, .. } => {
                    debug!(
                        message = "processing flashblock",
                        block_number = flashblock.metadata.block_number,
                        flashblock_index = flashblock.index
                    );
                    if self.mode == FlashblocksMode::HotOnly {
                        self.apply_hot_only_flashblock(flashblock).await;
                    } else {
                        let prev_pending_blocks = self.pending_blocks.load_full();
                        self.apply_flashblock(prev_pending_blocks, flashblock).await;
                    }
                }
            }
        }
    }

    fn hot_engine(&self) -> &Arc<Mutex<HotEngine<Client>>> {
        self.hot_engine.as_ref().expect("hot engine should exist in hot-only mode")
    }

    async fn apply_hot_only_canonical(&self, block: RecoveredBlock<BaseBlock>) {
        let outcome = self.hot_engine().lock().await.process_canonical_block(&block);
        match outcome {
            Ok(HotApplyOutcome::Delta { delta, snapshot, .. }) => {
                _ = self.fast_sender.send(FastFlashblockFeedEvent::Delta(Arc::new(delta)));
                if let Some(snapshot) = snapshot {
                    self.hot_snapshot_ring
                        .lock()
                        .expect("hot snapshot ring mutex poisoned")
                        .insert(Arc::new(snapshot));
                }
            }
            Ok(HotApplyOutcome::Duplicate) => {}
            Ok(HotApplyOutcome::Reset) => {
                self.clear_hot_snapshot_ring();
                _ = self.fast_sender.send(FastFlashblockFeedEvent::Resync);
            }
            Ok(HotApplyOutcome::InvalidateSession { reason }) => {
                self.handle_hot_invalidation(Some(block.number), reason).await;
                return;
            }
            Err(e) => {
                error!(message = "could not process canonical block", error = %e);
                return;
            }
        }

        let mut cache = self.cache.lock().await;
        cache.update_canonical(block.number);
        let cached = cache.drain_cached(block.number + 1);
        drop(cache);

        if !cached.is_empty() {
            Self::record_hot_cache_drain(&cached);
            debug!(
                message = "replaying cached flashblocks after canonical block",
                canonical_block = block.number,
                cached_count = cached.len(),
            );
            for flashblock in cached {
                if self.apply_hot_only_flashblock(flashblock.flashblock).await {
                    return;
                }
            }
        }
    }

    fn record_hot_cache_drain(cached: &[CachedFlashblock]) {
        Metrics::hot_cache_drain_flashblock_count().record(cached.len() as f64);
        for cached in cached {
            Metrics::hot_cache_dwell_duration().record(cached.inserted_at.elapsed());
        }
    }

    async fn apply_hot_only_flashblock(&self, flashblock: Flashblock) -> bool {
        let mut replay_queue = VecDeque::from([flashblock]);

        self.apply_hot_only_replay_queue(&mut replay_queue).await
    }

    async fn apply_hot_only_replay_queue(&self, replay_queue: &mut VecDeque<Flashblock>) -> bool {
        while let Some(flashblock) = replay_queue.pop_front() {
            let _flashblock_apply_timer =
                base_metrics::timed!(Metrics::flashblock_apply_duration());
            let block_processing_start = Instant::now();

            if self.cache_missing_first_hot_flashblock(&flashblock).await {
                continue;
            }

            let (outcome, missing_first_flashblock, hot_window_invalidated) = {
                let mut hot_engine = self.hot_engine().lock().await;
                let missing_first_flashblock = (hot_engine.window.execution.is_none()
                    || hot_engine.window.blocks.is_empty())
                    && flashblock.index > 0;
                let outcome = hot_engine.apply_flashblock(&flashblock);
                let hot_window_invalidated =
                    hot_engine.window.execution.is_none() || hot_engine.window.blocks.is_empty();
                (outcome, missing_first_flashblock, hot_window_invalidated)
            };

            match outcome {
                Ok(HotApplyOutcome::Delta { delta, snapshot, verified_parent_ready }) => {
                    _ = self.fast_sender.send(FastFlashblockFeedEvent::Delta(Arc::new(delta)));
                    if let Some(snapshot) = snapshot {
                        self.hot_snapshot_ring
                            .lock()
                            .expect("hot snapshot ring mutex poisoned")
                            .insert(Arc::new(snapshot));
                    }
                    Metrics::block_processing_duration().record(block_processing_start.elapsed());

                    if let Some(verified_parent_ready) = verified_parent_ready {
                        let child_block = verified_parent_ready.saturating_add(1);
                        let cached = {
                            let mut cache = self.cache.lock().await;
                            cache.drain_cached(child_block)
                        };
                        if !cached.is_empty() {
                            Self::record_hot_cache_drain(&cached);
                            debug!(
                                message = "replaying cached flashblocks after speculative parent verification",
                                verified_parent_block = verified_parent_ready,
                                child_block,
                                cached_count = cached.len(),
                            );
                            replay_queue.extend(cached.into_iter().map(|cached| cached.flashblock));
                        }
                    }
                }
                Ok(HotApplyOutcome::Duplicate) => {
                    Metrics::block_processing_duration().record(block_processing_start.elapsed());
                }
                Ok(HotApplyOutcome::Reset) => {
                    if hot_window_invalidated {
                        self.clear_hot_snapshot_ring();
                    }

                    if missing_first_flashblock {
                        let cached = {
                            let mut cache = self.cache.lock().await;
                            cache.has_flashblock(
                                flashblock.metadata.block_number,
                                flashblock.index - 1,
                            ) && cache.insert(flashblock)
                        };
                        if cached {
                            Metrics::hot_cache_insert_missing_first_count().increment(1);
                            continue;
                        }
                    }

                    if hot_window_invalidated {
                        _ = self.fast_sender.send(FastFlashblockFeedEvent::Resync);
                    }
                    Metrics::block_processing_duration().record(block_processing_start.elapsed());
                }
                Ok(HotApplyOutcome::InvalidateSession { reason }) => {
                    self.handle_hot_invalidation(None, reason).await;
                    Metrics::block_processing_duration().record(block_processing_start.elapsed());
                    return true;
                }
                Err(e) => {
                    if let StateProcessorError::Provider(ProviderError::MissingCanonicalHeader {
                        ..
                    }) = e
                    {
                        if self.cache.lock().await.insert(flashblock) {
                            Metrics::hot_cache_insert_missing_canonical_count().increment(1);
                            debug!(message = "cached flashblock pending canonical block", error = %e);
                        }
                        continue;
                    }

                    if hot_window_invalidated {
                        self.clear_hot_snapshot_ring();
                        _ = self.fast_sender.send(FastFlashblockFeedEvent::Resync);
                    }

                    error!(message = "could not process Flashblock", error = %e);
                    Metrics::block_processing_error().increment(1);
                }
            }
        }

        false
    }

    async fn cache_missing_first_hot_flashblock(&self, flashblock: &Flashblock) -> bool {
        if flashblock.index == 0 {
            return false;
        }

        let hot_window_invalidated = {
            let hot_engine = self.hot_engine().lock().await;
            hot_engine.window.execution.is_none() || hot_engine.window.blocks.is_empty()
        };

        if !hot_window_invalidated {
            return false;
        }

        let cached = {
            let mut cache = self.cache.lock().await;
            cache.has_flashblock(flashblock.metadata.block_number, flashblock.index - 1)
                && cache.insert(flashblock.clone())
        };

        if !cached {
            return false;
        }

        self.hot_engine().lock().await.reset();
        self.clear_hot_snapshot_ring();
        Metrics::hot_cache_insert_missing_first_count().increment(1);
        true
    }

    async fn apply_flashblock(
        &self,
        prev_pending_blocks: Option<Arc<PendingBlocks>>,
        flashblock: Flashblock,
    ) {
        let _flashblock_apply_timer = base_metrics::timed!(Metrics::flashblock_apply_duration());
        let block_processing_start = Instant::now();
        match self.process_flashblock(prev_pending_blocks.clone(), &flashblock) {
            Ok(new_pending_blocks) => {
                let fast_update = self.prepare_fast_flashblock_update(
                    prev_pending_blocks.as_ref(),
                    new_pending_blocks.as_ref(),
                );
                if new_pending_blocks.is_none() {
                    self.clear_snapshot_cache();
                }
                self.pending_blocks.swap(new_pending_blocks.clone());
                if let Some(delta) = fast_update {
                    _ = self.fast_sender.send(FastFlashblockFeedEvent::Delta(delta));
                }
                if let Some(ref pb) = new_pending_blocks {
                    _ = self.sender.send(Arc::clone(pb));
                }
                Metrics::block_processing_duration().record(block_processing_start.elapsed());
            }
            Err(e) => {
                match e {
                    StateProcessorError::Provider(ProviderError::MissingCanonicalHeader {
                        ..
                    }) => {
                        if self.cache.lock().await.insert(flashblock) {
                            debug!(message = "cached flashblock pending canonical block", error = %e);
                            return;
                        }
                    }
                    StateProcessorError::MissingFirstFlashblock => {
                        let mut cache = self.cache.lock().await;
                        // this error should only occur for non-zero index flashblocks, but check here for index safety
                        if flashblock.index > 0
                            && cache.has_flashblock(
                                flashblock.metadata.block_number,
                                flashblock.index - 1,
                            )
                            && cache.insert(flashblock)
                        {
                            return;
                        }
                        // we should ignore this error since it doesn't necessarily indicate a problem
                        return;
                    }
                    _ => {}
                }

                // skip logging expected caching case
                if !matches!(
                    e,
                    StateProcessorError::Provider(ProviderError::MissingCanonicalHeader { .. })
                ) {
                    error!(message = "could not process Flashblock", error = %e);
                    Metrics::block_processing_error().increment(1);
                }
            }
        }
    }

    #[instrument(level = "debug", skip_all, fields(block_number = block.number))]
    fn process_canonical_block(
        &self,
        prev_pending_blocks: Option<Arc<PendingBlocks>>,
        block: &RecoveredBlock<BaseBlock>,
    ) -> Result<Option<Arc<PendingBlocks>>> {
        let pending_blocks = match &prev_pending_blocks {
            Some(pb) => pb,
            None => {
                debug!(message = "no pending state to update with canonical block, skipping");
                return Ok(None);
            }
        };

        let mut flashblocks = pending_blocks.get_flashblocks();
        let num_flashblocks_for_canon =
            flashblocks.iter().filter(|fb| fb.metadata.block_number == block.number).count();
        Metrics::flashblocks_in_block().record(num_flashblocks_for_canon as f64);
        Metrics::pending_snapshot_height().set(pending_blocks.latest_block_number() as f64);

        // Check for reorg by comparing transaction sets
        let tracked_txns = pending_blocks.get_transactions_for_block(block.number);
        let tracked_txn_hashes: Vec<_> = tracked_txns.map(|tx| tx.tx_hash()).collect();
        let block_txn_hashes: Vec<_> = block.body().transactions().map(|tx| tx.tx_hash()).collect();

        let reorg_result = ReorgDetector::detect(&tracked_txn_hashes, &block_txn_hashes);
        let reorg_detected = reorg_result.is_reorg();

        // Determine the reconciliation strategy
        let strategy = CanonicalBlockReconciler::reconcile(
            Some(pending_blocks.earliest_block_number()),
            Some(pending_blocks.latest_block_number()),
            block.number,
            self.max_depth,
            reorg_detected,
        );

        match strategy {
            ReconciliationStrategy::CatchUp => {
                debug!(
                    message = "pending snapshot cleared because canonical caught up",
                    latest_pending_block = pending_blocks.latest_block_number(),
                    canonical_block = block.number,
                );
                Metrics::pending_clear_catchup().increment(1);
                Metrics::pending_snapshot_fb_index()
                    .set(pending_blocks.latest_flashblock_index() as f64);
                Ok(None)
            }
            ReconciliationStrategy::HandleReorg => {
                warn!(
                    message = "reorg detected, recomputing pending flashblocks going ahead of reorg",
                    tracked_txn_hashes = ?tracked_txn_hashes,
                    block_txn_hashes = ?block_txn_hashes,
                );
                Metrics::pending_clear_reorg().increment(1);
                self.clear_snapshot_cache();

                // If there is a reorg, we re-process all future flashblocks without reusing the existing pending state
                flashblocks.retain(|flashblock| flashblock.metadata.block_number > block.number);
                self.build_pending_state(None, &flashblocks)
            }
            ReconciliationStrategy::DepthLimitExceeded { depth, max_depth } => {
                debug!(
                    message = "pending blocks depth exceeds max depth, resetting pending blocks",
                    pending_blocks_depth = depth,
                    max_depth = max_depth,
                );
                self.clear_snapshot_cache();

                flashblocks.retain(|flashblock| flashblock.metadata.block_number > block.number);
                self.build_pending_state(None, &flashblocks)
            }
            ReconciliationStrategy::Continue => {
                debug!(
                    message = "canonical block behind latest pending block, continuing with existing pending state",
                    latest_pending_block = pending_blocks.latest_block_number(),
                    earliest_pending_block = pending_blocks.earliest_block_number(),
                    canonical_block = block.number,
                    pending_txns_for_block = ?tracked_txn_hashes.len(),
                    canonical_txns_for_block = ?block_txn_hashes.len(),
                );
                // If no reorg, we can continue building on top of the existing pending state
                // NOTE: We do not retain specific flashblocks here to avoid losing track of our "earliest" pending block number
                self.build_pending_state(prev_pending_blocks, &flashblocks)
            }
            ReconciliationStrategy::NoPendingState => {
                // This case is already handled above, but included for completeness
                debug!(message = "no pending state to update with canonical block, skipping");
                Ok(None)
            }
        }
    }

    #[instrument(
        level = "debug",
        skip_all,
        fields(
            block_number = flashblock.metadata.block_number,
            flashblock_index = flashblock.index
        )
    )]
    fn process_flashblock(
        &self,
        prev_pending_blocks: Option<Arc<PendingBlocks>>,
        flashblock: &Flashblock,
    ) -> Result<Option<Arc<PendingBlocks>>> {
        let pending_blocks = match &prev_pending_blocks {
            Some(pb) => pb,
            None => {
                if flashblock.index == 0 {
                    return self.build_pending_state(None, std::slice::from_ref(flashblock));
                }

                return Err(StateProcessorError::MissingFirstFlashblock);
            }
        };

        let validation_result = FlashblockSequenceValidator::validate(
            pending_blocks.latest_block_number(),
            pending_blocks.latest_flashblock_index(),
            flashblock.metadata.block_number,
            flashblock.index,
        );

        match validation_result {
            SequenceValidationResult::NextInSequence
            | SequenceValidationResult::FirstOfNextBlock => {
                // We have received the next flashblock for the current block
                // or the first flashblock for the next block
                let mut flashblocks = pending_blocks.get_flashblocks();
                flashblocks.push(flashblock.clone());
                self.build_pending_state(prev_pending_blocks, &flashblocks)
            }
            SequenceValidationResult::Duplicate => {
                // We have received a duplicate flashblock for the current block
                Metrics::unexpected_block_order().increment(1);
                warn!(
                    message = "Received duplicate Flashblock for current block, ignoring",
                    curr_block = %pending_blocks.latest_block_number(),
                    flashblock_index = %flashblock.index,
                );
                Ok(prev_pending_blocks)
            }
            SequenceValidationResult::InvalidNewBlockIndex { block_number, index: _ } => {
                // We have received a non-zero flashblock for a new block
                Metrics::unexpected_block_order().increment(1);
                error!(
                    message = "Received non-zero index Flashblock for new block, zeroing Flashblocks until we receive a base Flashblock",
                    curr_block = %pending_blocks.latest_block_number(),
                    new_block = %block_number,
                );
                Ok(None)
            }
            SequenceValidationResult::NonSequentialGap { expected: _, actual: _ } => {
                // We have received a non-sequential Flashblock for the current block
                Metrics::unexpected_block_order().increment(1);
                error!(
                    message = "Received non-sequential Flashblock for current block, zeroing Flashblocks until we receive a base Flashblock",
                    curr_block = %pending_blocks.latest_block_number(),
                    new_block = %flashblock.metadata.block_number,
                );
                Ok(None)
            }
        }
    }

    fn clear_snapshot_cache(&self) {
        self.snapshot_cache.lock().expect("snapshot cache mutex poisoned").clear();
    }

    fn clear_hot_snapshot_ring(&self) {
        self.hot_snapshot_ring.lock().expect("hot snapshot ring mutex poisoned").clear();
    }

    async fn handle_hot_invalidation(
        &self,
        canonical_block_number: Option<BlockNumber>,
        reason: HotInvalidationReason,
    ) {
        warn!(reason = ?reason, "invalidated hot flashblock session");
        self.clear_hot_snapshot_ring();

        let mut cache = self.cache.lock().await;
        cache.clear();
        if let Some(canonical_block_number) = canonical_block_number {
            cache.update_canonical(canonical_block_number);
        }
        drop(cache);

        _ = self.fast_sender.send(FastFlashblockFeedEvent::InvalidateSession);
    }

    fn next_snapshot_nonce(&self) -> u64 {
        self.next_snapshot_nonce.fetch_add(1, Ordering::Relaxed).saturating_add(1)
    }

    fn prepare_fast_flashblock_update(
        &self,
        prev_pending_blocks: Option<&Arc<PendingBlocks>>,
        new_pending_blocks: Option<&Arc<PendingBlocks>>,
    ) -> Option<Arc<FastFlashblockLogsDelta>> {
        if self.fast_sender.receiver_count() == 0 {
            return None;
        }

        let pending_blocks = match (prev_pending_blocks, new_pending_blocks) {
            (_, None) => return None,
            (Some(prev), Some(next)) if Arc::ptr_eq(prev, next) => return None,
            (_, Some(next)) => next,
        };

        let delta = Arc::new(base_metrics::time!(Metrics::fast_delta_build_duration(), {
            pending_blocks.get_latest_fast_flashblock_logs_delta(self.next_snapshot_nonce(), None)
        }));
        self.snapshot_cache
            .lock()
            .expect("snapshot cache mutex poisoned")
            .insert(delta.snapshot_id, Arc::clone(pending_blocks));
        Some(delta)
    }

    #[instrument(level = "debug", skip_all, fields(num_flashblocks = flashblocks.len()))]
    fn build_pending_state(
        &self,
        prev_pending_blocks: Option<Arc<PendingBlocks>>,
        flashblocks: &[Flashblock],
    ) -> Result<Option<Arc<PendingBlocks>>> {
        let _pending_state_build_timer =
            base_metrics::timed!(Metrics::pending_state_build_duration());

        // BTreeMap guarantees ascending order of keys while iterating
        let mut flashblocks_per_block = BTreeMap::<BlockNumber, Vec<Flashblock>>::new();
        for flashblock in flashblocks {
            flashblocks_per_block
                .entry(flashblock.metadata.block_number)
                .or_default()
                .push(flashblock.clone());
        }

        let earliest_block_number = flashblocks_per_block.keys().min().unwrap();
        let canonical_block = earliest_block_number - 1;
        let mut last_block_header = self
            .client
            .header_by_number(canonical_block)
            .map_err(|e| ProviderError::StateProvider(e.to_string()))?
            .ok_or(ProviderError::MissingCanonicalHeader { block_number: canonical_block })?;

        let evm_config = BaseEvmConfig::base(self.client.chain_spec());
        let state_provider = self
            .client
            .state_by_block_number_or_tag(BlockNumberOrTag::Number(canonical_block))
            .map_err(|e| ProviderError::StateProvider(e.to_string()))?;
        let state_provider_db = StateProviderDatabase::new(state_provider);
        let mut pending_blocks_builder = PendingBlocksBuilder::new();

        // Track state changes across flashblocks, accumulating bundle state
        // from previous pending blocks if available.
        let mut db = match &prev_pending_blocks {
            Some(pending_blocks) => State::builder()
                .with_database(state_provider_db)
                .with_bundle_update()
                .with_bundle_prestate(pending_blocks.get_bundle_state())
                .build(),
            None => State::builder().with_database(state_provider_db).with_bundle_update().build(),
        };

        let mut state_overrides =
            prev_pending_blocks.as_ref().map_or_else(StateOverride::default, |pending_blocks| {
                pending_blocks.get_state_overrides().unwrap_or_default()
            });

        for (_block_number, flashblocks) in flashblocks_per_block {
            // Use BlockAssembler to reconstruct the block from flashblocks
            let assembled = BlockAssembler::assemble(&flashblocks)?;

            pending_blocks_builder.with_flashblocks(assembled.flashblocks.clone());
            pending_blocks_builder.with_header(assembled.header.clone());

            // Extract L1 block info using the AssembledBlock method
            let l1_block_info = assembled.l1_block_info()?;

            let block_env_attributes = BaseNextBlockEnvAttributes {
                timestamp: assembled.base.timestamp,
                suggested_fee_recipient: assembled.base.fee_recipient,
                prev_randao: assembled.base.prev_randao,
                gas_limit: assembled.base.gas_limit,
                parent_beacon_block_root: Some(assembled.base.parent_beacon_block_root),
                extra_data: assembled.base.extra_data.clone(),
            };

            let evm_env = evm_config
                .next_evm_env(&last_block_header, &block_env_attributes)
                .map_err(|e| ExecutionError::EvmEnv(e.to_string()))?;
            let evm = evm_config.evm_with_env(db, evm_env);

            // Parallel sender recovery - batch all ECDSA operations upfront
            let recovery_start = Instant::now();
            let txs_with_senders: Vec<(BaseTxEnvelope, Address)> = assembled
                .block
                .body
                .transactions
                .par_iter()
                .cloned()
                .map(|tx| -> Result<(BaseTxEnvelope, Address)> {
                    let tx_hash = tx.tx_hash();
                    let sender = match prev_pending_blocks
                        .as_ref()
                        .and_then(|p| p.get_transaction_sender(&tx_hash))
                    {
                        Some(cached) => cached,
                        None => tx.recover_signer()?,
                    };
                    Ok((tx, sender))
                })
                .collect::<Result<_>>()?;
            Metrics::sender_recovery_duration().record(recovery_start.elapsed());

            // Clone header before moving block to avoid cloning the entire block
            let block_header = assembled.block.header.clone();

            let parent_hash = last_block_header.hash_slow();
            let parent_beacon_block_root = Some(assembled.base.parent_beacon_block_root);

            let mut pending_state_builder = PendingStateBuilder::new(
                self.client.chain_spec(),
                evm,
                assembled.block,
                prev_pending_blocks.clone(),
                l1_block_info,
                state_overrides,
            );

            pending_state_builder
                .apply_pre_execution_changes(parent_hash, parent_beacon_block_root)?;

            for (idx, (transaction, sender)) in txs_with_senders.into_iter().enumerate() {
                let tx_hash = transaction.tx_hash();

                pending_blocks_builder.with_transaction_sender(tx_hash, sender);
                pending_blocks_builder.increment_nonce(sender);

                let recovered_transaction = Recovered::new_unchecked(transaction, sender);

                let executed_transaction =
                    pending_state_builder.execute_transaction(idx, recovered_transaction)?;

                if let Some(time_us) = executed_transaction.execution_time_us {
                    pending_blocks_builder.with_execution_time(tx_hash, time_us);
                }

                for (address, account) in &executed_transaction.state {
                    if account.is_touched() {
                        pending_blocks_builder.with_account_balance(*address, account.info.balance);
                    }
                }

                pending_blocks_builder.with_transaction(executed_transaction.rpc_transaction);
                pending_blocks_builder.with_receipt(tx_hash, executed_transaction.receipt);
                pending_blocks_builder.with_transaction_state(tx_hash, executed_transaction.state);
                pending_blocks_builder
                    .with_transaction_result(tx_hash, executed_transaction.result);
            }

            (db, state_overrides) = pending_state_builder.into_db_and_state_overrides();
            last_block_header = block_header;
        }

        // Extract the accumulated bundle state for pending block serving.
        db.merge_transitions(BundleRetention::Reverts);
        pending_blocks_builder.with_bundle_state(db.take_bundle());
        pending_blocks_builder.with_state_overrides(state_overrides);

        Ok(Some(Arc::new(pending_blocks_builder.build()?)))
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::VecDeque, sync::Arc, time::Duration};

    use alloy_primitives::{Address, B256, Bloom, Bytes, U256, hex_literal::hex};
    use alloy_rpc_types_engine::PayloadId;
    use arc_swap::ArcSwapOption;
    use base_common_consensus::BasePrimitives;
    use base_common_flashblocks::{
        ExecutionPayloadBaseV1, ExecutionPayloadFlashblockDeltaV1, Flashblock, Metadata,
    };
    use base_execution_chainspec::{BaseChainSpec, BaseChainSpecBuilder};
    use reth_chainspec::ChainSpecProvider;
    use reth_provider::test_utils::MockEthProvider;
    use tokio::sync::{Mutex, broadcast, mpsc};
    use tokio::time::timeout;

    use super::StateProcessor;
    use crate::state_builder::PendingHeaderBuilder;
    use crate::{
        BlockAssembler, FastFlashblockFeedEvent, FlashblocksMode, HotPendingWindow,
        HotSnapshotRing, SnapshotCache,
    };

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

    fn test_hot_flashblock(
        index: u64,
        block_number: u64,
        payload_id: PayloadId,
        parent_hash: B256,
        with_base: bool,
        transactions: Vec<Bytes>,
    ) -> Flashblock {
        Flashblock {
            payload_id,
            index,
            base: with_base.then_some(ExecutionPayloadBaseV1 {
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
                block_hash: B256::ZERO,
                transactions,
                withdrawals: vec![],
                withdrawals_root: B256::ZERO,
                blob_gas_used: None,
            },
            metadata: Metadata { block_number },
        }
    }

    async fn recv_fast_flashblock_event(
        receiver: &mut broadcast::Receiver<FastFlashblockFeedEvent>,
    ) -> FastFlashblockFeedEvent {
        timeout(RECV_TIMEOUT, receiver.recv())
            .await
            .expect("fast flashblock event should arrive")
            .expect("fast flashblock channel should stay open")
    }

    #[test]
    fn state_processor_new_defaults_to_legacy_mode() {
        let (_tx, rx) = mpsc::unbounded_channel();
        let (fast_sender, _) = broadcast::channel(1);
        let (sender, _) = broadcast::channel(1);

        let processor = StateProcessor::new(
            test_client(),
            Arc::new(ArcSwapOption::new(None)),
            5,
            Arc::new(Mutex::new(rx)),
            fast_sender,
            sender,
            Arc::new(std::sync::Mutex::new(SnapshotCache::new(1, Duration::from_secs(1)))),
            Arc::new(std::sync::Mutex::new(HotSnapshotRing::new(1))),
        );

        assert_eq!(processor.mode(), FlashblocksMode::Legacy);
    }

    #[test]
    fn state_processor_new_with_mode_stores_hot_mode() {
        let (_tx, rx) = mpsc::unbounded_channel();
        let (fast_sender, _) = broadcast::channel(1);
        let (sender, _) = broadcast::channel(1);

        let processor = StateProcessor::new_with_mode(
            test_client(),
            Arc::new(ArcSwapOption::new(None)),
            5,
            FlashblocksMode::HotOnly,
            Arc::new(Mutex::new(rx)),
            fast_sender,
            sender,
            Arc::new(std::sync::Mutex::new(SnapshotCache::new(1, Duration::from_secs(1)))),
            Arc::new(std::sync::Mutex::new(HotSnapshotRing::new(1))),
        );

        assert_eq!(processor.mode(), FlashblocksMode::HotOnly);
    }

    #[tokio::test]
    async fn hot_only_replays_cached_next_block_suffixes_after_speculative_parent_verification() {
        let (_tx, rx) = mpsc::unbounded_channel();
        let (fast_sender, mut fast_receiver) = broadcast::channel(8);
        let (sender, _) = broadcast::channel(1);
        let client = test_client();
        let parent_hash = client.chain_spec().genesis_hash();

        let processor = StateProcessor::new_with_mode(
            client,
            Arc::new(ArcSwapOption::new(None)),
            5,
            FlashblocksMode::HotOnly,
            Arc::new(Mutex::new(rx)),
            fast_sender,
            sender,
            Arc::new(std::sync::Mutex::new(SnapshotCache::new(8, Duration::from_secs(1)))),
            Arc::new(std::sync::Mutex::new(HotSnapshotRing::new(8))),
        );

        processor
            .apply_hot_only_flashblock(test_hot_flashblock(
                0,
                1,
                PayloadId::new([0x81; 8]),
                parent_hash,
                true,
                vec![encoded_l1_info_tx()],
            ))
            .await;

        let first_event = recv_fast_flashblock_event(&mut fast_receiver).await;
        let FastFlashblockFeedEvent::Delta(first_delta) = first_event else {
            panic!("expected first hot-only delta event");
        };

        let _first_snapshot = processor
            .hot_snapshot_ring
            .lock()
            .expect("hot snapshot ring mutex poisoned")
            .get(first_delta.snapshot_id)
            .expect("first hot snapshot should be retained");

        let locally_sealed_parent = {
            let mut hot_engine = processor.hot_engine().lock().await;
            let chain_spec = hot_engine.client.chain_spec();
            let HotPendingWindow { execution, blocks, .. } = &mut hot_engine.window;
            let execution = execution
                .as_mut()
                .expect("hot execution state should exist after first flashblock");
            let pending_block = blocks
                .back_mut()
                .expect("active pending block should exist after first flashblock");
            let ordered_receipts = pending_block
                .transactions
                .iter()
                .map(|transaction| {
                    pending_block
                        .receipts
                        .get(&transaction.hash)
                        .cloned()
                        .expect("receipt should exist for executed transaction")
                })
                .collect::<Vec<_>>();
            let header_parts = PendingHeaderBuilder::from_post_state(
                chain_spec.as_ref(),
                pending_block.base.timestamp,
                pending_block.cumulative_gas_used,
                &mut execution.db,
                &ordered_receipts,
            )
            .expect("pending block should derive local header parts");

            BlockAssembler::header_from_local_execution(
                &pending_block.base,
                &pending_block.flashblocks,
                &header_parts,
            )
            .expect("pending block should assemble a locally sealed header")
            .hash_slow()
        };

        let cached_suffix = test_hot_flashblock(
            1,
            2,
            PayloadId::new([0x82; 8]),
            locally_sealed_parent,
            false,
            vec![],
        );
        assert!(processor.cache.lock().await.insert(cached_suffix));

        processor
            .apply_hot_only_flashblock(test_hot_flashblock(
                0,
                2,
                PayloadId::new([0x82; 8]),
                locally_sealed_parent,
                true,
                vec![encoded_l1_info_tx()],
            ))
            .await;

        let rollover_event = recv_fast_flashblock_event(&mut fast_receiver).await;
        let FastFlashblockFeedEvent::Delta(rollover_delta) = rollover_event else {
            panic!("expected rollover delta event");
        };
        assert_eq!(rollover_delta.block_number, 2);
        assert_eq!(rollover_delta.flashblock_index, 0);

        let replayed_event = recv_fast_flashblock_event(&mut fast_receiver).await;
        let FastFlashblockFeedEvent::Delta(replayed_delta) = replayed_event else {
            panic!("expected replayed cached suffix delta event");
        };
        assert_eq!(replayed_delta.block_number, 2);
        assert_eq!(replayed_delta.flashblock_index, 1);

        assert!(matches!(fast_receiver.try_recv(), Err(broadcast::error::TryRecvError::Empty)));
        assert!(!processor.cache.lock().await.has_flashblock(2, 1));

        let hot_engine = processor.hot_engine().lock().await;
        let active_block = hot_engine.window.blocks.back().expect("active block should exist");
        assert_eq!(active_block.block_number, 2);
        assert_eq!(active_block.latest_flashblock_index, 1);
    }

    #[tokio::test]
    async fn hot_only_flashblock_invalidate_session_emits_terminal_event() {
        let (_tx, rx) = mpsc::unbounded_channel();
        let (fast_sender, mut fast_receiver) = broadcast::channel(8);
        let (sender, _) = broadcast::channel(1);
        let client = test_client();
        let parent_hash = client.chain_spec().genesis_hash();

        let processor = StateProcessor::new_with_mode(
            client,
            Arc::new(ArcSwapOption::new(None)),
            3,
            FlashblocksMode::HotOnly,
            Arc::new(Mutex::new(rx)),
            fast_sender,
            sender,
            Arc::new(std::sync::Mutex::new(SnapshotCache::new(8, Duration::from_secs(1)))),
            Arc::new(std::sync::Mutex::new(HotSnapshotRing::new(8))),
        );

        processor
            .apply_hot_only_flashblock(test_hot_flashblock(
                0,
                1,
                PayloadId::new([0x91; 8]),
                parent_hash,
                true,
                vec![encoded_l1_info_tx()],
            ))
            .await;

        let first_event = recv_fast_flashblock_event(&mut fast_receiver).await;
        let FastFlashblockFeedEvent::Delta(first_delta) = first_event else {
            panic!("expected initial hot-only delta event");
        };
        assert!(
            processor
                .hot_snapshot_ring
                .lock()
                .expect("hot snapshot ring mutex poisoned")
                .get(first_delta.snapshot_id)
                .is_some()
        );

        processor
            .apply_hot_only_flashblock(test_hot_flashblock(
                0,
                2,
                PayloadId::new([0x92; 8]),
                B256::with_last_byte(0xee),
                true,
                vec![encoded_l1_info_tx()],
            ))
            .await;

        let invalidation_event = recv_fast_flashblock_event(&mut fast_receiver).await;
        assert!(matches!(invalidation_event, FastFlashblockFeedEvent::InvalidateSession));
        assert!(
            processor
                .hot_snapshot_ring
                .lock()
                .expect("hot snapshot ring mutex poisoned")
                .get(first_delta.snapshot_id)
                .is_none()
        );
    }

    #[tokio::test]
    async fn hot_only_replay_queue_invalidate_session_stops_remaining_entries() {
        let (_tx, rx) = mpsc::unbounded_channel();
        let (fast_sender, mut fast_receiver) = broadcast::channel(8);
        let (sender, _) = broadcast::channel(1);
        let client = test_client();
        let parent_hash = client.chain_spec().genesis_hash();

        let processor = StateProcessor::new_with_mode(
            client,
            Arc::new(ArcSwapOption::new(None)),
            3,
            FlashblocksMode::HotOnly,
            Arc::new(Mutex::new(rx)),
            fast_sender,
            sender,
            Arc::new(std::sync::Mutex::new(SnapshotCache::new(8, Duration::from_secs(1)))),
            Arc::new(std::sync::Mutex::new(HotSnapshotRing::new(8))),
        );

        processor
            .apply_hot_only_flashblock(test_hot_flashblock(
                0,
                1,
                PayloadId::new([0x93; 8]),
                parent_hash,
                true,
                vec![encoded_l1_info_tx()],
            ))
            .await;

        let first_event = recv_fast_flashblock_event(&mut fast_receiver).await;
        let FastFlashblockFeedEvent::Delta(first_delta) = first_event else {
            panic!("expected initial hot-only delta event");
        };

        let mut replay_queue = VecDeque::from([
            test_hot_flashblock(
                0,
                2,
                PayloadId::new([0x94; 8]),
                B256::with_last_byte(0xee),
                true,
                vec![encoded_l1_info_tx()],
            ),
            test_hot_flashblock(
                1,
                2,
                PayloadId::new([0x94; 8]),
                B256::with_last_byte(0xee),
                false,
                vec![],
            ),
        ]);

        assert!(processor.apply_hot_only_replay_queue(&mut replay_queue).await);

        let invalidation_event = recv_fast_flashblock_event(&mut fast_receiver).await;
        assert!(matches!(invalidation_event, FastFlashblockFeedEvent::InvalidateSession));
        assert_eq!(replay_queue.len(), 1);
        assert_eq!(replay_queue.front().map(|flashblock| flashblock.index), Some(1));
        assert!(
            processor
                .hot_snapshot_ring
                .lock()
                .expect("hot snapshot ring mutex poisoned")
                .get(first_delta.snapshot_id)
                .is_none()
        );
        assert!(matches!(fast_receiver.try_recv(), Err(broadcast::error::TryRecvError::Empty)));
    }

    #[tokio::test]
    async fn hot_only_canonical_conflict_invalidate_session_emits_terminal_event() {
        let (_tx, rx) = mpsc::unbounded_channel();
        let (fast_sender, mut fast_receiver) = broadcast::channel(8);
        let (sender, _) = broadcast::channel(1);
        let client = test_client();
        let parent_hash = client.chain_spec().genesis_hash();

        let processor = StateProcessor::new_with_mode(
            client.clone(),
            Arc::new(ArcSwapOption::new(None)),
            3,
            FlashblocksMode::HotOnly,
            Arc::new(Mutex::new(rx)),
            fast_sender,
            sender,
            Arc::new(std::sync::Mutex::new(SnapshotCache::new(8, Duration::from_secs(1)))),
            Arc::new(std::sync::Mutex::new(HotSnapshotRing::new(8))),
        );

        processor
            .apply_hot_only_flashblock(test_hot_flashblock(
                0,
                1,
                PayloadId::new([0xa1; 8]),
                parent_hash,
                true,
                vec![encoded_l1_info_tx()],
            ))
            .await;
        let first_event = recv_fast_flashblock_event(&mut fast_receiver).await;
        let FastFlashblockFeedEvent::Delta(first_delta) = first_event else {
            panic!("expected first hot-only delta event");
        };

        let locally_sealed_parent = {
            let mut hot_engine = processor.hot_engine().lock().await;
            let chain_spec = hot_engine.client.chain_spec();
            let HotPendingWindow { execution, blocks, .. } = &mut hot_engine.window;
            let execution = execution
                .as_mut()
                .expect("hot execution state should exist after first flashblock");
            let pending_block = blocks
                .back_mut()
                .expect("active pending block should exist after first flashblock");
            let ordered_receipts = pending_block
                .transactions
                .iter()
                .map(|transaction| {
                    pending_block
                        .receipts
                        .get(&transaction.hash)
                        .cloned()
                        .expect("receipt should exist for executed transaction")
                })
                .collect::<Vec<_>>();
            let header_parts = PendingHeaderBuilder::from_post_state(
                chain_spec.as_ref(),
                pending_block.base.timestamp,
                pending_block.cumulative_gas_used,
                &mut execution.db,
                &ordered_receipts,
            )
            .expect("pending block should derive local header parts");

            BlockAssembler::header_from_local_execution(
                &pending_block.base,
                &pending_block.flashblocks,
                &header_parts,
            )
            .expect("pending block should assemble a locally sealed header")
            .hash_slow()
        };

        processor
            .apply_hot_only_flashblock(test_hot_flashblock(
                0,
                2,
                PayloadId::new([0xa2; 8]),
                locally_sealed_parent,
                true,
                vec![encoded_l1_info_tx()],
            ))
            .await;
        let _rollover_event = recv_fast_flashblock_event(&mut fast_receiver).await;

        let conflicting_canonical = BlockAssembler::assemble(&[test_hot_flashblock(
            0,
            1,
            PayloadId::new([0xa3; 8]),
            parent_hash,
            true,
            vec![],
        )])
        .expect("conflicting canonical flashblock should assemble")
        .block;
        processor
            .apply_hot_only_canonical(reth_primitives::RecoveredBlock::new_unhashed(
                conflicting_canonical,
                Vec::new(),
            ))
            .await;

        let invalidation_event = recv_fast_flashblock_event(&mut fast_receiver).await;
        assert!(matches!(invalidation_event, FastFlashblockFeedEvent::InvalidateSession));
        assert!(
            processor
                .hot_snapshot_ring
                .lock()
                .expect("hot snapshot ring mutex poisoned")
                .get(first_delta.snapshot_id)
                .is_none()
        );
    }
}
