//! Flashblocks state processor.

use std::{
    collections::{BTreeMap, VecDeque},
    fmt,
    sync::{
        Arc, Mutex as StdMutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use alloy_consensus::{
    Header,
    transaction::{Recovered, SignerRecoverable},
};
use alloy_eips::BlockNumberOrTag;
use alloy_network::TransactionResponse;
use alloy_primitives::{Address, B256, BlockNumber};
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
use tokio::{
    sync::{Mutex, broadcast::Sender, mpsc::UnboundedReceiver},
    task::JoinHandle,
};

use crate::{
    AuditWindowSnapshot, BlockAssembler, CachedFlashblock, ExecutionError, FastFlashblockFeedEvent,
    FastFlashblockLogsDelta, FlashblockCache, FlashblocksMode, HotApplyOutcome, HotEngine,
    HotInvalidationReason, HotSnapshot, HotSnapshotRing, LatestHotDryRunSeedCache, PendingBlocks,
    PendingBlocksBuilder, PendingStateBuilder, PeriodicAuditFailure, PeriodicAuditResult,
    ProviderError, Result, ShadowRebuildCompletion, SnapshotCache, StateProcessorError,
    metrics::Metrics,
    validation::{
        CanonicalBlockReconciler, FlashblockSequenceValidator, ReconciliationStrategy,
        ReorgDetector, SequenceValidationResult,
    },
};

const HOT_PERIODIC_AUDIT_INTERVAL_BLOCKS: BlockNumber = 60;
const HOT_PERIODIC_AUDIT_TIMEOUT: Duration = Duration::from_secs(30);

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
    latest_hot_dry_run_seed: Arc<LatestHotDryRunSeedCache>,
    next_snapshot_nonce: Arc<AtomicU64>,
    hot_engine: Option<Arc<Mutex<HotEngine<Client>>>>,
    hot_periodic_audit: Arc<StdMutex<HotPeriodicAuditState<Client>>>,
    hot_periodic_audit_config: HotPeriodicAuditConfig,
}

/// Shared channels and caches required by the state processor runtime.
#[derive(Debug)]
pub struct StateProcessorHandles {
    rx: Arc<Mutex<UnboundedReceiver<StateUpdate>>>,
    fast_sender: Sender<FastFlashblockFeedEvent>,
    sender: Sender<Arc<PendingBlocks>>,
    snapshot_cache: Arc<StdMutex<SnapshotCache>>,
    hot_snapshot_ring: Arc<StdMutex<HotSnapshotRing>>,
    latest_hot_dry_run_seed: Arc<LatestHotDryRunSeedCache>,
}

impl StateProcessorHandles {
    /// Creates one grouped set of state processor handles.
    pub const fn new(
        rx: Arc<Mutex<UnboundedReceiver<StateUpdate>>>,
        fast_sender: Sender<FastFlashblockFeedEvent>,
        sender: Sender<Arc<PendingBlocks>>,
        snapshot_cache: Arc<StdMutex<SnapshotCache>>,
        hot_snapshot_ring: Arc<StdMutex<HotSnapshotRing>>,
        latest_hot_dry_run_seed: Arc<LatestHotDryRunSeedCache>,
    ) -> Self {
        Self { rx, fast_sender, sender, snapshot_cache, hot_snapshot_ring, latest_hot_dry_run_seed }
    }
}

#[derive(Clone, Copy, Debug)]
struct HotPeriodicAuditConfig {
    interval_blocks: BlockNumber,
    timeout: Duration,
}

impl Default for HotPeriodicAuditConfig {
    fn default() -> Self {
        Self {
            interval_blocks: HOT_PERIODIC_AUDIT_INTERVAL_BLOCKS,
            timeout: HOT_PERIODIC_AUDIT_TIMEOUT,
        }
    }
}

struct HotPeriodicAuditState<Client> {
    current_generation: u64,
    current_window_id: u64,
    last_started_audit_canonical_block: BlockNumber,
    last_successful_audit_canonical_block: BlockNumber,
    last_successful_audit_cursor_block: Option<BlockNumber>,
    latest_seen_canonical_block: BlockNumber,
    last_reconciled_canonical: Option<HotPeriodicAuditCanonicalBlock>,
    worker: Option<HotPeriodicAuditWorker<Client>>,
    cleanup_worker: Option<HotPeriodicAuditWorker<Client>>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct HotPeriodicAuditCanonicalBlock {
    number: BlockNumber,
    hash: B256,
}

impl HotPeriodicAuditCanonicalBlock {
    const fn new(number: BlockNumber, hash: B256) -> Self {
        Self { number, hash }
    }
}

#[derive(Clone, Debug, Default)]
struct HotPeriodicAuditCancellation {
    cancelled: Arc<AtomicBool>,
}

impl HotPeriodicAuditCancellation {
    fn new() -> Self {
        Self { cancelled: Arc::new(AtomicBool::new(false)) }
    }

    fn cancel(&self) {
        self.cancelled.store(true, Ordering::Relaxed);
    }

    fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Relaxed)
    }
}

impl<Client> HotPeriodicAuditState<Client> {
    const fn new(latest_seen_canonical_block: BlockNumber) -> Self {
        Self {
            current_generation: 0,
            current_window_id: 0,
            last_started_audit_canonical_block: latest_seen_canonical_block,
            last_successful_audit_canonical_block: latest_seen_canonical_block,
            last_successful_audit_cursor_block: None,
            latest_seen_canonical_block,
            last_reconciled_canonical: None,
            worker: None,
            cleanup_worker: None,
        }
    }
}

impl<Client> Drop for HotPeriodicAuditState<Client> {
    fn drop(&mut self) {
        if let Some(worker) = self.worker.take() {
            worker.cancel();
        }
        if let Some(worker) = self.cleanup_worker.take() {
            worker.cancel();
        }
    }
}

impl<Client> fmt::Debug for HotPeriodicAuditState<Client> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HotPeriodicAuditState")
            .field("current_generation", &self.current_generation)
            .field("current_window_id", &self.current_window_id)
            .field("last_started_audit_canonical_block", &self.last_started_audit_canonical_block)
            .field(
                "last_successful_audit_canonical_block",
                &self.last_successful_audit_canonical_block,
            )
            .field("last_successful_audit_cursor_block", &self.last_successful_audit_cursor_block)
            .field("latest_seen_canonical_block", &self.latest_seen_canonical_block)
            .field("last_reconciled_canonical", &self.last_reconciled_canonical)
            .field("has_worker", &self.worker.is_some())
            .field("has_cleanup_worker", &self.cleanup_worker.is_some())
            .finish()
    }
}

struct HotPeriodicAuditWorker<Client> {
    generation: u64,
    window_id: u64,
    started_at: Instant,
    timeout: Duration,
    cancellation: HotPeriodicAuditCancellation,
    handle: Option<JoinHandle<HotPeriodicAuditWorkerResult<Client>>>,
}

impl<Client> fmt::Debug for HotPeriodicAuditWorker<Client> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HotPeriodicAuditWorker")
            .field("generation", &self.generation)
            .field("window_id", &self.window_id)
            .field("started_at", &self.started_at)
            .field("timeout", &self.timeout)
            .field("cancelled", &self.cancellation.is_cancelled())
            .field("has_handle", &self.handle.is_some())
            .finish()
    }
}

impl<Client> HotPeriodicAuditWorker<Client> {
    fn cancel_for_cleanup(&self) {
        self.cancellation.cancel();
    }

    fn cancel(&self) {
        self.cancel_for_cleanup();
        if let Some(handle) = self.handle.as_ref() {
            handle.abort();
        }
    }

    fn is_finished(&self) -> bool {
        self.handle.as_ref().is_some_and(JoinHandle::is_finished)
    }

    async fn join(
        mut self,
    ) -> std::result::Result<HotPeriodicAuditWorkerResult<Client>, tokio::task::JoinError> {
        self.handle.take().expect("hot periodic audit worker handle should exist").await
    }
}

impl<Client> HotPeriodicAuditWorker<Client>
where
    Client: StateProviderFactory
        + ChainSpecProvider<ChainSpec: EthChainSpec<Header = Header> + Upgrades>
        + BlockReaderIdExt<Header = Header>
        + Clone
        + Send
        + 'static,
{
    fn spawn(
        client: Client,
        max_depth: u64,
        snapshot: AuditWindowSnapshot,
        trigger_canonical_block: BlockNumber,
        timeout: Duration,
    ) -> Self {
        let generation = snapshot.generation;
        let window_id = snapshot.window_id;
        let started_at = Instant::now();
        let cancellation = HotPeriodicAuditCancellation::new();
        let worker_cancellation = cancellation.clone();
        let handle = tokio::task::spawn_blocking(move || {
            let (audit_result, rebuilt_engine) =
                HotEngine::rebuild_shadow_from_snapshot_with_cancellation(
                    client,
                    max_depth,
                    &snapshot,
                    || worker_cancellation.is_cancelled(),
                );
            HotPeriodicAuditWorkerResult {
                snapshot,
                trigger_canonical_block,
                audit_result,
                rebuilt_engine,
            }
        });

        Self { generation, window_id, started_at, timeout, cancellation, handle: Some(handle) }
    }
}

impl<Client> Drop for HotPeriodicAuditWorker<Client> {
    fn drop(&mut self) {
        self.cancel();
    }
}

#[derive(Debug)]
struct HotPeriodicAuditWorkerResult<Client> {
    snapshot: AuditWindowSnapshot,
    trigger_canonical_block: BlockNumber,
    audit_result: PeriodicAuditResult,
    rebuilt_engine: Option<HotEngine<Client>>,
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
        handles: StateProcessorHandles,
    ) -> Self {
        Self::new_with_mode(client, pending_blocks, max_depth, FlashblocksMode::Legacy, handles)
    }

    /// Creates a new state processor wired to the provided channels and state with an explicit
    /// runtime mode.
    pub fn new_with_mode(
        client: Client,
        pending_blocks: Arc<ArcSwapOption<PendingBlocks>>,
        max_depth: u64,
        mode: FlashblocksMode,
        handles: StateProcessorHandles,
    ) -> Self {
        Self::new_with_mode_and_hot_periodic_audit_config(
            client,
            pending_blocks,
            max_depth,
            mode,
            handles,
            HotPeriodicAuditConfig::default(),
        )
    }

    fn new_with_mode_and_hot_periodic_audit_config(
        client: Client,
        pending_blocks: Arc<ArcSwapOption<PendingBlocks>>,
        max_depth: u64,
        mode: FlashblocksMode,
        handles: StateProcessorHandles,
        hot_periodic_audit_config: HotPeriodicAuditConfig,
    ) -> Self {
        let latest_canonical_block = client.best_block_number().unwrap_or_default();
        let cache = FlashblockCache::new(latest_canonical_block);
        let hot_engine = (mode == FlashblocksMode::HotOnly)
            .then(|| Arc::new(Mutex::new(HotEngine::new(client.clone(), max_depth))));
        let StateProcessorHandles {
            rx,
            fast_sender,
            sender,
            snapshot_cache,
            hot_snapshot_ring,
            latest_hot_dry_run_seed,
        } = handles;

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
            latest_hot_dry_run_seed,
            next_snapshot_nonce: Arc::new(AtomicU64::new(0)),
            hot_engine,
            hot_periodic_audit: Arc::new(StdMutex::new(HotPeriodicAuditState::new(
                latest_canonical_block,
            ))),
            hot_periodic_audit_config,
        }
    }

    /// Returns the configured flashblocks runtime mode.
    pub const fn mode(&self) -> FlashblocksMode {
        self.mode
    }

    /// Processes updates from the queue until the channel closes.
    pub async fn start(&self) {
        while let Some(update) = self.rx.lock().await.recv().await {
            if let StateUpdate::Flashblock { enqueued_at, .. } = &update {
                Metrics::state_queue_delay_duration().record(enqueued_at.elapsed());
            }

            if self.mode == FlashblocksMode::HotOnly {
                self.poll_hot_only_periodic_audit_worker().await;
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

        if self.mode == FlashblocksMode::HotOnly {
            self.cancel_hot_periodic_audit_workers();
        }
    }

    const fn hot_engine(&self) -> &Arc<Mutex<HotEngine<Client>>> {
        self.hot_engine.as_ref().expect("hot engine should exist in hot-only mode")
    }

    fn update_hot_periodic_audit_latest_seen_canonical(&self, block_number: BlockNumber) {
        let mut state = self.hot_periodic_audit.lock().expect("hot periodic audit mutex poisoned");
        state.latest_seen_canonical_block = block_number;
    }

    fn update_hot_periodic_audit_reconciled_canonical(
        &self,
        block_number: BlockNumber,
        block_hash: B256,
    ) {
        let mut state = self.hot_periodic_audit.lock().expect("hot periodic audit mutex poisoned");
        state.last_reconciled_canonical =
            Some(HotPeriodicAuditCanonicalBlock::new(block_number, block_hash));
    }

    fn cancel_hot_periodic_audit_worker_locked(state: &mut HotPeriodicAuditState<Client>) {
        let Some(worker) = state.worker.take() else {
            return;
        };

        worker.cancel_for_cleanup();
        debug_assert!(state.cleanup_worker.is_none(), "cleanup worker should be bounded to one");
        if state.cleanup_worker.is_none() {
            state.cleanup_worker = Some(worker);
        }
    }

    fn advance_hot_periodic_audit_generation(&self) {
        let mut state = self.hot_periodic_audit.lock().expect("hot periodic audit mutex poisoned");
        Self::cancel_hot_periodic_audit_worker_locked(&mut state);
        state.current_generation = state.current_generation.saturating_add(1);
        state.current_window_id = 0;
        state.last_successful_audit_cursor_block = None;
    }

    fn cancel_hot_periodic_audit_workers(&self) {
        let (worker, cleanup_worker) = {
            let mut state =
                self.hot_periodic_audit.lock().expect("hot periodic audit mutex poisoned");
            (state.worker.take(), state.cleanup_worker.take())
        };

        if let Some(worker) = worker {
            worker.cancel();
        }
        if let Some(worker) = cleanup_worker {
            worker.cancel();
        }
    }

    async fn poll_hot_only_periodic_audit_worker(&self) {
        let cleanup_worker = {
            let mut state =
                self.hot_periodic_audit.lock().expect("hot periodic audit mutex poisoned");
            if state.cleanup_worker.as_ref().is_some_and(HotPeriodicAuditWorker::is_finished) {
                Some(
                    state
                        .cleanup_worker
                        .take()
                        .expect("hot periodic audit cleanup worker should exist"),
                )
            } else {
                None
            }
        };
        if let Some(cleanup_worker) = cleanup_worker {
            _ = cleanup_worker.join().await;
        }

        let mut timed_out_duration = None;
        let completed_worker = {
            let mut state =
                self.hot_periodic_audit.lock().expect("hot periodic audit mutex poisoned");
            let Some((worker_is_current, worker_elapsed, worker_timeout, worker_finished)) =
                state.worker.as_ref().map(|worker| {
                    (
                        worker.generation == state.current_generation
                            && worker.window_id == state.current_window_id,
                        worker.started_at.elapsed(),
                        worker.timeout,
                        worker.is_finished(),
                    )
                })
            else {
                return;
            };

            if !worker_is_current {
                Self::cancel_hot_periodic_audit_worker_locked(&mut state);
                None
            } else if worker_elapsed > worker_timeout {
                timed_out_duration = Some(worker_elapsed);
                Self::cancel_hot_periodic_audit_worker_locked(&mut state);
                None
            } else if worker_finished {
                Some(state.worker.take().expect("hot periodic audit worker should exist"))
            } else {
                None
            }
        };

        if let Some(duration) = timed_out_duration {
            Metrics::hot_periodic_audit_duration().record(duration);
            Metrics::hot_periodic_audit_failure_count().increment(1);
            Metrics::hot_periodic_audit_timeout_count().increment(1);
            self.handle_hot_invalidation(
                None,
                HotInvalidationReason::PeriodicAuditFailed {
                    failure: PeriodicAuditFailure::Timeout,
                },
            )
            .await;
            return;
        }

        let Some(worker) = completed_worker else {
            return;
        };
        let duration = worker.started_at.elapsed();
        match worker.join().await {
            Ok(result) => {
                self.handle_hot_only_periodic_audit_result(result, duration).await;
            }
            Err(error) if error.is_cancelled() => {
                Metrics::hot_periodic_audit_stale_result_count().increment(1);
            }
            Err(_error) => {
                Metrics::hot_periodic_audit_duration().record(duration);
                Metrics::hot_periodic_audit_failure_count().increment(1);
                self.handle_hot_invalidation(
                    None,
                    HotInvalidationReason::PeriodicAuditFailed {
                        failure: PeriodicAuditFailure::WorkerError,
                    },
                )
                .await;
            }
        }
    }

    async fn handle_hot_only_periodic_audit_result(
        &self,
        result: HotPeriodicAuditWorkerResult<Client>,
        duration: Duration,
    ) {
        Metrics::hot_periodic_audit_duration().record(duration);
        let HotPeriodicAuditWorkerResult {
            snapshot,
            trigger_canonical_block,
            audit_result,
            rebuilt_engine,
        } = result;

        let mut hot_engine = self.hot_engine().lock().await;
        let completion = {
            let state = self.hot_periodic_audit.lock().expect("hot periodic audit mutex poisoned");
            hot_engine.complete_shadow_rebuild(
                state.current_generation,
                state.current_window_id,
                &snapshot,
                audit_result,
                rebuilt_engine,
            )
        };
        drop(hot_engine);

        match completion {
            ShadowRebuildCompletion::EquivalentSwapped => {
                Metrics::hot_periodic_audit_success_count().increment(1);
                self.clear_hot_snapshot_ring();
                {
                    let mut state =
                        self.hot_periodic_audit.lock().expect("hot periodic audit mutex poisoned");
                    state.last_successful_audit_canonical_block = trigger_canonical_block;
                    state.last_successful_audit_cursor_block = Some(snapshot.cursor.block_number);
                }
                self.prune_hot_only_checked_canonical_prefix(None, None).await;
            }
            ShadowRebuildCompletion::EquivalentNoOp => {
                Metrics::hot_periodic_audit_success_count().increment(1);
                {
                    let mut state =
                        self.hot_periodic_audit.lock().expect("hot periodic audit mutex poisoned");
                    state.last_successful_audit_canonical_block = trigger_canonical_block;
                    state.last_successful_audit_cursor_block = Some(snapshot.cursor.block_number);
                }
                self.prune_hot_only_checked_canonical_prefix(None, None).await;
            }
            ShadowRebuildCompletion::StaleIgnored => {
                Metrics::hot_periodic_audit_stale_result_count().increment(1);
            }
            ShadowRebuildCompletion::ActiveFailed { failure } => {
                Metrics::hot_periodic_audit_failure_count().increment(1);
                self.handle_hot_invalidation(
                    None,
                    HotInvalidationReason::PeriodicAuditFailed { failure },
                )
                .await;
            }
        }
    }

    async fn prune_hot_only_checked_canonical_prefix(
        &self,
        known_canonical_block_number: Option<BlockNumber>,
        known_canonical_hash: Option<B256>,
    ) {
        let (target_block_number, latest_checked_block_number) = {
            let state = self.hot_periodic_audit.lock().expect("hot periodic audit mutex poisoned");
            let Some(latest_checked_block_number) = state.last_successful_audit_cursor_block else {
                return;
            };
            let Some(reconciled_canonical) = state.last_reconciled_canonical else {
                return;
            };
            (
                latest_checked_block_number.min(reconciled_canonical.number),
                latest_checked_block_number,
            )
        };

        let should_prune = {
            let hot_engine = self.hot_engine().lock().await;
            hot_engine.window.anchor.block_number() < target_block_number
        };
        if !should_prune {
            return;
        }

        let canonical_hash = if known_canonical_block_number == Some(target_block_number) {
            match known_canonical_hash {
                Some(known_canonical_hash) => known_canonical_hash,
                None => return,
            }
        } else {
            let reconciled_hash = {
                let state =
                    self.hot_periodic_audit.lock().expect("hot periodic audit mutex poisoned");
                state
                    .last_reconciled_canonical
                    .filter(|reconciled_canonical| {
                        reconciled_canonical.number == target_block_number
                    })
                    .map(|reconciled_canonical| reconciled_canonical.hash)
            };
            if let Some(reconciled_hash) = reconciled_hash {
                reconciled_hash
            } else {
                let Some(header) = self
                    .client
                    .header_by_number(target_block_number)
                    .map_err(|error| {
                        error!(
                            block_number = target_block_number,
                            error = %error,
                            "failed to read canonical header for hot periodic audit prune"
                        );
                        error
                    })
                    .ok()
                    .flatten()
                else {
                    return;
                };
                header.hash_slow()
            }
        };

        self.hot_engine()
            .lock()
            .await
            .window
            .prune_canonicalized_prefix_through(target_block_number, canonical_hash);

        if target_block_number >= latest_checked_block_number {
            self.hot_periodic_audit
                .lock()
                .expect("hot periodic audit mutex poisoned")
                .last_successful_audit_cursor_block = None;
        }
    }

    async fn maybe_trigger_hot_only_periodic_audit(&self, block: &RecoveredBlock<BaseBlock>) {
        self.poll_hot_only_periodic_audit_worker().await;
        self.prune_hot_only_checked_canonical_prefix(
            Some(block.number),
            Some(block.header().hash_slow()),
        )
        .await;

        let (current_generation, next_window_id, interval_elapsed, has_in_flight_audit) = {
            let state = self.hot_periodic_audit.lock().expect("hot periodic audit mutex poisoned");
            let has_current_worker = state.worker.as_ref().is_some_and(|worker| {
                worker.generation == state.current_generation
                    && worker.window_id == state.current_window_id
            });
            let has_cleanup_worker = state.cleanup_worker.is_some();
            let has_in_flight_audit = has_current_worker || has_cleanup_worker;
            let baseline_block = if has_in_flight_audit {
                state.last_started_audit_canonical_block
            } else {
                state.last_successful_audit_canonical_block
            };
            (
                state.current_generation,
                state.current_window_id.saturating_add(1),
                block.number.saturating_sub(baseline_block)
                    >= self.hot_periodic_audit_config.interval_blocks,
                has_in_flight_audit,
            )
        };

        if !interval_elapsed {
            return;
        }

        if has_in_flight_audit {
            Metrics::hot_periodic_audit_failure_count().increment(1);
            Metrics::hot_periodic_audit_overlap_count().increment(1);
            self.handle_hot_invalidation(
                Some(block.number),
                HotInvalidationReason::PeriodicAuditFailed {
                    failure: PeriodicAuditFailure::Overlap,
                },
            )
            .await;
            return;
        }

        let snapshot = {
            let hot_engine = self.hot_engine().lock().await;
            let anchor = hot_engine.window.anchor;
            hot_engine.window.make_audit_snapshot(
                current_generation,
                next_window_id,
                anchor.block_number(),
                anchor.hash(),
            )
        };
        let Some(snapshot) = snapshot else {
            return;
        };

        Metrics::hot_periodic_audit_replayed_flashblock_count()
            .record(snapshot.flashblocks.len() as f64);
        let worker = HotPeriodicAuditWorker::spawn(
            self.client.clone(),
            self.max_depth,
            snapshot.clone(),
            block.number,
            self.hot_periodic_audit_config.timeout,
        );

        let overlap_detected = {
            let mut state =
                self.hot_periodic_audit.lock().expect("hot periodic audit mutex poisoned");
            if state.current_generation != snapshot.generation {
                let worker = worker;
                worker.cancel();
                if state.cleanup_worker.is_none() {
                    state.cleanup_worker = Some(worker);
                }
                false
            } else if state.worker.is_some() || state.cleanup_worker.is_some() {
                let worker = worker;
                worker.cancel();
                if state.cleanup_worker.is_none() {
                    state.cleanup_worker = Some(worker);
                }
                true
            } else {
                state.current_window_id = snapshot.window_id;
                state.last_started_audit_canonical_block = block.number;
                state.worker = Some(worker);
                false
            }
        };
        if overlap_detected {
            Metrics::hot_periodic_audit_failure_count().increment(1);
            Metrics::hot_periodic_audit_overlap_count().increment(1);
            self.handle_hot_invalidation(
                Some(block.number),
                HotInvalidationReason::PeriodicAuditFailed {
                    failure: PeriodicAuditFailure::Overlap,
                },
            )
            .await;
        }
    }

    async fn apply_hot_only_canonical(&self, block: RecoveredBlock<BaseBlock>) {
        self.update_hot_periodic_audit_latest_seen_canonical(block.number);
        let block_hash = block.header().hash_slow();
        let mut hot_engine = self.hot_engine().lock().await;
        let outcome = hot_engine.process_canonical_block(&block);
        match outcome {
            Ok(HotApplyOutcome::Delta { delta, snapshot, .. }) => {
                self.publish_hot_only_delta(&hot_engine, delta, snapshot);
            }
            Ok(HotApplyOutcome::Duplicate) => {}
            Ok(HotApplyOutcome::CanonicalWindowChanged) => {
                self.advance_hot_periodic_audit_generation();
                self.clear_hot_snapshot_ring();
            }
            Ok(HotApplyOutcome::Reset) => {
                self.advance_hot_periodic_audit_generation();
                self.clear_hot_snapshot_ring();
                _ = self.fast_sender.send(FastFlashblockFeedEvent::Resync);
            }
            Ok(HotApplyOutcome::InvalidateSession { reason }) => {
                drop(hot_engine);
                self.handle_hot_invalidation(Some(block.number), reason).await;
                return;
            }
            Err(e) => {
                error!(message = "could not process canonical block", error = %e);
                return;
            }
        }
        drop(hot_engine);

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

        self.update_hot_periodic_audit_reconciled_canonical(block.number, block_hash);
        self.maybe_trigger_hot_only_periodic_audit(&block).await;
    }

    fn record_hot_cache_drain(cached: &[CachedFlashblock]) {
        Metrics::hot_cache_drain_flashblock_count().record(cached.len() as f64);
        for cached in cached {
            Metrics::hot_cache_dwell_duration().record(cached.inserted_at.elapsed());
        }
    }

    fn publish_hot_only_delta(
        &self,
        hot_engine: &HotEngine<Client>,
        delta: Box<FastFlashblockLogsDelta>,
        snapshot: Option<Box<HotSnapshot>>,
    ) {
        let hot_snapshot = snapshot.map(|snapshot| {
            let snapshot = Arc::new(*snapshot);
            self.hot_snapshot_ring
                .lock()
                .expect("hot snapshot ring mutex poisoned")
                .insert(Arc::clone(&snapshot));
            snapshot
        });

        _ = self.fast_sender.send(FastFlashblockFeedEvent::Delta(Arc::new(*delta)));

        if let Some(hot_snapshot) = hot_snapshot {
            if let Some(seed) = hot_engine.fork_dry_run_seed(Arc::clone(&hot_snapshot)) {
                self.latest_hot_dry_run_seed.publish(Arc::new(seed));
            }
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

            let mut hot_engine = self.hot_engine().lock().await;
            let missing_first_flashblock = (hot_engine.window.execution.is_none()
                || hot_engine.window.blocks.is_empty())
                && flashblock.index > 0;
            let outcome = hot_engine.apply_flashblock(&flashblock);
            let hot_window_invalidated =
                hot_engine.window.execution.is_none() || hot_engine.window.blocks.is_empty();

            match outcome {
                Ok(HotApplyOutcome::Delta { delta, snapshot, ready_cached_block }) => {
                    self.publish_hot_only_delta(&hot_engine, delta, snapshot);
                    drop(hot_engine);
                    Metrics::block_processing_duration().record(block_processing_start.elapsed());

                    if let Some(ready_cached_block) = ready_cached_block {
                        let cached = {
                            let mut cache = self.cache.lock().await;
                            cache.drain_cached(ready_cached_block)
                        };
                        if !cached.is_empty() {
                            Self::record_hot_cache_drain(&cached);
                            debug!(
                                message = "replaying cached flashblocks after block acceptance",
                                ready_cached_block,
                                cached_count = cached.len(),
                            );
                            replay_queue.extend(cached.into_iter().map(|cached| cached.flashblock));
                        }
                    }
                }
                Ok(HotApplyOutcome::Duplicate) => {
                    drop(hot_engine);
                    Metrics::block_processing_duration().record(block_processing_start.elapsed());
                }
                Ok(HotApplyOutcome::CanonicalWindowChanged) => {
                    drop(hot_engine);
                    self.advance_hot_periodic_audit_generation();
                    self.clear_hot_snapshot_ring();
                    Metrics::block_processing_duration().record(block_processing_start.elapsed());
                }
                Ok(HotApplyOutcome::Reset) => {
                    drop(hot_engine);
                    self.advance_hot_periodic_audit_generation();
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
                    drop(hot_engine);
                    self.handle_hot_invalidation(None, reason).await;
                    Metrics::block_processing_duration().record(block_processing_start.elapsed());
                    return true;
                }
                Err(e) => {
                    drop(hot_engine);
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
                        self.advance_hot_periodic_audit_generation();
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
        self.advance_hot_periodic_audit_generation();
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
        self.latest_hot_dry_run_seed.clear();
    }

    async fn handle_hot_invalidation(
        &self,
        canonical_block_number: Option<BlockNumber>,
        reason: HotInvalidationReason,
    ) {
        if let Some(canonical_block_number) = canonical_block_number {
            self.update_hot_periodic_audit_latest_seen_canonical(canonical_block_number);
        }
        self.advance_hot_periodic_audit_generation();
        self.hot_engine().lock().await.reset();
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
    use std::{
        collections::VecDeque,
        sync::{Arc, mpsc as std_mpsc},
        time::{Duration, Instant},
    };

    use alloy_primitives::{Address, B256, BlockNumber, Bloom, Bytes, U256, hex_literal::hex};
    use alloy_rpc_types_engine::PayloadId;
    use arc_swap::ArcSwapOption;
    use base_common_consensus::{BaseBlock, BasePrimitives};
    use base_common_flashblocks::{
        ExecutionPayloadBaseV1, ExecutionPayloadFlashblockDeltaV1, Flashblock, Metadata,
    };
    use base_execution_chainspec::{BaseChainSpec, BaseChainSpecBuilder};
    use jsonrpsee::{
        RpcModule, SubscriptionSink,
        core::{EmptyServerParams, SubscriptionResult},
        server::SubscriptionMessage,
    };
    use reth_chainspec::ChainSpecProvider;
    use reth_primitives::RecoveredBlock;
    use reth_provider::test_utils::MockEthProvider;
    use reth_storage_api::HeaderProvider;
    use serde_json::Value;
    use tokio::{
        sync::{Mutex, broadcast, mpsc, oneshot},
        task::yield_now,
        time::timeout,
    };

    use super::{StateProcessor, StateProcessorHandles, StateUpdate};
    use crate::{
        BlockAssembler, FastFlashblockFeedEvent, FlashblockSnapshotId, FlashblocksMode, HotEngine,
        HotSnapshotRing, LatestHotDryRunSeedCache, PeriodicAuditFailure, PeriodicAuditResult,
        SnapshotCache,
    };

    const RECV_TIMEOUT: Duration = Duration::from_secs(1);

    type TestClient = MockEthProvider<BasePrimitives, Arc<BaseChainSpec>>;
    type TestProcessor = StateProcessor<TestClient>;

    struct HotOnlyAuditFixture {
        latest_snapshot_id: FlashblockSnapshotId,
    }

    struct TaskDropNotifier(Option<oneshot::Sender<()>>);

    impl Drop for TaskDropNotifier {
        fn drop(&mut self) {
            if let Some(sender) = self.0.take() {
                _ = sender.send(());
            }
        }
    }

    fn test_client() -> TestClient {
        let chain_spec = Arc::new(BaseChainSpecBuilder::base_mainnet().build());
        MockEthProvider::<BasePrimitives>::new().with_chain_spec(chain_spec).with_genesis_block()
    }

    fn test_state_processor_handles(
        rx: mpsc::UnboundedReceiver<StateUpdate>,
        fast_sender: broadcast::Sender<FastFlashblockFeedEvent>,
        sender: broadcast::Sender<Arc<crate::PendingBlocks>>,
        capacity: usize,
    ) -> StateProcessorHandles {
        StateProcessorHandles::new(
            Arc::new(Mutex::new(rx)),
            fast_sender,
            sender,
            Arc::new(std::sync::Mutex::new(SnapshotCache::new(capacity, Duration::from_secs(1)))),
            Arc::new(std::sync::Mutex::new(HotSnapshotRing::new(capacity))),
            Arc::new(LatestHotDryRunSeedCache::default()),
        )
    }

    fn hot_only_test_processor_with_audit_config(
        interval_blocks: BlockNumber,
        timeout_duration: Duration,
    ) -> (TestProcessor, broadcast::Receiver<FastFlashblockFeedEvent>) {
        let (processor, _fast_sender, fast_receiver) =
            hot_only_test_processor_with_audit_config_and_sender(interval_blocks, timeout_duration);

        (processor, fast_receiver)
    }

    fn hot_only_test_processor_with_audit_config_and_sender(
        interval_blocks: BlockNumber,
        timeout_duration: Duration,
    ) -> (
        TestProcessor,
        broadcast::Sender<FastFlashblockFeedEvent>,
        broadcast::Receiver<FastFlashblockFeedEvent>,
    ) {
        let (_tx, rx) = mpsc::unbounded_channel();
        let (fast_sender, fast_receiver) = broadcast::channel(8);
        let (sender, _) = broadcast::channel(1);
        let client = test_client();

        let processor = StateProcessor::new_with_mode_and_hot_periodic_audit_config(
            client,
            Arc::new(ArcSwapOption::new(None)),
            5,
            FlashblocksMode::HotOnly,
            test_state_processor_handles(rx, fast_sender.clone(), sender, 8),
            super::HotPeriodicAuditConfig { interval_blocks, timeout: timeout_duration },
        );

        (processor, fast_sender, fast_receiver)
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
                block_hash: fixture_wire_block_hash(block_number, index),
                transactions,
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

    fn test_hot_only_canonical_block(block_number: u64) -> RecoveredBlock<BaseBlock> {
        RecoveredBlock::new_unhashed(
            BlockAssembler::assemble(&[test_hot_flashblock(
                0,
                block_number,
                PayloadId::new([block_number as u8; 8]),
                B256::ZERO,
                true,
                vec![encoded_l1_info_tx()],
            )])
            .expect("canonical flashblock should assemble")
            .block,
            Vec::new(),
        )
    }

    async fn recv_fast_flashblock_event(
        receiver: &mut broadcast::Receiver<FastFlashblockFeedEvent>,
    ) -> FastFlashblockFeedEvent {
        timeout(RECV_TIMEOUT, receiver.recv())
            .await
            .expect("fast flashblock event should arrive")
            .expect("fast flashblock channel should stay open")
    }

    async fn pipe_fast_flashblock_logs_subscription_for_test(
        sink: SubscriptionSink,
        mut receiver: broadcast::Receiver<FastFlashblockFeedEvent>,
    ) {
        loop {
            tokio::select! {
                _ = sink.closed() => return,
                result = receiver.recv() => {
                    let event = match result {
                        Ok(event) => event,
                        Err(
                            broadcast::error::RecvError::Closed
                            | broadcast::error::RecvError::Lagged(_),
                        ) => return,
                    };
                    let delta = match event {
                        FastFlashblockFeedEvent::Delta(delta) => delta,
                        FastFlashblockFeedEvent::Resync => continue,
                        FastFlashblockFeedEvent::InvalidateSession => return,
                    };
                    let msg = match SubscriptionMessage::new(
                        sink.method_name(),
                        sink.subscription_id(),
                        delta.as_ref(),
                    ) {
                        Ok(msg) => msg,
                        Err(_error) => return,
                    };
                    if sink.send(msg).await.is_err() {
                        return;
                    }
                }
            }
        }
    }

    async fn wait_for_hot_only_periodic_audit_workers_to_clear(processor: &TestProcessor) {
        timeout(RECV_TIMEOUT, async {
            loop {
                processor.poll_hot_only_periodic_audit_worker().await;
                let workers_cleared = {
                    let state = processor
                        .hot_periodic_audit
                        .lock()
                        .expect("hot periodic audit mutex poisoned");
                    state.worker.is_none() && state.cleanup_worker.is_none()
                };
                if workers_cleared {
                    break;
                }
                yield_now().await;
            }
        })
        .await
        .expect("hot periodic audit worker should complete");
    }

    async fn seed_hot_only_periodic_audit_fixture(
        processor: &TestProcessor,
        fast_receiver: &mut broadcast::Receiver<FastFlashblockFeedEvent>,
    ) -> HotOnlyAuditFixture {
        let parent_hash = processor.client.chain_spec().genesis_hash();
        let first_flashblock = test_hot_flashblock(
            0,
            1,
            PayloadId::new([0xc1; 8]),
            parent_hash,
            true,
            vec![encoded_l1_info_tx()],
        );

        processor.apply_hot_only_flashblock(first_flashblock.clone()).await;
        let first_event = recv_fast_flashblock_event(fast_receiver).await;
        let FastFlashblockFeedEvent::Delta(_first_delta) = first_event else {
            panic!("expected first hot-only delta event");
        };

        let wire_parent_hash = {
            let hot_engine = processor.hot_engine().lock().await;
            hot_engine
                .window
                .active_block()
                .expect("active pending block should exist after first flashblock")
                .latest_wire_header_hash
        };
        let second_flashblock = test_hot_flashblock(
            0,
            2,
            PayloadId::new([0xc2; 8]),
            wire_parent_hash,
            true,
            vec![encoded_l1_info_tx()],
        );

        processor.apply_hot_only_flashblock(second_flashblock).await;
        let second_event = recv_fast_flashblock_event(fast_receiver).await;
        let FastFlashblockFeedEvent::Delta(second_delta) = second_event else {
            panic!("expected second hot-only delta event");
        };

        let canonical_block = RecoveredBlock::new_unhashed(
            BlockAssembler::assemble(&[first_flashblock])
                .expect("canonical flashblock should assemble")
                .block,
            Vec::new(),
        );
        processor
            .client
            .add_header(canonical_block.header().hash_slow(), canonical_block.header().clone());

        HotOnlyAuditFixture { latest_snapshot_id: second_delta.snapshot_id }
    }

    async fn capture_next_hot_only_periodic_audit_snapshot(
        processor: &TestProcessor,
    ) -> super::AuditWindowSnapshot {
        let (generation, window_id) = {
            let state =
                processor.hot_periodic_audit.lock().expect("hot periodic audit mutex poisoned");
            (state.current_generation, state.current_window_id.saturating_add(1))
        };
        let hot_engine = processor.hot_engine().lock().await;
        let anchor = hot_engine.window.anchor;

        hot_engine
            .window
            .make_audit_snapshot(generation, window_id, anchor.block_number(), anchor.hash())
            .expect("hot periodic audit snapshot should exist")
    }

    fn install_hot_only_periodic_audit_result(
        processor: &TestProcessor,
        snapshot: super::AuditWindowSnapshot,
        trigger_canonical_block: BlockNumber,
        audit_result: PeriodicAuditResult,
        rebuilt_engine: Option<HotEngine<TestClient>>,
        started_at: Instant,
        timeout_duration: Duration,
    ) {
        let result = super::HotPeriodicAuditWorkerResult {
            snapshot: snapshot.clone(),
            trigger_canonical_block,
            audit_result,
            rebuilt_engine,
        };

        let mut state =
            processor.hot_periodic_audit.lock().expect("hot periodic audit mutex poisoned");
        state.current_window_id = snapshot.window_id;
        state.last_started_audit_canonical_block = trigger_canonical_block;
        state.worker = Some(super::HotPeriodicAuditWorker {
            generation: snapshot.generation,
            window_id: snapshot.window_id,
            started_at,
            timeout: timeout_duration,
            cancellation: super::HotPeriodicAuditCancellation::new(),
            handle: Some(tokio::spawn(async move { result })),
        });
    }

    fn install_hot_only_open_periodic_audit_worker(
        processor: &TestProcessor,
        generation: u64,
        window_id: u64,
        started_at: Instant,
        timeout_duration: Duration,
    ) -> std_mpsc::Sender<super::HotPeriodicAuditWorkerResult<TestClient>> {
        let (tx, receiver) = std_mpsc::channel();

        let mut state =
            processor.hot_periodic_audit.lock().expect("hot periodic audit mutex poisoned");
        state.current_window_id = window_id;
        state.worker = Some(super::HotPeriodicAuditWorker {
            generation,
            window_id,
            started_at,
            timeout: timeout_duration,
            cancellation: super::HotPeriodicAuditCancellation::new(),
            handle: Some(tokio::task::spawn_blocking(move || {
                receiver.recv().expect("hot periodic audit result should send")
            })),
        });

        tx
    }

    fn stale_hot_only_periodic_audit_worker_result(
        generation: u64,
        window_id: u64,
    ) -> super::HotPeriodicAuditWorkerResult<TestClient> {
        super::HotPeriodicAuditWorkerResult {
            snapshot: super::AuditWindowSnapshot::new(
                generation,
                window_id,
                0,
                B256::ZERO,
                crate::AuditCursor::new(0, 0, PayloadId::new([0; 8]), B256::ZERO),
                Vec::new(),
                Vec::new(),
            ),
            trigger_canonical_block: 0,
            audit_result: PeriodicAuditResult::StaleIgnored,
            rebuilt_engine: None,
        }
    }

    fn pending_hot_only_periodic_audit_worker(
        generation: u64,
        window_id: u64,
        timeout_duration: Duration,
    ) -> (
        super::HotPeriodicAuditWorker<TestClient>,
        super::HotPeriodicAuditCancellation,
        oneshot::Receiver<()>,
    ) {
        let cancellation = super::HotPeriodicAuditCancellation::new();
        let cancellation_probe = cancellation.clone();
        let worker_cancellation = cancellation.clone();
        let (drop_tx, drop_rx) = oneshot::channel();
        let handle = tokio::spawn(async move {
            let _drop_notifier = TaskDropNotifier(Some(drop_tx));
            loop {
                if worker_cancellation.is_cancelled() {
                    break;
                }
                yield_now().await;
            }

            stale_hot_only_periodic_audit_worker_result(generation, window_id)
        });

        (
            super::HotPeriodicAuditWorker {
                generation,
                window_id,
                started_at: Instant::now(),
                timeout: timeout_duration,
                cancellation,
                handle: Some(handle),
            },
            cancellation_probe,
            drop_rx,
        )
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
            test_state_processor_handles(rx, fast_sender, sender, 1),
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
            test_state_processor_handles(rx, fast_sender, sender, 1),
        );

        assert_eq!(processor.mode(), FlashblocksMode::HotOnly);
    }

    #[tokio::test]
    async fn hot_only_periodic_audit_equivalent_swap_clears_hot_snapshot_ring() {
        let (processor, mut fast_receiver) =
            hot_only_test_processor_with_audit_config(120, Duration::from_secs(5));
        let fixture = seed_hot_only_periodic_audit_fixture(&processor, &mut fast_receiver).await;

        let snapshot = capture_next_hot_only_periodic_audit_snapshot(&processor).await;
        let (audit_result, rebuilt_engine) = HotEngine::rebuild_shadow_from_snapshot(
            processor.client.clone(),
            processor.max_depth,
            &snapshot,
        );
        install_hot_only_periodic_audit_result(
            &processor,
            snapshot,
            1,
            audit_result,
            rebuilt_engine,
            Instant::now(),
            Duration::from_secs(5),
        );

        wait_for_hot_only_periodic_audit_workers_to_clear(&processor).await;

        assert!(matches!(fast_receiver.try_recv(), Err(broadcast::error::TryRecvError::Empty)));
        assert!(
            processor
                .hot_snapshot_ring
                .lock()
                .expect("hot snapshot ring mutex poisoned")
                .get(fixture.latest_snapshot_id)
                .is_none()
        );

        let state = processor.hot_periodic_audit.lock().expect("hot periodic audit mutex poisoned");
        assert_eq!(state.last_successful_audit_canonical_block, 1);
        assert_eq!(state.last_successful_audit_cursor_block, Some(2));
    }

    #[tokio::test]
    async fn hot_only_canonical_reset_advances_audit_generation_and_resyncs() {
        let (processor, mut fast_receiver) =
            hot_only_test_processor_with_audit_config(120, Duration::from_secs(5));
        let fixture = seed_hot_only_periodic_audit_fixture(&processor, &mut fast_receiver).await;

        let generation_before = processor
            .hot_periodic_audit
            .lock()
            .expect("hot periodic audit mutex poisoned")
            .current_generation;
        let (worker, cancellation, drop_rx) =
            pending_hot_only_periodic_audit_worker(generation_before, 1, Duration::from_secs(5));
        {
            let mut state =
                processor.hot_periodic_audit.lock().expect("hot periodic audit mutex poisoned");
            state.current_window_id = 1;
            state.worker = Some(worker);
        }

        let canonical_block = RecoveredBlock::new_unhashed(
            BlockAssembler::assemble(&[test_hot_flashblock(
                0,
                1,
                PayloadId::new([0xc3; 8]),
                processor.client.chain_spec().genesis_hash(),
                true,
                vec![],
            )])
            .expect("conflicting canonical flashblock should assemble")
            .block,
            Vec::new(),
        );

        processor.apply_hot_only_canonical(canonical_block).await;

        let resync_event = recv_fast_flashblock_event(&mut fast_receiver).await;
        assert!(matches!(resync_event, FastFlashblockFeedEvent::Resync));

        assert!(cancellation.is_cancelled());
        let _ =
            timeout(RECV_TIMEOUT, drop_rx).await.expect("cancelled audit worker task should drop");

        {
            let state =
                processor.hot_periodic_audit.lock().expect("hot periodic audit mutex poisoned");
            assert_eq!(state.current_generation, generation_before.saturating_add(1));
            assert_eq!(state.current_window_id, 0);
            assert_eq!(state.last_successful_audit_canonical_block, 0);
            assert!(state.worker.is_none());
            assert!(state.cleanup_worker.is_some());
        }

        wait_for_hot_only_periodic_audit_workers_to_clear(&processor).await;

        assert!(
            processor
                .hot_snapshot_ring
                .lock()
                .expect("hot snapshot ring mutex poisoned")
                .get(fixture.latest_snapshot_id)
                .is_none()
        );

        let hot_engine = processor.hot_engine().lock().await;
        assert!(hot_engine.window.execution.is_none());
        assert!(hot_engine.window.blocks.is_empty());
    }

    #[tokio::test]
    async fn hot_only_periodic_audit_mismatch_invalidates_session() {
        let (processor, mut fast_receiver) =
            hot_only_test_processor_with_audit_config(120, Duration::from_secs(5));
        let fixture = seed_hot_only_periodic_audit_fixture(&processor, &mut fast_receiver).await;

        let snapshot = capture_next_hot_only_periodic_audit_snapshot(&processor).await;
        install_hot_only_periodic_audit_result(
            &processor,
            snapshot,
            1,
            PeriodicAuditResult::Diverged { failure: PeriodicAuditFailure::Mismatch },
            None,
            Instant::now(),
            Duration::from_secs(5),
        );

        wait_for_hot_only_periodic_audit_workers_to_clear(&processor).await;

        let invalidation_event = recv_fast_flashblock_event(&mut fast_receiver).await;
        assert!(matches!(invalidation_event, FastFlashblockFeedEvent::InvalidateSession));
        assert!(
            processor
                .hot_snapshot_ring
                .lock()
                .expect("hot snapshot ring mutex poisoned")
                .get(fixture.latest_snapshot_id)
                .is_none()
        );
    }

    #[tokio::test]
    async fn hot_only_periodic_audit_timeout_invalidates_session() {
        let (processor, mut fast_receiver) =
            hot_only_test_processor_with_audit_config(120, Duration::from_secs(5));
        let fixture = seed_hot_only_periodic_audit_fixture(&processor, &mut fast_receiver).await;

        let generation = processor
            .hot_periodic_audit
            .lock()
            .expect("hot periodic audit mutex poisoned")
            .current_generation;
        let hold_tx = install_hot_only_open_periodic_audit_worker(
            &processor,
            generation,
            1,
            Instant::now() - Duration::from_secs(1),
            Duration::from_millis(1),
        );

        processor.poll_hot_only_periodic_audit_worker().await;

        let invalidation_event = recv_fast_flashblock_event(&mut fast_receiver).await;
        assert!(matches!(invalidation_event, FastFlashblockFeedEvent::InvalidateSession));
        assert!(
            processor
                .hot_snapshot_ring
                .lock()
                .expect("hot snapshot ring mutex poisoned")
                .get(fixture.latest_snapshot_id)
                .is_none()
        );
        let hot_engine = processor.hot_engine().lock().await;
        assert!(hot_engine.window.execution.is_none());
        assert!(hot_engine.window.blocks.is_empty());
        drop(hot_engine);

        hold_tx
            .send(stale_hot_only_periodic_audit_worker_result(generation, 1))
            .expect("timed out audit worker result should still send for cleanup");
        wait_for_hot_only_periodic_audit_workers_to_clear(&processor).await;
    }

    #[tokio::test]
    async fn hot_only_periodic_audit_timeout_cancellation_discards_late_result() {
        let (processor, mut fast_receiver) =
            hot_only_test_processor_with_audit_config(120, Duration::from_secs(5));
        let fixture = seed_hot_only_periodic_audit_fixture(&processor, &mut fast_receiver).await;

        let snapshot = capture_next_hot_only_periodic_audit_snapshot(&processor).await;
        let (audit_result, rebuilt_engine) = HotEngine::rebuild_shadow_from_snapshot(
            processor.client.clone(),
            processor.max_depth,
            &snapshot,
        );
        let generation = snapshot.generation;
        let hold_tx = install_hot_only_open_periodic_audit_worker(
            &processor,
            generation,
            snapshot.window_id,
            Instant::now() - Duration::from_secs(1),
            Duration::from_millis(1),
        );

        processor.poll_hot_only_periodic_audit_worker().await;

        let invalidation_event = recv_fast_flashblock_event(&mut fast_receiver).await;
        assert!(matches!(invalidation_event, FastFlashblockFeedEvent::InvalidateSession));
        assert!(
            processor
                .hot_snapshot_ring
                .lock()
                .expect("hot snapshot ring mutex poisoned")
                .get(fixture.latest_snapshot_id)
                .is_none()
        );

        assert!(
            processor
                .hot_periodic_audit
                .lock()
                .expect("hot periodic audit mutex poisoned")
                .cleanup_worker
                .is_some()
        );

        hold_tx
            .send(super::HotPeriodicAuditWorkerResult {
                snapshot,
                trigger_canonical_block: 1,
                audit_result,
                rebuilt_engine,
            })
            .expect("timed out audit worker result should still send for cleanup");
        wait_for_hot_only_periodic_audit_workers_to_clear(&processor).await;

        assert!(matches!(fast_receiver.try_recv(), Err(broadcast::error::TryRecvError::Empty)));
        let hot_engine = processor.hot_engine().lock().await;
        assert!(hot_engine.window.execution.is_none());
        assert!(hot_engine.window.blocks.is_empty());
    }

    #[tokio::test]
    async fn hot_only_periodic_audit_pass_keeps_fast_subscription_live() {
        let (processor, fast_sender, mut fast_receiver) =
            hot_only_test_processor_with_audit_config_and_sender(120, Duration::from_secs(5));
        let mut module = RpcModule::new(());
        module
            .register_subscription::<SubscriptionResult, _, _>(
                "processor_audit_pass_fast_logs",
                "processor_audit_pass_fast_logs",
                "processor_audit_pass_fast_logs_unsubscribe",
                {
                    let fast_sender = fast_sender.clone();
                    move |_, pending, _, _| {
                        let fast_sender = fast_sender.clone();
                        async move {
                            let receiver = fast_sender.subscribe();
                            let sink = pending.accept().await?;
                            pipe_fast_flashblock_logs_subscription_for_test(sink, receiver).await;
                            Ok(())
                        }
                    }
                },
            )
            .unwrap();

        let mut subscription = module
            .subscribe_unbounded("processor_audit_pass_fast_logs", EmptyServerParams::new())
            .await
            .unwrap();

        seed_hot_only_periodic_audit_fixture(&processor, &mut fast_receiver).await;

        let first = timeout(RECV_TIMEOUT, subscription.next::<Value>()).await.unwrap();
        let (first, _) = first.expect("subscription should receive first hot delta").unwrap();
        assert_eq!(first["blockNumber"], "0x1");

        let second = timeout(RECV_TIMEOUT, subscription.next::<Value>()).await.unwrap();
        let (second, _) = second.expect("subscription should receive second hot delta").unwrap();
        assert_eq!(second["blockNumber"], "0x2");

        let snapshot = capture_next_hot_only_periodic_audit_snapshot(&processor).await;
        let (audit_result, rebuilt_engine) = HotEngine::rebuild_shadow_from_snapshot(
            processor.client.clone(),
            processor.max_depth,
            &snapshot,
        );
        install_hot_only_periodic_audit_result(
            &processor,
            snapshot,
            1,
            audit_result,
            rebuilt_engine,
            Instant::now(),
            Duration::from_secs(5),
        );

        wait_for_hot_only_periodic_audit_workers_to_clear(&processor).await;
        assert!(matches!(fast_receiver.try_recv(), Err(broadcast::error::TryRecvError::Empty)));

        let next_parent_hash = {
            let hot_engine = processor.hot_engine().lock().await;
            hot_engine
                .window
                .active_block()
                .expect("active pending block should remain after a passing audit")
                .latest_wire_header_hash
        };

        processor
            .apply_hot_only_flashblock(test_hot_flashblock(
                0,
                3,
                PayloadId::new([0xc3; 8]),
                next_parent_hash,
                true,
                vec![encoded_l1_info_tx()],
            ))
            .await;

        let next = timeout(RECV_TIMEOUT, subscription.next::<Value>()).await.unwrap();
        let (next, _) = next.expect("subscription should stay open after a passing audit").unwrap();
        assert_eq!(next["blockNumber"], "0x3");
        assert_eq!(next["flashblockIndex"], "0x0");
    }

    #[tokio::test]
    async fn hot_only_periodic_audit_failure_closes_fast_subscription() {
        let (processor, fast_sender, mut fast_receiver) =
            hot_only_test_processor_with_audit_config_and_sender(120, Duration::from_secs(5));
        let mut module = RpcModule::new(());
        module
            .register_subscription::<SubscriptionResult, _, _>(
                "processor_audit_failure_fast_logs",
                "processor_audit_failure_fast_logs",
                "processor_audit_failure_fast_logs_unsubscribe",
                {
                    let fast_sender = fast_sender.clone();
                    move |_, pending, _, _| {
                        let fast_sender = fast_sender.clone();
                        async move {
                            let receiver = fast_sender.subscribe();
                            let sink = pending.accept().await?;
                            pipe_fast_flashblock_logs_subscription_for_test(sink, receiver).await;
                            Ok(())
                        }
                    }
                },
            )
            .unwrap();

        let mut subscription = module
            .subscribe_unbounded("processor_audit_failure_fast_logs", EmptyServerParams::new())
            .await
            .unwrap();

        seed_hot_only_periodic_audit_fixture(&processor, &mut fast_receiver).await;

        let _ = timeout(RECV_TIMEOUT, subscription.next::<Value>()).await.unwrap();
        let _ = timeout(RECV_TIMEOUT, subscription.next::<Value>()).await.unwrap();

        let snapshot = capture_next_hot_only_periodic_audit_snapshot(&processor).await;
        install_hot_only_periodic_audit_result(
            &processor,
            snapshot,
            1,
            PeriodicAuditResult::Diverged { failure: PeriodicAuditFailure::Mismatch },
            None,
            Instant::now(),
            Duration::from_secs(5),
        );

        wait_for_hot_only_periodic_audit_workers_to_clear(&processor).await;

        let next = timeout(RECV_TIMEOUT, subscription.next::<Value>()).await.unwrap();
        assert!(
            next.is_none(),
            "subscription should close after an audit failure invalidates the session"
        );
    }

    #[tokio::test]
    async fn hot_only_continuity_failure_closes_fast_subscription() {
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
            test_state_processor_handles(rx, fast_sender.clone(), sender, 8),
        );

        let mut module = RpcModule::new(());
        module
            .register_subscription::<SubscriptionResult, _, _>(
                "processor_continuity_failure_fast_logs",
                "processor_continuity_failure_fast_logs",
                "processor_continuity_failure_fast_logs_unsubscribe",
                {
                    let fast_sender = fast_sender.clone();
                    move |_, pending, _, _| {
                        let fast_sender = fast_sender.clone();
                        async move {
                            let receiver = fast_sender.subscribe();
                            let sink = pending.accept().await?;
                            pipe_fast_flashblock_logs_subscription_for_test(sink, receiver).await;
                            Ok(())
                        }
                    }
                },
            )
            .unwrap();

        let mut subscription = module
            .subscribe_unbounded("processor_continuity_failure_fast_logs", EmptyServerParams::new())
            .await
            .unwrap();

        processor
            .apply_hot_only_flashblock(test_hot_flashblock(
                0,
                1,
                PayloadId::new([0xd1; 8]),
                parent_hash,
                true,
                vec![encoded_l1_info_tx()],
            ))
            .await;

        let first_event = recv_fast_flashblock_event(&mut fast_receiver).await;
        assert!(matches!(first_event, FastFlashblockFeedEvent::Delta(_)));

        let first = timeout(RECV_TIMEOUT, subscription.next::<Value>()).await.unwrap();
        let (first, _) = first.expect("subscription should receive initial hot delta").unwrap();
        assert_eq!(first["blockNumber"], "0x1");

        processor
            .apply_hot_only_flashblock(test_hot_flashblock(
                1,
                1,
                PayloadId::new([0xd2; 8]),
                B256::with_last_byte(0xee),
                false,
                vec![],
            ))
            .await;

        let next = timeout(RECV_TIMEOUT, subscription.next::<Value>()).await.unwrap();
        assert!(
            next.is_none(),
            "subscription should close after a continuity failure invalidates the session"
        );
    }

    #[tokio::test]
    async fn hot_only_periodic_audit_immediate_next_canonical_does_not_overlap_close() {
        let (processor, mut fast_receiver) =
            hot_only_test_processor_with_audit_config(2, Duration::from_secs(5));
        let fixture = seed_hot_only_periodic_audit_fixture(&processor, &mut fast_receiver).await;

        let generation = processor
            .hot_periodic_audit
            .lock()
            .expect("hot periodic audit mutex poisoned")
            .current_generation;
        let _hold_tx = install_hot_only_open_periodic_audit_worker(
            &processor,
            generation,
            1,
            Instant::now(),
            Duration::from_secs(5),
        );
        processor
            .hot_periodic_audit
            .lock()
            .expect("hot periodic audit mutex poisoned")
            .last_started_audit_canonical_block = 2;

        processor.maybe_trigger_hot_only_periodic_audit(&test_hot_only_canonical_block(3)).await;

        assert!(matches!(fast_receiver.try_recv(), Err(broadcast::error::TryRecvError::Empty)));
        assert!(
            processor
                .hot_snapshot_ring
                .lock()
                .expect("hot snapshot ring mutex poisoned")
                .get(fixture.latest_snapshot_id)
                .is_some()
        );
        let hot_engine = processor.hot_engine().lock().await;
        assert!(hot_engine.window.execution.is_some());
        assert!(!hot_engine.window.blocks.is_empty());
    }

    #[tokio::test]
    async fn hot_only_periodic_audit_overlap_invalidates_session() {
        let (processor, mut fast_receiver) =
            hot_only_test_processor_with_audit_config(2, Duration::from_secs(5));
        let fixture = seed_hot_only_periodic_audit_fixture(&processor, &mut fast_receiver).await;

        let generation = processor
            .hot_periodic_audit
            .lock()
            .expect("hot periodic audit mutex poisoned")
            .current_generation;
        let hold_tx = install_hot_only_open_periodic_audit_worker(
            &processor,
            generation,
            1,
            Instant::now(),
            Duration::from_secs(5),
        );
        processor
            .hot_periodic_audit
            .lock()
            .expect("hot periodic audit mutex poisoned")
            .last_started_audit_canonical_block = 2;

        processor.maybe_trigger_hot_only_periodic_audit(&test_hot_only_canonical_block(3)).await;

        assert!(matches!(fast_receiver.try_recv(), Err(broadcast::error::TryRecvError::Empty)));

        processor.maybe_trigger_hot_only_periodic_audit(&test_hot_only_canonical_block(4)).await;

        let invalidation_event = recv_fast_flashblock_event(&mut fast_receiver).await;
        assert!(matches!(invalidation_event, FastFlashblockFeedEvent::InvalidateSession));
        assert!(
            processor
                .hot_snapshot_ring
                .lock()
                .expect("hot snapshot ring mutex poisoned")
                .get(fixture.latest_snapshot_id)
                .is_none()
        );
        let hot_engine = processor.hot_engine().lock().await;
        assert!(hot_engine.window.execution.is_none());
        assert!(hot_engine.window.blocks.is_empty());
        drop(hot_engine);

        hold_tx
            .send(stale_hot_only_periodic_audit_worker_result(generation, 1))
            .expect("overlap audit worker result should still send for cleanup");
        wait_for_hot_only_periodic_audit_workers_to_clear(&processor).await;
    }

    #[tokio::test]
    async fn hot_only_periodic_audit_success_waits_full_interval_before_retrigger() {
        let (processor, mut fast_receiver) =
            hot_only_test_processor_with_audit_config(2, Duration::from_secs(5));
        seed_hot_only_periodic_audit_fixture(&processor, &mut fast_receiver).await;

        let snapshot = capture_next_hot_only_periodic_audit_snapshot(&processor).await;
        assert_eq!(snapshot.anchor_block_number, 0);
        let (audit_result, rebuilt_engine) = HotEngine::rebuild_shadow_from_snapshot(
            processor.client.clone(),
            processor.max_depth,
            &snapshot,
        );
        install_hot_only_periodic_audit_result(
            &processor,
            snapshot,
            1,
            audit_result,
            rebuilt_engine,
            Instant::now(),
            Duration::from_secs(5),
        );
        yield_now().await;

        processor.maybe_trigger_hot_only_periodic_audit(&test_hot_only_canonical_block(2)).await;

        {
            let state =
                processor.hot_periodic_audit.lock().expect("hot periodic audit mutex poisoned");
            assert_eq!(state.current_window_id, 1);
            assert_eq!(state.last_successful_audit_canonical_block, 1);
            assert!(state.worker.is_none());
        }

        let next_snapshot = capture_next_hot_only_periodic_audit_snapshot(&processor).await;
        assert_eq!(next_snapshot.window_id, 2);
        assert_eq!(next_snapshot.anchor_block_number, 0);

        processor.maybe_trigger_hot_only_periodic_audit(&test_hot_only_canonical_block(3)).await;

        {
            let state =
                processor.hot_periodic_audit.lock().expect("hot periodic audit mutex poisoned");
            assert_eq!(state.current_window_id, 2);
            assert_eq!(state.last_started_audit_canonical_block, 3);
            assert!(state.worker.is_some());
        }

        wait_for_hot_only_periodic_audit_workers_to_clear(&processor).await;
        assert!(matches!(fast_receiver.try_recv(), Err(broadcast::error::TryRecvError::Empty)));
    }

    #[tokio::test]
    async fn hot_only_periodic_audit_generation_advances_do_not_defer_trigger() {
        let (processor, mut fast_receiver) =
            hot_only_test_processor_with_audit_config(2, Duration::from_secs(5));
        seed_hot_only_periodic_audit_fixture(&processor, &mut fast_receiver).await;

        processor.update_hot_periodic_audit_latest_seen_canonical(1);
        processor.advance_hot_periodic_audit_generation();
        processor.update_hot_periodic_audit_latest_seen_canonical(2);
        processor.advance_hot_periodic_audit_generation();

        {
            let state =
                processor.hot_periodic_audit.lock().expect("hot periodic audit mutex poisoned");
            assert_eq!(state.last_successful_audit_canonical_block, 0);
            assert_eq!(state.current_window_id, 0);
            assert!(state.worker.is_none());
        }

        processor.maybe_trigger_hot_only_periodic_audit(&test_hot_only_canonical_block(2)).await;

        {
            let state =
                processor.hot_periodic_audit.lock().expect("hot periodic audit mutex poisoned");
            assert_eq!(state.last_successful_audit_canonical_block, 0);
            assert_eq!(state.current_window_id, 1);
            assert_eq!(state.last_started_audit_canonical_block, 2);
            assert!(state.worker.is_some());
        }

        wait_for_hot_only_periodic_audit_workers_to_clear(&processor).await;

        let state = processor.hot_periodic_audit.lock().expect("hot periodic audit mutex poisoned");
        assert_eq!(state.last_successful_audit_canonical_block, 2);
        drop(state);

        assert!(matches!(fast_receiver.try_recv(), Err(broadcast::error::TryRecvError::Empty)));
    }

    #[tokio::test]
    async fn hot_only_periodic_audit_cleanup_worker_overlap_invalidates_session() {
        let (processor, mut fast_receiver) =
            hot_only_test_processor_with_audit_config(2, Duration::from_secs(5));
        let fixture = seed_hot_only_periodic_audit_fixture(&processor, &mut fast_receiver).await;

        let generation = processor
            .hot_periodic_audit
            .lock()
            .expect("hot periodic audit mutex poisoned")
            .current_generation;
        let hold_tx = install_hot_only_open_periodic_audit_worker(
            &processor,
            generation,
            1,
            Instant::now(),
            Duration::from_secs(5),
        );
        {
            let mut state =
                processor.hot_periodic_audit.lock().expect("hot periodic audit mutex poisoned");
            state.last_started_audit_canonical_block = 2;
            super::StateProcessor::cancel_hot_periodic_audit_worker_locked(&mut state);
            assert!(state.worker.is_none());
            assert!(state.cleanup_worker.is_some());
        }

        processor.maybe_trigger_hot_only_periodic_audit(&test_hot_only_canonical_block(4)).await;

        let invalidation_event = recv_fast_flashblock_event(&mut fast_receiver).await;
        assert!(matches!(invalidation_event, FastFlashblockFeedEvent::InvalidateSession));
        assert!(
            processor
                .hot_snapshot_ring
                .lock()
                .expect("hot snapshot ring mutex poisoned")
                .get(fixture.latest_snapshot_id)
                .is_none()
        );

        hold_tx
            .send(stale_hot_only_periodic_audit_worker_result(generation, 1))
            .expect("cleanup audit worker result should still send for cleanup");
        wait_for_hot_only_periodic_audit_workers_to_clear(&processor).await;

        let hot_engine = processor.hot_engine().lock().await;
        assert!(hot_engine.window.execution.is_none());
        assert!(hot_engine.window.blocks.is_empty());
    }

    #[tokio::test]
    async fn hot_only_stale_audit_result_is_ignored_after_new_generation() {
        let (processor, mut fast_receiver) =
            hot_only_test_processor_with_audit_config(120, Duration::from_secs(5));
        let fixture = seed_hot_only_periodic_audit_fixture(&processor, &mut fast_receiver).await;

        let snapshot = capture_next_hot_only_periodic_audit_snapshot(&processor).await;
        let (audit_result, rebuilt_engine) = HotEngine::rebuild_shadow_from_snapshot(
            processor.client.clone(),
            processor.max_depth,
            &snapshot,
        );
        install_hot_only_periodic_audit_result(
            &processor,
            snapshot,
            1,
            audit_result,
            rebuilt_engine,
            Instant::now(),
            Duration::from_secs(5),
        );

        let live_block_count_before = processor.hot_engine().lock().await.window.blocks.len();
        processor.advance_hot_periodic_audit_generation();
        wait_for_hot_only_periodic_audit_workers_to_clear(&processor).await;

        assert!(matches!(fast_receiver.try_recv(), Err(broadcast::error::TryRecvError::Empty)));
        assert!(
            processor
                .hot_snapshot_ring
                .lock()
                .expect("hot snapshot ring mutex poisoned")
                .get(fixture.latest_snapshot_id)
                .is_some()
        );
        assert_eq!(
            processor.hot_engine().lock().await.window.blocks.len(),
            live_block_count_before,
            "stale result must not mutate the live hot window"
        );
    }

    #[tokio::test]
    async fn hot_only_prune_uses_last_reconciled_canonical_prefix() {
        let (processor, mut fast_receiver) =
            hot_only_test_processor_with_audit_config(120, Duration::from_secs(5));
        seed_hot_only_periodic_audit_fixture(&processor, &mut fast_receiver).await;

        let reconciled_hash = processor
            .client
            .header_by_number(1)
            .expect("canonical header lookup should succeed")
            .expect("canonical header for reconciled block should exist")
            .hash_slow();

        {
            let mut state =
                processor.hot_periodic_audit.lock().expect("hot periodic audit mutex poisoned");
            state.last_successful_audit_cursor_block = Some(2);
            state.latest_seen_canonical_block = 2;
            state.last_reconciled_canonical =
                Some(super::HotPeriodicAuditCanonicalBlock::new(1, reconciled_hash));
        }

        processor.prune_hot_only_checked_canonical_prefix(None, None).await;

        let hot_engine = processor.hot_engine().lock().await;
        assert_eq!(hot_engine.window.anchor.block_number(), 1);
        assert_eq!(hot_engine.window.anchor.hash(), reconciled_hash);
        assert_eq!(hot_engine.window.blocks.len(), 1);
        assert_eq!(hot_engine.window.blocks.front().map(|block| block.block_number), Some(2));
        drop(hot_engine);

        let state = processor.hot_periodic_audit.lock().expect("hot periodic audit mutex poisoned");
        assert_eq!(state.last_successful_audit_cursor_block, Some(2));
    }

    #[tokio::test]
    async fn hot_only_periodic_audit_worker_drop_cancels_task() {
        let (worker, cancellation, drop_rx) =
            pending_hot_only_periodic_audit_worker(0, 0, Duration::from_secs(5));

        drop(worker);

        assert!(cancellation.is_cancelled());
        let _ =
            timeout(RECV_TIMEOUT, drop_rx).await.expect("dropped audit worker task should abort");
    }

    #[tokio::test]
    async fn hot_only_start_shutdown_cancels_in_flight_audit_worker() {
        let (tx, rx) = mpsc::unbounded_channel();
        let (fast_sender, _) = broadcast::channel(1);
        let (sender, _) = broadcast::channel(1);

        let processor = StateProcessor::new_with_mode(
            test_client(),
            Arc::new(ArcSwapOption::new(None)),
            5,
            FlashblocksMode::HotOnly,
            test_state_processor_handles(rx, fast_sender, sender, 1),
        );

        let (worker, cancellation, drop_rx) =
            pending_hot_only_periodic_audit_worker(0, 1, Duration::from_secs(5));
        {
            let mut state =
                processor.hot_periodic_audit.lock().expect("hot periodic audit mutex poisoned");
            state.current_window_id = 1;
            state.worker = Some(worker);
        }

        drop(tx);
        processor.start().await;

        assert!(cancellation.is_cancelled());
        let _ = timeout(RECV_TIMEOUT, drop_rx)
            .await
            .expect("shutdown should cancel the in-flight audit worker");

        let state = processor.hot_periodic_audit.lock().expect("hot periodic audit mutex poisoned");
        assert!(state.worker.is_none());
        assert!(state.cleanup_worker.is_none());
    }

    #[tokio::test]
    async fn hot_only_replays_cached_next_block_suffixes_after_block_acceptance() {
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
            test_state_processor_handles(rx, fast_sender, sender, 8),
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

        let wire_parent_hash = {
            let hot_engine = processor.hot_engine().lock().await;
            let pending_block = hot_engine
                .window
                .active_block()
                .expect("active pending block should exist after first flashblock");
            assert!(pending_block.local_header_parts.is_none());
            pending_block.latest_wire_header_hash
        };

        let cached_suffix =
            test_hot_flashblock(1, 2, PayloadId::new([0x82; 8]), wire_parent_hash, false, vec![]);
        assert!(processor.cache.lock().await.insert(cached_suffix));

        processor
            .apply_hot_only_flashblock(test_hot_flashblock(
                0,
                2,
                PayloadId::new([0x82; 8]),
                wire_parent_hash,
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
            test_state_processor_handles(rx, fast_sender, sender, 8),
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
                parent_hash,
                false,
                vec![],
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
            test_state_processor_handles(rx, fast_sender, sender, 8),
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
            test_hot_flashblock(0, 2, PayloadId::new([0x94; 8]), parent_hash, false, vec![]),
            test_hot_flashblock(1, 2, PayloadId::new([0x94; 8]), parent_hash, false, vec![]),
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
    async fn hot_only_canonical_conflict_without_retained_proof_resets_to_resync() {
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
            test_state_processor_handles(rx, fast_sender, sender, 8),
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

        let wire_parent_hash = {
            let hot_engine = processor.hot_engine().lock().await;
            let pending_block = hot_engine
                .window
                .active_block()
                .expect("active pending block should exist after first flashblock");
            assert!(pending_block.local_header_parts.is_none());
            pending_block.latest_wire_header_hash
        };

        processor
            .apply_hot_only_flashblock(test_hot_flashblock(
                0,
                2,
                PayloadId::new([0xa2; 8]),
                wire_parent_hash,
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
        assert!(matches!(invalidation_event, FastFlashblockFeedEvent::Resync));
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
