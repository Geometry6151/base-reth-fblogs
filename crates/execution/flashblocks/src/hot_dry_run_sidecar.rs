//! Manager-owned state for the hot dry-run sidecar control plane.

use std::{
    collections::BTreeMap,
    fmt,
    sync::{
        Arc, Mutex as StdMutex,
        atomic::{AtomicBool, Ordering},
    },
    time::Instant,
};

use alloy_consensus::Header;
use alloy_primitives::B256;
use arc_swap::ArcSwapOption;
use base_common_chains::Upgrades;
use base_common_flashblocks::Flashblock;
use reth_chainspec::{ChainSpecProvider, EthChainSpec};
use reth_provider::{BlockReaderIdExt, StateProviderFactory};
use reth_revm::State;
use revm::Database;
use revm_database::{
    bal::BalState,
    states::{BundleState, CacheState, TransitionState},
};
use tokio::sync::{Mutex, mpsc};

use crate::{
    AuditWindowSnapshot, FlashblockSnapshotId, HotApplyOutcome, HotEngine, HotExecutionDb,
    HotExecutionState, HotSnapshot, HotSnapshotRing, Metrics, PeriodicAuditResult,
};

/// Low-cardinality availability and observability state for the hot dry-run sidecar.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum HotDryRunSidecarStatus {
    /// The sidecar currently holds a warm state.
    Warm,
    /// The sidecar is not currently warm.
    Unavailable,
    /// The bounded ingress queue overflowed.
    Overflow,
    /// The sidecar was explicitly reset.
    Reset,
    /// The sidecar replay diverged from the main hot snapshot.
    Mismatch,
    /// An authoritative rebuild failed.
    RebuildFailure,
    /// A sequential apply failed.
    ApplyFailure,
}

/// Sequential warm-sidecar apply input for one newly emitted hot snapshot.
#[derive(Clone, Debug)]
pub struct HotDryRunApplyInput {
    /// Generation observed when this work item was queued.
    pub generation: u64,
    /// Exact hot snapshot id the sidecar must reach after replay.
    pub snapshot_id: FlashblockSnapshotId,
    /// Raw flashblock required for internal replay.
    pub flashblock: Flashblock,
    /// Canonical base parent hash beneath the authoritative hot snapshot.
    pub canonical_base_parent_hash: B256,
    /// Expected authoritative latest header hash after replay completes.
    pub expected_latest_header_hash: B256,
}

/// Authoritative rebuild input for recovering a warm sidecar state.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HotDryRunRebuildInput {
    /// Generation observed when this work item was queued.
    pub generation: u64,
    /// Exact hot snapshot id the rebuilt sidecar state must represent.
    pub snapshot_id: FlashblockSnapshotId,
    /// Immutable retained replay snapshot from the main hot path.
    pub replay_snapshot: AuditWindowSnapshot,
    /// Expected authoritative latest header hash after rebuild completes.
    pub expected_latest_header_hash: B256,
}

/// Replay-capable sidecar work item queued off the hot path.
#[derive(Clone, Debug)]
pub enum HotDryRunSidecarInput {
    /// Apply one new flashblock while the sidecar remains warm and in sync.
    Apply(HotDryRunApplyInput),
    /// Rebuild a new warm state from an authoritative retained snapshot.
    RebuildFromSnapshot(HotDryRunRebuildInput),
}

impl HotDryRunSidecarInput {
    /// Returns the generation this work item was queued under.
    pub const fn generation(&self) -> u64 {
        match self {
            Self::Apply(input) => input.generation,
            Self::RebuildFromSnapshot(input) => input.generation,
        }
    }

    /// Returns the authoritative snapshot id this work item must produce.
    pub const fn snapshot_id(&self) -> FlashblockSnapshotId {
        match self {
            Self::Apply(input) => input.snapshot_id,
            Self::RebuildFromSnapshot(input) => input.snapshot_id,
        }
    }
}

/// Immutable request-safe state published by the hot dry-run sidecar.
#[derive(Clone, Debug)]
pub struct HotDryRunWarmState {
    /// Exact snapshot id represented by this warm state.
    pub snapshot_id: FlashblockSnapshotId,
    /// Request-safe immutable snapshot material for dry-run RPC.
    pub hot_snapshot: Arc<HotSnapshot>,
    execution_seed: HotDryRunExecutionSeed,
}

#[derive(Clone, Debug)]
struct HotDryRunExecutionSeed {
    cache: CacheState,
    transition_state: Option<TransitionState>,
    bundle_state: BundleState,
    use_preloaded_bundle: bool,
    block_hashes: BTreeMap<u64, B256>,
    bal_state: BalState,
}

impl Default for HotDryRunExecutionSeed {
    fn default() -> Self {
        let state = State::builder().build();

        Self {
            cache: state.cache,
            transition_state: state.transition_state,
            bundle_state: state.bundle_state,
            use_preloaded_bundle: state.use_preloaded_bundle,
            block_hashes: state.block_hashes,
            bal_state: state.bal_state,
        }
    }
}

impl HotDryRunExecutionSeed {
    fn from_execution_state(execution: &HotExecutionState<HotExecutionDb>) -> Self {
        let bundle_size = execution.db.bundle_state.state.len();
        let bundle_clone_start = Instant::now();
        let bundle_state = execution.db.bundle_state.clone();
        Metrics::bundle_state_clone_duration().record(bundle_clone_start.elapsed());
        Metrics::bundle_state_clone_size().record(bundle_size as f64);

        Self {
            cache: execution.db.cache.clone(),
            transition_state: execution.db.transition_state.clone(),
            bundle_state,
            use_preloaded_bundle: execution.db.use_preloaded_bundle,
            block_hashes: execution.db.block_hashes.clone(),
            bal_state: execution.db.bal_state.clone(),
        }
    }
}

impl HotDryRunWarmState {
    /// Creates a new published warm state from one immutable hot snapshot.
    pub fn new(hot_snapshot: Arc<HotSnapshot>) -> Self {
        Self {
            snapshot_id: hot_snapshot.snapshot_id,
            hot_snapshot,
            execution_seed: HotDryRunExecutionSeed::default(),
        }
    }

    /// Creates a new published warm state from one immutable hot snapshot plus sidecar execution.
    pub fn from_execution(
        hot_snapshot: Arc<HotSnapshot>,
        execution: &HotExecutionState<HotExecutionDb>,
    ) -> Self {
        Self {
            snapshot_id: hot_snapshot.snapshot_id,
            hot_snapshot,
            execution_seed: HotDryRunExecutionSeed::from_execution_state(execution),
        }
    }

    /// Builds a request-local execution state without mutating the published sidecar base.
    pub fn build_request_state<DB>(&self, database: DB) -> State<DB>
    where
        DB: Database,
    {
        State {
            cache: self.execution_seed.cache.clone(),
            database,
            transition_state: self.execution_seed.transition_state.clone(),
            bundle_state: self.execution_seed.bundle_state.clone(),
            use_preloaded_bundle: self.execution_seed.use_preloaded_bundle,
            block_hashes: self.execution_seed.block_hashes.clone(),
            bal_state: self.execution_seed.bal_state.clone(),
        }
    }
}

#[derive(Debug)]
struct HotDryRunSidecarWorker<Client> {
    client: Client,
    max_depth: u64,
    manager: Arc<HotDryRunSidecarManager>,
    hot_snapshot_ring: Arc<StdMutex<HotSnapshotRing>>,
    hot_engine: HotEngine<Client>,
}

impl<Client> HotDryRunSidecarWorker<Client>
where
    Client: StateProviderFactory
        + ChainSpecProvider<ChainSpec: EthChainSpec<Header = Header> + Upgrades>
        + BlockReaderIdExt<Header = Header>
        + Clone
        + Send
        + 'static,
{
    fn new(
        client: Client,
        max_depth: u64,
        manager: Arc<HotDryRunSidecarManager>,
        hot_snapshot_ring: Arc<StdMutex<HotSnapshotRing>>,
    ) -> Self {
        let hot_engine = HotEngine::new(client.clone(), max_depth);

        Self { client, max_depth, manager, hot_snapshot_ring, hot_engine }
    }

    fn process_input(&mut self, input: HotDryRunSidecarInput) {
        if !self.manager.generation_matches(input.generation()) {
            return;
        }

        match input {
            HotDryRunSidecarInput::Apply(input) => self.process_apply(input),
            HotDryRunSidecarInput::RebuildFromSnapshot(input) => self.process_rebuild(input),
        }
    }

    fn process_apply(&mut self, input: HotDryRunApplyInput) {
        let outcome = match self.hot_engine.apply_flashblock(&input.flashblock) {
            Ok(outcome) => outcome,
            Err(_error) => {
                self.manager
                    .try_mark_unavailable(input.generation, HotDryRunSidecarStatus::ApplyFailure);
                return;
            }
        };

        let HotApplyOutcome::Delta { snapshot: Some(snapshot), .. } = outcome else {
            self.manager
                .try_mark_unavailable(input.generation, HotDryRunSidecarStatus::ApplyFailure);
            return;
        };

        let sidecar_snapshot = Arc::new(*snapshot);
        let Some(authoritative_snapshot) = self.authoritative_snapshot(input.snapshot_id) else {
            self.manager.try_mark_unavailable(input.generation, HotDryRunSidecarStatus::Mismatch);
            return;
        };

        if !Self::authoritative_snapshot_matches_apply_input(
            &input,
            authoritative_snapshot.as_ref(),
        ) || !Self::sidecar_snapshot_matches_authoritative(
            sidecar_snapshot.as_ref(),
            authoritative_snapshot.as_ref(),
        ) {
            self.manager.try_mark_unavailable(input.generation, HotDryRunSidecarStatus::Mismatch);
            return;
        }

        let Some(execution) = self.hot_engine.window.execution.as_ref() else {
            self.manager
                .try_mark_unavailable(input.generation, HotDryRunSidecarStatus::ApplyFailure);
            return;
        };

        let warm_state =
            Arc::new(HotDryRunWarmState::from_execution(authoritative_snapshot, execution));
        let _ = self.manager.try_publish_warm_state(input.generation, warm_state);
    }

    fn process_rebuild(&mut self, input: HotDryRunRebuildInput) {
        let (audit_result, rebuilt_engine) = HotEngine::rebuild_shadow_from_snapshot(
            self.client.clone(),
            self.max_depth,
            &input.replay_snapshot,
        );

        if audit_result != PeriodicAuditResult::EquivalentPrefix {
            self.manager
                .try_mark_unavailable(input.generation, HotDryRunSidecarStatus::RebuildFailure);
            return;
        }

        let Some(rebuilt_engine) = rebuilt_engine else {
            self.manager
                .try_mark_unavailable(input.generation, HotDryRunSidecarStatus::RebuildFailure);
            return;
        };

        let Some(authoritative_snapshot) = self.authoritative_snapshot(input.snapshot_id) else {
            self.manager.try_mark_unavailable(input.generation, HotDryRunSidecarStatus::Mismatch);
            return;
        };

        if !Self::authoritative_snapshot_matches_rebuild_input(
            &input,
            authoritative_snapshot.as_ref(),
        ) || !Self::rebuilt_engine_matches_authoritative(
            &rebuilt_engine,
            authoritative_snapshot.as_ref(),
        ) {
            self.manager.try_mark_unavailable(input.generation, HotDryRunSidecarStatus::Mismatch);
            return;
        }

        let Some(execution) = rebuilt_engine.window.execution.as_ref() else {
            self.manager
                .try_mark_unavailable(input.generation, HotDryRunSidecarStatus::RebuildFailure);
            return;
        };

        let warm_state = Arc::new(HotDryRunWarmState::from_execution(
            Arc::clone(&authoritative_snapshot),
            execution,
        ));

        if self.manager.try_publish_warm_state(input.generation, warm_state) {
            self.hot_engine = rebuilt_engine;
        }
    }

    fn authoritative_snapshot(
        &self,
        snapshot_id: FlashblockSnapshotId,
    ) -> Option<Arc<HotSnapshot>> {
        self.hot_snapshot_ring.lock().expect("hot snapshot ring mutex poisoned").get(snapshot_id)
    }

    fn authoritative_snapshot_matches_apply_input(
        input: &HotDryRunApplyInput,
        authoritative_snapshot: &HotSnapshot,
    ) -> bool {
        authoritative_snapshot.snapshot_id == input.snapshot_id
            && authoritative_snapshot.canonical_base_parent_hash == input.canonical_base_parent_hash
            && authoritative_snapshot.latest_header.hash() == input.expected_latest_header_hash
    }

    fn authoritative_snapshot_matches_rebuild_input(
        input: &HotDryRunRebuildInput,
        authoritative_snapshot: &HotSnapshot,
    ) -> bool {
        authoritative_snapshot.snapshot_id == input.snapshot_id
            && authoritative_snapshot.latest_header.hash() == input.expected_latest_header_hash
    }

    fn sidecar_snapshot_matches_authoritative(
        sidecar_snapshot: &HotSnapshot,
        authoritative_snapshot: &HotSnapshot,
    ) -> bool {
        sidecar_snapshot.snapshot_id == authoritative_snapshot.snapshot_id
            && sidecar_snapshot.canonical_base_parent_hash
                == authoritative_snapshot.canonical_base_parent_hash
            && sidecar_snapshot.latest_header.hash() == authoritative_snapshot.latest_header.hash()
    }

    fn rebuilt_engine_matches_authoritative(
        rebuilt_engine: &HotEngine<Client>,
        authoritative_snapshot: &HotSnapshot,
    ) -> bool {
        let Some(sidecar_snapshot_id) = Self::latest_snapshot_id(rebuilt_engine) else {
            return false;
        };
        let Some(active_block) = rebuilt_engine.window.active_block() else {
            return false;
        };

        sidecar_snapshot_id == authoritative_snapshot.snapshot_id
            && rebuilt_engine.window.anchor.hash()
                == authoritative_snapshot.canonical_base_parent_hash
            && active_block.latest_header.hash() == authoritative_snapshot.latest_header.hash()
    }

    fn latest_snapshot_id(hot_engine: &HotEngine<Client>) -> Option<FlashblockSnapshotId> {
        let active_block = hot_engine.window.active_block()?;

        Some(FlashblockSnapshotId::new(
            hot_engine.next_snapshot_nonce,
            active_block.block_number,
            active_block.latest_flashblock_index,
            active_block.payload_id,
            active_block.parent_hash,
        ))
    }
}

/// Shared manager for bounded sidecar ingress and latest published warm state.
pub struct HotDryRunSidecarManager {
    sender: mpsc::Sender<HotDryRunSidecarInput>,
    receiver: Arc<Mutex<mpsc::Receiver<HotDryRunSidecarInput>>>,
    latest_warm_state: Arc<ArcSwapOption<HotDryRunWarmState>>,
    publication: StdMutex<(u64, HotDryRunSidecarStatus)>,
    force_next_after_send_failure_for_testing: AtomicBool,
}

impl HotDryRunSidecarManager {
    /// Creates a new bounded sidecar manager.
    pub fn new(capacity: usize) -> Self {
        let (sender, receiver) = mpsc::channel(capacity);

        Self {
            sender,
            receiver: Arc::new(Mutex::new(receiver)),
            latest_warm_state: Arc::new(ArcSwapOption::new(None)),
            publication: StdMutex::new((0, HotDryRunSidecarStatus::Unavailable)),
            force_next_after_send_failure_for_testing: AtomicBool::new(false),
        }
    }

    /// Returns the bounded work receiver for the sidecar worker.
    pub fn receiver(&self) -> Arc<Mutex<mpsc::Receiver<HotDryRunSidecarInput>>> {
        Arc::clone(&self.receiver)
    }

    /// Starts one independent sidecar worker task.
    pub fn spawn_worker<Client>(
        self: &Arc<Self>,
        client: Client,
        max_depth: u64,
        hot_snapshot_ring: Arc<StdMutex<HotSnapshotRing>>,
    ) where
        Client: StateProviderFactory
            + ChainSpecProvider<ChainSpec: EthChainSpec<Header = Header> + Upgrades>
            + BlockReaderIdExt<Header = Header>
            + Clone
            + Send
            + 'static,
    {
        let receiver = self.receiver();
        let manager = Arc::clone(self);

        tokio::spawn(async move {
            let mut worker =
                HotDryRunSidecarWorker::new(client, max_depth, manager, hot_snapshot_ring);

            loop {
                let input = {
                    let mut receiver = receiver.lock().await;
                    receiver.recv().await
                };
                let Some(input) = input else {
                    break;
                };

                worker.process_input(input);
            }
        });
    }

    /// Returns the currently active sidecar generation.
    pub fn current_generation(&self) -> u64 {
        self.publication.lock().expect("hot dry-run sidecar publication mutex poisoned").0
    }

    /// Returns the current low-cardinality sidecar status.
    pub fn status(&self) -> HotDryRunSidecarStatus {
        self.publication.lock().expect("hot dry-run sidecar publication mutex poisoned").1
    }

    /// Returns the current sidecar publication generation and status from one lock acquisition.
    pub fn publication_state(&self) -> (u64, HotDryRunSidecarStatus) {
        *self.publication.lock().expect("hot dry-run sidecar publication mutex poisoned")
    }

    /// Returns the most recently published warm state, if any.
    pub fn latest_warm_state(&self) -> Option<Arc<HotDryRunWarmState>> {
        self.latest_warm_state.load_full()
    }

    /// Attempts to enqueue one sidecar work item without blocking.
    pub fn try_publish_after_send(&self, input: HotDryRunSidecarInput) -> bool {
        if self.force_next_after_send_failure_for_testing.swap(false, Ordering::Relaxed) {
            return false;
        }

        self.sender.try_send(input).is_ok()
    }

    #[doc(hidden)]
    pub fn force_next_after_send_failure_for_testing(&self) {
        self.force_next_after_send_failure_for_testing.store(true, Ordering::Relaxed);
    }

    /// Publishes one newly warmed state if it still belongs to the active generation.
    pub fn try_publish_warm_state(
        &self,
        generation: u64,
        warm_state: Arc<HotDryRunWarmState>,
    ) -> bool {
        let mut publication =
            self.publication.lock().expect("hot dry-run sidecar publication mutex poisoned");

        if publication.0 != generation {
            return false;
        }

        self.latest_warm_state.store(Some(Arc::clone(&warm_state)));
        publication.1 = HotDryRunSidecarStatus::Warm;
        true
    }

    /// Returns whether the provided generation is still active.
    pub fn generation_matches(&self, generation: u64) -> bool {
        self.publication.lock().expect("hot dry-run sidecar publication mutex poisoned").0
            == generation
    }

    /// Clears the latest warm state only if the provided generation is still active.
    pub fn try_mark_unavailable(&self, generation: u64, status: HotDryRunSidecarStatus) -> bool {
        let mut publication =
            self.publication.lock().expect("hot dry-run sidecar publication mutex poisoned");

        if publication.0 != generation {
            return false;
        }

        self.mark_unavailable_locked(&mut publication, status);
        true
    }

    /// Clears the latest warm state and advances the generation for stale-work invalidation.
    pub fn mark_unavailable(&self, status: HotDryRunSidecarStatus) {
        let mut publication =
            self.publication.lock().expect("hot dry-run sidecar publication mutex poisoned");

        self.mark_unavailable_locked(&mut publication, status);
    }

    fn mark_unavailable_locked(
        &self,
        publication: &mut (u64, HotDryRunSidecarStatus),
        status: HotDryRunSidecarStatus,
    ) {
        self.latest_warm_state.store(None);
        publication.0 = publication.0.saturating_add(1);
        publication.1 = match status {
            HotDryRunSidecarStatus::Warm => HotDryRunSidecarStatus::Unavailable,
            status => status,
        };
    }
}

impl fmt::Debug for HotDryRunSidecarManager {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HotDryRunSidecarManager")
            .field("current_generation", &self.current_generation())
            .field("status", &self.status())
            .field("has_latest_warm_state", &self.latest_warm_state().is_some())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use std::{
        sync::{Arc, mpsc},
        thread,
    };

    use alloy_consensus::{Header, Sealed};
    use alloy_primitives::{Address, B256, Bloom, Bytes, U256};
    use alloy_rpc_types::state::StateOverride;
    use alloy_rpc_types_engine::PayloadId;
    use base_common_flashblocks::{
        ExecutionPayloadBaseV1, ExecutionPayloadFlashblockDeltaV1, Flashblock, Metadata,
    };

    use super::{
        HotDryRunApplyInput, HotDryRunSidecarInput, HotDryRunSidecarManager,
        HotDryRunSidecarStatus, HotDryRunWarmState,
    };
    use crate::{FlashblockSnapshotId, HotSnapshot};

    fn snapshot_id(nonce: u64) -> FlashblockSnapshotId {
        FlashblockSnapshotId::new(
            nonce,
            nonce,
            nonce,
            PayloadId::new([nonce as u8; 8]),
            B256::with_last_byte(nonce as u8),
        )
    }

    fn header(nonce: u64) -> Sealed<Header> {
        Sealed::new_unchecked(
            Header {
                number: nonce,
                parent_hash: B256::with_last_byte(nonce as u8),
                ..Default::default()
            },
            B256::with_last_byte((nonce + 1) as u8),
        )
    }

    fn flashblock(snapshot_id: FlashblockSnapshotId) -> Flashblock {
        Flashblock {
            payload_id: snapshot_id.payload_id(),
            index: snapshot_id.flashblock_index(),
            base: Some(ExecutionPayloadBaseV1 {
                parent_beacon_block_root: B256::ZERO,
                parent_hash: snapshot_id.parent_hash(),
                fee_recipient: Address::ZERO,
                prev_randao: B256::ZERO,
                block_number: snapshot_id.block_number(),
                gas_limit: 30_000_000,
                timestamp: 1_700_000_000 + snapshot_id.block_number(),
                extra_data: Bytes::default(),
                base_fee_per_gas: U256::from(1_000_000_000u64),
            }),
            diff: ExecutionPayloadFlashblockDeltaV1 {
                state_root: B256::ZERO,
                receipts_root: B256::ZERO,
                logs_bloom: Bloom::default(),
                gas_used: 21_000,
                block_hash: B256::with_last_byte((snapshot_id.flashblock_index() as u8) + 1),
                transactions: vec![],
                withdrawals: vec![],
                withdrawals_root: B256::ZERO,
                blob_gas_used: None,
            },
            metadata: Metadata { block_number: snapshot_id.block_number() },
        }
    }

    fn apply_input(generation: u64, snapshot_id: FlashblockSnapshotId) -> HotDryRunSidecarInput {
        HotDryRunSidecarInput::Apply(HotDryRunApplyInput {
            generation,
            snapshot_id,
            flashblock: flashblock(snapshot_id),
            canonical_base_parent_hash: B256::with_last_byte((snapshot_id.nonce() as u8) + 3),
            expected_latest_header_hash: B256::with_last_byte((snapshot_id.nonce() as u8) + 5),
        })
    }

    fn warm_state(snapshot_id: FlashblockSnapshotId) -> Arc<HotDryRunWarmState> {
        Arc::new(HotDryRunWarmState::new(Arc::new(HotSnapshot::new(
            snapshot_id,
            snapshot_id.parent_hash(),
            header(snapshot_id.nonce()),
            StateOverride::default(),
        ))))
    }

    #[test]
    fn bounded_ingress_returns_false_when_full() {
        let manager = HotDryRunSidecarManager::new(1);

        assert!(manager.try_publish_after_send(apply_input(0, snapshot_id(1))));
        assert!(!manager.try_publish_after_send(apply_input(0, snapshot_id(2))));
    }

    #[test]
    fn mark_unavailable_clears_latest_warm_state_handle() {
        let manager = HotDryRunSidecarManager::new(1);
        let generation = manager.current_generation();
        let warm_state = warm_state(snapshot_id(7));

        assert!(manager.try_publish_warm_state(generation, Arc::clone(&warm_state)));
        assert!(manager.latest_warm_state().is_some());

        manager.mark_unavailable(HotDryRunSidecarStatus::Mismatch);

        assert!(manager.latest_warm_state().is_none());
    }

    #[test]
    fn mark_unavailable_advances_generation_so_stale_work_cannot_publish() {
        let manager = HotDryRunSidecarManager::new(1);
        let generation = manager.current_generation();

        manager.mark_unavailable(HotDryRunSidecarStatus::Reset);

        assert!(manager.current_generation() > generation);
        assert!(!manager.try_publish_warm_state(generation, warm_state(snapshot_id(9))));
        assert!(manager.latest_warm_state().is_none());
    }

    #[test]
    fn publishing_warm_state_stores_it_under_its_exact_snapshot_id() {
        let manager = HotDryRunSidecarManager::new(1);
        let generation = manager.current_generation();
        let warm_state = warm_state(snapshot_id(11));

        assert!(manager.try_publish_warm_state(generation, Arc::clone(&warm_state)));

        let latest = manager.latest_warm_state().expect("warm state should publish");
        assert_eq!(latest.snapshot_id, warm_state.snapshot_id);
    }

    #[test]
    fn concurrent_invalidation_prevents_stale_publish_from_republishing_warm_state() {
        let manager = Arc::new(HotDryRunSidecarManager::new(1));
        let generation = manager.current_generation();

        assert!(manager.try_publish_warm_state(generation, warm_state(snapshot_id(13))));

        let mut publication =
            manager.publication.lock().expect("hot dry-run sidecar publication mutex poisoned");
        let (attempt_tx, attempt_rx) = mpsc::channel();
        let manager_for_thread = Arc::clone(&manager);
        let publish_thread = thread::spawn(move || {
            attempt_tx.send(()).expect("publisher should signal attempt");
            manager_for_thread.try_publish_warm_state(generation, warm_state(snapshot_id(15)))
        });

        attempt_rx.recv().expect("publisher should attempt publish");
        manager.mark_unavailable_locked(&mut publication, HotDryRunSidecarStatus::Reset);
        drop(publication);

        assert!(!publish_thread.join().expect("publisher thread should finish"));
        assert!(manager.current_generation() > generation);
        assert_eq!(manager.status(), HotDryRunSidecarStatus::Reset);
        assert!(manager.latest_warm_state().is_none());
    }

    #[test]
    fn forced_after_send_failure_only_blocks_one_enqueue_attempt() {
        let manager = HotDryRunSidecarManager::new(2);

        manager.force_next_after_send_failure_for_testing();

        assert!(!manager.try_publish_after_send(apply_input(0, snapshot_id(21))));
        assert!(manager.try_publish_after_send(apply_input(0, snapshot_id(22))));
    }
}
