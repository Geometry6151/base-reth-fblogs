//! Manager-owned state for the hot dry-run sidecar control plane.

use std::{
    fmt,
    sync::{
        Arc, Mutex as StdMutex,
        atomic::{AtomicU64, Ordering},
    },
};

use alloy_primitives::B256;
use arc_swap::ArcSwapOption;
use base_common_flashblocks::Flashblock;
use tokio::sync::{Mutex, mpsc};

use crate::{AuditWindowSnapshot, FlashblockSnapshotId, HotSnapshot};

/// Internal availability and observability state for the hot dry-run sidecar.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum HotDryRunSidecarStatus {
    /// The sidecar holds a warm state for the provided snapshot id.
    Warm {
        /// Snapshot id represented by the latest warm state.
        snapshot_id: FlashblockSnapshotId,
    },
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
}

impl HotDryRunWarmState {
    /// Creates a new published warm state from one immutable hot snapshot.
    pub fn new(hot_snapshot: Arc<HotSnapshot>) -> Self {
        Self { snapshot_id: hot_snapshot.snapshot_id, hot_snapshot }
    }
}

/// Shared manager for bounded sidecar ingress and latest published warm state.
pub struct HotDryRunSidecarManager {
    sender: mpsc::Sender<HotDryRunSidecarInput>,
    receiver: Arc<Mutex<mpsc::Receiver<HotDryRunSidecarInput>>>,
    latest_warm_state: Arc<ArcSwapOption<HotDryRunWarmState>>,
    current_generation: AtomicU64,
    status: StdMutex<HotDryRunSidecarStatus>,
}

impl HotDryRunSidecarManager {
    /// Creates a new bounded sidecar manager.
    pub fn new(capacity: usize) -> Self {
        let (sender, receiver) = mpsc::channel(capacity);

        Self {
            sender,
            receiver: Arc::new(Mutex::new(receiver)),
            latest_warm_state: Arc::new(ArcSwapOption::new(None)),
            current_generation: AtomicU64::new(0),
            status: StdMutex::new(HotDryRunSidecarStatus::Unavailable),
        }
    }

    /// Returns the bounded work receiver for the sidecar worker.
    pub fn receiver(&self) -> Arc<Mutex<mpsc::Receiver<HotDryRunSidecarInput>>> {
        Arc::clone(&self.receiver)
    }

    /// Returns the currently active sidecar generation.
    pub fn current_generation(&self) -> u64 {
        self.current_generation.load(Ordering::Acquire)
    }

    /// Returns the current low-cardinality sidecar status.
    pub fn status(&self) -> HotDryRunSidecarStatus {
        *self.status.lock().expect("hot dry-run sidecar status mutex poisoned")
    }

    /// Returns the most recently published warm state, if any.
    pub fn latest_warm_state(&self) -> Option<Arc<HotDryRunWarmState>> {
        self.latest_warm_state.load_full()
    }

    /// Attempts to enqueue one sidecar work item without blocking.
    pub fn try_publish_after_send(&self, input: HotDryRunSidecarInput) -> bool {
        self.sender.try_send(input).is_ok()
    }

    /// Publishes one newly warmed state if it still belongs to the active generation.
    pub fn try_publish_warm_state(
        &self,
        generation: u64,
        warm_state: Arc<HotDryRunWarmState>,
    ) -> bool {
        if self.current_generation() != generation {
            return false;
        }

        self.latest_warm_state.store(Some(Arc::clone(&warm_state)));
        *self.status.lock().expect("hot dry-run sidecar status mutex poisoned") =
            HotDryRunSidecarStatus::Warm { snapshot_id: warm_state.snapshot_id };
        true
    }

    /// Clears the latest warm state and advances the generation for stale-work invalidation.
    pub fn mark_unavailable(&self, status: HotDryRunSidecarStatus) {
        self.latest_warm_state.store(None);
        _ = self.current_generation.fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
            Some(current.saturating_add(1))
        });
        *self.status.lock().expect("hot dry-run sidecar status mutex poisoned") = match status {
            HotDryRunSidecarStatus::Warm { .. } => HotDryRunSidecarStatus::Unavailable,
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
    use std::sync::Arc;

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
}
