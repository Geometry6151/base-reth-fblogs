//! Request-local hot dry-run seeds and latest-wins cache.

use std::{sync::Arc, time::Instant};

use arc_swap::ArcSwapOption;
use reth_revm::State;
use revm_database::{
    bal::BalState,
    states::{BundleState, CacheState, TransitionState, block_hash_cache::BlockHashCache},
};

use crate::{FlashblockSnapshotId, HotExecutionDb, HotExecutionState, HotSnapshot, Metrics};

/// Request-local dry-run seed for a hot snapshot.
#[derive(Debug)]
pub struct HotDryRunSeed {
    /// Snapshot identity carried with this seed.
    pub snapshot_id: FlashblockSnapshotId,
    /// Snapshot metadata used by the dry-run request.
    pub hot_snapshot: Arc<HotSnapshot>,
    execution_seed: HotDryRunExecutionSeed,
}

impl HotDryRunSeed {
    /// Builds a request-safe dry-run seed from the live hot execution state.
    pub fn from_execution(
        hot_snapshot: Arc<HotSnapshot>,
        execution: &HotExecutionState<HotExecutionDb>,
    ) -> Self {
        let bundle_state_size = execution.db.bundle_state.state.len();
        let start = Instant::now();
        let seed = Self {
            snapshot_id: hot_snapshot.snapshot_id,
            hot_snapshot,
            execution_seed: HotDryRunExecutionSeed {
                cache: execution.db.cache.clone(),
                transition_state: execution.db.transition_state.clone(),
                bundle_state: execution.db.bundle_state.clone(),
                use_preloaded_bundle: execution.db.use_preloaded_bundle,
                block_hashes: execution.db.block_hashes.clone(),
                bal_state: execution.db.bal_state.clone(),
            },
        };

        Metrics::hot_dry_run_seed_fork_duration().record(start.elapsed());
        Metrics::hot_dry_run_seed_bundle_state_size().record(bundle_state_size as f64);

        seed
    }

    /// Builds a fresh request-local state using the supplied request database.
    pub fn build_request_state<DB>(&self, database: DB) -> State<DB> {
        State {
            cache: self.execution_seed.cache.clone(),
            database,
            transition_state: self.execution_seed.transition_state.clone(),
            bundle_state: self.execution_seed.bundle_state.clone(),
            use_preloaded_bundle: self.execution_seed.use_preloaded_bundle,
            block_hashes: self.execution_seed.block_hashes.clone(),
            bal_state: self.execution_seed.bal_state.clone(),
            state_hook: None,
        }
    }
}

/// Cloned execution data used to build per-request dry-run state.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct HotDryRunExecutionSeed {
    /// Cached accounts and contracts carried from the hot execution state.
    pub cache: CacheState,
    /// Pending transition state carried from the hot execution state.
    pub transition_state: Option<TransitionState>,
    /// Bundle state carried from the hot execution state.
    pub bundle_state: BundleState,
    /// Whether the request state should read from the preloaded bundle.
    pub use_preloaded_bundle: bool,
    /// Block hash overrides carried from the hot execution state.
    pub block_hashes: BlockHashCache,
    /// BAL state carried from the hot execution state.
    pub bal_state: BalState,
}

/// Latest-wins cache for the newest published hot dry-run seed.
#[derive(Debug)]
pub struct LatestHotDryRunSeedCache {
    latest: ArcSwapOption<HotDryRunSeed>,
}

impl Default for LatestHotDryRunSeedCache {
    fn default() -> Self {
        Self { latest: ArcSwapOption::new(None) }
    }
}

impl LatestHotDryRunSeedCache {
    /// Publishes the newest dry-run seed, replacing any previously cached seed.
    pub fn publish(&self, seed: Arc<HotDryRunSeed>) {
        self.latest.store(Some(seed));
    }

    /// Returns the latest published dry-run seed if one exists.
    pub fn latest(&self) -> Option<Arc<HotDryRunSeed>> {
        self.latest.load_full()
    }

    /// Clears the cached dry-run seed.
    pub fn clear(&self) {
        self.latest.store(None);
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use alloy_consensus::{Header, Sealed};
    use alloy_primitives::B256;
    use alloy_rpc_types::state::StateOverride;
    use alloy_rpc_types_engine::PayloadId;
    use revm::database::InMemoryDB;

    use super::{HotDryRunExecutionSeed, HotDryRunSeed, LatestHotDryRunSeedCache};
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

    fn seed(nonce: u64) -> Arc<HotDryRunSeed> {
        let hot_snapshot = Arc::new(HotSnapshot::new(
            snapshot_id(nonce),
            B256::with_last_byte((nonce + 2) as u8),
            header(nonce),
            StateOverride::default(),
        ));

        Arc::new(HotDryRunSeed {
            snapshot_id: hot_snapshot.snapshot_id,
            hot_snapshot,
            execution_seed: HotDryRunExecutionSeed::default(),
        })
    }

    #[test]
    fn latest_seed_cache_starts_empty() {
        let cache = LatestHotDryRunSeedCache::default();

        assert!(cache.latest().is_none());
    }

    #[test]
    fn latest_seed_cache_publishes_replaces_and_clears() {
        let cache = LatestHotDryRunSeedCache::default();
        let first = seed(1);
        let second = seed(2);

        cache.publish(Arc::clone(&first));
        assert_eq!(cache.latest().as_ref().map(|seed| seed.snapshot_id), Some(first.snapshot_id));

        cache.publish(Arc::clone(&second));
        assert_eq!(cache.latest().as_ref().map(|seed| seed.snapshot_id), Some(second.snapshot_id));

        cache.clear();
        assert!(cache.latest().is_none());
    }

    #[test]
    fn build_request_state_clones_seed_for_request_local_mutation() {
        let mut seed = HotDryRunSeed {
            snapshot_id: snapshot_id(3),
            hot_snapshot: Arc::new(HotSnapshot::new(
                snapshot_id(3),
                B256::with_last_byte(0x33),
                header(3),
                StateOverride::default(),
            )),
            execution_seed: HotDryRunExecutionSeed::default(),
        };
        seed.execution_seed.transition_state = Some(Default::default());
        seed.execution_seed.use_preloaded_bundle = true;
        seed.execution_seed.block_hashes.insert(1, B256::with_last_byte(0x11));

        let mut request_state = seed.build_request_state(InMemoryDB::default());
        request_state.transition_state = None;
        request_state.bundle_state.state_size = 77;
        request_state.use_preloaded_bundle = false;
        request_state.block_hashes.insert(2, B256::with_last_byte(0x22));

        assert!(seed.execution_seed.transition_state.is_some());
        assert_eq!(seed.execution_seed.bundle_state.state_size, 0);
        assert!(seed.execution_seed.use_preloaded_bundle);
        assert_eq!(seed.execution_seed.block_hashes.get(1), Some(B256::with_last_byte(0x11)));
        assert_eq!(seed.execution_seed.block_hashes.get(2), None);
    }
}
