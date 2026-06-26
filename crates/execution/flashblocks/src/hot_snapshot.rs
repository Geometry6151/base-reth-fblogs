//! Hot snapshot handles for pinned flashblock RPC.

use std::{
    collections::VecDeque,
    sync::{Arc, OnceLock},
};

use alloy_consensus::{Header, Sealed};
use alloy_eips::{BlockId, RpcBlockHash};
use alloy_primitives::{B256, U256};
use alloy_rpc_types::{BlockOverrides, state::StateOverride};

use crate::{
    FlashblockSnapshotId,
    hot_overlay::{HotOverlay, HotOverlayError},
};

/// Queryable snapshot produced by the hot engine.
#[derive(Clone, Debug)]
pub struct HotSnapshot {
    /// Identity of the emitted hot delta that produced this snapshot.
    pub snapshot_id: FlashblockSnapshotId,
    /// Canonical block id used as the base for pinned flashblock RPC.
    pub canonical_base_block: BlockId,
    /// Canonical parent hash beneath the pending window base.
    pub canonical_base_parent_hash: B256,
    /// Latest pending header represented by this snapshot.
    pub latest_header: Sealed<Header>,
    /// State overrides materialized from the hot execution state.
    pub state_overrides: StateOverride,
    /// Block environment overrides for pinned call-style RPC.
    pub block_overrides: BlockOverrides,
    /// Lazily initialized immutable overlay used by the direct dry-run evaluator.
    dry_run_overlay: Arc<OnceLock<Result<Arc<HotOverlay>, HotOverlayError>>>,
}

impl HotSnapshot {
    /// Creates a new hot snapshot from the current hot execution view.
    pub fn new(
        snapshot_id: FlashblockSnapshotId,
        canonical_base_parent_hash: B256,
        latest_header: Sealed<Header>,
        state_overrides: StateOverride,
    ) -> Self {
        let block_overrides = Self::block_overrides(&latest_header);

        Self {
            snapshot_id,
            canonical_base_block: BlockId::Hash(RpcBlockHash::from_hash(
                canonical_base_parent_hash,
                Some(true),
            )),
            canonical_base_parent_hash,
            latest_header,
            state_overrides,
            block_overrides,
            dry_run_overlay: Arc::new(OnceLock::new()),
        }
    }

    /// Returns the lazy overlay cell used by direct dry-run evaluation.
    pub fn dry_run_overlay(&self) -> &OnceLock<Result<Arc<HotOverlay>, HotOverlayError>> {
        self.dry_run_overlay.as_ref()
    }

    /// Builds pinned block overrides from the represented pending header.
    pub fn block_overrides(header: &Sealed<Header>) -> BlockOverrides {
        BlockOverrides {
            number: Some(U256::from(header.number)),
            difficulty: Some(header.difficulty),
            time: Some(header.timestamp),
            gas_limit: Some(header.gas_limit),
            coinbase: Some(header.beneficiary),
            random: Some(header.mix_hash),
            base_fee: header.base_fee_per_gas.map(U256::from),
            blob_base_fee: None,
            beacon_root: header.parent_beacon_block_root,
            block_hash: None,
        }
    }
}

/// Fixed-size in-memory snapshot ring for recent hot deltas.
#[derive(Debug)]
pub struct HotSnapshotRing {
    capacity: usize,
    entries: VecDeque<Arc<HotSnapshot>>,
}

impl HotSnapshotRing {
    /// Creates a new hot snapshot ring with the given capacity.
    pub fn new(capacity: usize) -> Self {
        Self { capacity, entries: VecDeque::with_capacity(capacity) }
    }

    /// Inserts a hot snapshot into the ring.
    pub fn insert(&mut self, snapshot: Arc<HotSnapshot>) {
        if self.capacity == 0 {
            return;
        }

        if self.entries.len() == self.capacity {
            self.entries.pop_front();
        }
        self.entries.push_back(snapshot);
    }

    /// Returns the most recently inserted hot snapshot if one exists.
    pub fn latest(&self) -> Option<Arc<HotSnapshot>> {
        self.entries.back().cloned()
    }

    /// Returns the hot snapshot for the given identifier if it exists.
    pub fn get(&self, snapshot_id: FlashblockSnapshotId) -> Option<Arc<HotSnapshot>> {
        self.entries.iter().find(|snapshot| snapshot.snapshot_id == snapshot_id).cloned()
    }

    /// Removes all hot snapshots from the ring.
    pub fn clear(&mut self) {
        self.entries.clear();
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, OnceLock};

    use alloy_consensus::{Header, Sealed};
    use alloy_primitives::B256;
    use alloy_rpc_types_engine::PayloadId;

    use super::{HotSnapshot, HotSnapshotRing};
    use crate::FlashblockSnapshotId;

    fn snapshot_id(nonce: u64) -> FlashblockSnapshotId {
        FlashblockSnapshotId::new(
            nonce,
            nonce,
            nonce,
            PayloadId::new([nonce as u8; 8]),
            B256::with_last_byte(nonce as u8),
        )
    }

    fn block_overrides() -> alloy_rpc_types::BlockOverrides {
        alloy_rpc_types::BlockOverrides {
            number: None,
            difficulty: None,
            time: None,
            gas_limit: None,
            coinbase: None,
            random: None,
            base_fee: None,
            blob_base_fee: None,
            beacon_root: None,
            block_hash: None,
        }
    }

    fn header(nonce: u64) -> Sealed<Header> {
        Sealed::new_unchecked(
            Header {
                number: nonce,
                parent_hash: B256::with_last_byte(nonce as u8),
                ..Default::default()
            },
            B256::with_last_byte((nonce + 2) as u8),
        )
    }

    fn hot_snapshot(nonce: u64) -> Arc<HotSnapshot> {
        Arc::new(HotSnapshot {
            snapshot_id: snapshot_id(nonce),
            canonical_base_block: alloy_eips::BlockId::Number(nonce.into()),
            canonical_base_parent_hash: B256::with_last_byte((nonce + 1) as u8),
            latest_header: header(nonce),
            state_overrides: alloy_rpc_types::state::StateOverride::default(),
            block_overrides: block_overrides(),
            dry_run_overlay: Arc::new(OnceLock::new()),
        })
    }

    #[test]
    fn hot_snapshot_new_derives_hash_base_and_block_overrides() {
        let snapshot_id = snapshot_id(7);
        let canonical_base_parent_hash = B256::with_last_byte(0x44);
        let latest_header = header(7);

        let snapshot = HotSnapshot::new(
            snapshot_id,
            canonical_base_parent_hash,
            latest_header.clone(),
            alloy_rpc_types::state::StateOverride::default(),
        );

        assert_eq!(snapshot.snapshot_id, snapshot_id);
        assert_eq!(snapshot.canonical_base_parent_hash, canonical_base_parent_hash);
        assert_eq!(snapshot.latest_header, latest_header);
        assert_eq!(snapshot.block_overrides, HotSnapshot::block_overrides(&latest_header));
        assert!(snapshot.dry_run_overlay().get().is_none());
        assert_eq!(
            snapshot.canonical_base_block,
            alloy_eips::BlockId::Hash(alloy_eips::RpcBlockHash::from_hash(
                canonical_base_parent_hash,
                Some(true),
            )),
        );
    }

    #[test]
    fn hot_snapshot_ring_insert_and_lookup_returns_same_arc() {
        let mut ring = HotSnapshotRing::new(2);
        let snapshot = hot_snapshot(1);

        ring.insert(Arc::clone(&snapshot));

        let cached = ring.get(snapshot.snapshot_id).expect("snapshot should be retained");
        assert!(Arc::ptr_eq(&snapshot, &cached));
    }

    #[test]
    fn hot_snapshot_ring_latest_returns_most_recent_snapshot() {
        let mut ring = HotSnapshotRing::new(2);
        let first = hot_snapshot(1);
        let second = hot_snapshot(2);

        assert!(ring.latest().is_none());

        ring.insert(Arc::clone(&first));
        assert!(Arc::ptr_eq(&first, &ring.latest().expect("latest first")));

        ring.insert(Arc::clone(&second));
        assert!(Arc::ptr_eq(&second, &ring.latest().expect("latest second")));
    }

    #[test]
    fn hot_snapshot_ring_retains_only_most_recent_snapshots() {
        let mut ring = HotSnapshotRing::new(2);
        let first = hot_snapshot(1);
        let second = hot_snapshot(2);
        let third = hot_snapshot(3);

        ring.insert(Arc::clone(&first));
        ring.insert(Arc::clone(&second));
        ring.insert(Arc::clone(&third));

        assert!(ring.get(first.snapshot_id).is_none());
        assert!(ring.get(second.snapshot_id).is_some());
        assert!(ring.get(third.snapshot_id).is_some());
    }
}
