//! Append-only pending window for the speed-first flashblock hot path.

use std::collections::{HashMap, VecDeque};

use alloy_consensus::{Header, Sealed};
use alloy_primitives::{Address, B256, BlockNumber, Bloom, TxHash};
use alloy_rpc_types::state::StateOverride;
use alloy_rpc_types_engine::PayloadId;
use base_common_evm::L1BlockInfo;
use base_common_flashblocks::{ExecutionPayloadBaseV1, Flashblock};
use base_common_rpc_types::{BaseTransactionReceipt, Transaction};

use crate::{
    AuditCursor, AuditWindowSnapshot, FastFlashblockLog, FastFlashblockTxMeta, RetainedFastOutput,
};

/// Execution state that carries forward across pending flashblocks and pending blocks.
#[derive(Debug)]
pub struct HotExecutionState<DB> {
    /// Mutable execution database carried forward across pending updates.
    pub db: DB,
    /// Latest executed header for the carried pending state.
    pub last_header: Sealed<Header>,
    /// Pending state overrides accumulated by execution.
    pub state_overrides: StateOverride,
    /// Latest L1 block info associated with the execution state.
    pub l1_block_info: L1BlockInfo,
}

/// Locally derived header material for sealing speculative blocks.
#[derive(Clone, Debug)]
pub struct HotExecutedHeaderParts {
    /// Total gas used across the locally executed block suffixes.
    pub gas_used: u64,
    /// Logs bloom derived from locally executed receipts.
    pub logs_bloom: Bloom,
    /// Receipts root derived from locally executed receipts.
    pub receipts_root: B256,
    /// State root derived from the locally executed post-state.
    pub state_root: B256,
    /// Withdrawals root derived from the locally executed post-state.
    pub withdrawals_root: B256,
    /// Blob gas used derived from locally executed receipts.
    pub blob_gas_used: Option<u64>,
    /// Requests hash derived from the locally executed payload sidecar semantics.
    pub requests_hash: Option<B256>,
}

/// Append-only data for one pending block.
#[derive(Clone, Debug)]
pub struct HotPendingBlock {
    /// Pending block number represented by this append-only block buffer.
    pub block_number: BlockNumber,
    /// Engine payload id for this pending block.
    pub payload_id: PayloadId,
    /// Base payload fields from the first flashblock for this pending block.
    pub base: ExecutionPayloadBaseV1,
    /// Parent hash for this pending block.
    pub parent_hash: B256,
    /// Highest flashblock index applied to this pending block.
    pub latest_flashblock_index: u64,
    /// Cumulative wire commitment for the full pending block after the latest applied suffix.
    ///
    /// This is updated from retained wire flashblocks during suffix execution and is not
    /// overwritten by local sealing.
    pub latest_wire_header_hash: B256,
    /// Latest header after applying the current flashblock suffix.
    pub latest_header: Sealed<Header>,
    /// Next transaction index to assign within the pending block.
    pub next_tx_index: u64,
    /// Next log index to assign within the pending block.
    pub next_log_index: u64,
    /// Cumulative gas used across all applied pending transactions.
    pub cumulative_gas_used: u64,
    /// Transaction metadata emitted into the hot fast-log stream.
    pub transactions: Vec<FastFlashblockTxMeta>,
    /// Logs emitted into the hot fast-log stream.
    pub logs: Vec<FastFlashblockLog>,
    /// Pending receipts keyed by transaction hash.
    pub receipts: HashMap<TxHash, BaseTransactionReceipt>,
    /// RPC transaction objects keyed by transaction hash.
    pub rpc_transactions: HashMap<TxHash, Transaction>,
    /// Transaction senders keyed by transaction hash.
    pub transaction_senders: HashMap<TxHash, Address>,
    /// Ordered flashblocks retained for periodic audit and retained-window snapshots.
    pub flashblocks: Vec<Flashblock>,
    /// Semantic fast outputs retained for later audit comparison.
    pub published_outputs: Vec<RetainedFastOutput>,
    /// Latest locally derived header material for this pending block.
    ///
    /// This is filled lazily only by legacy or non-hot sealing paths.
    pub local_header_parts: Option<HotExecutedHeaderParts>,
}

/// Canonical anchor retained beneath the speculative window.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct HotWindowAnchor {
    block_number: BlockNumber,
    hash: B256,
}

impl HotWindowAnchor {
    /// Creates a new speculative window anchor.
    pub const fn new(block_number: BlockNumber, hash: B256) -> Self {
        Self { block_number, hash }
    }

    /// Returns the canonical anchor block number.
    pub const fn block_number(&self) -> BlockNumber {
        self.block_number
    }

    /// Returns the canonical anchor block hash.
    pub const fn hash(&self) -> B256 {
        self.hash
    }
}

impl Default for HotWindowAnchor {
    fn default() -> Self {
        Self::new(0, B256::ZERO)
    }
}

/// Small multi-block pending window for exact hot execution continuity.
#[derive(Debug)]
pub struct HotPendingWindow<DB> {
    /// Maximum number of pending blocks retained in the window.
    pub max_depth: usize,
    /// Canonical anchor retained beneath the current speculative chain.
    pub anchor: HotWindowAnchor,
    /// Carried execution state used to continue pending execution.
    pub execution: Option<HotExecutionState<DB>>,
    /// Bounded append-only pending blocks ordered oldest to newest.
    pub blocks: VecDeque<HotPendingBlock>,
}

impl<DB> HotPendingWindow<DB> {
    /// Creates a new empty pending window.
    pub fn new(max_depth: usize) -> Self {
        Self {
            max_depth,
            anchor: HotWindowAnchor::default(),
            execution: None,
            blocks: VecDeque::with_capacity(max_depth),
        }
    }

    /// Clears the execution state and pending blocks.
    pub fn reset(&mut self) {
        self.anchor = HotWindowAnchor::default();
        self.execution = None;
        self.blocks.clear();
    }

    /// Pushes a new pending block into the append-only speculative window.
    pub fn push_block(&mut self, block: HotPendingBlock) {
        self.blocks.push_back(block);
    }

    /// Returns the active pending block.
    pub fn active_block(&self) -> Option<&HotPendingBlock> {
        self.blocks.back()
    }

    /// Returns the active pending block mutably.
    pub fn active_block_mut(&mut self) -> Option<&mut HotPendingBlock> {
        self.blocks.back_mut()
    }

    /// Returns the active pending block number.
    pub fn active_block_number(&self) -> Option<BlockNumber> {
        self.active_block().map(|block| block.block_number)
    }

    /// Returns the latest flashblock index within the active pending block.
    pub fn latest_flashblock_index(&self) -> Option<u64> {
        self.active_block().map(|block| block.latest_flashblock_index)
    }

    /// Returns the latest retained audit cursor in the pending window.
    pub fn latest_audit_cursor(&self) -> Option<AuditCursor> {
        self.blocks.iter().rev().find_map(|block| {
            block.published_outputs.last().map(|published_output| published_output.cursor)
        })
    }

    /// Returns retained flashblocks when the retained window starts immediately after the anchor.
    pub fn retained_flashblocks_from_anchor(
        &self,
        anchor_block_number: BlockNumber,
    ) -> Option<Vec<Flashblock>> {
        if !self.retained_audit_range_is_aligned(anchor_block_number) {
            return None;
        }

        Some(self.blocks.iter().flat_map(|block| block.flashblocks.iter().cloned()).collect())
    }

    /// Returns retained semantic outputs when the retained window starts immediately after the anchor.
    pub fn retained_outputs_from_anchor(
        &self,
        anchor_block_number: BlockNumber,
    ) -> Option<Vec<RetainedFastOutput>> {
        if !self.retained_audit_range_is_aligned(anchor_block_number) {
            return None;
        }

        Some(self.blocks.iter().flat_map(|block| block.published_outputs.iter().cloned()).collect())
    }

    /// Builds an immutable audit snapshot for the retained window.
    pub fn make_audit_snapshot(
        &self,
        generation: u64,
        window_id: u64,
        anchor_block_number: BlockNumber,
        anchor_hash: B256,
    ) -> Option<AuditWindowSnapshot> {
        if self.anchor != HotWindowAnchor::new(anchor_block_number, anchor_hash) {
            return None;
        }

        let flashblocks = self.retained_flashblocks_from_anchor(anchor_block_number)?;
        let expected_outputs = self.retained_outputs_from_anchor(anchor_block_number)?;
        let cursor = expected_outputs.last().map(|output| output.cursor)?;

        Some(AuditWindowSnapshot::new(
            generation,
            window_id,
            self.anchor.block_number(),
            self.anchor.hash(),
            cursor,
            flashblocks,
            expected_outputs,
        ))
    }

    /// Prunes retained blocks that have already been canonicalized through the given block.
    pub fn prune_canonicalized_prefix_through(
        &mut self,
        block_number: BlockNumber,
        canonical_hash: B256,
    ) {
        let canonical_anchor = HotWindowAnchor::new(block_number, canonical_hash);
        let mut pruned_any = false;

        while self
            .blocks
            .front()
            .is_some_and(|block| block.block_number <= canonical_anchor.block_number())
        {
            let Some(_retained_block) = self.blocks.pop_front() else {
                break;
            };
            pruned_any = true;
        }

        if pruned_any && canonical_anchor.block_number() >= self.anchor.block_number() {
            self.anchor = canonical_anchor;
        }

        if self.blocks.is_empty() {
            self.execution = None;
        }
    }

    fn first_retained_flashblock(&self) -> Option<&Flashblock> {
        self.blocks.iter().find_map(|block| block.flashblocks.first())
    }

    fn first_retained_output(&self) -> Option<&RetainedFastOutput> {
        self.blocks.iter().find_map(|block| block.published_outputs.first())
    }

    fn retained_audit_range_is_aligned(&self, anchor_block_number: BlockNumber) -> bool {
        let Some(expected_first_block_number) = anchor_block_number.checked_add(1) else {
            return false;
        };
        let Some(first_flashblock) = self.first_retained_flashblock() else {
            return false;
        };
        let Some(first_output) = self.first_retained_output() else {
            return false;
        };

        if first_flashblock.metadata.block_number != expected_first_block_number
            || first_flashblock.index != 0
            || first_output.cursor.block_number != expected_first_block_number
            || first_output.cursor.flashblock_index != 0
        {
            return false;
        }

        let mut expected_parent_hash = self.anchor.hash();

        for (block_offset, block) in self.blocks.iter().enumerate() {
            let Ok(block_offset) = u64::try_from(block_offset) else {
                return false;
            };
            let Some(expected_block_number) = expected_first_block_number.checked_add(block_offset)
            else {
                return false;
            };

            let Some(expected_retained_entries) = usize::try_from(block.latest_flashblock_index)
                .ok()
                .and_then(|latest_flashblock_index| latest_flashblock_index.checked_add(1))
            else {
                return false;
            };

            if block.flashblocks.is_empty() || block.block_number != expected_block_number {
                return false;
            }

            if block.parent_hash != expected_parent_hash
                || block.base.parent_hash != expected_parent_hash
            {
                return false;
            }

            if block.flashblocks.len() != expected_retained_entries
                || block.published_outputs.len() != expected_retained_entries
            {
                return false;
            }

            for (expected_flashblock_index, (flashblock, published_output)) in
                block.flashblocks.iter().zip(&block.published_outputs).enumerate()
            {
                let Ok(expected_flashblock_index) = u64::try_from(expected_flashblock_index) else {
                    return false;
                };

                if flashblock.metadata.block_number != expected_block_number
                    || flashblock.index != expected_flashblock_index
                    || flashblock.payload_id != published_output.cursor.payload_id
                    || published_output.cursor.block_number != expected_block_number
                    || published_output.cursor.flashblock_index != expected_flashblock_index
                    || published_output.cursor.parent_hash != block.parent_hash
                {
                    return false;
                }

                if expected_flashblock_index == 0
                    && flashblock.base.as_ref().map(|base| base.parent_hash)
                        != Some(block.parent_hash)
                {
                    return false;
                }
            }

            expected_parent_hash = block.latest_wire_header_hash;
        }

        true
    }
}

#[cfg(test)]
mod tests {
    use alloy_consensus::{Header, Sealed};
    use alloy_primitives::{Address, B256, Bloom, Bytes, U256};
    use alloy_rpc_types_engine::PayloadId;
    use base_common_flashblocks::{
        ExecutionPayloadBaseV1, ExecutionPayloadFlashblockDeltaV1, Flashblock, Metadata,
    };

    use super::{
        HotExecutedHeaderParts, HotExecutionState, HotPendingBlock, HotPendingWindow,
        HotWindowAnchor,
    };
    use crate::{AuditCursor, RetainedFastOutput};

    fn test_header(block_number: u64, parent_hash: B256) -> Sealed<Header> {
        Sealed::new_unchecked(
            Header {
                number: block_number,
                parent_hash,
                timestamp: 1_700_000_000 + block_number,
                ..Default::default()
            },
            B256::with_last_byte((block_number + 1) as u8),
        )
    }

    fn test_pending_block(block_number: u64, latest_flashblock_index: u64) -> HotPendingBlock {
        let parent_hash = B256::with_last_byte(block_number as u8);

        HotPendingBlock {
            block_number,
            payload_id: PayloadId::new([block_number as u8; 8]),
            base: ExecutionPayloadBaseV1 {
                parent_beacon_block_root: B256::ZERO,
                parent_hash,
                fee_recipient: Address::ZERO,
                prev_randao: B256::ZERO,
                block_number,
                gas_limit: 30_000_000,
                timestamp: 1_700_000_000 + block_number,
                extra_data: Bytes::default(),
                base_fee_per_gas: U256::from(1_000_000_000u64),
            },
            parent_hash,
            latest_flashblock_index,
            latest_wire_header_hash: test_header(block_number, parent_hash).hash(),
            latest_header: test_header(block_number, parent_hash),
            next_tx_index: latest_flashblock_index + 1,
            next_log_index: latest_flashblock_index + 2,
            cumulative_gas_used: 21_000 * (latest_flashblock_index + 1),
            transactions: vec![],
            logs: vec![],
            receipts: std::collections::HashMap::new(),
            rpc_transactions: std::collections::HashMap::new(),
            transaction_senders: std::collections::HashMap::new(),
            flashblocks: vec![],
            published_outputs: vec![],
            local_header_parts: None,
        }
    }

    fn test_flashblock_with_parent_hash(
        block_number: u64,
        flashblock_index: u64,
        parent_hash: B256,
    ) -> Flashblock {
        Flashblock {
            payload_id: PayloadId::new([block_number as u8; 8]),
            index: flashblock_index,
            base: (flashblock_index == 0).then_some(ExecutionPayloadBaseV1 {
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
                gas_used: 21_000u64.saturating_mul(flashblock_index.saturating_add(1)),
                block_hash: B256::with_last_byte(block_number as u8 ^ flashblock_index as u8),
                transactions: vec![],
                withdrawals: vec![],
                withdrawals_root: B256::ZERO,
                blob_gas_used: None,
            },
            metadata: Metadata { block_number },
        }
    }

    fn test_retained_output_with_parent_hash(
        block_number: u64,
        flashblock_index: u64,
        parent_hash: B256,
    ) -> RetainedFastOutput {
        let tx_hash = B256::with_last_byte(block_number as u8 ^ flashblock_index as u8 ^ 0x55);

        RetainedFastOutput {
            cursor: AuditCursor::new(
                block_number,
                flashblock_index,
                PayloadId::new([block_number as u8; 8]),
                parent_hash,
            ),
            block_timestamp: Some(1_700_000_000 + block_number),
            transactions: vec![crate::FastFlashblockTxMeta {
                hash: tx_hash,
                index: flashblock_index,
                status: Some(1),
            }],
            logs: vec![crate::FastFlashblockLog {
                tx_hash,
                tx_index: flashblock_index,
                log_index_in_tx: 0,
                log_index_in_block: flashblock_index,
                address: Address::with_last_byte((block_number as u8).saturating_add(1)),
                topics: vec![B256::with_last_byte((flashblock_index as u8).saturating_add(2))],
                data: Bytes::from_static(&[0xaa, 0xbb]),
            }],
        }
    }

    fn test_pending_block_with_retained_prefix(
        block_number: u64,
        latest_flashblock_index: u64,
    ) -> HotPendingBlock {
        test_pending_block_with_retained_prefix_and_parent_hash(
            block_number,
            latest_flashblock_index,
            B256::with_last_byte(block_number as u8),
        )
    }

    fn test_pending_block_with_retained_prefix_and_parent_hash(
        block_number: u64,
        latest_flashblock_index: u64,
        parent_hash: B256,
    ) -> HotPendingBlock {
        let mut pending_block = test_pending_block(block_number, latest_flashblock_index);

        pending_block.parent_hash = parent_hash;
        pending_block.base.parent_hash = parent_hash;
        pending_block.latest_wire_header_hash = test_header(block_number, parent_hash).hash();
        pending_block.latest_header = test_header(block_number, parent_hash);

        pending_block.flashblocks = (0..=latest_flashblock_index)
            .map(|flashblock_index| {
                test_flashblock_with_parent_hash(block_number, flashblock_index, parent_hash)
            })
            .collect();
        pending_block.published_outputs = (0..=latest_flashblock_index)
            .map(|flashblock_index| {
                test_retained_output_with_parent_hash(block_number, flashblock_index, parent_hash)
            })
            .collect();

        pending_block
    }

    #[test]
    fn hot_pending_window_push_block_retains_blocks_until_reconciliation_prunes_them() {
        let mut window = HotPendingWindow::<()>::new(2);

        window.push_block(test_pending_block(1, 0));
        window.push_block(test_pending_block(2, 1));
        window.push_block(test_pending_block(3, 2));

        assert_eq!(window.blocks.len(), 3);
        assert_eq!(window.blocks.front().map(|block| block.block_number), Some(1));
        assert_eq!(window.active_block_number(), Some(3));
        assert_eq!(window.latest_flashblock_index(), Some(2));
    }

    #[test]
    fn hot_pending_window_reset_clears_execution_and_blocks() {
        let mut window = HotPendingWindow::new(2);
        window.anchor = HotWindowAnchor::new(6, B256::with_last_byte(0x44));
        window.execution = Some(HotExecutionState {
            db: (),
            last_header: test_header(7, B256::with_last_byte(0x33)),
            state_overrides: alloy_rpc_types::state::StateOverride::default(),
            l1_block_info: base_common_evm::L1BlockInfo::default(),
        });
        window.push_block(test_pending_block(7, 9));

        window.reset();

        assert_eq!(window.anchor, HotWindowAnchor::default());
        assert!(window.execution.is_none());
        assert!(window.blocks.is_empty());
        assert!(window.active_block().is_none());
        assert!(window.active_block_mut().is_none());
        assert!(window.active_block_number().is_none());
        assert!(window.latest_flashblock_index().is_none());
    }

    #[test]
    fn hot_pending_block_tracks_local_header_parts() {
        let mut pending_block = test_pending_block(7, 0);
        let local_header_parts = HotExecutedHeaderParts {
            gas_used: 21_000,
            logs_bloom: Bloom::default(),
            receipts_root: B256::with_last_byte(0x31),
            state_root: B256::with_last_byte(0x32),
            withdrawals_root: B256::with_last_byte(0x33),
            blob_gas_used: Some(44),
            requests_hash: None,
        };

        pending_block.local_header_parts = Some(local_header_parts.clone());

        assert_eq!(
            pending_block.local_header_parts.as_ref().map(|parts| parts.gas_used),
            Some(21_000)
        );
        assert_eq!(
            pending_block.local_header_parts.as_ref().map(|parts| parts.state_root),
            Some(local_header_parts.state_root)
        );
    }

    #[test]
    fn hot_pending_window_make_audit_snapshot_retains_full_prefix_from_anchor() {
        let mut window = HotPendingWindow::<()>::new(4);
        let anchor = HotWindowAnchor::new(10, B256::with_last_byte(0xdd));
        let first_block =
            test_pending_block_with_retained_prefix_and_parent_hash(11, 1, anchor.hash());
        let second_parent_hash = first_block.latest_wire_header_hash;
        let expected_cursor =
            test_retained_output_with_parent_hash(12, 0, second_parent_hash).cursor;

        window.anchor = anchor;
        window.push_block(first_block);
        window.push_block(test_pending_block_with_retained_prefix_and_parent_hash(
            12,
            0,
            second_parent_hash,
        ));

        let latest_cursor = window.latest_audit_cursor();
        let retained_flashblocks = window.retained_flashblocks_from_anchor(10);
        let retained_outputs = window.retained_outputs_from_anchor(10);
        let snapshot = window.make_audit_snapshot(7, 9, 10, anchor.hash());

        assert_eq!(latest_cursor, Some(expected_cursor));
        assert_eq!(retained_flashblocks.as_ref().map(Vec::len), Some(3));
        assert_eq!(retained_outputs.as_ref().map(Vec::len), Some(3));

        let snapshot = snapshot.expect("aligned retained prefix should snapshot");
        assert_eq!(snapshot.generation, 7);
        assert_eq!(snapshot.window_id, 9);
        assert_eq!(snapshot.anchor_block_number, anchor.block_number());
        assert_eq!(snapshot.anchor_hash, anchor.hash());
        assert_eq!(snapshot.cursor, expected_cursor);
        assert_eq!(snapshot.flashblocks, retained_flashblocks.expect("flashblocks should exist"));
        assert_eq!(
            snapshot.expected_outputs,
            retained_outputs.expect("retained outputs should exist")
        );
    }

    #[test]
    fn hot_pending_window_make_audit_snapshot_rejects_mismatched_anchor_hash() {
        let mut window = HotPendingWindow::<()>::new(4);
        let anchor = HotWindowAnchor::new(10, B256::with_last_byte(0xdd));

        window.anchor = anchor;
        window.push_block(test_pending_block_with_retained_prefix_and_parent_hash(
            11,
            1,
            anchor.hash(),
        ));

        assert!(window.make_audit_snapshot(7, 9, 10, B256::with_last_byte(0xee)).is_none());
        assert!(window.make_audit_snapshot(7, 9, 10, anchor.hash()).is_some());
    }

    #[test]
    fn hot_pending_window_make_audit_snapshot_rejects_first_block_not_rooted_at_anchor_hash() {
        let mut window = HotPendingWindow::<()>::new(4);
        let anchor = HotWindowAnchor::new(10, B256::with_last_byte(0xdd));

        window.anchor = anchor;
        window.push_block(test_pending_block_with_retained_prefix(11, 1));

        assert!(window.retained_flashblocks_from_anchor(anchor.block_number()).is_none());
        assert!(window.retained_outputs_from_anchor(anchor.block_number()).is_none());
        assert!(window.make_audit_snapshot(7, 9, anchor.block_number(), anchor.hash()).is_none());
    }

    #[test]
    fn hot_pending_window_make_audit_snapshot_rejects_second_block_not_building_on_previous_wire_hash()
     {
        let mut window = HotPendingWindow::<()>::new(4);
        let anchor = HotWindowAnchor::new(10, B256::with_last_byte(0xdd));
        let first_block =
            test_pending_block_with_retained_prefix_and_parent_hash(11, 1, anchor.hash());
        let wrong_second_parent_hash = B256::with_last_byte(0xee);

        assert_ne!(first_block.latest_wire_header_hash, wrong_second_parent_hash);

        window.anchor = anchor;
        window.push_block(first_block);
        window.push_block(test_pending_block_with_retained_prefix_and_parent_hash(
            12,
            0,
            wrong_second_parent_hash,
        ));

        assert!(window.retained_flashblocks_from_anchor(anchor.block_number()).is_none());
        assert!(window.retained_outputs_from_anchor(anchor.block_number()).is_none());
        assert!(window.make_audit_snapshot(7, 9, anchor.block_number(), anchor.hash()).is_none());
    }

    #[test]
    fn hot_pending_window_make_audit_snapshot_rejects_output_cursor_parent_mismatch() {
        let mut window = HotPendingWindow::<()>::new(4);
        let anchor = HotWindowAnchor::new(10, B256::with_last_byte(0xdd));
        let mut first_block =
            test_pending_block_with_retained_prefix_and_parent_hash(11, 1, anchor.hash());

        first_block.published_outputs[1].cursor.parent_hash = B256::with_last_byte(0xee);

        window.anchor = anchor;
        window.push_block(first_block);

        assert!(window.retained_flashblocks_from_anchor(anchor.block_number()).is_none());
        assert!(window.retained_outputs_from_anchor(anchor.block_number()).is_none());
        assert!(window.make_audit_snapshot(7, 9, anchor.block_number(), anchor.hash()).is_none());
    }

    #[test]
    fn hot_pending_window_make_audit_snapshot_requires_pruned_prefix_for_later_anchor() {
        let mut window = HotPendingWindow::<()>::new(4);
        let anchor = HotWindowAnchor::new(10, B256::with_last_byte(0xaa));
        let mut pruned_block =
            test_pending_block_with_retained_prefix_and_parent_hash(11, 1, anchor.hash());
        let wire_anchor_hash = B256::with_last_byte(0xfe);
        let verified_header = test_header(11, B256::with_last_byte(0x11));
        let canonical_anchor = HotWindowAnchor::new(11, verified_header.hash());

        pruned_block.latest_wire_header_hash = wire_anchor_hash;

        window.anchor = anchor;
        window.push_block(pruned_block);
        window.push_block(test_pending_block_with_retained_prefix_and_parent_hash(
            12,
            0,
            canonical_anchor.hash(),
        ));

        assert!(window.retained_flashblocks_from_anchor(11).is_none());
        assert!(window.retained_outputs_from_anchor(11).is_none());
        assert!(window.make_audit_snapshot(1, 2, 11, canonical_anchor.hash()).is_none());
        assert!(window.make_audit_snapshot(1, 2, 11, wire_anchor_hash).is_none());

        window.prune_canonicalized_prefix_through(
            canonical_anchor.block_number(),
            canonical_anchor.hash(),
        );

        assert_eq!(window.anchor, canonical_anchor);
        assert_eq!(window.blocks.len(), 1);
        assert_eq!(window.blocks.front().map(|block| block.block_number), Some(12));
        assert!(window.make_audit_snapshot(1, 2, 11, wire_anchor_hash).is_none());

        let snapshot = window
            .make_audit_snapshot(1, 2, 11, canonical_anchor.hash())
            .expect("pruned prefix should allow later anchor snapshot");

        assert_eq!(snapshot.anchor_block_number, 11);
        assert_eq!(snapshot.anchor_hash, canonical_anchor.hash());
        assert_eq!(snapshot.flashblocks.len(), 1);
        assert_eq!(snapshot.expected_outputs.len(), 1);
        assert_eq!(
            snapshot.cursor,
            test_retained_output_with_parent_hash(12, 0, canonical_anchor.hash()).cursor
        );
    }

    #[test]
    fn hot_pending_window_make_audit_snapshot_requires_complete_retained_outputs() {
        let mut window = HotPendingWindow::<()>::new(4);
        let anchor = HotWindowAnchor::new(10, B256::with_last_byte(0xdd));
        let mut lagging_block =
            test_pending_block_with_retained_prefix_and_parent_hash(11, 2, anchor.hash());

        window.anchor = anchor;
        lagging_block.flashblocks.pop();
        lagging_block.published_outputs.pop();
        window.push_block(lagging_block);

        assert!(window.retained_flashblocks_from_anchor(anchor.block_number()).is_none());
        assert!(window.retained_outputs_from_anchor(anchor.block_number()).is_none());
        assert!(window.make_audit_snapshot(7, 9, anchor.block_number(), anchor.hash()).is_none());
    }
}
