//! Append-only pending window for the speed-first flashblock hot path.

use std::collections::{HashMap, VecDeque};

use alloy_consensus::{Header, Sealed};
use alloy_primitives::{Address, B256, BlockNumber, Bloom, TxHash};
use alloy_rpc_types::state::StateOverride;
use alloy_rpc_types_engine::PayloadId;
use base_common_evm::L1BlockInfo;
use base_common_flashblocks::{ExecutionPayloadBaseV1, Flashblock};
use base_common_rpc_types::{BaseTransactionReceipt, Transaction};

use crate::{FastFlashblockLog, FastFlashblockTxMeta};

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
    /// Ordered flashblocks retained so this pending block can be silently replayed.
    pub flashblocks: Vec<Flashblock>,
    /// Latest locally derived header material for this pending block.
    ///
    /// This is filled lazily only when the block must be locally sealed
    /// (for example at rollover or canonical reconciliation).
    pub local_header_parts: Option<HotExecutedHeaderParts>,
}

/// Locally retained proof for one speculative block that was verified by a child rollover.
#[derive(Clone, Debug)]
pub struct RetainedVerifiedBlock {
    /// Speculative block number represented by this retained proof.
    pub block_number: BlockNumber,
    /// Parent hash for the retained speculative block.
    pub parent_hash: B256,
    /// Locally sealed speculative header used for canonical reconciliation.
    pub sealed_header: Sealed<Header>,
    /// Ordered transaction hashes used to cross-check canonical equivalence.
    pub transaction_hashes: Vec<TxHash>,
}

/// Small multi-block pending window for exact hot execution continuity.
#[derive(Debug)]
pub struct HotPendingWindow<DB> {
    /// Maximum number of pending blocks retained in the window.
    pub max_depth: usize,
    /// Canonical anchor block number beneath the current speculative chain.
    pub speculative_anchor_block: BlockNumber,
    /// Canonical anchor block hash beneath the current speculative chain.
    pub canonical_base_parent_hash: B256,
    /// Carried execution state used to continue pending execution.
    pub execution: Option<HotExecutionState<DB>>,
    /// Retained proofs for speculative blocks already verified by child rollovers.
    pub verified_blocks: VecDeque<RetainedVerifiedBlock>,
    /// Bounded append-only pending blocks ordered oldest to newest.
    pub blocks: VecDeque<HotPendingBlock>,
}

impl<DB> HotPendingWindow<DB> {
    /// Creates a new empty pending window.
    pub fn new(max_depth: usize) -> Self {
        Self {
            max_depth,
            speculative_anchor_block: 0,
            canonical_base_parent_hash: B256::ZERO,
            execution: None,
            verified_blocks: VecDeque::with_capacity(max_depth),
            blocks: VecDeque::with_capacity(max_depth),
        }
    }

    /// Clears the execution state and pending blocks.
    pub fn reset(&mut self) {
        self.speculative_anchor_block = 0;
        self.canonical_base_parent_hash = B256::ZERO;
        self.execution = None;
        self.verified_blocks.clear();
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
}

#[cfg(test)]
mod tests {
    use alloy_consensus::{Header, Sealed};
    use alloy_primitives::{Address, B256, Bloom, Bytes, U256};
    use alloy_rpc_types_engine::PayloadId;
    use base_common_flashblocks::ExecutionPayloadBaseV1;

    use super::{
        HotExecutedHeaderParts, HotExecutionState, HotPendingBlock, HotPendingWindow,
        RetainedVerifiedBlock,
    };

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
            local_header_parts: None,
        }
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
        window.speculative_anchor_block = 6;
        window.canonical_base_parent_hash = B256::with_last_byte(0x44);
        window.execution = Some(HotExecutionState {
            db: (),
            last_header: test_header(7, B256::with_last_byte(0x33)),
            state_overrides: alloy_rpc_types::state::StateOverride::default(),
            l1_block_info: base_common_evm::L1BlockInfo::default(),
        });
        window.verified_blocks.push_back(RetainedVerifiedBlock {
            block_number: 6,
            parent_hash: B256::with_last_byte(0x11),
            sealed_header: test_header(6, B256::with_last_byte(0x11)),
            transaction_hashes: vec![B256::with_last_byte(0x22)],
        });
        window.push_block(test_pending_block(7, 9));

        window.reset();

        assert_eq!(window.speculative_anchor_block, 0);
        assert_eq!(window.canonical_base_parent_hash, B256::ZERO);
        assert!(window.execution.is_none());
        assert!(window.verified_blocks.is_empty());
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
}
