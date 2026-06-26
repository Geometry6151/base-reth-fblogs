//! Retained audit window types and semantic comparison helpers.

use alloy_primitives::{B256, BlockNumber};
use alloy_rpc_types_engine::PayloadId;
use base_common_flashblocks::Flashblock;

use crate::{FastFlashblockLog, FastFlashblockLogsDelta, FastFlashblockTxMeta};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum PeriodicAuditMismatchOrigin {
    SnapshotAnchorShape,
    ReplayNonDeltaOutcome,
    RebuiltOutputLength,
    RebuiltOutputSemantic,
    LiveFlashblockPrefix,
    LiveOutputPrefix,
    LiveAnchor,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum PeriodicAuditOutputMismatchField {
    Cursor,
    BlockTimestamp,
    Transactions,
    Logs,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct PeriodicAuditOutputMismatch {
    pub index: usize,
    pub field: PeriodicAuditOutputMismatchField,
    pub expected_cursor: AuditCursor,
    pub actual_cursor: AuditCursor,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RebuiltOutputMismatch {
    Length { expected_len: usize, actual_len: usize },
    Semantic(PeriodicAuditOutputMismatch),
}

/// Cursor identifying one retained fast output inside an audit window.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct AuditCursor {
    /// Pending block number represented by the retained output.
    pub block_number: BlockNumber,
    /// Flashblock index within the pending block.
    pub flashblock_index: u64,
    /// Engine payload identifier for the pending block.
    pub payload_id: PayloadId,
    /// Parent hash for the pending block.
    pub parent_hash: B256,
}

impl AuditCursor {
    /// Creates a new audit cursor.
    pub const fn new(
        block_number: BlockNumber,
        flashblock_index: u64,
        payload_id: PayloadId,
        parent_hash: B256,
    ) -> Self {
        Self { block_number, flashblock_index, payload_id, parent_hash }
    }

    /// Derives an audit cursor from one fast delta.
    pub const fn from_delta(delta: &FastFlashblockLogsDelta) -> Self {
        Self::new(delta.block_number, delta.flashblock_index, delta.payload_id, delta.parent_hash)
    }
}

/// Semantic fast-path output retained for later periodic audit comparison.
#[derive(Clone, Debug)]
pub struct RetainedFastOutput {
    /// Semantic identity for this retained fast output.
    pub cursor: AuditCursor,
    /// Pending block timestamp carried by the fast delta.
    pub block_timestamp: Option<u64>,
    /// Transaction metadata emitted for this flashblock update.
    pub transactions: Vec<FastFlashblockTxMeta>,
    /// Logs emitted for this flashblock update.
    pub logs: Vec<FastFlashblockLog>,
}

impl RetainedFastOutput {
    /// Derives a retained semantic output from one fast delta.
    pub fn from_delta(delta: &FastFlashblockLogsDelta) -> Self {
        Self {
            cursor: AuditCursor::from_delta(delta),
            block_timestamp: delta.block_timestamp,
            transactions: delta.transactions.clone(),
            logs: delta.logs.clone(),
        }
    }

    pub(crate) fn first_mismatch(
        expected_outputs: &[Self],
        actual_outputs: &[Self],
    ) -> Option<PeriodicAuditOutputMismatch> {
        expected_outputs.iter().zip(actual_outputs).enumerate().find_map(
            |(index, (expected_output, actual_output))| {
                expected_output.mismatch_field(actual_output).map(|field| {
                    PeriodicAuditOutputMismatch {
                        index,
                        field,
                        expected_cursor: expected_output.cursor,
                        actual_cursor: actual_output.cursor,
                    }
                })
            },
        )
    }

    /// Returns whether two retained outputs are semantically equivalent for audit purposes.
    pub fn semantically_matches(&self, other: &Self) -> bool {
        self.mismatch_field(other).is_none()
    }

    fn mismatch_field(&self, other: &Self) -> Option<PeriodicAuditOutputMismatchField> {
        if self.cursor != other.cursor {
            Some(PeriodicAuditOutputMismatchField::Cursor)
        } else if self.block_timestamp != other.block_timestamp {
            Some(PeriodicAuditOutputMismatchField::BlockTimestamp)
        } else if self.transactions != other.transactions {
            Some(PeriodicAuditOutputMismatchField::Transactions)
        } else if self.logs != other.logs {
            Some(PeriodicAuditOutputMismatchField::Logs)
        } else {
            None
        }
    }
}

impl PartialEq for RetainedFastOutput {
    fn eq(&self, other: &Self) -> bool {
        self.semantically_matches(other)
    }
}

impl Eq for RetainedFastOutput {}

/// Immutable retained window extracted for one periodic audit pass.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AuditWindowSnapshot {
    /// Session generation this snapshot belongs to.
    pub generation: u64,
    /// Monotonic audit window identifier within one generation.
    pub window_id: u64,
    /// Canonical anchor block number beneath the retained replay range.
    pub anchor_block_number: BlockNumber,
    /// Canonical anchor hash beneath the retained replay range.
    pub anchor_hash: B256,
    /// Latest retained cursor included in this snapshot.
    pub cursor: AuditCursor,
    /// Raw flashblocks retained for replay.
    pub flashblocks: Vec<Flashblock>,
    /// Expected semantic outputs for the retained replay range.
    pub expected_outputs: Vec<RetainedFastOutput>,
}

impl AuditWindowSnapshot {
    /// Creates a new immutable audit window snapshot.
    pub const fn new(
        generation: u64,
        window_id: u64,
        anchor_block_number: BlockNumber,
        anchor_hash: B256,
        cursor: AuditCursor,
        flashblocks: Vec<Flashblock>,
        expected_outputs: Vec<RetainedFastOutput>,
    ) -> Self {
        Self {
            generation,
            window_id,
            anchor_block_number,
            anchor_hash,
            cursor,
            flashblocks,
            expected_outputs,
        }
    }

    /// Returns whether the retained replay range still starts immediately after the anchor.
    pub fn first_replayed_flashblock_matches_anchor(&self) -> bool {
        let Some(first_replayed_block_number) = self.anchor_block_number.checked_add(1) else {
            return false;
        };
        let Some(first_flashblock) = self.flashblocks.first() else {
            return false;
        };

        first_flashblock.metadata.block_number == first_replayed_block_number
            && first_flashblock.index == 0
            && first_flashblock.base.as_ref().map(|base| base.parent_hash) == Some(self.anchor_hash)
    }

    /// Compares rebuilt retained outputs against the immutable audit snapshot.
    pub fn compare_rebuilt_outputs(
        &self,
        rebuilt_outputs: &[RetainedFastOutput],
    ) -> PeriodicAuditResult {
        match self.rebuilt_output_mismatch(rebuilt_outputs) {
            None => PeriodicAuditResult::EquivalentPrefix,
            Some(RebuiltOutputMismatch::Length { expected_len, actual_len }) => {
                warn!(
                    message = "periodic audit rebuilt output mismatch",
                    audit_origin = ?PeriodicAuditMismatchOrigin::RebuiltOutputLength,
                    generation = self.generation,
                    window_id = self.window_id,
                    anchor_block_number = self.anchor_block_number,
                    anchor_hash = %self.anchor_hash,
                    audit_cursor = ?self.cursor,
                    expected_len,
                    rebuilt_len = actual_len,
                );
                PeriodicAuditResult::Diverged { failure: PeriodicAuditFailure::Mismatch }
            }
            Some(RebuiltOutputMismatch::Semantic(mismatch)) => {
                let expected_output = &self.expected_outputs[mismatch.index];
                let rebuilt_output = &rebuilt_outputs[mismatch.index];

                warn!(
                    message = "periodic audit rebuilt output mismatch",
                    audit_origin = ?PeriodicAuditMismatchOrigin::RebuiltOutputSemantic,
                    generation = self.generation,
                    window_id = self.window_id,
                    anchor_block_number = self.anchor_block_number,
                    anchor_hash = %self.anchor_hash,
                    audit_cursor = ?self.cursor,
                    output_index = mismatch.index,
                    mismatch_field = ?mismatch.field,
                    expected_cursor = ?mismatch.expected_cursor,
                    rebuilt_cursor = ?mismatch.actual_cursor,
                    expected_block_timestamp = expected_output.block_timestamp,
                    rebuilt_block_timestamp = rebuilt_output.block_timestamp,
                    expected_transactions = expected_output.transactions.len(),
                    rebuilt_transactions = rebuilt_output.transactions.len(),
                    expected_logs = expected_output.logs.len(),
                    rebuilt_logs = rebuilt_output.logs.len(),
                );
                PeriodicAuditResult::Diverged { failure: PeriodicAuditFailure::Mismatch }
            }
        }
    }

    fn rebuilt_output_mismatch(
        &self,
        rebuilt_outputs: &[RetainedFastOutput],
    ) -> Option<RebuiltOutputMismatch> {
        if self.expected_outputs.len() != rebuilt_outputs.len() {
            return Some(RebuiltOutputMismatch::Length {
                expected_len: self.expected_outputs.len(),
                actual_len: rebuilt_outputs.len(),
            });
        }

        RetainedFastOutput::first_mismatch(&self.expected_outputs, rebuilt_outputs)
            .map(RebuiltOutputMismatch::Semantic)
    }
}

/// Failure category for one periodic audit attempt.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PeriodicAuditFailure {
    /// Rebuilt outputs diverged from retained fast outputs.
    Mismatch,
    /// The audit worker exceeded its deadline.
    Timeout,
    /// A new audit was requested while an older one was still in flight.
    Overlap,
    /// The requested anchor no longer matched canonical state.
    AnchorMismatch,
    /// The audit worker failed before producing a semantic result.
    WorkerError,
}

/// Outcome category for one periodic audit attempt.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PeriodicAuditResult {
    /// The retained fast-output prefix was semantically equivalent.
    EquivalentPrefix,
    /// The audit finished with a non-equivalent or failed result.
    Diverged {
        /// Failure category for the divergence.
        failure: PeriodicAuditFailure,
    },
    /// The result belonged to an older generation or window and was ignored.
    StaleIgnored,
}

#[cfg(test)]
mod tests {
    use alloy_primitives::{Address, B256, Bytes};
    use alloy_rpc_types_engine::PayloadId;

    use super::{
        AuditCursor, AuditWindowSnapshot, PeriodicAuditFailure, PeriodicAuditOutputMismatch,
        PeriodicAuditOutputMismatchField, PeriodicAuditResult, RebuiltOutputMismatch,
        RetainedFastOutput,
    };
    use crate::{
        FastFlashblockLog, FastFlashblockLogsDelta, FastFlashblockTxMeta, FlashblockSnapshotId,
    };

    fn test_delta(nonce: u64) -> FastFlashblockLogsDelta {
        FastFlashblockLogsDelta::new(
            FlashblockSnapshotId::new(
                nonce,
                11,
                2,
                PayloadId::new([0x44; 8]),
                B256::with_last_byte(0x55),
            ),
            Some(1_700_000_011),
            vec![FastFlashblockLog {
                tx_hash: B256::with_last_byte(0x66),
                tx_index: 3,
                log_index_in_tx: 0,
                log_index_in_block: 7,
                address: Address::with_last_byte(0x77),
                topics: vec![B256::with_last_byte(0x88)],
                data: Bytes::from_static(&[0x99]),
            }],
            vec![FastFlashblockTxMeta {
                hash: B256::with_last_byte(0xaa),
                index: 3,
                status: Some(1),
            }],
        )
    }

    #[test]
    fn periodic_audit_equivalent_outputs_ignore_snapshot_nonce() {
        let first = RetainedFastOutput::from_delta(&test_delta(1));
        let second = RetainedFastOutput::from_delta(&test_delta(99));

        assert_eq!(
            first.cursor,
            AuditCursor::new(11, 2, PayloadId::new([0x44; 8]), B256::with_last_byte(0x55))
        );
        assert_eq!(first, second);
        assert!(first.semantically_matches(&second));
    }

    #[test]
    fn retained_fast_output_semantic_comparison_detects_cursor_transaction_and_log_mismatch() {
        let expected = RetainedFastOutput::from_delta(&test_delta(1));

        let mut cursor_mismatch = expected.clone();
        cursor_mismatch.cursor = AuditCursor::new(
            expected.cursor.block_number,
            expected.cursor.flashblock_index.saturating_add(1),
            expected.cursor.payload_id,
            expected.cursor.parent_hash,
        );
        assert_ne!(expected, cursor_mismatch);

        let mut transaction_mismatch = expected.clone();
        transaction_mismatch.transactions[0].status = Some(0);
        assert_ne!(expected, transaction_mismatch);

        let mut log_mismatch = expected.clone();
        log_mismatch.logs[0].log_index_in_block =
            log_mismatch.logs[0].log_index_in_block.saturating_add(1);
        assert_ne!(expected, log_mismatch);
    }

    #[test]
    fn periodic_audit_output_mismatch_diverges() {
        let expected = RetainedFastOutput::from_delta(&test_delta(1));
        let mut rebuilt = RetainedFastOutput::from_delta(&test_delta(2));
        let snapshot = AuditWindowSnapshot::new(
            7,
            9,
            10,
            B256::with_last_byte(0x33),
            expected.cursor,
            vec![],
            vec![expected],
        );

        rebuilt.logs[0].log_index_in_block = rebuilt.logs[0].log_index_in_block.saturating_add(1);

        assert_eq!(
            snapshot.compare_rebuilt_outputs(&[rebuilt]),
            PeriodicAuditResult::Diverged { failure: PeriodicAuditFailure::Mismatch }
        );
    }

    #[test]
    fn periodic_audit_rebuilt_output_length_mismatch_is_classified() {
        let expected = RetainedFastOutput::from_delta(&test_delta(1));
        let snapshot = AuditWindowSnapshot::new(
            7,
            9,
            10,
            B256::with_last_byte(0x33),
            expected.cursor,
            vec![],
            vec![expected],
        );

        assert_eq!(
            snapshot.rebuilt_output_mismatch(&[]),
            Some(RebuiltOutputMismatch::Length { expected_len: 1, actual_len: 0 })
        );
    }

    #[test]
    fn periodic_audit_rebuilt_output_semantic_mismatch_reports_first_field() {
        let expected = RetainedFastOutput::from_delta(&test_delta(1));
        let mut rebuilt = expected.clone();
        rebuilt.block_timestamp = Some(1_700_000_099);

        assert_eq!(
            RetainedFastOutput::first_mismatch(
                std::slice::from_ref(&expected),
                std::slice::from_ref(&rebuilt),
            ),
            Some(PeriodicAuditOutputMismatch {
                index: 0,
                field: PeriodicAuditOutputMismatchField::BlockTimestamp,
                expected_cursor: expected.cursor,
                actual_cursor: expected.cursor,
            })
        );
    }
}
