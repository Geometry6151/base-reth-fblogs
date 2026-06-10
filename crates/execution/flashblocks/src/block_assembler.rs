//! Block assembly from flashblocks.
//!
//! This module provides the [`BlockAssembler`] which reconstructs blocks from flashblocks.

use alloy_consensus::{Header, Sealed};
use alloy_eips::{Decodable2718, Encodable2718, eip7685::EMPTY_REQUESTS_HASH};
use alloy_primitives::{B256, Bytes};
use alloy_rpc_types::Withdrawal;
use alloy_rpc_types_engine::{
    CancunPayloadFields, ExecutionPayloadV1, ExecutionPayloadV2, ExecutionPayloadV3,
    PraguePayloadFields,
};
use base_common_consensus::{BaseBlock, BaseTxEnvelope};
use base_common_evm::L1BlockInfo;
use base_common_flashblocks::{
    ExecutionPayloadBaseV1, ExecutionPayloadFlashblockDeltaV1, Flashblock,
};
use base_common_rpc_types_engine::{
    BaseExecutionPayload, BaseExecutionPayloadSidecar, BaseExecutionPayloadV4,
};

use crate::{ExecutionError, ProtocolError, Result};

/// Result of assembling a block from flashblocks.
#[derive(Debug, Clone)]
pub struct AssembledBlock {
    /// The reconstructed Base block.
    pub block: BaseBlock,
    /// The base payload data from the first flashblock.
    pub base: ExecutionPayloadBaseV1,
    /// The flashblocks used to assemble this block.
    pub flashblocks: Vec<Flashblock>,
    /// The sealed header for this block.
    pub header: Sealed<Header>,
}

impl AssembledBlock {
    /// Extracts L1 block info from the assembled block's body.
    ///
    /// This extracts the L1 attributes deposited transaction data from the
    /// block body, which contains information about the L1 origin.
    pub fn l1_block_info(&self) -> Result<L1BlockInfo> {
        base_execution_evm::extract_l1_info(&self.block.body)
            .map_err(|e| ExecutionError::L1BlockInfo(e.to_string()).into())
    }
}

/// Assembles blocks from flashblocks.
///
/// This component handles the reconstruction of complete blocks from
/// a sequence of flashblocks, extracting transactions, withdrawals,
/// and building the execution payload.
#[derive(Debug, Default)]
pub struct BlockAssembler;

impl BlockAssembler {
    /// Creates a new block assembler.
    pub const fn new() -> Self {
        Self
    }

    /// Returns the base payload from the first flashblock in a sequence.
    pub fn base_from_first_flashblock(flashblock: &Flashblock) -> Result<ExecutionPayloadBaseV1> {
        flashblock.base.clone().ok_or(ProtocolError::MissingBase.into())
    }

    /// Decodes only the transactions present in the provided flashblock suffix.
    pub fn decode_flashblock_transactions(flashblock: &Flashblock) -> Result<Vec<BaseTxEnvelope>> {
        flashblock
            .diff
            .transactions
            .iter()
            .map(|transaction| {
                BaseTxEnvelope::decode_2718_exact(transaction.as_ref())
                    .map_err(|error| ExecutionError::BlockConversion(error.to_string()).into())
            })
            .collect()
    }

    /// Builds a synthetic execution block from base metadata plus one flashblock suffix.
    pub fn execution_block_from_base_and_suffix(
        base: &ExecutionPayloadBaseV1,
        flashblock: &Flashblock,
        transactions: Vec<BaseTxEnvelope>,
    ) -> Result<BaseBlock> {
        Self::execution_block_from_parts(
            base,
            &flashblock.diff,
            flashblock.diff.withdrawals.clone(),
            transactions.into_iter().map(|transaction| transaction.encoded_2718().into()).collect(),
        )
    }

    /// Assembles a complete block from a slice of flashblocks.
    ///
    /// # Arguments
    /// * `flashblocks` - A slice of flashblocks for a single block number.
    ///
    /// # Returns
    /// An [`AssembledBlock`] containing the reconstructed block and metadata.
    ///
    /// # Errors
    /// Returns an error if:
    /// - The flashblocks slice is empty
    /// - The first flashblock is missing its base payload
    /// - Block conversion fails
    pub fn assemble(flashblocks: &[Flashblock]) -> Result<AssembledBlock> {
        let first = flashblocks.first().ok_or(ProtocolError::EmptyFlashblocks)?;
        let base = Self::base_from_first_flashblock(first)?;
        let latest_flashblock = flashblocks.last().ok_or(ProtocolError::EmptyFlashblocks)?;

        let transactions: Vec<Bytes> = flashblocks
            .iter()
            .flat_map(|flashblock| flashblock.diff.transactions.clone())
            .collect();

        let withdrawals: Vec<Withdrawal> =
            flashblocks.iter().flat_map(|flashblock| flashblock.diff.withdrawals.clone()).collect();

        let block = Self::execution_block_from_parts(
            &base,
            &latest_flashblock.diff,
            withdrawals,
            transactions,
        )?;

        // Zero block hash for flashblocks since the final hash isn't known yet
        let sealed_header = block.header.clone().seal(B256::ZERO);

        Ok(AssembledBlock { block, base, flashblocks: flashblocks.to_vec(), header: sealed_header })
    }

    fn execution_block_from_parts(
        base: &ExecutionPayloadBaseV1,
        diff: &ExecutionPayloadFlashblockDeltaV1,
        withdrawals: Vec<Withdrawal>,
        transactions: Vec<Bytes>,
    ) -> Result<BaseBlock> {
        let execution_payload = BaseExecutionPayloadV4 {
            payload_inner: ExecutionPayloadV3 {
                blob_gas_used: diff.blob_gas_used.unwrap_or_default(),
                excess_blob_gas: 0,
                payload_inner: ExecutionPayloadV2 {
                    withdrawals,
                    payload_inner: ExecutionPayloadV1 {
                        parent_hash: base.parent_hash,
                        fee_recipient: base.fee_recipient,
                        state_root: diff.state_root,
                        receipts_root: diff.receipts_root,
                        logs_bloom: diff.logs_bloom,
                        prev_randao: base.prev_randao,
                        block_number: base.block_number,
                        gas_limit: base.gas_limit,
                        gas_used: diff.gas_used,
                        timestamp: base.timestamp,
                        extra_data: base.extra_data.clone(),
                        base_fee_per_gas: base.base_fee_per_gas,
                        block_hash: diff.block_hash,
                        transactions,
                    },
                },
            },
            withdrawals_root: diff.withdrawals_root,
        };

        let sidecar = BaseExecutionPayloadSidecar::v4(
            CancunPayloadFields {
                parent_beacon_block_root: base.parent_beacon_block_root,
                versioned_hashes: vec![],
            },
            PraguePayloadFields::new(EMPTY_REQUESTS_HASH),
        );

        BaseExecutionPayload::V4(execution_payload)
            .try_into_block_with_sidecar(&sidecar)
            .map_err(|error| ExecutionError::BlockConversion(error.to_string()).into())
    }
}

#[cfg(test)]
mod tests {
    use alloy_consensus::{Signed, TxLegacy};
    use alloy_eips::Encodable2718;
    use alloy_primitives::{Address, Bloom, U256};
    use alloy_rpc_types_engine::PayloadId;
    use base_common_consensus::BaseTxEnvelope;
    use base_common_flashblocks::{
        ExecutionPayloadBaseV1, ExecutionPayloadFlashblockDeltaV1, Metadata,
    };

    use super::*;
    use crate::{ExecutionError, ProtocolError};

    fn create_test_flashblock(index: u64, with_base: bool) -> Flashblock {
        Flashblock {
            payload_id: PayloadId::default(),
            index,
            base: if with_base {
                Some(ExecutionPayloadBaseV1 {
                    parent_beacon_block_root: B256::ZERO,
                    parent_hash: B256::ZERO,
                    fee_recipient: Address::ZERO,
                    prev_randao: B256::ZERO,
                    block_number: 100,
                    gas_limit: 30_000_000,
                    timestamp: 1700000000,
                    extra_data: Bytes::default(),
                    base_fee_per_gas: U256::from(1000000000u64),
                })
            } else {
                None
            },
            diff: ExecutionPayloadFlashblockDeltaV1 {
                state_root: B256::ZERO,
                receipts_root: B256::ZERO,
                logs_bloom: Bloom::default(),
                gas_used: 21000,
                block_hash: B256::ZERO,
                transactions: vec![],
                withdrawals: vec![],
                withdrawals_root: B256::ZERO,
                blob_gas_used: None,
            },
            metadata: Metadata { block_number: 100 },
        }
    }

    fn create_encoded_legacy_tx() -> Bytes {
        let tx = TxLegacy {
            chain_id: Some(8453),
            nonce: 0,
            gas_price: 1_000_000_000,
            gas_limit: 21_000,
            to: alloy_primitives::TxKind::Call(Address::ZERO),
            value: U256::ZERO,
            input: Default::default(),
        };
        let envelope = BaseTxEnvelope::Legacy(Signed::new_unchecked(
            tx,
            alloy_primitives::Signature::test_signature(),
            B256::ZERO,
        ));

        envelope.encoded_2718().into()
    }

    #[test]
    fn test_assemble_single_flashblock() {
        let flashblocks = vec![create_test_flashblock(0, true)];

        let result = BlockAssembler::assemble(&flashblocks);
        assert!(result.is_ok());

        let assembled = result.unwrap();
        assert_eq!(assembled.base.block_number, 100);
        assert_eq!(assembled.flashblocks.len(), 1);
    }

    #[test]
    fn test_assemble_multiple_flashblocks() {
        let flashblocks = vec![
            create_test_flashblock(0, true),
            create_test_flashblock(1, false),
            create_test_flashblock(2, false),
        ];

        let result = BlockAssembler::assemble(&flashblocks);
        assert!(result.is_ok());

        let assembled = result.unwrap();
        assert_eq!(assembled.flashblocks.len(), 3);
    }

    #[test]
    fn test_assemble_propagates_blob_gas_used_from_latest_flashblock() {
        let mut fb0 = create_test_flashblock(0, true);
        fb0.diff.blob_gas_used = Some(10);

        let mut fb1 = create_test_flashblock(1, false);
        fb1.diff.blob_gas_used = Some(42_000);

        let assembled = BlockAssembler::assemble(&[fb0, fb1]).unwrap();
        assert_eq!(assembled.block.header.blob_gas_used, Some(42_000));
    }

    #[test]
    fn test_assemble_empty_flashblocks_fails() {
        let flashblocks: Vec<Flashblock> = vec![];
        let result = BlockAssembler::assemble(&flashblocks);
        assert!(matches!(
            result,
            Err(crate::StateProcessorError::Protocol(ProtocolError::EmptyFlashblocks))
        ));
    }

    #[test]
    fn test_assemble_missing_base_fails() {
        let flashblocks = vec![create_test_flashblock(0, false)]; // No base

        let result = BlockAssembler::assemble(&flashblocks);
        assert!(matches!(
            result,
            Err(crate::StateProcessorError::Protocol(ProtocolError::MissingBase))
        ));
    }

    #[test]
    fn test_base_from_first_flashblock_missing_base_fails() {
        let flashblock = create_test_flashblock(0, false);

        let result = BlockAssembler::base_from_first_flashblock(&flashblock);

        assert!(matches!(
            result,
            Err(crate::StateProcessorError::Protocol(ProtocolError::MissingBase))
        ));
    }

    #[test]
    fn test_decode_flashblock_transactions_rejects_malformed_transaction() {
        let mut flashblock = create_test_flashblock(0, true);
        flashblock.diff.transactions = vec![Bytes::from_static(&[0x03])];

        let result = BlockAssembler::decode_flashblock_transactions(&flashblock);

        assert!(matches!(
            result,
            Err(crate::StateProcessorError::Execution(ExecutionError::BlockConversion(_)))
        ));
    }

    #[test]
    fn test_decode_flashblock_transactions_decodes_only_current_flashblock_suffix() {
        let mut flashblock = create_test_flashblock(7, true);
        flashblock.diff.transactions = vec![create_encoded_legacy_tx()];

        let transactions = BlockAssembler::decode_flashblock_transactions(&flashblock).unwrap();

        assert_eq!(transactions.len(), 1);
    }
}
