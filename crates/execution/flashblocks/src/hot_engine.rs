//! Suffix-only hot engine for speed-first flashblock logs.

use alloy_consensus::{
    Header, Sealed, TxReceipt,
    transaction::{Recovered, SignerRecoverable},
};
use alloy_eips::BlockNumberOrTag;
use alloy_primitives::B256;
use alloy_rpc_types::state::StateOverride;
use base_common_chains::Upgrades;
use base_common_consensus::{BaseBlock, BaseTxEnvelope};
use base_common_evm::L1BlockInfo;
use base_common_flashblocks::{ExecutionPayloadBaseV1, Flashblock};
use base_execution_evm::BaseEvmConfig;
use reth_chainspec::{ChainSpecProvider, EthChainSpec};
use reth_evm::ConfigureEvm;
use reth_primitives::RecoveredBlock;
use reth_provider::{BlockReaderIdExt, StateProviderFactory};
use reth_revm::{State, database::StateProviderDatabase};
use reth_storage_api::StateProviderBox;
use revm_database::states::bundle_state::BundleRetention;

use crate::{
    BlockAssembler, ExecutionError, FastFlashblockLog, FastFlashblockLogsDelta,
    FastFlashblockTxMeta, FlashblockSequenceValidator, FlashblockSnapshotId, HotExecutionState,
    HotPendingBlock, HotPendingWindow, HotSnapshot, Metrics, PendingStateBuilder, ProviderError,
    Result, SequenceValidationResult, StateProcessorError,
};

/// Concrete DB state carried by the hot engine across pending flashblocks.
pub type HotExecutionDb = State<StateProviderDatabase<StateProviderBox>>;

/// Result of applying one flashblock to the hot engine.
#[derive(Debug)]
pub enum HotApplyOutcome {
    /// A new delta and optional pinned snapshot were produced.
    Delta {
        /// Delta emitted for the applied flashblock.
        delta: FastFlashblockLogsDelta,
        /// Optional pinned snapshot derived from the same post-apply state.
        snapshot: Option<HotSnapshot>,
    },
    /// The flashblock was already applied.
    Duplicate,
    /// The hot engine requires a downstream reset/resync.
    Reset,
}

/// Append-only exact-log engine.
#[derive(Debug)]
pub struct HotEngine<Client> {
    /// Backing client used for provider, chain spec, and canonical block access.
    pub client: Client,
    /// Maximum number of pending blocks retained in the hot window.
    pub max_depth: u64,
    /// Current append-only pending window.
    pub window: HotPendingWindow<HotExecutionDb>,
    /// Monotonic local nonce for future snapshot identifiers.
    pub next_snapshot_nonce: u64,
}

impl<Client> HotEngine<Client>
where
    Client: StateProviderFactory
        + ChainSpecProvider<ChainSpec: EthChainSpec<Header = Header> + Upgrades>
        + BlockReaderIdExt<Header = Header>
        + Clone
        + 'static,
{
    /// Creates a new empty hot engine shell.
    pub fn new(client: Client, max_depth: u64) -> Self {
        Self {
            client,
            max_depth,
            window: HotPendingWindow::new(max_depth as usize),
            next_snapshot_nonce: 0,
        }
    }

    /// Applies one flashblock to the hot engine.
    pub fn apply_flashblock(&mut self, flashblock: &Flashblock) -> Result<HotApplyOutcome> {
        if self.window.execution.is_none() || self.window.blocks.is_empty() {
            self.window.reset();
            return if flashblock.index == 0 {
                self.start_first_flashblock(flashblock)
            } else {
                Metrics::hot_window_reset_non_zero_first_index_count().increment(1);
                Ok(self.reset_for_flashblock())
            };
        }

        let active_block_number = self.window.active_block_number().ok_or_else(|| {
            StateProcessorError::HotEngine("missing active block for hot window".to_string())
        })?;
        let latest_flashblock_index = self.window.latest_flashblock_index().ok_or_else(|| {
            StateProcessorError::HotEngine(
                "missing latest flashblock index for hot window".to_string(),
            )
        })?;

        match FlashblockSequenceValidator::validate(
            active_block_number,
            latest_flashblock_index,
            flashblock.metadata.block_number,
            flashblock.index,
        ) {
            SequenceValidationResult::NextInSequence => self.append_same_block_suffix(flashblock),
            SequenceValidationResult::FirstOfNextBlock => self.rollover_to_next_block(flashblock),
            SequenceValidationResult::Duplicate => Ok(HotApplyOutcome::Duplicate),
            SequenceValidationResult::NonSequentialGap { .. } => {
                Metrics::hot_window_reset_sequence_gap_count().increment(1);
                Ok(self.reset_for_flashblock())
            }
            SequenceValidationResult::InvalidNewBlockIndex { .. } => {
                Metrics::hot_window_reset_invalid_new_block_index_count().increment(1);
                Ok(self.reset_for_flashblock())
            }
        }
    }

    /// Processes one new canonical block against the hot engine.
    pub fn process_canonical_block(
        &mut self,
        block: &RecoveredBlock<BaseBlock>,
    ) -> Result<HotApplyOutcome> {
        if self.window.execution.is_none() || self.window.blocks.is_empty() {
            return Ok(HotApplyOutcome::Duplicate);
        }

        let Some(oldest_pending_block) = self.window.blocks.front() else {
            return Ok(HotApplyOutcome::Duplicate);
        };
        let Some(latest_pending_block) = self.window.blocks.back() else {
            return Ok(HotApplyOutcome::Duplicate);
        };

        if block.number >= latest_pending_block.block_number {
            return Ok(self.reset_for_canonical());
        }

        if block.number > oldest_pending_block.block_number {
            return Ok(self.reset_for_canonical());
        }

        if block.number == oldest_pending_block.block_number {
            if !Self::canonical_block_matches_pending_block(oldest_pending_block, block) {
                return Ok(self.reset_for_canonical());
            }

            let retained_flashblocks = Self::retained_flashblocks(
                self.window
                    .blocks
                    .iter()
                    .filter(|pending_block| pending_block.block_number > block.number),
            );

            return Ok(self.replay_retained_flashblocks_from_canonical(retained_flashblocks));
        }

        if block.number + 1 == oldest_pending_block.block_number
            && block.header().hash_slow() != oldest_pending_block.parent_hash
        {
            return Ok(self.reset_for_canonical());
        }

        let retained_flashblocks = Self::retained_flashblocks(self.window.blocks.iter());
        Ok(self.replay_retained_flashblocks_from_canonical(retained_flashblocks))
    }

    /// Resets the current pending hot window.
    pub fn reset(&mut self) {
        self.window.reset();
    }

    fn next_snapshot_nonce(&mut self) -> u64 {
        self.next_snapshot_nonce = self.next_snapshot_nonce.saturating_add(1);
        self.next_snapshot_nonce
    }

    fn reset_for_flashblock(&mut self) -> HotApplyOutcome {
        Metrics::hot_window_reset_count().increment(1);
        self.reset();
        HotApplyOutcome::Reset
    }

    fn reset_for_canonical(&mut self) -> HotApplyOutcome {
        Metrics::hot_canonical_reset_count().increment(1);
        self.reset();
        HotApplyOutcome::Reset
    }

    fn replay_retained_flashblocks_from_canonical(
        &mut self,
        flashblocks: Vec<Flashblock>,
    ) -> HotApplyOutcome {
        if flashblocks.is_empty() {
            self.reset();
            return HotApplyOutcome::Duplicate;
        }

        if self.silently_replay_flashblocks(flashblocks) {
            HotApplyOutcome::Duplicate
        } else {
            self.reset_for_canonical()
        }
    }

    fn silently_replay_flashblocks(&mut self, flashblocks: Vec<Flashblock>) -> bool {
        let snapshot_nonce = self.next_snapshot_nonce;
        self.window.reset();

        for flashblock in flashblocks {
            let outcome = match self.apply_flashblock(&flashblock) {
                Ok(outcome) => outcome,
                Err(_) => {
                    self.next_snapshot_nonce = snapshot_nonce;
                    self.window.reset();
                    return false;
                }
            };

            if !matches!(outcome, HotApplyOutcome::Delta { .. } | HotApplyOutcome::Duplicate) {
                self.next_snapshot_nonce = snapshot_nonce;
                self.window.reset();
                return false;
            }
        }

        self.next_snapshot_nonce = snapshot_nonce;
        true
    }

    fn retained_flashblocks<'a>(
        blocks: impl Iterator<Item = &'a HotPendingBlock>,
    ) -> Vec<Flashblock> {
        blocks.flat_map(|block| block.flashblocks.iter().cloned()).collect()
    }

    fn start_first_flashblock(&mut self, flashblock: &Flashblock) -> Result<HotApplyOutcome> {
        let base = BlockAssembler::base_from_first_flashblock(flashblock)?;
        let canonical_block_number =
            flashblock.metadata.block_number.checked_sub(1).ok_or_else(|| {
                StateProcessorError::HotEngine(
                    "cannot start hot window before canonical genesis anchor".to_string(),
                )
            })?;

        let canonical_header = self
            .client
            .header_by_number(canonical_block_number)
            .map_err(|error| ProviderError::StateProvider(error.to_string()))?
            .ok_or(ProviderError::MissingCanonicalHeader {
                block_number: canonical_block_number,
            })?;
        let canonical_parent_hash = canonical_header.hash_slow();

        if base.parent_hash != canonical_parent_hash {
            Metrics::hot_window_reset_first_parent_mismatch_count().increment(1);
            return Ok(self.reset_for_flashblock());
        }
        let base_parent_hash = base.parent_hash;

        let state_provider = self
            .client
            .state_by_block_number_or_tag(BlockNumberOrTag::Number(canonical_block_number))
            .map_err(|error| ProviderError::StateProvider(error.to_string()))?;

        let execution = HotExecutionState {
            db: State::builder()
                .with_database(StateProviderDatabase::new(state_provider))
                .with_bundle_update()
                .build(),
            last_header: Self::seal_header(canonical_header),
            state_overrides: StateOverride::default(),
            l1_block_info: L1BlockInfo::default(),
        };

        let (execution, pending_block, delta, snapshot) = self.execute_new_block_suffix(
            flashblock,
            execution,
            base,
            canonical_parent_hash,
            base_parent_hash,
        )?;

        self.window.canonical_base_parent_hash = base_parent_hash;
        self.window.execution = Some(execution);
        self.window.push_block(pending_block);

        Ok(HotApplyOutcome::Delta { delta, snapshot: Some(snapshot) })
    }

    fn append_same_block_suffix(&mut self, flashblock: &Flashblock) -> Result<HotApplyOutcome> {
        let Some(active_block) = self.window.active_block() else {
            Metrics::hot_window_reset_missing_active_block_count().increment(1);
            return Ok(self.reset_for_flashblock());
        };

        if flashblock.base.as_ref().is_some_and(|base| base.parent_hash != active_block.parent_hash)
        {
            Metrics::hot_window_reset_same_block_parent_mismatch_count().increment(1);
            return Ok(self.reset_for_flashblock());
        }

        let canonical_base_parent_hash = self.window.canonical_base_parent_hash;
        let execution = self.window.execution.take().ok_or_else(|| {
            StateProcessorError::HotEngine("missing hot execution state".to_string())
        })?;
        let pending_block = self.window.blocks.pop_back().ok_or_else(|| {
            StateProcessorError::HotEngine("missing active pending block".to_string())
        })?;

        let carried_l1_block_info = execution.l1_block_info.clone();
        let base = pending_block.base.clone();
        let (execution_block, decoded_transactions) =
            Self::prepare_suffix_block(&base, flashblock)?;

        let (execution, pending_block, delta, snapshot) = self.execute_suffix(
            flashblock,
            execution,
            pending_block,
            execution_block,
            decoded_transactions,
            carried_l1_block_info,
            false,
            B256::ZERO,
            canonical_base_parent_hash,
        )?;

        self.window.execution = Some(execution);
        self.window.blocks.push_back(pending_block);

        Ok(HotApplyOutcome::Delta { delta, snapshot: Some(snapshot) })
    }

    fn rollover_to_next_block(&mut self, flashblock: &Flashblock) -> Result<HotApplyOutcome> {
        let _rollover_timer = base_metrics::timed!(Metrics::hot_window_rollover_duration());

        let base = BlockAssembler::base_from_first_flashblock(flashblock)?;
        let canonical_base_parent_hash = self.window.canonical_base_parent_hash;
        let execution = self.window.execution.take().ok_or_else(|| {
            StateProcessorError::HotEngine("missing hot execution state".to_string())
        })?;
        let expected_parent_hash = execution.last_header.hash();

        if base.parent_hash != expected_parent_hash {
            Metrics::hot_window_reset_rollover_parent_mismatch_count().increment(1);
            return Ok(self.reset_for_flashblock());
        }

        let (execution_block, decoded_transactions) =
            Self::prepare_suffix_block(&base, flashblock)?;
        let l1_block_info = Self::extract_l1_block_info(&execution_block)?;
        let pending_block = HotPendingBlock {
            block_number: flashblock.metadata.block_number,
            payload_id: flashblock.payload_id,
            base: base.clone(),
            parent_hash: base.parent_hash,
            latest_flashblock_index: 0,
            latest_header: Self::seal_header(execution_block.header.clone()),
            next_tx_index: 0,
            next_log_index: 0,
            cumulative_gas_used: 0,
            transactions: vec![],
            logs: vec![],
            receipts: std::collections::HashMap::new(),
            rpc_transactions: std::collections::HashMap::new(),
            transaction_senders: std::collections::HashMap::new(),
            flashblocks: vec![],
        };

        let (execution, pending_block, delta, snapshot) = self.execute_suffix(
            flashblock,
            execution,
            pending_block,
            execution_block,
            decoded_transactions,
            l1_block_info,
            true,
            expected_parent_hash,
            canonical_base_parent_hash,
        )?;

        self.window.execution = Some(execution);
        self.window.push_block(pending_block);

        Ok(HotApplyOutcome::Delta { delta, snapshot: Some(snapshot) })
    }

    fn execute_new_block_suffix(
        &mut self,
        flashblock: &Flashblock,
        execution: HotExecutionState<HotExecutionDb>,
        base: ExecutionPayloadBaseV1,
        pre_execution_parent_hash: B256,
        canonical_base_parent_hash: B256,
    ) -> Result<(
        HotExecutionState<HotExecutionDb>,
        HotPendingBlock,
        FastFlashblockLogsDelta,
        HotSnapshot,
    )> {
        let (execution_block, decoded_transactions) =
            Self::prepare_suffix_block(&base, flashblock)?;
        let l1_block_info = Self::extract_l1_block_info(&execution_block)?;
        let pending_block = HotPendingBlock {
            block_number: flashblock.metadata.block_number,
            payload_id: flashblock.payload_id,
            base: base.clone(),
            parent_hash: base.parent_hash,
            latest_flashblock_index: 0,
            latest_header: Self::seal_header(execution_block.header.clone()),
            next_tx_index: 0,
            next_log_index: 0,
            cumulative_gas_used: 0,
            transactions: vec![],
            logs: vec![],
            receipts: std::collections::HashMap::new(),
            rpc_transactions: std::collections::HashMap::new(),
            transaction_senders: std::collections::HashMap::new(),
            flashblocks: vec![],
        };

        self.execute_suffix(
            flashblock,
            execution,
            pending_block,
            execution_block,
            decoded_transactions,
            l1_block_info,
            true,
            pre_execution_parent_hash,
            canonical_base_parent_hash,
        )
    }

    fn execute_suffix(
        &mut self,
        flashblock: &Flashblock,
        mut execution: HotExecutionState<HotExecutionDb>,
        mut pending_block: HotPendingBlock,
        execution_block: BaseBlock,
        decoded_transactions: Vec<BaseTxEnvelope>,
        l1_block_info: L1BlockInfo,
        apply_pre_execution_changes: bool,
        pre_execution_parent_hash: B256,
        canonical_base_parent_hash: B256,
    ) -> Result<(
        HotExecutionState<HotExecutionDb>,
        HotPendingBlock,
        FastFlashblockLogsDelta,
        HotSnapshot,
    )> {
        let evm_config = BaseEvmConfig::base(self.client.chain_spec());
        let suffix_header = execution_block.header.clone();
        let block_timestamp = Some(suffix_header.timestamp);
        let start_tx_index = pending_block.next_tx_index;
        let start_log_index = pending_block.next_log_index;
        let mut cumulative_gas_used = pending_block.cumulative_gas_used;
        let mut next_log_index = start_log_index;
        let tx_count = decoded_transactions.len();
        let next_log_index_usize = Self::usize_from_u64(start_log_index, "next_log_index")?;
        let evm_env = evm_config
            .evm_env(&suffix_header)
            .map_err(|error| ExecutionError::EvmEnv(error.to_string()))?;
        let evm = evm_config.evm_with_env(execution.db, evm_env);
        let mut pending_state_builder = PendingStateBuilder::new_with_cursors(
            self.client.chain_spec(),
            evm,
            execution_block,
            None,
            l1_block_info.clone(),
            execution.state_overrides,
            cumulative_gas_used,
            next_log_index_usize,
        );

        if apply_pre_execution_changes {
            pending_state_builder.apply_pre_execution_changes(
                pre_execution_parent_hash,
                Some(pending_block.base.parent_beacon_block_root),
            )?;
        }

        Metrics::hot_suffix_tx_count().record(tx_count as f64);
        let mut executed_suffix = Vec::with_capacity(tx_count);
        {
            let _suffix_execute_timer =
                base_metrics::timed!(Metrics::hot_suffix_execute_duration());

            for (offset, transaction) in decoded_transactions.into_iter().enumerate() {
                let sender = transaction.recover_signer()?;
                let tx_hash = transaction.tx_hash();
                let tx_index = start_tx_index.saturating_add(offset as u64);
                let tx_index_usize = Self::usize_from_u64(tx_index, "tx_index")?;
                let recovered_transaction = Recovered::new_unchecked(transaction, sender);
                let executed_transaction = pending_state_builder
                    .execute_new_transaction(tx_index_usize, recovered_transaction)?;

                executed_suffix.push((
                    tx_index,
                    tx_hash,
                    sender,
                    executed_transaction.rpc_transaction,
                    executed_transaction.receipt,
                ));
            }
        }

        let (mut db, state_overrides) = pending_state_builder.into_db_and_state_overrides();
        db.merge_transitions(BundleRetention::Reverts);

        let delta = {
            let _delta_build_timer = base_metrics::timed!(Metrics::hot_delta_build_duration());
            let mut delta_transactions = Vec::with_capacity(executed_suffix.len());
            let mut delta_logs = Vec::new();

            for (tx_index, tx_hash, sender, rpc_transaction, receipt) in executed_suffix {
                cumulative_gas_used = receipt.inner.inner.cumulative_gas_used();

                let tx_meta = FastFlashblockTxMeta {
                    hash: tx_hash,
                    index: tx_index,
                    status: Some(if receipt.inner.inner.status() { 1 } else { 0 }),
                };
                delta_transactions.push(tx_meta.clone());
                pending_block.transactions.push(tx_meta);

                pending_block.receipts.insert(tx_hash, receipt.clone());
                pending_block.rpc_transactions.insert(tx_hash, rpc_transaction.clone());
                pending_block.transaction_senders.insert(tx_hash, sender);

                for (log_index_in_tx, log) in receipt.inner.logs().iter().enumerate() {
                    let log_index_in_block = log.log_index.unwrap_or(next_log_index);
                    next_log_index = next_log_index.max(log_index_in_block.saturating_add(1));

                    let fast_log = FastFlashblockLog {
                        tx_hash,
                        tx_index,
                        log_index_in_tx: log_index_in_tx as u64,
                        log_index_in_block,
                        address: log.inner.address,
                        topics: log.inner.data.topics().to_vec(),
                        data: log.inner.data.data.clone(),
                    };

                    delta_logs.push(fast_log.clone());
                    pending_block.logs.push(fast_log);
                }
            }

            let snapshot_id = FlashblockSnapshotId::new(
                self.next_snapshot_nonce(),
                pending_block.block_number,
                flashblock.index,
                flashblock.payload_id,
                pending_block.parent_hash,
            );

            FastFlashblockLogsDelta::new(
                snapshot_id,
                block_timestamp,
                delta_logs,
                delta_transactions,
            )
        };

        pending_block.payload_id = flashblock.payload_id;
        pending_block.latest_flashblock_index = flashblock.index;
        pending_block.next_tx_index = start_tx_index.saturating_add(tx_count as u64);
        pending_block.next_log_index = next_log_index;
        pending_block.cumulative_gas_used = cumulative_gas_used;
        pending_block.flashblocks.push(flashblock.clone());
        let latest_header = Self::assembled_pending_header(&pending_block)?;
        pending_block.latest_header = latest_header.clone();

        execution.db = db;
        execution.last_header = latest_header.clone();
        execution.state_overrides = state_overrides;
        execution.l1_block_info = l1_block_info;

        let snapshot = {
            let _snapshot_materialize_timer =
                base_metrics::timed!(Metrics::hot_snapshot_materialize_duration());
            HotSnapshot::new(
                delta.snapshot_id,
                canonical_base_parent_hash,
                latest_header,
                execution.state_overrides.clone(),
            )
        };

        Ok((execution, pending_block, delta, snapshot))
    }

    fn prepare_suffix_block(
        base: &ExecutionPayloadBaseV1,
        flashblock: &Flashblock,
    ) -> Result<(BaseBlock, Vec<BaseTxEnvelope>)> {
        let decoded_transactions = BlockAssembler::decode_flashblock_transactions(flashblock)?;
        let execution_block = BlockAssembler::execution_block_from_base_and_suffix(
            base,
            flashblock,
            decoded_transactions.clone(),
        )?;

        Ok((execution_block, decoded_transactions))
    }

    fn extract_l1_block_info(block: &BaseBlock) -> Result<L1BlockInfo> {
        base_execution_evm::extract_l1_info(&block.body)
            .map_err(|error| ExecutionError::L1BlockInfo(error.to_string()).into())
    }

    fn canonical_block_matches_pending_block(
        pending_block: &HotPendingBlock,
        block: &RecoveredBlock<BaseBlock>,
    ) -> bool {
        let Ok(assembled_pending_block) = BlockAssembler::assemble(&pending_block.flashblocks)
        else {
            return false;
        };
        let assembled_header = &assembled_pending_block.block.header;

        if block.header().parent_hash != assembled_header.parent_hash {
            return false;
        }

        if block.header().hash_slow() != assembled_header.hash_slow() {
            return false;
        }

        let pending_tx_hashes = assembled_pending_block
            .block
            .body
            .transactions
            .iter()
            .map(|transaction| transaction.tx_hash())
            .collect::<Vec<_>>();
        let canonical_tx_hashes =
            block.body().transactions().map(|tx| tx.tx_hash()).collect::<Vec<_>>();

        pending_tx_hashes == canonical_tx_hashes
    }

    fn seal_header(header: Header) -> Sealed<Header> {
        let hash = header.hash_slow();
        Sealed::new_unchecked(header, hash)
    }

    fn assembled_pending_header(pending_block: &HotPendingBlock) -> Result<Sealed<Header>> {
        let assembled = BlockAssembler::assemble(&pending_block.flashblocks)?;
        Ok(Self::seal_header(assembled.block.header))
    }

    fn usize_from_u64(value: u64, field: &str) -> Result<usize> {
        usize::try_from(value)
            .map_err(|_| StateProcessorError::HotEngine(format!("{field} does not fit into usize")))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use alloy_consensus::{BlockBody, Header, Signed, TxLegacy, transaction::SignerRecoverable};
    use alloy_eips::{Decodable2718, Encodable2718};
    use alloy_primitives::{Address, B256, Bloom, Bytes, TxKind, U256, hex_literal::hex};
    use alloy_rpc_types_engine::PayloadId;
    use base_common_consensus::{BaseBlock, BasePrimitives, BaseTxEnvelope};
    use base_common_flashblocks::{
        ExecutionPayloadBaseV1, ExecutionPayloadFlashblockDeltaV1, Flashblock, Metadata,
    };
    use base_execution_chainspec::{BaseChainSpec, BaseChainSpecBuilder};
    use reth_chainspec::ChainSpecProvider;
    use reth_primitives::RecoveredBlock;
    use reth_provider::test_utils::{ExtendedAccount, MockEthProvider};

    use crate::BlockAssembler;

    use super::{HotApplyOutcome, HotEngine};

    fn test_client() -> MockEthProvider<BasePrimitives, Arc<BaseChainSpec>> {
        let chain_spec = Arc::new(BaseChainSpecBuilder::base_mainnet().build());
        MockEthProvider::<BasePrimitives>::new().with_chain_spec(chain_spec).with_genesis_block()
    }

    fn encoded_l1_info_tx() -> Bytes {
        Bytes::from_static(&hex!(
            "7ef9015aa044bae9d41b8380d781187b426c6fe43df5fb2fb57bd4466ef6a701e1f01e015694deaddeaddeaddeaddeaddeaddeaddeaddead000194420000000000000000000000000000000000001580808408f0d18001b90104015d8eb900000000000000000000000000000000000000000000000000000000008057650000000000000000000000000000000000000000000000000000000063d96d10000000000000000000000000000000000000000000000000000000000009f35273d89754a1e0387b89520d989d3be9c37c1f32495a88faf1ea05c61121ab0d1900000000000000000000000000000000000000000000000000000000000000010000000000000000000000002d679b567db6187c0c8323fa982cfb88b74dbcc7000000000000000000000000000000000000000000000000000000000000083400000000000000000000000000000000000000000000000000000000000f4240"
        ))
    }

    fn log_emitter_init_code() -> Bytes {
        Bytes::from_static(&[
            0x60, 0x00, 0x60, 0x00, 0xa0, 0x60, 0x06, 0x60, 0x11, 0x60, 0x00, 0x39, 0x60, 0x06,
            0x60, 0x00, 0xf3, 0x60, 0x00, 0x60, 0x00, 0xa0, 0x00,
        ])
    }

    fn create_deploy_log_tx_with_gas_limit(hash_byte: u8, gas_limit: u64) -> BaseTxEnvelope {
        BaseTxEnvelope::Legacy(Signed::new_unchecked(
            TxLegacy {
                chain_id: Some(8453),
                nonce: 0,
                gas_price: 1_000_000_000,
                gas_limit,
                to: TxKind::Create,
                value: U256::ZERO,
                input: log_emitter_init_code(),
            },
            alloy_primitives::Signature::test_signature(),
            B256::with_last_byte(hash_byte),
        ))
    }

    fn create_deploy_log_tx(hash_byte: u8) -> BaseTxEnvelope {
        create_deploy_log_tx_with_gas_limit(hash_byte, 100_000)
    }

    fn create_call_log_tx(contract_address: Address, hash_byte: u8) -> BaseTxEnvelope {
        BaseTxEnvelope::Legacy(Signed::new_unchecked(
            TxLegacy {
                chain_id: Some(8453),
                nonce: 0,
                gas_price: 1_000_000_000,
                gas_limit: 100_000,
                to: TxKind::Call(contract_address),
                value: U256::ZERO,
                input: Bytes::default(),
            },
            alloy_primitives::Signature::test_signature(),
            B256::with_last_byte(hash_byte),
        ))
    }

    fn decoded_l1_info_tx() -> BaseTxEnvelope {
        BaseTxEnvelope::decode_2718_exact(encoded_l1_info_tx().as_ref())
            .expect("l1 info test transaction should decode")
    }

    fn decoded_tx_hash(transaction: &BaseTxEnvelope) -> B256 {
        BaseTxEnvelope::decode_2718_exact(transaction.encoded_2718().as_ref())
            .expect("test transaction should decode")
            .tx_hash()
    }

    fn decoded_transaction(transaction: &BaseTxEnvelope) -> BaseTxEnvelope {
        BaseTxEnvelope::decode_2718_exact(transaction.encoded_2718().as_ref())
            .expect("test transaction should decode")
    }

    fn seed_sender_balance(
        client: &MockEthProvider<BasePrimitives, Arc<BaseChainSpec>>,
        transaction: &BaseTxEnvelope,
    ) {
        let sender = transaction.recover_signer().expect("test transaction signer should recover");
        client.add_account(
            sender,
            ExtendedAccount::new(0, U256::from(10_000_000_000_000_000_000u128)),
        );
    }

    fn flashblock(
        index: u64,
        block_number: u64,
        payload_id: PayloadId,
        parent_hash: B256,
        with_base: bool,
        transactions: Vec<BaseTxEnvelope>,
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
                gas_used: 100_000u64.saturating_mul((index + 1) as u64),
                block_hash: B256::ZERO,
                transactions: transactions
                    .into_iter()
                    .map(|transaction| transaction.encoded_2718().into())
                    .collect(),
                withdrawals: vec![],
                withdrawals_root: B256::ZERO,
                blob_gas_used: None,
            },
            metadata: Metadata { block_number },
        }
    }

    fn canonical_block(
        block_number: u64,
        transactions: Vec<BaseTxEnvelope>,
    ) -> RecoveredBlock<BaseBlock> {
        canonical_block_with_header(
            Header { number: block_number, ..Default::default() },
            transactions,
        )
    }

    fn canonical_block_with_header(
        header: Header,
        transactions: Vec<BaseTxEnvelope>,
    ) -> RecoveredBlock<BaseBlock> {
        let senders = transactions
            .iter()
            .map(|transaction| {
                transaction.recover_signer().expect("canonical signer should recover")
            })
            .collect();

        RecoveredBlock::new_unhashed(
            BaseBlock { header, body: BlockBody { transactions, ..Default::default() } },
            senders,
        )
    }

    fn canonical_block_from_flashblocks(flashblocks: &[Flashblock]) -> RecoveredBlock<BaseBlock> {
        let block =
            BlockAssembler::assemble(flashblocks).expect("test flashblocks should assemble").block;
        let senders = block
            .body
            .transactions
            .iter()
            .map(|transaction| {
                transaction.recover_signer().expect("canonical signer should recover")
            })
            .collect();

        RecoveredBlock::new_unhashed(block, senders)
    }

    fn insert_canonical_header(
        client: &MockEthProvider<BasePrimitives, Arc<BaseChainSpec>>,
        block: &RecoveredBlock<BaseBlock>,
    ) {
        client.add_header(block.header().hash_slow(), block.header().clone());
    }

    #[test]
    fn hot_engine_new_initializes_empty_window_with_requested_depth() {
        let engine = HotEngine::new(test_client(), 3);

        assert_eq!(engine.max_depth, 3);
        assert_eq!(engine.window.max_depth, 3);
        assert!(engine.window.blocks.is_empty());
        assert!(engine.window.execution.is_none());
        assert_eq!(engine.next_snapshot_nonce, 0);
    }

    #[test]
    fn hot_engine_executes_only_new_same_block_suffix() {
        let client = test_client();
        let parent_hash = client.chain_spec().genesis_hash();
        let deploy_tx = create_deploy_log_tx(0x11);
        let contract_address =
            deploy_tx.recover_signer().expect("deploy signer should recover").create(0);
        let call_tx = create_call_log_tx(contract_address, 0x22);
        seed_sender_balance(&client, &deploy_tx);
        seed_sender_balance(&client, &call_tx);

        let mut engine = HotEngine::new(client, 3);
        let first_flashblock = flashblock(
            0,
            1,
            PayloadId::new([0x11; 8]),
            parent_hash,
            true,
            vec![decoded_l1_info_tx(), deploy_tx.clone()],
        );

        let HotApplyOutcome::Delta { delta: first_delta, .. } =
            engine.apply_flashblock(&first_flashblock).expect("first flashblock should apply")
        else {
            panic!("expected first flashblock delta outcome");
        };
        assert_eq!(first_delta.logs.len(), 1);
        assert_eq!(first_delta.logs[0].tx_index, 1);
        assert_eq!(first_delta.logs[0].log_index_in_block, 0);

        let second_flashblock =
            flashblock(1, 1, PayloadId::new([0x11; 8]), parent_hash, false, vec![call_tx.clone()]);

        let HotApplyOutcome::Delta { delta: second_delta, .. } =
            engine.apply_flashblock(&second_flashblock).expect("same-block suffix should apply")
        else {
            panic!("expected same-block delta outcome");
        };

        assert_eq!(second_delta.transactions.len(), 1);
        assert_eq!(second_delta.transactions[0].hash, decoded_tx_hash(&call_tx));
        assert_eq!(second_delta.transactions[0].index, 2);
        assert_eq!(second_delta.logs.len(), 1);
        assert_eq!(second_delta.logs[0].tx_hash, decoded_tx_hash(&call_tx));
        assert_eq!(second_delta.logs[0].tx_index, 2);
        assert_eq!(second_delta.logs[0].log_index_in_block, 1);

        let active_block = engine.window.active_block().expect("active block should be retained");
        assert_eq!(active_block.latest_flashblock_index, 1);
        assert_eq!(active_block.transactions.len(), 3);
        assert_eq!(active_block.logs.len(), 2);
        assert_eq!(active_block.next_tx_index, 3);
        assert_eq!(active_block.next_log_index, 2);
    }

    #[test]
    fn hot_engine_rolls_over_to_next_block_with_carried_execution_state() {
        let client = test_client();
        let canonical_parent_hash = client.chain_spec().genesis_hash();
        let deploy_tx = create_deploy_log_tx(0x31);
        let contract_address =
            deploy_tx.recover_signer().expect("deploy signer should recover").create(0);
        let call_tx = create_call_log_tx(contract_address, 0x32);
        seed_sender_balance(&client, &deploy_tx);
        seed_sender_balance(&client, &call_tx);

        let mut engine = HotEngine::new(client, 3);
        let first_flashblock = flashblock(
            0,
            1,
            PayloadId::new([0x21; 8]),
            canonical_parent_hash,
            true,
            vec![decoded_l1_info_tx(), deploy_tx.clone()],
        );
        engine.apply_flashblock(&first_flashblock).expect("first block should apply");

        let carried_parent_hash = engine
            .window
            .execution
            .as_ref()
            .expect("execution state should exist")
            .last_header
            .hash();
        let second_flashblock = flashblock(
            0,
            2,
            PayloadId::new([0x22; 8]),
            carried_parent_hash,
            true,
            vec![decoded_l1_info_tx(), call_tx.clone()],
        );

        let HotApplyOutcome::Delta { delta, .. } =
            engine.apply_flashblock(&second_flashblock).expect("next block should roll over")
        else {
            panic!("expected next-block delta outcome");
        };

        assert_eq!(delta.transactions.len(), 2);
        assert_eq!(delta.transactions[1].hash, decoded_tx_hash(&call_tx));
        assert_eq!(delta.transactions[1].status, Some(1));
        assert_eq!(delta.logs.len(), 1);
        assert_eq!(delta.logs[0].tx_hash, decoded_tx_hash(&call_tx));
        assert_eq!(delta.logs[0].tx_index, 1);
        assert_eq!(delta.logs[0].log_index_in_block, 0);

        assert_eq!(engine.window.blocks.len(), 2);
        assert_eq!(engine.window.blocks.front().map(|block| block.block_number), Some(1));
        assert_eq!(engine.window.blocks.back().map(|block| block.block_number), Some(2));
        assert_eq!(
            engine
                .window
                .execution
                .as_ref()
                .expect("execution state should persist")
                .last_header
                .number,
            2,
        );
    }

    #[test]
    fn hot_engine_rolls_over_after_multiple_flashblocks_using_assembled_parent_hash() {
        let client = test_client();
        let canonical_parent_hash = client.chain_spec().genesis_hash();
        let first_block_deploy_tx = create_deploy_log_tx(0x33);
        let first_block_contract =
            first_block_deploy_tx.recover_signer().expect("deploy signer should recover").create(0);
        let first_block_call_tx = create_call_log_tx(first_block_contract, 0x34);
        let second_block_deploy_tx = create_deploy_log_tx_with_gas_limit(0x35, 120_000);
        seed_sender_balance(&client, &first_block_deploy_tx);
        seed_sender_balance(&client, &first_block_call_tx);
        seed_sender_balance(&client, &second_block_deploy_tx);

        let mut engine = HotEngine::new(client, 3);
        let first_flashblock = flashblock(
            0,
            1,
            PayloadId::new([0x23; 8]),
            canonical_parent_hash,
            true,
            vec![decoded_l1_info_tx(), first_block_deploy_tx.clone()],
        );
        engine.apply_flashblock(&first_flashblock).expect("first flashblock should apply");

        let second_flashblock = flashblock(
            1,
            1,
            PayloadId::new([0x24; 8]),
            canonical_parent_hash,
            false,
            vec![first_block_call_tx],
        );
        engine.apply_flashblock(&second_flashblock).expect("second flashblock should apply");

        let assembled_parent_hash =
            BlockAssembler::assemble(&[first_flashblock.clone(), second_flashblock.clone()])
                .expect("block should assemble")
                .block
                .header
                .hash_slow();
        let third_flashblock = flashblock(
            0,
            2,
            PayloadId::new([0x25; 8]),
            assembled_parent_hash,
            true,
            vec![decoded_l1_info_tx(), second_block_deploy_tx],
        );

        let HotApplyOutcome::Delta { delta, .. } = engine
            .apply_flashblock(&third_flashblock)
            .expect("next block should roll over from assembled parent hash")
        else {
            panic!("expected next-block delta outcome after multi-flashblock parent");
        };

        assert_eq!(delta.transactions.len(), 2);
        assert_eq!(engine.window.blocks.len(), 2);
        assert_eq!(engine.window.blocks.front().map(|block| block.block_number), Some(1));
        assert_eq!(engine.window.blocks.back().map(|block| block.block_number), Some(2));
    }

    #[test]
    fn hot_engine_resets_on_non_sequential_gap() {
        let client = test_client();
        let parent_hash = client.chain_spec().genesis_hash();
        let deploy_tx = create_deploy_log_tx(0x41);
        seed_sender_balance(&client, &deploy_tx);

        let mut engine = HotEngine::new(client, 3);
        let first_flashblock = flashblock(
            0,
            1,
            PayloadId::new([0x31; 8]),
            parent_hash,
            true,
            vec![decoded_l1_info_tx(), deploy_tx],
        );
        engine.apply_flashblock(&first_flashblock).expect("first flashblock should apply");

        let gap_flashblock = flashblock(
            2,
            1,
            PayloadId::new([0x31; 8]),
            parent_hash,
            false,
            vec![create_deploy_log_tx(0x42)],
        );

        let outcome =
            engine.apply_flashblock(&gap_flashblock).expect("gap flashblock should return reset");

        assert!(matches!(outcome, HotApplyOutcome::Reset));
        assert!(engine.window.execution.is_none());
        assert!(engine.window.blocks.is_empty());
    }

    #[test]
    fn hot_engine_canonical_catchup_resets_without_legacy_rebuild() {
        let client = test_client();
        let parent_hash = client.chain_spec().genesis_hash();
        let deploy_tx = create_deploy_log_tx(0x51);
        seed_sender_balance(&client, &deploy_tx);

        let mut engine = HotEngine::new(client, 3);
        let first_flashblock = flashblock(
            0,
            1,
            PayloadId::new([0x41; 8]),
            parent_hash,
            true,
            vec![decoded_l1_info_tx(), deploy_tx.clone()],
        );
        engine.apply_flashblock(&first_flashblock).expect("first flashblock should apply");

        let outcome = engine
            .process_canonical_block(&canonical_block(1, vec![decoded_l1_info_tx(), deploy_tx]))
            .expect("canonical catch-up should reset");

        assert!(matches!(outcome, HotApplyOutcome::Reset));
        assert!(engine.window.execution.is_none());
        assert!(engine.window.blocks.is_empty());
    }

    #[test]
    fn hot_engine_canonical_same_number_multiflashblock_replays_newer_pending_block_state() {
        let client = test_client();
        let canonical_parent_hash = client.chain_spec().genesis_hash();
        let first_block_deploy_tx = create_deploy_log_tx(0x61);
        let first_block_contract =
            first_block_deploy_tx.recover_signer().expect("deploy signer should recover").create(0);
        let first_block_call_tx = create_call_log_tx(first_block_contract, 0x62);
        let second_block_deploy_tx = create_deploy_log_tx_with_gas_limit(0x63, 120_000);
        let second_block_contract = second_block_deploy_tx
            .recover_signer()
            .expect("deploy signer should recover")
            .create(0);
        let second_block_call_tx = create_call_log_tx(second_block_contract, 0x64);
        seed_sender_balance(&client, &first_block_deploy_tx);
        seed_sender_balance(&client, &first_block_call_tx);
        seed_sender_balance(&client, &second_block_deploy_tx);
        seed_sender_balance(&client, &second_block_call_tx);

        let mut engine = HotEngine::new(client.clone(), 3);
        let first_flashblock = flashblock(
            0,
            1,
            PayloadId::new([0x51; 8]),
            canonical_parent_hash,
            true,
            vec![decoded_l1_info_tx(), first_block_deploy_tx.clone()],
        );
        engine.apply_flashblock(&first_flashblock).expect("first block should apply");

        let second_flashblock = flashblock(
            1,
            1,
            PayloadId::new([0x52; 8]),
            canonical_parent_hash,
            false,
            vec![first_block_call_tx.clone()],
        );
        engine.apply_flashblock(&second_flashblock).expect("second block should apply");

        let canonical_flashblocks = [first_flashblock.clone(), second_flashblock.clone()];
        let canonical_block = canonical_block_from_flashblocks(&canonical_flashblocks);
        engine.window.execution.as_mut().expect("execution state should exist").last_header =
            HotEngine::<MockEthProvider<BasePrimitives, Arc<BaseChainSpec>>>::seal_header(
                canonical_block.header().clone(),
            );

        let carried_parent_hash = engine
            .window
            .execution
            .as_ref()
            .expect("execution state should exist")
            .last_header
            .hash();
        let third_flashblock = flashblock(
            0,
            2,
            PayloadId::new([0x53; 8]),
            carried_parent_hash,
            true,
            vec![decoded_l1_info_tx(), second_block_deploy_tx.clone()],
        );
        engine.apply_flashblock(&third_flashblock).expect("third block should apply");

        insert_canonical_header(&client, &canonical_block);

        let outcome = engine
            .process_canonical_block(&canonical_block)
            .expect("multi-flashblock canonical block should be silently replayed");

        assert!(matches!(outcome, HotApplyOutcome::Duplicate));
        assert_eq!(engine.window.blocks.len(), 1);
        assert_eq!(engine.window.blocks.front().map(|block| block.block_number), Some(2));
        assert_eq!(engine.window.active_block().map(|block| block.flashblocks.len()), Some(1));

        let fourth_flashblock = flashblock(
            1,
            2,
            PayloadId::new([0x53; 8]),
            carried_parent_hash,
            false,
            vec![second_block_call_tx.clone()],
        );

        let HotApplyOutcome::Delta { delta, .. } = engine
            .apply_flashblock(&fourth_flashblock)
            .expect("replayed pending block should accept later same-block suffix")
        else {
            panic!("expected same-block delta outcome after canonical replay");
        };

        assert_eq!(delta.transactions.len(), 1);
        assert_eq!(delta.transactions[0].hash, decoded_tx_hash(&second_block_call_tx));
        assert_eq!(delta.transactions[0].index, 2);
        assert_eq!(delta.logs.len(), 1);
        assert_eq!(delta.logs[0].tx_hash, decoded_tx_hash(&second_block_call_tx));
        assert_eq!(delta.logs[0].tx_index, 2);
        assert_eq!(delta.logs[0].log_index_in_block, 1);
    }

    #[test]
    fn hot_engine_canonical_same_number_keeps_newer_state_for_exact_match() {
        let client = test_client();
        let canonical_parent_hash = client.chain_spec().genesis_hash();
        let deploy_tx = create_deploy_log_tx(0x71);
        let next_block_tx = create_deploy_log_tx_with_gas_limit(0x72, 120_000);
        seed_sender_balance(&client, &deploy_tx);
        seed_sender_balance(&client, &next_block_tx);

        let mut engine = HotEngine::new(client.clone(), 3);
        let first_flashblock = flashblock(
            0,
            1,
            PayloadId::new([0x61; 8]),
            canonical_parent_hash,
            true,
            vec![decoded_l1_info_tx(), deploy_tx.clone()],
        );
        engine.apply_flashblock(&first_flashblock).expect("first block should apply");

        let carried_parent_hash = engine
            .window
            .execution
            .as_ref()
            .expect("execution state should exist")
            .last_header
            .hash();
        let second_flashblock = flashblock(
            0,
            2,
            PayloadId::new([0x62; 8]),
            carried_parent_hash,
            true,
            vec![decoded_l1_info_tx(), next_block_tx],
        );
        engine.apply_flashblock(&second_flashblock).expect("second block should apply");

        let canonical_block =
            canonical_block_from_flashblocks(std::slice::from_ref(&first_flashblock));
        insert_canonical_header(&client, &canonical_block);

        let outcome = engine
            .process_canonical_block(&canonical_block)
            .expect("matching canonical block should be accepted");

        assert!(matches!(outcome, HotApplyOutcome::Duplicate));
        assert_eq!(engine.window.blocks.len(), 1);
        assert_eq!(engine.window.blocks.front().map(|block| block.block_number), Some(2));
        assert_eq!(
            engine.window.execution.as_ref().map(|execution| execution.last_header.number),
            Some(2)
        );
        assert_eq!(engine.window.active_block().map(|block| block.flashblocks.len()), Some(1));
    }

    #[test]
    fn hot_engine_canonical_below_oldest_retained_resets_without_safe_replay_anchor() {
        let client = test_client();
        let canonical_parent_hash = client.chain_spec().genesis_hash();
        let first_block_tx = create_deploy_log_tx(0x81);
        let second_block_tx = create_deploy_log_tx_with_gas_limit(0x82, 110_000);
        let third_block_tx = create_deploy_log_tx_with_gas_limit(0x83, 120_000);
        seed_sender_balance(&client, &first_block_tx);
        seed_sender_balance(&client, &second_block_tx);
        seed_sender_balance(&client, &third_block_tx);

        let mut engine = HotEngine::new(client, 1);
        let first_flashblock = flashblock(
            0,
            1,
            PayloadId::new([0x71; 8]),
            canonical_parent_hash,
            true,
            vec![decoded_l1_info_tx(), first_block_tx.clone()],
        );
        engine.apply_flashblock(&first_flashblock).expect("first block should apply");

        let second_parent_hash = engine
            .window
            .execution
            .as_ref()
            .expect("execution state should exist")
            .last_header
            .hash();
        let second_flashblock = flashblock(
            0,
            2,
            PayloadId::new([0x72; 8]),
            second_parent_hash,
            true,
            vec![decoded_l1_info_tx(), second_block_tx],
        );
        engine.apply_flashblock(&second_flashblock).expect("second block should apply");

        let third_parent_hash = engine
            .window
            .execution
            .as_ref()
            .expect("execution state should exist")
            .last_header
            .hash();
        let third_flashblock = flashblock(
            0,
            3,
            PayloadId::new([0x73; 8]),
            third_parent_hash,
            true,
            vec![decoded_l1_info_tx(), third_block_tx],
        );
        engine.apply_flashblock(&third_flashblock).expect("third block should apply");

        assert_eq!(engine.window.blocks.len(), 1);
        assert_eq!(engine.window.blocks.front().map(|block| block.block_number), Some(3));

        let canonical_block =
            canonical_block_from_flashblocks(std::slice::from_ref(&first_flashblock));
        let outcome = engine
            .process_canonical_block(&canonical_block)
            .expect("unsafe below-oldest canonical should force reset");

        assert!(matches!(outcome, HotApplyOutcome::Reset));
        assert!(engine.window.execution.is_none());
        assert!(engine.window.blocks.is_empty());
    }
}
