use std::{sync::Arc, time::Instant};

use alloy_consensus::{Header, Sealed, TxReceipt};
use alloy_eips::BlockNumberOrTag;
use alloy_primitives::{
    Address, B256, BlockNumber, TxHash, U256, keccak256,
    map::foldhash::{HashMap, HashMapExt},
};
use alloy_provider::network::TransactionResponse;
use alloy_rpc_types::{BlockTransactions, Withdrawal, state::StateOverride};
use alloy_rpc_types_engine::PayloadId;
use alloy_rpc_types_eth::{Filter, Header as RPCHeader, Log};
use arc_swap::Guard;
use base_common_consensus::OpTxType;
use base_common_evm::{BaseHaltReason, BaseTxResult};
use base_common_flashblocks::Flashblock;
use base_common_network::Base;
use base_common_rpc_types::{BaseTransactionReceipt, Transaction};
use reth_evm::eth::EthTxResult;
use reth_revm::db::BundleState;
use reth_rpc_convert::RpcTransaction;
use reth_rpc_eth_api::{RpcBlock, RpcReceipt};
use revm::{
    context::result::ExecResultAndState, context_interface::result::ExecutionResult,
    state::EvmState,
};

use crate::{
    BuildError, FastFlashblockLog, FastFlashblockLogsDelta, FastFlashblockTxMeta, FlashblockLog,
    FlashblockLogsBatch, FlashblockSnapshotId, FlashblockTxMeta, PendingBlocksAPI,
    StateProcessorError, TransactionWithLogs, metrics::Metrics,
};

/// Builder for [`PendingBlocks`].
#[derive(Debug)]
pub struct PendingBlocksBuilder {
    flashblocks: Vec<Flashblock>,
    headers: Vec<Sealed<Header>>,

    transactions: Vec<Transaction>,
    account_balances: HashMap<Address, U256>,
    transaction_count: HashMap<Address, U256>,
    transaction_receipts: HashMap<B256, BaseTransactionReceipt>,
    transactions_by_hash: HashMap<B256, Transaction>,
    transaction_position: HashMap<B256, (BlockNumber, usize)>,
    next_position_per_block: HashMap<BlockNumber, usize>,
    transaction_state: HashMap<B256, EvmState>,
    transaction_senders: HashMap<B256, Address>,
    state_overrides: Option<StateOverride>,
    transaction_results: HashMap<B256, ExecutionResult<BaseHaltReason>>,
    execution_times: HashMap<B256, u128>,
    state_root_times: HashMap<B256, u128>,

    bundle_state: BundleState,

    // Deferred error from `with_transaction` (e.g. duplicate hash). Surfaced from `build()`.
    deferred_error: Option<BuildError>,
}

impl Default for PendingBlocksBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl PendingBlocksBuilder {
    /// Creates a new empty builder.
    pub fn new() -> Self {
        Self {
            flashblocks: Vec::new(),
            headers: Vec::new(),
            transactions: Vec::new(),
            account_balances: HashMap::new(),
            transaction_count: HashMap::new(),
            transaction_receipts: HashMap::new(),
            transactions_by_hash: HashMap::new(),
            transaction_position: HashMap::new(),
            next_position_per_block: HashMap::new(),
            transaction_state: HashMap::new(),
            transaction_senders: HashMap::new(),
            transaction_results: HashMap::new(),
            execution_times: HashMap::new(),
            state_root_times: HashMap::new(),
            state_overrides: None,
            bundle_state: BundleState::default(),
            deferred_error: None,
        }
    }

    /// Adds flashblocks to the builder.
    #[inline]
    pub fn with_flashblocks(&mut self, flashblocks: impl IntoIterator<Item = Flashblock>) -> &Self {
        self.flashblocks.extend(flashblocks);
        self
    }

    /// Adds a header to the builder.
    #[inline]
    pub fn with_header(&mut self, header: Sealed<Header>) -> &Self {
        self.headers.push(header);
        self
    }

    /// Stores a transaction in the builder.
    ///
    /// Each `tx_hash` may only be added once. A duplicate is recorded as a deferred
    /// [`BuildError::DuplicateTransaction`] and surfaced from [`Self::build`], rather
    /// than silently corrupting the per-block position index or the existing
    /// per-hash maps (`transactions_by_hash`, etc.) that would otherwise overwrite.
    #[inline]
    pub fn with_transaction(&mut self, transaction: Transaction) -> &Self {
        let tx_hash = transaction.tx_hash();
        if self.transaction_position.contains_key(&tx_hash) {
            self.deferred_error.get_or_insert(BuildError::DuplicateTransaction { tx_hash });
            return self;
        }
        let block_number = transaction.block_number.unwrap_or(0);
        let position = self.next_position_per_block.entry(block_number).or_insert(0);
        self.transaction_position.insert(tx_hash, (block_number, *position));
        *position += 1;
        self.transactions_by_hash.insert(tx_hash, transaction.clone());
        self.transactions.push(transaction);
        self
    }

    /// Stores the EVM state changes produced by a transaction.
    #[inline]
    pub fn with_transaction_state(&mut self, hash: B256, state: EvmState) -> &Self {
        self.transaction_state.insert(hash, state);
        self
    }

    /// Records the sender of a transaction.
    #[inline]
    pub fn with_transaction_sender(&mut self, hash: B256, sender: Address) -> &Self {
        self.transaction_senders.insert(hash, sender);
        self
    }

    /// Increments the pending nonce for an account.
    #[inline]
    pub fn increment_nonce(&mut self, sender: Address) -> &Self {
        let zero = U256::from(0);
        let current_count = self.transaction_count.get(&sender).unwrap_or(&zero);

        _ = self.transaction_count.insert(sender, *current_count + U256::from(1));
        self
    }

    /// Stores the receipt for a transaction.
    #[inline]
    pub fn with_receipt(&mut self, hash: B256, receipt: BaseTransactionReceipt) -> &Self {
        self.transaction_receipts.insert(hash, receipt);
        self
    }

    /// Records the balance of an account after execution.
    #[inline]
    pub fn with_account_balance(&mut self, address: Address, balance: U256) -> &Self {
        self.account_balances.insert(address, balance);
        self
    }

    /// Sets state overrides for the pending blocks.
    #[inline]
    pub fn with_state_overrides(&mut self, state_overrides: StateOverride) -> &Self {
        self.state_overrides = Some(state_overrides);
        self
    }

    /// Sets the accumulated bundle state.
    #[inline]
    pub fn with_bundle_state(&mut self, bundle_state: BundleState) -> &Self {
        self.bundle_state = bundle_state;
        self
    }

    /// Stores the execution result for a transaction.
    #[inline]
    pub fn with_transaction_result(
        &mut self,
        hash: B256,
        result: ExecutionResult<BaseHaltReason>,
    ) -> &Self {
        self.transaction_results.insert(hash, result);
        self
    }

    /// Stores per-transaction EVM execution time.
    #[inline]
    pub fn with_execution_time(&mut self, hash: B256, time_us: u128) -> &Self {
        self.execution_times.insert(hash, time_us);
        self
    }

    /// Stores per-transaction state root simulation time.
    #[inline]
    pub fn with_state_root_time(&mut self, hash: B256, time_us: u128) -> &Self {
        self.state_root_times.insert(hash, time_us);
        self
    }

    /// Builds the pending blocks.
    pub fn build(self) -> Result<PendingBlocks, StateProcessorError> {
        if let Some(err) = self.deferred_error {
            return Err(err.into());
        }
        let earliest_header = self.headers.first().cloned().ok_or(BuildError::MissingHeaders)?;
        let latest_header = self.headers.last().cloned().ok_or(BuildError::MissingHeaders)?;

        let latest_flashblock_index =
            self.flashblocks.last().map(|fb| fb.index).ok_or(BuildError::NoFlashblocks)?;

        for transaction in &self.transactions {
            let tx_hash = transaction.tx_hash();
            if !self.transaction_receipts.contains_key(&tx_hash) {
                return Err(BuildError::MissingReceipt { tx_hash }.into());
            }
        }

        Ok(PendingBlocks {
            earliest_header,
            latest_header,
            latest_flashblock_index,
            flashblocks: self.flashblocks,
            transactions: self.transactions,
            account_balances: self.account_balances,
            transaction_count: self.transaction_count,
            transaction_receipts: self.transaction_receipts,
            transactions_by_hash: self.transactions_by_hash,
            transaction_position: self.transaction_position,
            transaction_state: self.transaction_state,
            transaction_senders: self.transaction_senders,
            state_overrides: self.state_overrides,
            bundle_state: self.bundle_state,
            transaction_results: self.transaction_results,
            execution_times: self.execution_times,
            state_root_times: self.state_root_times,
        })
    }
}

/// Aggregated pending block state from flashblocks.
#[derive(Debug, Clone)]
pub struct PendingBlocks {
    earliest_header: Sealed<Header>,
    latest_header: Sealed<Header>,
    latest_flashblock_index: u64,
    flashblocks: Vec<Flashblock>,
    transactions: Vec<Transaction>,

    account_balances: HashMap<Address, U256>,
    transaction_count: HashMap<Address, U256>,
    transaction_receipts: HashMap<B256, BaseTransactionReceipt>,
    transactions_by_hash: HashMap<B256, Transaction>,
    transaction_position: HashMap<B256, (BlockNumber, usize)>,
    transaction_state: HashMap<B256, EvmState>,
    transaction_senders: HashMap<B256, Address>,
    state_overrides: Option<StateOverride>,
    transaction_results: HashMap<B256, ExecutionResult<BaseHaltReason>>,
    execution_times: HashMap<B256, u128>,
    state_root_times: HashMap<B256, u128>,

    bundle_state: BundleState,
}

impl PendingBlocks {
    fn transaction_with_logs(
        transaction: &Transaction,
        receipt: &BaseTransactionReceipt,
    ) -> TransactionWithLogs {
        TransactionWithLogs {
            transaction: transaction.clone(),
            logs: receipt.inner.logs().to_vec(),
            gas_used: receipt.inner.gas_used,
            status: receipt.inner.inner.status_or_post_state(),
            cumulative_gas_used: receipt.inner.inner.cumulative_gas_used(),
            contract_address: receipt.inner.contract_address,
            logs_bloom: receipt.inner.inner.logs_bloom,
        }
    }

    /// Returns the latest block number in the pending state.
    #[inline]
    pub fn latest_block_number(&self) -> BlockNumber {
        self.latest_header.number
    }

    /// Returns the canonical block number (the block before pending).
    #[inline]
    pub fn canonical_block_number(&self) -> BlockNumberOrTag {
        BlockNumberOrTag::Number(self.earliest_header.number - 1)
    }

    /// Returns the earliest block number in the pending state.
    #[inline]
    pub fn earliest_block_number(&self) -> BlockNumber {
        self.earliest_header.number
    }

    /// Returns the payload ID for the current build attempt.
    #[inline]
    pub fn payload_id(&self) -> PayloadId {
        self.flashblocks.first().map(|fb| fb.payload_id).unwrap_or_default()
    }

    #[inline]
    fn latest_payload_id(&self) -> PayloadId {
        self.flashblocks.last().map(|fb| fb.payload_id).unwrap_or_default()
    }

    /// Returns the index of the latest flashblock.
    #[inline]
    pub const fn latest_flashblock_index(&self) -> u64 {
        self.latest_flashblock_index
    }

    /// Returns the latest header.
    #[inline]
    pub fn latest_header(&self) -> Sealed<Header> {
        self.latest_header.clone()
    }

    /// Returns the parent hash of the earliest pending block.
    ///
    /// This is the canonical block hash on top of which the cached flashblock
    /// execution was performed. Consumers that reuse cached execution results
    /// MUST verify their incoming `parent_block_hash` matches this value, since
    /// during a reorg or sequencer failover two different parent hashes can
    /// share the same block number.
    #[inline]
    pub fn parent_hash(&self) -> B256 {
        self.earliest_header.parent_hash
    }

    /// Returns all flashblocks.
    pub fn get_flashblocks(&self) -> Vec<Flashblock> {
        self.flashblocks.clone()
    }

    /// Returns the EVM state for a transaction.
    pub fn get_transaction_state(&self, hash: &B256) -> Option<EvmState> {
        self.transaction_state.get(hash).cloned()
    }

    /// Returns the sender of a transaction.
    pub fn get_transaction_sender(&self, tx_hash: &B256) -> Option<Address> {
        self.transaction_senders.get(tx_hash).copied()
    }

    /// Returns a clone of the bundle state.
    ///
    /// NOTE: This clones the entire `BundleState`, which contains a `HashMap` of all touched
    /// accounts and their storage slots. The cost scales with the number of accounts and
    /// storage slots modified in the flashblock. Monitor `bundle_state_clone_duration` and
    /// `bundle_state_clone_size` metrics to track if this becomes a bottleneck.
    pub fn get_bundle_state(&self) -> BundleState {
        let size = self.bundle_state.state.len();
        let start = Instant::now();
        let cloned = self.bundle_state.clone();
        Metrics::bundle_state_clone_duration().record(start.elapsed());
        Metrics::bundle_state_clone_size().record(size as f64);
        cloned
    }

    /// Returns all transactions for a specific block number.
    pub fn get_transactions_for_block(
        &self,
        block_number: BlockNumber,
    ) -> impl Iterator<Item = &Transaction> {
        self.transactions.iter().filter(move |tx| tx.block_number.unwrap_or(0) == block_number)
    }

    /// Returns all withdrawals collected from flashblocks.
    fn get_withdrawals(&self) -> Vec<Withdrawal> {
        self.flashblocks.iter().flat_map(|fb| fb.diff.withdrawals.clone()).collect()
    }

    /// Returns the latest block, optionally with full transaction details.
    pub fn get_latest_block(&self, full: bool) -> RpcBlock<Base> {
        let header = self.latest_header();
        let block_number = header.number;
        let block_transactions: Vec<Transaction> =
            self.get_transactions_for_block(block_number).cloned().collect();

        let transactions = if full {
            BlockTransactions::Full(block_transactions)
        } else {
            let tx_hashes: Vec<B256> = block_transactions.iter().map(|tx| tx.tx_hash()).collect();
            BlockTransactions::Hashes(tx_hashes)
        };

        RpcBlock::<Base> {
            header: RPCHeader::from_consensus(header, None, None),
            transactions,
            uncles: Vec::new(),
            withdrawals: Some(self.get_withdrawals().into()),
        }
    }

    /// Returns the receipt for a transaction.
    pub fn get_receipt(&self, tx_hash: TxHash) -> Option<&BaseTransactionReceipt> {
        self.transaction_receipts.get(&tx_hash)
    }

    /// Returns the execution result for a transaction.
    pub fn get_transaction_result(
        &self,
        tx_hash: &B256,
    ) -> Option<&ExecutionResult<BaseHaltReason>> {
        self.transaction_results.get(tx_hash)
    }

    /// Returns the per-transaction EVM execution time in microseconds.
    pub fn get_execution_time(&self, tx_hash: &B256) -> Option<u128> {
        self.execution_times.get(tx_hash).copied()
    }

    /// Returns the per-transaction state root simulation time in microseconds.
    pub fn get_state_root_time(&self, tx_hash: &B256) -> Option<u128> {
        self.state_root_times.get(tx_hash).copied()
    }

    /// Returns the receipt and state for a transaction.
    pub fn get_tx_result(&self, tx_hash: &B256) -> Option<BaseTxResult<BaseHaltReason, OpTxType>> {
        let (((result, state), tx), sender) = self
            .get_transaction_result(tx_hash)
            .zip(self.get_transaction_state(tx_hash))
            .zip(self.get_transaction_by_hash(*tx_hash))
            .zip(self.get_transaction_sender(tx_hash))?;

        // Use blob_gas_used from receipt (DA footprint for Jovian) instead of
        // hardcoding 0, so that CachedExecutor correctly accumulates da_footprint_used.
        let blob_gas_used =
            self.get_receipt(*tx_hash).and_then(|r| r.inner.blob_gas_used).unwrap_or_default();

        let eth_tx_result = EthTxResult {
            result: ExecResultAndState::new(result.clone(), state),
            blob_gas_used,
            tx_type: tx.inner.inner.tx_type(),
        };

        let base_tx_result =
            BaseTxResult { inner: eth_tx_result, is_deposit: tx.inner.inner.is_deposit(), sender };

        Some(base_tx_result)
    }

    /// Returns a transaction by its hash.
    pub fn get_transaction_by_hash(&self, tx_hash: TxHash) -> Option<&Transaction> {
        self.transactions_by_hash.get(&tx_hash)
    }

    /// Returns true if the transaction hash is in the pending blocks.
    pub fn has_transaction_hash(&self, tx_hash: &B256) -> bool {
        self.transactions_by_hash.contains_key(tx_hash)
    }

    /// Returns the per-block position (0-indexed) of a transaction within `block_number`,
    /// or `None` if the hash is not present in the pending state for that block.
    pub fn transaction_position(&self, block_number: BlockNumber, tx_hash: &B256) -> Option<usize> {
        self.transaction_position
            .get(tx_hash)
            .and_then(|&(bn, pos)| (bn == block_number).then_some(pos))
    }

    /// Returns the transaction count for an address in pending state.
    pub fn get_transaction_count(&self, address: Address) -> U256 {
        self.transaction_count.get(&address).copied().unwrap_or_else(|| U256::from(0))
    }

    /// Returns the balance for an address in pending state.
    pub fn get_balance(&self, address: Address) -> Option<U256> {
        self.account_balances.get(&address).copied()
    }

    /// Returns the state overrides for the pending state.
    pub fn get_state_overrides(&self) -> Option<StateOverride> {
        self.state_overrides.clone()
    }

    /// Returns logs matching the filter from pending state.
    pub fn get_pending_logs(&self, filter: &Filter) -> Vec<Log> {
        let mut logs = Vec::new();

        for tx in &self.transactions {
            if let Some(receipt) = self.transaction_receipts.get(&tx.tx_hash()) {
                for log in receipt.inner.logs() {
                    if filter.matches(&log.inner) {
                        logs.push(log.clone());
                    }
                }
            }
        }

        logs
    }

    /// Returns all pending transactions from flashblocks.
    pub fn get_pending_transactions(&self) -> Vec<Transaction> {
        self.transactions.clone()
    }

    /// Returns all pending transactions with their associated logs from flashblocks.
    pub fn get_pending_transactions_with_logs(&self) -> Vec<TransactionWithLogs> {
        self.transactions
            .iter()
            .filter_map(|tx| {
                self.transaction_receipts
                    .get(&tx.tx_hash())
                    .map(|receipt| Self::transaction_with_logs(tx, receipt))
            })
            .collect()
    }

    /// Returns the hashes of all pending transactions from flashblocks.
    pub fn get_pending_transaction_hashes(&self) -> Vec<B256> {
        self.transactions.iter().map(|tx| tx.tx_hash()).collect()
    }

    /// Returns the number of transactions in all flashblocks except the latest one.
    /// This is used to compute the delta (transactions only in the latest flashblock).
    fn previous_flashblocks_tx_count(&self) -> usize {
        if self.flashblocks.len() <= 1 {
            return 0;
        }
        self.flashblocks[..self.flashblocks.len() - 1]
            .iter()
            .map(|fb| fb.diff.transactions.len())
            .sum()
    }

    /// Returns the transaction range covered by the latest flashblock.
    fn latest_flashblock_tx_range(&self) -> std::ops::Range<usize> {
        let start = self.previous_flashblocks_tx_count().min(self.transactions.len());
        let latest_len = self
            .flashblocks
            .last()
            .map(|flashblock| flashblock.diff.transactions.len())
            .unwrap_or_default();
        let end = start.saturating_add(latest_len).min(self.transactions.len());
        start..end
    }

    fn count_receipt_logs_before(&self, block_number: BlockNumber, tx_count: usize) -> u64 {
        self.transactions
            .iter()
            .take(tx_count)
            .filter(|tx| tx.block_number.unwrap_or(0) == block_number)
            .filter_map(|tx| self.transaction_receipts.get(&tx.tx_hash()))
            .map(|receipt| receipt.inner.logs().len() as u64)
            .sum()
    }

    fn latest_flashblock_tx_index(
        &self,
        latest_block_number: BlockNumber,
        absolute_tx_index: usize,
        tx_hash: B256,
        tx: &Transaction,
    ) -> u64 {
        tx.transaction_index()
            .or(tx.inner.transaction_index)
            .or_else(|| {
                self.transaction_position(latest_block_number, &tx_hash)
                    .map(|position| position as u64)
            })
            .unwrap_or_else(|| {
                self.transactions[..absolute_tx_index]
                    .iter()
                    .filter(|prior_tx| prior_tx.block_number.unwrap_or(0) == latest_block_number)
                    .count() as u64
            })
    }

    /// Returns logs matching the filter from only the latest flashblock (delta).
    ///
    /// Unlike `get_pending_logs`, this returns only logs from transactions
    /// that were added in the most recent flashblock, avoiding duplicates
    /// when streaming via WebSocket subscriptions.
    pub fn get_latest_flashblock_logs(&self, filter: &Filter) -> Vec<Log> {
        let prev_count = self.previous_flashblocks_tx_count();
        let mut logs = Vec::new();

        for tx in self.transactions.iter().skip(prev_count) {
            if let Some(receipt) = self.transaction_receipts.get(&tx.tx_hash()) {
                for log in receipt.inner.logs() {
                    if filter.matches(&log.inner) {
                        logs.push(log.clone());
                    }
                }
            }
        }

        logs
    }

    /// Returns a batch payload for logs and transaction metadata from only the latest flashblock.
    pub fn get_latest_flashblock_logs_batch(&self, filter: Option<&Filter>) -> FlashblockLogsBatch {
        let _logs_batch_build_timer = base_metrics::timed!(Metrics::logs_batch_build_duration());

        let latest_block_number = self.latest_block_number();
        let tx_range = self.latest_flashblock_tx_range();
        let mut transactions = Vec::new();
        let mut logs = Vec::new();
        let mut next_block_log_index =
            self.count_receipt_logs_before(latest_block_number, tx_range.start);

        for (relative_tx_index, tx) in self.transactions[tx_range.clone()].iter().enumerate() {
            let absolute_tx_index = tx_range.start + relative_tx_index;
            let tx_hash = tx.tx_hash();
            let tx_index = self.latest_flashblock_tx_index(
                latest_block_number,
                absolute_tx_index,
                tx_hash,
                tx,
            );
            let Some(receipt) = self.transaction_receipts.get(&tx_hash) else {
                continue;
            };

            transactions.push(FlashblockTxMeta {
                hash: tx_hash,
                index: tx_index,
                status: Some(if receipt.inner.inner.status() { 1 } else { 0 }),
            });

            for (log_index_in_tx, log) in receipt.inner.logs().iter().enumerate() {
                let log_index_in_block = log.log_index.unwrap_or(next_block_log_index);
                next_block_log_index =
                    next_block_log_index.max(log_index_in_block.saturating_add(1));

                if filter.is_some_and(|filter| !filter.matches(&log.inner)) {
                    continue;
                }

                logs.push(FlashblockLog {
                    tx_hash,
                    tx_index,
                    log_index_in_tx: log_index_in_tx as u64,
                    log_index_in_block,
                    address: log.inner.address,
                    topics: log.inner.data.topics().to_vec(),
                    data: log.inner.data.data.clone(),
                    removed: log.removed,
                });
            }
        }

        let mut batch = FlashblockLogsBatch {
            block_number: self.latest_block_number(),
            flashblock_index: self.latest_flashblock_index(),
            payload_id: self.latest_payload_id(),
            parent_hash: self.latest_header.parent_hash,
            batch_hash: B256::ZERO,
            block_timestamp: Some(self.latest_header.timestamp),
            logs,
            transactions,
        };
        batch.batch_hash = compute_flashblock_logs_batch_hash(&batch);
        batch
    }

    /// Returns a fast logs delta payload from only the latest flashblock.
    ///
    /// When `filter` is `None`, `transactions` contains metadata for every
    /// latest-flashblock transaction with a receipt.
    /// When `filter` is `Some`, `transactions` contains metadata only for
    /// transactions referenced by at least one returned log, keeping the
    /// fast-path payload smaller.
    pub fn get_latest_fast_flashblock_logs_delta(
        &self,
        snapshot_nonce: u64,
        filter: Option<&Filter>,
    ) -> FastFlashblockLogsDelta {
        let latest_block_number = self.latest_block_number();
        let latest_flashblock_index = self.latest_flashblock_index();
        let latest_payload_id = self.latest_payload_id();
        let parent_hash = self.latest_header.parent_hash;
        let block_timestamp = Some(self.latest_header.timestamp);
        let tx_range = self.latest_flashblock_tx_range();
        let mut transactions = Vec::new();
        let mut logs = Vec::new();
        let mut next_block_log_index =
            self.count_receipt_logs_before(latest_block_number, tx_range.start);

        for (relative_tx_index, tx) in self.transactions[tx_range.clone()].iter().enumerate() {
            let absolute_tx_index = tx_range.start + relative_tx_index;
            let tx_hash = tx.tx_hash();
            let tx_index = self.latest_flashblock_tx_index(
                latest_block_number,
                absolute_tx_index,
                tx_hash,
                tx,
            );
            let Some(receipt) = self.transaction_receipts.get(&tx_hash) else {
                continue;
            };

            let tx_meta = FastFlashblockTxMeta {
                hash: tx_hash,
                index: tx_index,
                status: Some(if receipt.inner.inner.status() { 1 } else { 0 }),
            };
            let log_count_before_tx = logs.len();

            for (log_index_in_tx, log) in receipt.inner.logs().iter().enumerate() {
                let log_index_in_block = log.log_index.unwrap_or(next_block_log_index);
                next_block_log_index =
                    next_block_log_index.max(log_index_in_block.saturating_add(1));

                if filter.is_some_and(|filter| !filter.matches(&log.inner)) {
                    continue;
                }

                logs.push(FastFlashblockLog {
                    tx_hash,
                    tx_index,
                    log_index_in_tx: log_index_in_tx as u64,
                    log_index_in_block,
                    address: log.inner.address,
                    topics: log.inner.data.topics().to_vec(),
                    data: log.inner.data.data.clone(),
                });
            }

            if filter.is_none() || logs.len() > log_count_before_tx {
                transactions.push(tx_meta);
            }
        }

        FastFlashblockLogsDelta::new(
            FlashblockSnapshotId::new(
                snapshot_nonce,
                latest_block_number,
                latest_flashblock_index,
                latest_payload_id,
                parent_hash,
            ),
            block_timestamp,
            logs,
            transactions,
        )
    }

    /// Returns transactions with their associated logs from only the latest flashblock (delta).
    ///
    /// Unlike `get_pending_transactions_with_logs`, this returns only transactions
    /// that were added in the most recent flashblock, avoiding duplicates
    /// when streaming via WebSocket subscriptions.
    pub fn get_latest_flashblock_transactions_with_logs(&self) -> Vec<TransactionWithLogs> {
        let prev_count = self.previous_flashblocks_tx_count();

        self.transactions
            .iter()
            .skip(prev_count)
            .filter_map(|tx| {
                self.transaction_receipts
                    .get(&tx.tx_hash())
                    .map(|receipt| Self::transaction_with_logs(tx, receipt))
            })
            .collect()
    }

    /// Returns transactions with their associated logs from only the latest flashblock (delta),
    /// filtered to include only transactions where at least one log matches the given filter.
    ///
    /// When a transaction matches, all of its logs are returned (not just the matching ones).
    /// This preserves full transaction context for subscribers who need complete log sets.
    pub fn get_latest_flashblock_transactions_with_logs_filtered(
        &self,
        filter: &Filter,
    ) -> Vec<TransactionWithLogs> {
        let prev_count = self.previous_flashblocks_tx_count();

        self.transactions
            .iter()
            .skip(prev_count)
            .filter_map(|tx| {
                let receipt = self.transaction_receipts.get(&tx.tx_hash())?;
                let logs = receipt.inner.logs();

                let has_match = logs.iter().any(|log| filter.matches(&log.inner));
                if !has_match {
                    return None;
                }

                Some(Self::transaction_with_logs(tx, receipt))
            })
            .collect()
    }

    /// Returns the hashes of transactions from only the latest flashblock (delta).
    ///
    /// Unlike `get_pending_transaction_hashes`, this returns only hashes
    /// of transactions that were added in the most recent flashblock,
    /// avoiding duplicates when streaming via WebSocket subscriptions.
    pub fn get_latest_flashblock_transaction_hashes(&self) -> Vec<B256> {
        let prev_count = self.previous_flashblocks_tx_count();
        self.transactions.iter().skip(prev_count).map(|tx| tx.tx_hash()).collect()
    }
}

fn compute_flashblock_logs_batch_hash(batch: &FlashblockLogsBatch) -> B256 {
    let mut bytes = Vec::new();
    push_u64(&mut bytes, batch.block_number);
    push_u64(&mut bytes, batch.flashblock_index);
    bytes.extend_from_slice(batch.payload_id.to_string().as_bytes());
    bytes.extend_from_slice(batch.parent_hash.as_slice());

    for tx in &batch.transactions {
        bytes.extend_from_slice(tx.hash.as_slice());
        push_u64(&mut bytes, tx.index);
        push_u64(&mut bytes, tx.status.unwrap_or(u64::MAX));
    }

    for log in &batch.logs {
        bytes.extend_from_slice(log.tx_hash.as_slice());
        push_u64(&mut bytes, log.tx_index);
        push_u64(&mut bytes, log.log_index_in_tx);
        push_u64(&mut bytes, log.log_index_in_block);
        bytes.extend_from_slice(log.address.as_slice());
        push_u64(&mut bytes, log.topics.len() as u64);
        for topic in &log.topics {
            bytes.extend_from_slice(topic.as_slice());
        }
        push_u64(&mut bytes, log.data.len() as u64);
        bytes.extend_from_slice(log.data.as_ref());
        bytes.push(u8::from(log.removed));
    }

    keccak256(bytes)
}

fn push_u64(bytes: &mut Vec<u8>, value: u64) {
    bytes.extend_from_slice(&value.to_be_bytes());
}

impl PendingBlocksAPI for Guard<Option<Arc<PendingBlocks>>> {
    fn get_canonical_block_number(&self) -> BlockNumberOrTag {
        self.as_ref().map(|pb| pb.canonical_block_number()).unwrap_or(BlockNumberOrTag::Latest)
    }

    fn get_transaction_count(&self, address: Address) -> U256 {
        self.as_ref().map(|pb| pb.get_transaction_count(address)).unwrap_or_else(|| U256::from(0))
    }

    fn get_block(&self, full: bool) -> Option<RpcBlock<Base>> {
        self.as_ref().map(|pb| pb.get_latest_block(full))
    }

    fn get_transaction_receipt(
        &self,
        tx_hash: alloy_primitives::TxHash,
    ) -> Option<RpcReceipt<Base>> {
        self.as_ref().and_then(|pb| pb.get_receipt(tx_hash).cloned())
    }

    fn get_transaction_by_hash(
        &self,
        tx_hash: alloy_primitives::TxHash,
    ) -> Option<RpcTransaction<Base>> {
        self.as_ref().and_then(|pb| pb.get_transaction_by_hash(tx_hash).cloned())
    }

    fn get_balance(&self, address: Address) -> Option<U256> {
        self.as_ref().and_then(|pb| pb.get_balance(address))
    }

    fn get_state_overrides(&self) -> Option<StateOverride> {
        self.as_ref().map(|pb| pb.get_state_overrides()).unwrap_or_default()
    }

    fn get_pending_logs(&self, filter: &Filter) -> Vec<Log> {
        self.as_ref().map(|pb| pb.get_pending_logs(filter)).unwrap_or_default()
    }
}

#[cfg(test)]
mod tests {
    use alloy_consensus::{
        Header, Receipt, ReceiptWithBloom, Sealed, Signed, transaction::Recovered,
    };
    use alloy_primitives::{
        Address, B256, Bloom, Bytes, Log as PrimitiveLog, LogData, Signature, TxKind, U256,
    };
    use alloy_provider::network::TransactionResponse;
    use alloy_rpc_types_engine::PayloadId;
    use base_common_consensus::{BaseReceipt, BaseTxEnvelope, TxDeposit};
    use base_common_flashblocks::{
        ExecutionPayloadBaseV1, ExecutionPayloadFlashblockDeltaV1, Flashblock, Metadata,
    };
    use base_common_rpc_types::{BaseTransactionReceipt, L1BlockInfo, Transaction};
    use revm::context_interface::result::ExecutionResult;

    use super::*;

    fn test_sender() -> Address {
        Address::repeat_byte(0x01)
    }

    fn test_flashblock() -> Flashblock {
        Flashblock {
            payload_id: PayloadId::default(),
            index: 0,
            base: Some(ExecutionPayloadBaseV1 {
                parent_beacon_block_root: B256::ZERO,
                parent_hash: B256::ZERO,
                fee_recipient: Address::ZERO,
                prev_randao: B256::ZERO,
                block_number: 1,
                gas_limit: 30_000_000,
                timestamp: 1_700_000_000,
                extra_data: Bytes::default(),
                base_fee_per_gas: U256::from(1_000_000_000u64),
            }),
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
            metadata: Metadata { block_number: 1 },
        }
    }

    fn test_legacy_transaction() -> Transaction {
        Transaction {
            inner: alloy_rpc_types_eth::Transaction {
                inner: Recovered::new_unchecked(
                    BaseTxEnvelope::Legacy(alloy_consensus::Signed::new_unchecked(
                        alloy_consensus::TxLegacy::default(),
                        Signature::test_signature(),
                        B256::ZERO,
                    )),
                    test_sender(),
                ),
                block_hash: None,
                block_number: Some(1),
                transaction_index: Some(0),
                effective_gas_price: Some(1_000_000_000),
            },
            deposit_nonce: None,
            deposit_receipt_version: None,
        }
    }

    /// Creates a [`Transaction`] whose `tx_hash()` equals `hash`.
    fn test_transaction_with_hash(hash: B256) -> Transaction {
        let legacy = alloy_consensus::TxLegacy {
            chain_id: Some(1),
            nonce: 0,
            gas_price: 1_000_000_000,
            gas_limit: 21_000,
            to: TxKind::Call(Address::ZERO),
            value: U256::ZERO,
            input: Bytes::new(),
        };
        let envelope = BaseTxEnvelope::Legacy(Signed::new_unchecked(
            legacy,
            Signature::test_signature(),
            hash,
        ));
        let recovered = Recovered::new_unchecked(envelope, Address::ZERO);
        Transaction {
            inner: alloy_rpc_types_eth::Transaction {
                inner: recovered,
                block_hash: Some(B256::ZERO),
                block_number: Some(1),
                transaction_index: Some(0),
                effective_gas_price: Some(1_000_000_000),
            },
            deposit_nonce: None,
            deposit_receipt_version: None,
        }
    }

    fn test_deposit_transaction() -> Transaction {
        let deposit = TxDeposit {
            source_hash: B256::repeat_byte(0xdd),
            from: test_sender(),
            to: alloy_primitives::TxKind::Call(Address::repeat_byte(0x02)),
            mint: 0,
            value: U256::ZERO,
            gas_limit: 21000,
            is_system_transaction: false,
            input: Bytes::new(),
        };
        Transaction {
            inner: alloy_rpc_types_eth::Transaction {
                inner: Recovered::new_unchecked(
                    BaseTxEnvelope::Deposit(Sealed::new_unchecked(deposit, B256::ZERO)),
                    test_sender(),
                ),
                block_hash: None,
                block_number: Some(1),
                transaction_index: Some(0),
                effective_gas_price: Some(0),
            },
            deposit_nonce: Some(42),
            deposit_receipt_version: Some(1),
        }
    }

    fn test_receipt(tx_hash: B256, blob_gas_used: Option<u64>) -> BaseTransactionReceipt {
        BaseTransactionReceipt {
            inner: alloy_rpc_types_eth::TransactionReceipt {
                inner: ReceiptWithBloom {
                    receipt: BaseReceipt::Legacy(Receipt {
                        status: alloy_consensus::Eip658Value::Eip658(true),
                        cumulative_gas_used: 21000,
                        logs: vec![],
                    }),
                    logs_bloom: Bloom::default(),
                },
                transaction_hash: tx_hash,
                transaction_index: Some(0),
                block_hash: None,
                block_number: Some(1),
                gas_used: 21000,
                effective_gas_price: 1_000_000_000,
                blob_gas_used,
                blob_gas_price: None,
                from: test_sender(),
                to: None,
                contract_address: None,
            },
            l1_block_info: L1BlockInfo::default(),
        }
    }

    /// Creates an [`BaseTransactionReceipt`] with a single log emitted from `log_address`.
    fn test_receipt_with_log(tx_hash: B256, log_address: Address) -> BaseTransactionReceipt {
        let log = Log {
            inner: PrimitiveLog {
                address: log_address,
                data: LogData::new_unchecked(vec![], Bytes::new()),
            },
            block_hash: Some(B256::ZERO),
            block_number: Some(1),
            block_timestamp: None,
            transaction_hash: Some(tx_hash),
            transaction_index: Some(0),
            log_index: Some(0),
            removed: false,
        };

        BaseTransactionReceipt {
            inner: alloy_rpc_types_eth::TransactionReceipt {
                inner: ReceiptWithBloom {
                    receipt: BaseReceipt::Legacy(Receipt {
                        status: alloy_consensus::Eip658Value::Eip658(true),
                        cumulative_gas_used: 21_000,
                        logs: vec![log],
                    }),
                    logs_bloom: Bloom::default(),
                },
                transaction_hash: tx_hash,
                transaction_index: Some(0),
                block_hash: Some(B256::ZERO),
                block_number: Some(1),
                gas_used: 21_000,
                effective_gas_price: 1_000_000_000,
                blob_gas_used: None,
                blob_gas_price: None,
                from: Address::ZERO,
                to: None,
                contract_address: None,
            },
            l1_block_info: Default::default(),
        }
    }

    fn test_receipt_with_subscription_fields(
        tx_hash: B256,
        log_address: Address,
        contract_address: Address,
        logs_bloom: Bloom,
    ) -> BaseTransactionReceipt {
        let mut receipt = test_receipt_with_log(tx_hash, log_address);
        receipt.inner.inner.receipt.as_receipt_mut().status =
            alloy_consensus::Eip658Value::Eip658(true);
        receipt.inner.inner.receipt.as_receipt_mut().cumulative_gas_used = 42_000;
        receipt.inner.inner.logs_bloom = logs_bloom;
        receipt.inner.contract_address = Some(contract_address);
        receipt
    }

    fn test_execution_result() -> ExecutionResult<BaseHaltReason> {
        ExecutionResult::Success {
            reason: revm::context::result::SuccessReason::Stop,
            gas_used: 21000,
            gas_refunded: 0,
            logs: vec![],
            output: revm::context::result::Output::Call(Bytes::new()),
        }
    }

    fn build_pending_blocks(tx: Transaction, blob_gas_used: Option<u64>) -> (B256, PendingBlocks) {
        let tx_hash = tx.tx_hash();
        let mut builder = PendingBlocksBuilder::default();
        builder.with_flashblocks([test_flashblock()]);
        builder.with_header(Sealed::new_unchecked(Header::default(), B256::ZERO));
        builder.with_transaction(tx);
        builder.with_transaction_sender(tx_hash, test_sender());
        builder.with_transaction_state(tx_hash, Default::default());
        builder.with_transaction_result(tx_hash, test_execution_result());
        builder.with_receipt(tx_hash, test_receipt(tx_hash, blob_gas_used));
        (tx_hash, builder.build().expect("should build pending blocks"))
    }

    /// Builds a [`PendingBlocks`] with the supplied (hash, `log_address`) pairs
    /// inserted in the given order.
    fn build_pending_blocks_with_logs(entries: &[(B256, Address)]) -> PendingBlocks {
        let header = Sealed::new_unchecked(Header::default(), B256::ZERO);
        let mut builder = PendingBlocksBuilder::new();
        builder.with_flashblocks([test_flashblock()]);
        builder.with_header(header);

        for &(hash, addr) in entries {
            builder.with_transaction(test_transaction_with_hash(hash));
            builder.with_receipt(hash, test_receipt_with_log(hash, addr));
        }

        builder.build().expect("build should succeed")
    }

    #[test]
    fn get_tx_result_reconstructs_all_fields_for_legacy_tx() {
        let da_footprint = 42_000u64;
        let (tx_hash, pending_blocks) =
            build_pending_blocks(test_legacy_transaction(), Some(da_footprint));

        let result = pending_blocks.get_tx_result(&tx_hash).expect("should return tx result");

        assert_eq!(result.inner.blob_gas_used, da_footprint);
        assert_eq!(result.inner.tx_type, OpTxType::Legacy);
        assert!(!result.is_deposit);
        assert_eq!(result.sender, test_sender());
        assert_eq!(result.inner.result.result.gas_used(), 21000);
    }

    #[test]
    fn get_tx_result_reconstructs_all_fields_for_deposit_tx() {
        let (tx_hash, pending_blocks) = build_pending_blocks(test_deposit_transaction(), Some(0));

        let result = pending_blocks.get_tx_result(&tx_hash).expect("should return tx result");

        assert_eq!(result.inner.blob_gas_used, 0);
        assert_eq!(result.inner.tx_type, OpTxType::Deposit);
        assert!(result.is_deposit);
        assert_eq!(result.sender, test_sender());
        assert_eq!(result.inner.result.result.gas_used(), 21000);
    }

    #[test]
    fn get_tx_result_defaults_blob_gas_to_zero_when_receipt_field_is_none() {
        let (tx_hash, pending_blocks) = build_pending_blocks(test_legacy_transaction(), None);

        let result = pending_blocks.get_tx_result(&tx_hash).expect("should return tx result");

        assert_eq!(result.inner.blob_gas_used, 0);
    }

    #[test]
    fn build_rejects_duplicate_transaction() {
        let tx = test_legacy_transaction();
        let tx_hash = tx.tx_hash();
        let mut builder = PendingBlocksBuilder::default();
        builder.with_flashblocks([test_flashblock()]);
        builder.with_header(Sealed::new_unchecked(Header::default(), B256::ZERO));
        builder.with_transaction(tx.clone());
        builder.with_transaction(tx);
        builder.with_transaction_sender(tx_hash, test_sender());
        builder.with_transaction_state(tx_hash, Default::default());
        builder.with_transaction_result(tx_hash, test_execution_result());
        builder.with_receipt(tx_hash, test_receipt(tx_hash, None));

        let err = builder.build().expect_err("build should fail on duplicate tx");
        assert_eq!(err, StateProcessorError::Build(BuildError::DuplicateTransaction { tx_hash }));
    }

    #[test]
    fn get_tx_result_defaults_blob_gas_to_zero_without_receipt() {
        let tx = test_legacy_transaction();
        let tx_hash = tx.tx_hash();
        let mut builder = PendingBlocksBuilder::default();
        builder.with_flashblocks([test_flashblock()]);
        builder.with_header(Sealed::new_unchecked(Header::default(), B256::ZERO));
        builder.with_transaction(tx);
        builder.with_transaction_sender(tx_hash, test_sender());
        builder.with_transaction_state(tx_hash, Default::default());
        builder.with_transaction_result(tx_hash, test_execution_result());
        // Intentionally skip with_receipt to verify pending blocks reject incomplete transactions.
        let err = builder.build().expect_err("build should fail without a receipt");

        assert_eq!(err, StateProcessorError::Build(BuildError::MissingReceipt { tx_hash }));
    }

    fn test_receipt_with_log_and_topic(
        tx_hash: B256,
        log_address: Address,
        topic0: B256,
    ) -> BaseTransactionReceipt {
        let log = Log {
            inner: PrimitiveLog {
                address: log_address,
                data: LogData::new_unchecked(vec![topic0], Bytes::new()),
            },
            block_hash: Some(B256::ZERO),
            block_number: Some(1),
            block_timestamp: None,
            transaction_hash: Some(tx_hash),
            transaction_index: Some(0),
            log_index: Some(0),
            removed: false,
        };

        BaseTransactionReceipt {
            inner: alloy_rpc_types_eth::TransactionReceipt {
                inner: ReceiptWithBloom {
                    receipt: BaseReceipt::Legacy(Receipt {
                        status: alloy_consensus::Eip658Value::Eip658(true),
                        cumulative_gas_used: 21_000,
                        logs: vec![log],
                    }),
                    logs_bloom: Bloom::default(),
                },
                transaction_hash: tx_hash,
                transaction_index: Some(0),
                block_hash: Some(B256::ZERO),
                block_number: Some(1),
                gas_used: 21_000,
                effective_gas_price: 1_000_000_000,
                blob_gas_used: None,
                blob_gas_price: None,
                from: Address::ZERO,
                to: None,
                contract_address: None,
            },
            l1_block_info: Default::default(),
        }
    }

    fn build_pending_blocks_with_topics(entries: &[(B256, Address, B256)]) -> PendingBlocks {
        let header = Sealed::new_unchecked(Header::default(), B256::ZERO);
        let mut builder = PendingBlocksBuilder::new();
        builder.with_flashblocks([test_flashblock()]);
        builder.with_header(header);

        for &(hash, addr, topic) in entries {
            builder.with_transaction(test_transaction_with_hash(hash));
            builder.with_receipt(hash, test_receipt_with_log_and_topic(hash, addr, topic));
        }

        builder.build().expect("build should succeed")
    }

    fn test_flashblock_with_index_and_tx_count(index: u64, tx_count: usize) -> Flashblock {
        let mut flashblock = test_flashblock();
        flashblock.index = index;
        flashblock.payload_id = PayloadId::new([index as u8; 8]);
        flashblock.diff.transactions = vec![Bytes::new(); tx_count];
        flashblock
    }

    fn test_flashblock_with_index_tx_count_and_block_number(
        index: u64,
        tx_count: usize,
        block_number: u64,
    ) -> Flashblock {
        let mut flashblock = test_flashblock_with_index_and_tx_count(index, tx_count);
        if let Some(base) = flashblock.base.as_mut() {
            base.block_number = block_number;
        }
        flashblock.metadata.block_number = block_number;
        flashblock
    }

    fn test_header() -> Sealed<Header> {
        Sealed::new_unchecked(
            Header {
                parent_hash: B256::with_last_byte(0x42),
                number: 1,
                timestamp: 1_700_000_000,
                ..Default::default()
            },
            B256::ZERO,
        )
    }

    fn test_header_for_block(number: u64, parent_hash: B256) -> Sealed<Header> {
        Sealed::new_unchecked(
            Header { parent_hash, number, timestamp: 1_700_000_000 + number, ..Default::default() },
            B256::ZERO,
        )
    }

    fn test_transaction_with_hash_for_block(
        hash: B256,
        block_number: u64,
        transaction_index: Option<u64>,
    ) -> Transaction {
        let mut tx = test_transaction_with_hash(hash);
        tx.inner.block_number = Some(block_number);
        tx.inner.transaction_index = transaction_index;
        tx
    }

    fn test_log(tx_hash: B256, log_address: Address, log_index: Option<u64>, removed: bool) -> Log {
        Log {
            inner: PrimitiveLog {
                address: log_address,
                data: LogData::new_unchecked(vec![], Bytes::new()),
            },
            block_hash: Some(B256::ZERO),
            block_number: Some(1),
            block_timestamp: None,
            transaction_hash: Some(tx_hash),
            transaction_index: Some(0),
            log_index,
            removed,
        }
    }

    fn test_log_for_block(
        tx_hash: B256,
        block_number: u64,
        transaction_index: Option<u64>,
        log_address: Address,
        log_index: Option<u64>,
        removed: bool,
    ) -> Log {
        let mut log = test_log(tx_hash, log_address, log_index, removed);
        log.block_number = Some(block_number);
        log.transaction_index = transaction_index;
        log
    }

    fn test_receipt_with_logs(tx_hash: B256, logs: Vec<Log>) -> BaseTransactionReceipt {
        BaseTransactionReceipt {
            inner: alloy_rpc_types_eth::TransactionReceipt {
                inner: ReceiptWithBloom {
                    receipt: BaseReceipt::Legacy(Receipt {
                        status: alloy_consensus::Eip658Value::Eip658(true),
                        cumulative_gas_used: 21_000,
                        logs,
                    }),
                    logs_bloom: Bloom::default(),
                },
                transaction_hash: tx_hash,
                transaction_index: Some(0),
                block_hash: Some(B256::ZERO),
                block_number: Some(1),
                gas_used: 21_000,
                effective_gas_price: 1_000_000_000,
                blob_gas_used: None,
                blob_gas_price: None,
                from: Address::ZERO,
                to: None,
                contract_address: None,
            },
            l1_block_info: Default::default(),
        }
    }

    fn test_receipt_with_logs_for_block(
        tx_hash: B256,
        block_number: u64,
        transaction_index: Option<u64>,
        logs: Vec<Log>,
    ) -> BaseTransactionReceipt {
        let mut receipt = test_receipt_with_logs(tx_hash, logs);
        receipt.inner.block_number = Some(block_number);
        receipt.inner.transaction_index = transaction_index;
        receipt
    }

    fn build_pending_blocks_for_flashblock_batch_tests(
        flashblocks: Vec<Flashblock>,
        headers: Vec<Sealed<Header>>,
        entries: Vec<(Transaction, BaseTransactionReceipt)>,
    ) -> PendingBlocks {
        let mut builder = PendingBlocksBuilder::new();
        builder.with_flashblocks(flashblocks);

        for header in headers {
            builder.with_header(header);
        }

        for (tx, receipt) in entries {
            let hash = tx.tx_hash();
            builder.with_transaction(tx);
            builder.with_receipt(hash, receipt);
        }

        builder.build().expect("build should succeed")
    }

    fn build_pending_blocks_for_latest_flashblock_batch_tests(
        flashblocks: Vec<Flashblock>,
        entries: Vec<(B256, BaseTransactionReceipt)>,
    ) -> PendingBlocks {
        let entries = entries
            .into_iter()
            .enumerate()
            .map(|(index, (hash, receipt))| {
                let mut tx = test_transaction_with_hash(hash);
                tx.inner.transaction_index = Some(index as u64);
                (tx, receipt)
            })
            .collect();

        build_pending_blocks_for_flashblock_batch_tests(flashblocks, vec![test_header()], entries)
    }

    #[test]
    fn latest_flashblock_logs_batch_emits_empty_flashblock() {
        let prev_hash = B256::with_last_byte(0xAA);
        let prev_addr = Address::with_last_byte(0x0A);

        let pending = build_pending_blocks_for_latest_flashblock_batch_tests(
            vec![
                test_flashblock_with_index_and_tx_count(0, 1),
                test_flashblock_with_index_and_tx_count(1, 0),
            ],
            vec![(prev_hash, test_receipt_with_log(prev_hash, prev_addr))],
        );

        let batch = pending.get_latest_flashblock_logs_batch(None);

        assert_eq!(batch.flashblock_index, 1);
        assert!(batch.logs.is_empty());
        assert!(batch.transactions.is_empty());
        assert_eq!(batch.batch_hash, compute_flashblock_logs_batch_hash(&batch));
    }

    #[test]
    fn latest_flashblock_logs_batch_includes_tx_meta_for_all_latest_txs() {
        let prev_hash = B256::with_last_byte(0xAA);
        let latest_hash_a = B256::with_last_byte(0xBB);
        let latest_hash_b = B256::with_last_byte(0xCC);

        let pending = build_pending_blocks_for_latest_flashblock_batch_tests(
            vec![
                test_flashblock_with_index_and_tx_count(0, 1),
                test_flashblock_with_index_and_tx_count(1, 2),
            ],
            vec![
                (prev_hash, test_receipt_with_log(prev_hash, Address::with_last_byte(0x01))),
                (
                    latest_hash_a,
                    test_receipt_with_log(latest_hash_a, Address::with_last_byte(0x02)),
                ),
                (latest_hash_b, test_receipt(latest_hash_b, None)),
            ],
        );

        let batch = pending.get_latest_flashblock_logs_batch(None);

        assert_eq!(batch.transactions.len(), 2);
        assert_eq!(batch.transactions[0].hash, latest_hash_a);
        assert_eq!(batch.transactions[0].index, 1);
        assert_eq!(batch.transactions[0].status, Some(1));
        assert_eq!(batch.transactions[1].hash, latest_hash_b);
        assert_eq!(batch.transactions[1].index, 2);
        assert_eq!(batch.transactions[1].status, Some(1));
    }

    #[test]
    fn latest_flashblock_logs_batch_filters_logs_but_keeps_tx_meta() {
        let prev_hash = B256::with_last_byte(0xAA);
        let latest_hash_a = B256::with_last_byte(0xBB);
        let latest_hash_b = B256::with_last_byte(0xCC);
        let keep_addr = Address::with_last_byte(0x0B);
        let drop_addr = Address::with_last_byte(0x0C);

        let pending = build_pending_blocks_for_latest_flashblock_batch_tests(
            vec![
                test_flashblock_with_index_and_tx_count(0, 1),
                test_flashblock_with_index_and_tx_count(1, 2),
            ],
            vec![
                (prev_hash, test_receipt_with_log(prev_hash, Address::with_last_byte(0x01))),
                (latest_hash_a, test_receipt_with_log(latest_hash_a, keep_addr)),
                (latest_hash_b, test_receipt_with_log(latest_hash_b, drop_addr)),
            ],
        );

        let filter = Filter::new().address(keep_addr);
        let batch = pending.get_latest_flashblock_logs_batch(Some(&filter));

        assert_eq!(batch.logs.len(), 1);
        assert_eq!(batch.logs[0].tx_hash, latest_hash_a);
        assert_eq!(batch.logs[0].address, keep_addr);
        assert_eq!(
            batch.transactions.iter().map(|tx| tx.hash).collect::<Vec<_>>(),
            vec![latest_hash_a, latest_hash_b]
        );
    }

    #[test]
    fn latest_flashblock_logs_batch_sets_tx_local_and_block_global_log_indices() {
        let prev_hash = B256::with_last_byte(0xAA);
        let latest_hash_a = B256::with_last_byte(0xBB);
        let latest_hash_b = B256::with_last_byte(0xCC);

        let pending = build_pending_blocks_for_latest_flashblock_batch_tests(
            vec![
                test_flashblock_with_index_and_tx_count(0, 1),
                test_flashblock_with_index_and_tx_count(1, 2),
            ],
            vec![
                (
                    prev_hash,
                    test_receipt_with_logs(
                        prev_hash,
                        vec![test_log(prev_hash, Address::with_last_byte(0x01), None, false)],
                    ),
                ),
                (
                    latest_hash_a,
                    test_receipt_with_logs(
                        latest_hash_a,
                        vec![
                            test_log(latest_hash_a, Address::with_last_byte(0x02), None, false),
                            test_log(latest_hash_a, Address::with_last_byte(0x03), None, true),
                        ],
                    ),
                ),
                (
                    latest_hash_b,
                    test_receipt_with_logs(
                        latest_hash_b,
                        vec![test_log(latest_hash_b, Address::with_last_byte(0x04), None, false)],
                    ),
                ),
            ],
        );

        let batch = pending.get_latest_flashblock_logs_batch(None);

        assert_eq!(batch.logs.len(), 3);
        assert_eq!(batch.logs[0].tx_hash, latest_hash_a);
        assert_eq!(batch.logs[0].log_index_in_tx, 0);
        assert_eq!(batch.logs[0].log_index_in_block, 1);
        assert_eq!(batch.logs[1].tx_hash, latest_hash_a);
        assert_eq!(batch.logs[1].log_index_in_tx, 1);
        assert_eq!(batch.logs[1].log_index_in_block, 2);
        assert!(batch.logs[1].removed);
        assert_eq!(batch.logs[2].tx_hash, latest_hash_b);
        assert_eq!(batch.logs[2].log_index_in_tx, 0);
        assert_eq!(batch.logs[2].log_index_in_block, 3);
    }

    #[test]
    fn latest_flashblock_logs_batch_uses_latest_block_local_tx_and_log_indexes_across_pending_blocks()
     {
        let prev_hash = B256::with_last_byte(0xAA);
        let latest_hash = B256::with_last_byte(0xBB);

        let pending = build_pending_blocks_for_flashblock_batch_tests(
            vec![
                test_flashblock_with_index_tx_count_and_block_number(0, 1, 1),
                test_flashblock_with_index_tx_count_and_block_number(1, 1, 2),
            ],
            vec![
                test_header_for_block(1, B256::with_last_byte(0x11)),
                test_header_for_block(2, B256::with_last_byte(0x22)),
            ],
            vec![
                (
                    test_transaction_with_hash_for_block(prev_hash, 1, Some(0)),
                    test_receipt_with_logs_for_block(
                        prev_hash,
                        1,
                        Some(0),
                        vec![test_log_for_block(
                            prev_hash,
                            1,
                            Some(0),
                            Address::with_last_byte(0x01),
                            None,
                            false,
                        )],
                    ),
                ),
                (
                    test_transaction_with_hash_for_block(latest_hash, 2, Some(0)),
                    test_receipt_with_logs_for_block(
                        latest_hash,
                        2,
                        Some(0),
                        vec![test_log_for_block(
                            latest_hash,
                            2,
                            Some(0),
                            Address::with_last_byte(0x02),
                            None,
                            false,
                        )],
                    ),
                ),
            ],
        );

        let batch = pending.get_latest_flashblock_logs_batch(None);

        assert_eq!(batch.transactions.len(), 1);
        assert_eq!(batch.transactions[0].hash, latest_hash);
        assert_eq!(batch.transactions[0].index, 0);
        assert_eq!(batch.logs.len(), 1);
        assert_eq!(batch.logs[0].tx_hash, latest_hash);
        assert_eq!(batch.logs[0].tx_index, 0);
        assert_eq!(batch.logs[0].log_index_in_block, 0);
    }

    #[test]
    fn latest_flashblock_logs_batch_uses_latest_flashblock_metadata_across_pending_blocks() {
        let earlier_payload_id = PayloadId::new([0x11; 8]);
        let latest_payload_id = PayloadId::new([0x22; 8]);
        let earlier_parent_hash = B256::with_last_byte(0x33);
        let latest_parent_hash = B256::with_last_byte(0x44);

        let mut earlier_flashblock = test_flashblock_with_index_tx_count_and_block_number(0, 0, 1);
        earlier_flashblock.payload_id = earlier_payload_id;
        let mut latest_flashblock = test_flashblock_with_index_tx_count_and_block_number(1, 0, 2);
        latest_flashblock.payload_id = latest_payload_id;

        let pending = build_pending_blocks_for_flashblock_batch_tests(
            vec![earlier_flashblock, latest_flashblock],
            vec![
                test_header_for_block(1, earlier_parent_hash),
                test_header_for_block(2, latest_parent_hash),
            ],
            vec![],
        );

        let batch = pending.get_latest_flashblock_logs_batch(None);

        assert_eq!(batch.payload_id, latest_payload_id);
        assert_eq!(batch.parent_hash, latest_parent_hash);
    }

    #[test]
    fn latest_flashblock_logs_batch_clamps_mismatched_transaction_counts() {
        let latest_hash = B256::with_last_byte(0xBB);

        let pending = build_pending_blocks_for_flashblock_batch_tests(
            vec![
                test_flashblock_with_index_tx_count_and_block_number(0, 2, 1),
                test_flashblock_with_index_tx_count_and_block_number(1, 1, 2),
            ],
            vec![
                test_header_for_block(1, B256::with_last_byte(0x11)),
                test_header_for_block(2, B256::with_last_byte(0x22)),
            ],
            vec![(
                test_transaction_with_hash_for_block(latest_hash, 2, Some(0)),
                test_receipt_with_logs_for_block(latest_hash, 2, Some(0), vec![]),
            )],
        );

        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            pending.get_latest_flashblock_logs_batch(None)
        }));

        assert!(result.is_ok(), "latest flashblock logs batch should not panic");

        let batch = result.expect("batch should be returned safely");
        assert!(batch.transactions.is_empty());
        assert!(batch.logs.is_empty());
    }

    #[test]
    fn latest_flashblock_logs_batch_preserves_filtered_block_global_log_positions() {
        let latest_hash = B256::with_last_byte(0xBB);
        let keep_addr = Address::with_last_byte(0x0B);
        let drop_addr = Address::with_last_byte(0x0C);

        let pending = build_pending_blocks_for_flashblock_batch_tests(
            vec![
                test_flashblock_with_index_tx_count_and_block_number(0, 0, 1),
                test_flashblock_with_index_tx_count_and_block_number(1, 1, 2),
            ],
            vec![
                test_header_for_block(1, B256::with_last_byte(0x11)),
                test_header_for_block(2, B256::with_last_byte(0x22)),
            ],
            vec![(
                test_transaction_with_hash_for_block(latest_hash, 2, Some(0)),
                test_receipt_with_logs_for_block(
                    latest_hash,
                    2,
                    Some(0),
                    vec![
                        test_log_for_block(latest_hash, 2, Some(0), drop_addr, Some(7), false),
                        test_log_for_block(latest_hash, 2, Some(0), keep_addr, None, false),
                    ],
                ),
            )],
        );

        let filter = Filter::new().address(keep_addr);
        let batch = pending.get_latest_flashblock_logs_batch(Some(&filter));

        assert_eq!(batch.transactions.len(), 1);
        assert_eq!(batch.logs.len(), 1);
        assert_eq!(batch.logs[0].address, keep_addr);
        assert_eq!(batch.logs[0].log_index_in_block, 8);
    }

    #[test]
    fn latest_flashblock_logs_batch_hash_changes_with_payload_contents() {
        let tx_hash = B256::with_last_byte(0xBB);

        let pending = build_pending_blocks_for_latest_flashblock_batch_tests(
            vec![test_flashblock_with_index_and_tx_count(0, 1)],
            vec![(
                tx_hash,
                test_receipt_with_logs(
                    tx_hash,
                    vec![test_log(tx_hash, Address::with_last_byte(0x02), Some(0), false)],
                ),
            )],
        );

        let batch = pending.get_latest_flashblock_logs_batch(None);
        let original_hash = compute_flashblock_logs_batch_hash(&batch);

        let mut tx_mutation = batch.clone();
        tx_mutation.transactions[0].index = tx_mutation.transactions[0].index.saturating_add(1);
        assert_ne!(compute_flashblock_logs_batch_hash(&tx_mutation), original_hash);

        let mut log_mutation = batch.clone();
        log_mutation.logs[0].removed = true;
        assert_ne!(compute_flashblock_logs_batch_hash(&log_mutation), original_hash);

        let mut batch_hash_mutation = batch;
        batch_hash_mutation.batch_hash = B256::with_last_byte(0xFF);
        assert_eq!(compute_flashblock_logs_batch_hash(&batch_hash_mutation), original_hash);
    }

    #[test]
    fn latest_fast_logs_delta_matches_batch_contents_unfiltered() {
        let prev_hash = B256::with_last_byte(0xAA);
        let latest_hash_a = B256::with_last_byte(0xBB);
        let latest_hash_b = B256::with_last_byte(0xCC);
        let snapshot_nonce = 7;

        let pending = build_pending_blocks_for_latest_flashblock_batch_tests(
            vec![
                test_flashblock_with_index_and_tx_count(0, 1),
                test_flashblock_with_index_and_tx_count(1, 2),
            ],
            vec![
                (
                    prev_hash,
                    test_receipt_with_logs(
                        prev_hash,
                        vec![test_log(prev_hash, Address::with_last_byte(0x01), Some(0), false)],
                    ),
                ),
                (
                    latest_hash_a,
                    test_receipt_with_logs(
                        latest_hash_a,
                        vec![
                            test_log(latest_hash_a, Address::with_last_byte(0x02), None, false),
                            test_log(latest_hash_a, Address::with_last_byte(0x03), None, true),
                        ],
                    ),
                ),
                (
                    latest_hash_b,
                    test_receipt_with_logs(
                        latest_hash_b,
                        vec![test_log(
                            latest_hash_b,
                            Address::with_last_byte(0x04),
                            Some(7),
                            false,
                        )],
                    ),
                ),
            ],
        );

        let batch = pending.get_latest_flashblock_logs_batch(None);
        let delta = pending.get_latest_fast_flashblock_logs_delta(snapshot_nonce, None);

        assert_eq!(delta.snapshot_id.nonce(), snapshot_nonce);
        assert_eq!(delta.snapshot_id.block_number(), batch.block_number);
        assert_eq!(delta.snapshot_id.flashblock_index(), batch.flashblock_index);
        assert_eq!(delta.snapshot_id.payload_id(), batch.payload_id);
        assert_eq!(delta.snapshot_id.parent_hash(), batch.parent_hash);
        assert_eq!(delta.block_number, batch.block_number);
        assert_eq!(delta.flashblock_index, batch.flashblock_index);
        assert_eq!(delta.payload_id, batch.payload_id);
        assert_eq!(delta.parent_hash, batch.parent_hash);
        assert_eq!(delta.block_timestamp, batch.block_timestamp);
        assert_eq!(delta.logs.len(), batch.logs.len());
        assert_eq!(delta.transactions.len(), batch.transactions.len());

        for (fast_tx, batch_tx) in delta.transactions.iter().zip(batch.transactions.iter()) {
            assert_eq!(fast_tx.hash, batch_tx.hash);
            assert_eq!(fast_tx.index, batch_tx.index);
            assert_eq!(fast_tx.status, batch_tx.status);
        }

        for (fast_log, batch_log) in delta.logs.iter().zip(batch.logs.iter()) {
            assert_eq!(fast_log.tx_hash, batch_log.tx_hash);
            assert_eq!(fast_log.tx_index, batch_log.tx_index);
            assert_eq!(fast_log.log_index_in_tx, batch_log.log_index_in_tx);
            assert_eq!(fast_log.log_index_in_block, batch_log.log_index_in_block);
            assert_eq!(fast_log.address, batch_log.address);
            assert_eq!(fast_log.topics, batch_log.topics);
            assert_eq!(fast_log.data, batch_log.data);
        }
    }

    #[test]
    fn latest_fast_logs_delta_filters_logs_like_batch() {
        let latest_hash = B256::with_last_byte(0xBB);
        let keep_addr = Address::with_last_byte(0x0B);
        let drop_addr = Address::with_last_byte(0x0C);

        let pending = build_pending_blocks_for_flashblock_batch_tests(
            vec![
                test_flashblock_with_index_tx_count_and_block_number(0, 0, 1),
                test_flashblock_with_index_tx_count_and_block_number(1, 1, 2),
            ],
            vec![
                test_header_for_block(1, B256::with_last_byte(0x11)),
                test_header_for_block(2, B256::with_last_byte(0x22)),
            ],
            vec![(
                test_transaction_with_hash_for_block(latest_hash, 2, Some(0)),
                test_receipt_with_logs_for_block(
                    latest_hash,
                    2,
                    Some(0),
                    vec![
                        test_log_for_block(latest_hash, 2, Some(0), drop_addr, Some(7), false),
                        test_log_for_block(latest_hash, 2, Some(0), keep_addr, None, false),
                    ],
                ),
            )],
        );

        let filter = Filter::new().address(keep_addr);
        let batch = pending.get_latest_flashblock_logs_batch(Some(&filter));
        let delta = pending.get_latest_fast_flashblock_logs_delta(11, Some(&filter));

        assert_eq!(delta.logs.len(), 1);
        assert_eq!(delta.logs[0].address, keep_addr);
        assert_eq!(delta.logs[0].log_index_in_block, 8);
        assert_eq!(delta.transactions.len(), 1);
        assert_eq!(delta.transactions[0].hash, latest_hash);
        assert_eq!(delta.transactions[0].index, 0);
        assert_eq!(delta.transactions[0].status, Some(1));

        for (fast_log, batch_log) in delta.logs.iter().zip(batch.logs.iter()) {
            assert_eq!(fast_log.tx_hash, batch_log.tx_hash);
            assert_eq!(fast_log.tx_index, batch_log.tx_index);
            assert_eq!(fast_log.log_index_in_tx, batch_log.log_index_in_tx);
            assert_eq!(fast_log.log_index_in_block, batch_log.log_index_in_block);
            assert_eq!(fast_log.address, batch_log.address);
            assert_eq!(fast_log.topics, batch_log.topics);
            assert_eq!(fast_log.data, batch_log.data);
        }
    }

    #[test]
    fn latest_fast_logs_delta_filtered_transactions_include_only_matching_log_sources() {
        let latest_hash_a = B256::with_last_byte(0xBB);
        let latest_hash_b = B256::with_last_byte(0xCC);
        let keep_addr = Address::with_last_byte(0x0B);
        let drop_addr = Address::with_last_byte(0x0C);

        let pending = build_pending_blocks_for_flashblock_batch_tests(
            vec![
                test_flashblock_with_index_tx_count_and_block_number(0, 0, 1),
                test_flashblock_with_index_tx_count_and_block_number(1, 2, 2),
            ],
            vec![
                test_header_for_block(1, B256::with_last_byte(0x11)),
                test_header_for_block(2, B256::with_last_byte(0x22)),
            ],
            vec![
                (
                    test_transaction_with_hash_for_block(latest_hash_a, 2, Some(0)),
                    test_receipt_with_logs_for_block(
                        latest_hash_a,
                        2,
                        Some(0),
                        vec![test_log_for_block(
                            latest_hash_a,
                            2,
                            Some(0),
                            keep_addr,
                            Some(0),
                            false,
                        )],
                    ),
                ),
                (
                    test_transaction_with_hash_for_block(latest_hash_b, 2, Some(1)),
                    test_receipt_with_logs_for_block(
                        latest_hash_b,
                        2,
                        Some(1),
                        vec![test_log_for_block(
                            latest_hash_b,
                            2,
                            Some(1),
                            drop_addr,
                            Some(1),
                            false,
                        )],
                    ),
                ),
            ],
        );

        let filter = Filter::new().address(keep_addr);
        let delta = pending.get_latest_fast_flashblock_logs_delta(11, Some(&filter));

        assert_eq!(delta.logs.len(), 1);
        assert_eq!(delta.logs[0].tx_hash, latest_hash_a);
        assert_eq!(delta.logs[0].address, keep_addr);
        assert_eq!(delta.transactions.len(), 1);
        assert_eq!(delta.transactions[0].hash, latest_hash_a);
        assert_eq!(delta.transactions[0].index, 0);
        assert_eq!(delta.transactions[0].status, Some(1));
    }

    #[test]
    fn latest_fast_logs_delta_filtered_returns_no_transactions_when_no_logs_match() {
        let latest_hash = B256::with_last_byte(0xBB);
        let log_addr = Address::with_last_byte(0x0B);
        let unmatched_addr = Address::with_last_byte(0x0C);

        let pending = build_pending_blocks_for_flashblock_batch_tests(
            vec![
                test_flashblock_with_index_tx_count_and_block_number(0, 0, 1),
                test_flashblock_with_index_tx_count_and_block_number(1, 1, 2),
            ],
            vec![
                test_header_for_block(1, B256::with_last_byte(0x11)),
                test_header_for_block(2, B256::with_last_byte(0x22)),
            ],
            vec![(
                test_transaction_with_hash_for_block(latest_hash, 2, Some(0)),
                test_receipt_with_logs_for_block(
                    latest_hash,
                    2,
                    Some(0),
                    vec![test_log_for_block(latest_hash, 2, Some(0), log_addr, Some(0), false)],
                ),
            )],
        );

        let filter = Filter::new().address(unmatched_addr);
        let delta = pending.get_latest_fast_flashblock_logs_delta(11, Some(&filter));

        assert!(delta.logs.is_empty());
        assert!(delta.transactions.is_empty());
    }

    #[test]
    fn get_pending_logs_returns_logs_in_transaction_order() {
        let hash_a = B256::with_last_byte(0xAA);
        let hash_b = B256::with_last_byte(0xBB);
        let hash_c = B256::with_last_byte(0xCC);

        let addr_a = Address::with_last_byte(0x0A);
        let addr_b = Address::with_last_byte(0x0B);
        let addr_c = Address::with_last_byte(0x0C);

        let pending =
            build_pending_blocks_with_logs(&[(hash_a, addr_a), (hash_b, addr_b), (hash_c, addr_c)]);

        let filter = Filter::default();
        let logs = pending.get_pending_logs(&filter);

        assert_eq!(logs.len(), 3, "should return one log per transaction");
        assert_eq!(logs[0].address(), addr_a);
        assert_eq!(logs[1].address(), addr_b);
        assert_eq!(logs[2].address(), addr_c);
    }

    #[test]
    fn filtered_transactions_returns_only_matching_by_address() {
        let hash_a = B256::with_last_byte(0xAA);
        let hash_b = B256::with_last_byte(0xBB);
        let hash_c = B256::with_last_byte(0xCC);

        let addr_a = Address::with_last_byte(0x0A);
        let addr_b = Address::with_last_byte(0x0B);
        let addr_c = Address::with_last_byte(0x0C);

        let pending =
            build_pending_blocks_with_logs(&[(hash_a, addr_a), (hash_b, addr_b), (hash_c, addr_c)]);

        let filter = Filter::new().address(addr_b);
        let txs = pending.get_latest_flashblock_transactions_with_logs_filtered(&filter);

        assert_eq!(txs.len(), 1);
        assert_eq!(txs[0].transaction.tx_hash(), hash_b);
        assert_eq!(txs[0].logs.len(), 1);
        assert_eq!(txs[0].logs[0].address(), addr_b);
    }

    #[test]
    fn filtered_transactions_returns_only_matching_by_topic0() {
        let hash_a = B256::with_last_byte(0xAA);
        let hash_b = B256::with_last_byte(0xBB);

        let addr = Address::with_last_byte(0x01);
        let topic_transfer = B256::with_last_byte(0x01);
        let topic_approval = B256::with_last_byte(0x02);

        let pending = build_pending_blocks_with_topics(&[
            (hash_a, addr, topic_transfer),
            (hash_b, addr, topic_approval),
        ]);

        let filter = Filter::new().event_signature(topic_transfer);
        let txs = pending.get_latest_flashblock_transactions_with_logs_filtered(&filter);

        assert_eq!(txs.len(), 1);
        assert_eq!(txs[0].transaction.tx_hash(), hash_a);
    }

    #[test]
    fn filtered_transactions_returns_all_logs_when_any_matches() {
        let hash_a = B256::with_last_byte(0xAA);
        let addr_match = Address::with_last_byte(0x0A);
        let addr_other = Address::with_last_byte(0x0B);

        let log_match = Log {
            inner: PrimitiveLog {
                address: addr_match,
                data: LogData::new_unchecked(vec![], Bytes::new()),
            },
            block_hash: Some(B256::ZERO),
            block_number: Some(1),
            block_timestamp: None,
            transaction_hash: Some(hash_a),
            transaction_index: Some(0),
            log_index: Some(0),
            removed: false,
        };
        let log_other = Log {
            inner: PrimitiveLog {
                address: addr_other,
                data: LogData::new_unchecked(vec![], Bytes::new()),
            },
            block_hash: Some(B256::ZERO),
            block_number: Some(1),
            block_timestamp: None,
            transaction_hash: Some(hash_a),
            transaction_index: Some(0),
            log_index: Some(1),
            removed: false,
        };

        let receipt = BaseTransactionReceipt {
            inner: alloy_rpc_types_eth::TransactionReceipt {
                inner: ReceiptWithBloom {
                    receipt: BaseReceipt::Legacy(Receipt {
                        status: alloy_consensus::Eip658Value::Eip658(true),
                        cumulative_gas_used: 42_000,
                        logs: vec![log_match, log_other],
                    }),
                    logs_bloom: Bloom::default(),
                },
                transaction_hash: hash_a,
                transaction_index: Some(0),
                block_hash: Some(B256::ZERO),
                block_number: Some(1),
                gas_used: 42_000,
                effective_gas_price: 1_000_000_000,
                blob_gas_used: None,
                blob_gas_price: None,
                from: Address::ZERO,
                to: None,
                contract_address: None,
            },
            l1_block_info: Default::default(),
        };

        let header = Sealed::new_unchecked(Header::default(), B256::ZERO);
        let mut builder = PendingBlocksBuilder::new();
        builder.with_flashblocks([test_flashblock()]);
        builder.with_header(header);
        builder.with_transaction(test_transaction_with_hash(hash_a));
        builder.with_receipt(hash_a, receipt);
        let pending = builder.build().expect("build should succeed");

        let filter = Filter::new().address(addr_match);
        let txs = pending.get_latest_flashblock_transactions_with_logs_filtered(&filter);

        assert_eq!(txs.len(), 1);
        assert_eq!(txs[0].logs.len(), 2, "should return ALL logs, not just matching");
        assert_eq!(txs[0].logs[0].address(), addr_match);
        assert_eq!(txs[0].logs[1].address(), addr_other);
    }

    #[test]
    fn filtered_transactions_returns_none_when_no_match() {
        let hash_a = B256::with_last_byte(0xAA);
        let addr_a = Address::with_last_byte(0x0A);
        let addr_unrelated = Address::with_last_byte(0xFF);

        let pending = build_pending_blocks_with_logs(&[(hash_a, addr_a)]);

        let filter = Filter::new().address(addr_unrelated);
        let txs = pending.get_latest_flashblock_transactions_with_logs_filtered(&filter);

        assert!(txs.is_empty());
    }

    #[test]
    fn filtered_transactions_populates_gas_used() {
        let hash_a = B256::with_last_byte(0xAA);
        let addr_a = Address::with_last_byte(0x0A);

        let pending = build_pending_blocks_with_logs(&[(hash_a, addr_a)]);

        let filter = Filter::new().address(addr_a);
        let txs = pending.get_latest_flashblock_transactions_with_logs_filtered(&filter);

        assert_eq!(txs.len(), 1);
        assert_eq!(txs[0].gas_used, 21_000);
    }

    #[test]
    fn unfiltered_transactions_populates_gas_used() {
        let hash_a = B256::with_last_byte(0xAA);
        let addr_a = Address::with_last_byte(0x0A);

        let pending = build_pending_blocks_with_logs(&[(hash_a, addr_a)]);

        let txs = pending.get_latest_flashblock_transactions_with_logs();

        assert_eq!(txs.len(), 1);
        assert_eq!(txs[0].gas_used, 21_000);
    }

    #[test]
    fn unfiltered_transactions_populate_receipt_fields() {
        let tx_hash = B256::with_last_byte(0xAA);
        let log_address = Address::with_last_byte(0x0A);
        let contract_address = Address::with_last_byte(0x0B);
        let logs_bloom: Bloom = [0x22; 256].into();

        let header = Sealed::new_unchecked(Header::default(), B256::ZERO);
        let mut builder = PendingBlocksBuilder::new();
        builder.with_flashblocks([test_flashblock()]);
        builder.with_header(header);
        builder.with_transaction(test_transaction_with_hash(tx_hash));
        builder.with_receipt(
            tx_hash,
            test_receipt_with_subscription_fields(
                tx_hash,
                log_address,
                contract_address,
                logs_bloom,
            ),
        );
        let pending = builder.build().expect("build should succeed");

        let txs = pending.get_latest_flashblock_transactions_with_logs();

        assert_eq!(txs.len(), 1);
        assert_eq!(txs[0].status, alloy_consensus::Eip658Value::Eip658(true));
        assert_eq!(txs[0].cumulative_gas_used, 42_000);
        assert_eq!(txs[0].contract_address, Some(contract_address));
        assert_eq!(txs[0].logs_bloom, logs_bloom);
    }

    #[test]
    fn filtered_transactions_with_combined_address_and_topic() {
        let hash_a = B256::with_last_byte(0xAA);
        let hash_b = B256::with_last_byte(0xBB);
        let hash_c = B256::with_last_byte(0xCC);

        let addr_usdc = Address::with_last_byte(0x0A);
        let addr_weth = Address::with_last_byte(0x0B);
        let topic_transfer = B256::with_last_byte(0x01);
        let topic_approval = B256::with_last_byte(0x02);

        let pending = build_pending_blocks_with_topics(&[
            (hash_a, addr_usdc, topic_transfer),
            (hash_b, addr_usdc, topic_approval),
            (hash_c, addr_weth, topic_transfer),
        ]);

        let filter = Filter::new().address(addr_usdc).event_signature(topic_transfer);
        let txs = pending.get_latest_flashblock_transactions_with_logs_filtered(&filter);

        assert_eq!(txs.len(), 1);
        assert_eq!(txs[0].transaction.tx_hash(), hash_a);
    }
}
